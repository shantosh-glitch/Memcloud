"""
Layer 8 building blocks: LLM/AI data structures on top of MemCloud.

Why a plain key/value block store is not enough for LLM work
------------------------------------------------------------
The block store is fine. The problem is *granularity* and *access frequency*,
not the key/value shape. A single round trip per access is only acceptable
when accesses are rare relative to the round-trip time.

Three LLM data structures, and whether remote RAM fits:

1. **Model weights, paged per layer.** Every generated token touches every
   layer. An 8B model in fp16 is ~16 GB; streaming that per token over a
   1 Gbps LAN (~110 MB/s practical) is ~145 s per token. **Not viable.**
   Do not attempt this.

2. **Live KV cache during decode.** At 30 tokens/s the whole token budget is
   ~33 ms, spread over ~32 layers, so roughly 1 ms per layer. LAN round trips
   are 1-25 ms on Wi-Fi. **Not viable** for interactive decode. This works in
   datacenters over RDMA/InfiniBand at single-digit microseconds; a laptop LAN
   is three orders of magnitude away from that.

3. **Precomputed prefix KV cache, reused across requests.** This is the one
   that fits. A long shared prefix -- a system prompt, a retrieved document,
   a codebase -- is prefilled once, and its KV cache is stored. A later
   request with the same prefix fetches the cache in **one bulk transfer**
   instead of re-running prefill. It is a single large sequential read, not a
   per-token round trip, so it amortises.

   Rough sizing for Llama-3-8B (32 layers, 8 KV heads with GQA, head_dim 128,
   fp16):
       bytes/token = 2 (K and V) * 32 * 8 * 128 * 2 = 131,072 = 128 KiB
       4,096 tokens ~= 512 MiB
   At ~110 MB/s that transfer is ~4.7 s. Whether that beats re-prefilling
   4,096 tokens depends entirely on the machine -- on a weak laptop CPU
   prefill can be far slower than 4.7 s, on a good GPU it is faster.
   **These are arithmetic estimates from published architecture parameters,
   not measurements. Measure both sides before claiming a win.**

4. **RAG / embedding corpora, image latents, activations, datasets.**
   Accessed at human timescales or in bulk. **Comfortably viable**, and the
   least risky place to demo an AI workload.

What this module provides
-------------------------
* `TensorStore`  -- store/fetch arrays with dtype+shape metadata, chunked and
  striped across peers, with slice reads that move only the bytes needed.
* `PrefixKVStore` -- content-addressed prefix KV cache: key is a hash of
  (model, tokenizer, prefix tokens), so a hit is exact and a miss is safe.

numpy is optional. Without it these work on raw `bytes`.
"""

from __future__ import annotations

import hashlib
import json
import math
import threading
import time
from typing import Any, Dict, List, Optional, Sequence, Tuple

try:
    import numpy as _np

    HAS_NUMPY = True
except Exception:  # pragma: no cover
    _np = None
    HAS_NUMPY = False

from .client import MemCloud, MissingBlock

_DTYPE_SIZE = {
    "float64": 8, "float32": 4, "float16": 2, "bfloat16": 2,
    "int64": 8, "int32": 4, "int16": 2, "int8": 1, "uint8": 1,
}


def dtype_size(dtype: str) -> int:
    if dtype not in _DTYPE_SIZE:
        raise ValueError(f"unsupported dtype {dtype!r}")
    return _DTYPE_SIZE[dtype]


def nbytes_for(shape: Sequence[int], dtype: str) -> int:
    n = 1
    for d in shape:
        n *= int(d)
    return n * dtype_size(dtype)


class TensorStore:
    """Arrays in distributed RAM, with metadata and slice reads."""

    def __init__(self, mc: MemCloud, chunk_bytes: int = 8 * 1024 * 1024) -> None:
        self.mc = mc
        self.chunk_bytes = chunk_bytes
        self._meta: Dict[str, Dict[str, Any]] = {}
        self._lock = threading.RLock()

    def put(self, name: str, array_or_bytes, shape=None, dtype=None) -> Dict[str, Any]:
        if HAS_NUMPY and isinstance(array_or_bytes, _np.ndarray):
            arr = _np.ascontiguousarray(array_or_bytes)
            raw = arr.tobytes()
            shape = list(arr.shape)
            dtype = str(arr.dtype)
        else:
            raw = bytes(array_or_bytes)
            if shape is None or dtype is None:
                raise ValueError("shape and dtype required for raw bytes")
            shape = list(shape)
            expect = nbytes_for(shape, dtype)
            if expect != len(raw):
                raise ValueError(f"shape/dtype imply {expect} bytes, got {len(raw)}")

        meta = {"shape": shape, "dtype": dtype, "itemsize": dtype_size(dtype)}
        man = self.mc.put_object(name, raw, meta=meta, chunk_bytes=self.chunk_bytes)
        with self._lock:
            self._meta[name] = meta
        return man

    def meta(self, name: str) -> Optional[Dict[str, Any]]:
        man = self.mc.object_manifest(name)
        return man["meta"] if man else None

    def get(self, name: str, parallel: int = 4):
        raw, man = self.mc.get_object(name, parallel=parallel)
        meta = man["meta"]
        if HAS_NUMPY and meta.get("dtype") in _DTYPE_SIZE and meta["dtype"] != "bfloat16":
            return _np.frombuffer(raw, dtype=meta["dtype"]).reshape(meta["shape"])
        return raw

    def get_rows(self, name: str, start: int, count: int):
        """Fetch `count` rows along axis 0 without moving the whole tensor."""
        man = self.mc.object_manifest(name)
        if man is None:
            raise MissingBlock(name)
        meta = man["meta"]
        shape, dtype = meta["shape"], meta["dtype"]
        row = nbytes_for(shape[1:], dtype) if len(shape) > 1 else dtype_size(dtype)
        raw = self.mc.get_object_range(name, start * row, count * row)
        if HAS_NUMPY and dtype != "bfloat16":
            return _np.frombuffer(raw, dtype=dtype).reshape([count] + list(shape[1:]))
        return raw

    def delete(self, name: str) -> int:
        with self._lock:
            self._meta.pop(name, None)
        return self.mc.delete_object(name)


def prefix_key(model: str, tokenizer: str, tokens: Sequence[int]) -> str:
    """Content address for a prompt prefix.

    Hashing the token ids (not the text) means the key matches exactly what
    the model actually consumed, so a hit can never be a near-miss.
    """
    h = hashlib.sha256()
    h.update(model.encode())
    h.update(b"\x00")
    h.update(tokenizer.encode())
    h.update(b"\x00")
    h.update(len(tokens).to_bytes(8, "big"))
    for t in tokens:
        h.update(int(t).to_bytes(4, "big"))
    return "kv_" + h.hexdigest()[:32]


class PrefixKVStore:
    """Precomputed prefix KV caches held in cluster RAM.

    Intended use: prefill a long shared prefix once, store the KV cache, and
    let any node reuse it instead of re-running prefill. This is a bulk
    sequential transfer, which is the only KV-cache pattern a LAN can carry.

    It is NOT a live decode-time KV cache. See the module docstring.
    """

    def __init__(self, mc: MemCloud, chunk_bytes: int = 16 * 1024 * 1024) -> None:
        self.mc = mc
        self.tensors = TensorStore(mc, chunk_bytes=chunk_bytes)
        self.hits = 0
        self.misses = 0
        self._index: Dict[str, Dict[str, Any]] = {}
        self._lock = threading.RLock()

    @staticmethod
    def estimate_bytes(
        n_tokens: int, layers: int, kv_heads: int, head_dim: int, dtype: str = "float16"
    ) -> int:
        """Arithmetic estimate of KV cache size. K and V, per layer, per token."""
        return 2 * layers * kv_heads * head_dim * dtype_size(dtype) * n_tokens

    def store(
        self,
        model: str,
        tokenizer: str,
        tokens: Sequence[int],
        kv_bytes,
        shape: Optional[Sequence[int]] = None,
        dtype: Optional[str] = None,
        extra: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        key = prefix_key(model, tokenizer, tokens)
        man = self.tensors.put(key, kv_bytes, shape=shape, dtype=dtype)
        rec = {
            "key": key,
            "model": model,
            "tokenizer": tokenizer,
            "n_tokens": len(tokens),
            "bytes": man["size"],
            "chunks": man["chunks"],
            "remote_chunks": sum(1 for p in man["placement"] if p != "local"),
            "extra": extra or {},
            "stored_at": time.time(),
        }
        with self._lock:
            self._index[key] = rec
        return rec

    def lookup(self, model: str, tokenizer: str, tokens: Sequence[int]) -> Optional[Dict]:
        key = prefix_key(model, tokenizer, tokens)
        with self._lock:
            return self._index.get(key)

    def fetch(self, model: str, tokenizer: str, tokens: Sequence[int]):
        """Return (kv, record, ms) or (None, None, ms) on a miss."""
        t0 = time.perf_counter()
        rec = self.lookup(model, tokenizer, tokens)
        if rec is None:
            with self._lock:
                self.misses += 1
            return None, None, (time.perf_counter() - t0) * 1000.0
        kv = self.tensors.get(rec["key"])
        with self._lock:
            self.hits += 1
        return kv, rec, (time.perf_counter() - t0) * 1000.0

    def report(self) -> Dict[str, Any]:
        with self._lock:
            entries = list(self._index.values())
            hits, misses = self.hits, self.misses
        total = sum(e["bytes"] for e in entries)
        return {
            "entries": len(entries),
            "total_bytes": total,
            "remote_chunks": sum(e["remote_chunks"] for e in entries),
            "hits": hits,
            "misses": misses,
            "hit_rate": round(100.0 * hits / (hits + misses), 1) if (hits + misses) else 0.0,
            "recent": sorted(entries, key=lambda e: -e["stored_at"])[:20],
        }

    def evict(self, model: str, tokenizer: str, tokens: Sequence[int]) -> int:
        key = prefix_key(model, tokenizer, tokens)
        with self._lock:
            self._index.pop(key, None)
        return self.tensors.delete(key)
