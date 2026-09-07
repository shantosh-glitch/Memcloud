# LLM notes — why key/value is fine, and what actually had to change

You asked whether a key/value block store is feasible for the LLM phase.
Short answer: **the key/value shape is not the problem. Granularity and
access frequency are.**

A block store is only workable when the number of network round trips is
small relative to the amount of useful work each trip buys. So the question
is never "key/value or not" — it is "how often does the model touch this data,
and how big is each touch?"

---

## The numbers that decide it

Assume a realistic laptop LAN: **1 Gbps ethernet ≈ 110 MB/s practical**, or
**Wi-Fi 5/6 ≈ 30–70 MB/s**, with **1–25 ms round-trip latency**.

Reference model: **Llama-3-8B** — 32 layers, 8 KV heads (grouped-query
attention), head_dim 128, fp16.

> These are arithmetic estimates derived from published architecture
> parameters, not measurements on your hardware. Measure before quoting them.

### 1. Model weights, paged per layer — NOT VIABLE

Every generated token touches every layer. 8B params in fp16 ≈ 16 GB.
Streaming that per token at 110 MB/s is **~145 seconds per token**.

Do not attempt this. If a reviewer asks "can you run a 70B model by pooling
laptop RAM?", the honest answer is no, not over a LAN.

### 2. Live KV cache during decode — NOT VIABLE

At 30 tokens/s, the whole per-token budget is **~33 ms**, spread across 32
layers → roughly **1 ms per layer**. A LAN round trip is 1–25 ms. You lose on
the first layer.

This *does* work in datacenters, over RDMA / InfiniBand, at single-digit
microseconds. A laptop LAN is about three orders of magnitude away from that.
Anyone who says "just put the KV cache on the other machine" is describing
NVLink-class hardware, not Wi-Fi.

### 3. Precomputed **prefix** KV cache, reused across requests — VIABLE

This is the one that fits, and it is what `memcloud/llm.py` implements.

A long shared prefix — a system prompt, a retrieved document, a codebase — is
prefilled **once**. Its KV cache is stored. A later request with the identical
prefix fetches that cache in **one bulk sequential transfer** instead of
re-running prefill.

Sizing:

```
bytes/token = 2 (K and V) × layers × kv_heads × head_dim × dtype_bytes
            = 2 × 32 × 8 × 128 × 2
            = 131,072 bytes = 128 KiB per token

4,096 tokens ≈ 512 MiB
```

At ~110 MB/s that transfer is **~4.7 s**. Whether that beats re-prefilling
4,096 tokens depends entirely on the hardware: on a weak laptop CPU, prefill
is often much slower than 4.7 s, so you win. On a decent GPU, prefill is
faster, so you lose. **Measure both sides before claiming a speedup.**

The honest framing for a reviewer: this is a *capacity* win first and a
*latency* win only sometimes. It lets you keep many more prefix caches
resident than one laptop's RAM allows.

### 4. RAG corpora, embeddings, image latents, activations, datasets — VIABLE

Accessed at human timescales or in bulk. Comfortable fit, lowest risk. If you
want an AI demo that cannot embarrass you, put a vector corpus in cluster RAM
and show retrieval working.

---

## What changed in the code

The block store stayed. Four things were added around it, because moving a
512 MB tensor as one opaque value through a single `get()` would have been
unusable.

### Range reads
`GET` now takes `off` / `len`. Reading 4 KB out of a 512 MB block moves 4 KB,
not 512 MB.

```python
piece, src = mc.get_range("kv_abc", off=1_048_576, length=4096)
```

### Chunked, striped objects
`put_object()` splits a large value into chunks (8 MB default) and schedules
each one independently.

Two consequences that matter:

* **Capacity aggregates.** A 4 GB tensor never has to fit in any single peer.
  With three laptops holding 2 GB each, it still lands.
* **Bandwidth aggregates.** `get_object()` fetches chunks from several peers
  concurrently instead of serialising on one link.

```python
manifest = mc.put_object("kv_cache_doc42", raw_bytes, chunk_bytes=8*1024*1024)
whole, man = mc.get_object("kv_cache_doc42", parallel=4)
slice_ = mc.get_object_range("kv_cache_doc42", off, length)   # spans chunks
```

### MGET
One round trip for many small blocks. RTT is amortised across the batch
instead of paid per element.

### Tensor and KV abstractions (`memcloud/llm.py`)

`TensorStore` — arrays with dtype and shape metadata, chunked and striped,
with `get_rows()` to pull a slice along axis 0 without moving the tensor.
numpy is used if present; raw `bytes` work without it.

`PrefixKVStore` — content-addressed prefix cache. The key is
`sha256(model ‖ tokenizer ‖ token_ids)`, so:

* a hit is **exact** — it can never be a near-miss on similar text,
* a different model or tokenizer can never collide with the same prompt,
* a miss is safe: you just prefill normally.

```python
rec = node.kv.store("llama3-8b", "tok-v1", token_ids, kv_bytes,
                    shape=[...], dtype="float16")
kv, rec, ms = node.kv.fetch("llama3-8b", "tok-v1", token_ids)
```

`PrefixKVStore.estimate_bytes(n_tokens, layers, kv_heads, head_dim, dtype)`
gives you the sizing arithmetic above, so you can budget before storing.

---

## Verified in `tests/test_e2e.py`

* A 60 MB object splits into 8 chunks; 5 stay local, 3 land on the peer, and
  it reassembles byte-exact.
* A range read spanning a chunk boundary returns exactly the right bytes.
* A KV cache stored under a token-id hash returns identical bytes; a prefix
  differing by one token is a clean miss.

## Still not done

* **No numpy-native zero-copy path.** Bytes are copied on reassembly. For
  multi-GB tensors that copy is itself expensive.
* **No integration with a real inference engine.** Nothing here has been
  wired into llama.cpp, vLLM, or Ollama. `PrefixKVStore` is the storage side
  only; extracting and re-injecting a real KV cache is engine-specific work
  and is genuinely non-trivial.
* **No compression.** KV caches quantise well (fp16 → int8 halves the
  transfer). Not implemented.
* **No measured comparison** of fetch-vs-reprefill on your hardware. Until
  that exists, do not claim a speedup — claim capacity.
