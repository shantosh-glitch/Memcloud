"""
Layers 2 and 4: placement scheduler + remote read/write.

`MemCloud` is the API an application calls. It looks like a dict, but a block
may physically live in this process's heap or in a peer's heap.

    mc.put("frame_0042", jpeg_bytes)
    data, source, ms = mc.get("frame_0042")   # source: "local" | node id

Placement rule (Layer 2):
    local RAM budget has room          -> store locally
    otherwise                          -> ask the scheduler for a peer
    no peer can take it                -> raise NoCapacity (caller falls to disk)

Scheduler scoring: among peers that can physically fit the block, prefer the
one with the most free reserve, lightly penalised by measured RTT. Ties broken
by system available RAM.
"""

from __future__ import annotations

import socket
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Dict, List, Optional, Tuple

from . import meminfo, protocol
from .discovery import Peer, PeerTable


class NoCapacity(Exception):
    """Neither local budget nor any peer could hold the block."""


class MissingBlock(Exception):
    """The block is not in local RAM and not retrievable from its owner."""


class ConnectionPool:
    """One reusable TCP connection per peer. Removes handshake cost from reads."""

    def __init__(self, ssl_ctx=None) -> None:
        self._conns: Dict[str, socket.socket] = {}
        self._locks: Dict[str, threading.Lock] = {}
        self._guard = threading.Lock()
        self.ssl_ctx = ssl_ctx
        # One TLS handshake per peer, then reused for every block. Keeping the
        # connection open is what stops TLS from costing a handshake per read.

    def _lock_for(self, node_id: str) -> threading.Lock:
        with self._guard:
            if node_id not in self._locks:
                self._locks[node_id] = threading.Lock()
            return self._locks[node_id]

    def call(
        self, peer: Peer, op: str, key: Optional[str] = None, payload: bytes = b"", **extra: Any
    ) -> Tuple[Dict[str, Any], bytes]:
        lock = self._lock_for(peer.node_id)
        with lock:
            for attempt in (0, 1):  # one silent retry on a stale socket
                sock = self._conns.get(peer.node_id)
                if sock is None:
                    sock = protocol.connect(
                        peer.host, peer.port, timeout=15.0, ssl_ctx=self.ssl_ctx
                    )
                    self._conns[peer.node_id] = sock
                try:
                    return protocol.request(sock, op, key, payload, **extra)
                except Exception:
                    try:
                        sock.close()
                    except Exception:
                        pass
                    self._conns.pop(peer.node_id, None)
                    if attempt == 1:
                        raise
            raise RuntimeError("unreachable")

    def drop(self, node_id: str) -> None:
        with self._guard:
            sock = self._conns.pop(node_id, None)
        if sock:
            try:
                sock.close()
            except Exception:
                pass


class Scheduler:
    """Layer 2 placement: pick the worker that should hold a block."""

    def __init__(self, rtt_penalty_per_ms: float = 8 * 1024 * 1024) -> None:
        # 1 ms of extra RTT is treated as being worth 8 MB less free RAM.
        self.rtt_penalty = rtt_penalty_per_ms

    def candidates(self, peers: List[Peer], nbytes: int) -> List[Tuple[Peer, float, str]]:
        scored: List[Tuple[Peer, float, str]] = []
        for p in peers:
            if not p.online:
                scored.append((p, float("-inf"), "offline"))
                continue
            if p.reserve_free < nbytes:
                scored.append((p, float("-inf"), "insufficient reserve"))
                continue
            score = float(p.reserve_free) - (p.rtt_ms * self.rtt_penalty)
            scored.append((p, score, "eligible"))
        scored.sort(key=lambda t: t[1], reverse=True)
        return scored

    def select(self, peers: List[Peer], nbytes: int) -> Optional[Peer]:
        for peer, score, _ in self.candidates(peers, nbytes):
            if score != float("-inf"):
                return peer
        return None


class MemCloud:
    """The distributed memory layer, as seen by an application on this node."""

    def __init__(
        self,
        node_id: str,
        peers: PeerTable,
        local_budget_bytes: int,
        min_free_bytes: int,
        emit=None,
        ssl_ctx=None,
        chunk_bytes: int = 8 * 1024 * 1024,
    ) -> None:
        self.node_id = node_id
        self.peers = peers
        self.local_budget = local_budget_bytes
        self.min_free = min_free_bytes
        self.emit = emit or (lambda *_a, **_k: None)

        self._local: Dict[str, bytes] = {}
        self._lru: Dict[str, float] = {}
        self._local_used = 0
        self._local_used_peak = 0
        self._index: Dict[str, str] = {}   # key -> "local" or peer node id
        self._sizes: Dict[str, int] = {}   # key -> byte length, wherever it lives
        self._lock = threading.RLock()

        self.pool = ConnectionPool(ssl_ctx=ssl_ctx)
        self.scheduler = Scheduler()
        self.chunk_bytes = chunk_bytes
        self._manifests: Dict[str, Dict[str, Any]] = {}

        self.stats = {
            "put_local": 0,
            "put_remote": 0,
            "put_failed": 0,
            "get_local": 0,
            "get_remote": 0,
            "get_miss": 0,
            "spilled_blocks": 0,
            "spilled_bytes": 0,
            "objects": 0,
            "range_reads": 0,
            "bytes_out": 0,
            "bytes_in": 0,
        }

    # -- introspection ------------------------------------------------
    @property
    def local_used(self) -> int:
        with self._lock:
            return self._local_used

    def report(self) -> Dict[str, Any]:
        with self._lock:
            remote_by_node: Dict[str, int] = {}
            for k, loc in self._index.items():
                if loc != "local":
                    remote_by_node[loc] = remote_by_node.get(loc, 0) + self._sizes.get(k, 0)
            return {
                "local_bytes": self._local_used,
                "local_blocks": len(self._local),
                "local_budget": self.local_budget,
                "local_free": max(0, self.local_budget - self._local_used),
                "remote_bytes": sum(remote_by_node.values()),
                "remote_blocks": sum(1 for v in self._index.values() if v != "local"),
                "remote_by_node": remote_by_node,
                "total_keys": len(self._index),
                "stats": dict(self.stats),
            }

    # -- pressure -----------------------------------------------------
    def under_pressure(self) -> Tuple[bool, str]:
        """True when we should stop growing local RAM."""
        with self._lock:
            if self._local_used >= self.local_budget:
                return True, "memcloud local budget exhausted"
        _, avail = meminfo.system_memory()
        if avail and avail < self.min_free:
            return True, "host available RAM below guard threshold"
        return False, ""

    def _fits_locally(self, nbytes: int) -> bool:
        with self._lock:
            if self._local_used + nbytes > self.local_budget:
                return False
        _, avail = meminfo.system_memory()
        if avail and (avail - nbytes) < self.min_free:
            return False
        return True

    # -- write path ---------------------------------------------------
    def put(self, key: str, data: bytes) -> str:
        """Store a block. Returns 'local' or the peer node id that took it."""
        nbytes = len(data)

        if self._fits_locally(nbytes):
            with self._lock:
                old = self._local.pop(key, None)
                if old is not None:
                    self._local_used -= len(old)
                self._local[key] = data
                self._lru[key] = time.time()
                self._local_used += nbytes
                self._index[key] = "local"
                self._sizes[key] = nbytes
                self.stats["put_local"] += 1
            return "local"

        peer = self.scheduler.select(self.peers.online(), nbytes)
        if peer is None:
            with self._lock:
                self.stats["put_failed"] += 1
            raise NoCapacity(
                f"no local budget and no peer can hold {meminfo.human(nbytes)}"
            )

        header, _ = self.pool.call(
            peer, protocol.OP_PUT, key, data, owner=self.node_id
        )
        if not header.get("ok"):
            with self._lock:
                self.stats["put_failed"] += 1
            raise NoCapacity(f"peer {peer.name} refused block: {header.get('err')}")

        with self._lock:
            old = self._local.pop(key, None)
            if old is not None:
                self._local_used -= len(old)
                self._lru.pop(key, None)
            self._index[key] = peer.node_id
            self._sizes[key] = nbytes
            self.stats["put_remote"] += 1
            self.stats["bytes_out"] += nbytes
        peer.reserve_free = max(0, peer.reserve_free - nbytes)
        self.emit(
            "remote_alloc",
            {"key": key, "bytes": nbytes, "node": peer.name, "node_id": peer.node_id},
        )
        return peer.node_id

    # -- read path ----------------------------------------------------
    def get(self, key: str) -> Tuple[bytes, str, float]:
        """Return (data, source, milliseconds). source is 'local' or a node id."""
        t0 = time.perf_counter()
        with self._lock:
            data = self._local.get(key)
            if data is not None:
                self._lru[key] = time.time()
                self.stats["get_local"] += 1
                return data, "local", (time.perf_counter() - t0) * 1000.0
            loc = self._index.get(key)

        if loc is None or loc == "local":
            with self._lock:
                self.stats["get_miss"] += 1
            raise MissingBlock(key)

        peer = self.peers.get(loc)
        if peer is None or not peer.online:
            with self._lock:
                self.stats["get_miss"] += 1
            raise MissingBlock(f"{key}: owner {loc} unavailable")

        header, payload = self.pool.call(peer, protocol.OP_GET, key)
        if not header.get("ok"):
            with self._lock:
                self.stats["get_miss"] += 1
            raise MissingBlock(f"{key}: {header.get('err')}")

        with self._lock:
            self.stats["get_remote"] += 1
            self.stats["bytes_in"] += len(payload)
        return payload, peer.node_id, (time.perf_counter() - t0) * 1000.0

    def delete(self, key: str) -> bool:
        with self._lock:
            loc = self._index.pop(key, None)
            self._sizes.pop(key, None)
            data = self._local.pop(key, None)
            if data is not None:
                self._local_used -= len(data)
                self._lru.pop(key, None)
                return True
        if loc and loc != "local":
            peer = self.peers.get(loc)
            if peer:
                try:
                    self.pool.call(peer, protocol.OP_DEL, key)
                    return True
                except Exception:
                    return False
        return False

    def location(self, key: str) -> Optional[str]:
        with self._lock:
            return self._index.get(key)

    # -- Layer 6: spill ------------------------------------------------
    def spill(self, target_bytes: int) -> Dict[str, Any]:
        """Move least-recently-used LOCAL blocks to peers to free local RAM.

        This is the visible 'Node A RAM drops, Node B RAM rises' moment.
        """
        moved = 0
        freed = 0
        errors: List[str] = []
        while freed < target_bytes:
            with self._lock:
                if not self._lru:
                    break
                key = min(self._lru, key=lambda k: self._lru[k])
                data = self._local.get(key)
                if data is None:
                    self._lru.pop(key, None)
                    continue
            nbytes = len(data)
            peer = self.scheduler.select(self.peers.online(), nbytes)
            if peer is None:
                errors.append("no peer with capacity")
                break
            try:
                header, _ = self.pool.call(
                    peer, protocol.OP_PUT, key, data, owner=self.node_id
                )
            except Exception as exc:
                errors.append(str(exc))
                break
            if not header.get("ok"):
                errors.append(str(header.get("err")))
                break
            with self._lock:
                self._local.pop(key, None)
                self._lru.pop(key, None)
                self._local_used -= nbytes
                self._index[key] = peer.node_id
                self.stats["spilled_blocks"] += 1
                self.stats["spilled_bytes"] += nbytes
                self.stats["bytes_out"] += nbytes
            peer.reserve_free = max(0, peer.reserve_free - nbytes)
            moved += 1
            freed += nbytes

        self.emit("spill", {"blocks": moved, "bytes": freed, "errors": errors})
        return {"blocks_moved": moved, "bytes_freed": freed, "errors": errors}

    # -- range reads --------------------------------------------------
    def get_range(self, key: str, off: int, length: int) -> Tuple[bytes, str]:
        """Read a byte slice of one block. Only the slice crosses the network."""
        with self._lock:
            data = self._local.get(key)
            if data is not None:
                self._lru[key] = time.time()
                self.stats["range_reads"] += 1
                end = len(data) if length < 0 else off + length
                return bytes(memoryview(data)[off:end]), "local"
            loc = self._index.get(key)
        if loc is None or loc == "local":
            raise MissingBlock(key)
        peer = self.peers.get(loc)
        if peer is None or not peer.online:
            raise MissingBlock(f"{key}: owner {loc} unavailable")
        header, payload = self.pool.call(
            peer, protocol.OP_GET, key, off=off, len=length
        )
        if not header.get("ok"):
            raise MissingBlock(f"{key}: {header.get('err')}")
        with self._lock:
            self.stats["range_reads"] += 1
            self.stats["bytes_in"] += len(payload)
        return payload, peer.node_id

    # -- chunked objects (Layer 8 building block) ----------------------
    def put_object(self, name: str, data: bytes, meta: Optional[Dict[str, Any]] = None,
                   chunk_bytes: Optional[int] = None) -> Dict[str, Any]:
        """Store a large object as independently placed chunks.

        A 4 GB tensor does not have to fit in any single peer: chunks are
        scheduled one at a time, so capacity aggregates across the cluster and
        reads can be fetched from several peers in parallel.
        """
        cb = chunk_bytes or self.chunk_bytes
        n = max(1, (len(data) + cb - 1) // cb)
        placement: List[str] = []
        view = memoryview(data)
        for i in range(n):
            ck = f"{name}#c{i:05d}"
            placement.append(self.put(ck, bytes(view[i * cb : (i + 1) * cb])))
        manifest = {
            "name": name,
            "size": len(data),
            "chunk_bytes": cb,
            "chunks": n,
            "placement": placement,
            "meta": meta or {},
            "created": time.time(),
        }
        with self._lock:
            self._manifests[name] = manifest
            self.stats["objects"] += 1
        self.emit("object_stored", {
            "name": name, "size": len(data), "chunks": n,
            "remote_chunks": sum(1 for p in placement if p != "local"),
        })
        return manifest

    def object_manifest(self, name: str) -> Optional[Dict[str, Any]]:
        with self._lock:
            return self._manifests.get(name)

    def get_object(self, name: str, parallel: int = 4) -> Tuple[bytes, Dict[str, Any]]:
        """Reassemble a chunked object, fetching remote chunks concurrently."""
        man = self.object_manifest(name)
        if man is None:
            raise MissingBlock(f"unknown object {name}")
        keys = [f"{name}#c{i:05d}" for i in range(man["chunks"])]
        parts: List[Optional[bytes]] = [None] * len(keys)

        def fetch(i: int) -> None:
            parts[i] = self.get(keys[i])[0]

        if parallel > 1 and len(keys) > 1:
            with ThreadPoolExecutor(max_workers=min(parallel, len(keys))) as ex:
                list(ex.map(fetch, range(len(keys))))
        else:
            for i in range(len(keys)):
                fetch(i)
        return b"".join(p or b"" for p in parts), man

    def get_object_range(self, name: str, off: int, length: int) -> bytes:
        """Read a slice of a chunked object, touching only the chunks it spans."""
        man = self.object_manifest(name)
        if man is None:
            raise MissingBlock(f"unknown object {name}")
        cb = man["chunk_bytes"]
        end = min(man["size"], off + length)
        out = bytearray()
        pos = off
        while pos < end:
            idx = pos // cb
            c_off = pos - idx * cb
            take = min(cb - c_off, end - pos)
            piece, _src = self.get_range(f"{name}#c{idx:05d}", c_off, take)
            out += piece
            pos += take
        return bytes(out)

    def delete_object(self, name: str) -> int:
        man = self.object_manifest(name)
        if man is None:
            return 0
        n = 0
        for i in range(man["chunks"]):
            if self.delete(f"{name}#c{i:05d}"):
                n += 1
        with self._lock:
            self._manifests.pop(name, None)
        return n

    def clear(self) -> None:
        with self._lock:
            remote = {k: v for k, v in self._index.items() if v != "local"}
            self._local.clear()
            self._lru.clear()
            self._index.clear()
            self._sizes.clear()
            self._manifests.clear()
            self._local_used = 0
            for k in self.stats:
                self.stats[k] = 0
        for key, node_id in remote.items():
            peer = self.peers.get(node_id)
            if peer:
                try:
                    self.pool.call(peer, protocol.OP_DEL, key)
                except Exception:
                    pass
