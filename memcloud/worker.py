"""
MemCloud worker (Layer 3: Remote RAM).

A threaded TCP server. When a peer sends PUT, the payload bytes are held in
this process's own heap -- `self._blocks[key] = payload`. That is a real
resident allocation in this machine's physical RAM, visible as process RSS
growth and as a drop in system available memory.

The worker enforces its own admission limit (`reserve_bytes`) so a peer can
never push this laptop into swap.
"""

from __future__ import annotations

import socket
import socketserver
import threading
import time
from typing import Any, Dict, List, Optional, Tuple

from . import meminfo, protocol


class BlockStore:
    """Thread-safe in-RAM block store with an admission budget."""

    def __init__(self, reserve_bytes: int, min_free_bytes: int) -> None:
        self.reserve_bytes = reserve_bytes
        self.min_free_bytes = min_free_bytes
        self._blocks: Dict[str, bytes] = {}
        self._owner: Dict[str, str] = {}
        self._touched: Dict[str, float] = {}
        self._used = 0
        self._lock = threading.RLock()
        self.puts = 0
        self.gets = 0
        self.rejected = 0

    # -- admission ----------------------------------------------------
    def can_accept(self, nbytes: int) -> Tuple[bool, str]:
        with self._lock:
            if self._used + nbytes > self.reserve_bytes:
                return False, (
                    f"reserve exhausted: used={self._used} + {nbytes} "
                    f"> reserve={self.reserve_bytes}"
                )
        _, avail = meminfo.system_memory()
        if avail and (avail - nbytes) < self.min_free_bytes:
            return False, (
                f"host memory guard: available={avail} would fall below "
                f"min_free={self.min_free_bytes}"
            )
        return True, ""

    # -- ops ----------------------------------------------------------
    def put(self, key: str, data: bytes, owner: str) -> Tuple[bool, str]:
        ok, why = self.can_accept(len(data) - len(self._blocks.get(key, b"")))
        if not ok:
            with self._lock:
                self.rejected += 1
            return False, why
        with self._lock:
            self._used -= len(self._blocks.get(key, b""))
            self._blocks[key] = data
            self._owner[key] = owner
            self._touched[key] = time.time()
            self._used += len(data)
            self.puts += 1
        return True, ""

    def get(self, key: str) -> Optional[bytes]:
        with self._lock:
            data = self._blocks.get(key)
            if data is not None:
                self._touched[key] = time.time()
                self.gets += 1
            return data

    def get_range(self, key: str, off: int, length: int) -> Optional[bytes]:
        """Return a byte slice without copying the whole block first.

        Tensor workloads read slices of large blocks; sending the entire block
        for a 4 KB slice would waste the link.
        """
        with self._lock:
            data = self._blocks.get(key)
            if data is None:
                return None
            self._touched[key] = time.time()
            self.gets += 1
            if length < 0:
                return bytes(memoryview(data)[off:])
            return bytes(memoryview(data)[off : off + length])

    def delete(self, key: str) -> bool:
        with self._lock:
            data = self._blocks.pop(key, None)
            if data is None:
                return False
            self._used -= len(data)
            self._owner.pop(key, None)
            self._touched.pop(key, None)
            return True

    def keys(self, owner: Optional[str] = None) -> List[str]:
        with self._lock:
            if owner is None:
                return list(self._blocks)
            return [k for k, o in self._owner.items() if o == owner]

    def clear(self) -> int:
        with self._lock:
            n = len(self._blocks)
            self._blocks.clear()
            self._owner.clear()
            self._touched.clear()
            self._used = 0
            return n

    # -- reporting ----------------------------------------------------
    @property
    def used(self) -> int:
        with self._lock:
            return self._used

    @property
    def count(self) -> int:
        with self._lock:
            return len(self._blocks)

    def stat(self) -> Dict[str, Any]:
        total, avail = meminfo.system_memory()
        with self._lock:
            used = self._used
            count = len(self._blocks)
            owners: Dict[str, int] = {}
            for k, o in self._owner.items():
                owners[o] = owners.get(o, 0) + len(self._blocks[k])
        return {
            "hosted_bytes": used,
            "hosted_blocks": count,
            "hosted_by_owner": owners,
            "reserve_bytes": self.reserve_bytes,
            "reserve_free": max(0, self.reserve_bytes - used),
            "ram_total": total,
            "ram_available": avail,
            "process_rss": meminfo.process_rss(),
            "puts": self.puts,
            "gets": self.gets,
            "rejected": self.rejected,
        }


class _Handler(socketserver.BaseRequestHandler):
    def setup(self) -> None:
        """Complete the TLS handshake here, in the per-connection thread, so a
        slow or hostile client cannot stall the accept loop. A client that
        cannot present the cluster certificate never reaches handle()."""
        node = self.server.node  # type: ignore[attr-defined]
        ctx = getattr(node, "ssl_server_ctx", None)
        self.request.settimeout(30.0)
        if ctx is not None:
            self.request = ctx.wrap_socket(self.request, server_side=True)

    def handle(self) -> None:  # noqa: C901
        sock = self.request
        try:
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        except OSError:
            pass
        sock.settimeout(120.0)
        node = self.server.node  # type: ignore[attr-defined]
        store: BlockStore = node.store
        try:
            while True:
                try:
                    header, payload = protocol.recv_frame(sock)
                except protocol.ProtocolError:
                    return
                except (ConnectionResetError, socket.timeout, OSError):
                    return

                op = header.get("op")
                key = header.get("key", "")
                owner = header.get("owner", "unknown")

                if op == protocol.OP_PING:
                    protocol.send_frame(
                        sock, {"ok": True, "node": node.identity(), "stat": store.stat()}
                    )

                elif op == protocol.OP_PUT:
                    ok, why = store.put(key, payload, owner)
                    protocol.send_frame(sock, {"ok": ok, "err": why or None})
                    if ok:
                        node.emit(
                            "block_stored",
                            {"key": key, "bytes": len(payload), "owner": owner},
                        )

                elif op == protocol.OP_GET:
                    off = int(header.get("off", 0))
                    length = int(header.get("len", -1))
                    if off or length >= 0:
                        data = store.get_range(key, off, length)
                    else:
                        data = store.get(key)
                    if data is None:
                        protocol.send_frame(sock, {"ok": False, "err": "not found"})
                    else:
                        protocol.send_frame(sock, {"ok": True}, data)

                elif op == protocol.OP_MGET:
                    keys = header.get("keys") or []
                    parts, sizes, missing = [], [], []
                    for k in keys:
                        d = store.get(k)
                        if d is None:
                            missing.append(k)
                            sizes.append(0)
                        else:
                            parts.append(d)
                            sizes.append(len(d))
                    protocol.send_frame(
                        sock,
                        {"ok": not missing, "sizes": sizes, "missing": missing},
                        b"".join(parts),
                    )

                elif op == protocol.OP_DEL:
                    protocol.send_frame(sock, {"ok": store.delete(key)})

                elif op == protocol.OP_STAT:
                    protocol.send_frame(
                        sock, {"ok": True, "stat": store.stat(), "node": node.identity()}
                    )

                elif op == protocol.OP_KEYS:
                    protocol.send_frame(
                        sock, {"ok": True, "keys": store.keys(header.get("owner"))}
                    )

                else:
                    protocol.send_frame(sock, {"ok": False, "err": f"unknown op {op!r}"})
        except Exception as exc:  # keep one bad peer from killing the worker
            try:
                protocol.send_frame(sock, {"ok": False, "err": str(exc)})
            except Exception:
                pass


class _Server(socketserver.ThreadingTCPServer):
    allow_reuse_address = True
    daemon_threads = True

    def __init__(self, addr, handler, node) -> None:
        self.node = node
        self.rejected_connections = 0
        super().__init__(addr, handler)

    def handle_error(self, request, client_address) -> None:
        """A failed TLS handshake is a rejected intruder, not a crash.

        Print one line instead of a stack trace so the demo terminal stays
        readable, and count it so the rejection is still visible.
        """
        import ssl as _ssl
        import sys as _sys

        exc = _sys.exc_info()[1]
        if isinstance(exc, (_ssl.SSLError, _ssl.SSLCertVerificationError, OSError)):
            self.rejected_connections += 1
            reason = getattr(exc, "reason", None) or type(exc).__name__
            print(f"  [security] rejected {client_address[0]}:{client_address[1]} "
                  f"-- {reason}", flush=True)
            try:
                self.node.emit("connection_rejected",
                               {"peer": f"{client_address[0]}:{client_address[1]}",
                                "reason": str(reason)})
            except Exception:
                pass
            return
        super().handle_error(request, client_address)


class Worker:
    """Owns the data-plane listener."""

    def __init__(self, node, host: str, port: int) -> None:
        self.node = node
        self.host = host
        self.port = port
        self._server: Optional[_Server] = None
        self._thread: Optional[threading.Thread] = None

    def start(self) -> int:
        self._server = _Server((self.host, self.port), _Handler, self.node)
        self.port = self._server.server_address[1]
        self._thread = threading.Thread(
            target=self._server.serve_forever, name="memcloud-worker", daemon=True
        )
        self._thread.start()
        return self.port

    def stop(self) -> None:
        if self._server:
            self._server.shutdown()
            self._server.server_close()
