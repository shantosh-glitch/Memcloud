"""
MemCloud data-plane wire protocol.

One frame on the wire:

    +--------------------+------------------------+-----------------+
    | header_len  uint32 | header  JSON (utf-8)   | payload  bytes  |
    |   big endian, 4 B  |   header_len bytes     |  header["size"] |
    +--------------------+------------------------+-----------------+

`payload` is the raw block of memory being moved. It is never base64'd or
JSON-wrapped, so a 1 MB frame costs 1 MB on the wire plus a ~120 byte header.

Requests   header: {"op": ..., "key": ..., "size": n, ...}
Responses  header: {"ok": bool, "err": str|None, "size": n, ...}

Operations
    PING   liveness + telemetry exchange
    PUT    store payload in the worker's RAM under `key`
    GET    return the block under `key`; optional `off`/`len` for a byte range
    MGET   return several blocks in one round trip (payload = concatenation,
           boundaries in the response header's `sizes` list)
    DEL    drop the block under `key`
    STAT   worker memory report (no payload)
    KEYS   list keys currently held for the caller

Range reads and MGET exist for tensor-shaped workloads: they let a caller pull
a slice of a large block, or many small blocks, without paying one network
round trip per element. See LLM_NOTES.md.

Transport: when a TLS context is supplied to `connect`, the socket is wrapped
before any frame is written, so the length prefix and every payload byte are
inside the TLS record layer.
"""

from __future__ import annotations

import json
import socket
import struct
from typing import Any, Dict, Optional, Tuple

MAGIC_HEADER_LEN = struct.Struct(">I")

# Refuse anything absurd so a corrupt stream cannot allocate the heap away.
MAX_HEADER = 1 << 20  # 1 MB of JSON header
MAX_PAYLOAD = 1 << 32  # 4 GB single block ceiling

OP_PING = "PING"
OP_PUT = "PUT"
OP_GET = "GET"
OP_DEL = "DEL"
OP_STAT = "STAT"
OP_KEYS = "KEYS"
OP_MGET = "MGET"


class ProtocolError(Exception):
    pass


def _recv_exact(sock: socket.socket, n: int) -> bytes:
    """Read exactly n bytes or raise. memoryview avoids copying big blocks."""
    if n == 0:
        return b""
    buf = bytearray(n)
    view = memoryview(buf)
    got = 0
    while got < n:
        chunk = sock.recv_into(view[got:], n - got)
        if chunk == 0:
            raise ProtocolError(f"connection closed after {got}/{n} bytes")
        got += chunk
    return bytes(buf)


def send_frame(sock: socket.socket, header: Dict[str, Any], payload: bytes = b"") -> None:
    header = dict(header)
    header["size"] = len(payload)
    raw = json.dumps(header, separators=(",", ":")).encode("utf-8")
    if len(raw) > MAX_HEADER:
        raise ProtocolError("header too large")
    sock.sendall(MAGIC_HEADER_LEN.pack(len(raw)))
    sock.sendall(raw)
    if payload:
        sock.sendall(payload)


def recv_frame(sock: socket.socket) -> Tuple[Dict[str, Any], bytes]:
    hlen = MAGIC_HEADER_LEN.unpack(_recv_exact(sock, 4))[0]
    if hlen == 0 or hlen > MAX_HEADER:
        raise ProtocolError(f"bad header length {hlen}")
    header = json.loads(_recv_exact(sock, hlen).decode("utf-8"))
    size = int(header.get("size", 0))
    if size < 0 or size > MAX_PAYLOAD:
        raise ProtocolError(f"bad payload size {size}")
    payload = _recv_exact(sock, size) if size else b""
    return header, payload


def connect(
    host: str,
    port: int,
    timeout: float = 10.0,
    ssl_ctx=None,
) -> socket.socket:
    """Open a data-plane connection, wrapped in TLS when a context is given."""
    sock = socket.create_connection((host, port), timeout=timeout)
    # Latency matters more than packet efficiency for a cache read.
    sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    if ssl_ctx is not None:
        try:
            sock = ssl_ctx.wrap_socket(sock, server_hostname=None)
        except Exception:
            sock.close()
            raise
    return sock


def request(
    sock: socket.socket,
    op: str,
    key: Optional[str] = None,
    payload: bytes = b"",
    **extra: Any,
) -> Tuple[Dict[str, Any], bytes]:
    header: Dict[str, Any] = {"op": op}
    if key is not None:
        header["key"] = key
    header.update(extra)
    send_frame(sock, header, payload)
    return recv_frame(sock)
