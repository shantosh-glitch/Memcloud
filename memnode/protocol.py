"""
Wire framing shared by both transports:

  local RPC (client <-> daemon, same machine): length-prefixed JSON
  peer protocol (daemon <-> daemon, LAN):      length-prefixed msgpack

Both use the exact same 4-byte big-endian length header. The only
difference is the payload codec. Every reader here checks the length
prefix against a caller-supplied ceiling BEFORE allocating a buffer for
the body -- that check is the fix for the unbounded-allocation bug found
in the audit of the reference implementation (a peer sending a 4GB
length prefix used to cost you a 4GB allocation; here it costs 4 bytes).
"""
from __future__ import annotations

import json
import struct
from typing import Optional

import msgpack

HEADER_SIZE = 4  # 4-byte big-endian unsigned length prefix


class FrameTooLarge(Exception):
    """Raised when a peer's declared frame length exceeds the configured max."""

    def __init__(self, declared: int, limit: int):
        self.declared = declared
        self.limit = limit
        super().__init__(f"frame length {declared} exceeds max {limit}")


async def read_frame(reader, max_size: int) -> bytes:
    """
    Read one length-prefixed frame. Raises FrameTooLarge WITHOUT touching
    the socket buffer for the body if the declared length is over
    max_size -- so a hostile length prefix costs us 4 bytes, not gigabytes.
    `reader` needs only a `readexactly(n) -> bytes` coroutine method, so
    this works against both real asyncio.StreamReader and test doubles.
    """
    header = await reader.readexactly(HEADER_SIZE)
    (length,) = struct.unpack(">I", header)
    if length > max_size:
        raise FrameTooLarge(length, max_size)
    if length == 0:
        return b""
    return await reader.readexactly(length)


async def write_frame(writer, payload: bytes) -> None:
    writer.write(struct.pack(">I", len(payload)) + payload)
    await writer.drain()


# ---------------------------------------------------------------------------
# Local RPC codec -- JSON. Deliberately human-typeable: you can `nc` the
# unix socket and hand-type a request during a demo to prove it's real.
# ---------------------------------------------------------------------------


def encode_rpc(message: dict) -> bytes:
    return json.dumps(message, separators=(",", ":")).encode("utf-8")


def decode_rpc(payload: bytes) -> dict:
    return json.loads(payload.decode("utf-8"))


# ---------------------------------------------------------------------------
# Peer wire codec -- msgpack. Compact binary, cross-language friendly (the
# JS SDK can speak the same format), this project's analog of the
# reference implementation's bincode-encoded Message enum.
# ---------------------------------------------------------------------------


def encode_peer(message: dict) -> bytes:
    return msgpack.packb(message, use_bin_type=True)


def decode_peer(payload: bytes) -> dict:
    return msgpack.unpackb(payload, raw=False)


# ---------------------------------------------------------------------------
# Peer message constructors -- plain dicts tagged by "type", the msgpack
# analog of the reference Message enum's variants.
# ---------------------------------------------------------------------------


def msg_hello(node_id: str, name: str, port: int, nonce: str, auth: str) -> dict:
    return {
        "type": "Hello",
        "version": 1,
        "node_id": node_id,
        "name": name,
        "port": port,
        "nonce": nonce,
        "auth": auth,          # HMAC(secret, node_id + nonce) -- see security.py
    }


def msg_welcome(node_id: str, name: str, port: int, nonce: str, auth: str, peers: list) -> dict:
    return {
        "type": "Welcome",
        "version": 1,
        "node_id": node_id,
        "name": name,
        "port": port,
        "nonce": nonce,
        "auth": auth,
        "peers": peers,        # [{node_id, name, host, port}, ...] for gossip-based mesh formation
    }


def msg_deny(reason: str) -> dict:
    return {"type": "Deny", "reason": reason}


def msg_store_block(block_id: str, data: bytes, mode: str) -> dict:
    return {"type": "StoreBlock", "block_id": block_id, "data": data, "mode": mode}


def msg_block_stored(block_id: str, ok: bool, error: Optional[str] = None) -> dict:
    return {"type": "BlockStored", "block_id": block_id, "ok": ok, "error": error}


def msg_request_block(block_id: str) -> dict:
    return {"type": "RequestBlock", "block_id": block_id}


def msg_block_data(block_id: str, data: Optional[bytes], found: bool) -> dict:
    return {"type": "BlockData", "block_id": block_id, "data": data, "found": found}


def msg_set_key(key: str, block_id: str) -> dict:
    return {"type": "SetKey", "key": key, "block_id": block_id}


def msg_get_key(key: str) -> dict:
    return {"type": "GetKey", "key": key}


def msg_key_found(key: str, block_id: Optional[str]) -> dict:
    return {"type": "KeyFound", "key": key, "block_id": block_id}


def msg_ping(free_quota_bytes: int) -> dict:
    return {"type": "Ping", "free_quota_bytes": free_quota_bytes}


def msg_pong(free_quota_bytes: int) -> dict:
    return {"type": "Pong", "free_quota_bytes": free_quota_bytes}


def msg_nack(in_reply_to: str, reason: str) -> dict:
    return {"type": "Nack", "in_reply_to": in_reply_to, "reason": reason}


def msg_bye() -> dict:
    return {"type": "Bye"}
