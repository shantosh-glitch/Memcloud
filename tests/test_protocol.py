import asyncio
import struct

import pytest

from memnode import protocol


class FakeStreamReader:
    """Minimal stand-in for asyncio.StreamReader exposing only readexactly,
    which is all protocol.read_frame needs."""

    def __init__(self, data: bytes):
        self._data = data
        self._pos = 0

    async def readexactly(self, n: int) -> bytes:
        if self._pos + n > len(self._data):
            remaining = self._data[self._pos:]
            raise asyncio.IncompleteReadError(remaining, n)
        chunk = self._data[self._pos:self._pos + n]
        self._pos += n
        return chunk


def _framed(payload: bytes) -> bytes:
    return struct.pack(">I", len(payload)) + payload


async def test_read_frame_roundtrip():
    payload = b"hello"
    reader = FakeStreamReader(_framed(payload))
    result = await protocol.read_frame(reader, max_size=1024)
    assert result == payload


async def test_read_frame_rejects_oversized_length_without_reading_body():
    # Declare a huge length but supply almost no body. If the
    # implementation allocated/read `length` bytes before checking the
    # bound, this would raise IncompleteReadError instead. Getting
    # FrameTooLarge proves the size check happens before any body read.
    huge_header = struct.pack(">I", 0xFFFFFFFF)
    reader = FakeStreamReader(huge_header)  # no body follows at all
    with pytest.raises(protocol.FrameTooLarge):
        await protocol.read_frame(reader, max_size=1024)


async def test_read_frame_allows_exactly_max_size():
    payload = b"x" * 1024
    reader = FakeStreamReader(_framed(payload))
    result = await protocol.read_frame(reader, max_size=1024)
    assert result == payload


async def test_read_frame_empty_frame():
    reader = FakeStreamReader(_framed(b""))
    result = await protocol.read_frame(reader, max_size=1024)
    assert result == b""


def test_peer_codec_roundtrip():
    msg = protocol.msg_ping(free_quota_bytes=12345)
    assert protocol.decode_peer(protocol.encode_peer(msg)) == msg


def test_peer_codec_preserves_binary_data():
    msg = protocol.msg_store_block("abc", b"\x00\x01\xff binary data", "cache")
    decoded = protocol.decode_peer(protocol.encode_peer(msg))
    assert decoded["data"] == b"\x00\x01\xff binary data"


def test_rpc_codec_roundtrip():
    msg = {"cmd": "stats"}
    assert protocol.decode_rpc(protocol.encode_rpc(msg)) == msg
