import asyncio
import base64
import struct

import pytest
import pytest_asyncio

from memnode import config, protocol
from memnode.blocks import BlockStore
from memnode.peers import PeerManager
from memnode.rpc import RpcServer
from memnode.rpc_client import connect_rpc


@pytest_asyncio.fixture
async def node(tmp_path, monkeypatch):
    monkeypatch.setattr(config, "RPC_SOCKET_PATH", tmp_path / "test.sock")
    identity = {"node_id": "node-test", "name": "test-node"}
    blocks = BlockStore(quota_bytes=1024 * 1024)
    peers = PeerManager(identity, blocks, "secret", 19099)
    server = RpcServer(blocks, peers, identity)
    await peers.start()
    await server.start()
    try:
        yield server, blocks, peers
    finally:
        await server.stop()
        await peers.stop()


async def _call(request: dict) -> dict:
    reader, writer = await connect_rpc()
    try:
        await protocol.write_frame(writer, protocol.encode_rpc(request))
        raw = await protocol.read_frame(reader, config.MAX_FRAME_SIZE * 2)
        return protocol.decode_rpc(raw)
    finally:
        writer.close()


async def test_store_and_load_roundtrip(node):
    payload = b"hello over rpc"
    result = await _call({"cmd": "store", "data_b64": base64.b64encode(payload).decode(), "mode": "cache"})
    assert "block_id" in result
    assert result["location"] == "local"

    loaded = await _call({"cmd": "load", "block_id": result["block_id"]})
    assert base64.b64decode(loaded["data_b64"]) == payload
    assert loaded["location"] == "local"


async def test_load_nonexistent_block_returns_clean_error(node):
    result = await _call({"cmd": "load", "block_id": "nope"})
    assert "error" in result


async def test_pinned_mode_roundtrip(node):
    payload = b"config data"
    result = await _call({"cmd": "store", "data_b64": base64.b64encode(payload).decode(), "mode": "pinned"})
    assert "block_id" in result
    stats = await _call({"cmd": "stats"})
    assert stats["stats"]["pinned_count"] == 1


async def test_invalid_mode_is_a_clean_error_not_a_crash(node):
    result = await _call({"cmd": "store", "data_b64": base64.b64encode(b"x").decode(), "mode": "bogus"})
    assert "error" in result


async def test_unknown_command_returns_error_not_crash(node):
    result = await _call({"cmd": "not-a-real-command"})
    assert "error" in result


async def test_malformed_json_does_not_kill_the_server(node):
    reader, writer = await connect_rpc()
    await protocol.write_frame(writer, b"{not valid json")
    raw = await protocol.read_frame(reader, config.MAX_FRAME_SIZE)
    response = protocol.decode_rpc(raw)
    assert "error" in response
    writer.close()

    # server must still be alive and answer a fresh client normally
    result = await _call({"cmd": "ping"})
    assert result.get("pong") is True


async def test_oversized_frame_is_rejected_gracefully_not_a_crash(node):
    reader, writer = await connect_rpc()
    huge_len = config.MAX_FRAME_SIZE + 1
    writer.write(struct.pack(">I", huge_len))
    await writer.drain()
    raw = await protocol.read_frame(reader, config.MAX_FRAME_SIZE * 2)
    response = protocol.decode_rpc(raw)
    assert "error" in response
    writer.close()

    result = await _call({"cmd": "ping"})
    assert result.get("pong") is True


async def test_invalid_base64_is_a_clean_error(node):
    result = await _call({"cmd": "store", "data_b64": "not valid base64!!", "mode": "cache"})
    assert "error" in result


async def test_stats_reports_expected_shape(node):
    result = await _call({"cmd": "stats"})
    stats = result["stats"]
    for field in ("quota_bytes", "used_bytes", "free_bytes", "block_count", "node_id", "name", "connected_peers"):
        assert field in stats


async def test_peers_empty_list_when_none_connected(node):
    result = await _call({"cmd": "peers"})
    assert result["peers"] == []


async def test_free_nonexistent_block_returns_false_not_error(node):
    result = await _call({"cmd": "free", "block_id": "nope"})
    assert result == {"ok": False}


async def test_two_nodes_store_remote_and_load_back_through_rpc(tmp_path, monkeypatch):
    """End-to-end: RPC on node A routes a store to node B over the real
    peer wire, and a subsequent RPC load call on node A fetches it back
    from node B -- the full path a real CLI/SDK call would take."""
    monkeypatch.setattr(config, "RPC_SOCKET_PATH", tmp_path / "a.sock")
    identity_a = {"node_id": "node-a", "name": "node-a"}
    blocks_a = BlockStore(quota_bytes=1024 * 1024)
    peers_a = PeerManager(identity_a, blocks_a, "secret", 19101)
    rpc_a = RpcServer(blocks_a, peers_a, identity_a)

    identity_b = {"node_id": "node-b", "name": "node-b"}
    blocks_b = BlockStore(quota_bytes=1024 * 1024)
    peers_b = PeerManager(identity_b, blocks_b, "secret", 19102)

    await peers_a.start()
    await rpc_a.start()
    await peers_b.start()

    try:
        connected = await peers_a.connect_to("127.0.0.1", 19102)
        assert connected is not None
        await asyncio.sleep(0.1)

        payload = b"cross-node payload"
        store_result = await _call({
            "cmd": "store",
            "data_b64": base64.b64encode(payload).decode(),
            "mode": "cache",
            "peer": "node-b",
        })
        assert store_result["location"] == "node-b"
        block_id = store_result["block_id"]

        # actually landed in node B's RAM, not node A's
        assert blocks_b.load(block_id) == payload
        assert blocks_a.load(block_id) is None

        load_result = await _call({"cmd": "load", "block_id": block_id})
        assert load_result["location"] == "remote"
        assert base64.b64decode(load_result["data_b64"]) == payload
    finally:
        await rpc_a.stop()
        await peers_a.stop()
        await peers_b.stop()