import asyncio

from memnode.blocks import BlockStore, Mode
from memnode.peers import PeerManager

# Distinct port ranges per test so tests can run in parallel without clashing.


async def _make_node(name, port, secret="shared-secret", quota=1024 * 1024):
    identity = {"node_id": f"node-{name}", "name": name}
    blocks = BlockStore(quota_bytes=quota)
    pm = PeerManager(identity, blocks, secret, port)
    await pm.start()
    return pm, blocks


async def test_two_nodes_handshake_and_see_each_other():
    pm_a, _ = await _make_node("alice", 18801)
    pm_b, _ = await _make_node("bob", 18802)
    try:
        peer = await pm_a.connect_to("127.0.0.1", 18802)
        assert peer is not None
        await asyncio.sleep(0.1)  # let bob's side finish registering
        assert "node-bob" in pm_a.peers
        assert "node-alice" in pm_b.peers
    finally:
        await pm_a.stop()
        await pm_b.stop()


async def test_handshake_rejected_with_mismatched_secret():
    pm_a, _ = await _make_node("alice", 18811, secret="secret-a")
    pm_b, _ = await _make_node("bob", 18812, secret="secret-b")
    try:
        peer = await pm_a.connect_to("127.0.0.1", 18812)
        assert peer is None
        assert "node-bob" not in pm_a.peers
        await asyncio.sleep(0.05)
        assert "node-alice" not in pm_b.peers
    finally:
        await pm_a.stop()
        await pm_b.stop()


async def test_duplicate_connect_is_a_noop():
    pm_a, _ = await _make_node("alice", 18821)
    pm_b, _ = await _make_node("bob", 18822)
    try:
        first = await pm_a.connect_to("127.0.0.1", 18822)
        assert first is not None
        second = await pm_a.connect_to("127.0.0.1", 18822)
        assert second is None  # already connected -- no duplicate peer entry
        assert len(pm_a.peers) == 1
    finally:
        await pm_a.stop()
        await pm_b.stop()


async def test_store_and_load_actually_land_on_the_remote_peer():
    pm_a, blocks_a = await _make_node("alice", 18831)
    pm_b, blocks_b = await _make_node("bob", 18832)
    try:
        peer = await pm_a.connect_to("127.0.0.1", 18832)
        assert peer is not None
        await asyncio.sleep(0.1)

        block_id = "test-block-1"
        ok = await pm_a.store_on_peer(peer, block_id, b"remote payload", Mode.CACHE)
        assert ok is True

        # the whole point: the bytes are actually in bob's RAM, not alice's
        assert blocks_b.load(block_id) == b"remote payload"
        assert blocks_a.load(block_id) is None

        fetched = await pm_a.load_from_peer(peer, block_id)
        assert fetched == b"remote payload"

        # load_remote should use the remembered location without broadcasting
        fetched2 = await pm_a.load_remote(block_id)
        assert fetched2 == b"remote payload"
    finally:
        await pm_a.stop()
        await pm_b.stop()


async def test_load_missing_block_returns_none_not_an_error():
    pm_a, _ = await _make_node("alice", 18841)
    pm_b, _ = await _make_node("bob", 18842)
    try:
        peer = await pm_a.connect_to("127.0.0.1", 18842)
        await asyncio.sleep(0.1)
        result = await pm_a.load_from_peer(peer, "nonexistent-block-id")
        assert result is None
    finally:
        await pm_a.stop()
        await pm_b.stop()


async def test_capacity_aware_placement_picks_the_peer_with_more_free_room():
    pm_a, _ = await _make_node("alice", 18851)
    pm_b, _ = await _make_node("bob", 18852)
    pm_c, _ = await _make_node("carol", 18853)
    try:
        peer_b = await pm_a.connect_to("127.0.0.1", 18852)
        peer_c = await pm_a.connect_to("127.0.0.1", 18853)
        peer_b.free_quota_bytes = 1000
        peer_c.free_quota_bytes = 5000
        best = pm_a.pick_best_peer()
        assert best.name == "carol"
    finally:
        await pm_a.stop()
        await pm_b.stop()
        await pm_c.stop()


async def test_disconnecting_peer_is_pruned_from_the_list():
    pm_a, _ = await _make_node("alice", 18861)
    pm_b, _ = await _make_node("bob", 18862)
    try:
        peer = await pm_a.connect_to("127.0.0.1", 18862)
        assert peer is not None
        await asyncio.sleep(0.1)
        assert "node-bob" in pm_a.peers

        await pm_b.stop()  # bob goes away without a clean Bye message
        await asyncio.sleep(0.3)

        assert "node-bob" not in pm_a.peers
    finally:
        await pm_a.stop()


async def test_stop_completes_promptly_with_active_connections():
    """
    Regression test: PeerManager.stop() used to close active peer
    connections *after* awaiting Server.wait_closed(), which blocks
    until every connection the server accepted has actually finished --
    a straightforward deadlock. stop() must return quickly even with a
    live peer connection open.
    """
    pm_a, _ = await _make_node("alice", 18881)
    pm_b, _ = await _make_node("bob", 18882)
    try:
        await pm_a.connect_to("127.0.0.1", 18882)
        await asyncio.sleep(0.1)
        await asyncio.wait_for(pm_b.stop(), timeout=2.0)
    finally:
        await pm_a.stop()


async def test_gossip_forms_a_mesh_through_a_third_node():
    """alice connects to bob; bob and carol are already connected to each
    other; alice should end up connected to carol too via gossip, without
    ever calling connect_to(carol) herself."""
    pm_a, _ = await _make_node("alice", 18871)
    pm_b, _ = await _make_node("bob", 18872)
    pm_c, _ = await _make_node("carol", 18873)
    try:
        await pm_b.connect_to("127.0.0.1", 18873)  # bob <-> carol
        await asyncio.sleep(0.1)

        await pm_a.connect_to("127.0.0.1", 18872)  # alice -> bob, should gossip carol
        await asyncio.sleep(0.3)

        assert "node-carol" in pm_a.peers
    finally:
        await pm_a.stop()
        await pm_b.stop()
        await pm_c.stop()
