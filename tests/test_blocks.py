import pytest

from memnode import config
from memnode.blocks import BlockStore, BlockTooLarge, Mode, QuotaExceeded


def test_store_and_load_roundtrip():
    store = BlockStore(quota_bytes=1024)
    block_id = store.store(b"hello world")
    assert store.load(block_id) == b"hello world"


def test_load_missing_returns_none():
    store = BlockStore(quota_bytes=1024)
    assert store.load("does-not-exist") is None


def test_free_removes_block():
    store = BlockStore(quota_bytes=1024)
    block_id = store.store(b"data")
    assert store.free(block_id) is True
    assert store.load(block_id) is None
    assert store.free(block_id) is False  # freeing twice is a no-op, not an error


def test_block_larger_than_max_is_rejected():
    store = BlockStore(quota_bytes=10 * config.MAX_BLOCK_SIZE)
    oversized = b"x" * (config.MAX_BLOCK_SIZE + 1)
    with pytest.raises(BlockTooLarge):
        store.store(oversized)


def test_pinned_block_fails_when_quota_full_and_nothing_to_evict():
    store = BlockStore(quota_bytes=10)
    store.store(b"1234567890", mode=Mode.PINNED)  # fills quota exactly
    with pytest.raises(QuotaExceeded):
        store.store(b"x", mode=Mode.PINNED)


def test_cache_blocks_evicted_to_make_room():
    store = BlockStore(quota_bytes=20)
    store.store(b"a" * 10, mode=Mode.CACHE)
    store.store(b"b" * 10, mode=Mode.CACHE)
    c = store.store(b"c" * 10, mode=Mode.CACHE)  # forces eviction of a or b
    assert store.stats()["used_bytes"] <= 20
    assert store.has(c)


def test_pinned_blocks_are_never_evicted():
    store = BlockStore(quota_bytes=20)
    pinned = store.store(b"p" * 10, mode=Mode.PINNED)
    store.store(b"c" * 10, mode=Mode.CACHE)
    store.store(b"d" * 10, mode=Mode.CACHE)  # eviction pressure, should only ever hit cache blocks
    assert store.has(pinned)
    assert store.load(pinned) == b"p" * 10


def test_named_keys():
    store = BlockStore(quota_bytes=1024)
    block_id = store.store(b"value")
    store.set_key("mykey", block_id)
    assert store.get_key("mykey") == block_id


def test_set_key_unknown_block_raises():
    store = BlockStore(quota_bytes=1024)
    with pytest.raises(KeyError):
        store.set_key("k", "nonexistent")


def test_freeing_a_block_drops_its_keys():
    store = BlockStore(quota_bytes=1024)
    block_id = store.store(b"value")
    store.set_key("mykey", block_id)
    store.free(block_id)
    assert store.get_key("mykey") is None
