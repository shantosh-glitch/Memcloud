"""
In-memory block store: the actual "RAM cache" layer.

Two durability modes:
  PINNED - never auto-evicted; a store fails outright if there's no room.
           Meant for config/session data.
  CACHE  - evictable under memory pressure via random-sampling LRU-ish
           eviction. Meant for build artifacts, logs, disposable data.

Every store path enforces MAX_BLOCK_SIZE before touching the quota, and
the daemon-wide quota is enforced regardless of mode (pinned writes just
can't evict *other* pinned blocks to make room -- they fail instead).
"""
from __future__ import annotations

import random
import time
import uuid
from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, Optional

from memnode import config


class Mode(str, Enum):
    PINNED = "pinned"
    CACHE = "cache"


class BlockTooLarge(Exception):
    def __init__(self, size: int, limit: int):
        self.size = size
        self.limit = limit
        super().__init__(f"block size {size} exceeds max {limit}")


class QuotaExceeded(Exception):
    def __init__(self, requested: int, available: int):
        self.requested = requested
        self.available = available
        super().__init__(f"requested {requested} bytes but only {available} available")


@dataclass
class BlockEntry:
    block_id: str
    data: bytes
    mode: Mode
    size: int
    created_at: float = field(default_factory=time.time)
    last_accessed: float = field(default_factory=time.time)


class BlockStore:
    """
    Single-threaded by design -- this project is asyncio-based and every
    method here is synchronous (no `await` inside), so all mutation
    happens atomically on the event loop with no lock needed. If this
    ever grows real threads, that invariant is the first thing to revisit.
    """

    def __init__(self, quota_bytes: int = config.DEFAULT_QUOTA_BYTES):
        self.quota_bytes = quota_bytes
        self.used_bytes = 0
        self._blocks: Dict[str, BlockEntry] = {}
        self._keys: Dict[str, str] = {}   # named key -> block_id

    # -- capacity ------------------------------------------------------

    @property
    def free_bytes(self) -> int:
        return max(0, self.quota_bytes - self.used_bytes)

    def stats(self) -> dict:
        pinned = sum(1 for b in self._blocks.values() if b.mode is Mode.PINNED)
        cached = len(self._blocks) - pinned
        return {
            "quota_bytes": self.quota_bytes,
            "used_bytes": self.used_bytes,
            "free_bytes": self.free_bytes,
            "block_count": len(self._blocks),
            "pinned_count": pinned,
            "cache_count": cached,
            "key_count": len(self._keys),
        }

    # -- eviction --------------------------------------------------------

    def _evict_to_fit(self, needed: int) -> bool:
        """
        Random-sampling LRU-ish eviction: repeatedly sample a handful of
        CACHE blocks and evict the least-recently-accessed of the sample.
        Cheap (no full sort of every block on every store) and good
        enough for a cache. Returns True once `needed` bytes are free,
        False if evicting every cache block still isn't enough (i.e.
        pinned blocks alone already exceed the room being requested).
        """
        sample_size = 5
        while self.free_bytes < needed:
            candidates = [b for b in self._blocks.values() if b.mode is Mode.CACHE]
            if not candidates:
                return False
            sample = random.sample(candidates, min(sample_size, len(candidates)))
            victim = min(sample, key=lambda b: b.last_accessed)
            self._remove(victim.block_id)
        return True

    def _remove(self, block_id: str) -> None:
        entry = self._blocks.pop(block_id, None)
        if entry is not None:
            self.used_bytes -= entry.size
            dead_keys = [k for k, v in self._keys.items() if v == block_id]
            for k in dead_keys:
                del self._keys[k]

    # -- core API ----------------------------------------------------------

    def store(self, data: bytes, mode: Mode = Mode.CACHE, block_id: Optional[str] = None) -> str:
        size = len(data)
        if size > config.MAX_BLOCK_SIZE:
            raise BlockTooLarge(size, config.MAX_BLOCK_SIZE)

        if self.free_bytes < size:
            # Eviction only ever sacrifices CACHE blocks, whether the
            # incoming write is PINNED or CACHE -- pinned data already
            # resident is never evicted to make room for anything.
            self._evict_to_fit(size)

        if self.free_bytes < size:
            raise QuotaExceeded(size, self.free_bytes)

        block_id = block_id or uuid.uuid4().hex
        self._blocks[block_id] = BlockEntry(block_id=block_id, data=data, mode=mode, size=size)
        self.used_bytes += size
        return block_id

    def load(self, block_id: str) -> Optional[bytes]:
        entry = self._blocks.get(block_id)
        if entry is None:
            return None
        entry.last_accessed = time.time()
        return entry.data

    def free(self, block_id: str) -> bool:
        existed = block_id in self._blocks
        self._remove(block_id)
        return existed

    def has(self, block_id: str) -> bool:
        return block_id in self._blocks

    # -- named keys ------------------------------------------------------

    def set_key(self, key: str, block_id: str) -> None:
        if block_id not in self._blocks:
            raise KeyError(f"unknown block_id {block_id}")
        self._keys[key] = block_id

    def get_key(self, key: str) -> Optional[str]:
        return self._keys.get(key)
