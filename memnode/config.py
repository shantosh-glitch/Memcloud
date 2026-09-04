"""
Central configuration for MemCloud.

Every size limit that gates a network-triggered allocation lives here,
in one place, so there's exactly one number to audit / bump / tighten.
"""
from __future__ import annotations

import os
from pathlib import Path

# ---------------------------------------------------------------------------
# Network bounds -- checked BEFORE allocating, never after.
# ---------------------------------------------------------------------------

# Hard ceiling on any single length-prefixed frame (local RPC or peer wire).
# A peer that sends a length prefix bigger than this gets disconnected, not
# allocated for. This is the fix for the classic "read u32 length, then
# vec![0u8; len]" DoS class of bug found in the reference implementation.
MAX_FRAME_SIZE = 64 * 1024 * 1024          # 64 MiB
# NOTE: local-RPC store requests carry block data base64-encoded inside a
# JSON frame (~1.33x inflation). Keep MAX_FRAME_SIZE comfortably above
# MAX_BLOCK_SIZE * 4/3, or legitimate max-size stores get rejected as
# "too large" by the frame check before they ever reach the block check.

# Hard ceiling on a single stored block's payload. Independent from
# MAX_FRAME_SIZE because streaming sends a large file as many blocks.
MAX_BLOCK_SIZE = 32 * 1024 * 1024          # 32 MiB

# Chunk size used by the CLI's streaming store/load path.
STREAM_CHUNK_SIZE = 1 * 1024 * 1024        # 1 MiB

# ---------------------------------------------------------------------------
# Ports / addresses
# ---------------------------------------------------------------------------

DEFAULT_PEER_PORT = 8080          # daemon <-> daemon, TCP
DEFAULT_RPC_TCP_PORT = 7070       # local RPC over TCP (127.0.0.1 only)
DEFAULT_RPC_HOST = "127.0.0.1"

# ---------------------------------------------------------------------------
# Filesystem locations
# ---------------------------------------------------------------------------

MEMCLOUD_HOME = Path(os.environ.get("MEMCLOUD_HOME", str(Path.home() / ".memcloud")))
IDENTITY_FILE = MEMCLOUD_HOME / "identity.json"
TRUSTED_PEERS_FILE = MEMCLOUD_HOME / "trusted_peers.json"
RPC_SOCKET_PATH = MEMCLOUD_HOME / "memnode.sock"

# ---------------------------------------------------------------------------
# Memory management
# ---------------------------------------------------------------------------

DEFAULT_QUOTA_BYTES = 512 * 1024 * 1024    # 512 MiB per node; override with --quota-mb

# ---------------------------------------------------------------------------
# Discovery
# ---------------------------------------------------------------------------

MDNS_SERVICE_TYPE = "_memcloud._tcp.local."

# ---------------------------------------------------------------------------
# Protocol / liveness
# ---------------------------------------------------------------------------

PROTOCOL_VERSION = 1
HEARTBEAT_INTERVAL_SECONDS = 5.0
PEER_TIMEOUT_SECONDS = 15.0        # no Pong/traffic in this long -> drop the peer

# ---------------------------------------------------------------------------
# Security -- shared-secret auth (NOT encryption; see security.py docstring
# for why that trade-off is deliberate for this project).
# ---------------------------------------------------------------------------


def get_cluster_secret() -> str:
    """
    The shared secret every node in the mesh must know to be allowed to
    join. Deliberately NOT hardcoded to something meaningful -- read from
    env so it isn't sitting in source control. Falls back to a well-known
    demo value so a fresh checkout still boots for local testing (loudly
    logged at startup, never silently).
    """
    return os.environ.get("MEMCLOUD_SECRET", "memcloud-hackathon-demo-secret")


def ensure_home() -> None:
    MEMCLOUD_HOME.mkdir(parents=True, exist_ok=True)
