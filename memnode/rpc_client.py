"""
Client-side counterpart to RpcServer.start() (see rpc.py): the one place
that knows how to dial the local RPC endpoint.

Unix sockets aren't available on every platform -- notably Windows, which
has neither asyncio.open_unix_connection nor asyncio.start_unix_server.
RpcServer already falls back to TCP-only when it can't listen on the
socket, so any client needs the matching fallback when it connects, or it
just hangs/crashes talking to a socket that was never opened.

This used to be implemented twice: once (correctly, with the fallback) in
memcli/cli.py, and once (missing the fallback) inline in
tests/test_rpc_integration.py, which is exactly how the two copies drifted
and the test suite started failing on Windows while the real CLI kept
working fine. Everything that needs to connect to the daemon's local RPC
-- the CLI, the test suite, anything else added later -- should import
connect_rpc() from here instead of re-deriving this logic.
"""
from __future__ import annotations

import asyncio

from memnode import config


async def connect_rpc():
    """
    Connect to the local memnode daemon's RPC endpoint.

    Tries the Unix socket first (fast, default). Falls back to TCP
    127.0.0.1:<DEFAULT_RPC_TCP_PORT> if Unix sockets aren't available on
    this platform, or if nothing is listening on the socket yet (e.g. the
    daemon fell back to TCP-only itself). Returns (reader, writer), same
    shape as the asyncio.open_* helpers it wraps.
    """
    if hasattr(asyncio, "open_unix_connection"):
        try:
            return await asyncio.open_unix_connection(str(config.RPC_SOCKET_PATH))
        except (FileNotFoundError, ConnectionRefusedError, NotImplementedError, OSError):
            pass
    return await asyncio.open_connection(config.DEFAULT_RPC_HOST, config.DEFAULT_RPC_TCP_PORT)