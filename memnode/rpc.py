"""
Local RPC server: the seam between "this machine's UI/CLI/SDK" and the
daemon process. Listens on a Unix socket (primary) and, as a fallback
for platforms without one (e.g. Windows) or tooling that prefers TCP, a
socket bound to 127.0.0.1.

Every request is JSON, length-prefixed with the same bounded framing
used on the peer wire. Every handler is wrapped so a malformed or
unexpected request comes back as a clean {"error": ...} instead of
killing the connection (or the whole daemon) -- this replaces the class
of unwrap()/expect() panic sites from the reference implementation audit
with exactly zero of them here.
"""
from __future__ import annotations

import asyncio
import base64
import logging
import os
import uuid
from typing import Optional

from memnode import config, protocol
from memnode.blocks import BlockStore, BlockTooLarge, Mode, QuotaExceeded
from memnode.peers import PeerManager

log = logging.getLogger("memnode.rpc")


class RpcServer:
    def __init__(self, blocks: BlockStore, peers: PeerManager, identity: dict,
                 rpc_tcp_port: int = config.DEFAULT_RPC_TCP_PORT):
        self.blocks = blocks
        self.peers = peers
        self.identity = identity
        self.rpc_tcp_port = rpc_tcp_port
        self._unix_server: Optional[asyncio.base_events.Server] = None
        self._tcp_server: Optional[asyncio.base_events.Server] = None

    async def start(self) -> None:
        config.ensure_home()
        sock_path = str(config.RPC_SOCKET_PATH)

        try:
            if os.path.exists(sock_path):
                os.remove(sock_path)
            self._unix_server = await asyncio.start_unix_server(self._on_client, path=sock_path)
        except (NotImplementedError, AttributeError, OSError) as e:
            log.warning("unix socket RPC unavailable (%s) -- falling back to TCP-only local RPC", e)
            self._unix_server = None

        self._tcp_server = await asyncio.start_server(self._on_client, config.DEFAULT_RPC_HOST, self.rpc_tcp_port)
        log.info("rpc listening on unix:%s and tcp:%s:%d",
                  sock_path if self._unix_server else "(disabled)", config.DEFAULT_RPC_HOST, self.rpc_tcp_port)

    async def stop(self) -> None:
        for server in (self._unix_server, self._tcp_server):
            if server:
                server.close()
                await server.wait_closed()

    async def _on_client(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        try:
            while True:
                try:
                    raw = await protocol.read_frame(reader, config.MAX_FRAME_SIZE)
                except protocol.FrameTooLarge as e:
                    await self._send(writer, {"error": f"request too large ({e.declared} > {e.limit} bytes)"})
                    break
                except (asyncio.IncompleteReadError, ConnectionResetError):
                    break
                if not raw:
                    continue

                try:
                    request = protocol.decode_rpc(raw)
                except Exception as e:
                    await self._send(writer, {"error": f"malformed request: {e}"})
                    continue

                try:
                    response = await self._dispatch(request)
                except Exception:
                    log.exception("unhandled error dispatching request: %r", request)
                    response = {"error": "internal error handling request"}

                await self._send(writer, response)
        finally:
            writer.close()

    async def _send(self, writer: asyncio.StreamWriter, message: dict) -> None:
        try:
            await protocol.write_frame(writer, protocol.encode_rpc(message))
        except Exception:
            log.exception("failed writing rpc response")

    async def _dispatch(self, request: dict) -> dict:
        cmd = request.get("cmd")

        if cmd == "store":
            return await self._cmd_store(request)
        if cmd == "load":
            return await self._cmd_load(request)
        if cmd == "free":
            ok = self.blocks.free(request.get("block_id", ""))
            return {"ok": ok}
        if cmd == "set_key":
            try:
                self.blocks.set_key(request["key"], request["block_id"])
                return {"ok": True}
            except Exception as e:
                return {"error": str(e)}
        if cmd == "get_key":
            block_id = self.blocks.get_key(request.get("key", ""))
            return {"block_id": block_id}
        if cmd == "peers":
            return {"peers": self.peers.list_peers()}
        if cmd == "connect":
            peer = await self.peers.connect_to(request["host"], int(request["port"]))
            return {"ok": peer is not None}
        if cmd == "stats":
            stats = self.blocks.stats()
            stats["node_id"] = self.identity["node_id"]
            stats["name"] = self.identity["name"]
            stats["connected_peers"] = len(self.peers.peers)
            return {"stats": stats}
        if cmd == "ping":
            return {"pong": True}

        return {"error": f"unknown command: {cmd!r}"}

    async def _cmd_store(self, request: dict) -> dict:
        data_b64 = request.get("data_b64")
        if not isinstance(data_b64, str):
            return {"error": "'data_b64' must be a base64-encoded string"}
        try:
            data = base64.b64decode(data_b64, validate=True)
        except Exception as e:
            return {"error": f"invalid base64 payload: {e}"}

        try:
            mode = Mode(request.get("mode", "cache"))
        except ValueError:
            return {"error": f"invalid mode {request.get('mode')!r}, expected 'pinned' or 'cache'"}

        target_name = request.get("peer")

        try:
            if target_name:
                peer = self.peers.get_peer_by_name(target_name)
                if peer is None:
                    return {"error": f"no connected peer named {target_name!r}"}
                if len(data) > config.MAX_BLOCK_SIZE:
                    return {"error": f"block too large ({len(data)} > {config.MAX_BLOCK_SIZE} bytes)"}
                block_id = uuid.uuid4().hex
                ok = await self.peers.store_on_peer(peer, block_id, data, mode)
                if not ok:
                    return {"error": f"peer {target_name!r} rejected or timed out on store"}
                return {"block_id": block_id, "location": target_name}

            if request.get("auto_remote"):
                best = self.peers.pick_best_peer()
                if best is not None and best.free_quota_bytes > self.blocks.free_bytes and len(data) <= config.MAX_BLOCK_SIZE:
                    block_id = uuid.uuid4().hex
                    ok = await self.peers.store_on_peer(best, block_id, data, mode)
                    if ok:
                        return {"block_id": block_id, "location": best.name}
                    # remote placement failed -- fall through to local store rather than error out

            block_id = self.blocks.store(data, mode)
            return {"block_id": block_id, "location": "local"}

        except BlockTooLarge as e:
            return {"error": f"block too large ({e.size} > {e.limit} bytes)"}
        except QuotaExceeded as e:
            return {"error": f"quota exceeded (need {e.requested}, have {e.available})"}

    async def _cmd_load(self, request: dict) -> dict:
        block_id = request.get("block_id", "")
        if not block_id:
            return {"error": "'block_id' is required"}

        data = self.blocks.load(block_id)
        if data is not None:
            return {"data_b64": base64.b64encode(data).decode(), "location": "local"}

        data = await self.peers.load_remote(block_id)
        if data is not None:
            return {"data_b64": base64.b64encode(data).decode(), "location": "remote"}

        return {"error": "block not found"}
