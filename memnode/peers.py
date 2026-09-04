"""
Peer manager: owns every daemon-to-daemon TCP connection, the handshake,
capacity-aware placement, and liveness tracking.

This is the module that answers "who do I ask for this block" and "who
has the most free room right now" -- the two questions the reference
implementation's naive --peer-flag-only routing never asked, and the
module that removes a peer the moment it's unreachable instead of
letting it linger in the peer list (the stale-peer TODO from the audit).
"""
from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass, field
from typing import Dict, Optional

from memnode import config, protocol, security
from memnode.blocks import BlockStore, Mode

log = logging.getLogger("memnode.peers")


@dataclass
class Peer:
    node_id: str
    name: str
    host: str
    port: int
    reader: asyncio.StreamReader
    writer: asyncio.StreamWriter
    free_quota_bytes: int = 0
    last_seen: float = field(default_factory=time.time)
    # in-flight StoreBlock/RequestBlock/GetKey requests we're waiting on,
    # keyed by block_id/key (one in-flight request per key at a time is
    # enough for this project).
    pending: Dict[str, "asyncio.Future"] = field(default_factory=dict)


def _log_task_exception(task: "asyncio.Task") -> None:
    """
    asyncio's classic gotcha: an exception raised inside a fire-and-forget
    task is swallowed unless something looks at task.exception(). This is
    the Python analog of "never .unwrap() in a connection handler" --
    attach this as a done-callback to every ensure_future() call so a bad
    peer message logs loudly instead of vanishing silently.
    """
    if task.cancelled():
        return
    exc = task.exception()
    if exc:
        log.error("background task crashed: %r", exc, exc_info=exc)


class PeerManager:
    def __init__(self, identity: dict, blocks: BlockStore, secret: str, listen_port: int):
        self.node_id = identity["node_id"]
        self.name = identity["name"]
        self.secret = secret
        self.listen_port = listen_port
        self.blocks = blocks
        self.peers: Dict[str, Peer] = {}            # node_id -> Peer
        self.remote_blocks: Dict[str, str] = {}      # block_id -> node_id, for blocks we originated remotely
        self._server: Optional[asyncio.base_events.Server] = None
        self._heartbeat_task: Optional[asyncio.Task] = None

    # -- lifecycle ----------------------------------------------------------

    async def start(self) -> None:
        self._server = await asyncio.start_server(self._on_incoming, "0.0.0.0", self.listen_port)
        self._heartbeat_task = asyncio.ensure_future(self._heartbeat_loop())
        self._heartbeat_task.add_done_callback(_log_task_exception)
        log.info("peer listener up on 0.0.0.0:%d", self.listen_port)

    async def stop(self) -> None:
        if self._heartbeat_task:
            self._heartbeat_task.cancel()
        # Close peer connections BEFORE awaiting the server's wait_closed():
        # asyncio.Server.wait_closed() blocks until every connection it
        # accepted has actually finished, so closing them afterwards would
        # deadlock -- easy to get backwards, worth the comment.
        for peer in list(self.peers.values()):
            try:
                peer.writer.close()
            except Exception:
                pass
        self.peers.clear()
        if self._server:
            self._server.close()
            await self._server.wait_closed()

    # -- connecting -----------------------------------------------------------

    async def connect_to(self, host: str, port: int) -> Optional[Peer]:
        if any(p.host == host and p.port == port for p in self.peers.values()):
            return None  # already connected, nothing to do
        try:
            reader, writer = await asyncio.open_connection(host, port)
        except OSError as e:
            log.warning("could not connect to %s:%d (%s)", host, port, e)
            return None
        try:
            peer = await self._handshake(reader, writer, host, port, initiator=True)
        except Exception:
            log.exception("handshake with %s:%d failed", host, port)
            writer.close()
            return None
        if peer is not None:
            asyncio.ensure_future(self._recv_loop(peer)).add_done_callback(_log_task_exception)
        return peer

    async def _on_incoming(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        peer_addr = writer.get_extra_info("peername")
        host = peer_addr[0] if peer_addr else "unknown"
        try:
            peer = await self._handshake(reader, writer, host, None, initiator=False)
        except Exception:
            log.exception("handshake with incoming connection from %s failed", host)
            writer.close()
            return
        if peer is not None:
            asyncio.ensure_future(self._recv_loop(peer)).add_done_callback(_log_task_exception)
        else:
            writer.close()

    # -- handshake: shared-secret HMAC auth over a fresh nonce, no encryption ---

    async def _handshake(self, reader, writer, host, port, initiator: bool) -> Optional[Peer]:
        nonce = security.new_nonce()
        auth = security.compute_auth(self.secret, self.node_id, nonce)

        if initiator:
            hello = protocol.msg_hello(self.node_id, self.name, self.listen_port, nonce, auth)
            await protocol.write_frame(writer, protocol.encode_peer(hello))

            raw = await protocol.read_frame(reader, config.MAX_FRAME_SIZE)
            reply = protocol.decode_peer(raw)

            if reply.get("type") == "Deny":
                log.warning("peer at %s:%s denied us: %s", host, port, reply.get("reason"))
                return None
            if reply.get("type") != "Welcome":
                return None
            if not security.verify_auth(self.secret, reply.get("node_id", ""), reply.get("nonce", ""), reply.get("auth", "")):
                log.warning("peer at %s:%s failed auth verification -- dropping", host, port)
                return None

            remote_id, remote_name = reply["node_id"], reply["name"]
            gossiped = reply.get("peers", [])
        else:
            raw = await protocol.read_frame(reader, config.MAX_FRAME_SIZE)
            hello = protocol.decode_peer(raw)

            if hello.get("type") != "Hello":
                return None
            if not security.verify_auth(self.secret, hello.get("node_id", ""), hello.get("nonce", ""), hello.get("auth", "")):
                deny = protocol.msg_deny("bad secret")
                await protocol.write_frame(writer, protocol.encode_peer(deny))
                return None

            welcome = protocol.msg_welcome(self.node_id, self.name, self.listen_port, nonce, auth, self._peer_summaries())
            await protocol.write_frame(writer, protocol.encode_peer(welcome))

            remote_id, remote_name = hello["node_id"], hello["name"]
            port = hello.get("port", self.listen_port)
            gossiped = []

        if remote_id == self.node_id:
            return None  # somehow connected to ourselves (loopback discovery); ignore

        if remote_id in self.peers:
            return None  # duplicate connection to an already-known peer

        peer = Peer(node_id=remote_id, name=remote_name, host=host, port=port, reader=reader, writer=writer)
        self.peers[remote_id] = peer
        log.info("peer connected: %s (%s) at %s:%s", remote_name, remote_id[:8], host, port)

        if gossiped:
            asyncio.ensure_future(self._gossip_connect(gossiped)).add_done_callback(_log_task_exception)

        return peer

    async def _gossip_connect(self, gossiped: list) -> None:
        """
        On a successful handshake, the peer we just met tells us who
        *it's* connected to. Try connecting to anyone in that list we
        don't already know -- this is how a mesh forms from a single
        mDNS discovery or manual connect instead of needing every pair
        of nodes to find each other independently.
        """
        for info in gossiped:
            node_id = info.get("node_id")
            if not node_id or node_id == self.node_id or node_id in self.peers:
                continue
            await self.connect_to(info["host"], info["port"])

    def _peer_summaries(self) -> list:
        return [{"node_id": p.node_id, "name": p.name, "host": p.host, "port": p.port} for p in self.peers.values()]

    # -- receive loop ---------------------------------------------------------

    async def _recv_loop(self, peer: Peer) -> None:
        try:
            while True:
                try:
                    raw = await protocol.read_frame(peer.reader, config.MAX_FRAME_SIZE)
                except protocol.FrameTooLarge as e:
                    log.warning("peer %s sent an oversized frame (%d > %d bytes) -- dropping connection",
                                peer.name, e.declared, e.limit)
                    break
                except (asyncio.IncompleteReadError, ConnectionResetError, ConnectionAbortedError):
                    break
                if not raw:
                    continue
                try:
                    message = protocol.decode_peer(raw)
                except Exception:
                    log.warning("peer %s sent an undecodable frame, ignoring it", peer.name)
                    continue
                await self._handle_message(peer, message)
        finally:
            self._drop_peer(peer.node_id)

    def _drop_peer(self, node_id: str) -> None:
        peer = self.peers.pop(node_id, None)
        if peer:
            try:
                peer.writer.close()
            except Exception:
                pass
            for fut in peer.pending.values():
                if not fut.done():
                    fut.cancel()
            log.info("peer disconnected: %s", peer.name)

    async def _handle_message(self, peer: Peer, message: dict) -> None:
        mtype = message.get("type")
        try:
            if mtype == "Ping":
                peer.free_quota_bytes = message.get("free_quota_bytes", 0)
                peer.last_seen = time.time()
                await protocol.write_frame(peer.writer, protocol.encode_peer(protocol.msg_pong(self.blocks.free_bytes)))

            elif mtype == "Pong":
                peer.free_quota_bytes = message.get("free_quota_bytes", 0)
                peer.last_seen = time.time()

            elif mtype == "StoreBlock":
                block_id, data, mode = message["block_id"], message["data"], message["mode"]
                try:
                    self.blocks.store(bytes(data), Mode(mode), block_id=block_id)
                    reply = protocol.msg_block_stored(block_id, True)
                except Exception as e:
                    reply = protocol.msg_block_stored(block_id, False, str(e))
                await protocol.write_frame(peer.writer, protocol.encode_peer(reply))

            elif mtype == "RequestBlock":
                block_id = message["block_id"]
                data = self.blocks.load(block_id)
                reply = protocol.msg_block_data(block_id, data, data is not None)
                await protocol.write_frame(peer.writer, protocol.encode_peer(reply))

            elif mtype == "GetKey":
                key = message["key"]
                block_id = self.blocks.get_key(key)
                await protocol.write_frame(peer.writer, protocol.encode_peer(protocol.msg_key_found(key, block_id)))

            elif mtype == "SetKey":
                try:
                    self.blocks.set_key(message["key"], message["block_id"])
                except Exception as e:
                    await protocol.write_frame(peer.writer, protocol.encode_peer(protocol.msg_nack("SetKey", str(e))))

            elif mtype in ("BlockStored", "BlockData", "KeyFound"):
                self._resolve_pending(peer, message)

            elif mtype == "Nack":
                log.debug("peer %s NACKed %s: %s", peer.name, message.get("in_reply_to"), message.get("reason"))

            elif mtype == "Bye":
                self._drop_peer(peer.node_id)

            else:
                log.debug("unhandled message type from %s: %s", peer.name, mtype)

        except Exception:
            # Never let a malformed/edge-case message kill the connection
            # handler -- this is the direct fix for the .unwrap()-panic
            # class of bug found in the audit of the reference daemon.
            log.exception("error handling %s from peer %s", mtype, peer.name)

    # -- request/response bridging (store-on-peer, load-from-peer) ------------

    def _resolve_pending(self, peer: Peer, message: dict) -> None:
        ident = message.get("block_id") or message.get("key")
        fut = peer.pending.pop(ident, None)
        if fut and not fut.done():
            fut.set_result(message)

    async def store_on_peer(self, peer: Peer, block_id: str, data: bytes, mode: Mode, timeout: float = 10.0) -> bool:
        fut: asyncio.Future = asyncio.get_event_loop().create_future()
        peer.pending[block_id] = fut
        await protocol.write_frame(peer.writer, protocol.encode_peer(protocol.msg_store_block(block_id, data, mode.value)))
        try:
            reply = await asyncio.wait_for(fut, timeout)
        except (asyncio.TimeoutError, asyncio.CancelledError):
            peer.pending.pop(block_id, None)
            return False
        ok = bool(reply.get("ok"))
        if ok:
            self.remote_blocks[block_id] = peer.node_id
        return ok

    async def load_from_peer(self, peer: Peer, block_id: str, timeout: float = 10.0) -> Optional[bytes]:
        fut: asyncio.Future = asyncio.get_event_loop().create_future()
        peer.pending[block_id] = fut
        await protocol.write_frame(peer.writer, protocol.encode_peer(protocol.msg_request_block(block_id)))
        try:
            reply = await asyncio.wait_for(fut, timeout)
        except (asyncio.TimeoutError, asyncio.CancelledError):
            peer.pending.pop(block_id, None)
            return None
        if not reply.get("found"):
            return None
        data = reply.get("data")
        return bytes(data) if data is not None else None

    async def load_from_any_peer(self, block_id: str, timeout: float = 10.0) -> Optional[bytes]:
        for peer in list(self.peers.values()):
            data = await self.load_from_peer(peer, block_id, timeout)
            if data is not None:
                return data
        return None

    async def load_remote(self, block_id: str, timeout: float = 10.0) -> Optional[bytes]:
        """Try the peer we remember placing this block on first (fast
        path), then fall back to asking every connected peer (for blocks
        we didn't originate, e.g. reached via a shared key)."""
        node_id = self.remote_blocks.get(block_id)
        if node_id and node_id in self.peers:
            data = await self.load_from_peer(self.peers[node_id], block_id, timeout)
            if data is not None:
                return data
        return await self.load_from_any_peer(block_id, timeout)

    # -- placement --------------------------------------------------------

    def pick_best_peer(self) -> Optional[Peer]:
        """Capacity-aware placement: the peer that most recently reported
        the most free quota, or None if we have no connected peers."""
        if not self.peers:
            return None
        return max(self.peers.values(), key=lambda p: p.free_quota_bytes)

    def get_peer_by_name(self, name: str) -> Optional[Peer]:
        for peer in self.peers.values():
            if peer.name == name:
                return peer
        return None

    def list_peers(self) -> list:
        return [
            {
                "node_id": p.node_id,
                "name": p.name,
                "host": p.host,
                "port": p.port,
                "free_quota_bytes": p.free_quota_bytes,
                "last_seen": p.last_seen,
            }
            for p in self.peers.values()
        ]

    # -- background loops ---------------------------------------------------

    async def _heartbeat_loop(self) -> None:
        while True:
            await asyncio.sleep(config.HEARTBEAT_INTERVAL_SECONDS)
            now = time.time()
            for peer in list(self.peers.values()):
                if now - peer.last_seen > config.PEER_TIMEOUT_SECONDS:
                    log.info("peer %s timed out, pruning stale entry", peer.name)
                    self._drop_peer(peer.node_id)
                    continue
                try:
                    await protocol.write_frame(peer.writer, protocol.encode_peer(protocol.msg_ping(self.blocks.free_bytes)))
                except Exception:
                    log.info("peer %s unreachable during heartbeat, pruning", peer.name)
                    self._drop_peer(peer.node_id)
