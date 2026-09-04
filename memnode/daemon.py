"""
Entrypoint: wires config + identity + BlockStore + PeerManager + RpcServer
+ Discovery together and runs until interrupted.

    python -m memnode.daemon --name alice --quota-mb 512
"""
from __future__ import annotations

import argparse
import asyncio
import logging
import signal

from memnode import config, security
from memnode.blocks import BlockStore
from memnode.discovery import Discovery
from memnode.peers import PeerManager
from memnode.rpc import RpcServer

log = logging.getLogger("memnode")


def parse_args(argv=None) -> argparse.Namespace:
    p = argparse.ArgumentParser(prog="memnode", description="memnode -- MemCloud daemon")
    p.add_argument("--peer-port", type=int, default=config.DEFAULT_PEER_PORT, help="TCP port for daemon<->daemon traffic")
    p.add_argument("--rpc-port", type=int, default=config.DEFAULT_RPC_TCP_PORT, help="TCP port for local RPC fallback (rarely needed -- mainly for running >1 node on one dev machine)")
    p.add_argument("--quota-mb", type=int, default=config.DEFAULT_QUOTA_BYTES // (1024 * 1024), help="RAM quota in MiB")
    p.add_argument("--name", type=str, default=None, help="display name advertised to peers")
    p.add_argument("--no-mdns", action="store_true", help="disable automatic LAN discovery")
    p.add_argument("--secret", type=str, default=None, help="override MEMCLOUD_SECRET for this process")
    p.add_argument("-v", "--verbose", action="store_true")
    return p.parse_args(argv)


async def run(argv=None) -> None:
    args = parse_args(argv)
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    config.ensure_home()
    identity = security.load_or_create_identity()
    if args.name:
        identity["name"] = args.name

    secret = args.secret or config.get_cluster_secret()
    if secret == "memcloud-hackathon-demo-secret":
        log.warning("using the default demo cluster secret -- set MEMCLOUD_SECRET before using this beyond local testing")

    blocks = BlockStore(quota_bytes=args.quota_mb * 1024 * 1024)
    peers = PeerManager(identity, blocks, secret, args.peer_port)
    rpc = RpcServer(blocks, peers, identity, rpc_tcp_port=args.rpc_port)

    await peers.start()
    await rpc.start()

    discovery = None
    if not args.no_mdns:
        discovery = Discovery(identity, args.peer_port, on_found=lambda h, p: peers.connect_to(h, p))
        try:
            await discovery.start()
        except Exception:
            log.exception("mDNS discovery failed to start -- continuing without it (use `memcli connect` manually)")
            discovery = None

    log.info(
        "memnode up: node_id=%s name=%s quota=%dMB peer_port=%d rpc_tcp_port=%d",
        identity["node_id"][:8], identity["name"], args.quota_mb, args.peer_port, args.rpc_port,
    )

    stop_event = asyncio.Event()

    def _signal_handler():
        log.info("shutting down...")
        stop_event.set()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _signal_handler)
        except NotImplementedError:
            pass  # Windows doesn't support add_signal_handler

    await stop_event.wait()

    if discovery:
        await discovery.stop()
    await rpc.stop()
    await peers.stop()


def main() -> None:
    try:
        asyncio.run(run())
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
