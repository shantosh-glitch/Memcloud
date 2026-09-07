"""
MemCloud node.

Every node runs all four roles at once:
    worker      holds blocks for peers in its own RAM
    discovery   finds peers, tracks their free RAM
    memcloud    the client API used by the local application
    imagecache  the Layer 7 demo application

Run:
    python3 -m memcloud.node --name laptop-a
    python3 -m memcloud.node --name laptop-b --peer 192.168.1.5:47801
"""

from __future__ import annotations

import argparse
import hashlib
import os
import platform
import queue
import socket
import sys
import threading
import time
import uuid
from typing import Any, Dict, List

from . import meminfo
from .api import APIServer
from .client import MemCloud
from .discovery import DISCOVERY_PORT, Discovery
from .imagecache import ImageCache
from .llm import PrefixKVStore, TensorStore
from .worker import BlockStore, Worker
from . import security

MB = 1024 * 1024
GB = 1024 * MB

DEFAULT_DATA_PORT = 47801
DEFAULT_API_PORT = 5892


def local_ip() -> str:
    """Best-effort LAN address of this host (no packets are actually sent)."""
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        s.connect(("8.8.8.8", 80))
        return s.getsockname()[0]
    except Exception:
        return "127.0.0.1"
    finally:
        s.close()


def stable_node_id(name: str) -> str:
    seed = f"{platform.node()}|{name}|{uuid.getnode()}"
    return hashlib.sha256(seed.encode()).hexdigest()[:16]


class Node:
    def __init__(self, args: argparse.Namespace) -> None:
        self.name = args.name or platform.node() or "node"
        self.node_id = stable_node_id(self.name)
        self.cluster = args.cluster
        self.started = time.time()
        self.ip = local_ip()

        total, _avail = meminfo.system_memory()
        self.ram_total = total

        # Budgets. Percentages of physical RAM keep this safe on any laptop.
        self.local_budget = (
            args.budget_mb * MB
            if args.budget_mb
            else int(total * args.budget_pct / 100.0)
        )
        self.reserve = (
            args.reserve_mb * MB
            if args.reserve_mb
            else int(total * args.reserve_pct / 100.0)
        )
        self.min_free = args.min_free_mb * MB

        self._subs: List["queue.Queue[Dict[str, Any]]"] = []
        self._subs_lock = threading.Lock()

        # ---- transport security -------------------------------------
        # Mutual TLS using one shared cluster certificate. Both contexts are
        # built up front so a bad cert path fails at startup, not mid-demo.
        self.tls_enabled = not args.insecure
        self.ssl_server_ctx = None
        self.ssl_client_ctx = None
        self.tls_fingerprint = None
        if self.tls_enabled:
            cert, key = security.ensure_cluster_cert(args.tls_cert, args.tls_key)
            self.ssl_server_ctx = security.server_context(cert, key)
            self.ssl_client_ctx = security.client_context(cert, key)
            self.tls_fingerprint = security.fingerprint(cert)
            self.tls_cert_path = cert

        self.store = BlockStore(self.reserve, self.min_free)
        self.worker = Worker(self, args.bind, args.data_port)
        self.discovery = Discovery(
            self, args.discovery_port, on_change=lambda e, p: self.emit(e, p.to_dict())
        )
        self.memcloud = MemCloud(
            self.node_id,
            self.discovery.table,
            self.local_budget,
            self.min_free,
            emit=self.emit,
            ssl_ctx=self.ssl_client_ctx,
            chunk_bytes=args.chunk_mb * MB,
        )
        self.cache = ImageCache(self.memcloud, args.frames_dir, emit=self.emit)
        self.tensors = TensorStore(self.memcloud, chunk_bytes=args.chunk_mb * MB)
        self.kv = PrefixKVStore(self.memcloud, chunk_bytes=args.chunk_mb * MB)
        self.api = APIServer(self, args.api_bind, args.api_port)

        self.data_port = args.data_port
        self.api_port = args.api_port
        for spec in args.peer or []:
            host, _, port = spec.partition(":")
            self.discovery.add_static_peer(host, int(port or DEFAULT_DATA_PORT))

    # -- events -------------------------------------------------------
    def subscribe(self, q) -> None:
        with self._subs_lock:
            self._subs.append(q)

    def unsubscribe(self, q) -> None:
        with self._subs_lock:
            if q in self._subs:
                self._subs.remove(q)

    def emit(self, event: str, data: Any) -> None:
        msg = {"event": event, "data": data, "at": time.time()}
        with self._subs_lock:
            subs = list(self._subs)
        for q in subs:
            try:
                q.put_nowait(msg)
            except Exception:
                pass

    # -- reporting ----------------------------------------------------
    def identity(self) -> Dict[str, Any]:
        return {
            "id": self.node_id,
            "name": self.name,
            "cluster": self.cluster,
            "host": self.ip,
            "data_port": self.data_port,
            "api_port": self.api_port,
            "os": platform.system(),
            "arch": platform.machine(),
            "uptime_s": int(time.time() - self.started),
            "tls": self.tls_enabled,
            "tls_fingerprint": self.tls_fingerprint,
        }

    def self_node(self) -> Dict[str, Any]:
        total, avail = meminfo.system_memory()
        st = self.store.stat()
        return {
            "id": self.node_id,
            "name": self.name,
            "self": True,
            "status": "online",
            "host": self.ip,
            "port": self.data_port,
            "rtt_ms": 0.0,
            "ram_total": total,
            "ram_available": avail,
            "ram_used": max(0, total - avail),
            "ram_percent": round(100.0 * (total - avail) / total, 1) if total else 0.0,
            "memcloud_bytes": self.memcloud.local_used,
            "hosted_bytes": st["hosted_bytes"],
            "hosted_blocks": st["hosted_blocks"],
            "reserve_free": st["reserve_free"],
            "process_rss": st["process_rss"],
            "static": False,
        }

    def state(self) -> Dict[str, Any]:
        nodes = [self.self_node()]
        for p in self.discovery.table.all():
            d = p.to_dict()
            d["self"] = False
            # A peer's "memcloud_bytes" from our vantage point is what it hosts.
            d["memcloud_bytes"] = d.get("hosted_bytes", 0)
            nodes.append(d)
        active, why = self.memcloud.under_pressure()
        return {
            "self": self.identity(),
            "nodes": nodes,
            "pressure": {"active": active, "reason": why},
            "cache": self.cache.report(),
            "budgets": {
                "local_budget": self.local_budget,
                "reserve": self.reserve,
                "min_free": self.min_free,
            },
            "discovery": {"broadcast": self.discovery.broadcast_ok},
            "security": {
                "tls": self.tls_enabled,
                "fingerprint": self.tls_fingerprint,
                "mode": "mutual TLS, shared cluster certificate"
                if self.tls_enabled else "PLAINTEXT (--insecure)",
            },
            "kv": self.kv.report(),
        }

    # -- lifecycle ----------------------------------------------------
    def start(self) -> None:
        self.data_port = self.worker.start()
        self.discovery.start()
        self.api_port = self.api.start()

        total, avail = meminfo.system_memory()
        print("=" * 66)
        print(f"  MemCloud node '{self.name}'  id={self.node_id}")
        print("=" * 66)
        print(f"  host RAM        {meminfo.human(total)} total, "
              f"{meminfo.human(avail)} available")
        print(f"  local budget    {meminfo.human(self.local_budget)}  "
              f"(app cache held in this process)")
        print(f"  peer reserve    {meminfo.human(self.reserve)}  "
              f"(RAM offered to other nodes)")
        print(f"  min free guard  {meminfo.human(self.min_free)}")
        if self.tls_enabled:
            print(f"  data plane      {self.ip}:{self.data_port}   "
                  f"(TCP + mutual TLS)")
            print(f"  cluster cert    {self.tls_cert_path}")
            print(f"  fingerprint     {self.tls_fingerprint[:47]}...")
            print(f"                  must match on every node")
        else:
            print(f"  data plane      {self.ip}:{self.data_port}   "
                  f"(TCP, *** PLAINTEXT -- --insecure ***)")
        print(f"  discovery       UDP :{self.discovery.port} "
              f"broadcast={'on' if self.discovery.broadcast_ok else 'OFF'}")
        print(f"  dashboard       http://127.0.0.1:{self.api_port}")
        print("=" * 66)
        print(f"  On the other laptop (copy the cert files first) run:")
        print(f"    python3 -m memcloud.node --name <other> "
              f"--peer {self.ip}:{self.data_port}")
        print("=" * 66, flush=True)

    def stop(self) -> None:
        self.discovery.stop()
        self.worker.stop()
        self.api.stop()


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="memcloud", description="MemCloud node")
    p.add_argument("--name", default=None, help="human name for this node")
    p.add_argument("--cluster", default="default", help="cluster name; must match on all nodes")
    p.add_argument("--bind", default="0.0.0.0", help="data plane bind address")
    p.add_argument("--data-port", type=int, default=DEFAULT_DATA_PORT)
    p.add_argument("--api-bind", default="127.0.0.1")
    p.add_argument("--api-port", type=int, default=DEFAULT_API_PORT)
    p.add_argument("--discovery-port", type=int, default=DISCOVERY_PORT)
    p.add_argument(
        "--peer", action="append",
        help="manual peer host:port (repeatable). Use when UDP broadcast is blocked.",
    )
    p.add_argument("--budget-mb", type=int, default=None,
                   help="local app cache budget in MB (overrides --budget-pct)")
    p.add_argument("--budget-pct", type=float, default=3.0,
                   help="local app cache budget as %% of physical RAM")
    p.add_argument("--reserve-mb", type=int, default=None,
                   help="RAM offered to peers in MB (overrides --reserve-pct)")
    p.add_argument("--reserve-pct", type=float, default=20.0,
                   help="RAM offered to peers as %% of physical RAM")
    p.add_argument("--min-free-mb", type=int, default=1024,
                   help="never let host available RAM fall below this")
    p.add_argument("--frames-dir", default=None, help="disk cold store directory")
    p.add_argument("--chunk-mb", type=int, default=8,
                   help="chunk size for large objects / tensors, in MB")
    p.add_argument("--tls-cert", default="cluster.pem",
                   help="shared cluster certificate (same file on every node)")
    p.add_argument("--tls-key", default="cluster.key",
                   help="shared cluster private key (same file on every node)")
    p.add_argument("--insecure", action="store_true",
                   help="disable TLS. Debugging only; traffic is plaintext.")
    return p


def main(argv: List[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.frames_dir is None:
        args.frames_dir = os.path.join(
            os.path.expanduser("~"), ".memcloud", (args.name or "node"), "frames"
        )
    node = Node(args)
    node.start()
    try:
        while True:
            time.sleep(1.0)
    except KeyboardInterrupt:
        print("\nshutting down")
        node.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
