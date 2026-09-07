"""
Layer 1: peer discovery and RAM monitoring.

Two independent paths, because campus/hotel Wi-Fi very often blocks broadcast:

  1. UDP broadcast beacons on DISCOVERY_PORT (zero config).
  2. `--peer host:port` seeds supplied on the command line (always works).

Both feed the same PeerTable. A peer learned by either route is probed over
the data plane (STAT) so the free-RAM figures in the table are the worker's
own measurements, not a guess.
"""

from __future__ import annotations

import json
import socket
import threading
import time
from typing import Any, Callable, Dict, List, Optional

from . import protocol

DISCOVERY_PORT = 47800
BEACON_INTERVAL = 2.0
OFFLINE_AFTER = 8.0
PROBE_INTERVAL = 2.0


class Peer:
    def __init__(self, node_id: str, name: str, host: str, port: int) -> None:
        self.node_id = node_id
        self.name = name
        self.host = host
        self.port = port
        self.last_seen = 0.0
        self.last_probe_ok = 0.0
        self.rtt_ms = 0.0
        self.ram_total = 0
        self.ram_available = 0
        self.reserve_free = 0
        self.hosted_bytes = 0
        self.hosted_blocks = 0
        self.process_rss = 0
        self.static = False  # came from --peer

    @property
    def online(self) -> bool:
        return (time.time() - max(self.last_seen, self.last_probe_ok)) < OFFLINE_AFTER

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.node_id,
            "name": self.name,
            "host": self.host,
            "port": self.port,
            "status": "online" if self.online else "offline",
            "rtt_ms": round(self.rtt_ms, 2),
            "ram_total": self.ram_total,
            "ram_available": self.ram_available,
            "ram_used": max(0, self.ram_total - self.ram_available),
            "ram_percent": (
                round(100.0 * (self.ram_total - self.ram_available) / self.ram_total, 1)
                if self.ram_total
                else 0.0
            ),
            "reserve_free": self.reserve_free,
            "hosted_bytes": self.hosted_bytes,
            "hosted_blocks": self.hosted_blocks,
            "process_rss": self.process_rss,
            "static": self.static,
            "last_seen": self.last_seen,
        }


class PeerTable:
    def __init__(self) -> None:
        self._peers: Dict[str, Peer] = {}
        self._lock = threading.RLock()

    def upsert(self, node_id: str, name: str, host: str, port: int) -> Peer:
        with self._lock:
            p = self._peers.get(node_id)
            if p is None:
                p = Peer(node_id, name, host, port)
                self._peers[node_id] = p
            p.name = name or p.name
            p.host = host
            p.port = port
            return p

    def get(self, node_id: str) -> Optional[Peer]:
        with self._lock:
            return self._peers.get(node_id)

    def all(self) -> List[Peer]:
        with self._lock:
            return list(self._peers.values())

    def online(self) -> List[Peer]:
        return [p for p in self.all() if p.online]


class Discovery:
    """Beacon sender + listener + STAT prober."""

    def __init__(
        self,
        node,
        port: int = DISCOVERY_PORT,
        on_change: Optional[Callable[[str, Peer], None]] = None,
    ) -> None:
        self.node = node
        self.port = port
        self.table = PeerTable()
        self.on_change = on_change
        self._stop = threading.Event()
        self._sock: Optional[socket.socket] = None
        self.broadcast_ok = False

    # -- seeds --------------------------------------------------------
    def add_static_peer(self, host: str, port: int) -> None:
        """Register a manually supplied peer before we know its node id."""
        pid = f"static:{host}:{port}"
        p = self.table.upsert(pid, f"{host}:{port}", host, port)
        p.static = True

    # -- lifecycle ----------------------------------------------------
    def start(self) -> None:
        try:
            self._sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            self._sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            self._sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
            self._sock.bind(("", self.port))
            self._sock.settimeout(1.0)
            self.broadcast_ok = True
        except Exception:
            self._sock = None
            self.broadcast_ok = False

        for target, name in (
            (self._listen_loop, "memcloud-discovery-listen"),
            (self._beacon_loop, "memcloud-discovery-beacon"),
            (self._probe_loop, "memcloud-discovery-probe"),
        ):
            threading.Thread(target=target, name=name, daemon=True).start()

    def stop(self) -> None:
        self._stop.set()
        if self._sock:
            try:
                self._sock.close()
            except Exception:
                pass

    # -- loops --------------------------------------------------------
    def _beacon_loop(self) -> None:
        while not self._stop.is_set():
            if self._sock is not None:
                msg = json.dumps(
                    {
                        "memcloud": 1,
                        "id": self.node.node_id,
                        "name": self.node.name,
                        "data_port": self.node.data_port,
                        "api_port": self.node.api_port,
                        "cluster": self.node.cluster,
                    }
                ).encode("utf-8")
                for addr in ("255.255.255.255", "<broadcast>"):
                    try:
                        self._sock.sendto(msg, (addr, self.port))
                        break
                    except Exception:
                        continue
            self._stop.wait(BEACON_INTERVAL)

    def _listen_loop(self) -> None:
        while not self._stop.is_set():
            if self._sock is None:
                self._stop.wait(1.0)
                continue
            try:
                data, addr = self._sock.recvfrom(4096)
            except socket.timeout:
                continue
            except Exception:
                self._stop.wait(0.5)
                continue
            try:
                msg = json.loads(data.decode("utf-8"))
            except Exception:
                continue
            if msg.get("memcloud") != 1:
                continue
            if msg.get("cluster") != self.node.cluster:
                continue
            nid = msg.get("id")
            if not nid or nid == self.node.node_id:
                continue
            p = self.table.upsert(nid, msg.get("name", nid), addr[0], int(msg["data_port"]))
            first = p.last_seen == 0.0
            p.last_seen = time.time()
            if first and self.on_change:
                self.on_change("peer_discovered", p)

    def _probe_loop(self) -> None:
        while not self._stop.is_set():
            for p in self.table.all():
                if self._stop.is_set():
                    break
                self._probe(p)
            self._stop.wait(PROBE_INTERVAL)

    def _probe(self, p: Peer) -> None:
        t0 = time.perf_counter()
        try:
            sock = protocol.connect(
                p.host, p.port, timeout=3.0,
                ssl_ctx=getattr(self.node, 'ssl_client_ctx', None),
            )
            try:
                header, _ = protocol.request(sock, protocol.OP_STAT)
            finally:
                sock.close()
        except Exception:
            return

        p.rtt_ms = (time.perf_counter() - t0) * 1000.0
        stat = header.get("stat", {})
        ident = header.get("node", {})
        p.ram_total = stat.get("ram_total", 0)
        p.ram_available = stat.get("ram_available", 0)
        p.reserve_free = stat.get("reserve_free", 0)
        p.hosted_bytes = stat.get("hosted_bytes", 0)
        p.hosted_blocks = stat.get("hosted_blocks", 0)
        p.process_rss = stat.get("process_rss", 0)
        p.last_probe_ok = time.time()

        # A statically seeded peer reveals its real node id on first STAT.
        real_id = ident.get("id")
        if real_id and real_id != p.node_id and p.node_id.startswith("static:"):
            with self.table._lock:  # noqa: SLF001 - internal promotion
                self.table._peers.pop(p.node_id, None)
                p.node_id = real_id
                p.name = ident.get("name", p.name)
                p.static = True
                self.table._peers[real_id] = p
            if self.on_change:
                self.on_change("peer_discovered", p)
