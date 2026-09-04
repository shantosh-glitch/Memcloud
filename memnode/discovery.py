"""
mDNS discovery via zeroconf: nodes advertise `_memcloud._tcp.local.` and
simultaneously browse for the same service. When a new node resolves an
existing one, we hand off to PeerManager.connect_to() -- the exact same
call a manual `memcli connect <ip>:<port>` makes.

mDNS is LAN-only by construction (multicast doesn't cross most routers),
so machines on a different subnet/VLAN need the manual connect fallback.
That's a known, documented limitation of mDNS itself, not something this
module tries to paper over.
"""
from __future__ import annotations

import asyncio
import logging
import socket
from typing import Callable, Optional

from zeroconf import ServiceStateChange, Zeroconf
from zeroconf.asyncio import AsyncServiceBrowser, AsyncServiceInfo, AsyncZeroconf

from memnode import config

log = logging.getLogger("memnode.discovery")


class Discovery:
    def __init__(self, identity: dict, listen_port: int, on_found: Callable[[str, int], "asyncio.Future"]):
        self.identity = identity
        self.listen_port = listen_port
        self.on_found = on_found          # async callback(host, port)
        self._aiozc: Optional[AsyncZeroconf] = None
        self._service_info: Optional[AsyncServiceInfo] = None
        self._browser: Optional[AsyncServiceBrowser] = None

    async def start(self) -> None:
        self._aiozc = AsyncZeroconf()
        local_ip = _local_ip()
        node_name = f"{self.identity['name']}-{self.identity['node_id'][:8]}"
        self._service_info = AsyncServiceInfo(
            config.MDNS_SERVICE_TYPE,
            f"{node_name}.{config.MDNS_SERVICE_TYPE}",
            addresses=[socket.inet_aton(local_ip)],
            port=self.listen_port,
            properties={"node_id": self.identity["node_id"], "name": self.identity["name"]},
        )
        await self._aiozc.async_register_service(self._service_info)
        self._browser = AsyncServiceBrowser(
            self._aiozc.zeroconf, config.MDNS_SERVICE_TYPE, handlers=[self._on_state_change]
        )
        log.info("mDNS advertising as %s on %s:%d", node_name, local_ip, self.listen_port)

    async def stop(self) -> None:
        if self._browser:
            await self._browser.async_cancel()
        if self._aiozc and self._service_info:
            await self._aiozc.async_unregister_service(self._service_info)
        if self._aiozc:
            await self._aiozc.async_close()

    def _on_state_change(self, zeroconf: Zeroconf, service_type: str, name: str, state_change: ServiceStateChange) -> None:
        if state_change is not ServiceStateChange.Added:
            return
        if self._service_info and name == self._service_info.name:
            return  # that's us
        asyncio.ensure_future(self._resolve_and_connect(zeroconf, service_type, name))

    async def _resolve_and_connect(self, zeroconf: Zeroconf, service_type: str, name: str) -> None:
        try:
            info = AsyncServiceInfo(service_type, name)
            ok = await info.async_request(zeroconf, 3000)
            if not ok or not info.addresses:
                return
            host = socket.inet_ntoa(info.addresses[0])
            port = info.port
            log.info("discovered peer %s at %s:%d", name, host, port)
            await self.on_found(host, port)
        except Exception:
            log.exception("error resolving/connecting to discovered service %s", name)


def _local_ip() -> str:
    """
    Best-effort LAN IP: open a UDP 'connection' to a public address (no
    packet actually sent, UDP connect() is just a routing-table lookup)
    to see which local interface the OS would route through. Falls back
    to loopback if the machine has no route out at all (e.g. fully
    offline), which just means mDNS won't find anyone -- not a crash.
    """
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        s.connect(("8.8.8.8", 80))
        return s.getsockname()[0]
    except OSError:
        return "127.0.0.1"
    finally:
        s.close()
