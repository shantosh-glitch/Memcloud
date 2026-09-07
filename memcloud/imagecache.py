"""
Layer 7: the image cache demo application.

Read path, tiered:

    request frame
        -> local RAM      (MemCloud local budget)      fastest
        -> peer RAM       (MemCloud remote block)      network hop
        -> disk           (cold store on this laptop)  slowest

Frames are real 24-bit BMP images written to a cold store on disk, so the
disk tier is a genuine file read, not a simulated delay. Every frame carries
a CRC32 recorded at generation time; every read is verified against it, which
is how we prove the bytes that came back over the network are the same bytes
that went out.
"""

from __future__ import annotations

import os
import struct
import threading
import time
import zlib
from collections import deque
from typing import Any, Deque, Dict, List, Optional, Tuple

from . import meminfo
from .client import MemCloud, MissingBlock, NoCapacity

SOURCE_LOCAL = "LOCAL"
SOURCE_PEER = "PEER"
SOURCE_DISK = "DISK"


def make_bmp(width: int, height: int, index: int) -> bytes:
    """Build a real 24-bit BMP. Content is a deterministic function of `index`.

    Each frame gets its own colour drift plus a 16-bit barcode of the frame
    number down the left edge, so two frames are visibly different and a wrong
    frame is obvious on screen.

    Rows are assembled with slice assignment rather than per-pixel Python
    loops, which keeps generation at roughly a few milliseconds per frame.
    """
    row_raw = width * 3
    pad = (4 - (row_raw % 4)) % 4
    row_size = row_raw + pad
    pixel_bytes = row_size * height
    file_size = 54 + pixel_bytes

    header = struct.pack("<2sIHHI", b"BM", file_size, 0, 0, 54)
    info = struct.pack(
        "<IiiHHIIiiII", 40, width, height, 1, 24, 0, pixel_bytes, 2835, 2835, 0, 0
    )

    # Per-frame palette drift.
    r0 = (index * 37) % 256
    g0 = (index * 91) % 256
    b0 = (index * 53) % 256

    denom = max(1, width - 1)
    # BMP pixels are stored B, G, R.
    blue = bytes(((b0 + (x * 255) // denom) & 0xFF) for x in range(width))
    red = bytes(((r0 + (x * 180) // denom) & 0xFF) for x in range(width))

    bar_px = min(24, width)
    bar_h = max(1, height // 16)
    white = b"\xff" * (bar_px * 3)
    black = b"\x00" * (bar_px * 3)
    padding = b"\x00" * pad

    rows = []
    ydenom = max(1, height - 1)
    for y in range(height):
        gv = (g0 + (y * 255) // ydenom) & 0xFF
        pix = bytearray(row_raw)
        pix[0::3] = blue
        pix[1::3] = bytes((gv,)) * width
        pix[2::3] = red
        bit = y // bar_h
        if bit < 16:
            pix[0 : bar_px * 3] = white if (index >> (15 - bit)) & 1 else black
        rows.append(bytes(pix) + padding if pad else bytes(pix))

    rows.reverse()  # BMP is stored bottom-up
    return header + info + b"".join(rows)


class ImageCache:
    """Frame cache backed by MemCloud, with a disk cold store beneath it."""

    def __init__(self, mc: MemCloud, cold_dir: str, emit=None) -> None:
        self.mc = mc
        self.cold_dir = cold_dir
        self.emit = emit or (lambda *_a, **_k: None)
        os.makedirs(cold_dir, exist_ok=True)

        self.frames: Dict[str, Dict[str, Any]] = {}  # id -> {crc, size, path}
        self.log: Deque[Dict[str, Any]] = deque(maxlen=200)
        self._lock = threading.RLock()

        self.counters = {
            "local_hits": 0,
            "peer_hits": 0,
            "disk_reads": 0,
            "errors": 0,
            "corrupt": 0,
        }
        self.latency: Dict[str, List[float]] = {
            SOURCE_LOCAL: [],
            SOURCE_PEER: [],
            SOURCE_DISK: [],
        }
        self.seeding = False
        self.seed_progress: Dict[str, Any] = {"done": 0, "total": 0}

    # -- generation ---------------------------------------------------
    def seed(
        self, count: int, width: int = 640, height: int = 480, write_disk: bool = True
    ) -> Dict[str, Any]:
        """Generate `count` frames, write the cold store, load into MemCloud."""
        with self._lock:
            if self.seeding:
                return {"error": "seed already running"}
            self.seeding = True
            self.seed_progress = {"done": 0, "total": count}

        placed_local = placed_remote = failed = 0
        total_bytes = 0
        t0 = time.perf_counter()
        try:
            for i in range(count):
                fid = f"frame_{i:05d}"
                data = make_bmp(width, height, i)
                crc = zlib.crc32(data) & 0xFFFFFFFF
                path = os.path.join(self.cold_dir, f"{fid}.bmp")
                if write_disk and not os.path.exists(path):
                    with open(path, "wb") as fh:
                        fh.write(data)

                with self._lock:
                    self.frames[fid] = {"crc": crc, "size": len(data), "path": path}
                total_bytes += len(data)

                try:
                    where = self.mc.put(fid, data)
                    if where == "local":
                        placed_local += 1
                    else:
                        placed_remote += 1
                except NoCapacity:
                    failed += 1  # stays on disk only; that is the DISK tier

                with self._lock:
                    self.seed_progress["done"] = i + 1
        finally:
            with self._lock:
                self.seeding = False

        elapsed = time.perf_counter() - t0
        result = {
            "frames": count,
            "frame_bytes": width * height * 3 + 54,
            "total_bytes": total_bytes,
            "placed_local": placed_local,
            "placed_remote": placed_remote,
            "disk_only": failed,
            "seconds": round(elapsed, 2),
        }
        self.emit("seed_complete", result)
        return result

    # -- read path ----------------------------------------------------
    def read(self, frame_id: str) -> Tuple[bytes, Dict[str, Any]]:
        with self._lock:
            meta = self.frames.get(frame_id)
        if meta is None:
            raise KeyError(frame_id)

        t0 = time.perf_counter()
        source = SOURCE_DISK
        detail = "cold store"
        data: Optional[bytes] = None

        loc = self.mc.location(frame_id)
        if loc is not None:
            try:
                data, src, _ms = self.mc.get(frame_id)
                if src == "local":
                    source, detail = SOURCE_LOCAL, "local RAM"
                else:
                    peer = self.mc.peers.get(src)
                    source = SOURCE_PEER
                    detail = peer.name if peer else src
            except MissingBlock:
                data = None

        if data is None:
            with open(meta["path"], "rb") as fh:
                data = fh.read()
            source, detail = SOURCE_DISK, "cold store"

        ms = (time.perf_counter() - t0) * 1000.0
        ok = (zlib.crc32(data) & 0xFFFFFFFF) == meta["crc"]

        with self._lock:
            if source == SOURCE_LOCAL:
                self.counters["local_hits"] += 1
            elif source == SOURCE_PEER:
                self.counters["peer_hits"] += 1
            else:
                self.counters["disk_reads"] += 1
            if not ok:
                self.counters["corrupt"] += 1
            samples = self.latency[source]
            samples.append(ms)
            if len(samples) > 500:
                del samples[: len(samples) - 500]
            entry = {
                "frame": frame_id,
                "source": source,
                "detail": detail,
                "ms": round(ms, 2),
                "bytes": len(data),
                "verified": ok,
                "at": time.time(),
            }
            self.log.appendleft(entry)

        return data, entry

    def read_many(self, frame_ids: List[str]) -> List[Dict[str, Any]]:
        out = []
        for fid in frame_ids:
            try:
                _, entry = self.read(fid)
                out.append(entry)
            except Exception as exc:
                with self._lock:
                    self.counters["errors"] += 1
                out.append({"frame": fid, "source": "ERROR", "detail": str(exc)})
        return out

    # -- reporting ----------------------------------------------------
    @staticmethod
    def _avg(xs: List[float]) -> Optional[float]:
        return round(sum(xs) / len(xs), 2) if xs else None

    @staticmethod
    def _p95(xs: List[float]) -> Optional[float]:
        if not xs:
            return None
        s = sorted(xs)
        return round(s[min(len(s) - 1, int(0.95 * len(s)))], 2)

    def report(self) -> Dict[str, Any]:
        with self._lock:
            counters = dict(self.counters)
            log = list(self.log)[:40]
            lat = {
                k: {
                    "avg_ms": self._avg(v),
                    "p95_ms": self._p95(v),
                    "n": len(v),
                }
                for k, v in self.latency.items()
            }
            total_frames = len(self.frames)
            seeding = self.seeding
            progress = dict(self.seed_progress)

        mcr = self.mc.report()
        served = counters["local_hits"] + counters["peer_hits"] + counters["disk_reads"]
        return {
            "frames_known": total_frames,
            "counters": counters,
            "hit_rate_ram": round(
                100.0 * (counters["local_hits"] + counters["peer_hits"]) / served, 1
            )
            if served
            else 0.0,
            "latency": lat,
            "recent": log,
            "memcloud": mcr,
            "seeding": seeding,
            "seed_progress": progress,
            "local_ram_human": meminfo.human(mcr["local_bytes"]),
            "remote_ram_human": meminfo.human(mcr["remote_bytes"]),
        }

    def reset(self, drop_disk: bool = False) -> None:
        self.mc.clear()
        with self._lock:
            if drop_disk:
                for meta in self.frames.values():
                    try:
                        os.remove(meta["path"])
                    except OSError:
                        pass
                self.frames.clear()
            self.log.clear()
            for k in self.counters:
                self.counters[k] = 0
            for k in self.latency:
                self.latency[k] = []
