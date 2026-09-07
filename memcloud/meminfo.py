"""
Cross-platform physical RAM telemetry.

Standard library only. Uses psutil if it happens to be installed, otherwise
falls back to platform-native sources:

  Linux   -> /proc/meminfo  (MemTotal, MemAvailable)
  macOS   -> sysctl hw.memsize + vm_stat
  Windows -> ctypes GlobalMemoryStatusEx

All values are BYTES.
"""

from __future__ import annotations

import ctypes
import os
import platform
import re
import subprocess
import sys
from typing import Tuple

try:  # optional, never required
    import psutil  # type: ignore

    _HAS_PSUTIL = True
except Exception:  # pragma: no cover
    _HAS_PSUTIL = False


_SYSTEM = platform.system()


def _linux() -> Tuple[int, int]:
    total = avail = 0
    with open("/proc/meminfo", "r") as fh:
        for line in fh:
            if line.startswith("MemTotal:"):
                total = int(line.split()[1]) * 1024
            elif line.startswith("MemAvailable:"):
                avail = int(line.split()[1]) * 1024
            if total and avail:
                break
    return total, avail


def _macos() -> Tuple[int, int]:
    total = int(
        subprocess.check_output(["sysctl", "-n", "hw.memsize"]).decode().strip()
    )
    out = subprocess.check_output(["vm_stat"]).decode()
    m = re.search(r"page size of (\d+) bytes", out)
    page = int(m.group(1)) if m else 4096

    def pages(name: str) -> int:
        mm = re.search(rf"{name}:\s+(\d+)", out)
        return int(mm.group(1)) if mm else 0

    # "Available" on macOS ~= free + inactive + speculative + purgeable.
    avail = (
        pages("Pages free")
        + pages("Pages inactive")
        + pages("Pages speculative")
        + pages("Pages purgeable")
    ) * page
    return total, min(avail, total)


class _MEMORYSTATUSEX(ctypes.Structure):
    _fields_ = [
        ("dwLength", ctypes.c_ulong),
        ("dwMemoryLoad", ctypes.c_ulong),
        ("ullTotalPhys", ctypes.c_ulonglong),
        ("ullAvailPhys", ctypes.c_ulonglong),
        ("ullTotalPageFile", ctypes.c_ulonglong),
        ("ullAvailPageFile", ctypes.c_ulonglong),
        ("ullTotalVirtual", ctypes.c_ulonglong),
        ("ullAvailVirtual", ctypes.c_ulonglong),
        ("ullAvailExtendedVirtual", ctypes.c_ulonglong),
    ]


def _windows() -> Tuple[int, int]:
    st = _MEMORYSTATUSEX()
    st.dwLength = ctypes.sizeof(_MEMORYSTATUSEX)
    ctypes.windll.kernel32.GlobalMemoryStatusEx(ctypes.byref(st))  # type: ignore[attr-defined]
    return int(st.ullTotalPhys), int(st.ullAvailPhys)


def system_memory() -> Tuple[int, int]:
    """Return (total_bytes, available_bytes) of PHYSICAL system RAM."""
    if _HAS_PSUTIL:
        try:
            vm = psutil.virtual_memory()
            return int(vm.total), int(vm.available)
        except Exception:
            pass
    try:
        if _SYSTEM == "Linux":
            return _linux()
        if _SYSTEM == "Darwin":
            return _macos()
        if _SYSTEM == "Windows":
            return _windows()
    except Exception:
        pass
    # Last resort so the node still runs rather than crashing the demo.
    return 0, 0


def process_rss() -> int:
    """Resident set size of THIS process in bytes. 0 if unavailable.

    This is the number that proves remote blocks are really held in the
    worker's RAM -- when a peer stores 500 MB for us, this grows by ~500 MB.
    """
    if _HAS_PSUTIL:
        try:
            return int(psutil.Process(os.getpid()).memory_info().rss)
        except Exception:
            pass
    try:
        if _SYSTEM == "Linux":
            with open("/proc/self/statm", "r") as fh:
                pages = int(fh.read().split()[1])
            return pages * os.sysconf("SC_PAGE_SIZE")
        if _SYSTEM == "Darwin":
            out = subprocess.check_output(
                ["ps", "-o", "rss=", "-p", str(os.getpid())]
            ).decode()
            return int(out.strip()) * 1024
        if _SYSTEM == "Windows":
            class _PMC(ctypes.Structure):
                _fields_ = [
                    ("cb", ctypes.c_ulong),
                    ("PageFaultCount", ctypes.c_ulong),
                    ("PeakWorkingSetSize", ctypes.c_size_t),
                    ("WorkingSetSize", ctypes.c_size_t),
                    ("QuotaPeakPagedPoolUsage", ctypes.c_size_t),
                    ("QuotaPagedPoolUsage", ctypes.c_size_t),
                    ("QuotaPeakNonPagedPoolUsage", ctypes.c_size_t),
                    ("QuotaNonPagedPoolUsage", ctypes.c_size_t),
                    ("PagefileUsage", ctypes.c_size_t),
                    ("PeakPagefileUsage", ctypes.c_size_t),
                ]

            counters = _PMC()
            counters.cb = ctypes.sizeof(_PMC)
            handle = ctypes.windll.kernel32.GetCurrentProcess()  # type: ignore[attr-defined]
            ctypes.windll.psapi.GetProcessMemoryInfo(  # type: ignore[attr-defined]
                handle, ctypes.byref(counters), counters.cb
            )
            return int(counters.WorkingSetSize)
    except Exception:
        pass
    return 0


def human(n: float) -> str:
    """Format bytes for humans."""
    n = float(n)
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if abs(n) < 1024.0 or unit == "TB":
            if unit == "B":
                return f"{int(n)} B"
            return f"{n:.1f} {unit}"
        n /= 1024.0
    return f"{n:.1f} TB"


if __name__ == "__main__":
    t, a = system_memory()
    print(f"system   total={human(t)} available={human(a)} used={human(t - a)}")
    print(f"process  rss={human(process_rss())}")
    print(f"backend  psutil={_HAS_PSUTIL} system={_SYSTEM} python={sys.version.split()[0]}")
