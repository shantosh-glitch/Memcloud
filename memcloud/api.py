"""Control-plane HTTP API + dashboard. Standard library http.server only."""

from __future__ import annotations

import json
import queue
import random
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Dict, Optional
from urllib.parse import parse_qs, urlparse

from . import meminfo
from .dashboard import DASHBOARD_HTML


class _Handler(BaseHTTPRequestHandler):
    server_version = "MemCloud/1.0"
    protocol_version = "HTTP/1.1"

    # -- helpers ------------------------------------------------------
    def log_message(self, fmt: str, *args: Any) -> None:  # quiet
        pass

    @property
    def node(self):
        return self.server.node  # type: ignore[attr-defined]

    def _send(self, code: int, body: bytes, ctype: str) -> None:
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        try:
            self.wfile.write(body)
        except (BrokenPipeError, ConnectionResetError):
            pass

    def _json(self, obj: Any, code: int = 200) -> None:
        self._send(code, json.dumps(obj).encode("utf-8"), "application/json")

    def _q(self, name: str, default: str = "") -> str:
        qs = parse_qs(urlparse(self.path).query)
        return qs.get(name, [default])[0]

    # -- routes -------------------------------------------------------
    def do_GET(self) -> None:  # noqa: N802
        path = urlparse(self.path).path
        node = self.node

        if path in ("/", "/index.html"):
            self._send(200, DASHBOARD_HTML.encode("utf-8"), "text/html; charset=utf-8")

        elif path == "/api/state":
            self._json(node.state())

        elif path == "/api/status":
            self._json(node.identity())

        elif path == "/api/nodes":
            self._json(node.state()["nodes"])

        elif path == "/api/cache":
            self._json(node.cache.report())

        elif path == "/api/events":
            self._sse()

        elif path.startswith("/api/frame/"):
            self._frame(path.rsplit("/", 1)[-1])

        else:
            self._json({"error": "not found", "path": path}, 404)

    def do_POST(self) -> None:  # noqa: N802
        path = urlparse(self.path).path
        node = self.node

        if path == "/api/demo/seed":
            frames = int(self._q("frames", "400"))
            width = int(self._q("width", "640"))
            height = int(self._q("height", "480"))
            self._json(node.cache.seed(frames, width, height))

        elif path == "/api/demo/reads":
            count = int(self._q("count", "100"))
            ids = list(node.cache.frames.keys())
            if not ids:
                self._json({"error": "cache empty, seed first"}, 400)
                return
            picks = [random.choice(ids) for _ in range(count)]
            entries = node.cache.read_many(picks)
            summary: Dict[str, int] = {}
            for e in entries:
                summary[e["source"]] = summary.get(e["source"], 0) + 1
            self._json({"reads": count, "by_source": summary})

        elif path == "/api/demo/spill":
            frac = float(self._q("fraction", "0.25"))
            target = int(node.memcloud.local_used * frac)
            self._json(node.memcloud.spill(target))

        elif path == "/api/demo/reset":
            node.cache.reset(drop_disk=self._q("disk") == "1")
            self._json({"ok": True})

        elif path == "/api/demo/pressure":
            self._json(node.memcloud.report())

        else:
            self._json({"error": "not found", "path": path}, 404)

    # -- specials -----------------------------------------------------
    def _frame(self, frame_id: str) -> None:
        cache = self.node.cache
        if frame_id in ("random", ""):
            ids = list(cache.frames.keys())
            if not ids:
                self._json({"error": "cache empty"}, 404)
                return
            frame_id = random.choice(ids)
        try:
            data, entry = cache.read(frame_id)
        except KeyError:
            self._json({"error": f"unknown frame {frame_id}"}, 404)
            return
        self.send_response(200)
        self.send_header("Content-Type", "image/bmp")
        self.send_header("Content-Length", str(len(data)))
        self.send_header("Cache-Control", "no-store")
        self.send_header("X-MemCloud-Source", entry["source"])
        self.send_header("X-MemCloud-Where", str(entry["detail"]))
        self.send_header("X-MemCloud-Latency-Ms", str(entry["ms"]))
        self.send_header("X-MemCloud-Verified", str(entry["verified"]))
        self.end_headers()
        try:
            self.wfile.write(data)
        except (BrokenPipeError, ConnectionResetError):
            pass

    def _sse(self) -> None:
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream")
        self.send_header("Cache-Control", "no-cache")
        self.send_header("Connection", "keep-alive")
        self.end_headers()
        q: "queue.Queue[Dict[str, Any]]" = queue.Queue(maxsize=256)
        self.node.subscribe(q)
        try:
            while True:
                try:
                    evt = q.get(timeout=15.0)
                    payload = json.dumps(evt)
                    self.wfile.write(f"data: {payload}\n\n".encode("utf-8"))
                except queue.Empty:
                    self.wfile.write(b": keepalive\n\n")
                self.wfile.flush()
        except Exception:
            pass
        finally:
            self.node.unsubscribe(q)


class _Server(ThreadingHTTPServer):
    daemon_threads = True
    allow_reuse_address = True

    def __init__(self, addr, handler, node) -> None:
        self.node = node
        super().__init__(addr, handler)


class APIServer:
    def __init__(self, node, host: str, port: int) -> None:
        self.node = node
        self.host = host
        self.port = port
        self._server: Optional[_Server] = None

    def start(self) -> int:
        self._server = _Server((self.host, self.port), _Handler, self.node)
        self.port = self._server.server_address[1]
        threading.Thread(
            target=self._server.serve_forever, name="memcloud-api", daemon=True
        ).start()
        return self.port

    def stop(self) -> None:
        if self._server:
            self._server.shutdown()
            self._server.server_close()
