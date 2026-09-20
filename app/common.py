"""Small standard-library helpers shared by the lab services."""

from __future__ import annotations

import json
import math
import os
import threading
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any


def now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def percentile(values: list[float], percentile_value: float = 0.95) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    index = min(len(ordered) - 1, max(0, math.ceil(len(ordered) * percentile_value) - 1))
    return ordered[index]


class JsonLogger:
    """Emit one structured, source-identifiable JSON log event per call."""

    def __init__(self, component: str) -> None:
        self.component = component
        self.service = os.environ.get("SERVICE_NAME", component)
        self.count = 0
        self._lock = threading.Lock()

    def write(self, level: str, message: str, **fields: Any) -> None:
        event = {
            "@timestamp": now(),
            "level": level,
            "message": message,
            "service": self.service,
            "component": self.component,
            "namespace": "commerce",
            "cluster": "fcapsule-lab",
            "container": self.component,
            **fields,
        }
        with self._lock:
            self.count += 1
        print(json.dumps(event, ensure_ascii=True), flush=True)


def labels(**items: str | int | float) -> str:
    if not items:
        return ""
    rendered_items = []
    for key, value in sorted(items.items()):
        escaped = str(value).replace("\\", "\\\\").replace('"', '\\"')
        rendered_items.append(f'{key}="{escaped}"')
    rendered = ",".join(rendered_items)
    return "{" + rendered + "}"


def counter_line(name: str, help_text: str, value: float, **metric_labels: str | int | float) -> str:
    return f"# HELP {name} {help_text}\n# TYPE {name} counter\n{name}{labels(**metric_labels)} {value}\n"


def gauge_line(name: str, help_text: str, value: float, **metric_labels: str | int | float) -> str:
    return f"# HELP {name} {help_text}\n# TYPE {name} gauge\n{name}{labels(**metric_labels)} {value}\n"


class QuietHandler(BaseHTTPRequestHandler):
    """HTTP handler base with JSON/text response helpers and no access-log noise."""

    def send_json(self, status: int, payload: dict[str, Any]) -> None:
        body = json.dumps(payload, ensure_ascii=True).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        try:
            self.wfile.write(body)
        except (BrokenPipeError, ConnectionResetError):
            return

    def send_text(self, status: int, body: str, content_type: str = "text/plain; version=0.0.4; charset=utf-8") -> None:
        encoded = body.encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(encoded)))
        self.end_headers()
        try:
            self.wfile.write(encoded)
        except (BrokenPipeError, ConnectionResetError):
            return

    def send_html(self, status: int, body: str) -> None:
        self.send_text(status, body, "text/html; charset=utf-8")

    def body_json(self) -> dict[str, Any]:
        length = int(self.headers.get("Content-Length", "0"))
        if not length:
            return {}
        value = json.loads(self.rfile.read(length))
        if not isinstance(value, dict):
            raise ValueError("JSON body must be an object")
        return value

    def log_message(self, _format: str, *args: Any) -> None:
        return


def serve(handler: type[BaseHTTPRequestHandler], port: int) -> None:
    server = ThreadingHTTPServer(("0.0.0.0", port), handler)
    server.serve_forever()
