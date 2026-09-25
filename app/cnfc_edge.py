"""Two-replica dependency probe used to demonstrate CNFC-scoped investigations."""

from __future__ import annotations

import json
import os
import re
import threading
import time
from http import HTTPStatus
from urllib.error import HTTPError, URLError
from urllib.parse import urlparse
from urllib.request import urlopen

from app.common import JsonLogger, QuietHandler, counter_line, gauge_line, serve


class EdgeState:
    def __init__(self) -> None:
        self.baseline_url = os.environ.get("INVENTORY_URL", "http://inventory-api:8081").rstrip("/")
        self.fault_url = os.environ.get("DRIFT_INVENTORY_URL", "http://inventory-api:8099").rstrip("/")
        self.cnfc = os.environ.get("CNFC_ID", "checkout-edge-east")
        self.logger = JsonLogger("cnfc-edge")
        self.lock = threading.Lock()
        self.url = self.baseline_url
        self.run_id: str | None = None
        self.expires_at = 0.0
        self.checks = {"success": 0, "failure": 0}

    def set_mode(self, mode: str, duration: int = 180, run_id: str | None = None) -> dict:
        if mode not in {"normal", "route-drift"}:
            raise ValueError("Unknown CNFC edge mode")
        if run_id is not None and not re.fullmatch(r"[0-9a-f]{32}", run_id):
            raise ValueError("Invalid run ID")
        if not 30 <= int(duration) <= 300:
            raise ValueError("Duration must be between 30 and 300 seconds")
        with self.lock:
            self.url = self.fault_url if mode == "route-drift" else self.baseline_url
            self.run_id = run_id
            self.expires_at = time.monotonic() + int(duration) if mode != "normal" else 0.0
            endpoint = urlparse(self.url)
        self.logger.write("WARN" if mode != "normal" else "INFO", "Dependency route applied",
                          cnfc=self.cnfc, mode=mode, dependency_host=endpoint.hostname,
                          dependency_port=endpoint.port, run_id=run_id)
        return {"status": "ok", "mode": mode, "run_id": run_id}

    def probe(self) -> bool:
        with self.lock:
            expired = self.expires_at and time.monotonic() >= self.expires_at
            url = self.url
            run_id = self.run_id
        if expired:
            self.set_mode("normal", run_id=run_id)
            url = self.baseline_url
        endpoint = urlparse(url)
        try:
            with urlopen(url + "/health", timeout=1.5) as response:
                if response.status != HTTPStatus.OK:
                    raise OSError(f"Dependency returned HTTP {response.status}")
        except (HTTPError, URLError, TimeoutError, OSError) as exc:
            with self.lock:
                self.checks["failure"] += 1
            self.logger.write("ERROR", "Inventory dependency probe failed", cnfc=self.cnfc,
                              dependency_host=endpoint.hostname, dependency_port=endpoint.port,
                              error_type=type(exc).__name__, run_id=run_id)
            return False
        with self.lock:
            self.checks["success"] += 1
            successes = self.checks["success"]
        if successes % 10 == 1:
            self.logger.write("INFO", "Inventory dependency probe succeeded", cnfc=self.cnfc,
                              dependency_host=endpoint.hostname, dependency_port=endpoint.port)
        return True

    def metrics(self) -> str:
        with self.lock:
            success, failure = self.checks["success"], self.checks["failure"]
            port = urlparse(self.url).port or 0
        return (counter_line("lab_cnfc_edge_dependency_checks_total", "Inventory dependency probes", success,
                             outcome="success")
                + counter_line("lab_cnfc_edge_dependency_failures_total", "Failed inventory dependency probes", failure)
                + gauge_line("lab_cnfc_edge_dependency_port", "Active inventory dependency port", port))


def handler(state: EdgeState) -> type[QuietHandler]:
    class EdgeHandler(QuietHandler):
        def do_GET(self) -> None:  # noqa: N802
            if self.path == "/health":
                self.send_json(HTTPStatus.OK, {"status": "ok"})
            elif self.path == "/metrics":
                self.send_text(HTTPStatus.OK, state.metrics())
            else:
                self.send_json(HTTPStatus.NOT_FOUND, {"error": "not_found"})

        def do_POST(self) -> None:  # noqa: N802
            if self.path != "/control/scenario":
                self.send_json(HTTPStatus.NOT_FOUND, {"error": "not_found"})
                return
            try:
                payload = self.body_json()
                result = state.set_mode(str(payload.get("mode", "")),
                                        int(payload.get("duration_seconds", 180)), payload.get("run_id"))
                self.send_json(HTTPStatus.OK, result)
            except (ValueError, TypeError, json.JSONDecodeError) as exc:
                self.send_json(HTTPStatus.BAD_REQUEST, {"error": str(exc)})

    return EdgeHandler


def main() -> None:
    state = EdgeState()

    def probe_loop() -> None:
        while True:
            state.probe()
            time.sleep(1)

    threading.Thread(target=probe_loop, daemon=True).start()
    serve(handler(state), int(os.environ.get("PORT", "8085")))


if __name__ == "__main__":
    main()
