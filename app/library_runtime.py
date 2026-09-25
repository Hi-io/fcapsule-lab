"""Bounded workload for the supplementary incident library."""

from __future__ import annotations

import hashlib
import hmac
import json
import os
import queue
import socket
import sqlite3
import tempfile
import threading
import time
import uuid
from collections import Counter
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.error import HTTPError
from urllib.request import urlopen

from app.common import JsonLogger, QuietHandler, labels, serve
from app.incident_library import load_cases, public_cases


class _UpstreamHandler(BaseHTTPRequestHandler):
    def do_GET(self) -> None:  # noqa: N802
        if self.path == "/delay":
            time.sleep(0.12)
            body, status = b'{"ok":true}', 200
        elif self.path == "/status":
            body, status = b'{"error":"upstream unavailable"}', 503
        else:
            body, status = b'{"revision":"v2","items":[]}', 200
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        try:
            self.wfile.write(body)
        except (BrokenPipeError, ConnectionResetError):
            pass

    def log_message(self, _format: str, *args: Any) -> None:
        pass


class FailureExecutor:
    """Exercise small real failures without changing the node or other workloads."""

    def __init__(self) -> None:
        self.upstream = ThreadingHTTPServer(("127.0.0.1", 0), _UpstreamHandler)
        threading.Thread(target=self.upstream.serve_forever, daemon=True).start()
        self.base_url = f"http://127.0.0.1:{self.upstream.server_port}"

    def close(self) -> None:
        self.upstream.shutdown()
        self.upstream.server_close()

    def execute(self, case: dict[str, Any]) -> None:
        mode = case["mechanism"]
        if mode == "db_unique":
            with sqlite3.connect(":memory:") as db:
                db.execute("CREATE TABLE records (key TEXT UNIQUE)")
                db.execute("INSERT INTO records VALUES ('duplicate-key')")
                db.execute("INSERT INTO records VALUES ('duplicate-key')")
        elif mode == "db_foreign_key":
            with sqlite3.connect(":memory:") as db:
                db.execute("PRAGMA foreign_keys=ON")
                db.execute("CREATE TABLE parents (id INTEGER PRIMARY KEY)")
                db.execute("CREATE TABLE children (parent_id INTEGER REFERENCES parents(id))")
                db.execute("INSERT INTO children VALUES (404)")
        elif mode == "db_locked":
            with tempfile.TemporaryDirectory() as directory:
                path = str(Path(directory) / "records.db")
                with sqlite3.connect(path) as first, sqlite3.connect(path, timeout=0.01) as second:
                    first.execute("CREATE TABLE records (id INTEGER)")
                    first.commit()
                    first.execute("BEGIN EXCLUSIVE")
                    first.execute("INSERT INTO records VALUES (1)")
                    second.execute("INSERT INTO records VALUES (2)")
        elif mode == "db_schema":
            with sqlite3.connect(":memory:") as db:
                db.execute("CREATE TABLE records (id INTEGER PRIMARY KEY)")
                db.execute("SELECT migrated_value FROM records")
        elif mode == "http_timeout":
            with urlopen(self.base_url + "/delay", timeout=0.025) as response:
                response.read()
        elif mode == "http_status":
            with urlopen(self.base_url + "/status", timeout=1) as response:
                response.read()
        elif mode == "http_contract":
            with urlopen(self.base_url + "/contract", timeout=1) as response:
                document = json.load(response)
            if document.get("revision") != "v1":
                raise ValueError(f"response revision {document.get('revision')} does not match v1")
        elif mode == "dns_lookup":
            socket.getaddrinfo("invalid host name", 443)
        elif mode == "tcp_refused":
            with socket.socket() as listener:
                listener.bind(("127.0.0.1", 0))
                port = listener.getsockname()[1]
            with socket.create_connection(("127.0.0.1", port), timeout=0.2):
                pass
        elif mode == "queue_poison":
            json.loads('{"event": invalid}')
        elif mode == "queue_backlog":
            tasks: queue.Queue[str] = queue.Queue(maxsize=1)
            tasks.put_nowait("first")
            tasks.put_nowait("second")
        elif mode == "auth_expired":
            issued_at = time.time() - 120
            if time.time() - issued_at > 60:
                raise PermissionError("token expired before downstream verification")
        elif mode == "auth_signature":
            signature = hmac.new(b"rotated-key", b"request", hashlib.sha256).digest()
            expected = hmac.new(b"previous-key", b"request", hashlib.sha256).digest()
            if not hmac.compare_digest(signature, expected):
                raise PermissionError("request signature verification failed")
        elif mode == "config_missing":
            settings = {"SERVICE_URL": "http://internal"}
            if "REQUIRED_PROFILE" not in settings:
                raise KeyError("REQUIRED_PROFILE")
        elif mode == "config_invalid":
            configured = "not-an-integer"
            int(configured)
        elif mode == "cache_stale":
            last_refresh = time.monotonic() - 90
            if time.monotonic() - last_refresh > 30:
                raise TimeoutError("cached entry exceeded its freshness window")
        elif mode == "file_missing":
            with tempfile.TemporaryDirectory() as directory:
                Path(directory, "not-published.json").read_text(encoding="utf-8")
        elif mode == "resource_memory":
            buffer = bytearray(2 * 1024 * 1024)
            if len(buffer) > 1024 * 1024:
                raise MemoryError("request buffer exceeded its 1 MiB application budget")
        elif mode == "resource_cpu":
            started = time.perf_counter()
            hashlib.pbkdf2_hmac("sha256", b"request", b"salt", 30000)
            if time.perf_counter() - started > 0.0001:
                raise TimeoutError("CPU work exceeded the request execution budget")
        elif mode == "rate_limit":
            tokens = 1
            for _ in range(2):
                if tokens == 0:
                    raise BlockingIOError("per-client request budget exhausted")
                tokens -= 1
        elif mode == "thread_pool":
            slots = threading.BoundedSemaphore(1)
            slots.acquire()
            try:
                if not slots.acquire(blocking=False):
                    raise TimeoutError("all worker slots are occupied")
            finally:
                slots.release()
        else:
            raise ValueError(f"Unsupported mechanism: {mode}")


class LibraryState:
    def __init__(self, cases: dict[str, dict[str, Any]] | None = None) -> None:
        self.cases = cases if cases is not None else load_cases()
        self.logger = JsonLogger("lab-incident-library")
        self.executor = FailureExecutor()
        self.lock = threading.RLock()
        self.active: dict[str, Any] | None = None
        self.attempts: Counter[str] = Counter()
        self.failures: Counter[str] = Counter()
        self.last_duration: dict[str, float] = {}

    def start(self, case_id: str, run_id: str, duration: int) -> dict[str, Any]:
        if case_id not in self.cases:
            raise ValueError("Unknown library case")
        if not isinstance(run_id, str) or len(run_id) != 32 or any(c not in "0123456789abcdef" for c in run_id):
            raise ValueError("Invalid run ID")
        if not isinstance(duration, int) or not 15 <= duration <= 300:
            raise ValueError("Duration must be between 15 and 300 seconds")
        with self.lock:
            if self.active and time.time() < self.active["expires_at"]:
                raise ValueError("Another library case is active")
            self.active = {"case_id": case_id, "run_id": run_id, "expires_at": time.time() + duration}
            self.logger.write("INFO", "Library workload activated", case_id=case_id, run_id=run_id,
                              simulated_service=self.cases[case_id]["service"])
            return {"ok": True, "run_id": run_id, "case_id": case_id}

    def recover(self, run_id: str) -> dict[str, Any]:
        with self.lock:
            if self.active and self.active["run_id"] != run_id:
                raise ValueError("Run ownership changed")
            self.active = None
            self.logger.write("INFO", "Library workload recovered", run_id=run_id)
            return {"ok": True, "run_id": run_id}

    def tick(self) -> None:
        with self.lock:
            active = dict(self.active) if self.active else None
            if active and time.time() >= active["expires_at"]:
                self.active = None
                active = None
        if not active:
            return
        case_id = active["case_id"]
        case = self.cases[case_id]
        request_id = uuid.uuid4().hex[:12]
        fields = {"case_id": case_id, "run_id": active["run_id"], "request_id": request_id,
                  "simulated_service": case["service"], "category": case["category"],
                  "scenario_context": case["context"]}
        self.logger.write("INFO", "Request accepted by simulated workload", **fields)
        self.logger.write("INFO", "Dependency operation started", **fields)
        started = time.perf_counter()
        error: Exception | None = None
        try:
            self.executor.execute(case)
        except Exception as exc:
            error = exc
        elapsed = time.perf_counter() - started
        with self.lock:
            self.attempts[case_id] += 1
            if error:
                self.failures[case_id] += 1
            self.last_duration[case_id] = elapsed
        if error:
            self.logger.write("ERROR", "Dependency operation failed", **fields,
                              error_type=type(error).__name__, error_detail=str(error)[:180],
                              duration_ms=round(elapsed * 1000, 2))
            self.logger.write("WARN", "Request completed with degraded outcome", **fields, status="failed")
        else:
            self.logger.write("INFO", "Request completed", **fields, status="ok")

    def run(self) -> None:
        while True:
            try:
                self.tick()
            except Exception as exc:
                self.logger.write("ERROR", "Library runner cycle failed", error_type=type(exc).__name__)
            time.sleep(0.5)

    def metrics(self) -> str:
        lines = [
            "# HELP lab_library_attempts_total Library workload attempts\n# TYPE lab_library_attempts_total counter\n",
            "# HELP lab_library_failures_total Library workload failures\n# TYPE lab_library_failures_total counter\n",
            "# HELP lab_library_active Active library scenario\n# TYPE lab_library_active gauge\n",
            "# HELP lab_library_last_duration_seconds Last attempted operation duration\n# TYPE lab_library_last_duration_seconds gauge\n",
        ]
        with self.lock:
            active = dict(self.active) if self.active else None
            keys = set(self.attempts) | ({active["case_id"]} if active else set())
            for key in sorted(keys):
                suffix = labels(scenario_id=key)
                lines.append(f"lab_library_attempts_total{suffix} {self.attempts[key]}\n")
                lines.append(f"lab_library_failures_total{suffix} {self.failures[key]}\n")
                lines.append(f"lab_library_active{suffix} {int(bool(active and active['case_id'] == key))}\n")
                lines.append(f"lab_library_last_duration_seconds{suffix} {self.last_duration.get(key, 0)}\n")
        return "".join(lines)


def handler(state: LibraryState) -> type[QuietHandler]:
    class LibraryHandler(QuietHandler):
        def do_GET(self) -> None:  # noqa: N802
            if self.path == "/health":
                self.send_json(HTTPStatus.OK, {"status": "ok"})
            elif self.path == "/metrics":
                self.send_text(HTTPStatus.OK, state.metrics())
            elif self.path == "/api/cases":
                self.send_json(HTTPStatus.OK, {"cases": public_cases(state.cases)})
            else:
                self.send_json(HTTPStatus.NOT_FOUND, {"error": "not_found"})

        def do_POST(self) -> None:  # noqa: N802
            try:
                body = self.body_json()
                if self.path == "/control/scenario":
                    if body.get("mode") == "normal":
                        self.send_json(HTTPStatus.OK, state.recover(body.get("run_id")))
                    else:
                        self.send_json(HTTPStatus.ACCEPTED, state.start(
                            body.get("mode"), body.get("run_id"), body.get("duration_seconds", 180)))
                else:
                    self.send_json(HTTPStatus.NOT_FOUND, {"error": "not_found"})
            except (ValueError, TypeError) as exc:
                self.send_json(HTTPStatus.BAD_REQUEST, {"error": str(exc)})

    return LibraryHandler


def main() -> None:
    state = LibraryState()
    threading.Thread(target=state.run, daemon=True).start()
    serve(handler(state), int(os.environ.get("PORT", "8085")))


if __name__ == "__main__":
    main()
