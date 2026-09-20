"""Background worker with observable CPU, memory, and poison-job failures."""

from __future__ import annotations

import json
import base64
import hashlib
import os
import threading
import time
from http import HTTPStatus
from typing import Any
from urllib.parse import urlparse

import pymysql

from app.common import JsonLogger, QuietHandler, counter_line, gauge_line, serve
from app.safety import lease_seconds


MODES = {"normal", "cpu-saturation", "memory-leak"}


class WorkerState:
    def __init__(self) -> None:
        self.logger = JsonLogger("worker")
        self.mode = "normal"
        self.jobs = 0
        self.cpu_iterations = 0
        self.allocated_bytes = 0
        self._memory: list[bytearray] = []
        self._mode_stop = threading.Event()
        self._lock = threading.Lock()
        self._expires_at = 0.0
        self.db = {
            "host": os.environ.get("MYSQL_HOST", "mysql"),
            "user": os.environ.get("MYSQL_USER", "inventory"),
            "password": os.environ.get("MYSQL_PASSWORD", "inventory-lab"),
            "database": os.environ.get("MYSQL_DATABASE", "inventory"),
        }

    def initialize(self) -> None:
        for attempt in range(1, 61):
            try:
                with self._connect() as connection:
                    with connection.cursor() as cursor:
                        cursor.execute(
                            "CREATE TABLE IF NOT EXISTS lab_jobs "
                            "(id BIGINT AUTO_INCREMENT PRIMARY KEY, kind VARCHAR(32) NOT NULL, payload TEXT, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
                        )
                    connection.commit()
                break
            except pymysql.MySQLError as exc:
                self.logger.write("WARN", "Worker waiting for MySQL", attempt=attempt, error=str(exc)[:180])
                time.sleep(1)
        threading.Thread(target=self._heartbeat_loop, daemon=True).start()

    def _connect(self) -> pymysql.Connection:
        return pymysql.connect(**self.db, connect_timeout=2, read_timeout=3, write_timeout=3, autocommit=False)

    def set_mode(self, mode: str, duration: int = 180) -> None:
        if mode not in MODES:
            raise ValueError(f"Unknown worker mode: {mode}")
        duration = lease_seconds(duration)
        self._mode_stop.set()
        self._mode_stop = threading.Event()
        self._memory = []
        with self._lock:
            self.mode = mode
            self.allocated_bytes = 0
            self._expires_at = time.monotonic() + duration if mode != "normal" else 0
        self.logger.write("INFO", "Batch scheduler configuration applied", export_delivery="buffered" if mode == "memory-leak" else "streaming", credential_rounds=1200000 if mode == "cpu-saturation" else 1000)
        if mode == "cpu-saturation":
            threading.Thread(target=self._burn_cpu, args=(self._mode_stop,), daemon=True).start()
        elif mode == "memory-leak":
            threading.Thread(target=self._leak_memory, args=(self._mode_stop,), daemon=True).start()

    def _burn_cpu(self, stop: threading.Event) -> None:
        while not stop.is_set():
            hashlib.pbkdf2_hmac("sha256", b"test-account-password", b"migration-salt", 1200000)
            with self._lock:
                self.cpu_iterations += 1
            self.logger.write("INFO", "Credential migration record completed", records=self.cpu_iterations, kdf="pbkdf2_sha256", rounds=1200000)

    def _leak_memory(self, stop: threading.Event) -> None:
        page = (json.dumps({"sku":"sku-red-widget", "quantity":1, "description":"Inventory ledger export"}) + "\n").encode() * 24000
        chunk_size = len(page)
        while not stop.is_set():
            buffer = bytearray(page)
            with self._lock:
                if stop.is_set():
                    break
                self._memory.append(buffer)
                self.allocated_bytes += chunk_size
                allocated = self.allocated_bytes
            self.logger.write(
                "INFO", "Export page encoded",
                buffered_bytes=allocated, page_bytes=chunk_size,
                delivery="buffered", rows=24000,
            )
            stop.wait(0.8)

    def _job_loop(self) -> None:
        while True:
            try:
                with self._connect() as connection:
                    with connection.cursor() as cursor:
                        cursor.execute("DELETE FROM lab_jobs WHERE created_at < UTC_TIMESTAMP() - INTERVAL 300 SECOND")
                        connection.commit()
                        cursor.execute("SELECT id, kind, payload FROM lab_jobs ORDER BY id LIMIT 1")
                        job = cursor.fetchone()
                        if job:
                            self.logger.write("INFO", "Import delivery received", job_id=job[0], acknowledgement="after_commit")
                            try:
                                decode_job(job[2])
                            except (ValueError, KeyError, TypeError) as exc:
                                self.logger.write("ERROR", "Import decoder rejected document", job_id=job[0], error_type=type(exc).__name__, error=str(exc), acknowledgement="pending")
                                raise
                            cursor.execute("DELETE FROM lab_jobs WHERE id=%s", (job[0],))
                            connection.commit()
                            with self._lock:
                                self.jobs += 1
            except pymysql.MySQLError as exc:
                self.logger.write("WARN", "Worker queue poll failed", error=str(exc)[:180])
            time.sleep(2)

    def _heartbeat_loop(self) -> None:
        while True:
            with self._lock:
                jobs = self.jobs
                expired = self._expires_at and time.monotonic() >= self._expires_at
            if expired:
                self.set_mode("normal")
            self.logger.write("INFO", "Worker scheduler heartbeat", completed_jobs=jobs)
            time.sleep(1)

    def status(self) -> dict[str, Any]:
        with self._lock:
            return {
                "status": "ok",
                "mode": self.mode,
                "allocated_bytes": self.allocated_bytes,
                "cpu_iterations": self.cpu_iterations,
                "completed_jobs": self.jobs,
            }

    def metrics(self) -> str:
        state = self.status()
        return "".join(
            [
                gauge_line("lab_worker_allocated_bytes", "Bytes retained by worker batch cache", state["allocated_bytes"]),
                counter_line("lab_worker_cpu_iterations_total", "CPU-bound worker iterations", state["cpu_iterations"]),
                counter_line("lab_worker_jobs_total", "Completed background jobs", state["completed_jobs"]),
                counter_line("lab_worker_log_events_total", "Structured worker log events emitted", self.logger.count),
            ]
        )


def handler(state: WorkerState) -> type[QuietHandler]:
    class WorkerHandler(QuietHandler):
        def do_GET(self) -> None:  # noqa: N802
            if self.path == "/health":
                self.send_json(HTTPStatus.OK, state.status())
                return
            if self.path == "/metrics":
                self.send_text(HTTPStatus.OK, state.metrics())
                return
            self.send_json(HTTPStatus.NOT_FOUND, {"error": "not_found"})

        def do_POST(self) -> None:  # noqa: N802
            if urlparse(self.path).path != "/control/scenario":
                self.send_json(HTTPStatus.NOT_FOUND, {"error": "not_found"})
                return
            try:
                payload = self.body_json()
                state.set_mode(str(payload.get("mode", "")), payload.get("duration_seconds", 180))
            except (ValueError, OSError, json.JSONDecodeError) as exc:
                self.send_json(HTTPStatus.BAD_REQUEST, {"error": str(exc)})
                return
            self.send_json(HTTPStatus.OK, state.status())

    return WorkerHandler


def main() -> None:
    state = WorkerState()
    state.initialize()
    threading.Thread(target=serve, args=(handler(state), int(os.environ.get("PORT", "8083"))), daemon=True).start()
    state._job_loop()


def decode_job(payload: str) -> dict:
    envelope = json.loads(payload)
    return json.loads(base64.b64decode(envelope["body"], validate=True))


if __name__ == "__main__":
    main()
