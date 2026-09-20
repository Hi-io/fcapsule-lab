"""Background worker with observable CPU, memory, and poison-job failures."""

from __future__ import annotations

import json
import os
import threading
import time
from http import HTTPStatus
from typing import Any
from urllib.parse import urlparse

import pymysql

from app.common import JsonLogger, QuietHandler, counter_line, gauge_line, serve


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
        threading.Thread(target=self._job_loop, daemon=True).start()
        threading.Thread(target=self._heartbeat_loop, daemon=True).start()

    def _connect(self) -> pymysql.Connection:
        return pymysql.connect(**self.db, connect_timeout=2, autocommit=False)

    def set_mode(self, mode: str) -> None:
        if mode not in MODES:
            raise ValueError(f"Unknown worker mode: {mode}")
        self._mode_stop.set()
        self._mode_stop = threading.Event()
        self._memory = []
        with self._lock:
            self.mode = mode
            self.allocated_bytes = 0
        self.logger.write("WARN" if mode != "normal" else "INFO", "Worker scenario changed", mode=mode)
        if mode == "cpu-saturation":
            threading.Thread(target=self._burn_cpu, args=(self._mode_stop,), daemon=True).start()
        elif mode == "memory-leak":
            threading.Thread(target=self._leak_memory, args=(self._mode_stop,), daemon=True).start()

    def _burn_cpu(self, stop: threading.Event) -> None:
        value = 1
        while not stop.is_set():
            for candidate in range(1, 250000):
                value = (value * 1664525 + candidate + 1013904223) & 0xFFFFFFFF
            with self._lock:
                self.cpu_iterations += 250000
            self.logger.write("WARN", "Worker batch remains CPU bound", iterations=self.cpu_iterations, checksum=value)

    def _leak_memory(self, stop: threading.Event) -> None:
        chunk_size = 8 * 1024 * 1024
        while not stop.is_set():
            self._memory.append(bytearray(os.urandom(chunk_size)))
            with self._lock:
                self.allocated_bytes += chunk_size
                allocated = self.allocated_bytes
            self.logger.write(
                "ERROR",
                "Unbounded batch cache retained after processing window",
                allocated_bytes=allocated,
                chunk_bytes=chunk_size,
                cache_policy="retain-until-batch-complete",
            )
            time.sleep(0.35)

    def _job_loop(self) -> None:
        while True:
            try:
                with self._connect() as connection:
                    with connection.cursor() as cursor:
                        cursor.execute("SELECT id, kind, payload FROM lab_jobs ORDER BY id LIMIT 1")
                        job = cursor.fetchone()
                        if job and job[1] == "poison":
                            for attempt in range(1, 41):
                                self.logger.write(
                                    "ERROR" if attempt < 40 else "FATAL",
                                    "Poison job failed deterministic decoder validation",
                                    job_id=job[0],
                                    payload=job[2],
                                    attempt=attempt,
                                    disposition="retry_without_ack",
                                )
                            os._exit(17)
                        if job:
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
                mode = self.mode
                jobs = self.jobs
            self.logger.write("INFO", "Worker scheduler heartbeat", mode=mode, completed_jobs=jobs)
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
                gauge_line("lab_worker_mode_info", "Current worker scenario", 1, mode=state["mode"]),
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
                state.set_mode(str(self.body_json().get("mode", "")))
            except (ValueError, OSError, json.JSONDecodeError) as exc:
                self.send_json(HTTPStatus.BAD_REQUEST, {"error": str(exc)})
                return
            self.send_json(HTTPStatus.OK, state.status())

    return WorkerHandler


def main() -> None:
    state = WorkerState()
    state.initialize()
    serve(handler(state), int(os.environ.get("PORT", "8083")))


if __name__ == "__main__":
    main()
