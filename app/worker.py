"""Background worker with observable CPU, memory, and poison-job failures."""

from __future__ import annotations

import json
import base64
import hashlib
import io
import os
import threading
import time
import uuid
from http import HTTPStatus
from typing import Any
from urllib.parse import urlparse

import pymysql

from app.common import JsonLogger, QuietHandler, counter_line, gauge_line, serve
from app.safety import lease_seconds


MODES = {"normal", "cpu-saturation", "memory-leak"}
EXPORT_ROWS_PER_PAGE = 24_000
MAX_BUFFERED_EXPORT_PAGES = 160
MAX_BUFFERED_EXPORT_BYTES = 96 * 1024 * 1024
CPU_MIGRATION_BATCH_SIZE = 512
CPU_MIGRATION_ROUNDS = 1_200_000


class WorkerState:
    def __init__(self) -> None:
        self.logger = JsonLogger("worker")
        self.mode = "normal"
        self.jobs = 0
        self.cpu_iterations = 0
        self.migration_backlog = 0
        self.migration_records_completed = 0
        self.export_pages_completed = 0
        self.export_bytes_streamed = 0
        self.allocated_bytes = 0
        self._memory: list[bytearray] = []
        self._mode_stop = threading.Event()
        self._lock = threading.Lock()
        self._mode_lock = threading.Lock()
        self._mode_thread: threading.Thread | None = None
        self._migration_batch_id: str | None = None
        self._export_page_number = 0
        self._control_run_id: str | None = None
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
                            "(id BIGINT AUTO_INCREMENT PRIMARY KEY, kind VARCHAR(32) NOT NULL, payload TEXT, "
                            "owner_run_id VARCHAR(64) NULL, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
                        )
                        cursor.execute("SHOW COLUMNS FROM lab_jobs LIKE 'owner_run_id'")
                        if cursor.fetchone() is None:
                            cursor.execute("ALTER TABLE lab_jobs ADD COLUMN owner_run_id VARCHAR(64) NULL")
                        cursor.execute(
                            "CREATE TABLE IF NOT EXISTS lab_import_records "
                            "(job_id BIGINT NOT NULL, record_number INT NOT NULL, sku VARCHAR(64) NOT NULL, "
                            "quantity INT NOT NULL, description VARCHAR(256) NOT NULL DEFAULT '', "
                            "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, PRIMARY KEY (job_id, record_number), "
                            "KEY idx_lab_import_records_created_at (created_at))"
                        )
                    connection.commit()
                break
            except pymysql.MySQLError as exc:
                self.logger.write("WARN", "Worker waiting for MySQL", attempt=attempt, error=str(exc)[:180])
                time.sleep(1)
        else:
            raise RuntimeError("MySQL did not become available for worker schema initialization")
        threading.Thread(target=self._heartbeat_loop, daemon=True).start()
        self._start_mode_work(self._mode_stop, "normal")

    def _connect(self) -> pymysql.Connection:
        return pymysql.connect(**self.db, connect_timeout=2, read_timeout=3, write_timeout=3, autocommit=False)

    def set_mode(self, mode: str, duration: int = 180, run_id: str | None = None) -> None:
        if mode not in MODES:
            raise ValueError(f"Unknown worker mode: {mode}")
        run_id = _validate_run_id(run_id)
        duration = lease_seconds(duration)
        with self._mode_lock:
            previous_stop = self._mode_stop
            previous_thread = self._mode_thread
            previous_stop.set()
            if previous_thread and previous_thread is not threading.current_thread():
                previous_thread.join(timeout=2)
                if previous_thread.is_alive():
                    raise ValueError("Previous bounded worker task has not stopped yet")
            stop = threading.Event()
            batch_id = uuid.uuid4().hex[:16] if mode == "cpu-saturation" else None
            with self._lock:
                previous_backlog = self.migration_backlog
                self.mode = mode
                self.allocated_bytes = 0
                self._memory = []
                self.migration_backlog = CPU_MIGRATION_BATCH_SIZE if mode == "cpu-saturation" else 0
                self._migration_batch_id = batch_id
                self._expires_at = time.monotonic() + duration if mode != "normal" else 0
                self._mode_stop = stop
                self._control_run_id = run_id
            if previous_backlog:
                self.logger.write("WARN", "Credential migration batch cancelled during mode change",
                                  remaining_records=previous_backlog)
            self.logger.write("INFO", "Worker scheduler configuration applied",
                              configuration_revision=_worker_config_revision(mode), run_id=run_id)
            self._start_mode_work(stop, mode)

    def _start_mode_work(self, stop: threading.Event, mode: str) -> None:
        target = {
            "normal": self._export_loop,
            "cpu-saturation": self._credential_migration_loop,
            "memory-leak": self._leak_memory,
        }[mode]
        thread = threading.Thread(target=target, args=(stop,), daemon=True, name=f"worker-{mode}")
        self._mode_thread = thread
        thread.start()

    def _burn_cpu(self, stop: threading.Event) -> None:
        self._credential_migration_loop(stop)

    def _credential_migration_loop(self, stop: threading.Event) -> None:
        while not stop.is_set():
            with self._lock:
                mode = self.mode
                backlog = self.migration_backlog
                batch_id = self._migration_batch_id
            if mode == "cpu-saturation" and backlog > 0:
                rounds = CPU_MIGRATION_ROUNDS
                record_number = CPU_MIGRATION_BATCH_SIZE - backlog + 1
                time.sleep(0)
            else:
                rounds = 1_000
                record_number = 1
                batch_id = "routine"
            if stop.is_set():
                break
            _migrate_credential_record(batch_id, record_number, rounds)
            with self._lock:
                self.cpu_iterations += 1
                self.migration_records_completed += 1
                remaining = self.migration_backlog
                if mode == "cpu-saturation" and remaining > 0:
                    self.migration_backlog = remaining - 1
                    remaining = self.migration_backlog
            if mode == "cpu-saturation" and (record_number % 8 == 0 or remaining == 0):
                self.logger.write("INFO", "Credential migration batch progress",
                                  batch_id=batch_id, record_number=record_number,
                                  records_completed=record_number, records_remaining=remaining,
                                  kdf="pbkdf2_sha256", rounds=rounds)
            if mode != "cpu-saturation":
                stop.wait(30)

    def _leak_memory(self, stop: threading.Event) -> None:
        while not stop.is_set():
            page_number, page = self._encode_export_page()
            buffer = bytearray(page)
            with self._lock:
                if stop.is_set():
                    break
                if (len(self._memory) >= MAX_BUFFERED_EXPORT_PAGES
                        or self.allocated_bytes + len(buffer) > MAX_BUFFERED_EXPORT_BYTES):
                    self.logger.write("WARN", "Export buffer reached its configured safety bound",
                                      buffered_pages=len(self._memory), buffered_bytes=self.allocated_bytes,
                                      maximum_buffered_bytes=MAX_BUFFERED_EXPORT_BYTES)
                    return
                self._memory.append(buffer)
                self.allocated_bytes += len(buffer)
                self.export_pages_completed += 1
                allocated = self.allocated_bytes
            self.logger.write("INFO", "Export page retained past its delivery boundary",
                              export_page=page_number, buffered_bytes=allocated,
                              page_bytes=len(buffer), delivery="buffered", rows=EXPORT_ROWS_PER_PAGE)
            stop.wait(0.8)

    def _export_loop(self, stop: threading.Event) -> None:
        while not stop.is_set():
            page_number, page = self._encode_export_page()
            _migrate_credential_record("routine", page_number, 1_000)
            with self._lock:
                self.export_pages_completed += 1
                self.export_bytes_streamed += len(page)
                self.cpu_iterations += 1
                self.migration_records_completed += 1
            self.logger.write("INFO", "Inventory export page streamed and released",
                              export_page=page_number, page_bytes=len(page),
                              rows=EXPORT_ROWS_PER_PAGE, delivery="streaming")
            stop.wait(30)

    def _encode_export_page(self) -> tuple[int, bytes]:
        with self._lock:
            self._export_page_number += 1
            page_number = self._export_page_number
        page = io.BytesIO()
        for row_number in range(EXPORT_ROWS_PER_PAGE):
            row = {
                "export_id": f"inventory-export-{page_number:06d}",
                "row": row_number,
                "sku": "sku-red-widget" if row_number % 2 == 0 else "sku-blue-widget",
                "quantity": 1000 + row_number % 250,
            }
            page.write(json.dumps(row, separators=(",", ":")).encode("utf-8"))
            page.write(b"\n")
        return page_number, page.getvalue()

    def _job_loop(self) -> None:
        while True:
            poison_retried = False
            try:
                with self._connect() as connection:
                    with connection.cursor() as cursor:
                        cursor.execute(
                            "SELECT id, kind, payload, owner_run_id FROM lab_jobs "
                            "WHERE kind='import' ORDER BY id LIMIT 1 FOR UPDATE"
                        )
                        job = cursor.fetchone()
                        if job:
                            job_id, _kind, raw_payload, owner_run_id = job
                            self.logger.write("INFO", "Import delivery received", job_id=job_id,
                                              owner_run_id=owner_run_id, acknowledgement="pending")
                            try:
                                document = decode_job(raw_payload)
                                records = _validated_import_records(document)
                                cursor.executemany(
                                    "INSERT INTO lab_import_records "
                                    "(job_id, record_number, sku, quantity, description) VALUES (%s, %s, %s, %s, %s)",
                                    ((job_id, idx, sku, quantity, description)
                                     for idx, (sku, quantity, description) in enumerate(records, start=1)),
                                )
                                cursor.execute("DELETE FROM lab_jobs WHERE id=%s", (job_id,))
                                connection.commit()
                            except (ValueError, KeyError, TypeError) as exc:
                                connection.rollback()
                                self.logger.write("ERROR", "Import decoder rejected document", job_id=job_id,
                                                  owner_run_id=owner_run_id, error_type=type(exc).__name__,
                                                  error=str(exc), acknowledgement="pending")
                                poison_retried = True
                            if not poison_retried:
                                with self._lock:
                                    self.jobs += 1
                                self.logger.write("INFO", "Import batch committed and acknowledged", job_id=job_id,
                                                  owner_run_id=owner_run_id, records=len(records), acknowledgement="committed")
            except pymysql.MySQLError as exc:
                self.logger.write("WARN", "Worker queue poll failed", error=str(exc)[:180])
            time.sleep(5 if poison_retried else 2)

    def _heartbeat_loop(self) -> None:
        while True:
            with self._lock:
                jobs = self.jobs
                expired = self._expires_at and time.monotonic() >= self._expires_at
                run_id = self._control_run_id
            if expired:
                self.set_mode("normal", run_id=run_id)
            self.logger.write("INFO", "Worker scheduler heartbeat", completed_jobs=jobs)
            time.sleep(1)

    def status(self) -> dict[str, Any]:
        with self._lock:
            return {
                "status": "ok",
                "mode": self.mode,
                "allocated_bytes": self.allocated_bytes,
                "cpu_iterations": self.cpu_iterations,
                "migration_backlog": self.migration_backlog,
                "migration_records_completed": self.migration_records_completed,
                "export_pages_completed": self.export_pages_completed,
                "export_bytes_streamed": self.export_bytes_streamed,
                "completed_jobs": self.jobs,
            }

    def metrics(self) -> str:
        state = self.status()
        return "".join(
            [
                gauge_line("lab_worker_allocated_bytes", "Bytes retained by worker batch cache", state["allocated_bytes"]),
                counter_line("lab_worker_cpu_iterations_total", "CPU-bound worker iterations", state["cpu_iterations"]),
                gauge_line("lab_worker_migration_backlog", "Credential records left in the active bounded migration batch", state["migration_backlog"]),
                counter_line("lab_worker_migration_records_total", "Credential records processed by normal and injected migration work", state["migration_records_completed"]),
                counter_line("lab_worker_export_pages_total", "Finite inventory export pages processed", state["export_pages_completed"]),
                counter_line("lab_worker_export_bytes_streamed_total", "Export bytes released after normal streaming", state["export_bytes_streamed"]),
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
                state.set_mode(str(payload.get("mode", "")), payload.get("duration_seconds", 180),
                               payload.get("run_id"))
            except (ValueError, OSError, json.JSONDecodeError) as exc:
                self.send_json(HTTPStatus.BAD_REQUEST, {"error": str(exc)})
                return
            self.send_json(HTTPStatus.OK, {**state.status(), "run_id": payload.get("run_id")})

    return WorkerHandler


def main() -> None:
    state = WorkerState()
    state.initialize()
    threading.Thread(target=serve, args=(handler(state), int(os.environ.get("PORT", "8083"))), daemon=True).start()
    state._job_loop()


def decode_job(payload: str) -> dict:
    envelope = json.loads(payload)
    if not isinstance(envelope, dict):
        raise ValueError("Import envelope must be an object")
    schema = envelope.get("schema")
    if schema is not None and schema != "inventory.import.v2":
        raise ValueError("Unsupported import envelope schema")
    document = json.loads(base64.b64decode(envelope["body"], validate=True))
    if not isinstance(document, dict):
        raise ValueError("Import body must be an object")
    return document


def _validated_import_records(document: dict[str, Any]) -> list[tuple[str, int, str]]:
    rows = document.get("records", [document])
    if not isinstance(rows, list) or not 1 <= len(rows) <= 5_000:
        raise ValueError("Import batch must contain between 1 and 5000 records")
    validated = []
    for row in rows:
        if not isinstance(row, dict):
            raise ValueError("Each imported inventory record must be an object")
        sku = row.get("sku")
        quantity = row.get("quantity")
        description = row.get("description", "")
        if not isinstance(sku, str) or not sku.strip() or len(sku) > 64:
            raise ValueError("Imported sku must be a non-empty string of at most 64 characters")
        if isinstance(quantity, bool) or not isinstance(quantity, int) or not 0 <= quantity <= 1_000_000_000:
            raise ValueError("Imported quantity must be an integer between 0 and 1000000000")
        if not isinstance(description, str):
            raise ValueError("Imported description must be text")
        validated.append((sku, quantity, description[:256]))
    return validated


def _migrate_credential_record(batch_id: str, record_number: int, rounds: int) -> None:
    account_material = f"test-account-{batch_id}-{record_number}".encode()
    hashlib.pbkdf2_hmac("sha256", account_material, b"credential-migration-salt", rounds)


def _worker_config_revision(mode: str) -> str:
    return hashlib.sha256(f"worker-scheduler:{mode}".encode()).hexdigest()[:12]


def _validate_run_id(value: Any) -> str | None:
    if value is None:
        return None
    if (not isinstance(value, str) or len(value) != 32
            or any(character not in "0123456789abcdef" for character in value)):
        raise ValueError("run_id must be a 32-character lowercase hexadecimal ID")
    return value


if __name__ == "__main__":
    main()
