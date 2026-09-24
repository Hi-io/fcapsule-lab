"""Inventory API backed by PostgreSQL with bounded, observable failure modes."""

from __future__ import annotations

import hashlib
import json
import os
import threading
import time
from http import HTTPStatus
from typing import Any
from urllib.parse import parse_qs, urlparse

import psycopg
from psycopg.conninfo import conninfo_to_dict, make_conninfo

from app.common import JsonLogger, QuietHandler, counter_line, gauge_line, serve
from app.safety import lease_seconds


FAILURE_MODES = {"normal", "lock-contention", "bad-database-config"}


class InventoryState:
    def __init__(self) -> None:
        self.database_url = os.environ["DATABASE_URL"]
        self.effective_database_url = self.database_url
        self.failure_mode = "normal"
        self.logger = JsonLogger("inventory-api")
        self.requests = {"success": 0, "error": 0}
        self.db_failures = {"lock_timeout": 0, "connection": 0, "authentication": 0, "query": 0}
        self.active_transactions = 0
        self.max_active_transactions = 0
        self._lock = threading.Lock()
        self._expires_at = 0.0
        self._control_run_id: str | None = None
        self._config_revision = _config_revision(self.database_url)
        threading.Thread(target=self._lease_loop, daemon=True, name="inventory-lease").start()

    def initialize_database(self) -> None:
        for attempt in range(1, 31):
            try:
                with psycopg.connect(self.database_url, connect_timeout=2) as connection:
                    with connection.cursor() as cursor:
                        cursor.execute(
                            "CREATE TABLE IF NOT EXISTS inventory_items (sku TEXT PRIMARY KEY, quantity INTEGER NOT NULL)"
                        )
                        cursor.execute(
                            "INSERT INTO inventory_items (sku, quantity) VALUES ('sku-red-widget', 1000000) "
                            "ON CONFLICT (sku) DO NOTHING"
                        )
                        cursor.execute(
                            "CREATE TABLE IF NOT EXISTS reservation_events "
                            "(idempotency_key TEXT PRIMARY KEY, order_id TEXT NOT NULL, "
                            "created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP)"
                        )
                        cursor.execute(
                            "CREATE INDEX IF NOT EXISTS idx_reservation_events_created_at "
                            "ON reservation_events (created_at)"
                        )
                self.logger.write("INFO", "Inventory database schema ready", attempt=attempt)
                return
            except psycopg.OperationalError as exc:
                self.logger.write("WARN", "Waiting for inventory database", attempt=attempt,
                                  error_type=type(exc).__name__)
                time.sleep(1)
        raise RuntimeError("PostgreSQL did not become available")

    def set_failure_mode(self, mode: str, duration: int = 180, run_id: str | None = None) -> None:
        if mode not in FAILURE_MODES:
            raise ValueError(f"Unknown failure mode: {mode}")
        run_id = _validate_run_id(run_id)
        duration = lease_seconds(duration)
        effective_url = self.database_url
        if mode == "bad-database-config":
            settings = conninfo_to_dict(self.database_url)
            settings["password"] = f"{settings.get('password', '')}-invalid"
            effective_url = make_conninfo(**settings)
        revision = _config_revision(effective_url)
        with self._lock:
            self.failure_mode = mode
            self.effective_database_url = effective_url
            self._config_revision = revision
            self._expires_at = time.monotonic() + duration if mode != "normal" else 0.0
            self._control_run_id = run_id
        self.logger.write("INFO", "Inventory runtime configuration applied",
                          configuration_revision=revision, run_id=run_id)

    def _lease_loop(self) -> None:
        while True:
            time.sleep(1)
            with self._lock:
                expired = self._expires_at and time.monotonic() >= self._expires_at
                run_id = self._control_run_id
            if expired:
                self.set_failure_mode("normal", run_id=run_id)

    def _record_request(self, outcome: str) -> None:
        with self._lock:
            self.requests[outcome] += 1

    def _record_failure(self, kind: str) -> None:
        with self._lock:
            self.db_failures[kind] += 1

    def _transaction_started(self) -> None:
        with self._lock:
            self.active_transactions += 1
            self.max_active_transactions = max(self.max_active_transactions, self.active_transactions)

    def _transaction_finished(self) -> None:
        with self._lock:
            self.active_transactions = max(0, self.active_transactions - 1)

    def reserve(self, order_id: str) -> tuple[int, dict[str, Any]]:
        if not order_id or len(order_id) > 128:
            return HTTPStatus.BAD_REQUEST, {"status": "invalid_order_id"}
        with self._lock:
            mode = self.failure_mode
            database_url = self.effective_database_url
        if mode == "bad-database-config":
            return self._bad_database_config(database_url, order_id)

        started = time.perf_counter()
        order_ref = _safe_ref(order_id)
        idempotency_key = "order-" + order_ref
        self._transaction_started()
        try:
            with psycopg.connect(database_url, connect_timeout=2) as connection:
                with connection.cursor() as cursor:
                    if mode == "lock-contention":
                        cursor.execute("SET LOCAL lock_timeout = '75ms'")
                    cursor.execute(
                        "SELECT quantity FROM inventory_items WHERE sku = %s FOR UPDATE",
                        ("sku-red-widget",),
                    )
                    stock_row = cursor.fetchone()
                    if not stock_row:
                        self._record_request("error")
                        self.logger.write("ERROR", "Inventory SKU is missing", order_ref=order_ref,
                                          sku="sku-red-widget", operation="reserve_stock")
                        return HTTPStatus.SERVICE_UNAVAILABLE, {"status": "inventory_missing"}
                    if mode == "lock-contention":
                        cursor.execute("SELECT pg_sleep(0.18)")
                    cursor.execute(
                        "INSERT INTO reservation_events (idempotency_key, order_id) VALUES (%s, %s) "
                        "ON CONFLICT (idempotency_key) DO NOTHING",
                        (idempotency_key, order_id),
                    )
                    inserted = cursor.rowcount == 1
                    if not inserted:
                        cursor.execute(
                            "SELECT order_id FROM reservation_events WHERE idempotency_key = %s",
                            (idempotency_key,),
                        )
                        existing = cursor.fetchone()
                        if existing and existing[0] == order_id:
                            connection.commit()
                            self._record_request("success")
                            self.logger.write("INFO", "Prior inventory reservation returned without a second decrement",
                                              order_ref=order_ref, outcome="idempotent_replay")
                            return HTTPStatus.OK, {"status": "reserved", "order_id": order_id, "replayed": True}
                        connection.rollback()
                        self._record_request("error")
                        self.logger.write("ERROR", "Reservation idempotency record has a different owner",
                                          order_ref=order_ref, outcome="idempotency_conflict")
                        return HTTPStatus.CONFLICT, {"status": "reservation_conflict"}
                    cursor.execute(
                        "UPDATE inventory_items SET quantity = quantity - 1 "
                        "WHERE sku = %s AND quantity > 0 RETURNING quantity",
                        ("sku-red-widget",),
                    )
                    remaining = cursor.fetchone()
                    if not remaining:
                        connection.rollback()
                        self._record_request("error")
                        self.logger.write("WARN", "Reservation rejected because no stock remained",
                                          order_ref=order_ref, sku="sku-red-widget", outcome="out_of_stock")
                        return HTTPStatus.CONFLICT, {"status": "out_of_stock"}
                connection.commit()
            duration_ms = round((time.perf_counter() - started) * 1000, 2)
            self._record_request("success")
            self.logger.write("INFO", "Inventory reservation committed", order_ref=order_ref,
                              sku="sku-red-widget", remaining_quantity=int(remaining[0]),
                              duration_ms=duration_ms, operation="reserve_stock")
            return HTTPStatus.OK, {"status": "reserved", "order_id": order_id}
        except psycopg.errors.LockNotAvailable as exc:
            self._record_request("error")
            self._record_failure("lock_timeout")
            self.logger.write("ERROR", "Reservation database lock timeout while acquiring inventory row",
                              order_ref=order_ref, sqlstate=exc.sqlstate, operation="reserve_stock")
            return HTTPStatus.SERVICE_UNAVAILABLE, {"status": "lock_timeout"}
        except psycopg.OperationalError as exc:
            self._record_request("error")
            kind = "authentication" if (exc.sqlstate or "").startswith("28") else "connection"
            self._record_failure(kind)
            self.logger.write("ERROR", "Inventory database connection failed", order_ref=order_ref,
                              failure_kind=kind, sqlstate=exc.sqlstate, error_type=type(exc).__name__)
            return HTTPStatus.SERVICE_UNAVAILABLE, {"status": "database_unavailable"}
        except psycopg.Error as exc:
            self._record_request("error")
            self._record_failure("query")
            self.logger.write("ERROR", "Inventory database query failed", order_ref=order_ref,
                              sqlstate=exc.sqlstate, error_type=type(exc).__name__)
            return HTTPStatus.SERVICE_UNAVAILABLE, {"status": "database_query_failed"}
        finally:
            self._transaction_finished()

    def _bad_database_config(self, bad_url: str, order_id: str) -> tuple[int, dict[str, Any]]:
        order_ref = _safe_ref(order_id)
        try:
            with psycopg.connect(bad_url, connect_timeout=1):
                pass
        except psycopg.OperationalError as exc:
            if (exc.sqlstate or "").startswith("28"):
                self._record_request("error")
                self._record_failure("authentication")
                self.logger.write("ERROR", "Inventory database rejected credentials after configuration change",
                                  order_ref=order_ref, sqlstate=exc.sqlstate,
                                  failure_kind="authentication", consumer_decision="reject_request")
                return HTTPStatus.SERVICE_UNAVAILABLE, {"status": "database_configuration_error"}
            self._record_request("error")
            self._record_failure("connection")
            self.logger.write("ERROR", "Inventory database connection failed after configuration change",
                              order_ref=order_ref, sqlstate=exc.sqlstate,
                              failure_kind="connection", error_type=type(exc).__name__)
            return HTTPStatus.SERVICE_UNAVAILABLE, {"status": "database_unavailable"}
        self._record_request("error")
        self.logger.write("ERROR", "Credential change did not produce a database authentication failure",
                          order_ref=order_ref, outcome="failure_not_reproduced")
        return HTTPStatus.INTERNAL_SERVER_ERROR, {"status": "failure_not_reproduced"}

    def metrics(self) -> str:
        with self._lock:
            requests = dict(self.requests)
            failures = dict(self.db_failures)
            active = self.active_transactions
            peak = self.max_active_transactions
            logs = self.logger.count
        return "".join(
            [
                *(counter_line("inventory_reservation_requests_total", "Inventory reservation requests", value, outcome=outcome) for outcome, value in requests.items()),
                *(counter_line("inventory_database_failures_total", "Inventory database failures", value, kind=kind) for kind, value in failures.items()),
                gauge_line("inventory_active_transactions", "Inventory PostgreSQL transactions currently active", active),
                gauge_line("inventory_peak_active_transactions", "Largest concurrent inventory transactions observed", peak),
                counter_line("inventory_log_events_total", "Structured inventory log events emitted", logs),
            ]
        )

    def status(self) -> dict[str, Any]:
        with self._lock:
            return {"status": "ok", "failure_mode": self.failure_mode,
                    "configuration_revision": self._config_revision}


def _safe_ref(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()[:12]


def _config_revision(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()[:12]


def _validate_run_id(value: Any) -> str | None:
    if value is None:
        return None
    if (not isinstance(value, str) or len(value) != 32
            or any(character not in "0123456789abcdef" for character in value)):
        raise ValueError("run_id must be a 32-character lowercase hexadecimal ID")
    return value


def handler(state: InventoryState) -> type[QuietHandler]:
    class InventoryHandler(QuietHandler):
        def do_GET(self) -> None:  # noqa: N802
            parsed = urlparse(self.path)
            if parsed.path == "/health":
                self.send_json(HTTPStatus.OK, state.status())
                return
            if parsed.path == "/metrics":
                self.send_text(HTTPStatus.OK, state.metrics())
                return
            if parsed.path == "/trace-probe":
                self.send_json(
                    HTTPStatus.OK,
                    {"available": False, "reason": "This five-container lab has no trace backend; FCAPSule should record this as unavailable."},
                )
                return
            if parsed.path == "/reserve":
                order_id = parse_qs(parsed.query).get("order_id", ["unknown"])[0]
                status, payload = state.reserve(order_id)
                self.send_json(status, payload)
                return
            self.send_json(HTTPStatus.NOT_FOUND, {"error": "not_found"})

        def do_POST(self) -> None:  # noqa: N802
            if urlparse(self.path).path != "/control/failure":
                self.send_json(HTTPStatus.NOT_FOUND, {"error": "not_found"})
                return
            try:
                payload = self.body_json()
                state.set_failure_mode(str(payload.get("mode", "")), payload.get("duration_seconds", 180),
                                       payload.get("run_id"))
            except (ValueError, OSError, json.JSONDecodeError) as exc:
                self.send_json(HTTPStatus.BAD_REQUEST, {"error": str(exc)})
                return
            self.send_json(HTTPStatus.OK, {"status": "updated", **state.status()})

    return InventoryHandler


def main() -> None:
    state = InventoryState()
    state.initialize_database()
    serve(handler(state), int(os.environ.get("PORT", "8081")))


if __name__ == "__main__":
    main()
