"""Inventory API backed by PostgreSQL with controllable realistic failure modes."""

from __future__ import annotations

import json
import os
import threading
import time
from http import HTTPStatus
from typing import Any
from urllib.parse import parse_qs, urlparse

import psycopg

from app.common import JsonLogger, QuietHandler, counter_line, gauge_line, serve


FAILURE_MODES = {"normal", "lock-contention", "bad-database-config"}


class InventoryState:
    def __init__(self) -> None:
        self.database_url = os.environ["DATABASE_URL"]
        self.failure_mode = os.environ.get("INITIAL_FAILURE_MODE", "normal")
        self.logger = JsonLogger("inventory-api")
        self.requests = {"success": 0, "error": 0}
        self.db_failures = {"lock_timeout": 0, "connection": 0, "query": 0}
        self.active_transactions = 0
        self.max_active_transactions = 0
        self._lock = threading.Lock()

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
                self.logger.write("INFO", "Inventory database schema ready", attempt=attempt)
                return
            except psycopg.OperationalError as exc:
                self.logger.write("WARN", "Waiting for inventory database", attempt=attempt, error=str(exc)[:180])
                time.sleep(1)
        raise RuntimeError("PostgreSQL did not become available")

    def set_failure_mode(self, mode: str) -> None:
        if mode not in FAILURE_MODES:
            raise ValueError(f"Unknown failure mode: {mode}")
        with self._lock:
            self.failure_mode = mode
        self.logger.write("WARN" if mode != "normal" else "INFO", "Inventory failure mode changed", mode=mode)

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
        with self._lock:
            mode = self.failure_mode
        if mode == "bad-database-config":
            return self._bad_database_config(order_id)

        started = time.perf_counter()
        self._transaction_started()
        try:
            with psycopg.connect(self.database_url, connect_timeout=2) as connection:
                with connection.cursor() as cursor:
                    if mode == "lock-contention":
                        cursor.execute("SET LOCAL lock_timeout = '75ms'")
                        cursor.execute("SELECT quantity FROM inventory_items WHERE sku = 'sku-red-widget' FOR UPDATE")
                        cursor.execute("SELECT pg_sleep(0.18)")
                    else:
                        cursor.execute("SELECT quantity FROM inventory_items WHERE sku = 'sku-red-widget'")
                    connection.commit()
            duration = time.perf_counter() - started
            self._record_request("success")
            self.logger.write(
                "INFO",
                "Inventory reservation committed",
                order_id=order_id,
                failure_mode=mode,
                duration_ms=round(duration * 1000, 2),
            )
            return HTTPStatus.OK, {"status": "reserved", "order_id": order_id}
        except psycopg.errors.LockNotAvailable as exc:
            self._record_request("error")
            self._record_failure("lock_timeout")
            self.logger.write(
                "ERROR",
                "Reservation database lock timeout while acquiring inventory row",
                order_id=order_id,
                failure_mode=mode,
                error=str(exc)[:180],
            )
            return HTTPStatus.SERVICE_UNAVAILABLE, {"status": "lock_timeout", "order_id": order_id}
        except psycopg.OperationalError as exc:
            self._record_request("error")
            self._record_failure("connection")
            self.logger.write("ERROR", "Inventory database connection failed", order_id=order_id, error=str(exc)[:180])
            return HTTPStatus.SERVICE_UNAVAILABLE, {"status": "database_unavailable", "order_id": order_id}
        except psycopg.Error as exc:
            self._record_request("error")
            self._record_failure("query")
            self.logger.write("ERROR", "Inventory database query failed", order_id=order_id, error=str(exc)[:180])
            return HTTPStatus.SERVICE_UNAVAILABLE, {"status": "database_query_failed", "order_id": order_id}
        finally:
            self._transaction_finished()

    def _bad_database_config(self, order_id: str) -> tuple[int, dict[str, Any]]:
        bad_url = "postgresql://inventory:wrong-password@postgres:5432/inventory"
        try:
            psycopg.connect(bad_url, connect_timeout=1).close()
        except psycopg.OperationalError as exc:
            self._record_request("error")
            self._record_failure("connection")
            self.logger.write(
                "ERROR",
                "Inventory database connection rejected after configuration change",
                order_id=order_id,
                failure_mode="bad-database-config",
                error=str(exc)[:180],
            )
            return HTTPStatus.SERVICE_UNAVAILABLE, {"status": "database_configuration_error", "order_id": order_id}
        self._record_request("error")
        self._record_failure("connection")
        return HTTPStatus.SERVICE_UNAVAILABLE, {"status": "unexpected_configuration_result", "order_id": order_id}

    def metrics(self) -> str:
        with self._lock:
            requests = dict(self.requests)
            failures = dict(self.db_failures)
            active = self.active_transactions
            peak = self.max_active_transactions
            mode = self.failure_mode
            logs = self.logger.count
        return "".join(
            [
                *(counter_line("inventory_reservation_requests_total", "Inventory reservation requests", value, outcome=outcome) for outcome, value in requests.items()),
                *(counter_line("inventory_database_failures_total", "Inventory database failures", value, kind=kind) for kind, value in failures.items()),
                gauge_line("inventory_active_transactions", "Inventory PostgreSQL transactions currently active", active),
                gauge_line("inventory_peak_active_transactions", "Largest concurrent inventory transactions observed", peak),
                gauge_line("inventory_failure_mode_info", "Current injected inventory failure mode", 1, mode=mode),
                counter_line("inventory_log_events_total", "Structured inventory log events emitted", logs),
            ]
        )


def handler(state: InventoryState) -> type[QuietHandler]:
    class InventoryHandler(QuietHandler):
        def do_GET(self) -> None:  # noqa: N802
            parsed = urlparse(self.path)
            if parsed.path == "/health":
                self.send_json(HTTPStatus.OK, {"status": "ok", "failure_mode": state.failure_mode})
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
                state.set_failure_mode(str(self.body_json().get("mode", "")))
            except (ValueError, OSError, json.JSONDecodeError) as exc:
                self.send_json(HTTPStatus.BAD_REQUEST, {"error": str(exc)})
                return
            self.send_json(HTTPStatus.OK, {"status": "updated", "failure_mode": state.failure_mode})

    return InventoryHandler


def main() -> None:
    state = InventoryState()
    state.initialize_database()
    serve(handler(state), int(os.environ.get("PORT", "8081")))


if __name__ == "__main__":
    main()
