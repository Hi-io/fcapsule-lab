"""Inventory API backed by MySQL with controllable resource failures."""

from __future__ import annotations

import json
import os
import threading
import time
from http import HTTPStatus
from typing import Any
from urllib.parse import parse_qs, urlparse

import pymysql

from app.common import JsonLogger, QuietHandler, counter_line, gauge_line, serve
from app.safety import lease_seconds


FAILURE_MODES = {
    "normal", "configured", "lock-contention", "connection-saturation",
    "response-contract", "token-collision", "deadlock", "downstream-latency",
    "fixed-latency",
}


class InventoryState:
    def __init__(self) -> None:
        self.host = os.environ.get("MYSQL_HOST", "mysql")
        self.user = os.environ.get("MYSQL_USER", "inventory")
        self.password = os.environ.get("MYSQL_PASSWORD", "inventory-lab")
        self.database = os.environ.get("MYSQL_DATABASE", "inventory")
        self.configured_max_connections = int(os.environ.get("MYSQL_MAX_CONNECTIONS", "40"))
        self.failure_mode = "normal"
        self.logger = JsonLogger("inventory-api")
        self.requests = {"success": 0, "error": 0}
        self.db_failures = {"lock_timeout": 0, "connection": 0, "query": 0}
        self.transaction_failures = {"constraint": 0, "deadlock": 0}
        self.active_transactions = 0
        self.threads_connected = 0
        self.server_max_connections = self.configured_max_connections
        self._held_connections: list[pymysql.Connection] = []
        self._storm_stop = threading.Event()
        self._lock = threading.Lock()
        self._operation_lock = threading.Lock()
        self._task_stop = threading.Event()
        self._expires_at = 0.0
        self.accepted_key_id = os.environ.get("INVENTORY_ACCEPTED_KEY_ID", "checkout-key-v1")
        self.response_schema = os.environ.get("INVENTORY_RESPONSE_SCHEMA", "v1")
        self.query_revision = os.environ.get("INVENTORY_QUERY_REVISION", "v1")
        self.fixed_latency = 0.0

    def connect(self, timeout: float = 2) -> pymysql.Connection:
        return pymysql.connect(
            host=self.host,
            user=self.user,
            password=self.password,
            database=self.database,
            connect_timeout=max(1, int(timeout)),
            read_timeout=3,
            write_timeout=3,
            autocommit=False,
        )

    def initialize_database(self) -> None:
        for attempt in range(1, 61):
            try:
                with self.connect() as connection:
                    with connection.cursor() as cursor:
                        cursor.execute(
                            "CREATE TABLE IF NOT EXISTS inventory_items "
                            "(sku VARCHAR(64) PRIMARY KEY, quantity INT NOT NULL) ENGINE=InnoDB"
                        )
                        cursor.execute(
                            "CREATE TABLE IF NOT EXISTS reservation_events "
                            "(token VARCHAR(64) PRIMARY KEY, order_id VARCHAR(64) NOT NULL, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP) ENGINE=InnoDB"
                        )
                        cursor.execute(
                            "INSERT INTO inventory_items (sku, quantity) VALUES ('sku-red-widget', 1000000) "
                            "ON DUPLICATE KEY UPDATE quantity=quantity"
                        )
                        cursor.execute(
                            "INSERT INTO inventory_items (sku, quantity) VALUES ('sku-blue-widget', 1000000) "
                            "ON DUPLICATE KEY UPDATE quantity=quantity"
                        )
                    connection.commit()
                self.logger.write("INFO", "Inventory MySQL schema ready", attempt=attempt)
                threading.Thread(target=self._sample_database, daemon=True).start()
                threading.Thread(target=self._lease_loop, daemon=True).start()
                return
            except pymysql.MySQLError as exc:
                self.logger.write("WARN", "Waiting for inventory MySQL", attempt=attempt, error=str(exc)[:180])
                time.sleep(1)
        raise RuntimeError("MySQL did not become available")

    def set_failure_mode(self, mode: str, duration: int = 180, settings: dict[str, Any] | None = None) -> None:
        if mode not in FAILURE_MODES:
            raise ValueError(f"Unknown failure mode: {mode}")
        duration = lease_seconds(duration)
        with self._operation_lock:
            self._task_stop.set()
            self._release_connections()
            self._task_stop = threading.Event()
            self._storm_stop = self._task_stop
            with self._lock:
                self.failure_mode = mode
                self._expires_at = time.monotonic() + duration if mode != "normal" else 0
                values = settings or {}
                self.accepted_key_id = str(values.get("INVENTORY_ACCEPTED_KEY_ID", "checkout-key-v1"))
                self.response_schema = str(values.get("INVENTORY_RESPONSE_SCHEMA", "v1"))
                self.query_revision = str(values.get("INVENTORY_QUERY_REVISION", "v1"))
                self.fixed_latency = 0.25 if mode == "fixed-latency" else 0.35 if mode == "downstream-latency" else 0.0
            if mode == "connection-saturation":
                threading.Thread(target=self._connection_storm, args=(self._task_stop,), daemon=True).start()
            elif mode == "lock-contention":
                threading.Thread(target=self._reconcile_stock, args=(self._task_stop,), daemon=True).start()
            elif mode == "deadlock":
                threading.Thread(target=self._deadlock_loop, args=(self._task_stop,), daemon=True).start()
        self.logger.write("INFO", "Inventory runtime configuration reloaded", configuration_revision=self.query_revision)

    def _lease_loop(self) -> None:
        while True:
            time.sleep(1)
            if self._expires_at and time.monotonic() >= self._expires_at:
                self.set_failure_mode("normal")

    def _reconcile_stock(self, stop: threading.Event) -> None:
        while not stop.is_set():
            try:
                with self.connect() as connection:
                    with connection.cursor() as cursor:
                        cursor.execute("SET SESSION innodb_lock_wait_timeout = 1")
                        cursor.execute("UPDATE inventory_items SET quantity=quantity WHERE sku='sku-red-widget'")
                        self.logger.write("INFO", "Stock reconciliation transaction opened", db_session=connection.thread_id(), sku="sku-red-widget", operation="reconcile_stock")
                        stop.wait(15)
                    connection.rollback()
                    self.logger.write("INFO", "Stock reconciliation transaction closed", db_session=connection.thread_id(), disposition="rollback")
            except pymysql.MySQLError as exc:
                self.logger.write("WARN", "Stock reconciliation interrupted", error=str(exc)[:180])
            stop.wait(0.1)

    def _connection_storm(self, stop: threading.Event) -> None:
        target = max(4, self.configured_max_connections - 1)
        while not stop.is_set() and len(self._held_connections) < target:
            try:
                connection = self.connect()
                with connection.cursor() as cursor:
                    cursor.execute("SELECT 1")
                with self._lock:
                    if stop.is_set():
                        connection.close()
                        break
                    self._held_connections.append(connection)
                self.logger.write(
                    "INFO", "Inventory database operation completed",
                    db_session=connection.thread_id(), operation="availability_lookup",
                    pool_checked_out=len(self._held_connections),
                )
            except pymysql.MySQLError as exc:
                self._record_failure("connection")
                self.logger.write(
                    "ERROR",
                    "Database session acquisition failed",
                    mysql_error_code=exc.args[0] if exc.args else None,
                    error=str(exc)[:180],
                )
            stop.wait(0.5)

    def _release_connections(self) -> None:
        self._storm_stop.set()
        with self._lock:
            connections = self._held_connections
            self._held_connections = []
        for connection in connections:
            try:
                connection.close()
            except pymysql.MySQLError:
                pass

    def _deadlock_loop(self, stop: threading.Event) -> None:
        while not stop.is_set():
            barrier = threading.Barrier(2)
            threads = [
                threading.Thread(target=self._deadlock_transaction, args=(first, second, barrier), daemon=True)
                for first, second in (("sku-red-widget", "sku-blue-widget"), ("sku-blue-widget", "sku-red-widget"))
            ]
            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join(timeout=5)
            stop.wait(0.4)

    def _deadlock_transaction(self, first: str, second: str, barrier: threading.Barrier) -> None:
        try:
            with self.connect() as connection:
                with connection.cursor() as cursor:
                    cursor.execute("UPDATE inventory_items SET quantity=quantity WHERE sku=%s", (first,))
                    barrier.wait(timeout=2)
                    cursor.execute("UPDATE inventory_items SET quantity=quantity WHERE sku=%s", (second,))
                connection.commit()
        except (pymysql.MySQLError, threading.BrokenBarrierError) as exc:
            code = int(exc.args[0]) if isinstance(exc, pymysql.MySQLError) and exc.args else 0
            if code == 1213:
                with self._lock:
                    self.transaction_failures["deadlock"] += 1
                self.logger.write("ERROR", "Inventory transaction rolled back by database", mysql_error_code=code,
                                  first_sku=first, second_sku=second, operation="stock_reconciliation")
            else:
                self.logger.write("WARN", "Inventory reconciliation pair interrupted", error_type=type(exc).__name__)

    def reserve(self, order_id: str, key_id: str = "") -> tuple[int, dict[str, Any]]:
        with self._lock:
            mode = self.failure_mode
            accepted_key_id = self.accepted_key_id
            response_schema = self.response_schema
            query_revision = self.query_revision
            latency = self.fixed_latency
            self.active_transactions += 1
        started = time.perf_counter()
        self.logger.write("INFO", "Reservation transaction requested", order_id=order_id, dependency="mysql", operation="reserve_stock")
        try:
            if key_id != accepted_key_id:
                self.logger.write("WARN", "Reservation request signature rejected", order_id=order_id,
                                  presented_key_id=key_id or "missing", accepted_key_id=accepted_key_id)
                self._record_request("error")
                return HTTPStatus.UNAUTHORIZED, {"status": "unauthorized", "order_id": order_id}
            if latency:
                time.sleep(latency)
            with self.connect() as connection:
                with connection.cursor() as cursor:
                    cursor.execute("SET SESSION innodb_lock_wait_timeout = 1")
                    if query_revision == "v2":
                        cursor.execute("SELECT quantity - reserved_quantity FROM inventory_items WHERE sku=%s", ("sku-red-widget",))
                    else:
                        cursor.execute("UPDATE inventory_items SET quantity=quantity-1 WHERE sku=%s AND quantity>0", ("sku-red-widget",))
                    if mode == "token-collision":
                        cursor.execute("INSERT INTO reservation_events (token, order_id) VALUES (%s, %s)",
                                       ("reservation-window-active", order_id))
                connection.commit()
            duration = time.perf_counter() - started
            self._record_request("success")
            self.logger.write(
                "INFO",
                "Inventory reservation committed",
                order_id=order_id,
                duration_ms=round(duration * 1000, 2),
            )
            if mode == "response-contract":
                return HTTPStatus.OK, {"result": "accepted", "reference": order_id}
            if response_schema == "v2":
                return HTTPStatus.OK, {"reservation": {"status": "reserved", "order_id": order_id}, "schema": "v2"}
            return HTTPStatus.OK, {"status": "reserved", "order_id": order_id, "schema": "v1"}
        except pymysql.err.IntegrityError as exc:
            code = int(exc.args[0]) if exc.args else 0
            self._record_request("error")
            with self._lock:
                self.transaction_failures["constraint"] += 1
            self.logger.write("ERROR", "Inventory reservation transaction rejected", order_id=order_id,
                              mysql_error_code=code, operation="reserve_stock", constraint="PRIMARY")
            return HTTPStatus.CONFLICT, {"status": "constraint", "order_id": order_id}
        except pymysql.err.OperationalError as exc:
            code = int(exc.args[0]) if exc.args else 0
            kind = "lock_timeout" if code == 1205 else "query" if code == 1054 else "connection"
            self._record_request("error")
            self._record_failure(kind)
            self.logger.write(
                "ERROR",
                "Inventory MySQL operation failed",
                order_id=order_id,
                mysql_error_code=code,
                failure_kind=kind,
                error=str(exc)[:180],
            )
            return HTTPStatus.SERVICE_UNAVAILABLE, {"status": kind, "order_id": order_id}
        except pymysql.MySQLError as exc:
            self._record_request("error")
            self._record_failure("query")
            self.logger.write("ERROR", "Inventory query failed", order_id=order_id, mysql_error_code=exc.args[0] if exc.args else None, error=str(exc)[:180])
            return HTTPStatus.SERVICE_UNAVAILABLE, {"status": "query_error", "order_id": order_id}
        finally:
            with self._lock:
                self.active_transactions = max(0, self.active_transactions - 1)

    def _sample_database(self) -> None:
        while True:
            try:
                with self.connect() as connection:
                    with connection.cursor() as cursor:
                        cursor.execute("SHOW STATUS LIKE 'Threads_connected'")
                        connected = int(cursor.fetchone()[1])
                        cursor.execute("SELECT @@max_connections")
                        maximum = int(cursor.fetchone()[0])
                with self._lock:
                    self.threads_connected = connected
                    self.server_max_connections = maximum
                    held = len(self._held_connections)
                    mode = self.failure_mode
                utilization = round(connected / max(1, maximum), 3)
                self.logger.write("INFO", "MySQL capacity sample", threads_connected=connected, max_connections=maximum, pool_checked_out=held, utilization=utilization)
            except pymysql.MySQLError as exc:
                self.logger.write("WARN", "Unable to sample MySQL capacity", error=str(exc)[:180])
            time.sleep(5)

    def _record_request(self, outcome: str) -> None:
        with self._lock:
            self.requests[outcome] += 1

    def _record_failure(self, kind: str) -> None:
        with self._lock:
            self.db_failures[kind] += 1

    def status(self) -> dict[str, Any]:
        with self._lock:
            return {
                "status": "ok",
                "failure_mode": self.failure_mode,
                "held_connections": len(self._held_connections),
                "threads_connected": self.threads_connected,
                "max_connections": self.server_max_connections,
                "query_revision": self.query_revision,
                "response_schema": self.response_schema,
            }

    def metrics(self) -> str:
        with self._lock:
            requests = dict(self.requests)
            failures = dict(self.db_failures)
            transaction_failures = dict(self.transaction_failures)
            active = self.active_transactions
            mode = self.failure_mode
            logs = self.logger.count
            connected = self.threads_connected
            maximum = self.server_max_connections
            held = len(self._held_connections)
        return "".join(
            [
                *(counter_line("inventory_reservation_requests_total", "Inventory reservation requests", value, outcome=outcome) for outcome, value in requests.items()),
                *(counter_line("inventory_database_failures_total", "Inventory database failures", value, kind=kind) for kind, value in failures.items()),
                *(counter_line("inventory_transaction_failures_total", "Inventory transaction failures", value, kind=kind) for kind, value in transaction_failures.items()),
                gauge_line("inventory_active_transactions", "Inventory MySQL transactions currently active", active),
                gauge_line("lab_mysql_threads_connected", "MySQL sessions observed by inventory", connected),
                gauge_line("lab_mysql_max_connections", "MySQL configured connection ceiling", maximum),
                gauge_line("lab_mysql_held_connections", "Inventory sessions checked out", held),
                counter_line("inventory_log_events_total", "Structured inventory log events emitted", logs),
            ]
        )


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
            if parsed.path == "/reserve":
                order_id = parse_qs(parsed.query).get("order_id", ["unknown"])[0]
                status, payload = state.reserve(order_id, self.headers.get("X-Signing-Key-Id", ""))
                self.send_json(status, payload)
                return
            self.send_json(HTTPStatus.NOT_FOUND, {"error": "not_found"})

        def do_POST(self) -> None:  # noqa: N802
            if urlparse(self.path).path != "/control/failure":
                self.send_json(HTTPStatus.NOT_FOUND, {"error": "not_found"})
                return
            try:
                payload = self.body_json()
                state.set_failure_mode(str(payload.get("mode", "")), payload.get("duration_seconds", 180), payload.get("settings"))
            except (ValueError, OSError, json.JSONDecodeError) as exc:
                self.send_json(HTTPStatus.BAD_REQUEST, {"error": str(exc)})
                return
            self.send_json(HTTPStatus.OK, state.status())

    return InventoryHandler


def main() -> None:
    state = InventoryState()
    state.initialize_database()
    serve(handler(state), int(os.environ.get("PORT", "8081")))


if __name__ == "__main__":
    main()
