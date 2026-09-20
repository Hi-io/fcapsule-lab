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


FAILURE_MODES = {"normal", "lock-contention", "connection-saturation"}


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
        self.active_transactions = 0
        self.threads_connected = 0
        self.server_max_connections = self.configured_max_connections
        self._held_connections: list[pymysql.Connection] = []
        self._storm_stop = threading.Event()
        self._lock = threading.Lock()

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
                            "INSERT INTO inventory_items (sku, quantity) VALUES ('sku-red-widget', 1000000) "
                            "ON DUPLICATE KEY UPDATE quantity=quantity"
                        )
                    connection.commit()
                self.logger.write("INFO", "Inventory MySQL schema ready", attempt=attempt)
                threading.Thread(target=self._sample_database, daemon=True).start()
                return
            except pymysql.MySQLError as exc:
                self.logger.write("WARN", "Waiting for inventory MySQL", attempt=attempt, error=str(exc)[:180])
                time.sleep(1)
        raise RuntimeError("MySQL did not become available")

    def set_failure_mode(self, mode: str) -> None:
        if mode not in FAILURE_MODES:
            raise ValueError(f"Unknown failure mode: {mode}")
        if mode != "connection-saturation":
            self._release_connections()
        with self._lock:
            self.failure_mode = mode
        if mode == "connection-saturation":
            self._storm_stop.clear()
            threading.Thread(target=self._connection_storm, daemon=True).start()
        self.logger.write(
            "WARN" if mode != "normal" else "INFO",
            "Inventory failure mode changed",
            mode=mode,
            configured_max_connections=self.configured_max_connections,
        )

    def _connection_storm(self) -> None:
        target = max(4, self.configured_max_connections - 1)
        while not self._storm_stop.is_set() and len(self._held_connections) < target:
            try:
                connection = self.connect()
                with connection.cursor() as cursor:
                    cursor.execute("SELECT 1")
                with self._lock:
                    self._held_connections.append(connection)
                self.logger.write(
                    "WARN",
                    "Leaked database session retained by connection pool",
                    held_connections=len(self._held_connections),
                    configured_max_connections=self.configured_max_connections,
                    pool_owner="inventory-runtime",
                )
            except pymysql.MySQLError as exc:
                self._record_failure("connection")
                self.logger.write(
                    "ERROR",
                    "MySQL rejected connection while inventory pool expanded",
                    held_connections=len(self._held_connections),
                    configured_max_connections=self.configured_max_connections,
                    error=str(exc)[:180],
                )
            time.sleep(0.08)

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

    def reserve(self, order_id: str) -> tuple[int, dict[str, Any]]:
        with self._lock:
            mode = self.failure_mode
            self.active_transactions += 1
        started = time.perf_counter()
        try:
            with self.connect() as connection:
                with connection.cursor() as cursor:
                    if mode == "lock-contention":
                        cursor.execute("SET SESSION innodb_lock_wait_timeout = 1")
                        cursor.execute("SELECT quantity FROM inventory_items WHERE sku=%s FOR UPDATE", ("sku-red-widget",))
                        cursor.execute("SELECT SLEEP(1.25)")
                    else:
                        cursor.execute("SELECT quantity FROM inventory_items WHERE sku=%s", ("sku-red-widget",))
                    cursor.fetchone()
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
        except pymysql.err.OperationalError as exc:
            code = int(exc.args[0]) if exc.args else 0
            kind = "lock_timeout" if code == 1205 else "connection"
            self._record_request("error")
            self._record_failure(kind)
            self.logger.write(
                "ERROR",
                "Inventory MySQL operation failed",
                order_id=order_id,
                failure_mode=mode,
                mysql_error_code=code,
                failure_kind=kind,
                error=str(exc)[:180],
            )
            return HTTPStatus.SERVICE_UNAVAILABLE, {"status": kind, "order_id": order_id}
        except pymysql.MySQLError as exc:
            self._record_request("error")
            self._record_failure("query")
            self.logger.write("ERROR", "Inventory query failed", order_id=order_id, error=str(exc)[:180])
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
                if mode == "connection-saturation" and held >= maximum - 4:
                    self.logger.write(
                        "ERROR",
                        "Connection pool retention is exhausting MySQL capacity",
                        pool_owner="inventory-runtime",
                        held_connections=held,
                        threads_connected=connected,
                        max_connections=maximum,
                        utilization=utilization,
                        expected_effect="new inventory connections may be rejected",
                    )
                else:
                    self.logger.write(
                        "INFO",
                        "MySQL capacity sample",
                        threads_connected=connected,
                        max_connections=maximum,
                        utilization=utilization,
                    )
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
            }

    def metrics(self) -> str:
        with self._lock:
            requests = dict(self.requests)
            failures = dict(self.db_failures)
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
                gauge_line("inventory_active_transactions", "Inventory MySQL transactions currently active", active),
                gauge_line("inventory_failure_mode_info", "Current injected inventory failure mode", 1, mode=mode),
                gauge_line("lab_mysql_threads_connected", "MySQL sessions observed by inventory", connected),
                gauge_line("lab_mysql_max_connections", "MySQL configured connection ceiling", maximum),
                gauge_line("lab_mysql_held_connections", "Sessions intentionally retained by the inventory pool", held),
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
            self.send_json(HTTPStatus.OK, state.status())

    return InventoryHandler


def main() -> None:
    state = InventoryState()
    state.initialize_database()
    serve(handler(state), int(os.environ.get("PORT", "8081")))


if __name__ == "__main__":
    main()
