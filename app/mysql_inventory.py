"""Inventory API backed by MySQL with controllable resource failures."""

from __future__ import annotations

import json
import hashlib
import math
import os
import threading
import time
import uuid
from contextlib import contextmanager
from http import HTTPStatus
from typing import Any, Iterator
from urllib.parse import parse_qs, urlparse

import pymysql

from app.common import JsonLogger, QuietHandler, counter_line, gauge_line, serve
from app.safety import lease_seconds


FAILURE_MODES = {
    "normal", "configured", "lock-contention", "connection-saturation",
    "response-contract", "token-collision", "deadlock", "downstream-latency",
    "fixed-latency",
}

RESERVATION_ADMISSION_WAIT_SECONDS = 0.25


class InventoryState:
    def __init__(self) -> None:
        self.host = os.environ.get("MYSQL_HOST", "mysql")
        self.user = os.environ.get("MYSQL_USER", "inventory")
        self.password = os.environ.get("MYSQL_PASSWORD", "inventory-lab")
        self.database = os.environ.get("MYSQL_DATABASE", "inventory")
        self.configured_max_connections = int(os.environ.get("MYSQL_MAX_CONNECTIONS", "40"))
        self.reservation_admission_limit = self._reservation_admission_limit(self.configured_max_connections)
        self.failure_mode = "normal"
        self.logger = JsonLogger("inventory-api")
        self.requests = {"success": 0, "error": 0}
        self.db_failures = {"lock_timeout": 0, "connection": 0, "query": 0, "deadlock": 0}
        self.transaction_failures = {"constraint": 0, "deadlock": 0}
        self.reservation_replays = 0
        self.active_transactions = 0
        self.threads_connected = 0
        self.server_max_connections = 0
        self.database_sample_timestamp = 0.0
        self.database_sample_failures = 0
        self.inventory_item_columns: tuple[str, ...] | None = None
        self.inventory_schema_sample_timestamp = 0.0
        self.inventory_schema_sample_failures = 0
        self.client_sessions_active = 0
        self.reservation_admissions = 0
        self.reservation_admissions_inflight = 0
        self.reservation_admission_rejections = 0
        self._reservation_slots = threading.BoundedSemaphore(self.reservation_admission_limit)
        self._held_connections: list[pymysql.Connection] = []
        self._collision_token: str | None = None
        self._collision_owner: str | None = None
        self._storm_stop = threading.Event()
        self._lock = threading.Lock()
        self._operation_lock = threading.Lock()
        self._task_stop = threading.Event()
        self._expires_at = 0.0
        self._control_run_id: str | None = None
        self.accepted_key_id = os.environ.get("INVENTORY_ACCEPTED_KEY_ID", "checkout-key-v1")
        self.response_schema = os.environ.get("INVENTORY_RESPONSE_SCHEMA", "v1")
        self.query_revision = os.environ.get("INVENTORY_QUERY_REVISION", "v1")
        self.configuration_revision = _configuration_revision(
            self.accepted_key_id, self.response_schema, self.query_revision,
        )
        self.fixed_latency = 0.0

    def connect(self, timeout: float = 2) -> pymysql.Connection:
        connection = pymysql.connect(
            host=self.host,
            user=self.user,
            password=self.password,
            database=self.database,
            connect_timeout=max(1, int(timeout)),
            read_timeout=3,
            write_timeout=3,
            autocommit=False,
        )
        with self._lock:
            self.client_sessions_active += 1
        return connection

    @contextmanager
    def _managed_connection(self, timeout: float = 2) -> Iterator[pymysql.Connection]:
        connection = self.connect(timeout)
        try:
            yield connection
        finally:
            self._close_connection(connection)

    def _close_connection(self, connection: pymysql.Connection) -> None:
        try:
            connection.close()
        finally:
            with self._lock:
                self.client_sessions_active = max(0, self.client_sessions_active - 1)

    def _transaction_started(self) -> None:
        with self._lock:
            self.active_transactions += 1

    def _transaction_finished(self) -> None:
        with self._lock:
            self.active_transactions = max(0, self.active_transactions - 1)

    @staticmethod
    def _reservation_admission_limit(max_connections: int) -> int:
        if isinstance(max_connections, bool) or not isinstance(max_connections, int) or max_connections <= 0:
            raise ValueError("MySQL max_connections must be a positive integer")
        # Every reservation updates the same stock row, so concurrent local transactions only queue on that row.
        return 1

    def initialize_database(self) -> None:
        for attempt in range(1, 61):
            try:
                with self._managed_connection() as connection:
                    with connection.cursor() as cursor:
                        cursor.execute(
                            "CREATE TABLE IF NOT EXISTS inventory_items "
                            "(sku VARCHAR(64) PRIMARY KEY, quantity INT NOT NULL) ENGINE=InnoDB"
                        )
                        cursor.execute(
                            "CREATE TABLE IF NOT EXISTS reservation_events "
                            "(token VARCHAR(64) PRIMARY KEY, order_id VARCHAR(64) NOT NULL, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, KEY idx_reservation_events_created_at (created_at)) ENGINE=InnoDB"
                        )
                        cursor.execute(
                            "CREATE TABLE IF NOT EXISTS inventory_reconciliation_audit "
                            "(id BIGINT AUTO_INCREMENT PRIMARY KEY, pair_id VARCHAR(36) NOT NULL, "
                            "sku VARCHAR(64) NOT NULL, observed_quantity INT NOT NULL, "
                            "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, KEY idx_reconciliation_created_at (created_at)) ENGINE=InnoDB"
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
                self._task_stop = threading.Event()
                self._storm_stop = self._task_stop
                threading.Thread(target=self._reconcile_stock, args=(self._task_stop, False), daemon=True).start()
                return
            except pymysql.MySQLError as exc:
                self.logger.write("WARN", "Waiting for inventory MySQL", attempt=attempt, error=str(exc)[:180])
                time.sleep(1)
        raise RuntimeError("MySQL did not become available")

    def set_failure_mode(self, mode: str, duration: int = 180, settings: dict[str, Any] | None = None,
                         run_id: str | None = None) -> None:
        if mode not in FAILURE_MODES:
            raise ValueError(f"Unknown failure mode: {mode}")
        run_id = _validate_run_id(run_id)
        duration = lease_seconds(duration)
        values = settings or {}
        if mode == "connection-saturation":
            self._connection_saturation_target(self.server_max_connections or self.configured_max_connections)
        if mode == "configured" and values.get("INVENTORY_QUERY_REVISION") == "v2":
            self._require_reserved_quantity_column_absent()
        with self._operation_lock:
            self._task_stop.set()
            self._release_connections()
            previous_token = self._collision_token
            previous_owner = self._collision_owner
            self._collision_token = None
            self._collision_owner = None
            if previous_token and previous_owner:
                self._cleanup_collision_record(previous_token, previous_owner)
            self._task_stop = threading.Event()
            self._storm_stop = self._task_stop
            accepted_key_id = str(values.get("INVENTORY_ACCEPTED_KEY_ID", "checkout-key-v1"))
            response_schema = str(values.get("INVENTORY_RESPONSE_SCHEMA", "v1"))
            query_revision = str(values.get("INVENTORY_QUERY_REVISION", "v1"))
            configuration_revision = _configuration_revision(
                accepted_key_id, response_schema, query_revision,
            )
            with self._lock:
                self.failure_mode = mode
                self._expires_at = time.monotonic() + duration if mode != "normal" else 0
                self.accepted_key_id = accepted_key_id
                self.response_schema = response_schema
                self.query_revision = query_revision
                self.configuration_revision = configuration_revision
                self.fixed_latency = 0.25 if mode == "fixed-latency" else 0.35 if mode == "downstream-latency" else 0.0
                self._collision_token = f"reservation-collision-{uuid.uuid4().hex}" if mode == "token-collision" else None
                self._control_run_id = run_id
            if mode == "connection-saturation":
                threading.Thread(target=self._connection_storm, args=(self._task_stop,), daemon=True).start()
            elif mode == "lock-contention":
                threading.Thread(target=self._reconcile_stock, args=(self._task_stop, True), daemon=True).start()
            elif mode == "deadlock":
                threading.Thread(target=self._deadlock_loop, args=(self._task_stop,), daemon=True).start()
            else:
                threading.Thread(target=self._reconcile_stock, args=(self._task_stop, False), daemon=True).start()
        self.logger.write("INFO", "Inventory runtime configuration reloaded",
                          configuration_revision=configuration_revision, run_id=run_id)

    def _cleanup_collision_record(self, token: str, owner: str) -> None:
        try:
            with self._managed_connection() as connection:
                with connection.cursor() as cursor:
                    cursor.execute("DELETE FROM reservation_events WHERE token=%s AND order_id=%s", (token, owner))
                connection.commit()
        except pymysql.MySQLError as exc:
            self.logger.write("WARN", "Owned reservation test record could not be removed during recovery",
                              token_ref=_safe_ref(token), owner_ref=_safe_ref(owner), error=str(exc)[:180])

    def _require_reserved_quantity_column_absent(self) -> None:
        with self._managed_connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute("SHOW COLUMNS FROM inventory_items LIKE 'reserved_quantity'")
                present = cursor.fetchone() is not None
        if present:
            raise ValueError("Schema-drift precondition failed: inventory_items.reserved_quantity already exists")

    def _lease_loop(self) -> None:
        while True:
            time.sleep(1)
            with self._lock:
                expired = self._expires_at and time.monotonic() >= self._expires_at
                run_id = self._control_run_id
            if expired:
                self.set_failure_mode("normal", run_id=run_id)

    def _reconcile_stock(self, stop: threading.Event, hold_for_contention: bool = False) -> None:
        while not stop.is_set():
            pair_id = uuid.uuid4().hex[:16]
            admission_acquired = False
            if not hold_for_contention:
                admission_acquired = self._reservation_slots.acquire(timeout=RESERVATION_ADMISSION_WAIT_SECONDS)
                if not admission_acquired:
                    self.logger.write("INFO", "Inventory reconciliation deferred while reservations are active",
                                      pair_id=pair_id, operation="stock_reconciliation",
                                      admission_limit=self.reservation_admission_limit)
                    stop.wait(2.0)
                    continue
            try:
                self._transaction_started()
                with self._managed_connection() as connection:
                    with connection.cursor() as cursor:
                        cursor.execute("SET SESSION innodb_lock_wait_timeout = 1")
                        cursor.execute("SELECT quantity FROM inventory_items WHERE sku=%s FOR UPDATE", ("sku-red-widget",))
                        red_quantity = int(cursor.fetchone()[0])
                        cursor.execute("SELECT quantity FROM inventory_items WHERE sku=%s FOR UPDATE", ("sku-blue-widget",))
                        blue_quantity = int(cursor.fetchone()[0])
                        self.logger.write(
                            "INFO", "Inventory reconciliation snapshot opened",
                            pair_id=pair_id, db_session=connection.thread_id(), first_sku="sku-red-widget",
                            second_sku="sku-blue-widget", operation="stock_reconciliation",
                        )
                        delay = 1.6 if hold_for_contention else 0.02
                        cancelled = stop.wait(delay)
                        if cancelled:
                            connection.rollback()
                            self.logger.write("INFO", "Inventory reconciliation cancelled",
                                              pair_id=pair_id, disposition="rollback", operation="stock_reconciliation")
                        else:
                            cursor.executemany(
                                "INSERT INTO inventory_reconciliation_audit (pair_id, sku, observed_quantity) VALUES (%s, %s, %s)",
                                ((pair_id, "sku-red-widget", red_quantity), (pair_id, "sku-blue-widget", blue_quantity)),
                            )
                            connection.commit()
                            self.logger.write("INFO", "Inventory reconciliation snapshot committed",
                                              pair_id=pair_id, rows_written=2, operation="stock_reconciliation")
                        cursor.execute("DELETE FROM inventory_reconciliation_audit WHERE created_at < UTC_TIMESTAMP() - INTERVAL 1 DAY")
                    connection.commit()
            except pymysql.MySQLError as exc:
                code = int(exc.args[0]) if exc.args else 0
                self.logger.write("WARN", "Inventory reconciliation interrupted", pair_id=pair_id,
                                  mysql_error_code=code, error=str(exc)[:180])
            finally:
                self._transaction_finished()
                if admission_acquired:
                    self._reservation_slots.release()
            stop.wait(2.0 if hold_for_contention else 12.0)

    def _connection_storm(self, stop: threading.Event) -> None:
        capacity = self.server_max_connections or self.configured_max_connections
        target = self._connection_saturation_target(capacity)
        self.logger.write("INFO", "Inventory session pressure bounded with server headroom",
                          checked_out_target=target,
                          observed_capacity=capacity,
                          reserved_connections=capacity - target)
        while not stop.is_set():
            with self._lock:
                at_target = len(self._held_connections) >= target
            if at_target:
                return
            connection = None
            try:
                connection = self.connect()
                with connection.cursor() as cursor:
                    cursor.execute("SELECT 1")
                with self._lock:
                    cancelled = stop.is_set()
                    if not cancelled:
                        self._held_connections.append(connection)
                        checked_out = len(self._held_connections)
                if cancelled:
                    self._close_connection(connection)
                    break
                self.logger.write(
                    "INFO", "Inventory database operation completed",
                    db_session=connection.thread_id(), operation="availability_lookup",
                    pool_checked_out=checked_out,
                )
            except pymysql.MySQLError as exc:
                if connection is not None:
                    try:
                        self._close_connection(connection)
                    except (pymysql.MySQLError, OSError):
                        pass
                self._record_failure("connection")
                self.logger.write(
                    "ERROR",
                    "Database session acquisition failed",
                    mysql_error_code=exc.args[0] if exc.args else None,
                    error=str(exc)[:180],
                )
            stop.wait(0.5)

    @staticmethod
    def _connection_saturation_target(max_connections: int) -> int:
        if isinstance(max_connections, bool) or not isinstance(max_connections, int) or max_connections <= 0:
            raise ValueError("MySQL max_connections must be a positive integer")
        reserve = max(3, math.ceil(max_connections * 0.15))
        target = max(4, max_connections - reserve)
        if target >= max_connections or target / max_connections <= 0.80:
            raise ValueError("Configured MySQL capacity cannot reach the alert threshold while reserving headroom")
        return target

    def _release_connections(self) -> None:
        self._storm_stop.set()
        with self._lock:
            connections = self._held_connections
            self._held_connections = []
        for connection in connections:
            try:
                self._close_connection(connection)
            except (pymysql.MySQLError, OSError):
                pass

    def _deadlock_loop(self, stop: threading.Event) -> None:
        while not stop.is_set():
            barrier = threading.Barrier(2)
            pair_id = uuid.uuid4().hex[:16]
            threads = [
                threading.Thread(target=self._deadlock_transaction, args=(first, second, barrier, pair_id), daemon=True)
                for first, second in (("sku-red-widget", "sku-blue-widget"), ("sku-blue-widget", "sku-red-widget"))
            ]
            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join(timeout=5)
            stop.wait(0.4)

    def _deadlock_transaction(self, first: str, second: str, barrier: threading.Barrier, pair_id: str | None = None) -> None:
        pair_id = pair_id or uuid.uuid4().hex[:16]
        transaction_id = uuid.uuid4().hex[:16]
        self._transaction_started()
        database_session_id: int | None = None
        try:
            with self._managed_connection() as connection:
                database_session_id = connection.thread_id()
                with connection.cursor() as cursor:
                    cursor.execute("SET SESSION innodb_lock_wait_timeout = 1")
                    cursor.execute("SELECT quantity FROM inventory_items WHERE sku=%s FOR UPDATE", (first,))
                    first_quantity = int(cursor.fetchone()[0])
                    self.logger.write("INFO", "Reconciliation acquired first stock row lock",
                                      transaction_id=transaction_id, pair_id=pair_id, sku=first, lock_order=1,
                                      db_session=database_session_id)
                    barrier.wait(timeout=2)
                    cursor.execute("SELECT quantity FROM inventory_items WHERE sku=%s FOR UPDATE", (second,))
                    second_quantity = int(cursor.fetchone()[0])
                    self.logger.write("INFO", "Reconciliation acquired second stock row lock",
                                      transaction_id=transaction_id, pair_id=pair_id, sku=second, lock_order=2,
                                      db_session=database_session_id)
                    cursor.executemany(
                        "INSERT INTO inventory_reconciliation_audit (pair_id, sku, observed_quantity) VALUES (%s, %s, %s)",
                        ((pair_id, first, first_quantity), (pair_id, second, second_quantity)),
                    )
                connection.commit()
                self.logger.write("INFO", "Stock reconciliation transaction committed",
                                  transaction_id=transaction_id, pair_id=pair_id, first_sku=first,
                                  second_sku=second, rows_written=2, operation="stock_reconciliation",
                                  db_session=database_session_id)
        except (pymysql.MySQLError, threading.BrokenBarrierError) as exc:
            code = int(exc.args[0]) if isinstance(exc, pymysql.MySQLError) and exc.args else 0
            if code == 1213:
                with self._lock:
                    self.transaction_failures["deadlock"] += 1
                    self.db_failures["deadlock"] += 1
                self.logger.write("ERROR", "Inventory reconciliation transaction selected as deadlock victim",
                                  mysql_error_code=code, transaction_id=transaction_id, pair_id=pair_id,
                                  first_sku=first, second_sku=second, outcome="rolled_back",
                                  operation="stock_reconciliation", db_session=database_session_id)
            else:
                self.logger.write("WARN", "Inventory reconciliation transaction interrupted",
                                  transaction_id=transaction_id, pair_id=pair_id,
                                  mysql_error_code=code, error_type=type(exc).__name__,
                                  db_session=database_session_id)
        finally:
            self._transaction_finished()

    def _reservation_token(self, order_id: str, mode: str) -> str:
        with self._lock:
            collision_token = self._collision_token if mode == "token-collision" else None
        if collision_token:
            return collision_token
        return "order-" + _safe_ref(order_id)

    def _existing_reservation_owner(self, token: str) -> str | None:
        with self._managed_connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute("SELECT order_id FROM reservation_events WHERE token=%s", (token,))
                row = cursor.fetchone()
        return str(row[0]) if row else None

    def _reservation_response(self, order_id: str, response_schema: str, mode: str) -> tuple[int, dict[str, Any]]:
        if mode == "response-contract":
            return HTTPStatus.OK, {"result": "accepted", "reference": order_id}
        if response_schema == "v2":
            return HTTPStatus.OK, {"reservation": {"status": "reserved", "order_id": order_id}, "schema": "v2"}
        return HTTPStatus.OK, {"status": "reserved", "order_id": order_id, "schema": "v1"}

    def reserve(self, order_id: str, key_id: str = "") -> tuple[int, dict[str, Any]]:
        with self._lock:
            mode = self.failure_mode
            accepted_key_id = self.accepted_key_id
            response_schema = self.response_schema
            query_revision = self.query_revision
            latency = self.fixed_latency
        started = time.perf_counter()
        order_ref = _safe_ref(order_id)
        self.logger.write("INFO", "Reservation transaction requested", order_ref=order_ref,
                          dependency="mysql", operation="reserve_stock")
        if key_id != accepted_key_id:
            self._record_request("error")
            self.logger.write("WARN", "Reservation request key ID was not accepted", order_ref=order_ref,
                              presented_key_id=key_id or "missing", accepted_key_id=accepted_key_id,
                              consumer_decision="reject_before_database")
            return HTTPStatus.UNAUTHORIZED, {"status": "unauthorized", "order_id": order_id}
        if latency:
            time.sleep(latency)

        if not self._reservation_slots.acquire(timeout=RESERVATION_ADMISSION_WAIT_SECONDS):
            with self._lock:
                self.reservation_admission_rejections += 1
            self._record_request("error")
            self.logger.write(
                "WARN", "Inventory reservation rejected before opening a MySQL session",
                order_ref=order_ref, failure_kind="admission", mysql_attempted=False,
                admission_limit=self.reservation_admission_limit,
                wait_seconds=RESERVATION_ADMISSION_WAIT_SECONDS,
            )
            return HTTPStatus.SERVICE_UNAVAILABLE, {"status": "inventory_busy", "order_id": order_id}

        with self._lock:
            self.reservation_admissions += 1
            self.reservation_admissions_inflight += 1
        token = self._reservation_token(order_id, mode)
        self._transaction_started()
        database_session_id: int | None = None
        try:
            with self._managed_connection() as connection:
                database_session_id = connection.thread_id()
                with connection.cursor() as cursor:
                    cursor.execute("SET SESSION innodb_lock_wait_timeout = 1")
                    cursor.execute("INSERT INTO reservation_events (token, order_id) VALUES (%s, %s)", (token, order_id))
                    if query_revision == "v2":
                        cursor.execute(
                            "UPDATE inventory_items SET quantity=quantity-1 "
                            "WHERE sku=%s AND quantity>reserved_quantity",
                            ("sku-red-widget",),
                        )
                    else:
                        cursor.execute(
                            "UPDATE inventory_items SET quantity=quantity-1 WHERE sku=%s AND quantity>0",
                            ("sku-red-widget",),
                        )
                    if cursor.rowcount == 0:
                        connection.rollback()
                        self._record_request("error")
                        self.logger.write("WARN", "Reservation rejected because no available stock remained",
                                          order_ref=order_ref, sku="sku-red-widget", consumer_decision="out_of_stock")
                        return HTTPStatus.CONFLICT, {"status": "out_of_stock", "order_id": order_id}
                connection.commit()
            if mode == "token-collision":
                with self._lock:
                    if self._collision_owner is None and self._collision_token == token:
                        self._collision_owner = order_id
            self._record_request("success")
            self.logger.write("INFO", "Inventory reservation committed", order_ref=order_ref,
                              duration_ms=round((time.perf_counter() - started) * 1000, 2),
                              upstream_status=int(HTTPStatus.OK), query_revision=query_revision,
                              db_session=database_session_id)
            return self._reservation_response(order_id, response_schema, mode)
        except pymysql.err.IntegrityError as exc:
            code = int(exc.args[0]) if exc.args else 0
            try:
                existing_order = self._existing_reservation_owner(token)
            except pymysql.MySQLError:
                existing_order = None
            if existing_order == order_id:
                with self._lock:
                    self.reservation_replays += 1
                self._record_request("success")
                self.logger.write("INFO", "Existing reservation returned without a second stock decrement",
                                  order_ref=order_ref, mysql_error_code=code, outcome="idempotent_replay")
                return self._reservation_response(order_id, response_schema, mode)
            with self._lock:
                self.transaction_failures["constraint"] += 1
            self._record_request("error")
            self.logger.write("ERROR", "Reservation token belongs to a different request",
                              order_ref=order_ref, existing_order_ref=_safe_ref(existing_order) if existing_order else "unavailable",
                              mysql_error_code=code, constraint="reservation_events.PRIMARY",
                              duplicate_key="reservation_events.token", token_ref=_safe_ref(token),
                              ownership_match=False, run_id=self._control_run_id,
                              db_session=database_session_id,
                              operation="reserve_stock")
            return HTTPStatus.CONFLICT, {"status": "reservation_conflict", "order_id": order_id}
        except pymysql.err.OperationalError as exc:
            code = int(exc.args[0]) if exc.args else 0
            kind = "lock_timeout" if code == 1205 else "deadlock" if code == 1213 else "query" if code == 1054 else "connection"
            self._record_request("error")
            self._record_failure(kind)
            if kind == "deadlock":
                with self._lock:
                    self.transaction_failures["deadlock"] += 1
            self.logger.write("ERROR", "Inventory MySQL operation failed", order_ref=order_ref,
                              mysql_error_code=code, failure_kind=kind, query_revision=query_revision,
                              operation="reserve_stock", table="inventory_items", sku="sku-red-widget",
                              db_session=database_session_id, error=str(exc)[:180])
            return HTTPStatus.SERVICE_UNAVAILABLE, {"status": kind, "order_id": order_id}
        except pymysql.MySQLError as exc:
            self._record_request("error")
            self._record_failure("query")
            self.logger.write("ERROR", "Inventory query failed", order_ref=order_ref,
                              mysql_error_code=exc.args[0] if exc.args else None,
                              query_revision=query_revision, operation="reserve_stock",
                              db_session=database_session_id, error=str(exc)[:180])
            return HTTPStatus.SERVICE_UNAVAILABLE, {"status": "query_error", "order_id": order_id}
        finally:
            self._transaction_finished()
            with self._lock:
                self.reservation_admissions_inflight = max(0, self.reservation_admissions_inflight - 1)
            self._reservation_slots.release()

    def _sample_database(self) -> None:
        while True:
            try:
                columns: tuple[str, ...] | None = None
                schema_error: pymysql.MySQLError | None = None
                with self._managed_connection() as connection:
                    with connection.cursor() as cursor:
                        cursor.execute("SHOW STATUS LIKE 'Threads_connected'")
                        connected = int(cursor.fetchone()[1])
                        cursor.execute("SELECT @@max_connections")
                        maximum = int(cursor.fetchone()[0])
                        try:
                            cursor.execute("SHOW COLUMNS FROM inventory_items")
                            columns = tuple(sorted(str(row[0]) for row in cursor.fetchall()))
                        except pymysql.MySQLError as exc:
                            schema_error = exc
                if connected < 0 or maximum <= 0:
                    raise ValueError("MySQL returned an invalid connection capacity sample")
                sampled_at = time.time()
                with self._lock:
                    self.threads_connected = connected
                    self.server_max_connections = maximum
                    self.database_sample_timestamp = sampled_at
                    held = len(self._held_connections)
                    previous_columns = self.inventory_item_columns
                    if columns is not None:
                        self.inventory_item_columns = columns
                        self.inventory_schema_sample_timestamp = sampled_at
                    else:
                        self.inventory_schema_sample_failures += 1
                if columns is not None and columns != previous_columns:
                    self.logger.write("INFO", "Inventory table columns sampled",
                                      table="inventory_items", columns=columns,
                                      column_count=len(columns), sample_timestamp_seconds=sampled_at)
                if schema_error is not None:
                    self.logger.write("WARN", "Unable to sample inventory schema; last sample retained with its timestamp",
                                      table="inventory_items", error=str(schema_error)[:180])
                utilization = round(connected / maximum, 3)
                self.logger.write("INFO", "MySQL capacity sample", threads_connected=connected,
                                  max_connections=maximum, pool_checked_out=held, utilization=utilization,
                                  sample_timestamp_seconds=sampled_at)
            except (pymysql.MySQLError, ValueError) as exc:
                with self._lock:
                    self.database_sample_failures += 1
                self.logger.write("WARN", "Unable to sample MySQL capacity; last sample retained with its timestamp",
                                  error=str(exc)[:180])
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
                "run_id": self._control_run_id,
                "held_connections": len(self._held_connections),
                "client_sessions_active": self.client_sessions_active,
                "threads_connected": self.threads_connected,
                "max_connections": self.server_max_connections,
                "database_sample_timestamp_seconds": self.database_sample_timestamp,
                "inventory_item_columns": list(self.inventory_item_columns) if self.inventory_item_columns is not None else None,
                "inventory_schema_sample_timestamp_seconds": self.inventory_schema_sample_timestamp,
                "inventory_schema_sample_failures": self.inventory_schema_sample_failures,
                "reservation_admission_limit": self.reservation_admission_limit,
                "reservation_admissions_inflight": self.reservation_admissions_inflight,
                "reservation_admission_rejections": self.reservation_admission_rejections,
                "configuration_revision": self.configuration_revision,
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
            client_sessions = self.client_sessions_active
            sample_timestamp = self.database_sample_timestamp
            sample_failures = self.database_sample_failures
            schema_sample_timestamp = self.inventory_schema_sample_timestamp
            schema_sample_failures = self.inventory_schema_sample_failures
            reservation_replays = self.reservation_replays
            reservation_admissions = self.reservation_admissions
            reservation_admissions_inflight = self.reservation_admissions_inflight
            admission_rejections = self.reservation_admission_rejections
            admission_limit = self.reservation_admission_limit
            schema_column_count = len(self.inventory_item_columns) if self.inventory_item_columns is not None else 0
        return "".join(
            [
                *(counter_line("inventory_reservation_requests_total", "Inventory reservation requests", value, outcome=outcome) for outcome, value in requests.items()),
                *(counter_line("inventory_database_failures_total", "Inventory database failures", value, kind=kind) for kind, value in failures.items()),
                *(counter_line("inventory_transaction_failures_total", "Inventory transaction failures", value, kind=kind) for kind, value in transaction_failures.items()),
                gauge_line("inventory_active_transactions", "Inventory MySQL transactions currently active", active),
                gauge_line("inventory_mysql_client_sessions_active", "Open MySQL sessions owned by this inventory API process", client_sessions),
                gauge_line("inventory_mysql_threads_connected", "Server Threads_connected from the last successful sample", connected),
                gauge_line("inventory_mysql_server_max_connections", "Server max_connections from the last successful sample", maximum),
                gauge_line("inventory_mysql_sample_timestamp_seconds", "Unix timestamp of the last successful MySQL capacity sample", sample_timestamp),
                counter_line("inventory_mysql_sample_failures_total", "Failed MySQL capacity sampling attempts", sample_failures),
                gauge_line("inventory_mysql_inventory_items_schema_sample_timestamp_seconds", "Unix timestamp of the last successful inventory_items schema sample", schema_sample_timestamp),
                counter_line("inventory_mysql_schema_sample_failures_total", "Failed inventory_items schema sampling attempts", schema_sample_failures),
                counter_line("inventory_reservation_replays_total", "Reservation retries served without decrementing stock again", reservation_replays),
                counter_line("inventory_reservation_admissions_total", "Reservation transactions admitted to a MySQL session", reservation_admissions),
                gauge_line("inventory_reservation_admission_inflight", "Reservation transactions currently admitted to execute MySQL work", reservation_admissions_inflight),
                counter_line("inventory_reservation_admission_rejections_total", "Reservations rejected before opening a MySQL session when the local database concurrency limit was reached", admission_rejections),
                gauge_line("inventory_reservation_admission_limit", "Maximum concurrent reservation transactions admitted by this inventory process", admission_limit),
                gauge_line("inventory_mysql_inventory_items_column_count", "Number of inventory_items columns in the latest successful schema sample", schema_column_count),
                gauge_line("lab_mysql_threads_connected", "MySQL sessions observed by inventory", connected),
                gauge_line("lab_mysql_max_connections", "MySQL server connection ceiling from the last successful sample", maximum),
                gauge_line("lab_mysql_held_connections", "Inventory sessions checked out", held),
                counter_line("inventory_log_events_total", "Structured inventory log events emitted", logs),
            ]
        )


def _safe_ref(value: str | None) -> str:
    return hashlib.sha256((value or "").encode("utf-8")).hexdigest()[:12]


def _configuration_revision(accepted_key_id: str, response_schema: str, query_revision: str) -> str:
    settings = json.dumps([accepted_key_id, response_schema, query_revision], separators=(",", ":"))
    return hashlib.sha256(settings.encode("utf-8")).hexdigest()[:12]


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
                state.set_failure_mode(str(payload.get("mode", "")), payload.get("duration_seconds", 180),
                                       payload.get("settings"), payload.get("run_id"))
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
