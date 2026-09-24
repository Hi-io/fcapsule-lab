from io import BytesIO
import threading
import unittest
from urllib.error import HTTPError
from unittest.mock import MagicMock, Mock, patch

import pymysql

from app.mysql_inventory import InventoryState
from app.orders import OrdersState


RUN_ID = "0123456789abcdef0123456789abcdef"


def mysql_connection(cursor):
    connection = MagicMock()
    connection.cursor.return_value.__enter__.return_value = cursor
    return connection


class MysqlInventoryAdmissionTests(unittest.TestCase):
    def test_admission_limit_serializes_the_shared_stock_row(self):
        self.assertEqual(InventoryState._reservation_admission_limit(40), 1)
        for value in (0, -1, True, 40.0):
            with self.subTest(value=value), self.assertRaises(ValueError):
                InventoryState._reservation_admission_limit(value)

    def test_reservation_admission_bounds_open_sessions_and_reports_waiters(self):
        state = InventoryState()
        state.logger = Mock()
        state.logger.count = 0
        statement_started = threading.Event()
        release_statement = threading.Event()
        cursor = MagicMock()
        cursor.rowcount = 1

        def connect(_timeout=2):
            return mysql_connection(cursor)

        def execute(_statement, *_params):
            statement_started.set()
            if not release_statement.wait(2):
                raise AssertionError("test did not release the admitted query")

        cursor.execute.side_effect = execute

        state.connect = Mock(side_effect=connect)
        outcomes = []
        admitted = threading.Thread(
            target=lambda: outcomes.append(state.reserve("order-a", "checkout-key-v1")),
        )
        admitted.start()

        self.assertTrue(statement_started.wait(2))
        limited_status, limited_body = state.reserve("order-b", "checkout-key-v1")
        release_statement.set()
        admitted.join(2)

        self.assertFalse(admitted.is_alive())
        self.assertEqual(limited_status, 503)
        self.assertEqual(limited_body["status"], "inventory_busy")
        self.assertEqual(state.connect.call_count, 1)
        self.assertEqual([status for status, _body in outcomes], [200])
        self.assertEqual(state.reservation_admission_rejections, 1)
        self.assertEqual(state.reservation_admissions, 1)
        self.assertEqual(state.reservation_admissions_inflight, 0)
        self.assertIn("inventory_reservation_admission_limit 1", state.metrics())
        self.assertIn("inventory_reservation_admissions_total 1", state.metrics())
        self.assertIn("inventory_reservation_admission_inflight 0", state.metrics())
        self.assertIn("inventory_reservation_admission_rejections_total 1", state.metrics())
        rejection = next(call.kwargs for call in state.logger.write.call_args_list
                         if call.args and call.args[1] == "Inventory reservation rejected before opening a MySQL session")
        self.assertFalse(rejection["mysql_attempted"])
        self.assertEqual(rejection["failure_kind"], "admission")

    def test_lock_contention_retries_cannot_create_connection_pressure(self):
        state = InventoryState()
        state.logger = Mock()
        state.logger.count = 0
        state.failure_mode = "lock-contention"
        update_started = threading.Event()
        release_updates = threading.Event()

        def connect(_timeout=2):
            cursor = MagicMock()
            cursor.rowcount = 1

            def execute(statement, *_params):
                if statement.startswith("UPDATE inventory_items"):
                    update_started.set()
                    if not release_updates.wait(2):
                        raise AssertionError("test did not release simulated lock waits")
                    raise pymysql.err.OperationalError(1205, "lock wait timeout")

            cursor.execute.side_effect = execute
            return mysql_connection(cursor)

        state.connect = Mock(side_effect=connect)
        outcomes = []
        admitted = threading.Thread(
            target=lambda: outcomes.append(state.reserve("order-lock-a", "checkout-key-v1")),
        )
        admitted.start()

        self.assertTrue(update_started.wait(2))
        limited_status, limited_body = state.reserve("order-lock-b", "checkout-key-v1")
        release_updates.set()
        admitted.join(2)

        self.assertFalse(admitted.is_alive())
        self.assertEqual(limited_status, 503)
        self.assertEqual(limited_body["status"], "inventory_busy")
        self.assertEqual(state.connect.call_count, 1)
        self.assertEqual([body["status"] for _status, body in outcomes], ["lock_timeout"])
        self.assertEqual(state.db_failures["lock_timeout"], 1)
        self.assertEqual(state.db_failures["connection"], 0)
        self.assertEqual(state.reservation_admission_rejections, 1)

    def test_normal_reconciliation_yields_to_an_active_reservation(self):
        state = InventoryState()
        state.logger = Mock()
        state.logger.count = 0
        state.connect = Mock()
        stop = threading.Event()

        def log_and_stop(_level, message, **_fields):
            if message == "Inventory reconciliation deferred while reservations are active":
                stop.set()

        state.logger.write.side_effect = log_and_stop
        self.assertTrue(state._reservation_slots.acquire(blocking=False))
        try:
            state._reconcile_stock(stop, hold_for_contention=False)
        finally:
            state._reservation_slots.release()

        state.connect.assert_not_called()
        self.assertTrue(any(call.args[1] == "Inventory reconciliation deferred while reservations are active"
                            for call in state.logger.write.call_args_list))

    def test_intentional_lock_contention_injector_bypasses_normal_admission_gate(self):
        state = InventoryState()
        state.logger = Mock()
        state.logger.count = 0
        stop = threading.Event()
        cursor = MagicMock()
        cursor.fetchone.return_value = (100,)
        connection = mysql_connection(cursor)
        state.connect = Mock(return_value=connection)
        state.logger.write.side_effect = lambda _level, message, **_fields: (
            stop.set() if message == "Inventory reconciliation snapshot opened" else None
        )

        self.assertTrue(state._reservation_slots.acquire(blocking=False))
        try:
            state._reconcile_stock(stop, hold_for_contention=True)
            state.connect.assert_called_once()
        finally:
            state._reservation_slots.release()

    def test_stalled_commit_rejects_new_work_before_the_dependency_timeout(self):
        state = InventoryState()
        state.logger = Mock()
        state.logger.count = 0
        cursor = MagicMock()
        cursor.rowcount = 1
        connection = mysql_connection(cursor)
        commit_started = threading.Event()
        release_commit = threading.Event()

        def commit():
            commit_started.set()
            if not release_commit.wait(2):
                raise AssertionError("test did not release the simulated COMMIT stall")

        connection.commit.side_effect = commit
        state.connect = Mock(return_value=connection)
        outcomes = []
        admitted = threading.Thread(
            target=lambda: outcomes.append(state.reserve("order-stalled-commit", "checkout-key-v1")),
        )
        admitted.start()

        self.assertTrue(commit_started.wait(2))
        started = threading.Event()
        second = threading.Thread(
            target=lambda: (started.set(), outcomes.append(state.reserve("order-during-stall", "checkout-key-v1"))),
        )
        second.start()
        self.assertTrue(started.wait(1))
        second.join(1.0)
        self.assertFalse(second.is_alive(), "admission rejection should finish within the 1.5 s dependency timeout")
        self.assertEqual(state.connect.call_count, 1)
        release_commit.set()
        admitted.join(2)
        second.join(2)

        self.assertFalse(admitted.is_alive())
        self.assertFalse(second.is_alive())
        self.assertEqual(sorted((status, body["status"]) for status, body in outcomes),
                         [(200, "reserved"), (503, "inventory_busy")])
        self.assertEqual(state.connect.call_count, 1)
        self.assertEqual(state.reservation_admission_rejections, 1)
        self.assertIn("inventory_database_failures_total{kind=\"connection\"} 0", state.metrics())

    def test_stalled_commit_retries_are_http_errors_not_timeout_amplification(self):
        inventory = InventoryState()
        inventory.logger = Mock()
        inventory.logger.count = 0
        cursor = MagicMock()
        cursor.rowcount = 1
        connection = mysql_connection(cursor)
        commit_started = threading.Event()
        release_commit = threading.Event()

        def commit():
            commit_started.set()
            if not release_commit.wait(3):
                raise AssertionError("test did not release the simulated COMMIT stall")

        connection.commit.side_effect = commit
        inventory.connect = Mock(return_value=connection)
        owner_result = []
        owner = threading.Thread(
            target=lambda: owner_result.append(inventory.reserve("order-stalled-commit", "checkout-key-v1")),
        )
        owner.start()
        self.assertTrue(commit_started.wait(2))

        with patch.dict("os.environ", {
            "INVENTORY_URL": "http://inventory-api:8081",
            "INVENTORY_TIMEOUT_SECONDS": "1.5",
            "MAX_RETRIES": "3",
        }):
            orders = OrdersState()
        orders.logger = Mock()

        def urlopen(request, timeout):
            self.assertEqual(timeout, 1.5)
            upstream_status, body = inventory.reserve("order-during-commit-stall", "checkout-key-v1")
            raise HTTPError(request.full_url, int(upstream_status), "inventory busy", {},
                            BytesIO(f'{{"status":"{body["status"]}"}}'.encode()))

        try:
            with patch("app.orders.urlopen", side_effect=urlopen):
                consumer_status, _body = orders.checkout("order-during-commit-stall")
            self.assertEqual(consumer_status, 503)
            self.assertEqual(orders.inventory_attempts, 3)
            self.assertEqual(orders.dependency_failures["http_error"], 3)
            self.assertEqual(orders.dependency_failures["timeout"], 0)
            self.assertEqual(inventory.connect.call_count, 1)
            self.assertEqual(inventory.reservation_admission_rejections, 3)
        finally:
            release_commit.set()
            owner.join(2)

        self.assertFalse(owner.is_alive())
        self.assertEqual(owner_result[0][0], 200)

    def test_real_mysql_1040_remains_a_connection_failure_not_an_admission_rejection(self):
        state = InventoryState()
        state.logger = Mock()
        state.logger.count = 0
        state.connect = Mock(side_effect=pymysql.err.OperationalError(1040, "Too many connections"))

        status, body = state.reserve("order-full", "checkout-key-v1")

        self.assertEqual(status, 503)
        self.assertEqual(body["status"], "connection")
        self.assertEqual(state.db_failures["connection"], 1)
        self.assertEqual(state.reservation_admission_rejections, 0)
        failure = next(call.kwargs for call in state.logger.write.call_args_list
                       if call.args and call.args[1] == "Inventory MySQL operation failed")
        self.assertEqual(failure["mysql_error_code"], 1040)
        self.assertEqual(failure["failure_kind"], "connection")

    def test_connection_saturation_keeps_its_capacity_evidence_with_admission_enabled(self):
        state = InventoryState()
        state.logger = Mock()
        state.logger.count = 0
        state.failure_mode = "connection-saturation"
        state.server_max_connections = 40
        state._held_connections = [MagicMock() for _ in range(34)]
        state.client_sessions_active = 34

        def connect(_timeout=2):
            state.client_sessions_active += 1
            cursor = MagicMock()
            cursor.rowcount = 1
            return mysql_connection(cursor)

        state.connect = Mock(side_effect=connect)

        status, _body = state.reserve("order-under-pressure", "checkout-key-v1")

        self.assertEqual(status, 200)
        self.assertIn("inventory_mysql_client_sessions_active 34", state.metrics())
        self.assertIn("lab_mysql_held_connections 34", state.metrics())
        self.assertEqual(state.reservation_admission_rejections, 0)


class MysqlInventoryEvidenceTests(unittest.TestCase):
    def test_collision_log_names_the_duplicate_key_without_exposing_token(self):
        state = InventoryState()
        state.logger = Mock()
        state.logger.count = 0
        with patch("app.mysql_inventory.threading.Thread"):
            state.set_failure_mode("token-collision", run_id=RUN_ID)

        first_cursor = MagicMock()
        first_cursor.rowcount = 1
        duplicate_cursor = MagicMock()
        duplicate_cursor.execute.side_effect = [None, pymysql.err.IntegrityError(1062, "duplicate key")]
        owner_cursor = MagicMock()
        owner_cursor.fetchone.return_value = ("order-first",)
        state.connect = Mock(side_effect=[
            mysql_connection(first_cursor), mysql_connection(duplicate_cursor), mysql_connection(owner_cursor),
        ])

        self.assertEqual(state.reserve("order-first", "checkout-key-v1")[0], 200)
        status, response = state.reserve("order-second", "checkout-key-v1")

        self.assertEqual(status, 409)
        self.assertEqual(response["status"], "reservation_conflict")
        collision = next(call.kwargs for call in state.logger.write.call_args_list
                         if call.args and call.args[1] == "Reservation token belongs to a different request")
        self.assertEqual(collision["duplicate_key"], "reservation_events.token")
        self.assertEqual(collision["run_id"], RUN_ID)
        self.assertEqual(collision["ownership_match"], False)
        self.assertNotIn(state._collision_token, collision.values())

    def test_schema_sample_exposes_observed_columns_through_health_metrics_and_log(self):
        state = InventoryState()
        state.logger = Mock()
        state.logger.count = 0
        connection = MagicMock()
        connection.__enter__.return_value = connection
        cursor = connection.cursor.return_value.__enter__.return_value
        cursor.fetchone.side_effect = [("Threads_connected", "3"), ("40",)]
        cursor.fetchall.return_value = [("quantity",), ("sku",)]

        with patch.object(state, "_managed_connection", return_value=connection), \
                patch("app.mysql_inventory.time.sleep", side_effect=KeyboardInterrupt):
            with self.assertRaises(KeyboardInterrupt):
                state._sample_database()

        self.assertEqual(state.status()["inventory_item_columns"], ["quantity", "sku"])
        self.assertIn("inventory_mysql_inventory_items_column_count 2", state.metrics())
        schema_event = next(call.kwargs for call in state.logger.write.call_args_list
                            if call.args and call.args[1] == "Inventory table columns sampled")
        self.assertEqual(schema_event["table"], "inventory_items")
        self.assertEqual(schema_event["columns"], ("quantity", "sku"))
        self.assertGreater(state.status()["inventory_schema_sample_timestamp_seconds"], 0)

    def test_schema_sample_failure_keeps_capacity_sample_and_reports_its_own_failure(self):
        state = InventoryState()
        state.logger = Mock()
        state.logger.count = 0
        connection = MagicMock()
        connection.__enter__.return_value = connection
        cursor = connection.cursor.return_value.__enter__.return_value
        cursor.fetchone.side_effect = [("Threads_connected", "3"), ("40",)]

        def execute(statement, *_params):
            if statement == "SHOW COLUMNS FROM inventory_items":
                raise pymysql.err.OperationalError(1142, "SHOW command denied")

        cursor.execute.side_effect = execute
        with patch.object(state, "_managed_connection", return_value=connection), \
                patch("app.mysql_inventory.time.sleep", side_effect=KeyboardInterrupt):
            with self.assertRaises(KeyboardInterrupt):
                state._sample_database()

        status = state.status()
        self.assertEqual(status["threads_connected"], 3)
        self.assertEqual(status["max_connections"], 40)
        self.assertGreater(status["database_sample_timestamp_seconds"], 0)
        self.assertEqual(status["inventory_item_columns"], None)
        self.assertEqual(status["inventory_schema_sample_timestamp_seconds"], 0)
        self.assertEqual(status["inventory_schema_sample_failures"], 1)
        self.assertIn("inventory_mysql_schema_sample_failures_total 1", state.metrics())
        self.assertIn("inventory_mysql_sample_failures_total 0", state.metrics())

    def test_lock_timeout_keeps_row_operation_and_transaction_outcome_evidence(self):
        state = InventoryState()
        state.logger = Mock()
        state.logger.count = 0
        cursor = MagicMock()
        cursor.execute.side_effect = [None, pymysql.err.OperationalError(1205, "lock wait timeout")]
        state.connect = Mock(return_value=mysql_connection(cursor))

        status, body = state.reserve("order-locked", "checkout-key-v1")

        self.assertEqual(status, 503)
        self.assertEqual(body["status"], "lock_timeout")
        self.assertEqual(state.db_failures["lock_timeout"], 1)
        failure = next(call.kwargs for call in state.logger.write.call_args_list
                       if call.args and call.args[1] == "Inventory MySQL operation failed")
        self.assertEqual(failure["operation"], "reserve_stock")
        self.assertEqual(failure["table"], "inventory_items")
        self.assertEqual(failure["sku"], "sku-red-widget")
        self.assertEqual(failure["mysql_error_code"], 1205)
        self.assertEqual(state.active_transactions, 0)

    def test_deadlock_victim_does_not_increment_lock_or_connection_failures(self):
        state = InventoryState()
        state.logger = Mock()
        state.logger.count = 0
        cursor = MagicMock()
        cursor.fetchone.return_value = (100,)
        cursor.execute.side_effect = [None, None, pymysql.err.OperationalError(1213, "deadlock")]
        state.connect = Mock(return_value=mysql_connection(cursor))

        state._deadlock_transaction("sku-red-widget", "sku-blue-widget", Mock(), "pair-deadlock")

        self.assertEqual(state.transaction_failures["deadlock"], 1)
        self.assertEqual(state.db_failures["deadlock"], 1)
        self.assertEqual(state.db_failures["lock_timeout"], 0)
        self.assertEqual(state.db_failures["connection"], 0)
        self.assertIn('inventory_transaction_failures_total{kind="deadlock"} 1', state.metrics())
        event = state.logger.write.call_args.kwargs
        self.assertEqual(event["pair_id"], "pair-deadlock")
        self.assertEqual(event["mysql_error_code"], 1213)

    def test_owned_recovery_stops_the_old_mode_and_releases_held_sessions(self):
        state = InventoryState()
        state.logger = Mock()
        connections = [MagicMock(), MagicMock()]
        state._held_connections = connections
        state.client_sessions_active = len(connections)
        state._collision_token = "owned-token"
        state._collision_owner = "owned-order"
        state._cleanup_collision_record = Mock()
        with patch("app.mysql_inventory.threading.Thread"):
            state.set_failure_mode("deadlock", run_id=RUN_ID)
            deadlock_stop = state._task_stop
            state.set_failure_mode("normal", run_id=RUN_ID)

        self.assertTrue(deadlock_stop.is_set())
        self.assertTrue(all(connection.close.called for connection in connections))
        self.assertEqual(state.client_sessions_active, 0)
        self.assertEqual(state.failure_mode, "normal")
        self.assertEqual(state.status()["run_id"], RUN_ID)
        state._cleanup_collision_record.assert_called_once_with("owned-token", "owned-order")


if __name__ == "__main__":
    unittest.main()
