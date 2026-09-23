import base64
import json
import threading
import unittest
from unittest.mock import MagicMock, Mock, patch

import psycopg
import pymysql

from app.inventory import InventoryState as PostgresInventoryState
from app.mysql_inventory import InventoryState as MysqlInventoryState
from app.orders import OrdersState
from app.worker import WorkerState, _validated_import_records, decode_job


RUN_ID = "a" * 32


def mysql_connection(cursor):
    connection = MagicMock()
    context_cursor = MagicMock()
    context_cursor.__enter__.return_value = cursor
    connection.cursor.return_value = context_cursor
    return connection


class ApplicationMechanicsTests(unittest.TestCase):
    def test_orders_idempotent_retry_avoids_second_inventory_call(self):
        with patch.dict("os.environ", {"INVENTORY_URL": "http://inventory-api:8081"}):
            state = OrdersState()
        state.logger = Mock()
        state._attempt_inventory_result = Mock(return_value={
            "status": 200, "kind": "success", "upstream_status": 200,
            "retryable": False, "duration_ms": 2.0,
        })

        first_status, first = state.checkout("order-17", "client-key-17")
        replay_status, replay = state.checkout("order-17", "client-key-17")

        self.assertEqual((first_status, replay_status), (200, 200))
        self.assertTrue(replay["replayed"])
        self.assertEqual(state._attempt_inventory_result.call_count, 1)

    def test_orders_rejects_same_idempotency_key_for_different_order(self):
        with patch.dict("os.environ", {"INVENTORY_URL": "http://inventory-api:8081"}):
            state = OrdersState()
        state.logger = Mock()
        state._attempt_inventory_result = Mock(return_value={
            "status": 200, "kind": "success", "upstream_status": 200,
            "retryable": False, "duration_ms": 2.0,
        })
        state.checkout("order-17", "client-key-17")

        status, response = state.checkout("order-18", "client-key-17")

        self.assertEqual(status, 409)
        self.assertEqual(response["status"], "idempotency_conflict")
        self.assertEqual(state._attempt_inventory_result.call_count, 1)

    def test_orders_configuration_log_correlates_run_id_and_rejects_invalid_id(self):
        with patch.dict("os.environ", {"INVENTORY_URL": "http://inventory-api:8081"}):
            state = OrdersState()
        state.logger = Mock()
        state.set_mode("normal", run_id=RUN_ID)
        self.assertEqual(state.logger.write.call_args.kwargs["run_id"], RUN_ID)
        with self.assertRaises(ValueError):
            state.set_mode("normal", run_id="not-a-run-id")

    def test_worker_configuration_log_correlates_run_id_without_oracle_fields(self):
        state = WorkerState()
        state.logger = Mock()
        with patch.object(state, "_start_mode_work"):
            state.set_mode("memory-leak", run_id=RUN_ID)
        event = state.logger.write.call_args.kwargs
        self.assertEqual(event["run_id"], RUN_ID)
        self.assertIn("configuration_revision", event)
        self.assertNotIn("mode", event)
        self.assertNotIn("scenario", event)

    def test_mysql_configuration_log_correlates_run_id_without_oracle_fields(self):
        state = MysqlInventoryState()
        state.logger = Mock()
        with patch("app.mysql_inventory.threading.Thread"):
            state.set_failure_mode("normal", run_id=RUN_ID)
        event = state.logger.write.call_args.kwargs
        self.assertEqual(event["run_id"], RUN_ID)
        self.assertNotIn("mode", event)
        self.assertNotIn("scenario", event)

    def test_mysql_successful_reservation_writes_idempotency_and_decrements_stock(self):
        state = MysqlInventoryState()
        state.logger = Mock()
        cursor = MagicMock()
        cursor.rowcount = 1
        connection = mysql_connection(cursor)
        state.connect = Mock(return_value=connection)

        status, response = state.reserve("order-17", "checkout-key-v1")

        self.assertEqual(status, 200)
        self.assertEqual(response["status"], "reserved")
        statements = [call.args[0] for call in cursor.execute.call_args_list]
        self.assertTrue(any("INSERT INTO reservation_events" in statement for statement in statements))
        self.assertTrue(any("quantity=quantity-1" in statement for statement in statements))
        connection.commit.assert_called_once()
        self.assertEqual(state.client_sessions_active, 0)

    def test_mysql_duplicate_reservation_is_replayed_without_second_decrement(self):
        state = MysqlInventoryState()
        state.logger = Mock()
        transaction_cursor = MagicMock()
        transaction_cursor.execute.side_effect = [None, pymysql.err.IntegrityError(1062, "duplicate key")]
        owner_cursor = MagicMock()
        owner_cursor.fetchone.return_value = ("order-17",)
        state.connect = Mock(side_effect=[mysql_connection(transaction_cursor), mysql_connection(owner_cursor)])

        status, response = state.reserve("order-17", "checkout-key-v1")

        self.assertEqual(status, 200)
        self.assertEqual(response["status"], "reserved")
        self.assertEqual(state.reservation_replays, 1)
        self.assertEqual(len(transaction_cursor.execute.call_args_list), 2)

    def test_mysql_client_capacity_metrics_have_no_scenario_labels(self):
        state = MysqlInventoryState()
        metrics = state.metrics()
        self.assertIn("inventory_mysql_client_sessions_active 0", metrics)
        self.assertIn("inventory_mysql_server_max_connections 0", metrics)
        self.assertIn("inventory_mysql_sample_timestamp_seconds 0", metrics)
        self.assertNotIn("failure_mode=", metrics)

    def test_postgres_failure_uses_actual_credential_config_and_run_id(self):
        state = PostgresInventoryState.__new__(PostgresInventoryState)
        state.database_url = "postgresql://inventory:secret@postgres:5432/inventory"
        state.effective_database_url = state.database_url
        state.failure_mode = "normal"
        state.logger = Mock()
        state.requests = {"success": 0, "error": 0}
        state.db_failures = {"lock_timeout": 0, "connection": 0, "authentication": 0, "query": 0}
        state.active_transactions = 0
        state.max_active_transactions = 0
        state._lock = threading.Lock()
        state._expires_at = 0
        state._control_run_id = None
        state._config_revision = "before"

        state.set_failure_mode("bad-database-config", run_id=RUN_ID)

        self.assertNotEqual(state.effective_database_url, state.database_url)
        self.assertIn("secret-invalid", state.effective_database_url)
        self.assertEqual(state._control_run_id, RUN_ID)
        self.assertEqual(state.logger.write.call_args.kwargs["run_id"], RUN_ID)
        self.assertNotIn("secret", str(state.logger.write.call_args))

    def test_postgres_successful_reservation_updates_stock_and_is_idempotent(self):
        state = PostgresInventoryState.__new__(PostgresInventoryState)
        state.database_url = "postgresql://inventory:secret@postgres:5432/inventory"
        state.effective_database_url = state.database_url
        state.failure_mode = "normal"
        state.logger = Mock()
        state.requests = {"success": 0, "error": 0}
        state.db_failures = {"lock_timeout": 0, "connection": 0, "authentication": 0, "query": 0}
        state.active_transactions = 0
        state.max_active_transactions = 0
        state._lock = threading.Lock()

        connection = MagicMock()
        connection.__enter__.return_value = connection
        cursor = MagicMock()
        cursor.fetchone.side_effect = [(100,), (99,)]
        cursor.rowcount = 1
        connection.__enter__.return_value = connection
        connection.cursor.return_value.__enter__.return_value = cursor
        with patch("app.inventory.psycopg.connect", return_value=connection):
            status, body = state.reserve("order-17")

        self.assertEqual(status, 200)
        self.assertEqual(body["status"], "reserved")
        statements = [call.args[0] for call in cursor.execute.call_args_list]
        self.assertTrue(any("INSERT INTO reservation_events" in statement for statement in statements))
        self.assertTrue(any("UPDATE inventory_items SET quantity" in statement for statement in statements))
        connection.commit.assert_called_once()
        self.assertEqual(state.active_transactions, 0)

    def test_worker_bad_import_is_retained_and_does_not_terminate_poll_loop(self):
        state = WorkerState()
        state.logger = Mock()
        stop = StopLoop()
        connection = MagicMock()
        connection.__enter__.return_value = connection
        cursor = MagicMock()
        cursor.fetchone.return_value = (41, "import", '{"schema":"inventory.import.v2","body":"%%%"}', RUN_ID)
        connection.cursor.return_value.__enter__.return_value = cursor
        state._connect = Mock(return_value=connection)
        with patch("app.worker.time.sleep", side_effect=stop):
            with self.assertRaises(StopLoop):
                state._job_loop()

        connection.rollback.assert_called_once()
        self.assertFalse(any("DELETE FROM lab_jobs" in call.args[0] for call in cursor.execute.call_args_list))
        rejected = [call.kwargs for call in state.logger.write.call_args_list
                    if call.args and call.args[1] == "Import decoder rejected document"]
        self.assertEqual(rejected[0]["owner_run_id"], RUN_ID)
        self.assertEqual(rejected[0]["acknowledgement"], "pending")

    def test_worker_import_validation_is_bounded_and_rejects_bool_quantity(self):
        valid = {"records": [{"sku": "sku-a", "quantity": 3, "description": "note"}]}
        self.assertEqual(_validated_import_records(valid), [("sku-a", 3, "note")])
        with self.assertRaises(ValueError):
            _validated_import_records({"records": [{"sku": "sku-a", "quantity": True}]})
        with self.assertRaises(ValueError):
            _validated_import_records({"records": []})

    def test_import_decoder_rejects_unknown_schema(self):
        payload = json.dumps({"schema": "other.v1", "body": base64.b64encode(b"{}").decode()})
        with self.assertRaises(ValueError):
            decode_job(payload)


class StopLoop(Exception):
    pass


if __name__ == "__main__":
    unittest.main()
