import base64
import json
import threading
import unittest
from io import BytesIO
from urllib.error import HTTPError, URLError
from unittest.mock import MagicMock, Mock, patch

import psycopg
import pymysql

from app.inventory import InventoryState as PostgresInventoryState
from app.mysql_inventory import InventoryState as MysqlInventoryState
from app.orders import OrdersState, handler as orders_handler
from app.worker import WorkerState, _validated_import_records, decode_job


RUN_ID = "a" * 32


class FakeResponse:
    def __init__(self, body: bytes, status: int = 200) -> None:
        self.body = body
        self.status = status

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def read(self, limit: int = -1) -> bytes:
        return self.body if limit < 0 else self.body[:limit]


class NeverStop:
    def is_set(self) -> bool:
        return False

    def wait(self, _timeout: float | None = None) -> bool:
        return False


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

    def test_orders_injected_idempotency_collision_is_rejected_before_second_inventory_call(self):
        with patch.dict("os.environ", {"INVENTORY_URL": "http://inventory-api:8081"}):
            state = OrdersState()
        state.logger = Mock()
        state._attempt_inventory_result = Mock(return_value={
            "status": 200, "kind": "success", "upstream_status": 200,
            "retryable": False, "duration_ms": 2.0,
        })
        state.set_mode("idempotency-conflict", run_id=RUN_ID)

        first_status, _ = state.checkout("order-17")
        second_status, second = state.checkout("order-18")

        self.assertEqual(first_status, 200)
        self.assertEqual(second_status, 409)
        self.assertEqual(second["status"], "idempotency_conflict")
        self.assertEqual(state._attempt_inventory_result.call_count, 1)
        self.assertEqual(state.internal_failures["idempotency_conflict"], 1)
        events = [call.kwargs for call in state.logger.write.call_args_list
                  if call.args and call.args[1] == "Idempotency key was reused for a different order"]
        self.assertEqual(events[0]["identity_field"], "order_id")
        self.assertEqual(events[0]["comparison"], "different_order_ref")
        completion = [call.kwargs for call in state.logger.write.call_args_list
                      if call.args and call.args[1] == "Checkout request completed"][-1]
        self.assertEqual(completion["idempotency_disposition"], "conflict")
        self.assertEqual(completion["dependency_attempts"], 0)

    def test_orders_configuration_log_correlates_run_id_and_rejects_invalid_id(self):
        with patch.dict("os.environ", {"INVENTORY_URL": "http://inventory-api:8081"}):
            state = OrdersState()
        state.logger = Mock()
        state.set_mode("normal", run_id=RUN_ID)
        self.assertEqual(state.logger.write.call_args.kwargs["run_id"], RUN_ID)
        self.assertEqual(state.status()["run_id"], RUN_ID)

        handler_type = orders_handler(state)
        request = handler_type.__new__(handler_type)
        request.path = "/control/scenario"
        request.body_json = Mock(return_value={"mode": "normal", "run_id": RUN_ID})
        request.send_json = Mock()
        request.do_POST()
        self.assertEqual(request.send_json.call_args.args[1]["run_id"], RUN_ID)

        with self.assertRaises(ValueError):
            state.set_mode("normal", run_id="not-a-run-id")

    def test_orders_applied_configuration_logs_effective_timeout_attempts_and_schema(self):
        with patch.dict("os.environ", {"INVENTORY_URL": "http://inventory-api:8081"}):
            state = OrdersState()
        state.logger = Mock()

        state.set_mode("configured", settings={
            "INVENTORY_TIMEOUT_SECONDS": "0.05",
            "MAX_RETRIES": "3",
            "ORDER_EXPECTED_SCHEMA": "v2",
        }, run_id=RUN_ID)

        event = state.logger.write.call_args.kwargs
        self.assertEqual(event["dependency_timeout_seconds"], 0.05)
        self.assertEqual(event["max_dependency_attempts"], 3)
        self.assertEqual(event["expected_response_schema"], "v2")
        self.assertEqual(event["run_id"], RUN_ID)

    def test_orders_settings_must_be_an_object_and_schema_identifier_is_bounded(self):
        with patch.dict("os.environ", {"INVENTORY_URL": "http://inventory-api:8081"}):
            state = OrdersState()

        with self.assertRaisesRegex(ValueError, "must be an object"):
            state.set_mode("configured", settings=["not", "an", "object"])
        with self.assertRaisesRegex(ValueError, "short identifier"):
            state.set_mode("configured", settings={"ORDER_EXPECTED_SCHEMA": "v" * 1000})

    def test_orders_startup_rejects_unbounded_retry_or_timeout_configuration(self):
        with patch.dict("os.environ", {"INVENTORY_URL": "http://inventory-api:8081", "MAX_RETRIES": "1000"}):
            with self.assertRaisesRegex(ValueError, "between 1 and 5 attempts"):
                OrdersState()
        with patch.dict("os.environ", {"INVENTORY_URL": "http://inventory-api:8081", "INVENTORY_TIMEOUT_SECONDS": "nan"}):
            with self.assertRaisesRegex(ValueError, "between 0.01 and 30 seconds"):
                OrdersState()

    def test_orders_rejects_oversized_and_malformed_response_documents_without_leaking_body(self):
        with patch.dict("os.environ", {"INVENTORY_URL": "http://inventory-api:8081"}):
            state = OrdersState()
        state.logger = Mock()
        oversized_secret = b"s" * 300_000

        with patch("app.orders.urlopen", return_value=FakeResponse(oversized_secret)):
            result = state._attempt_inventory_result("order-17")
        self.assertEqual((result["status"], result["kind"]), (502, "contract_shape"))
        event = state.logger.write.call_args.kwargs
        self.assertEqual(event["observed_type"], "oversized_document")
        self.assertEqual(event["response_bytes"], 256_001)
        self.assertNotIn("s" * 100, str(event))

        with patch("app.orders.urlopen", return_value=FakeResponse(b"not json")):
            result = state._attempt_inventory_result("order-17")
        self.assertEqual((result["status"], result["kind"]), (502, "contract_shape"))
        self.assertEqual(state.logger.write.call_args.kwargs["observed_type"], "invalid_json")

        deeply_nested = b"[" * 10_000 + b"]" * 10_000
        with patch("app.orders.urlopen", return_value=FakeResponse(deeply_nested)):
            result = state._attempt_inventory_result("order-17")
        self.assertEqual((result["status"], result["kind"]), (502, "contract_shape"))
        self.assertEqual(state.logger.write.call_args.kwargs["error_type"], "RecursionError")

    def test_orders_rejects_non_200_success_status_and_preserves_status_attribution(self):
        with patch.dict("os.environ", {"INVENTORY_URL": "http://inventory-api:8081"}):
            state = OrdersState()
        state.logger = Mock()
        valid_v1 = json.dumps({"schema": "v1", "status": "reserved"}).encode()

        with patch("app.orders.urlopen", return_value=FakeResponse(valid_v1, status=201)):
            result = state._attempt_inventory_result("order-17")

        self.assertEqual((result["status"], result["kind"], result["upstream_status"]),
                         (502, "contract_status", 201))
        event = state.logger.write.call_args.kwargs
        self.assertEqual(event["consumer_status"], 502)
        self.assertEqual(event["upstream_status"], 201)
        self.assertEqual(event["expected_upstream_status"], 200)

    def test_orders_response_validation_handles_non_object_and_bad_nested_schema_safely(self):
        with patch.dict("os.environ", {"INVENTORY_URL": "http://inventory-api:8081"}):
            state = OrdersState()
        state.logger = Mock()

        with patch("app.orders.urlopen", return_value=FakeResponse(b'["not", "an", "object"]')):
            result = state._attempt_inventory_result("order-17")
        self.assertEqual((result["status"], result["kind"]), (502, "contract_shape"))
        self.assertEqual(state.logger.write.call_args.kwargs["observed_type"], "list")

        state.expected_schema = "v2"
        with patch("app.orders.urlopen", return_value=FakeResponse(
                b'{"schema":"v2","reservation":[]}')):
            result = state._attempt_inventory_result("order-17")
        self.assertEqual((result["status"], result["kind"]), (502, "contract_shape"))
        self.assertEqual(state.logger.write.call_args.kwargs["observed_reservation_type"], "list")
        self.assertEqual(state.logger.write.call_args.kwargs["missing_fields"], ["reservation.status"])

    def test_orders_response_validation_reports_wrong_status_and_sanitizes_bad_schema_metadata(self):
        with patch.dict("os.environ", {"INVENTORY_URL": "http://inventory-api:8081"}):
            state = OrdersState()
        state.logger = Mock()

        with patch("app.orders.urlopen", return_value=FakeResponse(
                b'{"schema":"v1","status":"not-reserved"}')):
            result = state._attempt_inventory_result("order-17")
        self.assertEqual((result["status"], result["kind"]), (502, "contract_shape"))
        self.assertEqual(state.logger.write.call_args.kwargs["validation_failure"], "unexpected_status_value")
        self.assertEqual(state.logger.write.call_args.kwargs["observed_status"], "not-reserved")
        self.assertEqual(state.logger.write.call_args.kwargs["missing_fields"], [])

        with patch("app.orders.urlopen", return_value=FakeResponse(
                b'{"schema":{"private":"not-for-logs"},"status":"reserved"}')):
            result = state._attempt_inventory_result("order-17")
        self.assertEqual((result["status"], result["kind"]), (502, "contract_version"))
        event = state.logger.write.call_args.kwargs
        self.assertEqual(event["observed_schema"], "non_string:dict")
        self.assertNotIn("not-for-logs", str(event))

    def test_orders_normal_v1_and_v2_responses_are_accepted(self):
        for schema, document in (
            ("v1", {"schema": "v1", "status": "reserved", "order_id": "private-order"}),
            ("v2", {"schema": "v2", "reservation": {"status": "reserved", "order_id": "private-order"}}),
        ):
            with self.subTest(schema=schema), patch.dict("os.environ", {
                    "INVENTORY_URL": "http://inventory-api:8081", "ORDER_EXPECTED_SCHEMA": schema}):
                state = OrdersState()
                state.logger = Mock()
                with patch("app.orders.urlopen", return_value=FakeResponse(json.dumps(document).encode())):
                    status, body = state.checkout("order-17")
                self.assertEqual(status, 200)
                self.assertEqual(body["status"], "completed")
                self.assertEqual(state.inventory_attempts, 1)
                emitted = " ".join(str(call.kwargs) for call in state.logger.write.call_args_list)
                self.assertNotIn("private-order", emitted)

    def test_orders_contract_failure_is_non_retryable_and_checkout_logs_both_statuses(self):
        with patch.dict("os.environ", {"INVENTORY_URL": "http://inventory-api:8081", "MAX_RETRIES": "3"}):
            state = OrdersState()
        state.logger = Mock()
        legacy_document = json.dumps({"result": "accepted", "reference": "private-order"}).encode()

        with patch("app.orders.urlopen", return_value=FakeResponse(legacy_document)) as urlopen:
            status, body = state.checkout("order-17")

        self.assertEqual(status, 502)
        self.assertEqual(body["status"], "dependency_contract_rejected")
        self.assertEqual(urlopen.call_count, 1)
        attempt_events = [call.kwargs for call in state.logger.write.call_args_list
                          if call.args and call.args[1] == "Inventory dependency attempt completed"]
        self.assertEqual(len(attempt_events), 1)
        self.assertEqual(attempt_events[0]["upstream_status"], 200)
        self.assertEqual(attempt_events[0]["consumer_status"], 502)
        self.assertEqual(attempt_events[0]["missing_fields"], ["status"])
        completion = [call.kwargs for call in state.logger.write.call_args_list
                      if call.args and call.args[1] == "Checkout request completed"][-1]
        self.assertEqual(completion["consumer_status"], 502)
        self.assertEqual(completion["final_upstream_status"], 200)
        self.assertEqual(completion["final_dependency_outcome"], "contract_shape")

    def test_orders_http_auth_failure_keeps_dependency_and_consumer_status_distinct(self):
        with patch.dict("os.environ", {"INVENTORY_URL": "http://inventory-api:8081"}):
            state = OrdersState()
        state.logger = Mock()
        error = HTTPError("http://inventory-api:8081/reserve", 401, "unauthorized", {}, BytesIO(b"{}"))

        with patch("app.orders.urlopen", side_effect=error):
            result = state._attempt_inventory_result("order-17")

        self.assertEqual((result["status"], result["kind"], result["upstream_status"]),
                         (503, "authorization", 401))
        event = state.logger.write.call_args.kwargs
        self.assertEqual(event["consumer_status"], 503)
        self.assertEqual(event["upstream_status"], 401)
        self.assertEqual(event["consumer_decision"], "dependency_http_error")

    def test_orders_timeout_records_late_completion_and_attempts_are_bounded(self):
        with patch.dict("os.environ", {
            "INVENTORY_URL": "http://inventory-api:8081",
            "INVENTORY_TIMEOUT_SECONDS": "0.05",
            "MAX_RETRIES": "2",
        }):
            state = OrdersState()
        state.logger = Mock()

        with patch("app.orders.urlopen", side_effect=TimeoutError), patch("app.orders.time.sleep") as sleep:
            status, _body = state.checkout("order-17")

        self.assertEqual(status, 503)
        self.assertEqual(state.inventory_attempts, 2)
        self.assertEqual(state.inventory_retries, 1)
        self.assertEqual(sleep.call_count, 1)
        attempt_events = [call.kwargs for call in state.logger.write.call_args_list
                          if call.args and call.args[1] == "Inventory dependency attempt completed"]
        self.assertEqual(len(attempt_events), 2)
        self.assertTrue(all(item["late_completion_possible"] for item in attempt_events))
        self.assertTrue(all(item["remote_cancellation_propagated"] is False for item in attempt_events))
        self.assertTrue(attempt_events[1]["prior_timed_out_attempts_may_still_be_running"])
        completion = [call.kwargs for call in state.logger.write.call_args_list
                      if call.args and call.args[1] == "Checkout request completed"][-1]
        self.assertEqual(completion["dependency_attempts"], 2)
        self.assertEqual(completion["dependency_retries"], 1)
        self.assertEqual(completion["timed_out_attempts"], 2)
        self.assertTrue(completion["late_completion_possible"])
        self.assertEqual(state.inflight, 0)

    def test_orders_snapshots_effective_settings_for_all_attempts_in_a_checkout(self):
        with patch.dict("os.environ", {"INVENTORY_URL": "http://inventory-api:8081", "MAX_RETRIES": "2"}):
            state = OrdersState()
        state.logger = Mock()
        outcomes = [
            {"status": 503, "kind": "transport", "upstream_status": None,
             "retryable": True, "duration_ms": 1.0},
            {"status": 200, "kind": "success", "upstream_status": 200,
             "retryable": False, "duration_ms": 1.0},
        ]

        def attempt(_order_id, _request_id, _attempt, config, _prior_timed_out_attempts):
            if len(outcomes) == 2:
                state.set_mode("configured", settings={
                    "INVENTORY_URL": "http://changed-inventory:9090",
                    "INVENTORY_TIMEOUT_SECONDS": "0.2",
                })
            return outcomes.pop(0)

        state._attempt_inventory_result = Mock(side_effect=attempt)
        with patch("app.orders.time.sleep"):
            status, _body = state.checkout("order-17")

        self.assertEqual(status, 200)
        first_config = state._attempt_inventory_result.call_args_list[0].args[3]
        second_config = state._attempt_inventory_result.call_args_list[1].args[3]
        self.assertEqual(first_config, second_config)
        self.assertEqual(second_config["inventory_url"], "http://inventory-api:8081")
        self.assertEqual(second_config["timeout"], 0.45)

    def test_orders_waits_for_same_key_inflight_result_and_replays_without_second_call(self):
        with patch.dict("os.environ", {"INVENTORY_URL": "http://inventory-api:8081"}):
            state = OrdersState()
        state.logger = Mock()
        entered = threading.Event()
        release = threading.Event()
        outcomes = []

        def slow_success(*_args):
            entered.set()
            self.assertTrue(release.wait(2))
            return {"status": 200, "kind": "success", "upstream_status": 200,
                    "retryable": False, "duration_ms": 2.0}

        state._attempt_inventory_result = Mock(side_effect=slow_success)
        first = threading.Thread(target=lambda: outcomes.append(state.checkout("order-17", "key-17")))
        second = threading.Thread(target=lambda: outcomes.append(state.checkout("order-17", "key-17")))
        first.start()
        self.assertTrue(entered.wait(1))
        second.start()
        self.assertFalse(outcomes)
        release.set()
        first.join(2)
        second.join(2)

        self.assertFalse(first.is_alive())
        self.assertFalse(second.is_alive())
        self.assertEqual(len(outcomes), 2)
        self.assertEqual([item[0] for item in outcomes], [200, 200])
        self.assertEqual(sum(bool(item[1].get("replayed")) for item in outcomes), 1)
        self.assertEqual(state._attempt_inventory_result.call_count, 1)

    def test_orders_idempotency_cache_never_exceeds_bound_when_all_entries_are_active(self):
        with patch.dict("os.environ", {"INVENTORY_URL": "http://inventory-api:8081"}):
            state = OrdersState()
        state.logger = Mock()
        entered = threading.Event()
        release = threading.Event()
        first_outcome = []

        def slow_success(*_args):
            entered.set()
            self.assertTrue(release.wait(2))
            return {"status": 200, "kind": "success", "upstream_status": 200,
                    "retryable": False, "duration_ms": 2.0}

        state._attempt_inventory_result = Mock(side_effect=slow_success)
        with patch("app.orders.MAX_IDEMPOTENCY_ENTRIES", 1):
            first = threading.Thread(target=lambda: first_outcome.append(state.checkout("order-17", "key-17")))
            first.start()
            self.assertTrue(entered.wait(1))
            status, body = state.checkout("order-18", "key-18")
            self.assertEqual(status, 503)
            self.assertEqual(body["status"], "idempotency_store_busy")
            self.assertEqual(state._attempt_inventory_result.call_count, 1)
            release.set()
            first.join(2)

        self.assertFalse(first.is_alive())
        self.assertEqual(first_outcome[0][0], 200)
        self.assertEqual(state.internal_failures["idempotency_capacity"], 1)
        self.assertLessEqual(len(state._idempotency), 1)

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
            state.set_failure_mode("response-contract", run_id=RUN_ID)
        event = state.logger.write.call_args.kwargs
        self.assertEqual(event["run_id"], RUN_ID)
        self.assertNotIn("mode", event)
        self.assertNotIn("scenario", event)
        self.assertEqual(state.status()["run_id"], RUN_ID)

        with patch("app.mysql_inventory.threading.Thread"):
            state.set_failure_mode("normal", run_id=RUN_ID)
        self.assertEqual(state.status()["failure_mode"], "normal")
        self.assertEqual(state.status()["run_id"], RUN_ID)

    def test_mysql_configuration_revision_tracks_key_response_and_query_settings(self):
        state = MysqlInventoryState()
        state.logger = Mock()
        revisions = {state.status()["configuration_revision"]}

        with patch("app.mysql_inventory.threading.Thread"):
            for key, value in (
                    ("INVENTORY_ACCEPTED_KEY_ID", "checkout-key-v2"),
                    ("INVENTORY_RESPONSE_SCHEMA", "v2"),
                    ("INVENTORY_QUERY_REVISION", "v2")):
                settings = {
                    "INVENTORY_ACCEPTED_KEY_ID": "checkout-key-v1",
                    "INVENTORY_RESPONSE_SCHEMA": "v1",
                    "INVENTORY_QUERY_REVISION": "v1",
                    key: value,
                }
                if key == "INVENTORY_QUERY_REVISION":
                    state._require_reserved_quantity_column_absent = Mock()
                state.set_failure_mode("configured", settings=settings, run_id=RUN_ID)
                revisions.add(state.status()["configuration_revision"])

        self.assertEqual(len(revisions), 4)
        event = state.logger.write.call_args.kwargs
        self.assertEqual(event["configuration_revision"], state.status()["configuration_revision"])
        self.assertEqual(event["run_id"], RUN_ID)
        self.assertNotIn("accepted_key_id", event)

    def test_mysql_response_contract_scenario_reaches_checkout_as_a_contract_failure(self):
        inventory = MysqlInventoryState()
        inventory.logger = Mock()
        inventory.failure_mode = "response-contract"
        cursor = MagicMock()
        cursor.rowcount = 1
        inventory.connect = Mock(return_value=mysql_connection(cursor))
        upstream_status, upstream_body = inventory.reserve("order-17", "checkout-key-v1")
        self.assertEqual(upstream_status, 200)

        with patch.dict("os.environ", {"INVENTORY_URL": "http://inventory-api:8081"}):
            orders = OrdersState()
        orders.logger = Mock()
        response = FakeResponse(json.dumps(upstream_body).encode("utf-8"), status=upstream_status)
        with patch("app.orders.urlopen", return_value=response) as urlopen:
            status, body = orders.checkout("order-17")

        self.assertEqual(status, 502)
        self.assertEqual(body["status"], "dependency_contract_rejected")
        self.assertEqual(urlopen.call_count, 1)
        attempt = [call.kwargs for call in orders.logger.write.call_args_list
                   if call.args and call.args[1] == "Inventory dependency attempt completed"][0]
        self.assertEqual(attempt["upstream_status"], 200)
        self.assertEqual(attempt["outcome"], "contract_shape")
        self.assertEqual(attempt["missing_fields"], ["status"])

    def test_mysql_token_collision_is_observable_and_does_not_decrement_for_second_owner(self):
        state = MysqlInventoryState()
        state.logger = Mock()
        state.logger.count = 0
        with patch("app.mysql_inventory.threading.Thread"):
            state.set_failure_mode("token-collision", run_id=RUN_ID)
        first_cursor = MagicMock()
        first_cursor.rowcount = 1
        state.connect = Mock(return_value=mysql_connection(first_cursor))
        self.assertEqual(state.reserve("order-17", "checkout-key-v1")[0], 200)

        duplicate_cursor = MagicMock()
        duplicate_cursor.execute.side_effect = [None, pymysql.err.IntegrityError(1062, "duplicate key")]
        owner_cursor = MagicMock()
        owner_cursor.fetchone.return_value = ("order-17",)
        state.connect = Mock(side_effect=[mysql_connection(duplicate_cursor), mysql_connection(owner_cursor)])
        status, response = state.reserve("order-18", "checkout-key-v1")

        self.assertEqual(status, 409)
        self.assertEqual(response["status"], "reservation_conflict")
        self.assertFalse(any("UPDATE inventory_items" in call.args[0]
                             for call in duplicate_cursor.execute.call_args_list))
        self.assertIn('inventory_transaction_failures_total{kind="constraint"} 1', state.metrics())
        event = [call.kwargs for call in state.logger.write.call_args_list
                 if call.args and call.args[1] == "Reservation token belongs to a different request"][0]
        self.assertEqual(event["ownership_match"], False)
        self.assertEqual(event["mysql_error_code"], 1062)

    def test_mysql_deadlock_victim_has_pair_and_lock_order_evidence(self):
        state = MysqlInventoryState()
        state.logger = Mock()
        state.logger.count = 0
        cursor = MagicMock()
        cursor.fetchone.return_value = (100,)
        cursor.execute.side_effect = [None, None, pymysql.err.OperationalError(1213, "deadlock")]
        state.connect = Mock(return_value=mysql_connection(cursor))
        barrier = Mock()

        state._deadlock_transaction("sku-red-widget", "sku-blue-widget", barrier, "pair-1")

        barrier.wait.assert_called_once_with(timeout=2)
        self.assertIn('inventory_transaction_failures_total{kind="deadlock"} 1', state.metrics())
        event = state.logger.write.call_args.kwargs
        self.assertEqual(event["pair_id"], "pair-1")
        self.assertEqual(event["mysql_error_code"], 1213)
        self.assertEqual(event["first_sku"], "sku-red-widget")
        self.assertEqual(event["second_sku"], "sku-blue-widget")

    def test_mysql_capacity_storm_reports_bounded_headroom_from_sampled_capacity(self):
        state = MysqlInventoryState()
        state.logger = Mock()
        state.logger.count = 0
        state.configured_max_connections = 28
        state.server_max_connections = 40

        def connect_with_counter():
            state.client_sessions_active += 1
            return mysql_connection(MagicMock())

        state.connect = Mock(side_effect=connect_with_counter)
        with patch("app.mysql_inventory.time.sleep"):
            state._connection_storm(NeverStop())

        self.assertEqual(len(state._held_connections), 34)
        self.assertEqual(state.client_sessions_active, 34)
        pressure = [call.kwargs for call in state.logger.write.call_args_list
                    if call.args and call.args[1] == "Inventory session pressure bounded with server headroom"][0]
        self.assertEqual(pressure["observed_capacity"], 40)
        self.assertEqual(pressure["checked_out_target"], 34)
        self.assertEqual(pressure["reserved_connections"], 6)
        self.assertIn("inventory_mysql_client_sessions_active 34", state.metrics())
        state._release_connections()
        self.assertEqual(state.client_sessions_active, 0)

    def test_mysql_downstream_latency_is_applied_before_database_success(self):
        state = MysqlInventoryState()
        state.logger = Mock()
        with patch("app.mysql_inventory.threading.Thread"):
            state.set_failure_mode("downstream-latency")
        cursor = MagicMock()
        cursor.rowcount = 1
        state.connect = Mock(return_value=mysql_connection(cursor))

        status, _response = state.reserve("order-17", "checkout-key-v1")

        self.assertEqual(status, 200)
        self.assertGreaterEqual(state.logger.write.call_args.kwargs["duration_ms"], 300)

    def test_mysql_query_revision_skew_counts_missing_column_failure(self):
        state = MysqlInventoryState()
        state.logger = Mock()
        state.logger.count = 0
        with patch.object(state, "_require_reserved_quantity_column_absent"), \
                patch("app.mysql_inventory.threading.Thread"):
            state.set_failure_mode("configured", settings={
                "INVENTORY_QUERY_REVISION": "v2",
            })
        cursor = MagicMock()
        cursor.execute.side_effect = [None, None, pymysql.err.OperationalError(1054, "unknown column")]
        state.connect = Mock(return_value=mysql_connection(cursor))

        status, _response = state.reserve("order-17", "checkout-key-v1")

        self.assertEqual(status, 503)
        self.assertIn('inventory_database_failures_total{kind="query"} 1', state.metrics())
        failure = [call.kwargs for call in state.logger.write.call_args_list
                   if call.args and call.args[1] == "Inventory MySQL operation failed"][0]
        self.assertEqual(failure["query_revision"], "v2")
        self.assertEqual(failure["mysql_error_code"], 1054)

    def test_mysql_key_and_response_schema_skew_have_distinct_consumer_outcomes(self):
        state = MysqlInventoryState()
        state.logger = Mock()
        with patch("app.mysql_inventory.threading.Thread"):
            state.set_failure_mode("configured", settings={
                "INVENTORY_ACCEPTED_KEY_ID": "checkout-key-v2",
                "INVENTORY_RESPONSE_SCHEMA": "v2",
            })

        state.connect = Mock()
        status, _response = state.reserve("order-17", "checkout-key-v1")
        self.assertEqual(status, 401)
        state.connect.assert_not_called()
        key_rejection = [call.kwargs for call in state.logger.write.call_args_list
                         if call.args and call.args[1] == "Reservation request key ID was not accepted"][0]
        self.assertEqual(key_rejection["presented_key_id"], "checkout-key-v1")
        self.assertEqual(key_rejection["accepted_key_id"], "checkout-key-v2")

        cursor = MagicMock()
        cursor.rowcount = 1
        state.connect = Mock(return_value=mysql_connection(cursor))
        upstream_status, upstream_body = state.reserve("order-18", "checkout-key-v2")
        self.assertEqual(upstream_status, 200)
        with patch.dict("os.environ", {"INVENTORY_URL": "http://inventory-api:8081"}):
            orders = OrdersState()
        orders.logger = Mock()
        with patch("app.orders.urlopen", return_value=FakeResponse(
                json.dumps(upstream_body).encode("utf-8"), status=upstream_status)):
            checkout_status, _body = orders.checkout("order-18")

        self.assertEqual(checkout_status, 502)
        mismatch = [call.kwargs for call in orders.logger.write.call_args_list
                    if call.args and call.args[1] == "Inventory dependency attempt completed"][0]
        self.assertEqual(mismatch["outcome"], "contract_version")
        self.assertEqual(mismatch["expected_schema"], "v1")
        self.assertEqual(mismatch["observed_schema"], "v2")

    def test_orders_misrouted_dependency_is_transport_failure_with_effective_route(self):
        with patch.dict("os.environ", {"INVENTORY_URL": "http://inventory-api:8081"}):
            state = OrdersState()
        state.logger = Mock()
        state.set_mode("configured", settings={"INVENTORY_URL": "http://inventory-api:8099"})

        with patch("app.orders.urlopen", side_effect=URLError(ConnectionRefusedError("refused"))):
            result = state._attempt_inventory_result("order-17")

        self.assertEqual((result["status"], result["kind"]), (503, "transport"))
        event = state.logger.write.call_args.kwargs
        self.assertEqual(event["dependency_host"], "inventory-api")
        self.assertEqual(event["dependency_port"], 8099)

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
