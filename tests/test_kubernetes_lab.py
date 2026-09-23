import base64
import io
import json
import threading
import time
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from unittest.mock import Mock, MagicMock, call, patch

import pymysql
import yaml

from app.common import JsonLogger
from app.control import EXTERNAL_PROBE_OWNER, ControlState, SCENARIOS
from app.scenario_catalog import DEFAULT_SCENARIO_CONFIG, DISCOVERY_SCENARIOS, public_scenarios
from app.mysql_inventory import InventoryState
from app.orders import OrdersState
from app.safety import lease_seconds, memory_snapshot
from app.worker import CPU_MIGRATION_BATCH_SIZE, CPU_MIGRATION_ROUNDS, MAX_BUFFERED_EXPORT_BYTES, WorkerState, decode_job


ROOT = Path(__file__).resolve().parents[1]


class KubernetesLabTests(unittest.TestCase):
    def test_controller_memory_source_follows_its_scheduled_node(self):
        documents = [item for item in yaml.safe_load_all((ROOT / "deploy/kubernetes/applications.yaml").read_text()) if item]
        control = next(item for item in documents if item.get("kind") == "Deployment"
                       and item.get("metadata", {}).get("name") == "lab-control")
        env = {item["name"]: item for item in control["spec"]["template"]["spec"]["containers"][0]["env"]}
        self.assertEqual(env["NODE_IP"]["valueFrom"]["fieldRef"]["fieldPath"], "status.hostIP")
        self.assertEqual(env["NODE_EXPORTER_URL"]["value"], "http://$(NODE_IP):9100/metrics")
        self.assertNotIn("192.168.0.", env["NODE_EXPORTER_URL"]["value"])

    def test_fifteen_balanced_execution_contracts(self):
        self.assertEqual(len(SCENARIOS), 15)
        self.assertEqual(
            {group: sum(item["evidence_group"] == group for item in SCENARIOS.values())
             for group in ("logs", "metrics", "configuration")},
            {"logs": 5, "metrics": 5, "configuration": 5},
        )
        self.assertTrue(all(item["expected_alert"].startswith("Lab") for item in SCENARIOS.values()))
        self.assertEqual(DEFAULT_SCENARIO_CONFIG["INVENTORY_QUERY_REVISION"], "v1")

    def test_all_scenarios_are_in_one_operator_catalog_while_benchmark_membership_stays_stable(self):
        self.assertEqual(set(DISCOVERY_SCENARIOS), {"metrics-service-label-drift", "mysql-exporter-scrape-path"})
        self.assertEqual(len(SCENARIOS), 15)
        self.assertEqual(DISCOVERY_SCENARIOS["metrics-service-label-drift"]["expected_alert"], "LabApplicationMetricsDiscoveryMissing")
        catalog = public_scenarios()
        self.assertEqual(len(catalog), 17)
        self.assertTrue(catalog["mysql-exporter-scrape-path"]["runner_only"])
        for item in catalog.values():
            self.assertNotIn("expected_alert", item)
            self.assertNotIn("actions", item)
            self.assertNotIn("config", item)

    def test_external_runner_only_scenario_cannot_be_started_through_controller(self):
        state = ControlState()
        with self.assertRaisesRegex(ValueError, "external runner"):
            state.start("mysql-exporter-scrape-path")

    def test_controller_claim_refuses_external_screenshot_owner_before_mutation(self):
        state = ControlState()
        state._kubernetes_get = Mock(return_value={"metadata": {"annotations": {EXTERNAL_PROBE_OWNER: "a" * 32}}})
        state._kubernetes_patch = Mock()
        with self.assertRaisesRegex(ValueError, "external Prometheus screenshot run"):
            state._claim_run({"run_id": "b" * 32, "targets": [], "expires_at": 1}, {})
        state._kubernetes_patch.assert_not_called()

    def test_external_screenshot_lock_is_visible_to_the_control_status(self):
        state = ControlState()
        state._health = Mock(return_value={"reachable": True})
        state._kubernetes_get = Mock(return_value={"metadata": {"annotations": {EXTERNAL_PROBE_OWNER: "a" * 32}}})
        self.assertEqual(state.status()["external_probe"], {"available": True, "active": True, "owner": "a" * 32})

    def test_discovery_run_changes_only_the_service_selector_and_recovery_restores_it(self):
        state = ControlState()
        state.active = {"run_id": "discovery-run"}
        state.logger = Mock()
        state._patch_scenario_config = Mock()
        state._patch_metrics_service_label = Mock()
        state._post = Mock(return_value={})

        result = state._start("metrics-service-label-drift", 120)

        self.assertEqual(result["results"], [{"target": "metrics-service", "status": "label updated"}])
        state._patch_scenario_config.assert_not_called()
        state._patch_metrics_service_label.assert_called_once_with("ture")
        state._post.assert_not_called()

        state._patch_metrics_service_label.reset_mock()
        with patch("app.control.pymysql.connect"):
            recovery = state._recover("test")
        self.assertTrue(recovery["ok"])
        state._patch_scenario_config.assert_not_called()
        state._patch_metrics_service_label.assert_not_called()

    def test_real_decoder_accepts_valid_document_and_raises_on_invalid_encoding(self):
        value = {"sku": "example", "quantity": 4}
        payload = json.dumps({"body": base64.b64encode(json.dumps(value).encode()).decode()})
        self.assertEqual(decode_job(payload), value)
        with self.assertRaises(ValueError):
            decode_job('{"body":"not-valid-base64!"}')

    def test_memory_source_is_kernel_available_not_allocatable(self):
        response = MagicMock()
        response.__enter__.return_value.read.return_value = b"node_memory_MemAvailable_bytes 1500000000\nnode_memory_MemTotal_bytes 7000000000\n"
        environment = {"NODE_IP": "192.0.2.10", "NODE_EXPORTER_URL": "http://192.0.2.10:9100/metrics"}
        with patch.dict("os.environ", environment), patch("app.safety.urlopen", return_value=response):
            sample = memory_snapshot()
            self.assertEqual(sample["available_bytes"], 1500000000)
            self.assertTrue(sample["node_identity_verified"])
            self.assertEqual(sample["source_host"], "192.0.2.10")
            self.assertGreater(sample["sampled_at"], 0)
        response.__enter__.return_value.read.return_value = b"node_memory_MemTotal_bytes 7000000000\n"
        with patch.dict("os.environ", environment), patch("app.safety.urlopen", return_value=response), self.assertRaises(KeyError):
            memory_snapshot()

    def test_memory_source_must_match_the_scheduled_controller_node(self):
        with patch.dict("os.environ", {"NODE_IP": "192.0.2.10", "NODE_EXPORTER_URL": "http://192.0.2.11:9100/metrics"}), \
                patch("app.safety.urlopen") as request, self.assertRaisesRegex(ValueError, "does not match"):
            memory_snapshot()
        request.assert_not_called()

    def test_start_fails_closed_without_headroom_or_measurement(self):
        state = ControlState()
        state._start = Mock()
        with patch("app.control.memory_snapshot", return_value={"available_bytes": 900_000_000}), self.assertRaises(ValueError):
            state.start("memory-leak")
        with patch("app.control.memory_snapshot", side_effect=OSError), self.assertRaises(OSError):
            state.start("memory-leak")
        state._start.assert_not_called()
        self.assertIsNone(state.active)

        with patch("app.control.memory_snapshot", return_value={
            "available_bytes": 2_000_000_000, "node_identity_verified": False,
        }), self.assertRaisesRegex(ValueError, "node-specific memory source"):
            state.start("memory-leak")
        state._start.assert_not_called()

    def test_only_one_bounded_run_can_start(self):
        state = ControlState()
        state._start = Mock(return_value={"ok": True})
        state._health = Mock(return_value={"reachable": True})
        with patch("app.control.memory_snapshot", return_value={"available_bytes": 2_000_000_000, "node_identity_verified": True}):
            result = state.start("schema-drift", 120)
            self.assertEqual(result["run"]["duration_seconds"], 120)
            with self.assertRaises(ValueError):
                state.start("memory-leak")
        self.assertEqual(state._start.call_count, 1)
        for value in (0, 29, 301, 100000):
            with self.assertRaises(ValueError):
                lease_seconds(value)

    def test_watchdog_recovers_on_low_memory_expiry_and_missing_measurement(self):
        for memory, expiry, reason in ((500_000_000, 99999999999, "low_host_memory"), (2_000_000_000, 0, "lease_expired"), (None, 99999999999, "memory_measurement_unavailable")):
            state = ControlState()
            state.active = {"expires_at": expiry, "status": "running", "minimum_available_bytes": 2_000_000_000}
            state.recover = Mock()
            reading = {"available_bytes": memory, "node_identity_verified": True}
            with patch("app.control.memory_snapshot", side_effect=OSError if memory is None else None, return_value=reading), patch("app.control.time.sleep", side_effect=StopIteration):
                with self.assertRaises(StopIteration):
                    state.watchdog()
            state.recover.assert_called_once_with(reason)

    def test_logger_uses_kubernetes_identity(self):
        out = io.StringIO()
        with patch.dict("os.environ", {"POD_NAMESPACE":"real-ns", "LOG_NAMESPACE":"stale", "POD_NAME":"worker-abc"}), redirect_stdout(out):
            JsonLogger("worker").write("INFO", "Page complete")
        event = json.loads(out.getvalue())
        self.assertEqual((event["namespace"], event["pod"]), ("real-ns", "worker-abc"))

    def test_buffer_growth_encodes_real_pages_and_respects_stop(self):
        state = WorkerState()
        stop = threading.Event()
        state.logger = Mock()
        state.logger.write.side_effect = lambda *args, **kwargs: stop.set()
        state._leak_memory(stop)
        self.assertEqual(len(state._memory), 1)
        self.assertEqual(state.allocated_bytes, len(state._memory[0]))
        self.assertIn(b'"sku"', state._memory[0])
        state.set_mode("normal")
        self.assertEqual(state.allocated_bytes, 0)
        self.assertEqual(state._memory, [])

    def test_buffer_growth_stops_before_crossing_its_safety_cap(self):
        class NeverStopped:
            @staticmethod
            def is_set():
                return False

            @staticmethod
            def wait(_seconds):
                return False

        state = WorkerState()
        state.logger = Mock()
        state._encode_export_page = Mock(side_effect=[(1, b"1234"), (2, b"5678")])
        with patch("app.worker.MAX_BUFFERED_EXPORT_BYTES", 6), patch(
            "app.worker.MAX_BUFFERED_EXPORT_PAGES", 10
        ):
            state._leak_memory(NeverStopped())

        self.assertEqual(state._memory, [bytearray(b"1234")])
        self.assertEqual(state.allocated_bytes, 4)
        self.assertEqual(state.export_pages_completed, 1)
        self.assertEqual(state.logger.write.call_args.args[1], "Export buffer reached its configured safety bound")
        self.assertEqual(state.logger.write.call_args.kwargs["maximum_buffered_bytes"], 6)

    def test_cpu_migration_is_finite_and_returns_to_normal_export_work(self):
        state = WorkerState()
        state.logger = Mock()
        state.mode = "cpu-saturation"
        state.migration_backlog = 1
        state._migration_batch_id = "batch-1"
        stop = threading.Event()

        with patch("app.worker._migrate_credential_record") as migrate, \
                patch.object(state, "_export_loop", side_effect=lambda event: event.set()) as normal_work:
            state._credential_migration_loop(stop)

        migrate.assert_called_once_with("batch-1", CPU_MIGRATION_BATCH_SIZE, CPU_MIGRATION_ROUNDS)
        normal_work.assert_called_once_with(stop)
        self.assertEqual(state.mode, "normal")
        self.assertEqual(state.migration_backlog, 0)
        self.assertIsNone(state._migration_batch_id)
        self.assertEqual(state.migration_records_completed, 1)
        self.assertEqual(state._expires_at, 0)
        self.assertTrue(any(call.args[1] == "Credential migration batch completed"
                            for call in state.logger.write.call_args_list))

    def test_cpu_migration_observes_cancellation_between_bounded_records(self):
        state = WorkerState()
        state.logger = Mock()
        state.mode = "cpu-saturation"
        state.migration_backlog = 3
        state._migration_batch_id = "batch-2"
        stop = threading.Event()

        def finish_one(*_args):
            stop.set()

        with patch("app.worker._migrate_credential_record", side_effect=finish_one):
            state._credential_migration_loop(stop)

        self.assertEqual(state.migration_records_completed, 1)
        self.assertEqual(state.migration_backlog, 2)
        self.assertEqual(state.mode, "cpu-saturation")

    def test_mysql_session_pressure_reserves_capacity_for_source_health(self):
        self.assertEqual(InventoryState._connection_saturation_target(40), 34)
        self.assertGreater(34 / 40, 0.80)
        for maximum in (0, 6, 9):
            with self.assertRaises(ValueError):
                InventoryState._connection_saturation_target(maximum)

    def test_mysql_connection_mode_rejects_impossible_ceiling_before_changing_state(self):
        state = InventoryState()
        state.server_max_connections = 6
        state._connection_storm = Mock()
        with self.assertRaisesRegex(ValueError, "headroom"):
            state.set_failure_mode("connection-saturation")
        self.assertEqual(state.failure_mode, "normal")
        state._connection_storm.assert_not_called()

    def test_schema_drift_requires_the_column_to_be_absent_before_intervention(self):
        state = InventoryState()
        connection = MagicMock()
        connection.__enter__.return_value = connection
        cursor = connection.cursor.return_value.__enter__.return_value
        cursor.fetchone.return_value = ("reserved_quantity", "int", "NO", "", "0", "")
        with patch.object(state, "_managed_connection", return_value=connection) as connect:
            with self.assertRaisesRegex(ValueError, "already exists"):
                state._require_reserved_quantity_column_absent()
        connect.assert_called_once_with()
        cursor.execute.assert_called_once_with("SHOW COLUMNS FROM inventory_items LIKE 'reserved_quantity'")

    def test_schema_drift_precondition_runs_before_runtime_mode_is_applied(self):
        state = InventoryState()
        state._require_reserved_quantity_column_absent = Mock(side_effect=ValueError("column already exists"))
        with self.assertRaisesRegex(ValueError, "already exists"):
            state.set_failure_mode("configured", settings={"INVENTORY_QUERY_REVISION": "v2"})
        self.assertEqual(state.failure_mode, "normal")
        state._require_reserved_quantity_column_absent.assert_called_once_with()

    def test_invalid_import_is_rolled_back_and_counted_for_retry_without_crashing_worker(self):
        state = WorkerState()
        state.logger = Mock()
        connection = MagicMock()
        connection.__enter__.return_value = connection
        cursor = connection.cursor.return_value.__enter__.return_value
        cursor.fetchone.return_value = (42, "import", '{"body":"not-valid-base64!"}', "run-owner")
        state._connect = Mock(return_value=connection)

        with patch("app.worker.time.sleep", side_effect=KeyboardInterrupt), self.assertRaises(KeyboardInterrupt):
            state._job_loop()

        connection.rollback.assert_called_once()
        connection.commit.assert_not_called()
        self.assertEqual(state.status()["poison_retries"], 1)
        statements = [call.args[0] for call in cursor.execute.call_args_list]
        self.assertEqual(len(statements), 1)
        self.assertIn("SELECT id, kind, payload, owner_run_id", statements[0])
        self.assertEqual(state.logger.write.call_args_list[-1].args[1], "Import decoder rejected document")
        metrics = state.metrics()
        self.assertIn("lab_worker_poison_retries_total", metrics)
        self.assertIn("lab_worker_poison_retries_total 1", metrics)

    def test_schema_drift_executes_incompatible_query_and_records_server_code(self):
        state = InventoryState()
        state.query_revision = "v2"
        state.logger = Mock()
        connection = MagicMock()
        cursor = connection.cursor.return_value.__enter__.return_value
        cursor.execute.side_effect = [None, None, pymysql.err.OperationalError(1054, "Unknown column 'reserved_quantity'")]
        state.connect = Mock(return_value=connection)
        status, _ = state.reserve("order-1", "checkout-key-v1")
        self.assertEqual(status, 503)
        self.assertEqual(state.db_failures["query"], 1)
        self.assertIn("reserved_quantity", cursor.execute.call_args.args[0])
        self.assertEqual(state.logger.write.call_args.kwargs["mysql_error_code"], 1054)
        self.assertEqual(state.active_transactions, 0)

    def test_contract_rejection_preserves_upstream_status_and_consumer_decision(self):
        response = MagicMock(status=200)
        response.__enter__.return_value = response
        response.read.return_value = json.dumps({"result": "accepted", "reference": "order-1"}).encode()
        with patch.dict("os.environ", {"INVENTORY_URL": "http://inventory-api:8081"}), patch(
            "app.orders.urlopen", return_value=response
        ):
            state = OrdersState()
            state.logger = Mock()
            status = state._attempt_inventory("order-1")
        self.assertEqual(status, 502)
        event = state.logger.write.call_args.kwargs
        self.assertEqual(event["upstream_status"], 200)
        self.assertEqual(event["consumer_decision"], "reject_as_bad_gateway")
        self.assertEqual(event["observed_fields"], ["reference", "result"])

    def test_checkout_latency_metrics_expose_sample_count_and_latest_observation_time(self):
        with patch.dict("os.environ", {"INVENTORY_URL": "http://inventory-api:8081"}):
            state = OrdersState()
        now_monotonic = time.monotonic()
        now_wall = time.time()
        state.latencies = [(now_monotonic, now_wall, 0.35)]
        metrics = state.metrics()
        self.assertIn("orders_checkout_latency_sample_count 1", metrics)
        self.assertIn(f"orders_checkout_latency_latest_sample_timestamp_seconds {now_wall}", metrics)

        state.latencies = [(now_monotonic - 301, now_wall - 301, 0.35)]
        metrics = state.metrics()
        self.assertIn("orders_checkout_latency_sample_count 0", metrics)
        self.assertIn("orders_checkout_latency_latest_sample_timestamp_seconds 0", metrics)

    def test_stock_reconciliation_holds_real_row_locks_then_rolls_back_on_cancel(self):
        state = InventoryState()
        state.logger = Mock()
        stop = threading.Event()
        state.logger.write.side_effect = lambda *args, **kwargs: stop.set()
        connection = MagicMock()
        cursor = connection.cursor.return_value.__enter__.return_value
        cursor.fetchone.side_effect = [(10,), (12,)]
        state.connect = Mock(return_value=connection)
        state._reconcile_stock(stop)
        statements = [call.args[0] for call in cursor.execute.call_args_list]
        self.assertTrue(any("FOR UPDATE" in statement for statement in statements))
        cursor.executemany.assert_not_called()
        connection.rollback.assert_called_once()

    def test_alerts_report_symptoms_without_injected_answers(self):
        objects = list(yaml.safe_load_all((ROOT / "deploy/kubernetes/observability.yaml").read_text()))
        rules = [rule for obj in objects if obj["kind"] == "PrometheusRule" for group in obj["spec"]["groups"] for rule in group["rules"]]
        self.assertLessEqual({scenario["expected_alert"] for scenario in SCENARIOS.values()},
                             {rule["alert"] for rule in rules})
        annotations = json.dumps([rule["annotations"] for rule in rules]).lower()
        for answer in ("poison", "buffered", "schema mismatch", "pool is retaining", "pbkdf2"):
            self.assertNotIn(answer, annotations)
        memory_exit = next(rule for rule in rules if rule["alert"] == "LabWorkerOOMKilled")
        self.assertIn("kube_pod_container_status_last_terminated_reason", memory_exit["expr"])
        self.assertIn('reason="OOMKilled"', memory_exit["expr"])
        self.assertIn("kube_pod_container_status_last_terminated_timestamp", memory_exit["expr"])
        self.assertNotIn("last_terminated_exitcode", memory_exit["expr"])
        self.assertNotIn("restarts_total", memory_exit["expr"])
        buffer_pressure = next(rule for rule in rules if rule["alert"] == "LabWorkerBufferPressure")
        self.assertIn("lab_worker_allocated_bytes", buffer_pressure["expr"])
        self.assertIn("83886080", buffer_pressure["expr"])
        self.assertEqual(buffer_pressure["labels"]["signal_class"], "PM")
        self.assertLess(80 * 1024 * 1024, MAX_BUFFERED_EXPORT_BYTES)
        poison_retries = next(rule for rule in rules if rule["alert"] == "LabWorkerPoisonRetries")
        self.assertIn("lab_worker_poison_retries_total", poison_retries["expr"])
        self.assertIn("increase(", poison_retries["expr"])
        self.assertNotIn("base64", json.dumps(poison_retries["annotations"]).lower())
        connections = next(rule for rule in rules if rule["alert"] == "LabMySQLConnectionsSaturated")
        self.assertIn("inventory_mysql_client_sessions_active", connections["expr"])
        self.assertIn("inventory_mysql_server_max_connections", connections["expr"])
        self.assertIn("inventory_mysql_sample_timestamp_seconds", connections["expr"])
        self.assertIn('up{namespace="fcapsule-lab",service="inventory-api"}', connections["expr"])
        self.assertEqual(connections["labels"]["service"], "inventory-api")
        checkout_latency = next(rule for rule in rules if rule["alert"] == "LabCheckoutLatencyHigh")
        self.assertIn("orders_checkout_latency_sample_count", checkout_latency["expr"])
        self.assertIn("orders_checkout_latency_latest_sample_timestamp_seconds", checkout_latency["expr"])
        self.assertIn('up{namespace="fcapsule-lab",service="orders-api"}', checkout_latency["expr"])
        self.assertTrue(all(rule["for"] for rule in rules))

    def test_discovery_rule_and_service_monitor_use_the_real_service_label(self):
        objects = list(yaml.safe_load_all((ROOT / "deploy/kubernetes/observability.yaml").read_text()))
        monitor = next(item for item in objects if item["kind"] == "ServiceMonitor" and item["metadata"]["name"] == "fcapsule-lab-applications")
        self.assertEqual(monitor["spec"]["selector"]["matchLabels"], {"fcapsule.io/app-metrics": "true"})
        rules = [rule for obj in objects if obj["kind"] == "PrometheusRule" for group in obj["spec"]["groups"] for rule in group["rules"]]
        discovery = next(rule for rule in rules if rule["alert"] == "LabApplicationMetricsDiscoveryMissing")
        self.assertIn("absent_over_time", discovery["expr"])
        self.assertIn('service="orders-api"', discovery["expr"])


if __name__ == "__main__":
    unittest.main()
