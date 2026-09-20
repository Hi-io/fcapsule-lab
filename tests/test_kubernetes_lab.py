import base64
import io
import json
import threading
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from unittest.mock import Mock, MagicMock, patch

import pymysql
import yaml

from app.common import JsonLogger
from app.control import ControlState, SCENARIOS
from app.scenario_catalog import DEFAULT_SCENARIO_CONFIG
from app.mysql_inventory import InventoryState
from app.safety import lease_seconds, memory_snapshot
from app.worker import WorkerState, decode_job


ROOT = Path(__file__).resolve().parents[1]


class KubernetesLabTests(unittest.TestCase):
    def test_fifteen_balanced_execution_contracts(self):
        self.assertEqual(len(SCENARIOS), 15)
        self.assertEqual(
            {group: sum(item["evidence_group"] == group for item in SCENARIOS.values())
             for group in ("logs", "metrics", "configuration")},
            {"logs": 5, "metrics": 5, "configuration": 5},
        )
        self.assertTrue(all(item["expected_alert"].startswith("Lab") for item in SCENARIOS.values()))
        self.assertEqual(DEFAULT_SCENARIO_CONFIG["INVENTORY_QUERY_REVISION"], "v1")

    def test_real_decoder_accepts_valid_document_and_raises_on_invalid_encoding(self):
        value = {"sku": "example", "quantity": 4}
        payload = json.dumps({"body": base64.b64encode(json.dumps(value).encode()).decode()})
        self.assertEqual(decode_job(payload), value)
        with self.assertRaises(ValueError):
            decode_job('{"body":"not-valid-base64!"}')

    def test_memory_source_is_kernel_available_not_allocatable(self):
        response = MagicMock()
        response.__enter__.return_value.read.return_value = b"node_memory_MemAvailable_bytes 1500000000\nnode_memory_MemTotal_bytes 7000000000\n"
        with patch("app.safety.urlopen", return_value=response):
            self.assertEqual(memory_snapshot()["available_bytes"], 1500000000)
        response.__enter__.return_value.read.return_value = b"node_memory_MemTotal_bytes 7000000000\n"
        with patch("app.safety.urlopen", return_value=response), self.assertRaises(KeyError):
            memory_snapshot()

    def test_start_fails_closed_without_headroom_or_measurement(self):
        state = ControlState()
        state._start = Mock()
        with patch("app.control.memory_snapshot", return_value={"available_bytes": 900_000_000}), self.assertRaises(ValueError):
            state.start("memory-leak")
        with patch("app.control.memory_snapshot", side_effect=OSError), self.assertRaises(OSError):
            state.start("memory-leak")
        state._start.assert_not_called()
        self.assertIsNone(state.active)

    def test_only_one_bounded_run_can_start(self):
        state = ControlState()
        state._start = Mock(return_value={"ok": True})
        state._health = Mock(return_value={"reachable": True})
        with patch("app.control.memory_snapshot", return_value={"available_bytes": 2_000_000_000}):
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
            with patch("app.control.memory_snapshot", side_effect=OSError if memory is None else None, return_value={"available_bytes": memory}), patch("app.control.time.sleep", side_effect=StopIteration):
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

    def test_schema_drift_executes_incompatible_query_and_records_server_code(self):
        state = InventoryState()
        state.query_revision = "v2"
        state.logger = Mock()
        connection = MagicMock()
        cursor = connection.__enter__.return_value.cursor.return_value.__enter__.return_value
        cursor.execute.side_effect = [None, pymysql.err.OperationalError(1054, "Unknown column 'reserved_quantity'")]
        state.connect = Mock(return_value=connection)
        status, _ = state.reserve("order-1", "checkout-key-v1")
        self.assertEqual(status, 503)
        self.assertEqual(state.db_failures["query"], 1)
        self.assertIn("reserved_quantity", cursor.execute.call_args.args[0])
        self.assertEqual(state.logger.write.call_args.kwargs["mysql_error_code"], 1054)
        self.assertEqual(state.active_transactions, 0)

    def test_stock_reconciliation_holds_real_update_then_rolls_back(self):
        state = InventoryState()
        state.logger = Mock()
        stop = threading.Event()
        state.logger.write.side_effect = lambda *args, **kwargs: stop.set()
        connection = MagicMock()
        opened = connection.__enter__.return_value
        state.connect = Mock(return_value=connection)
        state._reconcile_stock(stop)
        cursor = opened.cursor.return_value.__enter__.return_value
        self.assertIn("UPDATE inventory_items", cursor.execute.call_args.args[0])
        opened.rollback.assert_called_once()

    def test_alerts_report_symptoms_without_injected_answers(self):
        objects = list(yaml.safe_load_all((ROOT / "deploy/kubernetes/observability.yaml").read_text()))
        rules = [rule for obj in objects if obj["kind"] == "PrometheusRule" for group in obj["spec"]["groups"] for rule in group["rules"]]
        annotations = json.dumps([rule["annotations"] for rule in rules]).lower()
        for answer in ("poison", "buffered", "schema mismatch", "pool is retaining", "pbkdf2"):
            self.assertNotIn(answer, annotations)
        connections = next(rule for rule in rules if rule["alert"] == "LabMySQLConnectionsSaturated")
        self.assertIn("mysql_global_status_threads_connected", connections["expr"])
        self.assertTrue(all(rule["for"] for rule in rules))


if __name__ == "__main__":
    unittest.main()
