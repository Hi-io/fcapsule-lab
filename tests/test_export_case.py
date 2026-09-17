from __future__ import annotations

import json
import sys
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch


TOOLS = Path(__file__).resolve().parents[1] / "tools"
sys.path.insert(0, str(TOOLS))
import export_case  # noqa: E402


class ExportCaseTests(unittest.TestCase):
    def test_export_writes_bounded_normalized_files(self) -> None:
        alerts = [
            {
                "alertname": "OrdersCheckoutFailureRateHigh",
                "status": "firing",
                "severity": "critical",
                "startsAt": "2026-09-18T10:00:00Z",
                "labels": {"service": "orders-api"},
                "annotations": {"summary": "Checkout failures are high"},
            }
        ]
        metrics = [
            {
                "metric": "orders_checkout_requests_total",
                "labels": {"status": "503"},
                "values": [["2026-09-18T10:00:00Z", 1.0], ["2026-09-18T10:00:05Z", 9.0]],
            }
        ]
        logs = [
            {"@timestamp": "2026-09-18T10:00:01Z", "level": "ERROR", "message": "Checkout retry budget exhausted", "service": "orders-api"}
        ]
        with tempfile.TemporaryDirectory() as directory:
            target = Path(directory) / "case"
            with patch.object(export_case, "fetch_alerts", return_value=alerts), patch.object(export_case, "fetch_metrics", return_value=metrics), patch.object(export_case, "parse_compose_logs", return_value=logs):
                result = export_case.export_case(target, 2)
            self.assertEqual(result["alerts"], 1)
            self.assertEqual(result["series"], 1)
            self.assertEqual(result["logs"], 1)
            self.assertTrue((target / "metadata.yaml").is_file())
            self.assertEqual(json.loads((target / "alert.json").read_text(encoding="utf-8"))[0]["alertname"], "OrdersCheckoutFailureRateHigh")
            self.assertEqual(json.loads((target / "opensearch_logs.json").read_text(encoding="utf-8"))["hits"], logs)

    def test_fetch_alerts_normalizes_prometheus_labels(self) -> None:
        payload = {
            "alerts": [
                {
                    "state": "firing",
                    "activeAt": "2026-09-18T10:00:00Z",
                    "labels": {"alertname": "InventoryLockTimeouts", "severity": "warning", "service": "inventory-api"},
                    "annotations": {"summary": "Lock timeout"},
                }
            ]
        }
        with patch.object(export_case, "fetch_json", return_value=payload):
            alerts = export_case.fetch_alerts("http://prometheus")
        self.assertEqual(alerts[0]["alertname"], "InventoryLockTimeouts")
        self.assertEqual(alerts[0]["labels"]["service"], "inventory-api")
        self.assertNotIn("alertname", alerts[0]["labels"])


if __name__ == "__main__":
    unittest.main()
