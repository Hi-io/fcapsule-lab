import json
import unittest
from io import BytesIO
from urllib.error import HTTPError
from unittest.mock import Mock, patch

from app.orders import OrdersState


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


def metric_value(exposition: str, name: str) -> float:
    line = next(line for line in exposition.splitlines() if line.startswith(f"{name} "))
    return float(line.split()[1])


class OrdersDiagnosticMetricTests(unittest.TestCase):
    def test_dependency_latency_metrics_capture_slow_success_and_are_bounded(self):
        with patch.dict("os.environ", {
                "INVENTORY_URL": "http://inventory-api:8081",
                "ORDER_SIGNING_KEY_ID": "checkout-kid-v2",
        }):
            state = OrdersState()
        state.logger = Mock()
        contract = json.dumps({"schema": "v1", "status": "reserved"}).encode()
        auth_error = HTTPError("http://inventory-api:8081/reserve", 401, "unauthorized", {}, BytesIO(b"{}"))

        empty = state.metrics()
        self.assertEqual(metric_value(empty, "orders_inventory_dependency_latency_p95_seconds"), 0)
        self.assertEqual(metric_value(empty, "orders_inventory_dependency_latency_sample_count"), 0)

        with patch("app.orders.MAX_LATENCY_SAMPLES", 2), \
                patch("app.orders.urlopen", side_effect=[
                    FakeResponse(contract), FakeResponse(contract), auth_error
                ]), \
                patch("app.orders.time.perf_counter", side_effect=[
                    1.0, 1.05, 2.0, 2.35, 3.0, 3.02
                ]):
            state._attempt_inventory_result("healthy-order")
            healthy = state.metrics()
            self.assertEqual(metric_value(healthy, "orders_inventory_dependency_latency_p95_seconds"), 0.05)

            state._attempt_inventory_result("slow-order")
            fault = state.metrics()
            self.assertEqual(metric_value(fault, "orders_inventory_dependency_latency_p95_seconds"), 0.35)

            state._attempt_inventory_result("unauthorized-order")

        exposition = state.metrics()
        self.assertEqual(metric_value(exposition, "orders_inventory_dependency_latency_sample_count"), 2)
        self.assertEqual(metric_value(exposition, "orders_inventory_dependency_latency_p95_seconds"), 0.35)
        self.assertGreater(metric_value(
            exposition, "orders_inventory_dependency_latency_oldest_sample_timestamp_seconds"), 0
        )
        self.assertGreaterEqual(
            metric_value(exposition, "orders_inventory_dependency_latency_latest_sample_timestamp_seconds"),
            metric_value(exposition, "orders_inventory_dependency_latency_oldest_sample_timestamp_seconds"),
        )

        auth_events = [call.kwargs for call in state.logger.write.call_args_list
                       if call.args and call.args[1] == "Inventory dependency attempt completed"
                       and call.kwargs.get("outcome") == "authorization"]
        self.assertEqual(len(auth_events), 1)
        self.assertEqual(auth_events[0]["request_key_id"], "checkout-kid-v2")
        self.assertNotIn("key", auth_events[0])

    def test_dependency_latency_metrics_expire_old_samples(self):
        with patch.dict("os.environ", {"INVENTORY_URL": "http://inventory-api:8081"}):
            state = OrdersState()
        now = 100.0
        state.inventory_latencies = [
            (now - 11, 1_700_000_000.0, 0.9),
            (now, 1_700_000_011.0, 0.1),
        ]

        with patch("app.orders.LATENCY_WINDOW_SECONDS", 10), \
                patch("app.orders.time.monotonic", return_value=now):
            exposition = state.metrics()

        self.assertEqual(metric_value(exposition, "orders_inventory_dependency_latency_sample_count"), 1)
        self.assertEqual(metric_value(exposition, "orders_inventory_dependency_latency_p95_seconds"), 0.1)


if __name__ == "__main__":
    unittest.main()
