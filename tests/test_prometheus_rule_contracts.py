"""Offline contracts between Lab alerts, application metrics and demo controls."""

import os
import re
import unittest
from pathlib import Path
from unittest.mock import patch

import yaml

from app.mysql_inventory import InventoryState
from app.orders import OrdersState
from app.scenario_catalog import DISCOVERY_SCENARIOS, SCENARIOS
from app.worker import WorkerState
from tools.evaluate_external_screenshot import ALERT as EXTERNAL_ALERT, rule_document
from tools.test_prometheus_rules import build_rule_test, load_rule_spec


ROOT = Path(__file__).resolve().parents[1]


def _rules(spec):
    return {rule["alert"]: rule for group in spec["groups"] for rule in group["rules"]}


def _metric_names(exposition):
    return {
        match.group(1)
        for line in exposition.splitlines()
        if (match := re.match(r"^([a-zA-Z_:][a-zA-Z0-9_:]*)(?:\{|\s)", line))
    }


class PrometheusRuleContractTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.objects = list(yaml.safe_load_all((ROOT / "deploy/kubernetes/observability.yaml").read_text(encoding="utf-8")))
        cls.prometheus_rule = next(item for item in cls.objects if item.get("kind") == "PrometheusRule")
        cls.rules = _rules(cls.prometheus_rule["spec"])

    def test_all_fifteen_diagnostic_and_two_discovery_controls_have_an_alert_rule(self):
        expected = {item["expected_alert"] for item in (*SCENARIOS.values(), *DISCOVERY_SCENARIOS.values())}
        external = rule_document("offline-test-owner")
        external_names = {rule["alert"] for group in external["spec"]["groups"] for rule in group["rules"]}

        self.assertEqual(len(SCENARIOS), 15)
        self.assertEqual(len(DISCOVERY_SCENARIOS), 2)
        self.assertEqual(len(expected), 17)
        self.assertEqual(EXTERNAL_ALERT, "LabExporterScrapeFailed")
        self.assertEqual(expected - {EXTERNAL_ALERT}, set(self.rules).intersection(expected))
        self.assertEqual(external_names, {EXTERNAL_ALERT})

    def test_changed_rule_guards_are_explicit_and_temporally_sustained(self):
        poison = self.rules["LabWorkerPoisonRetries"]
        poison_expr = " ".join(poison["expr"].split())
        self.assertIn('increase(lab_worker_poison_retries_total{namespace="fcapsule-lab",service="lab-worker"}[1m]) > 2', poison_expr)
        self.assertEqual(poison["for"], "10s")

        buffer = self.rules["LabWorkerBufferPressure"]
        buffer_expr = " ".join(buffer["expr"].split())
        self.assertIn("max by (namespace, pod, service)", buffer_expr)
        self.assertIn('lab_worker_allocated_bytes{namespace="fcapsule-lab",service="lab-worker"}', buffer_expr)
        self.assertTrue(buffer_expr.endswith(") > 83886080"))
        self.assertEqual(buffer["for"], "15s")

        mysql = self.rules["LabMySQLConnectionsSaturated"]
        mysql_expr = " ".join(mysql["expr"].split())
        for condition in (
            'inventory_mysql_client_sessions_active{namespace="fcapsule-lab",service="inventory-api"}',
            'inventory_mysql_server_max_connections{namespace="fcapsule-lab",service="inventory-api"} > 0',
            'time() - inventory_mysql_sample_timestamp_seconds{namespace="fcapsule-lab",service="inventory-api"} < 45',
            'time() - timestamp(inventory_mysql_client_sessions_active{namespace="fcapsule-lab",service="inventory-api"}) < 30',
            'up{namespace="fcapsule-lab",service="inventory-api"} == 1',
        ):
            self.assertIn(condition, mysql_expr)
        self.assertIn(") > 0.80", mysql_expr)
        self.assertEqual(mysql["for"], "20s")

        latency = self.rules["LabCheckoutLatencyHigh"]
        latency_expr = " ".join(latency["expr"].split())
        for condition in (
            'orders_checkout_latency_p95_seconds{namespace="fcapsule-lab",service="orders-api"} > 0.25',
            'orders_checkout_latency_sample_count{namespace="fcapsule-lab",service="orders-api"} >= 10',
            'time() - orders_checkout_latency_latest_sample_timestamp_seconds{namespace="fcapsule-lab",service="orders-api"} < 30',
            'up{namespace="fcapsule-lab",service="orders-api"} == 1',
        ):
            self.assertIn(condition, latency_expr)
        self.assertEqual(latency["for"], "20s")

        cpu = self.rules["LabWorkerCPUHigh"]
        cpu_expr = " ".join(cpu["expr"].split())
        self.assertIn("rate(container_cpu_usage_seconds_total", cpu_expr)
        self.assertIn('kube_pod_container_resource_limits{', cpu_expr)
        self.assertIn('resource="cpu", unit="core"', cpu_expr)
        self.assertTrue(cpu_expr.endswith(") > 0.75"))
        self.assertEqual(cpu["for"], "0s", "the one-minute rate already bounds transient CPU samples")

        discovery = self.rules["LabApplicationMetricsDiscoveryMissing"]
        discovery_expr = " ".join(discovery["expr"].split())
        self.assertIn('absent_over_time(up{namespace="fcapsule-lab",service="orders-api"}[1m])', discovery_expr)
        self.assertIn("and on(namespace)", discovery_expr)
        self.assertIn('kube_deployment_status_replicas_available{ namespace="fcapsule-lab",deployment="orders-api" } > 0',
                      discovery_expr)
        self.assertEqual(discovery["for"], "30s")

    def test_alert_metric_names_are_emitted_by_the_corresponding_local_apps(self):
        with patch.dict(os.environ, {"INVENTORY_URL": "http://inventory-api:8081"}):
            orders_exposition = OrdersState().metrics()
        inventory_exposition = InventoryState().metrics()
        worker_exposition = WorkerState().metrics()
        orders_metrics = _metric_names(orders_exposition)
        inventory_metrics = _metric_names(inventory_exposition)
        worker_metrics = _metric_names(worker_exposition)

        self.assertIn("lab_worker_allocated_bytes", worker_metrics)
        self.assertIn("lab_worker_poison_retries_total", worker_metrics)
        self.assertIn("lab_worker_allocated_bytes 0", worker_exposition)
        self.assertTrue({"inventory_mysql_client_sessions_active", "inventory_mysql_server_max_connections",
                         "inventory_mysql_sample_timestamp_seconds"}.issubset(inventory_metrics))
        self.assertIn("inventory_mysql_client_sessions_active 0", inventory_exposition)
        self.assertIn("inventory_mysql_server_max_connections 0", inventory_exposition)
        self.assertIn("inventory_mysql_sample_timestamp_seconds 0", inventory_exposition)
        self.assertTrue({"orders_checkout_latency_p95_seconds", "orders_checkout_latency_sample_count",
                         "orders_checkout_latency_latest_sample_timestamp_seconds"}.issubset(orders_metrics))
        self.assertIn("orders_checkout_latency_p95_seconds 0", orders_exposition)
        self.assertIn("orders_checkout_latency_sample_count 0", orders_exposition)
        self.assertIn("orders_checkout_latency_latest_sample_timestamp_seconds 0", orders_exposition)

    def test_target_identity_relabeling_matches_rule_join_labels(self):
        monitor = next(item for item in self.objects if item.get("kind") == "ServiceMonitor"
                       and item["metadata"]["name"] == "fcapsule-lab-applications")
        relabelings = monitor["spec"]["endpoints"][0]["relabelings"]
        service_relabel = next(item for item in relabelings if item.get("targetLabel") == "service")
        self.assertEqual(service_relabel["sourceLabels"], ["__meta_kubernetes_pod_label_app_kubernetes_io_name"])
        self.assertEqual(self.rules["LabMySQLConnectionsSaturated"]["labels"]["service"], "inventory-api")
        self.assertEqual(self.rules["LabCheckoutLatencyHigh"]["labels"]["service"], "orders-api")
        self.assertEqual(self.rules["LabWorkerBufferPressure"]["labels"]["service"], "lab-worker")

    def test_compose_rule_file_stays_parseable_and_covers_its_local_signals(self):
        compose = yaml.safe_load((ROOT / "prometheus/alerts.yml").read_text(encoding="utf-8"))
        compose_rules = {rule["alert"]: rule for group in compose["groups"] for rule in group["rules"]}
        self.assertTrue({"OrdersCheckoutFailureRateHigh", "InventoryLockTimeouts",
                         "OrdersRetryAmplification"}.issubset(compose_rules))
        self.assertTrue(all(rule.get("expr") and rule.get("for") for rule in compose_rules.values()))
        self.assertIn("orders_retry_amplification_ratio", compose_rules["OrdersRetryAmplification"]["expr"])
        self.assertIn("inventory_database_failures_total", compose_rules["InventoryLockTimeouts"]["expr"])

    def test_promtool_fixture_covers_all_scenario_alerts_and_recovery_paths(self):
        test = build_rule_test(self.prometheus_rule["spec"])
        alert_tests = [case for group in test["tests"] for case in group["alert_rule_test"]]
        by_alert = {}
        for case in alert_tests:
            by_alert.setdefault(case["alertname"], []).append(case)

        expected = {item["expected_alert"] for item in (*SCENARIOS.values(), *DISCOVERY_SCENARIOS.values())}
        self.assertEqual(expected, set(by_alert))
        for alertname in sorted(expected):
            with self.subTest(alertname=alertname):
                cases = by_alert[alertname]
                self.assertTrue(any(case["exp_alerts"] for case in cases), "expected a positive firing fixture")
                self.assertTrue(any(not case["exp_alerts"] for case in cases), "expected negative/stale fixtures")
                self.assertTrue(any(case["eval_time"] == "0s" and not case["exp_alerts"] for case in cases),
                                "the alert must be quiet before the scenario fault is injected")
                groups = [group for group in test["tests"]
                          if group["alert_rule_test"][0]["alertname"] == alertname]
                self.assertTrue(any(not group["input_series"] for group in groups),
                                "expected an empty-input/no-data fixture")
                self.assertTrue(any(
                    any(case["exp_alerts"] for case in group["alert_rule_test"])
                    and any(not case["exp_alerts"] and float(case["eval_time"].removesuffix("s"))
                            > min(float(firing["eval_time"].removesuffix("s"))
                                  for firing in group["alert_rule_test"] if firing["exp_alerts"])
                            for case in group["alert_rule_test"])
                    for group in groups
                ), "expected a firing fixture that later clears after recovery")

        expected_firing_windows = {
            "LabWorkerBufferPressure": ("15s", "20s"),
            "LabWorkerPoisonRetries": ("10s", "15s"),
            "LabMySQLConnectionsSaturated": ("20s", "25s"),
            "LabCheckoutLatencyHigh": ("20s", "25s"),
        }
        for alertname, (pending_at, firing_at) in expected_firing_windows.items():
            with self.subTest(alertname=alertname):
                cases = by_alert[alertname]
                self.assertTrue(any(case["eval_time"] == pending_at and not case["exp_alerts"]
                                    for case in cases), "must remain pending before the configured `for`")
                self.assertTrue(any(case["eval_time"] == firing_at and case["exp_alerts"]
                                    for case in cases), "must fire after the configured `for`")

        stale_buffer = any(
            "stale" in sample["values"]
            for group in test["tests"]
            if group["alert_rule_test"][0]["alertname"] == "LabWorkerBufferPressure"
            for sample in group["input_series"]
        )
        stale_poison = any(
            "stale" in sample["values"]
            for group in test["tests"]
            if group["alert_rule_test"][0]["alertname"] == "LabWorkerPoisonRetries"
            for sample in group["input_series"]
        )
        self.assertTrue(stale_buffer, "buffer alert should clear when its sampled gauge becomes stale")
        self.assertTrue(stale_poison, "counter alert should clear after a stale marker expires the range")
        for alertname in sorted(expected - {"LabWorkerBufferPressure", "LabWorkerPoisonRetries",
                                            "LabMySQLConnectionsSaturated", "LabCheckoutLatencyHigh",
                                            "LabApplicationMetricsDiscoveryMissing", "LabExporterScrapeFailed"}):
            with self.subTest(stale_alert=alertname):
                self.assertTrue(any("stale" in sample["values"]
                                    for group in test["tests"]
                                    if group["alert_rule_test"][0]["alertname"] == alertname
                                    for sample in group["input_series"]),
                                "counter alert should have a stale-marker recovery fixture")

        for alertname in ("LabWorkerCPUHigh", "LabApplicationMetricsDiscoveryMissing", "LabExporterScrapeFailed"):
            with self.subTest(stale_alert=alertname):
                self.assertTrue(any("stale" in sample["values"]
                                    for group in test["tests"]
                                    if group["alert_rule_test"][0]["alertname"] == alertname
                                    for sample in group["input_series"]),
                                "alert should distinguish stale samples from an active signal")

        cpu_cases = by_alert["LabWorkerCPUHigh"]
        self.assertTrue(any(case["eval_time"] == "0s" and not case["exp_alerts"] for case in cpu_cases))
        self.assertTrue(any(case["eval_time"] == "5s" and not case["exp_alerts"] for case in cpu_cases),
                        "the one-minute rate must not fire on the first post-injection sample")
        self.assertTrue(any(case["eval_time"] == "50s" and not case["exp_alerts"] for case in cpu_cases))
        self.assertTrue(any(case["eval_time"] == "55s" and case["exp_alerts"] for case in cpu_cases),
                        "CPU pressure should fire once the one-minute rate crosses the configured threshold")
        self.assertTrue(any(case["eval_time"] == "80s" and not case["exp_alerts"] for case in cpu_cases),
                        "CPU pressure should clear after the finite migration batch completes")
        external_cases = by_alert["LabExporterScrapeFailed"]
        self.assertTrue(any(case["eval_time"] == "0s" and not case["exp_alerts"] for case in external_cases))
        self.assertTrue(any(case["eval_time"] == "20s" and case["exp_alerts"] for case in external_cases))
        self.assertTrue(any(case["eval_time"] == "25s" and not case["exp_alerts"] for case in external_cases),
                        "exporter scrape alert should clear after the target recovers")
        self.assertGreaterEqual(len(test["tests"]), 70)


if __name__ == "__main__":
    unittest.main()
