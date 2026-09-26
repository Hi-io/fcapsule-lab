"""Validate Lab Prometheus rules and temporal edge cases with local promtool."""

import argparse
import subprocess
import tempfile
from pathlib import Path
from typing import Any

import yaml


RULE_FILE = Path("deploy/kubernetes/observability.yaml")
COMPOSE_RULE_FILE = Path("prometheus/alerts.yml")
LIBRARY_RULE_FILE = Path("deploy/kubernetes/incident_library.yaml")


def load_rule_spec(root: Path | None = None) -> dict[str, Any]:
    root = root or Path(__file__).resolve().parents[1]
    documents = yaml.safe_load_all((root / RULE_FILE).read_text(encoding="utf-8"))
    return next(item["spec"] for item in documents if item.get("kind") == "PrometheusRule")


def load_library_rule_spec(root: Path | None = None) -> dict[str, Any]:
    root = root or Path(__file__).resolve().parents[1]
    documents = yaml.safe_load_all((root / LIBRARY_RULE_FILE).read_text(encoding="utf-8"))
    return next(item["spec"] for item in documents if item.get("kind") == "PrometheusRule")


def _rules_by_name(spec: dict[str, Any]) -> dict[str, dict[str, Any]]:
    return {rule["alert"]: rule for group in spec.get("groups", []) for rule in group.get("rules", [])}


def _series(
    metric: str,
    pod: str,
    service: str,
    values: str,
    extra_labels: dict[str, str] | None = None,
) -> dict[str, str]:
    labels = {"namespace": "fcapsule-lab", "pod": pod, "service": service, **(extra_labels or {})}
    rendered_labels = ",".join(f'{name}="{value}"' for name, value in labels.items())
    return {
        "series": f"{metric}{{{rendered_labels}}}",
        "values": values,
    }


def _alert_expectation(
    rule: dict[str, Any],
    pod: str | None,
    service: str,
    extra_labels: dict[str, str] | None = None,
) -> dict[str, Any]:
    labels = {"namespace": "fcapsule-lab", **rule.get("labels", {}), **(extra_labels or {}), "service": service}
    if pod:
        labels["pod"] = pod
    return {"exp_labels": labels, "exp_annotations": rule.get("annotations", {})}


def _rule_case(
    rules: dict[str, dict[str, Any]],
    alertname: str,
    series: list[dict[str, str]],
    *,
    pod: str | None,
    service: str,
    extra_labels: dict[str, str] | None = None,
    absent_at: tuple[str, ...] = (),
    firing_at: tuple[str, ...] = (),
) -> dict[str, Any]:
    rule = rules[alertname]
    expected = _alert_expectation(rule, pod, service, extra_labels)
    expected_by_time = {value: [] for value in absent_at}
    expected_by_time.update({value: [expected] for value in firing_at})
    tests = [{"eval_time": value, "alertname": alertname, "exp_alerts": expected_by_time[value]}
             for value in sorted(expected_by_time, key=lambda item: float(item.removesuffix("s")))]
    return {"interval": "5s", "start_timestamp": "2023-11-14T22:13:20Z",
            "input_series": series, "alert_rule_test": tests}


def _load_external_rule() -> dict[str, Any]:
    try:
        from tools.evaluate_external_screenshot import rule_document
    except ModuleNotFoundError:
        from evaluate_external_screenshot import rule_document
    document = rule_document("promtool-fixture")
    return document["spec"]


COUNTER_SCENARIO_RULES = {
    # alert: (metric, distinguishing kind, strict threshold, lookback seconds, for seconds, service)
    "LabOrdersDependencyDocumentInvalid": ("orders_dependency_failures_total", "contract_shape", 5, 60, 15, "orders-api"),
    "LabInventoryConstraintFailures": ("inventory_transaction_failures_total", "constraint", 5, 60, 15, "inventory-api"),
    "LabInventoryDeadlockVictims": ("inventory_transaction_failures_total", "deadlock", 3, 60, 15, "inventory-api"),
    "LabInventoryAdmissionRejections": ("inventory_reservation_admission_rejections_total", None, 5, 60, 15, "inventory-api"),
    "LabOrdersIdempotencyConflicts": ("orders_internal_failures_total", "idempotency_conflict", 5, 60, 15, "orders-api"),
    "LabInventoryLockContention": ("inventory_database_failures_total", "lock_timeout", 5, 120, 15, "inventory-api"),
    "LabInventoryQueryFailures": ("inventory_database_failures_total", "query", 5, 60, 15, "inventory-api"),
    "LabOrdersDependencyTransportFailures": ("orders_dependency_failures_total", "transport", 5, 60, 15, "orders-api"),
    "LabOrdersDependencyTimeouts": ("orders_dependency_failures_total", "timeout", 5, 60, 15, "orders-api"),
    "LabOrdersDependencyAuthorizationFailures": ("orders_dependency_failures_total", "authorization", 5, 60, 15, "orders-api"),
    "LabOrdersDependencySchemaRejected": ("orders_dependency_failures_total", "contract_version", 5, 60, 15, "orders-api"),
}


def _counter_values(lookback_seconds: int, fault_delta: int) -> str:
    sample_count = (lookback_seconds + 10) // 5 + 1
    return " ".join(["0", str(fault_delta), *([str(fault_delta)] * (sample_count - 2))])


def _counter_rule_cases(rules: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
    tests = []
    for alertname, (metric, kind, threshold, lookback, hold, service) in COUNTER_SCENARIO_RULES.items():
        pod = alertname.lower()
        firing_seconds = 5 + hold
        pending_seconds = firing_seconds - 5
        recovery_seconds = lookback + 10
        labels = {"kind": kind} if kind else {}
        injected_values = _counter_values(lookback, threshold + 1)
        injected = _series(metric, pod, service, injected_values, labels)
        tests.append(_rule_case(
            rules, alertname, [injected], pod=pod, service=service, extra_labels=labels,
            absent_at=("0s", f"{pending_seconds}s", f"{recovery_seconds}s"),
            firing_at=(f"{firing_seconds}s",),
        ))

        low_delta = max(1, threshold // 3)
        below_threshold = _series(metric, pod, service, _counter_values(lookback, low_delta), labels)
        tests.append(_rule_case(rules, alertname, [below_threshold], pod=pod, service=service,
                                extra_labels=labels, absent_at=("0s", f"{firing_seconds}s")))

        if kind:
            wrong_kind = _series(metric, pod, service, injected_values, {"kind": "unrelated"})
            tests.append(_rule_case(rules, alertname, [wrong_kind], pod=pod, service=service,
                                    absent_at=(f"{firing_seconds}s",)))

        stale = _series(metric, pod, service, f"0 {threshold + 1} stale", labels)
        tests.append(_rule_case(rules, alertname, [stale], pod=pod, service=service,
                                extra_labels=labels, absent_at=(f"{recovery_seconds}s",)))
        tests.append(_rule_case(rules, alertname, [], pod=None, service=service,
                                absent_at=(f"{recovery_seconds}s",)))
    return tests


def build_rule_test(spec: dict[str, Any]) -> dict[str, Any]:
    """Build synthetic rule tests; no cluster or telemetry endpoint is contacted."""

    rules = _rules_by_name(spec)
    external_spec = _load_external_rule()
    rules.update(_rules_by_name(external_spec))
    tests = []

    # Buffer pressure must stay pending for 15 seconds; transient and exact-limit
    # values are negative controls for its strict greater-than comparison.
    tests.append(_rule_case(rules, "LabWorkerBufferPressure", [
        _series("lab_worker_allocated_bytes", "buffer-positive", "lab-worker",
                "0 90000000 90000000 90000000 90000000 90000000"),
    ], pod="buffer-positive", service="lab-worker", absent_at=("0s", "15s"), firing_at=("20s",)))
    tests.append(_rule_case(rules, "LabWorkerBufferPressure", [
        _series("lab_worker_allocated_bytes", "buffer-recovered", "lab-worker",
                "0 90000000 90000000 90000000 90000000 90000000 0 0"),
    ], pod="buffer-recovered", service="lab-worker", absent_at=("0s", "15s", "30s"),
                            firing_at=("20s",)))
    tests.append(_rule_case(rules, "LabWorkerBufferPressure", [
        _series("lab_worker_allocated_bytes", "buffer-transient", "lab-worker",
                "0 90000000 90000000 0 0 0 0"),
    ], pod="buffer-transient", service="lab-worker", absent_at=("30s",)))
    tests.append(_rule_case(rules, "LabWorkerBufferPressure", [
        _series("lab_worker_allocated_bytes", "buffer-stale", "lab-worker",
                "0 90000000 90000000 stale"),
    ], pod="buffer-stale", service="lab-worker", absent_at=("30s",)))
    tests.append(_rule_case(rules, "LabWorkerBufferPressure", [
        _series("lab_worker_allocated_bytes", "buffer-at-limit", "lab-worker",
                "83886080 83886080 83886080 83886080 83886080 83886080"),
    ], pod="buffer-at-limit", service="lab-worker", absent_at=("30s",)))
    tests.append(_rule_case(rules, "LabWorkerBufferPressure", [], pod=None, service="lab-worker",
                            absent_at=("30s",)))

    # The retry counter must increase inside its range. A flat counter, an
    # expired range and absent telemetry must not become a continuing alert.
    tests.append(_rule_case(rules, "LabWorkerPoisonRetries", [
        _series("lab_worker_poison_retries_total", "poison-positive", "lab-worker",
                "0 " + " ".join(["3"] * 14)),
    ], pod="poison-positive", service="lab-worker", absent_at=("0s", "10s", "70s"),
                            firing_at=("15s",)))
    tests.append(_rule_case(rules, "LabWorkerPoisonRetries", [
        _series("lab_worker_poison_retries_total", "poison-flat", "lab-worker",
                "4 4 4 4 4 4 4 4 4 4 4 4 4"),
    ], pod="poison-flat", service="lab-worker", absent_at=("60s",)))
    tests.append(_rule_case(rules, "LabWorkerPoisonRetries", [
        _series("lab_worker_poison_retries_total", "poison-expired", "lab-worker",
                "0 3 stale"),
    ], pod="poison-expired", service="lab-worker", absent_at=("80s",)))
    tests.append(_rule_case(rules, "LabWorkerPoisonRetries", [], pod=None, service="lab-worker",
                            absent_at=("60s",)))

    # These labels are supplied by the Lab ServiceMonitor relabelings. All
    # joined vectors use the same namespace/pod/service identity.
    mysql_pod = "mysql-positive"
    mysql_service = "inventory-api"
    mysql_series = [
        _series("inventory_mysql_client_sessions_active", mysql_pod, mysql_service, "0 9 9 9 9 9"),
        _series("inventory_mysql_server_max_connections", mysql_pod, mysql_service, "10 10 10 10 10 10"),
        _series("inventory_mysql_sample_timestamp_seconds", mysql_pod, mysql_service,
                "1700000000 1700000005 1700000005 1700000005 1700000005 1700000005"),
        _series("up", mysql_pod, mysql_service, "1 1 1 1 1 1"),
    ]
    tests.append(_rule_case(rules, "LabMySQLConnectionsSaturated", mysql_series,
                            pod=mysql_pod, service=mysql_service, absent_at=("0s", "15s", "20s"),
                            firing_at=("25s",)))
    recovered_mysql = [dict(item) for item in mysql_series]
    recovered_mysql[0] = _series("inventory_mysql_client_sessions_active", mysql_pod, mysql_service,
                                 "0 9 9 9 9 9 0")
    recovered_mysql[2] = _series("inventory_mysql_sample_timestamp_seconds", mysql_pod, mysql_service,
                                 "1700000000 1700000005 1700000005 1700000005 1700000005 1700000030 1700000030")
    tests.append(_rule_case(rules, "LabMySQLConnectionsSaturated", recovered_mysql,
                            pod=mysql_pod, service=mysql_service, absent_at=("0s", "20s", "30s"),
                            firing_at=("25s",)))
    stale_mysql = [dict(item) for item in mysql_series]
    stale_mysql[2] = _series("inventory_mysql_sample_timestamp_seconds", mysql_pod, mysql_service,
                             "1699999900 1699999900 1699999900 1699999900 1699999900 1699999900")
    tests.append(_rule_case(rules, "LabMySQLConnectionsSaturated", stale_mysql,
                            pod=mysql_pod, service=mysql_service, absent_at=("30s",)))
    zero_capacity = [dict(item) for item in mysql_series]
    zero_capacity[1] = _series("inventory_mysql_server_max_connections", mysql_pod, mysql_service,
                               "0 0 0 0 0 0")
    tests.append(_rule_case(rules, "LabMySQLConnectionsSaturated", zero_capacity,
                            pod=mysql_pod, service=mysql_service, absent_at=("30s",)))
    at_limit = [dict(item) for item in mysql_series]
    at_limit[0] = _series("inventory_mysql_client_sessions_active", mysql_pod, mysql_service,
                          "0 8 8 8 8 8")
    tests.append(_rule_case(rules, "LabMySQLConnectionsSaturated", at_limit,
                            pod=mysql_pod, service=mysql_service, absent_at=("30s",)))
    target_down = [dict(item) for item in mysql_series]
    target_down[3] = _series("up", mysql_pod, mysql_service, "0 0 0 0 0 0")
    tests.append(_rule_case(rules, "LabMySQLConnectionsSaturated", target_down,
                            pod=mysql_pod, service=mysql_service, absent_at=("30s",)))
    tests.append(_rule_case(rules, "LabMySQLConnectionsSaturated", [], pod=None,
                            service=mysql_service, absent_at=("30s",)))

    # The latency alert requires a high p95, at least ten observations, a recent
    # observation timestamp and a healthy scrape target.
    latency_pod = "latency-positive"
    latency_service = "orders-api"
    latency_series = [
        _series("orders_checkout_latency_p95_seconds", latency_pod, latency_service, ".1 .35 .35 .35 .35 .35"),
        _series("orders_checkout_latency_sample_count", latency_pod, latency_service, "0 10 10 10 10 10"),
        _series("orders_checkout_latency_latest_sample_timestamp_seconds", latency_pod, latency_service,
                "0 1700000005 1700000005 1700000005 1700000005 1700000005"),
        _series("up", latency_pod, latency_service, "1 1 1 1 1 1"),
    ]
    tests.append(_rule_case(rules, "LabCheckoutLatencyHigh", latency_series,
                            pod=latency_pod, service=latency_service, absent_at=("0s", "15s", "20s"),
                            firing_at=("25s",)))
    recovered_latency = [dict(item) for item in latency_series]
    recovered_latency[0] = _series("orders_checkout_latency_p95_seconds", latency_pod, latency_service,
                                   ".1 .35 .35 .35 .35 .35 .1")
    recovered_latency[2] = _series("orders_checkout_latency_latest_sample_timestamp_seconds",
                                   latency_pod, latency_service,
                                   "0 1700000005 1700000010 1700000015 1700000020 1700000025 1700000030")
    tests.append(_rule_case(rules, "LabCheckoutLatencyHigh", recovered_latency,
                            pod=latency_pod, service=latency_service, absent_at=("0s", "20s", "30s"),
                            firing_at=("25s",)))
    too_few = [dict(item) for item in latency_series]
    too_few[1] = _series("orders_checkout_latency_sample_count", latency_pod, latency_service,
                         "9 9 9 9 9 9")
    tests.append(_rule_case(rules, "LabCheckoutLatencyHigh", too_few,
                            pod=latency_pod, service=latency_service, absent_at=("30s",)))
    stale_latency = [dict(item) for item in latency_series]
    stale_latency[2] = _series("orders_checkout_latency_latest_sample_timestamp_seconds", latency_pod, latency_service,
                               "1699999900 1699999900 1699999900 1699999900 1699999900 1699999900")
    tests.append(_rule_case(rules, "LabCheckoutLatencyHigh", stale_latency,
                            pod=latency_pod, service=latency_service, absent_at=("30s",)))
    at_threshold = [dict(item) for item in latency_series]
    at_threshold[0] = _series("orders_checkout_latency_p95_seconds", latency_pod, latency_service,
                              ".1 .25 .25 .25 .25 .25")
    tests.append(_rule_case(rules, "LabCheckoutLatencyHigh", at_threshold,
                            pod=latency_pod, service=latency_service, absent_at=("30s",)))
    latency_target_down = [dict(item) for item in latency_series]
    latency_target_down[3] = _series("up", latency_pod, latency_service, "0 0 0 0 0 0")
    tests.append(_rule_case(rules, "LabCheckoutLatencyHigh", latency_target_down,
                            pod=latency_pod, service=latency_service, absent_at=("30s",)))
    tests.append(_rule_case(rules, "LabCheckoutLatencyHigh", [], pod=None,
                            service=latency_service, absent_at=("30s",)))

    # Dependency latency is measured per Orders-to-Inventory attempt, separate
    # from end-to-end checkout latency. Both conditions retain independent data
    # quality guards and can alert on the same Orders scrape target.
    dependency_pod = "dependency-latency-positive"
    dependency_service = "orders-api"
    dependency_series = [
        _series("orders_inventory_dependency_latency_p95_seconds", dependency_pod, dependency_service,
                ".1 .35 .35 .35 .35 .35"),
        _series("orders_inventory_dependency_latency_sample_count", dependency_pod, dependency_service,
                "0 10 10 10 10 10"),
        _series("orders_inventory_dependency_latency_latest_sample_timestamp_seconds",
                dependency_pod, dependency_service,
                "0 1700000005 1700000005 1700000005 1700000005 1700000005"),
        _series("up", dependency_pod, dependency_service, "1 1 1 1 1 1"),
    ]
    tests.append(_rule_case(rules, "LabInventoryDependencyLatencyHigh", dependency_series,
                            pod=dependency_pod, service=dependency_service,
                            absent_at=("0s", "20s"), firing_at=("25s",)))

    recovered_dependency = [dict(item) for item in dependency_series]
    recovered_dependency[0] = _series("orders_inventory_dependency_latency_p95_seconds",
                                       dependency_pod, dependency_service,
                                       ".1 .35 .35 .35 .35 .35 .1")
    recovered_dependency[2] = _series("orders_inventory_dependency_latency_latest_sample_timestamp_seconds",
                                       dependency_pod, dependency_service,
                                       "0 1700000005 1700000010 1700000015 1700000020 1700000025 1700000030")
    tests.append(_rule_case(rules, "LabInventoryDependencyLatencyHigh", recovered_dependency,
                            pod=dependency_pod, service=dependency_service,
                            absent_at=("0s", "20s", "30s"), firing_at=("25s",)))

    # Downstream latency raises both checkout and dependency p95 independently;
    # the operator runner accepts either as the primary incident for that case.
    co_firing_series = [
        _series("orders_checkout_latency_p95_seconds", dependency_pod, dependency_service,
                ".1 .35 .35 .35 .35 .35"),
        _series("orders_checkout_latency_sample_count", dependency_pod, dependency_service,
                "0 10 10 10 10 10"),
        _series("orders_checkout_latency_latest_sample_timestamp_seconds", dependency_pod, dependency_service,
                "0 1700000005 1700000005 1700000005 1700000005 1700000005"),
        *dependency_series,
    ]
    tests.append(_rule_case(rules, "LabCheckoutLatencyHigh", co_firing_series,
                            pod=dependency_pod, service=dependency_service, firing_at=("25s",)))
    tests.append(_rule_case(rules, "LabInventoryDependencyLatencyHigh", co_firing_series,
                            pod=dependency_pod, service=dependency_service, firing_at=("25s",)))

    dependency_at_threshold = [dict(item) for item in dependency_series]
    dependency_at_threshold[0] = _series("orders_inventory_dependency_latency_p95_seconds",
                                         dependency_pod, dependency_service,
                                         ".1 .25 .25 .25 .25 .25")
    tests.append(_rule_case(rules, "LabInventoryDependencyLatencyHigh", dependency_at_threshold,
                            pod=dependency_pod, service=dependency_service, absent_at=("30s",)))

    dependency_too_few = [dict(item) for item in dependency_series]
    dependency_too_few[1] = _series("orders_inventory_dependency_latency_sample_count",
                                     dependency_pod, dependency_service, "9 9 9 9 9 9")
    tests.append(_rule_case(rules, "LabInventoryDependencyLatencyHigh", dependency_too_few,
                            pod=dependency_pod, service=dependency_service, absent_at=("30s",)))

    stale_dependency = [dict(item) for item in dependency_series]
    stale_dependency[2] = _series("orders_inventory_dependency_latency_latest_sample_timestamp_seconds",
                                   dependency_pod, dependency_service,
                                   "1699999900 1699999900 1699999900 1699999900 1699999900 1699999900")
    tests.append(_rule_case(rules, "LabInventoryDependencyLatencyHigh", stale_dependency,
                            pod=dependency_pod, service=dependency_service, absent_at=("30s",)))

    dependency_target_down = [dict(item) for item in dependency_series]
    dependency_target_down[3] = _series("up", dependency_pod, dependency_service, "0 0 0 0 0 0")
    tests.append(_rule_case(rules, "LabInventoryDependencyLatencyHigh", dependency_target_down,
                            pod=dependency_pod, service=dependency_service, absent_at=("30s",)))

    missing_dependency_timestamp = [item for index, item in enumerate(dependency_series) if index != 2]
    tests.append(_rule_case(rules, "LabInventoryDependencyLatencyHigh", missing_dependency_timestamp,
                            pod=dependency_pod, service=dependency_service, absent_at=("30s",)))
    tests.append(_rule_case(rules, "LabInventoryDependencyLatencyHigh", [], pod=None,
                            service=dependency_service, absent_at=("30s",)))

    # CPU already uses a one-minute rate, which smooths short scheduling noise.
    # The bounded migration stops after its finite batch, so the rule should not
    # add a hold that can outlast the causal CPU signal.
    cpu_pod = "cpu-positive"
    cpu_service = "lab-worker"
    cpu_labels = {"container": "worker"}
    cpu_high = _series("container_cpu_usage_seconds_total", cpu_pod, cpu_service,
                       "0 0 5 10 15 20 25 30 35 40 45 50 55 55 55 55 55",
                       {**cpu_labels, "image": "worker-image"})
    cpu_limit = {
        "series": 'kube_pod_container_resource_limits{namespace="fcapsule-lab",pod="cpu-positive",'
                  'container="worker",resource="cpu",unit="core"}',
        "values": "1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1",
    }
    tests.append(_rule_case(rules, "LabWorkerCPUHigh", [cpu_high, cpu_limit], pod=cpu_pod,
                            service=cpu_service, extra_labels=cpu_labels,
                            absent_at=("0s", "5s", "50s", "80s"), firing_at=("55s",)))
    cpu_below = _series("container_cpu_usage_seconds_total", cpu_pod, cpu_service,
                        "0 2.5 5 7.5 10 12.5 15 17.5",
                        {**cpu_labels, "image": "worker-image"})
    cpu_below_limit = {**cpu_limit, "values": "1 1 1 1 1 1 1 1"}
    tests.append(_rule_case(rules, "LabWorkerCPUHigh", [cpu_below, cpu_below_limit], pod=cpu_pod,
                            service=cpu_service, extra_labels=cpu_labels, absent_at=("30s",)))
    cpu_stale = _series("container_cpu_usage_seconds_total", cpu_pod, cpu_service, "0 5 stale",
                        {**cpu_labels, "image": "worker-image"})
    tests.append(_rule_case(rules, "LabWorkerCPUHigh", [cpu_stale, cpu_limit], pod=cpu_pod,
                            service=cpu_service, extra_labels=cpu_labels, absent_at=("70s",)))
    tests.append(_rule_case(rules, "LabWorkerCPUHigh", [], pod=None, service=cpu_service,
                            absent_at=("30s",)))

    # Discovery loss is meaningful only while the workload is actually available;
    # an uninstalled or unready deployment must not page on the absence of `up`.
    ready_series = {
        "series": 'kube_deployment_status_replicas_available{namespace="fcapsule-lab",deployment="orders-api"}',
        "values": " ".join(["1"] * 21),
    }
    discovery_up = _series("up", "orders-ready", "orders-api",
                           "1 1 " + " ".join(["stale"] * 18) + " 1")
    tests.append(_rule_case(rules, "LabApplicationMetricsDiscoveryMissing",
                            [ready_series, discovery_up], pod=None, service="orders-api",
                            absent_at=("0s", "60s", "90s", "100s"), firing_at=("95s",)))
    unready_series = {**ready_series, "values": "0 0 0 0 0"}
    tests.append(_rule_case(rules, "LabApplicationMetricsDiscoveryMissing",
                            [unready_series], pod=None, service="orders-api",
                            absent_at=("30s", "120s")))
    tests.append(_rule_case(rules, "LabApplicationMetricsDiscoveryMissing", [], pod=None,
                            service="orders-api", absent_at=("120s",)))

    # The external screenshot runner's alert proves a discovered target is failing
    # scrapes, not merely absent. Healthy and restored `up` values are controls.
    exporter_pod = "exporter-positive"
    exporter_service = "mysql-exporter"
    exporter_up = _series("up", exporter_pod, exporter_service, "1 0 0 0 0 1")
    tests.append(_rule_case(rules, "LabExporterScrapeFailed", [exporter_up],
                            pod=exporter_pod, service=exporter_service,
                            absent_at=("0s", "15s", "25s"), firing_at=("20s",)))
    stale_exporter_up = _series("up", exporter_pod, exporter_service, "1 0 stale")
    tests.append(_rule_case(rules, "LabExporterScrapeFailed", [stale_exporter_up],
                            pod=exporter_pod, service=exporter_service, absent_at=("20s",)))
    tests.append(_rule_case(rules, "LabExporterScrapeFailed", [], pod=None,
                            service=exporter_service, absent_at=("30s",)))
    tests.append(_rule_case(rules, "LabExporterTargetUnavailable", [exporter_up],
                            pod=exporter_pod, service=exporter_service,
                            absent_at=("0s", "15s", "25s"), firing_at=("20s",)))
    tests.append(_rule_case(rules, "LabExporterTargetUnavailable", [stale_exporter_up],
                            pod=exporter_pod, service=exporter_service, absent_at=("20s",)))
    tests.append(_rule_case(rules, "LabExporterTargetUnavailable", [], pod=None,
                            service=exporter_service, absent_at=("30s",)))

    tests.extend(_counter_rule_cases(rules))

    library_rule = _rules_by_name(load_library_rule_spec())["LabLibraryOperationFailures"]
    library_id = {"scenario_id": "lib-01-001"}
    tests.append(_rule_case(
        {"LabLibraryOperationFailures": library_rule}, "LabLibraryOperationFailures", [
            _series("lab_library_failures_total", "library-positive", "lab-incident-library",
                    "0 5 5 5 5 5", library_id),
            _series("lab_library_active", "library-positive", "lab-incident-library",
                    "1 1 1 1 0 0", library_id),
        ], pod="library-positive", service="lab-incident-library", extra_labels=library_id,
        absent_at=("0s", "10s", "20s"), firing_at=("15s",),
    ))
    tests.append(_rule_case(
        {"LabLibraryOperationFailures": library_rule}, "LabLibraryOperationFailures", [
            _series("lab_library_failures_total", "library-duplicate", "lab-incident-library",
                    "0 5 5 5 5", {**library_id, "job": "first"}),
            _series("lab_library_failures_total", "library-duplicate", "lab-incident-library",
                    "0 5 5 5 5", {**library_id, "job": "second"}),
            _series("lab_library_active", "library-duplicate", "lab-incident-library",
                    "1 1 1 1 1", {**library_id, "job": "first"}),
            _series("lab_library_active", "library-duplicate", "lab-incident-library",
                    "1 1 1 1 1", {**library_id, "job": "second"}),
        ], pod="library-duplicate", service="lab-incident-library", extra_labels=library_id,
        absent_at=("0s", "10s"), firing_at=("15s",),
    ))
    tests.append(_rule_case(
        {"LabLibraryOperationFailures": library_rule}, "LabLibraryOperationFailures", [],
        pod=None, service="lab-incident-library", absent_at=("20s",),
    ))
    tests.append(_rule_case(
        {"LabLibraryOperationFailures": library_rule}, "LabLibraryOperationFailures", [
            _series("lab_library_failures_total", "library-stale", "lab-incident-library",
                    "0 5 stale", library_id),
            _series("lab_library_active", "library-stale", "lab-incident-library",
                    "1 1 stale", library_id),
        ], pod="library-stale", service="lab-incident-library", extra_labels=library_id,
        absent_at=("80s",),
    ))

    cnfc_rule = rules["LabCNFCInventoryRouteFailures"]
    cnfc_labels = {"cnfc": "checkout-edge-east"}
    fault_values = " ".join(str(value) for value in ([0] * 10 + [2 * step for step in range(1, 10)] + [18] * 16))
    duplicate_labels = {**cnfc_labels, "prometheus": "monitoring/pc-worker-agent",
                        "prometheus_replica": "prom-agent-pc-worker-agent-0"}
    expected_cnfc = {"exp_labels": {"namespace": "fcapsule-lab", **cnfc_labels, **cnfc_rule["labels"]},
                     "exp_annotations": cnfc_rule["annotations"]}
    tests.append({"interval": "5s", "start_timestamp": "2023-11-14T22:13:20Z",
                  "input_series": [
                      _series("lab_cnfc_edge_dependency_failures_total", "cnfc-edge-a", "cnfc-edge", fault_values, cnfc_labels),
                      _series("lab_cnfc_edge_dependency_failures_total", "cnfc-edge-a", "cnfc-edge", fault_values, duplicate_labels),
                      _series("lab_cnfc_edge_dependency_failures_total", "cnfc-edge-b", "cnfc-edge", " ".join(["0"] * 35), cnfc_labels),
                  ],
                  "alert_rule_test": [
                      {"eval_time": "0s", "alertname": "LabCNFCInventoryRouteFailures", "exp_alerts": []},
                      {"eval_time": "100s", "alertname": "LabCNFCInventoryRouteFailures", "exp_alerts": [expected_cnfc]},
                      {"eval_time": "160s", "alertname": "LabCNFCInventoryRouteFailures", "exp_alerts": []},
                  ]})
    tests.append({"interval": "5s", "start_timestamp": "2023-11-14T22:13:20Z",
                  "input_series": [], "alert_rule_test": [
                      {"eval_time": "100s", "alertname": "LabCNFCInventoryRouteFailures", "exp_alerts": []},
                  ]})
    tests.append({"interval": "5s", "start_timestamp": "2023-11-14T22:13:20Z",
                  "input_series": [_series("lab_cnfc_edge_dependency_failures_total", "cnfc-edge-a",
                                           "cnfc-edge", "0 10 stale", cnfc_labels)],
                  "alert_rule_test": [{"eval_time": "100s", "alertname": "LabCNFCInventoryRouteFailures",
                                       "exp_alerts": []}]})

    return {"evaluation_interval": "5s", "tests": tests}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--promtool", default="promtool", help="Path to the Prometheus promtool executable")
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[1]
    spec = load_rule_spec(root)
    with tempfile.TemporaryDirectory(prefix="fcapsule-promtool-") as temporary:
        folder = Path(temporary)
        rule_file = folder / "rules.yml"
        external_rule_file = folder / "external-rules.yml"
        library_rule_file = folder / "library-rules.yml"
        compose_rule_file = root / COMPOSE_RULE_FILE
        test_file = folder / "tests.yml"
        external_spec = _load_external_rule()
        library_spec = load_library_rule_spec(root)
        rule_file.write_text(yaml.safe_dump(spec), encoding="utf-8")
        external_rule_file.write_text(yaml.safe_dump(external_spec), encoding="utf-8")
        library_rule_file.write_text(yaml.safe_dump(library_spec), encoding="utf-8")
        test = {"rule_files": [str(rule_file), str(external_rule_file), str(library_rule_file), str(compose_rule_file)],
                **build_rule_test(spec)}
        test_file.write_text(yaml.safe_dump(test), encoding="utf-8")
        subprocess.run([args.promtool, "check", "rules", str(rule_file)], check=True)
        subprocess.run([args.promtool, "check", "rules", str(external_rule_file)], check=True)
        subprocess.run([args.promtool, "check", "rules", str(library_rule_file)], check=True)
        subprocess.run([args.promtool, "check", "rules", str(compose_rule_file)], check=True)
        subprocess.run([args.promtool, "test", "rules", str(test_file)], check=True)


if __name__ == "__main__":
    main()
