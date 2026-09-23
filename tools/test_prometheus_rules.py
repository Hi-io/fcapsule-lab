"""Validate Lab Prometheus rules and temporal edge cases with local promtool."""

import argparse
import subprocess
import tempfile
from pathlib import Path
from typing import Any

import yaml


RULE_FILE = Path("deploy/kubernetes/observability.yaml")


def load_rule_spec(root: Path | None = None) -> dict[str, Any]:
    root = root or Path(__file__).resolve().parents[1]
    documents = yaml.safe_load_all((root / RULE_FILE).read_text(encoding="utf-8"))
    return next(item["spec"] for item in documents if item.get("kind") == "PrometheusRule")


def _rules_by_name(spec: dict[str, Any]) -> dict[str, dict[str, Any]]:
    return {rule["alert"]: rule for group in spec.get("groups", []) for rule in group.get("rules", [])}


def _series(metric: str, pod: str, service: str, values: str) -> dict[str, str]:
    return {
        "series": f'{metric}{{namespace="fcapsule-lab",pod="{pod}",service="{service}"}}',
        "values": values,
    }


def _alert_expectation(rule: dict[str, Any], pod: str | None, service: str) -> dict[str, Any]:
    labels = {"namespace": "fcapsule-lab", **rule.get("labels", {}), "service": service}
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
    absent_at: tuple[str, ...] = (),
    firing_at: tuple[str, ...] = (),
) -> dict[str, Any]:
    rule = rules[alertname]
    expected = _alert_expectation(rule, pod, service)
    tests = [
        {"eval_time": value, "alertname": alertname,
         "exp_alerts": [] if value in absent_at else [expected]}
        for value in (*absent_at, *firing_at)
    ]
    return {"interval": "5s", "start_timestamp": "2023-11-14T22:13:20Z",
            "input_series": series, "alert_rule_test": tests}


def build_rule_test(spec: dict[str, Any]) -> dict[str, Any]:
    """Build synthetic rule tests; no cluster or telemetry endpoint is contacted."""

    rules = _rules_by_name(spec)
    tests = []

    # Buffer pressure must stay pending for 15 seconds; transient and exact-limit
    # values are negative controls for its strict greater-than comparison.
    tests.append(_rule_case(rules, "LabWorkerBufferPressure", [
        _series("lab_worker_allocated_bytes", "buffer-positive", "lab-worker",
                "0 90000000 90000000 90000000 90000000 90000000"),
    ], pod="buffer-positive", service="lab-worker", absent_at=("15s",), firing_at=("20s",)))
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
                "0 3 3 3 3 3 3 3"),
    ], pod="poison-positive", service="lab-worker", absent_at=("10s",), firing_at=("15s",)))
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
        _series("inventory_mysql_client_sessions_active", mysql_pod, mysql_service, "9 9 9 9 9 9"),
        _series("inventory_mysql_server_max_connections", mysql_pod, mysql_service, "10 10 10 10 10 10"),
        _series("inventory_mysql_sample_timestamp_seconds", mysql_pod, mysql_service,
                "1700000000 1700000000 1700000000 1700000000 1700000000 1700000000"),
        _series("up", mysql_pod, mysql_service, "1 1 1 1 1 1"),
    ]
    tests.append(_rule_case(rules, "LabMySQLConnectionsSaturated", mysql_series,
                            pod=mysql_pod, service=mysql_service, absent_at=("15s",), firing_at=("20s",)))
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
                          "8 8 8 8 8 8")
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
        _series("orders_checkout_latency_p95_seconds", latency_pod, latency_service, ".35 .35 .35 .35 .35 .35"),
        _series("orders_checkout_latency_sample_count", latency_pod, latency_service, "10 10 10 10 10 10"),
        _series("orders_checkout_latency_latest_sample_timestamp_seconds", latency_pod, latency_service,
                "1700000000 1700000000 1700000000 1700000000 1700000000 1700000000"),
        _series("up", latency_pod, latency_service, "1 1 1 1 1 1"),
    ]
    tests.append(_rule_case(rules, "LabCheckoutLatencyHigh", latency_series,
                            pod=latency_pod, service=latency_service, absent_at=("15s",), firing_at=("20s",)))
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
                              ".25 .25 .25 .25 .25 .25")
    tests.append(_rule_case(rules, "LabCheckoutLatencyHigh", at_threshold,
                            pod=latency_pod, service=latency_service, absent_at=("30s",)))
    latency_target_down = [dict(item) for item in latency_series]
    latency_target_down[3] = _series("up", latency_pod, latency_service, "0 0 0 0 0 0")
    tests.append(_rule_case(rules, "LabCheckoutLatencyHigh", latency_target_down,
                            pod=latency_pod, service=latency_service, absent_at=("30s",)))
    tests.append(_rule_case(rules, "LabCheckoutLatencyHigh", [], pod=None,
                            service=latency_service, absent_at=("30s",)))

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
        test_file = folder / "tests.yml"
        rule_file.write_text(yaml.safe_dump(spec), encoding="utf-8")
        test = {"rule_files": [str(rule_file)], **build_rule_test(spec)}
        test_file.write_text(yaml.safe_dump(test), encoding="utf-8")
        subprocess.run([args.promtool, "check", "rules", str(rule_file)], check=True)
        subprocess.run([args.promtool, "test", "rules", str(test_file)], check=True)


if __name__ == "__main__":
    main()
