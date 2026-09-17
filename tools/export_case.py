"""Export one bounded Compose incident window as FCAPSule's normalized case contract.

The exporter reads observable boundaries: Prometheus for FM/PM and Docker's stdout
log stream for application logs. It does not import FCAPSule or share its state.
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlencode
from urllib.request import urlopen


ROOT = Path(__file__).resolve().parents[1]
PROMETHEUS = "http://127.0.0.1:9090"
METRIC_QUERIES = (
    "orders_checkout_requests_total",
    "orders_checkout_latency_p95_seconds",
    "orders_retry_amplification_ratio",
    "inventory_database_failures_total",
    "inventory_active_transactions",
    "traffic_generator_shed_requests_total",
)


def utc_timestamp(value: float) -> str:
    return datetime.fromtimestamp(value, tz=timezone.utc).isoformat().replace("+00:00", "Z")


def fetch_json(url: str) -> dict[str, Any]:
    with urlopen(url, timeout=10) as response:
        value = json.loads(response.read().decode("utf-8"))
    if value.get("status") != "success":
        raise RuntimeError(f"Prometheus request failed: {value}")
    return value["data"]


def fetch_alerts(prometheus: str) -> list[dict[str, Any]]:
    alerts = fetch_json(f"{prometheus.rstrip('/')}/api/v1/alerts").get("alerts", [])
    converted: list[dict[str, Any]] = []
    for alert in alerts:
        labels = dict(alert.get("labels", {}))
        converted.append(
            {
                "alertname": labels.pop("alertname", "PrometheusAlert"),
                "status": str(alert.get("state", "firing")),
                "severity": str(labels.get("severity", "warning")),
                "startsAt": str(alert.get("activeAt") or datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")),
                "endsAt": None,
                "labels": labels,
                "annotations": dict(alert.get("annotations", {})),
            }
        )
    return converted


def fetch_metrics(prometheus: str, start: datetime, end: datetime) -> list[dict[str, Any]]:
    endpoint = f"{prometheus.rstrip('/')}/api/v1/query_range"
    series: list[dict[str, Any]] = []
    for query in METRIC_QUERIES:
        parameters = urlencode({"query": query, "start": start.timestamp(), "end": end.timestamp(), "step": "5s"})
        payload = fetch_json(f"{endpoint}?{parameters}")
        for result in payload.get("result", []):
            labels = {str(key): str(value) for key, value in result.get("metric", {}).items() if key != "__name__"}
            values = [[utc_timestamp(float(point[0])), float(point[1])] for point in result.get("values", [])]
            if len(values) >= 2:
                series.append({"metric": str(result.get("metric", {}).get("__name__", query)), "labels": labels, "values": values})
    return series


def parse_compose_logs(start: datetime) -> list[dict[str, Any]]:
    command = [
        "docker", "compose", "logs", "--no-color", "--since", start.isoformat(),
        "inventory-api", "orders-api", "traffic-generator",
    ]
    result = subprocess.run(command, cwd=ROOT, capture_output=True, text=True, check=False)
    if result.returncode:
        raise RuntimeError(result.stderr.strip() or "docker compose logs failed")
    hits: list[dict[str, Any]] = []
    for line in result.stdout.splitlines():
        marker = line.find("{")
        if marker < 0:
            continue
        try:
            value = json.loads(line[marker:])
        except json.JSONDecodeError:
            continue
        if isinstance(value, dict) and "@timestamp" in value and "message" in value:
            hits.append(value)
    return hits


def write_metadata(path: Path, case_id: str, title: str, start: datetime, end: datetime) -> None:
    text = "\n".join(
        [
            f"case_id: {case_id}", f"case_title: {json.dumps(title)}", "service: orders-api", "namespace: fcapsule-lab",
            "cluster: local-compose", "pod: orders-api", "window:",
            f"  start: '{start.isoformat().replace('+00:00', 'Z')}'",
            f"  end: '{end.isoformat().replace('+00:00', 'Z')}'", "timezone: UTC", "telemetry_sources:",
            "  logs: opensearch_logs.json", "  metrics: prometheus_metrics.json", "  alert: alert.json", "fields:",
            "  log_time_field: '@timestamp'", "  log_message_field: message", "  log_level_field: level", "  service_label: service",
            "trace_access:", "  available: false", "  raw_spans_retained: false", "notes:",
            "- Exported from FCAPSule Lab observable boundaries; raw sources remain outside FCAPSule.", "",
        ]
    )
    (path / "metadata.yaml").write_text(text, encoding="utf-8")


def export_case(output: Path, minutes: int, prometheus: str = PROMETHEUS) -> dict[str, Any]:
    if minutes < 1 or minutes > 30:
        raise ValueError("minutes must be between 1 and 30")
    end = datetime.now(timezone.utc)
    start = end - timedelta(minutes=minutes)
    alerts = fetch_alerts(prometheus)
    if not alerts:
        raise RuntimeError("No firing Prometheus alerts were found; enable a failure and wait for an alert rule first")
    metrics = fetch_metrics(prometheus, start, end)
    if not metrics:
        raise RuntimeError("Prometheus returned no usable metric series for the requested window")
    logs = parse_compose_logs(start)
    if not logs:
        raise RuntimeError("Docker returned no structured application logs for the requested window")
    case_id = f"lab-{end.strftime('%Y%m%dT%H%M%SZ')}"
    output = output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    title = str(alerts[0].get("annotations", {}).get("summary", "FCAPSule Lab incident"))
    write_metadata(output, case_id, title, start, end)
    (output / "alert.json").write_text(json.dumps(alerts, indent=2) + "\n", encoding="utf-8")
    (output / "prometheus_metrics.json").write_text(json.dumps({"series": metrics}, indent=2) + "\n", encoding="utf-8")
    (output / "opensearch_logs.json").write_text(json.dumps({"hits": logs}, indent=2) + "\n", encoding="utf-8")
    return {"case_dir": str(output), "case_id": case_id, "alerts": len(alerts), "series": len(metrics), "logs": len(logs)}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Export one FCAPSule Lab incident window")
    parser.add_argument("--out", required=True, help="New directory for the normalized case")
    parser.add_argument("--minutes", type=int, default=2, help="Bounded lookback window (1-30)")
    parser.add_argument("--prometheus", default=PROMETHEUS)
    args = parser.parse_args(argv)
    try:
        print(json.dumps(export_case(Path(args.out), args.minutes, args.prometheus), indent=2))
        return 0
    except (OSError, RuntimeError, ValueError, json.JSONDecodeError) as exc:
        print(f"Export failed: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
