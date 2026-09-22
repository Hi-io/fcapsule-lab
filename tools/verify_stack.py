"""Verify the running Kubernetes Lab without changing its workload state."""

from __future__ import annotations

import argparse
import json
import shutil
import subprocess
import sys
from urllib.parse import urlencode
from urllib.request import urlopen


KUBECTL = shutil.which("kubectl") or "/snap/bin/kubectl"
COMPONENTS = ("worker", "inventory", "orders")


def get_json(url: str) -> dict:
    with urlopen(url, timeout=10) as response:
        return json.loads(response.read().decode("utf-8"))


def query(prometheus: str, expression: str) -> list[dict]:
    payload = get_json(prometheus.rstrip("/") + "/api/v1/query?" + urlencode({"query": expression}))
    if payload.get("status") != "success":
        raise RuntimeError(f"Prometheus rejected the query: {payload}")
    return payload.get("data", {}).get("result", [])


def deployment_state(namespace: str) -> list[dict]:
    result = subprocess.run(
        [KUBECTL, "get", "deployments", "-n", namespace, "-o", "json"],
        check=True,
        capture_output=True,
        text=True,
        timeout=30,
    )
    return json.loads(result.stdout).get("items", [])


def scalar(rows: list[dict], label: str) -> float:
    if len(rows) != 1:
        raise RuntimeError(f"Expected one result for {label}; received {len(rows)}")
    return float(rows[0]["value"][1])


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--lab", default="http://192.168.0.102:30766")
    parser.add_argument("--prometheus", default="http://192.168.0.102:30090")
    parser.add_argument("--namespace", default="fcapsule-lab")
    parser.add_argument("--minimum-log-events-per-minute", type=float, default=10_000)
    parser.add_argument("--minimum-traffic-per-second", type=float, default=1)
    args = parser.parse_args()

    status = get_json(args.lab.rstrip("/") + "/api/status")
    unavailable = [name for name in COMPONENTS if not status.get(name, {}).get("reachable")]
    if unavailable:
        raise RuntimeError("Lab controller cannot reach: " + ", ".join(unavailable))
    if status.get("active"):
        raise RuntimeError("A scenario is active; recover it before stack verification")
    print("lab controller: worker, inventory and orders reachable")

    deployments = deployment_state(args.namespace)
    unhealthy = []
    for item in deployments:
        name = item["metadata"]["name"]
        desired = int(item.get("spec", {}).get("replicas") or 0)
        ready = int(item.get("status", {}).get("readyReplicas") or 0)
        if desired != ready:
            unhealthy.append(f"{name} ({ready}/{desired})")
    if unhealthy:
        raise RuntimeError("Deployments are not ready: " + ", ".join(unhealthy))
    print(f"kubernetes: {len(deployments)} deployments ready")

    targets = query(args.prometheus, f'up{{namespace="{args.namespace}"}}')
    down = [row for row in targets if float(row["value"][1]) != 1.0]
    if len(targets) < 5 or down:
        raise RuntimeError(f"Prometheus targets unhealthy: {len(targets)} found, {len(down)} down")
    print(f"prometheus: {len(targets)} Lab targets healthy")

    traffic = scalar(query(args.prometheus, f'sum(rate(traffic_generator_requests_total{{namespace="{args.namespace}"}}[1m]))'), "traffic rate")
    if traffic < args.minimum_traffic_per_second:
        raise RuntimeError(f"Traffic rate is below {args.minimum_traffic_per_second:g} requests/second: {traffic:g}")
    print(f"traffic rate: {traffic:.2f} requests/second")

    log_expression = (
        'sum(rate({__name__=~"(orders|inventory|traffic_generator|lab_worker)_log_events_total",'
        f'namespace="{args.namespace}"}}[1m]))'
    )
    log_rate = scalar(query(args.prometheus, log_expression), "log rate")
    log_events_per_minute = log_rate * 60
    if log_events_per_minute < args.minimum_log_events_per_minute:
        raise RuntimeError(
            f"Structured log rate is below {args.minimum_log_events_per_minute:g} events/minute: "
            f"{log_events_per_minute:.0f}"
        )
    print(f"structured log rate: {log_events_per_minute:.0f} events/minute")
    print("verification: passed")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(f"verification failed: {exc}", file=sys.stderr)
        raise SystemExit(1)
