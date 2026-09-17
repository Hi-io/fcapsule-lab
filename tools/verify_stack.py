"""Verify the running Compose lab without needing a browser."""

from __future__ import annotations

import json
import subprocess
import sys
from urllib.parse import urlencode
from urllib.request import urlopen


def get_json(url: str) -> dict:
    with urlopen(url, timeout=5) as response:
        return json.loads(response.read().decode("utf-8"))


def prometheus_query(expression: str) -> list[dict]:
    payload = get_json(f"http://127.0.0.1:9090/api/v1/query?{urlencode({'query': expression})}")
    if payload.get("status") != "success":
        raise RuntimeError(payload)
    return payload.get("data", {}).get("result", [])


def main() -> int:
    checks = {
        "orders-api": "http://127.0.0.1:8080/health",
        "inventory-api": "http://127.0.0.1:8081/health",
        "traffic-generator": "http://127.0.0.1:8082/health",
    }
    for name, url in checks.items():
        print(f"{name}: {json.dumps(get_json(url))}")

    targets = get_json("http://127.0.0.1:9090/api/v1/targets")
    active = targets.get("data", {}).get("activeTargets", [])
    unhealthy = [item.get("labels", {}).get("job", "unknown") for item in active if item.get("health") != "up"]
    if unhealthy:
        raise RuntimeError(f"Prometheus targets are not healthy: {', '.join(unhealthy)}")
    print(f"prometheus: {len(active)} scrape targets healthy")

    traffic = prometheus_query("sum(rate(traffic_generator_requests_total[1m]))")
    if not traffic:
        raise RuntimeError("Prometheus has not observed traffic yet")
    print(f"traffic rate: {traffic[0]['value'][1]} requests/second")

    logs = subprocess.run(
        ["docker", "compose", "logs", "--since", "1m", "--no-log-prefix"],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.splitlines()
    json_logs = sum(1 for line in logs if line.lstrip().startswith("{"))
    print(f"structured logs in the previous minute: {json_logs}")
    if json_logs < 10000:
        raise RuntimeError("Log volume is below the 10,000 events/minute target")
    print("verification: passed")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(f"verification failed: {exc}", file=sys.stderr)
        raise SystemExit(1)
