"""Validate lab rules and OOM timing with local promtool; no cluster mutations."""

import argparse
import subprocess
from pathlib import Path

import yaml


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--promtool", default="promtool", help="Path to the Prometheus promtool executable")
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[1]
    documents = yaml.safe_load_all((root / "deploy/kubernetes/observability.yaml").read_text())
    spec = next(item["spec"] for item in documents if item["kind"] == "PrometheusRule")
    rule = next(rule for group in spec["groups"] for rule in group["rules"] if rule["alert"] == "LabWorkerOOMKilled")
    command = [args.promtool]
    subprocess.run(command + ["check", "rules", "/dev/stdin"], input=yaml.safe_dump(spec), text=True, check=True)
    series = []
    expected = []
    for pod, oom, timestamp, restarts, waiting, fires in (
        ("oom-backoff", 1, "120+0x5", "5+0x5", 1, True),
        ("stale-oom", 1, "-600+0x5", "5+0x5", 0, False),
        ("ordinary-crash", 0, "0+0x5", "0+1x5", 1, False),
        ("oom-restarted", 1, "120+0x5", "0+1x5", 0, True),
    ):
        labels = f'namespace="fcapsule-lab",pod="{pod}",container="worker"'
        reason = 'kube_pod_container_status_last_terminated_reason{' + labels + ',reason="OOMKilled"}'
        series.extend([
            {"series": reason, "values": f"{oom}+0x5"},
            {"series": "kube_pod_container_status_last_terminated_timestamp{" + labels + "}", "values": timestamp},
            {"series": "kube_pod_container_status_restarts_total{" + labels + "}", "values": restarts},
            {"series": "kube_pod_container_status_waiting_reason{" + labels + ',reason="CrashLoopBackOff"}', "values": f"{waiting}+0x5"},
        ])
        if fires:
            expected.append({"labels": reason, "value": 1})
    test = {"evaluation_interval": "1m", "tests": [{"name": "OOM during backoff without a new restart, and stale-state negatives",
            "interval": "1m", "input_series": series, "promql_expr_test": [{"expr": rule["expr"], "eval_time": "3m", "exp_samples": expected}]}]}
    subprocess.run(command + ["test", "rules", "/dev/stdin"], input=yaml.safe_dump(test), text=True, check=True)


if __name__ == "__main__":
    main()
