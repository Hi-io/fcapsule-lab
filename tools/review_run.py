"""Summarize retained observations without grading or rewriting model answers."""

import argparse
import collections
import json
from pathlib import Path
from urllib.parse import urlencode
from urllib.request import urlopen


QUERIES = {
    "worker_cpu_cores": 'sum by(pod)(rate(container_cpu_usage_seconds_total{namespace="fcapsule-lab",container="worker",image!=""}[1m]))',
    "worker_cpu_limit": 'kube_pod_container_resource_limits{namespace="fcapsule-lab",container="worker",resource="cpu"}',
    "worker_restarts": 'kube_pod_container_status_restarts_total{namespace="fcapsule-lab",container="worker"}',
    "worker_oom_state": 'kube_pod_container_status_last_terminated_reason{namespace="fcapsule-lab",container="worker",reason="OOMKilled"}',
    "worker_memory": 'container_memory_working_set_bytes{namespace="fcapsule-lab",container="worker",image!=""}',
    "mysql_connections": 'mysql_global_status_threads_connected{namespace="fcapsule-lab"}',
    "mysql_limit": 'mysql_global_variables_max_connections{namespace="fcapsule-lab"}',
    "inventory_failures": 'inventory_database_failures_total{namespace="fcapsule-lab"}',
}


def structured_logs(path):
    for line in path.read_text(errors="replace").splitlines():
        body = line.split(" ", 1)[-1] if not line.startswith("{") else line
        try:
            row = json.loads(body)
        except ValueError:
            continue
        if isinstance(row, dict):
            yield row


def observations(folder):
    codes = collections.Counter()
    operations = collections.Counter()
    jobs = collections.Counter()
    maximum_buffer = 0
    seen = set()
    for path in folder.glob("*.log"):
        for row in structured_logs(path):
            # During backoff, current and --previous can return the same container log.
            if row.get("@timestamp"):
                identity = json.dumps(row, sort_keys=True)
                if identity in seen:
                    continue
                seen.add(identity)
            if row.get("mysql_error_code") is not None:
                codes[str(row["mysql_error_code"])] += 1
            if row.get("operation"):
                operations[str(row["operation"])] += 1
            if row.get("message") == "Import delivery received":
                jobs[str(row.get("job_id"))] += 1
            maximum_buffer = max(maximum_buffer, row.get("buffered_bytes", 0))
    return {"sql_error_codes": dict(codes), "operations": dict(operations),
            "import_deliveries_by_job": dict(jobs), "largest_logged_buffer_bytes": maximum_buffer,
            "limitation": "Counts describe deduplicated retained bounded log files, not all indexed logs. Missing records are not disproof."}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("folder", type=Path)
    parser.add_argument("--prometheus", default="http://192.168.0.102:30090")
    args = parser.parse_args()
    summaries = []
    for path in sorted(args.folder.glob("*/run.json")):
        run = json.loads(path.read_text())
        if not run.get("fault_ended_at"):
            continue
        series = {}
        for name, expression in QUERIES.items():
            url = args.prometheus.rstrip("/") + "/api/v1/query_range?" + urlencode({
                "query": expression, "start": run["baseline_at"], "end": run["fault_ended_at"], "step": 15})
            with urlopen(url, timeout=20) as response:
                payload = json.loads(response.read())
            if payload.get("status") != "success":
                raise RuntimeError("Historical metrics query failed")
            series[name] = payload["data"]["result"]
        (path.parent / "independent-metrics.json").write_text(json.dumps(series, indent=2) + "\n")
        memory = [sample["memory"]["available_bytes"] for sample in run["samples"] if sample.get("memory")]
        summaries.append({"scenario": run["scenario"], "outcome": run["outcome"],
                          "minimum_sampled_host_available_bytes": min(memory) if memory else None,
                          "observations": observations(path.parent), "logs": run.get("logs"),
                          "agent_runs": run.get("fcapsule"),
                          "note": "A ready response and a firing symptom alert are not proof of correct diagnosis."})
    output = args.folder / "observation-summary.json"
    output.write_text(json.dumps(summaries, indent=2) + "\n")
    print(output.resolve())


if __name__ == "__main__":
    main()
