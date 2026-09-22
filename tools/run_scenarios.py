"""Run bounded Kubernetes cases and retain evidence independently of FCAPSule."""

import argparse
import json
import shutil
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlencode
from urllib.request import Request, urlopen

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.scenario_catalog import SCENARIOS

EXPECTED = {key: item["expected_alert"] for key, item in SCENARIOS.items()}
KUBECTL = shutil.which("kubectl") or "/snap/bin/kubectl"


def now():
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def request(url, payload=None):
    req = Request(url, data=json.dumps(payload).encode() if payload is not None else None,
                  headers={"Content-Type": "application/json"})
    with urlopen(req, timeout=20) as response:
        return json.loads(response.read())


def save(path, value):
    path.write_text(json.dumps(value, indent=2) + "\n")


def firing(prometheus):
    return [alert for alert in request(prometheus + "/api/v1/alerts")["data"]["alerts"]
            if alert["state"] == "firing" and alert["labels"].get("namespace") == "fcapsule-lab"
            and alert["labels"].get("alertname", "").startswith("Lab")]


def healthy(lab):
    state = request(lab + "/api/status")
    return not state["active"] and all(state[key].get("reachable") for key in ("worker", "inventory"))


def settle(args):
    deadline = time.monotonic() + 480
    while time.monotonic() < deadline:
        if healthy(args.lab) and not firing(args.prometheus):
            return
        time.sleep(10)
    raise RuntimeError("Previous workload/alert window did not recover; no next case started")


def collect_workload_logs(root, start):
    counts = {}
    for name in ("orders-api", "inventory-api", "lab-worker", "traffic-generator", "mysql", "mysql-exporter"):
        command = [KUBECTL, "logs", "-n", "fcapsule-lab", "deployment/" + name, "--since-time=" + start, "--tail=12000", "--timestamps"]
        result = subprocess.run(command, capture_output=True, text=True, timeout=25)
        (root / (name + ".log")).write_text(result.stdout)
        counts[name] = {"retained_lines": len(result.stdout.splitlines()), "limit": 12000, "ok": result.returncode == 0}
        if name == "lab-worker":
            previous = subprocess.run(command + ["--previous"], capture_output=True, text=True, timeout=25)
            (root / "lab-worker-previous.log").write_text(previous.stdout)
            counts["lab-worker-previous"] = {"retained_lines": len(previous.stdout.splitlines()), "ok": previous.returncode == 0}
    return counts


def run_case(args, scenario, folder):
    root = folder / scenario
    root.mkdir()
    print(f"{now()} {scenario}: healthy baseline ({args.baseline}s)", flush=True)
    settle(args)
    baseline = now()
    time.sleep(args.baseline)
    if not healthy(args.lab) or firing(args.prometheus):
        raise RuntimeError("Healthy baseline did not hold; no fault injected")
    start = now()
    record = {"scenario": scenario, "expected_alert": EXPECTED[scenario], "baseline_at": baseline, "started_at": start,
              "samples": [], "observed_alerts": [], "fcapsule": [], "outcome": "running",
              "minimum_retained_log_lines": args.minimum_log_lines}
    save(root / "run.json", record)
    try:
        record["control"] = request(args.lab + f"/api/scenarios/{scenario}/start", {"duration_seconds": args.duration})
        print(f"{now()} {scenario}: started {record['control']['run']['run_id']}", flush=True)
        deadline = time.monotonic() + args.duration
        observed = {}
        expected_seen_at = None
        while time.monotonic() < deadline:
            status = request(args.lab + "/api/status")
            alerts = firing(args.prometheus)
            for alert in alerts:
                observed[alert["labels"]["alertname"] + str(alert["labels"].get("pod"))] = alert
            record["samples"].append({"time": now(), "memory": status.get("memory"), "active": status["active"], "alerts": [item["labels"]["alertname"] for item in alerts]})
            record["observed_alerts"] = list(observed.values())
            save(root / "run.json", record)
            if any(item["labels"]["alertname"] == EXPECTED[scenario] for item in alerts):
                expected_seen_at = expected_seen_at or time.monotonic()
            if expected_seen_at and time.monotonic() - expected_seen_at >= args.post_alert_hold:
                break
            if not status["active"]:
                last = status.get("history", [])[-1:]
                record["early_recovery"] = last
                break
            time.sleep(10)
        record["alert_observed"] = any(item["labels"]["alertname"] == EXPECTED[scenario] for item in observed.values())
    finally:
        record["recovery"] = request(args.lab + "/api/recover", {})
        record["fault_ended_at"] = now()
        save(root / "run.json", record)
    record["logs"] = collect_workload_logs(root, baseline)
    log_query = 'sum(increase({__name__=~"(orders|inventory|traffic_generator|lab_worker)_log_events_total",namespace="fcapsule-lab"}[' + str(args.duration + args.baseline) + 's]))'
    record["emitted_log_estimate"] = request(args.prometheus + "/api/v1/query?" + urlencode({"query": log_query}))
    deadline = time.monotonic() + 240
    while time.monotonic() < deadline:
        overview = request(args.fcapsule + "/api/state")["overview"]
        episodes = [episode for episode in overview["episodes"] if any(
            signal.get("created_at", "") >= start
            and "fcapsule-lab" in signal.get("app_id", "")
            and EXPECTED[scenario].casefold() in json.dumps(signal).casefold()
            for signal in episode["signals"])]
        results = []
        for episode in episodes:
            result = request(args.fcapsule + "/api/episodes/" + episode["episode_id"] + "/investigation")
            save(root / (episode["episode_id"] + ".json"), result)
            results.append({"episode_id": episode["episode_id"], "status": result["status"], "attempt": result.get("attempt"), "usage": result.get("usage")})
        record["fcapsule"] = results
        save(root / "run.json", record)
        if results and all(item["status"] in {"ready", "incomplete", "inconclusive", "not_configured"} for item in results):
            break
        time.sleep(10)
    record["outcome"] = "symptom_alert_observed" if record.get("alert_observed") else "expected_alert_missing"
    record["finished_at"] = now()
    save(root / "run.json", record)
    print(f"{now()} {scenario}: {record['outcome']}; agent states {[item['status'] for item in record['fcapsule']]}", flush=True)
    settle(args)
    return record


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--lab", default="http://192.168.0.102:30766")
    parser.add_argument("--prometheus", default="http://192.168.0.102:30090")
    parser.add_argument("--fcapsule", default="http://192.168.0.102:30765")
    parser.add_argument("--scenario", choices=[*EXPECTED, "all"], default="all")
    parser.add_argument("--duration", type=int, default=180, choices=range(120, 301))
    parser.add_argument("--baseline", type=int, default=120)
    parser.add_argument("--post-alert-hold", type=int, default=30)
    parser.add_argument("--minimum-log-lines", type=int, default=1000)
    parser.add_argument("--out", type=Path, default=Path("artifacts"))
    args = parser.parse_args()
    folder = args.out / datetime.now(timezone.utc).strftime("validation-%Y%m%dT%H%M%SZ")
    folder.mkdir(parents=True)
    results = []
    try:
        for scenario in EXPECTED if args.scenario == "all" else [args.scenario]:
            results.append(run_case(args, scenario, folder))
            save(folder / "summary.json", results)
    finally:
        request(args.lab + "/api/recover", {})
    print(str(folder.resolve()), flush=True)


if __name__ == "__main__":
    main()
