"""Run bounded Kubernetes cases and retain evidence independently of FCAPSule."""

import argparse
import json
import shutil
import subprocess
import sys
import time
import uuid
from datetime import datetime, timezone
from urllib.error import HTTPError, URLError
from pathlib import Path
from urllib.parse import urlencode
from urllib.request import Request, urlopen

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.scenario_catalog import SCENARIOS

EXPECTED = {key: item["expected_alert"] for key, item in SCENARIOS.items()}
KUBECTL = shutil.which("kubectl") or "/snap/bin/kubectl"
TERMINAL = {"ready", "incomplete", "inconclusive", "not_configured", "failed", "blocked"}


def now():
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def request(url, payload=None):
    req = Request(url, data=json.dumps(payload).encode() if payload is not None else None,
                  headers={"Content-Type": "application/json"})
    # Status reads are idempotent. A short retry keeps one temporary proxy or
    # API-server reset from invalidating a complete evaluation case. Mutations
    # deliberately remain single-shot to avoid duplicating a fault injection.
    attempts = 3 if payload is None else 1
    for attempt in range(attempts):
        try:
            with urlopen(req, timeout=20) as response:
                return json.loads(response.read())
        except (URLError, OSError, TimeoutError) as error:
            if attempt + 1 == attempts:
                raise error
            time.sleep(attempt + 1)
    raise RuntimeError("Unreachable request retry state")


def save(path, value):
    path.write_text(json.dumps(value, indent=2) + "\n")


def firing(prometheus):
    return [alert for alert in request(prometheus + "/api/v1/alerts")["data"]["alerts"]
            if alert["state"] == "firing" and alert["labels"].get("namespace") == "fcapsule-lab"
            and alert["labels"].get("alertname", "").startswith("Lab")]


def alert_identity(alert):
    """Identify one Prometheus alert lifecycle without relying on replica labels."""
    labels = alert.get("labels", {})
    return (
        labels.get("alertname", ""),
        labels.get("pod", ""),
        labels.get("container", ""),
        labels.get("service", ""),
        alert.get("activeAt", ""),
    )


def expected_alerts(alerts, expected):
    return [alert for alert in alerts if alert.get("labels", {}).get("alertname") == expected]


def fresh_expected_alerts(alerts, expected, baseline):
    baseline_identities = {alert_identity(alert) for alert in baseline}
    return [alert for alert in expected_alerts(alerts, expected)
            if alert_identity(alert) not in baseline_identities]


def request_investigation(fcapsule, episode_id, requested, incident_id=None):
    """Prefer the automatic pass; issue one explicit correction only if membership is missing."""
    url = fcapsule.rstrip("/") + "/api/episodes/" + episode_id + "/investigation"
    request_key = (episode_id, incident_id or "")
    current = request(url)
    if current.get("status") not in TERMINAL:
        return current
    if not incident_id:
        return current
    if incident_id and any(isinstance(item, dict) and item.get("incident_id") == incident_id
                           for item in ((current.get("context") or {}).get("alerts") or [])):
        return current
    if request_key not in requested:
        requested.add(request_key)
        return request(url, {"incident_id": incident_id} if incident_id else {})
    return current


def start_scenario(lab, scenario, duration, request_id=None):
    """Make one owned mutation request; reconcile transport ambiguity without retry."""
    request_id = request_id or uuid.uuid4().hex
    try:
        result = request(lab + f"/api/scenarios/{scenario}/start",
                         {"duration_seconds": duration, "request_id": request_id})
    except (URLError, OSError, TimeoutError) as exc:
        status = request(lab + "/api/status")
        active = status.get("active") or {}
        if active.get("run_id") == request_id and active.get("scenario") == scenario:
            return {"ok": True, "message": "The original owned start was confirmed after a lost response.", "run": active}
        if active:
            raise RuntimeError("A different Lab run is active; no second injection was attempted") from exc
        raise RuntimeError("Start response was lost and no matching owned run exists; no reinjection attempted") from exc
    if result.get("run", {}).get("run_id") != request_id:
        raise RuntimeError("Lab did not acknowledge the requested ownership ID")
    return result


def healthy(lab):
    state = request(lab + "/api/status")
    return not state["active"] and all(state[key].get("reachable") for key in ("worker", "inventory", "orders"))


def settle(args):
    # Kubernetes CrashLoopBackOff recovery can legitimately take roughly five
    # minutes after a poison workload has been cleared. This is independent of
    # Prometheus alert resolution, which is handled per scenario below.
    deadline = time.monotonic() + getattr(args, "settle_timeout", 480)
    while time.monotonic() < deadline:
        # Prometheus may legitimately retain an unrelated alert while the workload
        # itself has recovered. Scenario-specific freshness is checked separately.
        if healthy(args.lab):
            return
        time.sleep(5)
    raise RuntimeError("Previous workload did not recover before the next case; no fault injected")


def wait_for_expected_clear(args, expected):
    """Wait only for the prior lifecycle of this case's own alert to resolve."""
    deadline = time.monotonic() + getattr(args, "alert_clear_timeout", 180)
    while time.monotonic() < deadline:
        if not expected_alerts(firing(args.prometheus), expected):
            return
        time.sleep(5)
    raise RuntimeError(f"Expected alert {expected} did not resolve before its next evaluation")


def wait_for_lab_quiet(args):
    """Do not inject a new benchmark fault while a prior Lab alert is firing."""
    deadline = time.monotonic() + getattr(args, "lab_quiet_timeout", 420)
    while time.monotonic() < deadline:
        if not firing(args.prometheus):
            return
        time.sleep(5)
    names = sorted({item.get("labels", {}).get("alertname", "unknown") for item in firing(args.prometheus)})
    raise RuntimeError("Prior Lab alerts did not resolve before the next evaluation: " + ", ".join(names))


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
    wait_for_lab_quiet(args)
    wait_for_expected_clear(args, EXPECTED[scenario])
    baseline = now()
    time.sleep(args.baseline)
    if not healthy(args.lab):
        raise RuntimeError("Healthy baseline did not hold; no fault injected")
    baseline_alerts = firing(args.prometheus)
    baseline_expected = expected_alerts(baseline_alerts, EXPECTED[scenario])
    if baseline_expected:
        raise RuntimeError(f"Expected alert {EXPECTED[scenario]} was already firing before fault injection")
    start = now()
    owner_run_id = uuid.uuid4().hex
    record = {"scenario": scenario, "expected_alert": EXPECTED[scenario], "baseline_at": baseline, "started_at": start,
              "samples": [], "observed_alerts": [], "fcapsule": [], "outcome": "running",
              "owner_run_id": owner_run_id,
              "minimum_retained_log_lines": args.minimum_log_lines,
              "baseline_firing_alerts": baseline_alerts}
    save(root / "run.json", record)
    try:
        save(root / "injection-attempt.json", {"at": start, "owner_run_id": owner_run_id, "scenario": scenario})
        record["control"] = start_scenario(args.lab, scenario, args.duration, owner_run_id)
        print(f"{now()} {scenario}: started {record['control']['run']['run_id']}", flush=True)
        deadline = time.monotonic() + args.duration
        observed = {}
        observed_expected = {}
        expected_seen_at = None
        while time.monotonic() < deadline:
            status = request(args.lab + "/api/status")
            alerts = firing(args.prometheus)
            for alert in alerts:
                observed[alert_identity(alert)] = alert
            fresh_expected = fresh_expected_alerts(alerts, EXPECTED[scenario], baseline_expected)
            for alert in fresh_expected:
                observed_expected[alert_identity(alert)] = alert
            record["samples"].append({"time": now(), "memory": status.get("memory"), "active": status["active"], "alerts": [item["labels"]["alertname"] for item in alerts]})
            record["observed_alerts"] = list(observed.values())
            record["observed_expected_alerts"] = list(observed_expected.values())
            save(root / "run.json", record)
            if fresh_expected:
                expected_seen_at = expected_seen_at or time.monotonic()
            if expected_seen_at and time.monotonic() - expected_seen_at >= args.post_alert_hold:
                break
            if not status["active"]:
                last = status.get("history", [])[-1:]
                record["early_recovery"] = last
                break
            time.sleep(10)
        record["alert_observed"] = bool(observed_expected)
    finally:
        if record.get("control", {}).get("run", {}).get("run_id") == owner_run_id:
            record["recovery"] = request(args.lab + "/api/recover", {"expected_run_id": owner_run_id})
        else:
            record["recovery"] = {"ok": True, "message": "No owned run was confirmed; no recovery write attempted."}
        record["fault_ended_at"] = now()
        save(root / "run.json", record)
    record["logs"] = collect_workload_logs(root, baseline)
    log_query = 'sum(increase({__name__=~"(orders|inventory|traffic_generator|lab_worker)_log_events_total",namespace="fcapsule-lab"}[' + str(args.duration + args.baseline) + 's]))'
    record["emitted_log_estimate"] = request(args.prometheus + "/api/v1/query?" + urlencode({"query": log_query}))
    deadline = time.monotonic() + 240
    requested_investigations = set()
    while time.monotonic() < deadline:
        overview = request(args.fcapsule + "/api/state")["overview"]
        episodes = []
        for episode in overview["episodes"]:
            matches = [
                signal for signal in episode["signals"]
                if signal.get("created_at", "") >= start
                and "fcapsule-lab" in signal.get("app_id", "")
                and EXPECTED[scenario].casefold() in str(signal.get("incident_id", "")).casefold()
                and signal.get("report_ready")
            ]
            if matches:
                episodes.append((episode, max(matches, key=lambda signal: signal.get("created_at", ""))))
        results = []
        for episode, signal in episodes:
            result = request_investigation(
                args.fcapsule,
                episode["episode_id"],
                requested_investigations,
                signal.get("incident_id"),
            )
            save(root / (episode["episode_id"] + ".json"), result)
            results.append({
                "episode_id": episode["episode_id"],
                "incident_id": signal.get("incident_id"),
                "status": result["status"],
                "attempt": result.get("attempt"),
                "usage": result.get("usage"),
                "assessment_context_contains_incident": any(
                    isinstance(item, dict) and item.get("incident_id") == signal.get("incident_id")
                    for item in ((result.get("context") or {}).get("alerts") or [])),
            })
        record["fcapsule"] = results
        save(root / "run.json", record)
        if results and all(item["status"] in {"ready", "incomplete", "inconclusive", "not_configured", "failed", "blocked"}
                           and item["assessment_context_contains_incident"] for item in results):
            break
        time.sleep(10)
    record["assessment_pipeline_complete"] = bool(record.get("fcapsule")) and all(
        item.get("assessment_context_contains_incident") and item.get("status") in
        {"ready", "incomplete", "inconclusive", "not_configured", "failed", "blocked"}
        for item in record.get("fcapsule", []))
    record["outcome"] = ("captured" if record.get("alert_observed") and record["assessment_pipeline_complete"]
                         else "assessment_or_alert_incomplete")
    record["finished_at"] = now()
    save(root / "run.json", record)
    print(f"{now()} {scenario}: {record['outcome']}; agent states {[item['status'] for item in record['fcapsule']]}", flush=True)
    if not record.get("recovery", {}).get("ok"):
        raise RuntimeError("Owned recovery was not confirmed; stop the suite before another case")
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
    parser.add_argument("--lab-quiet-timeout", type=int, default=420)
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
