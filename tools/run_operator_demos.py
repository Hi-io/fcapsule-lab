"""Run scenario evaluations with explicit mutation and paid-call gates.

Plan/preflight are read-only. Every run uses fresh output directories; ambiguous
POSTs and failed assessments are never automatically retried. Oracle data stays
in this repository and the private run records, not FCAPSule request payloads.
"""

from __future__ import annotations

import argparse
import base64
from datetime import datetime, timezone
import hashlib
import json
import math
from pathlib import Path
import subprocess
import sys
import time
from types import SimpleNamespace
from urllib.parse import quote, urlencode, urlparse
import uuid

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.demo_catalog import DEMO_CASES
from app.scenario_catalog import DEFAULT_SCENARIO_CONFIG, DISCOVERY_SCENARIOS, SCENARIOS, public_scenarios
from tools import evaluate_external_screenshot as media
from tools.run_scenarios import firing, now, request, save, wait_for_lab_quiet

TERMINAL = media.TERMINAL
CONFIG_KEYS = ("provider", "model", "max_tokens", "max_total_tokens", "max_prompt_tokens", "max_checks")
MAXIMUMS = {"max_tokens": 3600, "max_total_tokens": 12000, "max_prompt_tokens": 3200, "max_checks": 1}
SIGNALS = {**SCENARIOS, **DISCOVERY_SCENARIOS}
SIGNALS["mysql-exporter-scrape-path"] = {"expected_alert": media.ALERT}
QUESTIONS = (
    "Using only this earlier retained capsule, what can be established about the incident, "
    "what remains uncertain, and which preserved evidence supports the next check? "
    "Do not treat an earlier assessment as evidence or claim current source access."
)


def read(path):
    return json.loads(path.read_text(encoding="utf-8"))


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def exclusive(path, data):
    with path.open("x", encoding="utf-8") as handle:
        json.dump(data, handle, indent=2)


def require_execute(args):
    if not args.execute:
        raise ValueError("Requires --execute after operator coordination; plan/preflight are read-only")


def configuration(base, expected=None):
    value = request(base.rstrip("/") + "/api/settings/ai")
    if value.get("model") != "deepseek-v4-pro" or value.get("provider") != "deepseek":
        raise ValueError("Keep the existing configured DeepSeek Pro; runner never changes model settings")
    if not value.get("api_key_configured") or value.get("capability", {}).get("status") != "ready":
        raise ValueError("Core capability must already be configured and validated")
    for key, limit in MAXIMUMS.items():
        if not isinstance(value.get(key), int) or not 0 < value[key] <= limit:
            raise ValueError(f"Require the bounded configured {key} <= {limit}")
    frozen = {key: value[key] for key in CONFIG_KEYS}
    if expected is not None and frozen != expected:
        raise ValueError("Model or run budgets changed; refuse a mixed comparison")
    return frozen


def query(base, expression, **params):
    endpoint = "/api/v1/query_range" if "start" in params else "/api/v1/query"
    value = request(base + endpoint + "?" + urlencode({"query": expression, **params}))
    if value.get("status") != "success":
        raise RuntimeError("Prometheus query did not succeed")
    return value


def snapshot(args, root, phase):
    data = media.snapshot(args, root, phase)
    data["config"] = media.get("configmap", "lab-scenario-config")
    data["service"] = media.get("service", "lab-app-metrics")
    data["application_monitor"] = media.get("servicemonitor", "fcapsule-lab-applications")
    data["memory_age"] = query(args.prometheus, "time() - timestamp(node_memory_MemAvailable_bytes)")
    save(root / (phase + ".json"), data)
    return data


def safety(data, lab_node, owner=None, minimum=1024**3):
    active = data["lab"].get("active")
    if active and active.get("run_id") != owner:
        raise RuntimeError("Another operator owns the active run")
    if owner is None:
        media.require_safe(data, minimum)
    else:
        # Readiness can degrade under the owned fault. Preserve that observation;
        # do not confuse intentional application failure with unsafe node pressure.
        if data["lab"].get("memory_error"):
            raise RuntimeError("Hosting-node memory is unavailable")
        media.require_node_headroom(data, minimum)
    if {p["node"] for p in data["pods"]} != {lab_node}:
        raise RuntimeError(f"All Lab pods must remain pinned to {lab_node}; no scheduling changes performed")
    readings = data["memory_age"]["data"]["result"]
    addresses = data["node_addresses"][lab_node]
    ages = [float(item["value"][1]) for item in readings if item["metric"].get("node") == lab_node
            or urlparse("//" + item["metric"].get("instance", "")).hostname in addresses]
    if not ages or any(not math.isfinite(age) or not 0 <= age < 60 for age in ages):
        raise RuntimeError("Hosting-node memory measurement missing or older than 60 seconds")


def baseline_config(data):
    if any(data["config"]["data"].get(k) != v for k, v in DEFAULT_SCENARIO_CONFIG.items()):
        raise RuntimeError("Lab scenario ConfigMap is not at baseline")
    if data["service"]["metadata"].get("labels", {}).get("fcapsule.io/app-metrics") != "true":
        raise RuntimeError("Metrics Service label is not at baseline")
    if data["monitor"]["spec"]["endpoints"][0].get("path") != "/metrics":
        raise RuntimeError("Exporter monitor is not at baseline")
    if media.OWNER in data["monitor"]["metadata"].get("annotations", {}):
        raise RuntimeError("External scrape probe is already owned")


def preflight(args, root, require_owned=True):
    result = {"at": now(), "model_config": configuration(args.fcapsule), "lab_node": args.lab_node,
              "lab": args.lab, "prometheus": args.prometheus, "fcapsule": args.fcapsule}
    data = snapshot(args, root, "preflight-sources")
    safety(data, args.lab_node)
    baseline_config(data)
    status = request(args.lab + "/api/status")
    result["owned_runs_supported"] = bool(status.get("capabilities", {}).get("owned_runs"))
    result["baseline_alerts"] = firing(args.prometheus)
    save(root / "preflight.json", result)
    if require_owned and not result["owned_runs_supported"]:
        raise RuntimeError("Deploy tested Lab revision with owned_runs support before running demos")
    if result["baseline_alerts"]:
        raise RuntimeError("Lab alerts still firing; wait for quiet baseline")
    return result


def capture(args, root, case):
    spec = capture_spec(case)
    if case == "mysql-connections":
        spec = {"view": "graph", "query":
                '{__name__=~"mysql_global_status_threads_connected|mysql_global_variables_max_connections",namespace="fcapsule-lab"}'}
    if not spec:
        return
    subprocess.run([args.node, str(ROOT / "tools/capture_demo.cjs"), args.prometheus,
                    str(root / "fault.png"), json.dumps(spec)], check=True, timeout=55)


def capture_spec(case):
    spec = DEMO_CASES.get(case, {}).get("capture")
    if spec:
        return spec
    if case == "mysql-connections":
        return {"view": "graph", "query":
                '{__name__=~"mysql_global_status_threads_connected|mysql_global_variables_max_connections",namespace="fcapsule-lab"}'}
    return None


def scenario_rounds(case):
    return DEMO_CASES.get(case, {}).get("rounds", 1)


def collect_logs(root, start):
    counts = {}
    for name in ("orders-api", "inventory-api", "lab-worker", "traffic-generator", "mysql", "mysql-exporter"):
        try:
            text = media.kubectl("logs", "deployment/" + name, "-n", "fcapsule-lab", "--since-time=" + start,
                                 "--tail=4000", "--timestamps", raw=True)
            (root / (name + ".log")).write_text(text, encoding="utf-8")
            counts[name] = {"ok": True, "retained_lines": len(text.splitlines()), "tail_limit": 4000}
        except (OSError, subprocess.SubprocessError) as error:
            counts[name] = {"ok": False, "error": str(error)}
    save(root / "log-retention.json", counts)


def promql(case):
    expressions = {
        "checkout_errors": 'sum(rate(orders_checkout_requests_total{namespace="fcapsule-lab",status="503"}[1m]))',
        "checkout_latency": 'orders_checkout_latency_p95_seconds{namespace="fcapsule-lab"}',
        "target_up": 'up{namespace="fcapsule-lab"}',
    }
    if case in {"connection-pressure", "mysql-connections"}:
        spec = DEMO_CASES.get(case, {}).get("capture") or {
            "query": '{__name__=~"mysql_global_status_threads_connected|mysql_global_variables_max_connections",namespace="fcapsule-lab"}'}
        expressions["sessions_and_limit"] = spec["query"]
        expressions["inventory_session_ownership"] = 'inventory_mysql_client_sessions_active{namespace="fcapsule-lab"}'
    if case in {"query-rollout-history", "schema-drift"}:
        expressions["query_errors"] = 'increase(inventory_database_failures_total{namespace="fcapsule-lab",kind="query"}[1m])'
    if case == "checkout-deadline":
        expressions["dependency_timeouts"] = 'increase(orders_dependency_failures_total{namespace="fcapsule-lab",kind="timeout"}[1m])'
    return expressions


def retain_metrics(args, root, case, start):
    for name, expr in promql(case).items():
        save(root / ("metrics-" + name + ".json"), query(args.prometheus, expr, start=start, end=now(), step="10s"))


def matching_signals(state, expected, started):
    cutoff = datetime.fromisoformat(started.replace("Z", "+00:00"))
    matches = []
    for episode in state["overview"]["episodes"]:
        for signal in episode.get("signals", []):
            created = signal.get("created_at")
            if not created or datetime.fromisoformat(created.replace("Z", "+00:00")) < cutoff:
                continue
            if datetime.fromisoformat(signal.get("started_at", created).replace("Z", "+00:00")) < cutoff:
                continue
            if "fcapsule-lab" not in signal.get("app_id", "") or not signal.get("report_ready"):
                continue
            if expected.lower() in signal.get("incident_id", "").lower():
                matches.append((episode, signal))
    return matches


def retain_assessment(root, value):
    folder = root / "raw-assessments"
    folder.mkdir(exist_ok=True)
    path = folder / (digest(value) + ".json")
    if not path.exists():
        exclusive(path, value)
    return value


def retain_revisions(record, output):
    if not record.get("capsule_id"):
        return
    try:
        value = request(record["fcapsule"] + "/artifacts/" + quote(record["capsule_id"], safe="") + "/investigation_revisions.json")
        save(output / "investigation-revisions.json", value)
    except (OSError, ValueError) as error:
        save(output / "revision-export-unavailable.json", {"at": now(), "error": str(error),
            "limitation": "Older automatic revisions may be missing; do not report last-run usage as total usage"})


def assessment_contains_incident(value, incident_id):
    context = value.get("context")
    alerts = context.get("alerts") if isinstance(context, dict) else None
    return isinstance(alerts, list) and any(
        isinstance(alert, dict) and alert.get("incident_id") == incident_id for alert in alerts
    )


def await_assessment(record, root, seconds=360, *, allow_retained_prior=False):
    base = record["fcapsule"] + "/api/episodes/" + quote(record["episode_id"], safe="")
    description = ("Retained assessment did not finish; no retry started" if allow_retained_prior else
                   "No terminal assessment includes the run incident in context.alerts; no retry started")
    value = media.wait_for(lambda: retain_assessment(root, request(base + "/investigation")),
                           lambda v: v.get("status") in TERMINAL and
                           (allow_retained_prior or assessment_contains_incident(v, record["incident_id"])),
                           seconds, description)
    save(root / "investigation-before.json", value)
    report = request(record["fcapsule"] + "/api/incidents/" + quote(record["incident_id"], safe="") + "/report")
    save(root / "report.json", report)
    capsule_id = report.get("record", {}).get("capsule_id")
    if capsule_id:
        capsule = request(record["fcapsule"] + "/api/capsules/" + quote(capsule_id, safe=""))
        save(root / "capsule.json", capsule)
        record["capsule_id"] = capsule_id
        record["capsule_sha256"] = digest(capsule["capsule"])
        retain_revisions(record, root)
    record["assessment_status"] = value.get("status")
    record["assessment_matches_incident"] = value.get("primary_incident_id") == record["incident_id"]
    record["assessment_context_contains_incident"] = assessment_contains_incident(value, record["incident_id"])
    record["assessment_context_policy"] = "retained_prior_read_only" if allow_retained_prior else "current_incident_required"
    if not record["assessment_context_contains_incident"]:
        record["comparison_valid"] = False
    record["usage"] = value.get("usage")
    record["quality"] = "requires_human_review"
    if value.get("model") != record["model_config"]["model"]:
        record["comparison_valid"] = False
        raise RuntimeError("Automatic assessment used a different model; preserve the result and stop")
    save(root / "run.json", record)
    return value


def recover_owned(args, root, owner):
    state = request(args.lab + "/api/status")
    active = state.get("active")
    if active and active.get("run_id") != owner:
        raise RuntimeError("Different run became active; no recovery write performed")
    if active:
        value = request(args.lab + "/api/recover", {"expected_run_id": owner})
        save(root / "recovery-request.json", value)
    media.wait_for(lambda: request(args.lab + "/api/status"),
                   lambda s: not s.get("active") and all(s.get(k, {}).get("reachable") for k in ("worker", "inventory", "orders")),
                   90, "Owned Lab run did not recover; inspect watchdog and recovery errors")
    after = snapshot(args, root, "after")
    safety(after, args.lab_node)
    baseline_config(after)
    save(root / "recovery.json", {"at": now(), "restored": True, "owner": owner})


def run_workload(args, case, root, config):
    scenario = DEMO_CASES.get(case, {}).get("scenario", case)
    expected = SIGNALS[scenario]["expected_alert"]
    before = snapshot(args, root, "before")
    safety(before, args.lab_node)
    baseline_config(before)
    status = request(args.lab + "/api/status")
    if not status.get("capabilities", {}).get("owned_runs"):
        raise RuntimeError("Controller does not support ownership; no injection performed")
    configuration(args.fcapsule, config)
    if firing(args.prometheus):
        raise RuntimeError("Previous alerts have not cleared")
    owner = uuid.uuid4().hex
    record = {"case": case, "scenario": scenario, "expected_alert": expected, "owner": owner, "baseline_at": now(),
              "model_config": config, "fcapsule": args.fcapsule, "lab": args.lab, "prometheus": args.prometheus,
              "outcome": "starting", "samples": [], "observed_alerts": []}
    save(root / "run.json", record)
    time.sleep(args.baseline)
    safety(snapshot(args, root, "baseline-end"), args.lab_node)
    if firing(args.prometheus):
        raise RuntimeError("New alert appeared during healthy baseline; no injection performed")
    record["started_at"] = now()
    duration = DEMO_CASES.get(case, {}).get("duration_seconds", 180)
    save(root / "run.json", record)
    try:
        # Persist ownership before the single POST, including ambiguous failures.
        exclusive(root / "injection-attempt.json", {"at": now(), "owner": owner})
        record["control"] = request(args.lab + "/api/scenarios/" + scenario + "/start",
                                    {"duration_seconds": duration, "request_id": owner})
        if record["control"].get("run", {}).get("run_id") != owner:
            raise RuntimeError("Controller did not acknowledge run ownership")
        deadline = time.monotonic() + duration
        seen_at = None
        while time.monotonic() < deadline:
            current = snapshot(args, root, "fault-latest")
            safety(current, args.lab_node, owner, 768 * 1024**2)
            alerts = firing(args.prometheus)
            matches = [a for a in alerts if a["labels"].get("alertname") == expected]
            record["samples"].append({"at": current["at"], "memory": current["memory"], "active": current["lab"]["active"]})
            if matches and seen_at is None:
                seen_at = time.monotonic()
                record["observed_alerts"] = alerts
                save(root / "fault.json", current)
                save(root / "alert.json", matches)
                capture(args, root, case)
            if seen_at is not None:
                pairs = matching_signals(request(args.fcapsule + "/api/state"), expected, record["started_at"])
                record["candidate_signals"] = [{"episode_id": e["episode_id"], "incident_id": s["incident_id"]} for e, s in pairs]
                if len(pairs) == 1:
                    episode, signal = pairs[0]
                    record.update(episode_id=episode["episode_id"], incident_id=signal["incident_id"])
                elif len(pairs) > 1:
                    raise RuntimeError("Multiple fresh signals match; no automatic episode choice")
            save(root / "run.json", record)
            if seen_at is not None and time.monotonic() - seen_at >= 30 and record.get("episode_id"):
                break
            if not current["lab"].get("active"):
                break
            time.sleep(10)
        record["outcome"] = "captured" if record.get("episode_id") else "capture_incomplete"
    except BaseException as error:
        record.update(outcome="incomplete", error=f"{type(error).__name__}: {error}")
        raise
    finally:
        try:
            recover_owned(args, root, owner)
        finally:
            record["fault_ended_at"] = now()
            save(root / "run.json", record)
    collect_logs(root, record["baseline_at"])
    retain_metrics(args, root, case, record["baseline_at"])
    if record["outcome"] != "captured":
        raise RuntimeError("Fresh alert/capsule not captured within the lease; no reinjection")
    await_assessment(record, root)
    return record


def run_exporter(args, root, config):
    # The existing probe owns its guarded patch, watchdog and restoration. Do not
    # duplicate that fault or grant the controller more Kubernetes permissions.
    probe = SimpleNamespace(**vars(args))
    probe.out = root
    media.run(probe)
    record = read(root / "run.json")
    record.update(case="exporter-scrape", scenario="mysql-exporter-scrape-path", model_config=config)
    save(root / "run.json", record)
    collect_logs(root, record["started_at"])
    retain_metrics(args, root, "exporter-scrape", record["started_at"])
    await_assessment(record, root)
    return record


def run_suite(args):
    require_execute(args)
    root = args.out.resolve()
    root.mkdir(parents=True, exist_ok=False)
    selected = ([*SCENARIOS, *DISCOVERY_SCENARIOS] if args.case == "all" else [args.case])
    summary = {"started_at": now(), "cases": selected, "rounds": [], "outcome": "running",
               "quality": "not_evaluated", "live_source_outage_induced": False}
    save(root / "suite.json", summary)
    try:
        print("Waiting for a quiet Lab alert baseline before preflight...", flush=True)
        wait_for_lab_quiet(args)
        checked = preflight(args, root)
        if any(capture_spec(case) for case in selected):
            subprocess.run([args.node, "-e", "require(process.env.PLAYWRIGHT_MODULE || 'playwright')"], check=True, timeout=15)
        for case in selected:
            print(f"Starting demo: {case}", flush=True)
            case_failed = False
            first_ordinal = 1
            if case == "query-rollout-history" and args.previous_round:
                previous = read(args.previous_round / "run.json")
                if previous.get("case") != case or previous.get("outcome") != "captured" or previous.get("fcapsule") != args.fcapsule:
                    raise ValueError("--previous-round must be a captured matching run on this FCAPSule origin")
                reference = root / case / "round-1"
                reference.mkdir(parents=True)
                save(reference / "reference.json", {"path": str(args.previous_round.resolve()), "reused_existing_episode": True})
                first_ordinal = 2
            for ordinal in range(1, scenario_rounds(case) + 1):
                if ordinal < first_ordinal:
                    continue
                wait_for_lab_quiet(args)
                if not case_failed and case == "query-rollout-history" and ordinal == 2:
                    wait_history_gap(args, root / case)
                configuration(args.fcapsule, checked["model_config"])
                round_dir = root / case / ("round-" + str(ordinal))
                round_dir.parent.mkdir(exist_ok=True)
                try:
                    if case in {"exporter-scrape", "mysql-exporter-scrape-path"}:
                        record = run_exporter(args, round_dir, checked["model_config"])
                    else:
                        round_dir.mkdir()
                        record = run_workload(args, case, round_dir, checked["model_config"])
                    summary["rounds"].append({"case": case, "round": ordinal, "path": str(round_dir),
                        "outcome": record["outcome"], "episode_id": record.get("episode_id"),
                        "assessment_status": record.get("assessment_status"), "usage": record.get("usage")})
                except Exception as error:
                    case_failed = True
                    summary["rounds"].append({"case": case, "round": ordinal, "path": str(round_dir),
                        "outcome": "failed", "error_type": type(error).__name__, "error": str(error)[:500]})
                    save(root / "suite.json", summary)
                    # If recovery was not confirmed, the next fault would be unsafe.
                    if not (round_dir / "recovery.json").exists() or not read(round_dir / "recovery.json").get("restored"):
                        raise
                    print(f"{case}: failed but owned recovery was confirmed; continuing with the next case", flush=True)
                save(root / "suite.json", summary)
                if not case_failed:
                    print(f"{case}: captured, recovered; assessment {record.get('assessment_status', 'unknown')}", flush=True)
                if case == "query-rollout-history" and ordinal == 2:
                    prior = read(history_round(root / case, 1) / "run.json")
                    if record["episode_id"] == prior["episode_id"]:
                        raise RuntimeError("Occurrences grouped into the same episode; no split or relabel attempted")
        wait_for_lab_quiet(args)
        summary["outcome"] = "completed_with_failures" if any(item.get("outcome") == "failed" for item in summary["rounds"]) else "captured_pending_media_and_human_review"
    except BaseException as error:
        summary.update(outcome="incomplete", error=f"{type(error).__name__}: {error}")
        raise
    finally:
        summary["finished_at"] = now()
        save(root / "suite.json", summary)


def validated_image(root, record):
    data = (root / "fault.png").read_bytes()
    metadata = read(root / "fault.png.json")
    expected = urlparse(record["prometheus"])
    source = urlparse(metadata["source_url"])
    spec = capture_spec(record["case"])
    if not spec or (source.scheme, source.netloc) != (expected.scheme, expected.netloc):
        raise ValueError("Screenshot must originate from the configured external Prometheus, not FCAPSule")
    if source.path != ("/targets" if spec["view"] == "targets" else "/query"):
        raise ValueError("Unexpected screenshot source page")
    if spec["view"] == "targets" and metadata.get("pool") != spec["pool"]:
        raise ValueError("Screenshot pool mismatch")
    if spec["view"] == "graph" and metadata.get("query") != spec["query"]:
        raise ValueError("Screenshot query mismatch")
    if metadata.get("sha256") != hashlib.sha256(data).hexdigest():
        raise ValueError("Screenshot bytes changed since capture")
    if not data.startswith(b"\x89PNG\r\n\x1a\n") or not 1024 < len(data) <= 6 * 1024**2:
        raise ValueError("Require a bounded real PNG screenshot")
    at = datetime.fromisoformat(metadata["observed_at"].replace("Z", "+00:00"))
    if not datetime.fromisoformat(record["started_at"].replace("Z", "+00:00")) <= at <= datetime.fromisoformat(record["fault_ended_at"].replace("Z", "+00:00")):
        raise ValueError("Screenshot was not captured within this incident window")
    return data, metadata


def paid_context(args):
    require_execute(args)
    root = args.case_dir.resolve()
    record = read(root / "run.json")
    if record.get("outcome") != "captured" or not read(root / "recovery.json").get("restored"):
        raise ValueError("Require captured evidence and confirmed recovery")
    configuration(record["fcapsule"], record["model_config"])
    base = record["fcapsule"] + "/api/episodes/" + quote(record["episode_id"], safe="")
    return root, record, base


def finish_update(root, output, record, base, before, attachment_id, *, incremental_evidence=False):
    queued = request(base + "/investigation/update", {})
    save(output / "update-request.json", queued)
    if not queued.get("revision_id") or queued["revision_id"] == before.get("revision_id"):
        raise RuntimeError("No new revision; inspect the stored state, do not retry automatically")
    after = media.wait_for(lambda: retain_assessment(root, request(base + "/investigation")),
        lambda r: r.get("revision_id") == queued["revision_id"] and r.get("status") in TERMINAL,
        360, "Reassessment not finished; do not repeat the paid POST")
    save(output / "investigation-after.json", after)
    retain_revisions(record, output)
    save(output / "evaluation.json", {
        "status": after.get("status"), "before_revision": before.get("revision_id"), "after_revision": after.get("revision_id"),
        "parent_linked": after.get("parent_revision_id") == before.get("revision_id"),
        "same_model": after.get("model") == before.get("model") == record["model_config"]["model"],
        "policy_before": before.get("policy_version"), "policy_after": after.get("policy_version"),
        "incremental_evidence": incremental_evidence,
        "image_delivery": media.evidence_delivery(after, attachment_id),
        "image_cited": media.assessment_cites(after, attachment_id),
        "before_usage": before.get("usage"), "after_usage": after.get("usage"),
        "value_verdict": "requires_human_review",
        "limitation": ("Incremental evidence, not isolated image ablation. " if incremental_evidence else "") +
                      "Post-recovery live observations may differ; not an image-only ablation."})


def attach(args):
    if not args.pixels_reviewed:
        raise ValueError("Inspect the actual external screenshot pixels, then pass --pixels-reviewed")
    incremental = getattr(args, "incremental_evidence", False)
    expected_revision = getattr(args, "expected_revision", None)
    if incremental and not expected_revision:
        raise ValueError("--incremental-evidence requires explicit --expected-revision")
    root, record, base = paid_context(args)
    output = root / "media-review"
    if output.exists():
        raise ValueError("Attachment/reassessment was already attempted; inspect its saved state")
    data, metadata = validated_image(root, record)
    existing = request(base + "/evidence")
    if existing and not incremental:
        raise ValueError("Existing media would contaminate the no-image baseline")
    settings = request(record["fcapsule"] + "/api/settings/media")
    if any(settings.get(k, {}).get("capability", {}).get("status") != "ready" for k in ("vision", "core_investigator")):
        raise ValueError("Existing media capabilities must already be validated")
    before = request(base + "/investigation")
    if before.get("status") not in TERMINAL or not before.get("revision_id"):
        raise ValueError("Automatic investigation is still active or absent; no additional call started")
    if expected_revision and before["revision_id"] != expected_revision:
        raise ValueError("Current revision differs from --expected-revision; no upload started")
    if not assessment_contains_incident(before, record["incident_id"]):
        raise ValueError("Current assessment context.alerts does not contain this run incident; no upload started")
    original = read(root / "investigation-before.json")
    if before["revision_id"] != original.get("revision_id") and not incremental:
        raise ValueError("Baseline changed; inspect and explicitly reassess instead of mixing comparisons")
    output.mkdir()
    exclusive(output / "attempt.json", {"at": now(), "pixels_reviewed": True, "sha256": metadata["sha256"],
        "prior_revision_id": before["revision_id"], "incremental_evidence": incremental, "expected_revision": expected_revision})
    save(output / "existing-evidence.json", existing)
    save(output / "capture.json", metadata)
    save(output / "baseline-provenance.json", {
        "incremental_evidence": incremental,
        "limitation": "Incremental evidence, not isolated image ablation." if incremental else "Post-recovery comparison, not image-only ablation.",
        "incident_id": record["incident_id"], "episode_id": record["episode_id"],
        "original_revision_id": original.get("revision_id"), "original_policy": original.get("policy_version"),
        "current_revision_id": before["revision_id"], "current_policy": before.get("policy_version"),
        "baseline_changed": original.get("revision_id") != before["revision_id"],
        "assessment_context_contains_incident": True,
        "existing_attachments": [{key: item.get(key) for key in ("attachment_id", "kind", "sha256", "status")} for item in existing],
        "capture_sha256": metadata["sha256"]})
    save(output / "investigation-original.json", original)
    save(output / "investigation-before.json", before)
    response = request(base + "/evidence", {"kind": "image", "filename": "prometheus-observation.png",
        "content_base64": base64.b64encode(data).decode(), "observed_at": metadata["observed_at"],
        "context_note": "External Prometheus page captured at the recorded observation time.", "source_redacted": False})
    save(output / "attachment-submitted.json", response)
    attachment_id = response["attachment_id"]
    extracted = media.wait_for(lambda: next((a for a in request(base + "/evidence") if a.get("attachment_id") == attachment_id), {}),
        lambda a: a.get("status") in TERMINAL, 140, "Extraction timeout; inspect without reuploading")
    save(output / "attachment.json", extracted)
    if extracted.get("status") != "ready" or extracted.get("sha256") != metadata["sha256"]:
        raise RuntimeError("Extraction failed or hash mismatch; no reassessment started")
    configuration(record["fcapsule"], record["model_config"])
    current = request(base + "/investigation")
    save(output / "investigation-pre-update.json", current)
    if (current.get("revision_id") != before["revision_id"] or current.get("status") not in TERMINAL or
            not assessment_contains_incident(current, record["incident_id"])):
        raise RuntimeError("Current assessment changed or lacks this incident; retain attachment and review explicitly")
    current_evidence = request(base + "/evidence")
    save(output / "evidence-pre-update.json", current_evidence)
    prior_evidence = [item for item in current_evidence if item.get("attachment_id") != attachment_id]
    if sorted(map(digest, prior_evidence)) != sorted(map(digest, existing)) or extracted not in current_evidence:
        raise RuntimeError("Evidence changed during attachment; retain saved state and review explicitly")
    finish_update(root, output, record, base, before, attachment_id, incremental_evidence=incremental)


def reassess(args):
    root, record, base = paid_context(args)
    if not args.label or len(args.label) > 40 or any(c not in "abcdefghijklmnopqrstuvwxyz0123456789-" for c in args.label):
        raise ValueError("Use a short unique lowercase --label for this explicit reassessment")
    if not args.expected_revision:
        raise ValueError("Explicit --expected-revision required")
    before = request(base + "/investigation")
    if before.get("revision_id") != args.expected_revision or before.get("status") not in TERMINAL:
        raise ValueError("Baseline changed or is active")
    saved = read(root / "media-review" / "attachment.json")
    attachments = request(base + "/evidence")
    if len(attachments) != 1 or any(attachments[0].get(k) != saved.get(k) for k in
                                  ("attachment_id", "sha256", "status", "extraction", "correction", "context_note", "observed_at")):
        raise ValueError("Attachment content differs from the previous reviewed evidence")
    output = root / "reassessments" / args.label
    output.mkdir(parents=True, exist_ok=False)
    exclusive(output / "attempt.json", {"at": now(), "prior_revision_id": before["revision_id"], "additional_vision_calls": 0})
    save(output / "investigation-before.json", before)
    finish_update(root, output, record, base, before, saved["attachment_id"])


def history_records(root):
    first, second = [read(history_round(root, n) / "run.json") for n in (1, 2)]
    if any(r.get("case") != "query-rollout-history" or r.get("outcome") != "captured" for r in (first, second)):
        raise ValueError("Two captured query-rollout-history occurrences are required")
    if first["incident_id"] == second["incident_id"] or first["episode_id"] == second["episode_id"] or not first.get("capsule_id"):
        raise ValueError("Require separate incident AND episode IDs and an earlier retained capsule")
    if first["fcapsule"] != second["fcapsule"] or first["model_config"] != second["model_config"]:
        raise ValueError("Occurrences must share product origin and model configuration")
    if not all(read(history_round(root, n) / "recovery.json").get("restored") for n in (1, 2)):
        raise ValueError("Both occurrences must be recovered")
    return first, second


def history(args):
    require_execute(args)
    root = args.case_dir.resolve()
    first, second = history_records(root)
    configuration(first["fcapsule"], first["model_config"])
    output = root / "history-review"
    output.mkdir(exist_ok=False)
    capsule = request(first["fcapsule"] + "/api/capsules/" + quote(first["capsule_id"], safe=""))
    save(output / "earlier-capsule-retrieved.json", capsule)
    if digest(capsule["capsule"]) != first["capsule_sha256"]:
        raise ValueError("Earlier capsule changed; no retained-only call started")
    # Ask about the earlier capsule itself. Recurrence lookup by the investigator
    # is separately audited, never implied by a successful retained-only review.
    base = first["fcapsule"] + "/api/episodes/" + quote(first["episode_id"], safe="")
    exclusive(output / "attempt.json", {"at": now(), "question": QUESTIONS, "earlier_capsule_id": first["capsule_id"],
        "earlier_incident_id": first["incident_id"], "recurrence_incident_id": second["incident_id"], "live_source_outage_induced": False})
    queued = request(base + "/source-review", {"question": QUESTIONS})
    save(output / "review-request.json", queued)
    finish_history_review(first, second, queued, output)


def finish_history_review(first, second, queued, output):
    review_id = queued.get("review_id")
    if not review_id or queued.get("episode_id") != first["episode_id"]:
        raise RuntimeError("Missing or mismatched accepted review identity; no fallback model call")
    report_url = first["fcapsule"] + "/api/incidents/" + quote(first["incident_id"], safe="") + "/report"

    def observe():
        report = request(report_url)
        reviewed = next((r for r in report.get("source_disconnected_reviews", [])
                         if r.get("review_id") == review_id and r.get("episode_id") == first["episode_id"]), {})
        snapshots = output / "raw-reviews"
        snapshots.mkdir(exist_ok=True)
        path = snapshots / (digest(reviewed) + ".json")
        if not path.exists():
            exclusive(path, reviewed)
        return reviewed

    reviewed = media.wait_for(observe, lambda r: r.get("status") in TERMINAL, 160,
                              "Retained-only review timeout; use history-status, do not repeat the POST")
    save(output / "source-review.json", reviewed)
    recurrence = request(second["fcapsule"] + "/api/episodes/" + quote(second["episode_id"], safe="") + "/investigation")
    save(output / "recurrence-investigation.json", recurrence)
    retain_revisions(first, output)
    save(output / "evaluation.json", {"earlier_capsule_unchanged": True, "different_incidents": True,
        "same_episode": first["episode_id"] == second["episode_id"], "status": reviewed.get("status"),
        "review_id": review_id, "completed": reviewed.get("status") in TERMINAL,
        "sufficiency": (reviewed.get("result") or {}).get("sufficiency"),
        "status_source": report_url, "completed_at": reviewed.get("completed_at"),
        "usage": reviewed.get("usage"), "value_verdict": "requires_human_review",
        "live_source_outage_induced": False, "source_unavailability": "review-isolated only, not a shared-source outage",
        "history_retrieval_by_investigator": "inspect recurrence checks; operator retrieval alone does not prove automatic reuse"})
    print(f"Retained review {review_id}: {reviewed.get('status')}; results: {output}", flush=True)


def history_status(args):
    """Reconcile an accepted review using GET only, preserving the original attempt."""
    root = args.case_dir.resolve()
    first, second = history_records(root)
    original = root / "history-review"
    attempt = read(original / "attempt.json")
    queued = read(original / "review-request.json")
    if (attempt.get("earlier_incident_id") != first["incident_id"] or
            attempt.get("recurrence_incident_id") != second["incident_id"] or
            attempt.get("earlier_capsule_id") != first["capsule_id"] or
            queued.get("episode_id") != first["episode_id"] or not queued.get("review_id")):
        raise ValueError("Saved history attempt and accepted review do not match the selected rounds")
    capsule = read(original / "earlier-capsule-retrieved.json")
    if digest(capsule["capsule"]) != first["capsule_sha256"]:
        raise ValueError("Previously retrieved capsule hash differs; no review request will be made")
    output = original / "status" / uuid.uuid4().hex
    output.mkdir(parents=True, exist_ok=False)
    exclusive(output / "reconciliation.json", {"at": now(), "mode": "history-status", "read_only": True,
        "original_attempt": str(original), "review_request_sha256": digest(queued), "provider_requests": 0})
    save(output / "review-request.json", queued)
    finish_history_review(first, second, queued, output)


def history_round(root, ordinal):
    path = root / ("round-" + str(ordinal))
    reference = path / "reference.json"
    return Path(read(reference)["path"]) if reference.exists() else path


def history_wait_seconds(state, app_id, quiet_seconds, timestamp):
    members = [s for e in state["overview"]["episodes"] if e.get("app_id") == app_id
               for s in e.get("signals", [])]
    if any(s.get("status") == "firing" for s in members):
        return quiet_seconds
    latest = max((datetime.fromisoformat(s["started_at"].replace("Z", "+00:00")).timestamp()
                  for s in members if s.get("started_at")), default=0)
    return max(0, latest + quiet_seconds - timestamp)


def wait_history_gap(args, root):
    previous = read(history_round(root, 1) / "run.json")
    report = read(history_round(root, 1) / "report.json")
    app_id = report["incident"]["app_id"]
    # Product store.EPISODE_JOIN_MINUTES is currently 15, not an exposed setting.
    # Keep a margin and retain the observed membership; still verify actual IDs.
    deadline = time.monotonic() + args.history_wait_timeout
    while True:
        state = request(args.fcapsule + "/api/state")
        remaining = history_wait_seconds(state, app_id, args.episode_quiet_seconds, time.time())
        save(root / "grouping-wait.json", {"at": now(), "earlier_episode_id": previous["episode_id"],
            "quiet_seconds": args.episode_quiet_seconds, "remaining_seconds": remaining,
            "basis": "15-minute incident-start join window plus margin; no IDs or settings modified"})
        if remaining <= 0:
            return
        if time.monotonic() >= deadline:
            raise TimeoutError("Grouping quiet period not reached; no recurrence injected")
        print(f"History grouping quiet period: {math.ceil(remaining)} seconds remaining", flush=True)
        time.sleep(min(30, remaining))


def retain_prior(args):
    """Read-only import of a resolved real prior incident, without changing its IDs."""
    if not args.incident_id or "labinventoryqueryfailures" not in args.incident_id.lower():
        raise ValueError("Select an existing LabInventoryQueryFailures --incident-id")
    root = args.out.resolve()
    root.mkdir(parents=True, exist_ok=False)
    checked = preflight(args, root, require_owned=False)
    state = request(args.fcapsule + "/api/state")
    pairs = [(e, s) for e in state["overview"]["episodes"] for s in e.get("signals", []) if s.get("incident_id") == args.incident_id]
    if len(pairs) != 1 or pairs[0][1].get("status") != "resolved" or "fcapsule-lab" not in pairs[0][1].get("app_id", ""):
        raise ValueError("Require one resolved retained Lab incident")
    episode, signal = pairs[0]
    record = {"case": "query-rollout-history", "scenario": "schema-drift", "outcome": "captured",
        "episode_id": episode["episode_id"], "incident_id": args.incident_id, "started_at": signal["started_at"],
        "fault_ended_at": signal["ended_at"], "model_config": checked["model_config"],
        "fcapsule": args.fcapsule, "lab": args.lab, "prometheus": args.prometheus,
        "origin": "existing retained matching symptom; historical injection not independently reverified",
        "retained_at": now()}
    save(root / "run.json", record)
    await_assessment(record, root, seconds=30, allow_retained_prior=True)
    if not record.get("capsule_id"):
        raise ValueError("Previous capsule is unavailable; do not substitute an invented prior")
    save(root / "recovery.json", {"at": now(), "restored": True,
        "basis": "Earlier incident resolved and current Lab baseline independently observed; no recovery write performed"})


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=("plan", "preflight", "retain-prior", "run", "attach", "reassess", "history", "history-status"))
    parser.add_argument("--case", choices=[*DEMO_CASES, *SCENARIOS, *DISCOVERY_SCENARIOS, "all"], default="all")
    parser.add_argument("--out", type=Path)
    parser.add_argument("--case-dir", type=Path)
    parser.add_argument("--execute", action="store_true")
    parser.add_argument("--pixels-reviewed", action="store_true")
    parser.add_argument("--incremental-evidence", action="store_true",
                        help="Attach to an existing evidence set; requires --expected-revision, never an isolated image ablation")
    parser.add_argument("--label")
    parser.add_argument("--expected-revision")
    parser.add_argument("--incident-id")
    parser.add_argument("--previous-round", type=Path)
    parser.add_argument("--episode-quiet-seconds", type=int, default=960)
    parser.add_argument("--history-wait-timeout", type=int, default=1200)
    parser.add_argument("--lab", default="http://192.168.0.102:30766")
    parser.add_argument("--prometheus", default="http://192.168.0.102:30090")
    parser.add_argument("--fcapsule", default="http://192.168.0.102:30765")
    parser.add_argument("--lab-node", help="Agreed hosting node; required for preflight, run and retain-prior")
    parser.add_argument("--node", default="node")
    parser.add_argument("--baseline", type=int, choices=range(30, 121), default=45)
    parser.add_argument("--lab-quiet-timeout", type=int, default=420)
    args = parser.parse_args()
    if args.incremental_evidence and (args.mode != "attach" or not args.expected_revision):
        parser.error("--incremental-evidence is attach-only and requires --expected-revision")
    for key in ("lab", "prometheus", "fcapsule"):
        value = getattr(args, key).rstrip("/")
        parsed = urlparse(value)
        if parsed.scheme not in {"http", "https"} or not parsed.netloc or parsed.username or parsed.password or parsed.path:
            parser.error("Service URLs must be HTTP(S) origins without credentials or paths")
        setattr(args, key, value)
    if args.prometheus == args.fcapsule:
        parser.error("Evidence source cannot be FCAPSule")
    if args.episode_quiet_seconds < 960:
        parser.error("Quiet period must cover the 15-minute grouping window plus a 60-second margin")
    if args.mode == "plan":
        print(json.dumps({"scenarios": public_scenarios(), "legacy_presets": DEMO_CASES,
                          "cluster_mutations": False, "paid_calls": 0}, indent=2))
        return
    if args.mode in {"run", "preflight", "retain-prior"} and not args.out:
        parser.error("--out must name a new private output directory")
    if args.mode in {"run", "preflight", "retain-prior"} and not args.lab_node:
        parser.error("--lab-node must name the agreed hosting node; runner never changes placement")
    if args.mode in {"attach", "reassess", "history", "history-status"} and not args.case_dir:
        parser.error("--case-dir is required")
    if args.mode == "preflight":
        args.out.mkdir(parents=True, exist_ok=False)
        preflight(args, args.out)
    else:
        {"run": run_suite, "attach": attach, "reassess": reassess, "history": history,
         "history-status": history_status, "retain-prior": retain_prior}[args.mode](args)


if __name__ == "__main__":
    main()
