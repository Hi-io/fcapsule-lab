"""Bounded scrape-path incident and one explicitly reviewed external-image update.

Run with Windows Python + WSL kubectl, or Linux Python + local kubectl. Capture
requires Node/Playwright/Chrome supplied by the operator. No product settings change.
"""

import argparse
import base64
import hashlib
import json
import math
import os
from pathlib import Path
import shutil
import subprocess
import sys
import time
from urllib.parse import urlencode, urlparse
import uuid

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
from tools.run_scenarios import now, request, save

NAMESPACE = "fcapsule-lab"
MONITOR = "fcapsule-lab-mysql"
POOL = f"serviceMonitor/{NAMESPACE}/{MONITOR}/0"
RULE = "fcapsule-lab-screenshot-scrape"
ALERT = "LabExporterScrapeFailed"
OWNER = "fcapsule.lab/screenshot-run"
CONTROL_OWNER = "fcapsule.io/lab-control-run"
FIELD_OWNER = "fcapsule.io/lab-control-field-owner"
FAULT_PATH = "/metrics-v2"
TERMINAL = {"ready", "incomplete", "inconclusive", "not_configured", "failed", "blocked"}


def kubectl(*args, body=None, raw=False):
    prefix = ["wsl.exe", "-d", "Ubuntu", "--exec", "/snap/bin/kubectl"] if os.name == "nt" else [shutil.which("kubectl") or "/snap/bin/kubectl"]
    result = subprocess.run([*prefix, *args], input=json.dumps(body) if body is not None else None,
                            capture_output=True, text=True, timeout=20, check=True)
    return json.loads(result.stdout) if not raw and result.stdout.strip().startswith("{") else result.stdout


def get(kind, name):
    return kubectl("get", kind, name, "-n", NAMESPACE, "-o", "json")


def claim_external_run(owner):
    config = get("configmap", "lab-scenario-config")
    metadata = config.get("metadata", {})
    annotations = metadata.get("annotations") or {}
    if annotations.get(CONTROL_OWNER) or annotations.get(FIELD_OWNER):
        raise RuntimeError("A controller-owned Lab run or recovery journal is active")
    if annotations.get(OWNER):
        raise RuntimeError("Another external Prometheus screenshot run owns the Lab")
    operations = [{"op": "test", "path": "/metadata/resourceVersion",
                   "value": metadata.get("resourceVersion")}]
    if metadata.get("annotations") is None:
        operations.append({"op": "add", "path": "/metadata/annotations", "value": {OWNER: owner}})
    else:
        operations.append({"op": "add", "path": "/metadata/annotations/" + OWNER.replace("/", "~1"), "value": owner})
    kubectl("patch", "configmap", "lab-scenario-config", "-n", NAMESPACE,
            "--type=json", "--patch", json.dumps(operations))


def release_external_run(owner):
    config = get("configmap", "lab-scenario-config")
    metadata = config.get("metadata", {})
    annotations = metadata.get("annotations") or {}
    current = annotations.get(OWNER)
    if current is None:
        return
    if current != owner:
        raise RuntimeError("External screenshot ownership changed; preserving the active owner's lock")
    operations = [
        {"op": "test", "path": "/metadata/resourceVersion", "value": metadata.get("resourceVersion")},
        {"op": "test", "path": "/metadata/annotations/" + OWNER.replace("/", "~1"), "value": owner},
        {"op": "remove", "path": "/metadata/annotations/" + OWNER.replace("/", "~1")},
    ]
    kubectl("patch", "configmap", "lab-scenario-config", "-n", NAMESPACE,
            "--type=json", "--patch", json.dumps(operations))


def patch_monitor(document, path, owner):
    operations = [
        {"op": "test", "path": "/metadata/resourceVersion", "value": document["metadata"]["resourceVersion"]},
        {"op": "replace", "path": "/spec/endpoints/0/path", "value": path},
        {"op": "add", "path": "/metadata/annotations/" + OWNER.replace("/", "~1"), "value": owner},
    ]
    if owner is None:
        operations[-1] = {"op": "remove", "path": operations[-1]["path"]}
    return kubectl("patch", "servicemonitor", MONITOR, "-n", NAMESPACE, "--type=json", "--patch", json.dumps(operations))


def rule_document(owner):
    return {"apiVersion": "monitoring.coreos.com/v1", "kind": "PrometheusRule",
            "metadata": {"name": RULE, "namespace": NAMESPACE,
                         "labels": {"release": "prometheus", "app.kubernetes.io/part-of": NAMESPACE},
                         "annotations": {OWNER: owner}},
            "spec": {"groups": [{"name": "fcapsule-lab.external-screenshot", "rules": [{
                "alert": ALERT, "expr": 'up{namespace="fcapsule-lab",service="mysql-exporter"} == 0',
                "for": "15s", "labels": {"severity": "warning", "signal_class": "discovery",
                                            "target_namespace": NAMESPACE,
                                            "target_service": "mysql-exporter",
                                            "target_workload": "mysql-exporter"},
                "annotations": {"summary": "The exporter metrics target is failing scrapes.",
                                "description": "Prometheus discovered the target but cannot collect its metrics. Workload health must be checked independently."},
            }]}]}}


def targets(prometheus):
    return request(prometheus + "/api/v1/targets?state=active")["data"]["activeTargets"]


def exporter_target(items):
    matches = [item for item in items if item.get("scrapePool") == POOL]
    if len(matches) != 1:
        raise RuntimeError("Expected exactly one discovered exporter target")
    return matches[0]


def snapshot(args, root, phase):
    status = request(args.lab + "/api/status")
    pods = kubectl("get", "pods", "-n", NAMESPACE, "-o", "json")["items"]
    scheduled = {pod["spec"]["nodeName"] for pod in pods if pod["spec"].get("nodeName")}
    nodes = kubectl("get", "nodes", "-o", "json")["items"]
    addresses = {node["metadata"]["name"]: [item["address"] for item in node["status"].get("addresses", [])]
                 for node in nodes if node["metadata"]["name"] in scheduled}
    memory = request(args.prometheus + "/api/v1/query?" + urlencode({"query": "node_memory_MemAvailable_bytes"}))
    data = {"at": now(), "lab": {key: status.get(key) for key in ("active", "worker", "inventory", "orders", "memory", "memory_error", "recovery_error")},
            "monitor": get("servicemonitor", MONITOR), "targets": targets(args.prometheus),
            "memory": memory, "node_addresses": addresses,
            "pods": [{"name": pod["metadata"]["name"], "node": pod["spec"].get("nodeName"),
                "uid": pod["metadata"]["uid"], "phase": pod["status"].get("phase"),
                "containers": [{key: c.get(key) for key in ("name", "ready", "restartCount")} for c in pod["status"].get("containerStatuses", [])]}
                for pod in pods]}
    save(root / (phase + ".json"), data)
    return data


def require_safe(data, minimum=1024**3):
    state = data["lab"]
    if state.get("active") or state.get("memory_error") or not all(state.get(key, {}).get("reachable") for key in ("worker", "inventory", "orders")):
        raise RuntimeError("Lab must be idle with healthy services and readable memory")
    if not data["pods"] or any(not p["containers"] or not all(c["ready"] for c in p["containers"]) for p in data["pods"]):
        raise RuntimeError("Lab pod readiness check failed")
    require_node_headroom(data, minimum)


def require_node_headroom(data, minimum=1024**3):
    """Host safety is independent of the workload symptoms deliberately injected."""
    if not data.get("pods"):
        raise RuntimeError("No scheduled Lab pods available for host safety checks")
    readings = data["memory"]["data"]["result"]
    for node in {pod["node"] for pod in data["pods"]}:
        addresses = data.get("node_addresses", {}).get(node, [])
        values = [float(item["value"][1]) for item in readings
                  if item["metric"].get("node") == node or
                  urlparse("//" + item["metric"].get("instance", "")).hostname in addresses]
        if not node or not values or any(not math.isfinite(value) for value in values) or min(values) < minimum:
            raise RuntimeError("Every scheduled Lab node requires measured MemAvailable above the safety threshold")


def capture(args, root, phase):
    try:
        subprocess.run([args.node, str(ROOT / "tools/capture_prometheus.cjs"), args.prometheus,
                        str(root / (phase + ".png")), POOL], check=True, timeout=55)
        result = {"phase": phase, "status": "captured", "source": args.prometheus}
    except (OSError, subprocess.SubprocessError, ValueError) as error:
        # Preserve the telemetry experiment even if the optional browser capture
        # cannot start; image attachment remains unavailable for this run.
        result = {"phase": phase, "status": "unavailable", "error_type": type(error).__name__,
                  "error": str(error)[:300]}
    save(root / ("capture-" + phase + ".json"), result)
    return result


def restore(root):
    record = json.loads((root / "run.json").read_text())
    owner = record["owner"]
    current = get("servicemonitor", MONITOR)
    baseline = json.loads((root / "before.json").read_text())["monitor"]
    if current["metadata"]["uid"] != baseline["metadata"]["uid"]:
        raise RuntimeError("Monitor identity changed; refusing to overwrite it")
    marker = current["metadata"].get("annotations", {}).get(OWNER)
    path = current["spec"]["endpoints"][0]["path"]
    if marker == owner and path == FAULT_PATH:
        patch_monitor(current, baseline["spec"]["endpoints"][0]["path"], None)
    elif marker is not None or path != baseline["spec"]["endpoints"][0]["path"]:
        raise RuntimeError("Concurrent monitor edit detected; manual recovery required")
    rule = kubectl("get", "prometheusrule", RULE, "-n", NAMESPACE, "--ignore-not-found", "-o", "json")
    if rule:
        if rule["metadata"].get("annotations", {}).get(OWNER) != owner:
            raise RuntimeError("Rule ownership changed; refusing removal")
        # DeleteOptions prevents deleting a replacement object after this read.
        kubectl("delete", "--raw", f"/apis/monitoring.coreos.com/v1/namespaces/{NAMESPACE}/prometheusrules/{RULE}",
                "-f", "-", body={"apiVersion": "v1", "kind": "DeleteOptions", "preconditions": {"uid": rule["metadata"]["uid"]}})
    save(root / "recovery.json", {"at": now(), "monitor_path": baseline["spec"]["endpoints"][0]["path"], "restored": True})
    release_external_run(owner)


def watchdog(root):
    record = json.loads((root / "run.json").read_text())
    while time.time() < record["rollback_at"]:
        if (root / "recovery.json").exists():
            return
        time.sleep(1)
    restore(root)


def wait_for(read, accept, seconds, description):
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        result = read()
        if accept(result):
            return result
        time.sleep(5)
    raise TimeoutError(description)


def run(args):
    root = args.out.resolve()
    root.mkdir(parents=True, exist_ok=False)
    before = snapshot(args, root, "before")
    require_safe(before)
    target = exporter_target(before["targets"])
    monitor = before["monitor"]
    if target["health"] != "up" or len(monitor["spec"]["endpoints"]) != 1 or monitor["spec"]["endpoints"][0]["path"] != "/metrics" or OWNER in monitor["metadata"].get("annotations", {}):
        raise RuntimeError("Expected the unmodified healthy /metrics baseline")
    if kubectl("get", "prometheusrule", RULE, "-n", NAMESPACE, "--ignore-not-found", "-o", "json"):
        raise RuntimeError("Another screenshot evaluation already owns the temporary rule")
    capture(args, root, "before")
    owner = uuid.uuid4().hex
    record = {"scenario": "mysql-exporter-scrape-path", "owner": owner, "started_at": now(),
              "rollback_at": time.time() + 240, "outcome": "running", "samples": [],
              "prometheus": args.prometheus, "lab": args.lab, "fcapsule": args.fcapsule}
    save(root / "run.json", record)
    claim_external_run(owner)
    guard = None
    try:
        guard = subprocess.Popen([sys.executable, str(Path(__file__).resolve()), "watchdog", "--out", str(root)],
                                 stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        locked = snapshot(args, root, "locked")
        require_safe(locked)
        locked_monitor = locked["monitor"]
        if (locked_monitor["metadata"].get("uid") != monitor["metadata"].get("uid")
                or locked_monitor["spec"]["endpoints"][0].get("path") != "/metrics"
                or locked["lab"].get("active")):
            raise RuntimeError("Lab changed between preflight and the external run lock")
        kubectl("create", "-f", "-", body=rule_document(record["owner"]))
        patch_monitor(monitor, FAULT_PATH, record["owner"])
        deadline = time.monotonic() + 180
        fault_observed = False
        while time.monotonic() < deadline:
            sample = snapshot(args, root, "fault-latest")
            require_safe(sample, 768 * 1024**2)
            target = exporter_target(sample["targets"])
            alerts = request(args.prometheus + "/api/v1/alerts")["data"]["alerts"]
            matching = [a for a in alerts if a["labels"].get("alertname") == ALERT and a["state"] == "firing"]
            record["samples"].append({"at": sample["at"], "health": target["health"], "error": target["lastError"], "alert_firing": bool(matching)})
            save(root / "run.json", record)
            if not fault_observed and target["health"] == "down" and "404" in target["lastError"]:
                save(root / "fault.json", sample)
                record["fault_screenshot"] = capture(args, root, "fault")
                fault_observed = True
                if record["fault_screenshot"]["status"] == "captured":
                    print("External 404 screenshot captured", flush=True)
            if fault_observed and matching:
                save(root / "alert.json", matching)
                overview = request(args.fcapsule + "/api/state")["overview"]
                matches = [(e, s) for e in overview["episodes"] for s in e["signals"]
                           if ALERT.lower() in s["incident_id"].lower() and s.get("created_at", "") >= record["started_at"] and s.get("report_ready")]
                if matches:
                    episode, signal = matches[0]
                    if request(args.fcapsule + f"/api/episodes/{episode['episode_id']}/evidence"):
                        raise RuntimeError("Episode already has media; refuse a contaminated comparison")
                    record.update(episode_id=episode["episode_id"], incident_id=signal["incident_id"], outcome="captured")
                    break
            time.sleep(5)
        if record["outcome"] != "captured":
            raise RuntimeError("No fresh captured episode within the bounded incident window")
    except Exception as error:
        record.update(outcome="incomplete", error=type(error).__name__ + ": " + str(error))
        raise
    finally:
        try:
            restore(root)
        finally:
            record["fault_ended_at"] = now()
            save(root / "run.json", record)
            if guard is not None:
                guard.wait(timeout=max(5, record["rollback_at"] - time.time() + 30))
    wait_for(lambda: exporter_target(targets(args.prometheus)), lambda t: t["health"] == "up" and urlparse(t["scrapeUrl"]).path == "/metrics", 100, "Exporter scrape did not recover")
    snapshot(args, root, "after")
    capture(args, root, "after")
    print(json.dumps({"out": str(root), "episode_id": record["episode_id"], "outcome": record["outcome"]}), flush=True)


def validated_image(root, prometheus):
    image = root / "fault.png"
    data = image.read_bytes()
    metadata = json.loads((root / "fault.png.json").read_text())
    expected, actual = urlparse(prometheus), urlparse(metadata["source_url"])
    if (actual.scheme, actual.netloc, actual.path) != (expected.scheme, expected.netloc, "/targets"):
        raise ValueError("Evidence must come from the configured external Prometheus /targets page")
    if metadata["pool"] != POOL or metadata["sha256"] != hashlib.sha256(data).hexdigest():
        raise ValueError("Capture provenance or image hash mismatch")
    if not data.startswith(b"\x89PNG\r\n\x1a\n") or not 1024 < len(data) <= 6 * 1024**2:
        raise ValueError("Expected a bounded actual PNG screenshot")
    return data, metadata


def assessment_cites(state, attachment):
    def references(value):
        if isinstance(value, dict):
            return any((key == "evidence_ids" and isinstance(item, list) and "A-" + attachment in item)
                       or references(item) for key, item in value.items())
        return isinstance(value, list) and any(references(item) for item in value)
    return references(state.get("assessment") or {})


def evidence_delivery(state, attachment):
    calls = [{"phase": call.get("phase"), "status": call.get("status"),
              "image_visible": "A-" + attachment in (call.get("visible_evidence_ids") or [])}
             for call in state.get("calls", [])]
    return {"calls": calls, "visible_in_any_call": any(call["image_visible"] for call in calls),
            "visible_in_every_call": bool(calls) and all(call["image_visible"] for call in calls)}


def reassess_existing(args):
    """One explicit post-fix review; never upload, reinject or replace prior results."""
    root = args.out.resolve()
    if not args.pixels_reviewed:
        raise ValueError("Inspect the external screenshot pixels before reassessment")
    label = getattr(args, "follow_up_label", None)
    if label and (len(label) > 40 or any(c not in "abcdefghijklmnopqrstuvwxyz0123456789-" for c in label)):
        raise ValueError("Follow-up label must be a short lowercase name")
    output = root / "reassessments" / label if label else root
    marker = output / "postfix-started.json"
    if marker.exists() or (output / "investigation-after-fix.json").exists():
        raise ValueError("Post-fix reassessment already attempted; inspect saved state instead of retrying")
    record = json.loads((root / "run.json").read_text())
    recovery = json.loads((root / "recovery.json").read_text())
    attempted = json.loads((root / "evaluation-started.json").read_text())
    previous = json.loads((root / "investigation-after.json").read_text())
    stored = json.loads((root / "attachment.json").read_text())
    _, metadata = validated_image(root, record["prometheus"])
    if record["outcome"] != "captured" or not recovery.get("restored") or not attempted.get("pixels_reviewed"):
        raise ValueError("Require a captured, reviewed and recovered original evaluation")
    if previous.get("episode_id") != record["episode_id"] or not previous.get("revision_id"):
        raise ValueError("Original investigation does not match the captured episode")
    base = record["fcapsule"] + "/api/episodes/" + record["episode_id"]
    current = request(base + "/investigation")
    expected_revision = getattr(args, "expected_revision", None) or previous["revision_id"]
    if current.get("revision_id") != expected_revision or current.get("status") not in TERMINAL:
        raise ValueError("Episode has newer or active work; refuse a mixed comparison")
    attachments = request(base + "/evidence")
    if len(attachments) != 1 or attachments[0].get("attachment_id") != stored["attachment_id"]:
        raise ValueError("Existing media differs from the original evaluation")
    attachment = attachments[0]
    if attachment.get("status") != "ready" or attachment.get("sha256") != metadata["sha256"]:
        raise ValueError("Existing attachment is not ready or its hash differs")
    if any(attachment.get(key) != stored.get(key) for key in ("extraction", "correction", "context_note", "observed_at")):
        raise ValueError("Existing evidence content changed since the original evaluation")
    output.mkdir(parents=True, exist_ok=True)
    with marker.open("x", encoding="utf-8") as handle:
        json.dump({"at": now(), "prior_revision_id": current["revision_id"],
                   "original_revision_id": previous["revision_id"],
                   "attachment_id": stored["attachment_id"], "pixels_reviewed": True,
                   "sha256": metadata["sha256"]}, handle, indent=2)
    save(output / "postfix-baseline.json", current)
    queued = request(base + "/investigation/update", {})
    save(output / "postfix-update-request.json", queued)
    if not queued.get("revision_id") or queued["revision_id"] == current["revision_id"]:
        raise RuntimeError("No new revision started; inspect state without retrying")
    after = wait_for(lambda: request(base + "/investigation"),
                     lambda r: r.get("revision_id") == queued["revision_id"] and r.get("status") in TERMINAL,
                     360, "Post-fix reassessment timeout")
    save(output / "investigation-after-fix.json", after)
    result = {"pipeline_status": after["status"], "policy_before": previous.get("policy_version"),
              "policy_after": after.get("policy_version"), "attachment_id": stored["attachment_id"],
              "image_cited_in_assessment": assessment_cites(after, stored["attachment_id"]),
              "delivery_before": evidence_delivery(previous, stored["attachment_id"]),
              "delivery_after": evidence_delivery(after, stored["attachment_id"]),
              "revision_linked": after.get("parent_revision_id") == current["revision_id"],
              "intervening_revision": current["revision_id"] != previous["revision_id"],
              "same_model": after.get("model") == previous.get("model"),
              "usage": after.get("usage"), "additional_vision_calls": 0,
              "value_verdict": "requires_human_review",
              "limitations": ["This is an explicit post-fix rerun, not an independent new incident.",
                              "Live-source observations can differ after recovery; not a screenshot-only causal ablation.",
                              "Image visibility and citations alone do not establish correctness."]}
    save(output / "evaluation-after-fix.json", result)
    print(json.dumps(result, indent=2), flush=True)


def evaluate(args):
    root = args.out.resolve()
    if not args.pixels_reviewed:
        raise ValueError("Inspect the external screenshot pixels, then pass --pixels-reviewed")
    record = json.loads((root / "run.json").read_text())
    if record["outcome"] != "captured" or not (root / "recovery.json").exists():
        raise ValueError("Require captured scenario and confirmed recovery before paid evaluation")
    if (root / "evaluation-started.json").exists():
        raise ValueError("An evaluation was already attempted; do not automatically repeat paid calls")
    data, metadata = validated_image(root, record["prometheus"])
    base = record["fcapsule"] + "/api/episodes/" + record["episode_id"]
    if request(base + "/evidence"):
        raise ValueError("Existing media would contaminate the no-image baseline")
    config = request(record["fcapsule"] + "/api/settings/media")
    if any(config[key]["capability"]["status"] != "ready" for key in ("vision", "core_investigator")):
        raise ValueError("Existing media and core settings must already be validated")
    before = wait_for(lambda: request(base + "/investigation"), lambda r: r["status"] in TERMINAL, 360, "Baseline investigation incomplete")
    if before["status"] != "ready" or not before.get("assessment"):
        raise ValueError("Require a usable automatic baseline; no extra paid retry is started")
    save(root / "investigation-before.json", before)
    save(root / "evaluation-started.json", {"at": now(), "pixels_reviewed": True, "sha256": metadata["sha256"]})
    attachment = request(base + "/evidence", {"kind": "image", "filename": "prometheus-targets.png",
        "content_base64": base64.b64encode(data).decode(), "observed_at": metadata["observed_at"],
        "context_note": "External Prometheus Targets page captured at the recorded observation time.", "source_redacted": False})
    save(root / "attachment-submitted.json", attachment)
    attachment_id = attachment["attachment_id"]
    extracted = wait_for(lambda: next(a for a in request(base + "/evidence") if a["attachment_id"] == attachment_id),
                         lambda a: a["status"] in TERMINAL, 140, "Vision extraction timeout")
    save(root / "attachment.json", extracted)
    if extracted["status"] != "ready":
        raise RuntimeError("Vision extraction failed; no automatic retry")
    queued = request(base + "/investigation/update", {})
    save(root / "update-request.json", queued)
    after = wait_for(lambda: request(base + "/investigation"),
                     lambda r: r.get("revision_id") == queued.get("revision_id") and r["status"] in TERMINAL,
                     360, "Reassessment timeout")
    save(root / "investigation-after.json", after)
    extraction = json.dumps(extracted.get("extraction", {})).lower()
    result = {"pipeline_status": after["status"], "attachment_id": attachment_id,
              "image_sha256_verified": extracted.get("sha256") == metadata["sha256"],
              "extraction_checks": {"http_404": "404" in extraction, "path": FAULT_PATH in extraction,
                                    "target_identity": "mysql-exporter" in extraction, "down_state": "down" in extraction},
              "image_cited_in_assessment": assessment_cites(after, attachment_id),
              "image_delivery": evidence_delivery(after, attachment_id),
              "revision_linked": after.get("parent_revision_id") == before.get("revision_id"),
              "same_model": before.get("model") == after.get("model"),
              "fingerprint_changed": bool(after.get("input_fingerprint")) and after.get("input_fingerprint") != before.get("input_fingerprint"),
              "vision_usage": extracted.get("usage"), "before_usage": before.get("usage"), "after_usage": after.get("usage"),
              "value_verdict": "requires_human_review", "limitations": [
                  "A ready pipeline or image citation is not diagnostic correctness.",
                  "Live-source reassessment after recovery is not a screenshot-only causal ablation.",
                  "A screenshot cannot establish database health or configuration cause on its own.",
              ]}
    save(root / "evaluation.json", result)
    print(json.dumps(result, indent=2), flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=("run", "evaluate", "reassess-existing", "restore", "watchdog"))
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--lab", default="http://192.168.0.102:30766")
    parser.add_argument("--prometheus", default="http://192.168.0.102:30090")
    parser.add_argument("--fcapsule", default="http://192.168.0.102:30765")
    parser.add_argument("--node", default="node")
    parser.add_argument("--pixels-reviewed", action="store_true")
    parser.add_argument("--expected-revision", help="Explicitly acknowledge a newer terminal baseline for reassess-existing")
    parser.add_argument("--follow-up-label", help="Named, separate follow-up after a further product fix; never overwrites earlier attempts")
    args = parser.parse_args()
    {"run": run, "evaluate": evaluate, "reassess-existing": reassess_existing,
     "restore": lambda a: restore(a.out), "watchdog": lambda a: watchdog(a.out)}[args.mode](args)


if __name__ == "__main__":
    main()
