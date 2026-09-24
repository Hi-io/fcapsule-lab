"""Deterministic, auditable scoring for saved FCAPSule investigations."""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parent


def load_ground_truth(path: Path = ROOT / "ground_truth.json") -> dict[str, dict[str, Any]]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    return {item["id"]: item for item in payload["scenarios"]}


def _text(value: Any) -> str:
    if isinstance(value, str):
        return value
    if isinstance(value, dict):
        return " ".join(_text(item) for item in value.values())
    if isinstance(value, list):
        return " ".join(_text(item) for item in value)
    return ""


def normalize(value: Any) -> str:
    return re.sub(r"\s+", " ", _text(value).casefold()).strip()


def group_matches(text: str, alternatives: list[str]) -> bool:
    return any(alternative.casefold() in text for alternative in alternatives)


def criterion_matches(text: str, criterion: list[list[str]]) -> bool:
    return all(group_matches(text, alternatives) for alternatives in criterion)


NEGATORS = re.compile(
    r"\b(?:not|no|never|without|cannot|can't|isn't|aren't|wasn't|weren't|doesn't|"
    r"don't|didn't|unlikely|unproven|unconfirmed|insufficient|rules? out|rather than)\b",
    re.I,
)


def positive_group_matches(text: str, alternatives: list[str]) -> bool:
    """Match an asserted phrase; a nearby explicit negation is not a finding."""
    for phrase in alternatives:
        for match in re.finditer(re.escape(phrase.casefold()), text):
            before = text[max(0, match.start() - 64):match.start()]
            clauses = re.split(r"[.!?;]|\bbut\b|\bhowever\b", before)
            before = clauses[-1]
            if not NEGATORS.search(before):
                return True
    return False


def cited_ids(assessment: dict[str, Any]) -> set[str]:
    found: set[str] = set()

    def visit(value: Any, key: str = "") -> None:
        if isinstance(value, dict):
            for child_key, child in value.items():
                visit(child, child_key)
        elif isinstance(value, list):
            if key.endswith("evidence_ids"):
                found.update(str(item) for item in value)
            else:
                for child in value:
                    visit(child, key)

    visit(assessment)
    return found


def available_citation_ids(investigation: dict[str, Any]) -> set[str]:
    available = {str(item.get("id")) for item in (investigation.get("context") or {}).get("evidence", [])
                 if item.get("id")}
    available.update(str(check.get("id")) for check in investigation.get("checks", [])
                     if check.get("id") and check.get("status") in {"completed", "ok"}
                     and check.get("result") not in (None, {}, [], ""))
    return available


def supported_assertions(assessment: dict[str, Any], investigation: dict[str, Any]) -> list[tuple[str, set[str]]]:
    """Pair a causal claim with its own citations, excluding unavailable evidence IDs."""
    available = available_citation_ids(investigation)
    hypotheses = assessment.get("hypotheses")
    if isinstance(hypotheses, list) and hypotheses:
        assertions = []
        for hypothesis in hypotheses:
            if not isinstance(hypothesis, dict) or hypothesis.get("status") != "supported":
                continue
            ids = {str(item) for item in hypothesis.get("evidence_ids", [])} & available
            explanation = normalize(hypothesis.get("explanation") or "")
            if explanation and ids:
                assertions.append((explanation, ids))
        return assertions

    ids = {str(item) for item in assessment.get("evidence_ids", [])} & available
    mechanism = normalize(assessment.get("likely_mechanism") or "")
    return [(mechanism, ids)] if mechanism and ids else []


def _domain(value: Any) -> set[str]:
    text = normalize(value)
    found: set[str] = set()
    if "log" in text or "opensearch" in text:
        found.add("logs")
    if any(word in text for word in ("metric", "performance", "timeseries", "prometheus")):
        found.add("metrics")
    if "config" in text or "kubernetes" in text:
        found.add("configuration")
    return found


def available_evidence_domains(investigation: dict[str, Any]) -> set[str]:
    domains: set[str] = set()
    for item in (investigation.get("context") or {}).get("evidence", []):
        domains.update(_domain({"domain": item.get("domain"), "title": item.get("title")}))
    for check in investigation.get("checks", []):
        result = check.get("result")
        if check.get("status") in {"completed", "ok"} and result not in (None, {}, [], ""):
            domains.update(_domain({"domain": check.get("domain"), "source": check.get("source_domain"),
                                    "tool": check.get("tool")}))
    return domains


def evidence_domains(investigation: dict[str, Any]) -> set[str]:
    citations = cited_ids(investigation.get("assessment") or {})
    domains: set[str] = set()
    for item in (investigation.get("context") or {}).get("evidence", []):
        if str(item.get("id")) in citations:
            domains.update(_domain({"domain": item.get("domain"), "source": item.get("source_domain")}))
    for check in investigation.get("checks", []):
        if (str(check.get("id")) not in citations or check.get("status") not in {"completed", "ok"}
                or check.get("result") in (None, {}, [], "")):
            continue
        domains.update(_domain({"domain": check.get("domain"), "source": check.get("source_domain"),
                                "tool": check.get("tool")}))
    return domains


def diagnostic_attribution(scenario: dict[str, Any], investigation: dict[str, Any]) -> dict[str, Any]:
    """Check whether a primary incident in retained alert context is the scored alert."""
    expected = scenario.get("expected_alert")
    context = investigation.get("context") or {}
    alerts = context.get("alerts") if isinstance(context, dict) else None
    if not expected or not isinstance(alerts, list) or not alerts:
        return {"status": "unavailable", "reason": "alert attribution metadata is absent"}

    primary = investigation.get("primary_incident_id")
    if not primary:
        return {"status": "mismatch", "reason": "context has alerts but no primary incident"}
    primary_alerts = [item for item in alerts if isinstance(item, dict)
                      and str(item.get("incident_id") or "") == str(primary)]
    if not primary_alerts:
        return {"status": "mismatch", "reason": "primary incident is not in retained alert context"}

    identities = set()
    for alert in primary_alerts:
        labels = alert.get("labels") if isinstance(alert.get("labels"), dict) else {}
        identity = alert.get("alert_identity") or alert.get("alertname") or labels.get("alertname")
        if identity:
            identities.add(str(identity))
    if not identities:
        return {"status": "unavailable", "reason": "primary alert identity is absent"}
    if expected not in identities:
        return {"status": "mismatch", "reason": "primary incident belongs to a different alert"}
    return {"status": "verified", "reason": "primary incident matches the expected alert"}


def score_investigation(scenario: dict[str, Any], investigation: dict[str, Any]) -> dict[str, Any]:
    attribution = diagnostic_attribution(scenario, investigation)
    status = investigation.get("status", "missing")
    if status not in {"ready", "incomplete", "inconclusive"} or not investigation.get("assessment"):
        return {"score": 0.0, "label": "failed", "status": status, "findings": [],
                "domains": [], "contradictions": [], "components": {},
                "attribution_check": attribution}

    if attribution["status"] == "mismatch":
        return {
            "score": 0.0, "label": "stale_incident_attribution", "status": status,
            "findings": [{"id": item["id"], "matched": False, "evidence_ids": [],
                          "weight": item["weight"]} for item in scenario["findings"]],
            "grounding_basis": "diagnostic scoring suppressed because the primary incident does not match the expected alert",
            "domains": [], "required_domains": sorted(set(scenario["required_domains"])),
            "action_criteria_matched": [False for _ in scenario.get("actions", [])],
            "contradictions": [],
            "components": {"causal_findings": 0.0, "cited_evidence": 0.0,
                           "next_action": 0.0, "epistemic_safety": 0},
            "attribution_check": attribution,
        }

    assessment = investigation["assessment"]
    assertions = supported_assertions(assessment, investigation)
    claim_text = " ".join(text for text, _ in assertions)
    finding_weight = sum(float(item["weight"]) for item in scenario["findings"]) or 1.0
    findings = [
        {"id": item["id"],
         "matched": any(
             all(positive_group_matches(text, alternatives) for alternatives in item["all"])
             for text, _evidence_ids in assertions
         ),
         "evidence_ids": sorted(set().union(*(
             evidence_ids for text, evidence_ids in assertions
             if all(positive_group_matches(text, alternatives) for alternatives in item["all"])
         ))),
         "weight": item["weight"]}
        for item in scenario["findings"]
    ]
    finding_score = 55 * sum(float(item["weight"]) for item in findings if item["matched"]) / finding_weight

    domains = evidence_domains(investigation)
    required = set(scenario["required_domains"])
    evidence_score = 20 * len(domains & required) / max(1, len(required))
    action_text = normalize({"next_action": assessment.get("next_action"),
                             "expected_finding": assessment.get("expected_finding")})
    action_matches = [positive_group_matches(action_text, alternatives) for alternatives in scenario.get("actions", [])]
    action_score = 15 * sum(action_matches) / max(1, len(action_matches))
    contradictions = [phrase for phrase in scenario.get("contradictions", [])
                      if positive_group_matches(claim_text, [phrase.casefold()])]
    safety_score = (5 if normalize(assessment.get("uncertainty")) else 0) + (5 if not contradictions else 0)
    total = round(max(0.0, finding_score + evidence_score + action_score + safety_score), 1)
    if assessment.get("provenance") == "deterministic_abstention":
        label = "inconclusive_with_retained_evidence"
    else:
        label = ("correct_and_actionable" if total >= 90 else "substantially_correct" if total >= 70
                 else "partially_helpful" if total >= 45 else "weak_or_misdirected")
    return {
        "score": total, "label": label, "status": status, "findings": findings,
        "grounding_basis": "claim-linked citations restricted to evidence present in this investigation",
        "attribution_check": attribution,
        "domains": sorted(domains), "required_domains": sorted(required),
        "action_criteria_matched": action_matches, "contradictions": contradictions,
        "components": {"causal_findings": round(finding_score, 1), "cited_evidence": round(evidence_score, 1),
                       "next_action": round(action_score, 1), "epistemic_safety": safety_score},
    }


def score_pipeline(scenario: dict[str, Any], run: dict[str, Any], investigation: dict[str, Any]) -> dict[str, Any]:
    """Score execution separately from diagnosis and report raw log volume only as context."""
    log_lines = sum(item.get("retained_lines", 0) for item in run.get("logs", {}).values()
                    if item.get("ok", True))
    status = investigation.get("status")
    terminal = status in {"ready", "incomplete", "inconclusive", "not_configured", "failed", "blocked"}
    usable = status in {"ready", "incomplete", "inconclusive"}
    expected_alert = run.get("alert_observed")
    if expected_alert is None:
        expected_name = run.get("expected_alert")
        observed = run.get("observed_alerts") or []
        expected_alert = bool(expected_name and any(
            (item.get("labels") or {}).get("alertname") == expected_name for item in observed
        ))
    if expected_alert is None:
        expected_alert = bool(run.get("outcome") == "captured" and run.get("alert.json"))
    # Episode membership alone is not enough: neighboring alerts may share an
    # episode while representing unrelated phases. Score only the queued primary.
    incident_id = run.get("incident_id")
    exact_incident = run.get("assessment_matches_incident")
    if exact_incident is None and incident_id:
        exact_incident = investigation.get("primary_incident_id") == incident_id
    if exact_incident is None:
        exact_incident = False
    recovery = run.get("recovery") or {}
    recovery_confirmed = bool(run.get("recovery_confirmed") or recovery.get("restored") or recovery.get("ok"))
    owner = run.get("owner_run_id") or run.get("owner") or (run.get("control") or {}).get("run", {}).get("run_id")
    fault_owned = bool(owner and (run.get("control") or {}).get("run", {}).get("run_id", owner) == owner)
    if run.get("scenario") == "mysql-exporter-scrape-path":
        fault_owned = bool(owner and run.get("outcome") == "captured")
    if "control" not in run and run.get("alert_observed") is not None:
        # Model-evaluation records retain the original runner's owned control acknowledgement.
        fault_owned = bool(run.get("injection_attempted", True) and run.get("alert_observed"))
    domains = set(run.get("captured_domains", []))
    required = set(scenario.get("required_domains", []))
    log_capture = run.get("logs") or {}
    logs_collected = bool(log_capture) and all(
        item.get("ok", False) for item in log_capture.values() if isinstance(item, dict)
    )
    checks = {
        "owned_fault_generation": fault_owned,
        "expected_alert": bool(expected_alert),
        "exact_incident_in_assessment": bool(exact_incident),
        "investigation_terminal": terminal,
        "investigation_usable": usable,
        "owned_recovery_confirmed": recovery_confirmed,
        "required_domains_available": required.issubset(domains),
        "local_log_collection_complete": logs_collected,
        "capsule_ready": bool(investigation.get("input_fingerprint")),
    }
    pipeline_weights = {
        "owned_fault_generation": 15,
        "expected_alert": 15,
        "exact_incident_in_assessment": 15,
        "investigation_terminal": 15,
        "investigation_usable": 20,
        "owned_recovery_confirmed": 20,
    }
    pipeline_score = sum(pipeline_weights[key] for key in pipeline_weights if checks[key])
    observability_score = round(100 * sum((
        bool(checks["required_domains_available"]),
        bool(checks["local_log_collection_complete"]),
        bool(checks["capsule_ready"]),
    )) / 3, 1)
    return {
        "version": 2,
        "score": pipeline_score,
        "pipeline_score": pipeline_score,
        "observability_score": observability_score,
        "checks": checks,
        "available_domains": sorted(domains),
        "required_domains": sorted(required),
        "retained_log_lines_context_only": log_lines,
        "log_count_scope": "bounded local kubectl tails; not total emitted, indexed or model-read records",
        "investigation_status": status or "missing",
        "human_review_required": True,
    }
