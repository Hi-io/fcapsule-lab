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
        if check.get("status") == "completed":
            domains.update(_domain({"tool": check.get("tool"), "question": check.get("question")}))
    return domains


def evidence_domains(investigation: dict[str, Any]) -> set[str]:
    citations = cited_ids(investigation.get("assessment") or {})
    domains: set[str] = set()
    for item in (investigation.get("context") or {}).get("evidence", []):
        if str(item.get("id")) in citations:
            domains.update(_domain({"domain": item.get("domain"), "source": item.get("source_domain")}))
    for check in investigation.get("checks", []):
        if str(check.get("id")) not in citations:
            continue
        domains.update(_domain({"tool": check.get("tool"), "question": check.get("question")}))
    return domains


def score_investigation(scenario: dict[str, Any], investigation: dict[str, Any]) -> dict[str, Any]:
    status = investigation.get("status", "missing")
    if status not in {"ready", "incomplete"} or not investigation.get("assessment"):
        return {"score": 0.0, "label": "failed", "status": status, "findings": [],
                "domains": [], "contradictions": [], "components": {}}

    assessment = investigation["assessment"]
    text = normalize(assessment)
    finding_weight = sum(float(item["weight"]) for item in scenario["findings"]) or 1.0
    findings = [
        {"id": item["id"], "matched": criterion_matches(text, item["all"]), "weight": item["weight"]}
        for item in scenario["findings"]
    ]
    finding_score = 55 * sum(float(item["weight"]) for item in findings if item["matched"]) / finding_weight

    domains = evidence_domains(investigation)
    required = set(scenario["required_domains"])
    evidence_score = 20 * len(domains & required) / max(1, len(required))
    action_text = normalize({"next_action": assessment.get("next_action"),
                             "expected_finding": assessment.get("expected_finding")})
    action_matches = [group_matches(action_text, alternatives) for alternatives in scenario.get("actions", [])]
    action_score = 15 * sum(action_matches) / max(1, len(action_matches))
    contradictions = [phrase for phrase in scenario.get("contradictions", []) if phrase.casefold() in text]
    safety_score = (5 if normalize(assessment.get("uncertainty")) else 0) + (5 if not contradictions else 0)
    total = round(max(0.0, finding_score + evidence_score + action_score + safety_score), 1)
    label = ("correct_and_actionable" if total >= 90 else "substantially_correct" if total >= 70
             else "partially_helpful" if total >= 45 else "weak_or_misdirected")
    return {
        "score": total, "label": label, "status": status, "findings": findings,
        "domains": sorted(domains), "required_domains": sorted(required),
        "action_criteria_matched": action_matches, "contradictions": contradictions,
        "components": {"causal_findings": round(finding_score, 1), "cited_evidence": round(evidence_score, 1),
                       "next_action": round(action_score, 1), "epistemic_safety": safety_score},
    }


def score_pipeline(scenario: dict[str, Any], run: dict[str, Any], investigation: dict[str, Any]) -> dict[str, Any]:
    log_lines = sum(item.get("retained_lines", 0) for item in run.get("logs", {}).values())
    checks = {
        "expected_alert": bool(run.get("alert_observed")),
        "substantial_logs": log_lines >= int(run.get("minimum_retained_log_lines", 1000)),
        "capsule_ready": bool(investigation.get("input_fingerprint")),
        "required_domains_available": set(scenario["required_domains"]).issubset(set(run.get("captured_domains", []))),
    }
    weights = {"expected_alert": 35, "substantial_logs": 25, "capsule_ready": 20,
               "required_domains_available": 20}
    return {"score": sum(weights[key] for key, passed in checks.items() if passed),
            "checks": checks, "retained_log_lines": log_lines}
