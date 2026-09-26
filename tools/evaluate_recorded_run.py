"""Reconcile a saved Lab run without contacting or mutating any live service."""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
ORACLE_FILES = {
    "frozen_workload_benchmark": ROOT / "evaluation/ground_truth.json",
    "monitoring_discovery_demo": ROOT / "evaluation/operational_ground_truth.json",
}


def read_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def digest_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def digest_json(value: Any) -> str:
    canonical = json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
    return digest_bytes(canonical)


def oracle_for(scenario_id: str) -> tuple[dict[str, Any], str, str]:
    for scope, path in ORACLE_FILES.items():
        payload = read_json(path)
        for scenario in payload["scenarios"]:
            if scenario.get("id") == scenario_id:
                return scenario, scope, path.name
    raise ValueError(f"No independent evaluation rubric exists for scenario {scenario_id!r}")


def _saved_score(path: Path, *keys: str) -> Any:
    if not path.exists():
        return None
    value = read_json(path)
    for key in keys:
        if key in value:
            return value[key]
    return None


def reconcile(run_dir: Path) -> dict[str, Any]:
    from evaluation.scoring import available_evidence_domains, score_investigation, score_pipeline

    run_path = run_dir / "run.json"
    investigation_path = run_dir / "investigation-before.json"
    run = read_json(run_path)
    investigation = read_json(investigation_path)
    scenario_id = run.get("scenario")
    if not isinstance(scenario_id, str) or not scenario_id:
        raise ValueError("run.json must identify its scenario")
    oracle, scope, oracle_name = oracle_for(scenario_id)

    assessment_hash = digest_json(investigation)
    expected_assessment_hash = run.get("assessment_sha256")
    assessment_integrity = (
        "verified" if expected_assessment_hash == assessment_hash
        else "mismatch" if expected_assessment_hash
        else "unavailable"
    )
    primary_matches_run = bool(
        run.get("incident_id") and investigation.get("primary_incident_id") == run.get("incident_id")
    )
    status_matches_run = run.get("assessment_status") == investigation.get("status")
    revision_matches_run = bool(
        run.get("assessment_revision_id")
        and run.get("assessment_revision_id") == investigation.get("revision_id")
    )

    diagnostic = score_investigation(oracle, investigation)
    diagnostic.update(scenario=scenario_id, scope=scope, requires_human_review=True)

    expected_alerts = oracle.get("acceptable_primary_alerts") or [oracle.get("expected_alert")]
    expected_alerts = [item for item in expected_alerts if item]
    observed_alerts = run.get("observed_alerts") or []
    if not observed_alerts and (run_dir / "alert.json").exists():
        observed_alerts = read_json(run_dir / "alert.json")
    expected_alert_observed = any(
        (item.get("labels") or {}).get("alertname") in expected_alerts
        for item in observed_alerts if isinstance(item, dict)
    )
    pipeline_input = dict(run)
    pipeline_input["captured_domains"] = sorted(available_evidence_domains(investigation))
    pipeline_input["alert_observed"] = expected_alert_observed
    pipeline_input["assessment_matches_incident"] = primary_matches_run
    recovery = run.get("recovery")
    if not isinstance(recovery, dict) and (run_dir / "recovery.json").exists():
        recovery = read_json(run_dir / "recovery.json")
    recovery = recovery if isinstance(recovery, dict) else {}
    pipeline_input["recovery"] = recovery
    pipeline_input["recovery_confirmed"] = bool(recovery.get("restored") or recovery.get("ok"))
    pipeline = score_pipeline(oracle, pipeline_input, investigation)
    pipeline_checks_complete = all(pipeline["checks"].values())
    technically_complete = (
        run.get("outcome") == "captured"
        and pipeline_checks_complete
        and pipeline["observability_score"] == 100
        and assessment_integrity == "verified"
        and status_matches_run
        and revision_matches_run
    )

    recorded_diagnostic = {
        "run_json": run.get("diagnostic_score"),
        "diagnostic-score.json": _saved_score(run_dir / "diagnostic-score.json", "score"),
    }
    recorded_pipeline = {
        "run_json": run.get("pipeline_score"),
        "pipeline-score.json": _saved_score(run_dir / "pipeline-score.json", "pipeline_score", "score"),
    }
    recorded_observability = {
        "run_json": run.get("observability_score"),
        "pipeline-score.json": _saved_score(run_dir / "pipeline-score.json", "observability_score"),
    }

    expected_evidence = {
        "category": oracle.get("category"),
        "target_service": oracle.get("target_service"),
        "expected_alerts": expected_alerts,
        "decisive_mechanism": oracle.get("mechanism"),
        "required_domains": oracle.get("required_domains", []),
        "findings": [
            {"id": item["id"], "weight": item["weight"], "criteria_groups": item["all"]}
            for item in oracle.get("findings", [])
        ],
        "action_criteria": oracle.get("actions", []),
        "contradictions": oracle.get("contradictions", []),
        "media_expectation": oracle.get("media_expectation"),
        "stored_separately_from_product_input": True,
    }

    def consistency(recorded: dict[str, Any], recomputed: Any) -> dict[str, Any]:
        values = {key: value for key, value in recorded.items() if value is not None}
        return {
            "recorded_values": recorded,
            "recomputed_value": recomputed,
            "matches": all(value == recomputed for value in values.values()) if values else None,
        }

    return {
        "contract_version": 1,
        "scenario_id": scenario_id,
        "scope": scope,
        "source": {
            "run_sha256": digest_bytes(run_path.read_bytes()),
            "investigation_sha256": assessment_hash,
            "recorded_assessment_sha256": expected_assessment_hash,
            "assessment_integrity": assessment_integrity,
            "recorded_assessment_status_matches": status_matches_run,
            "recorded_assessment_revision_matches": revision_matches_run,
            "primary_incident_matches_run": primary_matches_run,
            "oracle_file": oracle_name,
            "oracle_sha256": digest_bytes(ORACLE_FILES[scope].read_bytes()),
        },
        "technical_completion": {
            "status": "complete" if technically_complete else "incomplete_or_unverified",
            "run_outcome": run.get("outcome", "missing"),
            "pipeline_score": pipeline["pipeline_score"],
            "pipeline_checks": pipeline["checks"],
            "observability_score": pipeline["observability_score"],
            "available_domains": pipeline["available_domains"],
            "required_domains": pipeline["required_domains"],
            "assessment_integrity": assessment_integrity,
            "assessment_status_matches": status_matches_run,
            "assessment_revision_matches": revision_matches_run,
        },
        "diagnostic_usefulness": {
            "score": diagnostic["score"],
            "label": diagnostic["label"],
            "status": diagnostic["status"],
            "findings": diagnostic.get("findings", []),
            "domains": diagnostic.get("domains", []),
            "required_domains": diagnostic.get("required_domains", []),
            "action_criteria_matched": diagnostic.get("action_criteria_matched", []),
            "contradictions": diagnostic.get("contradictions", []),
            "components": diagnostic.get("components", {}),
            "attribution_check": diagnostic.get("attribution_check"),
            "human_review_required": True,
            "limitation": "The rubric is a lexical screening aid; it does not establish operational usefulness without human review.",
        },
        "expected_evidence": expected_evidence,
        "reconciliation": {
            "diagnostic_score": consistency(recorded_diagnostic, diagnostic["score"]),
            "pipeline_score": consistency(recorded_pipeline, pipeline["pipeline_score"]),
            "observability_score": consistency(recorded_observability, pipeline["observability_score"]),
        },
        "product_input": "not_sent; this tool only reads local run artifacts and repository oracle files",
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-dir", required=True, type=Path,
                        help="Saved scenario directory containing run.json and investigation-before.json")
    parser.add_argument("--out", type=Path,
                        help="New report path; defaults to <run-dir>/evaluation-contract.json")
    args = parser.parse_args()
    run_dir = args.run_dir.resolve()
    report_path = args.out.resolve() if args.out else run_dir / "evaluation-contract.json"
    if not run_dir.is_dir():
        parser.error("--run-dir must be an existing saved scenario directory")
    report = reconcile(run_dir)
    report_path.parent.mkdir(parents=True, exist_ok=True)
    with report_path.open("x", encoding="utf-8") as handle:
        json.dump(report, handle, indent=2)
        handle.write("\n")
    print(json.dumps({
        "report": str(report_path),
        "scenario": report["scenario_id"],
        "technical_completion": report["technical_completion"]["status"],
        "diagnostic_usefulness": report["diagnostic_usefulness"]["label"],
    }, indent=2))


if __name__ == "__main__":
    main()
