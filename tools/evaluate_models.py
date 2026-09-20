"""Run paired FCAPSule model evaluations against one captured episode per case."""

from __future__ import annotations

import argparse
import csv
import json
import statistics
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from urllib.error import HTTPError
from urllib.request import Request, urlopen

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.scenario_catalog import SCENARIOS
from evaluation.scoring import available_evidence_domains, load_ground_truth, score_investigation, score_pipeline
from tools.run_scenarios import run_case, save


TERMINAL = {"ready", "incomplete", "not_configured"}


def request(url: str, payload: dict | None = None) -> dict:
    body = json.dumps(payload).encode("utf-8") if payload is not None else None
    req = Request(url, data=body, headers={"Content-Type": "application/json"})
    with urlopen(req, timeout=30) as response:
        return json.loads(response.read())


def configure_model(fcapsule: str, model: str, max_tokens: int) -> dict:
    return request(fcapsule.rstrip("/") + "/api/settings/ai", {"model": model, "max_tokens": max_tokens})


def wait_for_investigation(fcapsule: str, episode_id: str, timeout: int) -> dict:
    url = fcapsule.rstrip("/") + f"/api/episodes/{episode_id}/investigation"
    deadline = time.monotonic() + timeout
    latest: dict = {}
    while time.monotonic() < deadline:
        latest = request(url)
        if latest.get("status") in TERMINAL:
            return latest
        time.sleep(5)
    raise TimeoutError(f"Investigation {episode_id} did not finish; last status={latest.get('status')}")


def rerun(fcapsule: str, episode_id: str, timeout: int) -> dict:
    url = fcapsule.rstrip("/") + f"/api/episodes/{episode_id}/investigation"
    request(url, {})
    return wait_for_investigation(fcapsule, episode_id, timeout)


def usage(run: dict) -> dict:
    value = run.get("usage") or {}
    return {"prompt_tokens": value.get("prompt_tokens", 0),
            "completion_tokens": value.get("completion_tokens", 0),
            "total_tokens": value.get("total_tokens", 0),
            "complete": value.get("complete", False)}


def write_reports(folder: Path, rows: list[dict], models: list[str]) -> None:
    columns = ["scenario", "category", "model", "score", "label", "pipeline_score",
               "elapsed_seconds", "total_tokens", "input_fingerprint", "comparison_valid"]
    with (folder / "results.csv").open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=columns)
        writer.writeheader()
        writer.writerows({key: row.get(key) for key in columns} for row in rows)

    lines = ["# FCAPSule Paired Model Evaluation", "",
             "Every model in a scenario used the same retained FCAPSule input fingerprint. Ground truth was held by the Lab and was never sent to the investigator.", "",
             "| Scenario | Evidence | " + " | ".join(models) + " | Valid |",
             "|---|---|" + "---:|" * len(models) + "---|"]
    for scenario in SCENARIOS:
        members = [row for row in rows if row["scenario"] == scenario]
        values = {}
        for model in models:
            model_rows = [row for row in members if row["model"] == model]
            if model_rows:
                mean = statistics.mean(row["score"] for row in model_rows)
                values[model] = f"{mean:.1f}" + (f" mean / {len(model_rows)} runs" if len(model_rows) > 1 else f" ({model_rows[0]['label']})")
        valid = bool(members) and all(row["comparison_valid"] for row in members) and all(
            any(row["model"] == model for row in members) for model in models)
        lines.append(f"| {scenario} | {SCENARIOS[scenario]['evidence_group']} | "
                     + " | ".join(values.get(model, "missing") for model in models) + f" | {'yes' if valid else 'no'} |")
    lines.extend(["", "## Aggregate", ""])
    for model in models:
        members = [row for row in rows if row["model"] == model]
        scores = [row["score"] for row in members]
        if scores:
            lines.append(f"- `{model}`: mean {statistics.mean(scores):.1f}, median {statistics.median(scores):.1f}, "
                         f"{sum(row['total_tokens'] for row in members):,} tokens across {len(members)} cases.")
        else:
            lines.append(f"- `{model}`: no completed cases.")
    if len(models) == 2:
        wins = {models[0]: 0, models[1]: 0, "ties": 0}
        grouped = {}
        for row in rows:
            grouped.setdefault((row["scenario"], row.get("repetition", 1)), {})[row["model"]] = row
        for pair in grouped.values():
            if not all(model in pair and pair[model]["comparison_valid"] for model in models):
                continue
            difference = pair[models[0]]["score"] - pair[models[1]]["score"]
            if abs(difference) < 0.1:
                wins["ties"] += 1
            else:
                wins[models[0] if difference > 0 else models[1]] += 1
        lines.append(f"- Paired outcomes: `{models[0]}` {wins[models[0]]} wins, `{models[1]}` {wins[models[1]]} wins, {wins['ties']} ties.")
    lines.extend(["", "Scores are diagnostic rubric coverage, not a universal model accuracy claim. One run per case is an exploratory paired benchmark; use `--repetitions` for variance estimates.", ""])
    (folder / "REPORT.md").write_text("\n".join(lines), encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--lab", default="http://192.168.0.102:30766")
    parser.add_argument("--prometheus", default="http://192.168.0.102:30090")
    parser.add_argument("--fcapsule", default="http://192.168.0.102:30765")
    parser.add_argument("--models", nargs="+", default=["deepseek-v4-flash", "deepseek-v4-pro"])
    parser.add_argument("--scenario", choices=[*SCENARIOS, "all"], default="all")
    parser.add_argument("--repetitions", type=int, default=1)
    parser.add_argument("--duration", type=int, default=120)
    parser.add_argument("--baseline", type=int, default=45)
    parser.add_argument("--post-alert-hold", type=int, default=30)
    parser.add_argument("--minimum-log-lines", type=int, default=1000)
    parser.add_argument("--investigation-timeout", type=int, default=420)
    parser.add_argument("--max-tokens", type=int, default=3600)
    parser.add_argument("--out", type=Path, default=Path("artifacts"))
    args = parser.parse_args()
    if args.repetitions < 1:
        parser.error("--repetitions must be at least one")
    selected = list(SCENARIOS) if args.scenario == "all" else [args.scenario]
    oracle = load_ground_truth()
    original = request(args.fcapsule.rstrip("/") + "/api/settings/ai")
    folder = args.out / datetime.now(timezone.utc).strftime("evaluation-%Y%m%dT%H%M%SZ")
    folder.mkdir(parents=True)
    rows: list[dict] = []
    save(folder / "run-config.json", {"models": args.models, "scenarios": selected,
                                      "repetitions": args.repetitions, "started_at": datetime.now(timezone.utc).isoformat()})
    try:
        ordinal = 0
        for repetition in range(1, args.repetitions + 1):
            for scenario_id in selected:
                first = args.models[ordinal % len(args.models)]
                case_name = scenario_id if args.repetitions == 1 else f"{scenario_id}-r{repetition}"
                record: dict = {}
                try:
                    configure_model(args.fcapsule, first, args.max_tokens)
                    record = run_case(args, scenario_id, folder)
                    if case_name != scenario_id:
                        (folder / scenario_id).rename(folder / case_name)
                    if not record.get("fcapsule"):
                        raise RuntimeError(f"No FCAPSule episode captured for {scenario_id}")
                    episode_id = record["fcapsule"][0]["episode_id"]
                    outputs: dict[str, dict] = {}
                    initial = wait_for_investigation(args.fcapsule, episode_id, args.investigation_timeout)
                    if initial.get("model") == first:
                        outputs[first] = initial
                    for model in args.models:
                        if model in outputs:
                            continue
                        configure_model(args.fcapsule, model, args.max_tokens)
                        outputs[model] = rerun(args.fcapsule, episode_id, args.investigation_timeout)
                    record["captured_domains"] = sorted(available_evidence_domains(next(iter(outputs.values()))))
                    fingerprints = {item.get("input_fingerprint") for item in outputs.values()}
                    valid = len(fingerprints) == 1 and None not in fingerprints
                    case_dir = folder / case_name
                    for model, investigation in outputs.items():
                        model_file = model.replace("/", "_") + ".json"
                        save(case_dir / model_file, investigation)
                        diagnosis = score_investigation(oracle[scenario_id], investigation)
                        pipeline = score_pipeline(oracle[scenario_id], record, investigation)
                        result = {"scenario": scenario_id, "repetition": repetition, "category": oracle[scenario_id]["category"],
                                  "model": model, **diagnosis, "pipeline": pipeline,
                                  "pipeline_score": pipeline["score"], "elapsed_seconds": investigation.get("elapsed_seconds", 0),
                                  "usage": usage(investigation), "total_tokens": usage(investigation)["total_tokens"],
                                  "input_fingerprint": investigation.get("input_fingerprint"), "comparison_valid": valid,
                                  "episode_id": episode_id}
                        save(case_dir / (model.replace("/", "_") + "-score.json"), result)
                        rows.append(result)
                except (HTTPError, OSError, TimeoutError, RuntimeError) as error:
                    case_dir = folder / case_name
                    case_dir.mkdir(exist_ok=True)
                    failure = {"scenario": scenario_id, "repetition": repetition,
                               "error_type": type(error).__name__, "error": str(error),
                               "record": record}
                    save(case_dir / "evaluation-error.json", failure)
                    pipeline = score_pipeline(oracle[scenario_id], record, {})
                    for model in args.models:
                        result = {"scenario": scenario_id, "repetition": repetition,
                                  "category": oracle[scenario_id]["category"], "model": model,
                                  "score": 0.0, "label": "pipeline_failed", "status": "evaluation_error",
                                  "findings": [], "domains": [], "contradictions": [], "components": {},
                                  "pipeline": pipeline, "pipeline_score": pipeline["score"],
                                  "elapsed_seconds": 0, "usage": usage({}), "total_tokens": 0,
                                  "input_fingerprint": None, "comparison_valid": False,
                                  "episode_id": None, "evaluation_error": str(error)}
                        save(case_dir / (model.replace("/", "_") + "-score.json"), result)
                        rows.append(result)
                finally:
                    try:
                        request(args.lab.rstrip("/") + "/api/recover", {})
                    except OSError:
                        pass
                write_reports(folder, rows, args.models)
                ordinal += 1
    finally:
        configure_model(args.fcapsule, original["model"], int(original["max_tokens"]))
        try:
            request(args.lab.rstrip("/") + "/api/recover", {})
        except OSError:
            pass
    print(folder.resolve())


if __name__ == "__main__":
    main()
