import unittest
import tempfile
from pathlib import Path

from app.scenario_catalog import SCENARIOS
from evaluation.scoring import load_ground_truth, score_investigation, score_pipeline
from tools.evaluate_models import observation_fingerprint, write_reports


class EvaluationTests(unittest.TestCase):
    def setUp(self):
        self.oracle = load_ground_truth()

    def test_ground_truth_matches_public_catalog_without_being_public_metadata(self):
        self.assertEqual(set(self.oracle), set(SCENARIOS))
        self.assertEqual(
            {group: sum(item["category"] == group for item in self.oracle.values())
             for group in ("logs", "metrics", "configuration")},
            {"logs": 5, "metrics": 5, "configuration": 5},
        )
        self.assertNotIn("mechanism", SCENARIOS["timeout-budget"])
        self.assertTrue(all(self.oracle[key]["expected_alert"] == value["expected_alert"]
                            for key, value in SCENARIOS.items()))

    def test_observation_fingerprint_is_stable_and_excludes_model_assessment(self):
        run = {
            "checks": [{"tool": "search_logs", "arguments": {"query": "timeout"},
                        "status": "ok", "result": {"count": 4}}],
            "assessment": {"likely_mechanism": "first model wording"},
        }
        same_observations = {
            "checks": list(run["checks"]),
            "assessment": {"likely_mechanism": "different model wording"},
        }
        changed_observations = {
            "checks": [{"tool": "search_logs", "arguments": {"query": "timeout"},
                        "status": "ok", "result": {"count": 5}}],
        }

        self.assertEqual(observation_fingerprint(run), observation_fingerprint(same_observations))
        self.assertNotEqual(observation_fingerprint(run), observation_fingerprint(changed_observations))

    def test_concept_groups_accept_paraphrases_and_require_all_parts(self):
        run = {
            "status": "ready",
            "assessment": {
                "likely_mechanism": "MySQL error 1062 shows a unique constraint collision because the same reservation token was reused for multiple reservations.",
                "next_action": "Fix token generation and inspect reservation_events before retrying.",
                "expected_finding": "Each order receives a unique key.",
                "uncertainty": "The producer path has not yet been inspected.",
                "evidence_ids": ["L1"],
            },
            "context": {"evidence": [{"id": "L1", "domain": "log_template"}]},
        }
        result = score_investigation(self.oracle["reservation-token-collision"], run)
        self.assertGreaterEqual(result["score"], 90)
        self.assertEqual(result["domains"], ["logs"])
        run["assessment"]["likely_mechanism"] = "A generic database error occurred."
        self.assertLess(score_investigation(self.oracle["reservation-token-collision"], run)["score"], result["score"])

    def test_only_cited_domains_receive_evidence_credit(self):
        run = {
            "status": "ready",
            "assessment": {"likely_mechanism": "PBKDF2 rounds caused CPU quota saturation.",
                           "next_action": "Benchmark and reduce rounds.", "uncertainty": "One worker observed.",
                           "evidence_ids": ["L1"]},
            "context": {"evidence": [{"id": "L1", "domain": "log_template"},
                                      {"id": "M1", "domain": "metric_anomaly"}]},
        }
        result = score_investigation(self.oracle["cpu-saturation"], run)
        self.assertEqual(result["domains"], ["logs"])
        self.assertEqual(result["components"]["cited_evidence"], 10.0)

    def test_negated_and_speculative_hypotheses_do_not_count_as_findings(self):
        run = {
            "status": "ready",
            "assessment": {
                "likely_mechanism": "The CPU is high, but PBKDF2 is not supported and is unlikely to explain it.",
                "hypotheses": [{"status": "unresolved", "explanation": "PBKDF2 rounds may be expensive."}],
                "next_action": "Check worker CPU quota and compare completed credential work.",
                "uncertainty": "The current logs do not include work-factor settings.",
                "evidence_ids": ["M1"],
            },
            "context": {"evidence": [{"id": "M1", "domain": "metric_anomaly"}]},
        }
        result = score_investigation(self.oracle["cpu-saturation"], run)
        self.assertFalse(any(item["matched"] for item in result["findings"]))

    def test_failed_or_empty_cited_check_does_not_satisfy_evidence_domain(self):
        run = {
            "status": "ready",
            "assessment": {"likely_mechanism": "The ServiceMonitor selector does not match the Service label.",
                           "next_action": "Read Service metadata.", "uncertainty": "Selector evidence unavailable.",
                           "evidence_ids": ["Q1"]},
            "context": {"evidence": []},
            "checks": [{"id": "Q1", "tool": "scrape_discovery", "question": "inspect configuration",
                        "status": "failed", "domain": "configuration", "result": {"error": "forbidden"}}],
        }
        self.assertEqual(score_investigation(self.oracle["response-contract"], run)["domains"], [])

    def test_pipeline_does_not_count_failed_log_collection_toward_volume(self):
        run = {"alert_observed": True,
               "logs": {"orders-api": {"ok": False, "retained_lines": 5000}},
               "minimum_retained_log_lines": 1000, "captured_domains": ["logs"]}
        investigation = {"status": "ready", "input_fingerprint": "x", "assessment": {"summary": "captured"}}
        result = score_pipeline(self.oracle["response-contract"], run, investigation)
        self.assertFalse(result["checks"]["substantial_logs"])
        self.assertEqual(result["retained_log_lines"], 0)

    def test_contract_rubric_accepts_structured_status_but_penalizes_false_transport_claim(self):
        run = {
            "status": "ready",
            "assessment": {
                "likely_mechanism": "Inventory schema was rejected with upstream_status 200, so this was not connectivity.",
                "next_action": "Inspect response serialization and compare payload fields.",
                "expected_finding": "The legacy response omits the v1 fields.",
                "uncertainty": "One dependency path was sampled.",
                "evidence_ids": ["L1"],
            },
            "context": {"evidence": [{"id": "L1", "domain": "log_template"}]},
        }
        correct = score_investigation(self.oracle["response-contract"], run)
        self.assertGreaterEqual(correct["score"], 90)
        run["assessment"]["likely_mechanism"] += " Inventory-api intermittently returned HTTP 502."
        contradicted = score_investigation(self.oracle["response-contract"], run)
        self.assertLess(contradicted["score"], correct["score"])
        self.assertEqual(len(contradicted["contradictions"]), 1)

    def test_pipeline_and_diagnosis_are_scored_separately(self):
        run = {"alert_observed": False, "logs": {"orders": {"retained_lines": 20}},
               "minimum_retained_log_lines": 1000, "captured_domains": ["logs"]}
        investigation = {"status": "ready", "input_fingerprint": "same", "assessment": {"summary": "No cause"}}
        pipeline = score_pipeline(self.oracle["response-contract"], run, investigation)
        self.assertEqual(pipeline["score"], 40)
        self.assertFalse(pipeline["checks"]["expected_alert"])

    def test_grounded_abstention_is_recorded_separately_from_a_pipeline_failure(self):
        run = {
            "status": "inconclusive",
            "assessment": {
                "provenance": "deterministic_abstention",
                "likely_mechanism": "No mechanism is asserted.",
                "next_action": "Review retained evidence and reassess.",
                "expected_finding": "A cited observation supports a mechanism.",
                "uncertainty": "No validated model conclusion is available.",
                "evidence_ids": ["L1"],
            },
            "context": {"evidence": [{"id": "L1", "domain": "log_template"}]},
        }
        result = score_investigation(self.oracle["response-contract"], run)
        self.assertEqual(result["status"], "inconclusive")
        self.assertEqual(result["label"], "inconclusive_with_retained_evidence")

    def test_live_source_differences_are_not_reported_as_strict_paired_wins(self):
        rows = []
        for model, observation, score in (("flash", "obs-a", 95), ("pro", "obs-b", 60)):
            rows.append({"scenario": "poison-job", "repetition": 1, "model": model,
                         "score": score, "label": "test", "comparison_valid": True,
                         "live_observations_equivalent": False, "observation_fingerprint": observation,
                         "input_fingerprint": "same", "category": "logs", "pipeline_score": 100,
                         "elapsed_seconds": 1, "total_tokens": 20})
        with tempfile.TemporaryDirectory() as directory:
            folder = Path(directory)
            write_reports(folder, rows, ["flash", "pro"])
            report = (folder / "REPORT.md").read_text()
        self.assertIn("`flash` 0 wins, `pro` 0 wins, 0 ties", report)
        self.assertIn("strictly comparable: 0; contextual live-source differences: 1", report)


if __name__ == "__main__":
    unittest.main()
