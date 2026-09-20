import unittest

from app.scenario_catalog import SCENARIOS
from evaluation.scoring import load_ground_truth, score_investigation, score_pipeline


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

    def test_pipeline_and_diagnosis_are_scored_separately(self):
        run = {"alert_observed": False, "logs": {"orders": {"retained_lines": 20}},
               "minimum_retained_log_lines": 1000, "captured_domains": ["logs"]}
        investigation = {"status": "ready", "input_fingerprint": "same", "assessment": {"summary": "No cause"}}
        pipeline = score_pipeline(self.oracle["response-contract"], run, investigation)
        self.assertEqual(pipeline["score"], 40)
        self.assertFalse(pipeline["checks"]["expected_alert"])


if __name__ == "__main__":
    unittest.main()
