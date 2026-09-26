import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from tools.evaluate_recorded_run import digest_json, main, reconcile


def retained_run(root, *, correct_diagnosis=True):
    investigation = {
        "status": "ready",
        "revision_id": "revision-1",
        "primary_incident_id": "incident-1",
        "input_fingerprint": "capsule-1",
        "context": {
            "alerts": [{"incident_id": "incident-1", "alert_identity": "LabWorkerPoisonRetries"}],
            "evidence": [{"id": "L1", "domain": "logs"}],
        },
        "assessment": {
            "likely_mechanism": (
                "The worker rejected malformed Base64 content and retried the same job."
                if correct_diagnosis else "A generic service failure occurred."
            ),
            "next_action": "Inspect the decoder contract and quarantine malformed payloads.",
            "uncertainty": "The original producer is not identified.",
            "evidence_ids": ["L1"],
        },
    }
    run = {
        "scenario": "poison-job",
        "outcome": "captured",
        "alert_observed": True,
        "observed_alerts": [{"labels": {"alertname": "LabWorkerPoisonRetries"}}],
        "expected_alert": "LabWorkerPoisonRetries",
        "owner": "owner-1",
        "control": {"run": {"run_id": "owner-1"}},
        "incident_id": "incident-1",
        "assessment_revision_id": "revision-1",
        "assessment_status": "ready",
        "assessment_matches_incident": True,
        "recovery": {"restored": True},
        "recovery_confirmed": True,
        "logs": {"worker": {"ok": True, "retained_lines": 12}},
    }
    run["assessment_sha256"] = digest_json(investigation)
    (root / "run.json").write_text(json.dumps(run), encoding="utf-8")
    (root / "investigation-before.json").write_text(json.dumps(investigation), encoding="utf-8")
    return run, investigation


class RecordedRunEvaluationTests(unittest.TestCase):
    def test_technical_completion_is_separate_from_diagnostic_usefulness(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            retained_run(root, correct_diagnosis=False)

            report = reconcile(root)

        self.assertEqual(report["technical_completion"]["status"], "complete")
        self.assertLess(report["diagnostic_usefulness"]["score"], 70)
        self.assertNotEqual(report["diagnostic_usefulness"]["label"], "correct_and_actionable")
        self.assertTrue(report["diagnostic_usefulness"]["human_review_required"])

    def test_expected_evidence_is_recorded_separately_and_no_product_input_is_sent(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            retained_run(root)

            report = reconcile(root)

        expected = report["expected_evidence"]
        self.assertEqual(expected["expected_alerts"], ["LabWorkerPoisonRetries"])
        self.assertIn("decode_failure", {item["id"] for item in expected["findings"]})
        self.assertTrue(expected["stored_separately_from_product_input"])
        self.assertTrue(report["product_input"].startswith("not_sent"))

    def test_fingerprint_mismatch_makes_technical_result_unverified(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            run, _ = retained_run(root)
            run["assessment_sha256"] = "stale"
            (root / "run.json").write_text(json.dumps(run), encoding="utf-8")

            report = reconcile(root)

        self.assertEqual(report["source"]["assessment_integrity"], "mismatch")
        self.assertEqual(report["technical_completion"]["status"], "incomplete_or_unverified")

    def test_recomputed_alert_and_incident_checks_do_not_trust_saved_booleans(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            run, investigation = retained_run(root)
            run["alert_observed"] = True
            run["observed_alerts"] = [{"labels": {"alertname": "LabUnrelatedAlert"}}]
            investigation["primary_incident_id"] = "neighboring-incident"
            run["assessment_sha256"] = digest_json(investigation)
            (root / "run.json").write_text(json.dumps(run), encoding="utf-8")
            (root / "investigation-before.json").write_text(json.dumps(investigation), encoding="utf-8")

            report = reconcile(root)

        self.assertFalse(report["technical_completion"]["pipeline_checks"]["expected_alert"])
        self.assertFalse(report["technical_completion"]["pipeline_checks"]["exact_incident_in_assessment"])
        self.assertEqual(report["technical_completion"]["status"], "incomplete_or_unverified")

    def test_cli_writes_a_new_report_and_preserves_it_on_rerun(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            retained_run(root)
            with patch("sys.argv", ["evaluate_recorded_run", "--run-dir", str(root)]):
                main()
            output = root / "evaluation-contract.json"
            original = output.read_bytes()
            with patch("sys.argv", ["evaluate_recorded_run", "--run-dir", str(root)]):
                with self.assertRaises(FileExistsError):
                    main()
            self.assertEqual(output.read_bytes(), original)


if __name__ == "__main__":
    unittest.main()
