import unittest
import tempfile
from pathlib import Path

from app.scenario_catalog import DISCOVERY_SCENARIOS, SCENARIOS
from evaluation.scoring import diagnostic_attribution, load_ground_truth, score_investigation, score_pipeline
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
        self.assertEqual(self.oracle["poison-job"]["required_domains"], ["logs"])
        for scenario in ("downstream-latency", "lock-contention"):
            self.assertEqual(self.oracle[scenario]["acceptable_primary_alerts"],
                             SCENARIOS[scenario]["acceptable_primary_alerts"])

    def test_diagnostic_attribution_accepts_declared_cofiring_primary_alerts_only(self):
        scenario = self.oracle["downstream-latency"]
        investigation = {
            "primary_incident_id": "incident-dependency-latency",
            "context": {"alerts": [{
                "incident_id": "incident-dependency-latency",
                "labels": {"alertname": "LabInventoryDependencyLatencyHigh"},
            }]},
        }
        self.assertEqual(diagnostic_attribution(scenario, investigation)["status"], "verified")
        investigation["context"]["alerts"][0]["labels"]["alertname"] = "LabCheckoutFailureRateHigh"
        self.assertEqual(diagnostic_attribution(scenario, investigation)["status"], "mismatch")

    def test_monitoring_discovery_demos_have_a_separate_diagnostic_rubric(self):
        operational = load_ground_truth(Path(__file__).resolve().parents[1] / "evaluation/operational_ground_truth.json")
        self.assertEqual(set(operational), set(DISCOVERY_SCENARIOS))
        for scenario, oracle in operational.items():
            self.assertEqual(oracle["expected_alert"], DISCOVERY_SCENARIOS[scenario]["expected_alert"])
            self.assertIn("metrics", oracle["required_domains"])
            self.assertIn("configuration", oracle["required_domains"])

    def test_service_label_discovery_diagnosis_requires_monitor_and_service_evidence(self):
        operational = load_ground_truth(Path(__file__).resolve().parents[1] / "evaluation/operational_ground_truth.json")
        run = {
            "status": "ready",
            "assessment": {
                "likely_mechanism": "The orders-api target is absent from Prometheus discovery because the ServiceMonitor expects fcapsule.io/app-metrics=true but the Service is labeled fcapsule.io/app-metrics=ture. The Pods remain Ready, so this is a loss of metrics coverage, not an application outage.",
                "next_action": "Restore the Service label to match the selector, then verify the target returns UP.",
                "uncertainty": "The observed mismatch explains the current missing target.",
                "evidence_ids": ["M1", "C1"],
            },
            "context": {"evidence": [{"id": "M1", "domain": "metrics"}, {"id": "C1", "domain": "configuration"}]},
        }
        result = score_investigation(operational["metrics-service-label-drift"], run)
        self.assertGreaterEqual(result["score"], 90)
        self.assertEqual(set(result["domains"]), {"metrics", "configuration"})

    def test_exporter_scrape_diagnosis_distinguishes_discovery_from_http_failure(self):
        operational = load_ground_truth(Path(__file__).resolve().parents[1] / "evaluation/operational_ground_truth.json")
        run = {
            "status": "ready",
            "assessment": {
                "likely_mechanism": "The MySQL exporter target remains discovered but is DOWN because the ServiceMonitor requests /metrics-v2 and receives HTTP 404; the exporter serves /metrics and its pod is Ready.",
                "next_action": "Restore the scrape path to /metrics and verify the target is UP with fresh metrics.",
                "uncertainty": "The observed 404 and active ServiceMonitor path support this cause.",
                "evidence_ids": ["M1", "C1"],
            },
            "context": {"evidence": [{"id": "M1", "domain": "metrics"}, {"id": "C1", "domain": "configuration"}]},
        }
        result = score_investigation(operational["mysql-exporter-scrape-path"], run)
        self.assertGreaterEqual(result["score"], 90)
        self.assertEqual(set(result["domains"]), {"metrics", "configuration"})

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

    def test_idempotency_rubric_accepts_a_specific_paraphrase(self):
        run = {
            "status": "ready",
            "primary_incident_id": "incident-current",
            "assessment": {
                "likely_mechanism": "Orders reused an idempotency key for a different order, so the local idempotency store rejected it before any inventory request.",
                "next_action": "Scope key generation to an order and preserve the original response for legitimate retries.",
                "uncertainty": "The captured records do not show the caller that supplied the duplicate key.",
                "evidence_ids": ["L1"],
            },
            "context": {
                "alerts": [{"incident_id": "incident-current", "alert_identity": "LabOrdersIdempotencyConflicts"}],
                "evidence": [{"id": "L1", "domain": "log_template"}],
            },
        }
        result = score_investigation(self.oracle["idempotency-conflict"], run)
        order_identity = next(item for item in result["findings"] if item["id"] == "order_identity")
        self.assertTrue(order_identity["matched"])
        self.assertGreaterEqual(result["score"], 90)
        self.assertEqual(result["attribution_check"]["status"], "verified")

    def test_saved_poison_job_paraphrase_counts_as_same_job_redelivery(self):
        run = {
            "status": "ready",
            "assessment": {
                "likely_mechanism": "The worker rejected malformed import content and retried the job.",
                "hypotheses": [{
                    "status": "supported",
                    "explanation": "A specific job was poisoned and repeatedly retried, causing the alert.",
                    "evidence_ids": ["L1"],
                }],
                "next_action": "Inspect the decoder contract and retained job.",
                "uncertainty": "The producer of the malformed content is not identified.",
                "evidence_ids": ["L1"],
            },
            "context": {"evidence": [{"id": "L1", "domain": "log_template"}]},
        }
        result = score_investigation(self.oracle["poison-job"], run)
        redelivery = next(item for item in result["findings"] if item["id"] == "redelivery")
        self.assertTrue(redelivery["matched"])
        self.assertEqual(redelivery["evidence_ids"], ["L1"])

    def test_saved_memory_accumulation_paraphrase_counts_without_claiming_threshold(self):
        run = {
            "status": "ready",
            "assessment": {
                "likely_mechanism": "Memory pressure from buffered export pages retained past delivery.",
                "hypotheses": [{
                    "status": "supported",
                    "explanation": "Buffered report export pages stayed retained, causing memory accumulation and buffer pressure.",
                    "evidence_ids": ["L1"],
                }],
                "next_action": "Inspect pod memory metrics and the memory breakdown at alert time.",
                "uncertainty": "No direct memory gauge is available for the alert interval.",
                "evidence_ids": ["L1"],
            },
            "context": {"evidence": [{"id": "L1", "domain": "log_template"}]},
        }
        result = score_investigation(self.oracle["memory-leak"], run)
        retention = next(item for item in result["findings"] if item["id"] == "buffer_retention")
        pressure = next(item for item in result["findings"] if item["id"] == "measured_pressure")
        self.assertTrue(retention["matched"])
        self.assertFalse(pressure["matched"])
        self.assertEqual(result["action_criteria_matched"], [False, True])
        self.assertEqual(result["domains"], ["logs"])

    def test_stale_primary_incident_cannot_score_against_neighboring_alert(self):
        run = {
            "status": "ready",
            "primary_incident_id": "older-incident",
            "assessment": {
                "likely_mechanism": "MySQL error 1062 shows a unique reservation token collision.",
                "next_action": "Inspect token generation and reservation events.",
                "uncertainty": "The request identities are not retained.",
                "evidence_ids": ["L1"],
            },
            "context": {
                "alerts": [
                    {"incident_id": "older-incident", "alert_identity": "LabOrdersDependencyDocumentInvalid"},
                    {"incident_id": "current-incident", "alert_identity": "LabInventoryConstraintFailures"},
                ],
                "evidence": [{"id": "L1", "domain": "log_template"}],
            },
        }
        result = score_investigation(self.oracle["reservation-token-collision"], run)
        self.assertEqual(result["score"], 0.0)
        self.assertEqual(result["label"], "stale_incident_attribution")
        self.assertEqual(result["attribution_check"]["status"], "mismatch")
        self.assertFalse(any(item["matched"] for item in result["findings"]))

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

    def test_supported_causal_claim_needs_its_own_available_evidence_ids(self):
        run = {
            "status": "ready",
            "assessment": {
                "likely_mechanism": "A generic database error occurred.",
                "hypotheses": [{"status": "supported",
                                "explanation": "MySQL error 1062 shows a unique constraint collision because the same reservation token was reused for multiple reservations.",
                                "evidence_ids": ["NOT-IN-RECORD"]}],
                "next_action": "Fix token generation and inspect reservation_events.",
                "uncertainty": "The indexed logs show one failure family.",
                "evidence_ids": ["L1"],
            },
            "context": {"evidence": [{"id": "L1", "domain": "log_template"}]},
        }
        result = score_investigation(self.oracle["reservation-token-collision"], run)
        self.assertFalse(any(item["matched"] for item in result["findings"]))

        run["assessment"]["hypotheses"][0]["evidence_ids"] = ["L1"]
        result = score_investigation(self.oracle["reservation-token-collision"], run)
        self.assertTrue(result["findings"][0]["matched"])
        self.assertEqual(result["findings"][0]["evidence_ids"], ["L1"])

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
        self.assertFalse(result["checks"]["local_log_collection_complete"])
        self.assertEqual(result["retained_log_lines_context_only"], 0)

    def test_negated_recommendation_does_not_satisfy_an_action_criterion(self):
        run = {
            "status": "ready",
            "assessment": {
                "likely_mechanism": "CPU quota is heavily used during credential migration.",
                "next_action": "Do not increase the CPU quota; consider scaling unrelated work instead.",
                "uncertainty": "The samples show pressure but not CFS throttling.",
                "evidence_ids": ["M1"],
            },
            "context": {"evidence": [{"id": "M1", "domain": "metrics"}]},
        }
        result = score_investigation(self.oracle["cpu-saturation"], run)
        self.assertEqual(result["action_criteria_matched"], [False, False])

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
        self.assertEqual(pipeline["score"], 35)
        self.assertFalse(pipeline["checks"]["expected_alert"])
        self.assertFalse(pipeline["checks"]["owned_fault_generation"])
        self.assertTrue(pipeline["checks"]["investigation_terminal"])
        self.assertTrue(pipeline["checks"]["investigation_usable"])
        self.assertEqual(pipeline["retained_log_lines_context_only"], 20)

    def test_pipeline_accepts_a_declared_cofiring_primary_alert(self):
        run = {
            "expected_alert": "LabCheckoutLatencyHigh",
            "acceptable_primary_alerts": ["LabCheckoutLatencyHigh", "LabInventoryDependencyLatencyHigh"],
            "observed_alerts": [{"labels": {"alertname": "LabInventoryDependencyLatencyHigh"}}],
        }
        pipeline = score_pipeline(self.oracle["downstream-latency"], run, {"status": "missing"})
        self.assertTrue(pipeline["checks"]["expected_alert"])

    def test_failed_investigation_is_terminal_but_not_a_successful_pipeline(self):
        run = {"alert_observed": True, "expected_alert": "LabOrdersDependencyDocumentInvalid",
               "observed_alerts": [{"labels": {"alertname": "LabOrdersDependencyDocumentInvalid"}}],
               "owner_run_id": "run-1", "control": {"run": {"run_id": "run-1"}},
               "incident_id": "incident-1", "assessment_context_contains_incident": True,
               "assessment_matches_incident": False,
               "recovery": {"ok": True}}
        investigation = {"status": "failed", "primary_incident_id": "older-incident",
                         "context": {"alerts": [{"incident_id": "incident-1"}]}}
        pipeline = score_pipeline(self.oracle["response-contract"], run, investigation)
        self.assertTrue(pipeline["checks"]["investigation_terminal"])
        self.assertFalse(pipeline["checks"]["investigation_usable"])
        self.assertFalse(pipeline["checks"]["exact_incident_in_assessment"])
        self.assertEqual(pipeline["pipeline_score"], 65)

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
