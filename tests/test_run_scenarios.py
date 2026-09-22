import unittest

from tools.run_scenarios import alert_identity, expected_alerts, fresh_expected_alerts


def alert(name, *, active_at="2026-09-23T00:00:00Z", pod="orders-1"):
    return {
        "state": "firing",
        "activeAt": active_at,
        "labels": {"alertname": name, "namespace": "fcapsule-lab", "pod": pod},
    }


class ScenarioRunnerAlertTests(unittest.TestCase):
    def test_alert_identity_ignores_prometheus_replica_labels(self):
        first = alert("LabCheckoutLatencyHigh")
        duplicate = alert("LabCheckoutLatencyHigh")
        duplicate["labels"]["prometheus_replica"] = "agent-1"
        self.assertEqual(alert_identity(first), alert_identity(duplicate))

    def test_unrelated_baseline_alert_does_not_block_the_next_case(self):
        baseline = [alert("LabWorkerCPUHigh")]
        self.assertEqual(expected_alerts(baseline, "LabCheckoutLatencyHigh"), [])
        self.assertEqual(
            fresh_expected_alerts([*baseline, alert("LabCheckoutLatencyHigh")], "LabCheckoutLatencyHigh", []),
            [alert("LabCheckoutLatencyHigh")],
        )

    def test_prior_instance_of_the_expected_alert_is_not_counted_as_new_evidence(self):
        baseline = [alert("LabCheckoutLatencyHigh")]
        self.assertEqual(
            fresh_expected_alerts(baseline, "LabCheckoutLatencyHigh", baseline),
            [],
        )
        restarted = alert("LabCheckoutLatencyHigh", active_at="2026-09-23T00:05:00Z")
        self.assertEqual(
            fresh_expected_alerts([restarted], "LabCheckoutLatencyHigh", baseline),
            [restarted],
        )


if __name__ == "__main__":
    unittest.main()
