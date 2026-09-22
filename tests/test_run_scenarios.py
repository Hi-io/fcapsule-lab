import unittest

from unittest.mock import patch
from types import SimpleNamespace

from tools.run_scenarios import alert_identity, expected_alerts, fresh_expected_alerts, request_investigation, wait_for_expected_clear


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

    def test_recurrence_starts_one_fresh_investigation_then_reads_the_same_run(self):
        requested = set()
        with patch("tools.run_scenarios.request", side_effect=[{"status": "queued"}, {"status": "ready"}]) as call:
            queued = request_investigation("http://fcapsule", "episode-1", requested)
            ready = request_investigation("http://fcapsule", "episode-1", requested)

        self.assertEqual((queued["status"], ready["status"]), ("queued", "ready"))
        self.assertEqual(call.call_args_list[0].args, ("http://fcapsule/api/episodes/episode-1/investigation", {}))
        self.assertEqual(call.call_args_list[1].args, ("http://fcapsule/api/episodes/episode-1/investigation",))

    def test_waits_for_only_the_expected_alert_to_clear(self):
        args = type("Args", (), {"prometheus": "http://prometheus", "alert_clear_timeout": 5})()
        with patch("tools.run_scenarios.firing", side_effect=[
            [alert("LabWorkerCrashLooping"), alert("LabWorkerCPUHigh")],
            [alert("LabWorkerCPUHigh")],
        ]), patch("tools.run_scenarios.time.sleep") as sleep:
            wait_for_expected_clear(args, "LabWorkerCrashLooping")

        sleep.assert_called_once_with(5)

    def test_recovery_window_allows_kubernetes_restart_backoff(self):
        args = SimpleNamespace(lab="http://lab")
        with patch("tools.run_scenarios.healthy", return_value=False), patch("tools.run_scenarios.time.monotonic", side_effect=[0, 0, 480]), patch("tools.run_scenarios.time.sleep"):
            with self.assertRaisesRegex(RuntimeError, "Previous workload did not recover"):
                from tools.run_scenarios import settle
                settle(args)


if __name__ == "__main__":
    unittest.main()
