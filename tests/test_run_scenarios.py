import unittest

from unittest.mock import patch
from types import SimpleNamespace

from tools import run_scenarios as runner
from tools.run_scenarios import (
    alert_identity,
    expected_alerts,
    fresh_expected_alerts,
    request_investigation,
    start_scenario,
    wait_for_expected_clear,
    wait_for_lab_quiet,
)


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
        with patch("tools.run_scenarios.request", side_effect=[{"status": "running"}, {"status": "ready"}]) as call:
            queued = request_investigation("http://fcapsule", "episode-1", requested)
            ready = request_investigation("http://fcapsule", "episode-1", requested)

        self.assertEqual((queued["status"], ready["status"]), ("running", "ready"))
        self.assertEqual(call.call_args_list[0].args, ("http://fcapsule/api/episodes/episode-1/investigation",))
        self.assertEqual(call.call_args_list[1].args, ("http://fcapsule/api/episodes/episode-1/investigation",))

    def test_automatic_assessment_is_not_repeated_and_one_followup_can_focus_a_new_signal(self):
        requested = set()
        current = {"status": "ready", "context": {"alerts": [{"incident_id": "incident-old"}]}}
        with patch("tools.run_scenarios.request", side_effect=[current, {"status": "queued"}]) as call:
            request_investigation("http://fcapsule", "episode-1", requested, "incident-new")

        self.assertEqual(
            call.call_args_list[1].args,
            ("http://fcapsule/api/episodes/episode-1/investigation", {"incident_id": "incident-new"}),
        )

    def test_start_confirms_an_active_run_after_a_connection_reset(self):
        request_id = "a" * 32
        active = {"scenario": "signing-key-skew", "run_id": request_id, "status": "running"}
        with patch("tools.run_scenarios.request", side_effect=[ConnectionResetError("reset"), {"active": active}]):
            result = start_scenario("http://lab", "signing-key-skew", 180, request_id)

        self.assertEqual(result["run"], active)
        self.assertIn("confirmed", result["message"])

    def test_ambiguous_start_is_never_retried_or_claimed_by_scenario_name(self):
        request_id = "a" * 32
        active = {"scenario": "signing-key-skew", "run_id": "b" * 32, "status": "running"}
        with patch("tools.run_scenarios.request", side_effect=[TimeoutError("lost"), {"active": active}]) as call:
            with self.assertRaisesRegex(RuntimeError, "different Lab run"):
                start_scenario("http://lab", "signing-key-skew", 180, request_id)
        self.assertEqual(call.call_count, 2)
        self.assertEqual(call.call_args_list[0].args[1]["request_id"], request_id)

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
        monotonic_values = iter([0, 0, 480])
        clock = SimpleNamespace(monotonic=lambda: next(monotonic_values), sleep=lambda _seconds: None)
        with patch.object(runner, "healthy", return_value=False), patch.object(runner, "time", clock):
            with self.assertRaisesRegex(RuntimeError, "Previous workload did not recover"):
                runner.settle(args)

    def test_waits_until_all_lab_alerts_are_quiet_between_cases(self):
        args = SimpleNamespace(prometheus="http://prometheus", lab_quiet_timeout=10)
        with patch("tools.run_scenarios.firing", side_effect=[[alert("LabWorkerCPUHigh")], []]), patch("tools.run_scenarios.time.sleep") as sleep:
            wait_for_lab_quiet(args)

        sleep.assert_called_once_with(5)


if __name__ == "__main__":
    unittest.main()
