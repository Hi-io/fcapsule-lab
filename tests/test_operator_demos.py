import copy
from datetime import datetime, timezone
import hashlib
import json
from http.server import ThreadingHTTPServer
from pathlib import Path
import tempfile
import threading
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

from app.control import ControlState, handler, HTML
from app.demo_catalog import DEMO_CASES, public_demos
from app.scenario_catalog import DEFAULT_SCENARIO_CONFIG, SCENARIOS
from tools import run_operator_demos as runner


def config():
    return {"provider": "deepseek", "model": "deepseek-v4-pro", **runner.MAXIMUMS,
            "api_key_configured": True, "capability": {"status": "ready"}}


def sample():
    return {"lab": {"active": None, "memory_error": None, **{k: {"reachable": True} for k in ("orders", "worker", "inventory")}},
        "pods": [{"node": "worker-1", "containers": [{"ready": True}]}], "node_addresses": {"worker-1": ["10.0.0.2"]},
        "memory": {"data": {"result": [{"metric": {"instance": "10.0.0.2:9100"}, "value": [0, str(2 * 1024**3)]}]}},
        "memory_age": {"data": {"result": [{"metric": {"instance": "10.0.0.2:9100"}, "value": [0, "10"]}]}},
        "config": {"data": DEFAULT_SCENARIO_CONFIG}, "service": {"metadata": {"labels": {"fcapsule.io/app-metrics": "true"}}},
        "monitor": {"spec": {"endpoints": [{"path": "/metrics"}]}, "metadata": {"annotations": {}}}}


class DemoCatalogTests(unittest.TestCase):
    def test_http_owned_start_recovery_and_downloadable_plan(self):
        state = Mock()
        state.start.return_value = {"ok": True}
        state.recover.return_value = {"ok": True}
        server = ThreadingHTTPServer(("127.0.0.1", 0), handler(state))
        thread = threading.Thread(target=server.serve_forever, daemon=True); thread.start()
        base = f"http://127.0.0.1:{server.server_port}"
        try:
            plan = runner.request(base + "/api/demos/exporter-scrape/plan")
            self.assertIn("--lab-node NODE", plan["command"])
            self.assertTrue(plan["demo"]["runner_only"])
            state.start.assert_not_called()
            runner.request(base + "/api/scenarios/mysql-connections/start", {"duration_seconds": 120, "request_id": "a" * 32})
            state.start.assert_called_once_with("mysql-connections", 120, "a" * 32)
            runner.request(base + "/api/recover", {"expected_run_id": "a" * 32})
            state.recover.assert_called_once_with(expected_run_id="a" * 32)
            self.assertIn('aria-busy="true"', HTML)
            self.assertIn('aria-live="polite">Loading', HTML)
        finally:
            server.shutdown(); thread.join(); server.server_close()

    def test_five_distinct_mechanisms_no_extra_benchmark_faults(self):
        self.assertEqual(len(SCENARIOS), 15)
        self.assertEqual(len(DEMO_CASES), 5)
        self.assertEqual(len({c["scenario"] for c in DEMO_CASES.values()}), 5)
        self.assertEqual(sum("capture" in c for c in DEMO_CASES.values()), 2)
        self.assertEqual(DEMO_CASES["query-rollout-history"]["rounds"], 2)

    def test_public_catalog_does_not_expose_oracle(self):
        text = json.dumps(public_demos())
        for key in ("expected_alert", "root_cause", "ground_truth", "actions", "config", "capture"):
            self.assertNotIn('"' + key + '"', text)
        self.assertTrue(public_demos()["exporter-scrape"]["runner_only"])

    def test_all_non_config_actions_receive_defined_baseline_settings(self):
        for name, case in SCENARIOS.items():
            if "config" in case or case["actions"][0]["target"] == "database":
                continue
            state = ControlState()
            state.active = {"run_id": "test"}; state.logger = Mock(); state._post = Mock(return_value={})
            state._patch_scenario_config = Mock()
            state._start(name, 120)
            self.assertEqual(state._post.call_args.args[1]["settings"], DEFAULT_SCENARIO_CONFIG)
            state._patch_scenario_config.assert_not_called()

    def test_owned_recovery_never_resets_another_run(self):
        state = ControlState(); state._recover = Mock()
        state.active = {"run_id": "other"}
        with self.assertRaisesRegex(ValueError, "ownership"):
            state.recover(expected_run_id="ours")
        state._recover.assert_not_called()
        state.active = None
        self.assertTrue(state.recover(expected_run_id="ours")["ok"])
        state._recover.assert_not_called()

    def test_request_id_validation_happens_before_any_mutation(self):
        state = ControlState()
        for invalid in ("", "../x", 12, "G" * 32):
            with patch("app.control.memory_snapshot") as memory, self.assertRaises(ValueError):
                state.start("mysql-connections", request_id=invalid)
            memory.assert_not_called()

    def test_owned_start_returns_request_id(self):
        state = ControlState(); state._health = Mock(return_value={"reachable": True}); state._start = Mock(return_value={})
        with patch("app.control.memory_snapshot", return_value={"available_bytes": 2 * 1024**3}):
            result = state.start("mysql-connections", 120, "a" * 32)
        self.assertEqual(result["run"]["run_id"], "a" * 32)

    def test_config_recovery_failure_does_not_skip_service_recovery(self):
        state = ControlState(); state.logger = Mock(); state._post = Mock()
        state._patch_scenario_config = Mock(side_effect=OSError("config unavailable"))
        state._patch_metrics_service_label = Mock()
        with patch("app.control.pymysql.connect"):
            self.assertFalse(state.recover()["ok"])
        state._patch_metrics_service_label.assert_called_once_with("true")


class DemoRunnerTests(unittest.TestCase):
    def test_mutations_and_paid_calls_require_execute(self):
        with patch.object(runner, "request") as api, self.assertRaisesRegex(ValueError, "execute"):
            runner.run_suite(SimpleNamespace(execute=False))
        api.assert_not_called()

    def test_exact_pro_configuration_and_bounds(self):
        with patch.object(runner, "request", return_value=config()) as api:
            frozen = runner.configuration("http://product")
        self.assertEqual(frozen["max_checks"], 1)
        self.assertEqual(len(api.call_args.args), 1)
        for key, value in (("model", "deepseek-v4-flash"), ("max_checks", 2), ("max_total_tokens", 12001), ("max_prompt_tokens", 2101)):
            with patch.object(runner, "request", return_value={**config(), key: value}), self.assertRaises(ValueError):
                runner.configuration("http://product")
        with patch.object(runner, "request", return_value=config()), self.assertRaisesRegex(ValueError, "changed"):
            runner.configuration("http://product", {**frozen, "max_tokens": 2000})

    def test_node_placement_memory_and_owned_activity_fail_closed(self):
        runner.safety(sample(), "worker-1")
        for edit in ("node", "owner", "stale", "missing", "nan", "low"):
            data = sample()
            if edit == "node": data["pods"][0]["node"] = "go15"
            if edit == "owner": data["lab"]["active"] = {"run_id": "other"}
            if edit == "stale": data["memory_age"]["data"]["result"][0]["value"][1] = "61"
            if edit == "missing": data["memory_age"]["data"]["result"] = []
            if edit == "nan": data["memory"]["data"]["result"][0]["value"][1] = "NaN"
            if edit == "low": data["memory"]["data"]["result"][0]["value"][1] = "100"
            with self.subTest(edit=edit), self.assertRaises(RuntimeError): runner.safety(data, "worker-1")
        data = sample(); data["lab"]["active"] = {"run_id": "ours"}
        runner.safety(data, "worker-1", "ours")

    def test_no_baseline_overwrites_unknown_config(self):
        data = sample(); runner.baseline_config(data)
        data["config"] = {"data": {**DEFAULT_SCENARIO_CONFIG, "INVENTORY_TIMEOUT_SECONDS": "2.0"}}
        with self.assertRaises(RuntimeError): runner.baseline_config(data)

    def test_owned_fault_keeps_degraded_health_but_never_ignores_host_pressure(self):
        data = sample()
        data["lab"]["inventory"] = {"reachable": False, "error": "Connection refused"}
        data["pods"][0]["containers"][0]["ready"] = False
        with self.assertRaises(RuntimeError):
            runner.safety(data, "worker-1")
        data["lab"]["active"] = {"run_id": "ours"}
        runner.safety(data, "worker-1", "ours")
        self.assertFalse(data["lab"]["inventory"]["reachable"])
        self.assertFalse(data["pods"][0]["containers"][0]["ready"])
        data["memory"]["data"]["result"][0]["value"][1] = "100"
        with self.assertRaises(RuntimeError):
            runner.safety(data, "worker-1", "ours")

    def test_delayed_old_signal_is_not_fresh_and_missing_reports_are_not_ready(self):
        signal = {"incident_id": "incident-LabInventoryQueryFailures-x", "created_at": "2026-09-23T12:00:30Z",
                  "started_at": "2026-09-23T11:59:00Z", "app_id": "go15:fcapsule-lab:inventory-api", "report_ready": 1}
        state = {"overview": {"episodes": [{"episode_id": "ep", "signals": [signal]}]}}
        self.assertEqual(runner.matching_signals(state, "LabInventoryQueryFailures", "2026-09-23T12:00:00Z"), [])
        signal["started_at"] = "2026-09-23T12:00:01Z"
        self.assertEqual(len(runner.matching_signals(state, "LabInventoryQueryFailures", "2026-09-23T12:00:00Z")), 1)
        signal["report_ready"] = 0
        self.assertEqual(runner.matching_signals(state, "LabInventoryQueryFailures", "2026-09-23T12:00:00Z"), [])

    def test_ambiguous_injection_is_not_retried_and_owned_recovery_runs(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            args = SimpleNamespace(lab="http://lab", prometheus="http://prom", fcapsule="http://product", lab_node="worker-1", baseline=0)
            def api(url, payload=None):
                if payload is None: return {"capabilities": {"owned_runs": True}}
                raise OSError("POST response lost")
            with patch.object(runner, "snapshot", return_value=sample()), patch.object(runner, "configuration"), \
                 patch.object(runner, "firing", return_value=[]), patch.object(runner, "request", side_effect=api) as calls, \
                 patch.object(runner, "recover_owned") as recover:
                with self.assertRaises(OSError): runner.run_workload(args, "connection-pressure", root, config())
            self.assertEqual(sum(len(c.args) == 2 for c in calls.call_args_list), 1)
            recover.assert_called_once()
            self.assertEqual(runner.read(root / "run.json")["outcome"], "incomplete")
            self.assertTrue((root / "injection-attempt.json").exists())

    def test_recovery_guard_does_not_post_to_different_owner(self):
        with patch.object(runner, "request", return_value={"active": {"run_id": "other"}}) as api:
            with self.assertRaises(RuntimeError): runner.recover_owned(SimpleNamespace(lab="http://lab"), Path("unused"), "ours")
        self.assertEqual(api.call_count, 1)

    def test_image_origin_hash_query_and_capture_window(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            data = b"\x89PNG\r\n\x1a\n" + b"test-only-not-a-demo-image" * 100
            (root / "fault.png").write_bytes(data)
            record = {"prometheus": "http://prom", "case": "connection-pressure", "started_at": "2026-09-23T12:00:00Z", "fault_ended_at": "2026-09-23T12:03:00Z"}
            meta = {"source_url": "http://prom/query", "sha256": hashlib.sha256(data).hexdigest(), "observed_at": "2026-09-23T12:01:00Z",
                    "query": DEMO_CASES["connection-pressure"]["capture"]["query"]}
            runner.save(root / "fault.png.json", meta)
            self.assertEqual(runner.validated_image(root, record)[0], data)
            for key, value in (("source_url", "http://product/console"), ("sha256", "edited"), ("query", "vector(1)"), ("observed_at", "2026-09-23T13:00:00Z")):
                runner.save(root / "fault.png.json", {**meta, key: value})
                with self.subTest(key=key), self.assertRaises(ValueError): runner.validated_image(root, record)

    def test_pixel_review_required_before_any_api(self):
        with patch.object(runner, "request") as api, self.assertRaises(ValueError):
            runner.attach(SimpleNamespace(pixels_reviewed=False))
        api.assert_not_called()

    def test_existing_attempt_blocks_automatic_reupload(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory); (root / "media-review").mkdir()
            with patch.object(runner, "paid_context", return_value=(root, {}, "base")), patch.object(runner, "request") as api:
                with self.assertRaisesRegex(ValueError, "already attempted"): runner.attach(SimpleNamespace(pixels_reviewed=True))
            api.assert_not_called()

    def test_no_new_revision_does_not_poll_or_repeat_paid_call(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            with patch.object(runner, "request", return_value={"revision_id": "same"}) as api:
                with self.assertRaisesRegex(RuntimeError, "No new revision"):
                    runner.finish_update(root, root, {}, "base", {"revision_id": "same"}, "attachment")
            api.assert_called_once_with("base/investigation/update", {})

    def test_raw_failed_assessments_are_retained_without_overwrite(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for status in ("incomplete", "ready", "ready"):
                runner.retain_assessment(root, {"revision_id": "r", "status": status})
            self.assertEqual(len(list((root / "raw-assessments").iterdir())), 2)

    def test_run_waits_for_prior_alert_resolution_before_preflight(self):
        with tempfile.TemporaryDirectory() as directory:
            args = SimpleNamespace(execute=True, out=Path(directory) / "run", case="checkout-deadline")
            calls = []
            def preflight(*_):
                calls.append("preflight")
                raise ValueError("Stop before injection")
            with patch.object(runner, "wait_for_lab_quiet", side_effect=lambda _: calls.append("quiet")), \
                 patch.object(runner, "preflight", side_effect=preflight), \
                 self.assertRaisesRegex(ValueError, "Stop before injection"):
                runner.run_suite(args)
            self.assertEqual(calls, ["quiet", "preflight"])
            self.assertEqual(runner.read(args.out / "suite.json")["outcome"], "incomplete")

    def test_history_wait_uses_latest_membership_start_not_old_episode_start(self):
        now = datetime(2026, 9, 23, 12, tzinfo=timezone.utc).timestamp()
        state = {"overview": {"episodes": [{"app_id": "app", "started_at": "2026-09-20T00:00:00Z", "signals": [
            {"started_at": "2026-09-23T11:59:00Z", "status": "resolved"}]}]}}
        self.assertEqual(runner.history_wait_seconds(state, "app", 960, now), 900)
        self.assertEqual(runner.history_wait_seconds(state, "app", 960, now + 1000), 0)
        state["overview"]["episodes"][0]["signals"][0]["status"] = "firing"
        self.assertEqual(runner.history_wait_seconds(state, "app", 960, now + 1000), 960)

    def test_history_rejects_two_incidents_in_same_episode(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for n in (1, 2):
                folder = root / f"round-{n}"; folder.mkdir()
                runner.save(folder / "run.json", {"case": "query-rollout-history", "outcome": "captured",
                    "incident_id": str(n), "episode_id": "same", "capsule_id": "c"})
            with patch.object(runner, "request") as api, self.assertRaisesRegex(ValueError, "episode"):
                runner.history(SimpleNamespace(execute=True, case_dir=root))
            api.assert_not_called()

    def test_history_reference_keeps_original_path_and_record(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory); (root / "round-1").mkdir()
            original = root / "original"; original.mkdir()
            runner.save(root / "round-1/reference.json", {"path": str(original)})
            self.assertEqual(runner.history_round(root, 1), original)
            self.assertEqual(runner.history_round(root, 2), root / "round-2")


class AssessmentProvenanceTests(unittest.TestCase):
    def record(self):
        return {"fcapsule": "http://product", "episode_id": "reused-episode",
                "incident_id": "new-incident", "model_config": config()}

    def assessment(self, status="ready", incident_id="new-incident"):
        return {"status": status, "model": config()["model"], "primary_incident_id": "old-incident",
                "context": {"alerts": [{"incident_id": incident_id}]}}

    def test_membership_requires_exact_structured_alert_not_primary_or_prose(self):
        contexts = (None, [], {}, {"alerts": None}, {"alerts": {}}, {"alerts": "new-incident"},
                    {"alerts": [None, "new-incident", {}, {"incident_id": "new-incident-suffix"}]})
        for context in contexts:
            with self.subTest(context=context):
                value = {"context": context, "primary_incident_id": "new-incident",
                         "assessment": "new-incident", "alerts": [{"incident_id": "new-incident"}]}
                self.assertFalse(runner.assessment_contains_incident(value, "new-incident"))
        self.assertFalse(runner.assessment_contains_incident({}, "new-incident"))
        self.assertTrue(runner.assessment_contains_incident(self.assessment(), "new-incident"))

    def test_run_waits_past_stale_terminal_and_current_running_revision(self):
        stale = {**self.assessment(incident_id="old-incident"), "revision_id": "r1"}
        running = {**self.assessment("running"), "revision_id": "r2"}
        current = {**self.assessment(), "revision_id": "r2"}
        current["context"]["alerts"].insert(0, {"incident_id": "old-incident"})
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            record = self.record()
            with patch.object(runner, "request", side_effect=[stale, running, current, {}]) as api, \
                 patch.object(runner.media.time, "monotonic", side_effect=[0, 0, 1, 2]), \
                 patch.object(runner.media.time, "sleep"):
                self.assertEqual(runner.await_assessment(record, root, seconds=3), current)
            self.assertEqual(runner.read(root / "investigation-before.json"), current)
            self.assertEqual(len(list((root / "raw-assessments").iterdir())), 3)
            saved = runner.read(root / "run.json")
            self.assertTrue(saved["assessment_context_contains_incident"])
            self.assertFalse(saved["assessment_matches_incident"])
            self.assertEqual(saved["assessment_context_policy"], "current_incident_required")
            self.assertEqual(api.call_count, 4)
            self.assertTrue(all(len(call.args) == 1 and not call.kwargs for call in api.call_args_list))

    def test_stale_only_times_out_without_selecting_baseline_or_triggering_request(self):
        stale = self.assessment(incident_id="old-incident")
        # Even primary metadata naming the new incident cannot replace actual context.
        stale["primary_incident_id"] = "new-incident"
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            with patch.object(runner, "request", return_value=stale) as api, \
                 patch.object(runner.media.time, "monotonic", side_effect=[0, 0, 1]), \
                 patch.object(runner.media.time, "sleep"), \
                 self.assertRaisesRegex(TimeoutError, "context.alerts; no retry started"):
                runner.await_assessment(self.record(), root, seconds=1)
            api.assert_called_once_with("http://product/api/episodes/reused-episode/investigation")
            self.assertEqual([runner.read(p) for p in (root / "raw-assessments").iterdir()], [stale])
            self.assertFalse((root / "investigation-before.json").exists())
            self.assertFalse((root / "report.json").exists())

    def test_matching_terminal_failures_are_preserved_not_forced_to_ready(self):
        for status in runner.TERMINAL:
            with self.subTest(status=status), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                value = self.assessment(status)
                record = self.record()
                with patch.object(runner, "request", side_effect=[value, {}]) as api:
                    self.assertEqual(runner.await_assessment(record, root), value)
                self.assertEqual(runner.read(root / "run.json")["assessment_status"], status)
                self.assertEqual(api.call_count, 2)

    def test_retain_prior_explicitly_permits_old_context_but_is_read_only_and_disclosed(self):
        incident = "incident-labinventoryqueryfailures-prior"
        state = {"overview": {"episodes": [{"episode_id": "reused-episode", "signals": [{
            "incident_id": incident, "status": "resolved", "app_id": "node:fcapsule-lab:inventory-api",
            "started_at": "2026-09-22T12:00:00Z", "ended_at": "2026-09-22T12:03:00Z"}]}]}}
        running = self.assessment("running", "another-member")
        terminal = self.assessment(incident_id="another-member")
        report = {"record": {"capsule_id": "prior-capsule"}}
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "import"
            args = SimpleNamespace(out=root, incident_id=incident, fcapsule="http://product",
                                   lab="http://lab", prometheus="http://prom")
            with patch.object(runner, "preflight", return_value={"model_config": config()}) as preflight, \
                 patch.object(runner, "request", side_effect=[state, running, terminal, report, {"capsule": {}}, {}]) as api, \
                 patch.object(runner.media.time, "monotonic", side_effect=[0, 0, 1]), \
                 patch.object(runner.media.time, "sleep"):
                runner.retain_prior(args)
            preflight.assert_called_once_with(args, root, require_owned=False)
            self.assertTrue(all(len(call.args) == 1 and not call.kwargs for call in api.call_args_list))
            self.assertEqual(api.call_count, 6)
            self.assertEqual(runner.read(root / "investigation-before.json"), terminal)
            saved = runner.read(root / "run.json")
            self.assertEqual(saved["incident_id"], incident)
            self.assertEqual(saved["capsule_id"], "prior-capsule")
            self.assertEqual(saved["assessment_context_policy"], "retained_prior_read_only")
            self.assertFalse(saved["assessment_context_contains_incident"])
            self.assertFalse(saved["comparison_valid"])

    def test_retained_prior_exception_still_rejects_changed_model(self):
        value = {**self.assessment(incident_id="another-member"), "model": "different-model"}
        with tempfile.TemporaryDirectory() as directory:
            with patch.object(runner, "request", side_effect=[value, {}]), \
                 self.assertRaisesRegex(RuntimeError, "different model"):
                runner.await_assessment(self.record(), Path(directory), allow_retained_prior=True)


if __name__ == "__main__":
    unittest.main()
