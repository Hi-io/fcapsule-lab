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
import yaml

from app.control import CONTROL_OWNER, ControlState, handler, HTML
from app.demo_catalog import DEMO_CASES, public_demos
from app.scenario_catalog import DEFAULT_SCENARIO_CONFIG, DISCOVERY_SCENARIOS, SCENARIOS, public_scenarios
from tools import run_operator_demos as runner


def config():
    return {"provider": "deepseek", "model": "deepseek-v4-pro", **runner.MAXIMUMS,
            "api_key_configured": True, "capability": {"status": "ready"}}


def media_clock(monotonic_values):
    return SimpleNamespace(monotonic=Mock(side_effect=monotonic_values), sleep=Mock())


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

    def test_unified_catalog_contains_all_unique_mechanisms_and_legacy_workflows(self):
        self.assertEqual(len(SCENARIOS), 15)
        self.assertEqual(len(DEMO_CASES), 5)
        self.assertEqual(len(set(SCENARIOS) | set(DISCOVERY_SCENARIOS)), 17)
        self.assertTrue(set(c["scenario"] for c in DEMO_CASES.values()).issubset(set(SCENARIOS) | set(DISCOVERY_SCENARIOS)))
        self.assertEqual(len({c["scenario"] for c in DEMO_CASES.values()}), 5)
        self.assertEqual(sum("capture" in c for c in DEMO_CASES.values()), 2)
        self.assertEqual(DEMO_CASES["query-rollout-history"]["rounds"], 2)
        self.assertEqual(len(public_scenarios()), 17)
        catalog = public_scenarios()
        self.assertEqual(catalog["mysql-connections"]["source_view"], "prometheus_graph")
        self.assertEqual(catalog["metrics-service-label-drift"]["source_view"], "prometheus_targets")
        self.assertEqual(catalog["mysql-exporter-scrape-path"]["execution"], "guarded_runner")
        self.assertEqual(catalog["memory-leak"]["resource_profile"], "bounded_memory")
        self.assertEqual(runner.scenario_rounds("mysql-connections"), 1)
        self.assertEqual(runner.capture_spec("mysql-connections")["view"], "graph")
        self.assertEqual(runner.ALL_RUN_CASES[-1], "query-rollout-history")
        self.assertEqual(len(runner.ALL_RUN_CASES), 18)

    def test_all_case_selection_includes_history_without_relabeling_it_as_a_new_fault(self):
        self.assertEqual(runner.ALL_RUN_CASES, [*SCENARIOS, *DISCOVERY_SCENARIOS, "query-rollout-history"])
        self.assertEqual(DEMO_CASES["query-rollout-history"]["scenario"], "schema-drift")
        self.assertEqual(runner.scenario_rounds("query-rollout-history"), 2)

    def test_load_shedding_keeps_lock_test_below_mysql_connection_ceiling(self):
        documents = list(yaml.safe_load_all((runner.ROOT / "deploy/kubernetes/configuration.yaml").read_text()))
        runtime = next(item["data"] for item in documents if item and item.get("kind") == "ConfigMap"
                       and item["metadata"]["name"] == "lab-runtime")
        mysql = next(item["data"] for item in documents if item and item.get("kind") == "ConfigMap"
                     and item["metadata"]["name"] == "lab-mysql-config")
        self.assertEqual(int(runtime["MAX_INFLIGHT"]), 12)
        self.assertLessEqual(int(runtime["MAX_INFLIGHT"]), int(mysql["MYSQL_MAX_CONNECTIONS"]) // 3)
        self.assertGreaterEqual(int(runtime["REQUESTS_PER_SECOND"]), int(runtime["MAX_INFLIGHT"]))

    def test_mysql_init_bootstraps_inventory_schema_after_ephemeral_volume_reset(self):
        documents = list(yaml.safe_load_all((runner.ROOT / "deploy/kubernetes/configuration.yaml").read_text()))
        init_sql = next(item["data"] for item in documents if item and item.get("kind") == "ConfigMap"
                        and item["metadata"]["name"] == "lab-mysql-init")
        bootstrap = init_sql["00-inventory.sql"].lower()
        for table in ("inventory_items", "reservation_events", "inventory_reconciliation_audit"):
            self.assertIn(f"create table if not exists {table}", bootstrap)
        self.assertIn("insert into inventory_items", bootstrap)
        self.assertIn("sku-red-widget", bootstrap)
        self.assertIn("sku-blue-widget", bootstrap)
        self.assertLess(list(init_sql).index("00-inventory.sql"), list(init_sql).index("01-exporter.sql"))

    def test_history_use_requires_retrieving_and_citing_the_exact_prior_episode(self):
        previous = {"episode_id": "episode-prior"}
        current = {"episode_id": "episode-current"}
        valid = {"assessment": {"historical_comparison": {
            "episode_id": "episode-prior", "status": "similar_mechanism", "evidence_ids": ["Q003"]}},
            "checks": [{"id": "Q003", "tool": "historical_episode", "status": "completed",
                        "arguments": {"episode_id": "episode-prior"}}]}
        self.assertEqual(runner.score_history_reuse(previous, current, valid)["score"], 100)
        valid["assessment"]["historical_comparison"]["episode_id"] = "invented"
        self.assertEqual(runner.score_history_reuse(previous, current, valid)["score"], 60)
        valid["assessment"]["historical_comparison"]["episode_id"] = "episode-prior"
        valid["assessment"]["historical_comparison"]["evidence_ids"] = []
        self.assertEqual(runner.score_history_reuse(previous, current, valid)["score"], 80)

    def test_public_catalog_does_not_expose_oracle(self):
        text = json.dumps(public_demos())
        for key in ("expected_alert", "root_cause", "ground_truth", "actions", "config", "capture"):
            self.assertNotIn('"' + key + '"', text)
        self.assertTrue(public_demos()["exporter-scrape"]["runner_only"])

    def test_baseline_preflight_refuses_any_persisted_run_owner(self):
        for key in ("fcapsule.io/lab-control-run", "fcapsule.io/lab-control-field-owner",
                    "fcapsule.lab/screenshot-run"):
            data = sample()
            data["config"]["metadata"] = {"annotations": {key: "owner"}}
            with self.subTest(key=key), self.assertRaises(RuntimeError):
                runner.baseline_config(data)

    def test_baseline_preflight_refuses_any_persisted_run_owner(self):
        for key in ("fcapsule.io/lab-control-run", "fcapsule.io/lab-control-field-owner",
                    "fcapsule.lab/screenshot-run"):
            data = sample()
            data["config"]["metadata"] = {"annotations": {key: "owner"}}
            with self.subTest(key=key), self.assertRaises(RuntimeError):
                runner.baseline_config(data)

    def test_all_non_config_actions_receive_defined_baseline_settings(self):
        for name, case in SCENARIOS.items():
            if "config" in case or case["actions"][0]["target"] == "database":
                continue
            state = ControlState()
            state.active = {"run_id": "a" * 32}; state.logger = Mock()
            state._post = Mock(return_value={"run_id": "a" * 32})
            state._patch_scenario_config = Mock()
            state._start(name, 120)
            self.assertEqual(state._post.call_args.args[1]["settings"], DEFAULT_SCENARIO_CONFIG)
            self.assertEqual(state._post.call_args.args[1]["run_id"], "a" * 32)
            state._patch_scenario_config.assert_not_called()

    def test_application_control_ack_must_match_owned_run(self):
        state = ControlState()
        state._post = Mock(return_value={"run_id": "b" * 32})
        with self.assertRaisesRegex(ValueError, "did not acknowledge"):
            state._post_for_run("http://app/control", {"mode": "normal"}, "a" * 32)

    def test_recovery_sends_and_verifies_same_run_id(self):
        state = ControlState(); state.logger = Mock()
        state.active = {"run_id": "a" * 32, "targets": ["worker"],
                        "baseline_settings": DEFAULT_SCENARIO_CONFIG}
        state._restore_owned_fields = Mock()
        state._post = Mock(return_value={"run_id": "a" * 32})
        state._clear_run_claim = Mock()
        result = state.recover()
        self.assertTrue(result["ok"])
        self.assertEqual(state._post.call_args.args[1], {"mode": "normal", "run_id": "a" * 32})

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
        with patch("app.control.memory_snapshot", return_value={
            "available_bytes": 2 * 1024**3, "node_identity_verified": True,
        }):
            result = state.start("mysql-connections", 120, "a" * 32)
        self.assertEqual(result["run"]["run_id"], "a" * 32)

    def test_watchdog_recovers_persisted_run_after_control_pod_restart(self):
        state = ControlState()
        state._persisted_run = Mock(return_value={
            "run_id": "a" * 32, "targets": ["inventory"],
            "baseline_settings": DEFAULT_SCENARIO_CONFIG, "expires_at": 2_000_000_000,
            "status": "recovering", "recovered_after_restart": True,
        })
        state.recover = Mock()
        with patch("app.control.memory_snapshot", return_value={
            "available_bytes": 2 * 1024**3, "node_identity_verified": True,
        }), patch("app.control.time.sleep", side_effect=StopIteration):
            with self.assertRaises(StopIteration):
                state.watchdog()
        state.recover.assert_called_once_with("controller_restart")
        self.assertEqual(state.active["run_id"], "a" * 32)
        self.assertEqual(state.active["targets"], ["inventory"])
        self.assertTrue(state.active["claim_acquired"])

    def test_recovery_does_not_reset_workloads_after_config_conflict(self):
        state = ControlState(); state.logger = Mock(); state._post = Mock()
        state.active = {"run_id": "a" * 32, "targets": ["inventory", "orders"],
                        "baseline_settings": DEFAULT_SCENARIO_CONFIG}
        state._restore_owned_fields = Mock(side_effect=[ValueError("concurrent edit"), None])
        state._clear_run_claim = Mock()
        connection = Mock()
        with patch("app.control.pymysql.connect", return_value=connection):
            result = state.recover()
        self.assertFalse(result["ok"])
        state._post.assert_not_called()
        state._clear_run_claim.assert_not_called()

    def test_unowned_recovery_is_a_noop(self):
        state = ControlState(); state.logger = Mock(); state._post = Mock()
        state._kubernetes_get = Mock(return_value=None)
        with patch("app.control.pymysql.connect") as connect:
            result = state.recover("startup")
        self.assertTrue(result["ok"])
        self.assertIn("no recovery writes", result["message"].lower())
        state._post.assert_not_called()
        connect.assert_not_called()

    def test_config_field_restore_is_scoped_to_owned_value_and_detects_edits(self):
        state = ControlState(); state.active = {"run_id": "a" * 32}
        original = {"metadata": {"resourceVersion": "7", "annotations": {CONTROL_OWNER: "a" * 32}},
                    "data": {"SETTING": "operator-baseline", "UNRELATED": "keep"}}
        state._kubernetes_get = Mock(return_value=original)
        state._kubernetes_patch = Mock()
        state._patch_scenario_config({"SETTING": "fault-value"})
        payload = state._kubernetes_patch.call_args.args[2]
        self.assertEqual(payload["data"], {"SETTING": "fault-value"})
        self.assertEqual(payload["metadata"]["resourceVersion"], "7")
        self.assertIn("UNRELATED", original["data"])

        changed = {"metadata": {"resourceVersion": "8", "annotations": {
            CONTROL_OWNER: "a" * 32, "fcapsule.io/lab-control-field-owner": "a" * 32,
            "fcapsule.io/lab-control-field-baseline": '{"SETTING":"operator-baseline"}',
            "fcapsule.io/lab-control-field-applied": '{"SETTING":"fault-value"}'}},
            "data": {"SETTING": "fault-value", "UNRELATED": "keep"}}
        state._kubernetes_get = Mock(return_value=changed)
        state._kubernetes_patch.reset_mock()
        state._restore_owned_fields("configmaps", "lab-scenario-config", "data", "a" * 32)
        restore = state._kubernetes_patch.call_args.args[2]
        self.assertEqual(restore["data"], {"SETTING": "operator-baseline"})
        self.assertIsNone(restore["metadata"]["annotations"]["fcapsule.io/lab-control-field-owner"])

        changed["data"]["SETTING"] = "new-operator-value"
        state._kubernetes_patch.reset_mock()
        with self.assertRaisesRegex(ValueError, "changed during the run"):
            state._restore_owned_fields("configmaps", "lab-scenario-config", "data", "a" * 32)
        state._kubernetes_patch.assert_not_called()

    def test_persisted_run_recovers_only_valid_owned_journal(self):
        state = ControlState()
        state._kubernetes_get = Mock(return_value={"metadata": {"annotations": {
            CONTROL_OWNER: "a" * 32, "fcapsule.io/lab-control-targets": '["database"]',
            "fcapsule.io/lab-control-baseline-settings": '{"MAX_RETRIES":"2"}',
            "fcapsule.io/lab-control-expires-at": "2000000000",
            "fcapsule.io/lab-control-job-id": "7"}}})
        self.assertEqual(state._persisted_run()["job_id"], 7)
        self.assertEqual(state._persisted_run()["targets"], ["database"])
        state._kubernetes_get.return_value["metadata"]["annotations"]["fcapsule.io/lab-control-targets"] = "not-json"
        with self.assertRaisesRegex(ValueError, "journal is incomplete"):
            state._persisted_run()

    def test_service_selector_recovery_targets_only_the_owned_nested_label(self):
        state = ControlState(); state.active = {"run_id": "a" * 32}
        current = {"metadata": {"resourceVersion": "12", "labels": {
            "fcapsule.io/app-metrics": "true", "unrelated": "keep"}, "annotations": {}}}
        state._kubernetes_get = Mock(return_value=current)
        state._kubernetes_patch = Mock()
        state._patch_metrics_service_label("ture")
        patch = state._kubernetes_patch.call_args.args[2]
        self.assertEqual(patch["metadata"]["labels"], {"fcapsule.io/app-metrics": "ture"})
        self.assertNotIn("unrelated", patch["metadata"]["labels"])

        current["metadata"]["annotations"] = {
            "fcapsule.io/lab-control-field-owner": "a" * 32,
            "fcapsule.io/lab-control-field-baseline": '{"fcapsule.io/app-metrics":"true"}',
            "fcapsule.io/lab-control-field-applied": '{"fcapsule.io/app-metrics":"ture"}',
        }
        current["metadata"]["labels"]["fcapsule.io/app-metrics"] = "ture"
        state._kubernetes_patch.reset_mock()
        state._restore_owned_fields("services", "lab-app-metrics", "labels", "a" * 32)
        restore = state._kubernetes_patch.call_args.args[2]
        self.assertEqual(restore["metadata"]["labels"], {"fcapsule.io/app-metrics": "true"})
        self.assertIsNone(restore["metadata"]["annotations"]["fcapsule.io/lab-control-field-owner"])


class DemoRunnerTests(unittest.TestCase):
    def test_mutations_and_paid_calls_require_execute(self):
        with patch.object(runner, "request") as api, self.assertRaisesRegex(ValueError, "execute"):
            runner.run_suite(SimpleNamespace(execute=False))
        api.assert_not_called()

    def test_exact_pro_configuration_and_bounds(self):
        with patch.object(runner, "request", return_value=config()) as api:
            frozen = runner.configuration("http://product")
        self.assertEqual(frozen["max_checks"], 1)
        self.assertEqual(frozen["max_prompt_tokens"], 3200)
        self.assertEqual(frozen["max_total_tokens"], 12000)
        self.assertEqual(frozen["max_tokens"], 3600)
        self.assertEqual(len(api.call_args.args), 1)
        for key, value in (("model", "deepseek-v4-flash"), ("max_checks", 2), ("max_total_tokens", 12001),
                           ("max_tokens", 3601), ("max_prompt_tokens", 3201)):
            with patch.object(runner, "request", return_value={**config(), key: value}), self.assertRaises(ValueError):
                runner.configuration("http://product")
        with patch.object(runner, "request", return_value=config()), self.assertRaisesRegex(ValueError, "changed"):
            runner.configuration("http://product", {**frozen, "max_tokens": 2000})

    def test_prompt_ceiling_accepts_separate_budgets_but_never_mixes_frozen_configs(self):
        for budget, other in ((2100, 3200), (3200, 2100)):
            with self.subTest(budget=budget), patch.object(runner, "request", return_value={**config(), "max_prompt_tokens": budget}):
                frozen = runner.configuration("http://product")
                self.assertEqual(frozen["max_prompt_tokens"], budget)
                self.assertEqual(runner.configuration("http://product", frozen), frozen)
                with self.assertRaisesRegex(ValueError, "changed; refuse a mixed comparison"):
                    runner.configuration("http://product", {**frozen, "max_prompt_tokens": other})

    def test_old_run_budget_change_blocks_paid_followup_without_rewriting_record(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            frozen = {key: config()[key] for key in runner.CONFIG_KEYS}
            frozen["max_prompt_tokens"] = 2100
            runner.save(root / "run.json", {"outcome": "captured", "fcapsule": "http://product", "model_config": frozen})
            runner.save(root / "recovery.json", {"restored": True})
            original = (root / "run.json").read_bytes()
            with patch.object(runner, "request", return_value=config()) as api, \
                 self.assertRaisesRegex(ValueError, "changed; refuse a mixed comparison"):
                runner.paid_context(SimpleNamespace(execute=True, case_dir=root))
            api.assert_called_once_with("http://product/api/settings/ai")
            self.assertEqual((root / "run.json").read_bytes(), original)

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

    def test_recovery_records_restoration_without_waiting_for_transient_pod_readiness(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            degraded = sample()
            degraded["lab"]["inventory"] = {"reachable": False, "error": "readiness probe is restarting"}
            degraded["pods"][0]["containers"][0]["ready"] = False
            statuses = iter([{"active": {"run_id": "ours"}}, {"active": None, "recovery_error": None}])

            def api(url, payload=None):
                if url == "http://lab/api/status":
                    return next(statuses)
                if url == "http://lab/api/recover":
                    self.assertEqual(payload, {"expected_run_id": "ours"})
                    return {"ok": True}
                self.fail("Unexpected recovery endpoint: " + url)

            def immediate_wait(read, accept, _seconds, description):
                value = read()
                if not accept(value):
                    raise TimeoutError(description)
                return value

            args = SimpleNamespace(lab="http://lab", lab_node="worker-1", prometheus="http://prom")
            with patch.object(runner, "request", side_effect=api), \
                 patch.object(runner.media, "wait_for", side_effect=immediate_wait), \
                 patch.object(runner, "snapshot", return_value=degraded):
                runner.recover_owned(args, root, "ours")

            recovery = runner.read(root / "recovery.json")
            self.assertTrue(recovery["restored"])
            self.assertTrue(recovery["controller_lease_released"])
            self.assertTrue(recovery["baseline_config_restored"])
            self.assertEqual(recovery["runtime_readiness"], {"services_reachable": False, "pods_ready": False})
            self.assertTrue((root / "recovery-request.json").exists())

    def test_next_fault_waits_for_ready_pods_and_stops_on_another_owner(self):
        args = SimpleNamespace(lab="http://lab", prometheus="http://prom", lab_node="worker-1")
        with tempfile.TemporaryDirectory() as directory, patch.object(runner, "snapshot", return_value=sample()) as snapshots, \
             patch.object(runner, "firing", return_value=[]):
            degraded = sample()
            degraded["pods"][0]["containers"][0]["ready"] = False
            snapshots.side_effect = [degraded, sample()]
            with patch.object(runner.time, "sleep"):
                runner.wait_for_lab_ready(args, Path(directory), "memory-leak", 1, timeout=2)
            self.assertEqual(snapshots.call_count, 2)

        active = sample()
        active["lab"]["active"] = {"run_id": "someone-else"}
        with patch.object(runner, "snapshot", return_value=active), tempfile.TemporaryDirectory() as directory:
            with self.assertRaisesRegex(RuntimeError, "Another Lab run"):
                runner.wait_for_lab_ready(args, Path(directory), "memory-leak", 1, timeout=2)

    def test_cpu_saturation_retains_pod_cpu_quota_throttling_and_batch_progress(self):
        expressions = runner.promql("cpu-saturation")
        self.assertIn("worker_cpu_cores", expressions)
        self.assertIn("worker_cpu_limit", expressions)
        self.assertIn("worker_cpu_throttled_ratio", expressions)
        self.assertIn("migration_backlog", expressions)
        self.assertIn("migration_progress", expressions)
        for key in ("worker_cpu_cores", "worker_cpu_limit", "worker_cpu_throttled_ratio"):
            self.assertIn("namespace=\"fcapsule-lab\"", expressions[key])
            self.assertIn("pod", expressions[key])

    def test_delayed_old_signal_is_not_fresh_and_missing_reports_are_not_ready(self):
        signal = {"incident_id": "incident-LabInventoryQueryFailures-x", "created_at": "2026-09-23T12:00:30Z",
                  "started_at": "2026-09-23T11:59:00Z", "app_id": "go15:fcapsule-lab:inventory-api", "report_ready": 1}
        state = {"overview": {"episodes": [{"episode_id": "ep", "signals": [signal]}]}}
        self.assertEqual(runner.matching_signals(state, "LabInventoryQueryFailures", "2026-09-23T12:00:00Z"), [])
        signal["started_at"] = "2026-09-23T12:00:01Z"
        self.assertEqual(len(runner.matching_signals(state, "LabInventoryQueryFailures", "2026-09-23T12:00:00Z")), 1)
        signal["report_ready"] = 0
        self.assertEqual(runner.matching_signals(state, "LabInventoryQueryFailures", "2026-09-23T12:00:00Z"), [])

    def test_repeated_fresh_signals_in_one_episode_select_the_newest(self):
        episode = {"episode_id": "episode-1"}
        matches = [
            (episode, {"incident_id": "incident-old", "created_at": "2026-09-23T12:00:10Z"}),
            (episode, {"incident_id": "incident-new", "created_at": "2026-09-23T12:00:40Z"}),
        ]

        self.assertEqual(runner.select_episode_signal(matches), matches[1])

    def test_matching_signals_across_episodes_stay_ambiguous(self):
        matches = [
            ({"episode_id": "episode-1"}, {"incident_id": "incident-1", "created_at": "2026-09-23T12:00:10Z"}),
            ({"episode_id": "episode-2"}, {"incident_id": "incident-2", "created_at": "2026-09-23T12:00:40Z"}),
        ]

        with self.assertRaisesRegex(RuntimeError, "Multiple fresh episodes"):
            runner.select_episode_signal(matches)

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

    def test_missing_optional_browser_capture_is_recorded_not_raised(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            args = SimpleNamespace(node="missing-node", prometheus="http://prometheus")
            with patch.object(runner.subprocess, "run", side_effect=FileNotFoundError("browser unavailable")):
                result = runner.capture(args, root, "mysql-connections")
            self.assertEqual(result["status"], "unavailable")
            self.assertEqual(runner.read(root / "media-capture.json"), result)
            with patch.object(runner.media.subprocess, "run", side_effect=FileNotFoundError("browser unavailable")):
                external = runner.media.capture(args, root, "fault")
            self.assertEqual(external["status"], "unavailable")
            self.assertEqual(runner.read(root / "capture-fault.json"), external)

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


class HistoryReviewPollingTests(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.root = Path(directory.name)
        self.capsule = {"capsule": {"retained": "observations"}}
        self.queued = {"review_id": "accepted-review", "episode_id": "earlier-episode", "status": "queued"}
        self.ready = {**self.queued, "status": "ready", "completed_at": "2026-09-23T13:50:28Z",
                      "result": {"sufficiency": "partially_sufficient", "answer": "Retained observations only"}}
        self.reviews = iter([self.ready])
        for ordinal, identity in ((1, "earlier"), (2, "recurrence")):
            folder = self.root / f"round-{ordinal}"
            folder.mkdir()
            runner.save(folder / "run.json", {"case": "query-rollout-history", "outcome": "captured",
                "incident_id": identity + "-incident", "episode_id": identity + "-episode",
                "fcapsule": "http://product", "model_config": config(), "capsule_id": identity + "-capsule",
                "capsule_sha256": runner.digest(self.capsule["capsule"])})
            runner.save(folder / "recovery.json", {"restored": True})
        configuration = patch.object(runner, "configuration", return_value=config())
        self.configuration = configuration.start()
        self.addCleanup(configuration.stop)
        requests = patch.object(runner, "request", side_effect=self.api)
        self.requests = requests.start()
        self.addCleanup(requests.stop)

    def api(self, url, payload=None):
        if url == "http://product/api/episodes/earlier-episode/source-review":
            self.assertEqual(payload, {"question": runner.QUESTIONS})
            return self.queued
        self.assertIsNone(payload)
        if url == "http://product/api/capsules/earlier-capsule": return self.capsule
        if url == "http://product/api/incidents/earlier-incident/report":
            return {"investigation": {"status": "ready"}, "source_disconnected_reviews": [
                {**self.ready, "review_id": "unrelated-review"}, next(self.reviews)]}
        if url == "http://product/api/episodes/recurrence-episode/investigation":
            return {"status": "ready", "checks": []}
        if url == "http://product/artifacts/earlier-capsule/investigation_revisions.json": return {}
        self.fail("Unexpected endpoint, including any episode investigation used for review polling: " + url)

    def accepted_attempt(self):
        output = self.root / "history-review"
        output.mkdir()
        runner.save(output / "attempt.json", {"earlier_incident_id": "earlier-incident",
            "recurrence_incident_id": "recurrence-incident", "earlier_capsule_id": "earlier-capsule"})
        runner.save(output / "review-request.json", self.queued)
        runner.save(output / "earlier-capsule-retrieved.json", self.capsule)
        return output

    def test_history_polls_report_contract_and_uses_top_level_review_status(self):
        self.reviews = iter([{**self.queued, "result": {"status": "ready"}}, self.ready])
        with patch.object(runner.media.time, "sleep"):
            runner.history(SimpleNamespace(case_dir=self.root, execute=True))
        output = self.root / "history-review"
        self.assertEqual(runner.read(output / "source-review.json"), self.ready)
        self.assertEqual(len(list((output / "raw-reviews").iterdir())), 2)
        evaluation = runner.read(output / "evaluation.json")
        self.assertEqual(evaluation["status"], "ready")
        self.assertTrue(evaluation["completed"])
        self.assertEqual(evaluation["sufficiency"], "partially_sufficient")
        self.assertEqual(evaluation["review_id"], "accepted-review")
        self.assertEqual(sum(len(call.args) == 2 for call in self.requests.call_args_list), 1)

    def test_history_status_get_only_preserves_all_original_attempt_files(self):
        original = self.accepted_attempt()
        runner.save(original / "source-review.json", {"status": "running"})
        runner.save(original / "evaluation.json", {"status": "incomplete", "error": "old timeout"})
        saved = {path: path.read_bytes() for path in original.iterdir()}
        runner.history_status(SimpleNamespace(case_dir=self.root))
        self.configuration.assert_not_called()
        self.assertTrue(all(len(call.args) == 1 and not call.kwargs for call in self.requests.call_args_list))
        for path, contents in saved.items(): self.assertEqual(path.read_bytes(), contents)
        output = next((original / "status").iterdir())
        self.assertEqual(runner.read(output / "source-review.json"), self.ready)
        self.assertEqual(runner.read(output / "review-request.json"), self.queued)
        self.assertEqual(runner.read(output / "reconciliation.json")["provider_requests"], 0)

    def test_failed_review_is_completed_but_never_promoted_to_ready(self):
        original = self.accepted_attempt()
        self.reviews = iter([{**self.queued, "status": "incomplete", "result": None}])
        runner.history_status(SimpleNamespace(case_dir=self.root))
        evaluation = runner.read(next((original / "status").iterdir()) / "evaluation.json")
        self.assertEqual(evaluation["status"], "incomplete")
        self.assertTrue(evaluation["completed"])
        self.assertIsNone(evaluation["sufficiency"])

    def test_reconcile_rejects_mismatched_accepted_identity_without_requests(self):
        original = self.accepted_attempt()
        for queued in ({"episode_id": "earlier-episode"}, {**self.queued, "episode_id": "other"}):
            runner.save(original / "review-request.json", queued)
            with self.assertRaisesRegex(ValueError, "do not match"):
                runner.history_status(SimpleNamespace(case_dir=self.root))
        self.requests.assert_not_called()

    def test_missing_review_times_out_without_retry_or_replacing_original_outputs(self):
        original = self.accepted_attempt()
        self.reviews = iter([{**self.ready, "episode_id": "wrong-episode"}])
        with patch.object(runner.media, "time", media_clock([0, 0, 160])), \
             self.assertRaisesRegex(TimeoutError, "history-status, do not repeat the POST"):
            runner.history_status(SimpleNamespace(case_dir=self.root))
        self.requests.assert_called_once_with("http://product/api/incidents/earlier-incident/report")
        self.assertEqual(runner.read(original / "review-request.json"), self.queued)
        output = next((original / "status").iterdir())
        self.assertFalse((output / "evaluation.json").exists())
        self.assertEqual([runner.read(path) for path in (output / "raw-reviews").iterdir()], [{}])


class AttachmentWorkflowTests(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.root = Path(directory.name)
        self.record = {"fcapsule": "http://product", "episode_id": "shared-episode",
                       "incident_id": "new-incident", "model_config": config()}
        self.args = SimpleNamespace(pixels_reviewed=True, incremental_evidence=True, expected_revision="r2")
        self.original = {"revision_id": "r1", "policy_version": "old-policy"}
        runner.save(self.root / "investigation-before.json", self.original)
        self.original_bytes = (self.root / "investigation-before.json").read_bytes()
        self.before = {"revision_id": "r2", "status": "ready", "model": config()["model"],
                       "primary_incident_id": "new-incident", "policy_version": "current-policy",
                       "context": {"alerts": [{"incident_id": "new-incident"}]}}
        self.existing = [{"attachment_id": "prior-image", "kind": "image", "sha256": "prior-hash", "status": "ready"},
                         {"attachment_id": "prior-note", "kind": "text", "sha256": "note-hash", "status": "ready"}]
        self.metadata = {"sha256": "capture-hash", "observed_at": "2026-09-23T13:13:00Z", "source_url": "http://prom/query"}
        self.attachment = {"attachment_id": "new-image", "kind": "image", "sha256": "capture-hash", "status": "ready"}
        self.uploaded = False
        self.updated = False
        for name, kwargs in (
            ("paid_context", {"return_value": (self.root, self.record, "http://product/api/episodes/shared-episode")}),
            ("validated_image", {"return_value": (b"unit-test-only", self.metadata)}),
            ("configuration", {"return_value": config()}),
            ("request", {"side_effect": self.api}),
        ):
            patcher = patch.object(runner, name, **kwargs)
            mocked = patcher.start()
            self.addCleanup(patcher.stop)
            if name == "request": self.requests = mocked

    def api(self, url, payload=None):
        if url.endswith("/evidence"):
            if payload is not None:
                self.assertTrue((self.root / "media-review" / "baseline-provenance.json").exists())
                self.assertTrue((self.root / "media-review" / "existing-evidence.json").exists())
                self.uploaded = True
                return {"attachment_id": "new-image"}
            return (getattr(self, "changed_existing", self.existing) + [self.attachment]) if self.uploaded else self.existing
        if url.endswith("/api/settings/media"):
            return {key: {"capability": {"status": "ready"}} for key in ("vision", "core_investigator")}
        if url.endswith("/investigation/update"):
            self.updated = True
            return {"revision_id": "r3"}
        if url.endswith("/investigation"):
            if self.updated:
                return {**self.before, "revision_id": "r3", "parent_revision_id": "r2"}
            return getattr(self, "changed_assessment", self.before) if self.uploaded else self.before
        self.fail("Unexpected request: " + url)

    def test_default_rejects_existing_evidence_even_with_expected_revision(self):
        self.args.incremental_evidence = False
        with self.assertRaisesRegex(ValueError, "Existing media"):
            runner.attach(self.args)
        self.assertFalse(self.uploaded)
        self.assertFalse((self.root / "media-review").exists())

    def test_default_rejects_changed_baseline_even_with_matching_expected_revision(self):
        self.args.incremental_evidence = False
        self.existing = []
        with self.assertRaisesRegex(ValueError, "Baseline changed"):
            runner.attach(self.args)
        self.assertFalse(self.uploaded)

    def test_incremental_requires_explicit_expected_revision_before_any_request(self):
        self.args.expected_revision = None
        with self.assertRaisesRegex(ValueError, "requires explicit --expected-revision"):
            runner.attach(self.args)
        self.requests.assert_not_called()

    def test_incremental_rejects_revision_mismatch_active_or_missing_incident(self):
        for edit in ("revision", "active", "context"):
            with self.subTest(edit=edit):
                before = copy.deepcopy(self.before)
                if edit == "revision": self.before["revision_id"] = "unexpected"
                if edit == "active": self.before["status"] = "running"
                if edit == "context":
                    self.before["context"]["alerts"] = [{"incident_id": "other-incident"}]
                    self.before["primary_incident_id"] = "new-incident"
                with self.assertRaises(ValueError): runner.attach(self.args)
                self.assertFalse(self.uploaded)
                self.assertFalse((self.root / "media-review").exists())
                self.before = before

    def test_incremental_audits_existing_evidence_intervening_policy_and_capture(self):
        runner.attach(self.args)
        output = self.root / "media-review"
        self.assertEqual((self.root / "investigation-before.json").read_bytes(), self.original_bytes)
        self.assertEqual(runner.read(output / "investigation-original.json"), self.original)
        self.assertEqual(runner.read(output / "investigation-before.json"), self.before)
        self.assertEqual(runner.read(output / "existing-evidence.json"), self.existing)
        self.assertEqual(runner.read(output / "capture.json"), self.metadata)
        audit = runner.read(output / "baseline-provenance.json")
        self.assertEqual(audit["existing_attachments"], self.existing)
        self.assertEqual((audit["original_revision_id"], audit["current_revision_id"]), ("r1", "r2"))
        self.assertEqual((audit["original_policy"], audit["current_policy"]), ("old-policy", "current-policy"))
        self.assertTrue(audit["baseline_changed"])
        self.assertEqual(audit["capture_sha256"], "capture-hash")
        result = runner.read(output / "evaluation.json")
        self.assertTrue(result["incremental_evidence"])
        self.assertIn("Incremental evidence, not isolated image ablation", result["limitation"])
        self.assertTrue(result["parent_linked"])
        self.assertEqual(result["before_revision"], "r2")
        self.assertEqual(sum(len(call.args) == 2 for call in self.requests.call_args_list), 2)
        originals = {p.name: p.read_bytes() for p in output.iterdir()}
        with self.assertRaisesRegex(ValueError, "already attempted"): runner.attach(self.args)
        self.assertEqual({p.name: p.read_bytes() for p in output.iterdir()}, originals)

    def test_incremental_allows_changed_baseline_with_no_existing_attachment(self):
        self.existing = []
        runner.attach(self.args)
        self.assertTrue(self.updated)
        self.assertEqual(runner.read(self.root / "media-review" / "existing-evidence.json"), [])

    def test_default_unchanged_baseline_with_no_evidence_still_works(self):
        self.args.incremental_evidence = False
        self.existing = []
        runner.save(self.root / "investigation-before.json", self.before)
        runner.attach(self.args)
        self.assertTrue(self.updated)
        self.assertFalse(runner.read(self.root / "media-review" / "evaluation.json")["incremental_evidence"])

    def test_changed_evidence_during_upload_stops_before_reassessment(self):
        self.changed_existing = [{**item, "sha256": "changed"} for item in self.existing]
        with self.assertRaisesRegex(RuntimeError, "Evidence changed"): runner.attach(self.args)
        self.assertTrue(self.uploaded)
        self.assertFalse(self.updated)
        self.assertEqual(runner.read(self.root / "media-review" / "existing-evidence.json"), self.existing)
        self.assertTrue((self.root / "media-review" / "evidence-pre-update.json").exists())

    def test_changed_context_after_extraction_stops_before_reassessment(self):
        self.changed_assessment = {**self.before, "context": {"alerts": [{"incident_id": "other-incident"}]}}
        with self.assertRaisesRegex(RuntimeError, "no longer makes this incident primary"): runner.attach(self.args)
        self.assertTrue(self.uploaded)
        self.assertFalse(self.updated)
        self.assertTrue((self.root / "media-review" / "investigation-pre-update.json").exists())


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
        self.assertFalse(runner.assessment_matches_primary(self.assessment(), "new-incident"))

    def test_run_waits_past_stale_terminal_and_current_running_revision(self):
        stale = {**self.assessment(incident_id="old-incident"), "revision_id": "r1"}
        running = {**self.assessment("running"), "revision_id": "r2"}
        current = {**self.assessment(), "revision_id": "r2"}
        current["primary_incident_id"] = "new-incident"
        current["context"]["alerts"].insert(0, {"incident_id": "old-incident"})
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            record = self.record()
            with patch.object(runner, "request", side_effect=[stale, running, current, {}]) as api, \
                 patch.object(runner.media, "time", media_clock([0, 0, 1, 2])):
                self.assertEqual(runner.await_assessment(record, root, seconds=3), current)
            self.assertEqual(runner.read(root / "investigation-before.json"), current)
            self.assertEqual(len(list((root / "raw-assessments").iterdir())), 3)
            saved = runner.read(root / "run.json")
            self.assertTrue(saved["assessment_context_contains_incident"])
            self.assertTrue(saved["assessment_matches_incident"])
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
                 patch.object(runner.media, "time", media_clock([0, 0, 1])), \
                 self.assertRaisesRegex(TimeoutError, "makes the run incident primary; no retry started"):
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
                value["primary_incident_id"] = "new-incident"
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
                 patch.object(runner.media, "time", media_clock([0, 0, 1])):
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
