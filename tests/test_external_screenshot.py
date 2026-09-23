import copy
import hashlib
import json
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from tools import evaluate_external_screenshot as runner


def baseline():
    return {"lab": {"active": None, "memory_error": None,
                    **{name: {"reachable": True} for name in ("worker", "inventory", "orders")}},
            "pods": [{"node": "node-1", "containers": [{"ready": True}]}],
            "node_addresses": {"node-1": ["node-1"]},
            "memory": {"data": {"result": [
                {"metric": {"instance": node}, "value": [0, str(2 * 1024**3)]} for node in ("node-1", "node-2")]}},
            "monitor": {"metadata": {"uid": "uid-1", "resourceVersion": "2", "annotations": {}},
                        "spec": {"endpoints": [{"path": "/metrics"}]}}}


class ExternalScreenshotTests(unittest.TestCase):
    def test_safety_accepts_measured_healthy_baseline(self):
        runner.require_safe(baseline())

    def test_safety_rejects_other_scenario_and_unknown_health(self):
        for key, value in (("active", {"run_id": "other"}), ("memory_error", "unavailable"), ("orders", {})):
            state = baseline()
            state["lab"][key] = value
            with self.assertRaises(RuntimeError):
                runner.require_safe(state)

    def test_safety_rejects_low_memory_and_missing_scheduled_node_samples(self):
        state = baseline()
        for reading in ("100", "NaN", "Inf"):
            state["memory"]["data"]["result"][0]["value"][1] = reading
            with self.subTest(reading=reading), self.assertRaises(RuntimeError):
                runner.require_safe(state)
        state = baseline()
        state["memory"]["data"]["result"][0]["metric"]["instance"] = "node-2"
        with self.assertRaises(RuntimeError):
            runner.require_safe(state)

    def test_safety_does_not_require_exact_node_count_or_check_unrelated_node(self):
        state = baseline()
        state["memory"]["data"]["result"] = state["memory"]["data"]["result"][:1]
        runner.require_safe(state)
        state["memory"]["data"]["result"].append({"metric": {"instance": "unrelated"}, "value": [0, "1"]})
        runner.require_safe(state)

    def test_selects_exact_pool_not_unrelated_down_targets(self):
        target = {"scrapePool": runner.POOL, "health": "up"}
        self.assertEqual(runner.exporter_target([target, {"scrapePool": "other", "health": "down"}]), target)
        for items in ([], [target, target]):
            with self.assertRaises(RuntimeError):
                runner.exporter_target(items)

    def test_rule_is_namespace_scoped_and_does_not_leak_injected_path(self):
        doc = runner.rule_document("owner")
        rule = doc["spec"]["groups"][0]["rules"][0]
        self.assertEqual(doc["metadata"]["namespace"], "fcapsule-lab")
        self.assertIn('service="mysql-exporter"', rule["expr"])
        self.assertNotIn("metrics-v2", json.dumps(rule))
        self.assertNotIn("404", json.dumps(rule))

    def test_patch_checks_resource_version_and_only_changes_path_and_owner(self):
        with patch.object(runner, "kubectl") as command:
            runner.patch_monitor(baseline()["monitor"], runner.FAULT_PATH, "owner")
        operations = json.loads(command.call_args.args[-1])
        self.assertEqual(operations[0], {"op": "test", "path": "/metadata/resourceVersion", "value": "2"})
        self.assertEqual([o["path"] for o in operations[1:]], ["/spec/endpoints/0/path", "/metadata/annotations/fcapsule.lab~1screenshot-run"])

    def test_restore_refuses_concurrent_owner_and_replaced_monitor(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            runner.save(root / "run.json", {"owner": "ours"})
            runner.save(root / "before.json", baseline())
            for change in ("owner", "uid"):
                current = copy.deepcopy(baseline()["monitor"])
                current["spec"]["endpoints"][0]["path"] = runner.FAULT_PATH
                current["metadata"]["annotations"][runner.OWNER] = "other" if change == "owner" else "ours"
                if change == "uid":
                    current["metadata"]["uid"] = "replacement"
                with patch.object(runner, "get", return_value=current), patch.object(runner, "patch_monitor") as edit:
                    with self.assertRaises(RuntimeError):
                        runner.restore(root)
                    edit.assert_not_called()

    def test_restore_uses_saved_path_and_uid_guarded_rule_delete(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            runner.save(root / "run.json", {"owner": "ours"})
            runner.save(root / "before.json", baseline())
            current = copy.deepcopy(baseline()["monitor"])
            current["metadata"]["annotations"][runner.OWNER] = "ours"
            current["spec"]["endpoints"][0]["path"] = runner.FAULT_PATH
            rule = {"metadata": {"uid": "rule-uid", "annotations": {runner.OWNER: "ours"}}}
            with patch.object(runner, "get", return_value=current), patch.object(runner, "patch_monitor") as edit, patch.object(runner, "kubectl", side_effect=[rule, {}]) as command:
                runner.restore(root)
            edit.assert_called_once_with(current, "/metrics", None)
            self.assertEqual(command.call_args.kwargs["body"]["preconditions"], {"uid": "rule-uid"})
            self.assertTrue((root / "recovery.json").exists())

    def test_rejects_product_ui_origin_and_changed_pixels(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            data = b"\x89PNG\r\n\x1a\n" + b"x" * 1500
            (root / "fault.png").write_bytes(data)
            metadata = {"pool": runner.POOL, "sha256": hashlib.sha256(data).hexdigest(), "source_url": "http://prometheus/targets"}
            runner.save(root / "fault.png.json", metadata)
            self.assertEqual(runner.validated_image(root, "http://prometheus")[0], data)
            for field, value in (("source_url", "http://fcapsule/console"), ("sha256", "wrong"), ("pool", "other")):
                runner.save(root / "fault.png.json", {**metadata, field: value})
                with self.assertRaises(ValueError):
                    runner.validated_image(root, "http://prometheus")

    def test_paid_evaluation_requires_pixel_review(self):
        with patch.object(runner, "request") as api:
            with self.assertRaisesRegex(ValueError, "pixels"):
                runner.evaluate(SimpleNamespace(out=Path("unused"), pixels_reviewed=False))
        api.assert_not_called()

    def test_text_mention_or_manifest_is_not_an_assessment_citation(self):
        self.assertFalse(runner.assessment_cites({"evidence_manifest": ["A-a"], "assessment": {"summary": "A-a"}}, "a"))
        self.assertTrue(runner.assessment_cites({"assessment": {"hypotheses": [{"evidence_ids": ["A-a"]}]}}, "a"))

    def test_media_visibility_uses_call_audit_not_retained_context(self):
        state = {"context": {"evidence": [{"id": "A-a"}]}, "calls": [
            {"phase": "assess", "visible_evidence_ids": ["A-a"]},
            {"phase": "review", "visible_evidence_ids": ["E1"]}]}
        delivery = runner.evidence_delivery(state, "a")
        self.assertTrue(delivery["visible_in_any_call"])
        self.assertFalse(delivery["visible_in_every_call"])
        self.assertFalse(runner.evidence_delivery({}, "a")["visible_in_every_call"])

    def test_existing_reassessment_preserves_results_and_never_uploads(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            args = SimpleNamespace(out=root, pixels_reviewed=True)
            prior = {"episode_id": "e", "revision_id": "old", "status": "ready", "model": "m"}
            attachment = {"attachment_id": "a", "sha256": "hash", "status": "ready", "extraction": {"text": "404"}}
            for name, value in {"run": {"outcome": "captured", "episode_id": "e", "prometheus": "http://prom",
                                        "fcapsule": "http://app"},
                                "recovery": {"restored": True}, "evaluation-started": {"pixels_reviewed": True},
                                "investigation-after": prior, "attachment": attachment}.items():
                runner.save(root / (name + ".json"), value)
            after = {**prior, "revision_id": "new", "parent_revision_id": "old"}
            original = (root / "investigation-after.json").read_bytes()
            for current, attachments in (
                ({**prior, "revision_id": "unacknowledged"}, [attachment]),
                ({**prior, "status": "running"}, [attachment]),
                (prior, [{**attachment, "sha256": "different"}]),
                (prior, [{**attachment, "correction": "new context"}]),
                (prior, [attachment, attachment]),
            ):
                with self.subTest(current=current, attachments=attachments), patch.object(
                    runner, "validated_image", return_value=(b"png", {"sha256": "hash"})
                ), patch.object(runner, "request", side_effect=[current, attachments]) as api:
                    with self.assertRaises(ValueError):
                        runner.reassess_existing(args)
                    self.assertFalse(any(len(call.args) > 1 for call in api.call_args_list))
                    self.assertFalse((root / "postfix-started.json").exists())
            with patch.object(runner, "validated_image", return_value=(b"png", {"sha256": "hash"})), patch.object(
                runner, "request", side_effect=[prior, [attachment], {"revision_id": "new"}]
            ) as api, patch.object(runner, "wait_for", return_value=after), patch("builtins.print"):
                runner.reassess_existing(args)
            writes = [call for call in api.call_args_list if len(call.args) > 1]
            self.assertEqual(len(writes), 1)
            self.assertTrue(writes[0].args[0].endswith("/investigation/update"))
            self.assertEqual(original, (root / "investigation-after.json").read_bytes())
            self.assertTrue((root / "investigation-after-fix.json").exists())
            with patch.object(runner, "request") as api:
                with self.assertRaisesRegex(ValueError, "already attempted"):
                    runner.reassess_existing(args)
                api.assert_not_called()

    def test_follow_up_name_cannot_escape_the_artifact_directory(self):
        with patch.object(runner, "request") as api:
            with self.assertRaisesRegex(ValueError, "label"):
                runner.reassess_existing(SimpleNamespace(out=Path("unused"), pixels_reviewed=True, follow_up_label="../escape"))
        api.assert_not_called()


if __name__ == "__main__":
    unittest.main()
