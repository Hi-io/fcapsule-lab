import copy
import unittest
from unittest.mock import Mock, patch

from app.control import ControlState, FIELD_OWNER
from app.scenario_catalog import public_scenarios


class OneClickEvidenceTests(unittest.TestCase):
    def setUp(self):
        self.state = ControlState()
        self.state.active = {"run_id": "a" * 32}
        self.doc = {"metadata": {"resourceVersion": "1", "annotations": {}},
                    "spec": {"endpoints": [{"path": "/metrics", "port": "metrics",
                                           "interval": "15s", "relabelings": [{"action": "replace"}]}]}}
        self.state._kubernetes_get = Mock(side_effect=lambda *_: copy.deepcopy(self.doc))
        def apply(resource, name, payload):
            for section, values in payload.items():
                self.doc.setdefault(section, {}).update(copy.deepcopy(values))
        self.state._kubernetes_patch = Mock(side_effect=apply)

    def test_catalog_is_clickable_without_external_runner(self):
        item = public_scenarios()["exporter-path-rollback"]
        self.assertEqual(item["track"], "demo")
        self.assertFalse(item["runner_only"])
        self.assertEqual(item["execution"], "controller")

    def test_real_endpoint_patch_and_exact_recovery(self):
        original = copy.deepcopy(self.doc["spec"])
        self.state._start("exporter-path-rollback", 180)
        self.assertEqual(self.doc["spec"]["endpoints"][0]["path"], "/metrics-v2")
        self.assertEqual(self.doc["spec"]["endpoints"][0]["relabelings"], original["endpoints"][0]["relabelings"])
        self.state._restore_owned_fields("servicemonitors", "fcapsule-lab-mysql", "spec", "a" * 32)
        self.assertEqual(self.doc["spec"], original)
        self.assertIsNone(self.doc["metadata"]["annotations"][FIELD_OWNER])

    def test_operator_edit_is_preserved(self):
        self.state._start("exporter-path-rollback", 180)
        self.doc["spec"]["endpoints"][0]["interval"] = "30s"
        with self.assertRaisesRegex(ValueError, "preserving operator edit"):
            self.state._restore_owned_fields("servicemonitors", "fcapsule-lab-mysql", "spec", "a" * 32)

    def test_unexpected_endpoint_blocks_mutation(self):
        self.doc["spec"]["endpoints"][0]["path"] = "/custom"
        with self.assertRaisesRegex(ValueError, "no change applied"):
            self.state._start("exporter-path-rollback", 180)
        self.state._kubernetes_patch.assert_not_called()

    def test_recovery_from_restart_uses_persisted_monitor_journal(self):
        original = copy.deepcopy(self.doc["spec"])
        self.state._start("exporter-path-rollback", 180)
        restarted = ControlState()
        restarted._kubernetes_get = self.state._kubernetes_get
        restarted._kubernetes_patch = self.state._kubernetes_patch
        restarted._restore_owned_fields("servicemonitors", "fcapsule-lab-mysql", "spec", "a" * 32)
        self.assertEqual(self.doc["spec"], original)
