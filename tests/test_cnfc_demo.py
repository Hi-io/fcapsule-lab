"""CNFC demo mechanics and pod-free alert contract."""

import unittest
from unittest.mock import Mock, patch
from urllib.error import URLError
from pathlib import Path

import yaml

from app.cnfc_edge import EdgeState
from app.scenario_catalog import OPERATOR_SCENARIOS, public_scenarios


ROOT = Path(__file__).resolve().parents[1]


class CnfcDemoTests(unittest.TestCase):
    def test_route_drift_changes_one_replica_and_recovers(self):
        state = EdgeState()
        state.logger = Mock()
        with patch("app.cnfc_edge.urlopen", side_effect=URLError("connection refused")):
            state.set_mode("route-drift", run_id="a" * 32)
            self.assertFalse(state.probe())
        self.assertIn("8099", state.metrics())
        self.assertIn("lab_cnfc_edge_dependency_failures_total 1", state.metrics())
        state.set_mode("normal", run_id="a" * 32)
        self.assertIn("8081", state.metrics())
        self.assertEqual(OPERATOR_SCENARIOS["cnfc-route-drift"]["expected_alert"],
                         "LabCNFCInventoryRouteFailures")
        self.assertEqual(public_scenarios()["cnfc-route-drift"]["track"], "demo")

    def test_alert_identity_is_cnfc_not_pod_or_service(self):
        documents = list(yaml.safe_load_all((ROOT / "deploy/kubernetes/observability.yaml").read_text()))
        rule = next(item for document in documents if document.get("kind") == "PrometheusRule"
                    for group in document["spec"]["groups"] for item in group["rules"]
                    if item["alert"] == "LabCNFCInventoryRouteFailures")
        self.assertIn("sum by (namespace, cnfc)", rule["expr"])
        self.assertIn("max by (namespace, cnfc, pod)", rule["expr"])
        self.assertNotIn("pod", rule["labels"])
        self.assertNotIn("service", rule["labels"])
        self.assertEqual(rule["for"], "15s")

        workloads = list(yaml.safe_load_all((ROOT / "deploy/kubernetes/cnfc_edge.yaml").read_text()))
        deployments = [item for item in workloads if item["kind"] == "Deployment"]
        self.assertEqual(len(deployments), 2)
        self.assertEqual({item["spec"]["template"]["metadata"]["labels"]["cnfc_id"]
                          for item in deployments}, {"checkout-edge-east"})
        self.assertEqual({item["spec"]["template"]["spec"]["nodeSelector"]["kubernetes.io/hostname"]
                          for item in deployments}, {"worker-1"})


if __name__ == "__main__":
    unittest.main()
