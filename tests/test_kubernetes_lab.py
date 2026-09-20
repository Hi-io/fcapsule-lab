import unittest
import sys
from pathlib import Path
from types import SimpleNamespace

sys.modules.setdefault("pymysql", SimpleNamespace(MySQLError=Exception))
from app.control import HTML, SCENARIOS


ROOT = Path(__file__).resolve().parents[1]


class KubernetesLabTests(unittest.TestCase):
    def test_four_primary_scenarios_cover_fault_and_performance_management(self):
        primary = {key: value for key, value in SCENARIOS.items() if key != "lock-contention"}

        self.assertEqual(len(primary), 4)
        self.assertEqual(sum(item["class"] == "FM" for item in primary.values()), 2)
        self.assertEqual(sum(item["class"] == "PM" for item in primary.values()), 2)
        self.assertIn("Recover all", HTML)

    def test_kustomization_includes_workloads_and_observability(self):
        kustomization = (ROOT / "deploy" / "kubernetes" / "kustomization.yaml").read_text(encoding="utf-8")
        observability = (ROOT / "deploy" / "kubernetes" / "observability.yaml").read_text(encoding="utf-8")

        self.assertIn("applications.yaml", kustomization)
        self.assertIn("mysql.yaml", kustomization)
        self.assertIn("LabWorkerOOMKilled", observability)
        self.assertIn("LabWorkerCrashLooping", observability)
        self.assertIn("LabWorkerCPUHigh", observability)
        self.assertIn("LabMySQLConnectionsSaturated", observability)


if __name__ == "__main__":
    unittest.main()
