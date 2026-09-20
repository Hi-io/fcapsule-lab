import unittest

from tools.replay_rule import expression


class RuleReplayTests(unittest.TestCase):
    def test_selects_named_rule_and_ignores_empty_or_unrelated_documents(self):
        manifest = """---
---
kind: ServiceMonitor
---
kind: PrometheusRule
spec:
  groups:
    - name: example
      rules:
        - alert: Other
          expr: up == 0
        - alert: Wanted
          expr: pending > 1
"""
        self.assertEqual(expression(manifest, "Wanted"), "pending > 1")

    def test_missing_alert_does_not_silently_replay_another_rule(self):
        with self.assertRaises(StopIteration):
            expression("kind: PrometheusRule\nspec: {groups: []}\n", "Missing")
