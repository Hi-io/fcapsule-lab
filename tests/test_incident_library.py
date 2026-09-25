import contextlib
import io
import unittest
from unittest.mock import Mock, patch

from app.incident_library import MECHANISMS, load_cases, public_cases
from app.control import ControlState
from app.library_runtime import FailureExecutor, LibraryState
from app.scenario_catalog import public_scenarios


class IncidentLibraryTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.cases = load_cases()

    def test_library_has_100_distinct_cases_outside_demo_catalog(self):
        self.assertEqual(len(self.cases), 100)
        self.assertEqual(len({item["title"] for item in self.cases.values()}), 100)
        self.assertEqual(len({item["category"] for item in self.cases.values()}), 20)
        self.assertTrue({item["mechanism"] for item in self.cases.values()} <= MECHANISMS)
        self.assertFalse(set(self.cases) & set(public_scenarios()))
        public = public_cases(self.cases)
        self.assertNotIn("parameters", next(iter(public.values())))
        self.assertNotIn("precursor", next(iter(public.values())))

    def test_every_profile_reaches_a_real_bounded_failure(self):
        executor = FailureExecutor()
        try:
            for key, case in self.cases.items():
                with self.subTest(case=key):
                    with self.assertRaises(Exception):
                        executor.execute(case)
        finally:
            executor.close()

    def test_run_ownership_recovery_and_prometheus_exposition(self):
        state = LibraryState(self.cases)
        state.logger = Mock()
        case_id = next(iter(self.cases))
        run_id = "a" * 32
        try:
            state.start(case_id, run_id, 30)
            with self.assertRaisesRegex(ValueError, "Another library case"):
                state.start(case_id, "b" * 32, 30)
            with contextlib.redirect_stdout(io.StringIO()):
                state.tick()
            self.assertEqual(state.attempts[case_id], 1)
            self.assertEqual(state.failures[case_id], 1)
            metrics = state.metrics()
            self.assertIn(f'lab_library_failures_total{{scenario_id="{case_id}"}} 1', metrics)
            self.assertEqual(metrics.count("# TYPE lab_library_failures_total"), 1)
            with self.assertRaisesRegex(ValueError, "ownership"):
                state.recover("b" * 32)
            state.recover(run_id)
            self.assertIn(f'lab_library_active{{scenario_id="{case_id}"}} 0', state.metrics())
        finally:
            state.executor.close()

    def test_controller_owns_library_start_and_recovery(self):
        state = ControlState()
        state.logger = Mock()
        state._health = Mock(return_value={"reachable": True})
        state._persisted_run = Mock(return_value=None)
        state._claim_run = Mock(return_value={})
        state._post_for_run = Mock(return_value={"run_id": "a" * 32, "ok": True})
        state._restore_owned_fields = Mock()
        state._clear_run_claim = Mock()
        case_id = next(iter(self.cases))
        with patch("app.control.memory_snapshot", return_value={
            "node_identity_verified": True, "available_bytes": 2 * 1024 ** 3,
        }):
            result = state.start(case_id, 30, "a" * 32)
        self.assertEqual(result["run"]["targets"], ["library"])
        self.assertEqual(state._post_for_run.call_args.args[0], state.library_url + "/control/scenario")
        self.assertEqual(state._post_for_run.call_args.args[1]["mode"], case_id)
        self.assertTrue(state.recover(expected_run_id="a" * 32)["ok"])
        self.assertEqual(state._post_for_run.call_args.args[1], {"mode": "normal"})


if __name__ == "__main__":
    unittest.main()
