from __future__ import annotations

from unittest import TestCase
from unittest.mock import MagicMock, Mock, patch

from app.worker import MAX_LOGGED_DELIVERY_ATTEMPT, WorkerState


RUN_ID = "a" * 32


class StopLoop(Exception):
    pass


class WorkerDeliveryTelemetryTests(TestCase):
    def test_rejected_job_logs_bounded_attempt_number_on_redelivery(self):
        state = WorkerState()
        state.logger = Mock()
        job = (41, "import", '{"schema":"inventory.import.v2","body":"%%%"}', RUN_ID)
        connection = MagicMock()
        connection.__enter__.return_value = connection
        cursor = MagicMock()
        connection.cursor.return_value.__enter__.return_value = cursor
        cursor.fetchone.side_effect = [job, job]
        state._connect = Mock(return_value=connection)

        with patch("app.worker.time.sleep", side_effect=[None, StopLoop]):
            with self.assertRaises(StopLoop):
                state._job_loop()

        received = [call.kwargs for call in state.logger.write.call_args_list
                    if call.args[1] == "Import delivery received"]
        rejected = [call.kwargs for call in state.logger.write.call_args_list
                    if call.args[1] == "Import decoder rejected document"]
        self.assertEqual([event["delivery_attempt"] for event in received], [1, 2])
        self.assertEqual([event["is_redelivery"] for event in received], [False, True])
        self.assertEqual([event["delivery_attempt"] for event in rejected], [1, 2])
        self.assertEqual([event["is_redelivery"] for event in rejected], [False, True])
        self.assertEqual(state._record_delivery_attempt(41), 3)

    def test_attempt_counter_is_capped_and_reset_when_cleared(self):
        state = WorkerState()
        self.assertEqual(state._record_delivery_attempt(41), 1)
        state._delivery_attempt = MAX_LOGGED_DELIVERY_ATTEMPT - 1
        self.assertEqual(state._record_delivery_attempt(41), MAX_LOGGED_DELIVERY_ATTEMPT)
        self.assertEqual(state._record_delivery_attempt(41), MAX_LOGGED_DELIVERY_ATTEMPT)

        state._clear_delivery_attempt(41)

        self.assertEqual(state._record_delivery_attempt(41), 1)


if __name__ == "__main__":
    import unittest

    unittest.main()
