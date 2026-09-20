import tempfile
import unittest
from pathlib import Path

from tools.review_run import observations, structured_logs


class ReviewTests(unittest.TestCase):
    def test_current_and_previous_container_overlap_is_not_a_redelivery(self):
        with tempfile.TemporaryDirectory() as directory:
            event = '{"@timestamp":"2026-09-20T12:00:00Z","message":"Import delivery received","job_id":7}\n'
            (Path(directory) / "worker.log").write_text(event)
            (Path(directory) / "worker-previous.log").write_text(event)
            self.assertEqual(observations(Path(directory))["import_deliveries_by_job"], {"7": 1})

    def test_reads_raw_and_kubectl_json_but_not_traceback_text(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "inventory.log"
            path.write_text('2026-09-20T00:00:00Z {"mysql_error_code":1205}\n'
                            '{"mysql_error_code":1054,"operation":"reserve_stock"}\n'
                            'Traceback (most recent call last):\n')
            self.assertEqual(len(list(structured_logs(path))), 2)
            result = observations(Path(directory))
            self.assertEqual(result["sql_error_codes"], {"1205": 1, "1054": 1})
            self.assertEqual(result["operations"], {"reserve_stock": 1})

    def test_tracks_redelivery_and_peak_logged_buffer_without_claiming_accuracy(self):
        with tempfile.TemporaryDirectory() as directory:
            (Path(directory) / "worker.log").write_text(
                '{"message":"Import delivery received","job_id":7}\n' * 2
                + '{"buffered_bytes":4194304}\n{"buffered_bytes":2097152}\n')
            result = observations(Path(directory))
            self.assertEqual(result["import_deliveries_by_job"], {"7": 2})
            self.assertEqual(result["largest_logged_buffer_bytes"], 4194304)
            self.assertIn("not all indexed logs", result["limitation"])
