import base64
from pathlib import Path
import tempfile
import unittest

from tools.evaluate_multimodal_demo import audio_payload


class MultimodalDemoTests(unittest.TestCase):
    def test_audio_payload_preserves_bytes_and_provenance(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "operator.wav"
            data = b"RIFF" + bytes(4) + b"WAVE" + bytes(64)
            path.write_bytes(data)
            payload, digest = audio_payload(path, "Synthetic narration for pipeline testing")
            self.assertEqual(base64.b64decode(payload["content_base64"]), data)
            self.assertEqual(payload["kind"], "audio")
            self.assertIn("Synthetic", payload["context_note"])
            self.assertEqual(len(digest), 64)

    def test_invalid_file_or_missing_provenance_is_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "operator.wav"
            path.write_bytes(b"not audio")
            with self.assertRaises(ValueError):
                audio_payload(path, "Operator recording")
            path.write_bytes(b"RIFF" + bytes(4) + b"WAVE" + bytes(64))
            with self.assertRaises(ValueError):
                audio_payload(path, "  ")


if __name__ == "__main__":
    unittest.main()
