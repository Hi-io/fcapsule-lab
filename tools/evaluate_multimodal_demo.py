"""Attach a reviewed external screenshot and operator audio to one captured demo.

Uses the existing bounded Prometheus fault runner; does not inject another fault,
change provider settings, retry paid requests, or inject the expected diagnosis.
"""

import argparse
import base64
import hashlib
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from tools.evaluate_external_screenshot import (
    TERMINAL, assessment_cites, evidence_delivery, validated_image, validate_capture, wait_for,
)
from tools.run_scenarios import now, request, save


def audio_payload(path, provenance):
    data = path.read_bytes()
    if not (44 < len(data) <= 8 * 1024**2
            and data[:4] == b"RIFF" and data[8:12] == b"WAVE"):
        raise ValueError("Provide a nonempty WAV recording no larger than 8 MiB")
    if not provenance.strip():
        raise ValueError("Describe whether the recording is human or synthesized")
    return {
        "kind": "audio", "filename": path.name,
        "content_base64": base64.b64encode(data).decode(),
        "context_note": provenance, "source_redacted": False,
    }, hashlib.sha256(data).hexdigest()


def evaluate(args):
    root = args.out.resolve()
    record = json.loads((root / "run.json").read_text())
    recovery = json.loads((root / "recovery.json").read_text())
    if record.get("outcome") != "captured" or not recovery.get("restored"):
        raise ValueError("Require a captured and recovered external screenshot run")
    if not args.pixels_reviewed:
        raise ValueError("Inspect the screenshot before uploading")
    if args.image_phase == "fault":
        image, metadata = validated_image(root, record["prometheus"])
    else:
        image_path = root / "after.png"
        validate_capture(image_path, record["prometheus"])
        image = image_path.read_bytes()
        metadata = json.loads(Path(str(image_path) + ".json").read_text())
    audio, audio_hash = audio_payload(args.audio, args.audio_provenance)
    base = (args.fcapsule or record["fcapsule"]).rstrip("/")
    episode = base + "/api/episodes/" + record["episode_id"]
    settings = request(base + "/api/settings/media")
    if any(settings[key]["capability"]["status"] != "ready"
           for key in ("vision", "audio", "core_investigator")):
        raise ValueError("Validate the image, audio and investigation models first")
    existing = request(episode + "/evidence")
    if existing and not args.expected_revision:
        raise ValueError("Existing evidence prevents a clean baseline; use another run")
    before = wait_for(lambda: request(episode + "/investigation"),
                      lambda item: item.get("status") in TERMINAL, 360,
                      "Automatic investigation did not finish")
    if before.get("status") != "ready" or not before.get("assessment"):
        raise ValueError("Baseline is not usable; inspect it without a paid retry")
    if args.expected_revision and before.get("revision_id") != args.expected_revision:
        raise ValueError("Expected revision no longer matches the current baseline")
    if not args.label or any(c not in "abcdefghijklmnopqrstuvwxyz0123456789-" for c in args.label):
        raise ValueError("Use a lowercase label to preserve each separate attempt")
    output = root / args.label
    output.mkdir(exist_ok=False)
    save(output / "before.json", before)
    save(output / "existing-evidence.json", existing)
    save(output / "attempt.json", {"at": now(), "episode_id": record["episode_id"],
         "image_sha256": metadata["sha256"], "audio_sha256": audio_hash,
         "audio_provenance": args.audio_provenance,
         "comparison": "Incremental image plus audio; not an isolated modality ablation"})
    payloads = [{"kind": "image", "filename": "prometheus-target-" + args.image_phase + ".png",
                 "content_base64": base64.b64encode(image).decode(),
                 "observed_at": metadata["observed_at"], "source_redacted": False,
                 "context_note": "External Prometheus Targets page at the recorded time."}, audio]
    attachments = []
    for payload in payloads:
        kind = payload["kind"]
        submitted = request(episode + "/evidence", payload)
        save(output / (kind + "-submitted.json"), submitted)
        attachment = wait_for(
            lambda: next(a for a in request(episode + "/evidence")
                         if a["attachment_id"] == submitted["attachment_id"]),
            lambda a: a["status"] in TERMINAL, 140, kind + " extraction timed out")
        save(output / (kind + ".json"), attachment)
        if attachment["status"] != "ready":
            raise RuntimeError(kind + " failed; inspect saved result, do not retry blindly")
        attachments.append(attachment)
    current = request(episode + "/investigation")
    if current.get("revision_id") != before.get("revision_id"):
        raise RuntimeError("Baseline changed during extraction; retained uploads, no reassessment")
    queued = request(episode + "/investigation/update", {})
    save(output / "update.json", queued)
    if not queued.get("revision_id") or queued["revision_id"] == before.get("revision_id"):
        raise RuntimeError("No new revision accepted; inspect server state")
    after = wait_for(lambda: request(episode + "/investigation"),
                     lambda r: r.get("revision_id") == queued["revision_id"]
                     and r.get("status") in TERMINAL, 360, "Reassessment timed out")
    save(output / "after.json", after)
    result = {
        "status": after["status"], "episode_id": record["episode_id"],
        "revision_linked": after.get("parent_revision_id") == before.get("revision_id"),
        "same_model": before.get("model") == after.get("model"),
        "before_usage": before.get("usage"), "after_usage": after.get("usage"),
        "evidence": [{"kind": a["kind"], "attachment_id": a["attachment_id"],
                      "usage": a.get("usage"),
                      "cited": assessment_cites(after, a["attachment_id"]),
                      "delivery": evidence_delivery(after, a["attachment_id"])}
                     for a in attachments],
        "value_verdict": "Human review required: distinguish new facts from corroboration",
        "limitations": ["Live telemetry may change after recovery.",
                        "Audio is operator testimony, not independently verified telemetry.",
                        "Successful extraction or citation does not prove better diagnosis."],
    }
    save(output / "result.json", result)
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--audio", type=Path, required=True)
    parser.add_argument("--audio-provenance", required=True)
    parser.add_argument("--fcapsule", help="Override the original API URL, e.g. a new local tunnel")
    parser.add_argument("--pixels-reviewed", action="store_true")
    parser.add_argument("--image-phase", choices=("fault", "after"), default="fault")
    parser.add_argument("--expected-revision", help="Explicitly acknowledge incremental evidence on this exact baseline")
    parser.add_argument("--label", default="multimodal", help="New attempt directory; existing attempts are never overwritten")
    evaluate(parser.parse_args())
