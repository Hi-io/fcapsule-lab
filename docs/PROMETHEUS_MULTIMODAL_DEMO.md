# When The Screenshot Remembers What Live Telemetry No Longer Shows

## The Demonstration

Use **Prometheus itself**, not a fabricated error page or an FCAPSule screenshot.
A monitoring configuration rollout accidentally changes the Lab MySQL exporter
scrape path from `/metrics` to `/metrics-v2`. Prometheus discovers the target but
receives a real HTTP 404. The database and exporter remain running.

The alert says that scraping failed. The Prometheus Targets screenshot preserves
the failed URL, target identity, DOWN state and HTTP error. An operator voice note
can add observations about timing and which services still responded. After a
rollback, a second Targets screenshot shows the same target UP at `/metrics`.
Together these records help distinguish a monitoring regression from a database
outage or exporter restart, even after the live configuration has changed.

This reuses `mysql-exporter-scrape-path`. It installs no application, creates no
new Pod, and does not change FCAPSule. The only injected change is the owned Lab
ServiceMonitor path plus a temporary Lab alert rule. The runner restores both.
Prometheus's [scrape configuration documentation](https://prometheus.io/docs/prometheus/latest/configuration/#scrape_config)
describes endpoint configuration; our guard and recovery details are in
[External Screenshot Evaluation](EXTERNAL_SCREENSHOT_EVALUATION.md).

## Prepare

Use an idle Lab with a healthy exporter. Validate the existing investigation,
image and audio models in FCAPSule Settings. The script checks actual node memory
headroom and workload health before injecting anything. Do not run another Lab
scenario concurrently.

Install from the Lab repository:

```text
python -m pip install -e ".[dev]"
```

Use the Python executable that has the installed dependencies. For the automated
capture, install Playwright with its browser or set `PLAYWRIGHT_MODULE` and
`CHROME_EXECUTABLE` to your existing installation. The runner verifies capture
before modifying the cluster. Windows Python uses WSL kubectl; Linux Python uses
Linux kubectl and Node. Do not mix Windows Node with Linux Python.

Keep a local-only API tunnel in a separate terminal, using your existing cluster
context. This avoids disabling HTTPS certificate validation in the evaluation:

```text
kubectl -n fcapsule port-forward svc/fcapsule 18765:8765 --address=127.0.0.1
```

On Windows with kubectl in WSL, run that command inside WSL. Your normal FCAPSule
browser URL remains the HTTPS address; the tunnel is only for the evaluation CLI.
Nothing exposes the API on a new LAN port.

## Run And Present

1. Open Prometheus **Status > Target health** (`/targets`). Filter the scrape pool
   to `serviceMonitor/fcapsule-lab/fcapsule-lab-mysql/0`. Leave it open beside
   FCAPSule Operations.
2. Start the existing guarded runner from the Lab repository:

   ```text
   python tools/evaluate_external_screenshot.py run --fcapsule http://127.0.0.1:18765 --out local_reports/multimodal-UNIQUE
   ```

   Override `--prometheus` and `--lab` for a different cluster. Use a new output
   directory each time. The defaults are the existing private Lab deployment.
3. Watch the target become DOWN with HTTP 404. Take your screenshot now, including
   the endpoint, pool and labels. The runner also saves unedited `fault.png` with
   URL, UTC timestamp and hash. It bounds the fault to roughly three minutes and
   restores sooner when FCAPSule has captured the report. Do not assume the fault
   will remain active until the presenter finishes speaking.
4. Open the new `LabExporterScrapeFailed` episode. Let its automatic assessment
   finish. Explain what the tool knows **before** you provide operator evidence.
5. Choose **Add evidence**. Paste or attach the external screenshot. Record a short
   voice note using the microphone on the HTTPS page. Example, only if observed:

   > This started after a monitoring configuration update. Inventory and orders
   > were still responding when I checked. This screenshot was taken during the
   > failure, before the configuration was restored.

   The recording is transcribed by the configured audio model. Review the text;
   do not assume transcription is perfect. Submit the evidence and use the
   evidence update/reassessment action after extraction has completed.
6. For a stronger temporal demonstration, also attach the recovery screenshot
   (`after.png`). Describe the actual rollback and whether Pods were restarted.
   Do not present a diagnosis as if it were an independent observation.
7. Show the revised explanation, evidence citations, the original images and the
   timeline. The useful result is a supported mechanism and next check, not a
   higher confidence number. Keep the earlier assessment visible in the revision
   record; do not hide an unsuccessful intermediate answer.

No voice note needs to include the expected `/metrics-v2` answer. The image should
carry that detail. In the successful recorded follow-up the voice note explained
the observed chronology, while the two images supplied the path comparison.

## Reproducible Image And Audio Evaluation

For a recorded evaluation instead of manual UI uploads, review the image pixels
and provide a short WAV recording (up to 8 MiB):

```text
python tools/evaluate_multimodal_demo.py --out local_reports/multimodal-UNIQUE --audio operator.wav --audio-provenance "Human operator recording of observations during the demo" --pixels-reviewed
```

The helper preserves the original automatic assessment, uploads each file once,
waits for extraction, requests one revision, and records model usage, delivery,
citations and linkage. It never changes model settings or retries paid requests.
An existing attempt directory blocks reruns after an ambiguous timeout. Inspect
the saved submitted IDs and live episode before taking another action.

A separately justified incremental recovery comparison is explicit:

```text
python tools/evaluate_multimodal_demo.py --out local_reports/multimodal-UNIQUE --audio recovery-note.wav --audio-provenance "Human operator recording of the observed rollback" --pixels-reviewed --image-phase after --expected-revision ACTUAL-CURRENT-REVISION --label recovery-evidence
```

The exact current revision is required. Existing evidence is retained and recorded;
this is not an isolated image-only or audio-only experiment. Use `--fcapsule` to
override the original API address if the local tunnel changes.

## Observed Evaluation: 27 September 2026 (Japan Time)

One actual cluster run was performed; no synthetic screenshot or alert was used.
The two voice recordings were **synthetic English narration of recorded actions
and health checks**, explicitly labelled as such in FCAPSule. This verifies file
transcription, not physical microphone capture or a human interview.

| Stage | Observed result |
| --- | --- |
| Fault | Real exporter target DOWN, `/metrics-v2`, HTTP 404; Prometheus alert fired and FCAPSule captured it. |
| Initial assessment | Already identified the failing endpoint, but could not resolve the difference between current and incident-time monitoring configuration. |
| Failure screenshot + first voice note | Both extracted successfully. Image cited, audio delivered but not cited. Assessment mixed the failing target with an unrelated dropped target in another pool. **Not a diagnostic improvement.** |
| Recovery screenshot + chronology note | Both cited. Assessment compared DOWN `/metrics-v2` against UP `/metrics`, identified a monitoring configuration change followed by rollback, and weakened the exporter-change explanation. **Useful improvement over the previous revision, with a more explicit chronology than baseline.** |
| Recovery | `/metrics` restored, temporary rule removed, target UP, unchanged Pod identities and restart counts. |

The final explanation still treats operator narration as testimony. It does not
claim to identify the person or configuration artifact responsible without further
inspection. An optional consistency review reported unavailable; the investigation
completed using its validated assessment. These limitations remain in the saved
records, rather than being relabelled as a perfect result.

The two explicitly compared baseline/revision assessments used 9,363, 7,821 and
5,611 investigator tokens respectively (22,795 combined); earlier automatic
revisions, if any, are additional. Image extraction used 1,506 and 1,329 tokens.
Audio durations were 19.506 and 37.838 seconds. This is one operational workflow,
not evidence of general model accuracy or token savings.

Private raw assessments, screenshots and provenance are under ignored
`local_reports/multimodal-demo-20260927/`. No credentials or cluster artifacts are
committed. The demo does not deliberately hide telemetry to make the image look
necessary: if FCAPSule already finds the cause, present the image as corroboration.

## Boundaries And Follow-Ups

- Use only the Lab monitor. Never break production target discovery for a demo.
- Recovery can happen before the investigation finishes. Label screenshot times;
  current healthy state does not invalidate a past failure.
- The local watchdog cannot survive loss of the operator machine. Use the saved
  run's guarded `restore` command if interrupted; do not issue broad rollback.
- Normal exclusion from an unrelated scrape pool is not a causal label defect.
  The first reassessment exposed this remaining investigation-quality limitation.
- The vision model made a minor scrape-pool OCR spelling error while correctly
  reading endpoint, status and HTTP error. Inspect the actual pixels and labels.
- Real browser microphone permissions and recording should be rehearsed by the
  presenter on the HTTPS URL. The automated evaluation does not operate the mic.
- Avoid claiming that screenshot plus audio always improves diagnosis. This case
  demonstrates the successful full workflow and also retains the weaker attempt.
