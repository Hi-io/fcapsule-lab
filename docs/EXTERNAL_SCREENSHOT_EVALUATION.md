# External Screenshot Evaluation

## Case and Boundary

`mysql-exporter-scrape-path` is a separate operational/media probe, not a sixteenth
scored workload case. A realistic monitoring configuration rollout changes the Lab
MySQL exporter's ServiceMonitor path from `/metrics` to `/metrics-v2`. The target is
still discovered but returns HTTP 404. MySQL, the exporter Pod and checkout remain
running. This differs from `metrics-service-label-drift`, where discovery removes
the target, and from database connection saturation.

The external source is **Prometheus Targets**, normally
`http://192.168.0.102:30090/targets`. Select scrape pool
`serviceMonitor/fcapsule-lab/fcapsule-lab-mysql/0`. The PNG must visibly show that
pool, exporter identity, endpoint path, DOWN state and HTTP 404. FCAPSule/Lab UI
images, generated screenshots, and evaluator-rendered HTML are not evidence.

The runner modifies only one Lab ServiceMonitor path and an ownership annotation.
It creates one temporary namespace-scoped PrometheusRule with a 15-second symptom
hold, then removes it. Neither its annotations nor the upload context supply the
expected cause. No Pod, application configuration, product file, provider setting,
namespace scope, or unrelated monitoring resource is changed.

## Safety and Capture

Requirements: idle healthy Lab, node-exporter memory readings above 1 GiB on every
node hosting a Lab Pod, healthy exporter target, existing validated vision/core providers, kubectl
permissions, and explicitly approved Chrome automation. No new cluster workloads.
The fault loop is 180 seconds; a separate local watchdog attempts owned rollback
at 240 seconds even if the parent process dies. Each sample stops the run below
768 MiB or on failed health. `finally` restores the saved path using resource-version
and ownership guards. A replacement or concurrently changed object is not overwritten.
The local watchdog cannot survive loss of the operator machine; retain `run.json`
and use the restore command promptly after such a failure.

From the Lab repository, with Node/Playwright/Chrome already installed:

```powershell
$env:PLAYWRIGHT_MODULE='C:/Users/Hiros/.cache/codex-runtimes/codex-primary-runtime/dependencies/node/node_modules/playwright'
$env:CHROME_EXECUTABLE='C:/Program Files/Google/Chrome/Application/chrome.exe'
$python='C:/Users/Hiros/.cache/codex-runtimes/codex-primary-runtime/dependencies/python/python.exe'
& $python tools/evaluate_external_screenshot.py run --out artifacts/external-scrape-<UTC>
```

Windows uses WSL Ubuntu kubectl; Linux uses local kubectl. `--node` and the three
service URLs are configurable. Use a new output directory for every attempt.
The helper drives normal Prometheus filtering and saves unedited viewport pixels,
UTC observation time, URL, pool and SHA-256. Inspect `before.png`, `fault.png`, and
`after.png` visually. Never infer screenshot success only from DOM text or API data.

Emergency guarded cleanup, requiring the saved baseline and ownership record:

```text
python tools/evaluate_external_screenshot.py restore --out artifacts/external-scrape-<UTC>
```

## One-Shot Media Evaluation

Only after recovery and pixel review:

```text
python tools/evaluate_external_screenshot.py evaluate --out artifacts/external-scrape-<UTC> --pixels-reviewed
```

The run uses the normal automatic no-image investigation as baseline. It refuses
episodes with existing media (including prior FCAPSule screenshots). Evaluation
saves the unedited baseline, submits **one** fault image to the existing evidence
API, waits for extraction, and requests **one** explicit evidence reassessment.
It never automatically retries a paid call or changes model settings. An attempt
marker deliberately blocks reruns after ambiguous network errors; inspect saved
attachment/revision state instead of deleting that marker to retry.

Artifacts include API/configuration/health snapshots before, during and after;
actual firing alert; recovery confirmation; image hashes/provenance; raw extraction;
unedited before/after investigations; token usage; and mechanical pipeline checks.
Review all failures as outcomes. A `ready` extraction can still omit crucial facts.

## Explicit Post-Fix Review

If the first attempt exposes a product defect, preserve it and fix the product. An
explicit follow-up can reuse the same processed attachment without another fault or
vision call:

```text
python tools/evaluate_external_screenshot.py reassess-existing --out artifacts/external-scrape-<UTC> --pixels-reviewed
```

The runner checks the original episode, external PNG hash, unchanged extracted
content, one ready attachment and a terminal baseline. It refuses intervening work
unless the operator inspects that work and supplies its exact `--expected-revision`.
It saves that actual baseline separately. A marker prevents automatic paid retries,
including after an ambiguous timeout. Never delete it to force a rerun.

A further, separately justified product fix can be tested with an explicit
`--follow-up-label <name>` and the inspected `--expected-revision`. Those artifacts
go to `reassessments/<name>/`; earlier results are never overwritten. This is not an
automatic retry loop or permission to rerun until a preferred diagnosis appears.

Both call-level image visibility and actual assessment citations are measured.
An image in retained storage does not prove the investigator received it, and a
masked/broken citation does not count as a usable evidence link. Report all attempts,
including startup-triggered revisions, rather than presenting only the last result.

## Frozen Human Rubric

Judge these separately; do not assign correctness from pipeline status or keywords:

1. Fidelity: extracted identity, `/metrics-v2`, DOWN and 404 match the actual pixels.
2. Diagnosis: still-discovered scrape failure is distinguished from discovery loss,
   database outage, exporter crash, or network refusal. Configuration cause requires
   corroboration, not merely the screenshot.
3. Action: inspect ServiceMonitor path against the exporter contract and verify
   restored `up`, without restarting MySQL or widening unrelated monitoring access.
4. Temporal handling: screenshot observation precedes recovery; current `up=1` does
   not invalidate past HTTP 404. Do not invent a precise onset from relative UI time.
5. Incremental value: compare cited claims and next actions against the preserved
   baseline. Classify as added information, corroboration, redundant, ignored,
   misleading, or extraction/pipeline failure, with concrete quotations/references.

Record whether Pod readiness/restarts, workload health, original monitor spec, and
unrelated target health were unchanged. Existing control-plane scrape failures and
the exporter CPU alert must not be claimed as caused by this short intervention.

The reassessment can query live sources after recovery and uses changed evidence;
therefore it is a realistic before/after workflow, **not** a controlled screenshot-only
ablation or a general accuracy estimate. Preserve this limitation even on success.

## Observed Run: 23 September 2026

Local artifacts: `artifacts/external-scrape-20260923T0215Z/` (ignored by Git).
Episode reference: `EP-ED30FC92`; episode ID:
`episode-incident-20260923021558-labexporterscrapefailed-a1de799114`.
The scenario, rubric and fault mechanism were defined before the evaluation.

### Real Fault and Recovery

- Actual Prometheus page, not FCAPSule, supplied all three screenshots. Pixel review
  confirmed `/metrics` and UP before, `/metrics-v2`, DOWN and HTTP 404 during, and
  `/metrics` and UP after recovery.
- Fault image observed at `2026-09-23T02:16:00.355Z`; SHA-256:
  `c3f103c8cd4911976795ec70682e61d50333d529674c24014e365b4e371f42cd`.
- `LabExporterScrapeFailed` fired and FCAPSule captured two alerts in one episode.
- The monitor was restored and the temporary rule removed. All seven Lab Pods
  remained ready, with unchanged restart counts. The worker's existing 30 restarts
  predated this probe; MySQL and its exporter had zero restarts before and after.
- No new Pod was installed. This tests a monitoring failure, not application load
  or log-volume performance. It does not replace the fifteen workload evaluations.

### Extraction and Product Defects

The single vision call used `qwen/qwen3-vl-30b-a3b-instruct`. It correctly extracted
DOWN, HTTP 404, `/metrics-v2` and `mysql-exporter`. It misspelled the scrape-pool
namespace/name as `fcapse-lab` in one OCR field. The original extraction is retained
unchanged; the visible screenshot and correct structured labels remain available for
verification. Its limitation explicitly excludes configuration and historical inference.

The first evidence-added revision reached `ready` but did not use the image. Audited
`visible_evidence_ids` showed it was absent from every model request: older member
priorities displaced it under the input cap. The retained attachment alone had falsely
suggested the feature was functioning end to end. FCAPSule policy 1.15 introduced a
separate revision-addition priority and removed omitted IDs from the priority list.

The next explicit review saw the image in every call and changed its diagnosis from
resource/exporter unresponsiveness toward the incorrect metrics endpoint. However,
masking transformed its validated citation into `A-attachment-<ID>`, breaking navigation.
FCAPSule policy 1.16 preserves exact known reference IDs while still masking source
data and model prose. A restart-induced redundant revision was also observed, recorded,
and fixed by consistent primary-report identity and completed-input deduplication.

No expected answer was added to the prompt or attachment note. The fault, alert,
screenshot and extraction did not change between follow-ups. New tests target generic
prompt delivery, citation integrity and restart deduplication, not this expected diagnosis.

### All Investigation Attempts

The investigator used `deepseek-v4-pro` throughout. Times are UTC; counts are actual
provider-reported tokens, not estimates of compression or quality.

| Attempt | Started | Policy | Tokens | Observed outcome |
| --- | --- | --- | ---: | --- |
| Automatic first member | 02:16:48 | 1.14 | 6,166 | No image; weak resource/upstream hypothesis |
| Automatic second member / saved baseline | 02:17:18 | 1.14 | 6,547 | No image; resource/application explanation |
| First image-added review | 02:24:46 | 1.14 | 6,678 | Image stored but omitted from all three requests |
| Startup-triggered review | 02:34:56 | 1.15 | 6,529 | Unwanted additional initial-capture revision; preserved in `postfix-baseline.json` |
| Explicit priority-fix review | 02:37:56 | 1.15 | 6,320 | Image visible in all three requests; useful path diagnosis, broken masked citation |
| Explicit citation-fix review | 02:43:46 | 1.16 | 6,378 | Image visible and cited correctly; original image opens from the UI |

Total investigator usage across the episode: **38,618 tokens**, including all failed
and redundant attempts, not just the final review. The one vision call used **1,484
tokens** (992 input, 492 output), with reported OpenRouter cost **USD 0.0005428**.
Total reported tokens across both providers: **40,102**. No DeepSeek dollar cost is
inferred from token counts. Follow-ups reused the processed image without another
vision call. The final investigator run used 5,082 input and 1,296 output tokens over
three calls under the unchanged 2,100-token estimated full-request input cap per call.

The original failed attempt is in `investigation-after.json`; the priority fix is in
`investigation-after-fix.json`; the final follow-up is in
`reassessments/citation-links/investigation-after-fix.json`. Mechanical evaluations
and actual intermediate baselines accompany these files. Startup and member-triggered
attempts are also retained in revision history; do not treat them as independent cases.

### Human Review Against the Frozen Rubric

1. **Fidelity: useful, not exact.** Key operational facts match the screenshot;
   the scrape-pool OCR typo remains a documented error.
2. **Diagnosis: useful narrowing, not independent root-cause proof.** The final
   assessment identifies an endpoint/configuration mismatch causing HTTP 404. It
   weakens network failure and does not equate pod readiness with successful scraping.
   It did not retrieve the exact ServiceMonitor patch, so configuration cause remains
   a hypothesis despite the evaluator's separate knowledge of the intervention.
3. **Action: relevant.** It recommends checking the exporter configuration and
   ServiceMonitor metrics path, not restarting MySQL. It does not explicitly include
   the final verification of restored `up`; the operator should still verify recovery.
4. **Temporal handling: partial.** The capture timestamp is preserved and presented,
   but the final summary uses present-tense DOWN and does not explicitly narrate that
   the screenshot precedes recovery. Current workload state is labeled separately.
   This is a remaining model-output limitation, not evidence of a continuing outage.
5. **Incremental value: added diagnostic information after product fixes.** The
   path/404 observation materially changes the next check compared with the saved
   no-image baseline. Before the priority fix, the image was ignored by the pipeline.
   Before the citation fix, the answer was more useful but its reference was unusable.

Conclusion: this live probe validates an external-image-assisted investigation
workflow and exposed concrete product defects. It does **not** establish universal
accuracy, a Pro-versus-Flash advantage, or a controlled causal effect size for images.
The final result is practically helpful but not a perfect, independently confirmed RCA.
