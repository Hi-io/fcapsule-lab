# Scenario Operator Guide

The control UI exposes one catalog of 17 distinct incident scenarios. All are intended to work as
operator demos and FCAPSule investigations. Fifteen workload cases retain the frozen
balanced diagnostic benchmark; two monitoring cases exercise discovery and scrape
failure. That scoring boundary is an evaluation detail, not a recommendation to omit
either monitoring case from a demonstration.

The recorded `--case all` workflow additionally runs two separate occurrences of the
schema-rollout case. This exercises retained memory; it is not an eighteenth fault
mechanism. The runner checks distinct episode IDs, assesses whether the second
investigation retrieved and cited the exact earlier episode, then requests one
retained-capsule-only review. Expect the configured episode quiet window (960 seconds
by default) and one additional paid review call. Weak results remain visible without
an automatic retry.

## Complete Scenario Catalog

| Scenario | Useful distinction the investigation should make | Symptom signal | Safety / execution |
|---|---|---|---|
| `poison-job` | Repeated delivery of one malformed import and decoder failure while the worker remains live, not a generic worker outage | `LabWorkerPoisonRetries` | Durable job is owned by the run and removed only by exact identity during recovery. |
| `response-contract` | Inventory returned HTTP 200, but its document did not satisfy checkout's required fields; this is not a transport failure | `LabOrdersDependencyDocumentInvalid` | Bounded lease; no real customer payloads. |
| `reservation-token-collision` | MySQL error 1062 links distinct reservations to a reused uniqueness token | `LabInventoryConstraintFailures` | Generated Lab requests only. |
| `transaction-deadlock` | MySQL deadlock victims (1213) follow opposite row-lock ordering; distinguish from lock-wait timeout | `LabInventoryDeadlockVictims` | Uses disposable Lab inventory rows. |
| `idempotency-conflict` | A reused checkout key is bound to a different order identifier; the rejection occurs before the inventory call | `LabOrdersIdempotencyConflicts` | Generated checkout requests only. The API currently models order identity, not a multi-field cart payload. |
| `memory-leak` | Export pages are retained; measured buffer exceeds 80 MiB and approaches a 96 MiB safety cap | `LabWorkerBufferPressure` | Bounded below the container memory limit; no OOM is expected or desired. |
| `cpu-saturation` | Credential migration's PBKDF2 work keeps worker CPU use near its configured limit while progress remains observable | `LabWorkerCPUHigh` | Leased, finite workload on the existing worker; this alert alone does not prove CFS throttling. |
| `mysql-connections` | Retained sessions approach the actual 40-connection ceiling; compare current connections with configured capacity | `LabMySQLConnectionsSaturated` | Bounded session count and automatic cleanup. |
| `lock-contention` | Reconciliation holds an InnoDB row while reservations wait and time out; distinguish from deadlocks | `LabInventoryLockContention` | Holds only Lab-owned rows for the lease. |
| `downstream-latency` | Successful inventory work around 350 ms pushes checkout latency above the alert threshold; retries are not expected | `LabCheckoutLatencyHigh` | Fixed bounded delay; no external dependency. |
| `schema-drift` | Query revision v2 references `reserved_quantity`, which must be absent from the active table; correlate SQL 1054 with the applied ConfigMap revision | `LabInventoryQueryFailures` | A read-only schema precondition blocks an invalid run if the column already exists; narrow ConfigMap update and guarded restoration. |
| `dependency-route` | Active URL targets port 8099 while the Kubernetes Service exposes 8081 | `LabOrdersDependencyTransportFailures` | Narrow ConfigMap update and guarded restoration. |
| `timeout-budget` | Caller timeout of 50 ms is shorter than the dependency's 250 ms work budget | `LabOrdersDependencyTimeouts` | Narrow ConfigMap update; runner starts the stopped, bounded traffic generator only after this run is acknowledged as owned, then restores its observed replica count during recovery. Current 25 RPS runs have produced Inventory admission co-alerts and assessment drift. |
| `signing-key-skew` | Caller and dependency key identifiers disagree; explain authorization errors without exposing key material | `LabOrdersDependencyAuthorizationFailures` | Uses non-secret key IDs only. |
| `response-schema-skew` | Checkout expects response v2 while Inventory emits v1 | `LabOrdersDependencySchemaRejected` | Narrow ConfigMap update; runner starts the stopped, bounded traffic generator only after this run is acknowledged as owned, then restores its observed replica count during recovery. Inventory remains fast for this scenario. |
| `metrics-service-label-drift` | ServiceMonitor selector no longer matches the Service label while application Pods remain healthy | `LabApplicationMetricsDiscoveryMissing` | Changes only the selected Service label and restores its captured baseline. |
| `mysql-exporter-scrape-path` | Prometheus still discovers the MySQL exporter but `/metrics-v2` returns HTTP 404; distinguish scrape failure from target absence or a database outage | `LabExporterScrapeFailed` | Runner-only: captures real Prometheus Targets images and uses a saved baseline plus guarded rollback/watchdog. |

The exporter screenshots must come from the external Prometheus Targets page during
the fault. A graph is optional for workload cases; when attached, it must show the
actual alert metric and affected workload/time range. Healthy preflight images, Lab
control screenshots, FCAPSule UI images, generated charts and rendered evaluator pages
are not incident evidence.

## Scenario Coverage Notes

| Scenario | Implementation note |
|---|---|
| `poison-job` | Included: one durable malformed import is correlated by exact run ownership and cleaned up without broad stale-job deletion. |
| `response-contract` | Included: HTTP 200 with an invalid dependency document is distinguished from connectivity and server errors. |
| `reservation-token-collision` | Included: real unique-key constraint rejection is generated by Lab traffic. |
| `transaction-deadlock` | Included: opposing row-lock order creates real MySQL deadlock victims. |
| `idempotency-conflict` | Included: different synthetic order identifiers reuse a key; ordinary same-order retries replay their stored result without a second inventory call. |
| `memory-leak` | Included: allocation pressure is observable above 80 MiB; the 96 MiB buffer cap prevents an expected OOM. |
| `cpu-saturation` | Included: finite PBKDF2 work is constrained by the existing worker quota and lease; diagnose throttling only when a throttling metric is available. |
| `mysql-connections` | Included: real session usage is compared with MySQL's configured connection ceiling. |
| `lock-contention` | Included: a Lab-owned transaction holds the target row only for the leased interval. |
| `downstream-latency` | Included: a bounded dependency delay propagates to checkout latency and concurrency. |
| `schema-drift` | Included: query revision and actual table schema diverge; a read-only precondition checks the column before injection and the active ConfigMap is retained. |
| `dependency-route` | Included: the configured dependency port disagrees with the actual Kubernetes Service port. |
| `timeout-budget` | Included: caller timeout is below normal dependency work duration. A live run at 25 RPS produced Inventory admission co-alerts, so the assessment can drift toward that secondary symptom. |
| `signing-key-skew` | Included: harmless key IDs differ; the scenario does not expose or compromise credentials. |
| `response-schema-skew` | Included: caller and dependency response-version settings disagree; bounded checkout traffic exercises the mismatch while Inventory remains fast. |
| `metrics-service-label-drift` | Included: target discovery is lost while the selected application Pods remain healthy. |
| `mysql-exporter-scrape-path` | Included as a runner-only case because real Prometheus screenshots and independent rollback are required. |

Audit also found a real controller defect: non-config action paths referenced an
uninitialized settings variable. The baseline settings are now defined for every
action, with regression coverage. ConfigMap recovery failure no longer skips
independent Service-label recovery. Owned requests prevent cleanup from resetting
a different operator's active run. These are controller changes, not fault copies.

## Coordination And Runtime

The deployment maintainer owns publishing, targeted deployment and live evaluation. Do not run
`run`, `attach`, `reassess` or `history` before coordination. They require `--execute`.
`plan`, `preflight`, `retain-prior` and `history-status` are read-only against the cluster and product
(local evidence files are written). No command changes provider settings or invokes
provider validation. No automatic retry of an injection, upload or model request.

Pass the agreed hosting node explicitly with **`--lab-node NODE`** and preserve its
exporter source and current scrape settings. The runner checks every scheduled Lab
Pod, real MemAvailable and measurement freshness on that node. Do not use another
node's headroom as a proxy. Admission requires 1 GiB; during fault sampling the floor is 768 MiB.
Healthy services and pod readiness are required before injection and after recovery.
During an owned fault, degraded application health is recorded as evidence, not
treated as node pressure. Ownership, placement, fresh measured memory and the
768 MiB safety floor remain enforced throughout the fault.
The Lab controller and workload leases remain independent safeguards. The traffic
generator stays at zero replicas by default. For `timeout-budget` and
`response-schema-skew` only, the runner
preflights its read/scale permissions, stopped replica/readiness state and 25 RPS / 12
in-flight profile, then scales to one replica only after the controller acknowledges
the owned scenario. Its `finally`/recovery path restores the observed prior replica
count with UID, generation, replica and resource-version guards. This does not run for
worker, discovery, MySQL connection, or other scenarios. The observed 25 RPS
`timeout-budget` co-alerts can shift diagnosis away from the intended timeout; prefer
`response-schema-skew` for the configuration demo because its Inventory path stays
fast. No other workload is scaled, and no OOM cases, CPU stress or shared-source outage
is introduced.

The generic `deploy_kubernetes.py` applies all manifests, which can replace local
placement or scrape customization. For this change a maintainer-coordinated update of
**only lab-control's existing source revision** is sufficient; preserve the rest of
its current Pod template. Publish the tested commit first. This guide intentionally
does not run or prescribe an unreviewed cluster mutation.

Use Windows Python for the recorded run on this machine: kubectl is dispatched to
WSL, while Node runs natively. Playwright's installed Chromium is the default; a
system Chrome executable is an optional override. The runner itself needs only the
standard library. WSL has no Node by default; Linux runs need local Node/Playwright.

```powershell
Set-Location \\wsl.localhost\Ubuntu\home\hiio\uol\cm3070\fcapsule-lab
$python = 'C:/Users/Hiros/.cache/codex-runtimes/codex-primary-runtime/dependencies/python/python.exe'
$env:PLAYWRIGHT_MODULE = 'C:/Users/Hiros/.cache/codex-runtimes/codex-primary-runtime/dependencies/node/node_modules/playwright'
# Optional system-browser override; normally leave unset to use Playwright Chromium.
# $env:CHROME_EXECUTABLE = 'C:/Program Files/Google/Chrome/Application/chrome.exe'
$labNode = '<agreed-lab-node>'
& $python tools/run_operator_demos.py plan
& $python tools/run_operator_demos.py preflight --lab-node $labNode --out local_reports/demo-preflight-UNIQUE
```

Default endpoints are Lab `30766`, Prometheus `30090`, FCAPSule HTTP `30765`.
`--fcapsule https://HOST:30767` is supported when the certificate is trusted by the
client. The runner does not disable TLS verification. Origins are stored in each
run and used by later commands. An old controller without `owned_runs` fails closed
at preflight; deploy the tested controller before proceeding.

## Recorded Run

Choose a new output directory for every attempt. Existing directories are never
overwritten, including after a timeout or ambiguous response.

```powershell
& $python tools/run_operator_demos.py run --case all --lab-node $labNode --execute --out local_reports/demo-UNIQUE
# Or one case at a time:
& $python tools/run_operator_demos.py run --case connection-pressure --lab-node $labNode --execute --out local_reports/connections-UNIQUE
```

Each workload occurrence waits for a quiet alert baseline, records 45 seconds of
health, injects one owned lease, waits for the specific fresh symptom plus 30 seconds
of evidence, recovers, confirms baseline ConfigMap/Service/monitor and node safety,
retains bounded logs and real Prometheus range results, and reads the automatic
assessment. A run accepts a terminal assessment only when its actual
`context.alerts` includes that run's exact incident ID; primary-incident metadata
or a mention in prose is not enough. Stale terminal revisions remain in
`raw-assessments/` while the runner waits within its existing timeout. A timeout
does not select a stale baseline or start another model request.
It never forces a new baseline model call to obtain a better answer.
Missing/ambiguous fresh signals, old delayed captures, missing reports, incomplete
assessments, and changed models remain explicit failures or limitations.

The exporter case composes the mature `evaluate_external_screenshot.py` run mode:
actual before/fault/after Targets captures, narrow owned JSON patch, saved monitor
baseline, temporary namespaced alert rule, independent local watchdog and guarded
restoration. The watchdog cannot survive loss of the operator machine.

```powershell
# Emergency external-probe rollback, using that exact probe's saved owner/baseline:
& $python tools/evaluate_external_screenshot.py restore --out local_reports/demo-UNIQUE/exporter-scrape/round-1
```

Workload recovery is also available through Lab **Recover all**. During a recorded
run avoid concurrent manual starts. The runner's cleanup uses `expected_run_id` and
refuses to touch another operator's active run; the controller retries recovery
while its active lease is marked recovering.

## Two Distinct History Episodes

The currently audited product uses `store.EPISODE_JOIN_MINUTES = 15`, based on
incident start times of *every member*, not only episode start or alert resolution.
It is not an exposed setting. The default repeat waits 960 seconds from the latest
same-application signal and also waits for no firing Lab alerts. Set a larger
`--episode-quiet-seconds` if the deployed policy changes. The wait is capped at
1200 seconds by default. The runner still checks actual distinct episode IDs and
fails rather than splitting or relabeling records.

To avoid a second new fault and a long grouping wait, reuse an actual resolved prior
query-failure incident. Select the full ID from FCAPSule; do not invent it:

```powershell
& $python tools/run_operator_demos.py retain-prior --lab-node $labNode --incident-id ACTUAL-INCIDENT-ID --out local_reports/prior-query-UNIQUE
& $python tools/run_operator_demos.py run --case all --lab-node $labNode --previous-round local_reports/prior-query-UNIQUE --execute --out local_reports/demo-UNIQUE
```

`retain-prior` saves the actual report, capsule, assessment and IDs with a disclosure
that a historical matching symptom was reused and its original intervention was
not independently repeated. Original records are not modified. A prior of the
wrong model, unresolved signal, absent capsule or wrong origin is rejected.
The earlier record is referenced by path, not rewritten as a new occurrence.
Only this read-only import permits a terminal assessment without the selected
old member in `context.alerts`. It records
`assessment_context_policy: retained_prior_read_only` and the actual
`assessment_context_contains_incident` result. A missing member marks
`comparison_valid: false`: the capsule can demonstrate retained history, but the
assessment is not a fresh diagnostic comparison for that incident.
`assessment_matches_incident` continues to describe primary-incident equality,
which is distinct from actual context membership.

The full `--case all` run performs the retained review automatically after both
genuinely separate episodes are captured. These standalone commands remain useful
when those two rounds were run separately:

```powershell
& $python tools/run_operator_demos.py history --case-dir local_reports/demo-UNIQUE/query-rollout-history --execute
```

This retrieves the **earlier** capsule again, verifies its content hash, and submits
one neutral question through `/source-review` on the earlier episode. The product
omits live-source clients from that retained-only review. This demonstrates
review-isolated source unavailability, **not** an actual Prometheus/OpenSearch
outage. The second episode's raw investigation is saved separately so automatic
history lookup can be audited. Operator retrieval or a ready retained-only answer
does not prove automatic historical reuse. Missing history context remains a gap.

The accepted review is polled by exact review and episode ID from
`GET /api/incidents/{earlier_incident_id}/report.source_disconnected_reviews`.
The episode investigation endpoint does not expose that list. Completion comes
from the review's top-level `status`, not `result.status`; `result.sufficiency`
describes the retained answer separately. A terminal failure remains a failure.

After an accepted request times out locally, reconcile its saved
`history-review/review-request.json` without another POST or provider request:

```powershell
& $python tools/run_operator_demos.py history-status --case-dir local_reports/demo-UNIQUE/query-rollout-history
```

No `--execute` is required. This validates the saved attempt, round identities and
previously retrieved capsule hash, then reads the original review and current
recurrence investigation. It writes a fresh `history-review/status/UNIQUE/`
directory and prints its path. The original attempt, accepted response and any
earlier results remain unchanged. Raw polled review snapshots are retained.
`evaluation.json` records terminal `status`, `completed`, review ID, completion
time, answer sufficiency and usage; completion is not a diagnostic success claim.
Absent/mismatched accepted IDs fail closed. Missing reviews time out without a
replacement review or automatic retry. Capsule validation here checks the original
retrieved snapshot, not a fresh capsule download.

## Images And Explicit Reassessments

Visually inspect the fault PNGs for identity, time window, graph series or target
state/error, legibility and unrelated private content. The capture helpers save
original pixels, URL, observation time, viewport, query/pool and SHA-256. Hash and
origin checks are provenance safeguards, not proof that the image is useful.

```powershell
& $python tools/run_operator_demos.py attach --case-dir local_reports/demo-UNIQUE/connection-pressure/round-1 --pixels-reviewed --execute
& $python tools/run_operator_demos.py attach --case-dir local_reports/demo-UNIQUE/exporter-scrape/round-1 --pixels-reviewed --execute
```

Each attachment permits one extraction and one explicit evidence reassessment.
The payload contains pixels and a neutral source/time note, never scenario names,
injection parameters, diagnoses, rubric, expected answer or evaluator commentary.
By default, an existing attachment or changed baseline revision is rejected.
Hash mismatches and changed Pro budgets are always rejected. The current terminal
assessment must contain this run's incident in `context.alerts`.
Attempt directories are written **before** the paid POST.
Keep them after a network error; inspect server state rather than delete and retry.

### Sequential Evidence In One Episode

Different real incidents can naturally share an episode. Do not split or relabel
them, or inject another fault to obtain an isolated baseline. After inspecting the
current assessment and existing attachments, explicitly authorize incremental
evidence with its exact current revision:

```powershell
& $python tools/run_operator_demos.py attach --case-dir local_reports/demo-UNIQUE/exporter-scrape/round-1 --incremental-evidence --expected-revision ACTUAL-CURRENT-REVISION-ID --pixels-reviewed --execute
& $python tools/run_operator_demos.py attach --case-dir local_reports/demo-UNIQUE/connection-pressure/round-1 --incremental-evidence --expected-revision ACTUAL-REVISION-AFTER-FIRST-ATTACH --pixels-reviewed --execute
```

The flag defaults to false, applies only to `attach`, and requires
`--expected-revision`. Run these separately: inspect the first result and read the
actual current revision before the second command. The revision must be terminal
and its actual `context.alerts` must include the incident from that command's
round. Passing `--expected-revision` without the flag does not relax the default
baseline/evidence guards. Each round still permits only one attachment attempt.

This is **incremental evidence, not isolated image ablation**. The round's original
`investigation-before.json` is never rewritten. Its new `media-review/` contains:

- `existing-evidence.json`: full existing attachment records, including IDs,
  kinds, hashes, statuses, extractions and corrections returned by the product.
- `baseline-provenance.json`: original/current revision and policy, incident
  membership, existing attachment identity summary and capture hash.
- `investigation-original.json` and `investigation-before.json`: the original
  baseline and the actual intervening assessment used for this incremental step.
- `capture.json`: the validated screenshot's original observation metadata.
- `investigation-pre-update.json` and `evidence-pre-update.json`: state checked
  again after extraction, before the explicit reassessment.

An intervening revision, missing incident, or changed evidence inventory stops
the reassessment, retaining the attachment and all local attempt records. Nothing
is automatically reuploaded or retried. Evaluation records explicitly label the
incremental comparison; existing evidence can influence either assessment.

After a product fix, an explicit named follow-up reuses unchanged evidence:

```powershell
& $python tools/run_operator_demos.py reassess --case-dir local_reports/demo-UNIQUE/exporter-scrape/round-1 --label POSTFIX-NAME --expected-revision ACTUAL-REVISION-ID --execute
```

Use a lowercase label. The follow-up stores its own before/after assessments and
revision linkage, with no second image upload. Compare policy/version, calls,
`visible_evidence_ids`, exact citations and actual token usage. A stored image does
not imply the model saw it; a citation does not imply diagnostic correctness.
Post-recovery source observations can change, so this is not an image-only ablation.
The `reassess` command retains its single-attachment guard; incremental attachment
does not relax that separate workflow.

## Review And Limits

Private records stay under ignored `local_reports/` (legacy probes use ignored
`artifacts/`). Never commit images, runtime telemetry or full assessments. The
runner saves baseline/fault/after Kubernetes and PM state, interventions, safety
samples, recovery, actual alerts, bounded log tails, PM range responses, capsule,
report, automatic assessment snapshots, attachment extraction and explicit revision
attempts. A log tail is not the total indexed volume. Recovery assertions are
operational checks, not root-cause scoring.

Manually review each case against the independent scenario contract and raw
observations: mechanism distinguished from plausible alternatives; logs/PM/config
used appropriately; screenshot adds actual information; safe actionable next check;
historical versus current facts and missing evidence stated honestly. Preserve
incorrect and incomplete results. `ready`, `captured`, extracted text and image
citations are pipeline states only. Sum usage across all automatic revisions,
explicit follow-ups and vision calls, not only the last successful answer.

Configured Pro limits are frozen at preflight and checked before each case and
paid action, with ceilings 3600 completion, 12000 total, 3200 prompt tokens and one
check. The runner never increases them. The product can independently initiate
more than one automatic member revision; this harness cannot enforce a global
provider spend cap. Review raw revision histories and stop manually if spending
limits require it. A completed run of all 17 unique scenarios and the history exercise still needs human review of
the unedited assessments and raw evidence; rubric scores are an auditable triage aid,
not proof of correctness or source-outage resilience.

The 3200 per-call prompt ceiling leaves more room for the bounded two-image
evidence ledger, without guaranteeing all evidence fits. Lower configured budgets
remain valid. The frozen configuration must still match exactly.
A prior 2100-prompt run cannot be attached or reassessed under 3200
as a same-budget comparison. Budget changes require a separately recorded run or
explicitly budget-changed follow-up, with the difference disclosed. Do not rewrite
the original `run.json` or its frozen `model_config` to bypass this guard.

## Local Verification

```text
python -m pip install -e .[dev]
python -m unittest discover -s tests
node --check app/control.js
node --check tools/capture_demo.cjs
node --check tools/capture_prometheus.cjs
node --test tests/ui_control.test.cjs
```

The controller UI can be previewed against real read-only Lab status using
`python tools/preview_control.py`; all POST requests are blocked by the local
preview. Screenshots of this preview are UI QA only, never demo evidence.
