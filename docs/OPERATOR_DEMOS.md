# Five Operator Demos

This is a practical demonstration suite, separate from the fifteen-case benchmark.
It reuses three existing workload mechanisms and the two existing monitoring probes.
It does not add five duplicate faults, change the independent benchmark rubric,
change FCAPSule providers, or insert expected diagnoses into evidence notes.

## Recommended Cases

| Selection | Existing mechanism | Useful evidence | Bounded fault |
|---|---|---|---|
| `connection-pressure` | `mysql-connections` | Session ownership/errors in logs; real sessions versus the 40-connection ceiling; MySQL configuration; actual Prometheus graph | 180 s |
| `checkout-deadline` | `timeout-budget` | Caller timeouts versus dependency timing and active 50 ms timeout; retries and checkout PM | 180 s |
| `metrics-discovery` | `metrics-service-label-drift` | ServiceMonitor selector, actual Service label, missing `up`, still-ready Pods and continued application logs | 180 s |
| `exporter-scrape` | Existing external scrape-path probe | Discovered exporter DOWN/404 in actual Prometheus Targets; retained monitor configuration and independent workload health | 180 s, independent rollback at 240 s |
| `query-rollout-history` | `schema-drift`, repeated or using a retained prior | SQL 1054, retained query revision, error PM; distinct episodes and earlier capsule retrieval; explicit retained-only question | 150 s per new occurrence |

The graph and Targets captures are the two required external images. The graph
selects both metric names directly: a PromQL `or` between identically labeled gauges
would drop one series. These screenshots must be captured during their respective
faults. Healthy preflight images, Lab control screenshots, FCAPSule UI images,
generated charts and rendered evaluator pages cannot substitute for incident evidence.

The Lab selector starts only individual leased occurrences. Its history action says
**Start one occurrence**, not "history passed". The exporter entry has no Start
button: **External screenshot runner** links to an executable run plan, because the
CLI probe owns its saved rollback record and watchdog. No RBAC expansion is needed.

## Audit Of Existing Cases

| Existing case | Demo decision |
|---|---|
| Poison job | Real durable redelivery; omit from the short suite because CrashLoopBackOff can delay recovery several minutes. |
| Response contract | Real consumer rejection of HTTP 200 document shape; useful log benchmark, less complementary here. |
| Reservation token collision | Real SQL uniqueness errors; keep in log benchmark. |
| Transaction deadlock | Real opposing lock order; keep in benchmark rather than add contention alongside session saturation. |
| Idempotency conflict | Real conflicting request payloads; keep in log benchmark. |
| Memory leak | Real buffered pages/cgroup OOM; omit from low-impact demo, not needed to show PM. |
| CPU saturation | Real PBKDF2 workload; omit to avoid unnecessary node CPU load. |
| MySQL connections | Selected; limited to the existing 40-session server, with leases and headroom checks. |
| Lock contention | Real row-lock blocker; useful alternate, overlaps database contention narrative. |
| Downstream latency | Real delayed work; timeout-budget case additionally demonstrates configuration discrimination. |
| Schema drift | Selected for repeat/history; real query revision before schema migration, no synthetic log errors. |
| Dependency route | Real unused Service port; keep as an alternate configuration case. |
| Timeout budget | Selected; small fixed dependency work, caller timeout mismatch, bounded retries. |
| Signing key skew | Real request contract skew; no real credential compromise is simulated. |
| Response schema skew | Real version configuration mismatch; omit alongside query and timeout configuration cases. |
| Discovery probe | Selected outside benchmark; missing target is not proof of an application outage. |
| External scrape probe | Selected outside benchmark; target remains discovered and fails scraping, distinct from discovery absence. |

Audit also found a real controller defect: non-config action paths referenced an
uninitialized settings variable. The baseline settings are now defined for every
action, with regression coverage. ConfigMap recovery failure no longer skips
independent Service-label recovery. Owned requests prevent cleanup from resetting
a different operator's active run. These are controller changes, not fault copies.

## Coordination And Runtime

The deployment maintainer owns publishing, targeted deployment and live evaluation. Do not run
`run`, `attach`, `reassess` or `history` before coordination. They require `--execute`.
`plan`, `preflight` and `retain-prior` are read-only against the cluster and product
(local evidence files are written). No command changes provider settings or invokes
provider validation. No automatic retry of an injection, upload or model request.

Pass the agreed hosting node explicitly with **`--lab-node NODE`** and preserve its
exporter source and current scrape settings. The runner checks every scheduled Lab
Pod, real MemAvailable and measurement freshness on that node. Do not use another
node's headroom as a proxy. Admission requires 1 GiB; during fault sampling the floor is 768 MiB.
The Lab controller and workload leases remain independent safeguards. No new Pods,
OOM cases, CPU stress, traffic scaling or shared-source outage is introduced.

The generic `deploy_kubernetes.py` applies all manifests, which can replace local
placement or scrape customization. For this change a maintainer-coordinated update of
**only lab-control's existing source revision** is sufficient; preserve the rest of
its current Pod template. Publish the tested commit first. This guide intentionally
does not run or prescribe an unreviewed cluster mutation.

Use Windows Python for the recorded run on this machine: kubectl is dispatched to
WSL, while Chrome/Node run natively. The runner itself needs only the standard
library. WSL has no Node by default; Linux runs need local Node/Chrome/Playwright.

```powershell
Set-Location \\wsl.localhost\Ubuntu\home\hiio\uol\cm3070\fcapsule-lab
$python = 'C:/Users/Hiros/.cache/codex-runtimes/codex-primary-runtime/dependencies/python/python.exe'
$env:PLAYWRIGHT_MODULE = 'C:/Users/Hiros/.cache/codex-runtimes/codex-primary-runtime/dependencies/node/node_modules/playwright'
$env:CHROME_EXECUTABLE = 'C:/Program Files/Google/Chrome/Application/chrome.exe'
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
assessment. It never forces a new baseline model call to obtain a better answer.
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

After two genuinely separate captured episodes:

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
An existing attachment, changed baseline revision, hash mismatch or changed Pro
budget is rejected. Attempt directories are written **before** the paid POST.
Keep them after a network error; inspect server state rather than delete and retry.

After a product fix, an explicit named follow-up reuses unchanged evidence:

```powershell
& $python tools/run_operator_demos.py reassess --case-dir local_reports/demo-UNIQUE/exporter-scrape/round-1 --label POSTFIX-NAME --expected-revision ACTUAL-REVISION-ID --execute
```

Use a lowercase label. The follow-up stores its own before/after assessments and
revision linkage, with no second image upload. Compare policy/version, calls,
`visible_evidence_ids`, exact citations and actual token usage. A stored image does
not imply the model saw it; a citation does not imply diagnostic correctness.
Post-recovery source observations can change, so this is not an image-only ablation.

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
paid action, with ceilings 3600 completion, 12000 total, 2100 prompt tokens and one
check. The runner never increases them. The product can independently initiate
more than one automatic member revision; this harness cannot enforce a global
provider spend cap. Review raw revision histories and stop manually if spending
limits require it. No claims about five-case correctness or source-outage resilience
are warranted until the operator completes the real run and reviews the results.

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
