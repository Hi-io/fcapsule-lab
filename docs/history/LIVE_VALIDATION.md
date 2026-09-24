# Kubernetes Validation Record

> Historical six-case run. For today's qualified demo cases and remaining gaps, use [scenario validation](../SCENARIO_VALIDATION.md).

Historical six-case series complete. Follow-up checks are recorded separately below.
The current Lab has since expanded to the fifteen-case paired suite documented in
`SCENARIO_CONTRACT.md`; this file preserves the earlier run without rewriting it.

## Method

This historical evaluation used the six contracts frozen at commit `b8cb1c9` before
execution. It did not give those expected diagnoses to FCAPSule. The lab supplied
ordinary application logs, Prometheus measurements and neutral symptom alerts.
The product accesses them through its existing Kubernetes, OpenSearch and Prometheus
integrations and uses the configured DeepSeek Pro provider for investigation.

The original series began on 2026-09-20 UTC (2026-09-21 in the operator's timezone).
Workload behavior was pinned to lab commit `b8cb1c9`; FCAPSule remained at `2358262`
through the original series. Later corrections are evaluated separately. One run
at a time receives a 180-second lease, preceded by at least 120 seconds of healthy
baseline and followed by a wait for the relevant alert windows to clear.

Local evidence is under `artifacts/validation-20260920T164115Z/`. Each case retains
its intervention time, memory samples, observed alerts, bounded workload log files,
unedited agent responses and provider token usage. `tools/review_run.py` adds raw
historical metric series and a structured observation summary. These local artifacts
are ignored by Git. The checked-in record reports observations without credentials.

`ready` means the agent returned a structurally accepted assessment, not that its
diagnosis was correct. Episodes can contain several temporally nearby cases, so
these are not six statistically independent model trials. No numerical accuracy
score is claimed. The initial and revised answers are retained, not replaced to
hide unsuccessful reasoning.

## Resource and Volume Checks

- Actual kernel memory, read through node-exporter: 7,019,782,144 total bytes;
  initial MemAvailable 1,514,065,920 bytes. Kubernetes allocatable was not used
  as a free-memory measurement.
- No additional always-on pods were added. Existing MySQL, exporter, two APIs,
  worker, traffic generator and controller were reused.
- Admission requires 1 GiB available; the controller recovers below 768 MiB or
  if it cannot read the memory source. Worker memory remains capped at 160 MiB.
- Healthy emitted volume measured about 7,572 structured log events/minute.
  An independent OpenSearch count over a one-minute interval overlapping the
  schema failure returned 12,609 indexed records across the lab namespace.
- API log files reached the evaluator's 12,000-line per-workload tail bound in
  the contention case. Those retained tails are not the full indexed volume.
- Worker cases naturally have fewer worker-local records than API cases. No
  filler errors were added to inflate their context. Background API traffic
  continued to generate thousands of records per minute.
- Minimum sampled MemAvailable during the six fault leases was 1,158,017,024 bytes
  (about 1.08 GiB). This is a sampled minimum, not a continuous kernel measurement.

## Original Findings

| Case | Independent observations | Original agent review |
|---|---|---|
| Schema mismatch | Real SQL 1054; 5,661 examples in the retained inventory tail; query-failure and checkout alerts fired | Inventory assessment identified the missing column and suggested checking migration compatibility. Orders assessment stopped at dependency 503s and delegated available downstream investigation to the operator. |
| Transaction contention | Real blocker transactions, SQL 1205 and secondary 1040; 1,503 and 4,063 examples respectively in retained logs; contention alert fired | Detected locks and connection pressure, but mixed this phase with the earlier schema failure and did not establish the blocker. |
| Connection saturation | Independent exporter reached 40 sessions against max_connections=40; capacity alert fired | Recognized the ceiling, but described activity as transient and argued against a persistent leak using older baseline samples. Session ownership remained unknown. |
| Credential migration | Measured maximum 0.50023 CPU cores against a 0.5-core limit, with migration progress and no restart; CPU alert fired | Correctly linked PBKDF2 work to CPU use. Suggested reducing rounds, an unsafe performance shortcut for real credential security policy. |
| Incompatible import | Real `binascii.Error`, exit code 1 and CrashLoopBackOff alert; OpenSearch independently contained five deliveries of the same job at different timestamps | Correctly separated the earlier PBKDF2 CPU phase from the later unhandled decoder exception; suggested input validation/error handling. The source of the invalid job remained unknown. |
| Buffered export | Real OOMKilled termination at 17:17:50Z, exit 137; largest logged buffer 141,432,000 bytes; old OOM alert did not activate until 17:20:48Z | First captured attempt was incomplete after response-format/citation failures. Later attempts recognized buffered export and the earlier decoder failure, but confused alert time with termination order and incorrectly compared buffer bytes with the container limit. |

The contention case also temporarily prevented the MySQL exporter from connecting.
This is an observability limitation, not evidence that the database process stopped.
Application SQL failures and independent measurements must be interpreted together.

During backoff, `kubectl logs` and `kubectl logs --previous` returned the same last
terminated container log. The evaluator now deduplicates timestamped records before
counting deliveries. The five-delivery finding above came from a separate bounded
OpenSearch query over the worker and fault interval, not from counting those two
overlapping files twice.

## Product Corrections Prompted by the Cases

1. A new bounded dependency tool follows explicitly configured, same-namespace
   Kubernetes Services. It resolves selectors rather than accepting model URLs,
   samples one dependency pod, and retains logs, fixed metrics and current config.
   Secret references, arbitrary endpoints and cross-namespace targets are excluded.
2. `mysql_error_code` is preserved during log grouping, keeping schema, lock and
   connection errors diagnostically distinct.
3. Literal log searches reserve capacity after the latest member alert rather
   than filling their budget with the earliest matching errors in a long episode.
4. Metric results separate samples at/after the latest alert from earlier baseline
   and include MySQL exporter collection availability. Missing metrics are not zero.
5. Capture-window end is no longer supplied as the resolution time of an active
   alert. The model receives explicit limitations on temporal episode grouping.
6. Active queue entries prefer currently firing signals over older resolved critical
   signals. Historical evidence remains available.
7. Investigation and review instructions prohibit weakening security controls as
   a performance shortcut and require preservation before proposing business-data
   deletion. These remain model instructions, not a guarantee of safe advice.
8. Timing instructions distinguish alert detection from container termination;
   memory reasoning must use compatible units and distinguish a component buffer
   from total container memory. A below-limit buffer does not disprove an OOM.
9. A final draft with too many otherwise valid citations can use the already
   reserved consistency-review call to repair the response contract. Invalid drafts
   are never published by silently truncating references; the call budget is unchanged.

These are product changes, not changes to the frozen scenario outcomes. The tool
does not execute remediation, run SQL against applications or claim an established
root cause solely because a hypothesis has supporting evidence.

## OOM Alert Correction

The original `expected_alert_missing` outcome remains in the evidence. The workload
really exceeded its cgroup limit, but Kubernetes had not restarted it yet: it was
waiting in CrashLoopBackOff. The old expression required a restart counter increase,
so it missed this period and activated about 178 seconds after termination.

The revised rule accepts an OOM termination with either a recent restart or current
restart backoff. A stale OOM reason on a healthy, non-restarting container does not
qualify. Local promtool 3.14.0 validated all eight rules and four OOM fixtures:
backoff without a counter increase, recent OOM restart, stale healthy OOM state,
and an ordinary non-OOM crash. All passed.

A read-only replay at 2026-09-20T17:18:30Z queried the same retained real samples:
old expression returned zero series; revised expression returned the affected worker.
The original and revised expressions and responses are retained in
`memory-leak/oom-rule-replay.json`. This is not a new live fault or a replay of the
alert engine's pending timer. The corrected rule was subsequently applied and its
live Prometheus rule health was `ok`.

## Follow-Up Verification

FCAPSule `0dc3142` was deployed with investigation policy `episode-investigation-1.6`.
Only read-only Service access was added to the observer's Kubernetes permissions;
Secrets remain excluded. The source installer now has bounded memory and CPU.
The Lab controller was updated to `9b6be66`; the other Python workloads remain at
`b8cb1c9`, with unchanged fault behavior. The revised Prometheus rule is applied
separately. This avoids restarting unrelated workloads merely to update control UI.

Reassessment of the original orders episode completed in 118.1 seconds. The agent
actually invoked `dependency_evidence` for inventory-api, obtained downstream SQL
1040/1205 evidence, and separated the second failure phase from the first rather
than asserting that both shared one demonstrated cause. It still left the first
phase unresolved because the bounded follow-up query focused on the later alert.
That remains a limitation, not a successful diagnosis of the entire episode.
The unedited response is saved as `schema-drift/revised-orders-investigation.json`.

This is not a controlled model comparison: tool behavior changed, queries occurred
later, and Kubernetes configuration is a current snapshot, not historical proof.
The recorded original assessments are retained alongside the new answer.

### Fresh Schema Failure

A new 180-second fault lease ran from 17:37:36Z after a fresh healthy baseline,
with recovery recorded at 17:40:43Z. Artifacts are in
`artifacts/validation-20260920T173530Z/`. The mechanism and workload revision did
not change. Both expected service symptom alerts fired; both automatic assessments
completed. Minimum sampled host available memory was 1,308,008,448 bytes (1.22 GiB).
Bounded retained logs contained 5,780 SQL 1054 records.

| Investigation | Elapsed | Provider-reported tokens | Observed result |
|---|---:|---:|---|
| Inventory | 33.18 s | 26,367 | Identified missing reserved_quantity and distinguished it from connection/resource exhaustion. No direct database schema snapshot was available. |
| Orders | 39.58 s | 55,174 | Followed the declared inventory Service, retrieved downstream error 1054, and connected it to 503 responses and exhausted checkout retries. |

The orders agent then tried to search the dependency pod with the root-pod-only
log tool. The server rejected this scope, and the UI honestly showed that additional
check as unavailable. The successful dependency observation remained citable.
Tool descriptions now explicitly direct dependency follow-up to `dependency_evidence`
with the declared Service and terms; permissions were not broadened. This guidance
change is unit-tested with the existing scope checks, not proof that a model will
never choose the wrong tool again.

Visual checks confirmed the active scenario, countdown, disabled competing start
buttons, duration selection and recovery status in the Lab. In FCAPSule, the new
orders incident opened inline and its Dependency evidence citation opened the actual
downstream metrics, logs and configuration. Screenshots are retained under
`artifacts/ui-audit/`. No claim of a comprehensive mobile-browser audit is made.

### Quantitative Review Follow-Up

Worker reassessment under policy 1.6 still made the false comparison that a roughly
141 MB buffer exceeded a 160 MiB container limit. The unchanged response is retained
as `memory-leak/revised-worker-investigation.json`. Adding a general instruction
alone had not corrected this reasoning error.

Policy 1.7 therefore enables low reasoning effort in the already reserved final
review and explicitly requires consistent-unit comparisons, component-versus-total
memory interpretation, and termination-versus-alert timestamp checks. It does not
add another model call or increase the per-call completion limit.

A single review-only replay of the exact retained worker evidence and draft returned
a structurally valid correction: the buffer was separated from process overhead,
the sampled working set remained below the limit, and the actual peak was described
as unsampled. It also distinguished the decoder and export phases without asserting
a demonstrated causal link. The response used 40,386 reported tokens (36,875 input,
3,511 completion, including 2,584 reasoning tokens). Full response and usage are in
`memory-leak/reasoned-review-replay.json`.

This is one successful correction, not a measured accuracy improvement across a
benchmark. It is a review-only replay, not another end-to-end live OOM experiment.
The final product revision is `01c306d`, including the policy 1.7 review and clearer
tool-scope instructions. Original answers are never silently rewritten.

After deployment, a complete worker investigation under policy 1.7 also finished
successfully in 94.68 seconds, using 169,846 reported tokens (160,951 input and
8,895 completion across calls). It queried source logs and resource history,
distinguished the credential migration, decoder-crash and buffered-export phases,
and explicitly reported the sampled working set below the limit with an unsampled
actual peak. It did not repeat the false component-buffer comparison. The actual
UI displayed this result, its source citations, check activity and token count.
The unedited response is `memory-leak/final-live-worker-investigation.json`.
This is a new investigation of historical events through the live integrations,
not a second injected OOM. Its mitigation wording still needs operator judgment;
streaming/bounded buffering was proposed, not implemented or validated on a real
production export service.

## Node Headroom During Follow-Up

After the original series, available memory fell to roughly 750 MiB. The traffic
generator was paused. Historical working-set measurements showed Grafana growing
from about 232 MiB to 877 MiB; this observation does not establish why it grew.
No unrelated service was restarted without approval.

After explicit operator approval, Grafana received SIGTERM in its existing pod.
Its distroless image required a short-lived, unprivileged debug helper targeting
only that container's process namespace. The original pod UID and emptyDir data
volumes were preserved; deleting or rolling out that pod would have lost its local
storage. Restart count advanced from zero to one and readiness recovered. Available
memory subsequently reached about 1.30 GiB and baseline Lab traffic was resumed.
The helper exits automatically and does not become a permanent workload.

At final verification all seven Lab pods were ready, no scenario was active, MySQL
reported one connected session and the recent checkout-503 rate was zero. Baseline
traffic remained enabled. GO15 was Ready without memory/disk/PID pressure and had
about 1.33 GiB available. An older informational exporter CPU-throttling alert
remained visible; this is not a claim that the entire cluster had no firing alerts.

## Verification Boundaries

- Local tests: 92 FCAPSule Python tests, nine UI helper tests, 18 Lab tests and the
  Prometheus rule checks passed after these changes.
- A ready AI assessment is not proof of its explanation. Advice still requires
  operator review; the product does not execute remediation.
- Declared Services and their current selectors establish a bounded investigation
  scope, not a historical dependency graph or a causal relationship.
- Sampled resource series can miss short peaks; exporter failure is missing
  observation, not proof that MySQL stopped. Session ownership remains unproven.
- Terminology and causal confidence still need operator review: for example, one
  inventory answer called MySQL error number 1054 a SQLSTATE. Structural/citation
  validation cannot verify every technical statement in generated prose.
- Log tails, source query caps and retained templates are bounded evidence, not a
  complete record of every request. No GB-to-MB compression claim is made here.
