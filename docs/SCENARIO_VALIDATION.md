# Scenario validation and demo track

This document records what has actually been observed, not what the scenario
catalog promises to produce. The catalog remains available for development;
only cases with an isolated alert, a recovered Lab, a completed FCAPSule
assessment, and a useful explanation beyond the alert belong in a live demo.
Scores are screening aids, not a substitute for reviewing the report and its
citations. Private run artifacts stay under ignored `local_reports/`.

## Current demo track

| Domain | Scenario | Status | What the operator can show | Remaining polish |
| --- | --- | --- | --- | --- |
| Logs | `poison-job` | Qualified | FCAPSule links decoder rejection of non-base64 import data to repeated job retries, with cited retained logs. The fault and recovery were isolated. | A newer evidence-selection change preserves redelivery fields; re-assess once to confirm the final explanation and next action explicitly cover the unacknowledged job. |
| Performance metrics | `cpu-saturation` | Qualified | FCAPSule links the CPU alert to a credential-migration batch using PBKDF2 with 600,000 rounds, supported by progress logs and CPU samples. The fault and recovery were isolated. | The current next action asks to read logs already checked. Verify the improved synthesis before presenting it as the final recommendation. |
| Configuration | `timeout-budget` | Pending live validation | The intended demonstration compares the caller's 50 ms timeout with the dependency's approximately 250 ms processing time, using logs, metrics, and the active ConfigMap. | Run with bounded request traffic, verify a fresh episode and recovery, then review whether the assessment actually states the mismatch and a useful remedy. |

"Qualified" means the mechanism is useful for a demo; it does not mean every
sentence or suggested action is already ideal. Do not substitute an old episode
or a manually supplied answer for a fresh result.

## Deferred scenario backlog

| Domain | Scenario | Current evidence or blocker | Next validation step |
| --- | --- | --- | --- |
| Logs | `response-contract` | Older run shared an Orders episode with later faults. | Run in its own quiet window with owned traffic; verify the accepted HTTP response and rejected document contract are both retained. |
| Logs | `reservation-token-collision` | Older attribution was contaminated by another Inventory episode. | Re-run with request traffic and check the uniqueness-token evidence, not just the constraint alert. |
| Logs | `transaction-deadlock` | Older run saw an alert and recovered but did not finish an isolated assessment. | Re-run after the Inventory quiet period; accept only an episode with the deadlock alert as its primary or explicitly accepted co-firing alert. |
| Logs | `idempotency-conflict` | Current isolated attempt produced no alert because the traffic generator was at zero replicas. | Give the runner bounded owned traffic, then verify the same key was used with two different orders. |
| Metrics | `memory-leak` | Current run completed, but the assessment did not distinguish the 96 MiB application buffer bound from the 160 MiB container limit and suggested re-reading collected metrics. | Re-assess after generic synthesis improvements; retain the sampled values and recommend bounding or releasing export pages. |
| Metrics | `mysql-connections` | Current run completed, but the assessment repeated high connections without using the decisive 34 active of 40 maximum sample. | Verify numeric peak evidence survives log grouping and context compaction; no further database fault run is required for the initial demo. |
| Metrics | `lock-contention` | Older alert/recovery did not yield a clean current-source assessment. | Re-run only after the demo track is complete; correlate lock-owner evidence with waiters and distinguish it from admission or DNS latency. |
| Metrics | `downstream-latency` | Older Orders episode was reused by other cases. | Run with owned traffic and confirm a fresh latency alert plus inventory timing, then review the diagnosis. |
| Configuration | `schema-drift` | Older episode was reused with connection pressure. | Run in its own Inventory window; verify unknown-column logs and fault-time query revision. |
| Configuration | `dependency-route` | Older Orders episode was reused. | Validate the active endpoint against the Service port and distinguish misrouting from an unavailable pod. |
| Configuration | `signing-key-skew` | Current isolated run completed, but the assessment only explained the caller-side 401 to 503 path. The decisive key IDs were in Inventory logs outside the Orders pod-scoped evidence. A later configuration read reflected recovery state. | Add a bounded same-namespace dependency-evidence follow-up and preserve the distinction between incident-time and current configuration before promoting this case. |
| Configuration | `response-schema-skew` | No isolated current-source assessment. | Run with owned traffic; verify expected and observed schema versions and the dependency's successful HTTP response are retained together. |
| Discovery | `metrics-service-label-drift` | No clean current-source run. | Confirm the application remains available while its Prometheus target disappears and validate the exact Service-label mismatch. |
| Discovery | `mysql-exporter-scrape-path` | Current run captured and recovered but was inconclusive: scrape discovery returned an oversized response and the external screenshot runner lacked a usable browser runtime. Bounded scrape-pool querying and Playwright Chromium capture are now implemented. | Re-run the fault and external screenshot together, then verify the target/path evidence reaches a completed assessment. |
| Retained history | `query-rollout-history` | The older suite did not finish both isolated rounds. | Complete two separate query-revision incidents with a quiet gap; verify the second investigation cites the first retained capsule without relying on expired source data. |

## Validation rules for resuming the backlog

1. Check node headroom, source readiness, an empty alert baseline, and the
   configured AI provider before mutating the Lab.
2. Use one fault at a time. Request-driven cases need traffic owned by the
   runner; a zero-replica ambient generator is not evidence of a broken alert.
3. Require a fresh matching primary incident and verify the episode isolation
   record before scoring an assessment. Older shared episodes are not passes.
4. Wait for fault recovery and a stable assessment revision. Review the
   explanation, cited evidence, uncertainty, and next action as an operator.
5. Record source revision, deployment revision, run artifacts, and any
   unavailable source. Keep evaluator answers outside FCAPSule input and do not
   alter a scenario merely to match a scoring phrase.

The first pass is intentionally limited to one strong case each for logs,
performance metrics, and configuration. The remaining cases are retained as
regression and evaluation work, not presented as finished demos.
