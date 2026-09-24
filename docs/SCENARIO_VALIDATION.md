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
| Logs | `transaction-deadlock` | Qualified | A fresh 25 September run created MySQL 1213 deadlock victims from opposite SKU lock orders, produced the exact alert in a new episode, and recovered. After a general Product validation fix, one incident-pinned retained-capsule reassessment identified the affected Inventory pod, reversed SKU ordering, rollback, and a consistent-lock-order action. The frozen rubric scored the ready revision 100; human review confirmed the cited logs and metric support the mechanism. | The first automatic assessment was inconclusive because an optional uncited historical abstention invalidated otherwise grounded current findings. A second same-name alert also shifted the default episode primary; the qualified revision is explicitly pinned to the first incident. Show that revision or verify default-primary stability before using the automatic episode view in a demo. |
| Performance metrics | `cpu-saturation` | Qualified | FCAPSule links the CPU alert to a credential-migration batch using PBKDF2 with 600,000 rounds, supported by progress logs and CPU samples. The fault and recovery were isolated. | The current next action asks to read logs already checked. Verify the improved synthesis before presenting it as the final recommendation. |
| Configuration | `response-schema-skew` | Qualified | An isolated run alerted and recovered. The retained Orders logs showed Inventory returning schema v1 while Orders expected v2, with 502 contract rejection. A completed, incident-pinned reassessment identified both versions, the affected Orders pod, and the missing `reservation.status` field; it recommended checking the Inventory deployment/configuration for rollback or absent v2 fields. | The first assessment gave a redundant next check. A later related latency alert shifted the episode's default primary incident, so the useful reassessment explicitly selected the schema incident. Verify primary-incident stability before relying on automatic episode presentation. |
| Monitoring discovery | `metrics-service-label-drift` | Qualified | A fresh `LabApplicationMetricsDiscoveryMissing` alert was isolated while Orders remained Ready, then the Lab restored the Service label. The completed assessment identified `lab-app-metrics` label `fcapsule.io/app-metrics=ture` versus the ServiceMonitor's required `true`, explained why the target was dropped, and recommended the exact correction. | The observed label snapshot was 33 seconds after alert onset, not a historical Service snapshot. The answer says so. It did not explicitly cite the healthy-pod check in its short explanation, although that check was retained. |

"Qualified" means the mechanism is useful for a demo; it does not mean every
sentence or suggested action is already ideal. Do not substitute an old episode
or a manually supplied answer for a fresh result.

## Deferred scenario backlog

| Domain | Scenario | Current evidence or blocker | Next validation step |
| --- | --- | --- | --- |
| Logs | `response-contract` | Older run shared an Orders episode with later faults. | Run in its own quiet window with owned traffic; verify the accepted HTTP response and rejected document contract are both retained. |
| Logs | `reservation-token-collision` | Older attribution was contaminated by another Inventory episode. | Re-run with request traffic and check the uniqueness-token evidence, not just the constraint alert. |
| Logs | `idempotency-conflict` | Current isolated attempt produced no alert because the traffic generator was at zero replicas. | Give the runner bounded owned traffic, then verify the same key was used with two different orders. |
| Metrics | `memory-leak` | A fresh run on 25 September completed and recovered. After a generic evidence-compaction fix, a retained reassessment used the numeric warning but incorrectly claimed the 100,504,940-byte buffer exceeded its 100,663,296-byte bound; the value was actually below it. The action remained generic. | Preserve the failed result. Improve numeric comparison and synthesis generally, then verify a bounded or releasing-pages action before promotion; do not weaken the expected diagnosis. |
| Metrics | `mysql-connections` | The isolated run recovered and a single retained-capsule reassessment completed on 25 September without a new database fault. It now cites 34 checked-out sessions, 36 server connections, and the 40-connection ceiling, with inventory reconciliation activity. However, it does not establish that sessions remained checked out instead of being released, and its next action asks to review pool usage already captured rather than test ownership or release behavior. The frozen rubric still scores it 23.3 (`weak_or_misdirected`); it is not a qualified demo. | Improve the general investigation's use of session-lifecycle evidence and action synthesis. Require a cited retention/release distinction and a concrete pool-owner or session-release check before promotion. Do not rerun the database fault merely to improve a score. |
| Metrics | `lock-contention` | Older alert/recovery did not yield a clean current-source assessment. | Re-run only after the demo track is complete; correlate lock-owner evidence with waiters and distinguish it from admission or DNS latency. |
| Metrics | `downstream-latency` | Older Orders episode was reused by other cases. | Run with owned traffic and confirm a fresh latency alert plus inventory timing, then review the diagnosis. |
| Configuration | `schema-drift` | Older episode was reused with connection pressure. | Run in its own Inventory window; verify unknown-column logs and fault-time query revision. |
| Configuration | `dependency-route` | Older Orders episode was reused. | Validate the active endpoint against the Service port and distinguish misrouting from an unavailable pod. |
| Configuration | `signing-key-skew` | Current isolated run completed, but the assessment only explained the caller-side 401 to 503 path. The decisive key IDs were in Inventory logs outside the Orders pod-scoped evidence. A later configuration read reflected recovery state. | Add a bounded same-namespace dependency-evidence follow-up and preserve the distinction between incident-time and current configuration before promoting this case. |
| Configuration | `timeout-budget` | Current run captured the timeout alert and recovered, but 25 requests/second also caused Inventory admission alerts. A correlated latency incident became the episode's primary assessment, which did not identify the 50 ms versus approximately 250 ms budget mismatch. | Revisit with a lower traffic rate and verify the episode still focuses on the timeout mechanism. Do not score the current assessment as a timeout diagnosis. |
| Discovery | `mysql-exporter-scrape-path` | Two fresh 25 September runs captured real Prometheus Targets screenshots: the discovered exporter changed from UP at `/metrics` to DOWN at `/metrics-v2` with HTTP 404, then recovered to UP. The first investigation misdiagnosed a dropped target in another pool and lost its exact primary incident. After a general Product target-scope fix, the second isolated run reached a ready, incident-pinned assessment that correctly named `/metrics-v2`, HTTP 404, a Ready pod, and a path/configuration check. Its external screenshot was delivered to the vision model and cited in a ready reassessment. However, that reassessment also cited an unrelated ServiceMonitor's dropped target as supporting evidence; the optional consistency review was unavailable. The fault-time Q002 check saw DOWN `/metrics-v2`, but evidence-added reassessment queried again after recovery and saw UP `/metrics`; it did not reuse the earlier timestamped check. This remains a partially helpful, unqualified demo, not a clean image-assisted diagnosis. | Make earlier fault-time checks available as explicitly timestamped retained observations during evidence-added reassessment, distinct from fresh post-recovery checks. Then scope compact monitor selections to the exact affected pool without deleting unrelated raw capsule data. Verify image-assisted synthesis and correct primary incident before promotion. Do not repeat the fault merely to improve a score. |
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

## Known issues and follow-up

- Episode correlation may choose a newer related alert as the primary incident,
  even when the operator opened an earlier, more diagnostic alert. Preserve
  incident-specific links and make the chosen assessment explicit in the UI.
- `response-schema-skew` requires bounded request traffic. Its qualification used
  the recorded runner, which starts and restores traffic it owns. The Lab UI can
  inject the configuration skew, but with the default zero-replica generator its
  Start button alone does not guarantee an alert or assessment.
- The logs and metrics demo cases were qualified before the latest evidence and
  next-action synthesis changes. Their mechanisms are valid, but their final
  recommendations still need one read-only review of the retained revisions.
- The discovery diagnosis passed human review on the fresh 25 September run, but
  its lexical evaluator still scores it weakly. An audit fixed a general bug
  that missed domains in cited completed checks. The remaining gap includes a
  real omission of the healthy workload in the short answer, plus phrasing
  differences between the supported hypothesis and the scoring contract. Do
  not use the score alone as evidence of model quality or tune the oracle to
  make this single response pass.
- No backlog case is a claimed pass. Some need isolated traffic, dependency
  evidence, or a clearer incident-time configuration snapshot. The table above
  identifies the specific next step for each one.

The qualified set covers logs, performance metrics, configuration, and monitoring
discovery. The remaining cases are retained as
regression and evaluation work, not presented as finished demos.
