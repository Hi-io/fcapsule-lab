# Scenario Contract and Audit

This contract is the evaluator's oracle. It is stored in the standalone Lab and is
never sent to FCAPSule, embedded in alert annotations, or exposed by the Lab control
API. Application evidence remains realistic: SQL codes, request identities, timing,
resource measurements and active Kubernetes configuration are observable. The
operator catalog contains all 17 scenarios; the model-diagnosis benchmark remains
the fifteen workload cases below, with two additional monitoring-discovery probes.

## Evaluation Matrix

The benchmark contains fifteen independent workload mechanisms, balanced by the
evidence that should carry the diagnosis. The grouping is not an FCAPS classification.
It describes what an investigator must use to distinguish the root mechanism from
similar symptoms. The two discovery probes are also first-class operator demos, but
are kept separate from this legacy workload score because their target is monitoring
coverage, not an application failure.

| Group | Scenario | Decisive mechanism | Expected symptom alert |
|---|---|---|---|
| Logs | `poison-job` | Invalid Base64 import is rolled back before acknowledgement and retried by the still-running worker | `LabWorkerPoisonRetries` |
| Logs | `response-contract` | A successful dependency response has the wrong document shape | `LabOrdersDependencyDocumentInvalid` |
| Logs | `reservation-token-collision` | Different reservations reuse a unique database token | `LabInventoryConstraintFailures` |
| Logs | `transaction-deadlock` | Transactions acquire two stock rows in opposite order | `LabInventoryDeadlockVictims` |
| Logs | `idempotency-conflict` | One idempotency key is bound to the first order identifier; a later order using that key is rejected before inventory is called | `LabOrdersIdempotencyConflicts` |
| Logs + metrics | `memory-leak` | Buffered export pages grow above the measured 80 MiB warning threshold toward a 96 MiB application safety cap; OOM is not expected | `LabWorkerBufferPressure` |
| Logs + metrics | `cpu-saturation` | A finite credential-migration batch drives measured worker CPU use near its quota while progress is logged | `LabWorkerCPUHigh` |
| Logs + metrics | `mysql-connections` | Inventory-owned checked-out sessions exceed 80% of a fresh capacity sample; the exporter independently corroborates server usage | `LabMySQLConnectionsSaturated` |
| Logs + metrics | `lock-contention` | Reconciliation holds the stock row while reservations wait | `LabInventoryLockContention` |
| Logs + metrics | `downstream-latency` | Successful inventory processing adds 350 ms, raising checkout p95 without an expected retry or timeout | `LabCheckoutLatencyHigh` |
| Configuration | `schema-drift` | Query revision v2 references `reserved_quantity` while the actual table lacks the column; the controller checks this precondition before injection | `LabInventoryQueryFailures` |
| Configuration | `dependency-route` | `INVENTORY_URL` uses port 8099 while the Service uses 8081 | `LabOrdersDependencyTransportFailures` |
| Configuration | `timeout-budget` | A 50 ms caller timeout is below 250 ms dependency work | `LabOrdersDependencyTimeouts` |
| Configuration | `signing-key-skew` | Caller and dependency use different signing key IDs | `LabOrdersDependencyAuthorizationFailures` |
| Configuration | `response-schema-skew` | Orders expects response v2 while inventory emits v1 | `LabOrdersDependencySchemaRejected` |

Configuration cases update `ConfigMap/lab-scenario-config` through namespace-scoped
RBAC and apply the same values to the process control surface. FCAPSule therefore sees
the actual active Kubernetes object, not an evaluator note or a manufactured answer.
Recovery restores the baseline ConfigMap and process settings.

## Operational Discovery Probe

`metrics-service-label-drift` is intentionally outside the fifteen-case diagnostic
matrix and model score. It is an observability capability probe: the controller changes
`Service/lab-app-metrics` from `fcapsule.io/app-metrics=true` to the typo `ture` for a
bounded lease. The application `ServiceMonitor` selects Services, not Pods, using that
exact label. Its application Pods keep `fcapsule.io/metrics=true` and remain healthy.

After the prior `up` sample ages out, `LabApplicationMetricsDiscoveryMissing` fires.
Its `service` label remains the logical `orders-api` workload; `target_service`
identifies the shared Kubernetes `lab-app-metrics` Service whose selector is
relevant to the investigation, and `target_workload` identifies the affected
workload without pretending that the shared Service selects only one application.
The expected investigation path is bounded and independently observable:

| Evidence | What it establishes |
|---|---|
| Alert rule | The symptom is missing application metrics, not an asserted application failure. |
| Prometheus target state | The orders target is absent or no longer selected. |
| `ServiceMonitor` selector | Discovery requires `fcapsule.io/app-metrics=true` on a Service. |
| `Service/lab-app-metrics` metadata | The actual Service label is `ture`, which does not satisfy the selector. |
| Pod health and labels | Workload Pods remain ready and retain their unrelated metrics label. |
| Recovery | Restoring the Service label re-enables target discovery. |

An investigation may say that the label mismatch is observed and that it explains the
missing target. It must not infer a workload outage only because observability coverage
was lost. Allow at least 90 seconds for the one-minute absence window and 30-second
alert hold before judging the result.

## Execution Protocol

1. Verify real node `MemAvailable`; do not infer headroom from allocatable memory.
2. Hold healthy traffic before injecting exactly one leased scenario.
3. Record the intervention only in the ignored evaluator artifacts.
4. Wait for the scenario-specific symptom alert and retain a post-alert evidence window.
5. Recover the workload and wait for alert windows to clear.
6. Capture one FCAPSule episode and its immutable `input_fingerprint`.
7. Run each candidate model against that same episode; reject comparisons whose
   fingerprints differ.
8. Restore the original FCAPSule model setting even after interruption.

The healthy baseline schedules 25 checkouts per second and caps concurrency at 12.
The generator drops attempts when all slots are occupied instead of queueing them. A
successful checkout typically emits six structured events across the traffic, orders
and inventory services, or about 9,000 checkout-path events per minute at the configured
rate. Twelve is the runtime safety ceiling even if the ConfigMap requests a larger
limit; startup logs and metrics preserve the configured/effective difference. The
evaluator records observed counts and bounded retained lines rather than assuming that
configured throughput was achieved or that a local log tail equals total indexed volume.

## Scoring Contract

`evaluation/ground_truth.json` describes weighted causal findings, required evidence
domains, acceptable action concepts and scenario-specific contradictions. The scorer:

- awards 55 points for causal findings expressed through concept groups;
- awards 20 points only for required domains cited by the final assessment;
- awards 15 points for a relevant verification or remediation action;
- awards 10 points for explicit uncertainty and absence of known contradictions.

The action matcher rejects a matching phrase when it is explicitly negated. It still
uses transparent phrase groups rather than a semantic entailment model; disputed or
ambiguous matches require human review. CPU quota utilization is not evidence of CFS
throttling by itself, and the CPU rubric intentionally does not require a throttling
claim or recommend weakening password security.

Labels are `correct_and_actionable` (90-100), `substantially_correct` (70-89),
`partially_helpful` (45-69), `weak_or_misdirected` (1-44), and `failed` (0).
This is a transparent phrase-and-citation rubric, not semantic truth or an accuracy
certificate. It can miss paraphrases or accept ambiguous language; every score retains
matched criteria, citations and the unedited model output for human adjudication.

A separate versioned pipeline score records owned fault generation, the expected
alert, exact incident membership, a terminal and usable investigation, and confirmed
owned recovery. A separate observability score reports required evidence domains,
bounded local log collection, and the retained capsule fingerprint. Local `kubectl`
tail counts are context only: they are not a proxy for total emitted, indexed,
selected, or model-read log records. A missing source cannot silently become a
model diagnosis failure, and pipeline completion does not imply that the diagnosis
is correct.

## Validity Limits

One run per scenario is an exploratory paired benchmark. It can show a concrete model
difference on a fixed suite but cannot establish general accuracy. Use repetitions,
report dispersion, preserve failures, and add future cases without changing old answers.
Do not modify mechanisms or rubric criteria in response to a candidate model's output.

The controller continues to enforce one active run, a 1 GiB admission threshold,
automatic expiry and recovery below 768 MiB. The buffered-export case is intentionally
bounded below the worker's cgroup limit. An actual `OOMKilled` event remains a separate
defensive alert, not a desired test outcome. The Lab must never exhaust the node or
restart unrelated services.
