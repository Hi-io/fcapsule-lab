# Scenario Contract and Audit

Frozen before the new live evaluation. This is the evaluator's oracle, not input
to FCAPSule. Keep it outside captured workload configuration and telemetry.

## Audit Findings

The previous lab disclosed causes in alert annotations (for example, "poison job")
and emitted explanatory error messages rather than recording ordinary execution.
The poison job printed forty manufactured errors before calling `os._exit(17)`;
the CPU case ran a generic arithmetic loop; contention slept inside every request.
Mode names appeared in workload logs/metrics. Those shortcuts made diagnosis easier
and obscured whether the investigator actually combined evidence.

Recovery had no lease, concurrency guard or host-memory check. Repeated starts
could leave background threads running. MySQL sampling could fail precisely at
saturation and leave a stale connection gauge. Current tests checked strings in
source code rather than workload behavior. High-volume traffic existed, but its
actual rate and log coverage were not independent evaluation acceptance criteria.

## Fixed Scenarios

| ID | Mechanism | Independently observable success criterion | What a useful diagnosis must distinguish |
|---|---|---|---|
| memory-leak | A report export buffers encoded pages until completion instead of streaming them | Worker cgroup OOM termination and restart; export page/buffer growth preceding it | Buffered export growth versus unrelated application crash; sampled memory need not contain the peak |
| poison-job | A durable import message contains invalid Base64; the consumer's real decoder exception escapes before acknowledgement | Decoder exception, same job redelivered after process restart, restart/backoff alert | Payload/consumer handling failure versus resource termination; no claim that queue deletion happened without evidence |
| cpu-saturation | An account credential migration uses excessive PBKDF2 work per record | Sustained CPU utilization relative to limit and progress logs, without OOM/restart | Expensive computation versus database wait or memory pressure |
| mysql-connections | Session handles are retained after an inventory operation instead of released | MySQL connection count grows and real connection rejection occurs; orders degrade | Pool/session lifetime versus insufficient database size alone; exporter remains an independent observer |
| lock-contention | Stock reconciliation holds an actual InnoDB row lock while subsequent reservations wait | Error 1205, increased latency/retries; blocker transaction lifecycle in inventory logs | Lock ownership/waiting versus CPU saturation or schema incompatibility |
| schema-drift | A new inventory query expects `reserved_quantity` before its migration is applied | MySQL error 1054; reservations and checkout fail while DB remains reachable | API/schema version mismatch versus connection ceiling or row lock timeout |

Do not alter these outcomes to match an LLM answer. Record missing alerts, missing
evidence, unsupported claims and inconclusive answers separately. A matching word
or a plausible summary alone is not diagnostic success. No subjective numerical
"accuracy" score is inferred from six hand-built cases.

## Execution Protocol

1. Check actual kernel MemAvailable through node-exporter, not Kubernetes allocatable.
2. Start healthy; observe at least two minutes of ordinary traffic before a fault.
3. Run exactly one scenario, for at most five minutes, with automatic expiration.
4. Save the run ID/times and intervention only in the lab evaluator record.
5. Verify the real symptom and expected Prometheus alert independently of FCAPSule.
6. Save FCAPSule's original assessment before any product correction; retain failures.
7. Recover and wait for relevant alert windows to clear before another scenario.
8. Query log counts and memory minima over the run. Volume is measured, not assumed.

Healthy traffic targets 25 requests/second, normally several thousand structured
events per minute. Backpressure deliberately reduces throughput during failures;
do not manufacture unrelated error lines just to achieve a log-count target.

## Resource Envelope

The initial GO15 measurement was 7,019,782,144 total bytes and 1,514,065,920 available
bytes, with no swap. Reuse existing pods and MySQL. Start requires at least 1 GiB
MemAvailable; abort below 768 MiB or when the memory source cannot be read. These
are conservative lab guardrails, not a guarantee against concurrent node workloads.
Worker OOM is restricted by its existing 160 MiB container limit. Never deliberately
exhaust host memory, kill unrelated pods or change cluster eviction thresholds.

Alerts describe observed symptoms only. Fault names, expected answers and evaluator
judgments must not appear in application alert descriptions or captured ConfigMaps.
Real SQL codes, stack traces, configuration, request IDs and resource measurements
remain visible: removing legitimate clues would be as misleading as adding answers.

## Reference Semantics

- [Kubernetes resource limits](https://kubernetes.io/docs/concepts/configuration/manage-resources-containers/): scheduling requests are not live free memory; container OOM differs from node eviction.
- [Node pressure](https://kubernetes.io/docs/concepts/scheduling-eviction/node-pressure-eviction/): observe available memory and pressure independently of nominal capacity.
- [MySQL connection ceiling](https://dev.mysql.com/doc/refman/8.4/en/too-many-connections.html): rejection is an actual server response.
- [InnoDB error handling](https://dev.mysql.com/doc/refman/8.4/en/innodb-error-handling.html): lock timeouts require transaction cleanup; they are not automatically deadlocks.
