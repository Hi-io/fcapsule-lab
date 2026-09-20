# Product Review

## User Job

An evaluator needs to deploy a stable production-like workload, trigger one credible
incident after baseline telemetry exists, observe thousands of noisy source records, and
then judge whether FCAPSule preserves the small set of evidence needed to investigate.

## Product Decisions

- A control UI is appropriate here because scenario activation is the user's primary
  job; it is not part of the FCAPSule product UI.
- Six cases cover termination, decoder compatibility, computation, session lifetime,
  transaction blocking and application/schema compatibility. Their independent oracle
  is defined before running FCAPSule in `SCENARIO_CONTRACT.md`.
- Baseline traffic is high enough to create a meaningful log-reduction problem but has
  bounded concurrency and explicit Kubernetes limits.
- MySQL configuration is a ConfigMap referenced by the affected inventory workload, so
  configuration evidence is obtainable through FCAPSule's existing read-only adapter.
- The MySQL exporter complements application metrics. It verifies the database symptom
  independently without making FCAPSule responsible for metric collection.

## Acceptance Criteria

1. Sequential deployment starts healthy without surge replicas on a constrained node.
2. Prometheus reports the application and MySQL targets as up.
3. Source log counts are measured; healthy traffic produces thousands of events without
   manufacturing errors or ignoring backpressure during failures.
4. Every scenario starts after deployment and has a corresponding alert rule.
5. FM scenarios create a genuine Kubernetes restart or waiting state.
6. PM scenarios fire without restarting the affected workload.
7. Recover all returns the lab to healthy traffic without redeployment.
8. FCAPSule discovers current pods and can capture logs, metrics, alerts, and ConfigMaps.
9. Start is blocked below the memory floor; leases and low-memory recovery are tested.
10. Failed or unsupported agent conclusions remain in the evaluation record.
