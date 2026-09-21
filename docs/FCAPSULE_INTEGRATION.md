# FCAPSule Integration

The Kubernetes lab is an external workload, not a plugin or runtime dependency.
FCAPSule uses its normal configured Prometheus, OpenSearch and Kubernetes APIs.
There are no lab-specific scenario IDs, expected answers or score labels in those
application alert annotations. The control UI and evaluator know the intervention;
the investigator must work from observed workload evidence.

## Live Workflow

1. Deploy the lab and confirm healthy application traffic and Prometheus targets.
2. Include `fcapsule-lab` in FCAPSule's configured namespace scope.
3. Start a leased scenario through the separate lab UI or evaluation runner.
4. Prometheus fires a symptom rule. FCAPSule captures the alert and a bounded window
   of source logs, performance measurements and available configuration.
5. Inspect the episode assessment, actual agent observations and retained capsule.
6. Compare the unedited result with `SCENARIO_CONTRACT.md` outside FCAPSule.
7. Recover the workload and allow alert windows to clear before the next run.

The optional `metrics-service-label-drift` control is not part of the scored fifteen
scenario suite. It is a live integration check for the Prometheus discovery adapter:
the alert is expected to lead to a selector-versus-Service-label finding while the
workloads stay healthy. It verifies an FCAPSule capability without supplying an
expected cause to the investigator.

Metrics preserve namespace/pod identity. OpenSearch receives stdout/stderr through
the cluster's existing collector. MySQL exposes its own metrics through the official
exporter; the connection pressure alert does not depend on a possibly stale gauge
inside the failing inventory service. Cross-application dependency discovery is a
capability to evaluate, not something the lab assumes FCAPSule already solves.

## Evidence Ownership

Prometheus owns metric history; OpenSearch owns raw logs; MySQL owns disposable
application rows. FCAPSule owns selected evidence and incident artifacts. No source
retention period is assumed. The evaluator's artifact folder keeps independent run
times, expected symptom, raw workload examples and original agent assessments.

No distributed tracing backend is configured. Request/order identifiers help manual
correlation but are not presented as collected distributed traces.

## Legacy Compose Path

`make up`, `make fault-on` and `make export-case` still support the older PostgreSQL
Compose workload and its one-time normalized export. That workflow is distinct from
the live Kubernetes suite and is not the path used to validate the current agent.
