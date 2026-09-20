# Kubernetes Architecture

FCAPSule Lab is an external workload system. FCAPSule observes it through the same
Prometheus, OpenSearch, and Kubernetes APIs used for any other namespace.

```text
traffic-generator -> orders-api -> inventory-api -> mysql
                                           |          |
lab-control -> worker ---------------------+     mysql-exporter
     |             |
     +-- scenario controls              durable jobs

application /metrics -> ServiceMonitor -> cluster Prometheus -> alerts
container stdout     -> Filebeat       -> OpenSearch
pod + ConfigMaps     -> Kubernetes API -> FCAPSule
```

## Design Boundaries

- The lab owns workloads and failure injection; FCAPSule owns capture and investigation.
- Prometheus and OpenSearch are shared cluster infrastructure, not bundled duplicates.
- Raw logs remain in OpenSearch. The lab does not send logs directly to FCAPSule.
- MySQL data is disposable. The persistent object under test is the incident capsule,
  not the lab database.
- Scenarios are off after deployment and require an explicit UI action.

## Failure Semantics

The FM scenarios produce real Kubernetes state transitions. Memory allocation crosses a
cgroup limit and is handled by the kernel; an import decoder exception exits the worker process while
leaving the MySQL queue item unacknowledged. Kubernetes then supplies restart,
termination-reason, and CrashLoop evidence.

The PM scenarios deliberately remain below termination boundaries. CPU is throttled at
the configured limit, while retained MySQL sessions consume an explicit connection
budget. This lets Prometheus fire on performance symptoms while configuration and logs
explain why the resource changed.

Every application metric target is relabeled with pod and namespace identity. Alert
rules preserve those labels, allowing FCAPSule to map the alert to the affected workload
instead of guessing from a service name.

## Safety and Evaluation Boundary

The controller reads node-exporter MemAvailable, admits one bounded run and records its
intervention separately from application telemetry. Both worker and inventory enforce
local leases. MySQL import rows have a five-minute expiration checked by each consumer
restart. Recovery releases inventory sessions before attempting queue cleanup.

Kustomize disables rolling surge for Python deployments. The sequential deployment
helper pauses traffic and pins source to a published commit. No new always-on database,
broker or tracing backend is required. Downward API identity overrides legacy log
defaults to avoid contradictory namespaces in captured data.

The evaluator stores run records and source logs under ignored `artifacts/`; FCAPSule
receives none of the scenario oracle through a direct integration. The system under
test must explain symptoms using its normal observability APIs.
