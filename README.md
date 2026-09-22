# FCAPSule Lab

FCAPSule Lab is a standalone observable workload for exercising FCAPSule against real
Kubernetes failures. It starts healthy, produces sustained structured telemetry, and
lets an operator trigger and recover realistic incidents after deployment. It is not a
runtime dependency of FCAPSule.

## Kubernetes Stack

`kubectl apply -k deploy/kubernetes` creates the `fcapsule-lab` namespace and these
workloads:

| Workload | Role | Default limit |
|---|---|---:|
| `mysql` | Inventory database with an explicit `max_connections=40` | 600m CPU / 640 MiB |
| `mysql-exporter` | Official Prometheus MySQL exporter | 100m CPU / 96 MiB |
| `inventory-api` | MySQL-backed reservation service | 400m CPU / 256 MiB |
| `orders-api` | Checkout orchestration and retry behavior | 400m CPU / 192 MiB |
| `traffic-generator` | Continuous 25 request/second workload | 300m CPU / 128 MiB |
| `lab-worker` | Background jobs and resource incidents | 500m CPU / 160 MiB |
| `lab-control` | Scenario UI and recovery controller | 150m CPU / 128 MiB |

Requests are intentionally modest. Healthy traffic targets several thousand structured
events per minute; the evaluation runner records observed counts rather than assuming
that configured throughput was achieved. Filebeat remains the log owner and sends these container logs to the cluster's
OpenSearch installation.

MySQL uses an ephemeral `emptyDir` because this is a disposable test environment. The
database is recreated when its pod is replaced. No FCAPSule code or credentials are
included in the lab.

## Deploy

Requirements:

- a Kubernetes cluster with the Prometheus Operator CRDs;
- an existing Prometheus selecting `ServiceMonitor` and `PrometheusRule` resources with
  `release: prometheus`;
- Filebeat or another Kubernetes container-log collector for log-domain testing;
- network access from the node to GitHub during pod initialization.

```bash
python3 -m pip install '.[dev]'
python3 tools/deploy_kubernetes.py --node-exporter http://<node-ip>:9100
make k8s-status
python3 tools/verify_stack.py
```

The deployment helper pins the published local commit, checks real host memory, pauses
traffic during the upgrade and updates deployments sequentially without surge replicas.
For an ordinary cluster with sufficient capacity, `kubectl apply -k deploy/kubernetes`
also works; that path installs the current published `main` branch. Publish local code
before using either path. No container registry or Docker build is required for this
development setup; a pinned prebuilt image is preferable for an offline installation.

The control UI is exposed at:

```text
http://<node-ip>:30766
```

The default cluster used during development is available at
`http://192.168.0.102:30766`.

## Incident Scenarios

The UI admits only one active run after both services are reachable and at least 1 GiB
of host MemAvailable remains. Choose a duration of two to five minutes. **Recover all**
stops early; a watchdog also recovers on expiry, unavailable memory readings or less
than 768 MiB available. The UI shows current headroom, active run and remaining time.
Worker/inventory operations have their own leases; durable import jobs expire after
five minutes even if the controller is unavailable. This is a single-node test harness,
not a guarantee against unrelated workloads exhausting the host.

The evaluation catalog contains fifteen cases: five led by logs, five requiring logs
plus performance metrics, and five where Kubernetes configuration is decisive. Cases
cover poison-message redelivery, dependency contract changes, unique-key collisions,
real MySQL deadlocks, idempotency conflicts, memory and CPU pressure, connection and
lock saturation, downstream latency, schema rollout order, dependency routing, timeout
budgets, signing-key skew and response-version skew.

The complete matrix and independently observable outcomes are frozen in the
[scenario contract](docs/SCENARIO_CONTRACT.md). Configuration scenarios update a
dedicated ConfigMap through narrow namespace RBAC, then restore its baseline values.

The Lab also includes one separate operational probe, **Metrics Service label drift**.
It changes the actual `Service` label selected by the application `ServiceMonitor`,
while application Pods remain healthy. The resulting discovery alert is useful for
testing FCAPSule's Prometheus target investigation, but is deliberately excluded from
the fifteen-case model benchmark: losing a metrics target is an observability problem,
not a workload root-cause label.

Alerts state symptoms, not injected causes. Application telemetry contains ordinary
operation names, SQL codes and identities, not scenario labels or expected answers.
The independent [scenario contract](docs/SCENARIO_CONTRACT.md) defines what the evaluator
must verify and what cannot be claimed from these cases. It is not sent to FCAPSule.

## Run a Recorded Evaluation

```bash
python3 tools/run_scenarios.py --scenario schema-drift
python3 tools/run_scenarios.py --scenario all
python3 tools/evaluate_models.py --scenario all --models deepseek-v4-flash deepseek-v4-pro
python3 tools/review_run.py artifacts/validation-<UTC>
python3 tools/test_prometheus_rules.py --promtool /path/to/promtool
```

Use `--lab`, `--prometheus` and `--fcapsule` to override the development URLs. The runner
waits for healthy traffic and previous alert windows, records the intervention and
memory samples, collects bounded workload logs and preserves FCAPSule's unedited
assessment. Results go under ignored `artifacts/validation-<UTC>/`. Alert detection and
diagnostic quality are separate outcomes; a ready assessment is not an accuracy score.
The full suite takes time because alert windows must clear between cases.
The review command saves historical Prometheus series and summarizes real SQL error
codes, repeated import deliveries and logged export-buffer sizes. It does not score
model prose or modify the saved answers. Log files are bounded tails; their line
counts must not be presented as the total indexed volume.
The local promtool check validates the rule set and tests OOM during restart
backoff, a completed OOM restart, stale OOM state, and non-OOM crashes. It does not
start a cluster pod or modify Prometheus data.

The [paired evaluation guide](docs/EVALUATION.md) explains same-capsule model
comparison, objective rubric components, token/latency capture and validity limits.
The [live validation record](docs/LIVE_VALIDATION.md) separates original failures,
product corrections and follow-up checks. To compare a changed expression against
the same historical Prometheus samples without injecting another fault:

```bash
python3 tools/replay_rule.py --alert LabWorkerOOMKilled \
  --time 2026-09-20T17:18:30Z --reference b8cb1c9 \
  --out artifacts/oom-rule-replay.json
```

This needs the original samples to remain available in Prometheus. It evaluates
expressions at a timestamp, not alert pending duration or a new live firing.

To stop ongoing traffic after testing:

```bash
kubectl scale deployment/traffic-generator -n fcapsule-lab --replicas=0
```

Scale back to one before a new evaluation. The lab UI and database can remain running.

## Prometheus Integration

The manifests create:

- `ServiceMonitor/fcapsule-lab-applications` with a 10-second scrape interval;
- `ServiceMonitor/fcapsule-lab-mysql` for the official MySQL exporter;
- `PrometheusRule/fcapsule-lab-incidents` containing FM and PM rules.

They also include `LabApplicationMetricsDiscoveryMissing`, which fires when the
`orders-api` application target has been absent for more than one minute. The
discovery probe intentionally introduces a typo in the **Service** label selected by
the `ServiceMonitor`; it does not change Pod labels, process configuration, or workload
health. This gives FCAPSule a concrete path to compare the monitor selector, Service
metadata, target state, and still-healthy Pods before recommending a monitoring fix.

Relabeling preserves `namespace`, `pod`, and `service` on application metrics. This is
important because FCAPSule uses those labels to resolve an alert to a Kubernetes
workload. The inventory pod references `lab-mysql-config`, so an incident capture can
retain the configured `MYSQL_MAX_CONNECTIONS` and masked configuration snapshot without
reading Secrets.

Configure FCAPSule to observe `fcapsule-lab` in addition to the namespaces already in
use. After synchronization, Targets should show each lab workload with PM, logs, and
configuration coverage.

## Observe Raw Telemetry

```bash
make k8s-logs

kubectl get pods -n fcapsule-lab -w
kubectl get prometheusrule -n fcapsule-lab
```

In Prometheus, query:

```promql
mysql_global_status_threads_connected / mysql_global_variables_max_connections
rate(container_cpu_usage_seconds_total{namespace="fcapsule-lab",container="worker"}[1m])
increase(kube_pod_container_status_restarts_total{namespace="fcapsule-lab"}[5m])
rate(orders_checkout_requests_total{namespace="fcapsule-lab",status="503"}[1m])
```

## Remove

```bash
make k8s-down
```

The older five-container Compose stack remains available for local adapter experiments.
It uses PostgreSQL and its terminal controls; Kubernetes is the primary path for live
FCAPSule testing.
