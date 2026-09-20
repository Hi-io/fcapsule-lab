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

| Scenario | Signal | Real failure mechanism | Expected evidence |
|---|---|---|---|
| Buffered report export | FM | Serialized export pages remain buffered until completion and cross the worker's 160 MiB cgroup limit | Page/buffer growth, OOM reason, restart and sampled memory |
| Incompatible import message | FM | Real Base64 decoding fails before acknowledging a durable job; the exception escapes the consumer | Stack trace, repeated delivery identity, restarts/backoff |
| Credential migration backlog | PM | Expensive PBKDF2 work per account consumes the worker quota | Computation progress, CPU rate, no required restart |
| MySQL connection saturation | PM | Completed inventory operations retain session handles | Independent exporter measurements, server rejection, checkout impact |
| Inventory lock contention | PM | Stock reconciliation holds an actual InnoDB update lock while reservations wait | Blocking transaction lifecycle, SQL 1205, retry amplification |
| Inventory schema mismatch | PM/FM | A query expects a column before the corresponding migration exists | Real SQL 1054, query failures, healthy reachable database |

The first two are fault-management cases because Kubernetes changes workload state. The
resource cases are performance-management cases: Prometheus raises an alert while the
pod remains running, allowing FCAPSule to correlate time series, logs, and runtime
configuration.

Alerts state symptoms, not injected causes. Application telemetry contains ordinary
operation names, SQL codes and identities, not scenario labels or expected answers.
The independent [scenario contract](docs/SCENARIO_CONTRACT.md) defines what the evaluator
must verify and what cannot be claimed from these cases. It is not sent to FCAPSule.

## Run a Recorded Evaluation

```bash
python3 tools/run_scenarios.py --scenario schema-drift
python3 tools/run_scenarios.py --scenario all
```

Use `--lab`, `--prometheus` and `--fcapsule` to override the development URLs. The runner
waits for healthy traffic and previous alert windows, records the intervention and
memory samples, collects bounded workload logs and preserves FCAPSule's unedited
assessment. Results go under ignored `artifacts/validation-<UTC>/`. Alert detection and
diagnostic quality are separate outcomes; a ready assessment is not an accuracy score.
The full suite takes time because alert windows must clear between cases.

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
