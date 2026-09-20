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

Requests are intentionally modest. Under healthy operation the request path emits about
6,000 structured JSON events per minute across traffic, orders, inventory, and worker
pods. Filebeat remains the log owner and sends these container logs to the cluster's
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
make k8s-up
make k8s-status
```

The control UI is exposed at:

```text
http://<node-ip>:30766
```

The default cluster used during development is available at
`http://192.168.0.102:30766`.

## Incident Scenarios

The UI starts every scenario after the stack is healthy. Use one primary scenario at a
time and select **Recover all** before moving to the next one.

| Scenario | Signal | Real failure mechanism | Expected evidence |
|---|---|---|---|
| Worker memory leak | FM | An unbounded batch cache allocates 8 MiB chunks until the 160 MiB cgroup limit causes an OOM kill | Kubernetes termination reason, restart counter, memory curve, allocation logs |
| Poison job crash loop | FM | A malformed durable MySQL queue item terminates each worker before acknowledgement | CrashLoopBackOff, repeated fatal decoder logs, durable queue context |
| CPU saturation | PM | A compute-bound batch sustains more than 75% of the worker CPU limit without terminating the pod | cAdvisor CPU series, custom iteration counter, healthy pod without restart |
| MySQL connection saturation | PM | The inventory pool retains 36 sessions against `max_connections=40` | connection ratio, rejected connections, checkout retries, ConfigMap value |
| Inventory lock contention | PM | Concurrent InnoDB transactions lock the same inventory row beyond the timeout | lock failures, latency, retries, checkout impact |

The first two are fault-management cases because Kubernetes changes workload state. The
resource cases are performance-management cases: Prometheus raises an alert while the
pod remains running, allowing FCAPSule to correlate time series, logs, and runtime
configuration.

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
lab_mysql_threads_connected / lab_mysql_max_connections
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
