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
| `mysql` | Inventory database with an explicit `max_connections=40` | 600m CPU / 1 GiB |
| `mysql-exporter` | Official Prometheus MySQL exporter | 100m CPU / 96 MiB |
| `inventory-api` | MySQL-backed reservation service | 400m CPU / 256 MiB |
| `orders-api` | Checkout orchestration and retry behavior | 400m CPU / 192 MiB |
| `traffic-generator` | Opt-in checkout workload at 25 requests/second, capped at 12 in flight | 300m CPU / 128 MiB |
| `lab-worker` | Background jobs and resource incidents | 500m CPU / 192 MiB (effective `-k` limit) |
| `lab-control` | Scenario UI and recovery controller | 150m CPU / 128 MiB |

The traffic generator is stopped by default to avoid normal DNS-induced false alerts.
The operator runner starts it only for owned `timeout-budget` and
`response-schema-skew` scenarios, using the bounded profile of 25 checkout requests
per second and at most 12 in flight. When all slots are busy, it sheds new attempts
instead of building a request queue. The current 25 RPS `timeout-budget` case has
produced Inventory admission co-alerts during live demos; `response-schema-skew` keeps
Inventory fast and is the preferred configuration demo. That
concurrency cap is 30% of MySQL's configured 40-session ceiling even if every active
checkout owns a database session. Twelve is also an
explicit runtime safety ceiling: if a stale or edited ConfigMap requests a higher
`MAX_INFLIGHT`, the generator uses 12, emits a startup warning, and exposes both
configured and effective limits in its health response and metrics. A healthy completed
checkout typically emits six structured events across the traffic generator, orders API
and inventory API, so the configured rate can produce about 9,000 checkout-path events
per minute before background service logs.
The evaluation runner records observed counts rather than assuming that configured
throughput was achieved. Filebeat remains the log owner and sends these container logs
to the cluster's OpenSearch installation.

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

The deployment helper installs a full commit SHA (local `HEAD` by default), checks real
host memory, pauses traffic during the upgrade and updates deployments sequentially
without surge replicas. Since pods fetch the source archive from GitHub, pass a published
commit SHA for a reproducible deployment. `kubectl` can be selected with `--kubectl` or
`KUBECTL`; otherwise the helper searches `PATH` and then `~/.local/bin/kubectl`.
For a source-only update to an existing lab, use `--apps-only` with the published full
commit SHA. This updates the source-installed API, worker, controller and generator
Deployments while leaving MySQL, credentials, runtime configuration, Services and
monitoring objects untouched. The helper preserves the traffic generator's live replica
count; it remains stopped if it was already scaled to zero. Update monitoring resources
separately when needed with `kubectl apply -f deploy/kubernetes/observability.yaml`, which
updates the named ServiceMonitors and PrometheusRule in place without pruning other rules.

```bash
REVISION='your-reviewed-published-full-commit-sha'
python3 tools/deploy_kubernetes.py \
  --node-exporter http://YOUR_LAB_NODE_IP:9100 \
  --revision "$REVISION" \
  --apps-only
```

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

The Lab control UI presents one catalog of 17 operator scenarios. Fifteen are
workload-diagnosis cases balanced across log-led, metrics-led and configuration-led
evidence; two more exercise Prometheus discovery and scrape-path failures. Every case
is a usable demo. The grouping exists only to keep model evaluation comparable, not to
suggest that some scenarios are second-class. Cases cover poison-message redelivery,
dependency contract changes, unique-key collisions, real MySQL deadlocks, idempotency
conflicts, bounded memory and CPU pressure, connection and lock saturation, downstream
latency, schema rollout order, dependency routing, timeout budgets, signing-key skew,
response-version skew, and monitoring coverage failures.

The complete matrix and independently observable outcomes are frozen in the
[scenario contract](docs/SCENARIO_CONTRACT.md). Configuration scenarios update a
dedicated ConfigMap through narrow namespace RBAC, then restore its baseline values.
The recorded `--case all` run also includes a two-occurrence schema-rollout history
exercise after the 17 distinct scenarios. It checks whether the second assessment
cites the earlier retained episode and requests one retained-capsule-only review.
The 16-minute grouping wait is intentional and is not another fault mechanism.

The two monitoring scenarios are first-class demos: **Metrics Service label drift**
removes an otherwise healthy application target from discovery; **MySQL exporter scrape
path failure** keeps the target discovered but causes its scrape to return HTTP 404.
Both distinguish loss of observability from an application outage. The exporter case
is runner-only because it needs actual Prometheus Targets screenshots and its own
guarded rollback procedure.

Alerts state symptoms, not injected causes. Application telemetry contains ordinary
operation names, SQL codes and identities, not scenario labels or expected answers.
The independent [scenario contract](docs/SCENARIO_CONTRACT.md) defines what the evaluator
must verify and what cannot be claimed from these cases. It is not sent to FCAPSule.

## Run a Recorded Evaluation

For the full guide, including the 17 distinct scenarios, recurrence exercise, both
monitoring discovery cases, external screenshot evidence and safe recorded execution, see
[Operator demos](docs/OPERATOR_DEMOS.md). Preview the full catalog without network
access:

```bash
python tools/run_operator_demos.py plan
```

Live execution and paid evidence reviews require explicit `--execute` after operator
coordination. The runner never changes FCAPSule's configured Pro model or budgets.

```bash
python3 tools/run_operator_demos.py preflight --lab-node NODE --out local_reports/preflight-<UTC>
python3 tools/run_operator_demos.py run --case all --lab-node NODE --execute --out local_reports/evaluation-<UTC>
python3 tools/evaluate_models.py --scenario all --models deepseek-v4-flash deepseek-v4-pro
python3 tools/review_run.py artifacts/validation-<UTC>
python3 tools/test_prometheus_rules.py --promtool /path/to/promtool
```

`tools/run_scenarios.py` remains as a lower-level compatibility API used by the
model evaluator. For a complete operator run, use `run_operator_demos.py`, which
captures the automatic investigation before recovery and records owned evidence,
recovery and the separated pipeline/diagnosis scores.

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

The [external screenshot evaluation](docs/EXTERNAL_SCREENSHOT_EVALUATION.md) details
the exporter scrape-path case, actual Prometheus before/fault/after screenshots, and
an optional one-shot media reassessment. This case is in the 17-scenario operator
catalog but outside the fifteen-case workload score. Evidence must come from
Prometheus itself; FCAPSule UI screenshots are not valid incident evidence.

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

The older five-container Compose stack remains available for local adapter
compatibility experiments. Its PostgreSQL lock and authentication failure modes are
legacy-only: they are not part of the supported 17 Kubernetes scenarios or the
diagnostic benchmark. See [Compose compatibility status](docs/COMPOSE_COMPATIBILITY.md)
before using those local controls; passing a Compose smoke test is not evidence that
they meet the live FCAPSule scenario contract.
