# FCAPSule Lab

FCAPSule Lab is a standalone Docker Compose workload used to generate realistic,
high-volume operational telemetry for FCAPSule integration, demonstrations, and
regression tests. It is deliberately a separate project: FCAPSule is the evidence and
incident tool; this repository is a workload that can fail and be observed.

## What Runs

`docker compose up --build` starts exactly five containers:

| Container | Role | Exposes |
|---|---|---|
| `postgres` | Inventory database | internal PostgreSQL |
| `inventory-api` | Reservation service backed by PostgreSQL | `:8081`, `/metrics` |
| `orders-api` | Checkout service that retries inventory | `:8080`, `/metrics` |
| `traffic-generator` | Continuous concurrent checkout traffic | `:8082`, `/metrics` |
| `prometheus` | Independent metrics collector and alert evaluator | `:9090` |

At the default 120 requests/second, the traffic generator, orders API, and inventory API
emit more than 10,000 structured JSON log events per minute in aggregate. The default
uses bounded concurrency and modest per-container memory limits so it is appropriate for
local hardware. Increase `REQUESTS_PER_SECOND` only after verifying the baseline on the
host.

## Quick Start

```bash
cp .env.example .env
make up
make status
make verify
```

Watch the workload directly from a terminal:

```bash
make logs
```

Prometheus is available at `http://127.0.0.1:9090`. Its Targets page should show the
three instrumented services as healthy. Its Alerts page will show rules after a failure
has persisted for their configured interval.

## Failure Modes

The lab starts healthy. Enable a realistic failure through the terminal:

```bash
make fault-on
```

`lock-contention` causes concurrent inventory reservations to acquire a PostgreSQL row
lock with a 75 ms lock timeout while a holder sleeps for 180 ms. This creates genuine
database lock timeouts. Orders retries failed reservations up to three times while its
circuit breaker remains closed, creating retry amplification, rising latency, checkout
errors, and PM/FM signals.

The secondary configuration-failure mode is useful for an independent investigation
path:

```bash
make fault-bad
```

It simulates an inventory runtime configuration change with an invalid database password
and produces actual PostgreSQL authentication failures. Recover either mode with:

```bash
make fault-off
```

The default failure is intentionally not an out-of-memory kill. A deterministic database
contention incident is safer to reproduce, produces meaningful cross-service evidence,
and permits recovery without tearing down the stack. Resource-limit scenarios can be
added as a separate Compose profile later; they should not be the default because host
and Docker runtime behavior makes them less repeatable.

## Signals for FCAPSule

The services write JSON logs to stdout. Docker remains the log owner, so inspect them
with `docker compose logs`; FCAPSule should query a real log source or a bounded export,
not receive a copied permanent log store.

Prometheus scrapes application-owned `/metrics` endpoints every five seconds. Important
metrics include:

* `orders_checkout_requests_total` and `orders_checkout_failures_total`
* `orders_checkout_latency_p95_seconds`
* `orders_inventory_attempts_total`, `orders_inventory_retries_total`, and
  `orders_retry_amplification_ratio`
* `inventory_reservation_requests_total` and `inventory_database_failures_total`
* `inventory_active_transactions` and `inventory_failure_mode_info`
* `traffic_generator_requests_total`

Alert rules are included for checkout failure rate, inventory lock timeouts, and retry
amplification. Prometheus evaluates them locally. Alertmanager is intentionally not a
sixth service: FCAPSule can query Prometheus alert state for this lab or an integration
can add an Alertmanager endpoint later.

See [FCAPSule integration](docs/FCAPSULE_INTEGRATION.md) for the proposed boundary.

## Export a Bounded Incident

After a rule is firing, export only the needed window. The exporter queries Prometheus
for alert state and time series, and asks Docker for the corresponding structured stdout
records. It writes FCAPSule's normalized case files but does not retain a duplicate
telemetry store:

```bash
make export-case

cd ../fcapsule
python3 -m fcapsule.cli ingest-case \
  --case ../fcapsule-lab/artifacts/<case-directory> \
  --app-id checkout-lab \
  --app-name "Checkout Lab"
python3 -m fcapsule.cli serve
```

The Operations queue will show the captured incident. Select **Build report** to run
the FCAPSule evidence pipeline, then open the resulting responder report.

## Useful Commands

```bash
make up          # Build and start the five containers
make logs        # Follow structured logs from all services
make status      # Show container and health status
make fault-on    # Enable PostgreSQL lock contention
make fault-bad   # Enable invalid database-configuration failure
make fault-off   # Return inventory to normal behavior
make verify      # Check health, Prometheus targets, traffic, and one-minute log volume
make export-case # Export a bounded normalized case for FCAPSule
make down        # Stop containers while retaining named volumes
make clean       # Stop containers and remove named volumes
```

## Project Boundary

This repository should remain useful on its own. It does not import FCAPSule code and it
does not require an FCAPSule API key. FCAPSule should integrate through explicit source
adapters or an import boundary, using Prometheus queries, alert data, bounded log queries,
and configuration/topology metadata. This keeps the workload realistic and prevents
FCAPSule from becoming another Prometheus or another container orchestrator.

## Validation Note

The repository includes a `make verify` workflow, but Docker is not installed on the
development host that created this initial version. The Python code and Compose files are
statically validated in this repository; run the Compose verification on a host with
Docker Engine or Docker Desktop before treating the lab as a tested runtime.
