# Architecture

FCAPSule Lab is an observability source stack, not a component of FCAPSule. The compose
network contains five services with explicit dependency health checks:

```text
traffic-generator --> orders-api --> inventory-api --> postgres
        |                 |               |
        +-----------------+---------------+--> Prometheus scrapes /metrics
```

The steady workload is online-serving traffic. It has request, error, latency, retry,
and concurrency signals. Logs are emitted as JSON to each container's standard output.
Prometheus polls the application endpoints and evaluates alert rules. The database is
intentionally a real PostgreSQL container because the primary failure needs true locking
and authentication semantics instead of handcrafted error messages.

## Failure Design

The primary mode is `lock-contention`:

1. Concurrent inventory reservations lock the same `inventory_items` row.
2. A holder sleeps for 180 ms inside the transaction.
3. Other transactions have a 75 ms PostgreSQL lock timeout and fail.
4. Orders sees a 503 and retries up to three times while the breaker remains closed.
5. Retried calls increase dependency work and log volume.
6. Prometheus observes lock failures, retry amplification, checkout errors, and latency.

This produces a plausible multi-domain troubleshooting problem. It does not assert that
any future FCAPSule report should call lock contention a confirmed root cause without
additional evidence.

## Why Prometheus Belongs Here

Prometheus is a collector that scrapes application metrics endpoints. It belongs beside
the workload in this development stack, just as a real deployment would use an existing
Prometheus installation or managed equivalent. FCAPSule should query it through a source
adapter and retain only selected evidence. Embedding or reimplementing Prometheus inside
FCAPSule would duplicate ownership, storage, and alerting responsibilities.

Metrics use stable names and bounded labels. No request IDs, order IDs, pod IDs, or other
high-cardinality values are labels. This follows Prometheus guidance that labels create
time series and should not be overused.
