"""Public incident controls without evaluator answers or scoring criteria."""

from __future__ import annotations


DEFAULT_SCENARIO_CONFIG = {
    "INVENTORY_URL": "http://inventory-api:8081",
    "INVENTORY_TIMEOUT_SECONDS": "1.5",
    "MAX_RETRIES": "3",
    "ORDER_SIGNING_KEY_ID": "checkout-key-v1",
    "INVENTORY_ACCEPTED_KEY_ID": "checkout-key-v1",
    "ORDER_EXPECTED_SCHEMA": "v1",
    "INVENTORY_RESPONSE_SCHEMA": "v1",
    "INVENTORY_QUERY_REVISION": "v1",
}


def action(target: str, mode: str = "normal") -> dict[str, str]:
    return {"target": target, "mode": mode}


SCENARIOS = {
    # Log-led cases: the distinguishing mechanism is recorded in execution logs.
    "poison-job": {
        "title": "Import worker restart loop", "class": "FM", "evidence_group": "logs",
        "summary": "A durable import repeatedly fails while the worker processes it.",
        "actions": [action("database", "poison")], "expected_alert": "LabWorkerCrashLooping",
    },
    "response-contract": {
        "title": "Dependency response contract regression", "class": "FM", "evidence_group": "logs",
        "summary": "Checkout receives successful responses that no longer satisfy its dependency contract.",
        "actions": [action("inventory", "response-contract")],
        "expected_alert": "LabOrdersDependencyDocumentInvalid",
    },
    "reservation-token-collision": {
        "title": "Reservation token collision", "class": "FM", "evidence_group": "logs",
        "summary": "Concurrent reservations reuse a database uniqueness token and transactions are rejected.",
        "actions": [action("inventory", "token-collision")],
        "expected_alert": "LabInventoryConstraintFailures",
    },
    "transaction-deadlock": {
        "title": "Inventory transaction deadlock", "class": "FM", "evidence_group": "logs",
        "summary": "Two reconciliation paths acquire stock rows in opposite order.",
        "actions": [action("inventory", "deadlock")],
        "expected_alert": "LabInventoryDeadlockVictims",
    },
    "idempotency-conflict": {
        "title": "Checkout idempotency collision", "class": "FM", "evidence_group": "logs",
        "summary": "Different checkout payloads are assigned the same idempotency record.",
        "actions": [action("orders", "idempotency-conflict")],
        "expected_alert": "LabOrdersIdempotencyConflicts",
    },

    # Metric-led cases: time-series behavior is required to distinguish the mechanism.
    "memory-leak": {
        "title": "Buffered report export", "class": "PM", "evidence_group": "metrics",
        "summary": "A report export buffers pages instead of streaming them and reaches its memory limit.",
        "actions": [action("worker", "memory-leak")], "expected_alert": "LabWorkerOOMKilled",
    },
    "cpu-saturation": {
        "title": "Credential migration backlog", "class": "PM", "evidence_group": "metrics",
        "summary": "A migration applies expensive password derivation under a small CPU quota.",
        "actions": [action("worker", "cpu-saturation")], "expected_alert": "LabWorkerCPUHigh",
    },
    "mysql-connections": {
        "title": "MySQL connection saturation", "class": "PM", "evidence_group": "metrics",
        "summary": "Inventory retains sessions until the database connection ceiling is approached.",
        "actions": [action("inventory", "connection-saturation")],
        "expected_alert": "LabMySQLConnectionsSaturated",
    },
    "lock-contention": {
        "title": "Inventory lock contention", "class": "PM", "evidence_group": "metrics",
        "summary": "Stock reconciliation holds a row while reservations wait and callers retry.",
        "actions": [action("inventory", "lock-contention")],
        "expected_alert": "LabInventoryLockContention",
    },
    "downstream-latency": {
        "title": "Inventory latency amplification", "class": "PM", "evidence_group": "metrics",
        "summary": "Inventory responses slow down enough to increase checkout concurrency and retries.",
        "actions": [action("inventory", "downstream-latency")],
        "expected_alert": "LabCheckoutLatencyHigh",
    },

    # Configuration-led cases: the decisive difference is retained in a real ConfigMap.
    "schema-drift": {
        "title": "Inventory query revision mismatch", "class": "CM", "evidence_group": "configuration",
        "summary": "A query revision is enabled before the corresponding database migration.",
        "config": {"INVENTORY_QUERY_REVISION": "v2"},
        "actions": [action("inventory", "configured")],
        "expected_alert": "LabInventoryQueryFailures",
    },
    "dependency-route": {
        "title": "Inventory route misconfiguration", "class": "CM", "evidence_group": "configuration",
        "summary": "Checkout is reloaded with a dependency endpoint that is not serving traffic.",
        "config": {"INVENTORY_URL": "http://inventory-api:8099"},
        "actions": [action("orders", "configured")],
        "expected_alert": "LabOrdersDependencyTransportFailures",
    },
    "timeout-budget": {
        "title": "Dependency timeout budget mismatch", "class": "CM", "evidence_group": "configuration",
        "summary": "The caller timeout is shorter than the dependency's normal processing budget.",
        "config": {"INVENTORY_TIMEOUT_SECONDS": "0.05"},
        "actions": [action("inventory", "fixed-latency"), action("orders", "configured")],
        "expected_alert": "LabOrdersDependencyTimeouts",
    },
    "signing-key-skew": {
        "title": "Request key policy mismatch", "class": "CM", "evidence_group": "configuration",
        "summary": "The caller's request key identifier is not accepted by its dependency.",
        "config": {"INVENTORY_ACCEPTED_KEY_ID": "checkout-key-v2"},
        "actions": [action("inventory", "configured"), action("orders", "configured")],
        "expected_alert": "LabOrdersDependencyAuthorizationFailures",
    },
    "response-schema-skew": {
        "title": "Response schema configuration skew", "class": "CM", "evidence_group": "configuration",
        "summary": "Checkout expects a response schema version the inventory service does not emit.",
        "config": {"ORDER_EXPECTED_SCHEMA": "v2"},
        "actions": [action("inventory", "configured"), action("orders", "configured")],
        "expected_alert": "LabOrdersDependencySchemaRejected",
    },
}


# This is intentionally separate from the fifteen-case diagnostic benchmark.
# It exercises the monitoring-discovery path rather than a workload diagnosis.
DISCOVERY_SCENARIOS = {
    "metrics-service-label-drift": {
        "title": "Metrics Service label drift", "class": "Discovery", "evidence_group": "discovery",
        "summary": "Metrics are no longer discovered even though the application pods remain healthy.",
        "service_metrics_label": "ture",
        "expected_alert": "LabApplicationMetricsDiscoveryMissing",
    },
    "mysql-exporter-scrape-path": {
        "title": "MySQL metrics target scrape failure", "class": "Discovery", "evidence_group": "discovery",
        "summary": "Prometheus discovers the exporter but cannot collect its metrics.",
        "runner_only": True,
        "expected_alert": "LabExporterScrapeFailed",
    },
}


def public_scenarios() -> dict[str, dict[str, str | bool]]:
    """Safe operator-facing catalog metadata without mutation settings or oracle labels."""
    scenarios = {}
    for key, item in {**SCENARIOS, **DISCOVERY_SCENARIOS}.items():
        scenarios[key] = {
            field: item[field] for field in ("title", "class", "evidence_group", "summary") if field in item
        }
        scenarios[key]["runner_only"] = bool(item.get("runner_only"))
    return scenarios
