"""Public incident controls without evaluator answers or scoring criteria."""

from __future__ import annotations

from typing import Any


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
        "title": "Import retry loop", "class": "FM", "evidence_group": "logs",
        "summary": "A durable import remains unacknowledged and is retried after decode failures.",
        "actions": [action("database", "poison")], "expected_alert": "LabWorkerPoisonRetries",
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
        "title": "Checkout idempotency key reused", "class": "FM", "evidence_group": "logs",
        "summary": "The same idempotency key is bound to a different order before inventory is called.",
        "actions": [action("orders", "idempotency-conflict")],
        "expected_alert": "LabOrdersIdempotencyConflicts",
    },

    # Metric-led cases: time-series behavior is required to distinguish the mechanism.
    "memory-leak": {
        "title": "Buffered report export", "class": "PM", "evidence_group": "metrics",
        "summary": "A report export retains pages and approaches its configured memory guardrail.",
        "actions": [action("worker", "memory-leak")], "expected_alert": "LabWorkerBufferPressure",
    },
    "cpu-saturation": {
        "title": "Credential migration backlog", "class": "PM", "evidence_group": "metrics",
        "summary": "A migration applies expensive password derivation under a small CPU quota.",
        "actions": [action("worker", "cpu-saturation")], "expected_alert": "LabWorkerCPUHigh",
    },
    "mysql-connections": {
        "title": "MySQL connection saturation", "class": "PM", "evidence_group": "metrics",
        "summary": "Inventory retains sessions until the database connection ceiling is approached.",
        "source_view": "prometheus_graph",
        "actions": [action("inventory", "connection-saturation")],
        "expected_alert": "LabMySQLConnectionsSaturated",
    },
    "lock-contention": {
        "title": "Inventory lock contention", "class": "PM", "evidence_group": "metrics",
        "summary": "Stock reconciliation holds a row while reservations wait and callers retry.",
        "actions": [action("inventory", "lock-contention")],
        "expected_alert": "LabInventoryLockContention",
        "acceptable_primary_alerts": ["LabInventoryLockContention", "LabInventoryAdmissionRejections"],
    },
    "downstream-latency": {
        "title": "Inventory latency amplification", "class": "PM", "evidence_group": "metrics",
        "summary": "Inventory completes successfully, but its added processing delay pushes checkout latency above its alert threshold.",
        "actions": [action("inventory", "downstream-latency")],
        "expected_alert": "LabCheckoutLatencyHigh",
        "acceptable_primary_alerts": ["LabCheckoutLatencyHigh", "LabInventoryDependencyLatencyHigh"],
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


# These probes are included in the operator catalog, but remain outside the frozen
# fifteen-case workload-diagnosis benchmark.
DISCOVERY_SCENARIOS = {
    "metrics-service-label-drift": {
        "title": "Metrics Service label drift", "class": "Discovery", "evidence_group": "discovery",
        "summary": "Metrics are no longer discovered even though the application pods remain healthy.",
        "source_view": "prometheus_targets",
        "service_metrics_label": "ture",
        "expected_alert": "LabApplicationMetricsDiscoveryMissing",
    },
    "mysql-exporter-scrape-path": {
        "title": "MySQL metrics target scrape failure", "class": "Discovery", "evidence_group": "discovery",
        "summary": "Prometheus discovers the exporter but cannot collect its metrics.",
        "source_view": "prometheus_targets",
        "runner_only": True,
        "expected_alert": "LabExporterScrapeFailed",
    },
}


def public_scenarios() -> dict[str, dict[str, Any]]:
    """Safe operator-facing catalog metadata without mutation settings or oracle labels."""
    scenarios = {}
    for key, item in {**SCENARIOS, **DISCOVERY_SCENARIOS}.items():
        evidence_group = item["evidence_group"]
        domains = {
            "logs": ["logs", "metrics"],
            "metrics": ["metrics", "logs"],
            "configuration": ["configuration", "logs", "metrics"],
            "discovery": ["metrics", "configuration"],
        }[evidence_group]
        scenarios[key] = {
            field: item[field] for field in ("title", "class", "evidence_group", "summary", "source_view")
            if field in item
        }
        scenarios[key]["evidence_domains"] = domains
        scenarios[key]["execution"] = "guarded_runner" if item.get("runner_only") else "controller"
        scenarios[key]["resource_profile"] = (
            "bounded_memory" if key == "memory-leak" else
            "bounded_cpu" if key == "cpu-saturation" else
            "bounded_database_sessions" if key == "mysql-connections" else "bounded"
        )
        scenarios[key]["screenshot_available"] = key == "mysql-exporter-scrape-path"
        scenarios[key]["runner_only"] = bool(item.get("runner_only"))
    return scenarios
