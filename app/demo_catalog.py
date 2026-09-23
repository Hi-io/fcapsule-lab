"""Operator demo selections. Execution reuses the benchmark and probe mechanisms.

No evaluator answers belong here. This catalog is only served by the Lab, never
included in alert annotations, application telemetry or FCAPSule evidence notes.
"""

DEMO_CASES = {
    "connection-pressure": {
        "title": "Checkout under connection pressure", "class": "PM",
        "summary": "Checkout failures, database session use and the configured connection ceiling.",
        "scenario": "mysql-connections", "rounds": 1, "duration_seconds": 180,
        "evidence": ["logs", "metrics", "configuration", "external image"],
        "capture": {"view": "graph", "query":
            '{__name__=~"mysql_global_status_threads_connected|mysql_global_variables_max_connections",namespace="fcapsule-lab"}'},
    },
    "checkout-deadline": {
        "title": "Checkout deadlines exceeded", "class": "CM",
        "summary": "Caller timeouts, dependency timing and the active request budget.",
        "scenario": "timeout-budget", "rounds": 1, "duration_seconds": 180,
        "evidence": ["logs", "metrics", "configuration"],
    },
    "metrics-discovery": {
        "title": "Application metrics disappeared", "class": "Discovery",
        "summary": "Application availability and metrics coverage diverge.",
        "scenario": "metrics-service-label-drift", "rounds": 1, "duration_seconds": 180,
        "evidence": ["logs", "metrics", "configuration"],
    },
    "exporter-scrape": {
        "title": "Exporter target cannot be scraped", "class": "Discovery",
        "summary": "A discovered metrics target fails while the workload stays available.",
        "scenario": "mysql-exporter-scrape-path", "rounds": 1, "duration_seconds": 180,
        "evidence": ["logs", "metrics", "configuration", "external image"],
        "runner_only": True,
        "capture": {"view": "targets", "pool": "serviceMonitor/fcapsule-lab/fcapsule-lab-mysql/0"},
    },
    "query-rollout-history": {
        "title": "Inventory query failures recur", "class": "History",
        "summary": "Two separate incident windows, followed by retrieval of the earlier retained capsule.",
        "scenario": "schema-drift", "rounds": 2, "duration_seconds": 150,
        "evidence": ["logs", "metrics", "configuration", "retained history"],
    },
}


def public_demos():
    return {key: {field: item[field] for field in
                  ("title", "class", "summary", "scenario", "rounds", "duration_seconds", "evidence")}
            | {"runner_only": item.get("runner_only", False)}
            for key, item in DEMO_CASES.items()}
