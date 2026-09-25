"""Load the bounded incident library without mixing it with qualified demos."""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any


MECHANISMS = frozenset({
    "db_unique", "db_foreign_key", "db_locked", "db_schema",
    "http_timeout", "http_status", "http_contract", "dns_lookup", "tcp_refused",
    "queue_poison", "queue_backlog", "auth_expired", "auth_signature",
    "config_missing", "config_invalid", "cache_stale", "file_missing",
    "resource_memory", "resource_cpu", "rate_limit", "thread_pool",
})
FIELDS = frozenset({
    "id", "title", "category", "service", "mechanism", "summary",
    "precursor", "impact", "parameters", "context",
})
CASE_DIR = Path(__file__).with_name("library_cases")


def load_cases(directory: Path = CASE_DIR) -> dict[str, dict[str, Any]]:
    cases: dict[str, dict[str, Any]] = {}
    files = sorted(directory.glob("batch_[0-9][0-9].json"))
    for path in files:
        batch = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(batch, list) or len(batch) != 5:
            raise ValueError(f"{path.name} must contain exactly five cases")
        prefix = path.stem.replace("batch_", "lib-") + "-"
        for item in batch:
            if not isinstance(item, dict) or set(item) != FIELDS:
                raise ValueError(f"{path.name}: case fields do not match the library contract")
            if (not isinstance(item["id"], str) or not item["id"].startswith(prefix)
                    or not re.fullmatch(r"lib-[0-9]{2}-[a-z0-9]+(?:-[a-z0-9]+)*", item["id"])):
                raise ValueError(f"{path.name}: invalid case ID")
            if item["id"] in cases:
                raise ValueError(f"Duplicate library case {item['id']}")
            if item["mechanism"] not in MECHANISMS:
                raise ValueError(f"{item['id']}: unsupported mechanism")
            for field in ("title", "category", "service", "summary", "precursor", "impact"):
                if not isinstance(item[field], str) or not 3 <= len(item[field].strip()) <= 300:
                    raise ValueError(f"{item['id']}: invalid {field}")
            if not isinstance(item["parameters"], dict) or not isinstance(item["context"], dict):
                raise ValueError(f"{item['id']}: parameters and context must be objects")
            if not 2 <= len(item["context"]) <= 4:
                raise ValueError(f"{item['id']}: context must contain 2-4 fields")
            if any(not isinstance(key, str) or not isinstance(value, (str, int, float, bool))
                   for key, value in item["context"].items()):
                raise ValueError(f"{item['id']}: context must be scalar")
            cases[item["id"]] = item
    return cases


def public_cases(cases: dict[str, dict[str, Any]]) -> dict[str, dict[str, str]]:
    return {key: {field: item[field] for field in
                  ("title", "category", "service", "summary", "impact")}
            for key, item in cases.items()}
