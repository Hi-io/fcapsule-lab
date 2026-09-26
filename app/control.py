"""Small control surface for starting and recovering lab incidents."""

from __future__ import annotations

import json
import os
import re
import ssl
import threading
import time
import uuid
from datetime import datetime, timezone
from http import HTTPStatus
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlparse
from urllib.request import Request, urlopen

import pymysql

from app.common import JsonLogger, QuietHandler, serve
from app.demo_catalog import public_demos
from app.incident_library import load_cases, public_cases
from app.scenario_catalog import DEFAULT_SCENARIO_CONFIG, DISCOVERY_SCENARIOS, OPERATOR_SCENARIOS, SCENARIOS, public_scenarios
from app.safety import lease_seconds, memory_snapshot


CONTROL_OWNER = "fcapsule.io/lab-control-run"
EXTERNAL_PROBE_OWNER = "fcapsule.lab/screenshot-run"
CONTROL_TARGETS = "fcapsule.io/lab-control-targets"
CONTROL_BASELINE = "fcapsule.io/lab-control-baseline-settings"
CONTROL_EXPIRY = "fcapsule.io/lab-control-expires-at"
CONTROL_JOB_ID = "fcapsule.io/lab-control-job-id"
FIELD_OWNER = "fcapsule.io/lab-control-field-owner"
FIELD_BASELINE = "fcapsule.io/lab-control-field-baseline"
FIELD_APPLIED = "fcapsule.io/lab-control-field-applied"
CONTROLLED_RESOURCES = (("configmaps", "lab-scenario-config"), ("services", "lab-app-metrics"),
                        ("servicemonitors", "fcapsule-lab-mysql"))
class ControlState:
    def __init__(self) -> None:
        self.worker_url = os.environ.get("WORKER_URL", "http://lab-worker:8083")
        self.inventory_url = os.environ.get("INVENTORY_URL", "http://inventory-api:8081")
        self.orders_url = os.environ.get("ORDERS_URL", "http://orders-api:8080")
        self.cnfc_edge_url = os.environ.get("CNFC_EDGE_URL", "http://cnfc-edge-a:8085")
        self.library_url = os.environ.get("LIBRARY_URL", "http://lab-incident-library:8085")
        self.library_cases = load_cases()
        self.logger = JsonLogger("lab-control")
        self.lock = threading.RLock()
        self.active = None
        self.history = []
        self.recovery_error = None
        self.memory = None
        self.memory_error = None
        self.db = {
            "host": os.environ.get("MYSQL_HOST", "mysql"),
            "user": os.environ.get("MYSQL_USER", "inventory"),
            "password": os.environ.get("MYSQL_PASSWORD", "inventory-lab"),
            "database": os.environ.get("MYSQL_DATABASE", "inventory"),
        }

    def _kubernetes_request(self, method: str, resource: str, name: str,
                            payload: dict[str, Any] | None = None) -> dict[str, Any] | None:
        host = os.environ.get("KUBERNETES_SERVICE_HOST")
        if not host:
            return None
        port = os.environ.get("KUBERNETES_SERVICE_PORT_HTTPS", "443")
        namespace = os.environ.get("POD_NAMESPACE", "fcapsule-lab")
        token = Path("/var/run/secrets/kubernetes.io/serviceaccount/token").read_text(encoding="utf-8").strip()
        context = ssl.create_default_context(cafile="/var/run/secrets/kubernetes.io/serviceaccount/ca.crt")
        headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}
        body = None
        if payload is not None:
            body = json.dumps(payload).encode("utf-8")
            headers["Content-Type"] = "application/merge-patch+json"
        api = "apis/monitoring.coreos.com/v1" if resource == "servicemonitors" else "api/v1"
        request = Request(
            f"https://{host}:{port}/{api}/namespaces/{namespace}/{resource}/{name}",
            data=body, method=method, headers=headers,
        )
        with urlopen(request, timeout=5, context=context) as response:
            if response.status >= 300:
                raise OSError(f"Kubernetes {method} {resource}/{name} returned {response.status}")
            raw = response.read()
            return json.loads(raw) if raw else {}

    def _kubernetes_get(self, resource: str, name: str) -> dict[str, Any] | None:
        return self._kubernetes_request("GET", resource, name)

    def _kubernetes_patch(self, resource: str, name: str, payload: dict[str, Any]) -> None:
        """Patch the two bounded Lab resources with resourceVersion preconditions."""

        self._kubernetes_request("PATCH", resource, name, payload)

    def _claim_run(self, run: dict[str, Any], scenario: dict[str, Any]) -> dict[str, str]:
        """Persist restart-recovery ownership before any scenario mutation."""
        current = self._kubernetes_get("configmaps", "lab-scenario-config")
        baseline = dict(DEFAULT_SCENARIO_CONFIG)
        if current:
            annotations = current.get("metadata", {}).get("annotations", {})
            existing_owner = annotations.get(CONTROL_OWNER)
            if existing_owner:
                raise ValueError("A persisted Lab run already owns scenario resources; recover it first")
            if annotations.get(EXTERNAL_PROBE_OWNER):
                raise ValueError("The external Prometheus screenshot run owns the Lab; recover it first")
            baseline = {key: str(current.get("data", {}).get(key, default))
                        for key, default in DEFAULT_SCENARIO_CONFIG.items()}
            patch = {
                "metadata": {"resourceVersion": current.get("metadata", {}).get("resourceVersion"),
                    "annotations": {
                        CONTROL_OWNER: run["run_id"],
                        CONTROL_TARGETS: json.dumps(run["targets"], separators=(",", ":")),
                        CONTROL_BASELINE: json.dumps(baseline, sort_keys=True, separators=(",", ":")),
                        CONTROL_EXPIRY: str(int(run["expires_at"])),
                    }},
            }
            self._kubernetes_patch("configmaps", "lab-scenario-config", patch)
        run["baseline_settings"] = baseline
        return baseline

    def _external_probe_status(self) -> dict[str, Any]:
        try:
            current = self._kubernetes_get("configmaps", "lab-scenario-config")
        except (OSError, TimeoutError, ValueError, KeyError) as exc:
            return {"available": False, "active": False, "error": type(exc).__name__}
        if current is None:
            return {"available": False, "active": False}
        owner = current.get("metadata", {}).get("annotations", {}).get(EXTERNAL_PROBE_OWNER)
        return {"available": True, "active": bool(owner), "owner": owner}

    def _claim_fields(self, resource: str, name: str, section: str, updates: dict[str, Any]) -> None:
        current = self._kubernetes_get(resource, name)
        if current is None:
            return
        owner = self.active["run_id"]
        annotations = dict(current.get("metadata", {}).get("annotations", {}))
        existing = annotations.get(FIELD_OWNER)
        if existing and existing != owner:
            raise ValueError(f"{resource}/{name} is owned by a different Lab intervention")
        if resource == "configmaps" and annotations.get(CONTROL_OWNER) != owner:
            raise ValueError("Scenario ConfigMap ownership was lost before mutation")
        target = (current.get("metadata", {}).get("labels", {}) if section == "labels"
                  else current.get(section, {}))
        baseline = json.loads(annotations.get(FIELD_BASELINE, "{}")) if existing == owner else {}
        applied = json.loads(annotations.get(FIELD_APPLIED, "{}")) if existing == owner else {}
        for key, value in updates.items():
            if key not in baseline:
                baseline[key] = target.get(key)
            applied[key] = value if section == "spec" else str(value)
        annotations.update({
            FIELD_OWNER: owner,
            FIELD_BASELINE: json.dumps(baseline, sort_keys=True, separators=(",", ":")),
            FIELD_APPLIED: json.dumps(applied, sort_keys=True, separators=(",", ":")),
        })
        metadata_patch = {"resourceVersion": current.get("metadata", {}).get("resourceVersion"),
                          "annotations": annotations}
        if section == "labels":
            metadata_patch["labels"] = {key: str(value) for key, value in updates.items()}
            patch = {"metadata": metadata_patch}
        else:
            patch = {"metadata": metadata_patch, section: {
                key: value if section == "spec" else str(value) for key, value in updates.items()}}
        self._kubernetes_patch(resource, name, patch)

    def _restore_owned_fields(self, resource: str, name: str, section: str,
                              expected_owner: str | None) -> None:
        current = self._kubernetes_get(resource, name)
        if current is None:
            return
        metadata = current.get("metadata", {})
        annotations = dict(metadata.get("annotations", {}))
        owner = annotations.get(FIELD_OWNER)
        if resource == "configmaps" and annotations.get(CONTROL_OWNER) != expected_owner:
            raise ValueError(f"{resource}/{name} run ownership changed; preserving current value")
        if not owner:
            return
        if expected_owner is not None and owner != expected_owner:
            raise ValueError(f"{resource}/{name} ownership changed; preserving current value")
        baseline = json.loads(annotations.get(FIELD_BASELINE, "{}"))
        applied = json.loads(annotations.get(FIELD_APPLIED, "{}"))
        target = (metadata.get("labels", {}) if section == "labels" else current.get(section, {}))
        restore = {}
        for key, value in applied.items():
            if target.get(key) != value:
                raise ValueError(f"{resource}/{name} field {key} changed during the run; preserving operator edit")
            restore[key] = baseline.get(key)
        for key in (FIELD_OWNER, FIELD_BASELINE, FIELD_APPLIED):
            annotations[key] = None
        data_patch = {key: value for key, value in restore.items()}
        metadata_patch = {"resourceVersion": metadata.get("resourceVersion"), "annotations": annotations}
        if section == "labels":
            metadata_patch["labels"] = data_patch
            patch = {"metadata": metadata_patch}
        else:
            patch = {"metadata": metadata_patch, section: data_patch}
        self._kubernetes_patch(resource, name, patch)

    def _clear_run_claim(self, expected_owner: str) -> None:
        current = self._kubernetes_get("configmaps", "lab-scenario-config")
        if current is None:
            return
        metadata = current.get("metadata", {})
        annotations = dict(metadata.get("annotations", {}))
        if annotations.get(CONTROL_OWNER) != expected_owner:
            raise ValueError("Persisted Lab run ownership changed; recovery journal retained")
        for key in (CONTROL_OWNER, CONTROL_TARGETS, CONTROL_BASELINE, CONTROL_EXPIRY, CONTROL_JOB_ID):
            annotations[key] = None
        self._kubernetes_patch("configmaps", "lab-scenario-config", {
            "metadata": {"resourceVersion": metadata.get("resourceVersion"), "annotations": annotations},
        })

    def _persisted_run(self) -> dict[str, Any] | None:
        current = self._kubernetes_get("configmaps", "lab-scenario-config")
        annotations = current.get("metadata", {}).get("annotations", {}) if current else {}
        owner = annotations.get(CONTROL_OWNER)
        if not owner:
            for resource, name in CONTROLLED_RESOURCES:
                orphan = self._kubernetes_get(resource, name)
                if orphan and orphan.get("metadata", {}).get("annotations", {}).get(FIELD_OWNER):
                    raise ValueError(f"Orphaned owned-field journal on {resource}/{name}; refusing to overwrite it")
            return None
        if not re.fullmatch(r"[0-9a-f]{32}", owner):
            raise ValueError("Persisted Lab run identity is invalid; refusing automatic recovery")
        try:
            targets = json.loads(annotations[CONTROL_TARGETS])
            baseline = json.loads(annotations[CONTROL_BASELINE])
            expires_at = int(annotations[CONTROL_EXPIRY])
        except (KeyError, TypeError, ValueError, json.JSONDecodeError) as exc:
            raise ValueError("Persisted Lab recovery journal is incomplete; refusing broad cleanup") from exc
        if not isinstance(targets, list) or not all(isinstance(item, str) for item in targets):
            raise ValueError("Persisted Lab recovery targets are invalid")
        if not isinstance(baseline, dict):
            raise ValueError("Persisted Lab baseline settings are invalid")
        return {"run_id": owner, "targets": targets, "baseline_settings": baseline,
                "job_id": int(annotations[CONTROL_JOB_ID]) if annotations.get(CONTROL_JOB_ID) else None,
                "expires_at": expires_at, "status": "recovering", "recovered_after_restart": True}

    def _persist_job_id(self, owner: str, job_id: int) -> None:
        current = self._kubernetes_get("configmaps", "lab-scenario-config")
        if current is None:
            return
        metadata = current.get("metadata", {})
        annotations = dict(metadata.get("annotations", {}))
        if annotations.get(CONTROL_OWNER) != owner:
            raise ValueError("Lab run ownership changed before the queue item was recorded")
        annotations[CONTROL_JOB_ID] = str(job_id)
        self._kubernetes_patch("configmaps", "lab-scenario-config", {
            "metadata": {"resourceVersion": metadata.get("resourceVersion"), "annotations": annotations},
        })

    def _post(self, url: str, payload: dict[str, Any]) -> dict[str, Any]:
        request = Request(
            url,
            data=json.dumps(payload).encode("utf-8"),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urlopen(request, timeout=4) as response:
            return json.loads(response.read())

    def _post_for_run(self, url: str, payload: dict[str, Any], run_id: str) -> dict[str, Any]:
        response = self._post(url, {**payload, "run_id": run_id})
        if not isinstance(response, dict) or response.get("run_id") != run_id:
            raise ValueError("Application did not acknowledge the active Lab run ID")
        return response

    def _health(self, url: str) -> dict[str, Any]:
        try:
            with urlopen(url + "/health", timeout=2) as response:
                return {"reachable": True, **json.loads(response.read())}
        except (HTTPError, URLError, TimeoutError, OSError, json.JSONDecodeError) as exc:
            return {"reachable": False, "error": str(exc)}

    def start(self, scenario_id: str, duration: int = 180, request_id: str | None = None) -> dict[str, Any]:
        duration = lease_seconds(duration)
        catalog = {**SCENARIOS, **DISCOVERY_SCENARIOS, **OPERATOR_SCENARIOS}
        if scenario_id not in catalog and scenario_id not in self.library_cases:
            raise ValueError(f"Unknown scenario: {scenario_id}")
        if (scenario_id in catalog and (catalog[scenario_id].get("runner_only")
                or not catalog[scenario_id].get("actions")
                and not catalog[scenario_id].get("service_metrics_label")
                and not catalog[scenario_id].get("scrape_path"))):
            raise ValueError("This scenario requires its guarded external runner")
        if request_id is not None and (not isinstance(request_id, str) or len(request_id) != 32
                                       or any(c not in "0123456789abcdef" for c in request_id)):
            raise ValueError("request_id must be a 32-character lowercase hexadecimal ID")
        with self.lock:
            if self.active:
                raise ValueError("A run is already active or recovering; recover it before starting another")
            self.memory = memory_snapshot()
            if not self.memory.get("node_identity_verified"):
                raise ValueError("Start blocked: node-specific memory source is not verified")
            if self.memory["available_bytes"] < 1024 * 1024 * 1024:
                raise ValueError("Start blocked: node MemAvailable is below 1 GiB")
            if not all(self._health(url).get("reachable") for url in (self.worker_url, self.inventory_url, self.orders_url)):
                raise ValueError("Start blocked: wait until worker, inventory and orders are healthy")
            if scenario_id in self.library_cases and not self._health(self.library_url).get("reachable"):
                raise ValueError("Start blocked: incident library workload is unavailable")
            if self._persisted_run():
                raise ValueError("A persisted Lab intervention needs recovery before another run")
            scenario = catalog.get(scenario_id, self.library_cases.get(scenario_id))
            targets = (["library"] if scenario_id in self.library_cases else
                       [action["target"] for action in scenario.get("actions", [])])
            if scenario.get("service_metrics_label"):
                targets.append("metrics-service")
            if scenario.get("scrape_path"):
                targets.append("exporter-monitor")
            targets = list(dict.fromkeys(targets))
            run = {"run_id": request_id or uuid.uuid4().hex, "scenario": scenario_id, "status": "starting",
                   "started_at": datetime.now(timezone.utc).isoformat(), "duration_seconds": duration,
                   "expires_at": time.time() + duration, "minimum_available_bytes": self.memory["available_bytes"],
                   "targets": targets}
            self.active = run
            claimed = False
            try:
                self._claim_run(run, scenario)
                claimed = True
                run["claim_acquired"] = True
                result = self._start(scenario_id, duration)
            except Exception:
                if not claimed:
                    try:
                        persisted = self._persisted_run()
                        claimed = bool(persisted and persisted.get("run_id") == run["run_id"])
                    except (HTTPError, URLError, TimeoutError, OSError, ValueError):
                        pass
                if claimed:
                    run["status"] = "recovering"
                    run["claim_acquired"] = True
                    self.recover("start_failed", expected_run_id=run["run_id"])
                else:
                    self.active = None
                raise
            run["status"] = "running"
            return {**result, "run": {key: value for key, value in run.items()
                                      if key not in {"baseline_settings", "claim_acquired"}}}

    def _start(self, scenario_id: str, duration: int) -> dict[str, Any]:
        if scenario_id in self.library_cases:
            result = self._post_for_run(self.library_url + "/control/scenario", {
                "mode": scenario_id, "duration_seconds": duration,
            }, self.active["run_id"])
            return {"ok": True, "scenario": scenario_id, "results": [{"target": "library", **result}]}
        scenario = {**SCENARIOS, **DISCOVERY_SCENARIOS, **OPERATOR_SCENARIOS}.get(scenario_id)
        if not scenario:
            raise ValueError(f"Unknown scenario: {scenario_id}")
        if scenario.get("runner_only"):
            raise ValueError("This scenario requires its guarded external runner")
        baseline = self.active.get("baseline_settings", DEFAULT_SCENARIO_CONFIG)
        config = {**baseline, **scenario.get("config", {})}
        if "config" in scenario:
            self._patch_scenario_config(scenario["config"])
        target_count = len(scenario.get("actions", [])) + int("service_metrics_label" in scenario)
        self.logger.write(
            "WARN", "Bounded lab intervention started", run_id=self.active["run_id"], target_count=target_count,
        )
        results = []
        if scenario.get("scrape_path"):
            monitor = self._kubernetes_get("servicemonitors", "fcapsule-lab-mysql")
            endpoints = monitor.get("spec", {}).get("endpoints", []) if monitor else []
            if len(endpoints) != 1 or endpoints[0].get("path", "/metrics") != "/metrics":
                raise ValueError("Expected one healthy exporter endpoint at /metrics; no change applied")
            if monitor.get("metadata", {}).get("annotations", {}).get(EXTERNAL_PROBE_OWNER):
                raise ValueError("The external screenshot runner owns this monitor")
            changed = [{**endpoints[0], "path": scenario["scrape_path"]}]
            self._claim_fields("servicemonitors", "fcapsule-lab-mysql", "spec", {"endpoints": changed})
            return {"ok": True, "scenario": scenario_id, "results": [{"target": "exporter-monitor", "status": "updated"}]}
        if scenario.get("service_metrics_label"):
            self._patch_metrics_service_label(str(scenario["service_metrics_label"]))
            self.logger.write(
                "WARN", "Bounded intervention applied", run_id=self.active["run_id"],
                target_count=1,
            )
            results.append({"target": "metrics-service", "status": "label updated"})
            return {"ok": True, "scenario": scenario_id, "results": results}
        for action in scenario["actions"]:
            target, mode = action["target"], action["mode"]
            if target == "database":
                with pymysql.connect(**self.db, connect_timeout=3, read_timeout=3, write_timeout=3, autocommit=True) as connection:
                    with connection.cursor() as cursor:
                        cursor.execute(
                            "INSERT INTO lab_jobs (kind, payload, owner_run_id) VALUES (%s, %s, %s)",
                            ("import", '{"schema":"inventory.import.v2","body":"not-valid-base64!"}',
                             self.active["run_id"]),
                        )
                    job_id = getattr(cursor, "lastrowid", None)
                if job_id is not None:
                    self.active["job_id"] = int(job_id)
                    self._persist_job_id(self.active["run_id"], int(job_id))
                results.append({"target": target, "status": "queued", "job_id": job_id})
                continue
            url, endpoint = {
                "worker": (self.worker_url, "/control/scenario"),
                "inventory": (self.inventory_url, "/control/failure"),
                "orders": (self.orders_url, "/control/scenario"),
                "cnfc-edge": (self.cnfc_edge_url, "/control/scenario"),
            }[target]
            results.append(self._post_for_run(url + endpoint, {
                "mode": mode, "duration_seconds": duration, "settings": config,
            }, self.active["run_id"]))
        return {"ok": True, "scenario": scenario_id, "results": results}

    def _patch_scenario_config(self, values: dict[str, str]) -> None:
        """Apply only changed keys while retaining an owned, reversible field journal."""
        self._claim_fields("configmaps", "lab-scenario-config", "data", values)

    def _patch_metrics_service_label(self, value: str) -> None:
        """Alter the ServiceMonitor's actual Service selector target for a bounded run."""

        self._claim_fields("services", "lab-app-metrics", "labels", {"fcapsule.io/app-metrics": value})

    def recover(self, reason: str = "operator", expected_run_id: str | None = None) -> dict[str, Any]:
        with self.lock:
            if expected_run_id is not None:
                current_owner = self.active.get("run_id") if self.active else None
                if current_owner is None:
                    persisted = self._persisted_run()
                    current_owner = persisted.get("run_id") if persisted else None
                if current_owner is None:
                    return {"ok": True, "message": "No active run; no recovery writes performed."}
                if current_owner != expected_run_id:
                    raise ValueError("Run ownership changed; refusing to recover another operator's run")
            return self._recover(reason)

    def _recover(self, reason: str) -> dict[str, Any]:
        errors: list[str] = []
        if self.active is None:
            try:
                self.active = self._persisted_run()
            except (HTTPError, URLError, TimeoutError, OSError, ValueError) as exc:
                self.recovery_error = str(exc)
                return {"ok": False, "errors": [f"recovery journal: {exc}"],
                        "message": "Recovery journal could not be validated; no broad cleanup performed."}
        if self.active is None:
            self.recovery_error = None
            return {"ok": True, "errors": [], "message": "No owned Lab run; no recovery writes performed."}
        if self.active.get("claim_acquired") is False:
            return {"ok": True, "errors": [], "message": "No owned Lab run; no recovery writes performed."}
        owner = self.active["run_id"]
        targets = set(self.active.get("targets", []))
        baseline = self.active.get("baseline_settings", DEFAULT_SCENARIO_CONFIG)
        if "exporter-monitor" in targets:
            try:
                self._restore_owned_fields("servicemonitors", "fcapsule-lab-mysql", "spec", owner)
            except (HTTPError, URLError, TimeoutError, OSError, ValueError, json.JSONDecodeError) as exc:
                errors.append(f"exporter monitor: {exc}")
        config_restored = True
        try:
            self._restore_owned_fields("configmaps", "lab-scenario-config", "data", owner)
        except (HTTPError, URLError, TimeoutError, OSError, ValueError, json.JSONDecodeError) as exc:
            config_restored = False
            errors.append(f"kubernetes config: {exc}")
        try:
            self._restore_owned_fields("services", "lab-app-metrics", "labels", owner)
        except (HTTPError, URLError, TimeoutError, OSError, ValueError, json.JSONDecodeError) as exc:
            errors.append(f"kubernetes: {exc}")
        if "inventory" in targets and config_restored:
            try:
                self._post_for_run(self.inventory_url + "/control/failure",
                                   {"mode": "normal", "settings": baseline}, owner)
            except (HTTPError, URLError, TimeoutError, OSError) as exc:
                errors.append(f"inventory: {exc}")
            except ValueError as exc:
                errors.append(f"inventory: {exc}")
        if "database" in targets:
            try:
                with pymysql.connect(**self.db, connect_timeout=3, read_timeout=3, write_timeout=3, autocommit=True) as connection:
                    with connection.cursor() as cursor:
                        job_id = self.active.get("job_id")
                        if job_id is not None:
                            cursor.execute("DELETE FROM lab_jobs WHERE id=%s AND owner_run_id=%s", (job_id, owner))
                        else:
                            cursor.execute("DELETE FROM lab_jobs WHERE owner_run_id=%s", (owner,))
            except pymysql.MySQLError as exc:
                errors.append(f"queue: {exc}")
        if "worker" in targets:
            try:
                self._post_for_run(self.worker_url + "/control/scenario", {"mode": "normal"}, owner)
            except (HTTPError, URLError, TimeoutError, OSError) as exc:
                errors.append(f"worker: {exc}")
            except ValueError as exc:
                errors.append(f"worker: {exc}")
        if "orders" in targets and config_restored:
            try:
                self._post_for_run(self.orders_url + "/control/scenario",
                                   {"mode": "normal", "settings": baseline}, owner)
            except (HTTPError, URLError, TimeoutError, OSError) as exc:
                errors.append(f"orders: {exc}")
            except ValueError as exc:
                errors.append(f"orders: {exc}")
        if "cnfc-edge" in targets:
            try:
                self._post_for_run(self.cnfc_edge_url + "/control/scenario", {"mode": "normal"}, owner)
            except (HTTPError, URLError, TimeoutError, OSError, ValueError) as exc:
                errors.append(f"cnfc-edge: {exc}")
        if "library" in targets:
            try:
                self._post_for_run(self.library_url + "/control/scenario", {"mode": "normal"}, owner)
            except (HTTPError, URLError, TimeoutError, OSError, ValueError) as exc:
                errors.append(f"library: {exc}")
        if not errors:
            try:
                self._clear_run_claim(owner)
            except (HTTPError, URLError, TimeoutError, OSError, ValueError) as exc:
                errors.append(f"recovery journal: {exc}")
        if self.active:
            self.active.update(status="recovering" if errors else "recovered", recovery_reason=reason)
            if not errors:
                self.recovery_error = None
                self.active["finished_at"] = datetime.now(timezone.utc).isoformat()
                self.history.append({key: value for key, value in self.active.items()
                                     if key not in {"baseline_settings", "claim_acquired"} and not key.startswith("_")})
                self.history = self.history[-20:]
                self.active = None
            else:
                self.recovery_error = "; ".join(errors)
        self.logger.write("INFO", "Lab recovery requested", remaining_errors=errors)
        return {"ok": not errors, "errors": errors, "message": "Recovery complete." if not errors else "Recovery pending; waiting for workload health."}

    def watchdog(self):
        while True:
            try:
                memory = memory_snapshot()
                if not memory.get("node_identity_verified"):
                    raise ValueError("Node-specific memory source could not be verified")
                recover_reason = None
                with self.lock:
                    self.memory, self.memory_error = memory, None
                    if self.active:
                        self.active["minimum_available_bytes"] = min(memory["available_bytes"], self.active["minimum_available_bytes"])
                        if memory["available_bytes"] < 768 * 1024 * 1024:
                            recover_reason = "low_host_memory"
                        elif time.time() >= self.active["expires_at"] or self.active["status"] == "recovering":
                            recover_reason = self.active.get("recovery_reason", "lease_expired")
                    else:
                        persisted = self._persisted_run()
                        if persisted:
                            self.active = {
                                **persisted,
                                "minimum_available_bytes": memory["available_bytes"],
                                "claim_acquired": True,
                                "recovery_reason": "controller_restart",
                            }
                            recover_reason = "controller_restart"
                    if recover_reason:
                        self.recover(recover_reason)
            except (OSError, ValueError, KeyError, TimeoutError) as exc:
                with self.lock:
                    self.memory_error = type(exc).__name__
                    if self.active:
                        self.recover("memory_measurement_unavailable")
            time.sleep(5)

    def status(self) -> dict[str, Any]:
        library_public = {
            key: {**item, "class": "Library", "evidence_domains": ["logs", "metrics"],
                  "track": "library", "execution": "controller", "resource_profile": "bounded",
                  "runner_only": False, "screenshot_available": False}
            for key, item in public_cases(self.library_cases).items()
        }
        return {
            "worker": self._health(self.worker_url),
            "inventory": self._health(self.inventory_url),
            "orders": self._health(self.orders_url),
            "library": self._health(self.library_url),
            "scenarios": {**public_scenarios(), **library_public},
            "active": ({key: value for key, value in self.active.items()
                        if key not in {"baseline_settings", "claim_acquired"} and not key.startswith("_")}
                       if self.active else None),
            "history": self.history,
            "recovery_error": self.recovery_error,
            "memory": self.memory,
            "memory_error": self.memory_error,
            "external_probe": self._external_probe_status(),
            "demos": public_demos(),
            "capabilities": {"owned_runs": True, "scenario_catalog_version": 2},
            "prometheus_url": os.environ.get("PROMETHEUS_PUBLIC_URL", "http://192.168.0.102:30090"),
        }


HTML = r"""<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>FCAPSule Lab</title><style>
:root{--ink:#172129;--muted:#66747d;--line:#d4dadd;--paper:#fff;--bg:#f3f5f6;--nav:#11181d;--green:#167052;--amber:#9a5a12;--red:#a23838;--blue:#286a96}
*{box-sizing:border-box}body{margin:0;background:var(--bg);color:var(--ink);font:14px/1.45 Inter,"Segoe UI",Arial,sans-serif}header{height:58px;background:var(--nav);border-bottom:3px solid var(--green);color:#fff;display:flex;align-items:center;justify-content:space-between;padding:0 24px}header strong{font-size:20px}header strong span{color:#60bf9a}header small{color:#b9c3c8;text-transform:uppercase}main{width:min(1120px,calc(100% - 32px));margin:24px auto 48px}.head{display:flex;justify-content:space-between;align-items:end;margin-bottom:18px}.head h1{margin:0;font-size:26px}.head p{margin:4px 0 0;color:var(--muted)}button{border:1px solid #16583f;border-radius:2px;background:var(--green);color:#fff;min-height:36px;padding:7px 13px;font-weight:650;cursor:pointer}button.secondary{color:var(--ink);background:#fff;border-color:#aeb8be}.grid{display:grid;grid-template-columns:1fr 1fr;gap:12px}.scenario{background:var(--paper);border:1px solid var(--line);border-left:4px solid var(--blue);padding:15px}.scenario.fm{border-left-color:var(--red)}.scenario.pm{border-left-color:var(--amber)}.scenario h2{font-size:16px;margin:0 0 5px}.scenario p{color:var(--muted);margin:0 0 14px;min-height:42px}.tag{font-size:10px;font-weight:750;border:1px solid var(--line);padding:2px 5px;margin-left:6px}.actions{display:flex;gap:7px}.state{display:flex;gap:18px;align-items:center;margin-top:14px;background:#fff;border:1px solid var(--line);padding:12px}.dot{width:8px;height:8px;border-radius:50%;background:var(--green);display:inline-block;margin-right:6px}.dot.down{background:var(--red)}#notice{color:var(--muted);margin-left:auto}@media(max-width:720px){.grid{grid-template-columns:1fr}.head{align-items:start;flex-direction:column;gap:10px}.scenario p{min-height:0}.state{align-items:start;flex-direction:column;gap:7px}#notice{margin-left:0}}
button:disabled{background:#e8edef;color:#5b6870;border-color:#cbd3d7;cursor:not-allowed}button:focus-visible,select:focus-visible{outline:2px solid var(--blue);outline-offset:3px}.scenario.active{border-color:var(--green);border-left-width:4px}.actions{align-items:center;flex-wrap:wrap}select{min-height:36px;border:1px solid #aeb8be;background:#fff;color:var(--ink);padding:5px}header small{font-size:10px}.scenario-evidence{display:block;color:var(--muted);margin:-7px 0 12px;text-transform:capitalize}
.dot{background:var(--muted)}.dot.up{background:var(--green)}.actions label{max-width:100%}select{max-width:100%}
.track-title{font-size:18px;margin:0 0 10px}.development-track{margin-top:22px}.development-track>summary{cursor:pointer;font-weight:700;padding:10px 0;border-top:1px solid var(--line)}.development-track>summary:focus-visible{outline:2px solid var(--blue);outline-offset:3px}.track-count{color:var(--muted);font-weight:400;margin-left:6px}
.library-tools{display:flex;gap:8px;flex-wrap:wrap;padding:5px 0 13px}.library-tools input{min-height:36px;border:1px solid #aeb8be;padding:6px 9px;flex:1;min-width:180px}.library-tools select{min-width:160px}.library-tools input:focus-visible{outline:2px solid var(--blue);outline-offset:3px}
.scenario details{margin-top:14px;border-top:1px solid var(--line);padding-top:10px}.scenario details summary{cursor:pointer;color:var(--blue);font-weight:600}.scenario details p{min-height:0;margin:10px 0;overflow-wrap:anywhere}.scenario summary:focus-visible{outline:2px solid var(--blue);outline-offset:3px}
</style></head><body><header><strong><span>FCAPS</span>ule Lab</strong><small>Failure control</small></header><main>
<div class="head"><div><h1>Incident scenarios</h1><p id="run-state">Checking node headroom</p></div><div class="actions"><label>Duration <select id="duration"><option value="120">2 minutes</option><option value="180" selected>3 minutes</option><option value="300">5 minutes</option></select></label><button class="secondary" id="recover">Recover all</button></div></div>
<div id="scenario-catalog" aria-busy="true"><section aria-labelledby="demo-track-heading"><h2 class="track-title" id="demo-track-heading">Demo track</h2><div class="grid" id="demo-scenarios"></div></section><details class="development-track"><summary>Development backlog<span class="track-count" id="development-count"></span></summary><div class="grid" id="development-scenarios"></div></details><details class="development-track"><summary>Incident library<span class="track-count" id="library-count"></span></summary><div class="library-tools"><input id="library-search" type="search" placeholder="Search cases" aria-label="Search library cases"><select id="library-category" aria-label="Library category"><option value="">All categories</option></select></div><div class="grid" id="library-scenarios"></div></details></div><div class="state"><span><i class="dot" id="worker-dot"></i>Worker: <b id="worker-state">checking</b></span><span><i class="dot" id="inventory-dot"></i>Inventory: <b id="inventory-state">checking</b></span><span><i class="dot" id="orders-dot"></i>Orders: <b id="orders-state">checking</b></span><span id="notice" role="status" aria-live="polite">Loading</span></div>
</main><script src="/assets/control.js"></script></body></html>"""


def handler(state: ControlState) -> type[QuietHandler]:
    class ControlHandler(QuietHandler):
        def do_GET(self) -> None:  # noqa: N802
            path = urlparse(self.path).path
            if path in {"/", "/console"}:
                self.send_html(HTTPStatus.OK, HTML)
                return
            if path == "/health":
                self.send_json(HTTPStatus.OK, {"status": "ok"})
                return
            if path == "/api/status":
                self.send_json(HTTPStatus.OK, state.status())
                return
            if path.startswith("/api/demos/") and path.endswith("/plan"):
                demo_id = path.removeprefix("/api/demos/").removesuffix("/plan").strip("/")
                demo = public_demos().get(demo_id)
                if not demo:
                    self.send_json(HTTPStatus.NOT_FOUND, {"error": "Unknown demo"})
                    return
                self.send_json(HTTPStatus.OK, {"demo": demo, "execution": "explicit CLI after coordination",
                    "command": f"python tools/run_operator_demos.py run --case {demo_id} --lab-node NODE --execute --out local_reports/demo-UNIQUE",
                    "screenshot_review": "Inspect real Prometheus pixels before explicit attach; no model retry is automatic."})
                return
            if path == "/api/scenarios/mysql-exporter-scrape-path/plan":
                demo = public_demos()["exporter-scrape"]
                self.send_json(HTTPStatus.OK, {"scenario": "mysql-exporter-scrape-path", "execution": "guarded external runner",
                    "command": "python tools/run_operator_demos.py run --case exporter-scrape --lab-node NODE --execute --out local_reports/run-UNIQUE",
                    "screenshot_review": "Capture real Prometheus Targets pixels during the failed scrape and review before attaching."})
                return
            if path == "/assets/control.js":
                data = Path(__file__).with_name("control.js").read_bytes()
                self.send_response(HTTPStatus.OK)
                self.send_header("Content-Type", "text/javascript; charset=utf-8")
                self.send_header("Content-Length", str(len(data)))
                self.end_headers()
                self.wfile.write(data)
                return
            self.send_json(HTTPStatus.NOT_FOUND, {"error": "not_found"})

        def do_POST(self) -> None:  # noqa: N802
            path = urlparse(self.path).path
            try:
                if path == "/api/recover":
                    self.send_json(HTTPStatus.OK, state.recover(expected_run_id=self.body_json().get("expected_run_id")))
                    return
                if path.startswith("/api/scenarios/") and path.endswith("/start"):
                    scenario_id = path.removeprefix("/api/scenarios/").removesuffix("/start").strip("/")
                    payload = self.body_json()
                    self.send_json(HTTPStatus.ACCEPTED, state.start(scenario_id, payload.get("duration_seconds", 180), payload.get("request_id")))
                    return
                self.send_json(HTTPStatus.NOT_FOUND, {"error": "not_found"})
            except (ValueError, HTTPError, URLError, TimeoutError, OSError, pymysql.MySQLError) as exc:
                self.send_json(HTTPStatus.BAD_REQUEST, {"error": str(exc)})

    return ControlHandler


def main() -> None:
    state = ControlState()
    state.recover("control_startup")
    threading.Thread(target=state.watchdog, daemon=True).start()
    serve(handler(state), int(os.environ.get("PORT", "8084")))


if __name__ == "__main__":
    main()
