"""Small control surface for starting and recovering lab incidents."""

from __future__ import annotations

import json
import os
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
from app.scenario_catalog import DEFAULT_SCENARIO_CONFIG, DISCOVERY_SCENARIOS, SCENARIOS
from app.safety import lease_seconds, memory_snapshot


class ControlState:
    def __init__(self) -> None:
        self.worker_url = os.environ.get("WORKER_URL", "http://lab-worker:8083")
        self.inventory_url = os.environ.get("INVENTORY_URL", "http://inventory-api:8081")
        self.orders_url = os.environ.get("ORDERS_URL", "http://orders-api:8080")
        self.logger = JsonLogger("lab-control")
        self.lock = threading.RLock()
        self.active = None
        self.history = []
        self.memory = None
        self.memory_error = None
        self.db = {
            "host": os.environ.get("MYSQL_HOST", "mysql"),
            "user": os.environ.get("MYSQL_USER", "inventory"),
            "password": os.environ.get("MYSQL_PASSWORD", "inventory-lab"),
            "database": os.environ.get("MYSQL_DATABASE", "inventory"),
        }

    def _post(self, url: str, payload: dict[str, Any]) -> dict[str, Any]:
        request = Request(
            url,
            data=json.dumps(payload).encode("utf-8"),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urlopen(request, timeout=4) as response:
            return json.loads(response.read())

    def _health(self, url: str) -> dict[str, Any]:
        try:
            with urlopen(url + "/health", timeout=2) as response:
                return {"reachable": True, **json.loads(response.read())}
        except (HTTPError, URLError, TimeoutError, OSError, json.JSONDecodeError) as exc:
            return {"reachable": False, "error": str(exc)}

    def start(self, scenario_id: str, duration: int = 180, request_id: str | None = None) -> dict[str, Any]:
        duration = lease_seconds(duration)
        if scenario_id not in {**SCENARIOS, **DISCOVERY_SCENARIOS}:
            raise ValueError(f"Unknown scenario: {scenario_id}")
        if request_id is not None and (not isinstance(request_id, str) or len(request_id) != 32
                                       or any(c not in "0123456789abcdef" for c in request_id)):
            raise ValueError("request_id must be a 32-character lowercase hexadecimal ID")
        with self.lock:
            if self.active:
                raise ValueError("A run is already active or recovering; recover it before starting another")
            self.memory = memory_snapshot()
            if self.memory["available_bytes"] < 1024 * 1024 * 1024:
                raise ValueError("Start blocked: node MemAvailable is below 1 GiB")
            if not all(self._health(url).get("reachable") for url in (self.worker_url, self.inventory_url, self.orders_url)):
                raise ValueError("Start blocked: wait until worker, inventory and orders are healthy")
            run = {"run_id": request_id or uuid.uuid4().hex, "scenario": scenario_id, "status": "starting",
                   "started_at": datetime.now(timezone.utc).isoformat(), "duration_seconds": duration,
                   "expires_at": time.time() + duration, "minimum_available_bytes": self.memory["available_bytes"]}
            self.active = run
            try:
                result = self._start(scenario_id, duration)
            except Exception:
                run["status"] = "recovering"
                self.recover("start_failed")
                raise
            run["status"] = "running"
            return {**result, "run": dict(run)}

    def _start(self, scenario_id: str, duration: int) -> dict[str, Any]:
        scenario = {**SCENARIOS, **DISCOVERY_SCENARIOS}.get(scenario_id)
        if not scenario:
            raise ValueError(f"Unknown scenario: {scenario_id}")
        config = {**DEFAULT_SCENARIO_CONFIG, **scenario.get("config", {})}
        if "config" in scenario:
            self._patch_scenario_config(config)
        target_count = len(scenario.get("actions", [])) + int("service_metrics_label" in scenario)
        self.logger.write(
            "WARN", "Bounded lab intervention started", run_id=self.active["run_id"], target_count=target_count,
        )
        results = []
        if scenario.get("service_metrics_label"):
            self._patch_metrics_service_label(str(scenario["service_metrics_label"]))
            self.logger.write(
                "WARN", "Metrics discovery intervention applied", run_id=self.active["run_id"],
                service="lab-app-metrics",
            )
            results.append({"target": "metrics-service", "status": "label updated"})
            return {"ok": True, "scenario": scenario_id, "results": results}
        for action in scenario["actions"]:
            target, mode = action["target"], action["mode"]
            if target == "database":
                with pymysql.connect(**self.db, connect_timeout=3, read_timeout=3, write_timeout=3, autocommit=True) as connection:
                    with connection.cursor() as cursor:
                        cursor.execute(
                            "INSERT INTO lab_jobs (kind, payload) VALUES (%s, %s)",
                            ("import", '{"schema":"inventory.import.v2","body":"not-valid-base64!"}'),
                        )
                results.append({"target": target, "status": "queued"})
                continue
            url, endpoint = {
                "worker": (self.worker_url, "/control/scenario"),
                "inventory": (self.inventory_url, "/control/failure"),
                "orders": (self.orders_url, "/control/scenario"),
            }[target]
            results.append(self._post(url + endpoint, {
                "mode": mode, "duration_seconds": duration, "settings": config,
            }))
        return {"ok": True, "scenario": scenario_id, "results": results}

    def _kubernetes_patch(self, resource: str, name: str, payload: dict[str, Any]) -> None:
        """Patch only the two bounded Lab resources managed by this control surface."""

        host = os.environ.get("KUBERNETES_SERVICE_HOST")
        if not host:
            return
        port = os.environ.get("KUBERNETES_SERVICE_PORT_HTTPS", "443")
        namespace = os.environ.get("POD_NAMESPACE", "fcapsule-lab")
        token = open("/var/run/secrets/kubernetes.io/serviceaccount/token", encoding="utf-8").read().strip()
        context = ssl.create_default_context(cafile="/var/run/secrets/kubernetes.io/serviceaccount/ca.crt")
        request = Request(
            f"https://{host}:{port}/api/v1/namespaces/{namespace}/{resource}/{name}",
            data=json.dumps(payload).encode("utf-8"), method="PATCH",
            headers={"Authorization": f"Bearer {token}", "Content-Type": "application/merge-patch+json"},
        )
        with urlopen(request, timeout=5, context=context) as response:
            if response.status >= 300:
                raise OSError(f"Kubernetes configuration update returned {response.status}")

    def _patch_scenario_config(self, values: dict[str, str]) -> None:
        """Persist active settings in Kubernetes so configuration evidence is real."""

        self._kubernetes_patch("configmaps", "lab-scenario-config", {"data": values})

    def _patch_metrics_service_label(self, value: str) -> None:
        """Alter the ServiceMonitor's actual Service selector target for a bounded run."""

        self._kubernetes_patch(
            "services", "lab-app-metrics", {"metadata": {"labels": {"fcapsule.io/app-metrics": value}}},
        )

    def recover(self, reason: str = "operator", expected_run_id: str | None = None) -> dict[str, Any]:
        with self.lock:
            if expected_run_id is not None:
                if not self.active:
                    return {"ok": True, "message": "No active run; no recovery writes performed."}
                if self.active["run_id"] != expected_run_id:
                    raise ValueError("Run ownership changed; refusing to recover another operator's run")
            return self._recover(reason)

    def _recover(self, reason: str) -> dict[str, Any]:
        errors: list[str] = []
        try:
            self._patch_scenario_config(DEFAULT_SCENARIO_CONFIG)
        except (HTTPError, URLError, TimeoutError, OSError) as exc:
            errors.append(f"kubernetes config: {exc}")
        try:
            self._patch_metrics_service_label("true")
        except (HTTPError, URLError, TimeoutError, OSError) as exc:
            errors.append(f"kubernetes: {exc}")
        try:
            self._post(self.inventory_url + "/control/failure", {"mode": "normal", "settings": DEFAULT_SCENARIO_CONFIG})
        except (HTTPError, URLError, TimeoutError, OSError) as exc:
            errors.append(f"inventory: {exc}")
        try:
            with pymysql.connect(**self.db, connect_timeout=3, read_timeout=3, write_timeout=3, autocommit=True) as connection:
                with connection.cursor() as cursor:
                    cursor.execute("DELETE FROM lab_jobs WHERE kind IN ('poison', 'import')")
        except pymysql.MySQLError as exc:
            errors.append(f"queue: {exc}")
        try:
            self._post(self.worker_url + "/control/scenario", {"mode": "normal"})
        except (HTTPError, URLError, TimeoutError, OSError) as exc:
            errors.append(f"worker: {exc}")
        try:
            self._post(self.orders_url + "/control/scenario", {"mode": "normal", "settings": DEFAULT_SCENARIO_CONFIG})
        except (HTTPError, URLError, TimeoutError, OSError) as exc:
            errors.append(f"orders: {exc}")
        if self.active:
            self.active.update(status="recovering" if errors else "recovered", recovery_reason=reason)
            if not errors:
                self.active["finished_at"] = datetime.now(timezone.utc).isoformat()
                self.history.append(dict(self.active))
                self.history = self.history[-20:]
                self.active = None
        self.logger.write("INFO", "Lab recovery requested", remaining_errors=errors)
        return {"ok": not errors, "errors": errors, "message": "Recovery complete." if not errors else "Recovery pending; waiting for workload health."}

    def watchdog(self):
        while True:
            try:
                memory = memory_snapshot()
                with self.lock:
                    self.memory, self.memory_error = memory, None
                    if self.active:
                        self.active["minimum_available_bytes"] = min(memory["available_bytes"], self.active["minimum_available_bytes"])
                        if memory["available_bytes"] < 768 * 1024 * 1024:
                            self.recover("low_host_memory")
                        elif time.time() >= self.active["expires_at"] or self.active["status"] == "recovering":
                            self.recover(self.active.get("recovery_reason", "lease_expired"))
            except (OSError, ValueError, KeyError, TimeoutError) as exc:
                with self.lock:
                    self.memory_error = type(exc).__name__
                    if self.active:
                        self.recover("memory_measurement_unavailable")
            time.sleep(5)

    def status(self) -> dict[str, Any]:
        return {
            "worker": self._health(self.worker_url),
            "inventory": self._health(self.inventory_url),
            "orders": self._health(self.orders_url),
            "scenarios": {
                key: {field: value for field, value in item.items()
                      if field not in {"actions", "config", "expected_alert", "service_metrics_label"}}
                for key, item in {**SCENARIOS, **DISCOVERY_SCENARIOS}.items()
            },
            "active": self.active,
            "history": self.history,
            "memory": self.memory,
            "memory_error": self.memory_error,
            "demos": public_demos(),
            "capabilities": {"owned_runs": True, "demo_catalog_version": 1},
            "prometheus_url": os.environ.get("PROMETHEUS_PUBLIC_URL", "http://192.168.0.102:30090"),
        }


HTML = r"""<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>FCAPSule Lab</title><style>
:root{--ink:#172129;--muted:#66747d;--line:#d4dadd;--paper:#fff;--bg:#f3f5f6;--nav:#11181d;--green:#167052;--amber:#9a5a12;--red:#a23838;--blue:#286a96}
*{box-sizing:border-box}body{margin:0;background:var(--bg);color:var(--ink);font:14px/1.45 Inter,"Segoe UI",Arial,sans-serif}header{height:58px;background:var(--nav);border-bottom:3px solid var(--green);color:#fff;display:flex;align-items:center;justify-content:space-between;padding:0 24px}header strong{font-size:20px}header strong span{color:#60bf9a}header small{color:#b9c3c8;text-transform:uppercase}main{width:min(1120px,calc(100% - 32px));margin:24px auto 48px}.head{display:flex;justify-content:space-between;align-items:end;margin-bottom:18px}.head h1{margin:0;font-size:26px}.head p{margin:4px 0 0;color:var(--muted)}button{border:1px solid #16583f;border-radius:2px;background:var(--green);color:#fff;min-height:36px;padding:7px 13px;font-weight:650;cursor:pointer}button.secondary{color:var(--ink);background:#fff;border-color:#aeb8be}.grid{display:grid;grid-template-columns:1fr 1fr;gap:12px}.scenario{background:var(--paper);border:1px solid var(--line);border-left:4px solid var(--blue);padding:15px}.scenario.fm{border-left-color:var(--red)}.scenario.pm{border-left-color:var(--amber)}.scenario h2{font-size:16px;margin:0 0 5px}.scenario p{color:var(--muted);margin:0 0 14px;min-height:42px}.tag{font-size:10px;font-weight:750;border:1px solid var(--line);padding:2px 5px;margin-left:6px}.actions{display:flex;gap:7px}.state{display:flex;gap:18px;align-items:center;margin-top:14px;background:#fff;border:1px solid var(--line);padding:12px}.dot{width:8px;height:8px;border-radius:50%;background:var(--green);display:inline-block;margin-right:6px}.dot.down{background:var(--red)}#notice{color:var(--muted);margin-left:auto}@media(max-width:720px){.grid{grid-template-columns:1fr}.head{align-items:start;flex-direction:column;gap:10px}.scenario p{min-height:0}.state{align-items:start;flex-direction:column;gap:7px}#notice{margin-left:0}}
button:disabled{background:#e8edef;color:#5b6870;border-color:#cbd3d7;cursor:not-allowed}button:focus-visible,select:focus-visible{outline:2px solid var(--blue);outline-offset:3px}.scenario.active{border-color:var(--green);border-left-width:4px}.actions{align-items:center;flex-wrap:wrap}select{min-height:36px;border:1px solid #aeb8be;background:#fff;color:var(--ink);padding:5px}header small{font-size:10px}
.dot{background:var(--muted)}.dot.up{background:var(--green)}.actions label{max-width:100%}select{max-width:100%}
</style></head><body><header><strong><span>FCAPS</span>ule Lab</strong><small>Failure control</small></header><main>
<div class="head"><div><h1>Incident scenarios</h1><p id="run-state">Checking node headroom</p></div><div class="actions"><label>Duration <select id="duration"><option value="120">2 minutes</option><option value="180" selected>3 minutes</option><option value="300">5 minutes</option></select></label><button class="secondary" id="recover">Recover all</button></div></div>
<div class="actions" style="margin-bottom:16px"><label>Catalog <select id="catalog"><option value="demos">Operator demos (5)</option><option value="scenarios">All workload cases and discovery probe</option></select></label></div>
<div class="grid" id="scenarios" aria-busy="true"></div><div class="state"><span><i class="dot" id="worker-dot"></i>Worker: <b id="worker-state">checking</b></span><span><i class="dot" id="inventory-dot"></i>Inventory: <b id="inventory-state">checking</b></span><span id="notice" role="status" aria-live="polite">Loading</span></div>
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
