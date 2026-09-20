"""Small control surface for starting and recovering lab incidents."""

from __future__ import annotations

import json
import os
import threading
import time
import uuid
from datetime import datetime, timezone
from http import HTTPStatus
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlparse
from urllib.request import Request, urlopen

import pymysql

from app.common import JsonLogger, QuietHandler, serve
from app.safety import lease_seconds, memory_snapshot


SCENARIOS = {
    "memory-leak": {
        "title": "Buffered report export",
        "class": "FM",
        "summary": "An export buffers pages instead of streaming them, then reaches its container memory limit.",
        "target": "worker",
        "mode": "memory-leak",
    },
    "poison-job": {
        "title": "Incompatible import message",
        "class": "FM",
        "summary": "A durable import exposes an unhandled decoder exception before acknowledgement.",
        "target": "database",
        "mode": "poison",
    },
    "cpu-saturation": {
        "title": "Credential migration backlog",
        "class": "PM",
        "summary": "A migration applies expensive password derivation to every record under a small CPU quota.",
        "target": "worker",
        "mode": "cpu-saturation",
    },
    "mysql-connections": {
        "title": "MySQL connection saturation",
        "class": "PM",
        "summary": "The inventory pool retains sessions near max_connections and checkout failures rise.",
        "target": "inventory",
        "mode": "connection-saturation",
    },
    "lock-contention": {
        "title": "Inventory lock contention",
        "class": "PM",
        "summary": "Stock reconciliation keeps a transaction open while reservations wait and callers retry.",
        "target": "inventory",
        "mode": "lock-contention",
    },
    "schema-drift": {
        "title": "Inventory schema mismatch", "class": "PM",
        "summary": "A new query is enabled before its database migration; MySQL rejects real reservations.",
        "target": "inventory", "mode": "schema-drift",
    },
}


class ControlState:
    def __init__(self) -> None:
        self.worker_url = os.environ.get("WORKER_URL", "http://lab-worker:8083")
        self.inventory_url = os.environ.get("INVENTORY_URL", "http://inventory-api:8081")
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

    def start(self, scenario_id: str, duration: int = 180) -> dict[str, Any]:
        duration = lease_seconds(duration)
        with self.lock:
            if self.active:
                raise ValueError("A run is already active or recovering; recover it before starting another")
            self.memory = memory_snapshot()
            if self.memory["available_bytes"] < 1024 * 1024 * 1024:
                raise ValueError("Start blocked: node MemAvailable is below 1 GiB")
            if not all(self._health(url).get("reachable") for url in (self.worker_url, self.inventory_url)):
                raise ValueError("Start blocked: wait until both workloads are healthy")
            run = {"run_id": uuid.uuid4().hex, "scenario": scenario_id, "status": "starting",
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
        scenario = SCENARIOS.get(scenario_id)
        if not scenario:
            raise ValueError(f"Unknown scenario: {scenario_id}")
        self.logger.write("WARN", "Lab incident requested", scenario=scenario_id, signal_class=scenario["class"])
        if scenario_id == "poison-job":
            with pymysql.connect(**self.db, connect_timeout=3, autocommit=True) as connection:
                with connection.cursor() as cursor:
                    cursor.execute(
                        "INSERT INTO lab_jobs (kind, payload) VALUES (%s, %s)",
                        ("import", '{"schema":"inventory.import.v2","body":"not-valid-base64!"}'),
                    )
            return {"ok": True, "scenario": scenario_id, "message": "Poison job added to the durable queue."}
        if scenario["target"] == "worker":
            result = self._post(self.worker_url + "/control/scenario", {"mode": scenario["mode"], "duration_seconds": duration})
        else:
            result = self._post(self.inventory_url + "/control/failure", {"mode": scenario["mode"], "duration_seconds": duration})
        return {"ok": True, "scenario": scenario_id, "result": result}

    def recover(self, reason: str = "operator") -> dict[str, Any]:
        with self.lock:
            return self._recover(reason)

    def _recover(self, reason: str) -> dict[str, Any]:
        errors: list[str] = []
        try:
            self._post(self.inventory_url + "/control/failure", {"mode": "normal"})
        except (HTTPError, URLError, TimeoutError, OSError) as exc:
            errors.append(f"inventory: {exc}")
        try:
            with pymysql.connect(**self.db, connect_timeout=3, autocommit=True) as connection:
                with connection.cursor() as cursor:
                    cursor.execute("DELETE FROM lab_jobs WHERE kind IN ('poison', 'import')")
        except pymysql.MySQLError as exc:
            errors.append(f"queue: {exc}")
        try:
            self._post(self.worker_url + "/control/scenario", {"mode": "normal"})
        except (HTTPError, URLError, TimeoutError, OSError) as exc:
            errors.append(f"worker: {exc}")
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
            "scenarios": SCENARIOS,
            "active": self.active,
            "history": self.history,
            "memory": self.memory,
            "memory_error": self.memory_error,
        }


HTML = r"""<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>FCAPSule Lab</title><style>
:root{--ink:#172129;--muted:#66747d;--line:#d4dadd;--paper:#fff;--bg:#f3f5f6;--nav:#11181d;--green:#167052;--amber:#9a5a12;--red:#a23838;--blue:#286a96}
*{box-sizing:border-box}body{margin:0;background:var(--bg);color:var(--ink);font:14px/1.45 Inter,"Segoe UI",Arial,sans-serif}header{height:58px;background:var(--nav);border-bottom:3px solid var(--green);color:#fff;display:flex;align-items:center;justify-content:space-between;padding:0 24px}header strong{font-size:20px}header strong span{color:#60bf9a}header small{color:#b9c3c8;text-transform:uppercase}main{width:min(1120px,calc(100% - 32px));margin:24px auto 48px}.head{display:flex;justify-content:space-between;align-items:end;margin-bottom:18px}.head h1{margin:0;font-size:26px}.head p{margin:4px 0 0;color:var(--muted)}button{border:1px solid #16583f;border-radius:2px;background:var(--green);color:#fff;min-height:36px;padding:7px 13px;font-weight:650;cursor:pointer}button.secondary{color:var(--ink);background:#fff;border-color:#aeb8be}.grid{display:grid;grid-template-columns:1fr 1fr;gap:12px}.scenario{background:var(--paper);border:1px solid var(--line);border-left:4px solid var(--blue);padding:15px}.scenario.fm{border-left-color:var(--red)}.scenario.pm{border-left-color:var(--amber)}.scenario h2{font-size:16px;margin:0 0 5px}.scenario p{color:var(--muted);margin:0 0 14px;min-height:42px}.tag{font-size:10px;font-weight:750;border:1px solid var(--line);padding:2px 5px;margin-left:6px}.actions{display:flex;gap:7px}.state{display:flex;gap:18px;align-items:center;margin-top:14px;background:#fff;border:1px solid var(--line);padding:12px}.dot{width:8px;height:8px;border-radius:50%;background:var(--green);display:inline-block;margin-right:6px}.dot.down{background:var(--red)}#notice{color:var(--muted);margin-left:auto}@media(max-width:720px){.grid{grid-template-columns:1fr}.head{align-items:start;flex-direction:column;gap:10px}.scenario p{min-height:0}.state{align-items:start;flex-direction:column;gap:7px}#notice{margin-left:0}}
</style></head><body><header><strong><span>FCAPS</span>ule Lab</strong><small>Failure control</small></header><main>
<div class="head"><div><h1>Incident scenarios</h1><p id="run-state">Checking node headroom</p></div><div class="actions"><label>Duration <select id="duration"><option value="120">2 minutes</option><option value="180" selected>3 minutes</option><option value="300">5 minutes</option></select></label><button class="secondary" id="recover">Recover all</button></div></div>
<div class="grid" id="scenarios"></div><div class="state"><span><i class="dot" id="worker-dot"></i>Worker: <b id="worker-state">checking</b></span><span><i class="dot" id="inventory-dot"></i>Inventory: <b id="inventory-state">checking</b></span><span id="notice">Ready</span></div>
</main><script>
const safe=v=>String(v??'').replace(/[&<>'"]/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;',"'":'&#39;','"':'&quot;'}[c]));
let scenarios={};
async function status(){try{const r=await fetch('/api/status',{cache:'no-store'}),d=await r.json();scenarios=d.scenarios;const blocked=d.active||d.memory_error||!d.memory||d.memory.available_bytes<1073741824;document.querySelector('#run-state').textContent=(d.memory?'Node available: '+(d.memory.available_bytes/1073741824).toFixed(2)+' GiB':'Memory check unavailable')+(d.active?' | '+scenarios[d.active.scenario].title+' | '+d.active.status+' | '+Math.max(0,Math.ceil(d.active.expires_at-Date.now()/1000))+'s remaining':' | No active run');document.querySelector('#scenarios').innerHTML=Object.entries(scenarios).map(([id,s])=>`<article class="scenario ${s.class.toLowerCase()}"><h2>${safe(s.title)}<span class="tag">${safe(s.class)}</span></h2><p>${safe(s.summary)}</p><div class="actions"><button data-start="${safe(id)}" ${blocked?'disabled':''}>${d.active?.scenario===id?'Running':'Start scenario'}</button></div></article>`).join('');document.querySelectorAll('[data-start]').forEach(b=>b.onclick=()=>start(b.dataset.start));setState('worker',d.worker);setState('inventory',d.inventory)}catch(e){document.querySelector('#notice').textContent='Control API unavailable'}}
function setState(name,value){const up=value.reachable;document.querySelector(`#${name}-dot`).className='dot'+(up?'':' down');document.querySelector(`#${name}-state`).textContent=up?(value.mode||value.failure_mode||'healthy'):'unreachable'}
async function start(id){document.querySelector('#notice').textContent='Starting '+scenarios[id].title+'...';document.querySelectorAll('[data-start]').forEach(b=>b.disabled=true);try{const r=await fetch('/api/scenarios/'+encodeURIComponent(id)+'/start',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({duration_seconds:Number(document.querySelector('#duration').value)})}),d=await r.json();document.querySelector('#notice').textContent=d.message||d.error||(r.ok?'Scenario started':'Failed')}catch(e){document.querySelector('#notice').textContent='Start could not be confirmed; refreshing status'}status()}
document.querySelector('#recover').onclick=async()=>{document.querySelector('#notice').textContent='Applying recovery...';const r=await fetch('/api/recover',{method:'POST'}),d=await r.json();document.querySelector('#notice').textContent=d.message||d.error;setTimeout(status,1500)};
status();setInterval(status,5000);
</script></body></html>"""


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
            self.send_json(HTTPStatus.NOT_FOUND, {"error": "not_found"})

        def do_POST(self) -> None:  # noqa: N802
            path = urlparse(self.path).path
            try:
                if path == "/api/recover":
                    self.send_json(HTTPStatus.OK, state.recover())
                    return
                if path.startswith("/api/scenarios/") and path.endswith("/start"):
                    scenario_id = path.removeprefix("/api/scenarios/").removesuffix("/start").strip("/")
                    self.send_json(HTTPStatus.ACCEPTED, state.start(scenario_id, self.body_json().get("duration_seconds", 180)))
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
