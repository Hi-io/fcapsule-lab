"""Small control surface for starting and recovering lab incidents."""

from __future__ import annotations

import json
import os
from http import HTTPStatus
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlparse
from urllib.request import Request, urlopen

import pymysql

from app.common import JsonLogger, QuietHandler, serve


SCENARIOS = {
    "memory-leak": {
        "title": "Worker memory leak",
        "class": "FM",
        "summary": "A retained batch cache grows until Kubernetes OOM-kills and restarts the worker.",
        "target": "worker",
        "mode": "memory-leak",
    },
    "poison-job": {
        "title": "Poison job crash loop",
        "class": "FM",
        "summary": "A durable malformed job repeatedly terminates the worker until the queue item is removed.",
        "target": "database",
        "mode": "poison",
    },
    "cpu-saturation": {
        "title": "CPU saturation",
        "class": "PM",
        "summary": "A compute-bound batch consumes the worker CPU limit without terminating the pod.",
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
        "summary": "Concurrent reservations serialize on one InnoDB row and exceed the lock timeout.",
        "target": "inventory",
        "mode": "lock-contention",
    },
}


class ControlState:
    def __init__(self) -> None:
        self.worker_url = os.environ.get("WORKER_URL", "http://lab-worker:8083")
        self.inventory_url = os.environ.get("INVENTORY_URL", "http://inventory-api:8081")
        self.logger = JsonLogger("lab-control")
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

    def start(self, scenario_id: str) -> dict[str, Any]:
        scenario = SCENARIOS.get(scenario_id)
        if not scenario:
            raise ValueError(f"Unknown scenario: {scenario_id}")
        self.logger.write("WARN", "Lab incident requested", scenario=scenario_id, signal_class=scenario["class"])
        if scenario_id == "poison-job":
            with pymysql.connect(**self.db, connect_timeout=3, autocommit=True) as connection:
                with connection.cursor() as cursor:
                    cursor.execute(
                        "INSERT INTO lab_jobs (kind, payload) VALUES (%s, %s)",
                        ("poison", '{"schema":"order.created.v0","encoding":"invalid"}'),
                    )
            return {"ok": True, "scenario": scenario_id, "message": "Poison job added to the durable queue."}
        if scenario["target"] == "worker":
            result = self._post(self.worker_url + "/control/scenario", {"mode": scenario["mode"]})
        else:
            result = self._post(self.inventory_url + "/control/failure", {"mode": scenario["mode"]})
        return {"ok": True, "scenario": scenario_id, "result": result}

    def recover(self) -> dict[str, Any]:
        errors: list[str] = []
        try:
            self._post(self.worker_url + "/control/scenario", {"mode": "normal"})
        except (HTTPError, URLError, TimeoutError, OSError) as exc:
            errors.append(f"worker: {exc}")
        try:
            self._post(self.inventory_url + "/control/failure", {"mode": "normal"})
        except (HTTPError, URLError, TimeoutError, OSError) as exc:
            errors.append(f"inventory: {exc}")
        try:
            with pymysql.connect(**self.db, connect_timeout=3, autocommit=True) as connection:
                with connection.cursor() as cursor:
                    cursor.execute("DELETE FROM lab_jobs WHERE kind='poison'")
        except pymysql.MySQLError as exc:
            errors.append(f"queue: {exc}")
        self.logger.write("INFO", "Lab recovery requested", remaining_errors=errors)
        return {"ok": not errors, "errors": errors, "message": "Recovery applied. Kubernetes may need time to restart the worker."}

    def status(self) -> dict[str, Any]:
        return {
            "worker": self._health(self.worker_url),
            "inventory": self._health(self.inventory_url),
            "scenarios": SCENARIOS,
        }


HTML = r"""<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>FCAPSule Lab</title><style>
:root{--ink:#172129;--muted:#66747d;--line:#d4dadd;--paper:#fff;--bg:#f3f5f6;--nav:#11181d;--green:#167052;--amber:#9a5a12;--red:#a23838;--blue:#286a96}
*{box-sizing:border-box}body{margin:0;background:var(--bg);color:var(--ink);font:14px/1.45 Inter,"Segoe UI",Arial,sans-serif}header{height:58px;background:var(--nav);border-bottom:3px solid var(--green);color:#fff;display:flex;align-items:center;justify-content:space-between;padding:0 24px}header strong{font-size:20px}header strong span{color:#60bf9a}header small{color:#b9c3c8;text-transform:uppercase}main{width:min(1120px,calc(100% - 32px));margin:24px auto 48px}.head{display:flex;justify-content:space-between;align-items:end;margin-bottom:18px}.head h1{margin:0;font-size:26px}.head p{margin:4px 0 0;color:var(--muted)}button{border:1px solid #16583f;border-radius:2px;background:var(--green);color:#fff;min-height:36px;padding:7px 13px;font-weight:650;cursor:pointer}button.secondary{color:var(--ink);background:#fff;border-color:#aeb8be}.grid{display:grid;grid-template-columns:1fr 1fr;gap:12px}.scenario{background:var(--paper);border:1px solid var(--line);border-left:4px solid var(--blue);padding:15px}.scenario.fm{border-left-color:var(--red)}.scenario.pm{border-left-color:var(--amber)}.scenario h2{font-size:16px;margin:0 0 5px}.scenario p{color:var(--muted);margin:0 0 14px;min-height:42px}.tag{font-size:10px;font-weight:750;border:1px solid var(--line);padding:2px 5px;margin-left:6px}.actions{display:flex;gap:7px}.state{display:flex;gap:18px;align-items:center;margin-top:14px;background:#fff;border:1px solid var(--line);padding:12px}.dot{width:8px;height:8px;border-radius:50%;background:var(--green);display:inline-block;margin-right:6px}.dot.down{background:var(--red)}#notice{color:var(--muted);margin-left:auto}@media(max-width:720px){.grid{grid-template-columns:1fr}.head{align-items:start;flex-direction:column;gap:10px}.scenario p{min-height:0}.state{align-items:start;flex-direction:column;gap:7px}#notice{margin-left:0}}
</style></head><body><header><strong><span>FCAPS</span>ule Lab</strong><small>Failure control</small></header><main>
<div class="head"><div><h1>Incident scenarios</h1><p>Start one scenario at a time, then inspect its telemetry in FCAPSule.</p></div><button class="secondary" id="recover">Recover all</button></div>
<div class="grid" id="scenarios"></div><div class="state"><span><i class="dot" id="worker-dot"></i>Worker: <b id="worker-state">checking</b></span><span><i class="dot" id="inventory-dot"></i>Inventory: <b id="inventory-state">checking</b></span><span id="notice">Ready</span></div>
</main><script>
const safe=v=>String(v??'').replace(/[&<>'"]/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;',"'":'&#39;','"':'&quot;'}[c]));
let scenarios={};
async function status(){try{const r=await fetch('/api/status',{cache:'no-store'}),d=await r.json();scenarios=d.scenarios;document.querySelector('#scenarios').innerHTML=Object.entries(scenarios).map(([id,s])=>`<article class="scenario ${s.class.toLowerCase()}"><h2>${safe(s.title)}<span class="tag">${safe(s.class)}</span></h2><p>${safe(s.summary)}</p><div class="actions"><button data-start="${safe(id)}">Start scenario</button></div></article>`).join('');document.querySelectorAll('[data-start]').forEach(b=>b.onclick=()=>start(b.dataset.start));setState('worker',d.worker);setState('inventory',d.inventory)}catch(e){document.querySelector('#notice').textContent='Control API unavailable'}}
function setState(name,value){const up=value.reachable;document.querySelector(`#${name}-dot`).className='dot'+(up?'':' down');document.querySelector(`#${name}-state`).textContent=up?(value.mode||value.failure_mode||'healthy'):'unreachable'}
async function start(id){document.querySelector('#notice').textContent='Starting '+scenarios[id].title+'...';const r=await fetch('/api/scenarios/'+encodeURIComponent(id)+'/start',{method:'POST'}),d=await r.json();document.querySelector('#notice').textContent=d.message||d.error||(r.ok?'Scenario started':'Failed');setTimeout(status,1000)}
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
                    self.send_json(HTTPStatus.ACCEPTED, state.start(scenario_id))
                    return
                self.send_json(HTTPStatus.NOT_FOUND, {"error": "not_found"})
            except (ValueError, HTTPError, URLError, TimeoutError, OSError, pymysql.MySQLError) as exc:
                self.send_json(HTTPStatus.BAD_REQUEST, {"error": str(exc)})

    return ControlHandler


def main() -> None:
    serve(handler(ControlState()), int(os.environ.get("PORT", "8084")))


if __name__ == "__main__":
    main()
