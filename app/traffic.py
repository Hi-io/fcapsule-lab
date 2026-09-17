"""Continuous checkout load generator with an HTTP health and Prometheus surface."""

from __future__ import annotations

import os
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from http import HTTPStatus
from urllib.error import HTTPError, URLError
from urllib.request import urlopen

from app.common import JsonLogger, QuietHandler, counter_line, gauge_line, serve


class TrafficState:
    def __init__(self) -> None:
        self.orders_url = os.environ["ORDERS_URL"].rstrip("/")
        self.rate = int(os.environ.get("REQUESTS_PER_SECOND", "120"))
        self.max_inflight = int(os.environ.get("MAX_INFLIGHT", "32"))
        self.logger = JsonLogger("traffic-generator")
        self.requests = {"200": 0, "503": 0, "transport_error": 0}
        self.inflight = 0
        self.shed = 0
        self._lock = threading.Lock()

    def claim_slot(self) -> bool:
        with self._lock:
            if self.inflight >= self.max_inflight:
                self.shed += 1
                return False
            self.inflight += 1
            return True

    def run_request(self) -> None:
        order_id = uuid.uuid4().hex
        try:
            try:
                with urlopen(f"{self.orders_url}/checkout?order_id={order_id}", timeout=3) as response:
                    response.read()
                    status = str(response.status)
            except HTTPError as exc:
                exc.read()
                status = str(exc.code)
            except (URLError, TimeoutError, OSError):
                status = "transport_error"
            if status not in self.requests:
                status = "503" if status.startswith("5") else "transport_error"
            with self._lock:
                self.requests[status] += 1
            self.logger.write(
                "INFO" if status == "200" else "WARN",
                "Synthetic checkout request completed",
                order_id=order_id,
                status=status,
            )
        finally:
            with self._lock:
                self.inflight = max(0, self.inflight - 1)

    def metrics(self) -> str:
        with self._lock:
            requests = dict(self.requests)
            inflight = self.inflight
            shed = self.shed
            logs = self.logger.count
        return "".join(
            [
                *(counter_line("traffic_generator_requests_total", "Synthetic checkout requests generated", value, status=status) for status, value in requests.items()),
                gauge_line("traffic_generator_inflight", "Synthetic checkout requests currently in flight", inflight),
                counter_line("traffic_generator_shed_requests_total", "Synthetic requests not started because the concurrency limit was reached", shed),
                gauge_line("traffic_generator_configured_rps", "Configured synthetic request rate per second", self.rate),
                counter_line("traffic_generator_log_events_total", "Structured traffic generator log events emitted", logs),
            ]
        )


def handler(state: TrafficState) -> type[QuietHandler]:
    class TrafficHandler(QuietHandler):
        def do_GET(self) -> None:  # noqa: N802
            if self.path == "/health":
                self.send_json(HTTPStatus.OK, {"status": "ok", "configured_rps": state.rate})
                return
            if self.path == "/metrics":
                self.send_text(HTTPStatus.OK, state.metrics())
                return
            self.send_json(HTTPStatus.NOT_FOUND, {"error": "not_found"})

    return TrafficHandler


def load_loop(state: TrafficState) -> None:
    interval = 1 / max(1, state.rate)
    next_start = time.monotonic()
    with ThreadPoolExecutor(max_workers=state.max_inflight) as executor:
        while True:
            now = time.monotonic()
            if now < next_start:
                time.sleep(next_start - now)
            if state.claim_slot():
                executor.submit(state.run_request)
            elif state.shed % 25 == 0:
                state.logger.write("WARN", "Synthetic request shed because max inflight limit was reached", max_inflight=state.max_inflight)
            next_start += interval
            if time.monotonic() - next_start > 1:
                next_start = time.monotonic()


def main() -> None:
    state = TrafficState()
    state.logger.write("INFO", "Traffic generator started", requests_per_second=state.rate, max_inflight=state.max_inflight)
    threading.Thread(target=load_loop, args=(state,), daemon=True).start()
    serve(handler(state), int(os.environ.get("PORT", "8082")))


if __name__ == "__main__":
    main()
