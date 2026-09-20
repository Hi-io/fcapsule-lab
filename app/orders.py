"""Orders API that turns an inventory database failure into realistic retry pressure."""

from __future__ import annotations

import json
import os
import threading
import time
import uuid
from http import HTTPStatus
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import parse_qs, urlencode, urlparse
from urllib.request import urlopen

from app.common import JsonLogger, QuietHandler, counter_line, gauge_line, percentile, serve


class OrdersState:
    def __init__(self) -> None:
        self.inventory_url = os.environ["INVENTORY_URL"].rstrip("/")
        self.timeout = float(os.environ.get("INVENTORY_TIMEOUT_SECONDS", "0.45"))
        self.max_retries = int(os.environ.get("MAX_RETRIES", "3"))
        self.logger = JsonLogger("orders-api")
        self.requests = {"200": 0, "503": 0}
        self.inventory_attempts = 0
        self.inventory_retries = 0
        self.inflight = 0
        self.latencies: list[float] = []
        self._lock = threading.Lock()

    def _attempt_inventory(self, order_id: str) -> int:
        endpoint = f"{self.inventory_url}/reserve?{urlencode({'order_id': order_id})}"
        try:
            with urlopen(endpoint, timeout=self.timeout) as response:
                response.read()
                return response.status
        except HTTPError as exc:
            exc.read()
            return exc.code
        except (URLError, TimeoutError, OSError):
            return HTTPStatus.SERVICE_UNAVAILABLE

    def checkout(self, order_id: str) -> tuple[int, dict[str, Any]]:
        request_id = uuid.uuid4().hex
        started = time.perf_counter()
        with self._lock:
            self.inflight += 1
        self.logger.write("INFO", "Checkout request accepted", request_id=request_id, order_id=order_id)
        status = HTTPStatus.SERVICE_UNAVAILABLE
        try:
            for attempt in range(1, self.max_retries + 1):
                with self._lock:
                    self.inventory_attempts += 1
                    if attempt > 1:
                        self.inventory_retries += 1
                status = self._attempt_inventory(order_id)
                if status == HTTPStatus.OK:
                    break
                self.logger.write(
                    "WARN",
                    "Inventory reservation attempt failed",
                    request_id=request_id,
                    order_id=order_id,
                    attempt=attempt,
                    max_attempts=self.max_retries,
                    inventory_status=int(status),
                    dependency="inventory-api", timeout_seconds=self.timeout,
                )
                time.sleep(0.006 * attempt)
        finally:
            duration = time.perf_counter() - started
            with self._lock:
                self.inflight = max(0, self.inflight - 1)
                self.latencies.append(duration)
                if len(self.latencies) > 5000:
                    self.latencies = self.latencies[-5000:]
                key = "200" if status == HTTPStatus.OK else "503"
                self.requests[key] += 1

        if status == HTTPStatus.OK:
            self.logger.write("INFO", "Checkout completed", request_id=request_id, order_id=order_id, duration_ms=round(duration * 1000, 2))
            return HTTPStatus.OK, {"status": "completed", "request_id": request_id}

        self.logger.write(
            "ERROR",
            "Checkout aborted after inventory retry budget exhausted",
            request_id=request_id,
            order_id=order_id,
            duration_ms=round(duration * 1000, 2),
            dependency="inventory-api",
        )
        return HTTPStatus.SERVICE_UNAVAILABLE, {"status": "inventory_unavailable", "request_id": request_id}

    def metrics(self) -> str:
        with self._lock:
            requests = dict(self.requests)
            attempts = self.inventory_attempts
            retries = self.inventory_retries
            inflight = self.inflight
            p95 = percentile(self.latencies)
            total = max(1, sum(requests.values()))
            amplification = attempts / total
            logs = self.logger.count
        return "".join(
            [
                *(counter_line("orders_checkout_requests_total", "Completed checkout requests", value, status=status) for status, value in requests.items()),
                counter_line("orders_checkout_failures_total", "Failed checkout requests", requests["503"]),
                counter_line("orders_inventory_attempts_total", "Inventory calls made by checkout", attempts),
                counter_line("orders_inventory_retries_total", "Inventory retry calls made by checkout", retries),
                gauge_line("orders_retry_amplification_ratio", "Inventory attempts per checkout request", amplification),
                gauge_line("orders_checkout_inflight", "Checkout requests currently in flight", inflight),
                gauge_line("orders_checkout_latency_p95_seconds", "Checkout p95 response time over local observation window", p95),
                counter_line("orders_log_events_total", "Structured orders log events emitted", logs),
            ]
        )


def handler(state: OrdersState) -> type[QuietHandler]:
    class OrdersHandler(QuietHandler):
        def do_GET(self) -> None:  # noqa: N802
            parsed = urlparse(self.path)
            if parsed.path == "/health":
                self.send_json(HTTPStatus.OK, {"status": "ok", "inventory_url": state.inventory_url})
                return
            if parsed.path == "/metrics":
                self.send_text(HTTPStatus.OK, state.metrics())
                return
            if parsed.path == "/checkout":
                order_id = parse_qs(parsed.query).get("order_id", [uuid.uuid4().hex])[0]
                status, payload = state.checkout(order_id)
                self.send_json(status, payload)
                return
            self.send_json(HTTPStatus.NOT_FOUND, {"error": "not_found"})

    return OrdersHandler


def main() -> None:
    state = OrdersState()
    state.logger.write("INFO", "Orders API started", inventory_url=state.inventory_url)
    serve(handler(state), int(os.environ.get("PORT", "8080")))


if __name__ == "__main__":
    main()
