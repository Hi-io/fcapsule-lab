"""Orders API that turns an inventory database failure into realistic retry pressure."""

from __future__ import annotations

import json
import os
import socket
import threading
import time
import uuid
from http import HTTPStatus
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import parse_qs, urlencode, urlparse
from urllib.request import Request, urlopen

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
        self.mode = "normal"
        self.signing_key_id = os.environ.get("ORDER_SIGNING_KEY_ID", "checkout-key-v1")
        self.expected_schema = os.environ.get("ORDER_EXPECTED_SCHEMA", "v1")
        self.dependency_failures = {"transport": 0, "timeout": 0, "authorization": 0,
                                    "contract_shape": 0, "contract_version": 0}
        self.internal_failures = {"idempotency_conflict": 0}
        self._idempotency: dict[str, str] = {}
        self._expires_at = 0.0
        self._lock = threading.Lock()
        threading.Thread(target=self._lease_loop, daemon=True).start()

    def set_mode(self, mode: str, duration: int = 180, settings: dict[str, Any] | None = None) -> None:
        if mode not in {"normal", "configured", "idempotency-conflict"}:
            raise ValueError(f"Unknown orders mode: {mode}")
        values = settings or {}
        with self._lock:
            self.mode = mode
            self.inventory_url = str(values.get("INVENTORY_URL", os.environ.get("INVENTORY_URL", "http://inventory-api:8081"))).rstrip("/")
            self.timeout = float(values.get("INVENTORY_TIMEOUT_SECONDS", "1.5"))
            self.max_retries = int(values.get("MAX_RETRIES", "3"))
            self.signing_key_id = str(values.get("ORDER_SIGNING_KEY_ID", "checkout-key-v1"))
            self.expected_schema = str(values.get("ORDER_EXPECTED_SCHEMA", "v1"))
            self._idempotency = {}
            self._expires_at = time.monotonic() + max(30, min(300, int(duration))) if mode != "normal" else 0.0
        self.logger.write("INFO", "Checkout runtime configuration reloaded", configuration_revision=self.expected_schema,
                          dependency_host=urlparse(self.inventory_url).hostname)

    def _lease_loop(self) -> None:
        while True:
            time.sleep(1)
            with self._lock:
                expired = self._expires_at and time.monotonic() >= self._expires_at
            if expired:
                self.set_mode("normal")

    def _dependency_failure(self, kind: str) -> None:
        with self._lock:
            self.dependency_failures[kind] += 1

    def _attempt_inventory(self, order_id: str) -> int:
        endpoint = f"{self.inventory_url}/reserve?{urlencode({'order_id': order_id})}"
        try:
            request = Request(endpoint, headers={"X-Signing-Key-Id": self.signing_key_id})
            with urlopen(request, timeout=self.timeout) as response:
                body = response.read()
                status = response.status
            try:
                payload = json.loads(body)
            except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                self._dependency_failure("contract_shape")
                self.logger.write("ERROR", "Inventory response could not be decoded", order_id=order_id,
                                  error_type=type(exc).__name__, response_bytes=len(body))
                return HTTPStatus.BAD_GATEWAY
            valid = (payload.get("status") == "reserved" if self.expected_schema == "v1"
                     else (payload.get("reservation") or {}).get("status") == "reserved")
            if status == HTTPStatus.OK and not valid:
                kind = "contract_version" if payload.get("schema") and payload.get("schema") != self.expected_schema else "contract_shape"
                self._dependency_failure(kind)
                self.logger.write("ERROR", "Inventory response violated checkout contract", order_id=order_id,
                                  upstream_status=int(status), expected_schema=self.expected_schema,
                                  observed_schema=payload.get("schema", "unspecified"), observed_fields=sorted(payload),
                                  consumer_decision="reject_as_bad_gateway")
                return HTTPStatus.BAD_GATEWAY
            return status
        except HTTPError as exc:
            exc.read()
            if exc.code in {HTTPStatus.UNAUTHORIZED, HTTPStatus.FORBIDDEN}:
                self._dependency_failure("authorization")
            return exc.code
        except (TimeoutError, socket.timeout):
            self._dependency_failure("timeout")
            self.logger.write("WARN", "Inventory request exceeded dependency timeout", order_id=order_id,
                              timeout_seconds=self.timeout, dependency_host=urlparse(self.inventory_url).hostname)
            return HTTPStatus.SERVICE_UNAVAILABLE
        except URLError as exc:
            kind = "timeout" if isinstance(exc.reason, (TimeoutError, socket.timeout)) else "transport"
            self._dependency_failure(kind)
            self.logger.write("WARN", "Inventory request transport failed", order_id=order_id,
                              error_type=type(exc.reason).__name__, dependency_host=urlparse(self.inventory_url).hostname)
            return HTTPStatus.SERVICE_UNAVAILABLE
        except OSError as exc:
            self._dependency_failure("transport")
            self.logger.write("WARN", "Inventory request transport failed", order_id=order_id,
                              error_type=type(exc).__name__, dependency_host=urlparse(self.inventory_url).hostname)
            return HTTPStatus.SERVICE_UNAVAILABLE

    def checkout(self, order_id: str) -> tuple[int, dict[str, Any]]:
        request_id = uuid.uuid4().hex
        started = time.perf_counter()
        with self._lock:
            self.inflight += 1
        self.logger.write("INFO", "Checkout request accepted", request_id=request_id, order_id=order_id)
        with self._lock:
            mode = self.mode
            if mode == "idempotency-conflict":
                key = "checkout-window-active"
                previous = self._idempotency.get(key)
                self._idempotency.setdefault(key, order_id)
                if previous and previous != order_id:
                    self.internal_failures["idempotency_conflict"] += 1
                    self.inflight = max(0, self.inflight - 1)
                    self.requests["503"] += 1
                    self.logger.write("ERROR", "Idempotency record conflicts with request payload",
                                      request_id=request_id, idempotency_key=key,
                                      original_order_id=previous, conflicting_order_id=order_id)
                    return HTTPStatus.SERVICE_UNAVAILABLE, {"status": "idempotency_conflict", "request_id": request_id}
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
                    effective_status=int(status), status_semantics="orders_api_decision",
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
            dependency_failures = dict(self.dependency_failures)
            internal_failures = dict(self.internal_failures)
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
                *(counter_line("orders_dependency_failures_total", "Checkout dependency failures", value, kind=kind)
                  for kind, value in dependency_failures.items()),
                *(counter_line("orders_internal_failures_total", "Checkout internal failures", value, kind=kind)
                  for kind, value in internal_failures.items()),
            ]
        )

    def status(self) -> dict[str, Any]:
        with self._lock:
            return {"status": "ok", "mode": self.mode, "inventory_url": self.inventory_url,
                    "timeout_seconds": self.timeout, "expected_schema": self.expected_schema}


def handler(state: OrdersState) -> type[QuietHandler]:
    class OrdersHandler(QuietHandler):
        def do_GET(self) -> None:  # noqa: N802
            parsed = urlparse(self.path)
            if parsed.path == "/health":
                self.send_json(HTTPStatus.OK, state.status())
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

        def do_POST(self) -> None:  # noqa: N802
            if urlparse(self.path).path != "/control/scenario":
                self.send_json(HTTPStatus.NOT_FOUND, {"error": "not_found"})
                return
            try:
                payload = self.body_json()
                state.set_mode(str(payload.get("mode", "")), payload.get("duration_seconds", 180), payload.get("settings"))
            except (ValueError, OSError, json.JSONDecodeError) as exc:
                self.send_json(HTTPStatus.BAD_REQUEST, {"error": str(exc)})
                return
            self.send_json(HTTPStatus.OK, state.status())

    return OrdersHandler


def main() -> None:
    state = OrdersState()
    state.logger.write("INFO", "Orders API started", inventory_url=state.inventory_url)
    serve(handler(state), int(os.environ.get("PORT", "8080")))


if __name__ == "__main__":
    main()
