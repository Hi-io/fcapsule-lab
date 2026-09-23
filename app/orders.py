"""Orders API that turns an inventory database failure into realistic retry pressure."""

from __future__ import annotations

import json
import hashlib
import os
import socket
import threading
import time
import uuid
from collections import OrderedDict
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
        self.requests = {"200": 0, "409": 0, "502": 0, "503": 0}
        self.checkout_errors = 0
        self.inventory_attempts = 0
        self.inventory_retries = 0
        self.inflight = 0
        self.latencies: list[tuple[float, float, float]] = []
        self.mode = "normal"
        self.signing_key_id = os.environ.get("ORDER_SIGNING_KEY_ID", "checkout-key-v1")
        self.expected_schema = os.environ.get("ORDER_EXPECTED_SCHEMA", "v1")
        self.dependency_failures = {"transport": 0, "timeout": 0, "authorization": 0,
                                    "contract_shape": 0, "contract_version": 0}
        self.internal_failures = {"idempotency_conflict": 0}
        self.idempotency_replays = 0
        self._idempotency: OrderedDict[str, dict[str, Any]] = OrderedDict()
        self._lock = threading.Lock()
        self._idempotency_condition = threading.Condition(self._lock)
        self._fault_namespace: str | None = None
        self._control_run_id: str | None = None
        self._expires_at = 0.0
        threading.Thread(target=self._lease_loop, daemon=True).start()

    def set_mode(self, mode: str, duration: int = 180, settings: dict[str, Any] | None = None,
                 run_id: str | None = None) -> None:
        if mode not in {"normal", "configured", "idempotency-conflict"}:
            raise ValueError(f"Unknown orders mode: {mode}")
        run_id = _validate_run_id(run_id)
        values = settings or {}
        inventory_url = str(values.get("INVENTORY_URL", os.environ.get("INVENTORY_URL", "http://inventory-api:8081"))).rstrip("/")
        timeout = float(values.get("INVENTORY_TIMEOUT_SECONDS", "1.5"))
        max_retries = int(values.get("MAX_RETRIES", "3"))
        if not 0.01 <= timeout <= 30:
            raise ValueError("Inventory timeout must be between 0.01 and 30 seconds")
        if not 1 <= max_retries <= 5:
            raise ValueError("MAX_RETRIES must be between 1 and 5 attempts")
        expected_schema = str(values.get("ORDER_EXPECTED_SCHEMA", "v1"))
        parsed = urlparse(inventory_url)
        try:
            dependency_port = parsed.port
        except ValueError as exc:
            raise ValueError("Inventory URL contains an invalid port") from exc
        with self._lock:
            self.mode = mode
            self.inventory_url = inventory_url
            self.timeout = timeout
            self.max_retries = max_retries
            self.signing_key_id = str(values.get("ORDER_SIGNING_KEY_ID", "checkout-key-v1"))
            self.expected_schema = expected_schema
            self._fault_namespace = uuid.uuid4().hex if mode == "idempotency-conflict" else None
            self._expires_at = time.monotonic() + max(30, min(300, int(duration))) if mode != "normal" else 0.0
            self._control_run_id = run_id
        revision = hashlib.sha256(
            f"{self.inventory_url}|{self.timeout}|{self.max_retries}|{self.signing_key_id}|{self.expected_schema}".encode()
        ).hexdigest()[:12]
        self.logger.write("INFO", "Checkout runtime configuration applied", configuration_revision=revision,
                          dependency_host=parsed.hostname, dependency_port=dependency_port, run_id=run_id)

    def _lease_loop(self) -> None:
        while True:
            time.sleep(1)
            with self._lock:
                expired = self._expires_at and time.monotonic() >= self._expires_at
                run_id = self._control_run_id
            if expired:
                self.set_mode("normal", run_id=run_id)

    def _dependency_failure(self, kind: str) -> None:
        with self._lock:
            if kind not in self.dependency_failures:
                self.dependency_failures[kind] = 0
            self.dependency_failures[kind] += 1

    def _attempt_inventory_result(self, order_id: str, request_id: str = "", attempt: int = 1) -> dict[str, Any]:
        endpoint = f"{self.inventory_url}/reserve?{urlencode({'order_id': order_id})}"
        parsed = urlparse(self.inventory_url)
        try:
            dependency_port = parsed.port
        except ValueError as exc:
            dependency_port = None
            parse_error = exc
        else:
            parse_error = None
        started = time.perf_counter()

        def result(status: int, kind: str, upstream_status: int | None, retryable: bool,
                   **evidence: Any) -> dict[str, Any]:
            duration_ms = round((time.perf_counter() - started) * 1000, 2)
            if kind != "success":
                self._dependency_failure(kind)
            self.logger.write(
                "INFO" if kind == "success" else "WARN" if retryable else "ERROR",
                "Inventory dependency attempt completed",
                request_id=request_id or None,
                order_ref=_safe_ref(order_id),
                attempt=attempt,
                dependency_host=parsed.hostname,
                dependency_port=dependency_port,
                dependency_duration_ms=duration_ms,
                upstream_status=upstream_status,
                consumer_status=int(status),
                outcome=kind,
                retryable=retryable,
                **evidence,
            )
            return {"status": int(status), "kind": kind, "upstream_status": upstream_status,
                    "retryable": retryable, "duration_ms": duration_ms}

        try:
            if parse_error:
                return result(HTTPStatus.SERVICE_UNAVAILABLE, "transport", None, False,
                              error_type=type(parse_error).__name__, consumer_decision="invalid_dependency_url")
            request = Request(endpoint, headers={"X-Signing-Key-Id": self.signing_key_id})
            with urlopen(request, timeout=self.timeout) as response:
                body = response.read(256_000)
                upstream_status = int(response.status)
            try:
                payload = json.loads(body)
            except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                return result(HTTPStatus.BAD_GATEWAY, "contract_shape", upstream_status, False,
                              expected_schema=self.expected_schema, observed_type="invalid_json",
                              error_type=type(exc).__name__, response_bytes=len(body),
                              consumer_decision="reject_as_bad_gateway")

            if not isinstance(payload, dict):
                return result(HTTPStatus.BAD_GATEWAY, "contract_shape", upstream_status, False,
                              expected_schema=self.expected_schema, observed_type=type(payload).__name__,
                              consumer_decision="reject_as_bad_gateway")
            observed_schema = payload.get("schema")
            if self.expected_schema not in {"v1", "v2"}:
                return result(HTTPStatus.BAD_GATEWAY, "contract_version", upstream_status, False,
                              expected_schema=self.expected_schema, observed_schema=observed_schema,
                              observed_fields=sorted(str(key) for key in payload.keys())[:24],
                              consumer_decision="reject_unsupported_consumer_schema")
            if self.expected_schema == "v1":
                reservation = payload
                valid = payload.get("status") == "reserved"
                missing_fields = [] if "status" in payload else ["status"]
            else:
                reservation = payload.get("reservation")
                valid = isinstance(reservation, dict) and reservation.get("status") == "reserved"
                missing_fields = ["reservation.status"] if not (isinstance(reservation, dict) and "status" in reservation) else []
            version_mismatch = observed_schema is not None and observed_schema != self.expected_schema
            if upstream_status == HTTPStatus.OK and (version_mismatch or not valid):
                kind = "contract_version" if version_mismatch else "contract_shape"
                return result(HTTPStatus.BAD_GATEWAY, kind, upstream_status, False,
                              expected_schema=self.expected_schema, observed_schema=observed_schema or "unspecified",
                              observed_fields=sorted(str(key) for key in payload.keys())[:24],
                              observed_reservation_type=type(reservation).__name__,
                              missing_fields=missing_fields,
                              consumer_decision="reject_as_bad_gateway")
            return result(upstream_status, "success" if upstream_status < 400 else "http_error",
                          upstream_status, upstream_status in {408, 425, 429} or upstream_status >= 500,
                          expected_schema=self.expected_schema, observed_schema=observed_schema or "unspecified")
        except HTTPError as exc:
            try:
                exc.read(64_000)
            except OSError:
                pass
            kind = "authorization" if exc.code in {HTTPStatus.UNAUTHORIZED, HTTPStatus.FORBIDDEN} else "http_error"
            retryable = exc.code in {408, 425, 429} or exc.code >= 500
            return result(HTTPStatus.SERVICE_UNAVAILABLE, kind, int(exc.code), retryable,
                          consumer_decision="dependency_http_error")
        except (TimeoutError, socket.timeout):
            return result(HTTPStatus.SERVICE_UNAVAILABLE, "timeout", None, True,
                          timeout_seconds=self.timeout, consumer_decision="dependency_timeout")
        except URLError as exc:
            kind = "timeout" if isinstance(exc.reason, (TimeoutError, socket.timeout)) else "transport"
            return result(HTTPStatus.SERVICE_UNAVAILABLE, kind, None, kind == "timeout",
                          error_type=type(exc.reason).__name__, timeout_seconds=self.timeout,
                          consumer_decision="dependency_transport_error")
        except (OSError, ValueError) as exc:
            kind = "timeout" if isinstance(exc, (TimeoutError, socket.timeout)) else "transport"
            return result(HTTPStatus.SERVICE_UNAVAILABLE, kind, None, kind == "timeout",
                          error_type=type(exc).__name__, timeout_seconds=self.timeout,
                          consumer_decision="dependency_transport_error")

    def _attempt_inventory(self, order_id: str) -> int:
        return int(self._attempt_inventory_result(order_id)["status"])

    def _prune_idempotency(self, now: float) -> None:
        expired = [key for key, entry in self._idempotency.items()
                   if entry.get("completed") and now - entry["created_at"] > 900]
        for key in expired:
            self._idempotency.pop(key, None)
        while len(self._idempotency) > 10_000:
            oldest = next(iter(self._idempotency))
            if not self._idempotency[oldest].get("completed"):
                break
            self._idempotency.popitem(last=False)

    def checkout(self, order_id: str, idempotency_key: str | None = None) -> tuple[int, dict[str, Any]]:
        request_id = uuid.uuid4().hex
        started = time.perf_counter()
        start_monotonic = time.monotonic()
        status = HTTPStatus.SERVICE_UNAVAILABLE
        body: dict[str, Any] = {"status": "inventory_unavailable", "request_id": request_id}
        entry_key: str | None = None
        owns_entry = False
        completed_successfully = False
        entry: dict[str, Any] | None = None
        with self._lock:
            self.inflight += 1
            mode = self.mode
            fault_namespace = self._fault_namespace
        self.logger.write("INFO", "Checkout request accepted", request_id=request_id,
                          order_ref=_safe_ref(order_id), idempotency_key_present=bool(idempotency_key))
        try:
            if idempotency_key is not None and (not idempotency_key.strip() or len(idempotency_key) > 128):
                status = HTTPStatus.BAD_REQUEST
                body = {"status": "invalid_idempotency_key", "request_id": request_id}
                return status, body

            if mode == "idempotency-conflict":
                raw_key = f"fault:{fault_namespace}:shared-checkout-key"
            else:
                raw_key = idempotency_key or order_id
            entry_key = hashlib.sha256(raw_key.encode("utf-8")).hexdigest()
            fingerprint = hashlib.sha256(json.dumps({"order_id": order_id}, sort_keys=True).encode()).hexdigest()
            deadline = time.monotonic() + min(30.0, max(5.0, self.timeout * self.max_retries + 2.0))

            with self._idempotency_condition:
                self._prune_idempotency(time.monotonic())
                while True:
                    entry = self._idempotency.get(entry_key)
                    if entry is None:
                        entry = {"fingerprint": fingerprint, "created_at": time.monotonic(),
                                 "completed": False, "injected": mode == "idempotency-conflict"}
                        self._idempotency[entry_key] = entry
                        owns_entry = True
                        break
                    if entry["fingerprint"] != fingerprint:
                        self.internal_failures["idempotency_conflict"] += 1
                        status = HTTPStatus.CONFLICT
                        body = {"status": "idempotency_conflict", "request_id": request_id}
                        self.logger.write("ERROR", "Idempotency key was reused for a different order",
                                          request_id=request_id, order_ref=_safe_ref(order_id),
                                          existing_order_ref=entry.get("order_ref", "unavailable"),
                                          idempotency_key_ref=entry_key[:12], consumer_decision="reject_before_dependency")
                        return status, body
                    if entry["completed"]:
                        self.idempotency_replays += 1
                        status = int(entry["status"])
                        original = entry["response"]
                        body = {"status": original.get("status", "completed"),
                                "request_id": request_id, "replayed": True,
                                "original_request_id": original.get("request_id")}
                        self._idempotency.move_to_end(entry_key)
                        self.logger.write("INFO", "Prior checkout result returned for an identical retry",
                                          request_id=request_id, order_ref=_safe_ref(order_id),
                                          idempotency_key_ref=entry_key[:12], original_request_id=original.get("request_id"),
                                          dependency_called=False)
                        return status, body
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        status = HTTPStatus.CONFLICT
                        body = {"status": "same_request_still_in_progress", "request_id": request_id}
                        self.logger.write("WARN", "Identical checkout retry is still awaiting its original request",
                                          request_id=request_id, order_ref=_safe_ref(order_id),
                                          idempotency_key_ref=entry_key[:12], dependency_called=False)
                        return status, body
                    self._idempotency_condition.wait(remaining)

            last_attempt: dict[str, Any] | None = None
            for attempt in range(1, self.max_retries + 1):
                with self._lock:
                    self.inventory_attempts += 1
                    if attempt > 1:
                        self.inventory_retries += 1
                last_attempt = self._attempt_inventory_result(order_id, request_id, attempt)
                status = int(last_attempt["status"])
                if status == HTTPStatus.OK:
                    break
                if not last_attempt["retryable"]:
                    break
                if attempt < self.max_retries:
                    time.sleep(0.006 * attempt)

            if status == HTTPStatus.OK:
                completed_successfully = True
                body = {"status": "completed", "request_id": request_id}
                with self._idempotency_condition:
                    entry["completed"] = True
                    entry["status"] = int(status)
                    entry["response"] = dict(body)
                    entry["created_at"] = time.monotonic()
                    entry["order_ref"] = _safe_ref(order_id)
                    self._idempotency_condition.notify_all()
                self.logger.write("INFO", "Checkout completed", request_id=request_id,
                                  order_ref=_safe_ref(order_id), dependency_attempts=attempt,
                                  dependency_duration_ms=last_attempt["duration_ms"] if last_attempt else 0)
                return status, body

            terminal_status = HTTPStatus.BAD_GATEWAY if status == HTTPStatus.BAD_GATEWAY else HTTPStatus.SERVICE_UNAVAILABLE
            status = terminal_status
            body = {"status": "dependency_contract_rejected" if status == HTTPStatus.BAD_GATEWAY else "inventory_unavailable",
                    "request_id": request_id}
            self.logger.write("ERROR", "Checkout could not complete within the dependency contract and retry policy",
                              request_id=request_id, order_ref=_safe_ref(order_id),
                              dependency_attempts=attempt, final_upstream_status=last_attempt.get("upstream_status") if last_attempt else None,
                              final_dependency_outcome=last_attempt.get("kind") if last_attempt else "unknown",
                              duration_ms=round((time.perf_counter() - started) * 1000, 2))
            return status, body
        finally:
            with self._idempotency_condition:
                if owns_entry and entry_key and entry is not None:
                    if not completed_successfully:
                        self._idempotency.pop(entry_key, None)
                    self._idempotency_condition.notify_all()
                self.inflight = max(0, self.inflight - 1)
                self.requests[str(int(status))] = self.requests.get(str(int(status)), 0) + 1
                if status in {HTTPStatus.BAD_GATEWAY, HTTPStatus.SERVICE_UNAVAILABLE}:
                    self.checkout_errors += 1
                finished = time.monotonic()
                self.latencies.append((finished, time.time(), finished - start_monotonic))
                self.latencies = [item for item in self.latencies if finished - item[0] <= 300][-5000:]

    def metrics(self) -> str:
        with self._lock:
            requests = dict(self.requests)
            attempts = self.inventory_attempts
            retries = self.inventory_retries
            inflight = self.inflight
            now = time.monotonic()
            self.latencies = [item for item in self.latencies if now - item[0] <= 300][-5000:]
            latency_samples = [item[2] for item in self.latencies]
            p95 = percentile(latency_samples)
            latency_count = len(latency_samples)
            oldest_latency_timestamp = min((item[1] for item in self.latencies), default=0.0)
            checkout_errors = self.checkout_errors
            idempotency_replays = self.idempotency_replays
            total = max(1, sum(requests.values()))
            amplification = attempts / total
            logs = self.logger.count
            dependency_failures = dict(self.dependency_failures)
            internal_failures = dict(self.internal_failures)
        return "".join(
            [
                *(counter_line("orders_checkout_requests_total", "Completed checkout requests", value, status=status) for status, value in requests.items()),
                counter_line("orders_checkout_failures_total", "Checkout requests ending in a dependency error", checkout_errors),
                counter_line("orders_inventory_attempts_total", "Inventory calls made by checkout", attempts),
                counter_line("orders_inventory_retries_total", "Inventory retry calls made by checkout", retries),
                gauge_line("orders_retry_amplification_ratio", "Inventory attempts per checkout request", amplification),
                gauge_line("orders_checkout_inflight", "Checkout requests currently in flight", inflight),
                gauge_line("orders_checkout_latency_p95_seconds", "Checkout p95 response time over local observation window", p95),
                gauge_line("orders_checkout_latency_sample_count", "Checkout completions in the rolling five-minute latency window", latency_count),
                gauge_line("orders_checkout_latency_oldest_sample_timestamp_seconds", "Unix timestamp of the oldest checkout in the active latency window", oldest_latency_timestamp),
                counter_line("orders_checkout_idempotency_replays_total", "Identical checkout retries served from a completed result", idempotency_replays),
                counter_line("orders_log_events_total", "Structured orders log events emitted", logs),
                *(counter_line("orders_dependency_failures_total", "Checkout dependency failures", value, kind=kind)
                  for kind, value in dependency_failures.items()),
                *(counter_line("orders_internal_failures_total", "Checkout internal failures", value, kind=kind)
                  for kind, value in internal_failures.items()),
            ]
        )

    def status(self) -> dict[str, Any]:
        with self._lock:
            return {"status": "ok", "mode": self.mode,
                    "inventory_url": _safe_endpoint_url(self.inventory_url),
                    "timeout_seconds": self.timeout, "expected_schema": self.expected_schema}


def _safe_ref(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()[:12]


def _validate_run_id(value: Any) -> str | None:
    if value is None:
        return None
    if (not isinstance(value, str) or len(value) != 32
            or any(character not in "0123456789abcdef" for character in value)):
        raise ValueError("run_id must be a 32-character lowercase hexadecimal ID")
    return value


def _safe_endpoint_url(value: str) -> str:
    try:
        parsed = urlparse(value)
        host = parsed.hostname or ""
        if ":" in host and not host.startswith("["):
            host = f"[{host}]"
        try:
            port = parsed.port
        except ValueError:
            port = None
        suffix = f":{port}" if port is not None else ""
        return f"{parsed.scheme}://{host}{suffix}{parsed.path}"
    except ValueError:
        return "[invalid dependency URL]"


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
                status, payload = state.checkout(order_id, self.headers.get("Idempotency-Key"))
                self.send_json(status, payload)
                return
            self.send_json(HTTPStatus.NOT_FOUND, {"error": "not_found"})

        def do_POST(self) -> None:  # noqa: N802
            if urlparse(self.path).path != "/control/scenario":
                self.send_json(HTTPStatus.NOT_FOUND, {"error": "not_found"})
                return
            try:
                payload = self.body_json()
                state.set_mode(str(payload.get("mode", "")), payload.get("duration_seconds", 180),
                               payload.get("settings"), payload.get("run_id"))
            except (ValueError, OSError, json.JSONDecodeError) as exc:
                self.send_json(HTTPStatus.BAD_REQUEST, {"error": str(exc)})
                return
            self.send_json(HTTPStatus.OK, state.status())

    return OrdersHandler


def main() -> None:
    state = OrdersState()
    state.logger.write("INFO", "Orders API started", inventory_url=_safe_endpoint_url(state.inventory_url))
    serve(handler(state), int(os.environ.get("PORT", "8080")))


if __name__ == "__main__":
    main()
