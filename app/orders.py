"""Orders API that turns an inventory database failure into realistic retry pressure."""

from __future__ import annotations

import json
import hashlib
import os
import re
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


MAX_INVENTORY_RESPONSE_BYTES = 256_000
MAX_IDEMPOTENCY_ENTRIES = 10_000
IDEMPOTENCY_TTL_SECONDS = 900
MAX_ORDER_ID_LENGTH = 64
MAX_IDEMPOTENCY_KEY_LENGTH = 128
MAX_SCHEMA_ID_LENGTH = 32
LATENCY_WINDOW_SECONDS = 300
MAX_LATENCY_SAMPLES = 5_000


def _short_identifier(value: Any, field: str, max_length: int) -> str:
    if (not isinstance(value, str) or len(value) > max_length
            or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]*", value)):
        raise ValueError(f"{field} must be a short identifier")
    return value


def _safe_schema_label(value: Any) -> str:
    if value is None:
        return "unspecified"
    if isinstance(value, str) and len(value) <= MAX_SCHEMA_ID_LENGTH and re.fullmatch(
            r"[A-Za-z0-9][A-Za-z0-9._-]*", value):
        return value
    return f"non_string:{type(value).__name__}"


def _safe_contract_value(value: Any) -> str:
    if value is None:
        return "null"
    if isinstance(value, str):
        if len(value) <= 32 and re.fullmatch(r"[A-Za-z0-9._-]+", value):
            return value
        return f"string_length:{len(value)}"
    if isinstance(value, bool):
        return f"boolean:{str(value).lower()}"
    if isinstance(value, (int, float)):
        return "number"
    return f"type:{type(value).__name__}"


def _observed_fields(payload: dict[str, Any]) -> list[str]:
    return sorted(str(key)[:64] for key in payload)[:24]


class OrdersState:
    def __init__(self) -> None:
        self.inventory_url = os.environ["INVENTORY_URL"].rstrip("/")
        self.timeout = float(os.environ.get("INVENTORY_TIMEOUT_SECONDS", "0.45"))
        self.max_retries = int(os.environ.get("MAX_RETRIES", "3"))
        if not 0.01 <= self.timeout <= 30:
            raise ValueError("Inventory timeout must be between 0.01 and 30 seconds")
        if not 1 <= self.max_retries <= 5:
            raise ValueError("MAX_RETRIES must be between 1 and 5 attempts")
        self.logger = JsonLogger("orders-api")
        self.requests = {"200": 0, "409": 0, "502": 0, "503": 0}
        self.checkout_errors = 0
        self.inventory_attempts = 0
        self.inventory_retries = 0
        self.inflight = 0
        self.latencies: list[tuple[float, float, float]] = []
        self.inventory_latencies: list[tuple[float, float, float]] = []
        self.mode = "normal"
        self.signing_key_id = _short_identifier(
            os.environ.get("ORDER_SIGNING_KEY_ID", "checkout-key-v1"), "Request key ID", 64
        )
        self.expected_schema = _short_identifier(
            os.environ.get("ORDER_EXPECTED_SCHEMA", "v1"), "Response schema identifier", MAX_SCHEMA_ID_LENGTH
        )
        self.dependency_failures = {"transport": 0, "timeout": 0, "authorization": 0,
                                    "contract_shape": 0, "contract_version": 0}
        self.internal_failures = {"idempotency_conflict": 0, "idempotency_capacity": 0}
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
        if settings is not None and not isinstance(settings, dict):
            raise ValueError("Orders settings must be an object")
        values = settings or {}
        raw_inventory_url = values.get("INVENTORY_URL", os.environ.get("INVENTORY_URL", "http://inventory-api:8081"))
        if not isinstance(raw_inventory_url, str) or not raw_inventory_url or len(raw_inventory_url) > 2048:
            raise ValueError("Inventory URL must be a non-empty URL under 2048 characters")
        inventory_url = raw_inventory_url.rstrip("/")
        timeout = float(values.get("INVENTORY_TIMEOUT_SECONDS", "1.5"))
        max_retries = int(values.get("MAX_RETRIES", "3"))
        if not 0.01 <= timeout <= 30:
            raise ValueError("Inventory timeout must be between 0.01 and 30 seconds")
        if not 1 <= max_retries <= 5:
            raise ValueError("MAX_RETRIES must be between 1 and 5 attempts")
        expected_schema = _short_identifier(
            values.get("ORDER_EXPECTED_SCHEMA", "v1"), "Response schema identifier", MAX_SCHEMA_ID_LENGTH
        )
        request_key_id = _short_identifier(
            values.get("ORDER_SIGNING_KEY_ID", "checkout-key-v1"), "Request key ID", 64
        )
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
            self.signing_key_id = request_key_id
            self.expected_schema = expected_schema
            self._fault_namespace = uuid.uuid4().hex if mode == "idempotency-conflict" else None
            self._expires_at = time.monotonic() + max(30, min(300, int(duration))) if mode != "normal" else 0.0
            self._control_run_id = run_id
        revision = hashlib.sha256(
            f"{inventory_url}|{timeout}|{max_retries}|{request_key_id}|{expected_schema}".encode()
        ).hexdigest()[:12]
        self.logger.write("INFO", "Checkout runtime configuration applied", configuration_revision=revision,
                          dependency_host=parsed.hostname, dependency_port=dependency_port,
                          dependency_timeout_seconds=timeout, max_dependency_attempts=max_retries,
                          request_key_id=request_key_id, expected_response_schema=expected_schema,
                          run_id=run_id)

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

    def _attempt_inventory_result(self, order_id: str, request_id: str = "", attempt: int = 1,
                                  config: dict[str, Any] | None = None,
                                  prior_timed_out_attempts: int = 0) -> dict[str, Any]:
        if config is None:
            with self._lock:
                config = {
                    "inventory_url": self.inventory_url,
                    "timeout": self.timeout,
                    "max_attempts": self.max_retries,
                    "signing_key_id": self.signing_key_id,
                    "expected_schema": self.expected_schema,
                }
        inventory_url = config["inventory_url"]
        timeout = config["timeout"]
        max_attempts = config["max_attempts"]
        signing_key_id = config["signing_key_id"]
        expected_schema = config["expected_schema"]
        endpoint = f"{inventory_url}/reserve?{urlencode({'order_id': order_id})}"
        try:
            parsed = urlparse(inventory_url)
            dependency_port = parsed.port
            dependency_host = parsed.hostname
        except ValueError as exc:
            parsed = None
            dependency_port = None
            dependency_host = None
            parse_error = exc
        else:
            parse_error = None
        started = time.perf_counter()

        def result(status: int, kind: str, upstream_status: int | None, retryable: bool,
                   **evidence: Any) -> dict[str, Any]:
            duration_ms = round((time.perf_counter() - started) * 1000, 2)
            finished = time.monotonic()
            with self._lock:
                self.inventory_latencies.append((finished, time.time(), duration_ms / 1000))
                self.inventory_latencies = [item for item in self.inventory_latencies
                                            if finished - item[0] <= LATENCY_WINDOW_SECONDS][
                                                -MAX_LATENCY_SAMPLES:]
            if kind != "success":
                self._dependency_failure(kind)
            if kind == "timeout":
                evidence.setdefault("remote_cancellation_propagated", False)
                evidence.setdefault("late_completion_possible", True)
                evidence.setdefault("prior_timed_out_attempts_may_still_be_running",
                                    prior_timed_out_attempts > 0)
            self.logger.write(
                "INFO" if kind == "success" else "WARN" if retryable else "ERROR",
                "Inventory dependency attempt completed",
                request_id=request_id or None,
                order_ref=_safe_ref(order_id),
                attempt=attempt,
                attempt_limit=max_attempts,
                dependency_host=dependency_host,
                dependency_port=dependency_port,
                dependency_duration_ms=duration_ms,
                upstream_status=upstream_status,
                consumer_status=int(status),
                outcome=kind,
                retryable=retryable,
                **evidence,
            )
            return {"status": int(status), "kind": kind, "upstream_status": upstream_status,
                    "retryable": retryable, "duration_ms": duration_ms,
                    "remote_cancellation_propagated": False if kind == "timeout" else None,
                    "late_completion_possible": kind == "timeout"}

        try:
            if parse_error:
                return result(HTTPStatus.SERVICE_UNAVAILABLE, "transport", None, False,
                              error_type=type(parse_error).__name__, consumer_decision="invalid_dependency_url")
            request = Request(endpoint, headers={"X-Signing-Key-Id": signing_key_id})
            with urlopen(request, timeout=timeout) as response:
                body = response.read(MAX_INVENTORY_RESPONSE_BYTES + 1)
                upstream_status = int(response.status)
            if upstream_status != HTTPStatus.OK:
                if 200 <= upstream_status < 300:
                    return result(HTTPStatus.BAD_GATEWAY, "contract_status", upstream_status, False,
                                  expected_upstream_status=int(HTTPStatus.OK),
                                  response_bytes=min(len(body), MAX_INVENTORY_RESPONSE_BYTES + 1),
                                  consumer_decision="reject_unexpected_success_status")
                retryable = upstream_status in {408, 425, 429} or upstream_status >= 500
                return result(HTTPStatus.SERVICE_UNAVAILABLE, "http_error", upstream_status, retryable,
                              expected_upstream_status=int(HTTPStatus.OK),
                              consumer_decision="dependency_http_status_rejected")
            if len(body) > MAX_INVENTORY_RESPONSE_BYTES:
                return result(HTTPStatus.BAD_GATEWAY, "contract_shape", upstream_status, False,
                              expected_schema=expected_schema, observed_type="oversized_document",
                              response_bytes=len(body), max_response_bytes=MAX_INVENTORY_RESPONSE_BYTES,
                              consumer_decision="reject_oversized_document")
            try:
                payload = json.loads(body)
            except (UnicodeDecodeError, ValueError, RecursionError) as exc:
                return result(HTTPStatus.BAD_GATEWAY, "contract_shape", upstream_status, False,
                              expected_schema=expected_schema, observed_type="invalid_json",
                              error_type=type(exc).__name__, response_bytes=len(body),
                              consumer_decision="reject_as_bad_gateway")

            if not isinstance(payload, dict):
                return result(HTTPStatus.BAD_GATEWAY, "contract_shape", upstream_status, False,
                              expected_schema=expected_schema, observed_type=type(payload).__name__,
                              consumer_decision="reject_as_bad_gateway")
            observed_schema = payload.get("schema")
            observed_schema_label = _safe_schema_label(observed_schema)
            if expected_schema not in {"v1", "v2"}:
                return result(HTTPStatus.BAD_GATEWAY, "contract_version", upstream_status, False,
                              expected_schema=expected_schema, observed_schema=observed_schema_label,
                              observed_fields=_observed_fields(payload),
                              consumer_decision="reject_unsupported_consumer_schema")
            if expected_schema == "v1":
                reservation = payload
                status_present = "status" in payload
                observed_status = _safe_contract_value(payload.get("status")) if status_present else "missing"
                valid = status_present and payload.get("status") == "reserved"
                missing_fields = [] if status_present else ["status"]
            else:
                reservation = payload.get("reservation")
                status_present = isinstance(reservation, dict) and "status" in reservation
                observed_status = _safe_contract_value(reservation.get("status")) if status_present else "missing"
                valid = status_present and reservation.get("status") == "reserved"
                missing_fields = [] if status_present else ["reservation.status"]
            version_mismatch = observed_schema is not None and (
                not isinstance(observed_schema, str) or observed_schema != expected_schema
            )
            if version_mismatch or not valid:
                kind = "contract_version" if version_mismatch else "contract_shape"
                return result(HTTPStatus.BAD_GATEWAY, kind, upstream_status, False,
                              expected_schema=expected_schema, observed_schema=observed_schema_label,
                              observed_fields=_observed_fields(payload),
                              observed_reservation_type=type(reservation).__name__,
                              observed_status=observed_status,
                              validation_failure=(
                                  "schema_mismatch" if version_mismatch else
                                  "missing_required_field" if missing_fields else
                                  "unexpected_status_value"
                              ),
                              missing_fields=missing_fields,
                              consumer_decision="reject_as_bad_gateway")
            return result(upstream_status, "success", upstream_status, False,
                          expected_schema=expected_schema, observed_schema=observed_schema_label)
        except HTTPError as exc:
            try:
                exc.read(64_000)
            except OSError:
                pass
            kind = "authorization" if exc.code in {HTTPStatus.UNAUTHORIZED, HTTPStatus.FORBIDDEN} else "http_error"
            retryable = exc.code in {408, 425, 429} or exc.code >= 500
            if kind == "authorization":
                evidence = {"consumer_decision": "dependency_http_error", "request_key_id": signing_key_id}
            else:
                evidence = {"consumer_decision": "dependency_http_error"}
            return result(HTTPStatus.SERVICE_UNAVAILABLE, kind, int(exc.code), retryable,
                          **evidence)
        except (TimeoutError, socket.timeout):
            return result(HTTPStatus.SERVICE_UNAVAILABLE, "timeout", None, True,
                          timeout_seconds=timeout, consumer_decision="dependency_timeout")
        except URLError as exc:
            kind = "timeout" if isinstance(exc.reason, (TimeoutError, socket.timeout)) else "transport"
            return result(HTTPStatus.SERVICE_UNAVAILABLE, kind, None, kind == "timeout",
                          error_type=type(exc.reason).__name__, timeout_seconds=timeout,
                          consumer_decision="dependency_transport_error")
        except (OSError, ValueError) as exc:
            kind = "timeout" if isinstance(exc, (TimeoutError, socket.timeout)) else "transport"
            return result(HTTPStatus.SERVICE_UNAVAILABLE, kind, None, kind == "timeout",
                          error_type=type(exc).__name__, timeout_seconds=timeout,
                          consumer_decision="dependency_transport_error")

    def _attempt_inventory(self, order_id: str) -> int:
        return int(self._attempt_inventory_result(order_id)["status"])

    def _prune_idempotency(self, now: float) -> None:
        expired = [key for key, entry in self._idempotency.items()
                   if entry.get("completed") and now - entry["created_at"] > IDEMPOTENCY_TTL_SECONDS]
        for key in expired:
            self._idempotency.pop(key, None)

    def _make_idempotency_room(self) -> bool:
        while len(self._idempotency) >= MAX_IDEMPOTENCY_ENTRIES:
            completed_key = next((key for key, entry in self._idempotency.items()
                                  if entry.get("completed")), None)
            if completed_key is None:
                return False
            self._idempotency.pop(completed_key, None)
        return True

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
        attempts_made = 0
        timed_out_attempts = 0
        idempotency_disposition = "not_checked"
        last_attempt: dict[str, Any] | None = None
        with self._lock:
            self.inflight += 1
            mode = self.mode
            fault_namespace = self._fault_namespace
            config = {
                "inventory_url": self.inventory_url,
                "timeout": self.timeout,
                "max_attempts": self.max_retries,
                "signing_key_id": self.signing_key_id,
                "expected_schema": self.expected_schema,
            }
        safe_order_ref = _safe_ref(order_id) if isinstance(order_id, str) else None
        self.logger.write("INFO", "Checkout request accepted", request_id=request_id,
                          order_ref=safe_order_ref, idempotency_key_present=bool(idempotency_key),
                          idempotency_identity_field="order_id")
        try:
            if (not isinstance(order_id, str) or not order_id or len(order_id) > MAX_ORDER_ID_LENGTH):
                status = HTTPStatus.BAD_REQUEST
                body = {"status": "invalid_order_id", "request_id": request_id}
                idempotency_disposition = "invalid_order_id"
                return status, body

            if idempotency_key is not None and (
                    not isinstance(idempotency_key, str) or not idempotency_key.strip()
                    or len(idempotency_key) > MAX_IDEMPOTENCY_KEY_LENGTH):
                status = HTTPStatus.BAD_REQUEST
                body = {"status": "invalid_idempotency_key", "request_id": request_id}
                idempotency_disposition = "invalid_key"
                return status, body

            if mode == "idempotency-conflict":
                raw_key = f"fault:{fault_namespace}:shared-checkout-key"
            else:
                raw_key = idempotency_key or order_id
            entry_key = hashlib.sha256(raw_key.encode("utf-8")).hexdigest()
            fingerprint = hashlib.sha256(json.dumps({"order_id": order_id}, sort_keys=True).encode()).hexdigest()
            deadline = time.monotonic() + min(30.0, max(5.0, config["timeout"] * config["max_attempts"] + 2.0))
            idempotency_disposition = "checking"

            with self._idempotency_condition:
                self._prune_idempotency(time.monotonic())
                while True:
                    entry = self._idempotency.get(entry_key)
                    if entry is None:
                        if not self._make_idempotency_room():
                            self.internal_failures["idempotency_capacity"] += 1
                            status = HTTPStatus.SERVICE_UNAVAILABLE
                            body = {"status": "idempotency_store_busy", "request_id": request_id}
                            idempotency_disposition = "capacity_rejected"
                            self.logger.write("WARN", "Checkout rejected because the idempotency store is at capacity",
                                              request_id=request_id, order_ref=_safe_ref(order_id),
                                              capacity=MAX_IDEMPOTENCY_ENTRIES, dependency_called=False)
                            return status, body
                        entry = {"fingerprint": fingerprint, "created_at": time.monotonic(),
                                 "completed": False, "injected": mode == "idempotency-conflict"}
                        self._idempotency[entry_key] = entry
                        owns_entry = True
                        idempotency_disposition = "new_request"
                        break
                    if entry["fingerprint"] != fingerprint:
                        self.internal_failures["idempotency_conflict"] += 1
                        status = HTTPStatus.CONFLICT
                        body = {"status": "idempotency_conflict", "request_id": request_id}
                        idempotency_disposition = "conflict"
                        self.logger.write("ERROR", "Idempotency key was reused for a different order",
                                          request_id=request_id, order_ref=_safe_ref(order_id),
                                          existing_order_ref=entry.get("order_ref", "unavailable"),
                                          idempotency_key_ref=entry_key[:12], identity_field="order_id",
                                          comparison="different_order_ref", consumer_decision="reject_before_dependency")
                        return status, body
                    if entry["completed"]:
                        self.idempotency_replays += 1
                        status = int(entry["status"])
                        original = entry["response"]
                        idempotency_disposition = "replayed"
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
                        idempotency_disposition = "in_progress_rejected"
                        self.logger.write("WARN", "Identical checkout retry is still awaiting its original request",
                                          request_id=request_id, order_ref=_safe_ref(order_id),
                                          idempotency_key_ref=entry_key[:12], dependency_called=False)
                        return status, body
                    self._idempotency_condition.wait(remaining)

            for attempt in range(1, config["max_attempts"] + 1):
                attempts_made = attempt
                with self._lock:
                    self.inventory_attempts += 1
                    if attempt > 1:
                        self.inventory_retries += 1
                last_attempt = self._attempt_inventory_result(
                    order_id, request_id, attempt, config, timed_out_attempts
                )
                if last_attempt["kind"] == "timeout":
                    timed_out_attempts += 1
                status = int(last_attempt["status"])
                if status == HTTPStatus.OK:
                    break
                if not last_attempt["retryable"]:
                    break
                if attempt < config["max_attempts"]:
                    time.sleep(0.006 * attempt)

            if status == HTTPStatus.OK:
                completed_successfully = True
                body = {"status": "completed", "request_id": request_id}
                idempotency_disposition = "stored_result"
                with self._idempotency_condition:
                    entry["completed"] = True
                    entry["status"] = int(status)
                    entry["response"] = dict(body)
                    entry["created_at"] = time.monotonic()
                    entry["order_ref"] = _safe_ref(order_id)
                    self._idempotency_condition.notify_all()
                return status, body

            terminal_status = HTTPStatus.BAD_GATEWAY if status == HTTPStatus.BAD_GATEWAY else HTTPStatus.SERVICE_UNAVAILABLE
            status = terminal_status
            body = {"status": "dependency_contract_rejected" if status == HTTPStatus.BAD_GATEWAY else "inventory_unavailable",
                    "request_id": request_id}
            idempotency_disposition = "failed_not_cached"
            return status, body
        finally:
            duration_ms = round((time.perf_counter() - started) * 1000, 2)
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
                self.latencies = [item for item in self.latencies
                                  if finished - item[0] <= LATENCY_WINDOW_SECONDS][
                                      -MAX_LATENCY_SAMPLES:]
            self.logger.write("INFO" if status < 400 else "WARN", "Checkout request completed",
                              request_id=request_id, order_ref=safe_order_ref,
                              consumer_status=int(status), duration_ms=duration_ms,
                              dependency_attempts=attempts_made,
                              configured_dependency_attempt_limit=config["max_attempts"],
                              dependency_retries=max(0, attempts_made - 1),
                              final_upstream_status=last_attempt.get("upstream_status") if last_attempt else None,
                              final_dependency_outcome=last_attempt.get("kind") if last_attempt else None,
                              idempotency_disposition=idempotency_disposition,
                              timed_out_attempts=timed_out_attempts,
                              remote_cancellation_propagated=False if timed_out_attempts else None,
                              late_completion_possible=bool(timed_out_attempts))

    def metrics(self) -> str:
        with self._lock:
            requests = dict(self.requests)
            attempts = self.inventory_attempts
            retries = self.inventory_retries
            inflight = self.inflight
            now = time.monotonic()
            self.latencies = [item for item in self.latencies
                              if now - item[0] <= LATENCY_WINDOW_SECONDS][-MAX_LATENCY_SAMPLES:]
            latency_samples = [item[2] for item in self.latencies]
            p95 = percentile(latency_samples)
            latency_count = len(latency_samples)
            oldest_latency_timestamp = min((item[1] for item in self.latencies), default=0.0)
            latest_latency_timestamp = max((item[1] for item in self.latencies), default=0.0)
            self.inventory_latencies = [item for item in self.inventory_latencies
                                        if now - item[0] <= LATENCY_WINDOW_SECONDS][
                                            -MAX_LATENCY_SAMPLES:]
            inventory_latency_samples = [item[2] for item in self.inventory_latencies]
            inventory_p95 = percentile(inventory_latency_samples)
            inventory_latency_count = len(inventory_latency_samples)
            oldest_inventory_latency_timestamp = min(
                (item[1] for item in self.inventory_latencies), default=0.0
            )
            latest_inventory_latency_timestamp = max(
                (item[1] for item in self.inventory_latencies), default=0.0
            )
            checkout_errors = self.checkout_errors
            idempotency_replays = self.idempotency_replays
            total = max(1, sum(requests.values()))
            amplification = attempts / total
            logs = self.logger.count
            dependency_failures = dict(self.dependency_failures)
            internal_failures = dict(self.internal_failures)
        return "".join(
            [
                *(counter_line("orders_checkout_requests_total", "Checkout requests completed by the Orders API", value, status=status) for status, value in requests.items()),
                counter_line("orders_checkout_failures_total", "Checkout requests ending with HTTP 502 or 503", checkout_errors),
                counter_line("orders_inventory_attempts_total", "Inventory calls made by checkout", attempts),
                counter_line("orders_inventory_retries_total", "Inventory retry calls made by checkout", retries),
                gauge_line("orders_retry_amplification_ratio", "Inventory attempts per checkout request", amplification),
                gauge_line("orders_checkout_inflight", "Checkout requests currently in flight", inflight),
                gauge_line("orders_checkout_latency_p95_seconds", "Checkout p95 response time over local observation window", p95),
                gauge_line("orders_checkout_latency_sample_count", "Checkout completions in the rolling five-minute latency window", latency_count),
                gauge_line("orders_checkout_latency_oldest_sample_timestamp_seconds", "Unix timestamp of the oldest checkout in the active latency window", oldest_latency_timestamp),
                gauge_line("orders_checkout_latency_latest_sample_timestamp_seconds", "Unix timestamp of the latest checkout in the active latency window", latest_latency_timestamp),
                gauge_line("orders_inventory_dependency_latency_p95_seconds", "Inventory dependency attempt p95 latency over the local observation window", inventory_p95),
                gauge_line("orders_inventory_dependency_latency_sample_count", "Inventory dependency attempts in the rolling five-minute latency window", inventory_latency_count),
                gauge_line("orders_inventory_dependency_latency_oldest_sample_timestamp_seconds", "Unix timestamp of the oldest inventory dependency attempt in the active latency window", oldest_inventory_latency_timestamp),
                gauge_line("orders_inventory_dependency_latency_latest_sample_timestamp_seconds", "Unix timestamp of the latest inventory dependency attempt in the active latency window", latest_inventory_latency_timestamp),
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
                    "run_id": self._control_run_id,
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
