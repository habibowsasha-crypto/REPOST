"""BingX USDT-M Perpetual Futures adapter for ANTILUD VIP CORE.

This adapter intentionally exposes the same project-facing method names and
return-shapes as the MEXC adapter so the Telegram/risk/lifecycle layers can be
reused by the BingX-only build.

BingX specifics used here:
  - Symbol format: BTC-USDT. We accept BTCUSDT, BTC_USDT, BTC/USDT, BTC-USDT.
  - Auth: HMAC-SHA256 over ASCII-sorted key=value parameters.
  - Headers: X-BX-APIKEY and X-SOURCE-KEY.
  - Order IDs are handled as strings to avoid precision loss.
  - Hedge mode is required by the bot; the adapter attempts to enforce it before writes.
"""

from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
import logging
import math
import time
import urllib.parse
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal, ROUND_DOWN, ROUND_HALF_UP, ROUND_UP, InvalidOperation
from typing import Any, Dict, Iterable, List, Optional

import httpx

from app.services.exchange_identity import clean_exchange_id
from app.exchanges.bingx.symbols import (
    bingx_tradfi_exchange_symbol,
    canonical_bingx_tradfi_symbol,
)

log = logging.getLogger(__name__)

_SHARED_HTTP_CLIENT: httpx.AsyncClient | None = None

# Process-wide TP write locks, matching the MEXC safety pattern.
# Several bot services can create TP for the same BingX position (MARKET entry,
# LIMIT catch-up, BE/recovery).  Serialising by API key + symbol + side prevents
# two coroutines from reading the same empty TP set and both submitting a target.
_TP_WRITE_LOCKS: Dict[str, asyncio.Lock] = {}
_TP_WRITE_LOCKS_META = asyncio.Lock()
_MAX_TP_WRITE_LOCKS = 2000

# Process-wide STOP write locks.  STOP protection is even more critical than TP:
# a fallback STOP must not race with BE/recovery or another post-fill verifier.
_STOP_WRITE_LOCKS: Dict[str, asyncio.Lock] = {}
_STOP_WRITE_LOCKS_META = asyncio.Lock()
_MAX_STOP_WRITE_LOCKS = 2000

# Process-wide exact cancel locks.  They mirror the MEXC adapter safety model:
# one exact order identity should not receive two racing cancel writes from BE,
# lifecycle cleanup and LIMIT catch-up at the same time.
_CANCEL_WRITE_LOCKS: Dict[str, asyncio.Lock] = {}
_CANCEL_WRITE_LOCKS_META = asyncio.Lock()
_MAX_CANCEL_WRITE_LOCKS = 4000

# Process-wide bounded diagnostics for exchange enum drift.  Adapters are
# intentionally short lived (they are rebuilt from the encrypted API row in
# several workers), so an instance-local cache does not actually rate-limit
# production logs.  The helper using this cache is synchronous and contains no
# await point, therefore event-loop tasks cannot interleave the mutation.
_OPAQUE_OPEN_STATUS_WARNED_AT: Dict[tuple[str, str, str], float] = {}
_OPAQUE_OPEN_STATUS_WARNING_WINDOW_SEC = 900.0
_MAX_OPAQUE_OPEN_STATUS_WARNINGS = 256


async def _get_tp_write_lock(key: str) -> asyncio.Lock:
    async with _TP_WRITE_LOCKS_META:
        lock = _TP_WRITE_LOCKS.get(key)
        if lock is None:
            lock = asyncio.Lock()
            _TP_WRITE_LOCKS[key] = lock
        if len(_TP_WRITE_LOCKS) > _MAX_TP_WRITE_LOCKS:
            for old_key, old_lock in list(_TP_WRITE_LOCKS.items()):
                if old_key != key and not old_lock.locked():
                    _TP_WRITE_LOCKS.pop(old_key, None)
                    if len(_TP_WRITE_LOCKS) <= _MAX_TP_WRITE_LOCKS:
                        break
        return lock


async def _get_stop_write_lock(key: str) -> asyncio.Lock:
    async with _STOP_WRITE_LOCKS_META:
        lock = _STOP_WRITE_LOCKS.get(key)
        if lock is None:
            lock = asyncio.Lock()
            _STOP_WRITE_LOCKS[key] = lock
        if len(_STOP_WRITE_LOCKS) > _MAX_STOP_WRITE_LOCKS:
            for old_key, old_lock in list(_STOP_WRITE_LOCKS.items()):
                if old_key != key and not old_lock.locked():
                    _STOP_WRITE_LOCKS.pop(old_key, None)
                    if len(_STOP_WRITE_LOCKS) <= _MAX_STOP_WRITE_LOCKS:
                        break
        return lock


async def _get_cancel_write_lock(key: str) -> asyncio.Lock:
    async with _CANCEL_WRITE_LOCKS_META:
        lock = _CANCEL_WRITE_LOCKS.get(key)
        if lock is None:
            lock = asyncio.Lock()
            _CANCEL_WRITE_LOCKS[key] = lock
        if len(_CANCEL_WRITE_LOCKS) > _MAX_CANCEL_WRITE_LOCKS:
            for old_key, old_lock in list(_CANCEL_WRITE_LOCKS.items()):
                if old_key != key and not old_lock.locked():
                    _CANCEL_WRITE_LOCKS.pop(old_key, None)
                    if len(_CANCEL_WRITE_LOCKS) <= _MAX_CANCEL_WRITE_LOCKS:
                        break
        return lock


def _normalize_symbol(symbol: Any) -> str:
    tradfi = canonical_bingx_tradfi_symbol(symbol)
    if tradfi:
        return tradfi
    s = str(symbol or "").strip().upper()
    if not s:
        return ""
    s = s.replace("/", "-").replace("_", "-")
    if "-" in s:
        base, quote = s.split("-", 1)
        return f"{base}{quote}"
    return s


def _to_bingx_symbol(symbol: Any) -> str:
    tradfi = bingx_tradfi_exchange_symbol(symbol)
    if tradfi:
        return tradfi
    norm = _normalize_symbol(symbol)
    if norm.endswith("USDT") and len(norm) > 4:
        return f"{norm[:-4]}-USDT"
    if norm.endswith("USDC") and len(norm) > 4:
        return f"{norm[:-4]}-USDC"
    if "-" in str(symbol or ""):
        return str(symbol).strip().upper().replace("/", "-").replace("_", "-")
    return norm


def _side_open(side: str) -> str:
    return "BUY" if str(side).lower() == "long" else "SELL"


def _side_close(side: str) -> str:
    return "SELL" if str(side).lower() == "long" else "BUY"


def _position_side(side: str) -> str:
    return "LONG" if str(side).lower() == "long" else "SHORT"


def _first_present_finite(row: Dict[str, Any], keys: tuple[str, ...] | list[str]) -> float:
    for key in keys:
        try:
            value = float(row.get(key) or 0.0)
        except (TypeError, ValueError):
            value = 0.0
        if math.isfinite(value) and value > 0:
            return float(value)
    return 0.0


@dataclass(frozen=True)
class InstrumentInfo:
    symbol: str
    min_qty: float = 0.0
    qty_step: float = 0.000001
    price_tick: float = 0.000001
    min_notional: float = 2.0
    max_leverage: int = 0
    contract_size: float = 1.0
    taker_fee_rate: float = 0.0005
    stop_only_fair: bool = False


class BingxApiError(RuntimeError):
    pass


class BingxResponseIntegrityError(BingxApiError):
    """A successful HTTP response whose data shape cannot be trusted safely."""

    def __init__(self, *, endpoint: str, reason: str) -> None:
        self.endpoint = str(endpoint or "unknown")
        self.reason = str(reason or "invalid response data")
        super().__init__(
            f"BingX response integrity check failed for {self.endpoint}: {self.reason}"
        )


class BingxNetworkAmbiguousError(BingxApiError):
    """Private write result is unknown and must be reconciled before retry."""


class BingxExchangeRejected(BingxApiError):
    def __init__(self, *, http_status: int, error_code: Any, error_message: str, response_audit: Dict[str, Any] | None = None) -> None:
        self.http_status = int(http_status)
        self.error_code = error_code
        self.error_message = str(error_message or "").strip()
        self.retryable = str(error_code) in {"100410", "100500", "100503"}
        self.response_audit = dict(response_audit or {})
        super().__init__(f"BingX HTTP {self.http_status} code={self.error_code}: {self.error_message or 'exchange rejected request'}")


class BingxOrderCancelRejected(BingxApiError):
    def __init__(self, *, order_id: str, error_code: int | None, error_message: str, retryable: bool, response_audit: Dict[str, Any] | None = None) -> None:
        self.order_id = clean_exchange_id(order_id)
        self.error_code = error_code
        self.error_message = str(error_message or "").strip()
        self.retryable = bool(retryable)
        self.response_audit = dict(response_audit or {})
        super().__init__(f"BingX exact order cancellation rejected order_id={self.order_id}: {self.error_message or self.error_code}")


class BingxOrderCancelUnconfirmed(BingxApiError):
    def __init__(self, *, order_id: str, error_message: str, response_audit: Dict[str, Any] | None = None) -> None:
        self.order_id = clean_exchange_id(order_id)
        self.error_code = None
        self.error_message = str(error_message or "").strip()
        self.retryable = None
        self.response_audit = dict(response_audit or {})
        super().__init__(f"BingX exact order cancellation could not be confirmed order_id={self.order_id}: {self.error_message}")


class BingxTpCoverageError(BingxApiError):
    pass


class BingxTpOwnershipError(BingxTpCoverageError):
    """A TP/SL write would have to adopt or overwrite an unowned order."""


class BingxMarketProtectionError(BingxApiError):
    """MARKET entry was accepted, but post-fill STOP protection failed.

    Mirrors the MEXC structured exception so the executor can record whether the
    emergency rollback was confirmed instead of pretending no write happened.
    """

    def __init__(
        self,
        message: str,
        *,
        emergency_close_confirmed: bool = False,
        entry_order: Optional[Dict[str, Any]] = None,
        protection_order: Optional[Dict[str, Any]] = None,
        emergency_close_order: Optional[Dict[str, Any]] = None,
        position_id: int | str | None = None,
        opened_qty: float = 0.0,
        actual_entry: float = 0.0,
    ) -> None:
        self.emergency_close_confirmed = bool(emergency_close_confirmed)
        self.entry_order = dict(entry_order or {})
        self.protection_order = dict(protection_order or {})
        self.emergency_close_order = dict(emergency_close_order or {})
        self.position_id = clean_exchange_id(position_id)
        try:
            self.opened_qty = float(opened_qty or 0.0)
        except (TypeError, ValueError, OverflowError):
            self.opened_qty = 0.0
        try:
            self.actual_entry = float(actual_entry or 0.0)
        except (TypeError, ValueError, OverflowError):
            self.actual_entry = 0.0
        super().__init__(message)


class BingxSymbolNotSupported(BingxApiError):
    pass


# Compatibility aliases for services/tests that still import the old MEXC names
# through the common exchange layer.
MexcApiError = BingxApiError
MexcNetworkAmbiguousError = BingxNetworkAmbiguousError
MexcExchangeRejected = BingxExchangeRejected
MexcOrderCancelRejected = BingxOrderCancelRejected
MexcOrderCancelUnconfirmed = BingxOrderCancelUnconfirmed
MexcTpCoverageError = BingxTpCoverageError
MexcMarketProtectionError = BingxMarketProtectionError
MexcSymbolNotSupported = BingxSymbolNotSupported


def _get_shared_http_client() -> httpx.AsyncClient:
    global _SHARED_HTTP_CLIENT
    if _SHARED_HTTP_CLIENT is None or _SHARED_HTTP_CLIENT.is_closed:
        from app.config import get_settings
        settings = get_settings()
        max_connections = max(4, int(getattr(settings, "BINGX_HTTP_MAX_CONNECTIONS", 40) or 40))
        max_keepalive = max(2, min(max_connections, int(getattr(settings, "BINGX_HTTP_MAX_KEEPALIVE_CONNECTIONS", 20) or 20)))
        keepalive_expiry = max(5.0, float(getattr(settings, "BINGX_HTTP_KEEPALIVE_EXPIRY_SEC", 30) or 30))
        _SHARED_HTTP_CLIENT = httpx.AsyncClient(
            base_url=BingxAdapter.PROD_BASE_URL,
            headers={"User-Agent": "antilud-bingx-core"},
            limits=httpx.Limits(
                max_connections=max_connections,
                max_keepalive_connections=max_keepalive,
                keepalive_expiry=keepalive_expiry,
            ),
            http2=False,
        )
    return _SHARED_HTTP_CLIENT


async def close_shared_http_client() -> None:
    global _SHARED_HTTP_CLIENT
    client = _SHARED_HTTP_CLIENT
    _SHARED_HTTP_CLIENT = None
    if client is not None and not client.is_closed:
        await client.aclose()


class BingxAdapter:
    _CONDITIONAL_ORDER_TYPES = {
        "STOP",
        "STOP_MARKET",
        "TAKE_PROFIT",
        "TAKE_PROFIT_MARKET",
        "TRIGGER",
        "TRIGGER_MARKET",
        "TRIGGER_LIMIT",
        "TAKE_STOP",
        "TAKE_STOP_MARKET",
        "TRAILING_STOP",
        "TRAILING_STOP_MARKET",
        "TRAILING_TP_SL",
    }
    _STOP_LIKE_TYPES = {
        "STOP",
        "STOP_MARKET",
        "TRIGGER",
        "TRIGGER_MARKET",
        "TRIGGER_LIMIT",
        "TAKE_STOP",
        "TAKE_STOP_MARKET",
        "TRAILING_STOP",
        "TRAILING_STOP_MARKET",
        "TRAILING_TP_SL",
    }
    _TAKE_PROFIT_LIKE_TYPES = {"TAKE_PROFIT", "TAKE_PROFIT_MARKET"}
    _KNOWN_ORDER_TYPES = {
        "MARKET",
        "LIMIT",
        "STOP",
        "STOP_MARKET",
        "TAKE_PROFIT",
        "TAKE_PROFIT_MARKET",
        "TRIGGER",
        "TRIGGER_MARKET",
        "TRIGGER_LIMIT",
        "TAKE_STOP",
        "TAKE_STOP_MARKET",
        "TRAILING_STOP",
        "TRAILING_STOP_MARKET",
        "TRAILING_TP_SL",
    }
    _SUPPORTED_CONTRACT_QUOTES = ("USDT", "USDC", "VST")
    _KNOWN_ORDER_STATUS_TEXTS = {
        "NEW",
        "PENDING",
        "OPEN",
        "WORKING",
        "ACCEPTED",
        "CREATED",
        "UNTRIGGERED",
        "TRIGGER_PENDING",
        "PENDING_TRIGGER",
        "PARTIALLY_FILLED",
        "PARTIALLYFILLED",
        "PARTIAL_FILLED",
        "PARTIAL",
        "PART_FILLED",
        "FILLED",
        "FULLY_FILLED",
        "EXECUTED",
        "COMPLETED",
        "DONE",
        "CANCELED",
        "CANCELLED",
        "CANCEL",
        "USER_CANCELED",
        "USER_CANCELLED",
        "REJECTED",
        "EXPIRED",
        "FAILED",
        "INVALID",
        "ERROR",
    }
    _KNOWN_ORDER_STATUS_NUMERIC = {0, 1, 2, 3, 4, 5}

    @classmethod
    def _is_conditional_close_type(cls, order_type: Any) -> bool:
        typ = str(order_type or "").upper()
        return typ in cls._CONDITIONAL_ORDER_TYPES or "STOP" in typ or "TAKE_PROFIT" in typ or typ.startswith("TRIGGER")

    @classmethod
    def _is_stop_like_type(cls, order_type: Any) -> bool:
        typ = str(order_type or "").upper()
        return typ in cls._STOP_LIKE_TYPES or ("STOP" in typ and "TAKE_PROFIT" not in typ) or typ.startswith("TRIGGER")

    @classmethod
    def _is_take_profit_like_type(cls, order_type: Any) -> bool:
        typ = str(order_type or "").upper()
        return typ in cls._TAKE_PROFIT_LIKE_TYPES or "TAKE_PROFIT" in typ

    PROD_BASE_URL = "https://open-api.bingx.com"
    PROD_FALLBACK_BASE_URL = "https://open-api.bingx.pro"
    VST_BASE_URL = "https://open-api-vst.bingx.com"
    VST_FALLBACK_BASE_URL = "https://open-api-vst.bingx.pro"

    def __init__(self, api_key: str, api_secret: str, password: str = "", *, testnet: bool = False, timeout_ms: int = 10000) -> None:
        self.api_key = str(api_key or "")
        self.api_secret = str(api_secret or "")
        self.password = password
        self.testnet = bool(testnet)
        self.timeout = max(1.0, float(timeout_ms or 10000) / 1000.0)
        self._instrument_cache: dict[str, InstrumentInfo] = {}
        self._api_symbols_cache: set[str] | None = None
        self._api_symbols_cache_ts = 0.0
        self._max_leverage_cache: dict[tuple[str, str], int] = {}
        self._hedge_checked = False

    async def close(self) -> None:
        return None

    def _http(self) -> httpx.AsyncClient:
        return _get_shared_http_client()

    @staticmethod
    def _canonical(params: Dict[str, Any]) -> str:
        return "&".join(f"{k}={params[k]}" for k in sorted(params))

    @staticmethod
    def _encode_query(params: Dict[str, Any], signature: str | None = None) -> str:
        """Return the exact signed parameter string sent to BingX.

        BingX validates the HMAC over the assembled ``key=value`` parameter
        string.  For private form POSTs, especially ``/swap/v2/trade/order``
        with attached ``stopLoss``/``takeProfit`` JSON strings, the body must
        match that signed string byte-for-byte.  Percent-encoding the JSON
        values after signing makes live order creation fail with code 100001
        even though simpler signed endpoints keep working.
        """
        pairs = [f"{k}={params[k]}" for k in sorted(params)]
        if signature is not None:
            pairs.append(f"signature={signature}")
        return "&".join(pairs)

    @staticmethod
    def _validate_params(params: Dict[str, Any]) -> None:
        for k, v in params.items():
            s = str(v)
            if any(ch in s for ch in ["&", "=", "?", "#", "\r", "\n"]):
                raise BingxApiError(f"BingX parameter {k!r} contains forbidden query metacharacters")

    def _sign(self, params: Dict[str, Any]) -> str:
        canonical = self._canonical(params)
        return hmac.new(self.api_secret.encode(), canonical.encode(), hashlib.sha256).hexdigest()

    async def _request(self, method: str, path: str, *, params: Dict[str, Any] | None = None, auth: bool = False, write: bool = False) -> Any:
        method = method.upper()
        request_params: Dict[str, Any] = dict(params or {})
        headers = {"X-SOURCE-KEY": "BX-AI-SKILL"}
        if auth:
            if not self.api_key or not self.api_secret:
                raise BingxApiError("BingX API key/secret are required for private endpoint")
            request_params.setdefault("recvWindow", 5000)
            request_params["timestamp"] = int(time.time() * 1000)
            self._validate_params(request_params)
            signature = self._sign(request_params)
            headers["X-BX-APIKEY"] = self.api_key
        else:
            signature = None

        bases = ([self.VST_BASE_URL, self.VST_FALLBACK_BASE_URL] if self.testnet else [self.PROD_BASE_URL, self.PROD_FALLBACK_BASE_URL])
        last_exc: Exception | None = None
        for idx, base in enumerate(bases):
            url = base + path
            body = None
            req_headers = dict(headers)
            if auth:
                if method in {"POST"}:
                    body = self._encode_query(request_params, signature)
                    req_headers["Content-Type"] = "application/x-www-form-urlencoded"
                else:
                    url += "?" + self._encode_query(request_params, signature)
            elif request_params:
                url += "?" + urllib.parse.urlencode(request_params)
            start = time.perf_counter()
            workload_metrics: dict[str, Any] = {}
            response_status: int | None = None
            diagnostic_success = False
            try:
                # v1.6.57: route every BingX HTTP call through the process-wide
                # governor.  It bounds global/per-account load and prioritises
                # STOP/cancel/emergency writes over entries and monitor reads.
                from app.services.workload_manager import govern_bingx_request

                async with govern_bingx_request(
                    api_key=self.api_key,
                    auth=bool(auth),
                    method=method,
                    path=path,
                    body=request_params,
                ) as workload_metrics:
                    res = await self._http().request(method, url, headers=req_headers, content=body, timeout=self.timeout)
                response_status = int(res.status_code)
                raw = res.text
                try:
                    payload = json.loads(raw) if raw else {}
                except json.JSONDecodeError as exc:
                    if write or auth:
                        raise BingxNetworkAmbiguousError(f"BingX returned non-JSON response for {method} {path}: {exc}") from exc
                    raise BingxApiError(f"BingX returned non-JSON response for {method} {path}: {exc}") from exc
                if not isinstance(payload, dict):
                    payload_type = type(payload).__name__
                    if write:
                        raise BingxNetworkAmbiguousError(
                            f"BingX private write returned malformed top-level JSON "
                            f"for {method} {path}: {payload_type}"
                        )
                    if auth:
                        raise BingxResponseIntegrityError(
                            endpoint=path,
                            reason=f"top-level JSON must be an object, got {payload_type}",
                        )
                    raise BingxApiError(
                        f"BingX response for {method} {path} must be a JSON object, "
                        f"got {payload_type}"
                    )
                network_ms = int((time.perf_counter() - start) * 1000)
                workload_wait_ms = int((workload_metrics or {}).get("wait_ms") or 0)
                if network_ms >= 1500 or workload_wait_ms >= 1000:
                    log.warning(
                        "slow BingX request method=%s path=%s status=%s network_ms=%s workload_wait_ms=%s",
                        method,
                        path,
                        res.status_code,
                        network_ms,
                        workload_wait_ms,
                    )
                code = payload.get("code", 0)
                if res.status_code >= 500 and (write or auth):
                    raise BingxNetworkAmbiguousError(f"BingX HTTP {res.status_code} after private request {method} {path}: {payload}")
                if res.status_code >= 400:
                    raise BingxExchangeRejected(http_status=res.status_code, error_code=code, error_message=payload.get("msg") or raw[:200], response_audit={"path": path})
                if str(code) not in {"0", ""}:
                    retryable = str(code) in {"100410", "100500", "100503"}
                    if write and retryable:
                        raise BingxNetworkAmbiguousError(f"BingX ambiguous/retryable write code={code}: {payload.get('msg')}")
                    raise BingxExchangeRejected(http_status=res.status_code, error_code=code, error_message=payload.get("msg") or "BingX rejected request", response_audit={"path": path, "payload": payload})
                diagnostic_success = True
                return payload.get("data", payload)
            except (httpx.TimeoutException, httpx.TransportError, BingxNetworkAmbiguousError) as exc:
                last_exc = exc
                if idx < len(bases) - 1 and isinstance(exc, (httpx.TimeoutException, httpx.TransportError)):
                    continue
                if write or auth:
                    if isinstance(exc, BingxNetworkAmbiguousError):
                        raise
                    raise BingxNetworkAmbiguousError(f"BingX private request outcome unknown {method} {path}: {exc}") from exc
                raise BingxApiError(f"BingX request failed {method} {path}: {exc}") from exc
            finally:
                # v1.0.7a1 diagnostics only: attribute the already-executed HTTP
                # attempt to its monitor span.  Query parameters, signatures, API
                # keys and response payloads are intentionally never recorded.
                try:
                    from app.services.monitor_diagnostics import record_http_request

                    total_ms = int((time.perf_counter() - start) * 1000)
                    wait_ms = int((workload_metrics or {}).get("wait_ms") or 0)
                    record_http_request(
                        method=method,
                        path=path,
                        status_code=response_status,
                        total_ms=total_ms,
                        network_ms=max(0, total_ms - wait_ms),
                        workload_metrics=workload_metrics,
                        error=not diagnostic_success,
                    )
                except Exception:
                    # Observability must never alter exchange behavior.
                    pass
        raise BingxApiError(f"BingX request failed {method} {path}: {last_exc}")

    @staticmethod
    def _rows(data: Any) -> List[Dict[str, Any]]:
        if isinstance(data, list):
            return [x for x in data if isinstance(x, dict)]
        if isinstance(data, dict):
            for key in ("orders", "list", "data"):
                val = data.get(key)
                if isinstance(val, list):
                    return [x for x in val if isinstance(x, dict)]
            return [data]
        return []

    @classmethod
    def _strict_collection_rows(
        cls,
        data: Any,
        *,
        endpoint: str,
        wrapper_keys: tuple[str, ...],
    ) -> List[Dict[str, Any]]:
        """Parse list endpoints without turning malformed payloads into an empty list.

        ``_request`` already checks HTTP and BingX business codes and returns the
        top-level ``data`` value.  These account/trade endpoints are expected to
        return either a list directly or one explicit list wrapper.  Any other
        shape is UNKNOWN state and must fail closed instead of meaning "nothing
        is open".
        """

        if isinstance(data, list):
            raw_rows = data
        elif isinstance(data, dict):
            present_keys = [key for key in wrapper_keys if key in data]
            if len(present_keys) != 1:
                keys = ",".join(sorted(str(key) for key in data.keys())[:12])
                raise BingxResponseIntegrityError(
                    endpoint=endpoint,
                    reason=(
                        "expected exactly one list wrapper "
                        f"{wrapper_keys}, got keys=[{keys}]"
                    ),
                )
            wrapper_key = present_keys[0]
            raw_rows = data.get(wrapper_key)
            if not isinstance(raw_rows, list):
                raise BingxResponseIntegrityError(
                    endpoint=endpoint,
                    reason=f"wrapper {wrapper_key!r} is not a list",
                )
        else:
            raise BingxResponseIntegrityError(
                endpoint=endpoint,
                reason=f"expected list payload, got {type(data).__name__}",
            )

        rows: list[dict[str, Any]] = []
        for index, row in enumerate(raw_rows):
            if not isinstance(row, dict):
                raise BingxResponseIntegrityError(
                    endpoint=endpoint,
                    reason=f"row[{index}] is not an object ({type(row).__name__})",
                )
            rows.append(row)
        return rows

    @staticmethod
    def _strict_finite_decimal(
        value: Any,
        *,
        endpoint: str,
        field: str,
        row_index: int,
    ) -> Decimal:
        try:
            parsed = Decimal(str(value))
        except (InvalidOperation, TypeError, ValueError):
            raise BingxResponseIntegrityError(
                endpoint=endpoint,
                reason=f"row[{row_index}].{field} is not numeric",
            ) from None
        if not parsed.is_finite():
            raise BingxResponseIntegrityError(
                endpoint=endpoint,
                reason=f"row[{row_index}].{field} is not finite",
            )
        return parsed

    @classmethod
    def _strict_contract_symbol(
        cls,
        value: Any,
        *,
        endpoint: str,
        field: str,
        row_index: int,
    ) -> str:
        if not isinstance(value, str):
            raise BingxResponseIntegrityError(
                endpoint=endpoint,
                reason=f"row[{row_index}].{field} is missing or not a string",
            )
        normalized = _normalize_symbol(value)
        if (
            not normalized
            or not normalized.isascii()
            or not normalized.isalnum()
            or len(normalized) > 40
        ):
            raise BingxResponseIntegrityError(
                endpoint=endpoint,
                reason=f"row[{row_index}].{field} has invalid contract-symbol syntax",
            )
        quote = next(
            (suffix for suffix in cls._SUPPORTED_CONTRACT_QUOTES if normalized.endswith(suffix)),
            "",
        )
        if not quote or len(normalized) <= len(quote):
            raise BingxResponseIntegrityError(
                endpoint=endpoint,
                reason=f"row[{row_index}].{field} has unsupported or missing quote asset",
            )
        return normalized

    @classmethod
    def _select_requested_symbol_row(
        cls,
        rows: list[dict[str, Any]],
        *,
        requested_symbol: Any,
        endpoint: str,
        allow_single_symbol_missing: bool = False,
    ) -> dict[str, Any]:
        """Select exactly one row for the requested contract, fail closed otherwise.

        Some BingX endpoints normally return one object but may return a list.
        Taking ``rows[0]`` can mix BTC metadata/last price/leverage into another
        symbol when the exchange ignores a filter or changes its wrapper shape.
        A symbol-less single row is accepted only for endpoints whose documented
        response is account-scoped (currently the private leverage read).
        """

        wanted = _normalize_symbol(requested_symbol)
        if not wanted:
            raise BingxResponseIntegrityError(
                endpoint=endpoint,
                reason="requested symbol is empty or invalid",
            )

        exact: list[tuple[int, dict[str, Any], Any]] = []
        symbol_missing: list[dict[str, Any]] = []
        for index, row in enumerate(rows):
            raw_symbol = row.get("symbol") if isinstance(row, dict) else None
            if raw_symbol in (None, "") and isinstance(row, dict):
                raw_symbol = row.get("s")
            if raw_symbol in (None, ""):
                symbol_missing.append(row)
                continue
            if _normalize_symbol(raw_symbol) == wanted:
                exact.append((index, row, raw_symbol))

        if len(exact) == 1:
            index, row, raw_symbol = exact[0]
            cls._strict_contract_symbol(
                raw_symbol,
                endpoint=endpoint,
                field="symbol",
                row_index=index,
            )
            return row
        if len(exact) > 1:
            raise BingxResponseIntegrityError(
                endpoint=endpoint,
                reason=f"multiple rows returned for requested symbol {wanted}",
            )
        if (
            allow_single_symbol_missing
            and len(rows) == 1
            and len(symbol_missing) == 1
        ):
            return symbol_missing[0]
        raise BingxResponseIntegrityError(
            endpoint=endpoint,
            reason=f"requested symbol {wanted} is absent from response rows",
        )

    @staticmethod
    def _strict_optional_bool(
        value: Any,
        *,
        endpoint: str,
        field: str,
        row_index: int,
        default: bool = False,
    ) -> bool:
        if value is None:
            return bool(default)
        if isinstance(value, bool):
            return value
        if isinstance(value, int) and value in {0, 1}:
            return bool(value)
        if isinstance(value, str):
            text = value.strip().lower()
            if text in {"true", "1"}:
                return True
            if text in {"false", "0"}:
                return False
        raise BingxResponseIntegrityError(
            endpoint=endpoint,
            reason=f"row[{row_index}].{field} is not a valid boolean",
        )

    @classmethod
    def _validate_open_order_status(
        cls,
        raw_status: Any,
        *,
        endpoint: str,
        row_index: int,
        allow_opaque: bool = True,
    ) -> tuple[str, bool]:
        """Validate a status returned by BingX ``openOrders``.

        ``openOrders`` is itself the authoritative *open-set* endpoint.  BingX
        can introduce a new textual lifecycle label before its public schema or
        this adapter is updated.  Rejecting one such syntactically valid label
        made the whole symbol read unusable after a TP write, leaving a real TP
        visible on BingX but without a durable confirmed checkpoint in the bot.

        We therefore keep strict validation for missing, malformed and unknown
        numeric states, while accepting a bounded ASCII token as an opaque live
        status.  Opaque statuses are never interpreted as FILLED/CANCELED; they
        remain conservatively active until a known terminal state or exact
        history evidence proves otherwise.
        """
        if isinstance(raw_status, bool) or raw_status is None:
            raise BingxResponseIntegrityError(
                endpoint=endpoint,
                reason=f"row[{row_index}].status is missing or invalid",
            )
        if isinstance(raw_status, int):
            if raw_status in cls._KNOWN_ORDER_STATUS_NUMERIC:
                return str(raw_status), True
            raise BingxResponseIntegrityError(
                endpoint=endpoint,
                reason=f"row[{row_index}].status numeric value is unknown",
            )
        status_text = str(raw_status).strip().upper()
        if not status_text:
            raise BingxResponseIntegrityError(
                endpoint=endpoint,
                reason=f"row[{row_index}].status is empty",
            )
        if status_text.isdigit():
            if int(status_text) in cls._KNOWN_ORDER_STATUS_NUMERIC:
                return status_text, True
            raise BingxResponseIntegrityError(
                endpoint=endpoint,
                reason=f"row[{row_index}].status numeric value is unknown",
            )
        if status_text in cls._KNOWN_ORDER_STATUS_TEXTS:
            return status_text, True
        safe_opaque = (
            len(status_text) <= 64
            and status_text.isascii()
            and status_text[0].isalnum()
            and all(ch.isalnum() or ch in {"_", "-"} for ch in status_text)
        )
        if safe_opaque and allow_opaque:
            return status_text, False
        raise BingxResponseIntegrityError(
            endpoint=endpoint,
            reason=(
                f"row[{row_index}].status text is unknown for this endpoint"
                if safe_opaque
                else f"row[{row_index}].status text is malformed or unsafe"
            ),
        )

    @classmethod
    def _validated_order_status_from_payload(
        cls,
        payload: Dict[str, Any],
        *,
        endpoint: str,
        row_index: int,
        allow_opaque: bool,
    ) -> tuple[str, bool, int, bool, bool]:
        """Validate all status aliases and return one coherent core state.

        BingX payloads can contain more than one alias (``status``, ``state``,
        ``orderStatus`` or ``order_state``).  Reading only the first one allows
        a known terminal numeric state to be hidden behind an unrelated textual
        value.  For protective-order confirmation that can turn a terminal row
        into a false live confirmation.

        Active aliases 0/1/2 are treated as one conservative open class because
        BingX generations have used different numeric labels for new/partial
        states.  Terminal classes 3/4/5 must agree exactly.  Any active/terminal
        contradiction fails closed.
        """

        aliases = ("status", "state", "orderStatus", "order_state")
        candidates: list[tuple[str, str, bool, int, bool, bool]] = []
        for field in aliases:
            if field not in payload:
                continue
            # Parse bounded opaque tokens first, then apply endpoint semantics
            # below. ``openOrders`` may preserve one opaque live label; exact
            # detail and history require every present alias to be recognized.
            status_text, recognized = cls._validate_open_order_status(
                payload.get(field),
                endpoint=endpoint,
                row_index=row_index,
                allow_opaque=True,
            )
            state, _name, terminal, fully_filled = cls._bingx_status_to_core_state(
                status_text
            )
            candidates.append(
                (field, status_text, recognized, int(state), bool(terminal), bool(fully_filled))
            )

        if not candidates:
            raise BingxResponseIntegrityError(
                endpoint=endpoint,
                reason=f"row[{row_index}].status is missing or invalid",
            )

        all_recognized = all(item[2] for item in candidates)
        if not allow_opaque and not all_recognized:
            opaque_fields = ",".join(
                f"{field}={text}" for field, text, recognized, *_ in candidates if not recognized
            )
            raise BingxResponseIntegrityError(
                endpoint=endpoint,
                reason=(
                    f"row[{row_index}] has unknown status alias(es) "
                    f"({opaque_fields or 'unknown'})"
                ),
            )

        terminal_candidates = [item for item in candidates if item[4]]
        active_candidates = [item for item in candidates if not item[4]]
        if terminal_candidates and active_candidates:
            details = ",".join(f"{field}={text}" for field, text, *_ in candidates)
            raise BingxResponseIntegrityError(
                endpoint=endpoint,
                reason=f"row[{row_index}] has contradictory status aliases ({details})",
            )
        if terminal_candidates:
            terminal_states = {item[3] for item in terminal_candidates}
            if len(terminal_states) != 1:
                details = ",".join(f"{field}={text}" for field, text, *_ in candidates)
                raise BingxResponseIntegrityError(
                    endpoint=endpoint,
                    reason=f"row[{row_index}] has contradictory terminal status aliases ({details})",
                )

        # Preserve the public status field when present, but derive the core
        # state from a recognized alias whenever one exists. ``openOrders`` is
        # an authoritative open-set endpoint, so one bounded opaque alias may
        # coexist with a recognized active alias there. Exact order detail and
        # allOrders history are not open-set endpoints: every present alias must
        # be recognized, otherwise a newly introduced terminal enum could be
        # misclassified as live/filled merely because another alias looks active.
        preferred = next((item for item in candidates if item[0] == "status"), candidates[0])
        recognized_candidates = [item for item in candidates if item[2]]
        semantic = recognized_candidates[0] if recognized_candidates else preferred
        recognition_sufficient = all_recognized
        return (
            preferred[1],
            bool(recognition_sufficient),
            int(semantic[3]),
            bool(semantic[4]),
            bool(semantic[5]),
        )

    def _warn_opaque_open_order_status(
        self,
        *,
        symbol: str,
        order_id: str,
        order_type: str,
        status_text: str,
    ) -> None:
        """Emit a bounded diagnostic for a new BingX open-order status."""
        now = time.monotonic()
        cache = _OPAQUE_OPEN_STATUS_WARNED_AT
        # Preserve the historical inspection attribute for tests/debug tooling,
        # but make every adapter instance point at the same process-wide cache.
        self._opaque_open_status_warned_at = cache
        key = (str(symbol), str(order_type), str(status_text))
        last = float(cache.get(key) or 0.0)
        if last and now - last < _OPAQUE_OPEN_STATUS_WARNING_WINDOW_SEC:
            return
        cache[key] = now
        if len(cache) > _MAX_OPAQUE_OPEN_STATUS_WARNINGS:
            for old_key, _old_value in sorted(cache.items(), key=lambda item: item[1]):
                if len(cache) <= _MAX_OPAQUE_OPEN_STATUS_WARNINGS:
                    break
                if old_key != key:
                    cache.pop(old_key, None)
        log.warning(
            "BINGX_OPEN_ORDER_STATUS_OPAQUE_ACTIVE symbol=%s order_id=%s type=%s status=%s semantics=open_orders_active",
            symbol,
            order_id,
            order_type,
            status_text,
        )

    @staticmethod
    def _f(value: Any, default: float = 0.0) -> float:
        try:
            v = float(value)
            return v if math.isfinite(v) else default
        except Exception:
            return default

    @classmethod
    def _max_nonnegative_quantity_alias(
        cls, row: Dict[str, Any], keys: tuple[str, ...]
    ) -> tuple[float, bool]:
        """Return conservative quantity evidence across BingX aliases.

        BingX numeric fields are commonly strings, so ``"0" or "4"`` returns
        ``"0"`` and can hide a real fill. Any positive alias must prevent an
        order from being classified as unfilled. The conflict flag preserves
        the disagreement for diagnostics.
        """

        values: list[float] = []
        for key in keys:
            if key not in row or row.get(key) in (None, ""):
                continue
            values.append(max(0.0, cls._f(row.get(key), 0.0)))
        if not values:
            return 0.0, False
        normalized = {format(value, ".15g") for value in values}
        return max(values), len(normalized) > 1

    @staticmethod
    def _fmt_num(value: float, step: float | None = None) -> str:
        try:
            d = Decimal(str(value))
        except InvalidOperation:
            d = Decimal("0")
        if step and step > 0:
            try:
                q = Decimal(str(step))
                d = (d / q).to_integral_value(rounding=ROUND_DOWN) * q
            except Exception:
                pass
        s = format(d.normalize(), "f")
        return s.rstrip("0").rstrip(".") if "." in s else s

    _STEP_UNIT_SNAP_TOLERANCE = Decimal("1e-9")

    @staticmethod
    def _qty_decimal(value: Any) -> Decimal:
        """Parse a non-negative finite exchange quantity without float subtraction."""
        try:
            out = Decimal(str(value))
        except Exception:
            return Decimal("0")
        if not out.is_finite() or out <= 0:
            return Decimal("0")
        return out

    @classmethod
    def _floor_to_step(cls, value: Any, step: float) -> float:
        """Floor to the exchange step while snapping float-noise near an exact lot.

        Python subtraction can turn an exact exchange remainder such as
        ``411.4 - 298.7`` into ``112.69999999999999``.  A blind floor would
        submit ``112.6`` and leave one 0.1 lot open.  Only values within one
        billionth of a step-unit from an integer are snapped; genuine excess is
        still rounded down.
        """
        if step <= 0:
            return float(value)
        try:
            d = Decimal(str(value))
            q = Decimal(str(step))
            units = d / q
            nearest = units.to_integral_value(rounding=ROUND_HALF_UP)
            if abs(units - nearest) <= cls._STEP_UNIT_SNAP_TOLERANCE:
                aligned_units = nearest
            else:
                aligned_units = units.to_integral_value(rounding=ROUND_DOWN)
            return float(aligned_units * q)
        except Exception:
            return math.floor(float(value) / float(step)) * float(step)

    @staticmethod
    def _is_quantity_constraint_rejection(exc: Exception) -> bool:
        if not isinstance(exc, BingxExchangeRejected):
            return False
        msg = f"{getattr(exc, 'error_message', '')} {getattr(exc, 'error_code', '')} {getattr(exc, 'response_audit', {})}".lower()
        if any(marker in msg for marker in ("insufficient", "margin", "balance", "permission", "clientorderid unique")):
            return False
        has_qty = any(marker in msg for marker in ("quantity", "qty", "volume", "vol", "trademinquantity", "minqty"))
        has_rule = any(marker in msg for marker in ("precision", "accur", "step", "min", "minimum", "less", "small", "invalid", "scale"))
        return bool(has_qty and has_rule)

    async def _post_trade_order_with_quantity_retry(
        self,
        *,
        params: Dict[str, Any],
        symbol: str,
        context: str,
        min_acceptable_qty: float | None = None,
    ) -> tuple[Any, Dict[str, Any] | None]:
        """POST /trade/order once, then retry once after refreshing qty rules.

        This is a BingX version of the MEXC defensive sizing retry.  It is
        intentionally narrow: only quantity/minQty/precision rejections are
        eligible, the retry can only reduce quantity to the fresh step, and it
        never increases risk to satisfy minQty.  STOP callers can pass
        ``min_acceptable_qty`` so the bot fails closed instead of creating an
        under-covered protective STOP.
        """

        first_params = dict(params)
        try:
            data = await self._request("POST", "/openApi/swap/v2/trade/order", params=first_params, auth=True, write=True)
            return data, None
        except BingxExchangeRejected as exc:
            if not self._is_quantity_constraint_rejection(exc):
                raise
            old_qty = self._f(first_params.get("quantity"), 0.0)
            fresh = await self.instrument_info(symbol, force=True)
            new_qty = self._floor_to_step(old_qty, fresh.qty_step) if old_qty > 0 else 0.0
            min_qty = max(float(fresh.min_qty or 0.0), float(fresh.qty_step or 0.0))
            tolerance = max(float(fresh.qty_step or 0.0) * 0.51, old_qty * 1e-9, 1e-12)
            retry_audit = {
                "context": context,
                "reason": "quantity_constraint_rejection",
                "first_error_code": getattr(exc, "error_code", None),
                "first_error_message": getattr(exc, "error_message", ""),
                "old_quantity": old_qty,
                "fresh_qty_step": fresh.qty_step,
                "fresh_min_qty": fresh.min_qty,
                "fresh_min_notional": fresh.min_notional,
                "retry_quantity": new_qty,
            }
            if old_qty <= 0 or new_qty <= 0 or new_qty >= old_qty - max(abs(old_qty) * 1e-12, 1e-12):
                exc.response_audit.setdefault("quantity_retry", retry_audit | {"retry_attempted": False, "blocked_reason": "no_safe_downward_quantity"})
                raise
            if min_qty > 0 and new_qty + tolerance < min_qty:
                exc.response_audit.setdefault("quantity_retry", retry_audit | {"retry_attempted": False, "blocked_reason": "below_fresh_min_qty"})
                raise
            if min_acceptable_qty is not None and new_qty + tolerance < float(min_acceptable_qty):
                exc.response_audit.setdefault("quantity_retry", retry_audit | {"retry_attempted": False, "blocked_reason": "below_min_acceptable_qty"})
                raise

            retry_params = dict(first_params)
            retry_params["quantity"] = self._fmt_num(new_qty, fresh.qty_step)
            if "price" in retry_params:
                retry_params["price"] = self._fmt_num(self._f(retry_params.get("price"), 0.0), fresh.price_tick)
            if "stopPrice" in retry_params:
                retry_params["stopPrice"] = self._fmt_num(self._f(retry_params.get("stopPrice"), 0.0), fresh.price_tick)
            try:
                data = await self._request("POST", "/openApi/swap/v2/trade/order", params=retry_params, auth=True, write=True)
            except BingxExchangeRejected as retry_exc:
                retry_exc.response_audit.setdefault("quantity_retry", retry_audit | {"retry_attempted": True, "retry_failed": True})
                raise
            retry_audit.update({"retry_attempted": True, "submitted_quantity": new_qty})
            return data, retry_audit

    @staticmethod
    def _price_round(price: float, tick: float, *, side: str, kind: str) -> float:
        if tick <= 0:
            return float(price)
        d = Decimal(str(price))
        q = Decimal(str(tick))
        # For safety: LONG SL rounds up? Keep close to user value but exchange-valid.
        mode = ROUND_DOWN
        if kind.lower() == "sl" and side.lower() == "long":
            mode = ROUND_UP
        if kind.lower() == "tp" and side.lower() == "short":
            mode = ROUND_UP
        out = (d / q).to_integral_value(rounding=mode) * q
        return float(out)

    @staticmethod
    def _client_id(prefix: str, fixed: str | None = None) -> str:
        base = str(fixed or "").strip().lower()
        if base:
            safe = "".join(ch if ch.isalnum() or ch in "_-" else "-" for ch in base)[:40]
            return safe or f"{prefix}-{uuid.uuid4().hex[:16]}"[:40]
        return f"{prefix}-{uuid.uuid4().hex[:18]}"[:40].lower()

    @staticmethod
    def _attempt_client_id(prefix: str, fixed: str | None = None) -> str:
        """Return a BingX clientOrderID that is unique per write attempt.

        Entry order IDs are already unique per trade group.  Manual/emergency
        market closes can be retried after an ambiguous network outcome, so a
        stable client ID would make BingX reject the retry with
        ``code=101400 clientOrderID unique check failed``.  Keep a short human
        readable prefix/base while appending entropy inside BingX's 40-char
        budget.
        """

        base = str(fixed or prefix or "abx").strip().lower()
        safe = "".join(ch if ch.isalnum() or ch in "_-" else "-" for ch in base)
        suffix = f"-{int(time.time() * 1000) % 100000000:x}-{uuid.uuid4().hex[:8]}"
        head_len = max(1, 40 - len(suffix))
        return (safe[:head_len] + suffix)[:40] or f"{prefix}-{uuid.uuid4().hex[:18]}"[:40].lower()

    @staticmethod
    def _order_payload_from_response(response: Any) -> Dict[str, Any]:
        """Return the actual BingX order object from flat/nested payloads.

        BingX order endpoints can return either a flat row or a nested shape like
        ``{"data": {"order": {...}}}`` / ``{"order": {...}}``.  The core
        services need exact order identities for safe cancel, LIMIT fill
        reconciliation, STOP/TP ownership and close notifications.  Losing the
        nested ``orderId`` forces the bot into legacy identityless mode and can
        leave phantom LIMIT rows behind.
        """

        if not isinstance(response, dict):
            return {}
        data = response.get("data")
        if isinstance(data, dict):
            nested = data.get("order")
            if isinstance(nested, dict):
                return nested
            if any(
                k in data
                for k in (
                    "orderId",
                    "orderID",
                    "order_id",
                    "id",
                    "stopOrderId",
                    "stopPlanOrderId",
                    "planOrderId",
                    "placeOrderId",
                    "clientOrderId",
                    "clientOrderID",
                )
            ):
                return data
        nested = response.get("order")
        if isinstance(nested, dict):
            return nested
        return response

    @staticmethod
    def _order_id_from_response(response: Any) -> str:
        if isinstance(response, dict):
            data = BingxAdapter._order_payload_from_response(response)
            oid = (
                data.get("orderID")
                or data.get("orderId")
                or data.get("order_id")
                or data.get("id")
                or data.get("stopPlanOrderId")
                or data.get("stopOrderId")
                or data.get("planOrderId")
                or data.get("placeOrderId")
            )
            return clean_exchange_id(oid)
        return ""

    @staticmethod
    def _client_order_id_from_response(response: Any) -> str:
        if isinstance(response, dict):
            data = BingxAdapter._order_payload_from_response(response)
            oid = (
                data.get("clientOrderID")
                or data.get("clientOrderId")
                or data.get("client_order_id")
                or data.get("externalOid")
                or response.get("_entry_external_oid")
                or response.get("clientOrderID")
                or response.get("clientOrderId")
            )
            return clean_exchange_id(oid)
        return ""

    @staticmethod
    def _symbol_from_order_response(response: Dict[str, Any] | None) -> str:
        if not isinstance(response, dict):
            return ""
        data = BingxAdapter._order_payload_from_response(response)
        nested_data = response.get("data") if isinstance(response.get("data"), dict) else {}
        return str(response.get("symbol") or data.get("symbol") or nested_data.get("symbol") or "")

    # Account ----------------------------------------------------------------

    @classmethod
    def _balance_row(cls, data: Any) -> Dict[str, Any]:
        """Return the USDT balance row from BingX account payloads.

        The current v3 account endpoint returns ``data.balance`` as an object
        (``data.balance.balance``, ``data.balance.equity``,
        ``data.balance.availableMargin``).  Older/fake responses and some
        tests use a flat row/list.  Normalize both shapes here so the core
        risk preflight always receives the MEXC-compatible aliases it expects.
        """

        if isinstance(data, dict):
            nested = data.get("balance")
            if isinstance(nested, dict):
                return nested
            if isinstance(nested, list):
                rows = [x for x in nested if isinstance(x, dict)]
                return next(
                    (r for r in rows if str(r.get("asset") or r.get("currency") or "").upper() == "USDT"),
                    rows[0] if rows else {},
                )
        rows = cls._rows(data)
        return next(
            (r for r in rows if str(r.get("asset") or r.get("currency") or "").upper() == "USDT"),
            rows[0] if rows else {},
        )

    async def fetch_balance_details(self) -> Dict[str, float]:
        data = await self._request("GET", "/openApi/swap/v3/user/balance", auth=True)
        usdt = self._balance_row(data)
        balance = self._f(usdt.get("equity") or usdt.get("balance"), 0.0)
        wallet = self._f(usdt.get("balance"), balance)
        available = self._f(usdt.get("availableMargin") or usdt.get("availableBalance"), 0.0)
        used_margin = self._f(usdt.get("usedMargin"), 0.0)
        unrealized_pnl = self._f(usdt.get("unrealizedProfit"), 0.0)
        return {
            # BingX/native aliases used by the BingX UI and adapter-level tests.
            "balance": balance,
            "equity": balance,
            "available": available,
            "availableBalance": available,
            "wallet_balance": wallet,
            "used_margin": used_margin,
            "unrealized_pnl": unrealized_pnl,
            # Core/MEXC-compatible aliases required by signal_executor and
            # balance handlers.  Without these keys the bot fails closed with
            # zero balance even when BingX returns 200 OK with positive USDT.
            "total_equity": balance,
            "total_wallet_balance": wallet,
            "available_balance": available,
            "coin_equity": balance,
            "coin_wallet_balance": wallet,
            "USDT": available,
        }

    async def fetch_balance_usdt(self) -> float:
        return float((await self.fetch_balance_details()).get("equity") or 0.0)

    async def fetch_open_positions(self, symbol: Optional[str] = None, side: Optional[str] = None) -> List[Dict[str, Any]]:
        endpoint = "/openApi/swap/v2/user/positions"
        params: Dict[str, Any] = {}
        if symbol:
            params["symbol"] = _to_bingx_symbol(symbol)
        data = await self._request("GET", endpoint, params=params, auth=True)
        rows = self._strict_collection_rows(
            data,
            endpoint=endpoint,
            wrapper_keys=("positions", "list", "data"),
        )
        out: list[dict[str, Any]] = []
        target = _normalize_symbol(symbol) if symbol else ""
        target_side = str(side or "").lower()
        for index, row in enumerate(rows):
            sym = self._strict_contract_symbol(
                row.get("symbol"),
                endpoint=endpoint,
                field="symbol",
                row_index=index,
            )

            pos_side = str(row.get("positionSide") or "").strip().upper()
            if pos_side not in {"LONG", "SHORT"}:
                raise BingxResponseIntegrityError(
                    endpoint=endpoint,
                    reason=f"row[{index}].positionSide is missing or invalid",
                )

            qty_field = (
                "positionAmt"
                if "positionAmt" in row
                else "availableAmt"
                if "availableAmt" in row
                else ""
            )
            if not qty_field:
                raise BingxResponseIntegrityError(
                    endpoint=endpoint,
                    reason=f"row[{index}] has no position quantity field",
                )
            qty_decimal = self._strict_finite_decimal(
                row.get(qty_field),
                endpoint=endpoint,
                field=qty_field,
                row_index=index,
            )
            if qty_decimal < 0:
                raise BingxResponseIntegrityError(
                    endpoint=endpoint,
                    reason=f"row[{index}].{qty_field} is negative",
                )
            size = float(qty_decimal)

            available_decimal = qty_decimal
            if "availableAmt" in row:
                available_decimal = self._strict_finite_decimal(
                    row.get("availableAmt"),
                    endpoint=endpoint,
                    field="availableAmt",
                    row_index=index,
                )
                if available_decimal < 0:
                    raise BingxResponseIntegrityError(
                        endpoint=endpoint,
                        reason=f"row[{index}].availableAmt is negative",
                    )
                if "positionAmt" in row and available_decimal > qty_decimal:
                    raise BingxResponseIntegrityError(
                        endpoint=endpoint,
                        reason=(
                            f"row[{index}].availableAmt exceeds positionAmt "
                            f"({available_decimal} > {qty_decimal})"
                        ),
                    )
            available_size = float(available_decimal)

            isolated = self._strict_optional_bool(
                row.get("isolated"),
                endpoint=endpoint,
                field="isolated",
                row_index=index,
                default=False,
            )

            leverage = 0
            if row.get("leverage") not in (None, ""):
                leverage_decimal = self._strict_finite_decimal(
                    row.get("leverage"),
                    endpoint=endpoint,
                    field="leverage",
                    row_index=index,
                )
                if leverage_decimal < 0:
                    raise BingxResponseIntegrityError(
                        endpoint=endpoint,
                        reason=f"row[{index}].leverage is negative",
                    )
                leverage = int(leverage_decimal)

            if target and sym != target:
                continue
            if target_side and pos_side.lower() != target_side:
                continue
            if size <= 0:
                continue

            out.append({
                "symbol": sym,
                "side": pos_side.lower(),
                "positionSide": pos_side,
                "size": size,
                "availableSize": available_size,
                "contracts": size,
                "positionId": clean_exchange_id(row.get("positionId")),
                "openType": 1 if isolated else 2,
                "entryPrice": self._f(row.get("avgPrice"), 0.0),
                "breakEvenPrice": self._f(row.get("breakEvenPrice"), 0.0),
                "leverage": leverage,
                "raw": row,
            })
        return out

    # Orders -----------------------------------------------------------------

    @classmethod
    def _normalize_entry_order_status(cls, row: Dict[str, Any], *, order_id: str) -> Dict[str, Any]:
        """Normalize BingX order detail into the core/MEXC-compatible shape.

        The LIMIT TP catch-up engine waits for ``terminal=True`` before locking
        the immutable TP plan.  BingX returns string statuses such as
        ``FILLED``/``CANCELED`` while the inherited core logic was originally
        built around numeric MEXC states.  Returning the raw string in
        ``state`` made a fully executed BingX LIMIT look non-terminal forever,
        so TP orders were never placed after fill.
        """

        row = cls._order_payload_from_response(row) if isinstance(row, dict) else {}
        order_id = clean_exchange_id(order_id or cls._order_id_from_response(row))
        client_order_id = cls._client_order_id_from_response(row)
        raw_status = (
            row.get("status")
            or row.get("state")
            or row.get("orderStatus")
            or row.get("order_state")
            or ""
        )
        status_text = str(raw_status or "").strip().upper()
        exchange_status_text = status_text

        requested_qty, requested_qty_alias_conflict = (
            cls._max_nonnegative_quantity_alias(
                row, ("origQty", "quantity", "qty", "vol")
            )
        )
        filled_qty, filled_qty_alias_conflict = cls._max_nonnegative_quantity_alias(
            row,
            (
                "executedQty",
                "cumQty",
                "filledQty",
                "dealVol",
                "executedVolume",
            ),
        )

        numeric_state: int
        raw_numeric_state: int | None = None
        try:
            raw_numeric_state = int(raw_status)
        except (TypeError, ValueError):
            raw_numeric_state = None

        if raw_numeric_state is not None:
            numeric_state = raw_numeric_state
            terminal = numeric_state in {3, 4, 5}
            fully_filled = numeric_state == 3
            if not status_text:
                status_text = {
                    1: "NEW",
                    2: "PARTIALLY_FILLED",
                    3: "FILLED",
                    4: "CANCELED",
                    5: "REJECTED",
                }.get(numeric_state, str(numeric_state))
        else:
            filled_statuses = {
                "FILLED",
                "FULLY_FILLED",
                "EXECUTED",
                "COMPLETED",
                "DONE",
            }
            partial_statuses = {
                "PARTIALLY_FILLED",
                "PARTIALLYFILLED",
                "PARTIAL_FILLED",
                "PARTIAL",
                "PART_FILLED",
            }
            canceled_statuses = {
                "CANCELED",
                "CANCELLED",
                "CANCEL",
                "USER_CANCELED",
                "USER_CANCELLED",
            }
            rejected_statuses = {
                "REJECTED",
                "EXPIRED",
                "FAILED",
                "INVALID",
                "ERROR",
            }
            if status_text in filled_statuses:
                numeric_state = 3
                terminal = True
                fully_filled = True
            elif status_text in canceled_statuses:
                numeric_state = 4
                terminal = True
                fully_filled = False
            elif status_text in rejected_statuses:
                numeric_state = 5
                terminal = True
                fully_filled = False
            elif status_text in partial_statuses:
                numeric_state = 2
                terminal = False
                fully_filled = False
            else:
                numeric_state = 1 if status_text else 0
                terminal = False
                fully_filled = False

        if fully_filled and filled_qty <= 0 and requested_qty > 0:
            # Some order-detail rows return only origQty after final FILLED.
            # Using origQty as a fill fallback is safe only after a terminal
            # filled status; using it for NEW/PENDING would create false fills.
            filled_qty = requested_qty

        zero_fill_partial_status = bool(numeric_state == 2 and filled_qty <= 0)
        core_status_text = status_text
        if zero_fill_partial_status:
            # Some live BingX LIMIT rows report PARTIALLYFILLED while every
            # executed-quantity alias is exactly zero and no position exists.
            # Preserve the exchange string for diagnostics, but classify the
            # core fill state from the authoritative quantity: active/unfilled.
            numeric_state = 1
            terminal = False
            fully_filled = False
            core_status_text = "NEW"

        qty_tolerance = max(requested_qty * 1e-9, 1e-12) if requested_qty > 0 else 1e-12
        if requested_qty > 0 and filled_qty + qty_tolerance >= requested_qty and status_text not in {"CANCELED", "CANCELLED", "REJECTED", "EXPIRED", "FAILED", "INVALID", "ERROR"}:
            fully_filled = True
            if terminal or status_text in {"FILLED", "FULLY_FILLED", "EXECUTED", "COMPLETED", "DONE"}:
                terminal = True
                numeric_state = 3
                status_text = status_text or "FILLED"
                core_status_text = status_text

        avg = cls._f(row.get("avgPrice") or row.get("price") or row.get("dealAvgPrice"), 0.0)
        return {
            "filled": bool(fully_filled or filled_qty > 0),
            "terminal": bool(terminal),
            "fully_filled": bool(fully_filled),
            "state": int(numeric_state),
            "state_name": core_status_text or str(numeric_state),
            "status": core_status_text or str(numeric_state),
            "exchange_status": exchange_status_text,
            "fill_state": (
                "unfilled_active"
                if filled_qty <= 0 and not terminal
                else "partially_filled"
                if filled_qty > 0 and not fully_filled
                else "fully_filled"
                if fully_filled
                else "terminal_no_fill"
            ),
            "status_diagnostic": (
                "exchange_partial_status_without_filled_quantity"
                if zero_fill_partial_status
                else "filled_quantity_alias_conflict_conservative_max"
                if filled_qty_alias_conflict
                else "requested_quantity_alias_conflict_conservative_max"
                if requested_qty_alias_conflict
                else ""
            ),
            "order_id": order_id,
            "orderId": order_id,
            "clientOrderId": client_order_id,
            "clientOrderID": client_order_id,
            "filled_qty": float(filled_qty),
            "requested_qty": float(requested_qty),
            "avg_price": float(avg),
            "avg_fill_price": float(avg),
            "raw": row,
        }

    async def fetch_entry_order_fill_status(self, *, symbol: str | None = None, order_response: Dict[str, Any]) -> Dict[str, Any]:
        order_id = self.entry_order_id(order_response)
        client_order_id = self.entry_client_order_id(order_response)
        if not order_id and not client_order_id:
            return {
                "filled": False,
                "terminal": False,
                "fully_filled": False,
                "state": 0,
                "state_name": "UNKNOWN",
                "status": "UNKNOWN",
                "order_id": "",
                "clientOrderId": "",
                "filled_qty": 0.0,
            }
        sym = symbol or self._symbol_from_order_response(order_response)
        params = {"symbol": _to_bingx_symbol(sym)}
        if order_id:
            params["orderId"] = order_id
        else:
            # BingX order-query docs use clientOrderID (capital D).  This lets
            # us reconcile orders created before a nested orderId was persisted.
            params["clientOrderID"] = client_order_id
        detail = await self._request("GET", "/openApi/swap/v2/trade/order", params=params, auth=True)
        row = self._order_payload_from_response(detail) if isinstance(detail, dict) else {}

        returned_symbol = self._symbol_from_order_response(row)
        if returned_symbol and _normalize_symbol(returned_symbol) != _normalize_symbol(sym):
            raise BingxResponseIntegrityError(
                endpoint="/openApi/swap/v2/trade/order",
                reason=(
                    "order detail symbol mismatch: "
                    f"requested={_normalize_symbol(sym)} returned={_normalize_symbol(returned_symbol)}"
                ),
            )
        returned_order_id = self._order_id_from_response(row)
        if order_id and returned_order_id and returned_order_id != order_id:
            raise BingxResponseIntegrityError(
                endpoint="/openApi/swap/v2/trade/order",
                reason=f"order detail id mismatch: requested={order_id} returned={returned_order_id}",
            )
        returned_client_id = self._client_order_id_from_response(row)
        if client_order_id and returned_client_id and returned_client_id != client_order_id:
            raise BingxResponseIntegrityError(
                endpoint="/openApi/swap/v2/trade/order",
                reason=(
                    "order detail client id mismatch: "
                    f"requested={client_order_id} returned={returned_client_id}"
                ),
            )

        normalized = self._normalize_entry_order_status(
            row,
            order_id=order_id or returned_order_id,
        )
        if client_order_id:
            normalized["clientOrderId"] = client_order_id
            normalized["clientOrderID"] = client_order_id
        return normalized

    def entry_order_id(self, order_response: Dict[str, Any]) -> str:
        return self._order_id_from_response(order_response)

    def entry_client_order_id(self, order_response: Dict[str, Any]) -> str:
        return self._client_order_id_from_response(order_response)

    @staticmethod
    def _bounded_audit_value(value: Any, *, depth: int = 0) -> Any:
        """Return a bounded JSON-ish value suitable for durable audit logs."""

        if depth >= 4:
            return "..."
        if isinstance(value, dict):
            out: dict[str, Any] = {}
            for idx, (key, item) in enumerate(value.items()):
                if idx >= 25:
                    out["..."] = f"{len(value) - idx} more keys"
                    break
                out[str(key)[:120]] = BingxAdapter._bounded_audit_value(item, depth=depth + 1)
            return out
        if isinstance(value, list):
            return [BingxAdapter._bounded_audit_value(item, depth=depth + 1) for item in value[:25]]
        if isinstance(value, (str, int, float, bool)) or value is None:
            text = str(value)
            return text[:500] if isinstance(value, str) else value
        return str(value)[:300]

    @classmethod
    def _cancel_response_audit(
        cls,
        response: Any,
        *,
        symbol: str,
        order_id: str = "",
        client_order_id: str = "",
        readback: dict[str, Any] | None = None,
        write_error: Exception | None = None,
    ) -> dict[str, Any]:
        err_payload: dict[str, Any] | None = None
        if write_error is not None:
            err_payload = {
                "type": type(write_error).__name__,
                "message": str(write_error)[:500],
                "error_code": cls._bounded_audit_value(getattr(write_error, "error_code", None)),
                "retryable": cls._bounded_audit_value(getattr(write_error, "retryable", None)),
                "response_audit": cls._bounded_audit_value(getattr(write_error, "response_audit", None)),
            }
        return {
            "version": 1,
            "exchange": "bingx",
            "symbol": _normalize_symbol(symbol),
            "order_id": clean_exchange_id(order_id),
            "client_order_id": clean_exchange_id(client_order_id),
            "write_response": cls._bounded_audit_value(response),
            "write_error": err_payload,
            "readback": cls._bounded_audit_value(readback or {}),
        }

    async def _cancel_readback_once(
        self,
        *,
        symbol: str,
        order_id: str = "",
        client_order_id: str = "",
    ) -> dict[str, Any]:
        """Read exact order/open-order state after a cancel write.

        The caller uses the result to decide whether a cancel write can be
        treated as terminal.  This intentionally checks both order detail and
        openOrders because either BingX read model can lag the other.
        """

        oid = clean_exchange_id(order_id)
        cid = clean_exchange_id(client_order_id)
        norm = _normalize_symbol(symbol)
        status: dict[str, Any] | None = None
        open_orders: list[dict[str, Any]] | None = None
        errors: dict[str, str] = {}

        if oid or cid:
            try:
                status = await self.fetch_entry_order_fill_status(
                    symbol=norm,
                    order_response={"symbol": norm, "orderId": oid, "clientOrderID": cid},
                )
            except Exception as exc:
                errors["order_get"] = f"{type(exc).__name__}: {exc}"[:500]
        try:
            open_orders = await self.fetch_open_orders(norm)
        except Exception as exc:
            errors["open_orders"] = f"{type(exc).__name__}: {exc}"[:500]

        exact_matches: list[dict[str, Any]] = []
        if open_orders is not None:
            for row in open_orders:
                if not isinstance(row, dict):
                    continue
                raw = row.get("raw") if isinstance(row.get("raw"), dict) else {}
                row_oid = clean_exchange_id(row.get("orderId") or row.get("id") or raw.get("orderId"))
                row_cid = clean_exchange_id(row.get("clientOrderID") or row.get("clientOrderId") or raw.get("clientOrderID") or raw.get("clientOrderId"))
                if (oid and row_oid == oid) or (cid and row_cid == cid):
                    exact_matches.append(row)

        terminal = bool(status.get("terminal")) if isinstance(status, dict) else False
        fully_filled = bool(status.get("fully_filled")) if isinstance(status, dict) else False
        filled_qty = 0.0
        state = 0
        state_name = "UNKNOWN"
        if isinstance(status, dict):
            filled_qty = self._f(status.get("filled_qty"), 0.0)
            try:
                state = int(status.get("state") or 0)
            except (TypeError, ValueError, OverflowError):
                state = 0
            state_name = str(status.get("state_name") or status.get("status") or "UNKNOWN")

        open_absent = open_orders is not None and not exact_matches
        unknown_order_detail = (
            isinstance(status, dict)
            and int(self._f(status.get("state"), 0)) == 0
            and str(status.get("state_name") or status.get("status") or "").upper() in {"", "UNKNOWN", "0"}
            and filled_qty <= 0
        )
        # Confirmed means the exact order is not live anymore and either order/get
        # is terminal, order/get is unavailable/not-found, or order/get returned an
        # unusable/empty row while openOrders authoritatively shows the exact ID
        # absent.  A filled terminal is not a cancel success semantically, but it
        # is a terminal outcome and callers must prefer fill over retrying cancel.
        confirmed = bool(open_absent and (terminal or "order_get" in errors or unknown_order_detail))
        return {
            "version": 1,
            "symbol": norm,
            "order_id": oid,
            "client_order_id": cid,
            "confirmed_terminal_or_absent": confirmed,
            "terminal": terminal,
            "fully_filled": fully_filled,
            "filled_qty": float(filled_qty),
            "state": state,
            "state_name": state_name,
            "open_orders_state": "error" if open_orders is None else "present" if exact_matches else "absent",
            "open_exact_match_count": len(exact_matches),
            "open_symbol_order_count": len(open_orders or []),
            "exact_matches": [
                {
                    "orderId": clean_exchange_id(row.get("orderId") or row.get("id")),
                    "clientOrderID": clean_exchange_id(row.get("clientOrderID") or row.get("clientOrderId")),
                    "symbol": row.get("symbol"),
                    "type": row.get("type"),
                    "state": row.get("state"),
                    "status": row.get("status"),
                    "qty": row.get("origQty") or row.get("qty") or row.get("quantity"),
                }
                for row in exact_matches[:3]
            ],
            "errors": errors,
        }

    async def _confirm_exact_cancel_readback(
        self,
        *,
        symbol: str,
        order_id: str = "",
        client_order_id: str = "",
        write_response: Any = None,
        write_error: Exception | None = None,
    ) -> dict[str, Any]:
        delays = (0.0, 0.15, 0.35, 0.75)
        latest: dict[str, Any] = {}
        for delay in delays:
            if delay > 0:
                await asyncio.sleep(delay)
            latest = await self._cancel_readback_once(
                symbol=symbol,
                order_id=order_id,
                client_order_id=client_order_id,
            )
            if bool(latest.get("confirmed_terminal_or_absent")):
                return latest
        audit = self._cancel_response_audit(
            write_response,
            symbol=symbol,
            order_id=order_id,
            client_order_id=client_order_id,
            readback=latest,
            write_error=write_error,
        )
        label = order_id or client_order_id
        if bool(latest.get("fully_filled")) or int(self._f(latest.get("state"), 0)) == 3:
            # A filled order is terminal and should not be retried as cancel even
            # when openOrders lagged.  Return the audit and let higher layers
            # classify the fill with their own authoritative reads.
            return latest
        raise BingxOrderCancelUnconfirmed(
            order_id=label,
            error_message=(
                "BingX exact cancel write was not confirmed by read-back; "
                f"open_orders_state={latest.get('open_orders_state')} state={latest.get('state_name')}"
            ),
            response_audit=audit,
        )

    async def _cancel_exact_single(
        self,
        *,
        symbol: str,
        order_id: str = "",
        client_order_id: str = "",
    ) -> Dict[str, Any]:
        oid = clean_exchange_id(order_id)
        cid = clean_exchange_id(client_order_id)
        label = oid or cid
        if not label:
            raise BingxOrderCancelUnconfirmed(order_id="", error_message="missing exact orderId/clientOrderID")
        if not symbol:
            raise BingxOrderCancelUnconfirmed(order_id=label, error_message="BingX exact cancel requires symbol")
        lock_key = f"{self.api_key[:12]}:{_normalize_symbol(symbol)}:{label}"
        lock = await _get_cancel_write_lock(lock_key)
        async with lock:
            write_response: Any = None
            write_error: Exception | None = None
            params = {"symbol": _to_bingx_symbol(symbol)}
            if oid:
                params["orderId"] = oid
            else:
                params["clientOrderID"] = cid
            try:
                write_response = await self._request(
                    "DELETE",
                    "/openApi/swap/v2/trade/order",
                    params=params,
                    auth=True,
                    write=True,
                )
            except Exception as exc:
                # MEXC parity: a cancel write exception is not final.  The order
                # may have filled/cancelled concurrently or the response may have
                # been lost.  Read-back below decides if retrying would be unsafe.
                write_error = exc
            readback = await self._confirm_exact_cancel_readback(
                symbol=symbol,
                order_id=oid,
                client_order_id=cid,
                write_response=write_response,
                write_error=write_error,
            )
            audit = self._cancel_response_audit(
                write_response,
                symbol=symbol,
                order_id=oid,
                client_order_id=cid,
                readback=readback,
                write_error=write_error,
            )
            readback_filled = bool(readback.get("fully_filled")) or self._f(readback.get("filled_qty"), 0.0) > 0 or int(self._f(readback.get("state"), 0)) == 3
            if write_error is not None and not bool(readback.get("confirmed_terminal_or_absent")) and not readback_filled:
                raw_code = getattr(write_error, "error_code", None)
                try:
                    code_int = int(raw_code) if raw_code not in (None, "") and not isinstance(raw_code, bool) else None
                except (TypeError, ValueError, OverflowError):
                    code_int = None
                raise BingxOrderCancelRejected(
                    order_id=label,
                    error_code=code_int,
                    error_message=str(getattr(write_error, "error_message", "") or write_error),
                    retryable=bool(getattr(write_error, "retryable", False)),
                    response_audit=audit,
                )
            return {
                "success": True,
                "code": 0,
                "data": write_response,
                "orderId": oid,
                "clientOrderID": cid,
                "clientOrderId": cid,
                "_exact_cancel_result": {
                    "order_id": oid,
                    "client_order_id": cid,
                    "terminal": bool(readback.get("terminal")),
                    "filled": bool(readback.get("fully_filled")) or self._f(readback.get("filled_qty"), 0.0) > 0,
                    "state": readback.get("state"),
                    "state_name": readback.get("state_name"),
                    "error_code": 0,
                    "error_message": "",
                    "response_audit": audit,
                },
            }

    async def cancel_entry_order(self, order_response: Dict[str, Any]) -> Dict[str, Any]:
        order_id = self.entry_order_id(order_response)
        client_order_id = self.entry_client_order_id(order_response)
        sym = self._symbol_from_order_response(order_response) or order_response.get("symbol") or order_response.get("data", {}).get("symbol")
        if not sym:
            raise BingxOrderCancelUnconfirmed(order_id=order_id or client_order_id, error_message="missing symbol for exact BingX cancel")
        if order_id:
            return await self.cancel_regular_orders_exact([order_id], symbol=sym)
        if client_order_id:
            return await self.cancel_regular_order_by_client_id(client_order_id, symbol=sym)
        raise BingxOrderCancelUnconfirmed(order_id="", error_message="missing exact orderId/clientOrderID")

    async def cancel_regular_order_by_client_id(self, client_order_id: str, symbol: str | None = None) -> Dict[str, Any]:
        cid = clean_exchange_id(client_order_id)
        if not cid:
            raise BingxOrderCancelUnconfirmed(order_id="", error_message="missing exact clientOrderID")
        if not symbol:
            raise BingxOrderCancelUnconfirmed(order_id=cid, error_message="BingX cancel by clientOrderID requires symbol")
        return await self._cancel_exact_single(symbol=symbol, client_order_id=cid)

    async def cancel_regular_orders_exact(self, order_ids: List[str | int], symbol: str | None = None) -> Dict[str, Any]:
        cleaned = [clean_exchange_id(x) for x in order_ids if clean_exchange_id(x)]
        if not cleaned:
            return {"success": True, "code": 0, "data": []}
        if not symbol:
            raise BingxOrderCancelUnconfirmed(order_id=cleaned[0], error_message="BingX exact cancel requires symbol")
        # BingX exposes a batch endpoint, but MEXC-parity safety is better served
        # by exact single-order writes plus read-back for each identity.  This
        # prevents a partial batch response from being treated as success.
        results = []
        for oid in cleaned:
            results.append(await self._cancel_exact_single(symbol=symbol, order_id=oid))
        first_exact = (results[0].get("_exact_cancel_result") if results and isinstance(results[0], dict) else {}) or {}
        return {
            "success": True,
            "code": 0,
            "data": results,
            "_exact_cancel_result": first_exact,
            "_batch_exact_cancel_results": [r.get("_exact_cancel_result") for r in results if isinstance(r, dict)],
        }

    async def cancel_conditional_orders_exact(self, stop_order_ids: List[str | int], symbol: str | None = None) -> Dict[str, Any]:
        # BingX v2 uses the same order endpoint for STOP_MARKET/TAKE_PROFIT_MARKET.
        # The caller must pass exact IDs and the symbol; broad/symbol-less cancel
        # is intentionally forbidden because unrelated manual/user orders may exist.
        cleaned = [clean_exchange_id(x) for x in stop_order_ids if clean_exchange_id(x)]
        if not cleaned:
            return {"success": True, "code": 0, "data": []}
        if not symbol:
            raise BingxOrderCancelUnconfirmed(
                order_id=cleaned[0],
                error_message="BingX exact conditional cancel requires symbol",
            )
        return await self.cancel_regular_orders_exact(cleaned, symbol=symbol)

    async def fetch_open_orders(self, symbol: str | None = None) -> List[Dict[str, Any]]:
        endpoint = "/openApi/swap/v2/trade/openOrders"
        params: Dict[str, Any] = {}
        requested_symbol = _normalize_symbol(symbol) if symbol else ""
        if symbol:
            params["symbol"] = _to_bingx_symbol(symbol)
        data = await self._request("GET", endpoint, params=params, auth=True)
        raw_rows = self._strict_collection_rows(
            data,
            endpoint=endpoint,
            wrapper_keys=("orders", "list", "data"),
        )
        rows: list[dict[str, Any]] = []
        for index, raw_row in enumerate(raw_rows):
            payload = self._order_payload_from_response(raw_row)

            # Validate scope before any other row fields.  BingX has returned
            # valid rows for another symbol even when ``symbol=...`` was sent.
            # A malformed/missing symbol still fails closed, but a provably
            # cross-symbol row must not poison STOP/TP verification for the
            # requested contract merely because its status/type schema differs.
            raw_symbol = self._symbol_from_order_response(payload)
            row_symbol = self._strict_contract_symbol(
                raw_symbol,
                endpoint=endpoint,
                field="symbol",
                row_index=index,
            )
            if requested_symbol and row_symbol != requested_symbol:
                continue

            order_id = self._order_id_from_response(payload)
            if not order_id:
                raise BingxResponseIntegrityError(
                    endpoint=endpoint,
                    reason=f"row[{index}].orderId is missing or invalid",
                )

            order_type = str(payload.get("type") or "").strip().upper()
            if order_type not in self._KNOWN_ORDER_TYPES:
                raise BingxResponseIntegrityError(
                    endpoint=endpoint,
                    reason=f"row[{index}].type is missing or unknown",
                )

            (
                status_text,
                status_recognized,
                status_state,
                status_terminal,
                status_fully_filled,
            ) = self._validated_order_status_from_payload(
                payload,
                endpoint=endpoint,
                row_index=index,
                allow_opaque=True,
            )
            if not status_recognized:
                self._warn_opaque_open_order_status(
                    symbol=row_symbol,
                    order_id=order_id,
                    order_type=order_type,
                    status_text=status_text,
                )

            raw_side = str(payload.get("side") or "").strip().upper()
            if raw_side not in {"BUY", "SELL"}:
                raise BingxResponseIntegrityError(
                    endpoint=endpoint,
                    reason=f"row[{index}].side is missing or invalid",
                )

            raw_position_side = str(payload.get("positionSide") or "").strip().upper()
            if raw_position_side and raw_position_side not in {"BOTH", "LONG", "SHORT"}:
                raise BingxResponseIntegrityError(
                    endpoint=endpoint,
                    reason=f"row[{index}].positionSide is invalid",
                )

            original_quantities: list[tuple[str, Decimal]] = []
            for qty_field in ("origQty", "quantity", "qty", "vol"):
                if qty_field not in payload:
                    continue
                quantity = self._strict_finite_decimal(
                    payload.get(qty_field),
                    endpoint=endpoint,
                    field=qty_field,
                    row_index=index,
                )
                if quantity < 0:
                    raise BingxResponseIntegrityError(
                        endpoint=endpoint,
                        reason=f"row[{index}].{qty_field} is negative",
                    )
                original_quantities.append((qty_field, quantity))

            positive_originals = [quantity for _field, quantity in original_quantities if quantity > 0]
            if len({quantity.normalize() for quantity in positive_originals}) > 1:
                raise BingxResponseIntegrityError(
                    endpoint=endpoint,
                    reason=f"row[{index}] has contradictory original quantity fields",
                )
            original_qty = positive_originals[0] if positive_originals else Decimal("0")
            if not self._is_conditional_close_type(order_type) and original_qty <= 0:
                raise BingxResponseIntegrityError(
                    endpoint=endpoint,
                    reason=f"row[{index}] regular order has no positive quantity",
                )

            filled_quantities: list[tuple[str, Decimal]] = []
            for filled_field in (
                "executedQty",
                "cumQty",
                "filledQty",
                "dealVol",
                "executedVolume",
                "realityVol",
            ):
                if filled_field not in payload:
                    continue
                filled_quantity = self._strict_finite_decimal(
                    payload.get(filled_field),
                    endpoint=endpoint,
                    field=filled_field,
                    row_index=index,
                )
                if filled_quantity < 0:
                    raise BingxResponseIntegrityError(
                        endpoint=endpoint,
                        reason=f"row[{index}].{filled_field} is negative",
                    )
                filled_quantities.append((filled_field, filled_quantity))

            if len({quantity.normalize() for _field, quantity in filled_quantities}) > 1:
                raise BingxResponseIntegrityError(
                    endpoint=endpoint,
                    reason=f"row[{index}] has contradictory filled quantity fields",
                )
            filled_qty = filled_quantities[0][1] if filled_quantities else Decimal("0")
            if original_qty > 0:
                quantity_tolerance = max(
                    Decimal("1e-12"),
                    abs(original_qty) * Decimal("1e-12"),
                )
                if filled_qty > original_qty + quantity_tolerance:
                    raise BingxResponseIntegrityError(
                        endpoint=endpoint,
                        reason=(
                            f"row[{index}] filled quantity exceeds original quantity "
                            f"({filled_qty} > {original_qty})"
                        ),
                    )

            reduce_only = self._strict_optional_bool(
                payload.get("reduceOnly"),
                endpoint=endpoint,
                field="reduceOnly",
                row_index=index,
                default=False,
            )

            normalized = self._normalize_order_row(payload)
            zero_fill_partial_status = bool(
                int(status_state) == 2
                and filled_qty <= 0
                and not self._is_conditional_close_type(order_type)
            )
            # Preserve the exact validated values even if a future normalizer
            # fallback changes.  This makes the pre-entry guard fail closed on
            # malformed input rather than silently losing identity/symbol.
            normalized["orderId"] = order_id
            normalized["id"] = order_id
            normalized["symbol"] = row_symbol
            normalized["type"] = order_type
            normalized["reduceOnly"] = bool(
                reduce_only or self._is_conditional_close_type(order_type)
            )
            normalized["exchangeStatus"] = status_text
            normalized["statusRecognized"] = bool(status_recognized)
            normalized["_open_orders_status_opaque_active"] = not bool(status_recognized)
            normalized["state"] = 1 if zero_fill_partial_status else int(status_state)
            normalized["state_name"] = (
                "NEW" if zero_fill_partial_status else status_text
            )
            normalized["status"] = "NEW" if zero_fill_partial_status else status_text
            normalized["fill_state"] = (
                "unfilled_active"
                if filled_qty <= 0
                else "partially_filled"
                if not status_fully_filled
                else "fully_filled"
            )
            normalized["status_diagnostic"] = (
                "exchange_partial_status_without_filled_quantity"
                if zero_fill_partial_status
                else ""
            )
            normalized["terminal"] = bool(status_terminal)
            normalized["fully_filled"] = bool(status_fully_filled)
            normalized["tpExecuted"] = bool(
                status_fully_filled
                and self._is_take_profit_like_type(order_type)
                and self._f(normalized.get("filledQty"), 0.0) > 0
            )
            normalized["stopExecuted"] = bool(
                status_fully_filled
                and self._is_stop_like_type(order_type)
                and self._f(normalized.get("filledQty"), 0.0) > 0
            )
            rows.append(normalized)

        if requested_symbol:
            # BingX normally honours the symbol filter, but the trade safety
            # preflight must never trust an exchange/server quirk blindly.  A
            # valid cross-symbol row is ignored; a row with no symbol is rejected
            # above because its ownership/scope cannot be proved safely.
            rows = [r for r in rows if _normalize_symbol(r.get("symbol")) == requested_symbol]
        return rows

    async def fetch_open_algo_orders(self, symbol: str | None = None) -> List[Dict[str, Any]]:
        # BingX current openOrders includes STOP/TAKE_PROFIT/trigger rows.  Keep
        # this compatibility split intentionally broad: live payloads may return
        # TRIGGER_* or TAKE_STOP_* names instead of the narrow STOP_MARKET /
        # TAKE_PROFIT_MARKET pair, while still representing protective TP/SL.
        rows = await self.fetch_open_orders(symbol)
        return [r for r in rows if self._is_conditional_close_type(r.get("type")) or _first_present_finite(r, ("stopLossPrice", "takeProfitPrice", "triggerPrice", "stopPrice")) > 0]

    async def fetch_public_klines(
        self,
        *,
        symbol: str,
        start_time_ms: int,
        end_time_ms: int,
        interval: str = "1m",
        limit: int = 1440,
    ) -> List[Dict[str, Any]]:
        """Return strictly validated public candles for statistics gap recovery.

        This method is read-only and intentionally limited to 1-minute candles.
        It accepts the documented BingX array shape
        ``[openTime, open, high, low, close, volume, closeTime]`` and explicit
        object rows. The endpoint remains routed through the adapter's existing
        market-data request path so no trading write is introduced.
        Unknown array layouts fail closed instead of guessing intrabar prices.
        """

        endpoint = "/openApi/swap/v3/quote/klines"
        try:
            start_ts = int(start_time_ms)
            end_ts = int(end_time_ms)
            bounded_limit = int(limit)
        except (TypeError, ValueError, OverflowError) as exc:
            raise BingxApiError("BingX kline parameters are invalid") from exc
        if start_ts <= 0 or end_ts <= start_ts:
            raise BingxApiError("BingX klines require 0 < startTime < endTime")
        if str(interval).strip() != "1m":
            raise BingxApiError("statistics gap recovery supports only 1m klines")
        if bounded_limit < 1 or bounded_limit > 1440:
            raise BingxApiError("BingX kline limit must be between 1 and 1440")
        if end_ts - start_ts > bounded_limit * 60_000:
            raise BingxApiError("BingX kline request exceeds the bounded 1m window")

        requested_symbol = self._strict_contract_symbol(
            _to_bingx_symbol(symbol),
            endpoint=endpoint,
            field="symbol",
            row_index=0,
        )
        params: Dict[str, Any] = {
            "symbol": _to_bingx_symbol(requested_symbol),
            "interval": "1m",
            "startTime": start_ts,
            "endTime": end_ts,
            "limit": bounded_limit,
        }
        from app.services.workload_manager import (
            PRIORITY_FINANCIAL,
            bingx_request_context,
        )

        with bingx_request_context(
            priority=PRIORITY_FINANCIAL,
            label="statistics_gap_recovery_klines",
        ):
            data = await self._request("GET", endpoint, params=params, auth=False)

        if isinstance(data, list):
            raw_rows = data
        elif isinstance(data, dict):
            present = [key for key in ("klines", "list", "rows", "data") if key in data]
            if len(present) != 1 or not isinstance(data.get(present[0]), list):
                raise BingxResponseIntegrityError(
                    endpoint=endpoint,
                    reason="expected one explicit kline list wrapper",
                )
            raw_rows = data[present[0]]
        else:
            raise BingxResponseIntegrityError(
                endpoint=endpoint,
                reason=f"expected kline list payload, got {type(data).__name__}",
            )

        normalized: dict[int, dict[str, Any]] = {}
        for row_index, raw in enumerate(raw_rows):
            if isinstance(raw, dict):
                raw_time = next(
                    (raw.get(key) for key in ("openTime", "time", "timestamp", "T")
                     if raw.get(key) not in (None, "")),
                    None,
                )
                raw_close_time = next(
                    (raw.get(key) for key in ("closeTime", "close_time")
                     if raw.get(key) not in (None, "")),
                    None,
                )
                raw_open = next((raw.get(k) for k in ("open", "o") if raw.get(k) not in (None, "")), None)
                raw_close = next((raw.get(k) for k in ("close", "c") if raw.get(k) not in (None, "")), None)
                raw_high = next((raw.get(k) for k in ("high", "h") if raw.get(k) not in (None, "")), None)
                raw_low = next((raw.get(k) for k in ("low", "l") if raw.get(k) not in (None, "")), None)
            elif isinstance(raw, (list, tuple)) and len(raw) >= 7:
                # BingX swap v3 documented array:
                # [openTime, open, high, low, close, volume, closeTime].
                raw_time, raw_open, raw_high, raw_low, raw_close, _, raw_close_time = raw[:7]
            else:
                raise BingxResponseIntegrityError(
                    endpoint=endpoint,
                    reason=f"row[{row_index}] has an unsupported kline shape",
                )
            if isinstance(raw_time, bool):
                raise BingxResponseIntegrityError(
                    endpoint=endpoint, reason=f"row[{row_index}].time is invalid"
                )
            try:
                candle_time = int(str(raw_time).strip())
            except (TypeError, ValueError, OverflowError):
                raise BingxResponseIntegrityError(
                    endpoint=endpoint, reason=f"row[{row_index}].time is invalid"
                ) from None
            if candle_time < start_ts - 60_000 or candle_time > end_ts + 60_000:
                raise BingxResponseIntegrityError(
                    endpoint=endpoint,
                    reason=f"row[{row_index}].time is outside request scope",
                )
            if raw_close_time not in (None, ""):
                if isinstance(raw_close_time, bool):
                    raise BingxResponseIntegrityError(
                        endpoint=endpoint,
                        reason=f"row[{row_index}].closeTime is invalid",
                    )
                try:
                    close_time = int(str(raw_close_time).strip())
                except (TypeError, ValueError, OverflowError):
                    raise BingxResponseIntegrityError(
                        endpoint=endpoint,
                        reason=f"row[{row_index}].closeTime is invalid",
                    ) from None
                # Some APIs publish the inclusive millisecond (open+59999),
                # others the exclusive boundary (open+60000). Accept only those
                # two explicit 1-minute representations.
                if close_time not in {candle_time + 59_999, candle_time + 60_000}:
                    raise BingxResponseIntegrityError(
                        endpoint=endpoint,
                        reason=f"row[{row_index}] is not an exact 1m candle",
                    )
            open_d = self._strict_finite_decimal(
                raw_open, endpoint=endpoint, field="open", row_index=row_index
            )
            close_d = self._strict_finite_decimal(
                raw_close, endpoint=endpoint, field="close", row_index=row_index
            )
            high_d = self._strict_finite_decimal(
                raw_high, endpoint=endpoint, field="high", row_index=row_index
            )
            low_d = self._strict_finite_decimal(
                raw_low, endpoint=endpoint, field="low", row_index=row_index
            )
            if min(open_d, close_d, high_d, low_d) <= 0:
                raise BingxResponseIntegrityError(
                    endpoint=endpoint, reason=f"row[{row_index}] contains non-positive prices"
                )
            if high_d < max(open_d, close_d) or low_d > min(open_d, close_d) or high_d < low_d:
                raise BingxResponseIntegrityError(
                    endpoint=endpoint, reason=f"row[{row_index}] OHLC bounds are invalid"
                )
            canonical = {
                "symbol": requested_symbol,
                "interval": "1m",
                "openTime": candle_time,
                "closeTime": candle_time + 60_000,
                "open": format(open_d, "f"),
                "high": format(high_d, "f"),
                "low": format(low_d, "f"),
                "close": format(close_d, "f"),
            }
            previous = normalized.get(candle_time)
            if previous is not None and previous != canonical:
                raise BingxResponseIntegrityError(
                    endpoint=endpoint, reason=f"conflicting duplicate candle {candle_time}"
                )
            normalized[candle_time] = canonical
        return [normalized[key] for key in sorted(normalized)]


    async def fetch_funding_income(
        self,
        *,
        symbol: str,
        start_time_ms: int,
        end_time_ms: int,
        limit: int = 1000,
    ) -> List[Dict[str, Any]]:
        """Return strictly validated BingX FUNDING_FEE income rows.

        This authenticated read-only endpoint is used only by the existing
        low-priority financial reconciliation worker.  It never participates
        in trade execution or position protection.
        """

        endpoint = "/openApi/swap/v2/user/income"
        if isinstance(start_time_ms, bool) or isinstance(end_time_ms, bool):
            raise BingxApiError("BingX income time window is invalid")
        try:
            start_ts = int(start_time_ms)
            end_ts = int(end_time_ms)
            bounded_limit = int(limit)
        except (TypeError, ValueError, OverflowError) as exc:
            raise BingxApiError("BingX income parameters are invalid") from exc
        if start_ts <= 0 or end_ts <= 0 or start_ts >= end_ts:
            raise BingxApiError("BingX income requires 0 < startTime < endTime")
        if end_ts - start_ts > 90 * 24 * 60 * 60 * 1000:
            raise BingxApiError("BingX income history supports at most 3 months")
        if bounded_limit < 1 or bounded_limit > 1000:
            raise BingxApiError("BingX income limit must be between 1 and 1000")

        requested_symbol = self._strict_contract_symbol(
            _to_bingx_symbol(symbol),
            endpoint=endpoint,
            field="symbol",
            row_index=0,
        )
        quote = "USDT" if requested_symbol.endswith("USDT") else (
            "USDC" if requested_symbol.endswith("USDC") else ""
        )
        if not quote:
            raise BingxApiError("BingX funding income supports USDT/USDC symbols")

        params: Dict[str, Any] = {
            "symbol": _to_bingx_symbol(requested_symbol),
            "incomeType": "FUNDING_FEE",
            "startTime": start_ts,
            "endTime": end_ts,
            "limit": bounded_limit,
        }
        from app.services.workload_manager import (
            PRIORITY_FINANCIAL,
            bingx_request_context,
        )

        with bingx_request_context(
            priority=PRIORITY_FINANCIAL,
            label="financial_funding_income",
        ):
            data = await self._request("GET", endpoint, params=params, auth=True)

        raw_rows = self._strict_collection_rows(
            data,
            endpoint=endpoint,
            wrapper_keys=("income", "list", "rows"),
        )
        # The endpoint has no documented page cursor.  A full page cannot prove
        # completeness and therefore must not be treated as FINAL.
        if len(raw_rows) >= bounded_limit:
            raise BingxResponseIntegrityError(
                endpoint=endpoint,
                reason="funding income result may be truncated",
            )

        normalized: list[dict[str, Any]] = []
        seen: dict[str, tuple[str, ...]] = {}
        for row_index, raw_row in enumerate(raw_rows):
            row_symbol = self._strict_contract_symbol(
                raw_row.get("symbol"),
                endpoint=endpoint,
                field="symbol",
                row_index=row_index,
            )
            if row_symbol != requested_symbol:
                raise BingxResponseIntegrityError(
                    endpoint=endpoint,
                    reason=f"row[{row_index}].symbol is outside exact request scope",
                )
            income_type = str(raw_row.get("incomeType") or "").strip().upper()
            if income_type != "FUNDING_FEE":
                raise BingxResponseIntegrityError(
                    endpoint=endpoint,
                    reason=f"row[{row_index}].incomeType is outside exact request scope",
                )
            income = self._strict_finite_decimal(
                raw_row.get("income"),
                endpoint=endpoint,
                field="income",
                row_index=row_index,
            )
            asset = str(raw_row.get("asset") or "").strip().upper()
            if asset != quote:
                raise BingxResponseIntegrityError(
                    endpoint=endpoint,
                    reason=f"row[{row_index}].asset is outside exact request scope",
                )
            raw_time = raw_row.get("time")
            if isinstance(raw_time, bool):
                raise BingxResponseIntegrityError(
                    endpoint=endpoint,
                    reason=f"row[{row_index}].time is invalid",
                )
            try:
                event_time = int(str(raw_time).strip())
            except (TypeError, ValueError, OverflowError):
                raise BingxResponseIntegrityError(
                    endpoint=endpoint,
                    reason=f"row[{row_index}].time is invalid",
                ) from None
            if event_time < start_ts or event_time > end_ts:
                raise BingxResponseIntegrityError(
                    endpoint=endpoint,
                    reason=f"row[{row_index}].time is outside exact request scope",
                )
            def _plain_decimal(value: Decimal) -> str:
                result = format(value, "f")
                if "." in result:
                    result = result.rstrip("0").rstrip(".")
                return "0" if result in {"", "-0"} else result

            tran_id = clean_exchange_id(raw_row.get("tranId"))
            trade_id = clean_exchange_id(raw_row.get("tradeId"))
            event_id = tran_id or trade_id
            identity_source = "exchange"
            if not event_id:
                # The live BingX income endpoint can omit both tranId and
                # tradeId for otherwise complete FUNDING_FEE rows.  Rejecting
                # such rows made an exact closed execution permanently
                # AMBIGUOUS even though symbol, amount, asset and event time
                # were all present.  Build a deterministic account-local
                # identity from the immutable canonical event fields instead.
                # The database uniqueness key also includes user_id, so the
                # same funding tuple on two accounts cannot collide.
                identity_payload = {
                    "asset": asset,
                    "income": _plain_decimal(income),
                    "incomeType": income_type,
                    "symbol": row_symbol,
                    "time": event_time,
                }
                digest = hashlib.sha256(
                    json.dumps(
                        identity_payload,
                        ensure_ascii=False,
                        sort_keys=True,
                        separators=(",", ":"),
                    ).encode("utf-8")
                ).hexdigest()
                event_id = f"derived-funding:{digest}"
                identity_source = "derived_canonical_fields"

            result = {
                "exchangeEventId": event_id,
                "tranId": tran_id or None,
                "tradeId": trade_id or None,
                "identitySource": identity_source,
                "symbol": row_symbol,
                "incomeType": income_type,
                "income": _plain_decimal(income),
                "asset": asset,
                "time": event_time,
                "info": str(raw_row.get("info") or "")[:500],
                "raw": dict(raw_row),
            }
            fingerprint = (
                row_symbol,
                result["income"],
                asset,
                str(event_time),
                str(tran_id or ""),
                str(trade_id or ""),
            )
            previous = seen.get(event_id)
            if previous is not None and previous != fingerprint:
                raise BingxResponseIntegrityError(
                    endpoint=endpoint,
                    reason=f"conflicting duplicate funding event {event_id}",
                )
            if previous is None:
                seen[event_id] = fingerprint
                normalized.append(result)

        return sorted(
            normalized,
            key=lambda row: (int(row["time"]), str(row["exchangeEventId"])),
        )


    async def fetch_funding_income_recovery_variant(
        self,
        *,
        symbol: str,
        start_time_ms: int,
        end_time_ms: int,
        limit: int = 1000,
        variant: str,
    ) -> Dict[str, Any]:
        """Read one G47 funding-recovery query scope and filter it exactly.

        The method is read-only and supports the fixed G47/G48 query shapes.
        Broader responses are fully shape-validated, bounded by
        ``limit``, and then filtered locally to the requested symbol and
        ``FUNDING_FEE``.  It never turns ``data: null`` into an empty list.
        """

        endpoint = "/openApi/swap/v2/user/income"
        variant_name = str(variant or "").strip().lower()
        allowed_variants = {
            "exact": (True, True),
            "symbol_all_types": (True, False),
            "all_symbols_funding": (False, True),
            # G48 adds a fourth independent query shape with neither optional
            # server-side filter.  The response is still strictly validated,
            # bounded, and locally reduced to the exact symbol + FUNDING_FEE
            # set.  A full page remains ambiguous and data:null still fails.
            "all_income_unfiltered": (False, False),
        }
        if variant_name not in allowed_variants:
            raise BingxApiError(f"Unsupported funding recovery variant: {variant_name}")
        include_symbol, include_income_type = allowed_variants[variant_name]

        if isinstance(start_time_ms, bool) or isinstance(end_time_ms, bool):
            raise BingxApiError("BingX income time window is invalid")
        try:
            start_ts = int(start_time_ms)
            end_ts = int(end_time_ms)
            bounded_limit = int(limit)
        except (TypeError, ValueError, OverflowError) as exc:
            raise BingxApiError("BingX income parameters are invalid") from exc
        if start_ts <= 0 or end_ts <= 0 or start_ts >= end_ts:
            raise BingxApiError("BingX income requires 0 < startTime < endTime")
        if end_ts - start_ts > 90 * 24 * 60 * 60 * 1000:
            raise BingxApiError("BingX income history supports at most 3 months")
        if bounded_limit < 1 or bounded_limit > 1000:
            raise BingxApiError("BingX income limit must be between 1 and 1000")

        requested_symbol = self._strict_contract_symbol(
            _to_bingx_symbol(symbol),
            endpoint=endpoint,
            field="symbol",
            row_index=0,
        )
        quote = "USDT" if requested_symbol.endswith("USDT") else (
            "USDC" if requested_symbol.endswith("USDC") else ""
        )
        if not quote:
            raise BingxApiError("BingX funding income supports USDT/USDC symbols")

        params: Dict[str, Any] = {
            "startTime": start_ts,
            "endTime": end_ts,
            "limit": bounded_limit,
        }
        if include_symbol:
            params["symbol"] = _to_bingx_symbol(requested_symbol)
        if include_income_type:
            params["incomeType"] = "FUNDING_FEE"

        from app.services.workload_manager import (
            PRIORITY_FINANCIAL,
            bingx_request_context,
        )

        with bingx_request_context(
            priority=PRIORITY_FINANCIAL,
            label=f"financial_funding_recovery_{variant_name}",
        ):
            data = await self._request("GET", endpoint, params=params, auth=True)

        raw_rows = self._strict_collection_rows(
            data,
            endpoint=endpoint,
            wrapper_keys=("income", "list", "rows"),
        )
        if len(raw_rows) >= bounded_limit:
            raise BingxResponseIntegrityError(
                endpoint=endpoint,
                reason=f"funding recovery variant {variant_name} may be truncated",
            )

        def _plain_decimal(value: Decimal) -> str:
            result = format(value, "f")
            if "." in result:
                result = result.rstrip("0").rstrip(".")
            return "0" if result in {"", "-0"} else result

        normalized: list[dict[str, Any]] = []
        seen: dict[str, tuple[str, ...]] = {}
        ignored_non_target_rows = 0
        for row_index, raw_row in enumerate(raw_rows):
            # G59: BingX has been observed leaking unrelated income rows even
            # when a recovery query supplies server-side symbol/incomeType
            # filters. Recovery views therefore classify the minimum safe fields
            # first and validate the full contract only for the exact target
            # candidate. The ordinary fetch_funding_income() reader above stays
            # intentionally strict and unchanged.
            income_type = str(raw_row.get("incomeType") or "").strip().upper()
            if not income_type or len(income_type) > 64 or not all(
                ch.isalnum() or ch == "_" for ch in income_type
            ):
                raise BingxResponseIntegrityError(
                    endpoint=endpoint,
                    reason=f"row[{row_index}].incomeType is invalid",
                )
            if income_type != "FUNDING_FEE":
                ignored_non_target_rows += 1
                continue

            row_symbol = self._strict_contract_symbol(
                raw_row.get("symbol"),
                endpoint=endpoint,
                field="symbol",
                row_index=row_index,
            )
            if row_symbol != requested_symbol:
                ignored_non_target_rows += 1
                continue

            # From this point onward the row is an exact target candidate:
            # FUNDING_FEE + requested contract. Any malformed value is a hard
            # integrity failure; no candidate field is guessed or defaulted.
            income = self._strict_finite_decimal(
                raw_row.get("income"),
                endpoint=endpoint,
                field="income",
                row_index=row_index,
            )
            asset = str(raw_row.get("asset") or "").strip().upper()
            if not asset or len(asset) > 16 or not asset.isalnum():
                raise BingxResponseIntegrityError(
                    endpoint=endpoint,
                    reason=f"row[{row_index}].asset is invalid",
                )
            if asset != quote:
                raise BingxResponseIntegrityError(
                    endpoint=endpoint,
                    reason=f"row[{row_index}].asset is outside exact funding scope",
                )
            raw_time = raw_row.get("time")
            if isinstance(raw_time, bool):
                raise BingxResponseIntegrityError(
                    endpoint=endpoint,
                    reason=f"row[{row_index}].time is invalid",
                )
            try:
                event_time = int(str(raw_time).strip())
            except (TypeError, ValueError, OverflowError):
                raise BingxResponseIntegrityError(
                    endpoint=endpoint,
                    reason=f"row[{row_index}].time is invalid",
                ) from None
            if event_time < start_ts or event_time > end_ts:
                raise BingxResponseIntegrityError(
                    endpoint=endpoint,
                    reason=f"row[{row_index}].time is outside recovery request scope",
                )

            tran_id = clean_exchange_id(raw_row.get("tranId"))
            trade_id = clean_exchange_id(raw_row.get("tradeId"))
            event_id = tran_id or trade_id
            identity_source = "exchange"
            if not event_id:
                identity_payload = {
                    "asset": asset,
                    "income": _plain_decimal(income),
                    "incomeType": income_type,
                    "symbol": row_symbol,
                    "time": event_time,
                }
                digest = hashlib.sha256(
                    json.dumps(
                        identity_payload,
                        ensure_ascii=False,
                        sort_keys=True,
                        separators=(",", ":"),
                    ).encode("utf-8")
                ).hexdigest()
                event_id = f"derived-funding:{digest}"
                identity_source = "derived_canonical_fields"

            raw_metadata = dict(raw_row)
            raw_metadata["statistics_recovery_variant"] = variant_name
            raw_metadata["statistics_recovery_scope_row_count"] = len(raw_rows)
            result = {
                "exchangeEventId": event_id,
                "tranId": tran_id or None,
                "tradeId": trade_id or None,
                "identitySource": identity_source,
                "symbol": row_symbol,
                "incomeType": income_type,
                "income": _plain_decimal(income),
                "asset": asset,
                "time": event_time,
                "info": str(raw_row.get("info") or "")[:500],
                "raw": raw_metadata,
            }
            fingerprint = (
                row_symbol,
                result["income"],
                asset,
                str(event_time),
                str(tran_id or ""),
                str(trade_id or ""),
            )
            previous = seen.get(event_id)
            if previous is not None and previous != fingerprint:
                raise BingxResponseIntegrityError(
                    endpoint=endpoint,
                    reason=f"conflicting duplicate funding event {event_id}",
                )
            if previous is None:
                seen[event_id] = fingerprint
                normalized.append(result)

        normalized.sort(key=lambda row: (int(row["time"]), str(row["exchangeEventId"])))
        return {
            "variant": variant_name,
            "scope_row_count": len(raw_rows),
            "ignored_non_target_rows": ignored_non_target_rows,
            "rows": normalized,
        }

    async def fetch_trade_fills(
        self,
        *,
        symbol: str,
        order_id: str,
        start_time_ms: int,
        end_time_ms: int,
        trading_unit: str = "CONT",
        currency: str | None = None,
    ) -> List[Dict[str, Any]]:
        """Return exact, strictly validated BingX trade fills for one order.

        This read is reserved for deferred financial reconciliation. It always
        runs under ``PRIORITY_FINANCIAL`` and never participates in ENTRY, STOP,
        TP, BE, lifecycle, or risk-slot decisions. The exchange-side orderId,
        contract symbol and requested time window are treated as hard scope:
        any cross-scope row fails closed instead of being ignored.
        """

        endpoint = "/openApi/swap/v2/trade/fillHistory"
        exact_order_id = clean_exchange_id(order_id)
        if not exact_order_id or not exact_order_id.isdigit():
            raise BingxApiError("BingX fillHistory requires an exact numeric orderId")
        if isinstance(start_time_ms, bool) or isinstance(end_time_ms, bool):
            raise BingxApiError("BingX fillHistory time window is invalid")
        try:
            start_ts = int(start_time_ms)
            end_ts = int(end_time_ms)
        except (TypeError, ValueError, OverflowError) as exc:
            raise BingxApiError("BingX fillHistory time window is invalid") from exc
        if start_ts <= 0 or end_ts <= 0 or start_ts >= end_ts:
            raise BingxApiError("BingX fillHistory requires 0 < startTs < endTs")

        # The documented endpoint accepts at most the latest seven days.  The
        # worker normally runs seconds after terminal closure, but its durable
        # fallback lookback may be wider.  Clamp the request instead of sending
        # a range BingX rejects with a business error.
        max_history_range_ms = 7 * 24 * 60 * 60 * 1000 - 60_000
        start_ts = max(start_ts, end_ts - max_history_range_ms)

        requested_symbol = self._strict_contract_symbol(
            _to_bingx_symbol(symbol),
            endpoint=endpoint,
            field="symbol",
            row_index=0,
        )
        unit = str(trading_unit or "CONT").strip().upper()
        if unit != "CONT":
            raise BingxApiError("BingX financial fillHistory requires CONT quantities")
        if requested_symbol.endswith("USDT"):
            quote = "USDT"
        elif requested_symbol.endswith("USDC"):
            quote = "USDC"
        else:
            raise BingxApiError(
                "BingX fillHistory supports only USDT/USDC settlement symbols"
            )
        settlement = str(currency or quote).strip().upper()
        if settlement not in {"USDT", "USDC"} or settlement != quote:
            raise BingxApiError("BingX fillHistory currency must match symbol quote")

        params: Dict[str, Any] = {
            "symbol": _to_bingx_symbol(requested_symbol),
            "currency": settlement,
            "orderId": exact_order_id,
            "lastFillId": 0,
            "pageIndex": 1,
            "pageSize": 1000,
            "startTs": start_ts,
            "endTs": end_ts,
        }
        from app.services.workload_manager import (
            PRIORITY_FINANCIAL,
            bingx_request_context,
        )

        with bingx_request_context(
            priority=PRIORITY_FINANCIAL,
            label="financial_fill_history",
        ):
            data = await self._request("GET", endpoint, params=params, auth=True)

        raw_rows = self._strict_collection_rows(
            data,
            endpoint=endpoint,
            wrapper_keys=("fill_history_orders",),
        )
        if isinstance(data, dict) and data.get("total") not in (None, ""):
            try:
                total = int(str(data.get("total")).strip())
            except (TypeError, ValueError, OverflowError):
                raise BingxResponseIntegrityError(
                    endpoint=endpoint,
                    reason="total is invalid",
                ) from None
            if total < len(raw_rows):
                raise BingxResponseIntegrityError(
                    endpoint=endpoint,
                    reason="total is smaller than returned fill count",
                )
            if total > len(raw_rows):
                raise BingxResponseIntegrityError(
                    endpoint=endpoint,
                    reason="exact order fill history is truncated",
                )

        def _required_alias(
            row: Dict[str, Any],
            aliases: tuple[str, ...],
            *,
            field: str,
            row_index: int,
        ) -> Any:
            present = [(key, row.get(key)) for key in aliases if row.get(key) not in (None, "")]
            if not present:
                raise BingxResponseIntegrityError(
                    endpoint=endpoint,
                    reason=f"row[{row_index}].{field} is missing",
                )
            canonical = str(present[0][1]).strip()
            if any(str(value).strip() != canonical for _key, value in present[1:]):
                raise BingxResponseIntegrityError(
                    endpoint=endpoint,
                    reason=f"row[{row_index}] has contradictory {field} aliases",
                )
            return present[0][1]

        normalized_rows: list[dict[str, Any]] = []
        seen_trade_ids: dict[str, tuple[str, ...]] = {}
        for row_index, raw_row in enumerate(raw_rows):
            trade_id = clean_exchange_id(
                _required_alias(
                    raw_row,
                    ("tradeId", "tradeID"),
                    field="tradeId",
                    row_index=row_index,
                )
            )
            row_order_id = clean_exchange_id(
                _required_alias(
                    raw_row,
                    ("orderId", "orderID"),
                    field="orderId",
                    row_index=row_index,
                )
            )
            if not trade_id or not trade_id.isdigit():
                raise BingxResponseIntegrityError(
                    endpoint=endpoint,
                    reason=f"row[{row_index}].tradeId is invalid",
                )
            if row_order_id != exact_order_id:
                raise BingxResponseIntegrityError(
                    endpoint=endpoint,
                    reason=f"row[{row_index}].orderId is outside exact request scope",
                )

            row_symbol = self._strict_contract_symbol(
                _required_alias(
                    raw_row,
                    ("symbol", "s"),
                    field="symbol",
                    row_index=row_index,
                ),
                endpoint=endpoint,
                field="symbol",
                row_index=row_index,
            )
            if row_symbol != requested_symbol:
                raise BingxResponseIntegrityError(
                    endpoint=endpoint,
                    reason=f"row[{row_index}].symbol is outside exact request scope",
                )

            side = str(
                _required_alias(
                    raw_row,
                    ("side",),
                    field="side",
                    row_index=row_index,
                )
            ).strip().upper()
            if side not in {"BUY", "SELL"}:
                raise BingxResponseIntegrityError(
                    endpoint=endpoint,
                    reason=f"row[{row_index}].side is invalid",
                )
            position_side = str(raw_row.get("positionSide") or "").strip().upper()
            if position_side not in {"LONG", "SHORT"}:
                raise BingxResponseIntegrityError(
                    endpoint=endpoint,
                    reason=f"row[{row_index}].positionSide is invalid",
                )

            price = self._strict_finite_decimal(
                _required_alias(
                    raw_row,
                    ("price",),
                    field="price",
                    row_index=row_index,
                ),
                endpoint=endpoint,
                field="price",
                row_index=row_index,
            )
            qty = self._strict_finite_decimal(
                _required_alias(
                    raw_row,
                    ("qty", "quantity"),
                    field="qty",
                    row_index=row_index,
                ),
                endpoint=endpoint,
                field="qty",
                row_index=row_index,
            )
            pnl_values = [
                raw_row.get(key)
                for key in (
                    "realizedPnl",
                    "realizedPNL",
                    "realisedPnl",
                    "realisedPNL",
                )
                if raw_row.get(key) not in (None, "")
            ]
            if pnl_values:
                realized_pnl = self._strict_finite_decimal(
                    pnl_values[0],
                    endpoint=endpoint,
                    field="realizedPnl",
                    row_index=row_index,
                )
                if any(
                    self._strict_finite_decimal(
                        value,
                        endpoint=endpoint,
                        field="realizedPnl",
                        row_index=row_index,
                    )
                    != realized_pnl
                    for value in pnl_values[1:]
                ):
                    raise BingxResponseIntegrityError(
                        endpoint=endpoint,
                        reason=f"row[{row_index}] has contradictory realizedPnl aliases",
                    )
                pnl_source = "exchange"
            else:
                # U-M fillHistory documents prices, quantities and fees but no
                # realizedPnl field.  The reconciliation aggregate derives the
                # linear-contract gross PnL from exact entry/close fills.
                realized_pnl = Decimal()
                pnl_source = "derived_from_fill_prices"
            fee = self._strict_finite_decimal(
                _required_alias(
                    raw_row,
                    ("commission", "fee"),
                    field="fee",
                    row_index=row_index,
                ),
                endpoint=endpoint,
                field="fee",
                row_index=row_index,
            )
            if price <= 0 or qty <= 0:
                raise BingxResponseIntegrityError(
                    endpoint=endpoint,
                    reason=f"row[{row_index}] price and qty must be positive",
                )

            numeric_times = [
                raw_row.get(key)
                for key in ("time", "tradeTime")
                if raw_row.get(key) not in (None, "")
            ]
            display_time = raw_row.get("filledTm") or raw_row.get("filledTime")
            if not numeric_times and display_time in (None, ""):
                raise BingxResponseIntegrityError(
                    endpoint=endpoint,
                    reason=f"row[{row_index}].time is missing",
                )

            def _time_millis(raw_time: Any, *, row_number: int = row_index) -> int:
                if isinstance(raw_time, bool):
                    raise BingxResponseIntegrityError(
                        endpoint=endpoint,
                        reason=f"row[{row_number}].time is invalid",
                    )
                text = str(raw_time).strip()
                try:
                    if text.lstrip("+-").isdigit():
                        return int(text)
                    parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
                    if parsed.tzinfo is None:
                        raise ValueError("timezone is missing")
                    return int(parsed.astimezone(timezone.utc).timestamp() * 1000)
                except (TypeError, ValueError, OverflowError):
                    raise BingxResponseIntegrityError(
                        endpoint=endpoint,
                        reason=f"row[{row_number}].time is invalid",
                    ) from None

            parsed_times = [_time_millis(value) for value in numeric_times]
            if len(set(parsed_times)) > 1:
                raise BingxResponseIntegrityError(
                    endpoint=endpoint,
                    reason=f"row[{row_index}] has contradictory time aliases",
                )
            # ``filledTm`` is the documented canonical UTC timestamp.  The
            # legacy ``filledTime`` alias has historically used a display-zone
            # offset and must not override a numeric trade time or filledTm.
            fill_time = parsed_times[0] if parsed_times else _time_millis(display_time)
            if fill_time < start_ts or fill_time > end_ts:
                raise BingxResponseIntegrityError(
                    endpoint=endpoint,
                    reason=f"row[{row_index}].time is outside exact request scope",
                )

            fee_assets = [
                str(raw_row.get(key)).strip().upper()
                for key in ("commissionAsset", "feeAsset", "currency")
                if raw_row.get(key) not in (None, "")
            ]
            if not fee_assets:
                raise BingxResponseIntegrityError(
                    endpoint=endpoint,
                    reason=f"row[{row_index}].fee asset is missing",
                )
            if fee_assets and any(value != fee_assets[0] for value in fee_assets[1:]):
                raise BingxResponseIntegrityError(
                    endpoint=endpoint,
                    reason=f"row[{row_index}] has contradictory fee asset aliases",
                )
            fee_asset = fee_assets[0]
            if fee_asset != settlement:
                raise BingxResponseIntegrityError(
                    endpoint=endpoint,
                    reason=f"row[{row_index}].fee asset is outside exact request scope",
                )

            def _plain(value: Decimal) -> str:
                text = format(value, "f")
                if "." in text:
                    text = text.rstrip("0").rstrip(".")
                return "0" if text in {"", "-0"} else text

            normalized = {
                "tradeId": trade_id,
                "orderId": row_order_id,
                "symbol": row_symbol,
                "side": side,
                "positionSide": position_side,
                "price": _plain(price),
                "qty": _plain(qty),
                "realizedPnl": _plain(realized_pnl),
                "fee": _plain(fee),
                "feeAsset": fee_asset,
                "time": fill_time,
                "tradingUnit": unit,
                "realizedPnlSource": pnl_source,
                "raw": dict(raw_row),
            }
            fingerprint = (
                row_order_id,
                row_symbol,
                side,
                position_side,
                normalized["price"],
                normalized["qty"],
                normalized["realizedPnl"],
                normalized["fee"],
                str(fill_time),
            )
            previous = seen_trade_ids.get(trade_id)
            if previous is not None and previous != fingerprint:
                raise BingxResponseIntegrityError(
                    endpoint=endpoint,
                    reason=f"conflicting duplicate tradeId {trade_id}",
                )
            if previous is None:
                seen_trade_ids[trade_id] = fingerprint
                normalized_rows.append(normalized)

        return sorted(
            normalized_rows,
            key=lambda row: (int(row["time"]), str(row["tradeId"])),
        )

    async def fetch_position_tpsl_history(self, symbol: str, *, is_finished: int | bool | None = None, start_time_ms: int | None = None, end_time_ms: int | None = None, page_size: int = 100, max_pages: int = 1) -> List[Dict[str, Any]]:
        endpoint = "/openApi/swap/v2/trade/allOrders"
        limit = min(max(1, int(page_size or 100)), 1000)
        pages = max(1, min(int(max_pages or 1), 10))
        requested_symbol = _normalize_symbol(symbol)
        # BingX rejects allOrders requests whose explicit range is wider than
        # seven days (production code=109400). Lifecycle passes the execution
        # creation time, which can be weeks old, so scan backwards in bounded
        # windows. A one-minute margin avoids server/client millisecond edge
        # disagreement.
        max_query_range_ms = 7 * 24 * 60 * 60 * 1000 - 60_000
        current_end = int(end_time_ms) if end_time_ms else int(time.time() * 1000)
        start = int(start_time_ms) if start_time_ms else None
        if current_end <= 0:
            raise BingxApiError("BingX allOrders endTime must be positive")
        if start is not None and start <= 0:
            start = None
        if start is not None and start >= current_end:
            return []

        collected: list[dict[str, Any]] = []
        seen: set[str] = set()
        for page_index in range(pages):
            params: Dict[str, Any] = {"symbol": _to_bingx_symbol(symbol), "limit": limit}
            request_start: int | None = None
            if start is not None:
                request_start = max(start, current_end - max_query_range_ms)
                params["startTime"] = request_start
            params["endTime"] = current_end
            data = await self._request("GET", endpoint, params=params, auth=True)
            raw_rows = self._strict_collection_rows(
                data,
                endpoint=endpoint,
                wrapper_keys=("orders", "list", "data"),
            )
            if not raw_rows:
                if start is not None and request_start is not None and request_start > start:
                    next_end = request_start - 1
                    if next_end >= current_end:
                        break
                    if page_index + 1 >= pages:
                        raise BingxResponseIntegrityError(
                            endpoint=endpoint,
                            reason="history pagination budget exhausted before scan completed",
                        )
                    current_end = next_end
                    continue
                break

            # Paginate by the oldest row in the complete raw page, not only by
            # STOP/TP rows. Regular orders can fill a page while the relevant
            # protective close sits deeper in history.
            min_page_ts: int | None = None
            for row_index, raw_row in enumerate(raw_rows):
                ts_raw = (
                    raw_row.get("updateTime")
                    or raw_row.get("time")
                    or raw_row.get("transactTime")
                    or raw_row.get("workingTime")
                )
                try:
                    ts = int(float(ts_raw)) if ts_raw not in (None, "") else 0
                except (TypeError, ValueError, OverflowError):
                    ts = 0
                if ts > 0:
                    min_page_ts = ts if min_page_ts is None else min(min_page_ts, ts)

                # Scope before semantic validation. A valid cross-symbol row is
                # irrelevant to this execution and must not poison reconciliation
                # merely because BingX ignored the request filter. A missing or
                # malformed symbol cannot prove ownership and is likewise ignored.
                raw_symbol = raw_row.get("symbol") or raw_row.get("s")
                if _normalize_symbol(raw_symbol) != requested_symbol:
                    continue

                normalized = self._normalize_order_row(raw_row)
                typ = str(normalized.get("type") or "").upper()
                conditional_close = self._is_conditional_close_type(typ)
                raw_reduce_only = bool(normalized.get("reduceOnly"))
                if not conditional_close and not raw_reduce_only:
                    continue

                (
                    status_text,
                    status_recognized,
                    status_state,
                    status_terminal,
                    status_fully_filled,
                ) = self._validated_order_status_from_payload(
                    raw_row,
                    endpoint=endpoint,
                    row_index=row_index,
                    allow_opaque=False,
                )
                # ``allow_opaque=False`` requires every alias to be recognized.
                # Keep this explicit guard as a future-proof invariant if the
                # validator contract changes.
                if not status_recognized:
                    raise BingxResponseIntegrityError(
                        endpoint=endpoint,
                        reason=f"row[{row_index}] history status is not recognized",
                    )
                normalized["exchangeStatus"] = status_text
                normalized["statusRecognized"] = True
                normalized["state"] = int(status_state)
                normalized["state_name"] = status_text
                normalized["status"] = status_text
                normalized["terminal"] = bool(status_terminal)
                normalized["fully_filled"] = bool(status_fully_filled)
                filled_qty = self._f(normalized.get("filledQty"), 0.0)
                normalized["tpExecuted"] = bool(
                    status_fully_filled
                    and self._is_take_profit_like_type(typ)
                    and filled_qty > 0
                )
                normalized["stopExecuted"] = bool(
                    status_fully_filled
                    and self._is_stop_like_type(typ)
                    and filled_qty > 0
                )

                delegated_reduce_only_fill = bool(
                    raw_reduce_only and status_fully_filled and filled_qty > 0
                )
                # BingX can materialize a triggered TP as a regular MARKET/LIMIT
                # child order. Preserve only a terminal reduce-only row; ordinary
                # entry/regular history stays excluded.
                if not conditional_close and not delegated_reduce_only_fill:
                    continue

                oid = clean_exchange_id(
                    normalized.get("orderId")
                    or normalized.get("id")
                    or normalized.get("stopPlanOrderId")
                )
                identity_keys = (
                    "stopOrderId",
                    "stopPlanOrderId",
                    "planOrderId",
                    "orderId",
                    "id",
                    "placeOrderId",
                    "delegatedOrderId",
                    "childOrderId",
                    "triggerOrderId",
                    "algoOrderId",
                    "clientOrderId",
                    "clientOrderID",
                    "client_order_id",
                    "origClientOrderId",
                    "origClientOrderID",
                    "orig_client_order_id",
                )
                identity_values: set[str] = set()
                for scope in (
                    normalized,
                    normalized.get("raw") if isinstance(normalized.get("raw"), dict) else {},
                ):
                    for identity_key in identity_keys:
                        identity = clean_exchange_id(scope.get(identity_key))
                        if identity:
                            identity_values.add(identity)
                # Do not collapse a CANCELED plan row and a FILLED delegated
                # representation merely because BingX reuses one orderId. Keep
                # distinct terminal states while deduplicating page overlap.
                key = ":".join(
                    (
                        oid,
                        str(normalized.get("symbol") or ""),
                        typ,
                        str(normalized.get("positionSide") or ""),
                        str(normalized.get("status") or normalized.get("state_name") or ""),
                        str(normalized.get("filledQty") or ""),
                        str(normalized.get("triggerPrice") or ""),
                        str(ts),
                        ",".join(sorted(identity_values)),
                    )
                )
                if not oid:
                    key = ":".join(
                        (
                            str(normalized.get("symbol") or ""),
                            typ,
                            str(normalized.get("positionSide") or ""),
                            str(normalized.get("triggerPrice") or ""),
                            str(normalized.get("origQty") or ""),
                            str(normalized.get("status") or normalized.get("state_name") or ""),
                            str(normalized.get("filledQty") or ""),
                            str(ts),
                            ",".join(sorted(identity_values)),
                        )
                    )
                if key in seen:
                    continue
                seen.add(key)
                collected.append(normalized)

            if len(raw_rows) >= limit and min_page_ts is None:
                raise BingxResponseIntegrityError(
                    endpoint=endpoint,
                    reason="full history page has no valid pagination timestamp",
                )
            if min_page_ts is not None and min_page_ts > current_end:
                raise BingxResponseIntegrityError(
                    endpoint=endpoint,
                    reason="history page timestamp is newer than requested endTime",
                )

            next_end: int | None = None
            if (
                len(raw_rows) >= limit
                and min_page_ts
                and (request_start is None or min_page_ts - 1 >= request_start)
            ):
                next_end = min_page_ts - 1
            elif start is not None and request_start is not None and request_start > start:
                next_end = request_start - 1

            if next_end is None or next_end >= current_end:
                break
            if start is not None and next_end < start:
                break
            if page_index + 1 >= pages:
                raise BingxResponseIntegrityError(
                    endpoint=endpoint,
                    reason="history pagination budget exhausted before scan completed",
                )
            current_end = next_end
        return collected

    @classmethod
    def _bingx_status_to_core_state(cls, raw_status: Any) -> tuple[int, str, bool, bool]:
        """Map BingX string/numeric order statuses to the core state model.

        Core reconciliation code expects numeric states compatible with the old
        MEXC adapter: 3=filled, 4=canceled, 5=rejected/expired.  BingX returns
        strings such as ``FILLED`` in both order detail and allOrders history.
        Leaving those strings in ``state`` makes STOP/TP history invisible to
        lifecycle workers, so close notifications can be missed.
        """

        status_text = str(raw_status or "").strip().upper()
        try:
            numeric_state = int(raw_status)
        except (TypeError, ValueError, OverflowError):
            numeric_state = None
        if numeric_state is not None:
            terminal = numeric_state in {3, 4, 5}
            fully_filled = numeric_state == 3
            name = {
                1: "NEW",
                2: "PARTIALLY_FILLED",
                3: "FILLED",
                4: "CANCELED",
                5: "REJECTED",
            }.get(numeric_state, status_text or str(numeric_state))
            return numeric_state, name, terminal, fully_filled

        filled_statuses = {
            "FILLED",
            "FULLY_FILLED",
            "EXECUTED",
            "COMPLETED",
            "DONE",
        }
        partial_statuses = {
            "PARTIALLY_FILLED",
            "PARTIALLYFILLED",
            "PARTIAL_FILLED",
            "PARTIAL",
            "PART_FILLED",
        }
        canceled_statuses = {
            "CANCELED",
            "CANCELLED",
            "CANCEL",
            "USER_CANCELED",
            "USER_CANCELLED",
        }
        rejected_statuses = {
            "REJECTED",
            "EXPIRED",
            "FAILED",
            "INVALID",
            "ERROR",
        }
        if status_text in filled_statuses:
            return 3, status_text, True, True
        if status_text in canceled_statuses:
            return 4, status_text, True, False
        if status_text in rejected_statuses:
            return 5, status_text, True, False
        if status_text in partial_statuses:
            return 2, status_text, False, False
        return (1 if status_text else 0), (status_text or "UNKNOWN"), False, False

    def _normalize_order_row(self, row: Dict[str, Any]) -> Dict[str, Any]:
        row = self._order_payload_from_response(row) if isinstance(row, dict) else {}
        sym = _normalize_symbol(row.get("symbol"))
        order_id = self._order_id_from_response(row)
        typ = str(row.get("type") or "").upper()
        raw_reduce_only = (
            row.get("reduceOnly") is True
            or str(row.get("reduceOnly") or "").strip().lower() in {"true", "1"}
        )
        side = str(row.get("positionSide") or "").upper()
        if side not in {"LONG", "SHORT"}:
            raw_side = str(row.get("side") or "").upper()
            # BingX protective TP/SL and reduce-only delegated child orders are
            # close orders. In hedge mode SELL closes LONG and BUY closes SHORT.
            # Entry/non-reduce-only regular rows keep BUY=>LONG / SELL=>SHORT.
            if self._is_conditional_close_type(typ) or raw_reduce_only:
                side = (
                    "LONG"
                    if raw_side == "SELL"
                    else "SHORT"
                    if raw_side == "BUY"
                    else ""
                )
            else:
                side = (
                    "LONG"
                    if raw_side == "BUY"
                    else "SHORT"
                    if raw_side == "SELL"
                    else ""
                )
        state, status_name, terminal, fully_filled = self._bingx_status_to_core_state(
            row.get("status")
            or row.get("state")
            or row.get("orderStatus")
            or row.get("order_state")
        )
        qty, _qty_alias_conflict = self._max_nonnegative_quantity_alias(
            row, ("origQty", "quantity", "qty", "vol")
        )
        filled_qty, _filled_qty_alias_conflict = (
            self._max_nonnegative_quantity_alias(
                row,
                (
                    "executedQty",
                    "cumQty",
                    "filledQty",
                    "dealVol",
                    "executedVolume",
                    "realityVol",
                ),
            )
        )
        if fully_filled and filled_qty <= 0 and qty > 0:
            filled_qty = qty
        trigger_price = self._f(
            row.get("stopPrice")
            or row.get("triggerPrice")
            or row.get("stopLossPrice")
            or row.get("takeProfitPrice")
            or row.get("activationPrice"),
            0.0,
        )
        return {
            "id": order_id,
            "orderId": order_id,
            "stopOrderId": order_id,
            "stopPlanOrderId": order_id,
            "placeOrderId": clean_exchange_id(row.get("placeOrderId")),
            "clientOrderId": clean_exchange_id(row.get("clientOrderID") or row.get("clientOrderId") or row.get("client_order_id") or row.get("externalOid")),
            "clientOrderID": clean_exchange_id(row.get("clientOrderID") or row.get("clientOrderId") or row.get("client_order_id") or row.get("externalOid")),
            "symbol": sym,
            "side": side.lower() if side else "",
            "positionSide": side,
            "type": typ,
            "price": self._f(row.get("price"), 0.0),
            "triggerPrice": trigger_price,
            "stopLossPrice": trigger_price if self._is_stop_like_type(typ) else 0.0,
            "takeProfitPrice": trigger_price if self._is_take_profit_like_type(typ) else 0.0,
            "origQty": qty,
            "vol": qty,
            "quantity": qty,
            "filledQty": filled_qty,
            "executedQty": filled_qty,
            "state": state,
            "state_name": status_name,
            "status": status_name,
            "terminal": terminal,
            "fully_filled": fully_filled,
            "tpExecuted": bool(fully_filled and self._is_take_profit_like_type(typ) and filled_qty > 0),
            "stopExecuted": bool(fully_filled and self._is_stop_like_type(typ) and filled_qty > 0),
            "updateTime": row.get("updateTime") or row.get("time") or row.get("transactTime") or row.get("workingTime"),
            "reduceOnly": raw_reduce_only or self._is_conditional_close_type(typ),
            "raw": row,
        }

    @classmethod
    def _protective_identity_snapshot(
        cls,
        *rows: Dict[str, Any],
        queried_order_id: str = "",
    ) -> Dict[str, Any]:
        """Build a bounded, secret-free exact identity snapshot for TP/SL rows.

        BingX may expose a protective plan and its delegated child through
        different aliases.  Persisting only one ``orderId`` loses the exact
        plan->child bridge after the plan disappears from ``openOrders``.  This
        helper retains only exchange identities and non-secret order facts; it
        deliberately excludes request signatures, API credentials and arbitrary
        raw payload fields.
        """

        id_keys = (
            "stopOrderId",
            "stopPlanOrderId",
            "planOrderId",
            "orderId",
            "orderID",
            "order_id",
            "id",
            "placeOrderId",
            "delegatedOrderId",
            "childOrderId",
            "triggerOrderId",
            "algoOrderId",
            "clientOrderId",
            "clientOrderID",
            "client_order_id",
            "origClientOrderId",
            "origClientOrderID",
            "orig_client_order_id",
            "externalOid",
        )
        scopes: list[Dict[str, Any]] = []
        visited: set[int] = set()

        def collect(value: Any, depth: int = 0) -> None:
            if depth > 4 or not isinstance(value, dict) or id(value) in visited:
                return
            visited.add(id(value))
            scopes.append(value)
            payload = cls._order_payload_from_response(value)
            if isinstance(payload, dict) and payload is not value:
                collect(payload, depth + 1)
            for key in (
                "raw",
                "data",
                "order",
                "trigger",
                "detail",
                "result",
                "verification",
            ):
                collect(value.get(key), depth + 1)

        for row in rows:
            collect(row)

        identities: dict[str, str] = {}
        identity_aliases: dict[str, list[str]] = {}
        all_ids: set[str] = set()
        for scope in scopes:
            for key in id_keys:
                cleaned = clean_exchange_id(scope.get(key))
                if not cleaned:
                    continue
                all_ids.add(cleaned)
                identities.setdefault(key, cleaned)
                bucket = identity_aliases.setdefault(key, [])
                if cleaned not in bucket and len(bucket) < 8:
                    bucket.append(cleaned)

        queried = clean_exchange_id(queried_order_id)
        primary = next((scope for scope in scopes if isinstance(scope, dict)), {})
        # ``_normalize_order_row`` is an instance method.  Callers pass an
        # already normalized row as one of ``rows``; prefer its canonical facts.
        canonical = next(
            (
                scope
                for scope in scopes
                if isinstance(scope, dict)
                and str(scope.get("symbol") or "").strip()
                and str(scope.get("type") or "").strip()
            ),
            primary,
        )
        return {
            "version": 2,
            "queried_order_id": queried,
            "identity_ids": sorted(all_ids)[:32],
            "identities": identities,
            "identity_aliases": identity_aliases,
            "symbol": _normalize_symbol(canonical.get("symbol")),
            "side": str(canonical.get("side") or "").lower(),
            "positionSide": str(canonical.get("positionSide") or "").upper(),
            "type": str(canonical.get("type") or "").upper(),
            "status": str(canonical.get("status") or canonical.get("state_name") or "").upper(),
            "state": canonical.get("state"),
            "terminal": bool(canonical.get("terminal")),
            "fully_filled": bool(canonical.get("fully_filled")),
            "filledQty": cls._f(
                canonical.get("filledQty")
                or canonical.get("executedQty")
                or canonical.get("realityVol"),
                0.0,
            ),
            "origQty": cls._f(
                canonical.get("origQty")
                or canonical.get("quantity")
                or canonical.get("qty"),
                0.0,
            ),
            "triggerPrice": cls._f(
                canonical.get("triggerPrice")
                or canonical.get("stopPrice")
                or canonical.get("takeProfitPrice")
                or canonical.get("stopLossPrice"),
                0.0,
            ),
            "reduceOnly": bool(canonical.get("reduceOnly")),
            "updateTime": canonical.get("updateTime")
            or canonical.get("time")
            or canonical.get("transactTime"),
        }

    @staticmethod
    def _snapshot_identity_values(
        snapshot: Dict[str, Any], keys: tuple[str, ...]
    ) -> list[str]:
        """Return bounded identity values while preserving alias provenance."""

        out: list[str] = []
        alias_map = snapshot.get("identity_aliases")
        aliases = alias_map if isinstance(alias_map, dict) else {}
        primary_map = snapshot.get("identities")
        primary = primary_map if isinstance(primary_map, dict) else {}
        for key in keys:
            values = aliases.get(key)
            if isinstance(values, (list, tuple, set)):
                for value in values:
                    cleaned = clean_exchange_id(value)
                    if cleaned and cleaned not in out:
                        out.append(cleaned)
            cleaned = clean_exchange_id(primary.get(key))
            if cleaned and cleaned not in out:
                out.append(cleaned)
            if len(out) >= 16:
                break
        return out[:16]

    @classmethod
    def _confirmed_protective_identity_pair(
        cls,
        snapshot: Dict[str, Any],
        *,
        fallback_id: str = "",
    ) -> tuple[str, str]:
        """Return ``(plan_id, order_id)`` without collapsing distinct aliases.

        Normalized BingX rows may mirror ``orderId`` into plan-shaped fields for
        compatibility.  The snapshot therefore retains every value seen per
        alias.  When an exact detail exposes both a delegated child and its
        parent plan, prefer the plan-shaped value that is distinct from the
        selected child order id.
        """

        order_ids = cls._snapshot_identity_values(
            snapshot,
            (
                "orderId",
                "orderID",
                "order_id",
                "id",
                "placeOrderId",
                "delegatedOrderId",
                "childOrderId",
            ),
        )
        plan_ids = cls._snapshot_identity_values(
            snapshot,
            (
                "stopPlanOrderId",
                "planOrderId",
                "stopOrderId",
                "triggerOrderId",
                "algoOrderId",
            ),
        )
        fallback = clean_exchange_id(fallback_id)
        order_id = next(iter(order_ids), fallback)
        plan_id = next((value for value in plan_ids if value != order_id), "")
        if not plan_id:
            plan_id = next(iter(plan_ids), fallback or order_id)
        if not order_id:
            order_id = fallback or plan_id
        return plan_id, order_id

    async def cancel_all_open_orders(self, symbol: str | None = None, *, confirmed_broad_cancel: bool = False) -> Dict[str, Any]:
        if not confirmed_broad_cancel:
            raise BingxApiError("BingX broad cancel is disabled without confirmed_broad_cancel=True")
        params = {"symbol": _to_bingx_symbol(symbol)} if symbol else {}
        data = await self._request("DELETE", "/openApi/swap/v2/trade/allOpenOrders", params=params, auth=True, write=True)
        return {"success": True, "code": 0, "data": data}

    async def cancel_all_conditional_orders(self, symbol: str | None = None, *, position_id: int | str | None = None, confirmed_broad_cancel: bool = False) -> Dict[str, Any]:
        if not confirmed_broad_cancel:
            raise BingxApiError("BingX broad conditional cancel is disabled without confirmed_broad_cancel=True")
        if not symbol:
            # BingX endpoint can cancel all, but the bot must never do broad cancels.
            raise BingxApiError("cancel_all_conditional_orders requires symbol on BingX")
        # Use the same expanded conditional type set that visibility/history use.
        # This method is guarded and intended only for confirmed panic/admin
        # cleanup; normal lifecycle continues to use exact order IDs.
        results: dict[str, Any] = {}
        for typ in sorted(self._CONDITIONAL_ORDER_TYPES):
            try:
                results[typ] = await self._request(
                    "DELETE",
                    "/openApi/swap/v2/trade/allOpenOrders",
                    params={"symbol": _to_bingx_symbol(symbol), "type": typ},
                    auth=True,
                    write=True,
                )
            except BingxExchangeRejected as exc:
                results[typ] = {"error": str(exc), "code": exc.error_code}
        return {"success": True, "code": 0, "data": results}

    # Market data ------------------------------------------------------------

    async def fetch_api_trading_symbols(self, *, force: bool = False) -> set[str]:
        if not force and self._api_symbols_cache is not None and time.time() - self._api_symbols_cache_ts < 3600:
            return set(self._api_symbols_cache)
        data = await self._request("GET", "/openApi/swap/v2/quote/contracts")
        symbols = set()
        for row in self._rows(data):
            if str(row.get("currency") or "").upper() != "USDT":
                continue
            if int(self._f(row.get("status"), 0)) != 1:
                continue
            if str(row.get("apiStateOpen") or "true").lower() == "false":
                continue
            normalized_symbol = _normalize_symbol(row.get("symbol"))
            if normalized_symbol:
                symbols.add(normalized_symbol)
        self._api_symbols_cache = symbols
        self._api_symbols_cache_ts = time.time()
        return set(symbols)

    async def instrument_info(self, symbol: str, *, force: bool = False) -> InstrumentInfo:
        norm = _normalize_symbol(symbol)
        cached = self._instrument_cache.get(norm)
        if cached and not force:
            return cached
        try:
            data = await self._request("GET", "/openApi/swap/v2/quote/contracts", params={"symbol": _to_bingx_symbol(symbol)})
        except BingxExchangeRejected as exc:
            if str(getattr(exc, "error_code", "") or "") == "109425":
                raise BingxSymbolNotSupported(f"Пара {norm} не поддерживается на BingX Futures") from exc
            raise
        rows = self._rows(data)
        if not rows:
            raise BingxSymbolNotSupported(f"{norm} не найдена на BingX Futures")
        row = self._select_requested_symbol_row(
            rows,
            requested_symbol=norm,
            endpoint="/openApi/swap/v2/quote/contracts",
        )
        if str(row.get("apiStateOpen") or "true").lower() == "false":
            raise BingxSymbolNotSupported(f"Пара {norm} не разрешена для API-открытия на BingX")
        q_precision = int(self._f(row.get("quantityPrecision"), 6))
        p_precision = int(self._f(row.get("pricePrecision"), 6))
        qty_step = 10 ** (-q_precision) if q_precision >= 0 else 0.000001
        price_tick = 10 ** (-p_precision) if p_precision >= 0 else 0.000001
        # ``/quote/contracts`` is public instrument metadata on BingX and does
        # not reliably include the account/side leverage limits.  The private
        # ``/trade/leverage`` read is used during execution, after API keys are
        # known, so we deliberately keep this value zero here instead of
        # silently falling back to 1x.
        max_lev = max(int(self._f(row.get("maxLongLeverage"), 0)), int(self._f(row.get("maxShortLeverage"), 0)))
        info = InstrumentInfo(
            symbol=norm,
            min_qty=max(float(row.get("tradeMinQuantity") or 0.0), qty_step),
            qty_step=qty_step,
            price_tick=price_tick,
            min_notional=float(row.get("tradeMinUSDT") or 2.0),
            max_leverage=max_lev,
            contract_size=float(row.get("size") or 1.0),
            taker_fee_rate=float(row.get("takerFeeRate") or 0.0005),
            stop_only_fair=False,
        )
        self._instrument_cache[norm] = info
        return info

    async def fetch_max_leverage(self, symbol: str, side: str = "long") -> int:
        """Return BingX's side-specific maximum leverage for a symbol.

        Live logs showed that public ``/quote/contracts`` can omit
        ``maxLongLeverage``/``maxShortLeverage``.  Falling back from that public
        metadata to ``1x`` makes the bot open valid orders with the wrong
        leverage.  The private leverage endpoint is the authoritative source
        before an entry write because it returns both the current leverage and
        the maximum leverage allowed for the account/symbol side.
        """

        norm = _normalize_symbol(symbol)
        side_l = str(side or "long").lower()
        side_key = "short" if side_l == "short" else "long"
        cache_key = (norm, side_key)
        cached = self._max_leverage_cache.get(cache_key)
        if cached and int(cached) > 0:
            return int(cached)

        data = await self._request(
            "GET",
            "/openApi/swap/v2/trade/leverage",
            params={"symbol": _to_bingx_symbol(symbol)},
            auth=True,
        )
        rows = self._rows(data)
        if not rows:
            raise BingxApiError(
                f"BingX не вернула данные плеча для {norm} {side_key.upper()}"
            )
        row = self._select_requested_symbol_row(
            rows,
            requested_symbol=norm,
            endpoint="/openApi/swap/v2/trade/leverage",
            allow_single_symbol_missing=True,
        )
        if side_key == "short":
            raw = row.get("maxShortLeverage")
        else:
            raw = row.get("maxLongLeverage")
        # Keep only documented maximum fields.  Do not use current
        # longLeverage/shortLeverage as a fallback: if the current value is 1x,
        # that would recreate the exact bug this patch fixes.
        max_lev = int(self._f(raw, 0))
        if max_lev <= 0:
            raise BingxApiError(
                f"BingX не вернула максимальное плечо для {norm} {side_key.upper()} через /trade/leverage"
            )
        self._max_leverage_cache[cache_key] = max_lev
        return max_lev

    @staticmethod
    def _preserve_exchange_rejection_context(
        exc: BingxExchangeRejected,
        *,
        context: str,
    ) -> BingxExchangeRejected:
        """Keep the original business code visible to the execution layer.

        Permission quarantine intentionally matches only the exact BingX
        business code ``100004``.  Re-wrapping a ``BingxExchangeRejected`` in a
        plain ``BingxApiError`` erases ``error_code`` and makes a deterministic
        permission failure look like a generic execution error.  Add only a
        non-sensitive context marker and re-raise the original object.
        """

        audit = getattr(exc, "response_audit", None)
        if isinstance(audit, dict):
            audit.setdefault("context", str(context or "unknown")[:120])
        return exc

    @staticmethod
    def _is_margin_mode_noop_rejection(exc: BingxExchangeRejected) -> bool:
        """Return True only for an explicit already-in-desired-mode rejection.

        Margin-mode configuration is part of the pre-entry safety boundary.
        Numeric business codes are not stable enough to treat every rejection
        as a harmless no-op, so acceptance requires explicit message evidence.
        """

        text = " ".join(
            str(getattr(exc, "error_message", "") or exc or "").lower().split()
        )
        if not text or not any(token in text for token in ("margin", "mode", "type")):
            return False
        failure_markers = (
            "not already set",
            "is not already set",
            "isn't already set",
            "invalid",
            "mismatch",
            "failed",
            "failure",
            "error",
            "denied",
            "permission",
            "rate limit",
            "unsupported",
            "not exist",
            "does not exist",
            "cannot",
            "can't",
            "unable",
            "forbidden",
        )
        if any(marker in text for marker in failure_markers):
            return False
        no_op_phrases = (
            "margin mode already set",
            "margin mode is already set",
            "margin type already set",
            "margin type is already set",
            "mode already set",
            "mode is already set",
            "already in margin mode",
            "already in the margin mode",
            "no need to change margin",
            "not need to change margin",
            "no need to switch margin",
            "not need to switch margin",
            "same margin mode",
            "same margin type",
        )
        return any(phrase in text for phrase in no_op_phrases)

    async def set_margin_and_max_leverage(self, symbol: str, max_leverage: int, margin_mode: str = "cross", side: str = "long") -> int:
        await self._ensure_hedge_mode()
        wire = _to_bingx_symbol(symbol)
        desired_margin = "ISOLATED" if str(margin_mode).lower() == "isolated" else "CROSSED"
        try:
            await self._request("POST", "/openApi/swap/v2/trade/marginType", params={"symbol": wire, "marginType": desired_margin}, auth=True, write=True)
        except BingxExchangeRejected as exc:
            self._preserve_exchange_rejection_context(
                exc, context="set_margin_type"
            )
            # ``100004`` is a deterministic account-permission failure.  It
            # must reach signal_executor unchanged so the durable API
            # quarantine can be created before any ENTRY write.
            if str(getattr(exc, "error_code", "") or "").strip() == "100004":
                raise
            # Ignore only an explicit already-in-desired-mode response.  A
            # different deterministic rejection (invalid payload, account
            # restriction, rate limit, unsupported contract, etc.) must fail
            # closed before leverage/ENTRY rather than silently proceeding.
            if self._is_margin_mode_noop_rejection(exc):
                log.info(
                    "BingX margin mode no-op confirmed symbol=%s: %s", wire, exc
                )
            else:
                log.warning(
                    "BingX margin mode rejected fail-closed symbol=%s code=%s: %s",
                    wire,
                    str(getattr(exc, "error_code", "") or "-"),
                    exc,
                )
                raise
        requested = int(max_leverage or 0)
        if requested <= 0:
            requested = await self.fetch_max_leverage(symbol, side)
        lev = max(1, requested)
        pos_side = _position_side(side)
        try:
            await self._request("POST", "/openApi/swap/v2/trade/leverage", params={"symbol": wire, "side": pos_side, "leverage": lev}, auth=True, write=True)
        except BingxExchangeRejected as exc:
            self._preserve_exchange_rejection_context(
                exc, context="set_leverage"
            )
            # Re-raise the exact exchange rejection.  The subclass already is a
            # BingxApiError, while retaining error_code/error_message/path for
            # strict quarantine matching and diagnostics.
            raise
        return lev

    async def _ensure_hedge_mode(self) -> None:
        if self._hedge_checked:
            return
        try:
            data = await self._request("GET", "/openApi/swap/v1/positionSide/dual", auth=True)
            if isinstance(data, dict) and data.get("dualSidePosition") is True:
                self._hedge_checked = True
                return
            await self._request("POST", "/openApi/swap/v1/positionSide/dual", params={"dualSidePosition": "true"}, auth=True, write=True)
            self._hedge_checked = True
        except BingxExchangeRejected as exc:
            self._preserve_exchange_rejection_context(
                exc, context="ensure_hedge_mode"
            )
            # Do not erase deterministic BingX business codes (notably
            # 100004).  The caller already handles BingxApiError subclasses.
            raise

    async def create_entry_order_with_attached_stop(self, *, symbol: str, side: str, qty: float, entry: float, stop: float, order_type: str = "limit", take_profit: float | None = None, client_id: str | None = None) -> Dict[str, Any]:
        await self._ensure_hedge_mode()
        info = await self.instrument_info(symbol)
        wire = _to_bingx_symbol(symbol)
        side_l = str(side).lower()
        typ = "MARKET" if str(order_type).lower() == "market" else "LIMIT"
        params: Dict[str, Any] = {
            "symbol": wire,
            "side": _side_open(side_l),
            "positionSide": _position_side(side_l),
            "type": typ,
            "quantity": self._fmt_num(qty, info.qty_step),
            # BingX's documented field name is clientOrderID (capital D).
            # Read paths still accept/return both aliases for legacy rows.
            "clientOrderID": self._client_id("abx-entry", client_id),
        }
        if typ == "LIMIT":
            params["price"] = self._fmt_num(entry, info.price_tick)
            params["timeInForce"] = "GTC"
        # BingX validates attached TP/SL JSON types strictly.  Live API first
        # rejected JSON boolean ``stopGuaranteed:false`` and then rejected
        # stringified numeric fields such as ``"stopPrice":"0.184"`` with
        # business code 109400 ("Mismatch type float64 with value string").
        # Keep outer form parameters string-compatible for signing, but encode
        # numeric fields inside attached stopLoss/takeProfit JSON as JSON
        # numbers, not strings.
        stop_payload = {"type": "STOP_MARKET", "stopPrice": float(self._fmt_num(stop, info.price_tick)), "workingType": "MARK_PRICE"}
        params["stopLoss"] = json.dumps(stop_payload, separators=(",", ":"))
        if take_profit:
            tp_payload = {"type": "TAKE_PROFIT_MARKET", "stopPrice": float(self._fmt_num(take_profit, info.price_tick)), "workingType": "MARK_PRICE"}
            params["takeProfit"] = json.dumps(tp_payload, separators=(",", ":"))
        data, quantity_retry_audit = await self._post_trade_order_with_quantity_retry(
            params=params,
            symbol=symbol,
            context=f"entry:{typ}",
        )
        order_id = self._order_id_from_response(data)
        client_order_id = self._client_order_id_from_response(data) or params["clientOrderID"]
        submitted_qty = float((quantity_retry_audit or {}).get("submitted_quantity") or qty)
        result = {
            "success": True,
            "code": 0,
            "data": data,
            "symbol": wire,
            "orderId": order_id,
            "orderID": order_id,
            "clientOrderId": client_order_id,
            "clientOrderID": client_order_id,
            "_entry_external_oid": client_order_id,
            "_exchange": "bingx",
            "_submitted_quantity": submitted_qty,
        }
        if quantity_retry_audit:
            result["_quantity_retry"] = quantity_retry_audit
        if typ == "MARKET":
            # A post-fill STOP exact confirmation is performed by the monitor/lifecycle.
            result["_post_fill_stop"] = {"submitted_attached": True, "stop": float(stop), "qty": submitted_qty}
        return result

    async def create_take_profit(self, *, symbol: str, side: str, qty: float, price: float, client_id: str | None = None, position_id: int | str | None = None, adopt_existing: bool = True, owned_order_ids: Iterable[str] | None = None) -> Dict[str, Any]:
        """Create one BingX TP with MEXC-style preflight and read-back confirmation.

        BingX TP/SL trigger orders may not expose a durable clientOrderID in every
        openOrders payload, so treating a POST response as success is unsafe.  This
        method serialises TP writes for the same account/symbol/side, checks live
        position coverage before writing, suppresses exact idempotent duplicates,
        and returns success only after the TP is visible in BingX openOrders with
        matching symbol/side/target/qty.  MEXC-parity write flows can pass
        ``position_id`` and ``adopt_existing=False`` so manual/external same-price
        TP orders are never silently journaled as bot-owned. ``owned_order_ids``
        carries exact TP order ids already confirmed during the same bot write
        flow; BingX can omit positionId from openOrders, so those exact ids are
        allowed to count as bot-owned while unrelated unscoped TP rows still
        fail closed.
        """
        symbol_norm = _normalize_symbol(symbol)
        side_l = str(side or "").strip().lower()
        if side_l not in {"long", "short"}:
            raise BingxApiError(f"Unsupported BingX TP side: {side!r}")
        requested_qty = self._require_positive_finite(qty, field="BingX TP qty")
        requested_price = self._require_positive_finite(price, field="BingX TP price")

        await self._ensure_hedge_mode()
        info = await self.instrument_info(symbol_norm)
        n = await self.normalize_position_tpsl_request(
            symbol=symbol_norm, side=side_l, qty=requested_qty, price=requested_price, kind="tp"
        )
        target = float(n["price"])
        normalized_qty = float(n["qty"])
        if normalized_qty <= 0:
            raise BingxTpCoverageError(
                f"BingX TP qty after rounding is zero for {symbol_norm} {side_l}; TP write aborted"
            )

        lock_seed = f"{self.api_key}|{symbol_norm}|{side_l.upper()}"
        lock_key = hashlib.sha256(lock_seed.encode("utf-8")).hexdigest()
        lock = await _get_tp_write_lock(lock_key)

        async with lock:
            from app.database import db as _db

            async with _db.distributed_advisory_lock(f"bingx-tp:{lock_key}"):
                return await self._create_take_profit_under_lock(
                    symbol=symbol_norm,
                    side=side_l,
                    requested_qty=requested_qty,
                    normalized_qty=normalized_qty,
                    target=target,
                    price_tick=float(n.get("price_tick") or info.price_tick or 0.0),
                    qty_step=float(n.get("qty_step") or info.qty_step or 0.0),
                    client_id=client_id,
                    position_id=position_id,
                    adopt_existing=adopt_existing,
                    owned_order_ids=owned_order_ids,
                )

    @staticmethod
    def _require_positive_finite(value: Any, *, field: str) -> float:
        if isinstance(value, bool):
            raise BingxApiError(f"{field} must be a positive finite number")
        try:
            parsed = float(value)
        except (TypeError, ValueError, OverflowError) as exc:
            raise BingxApiError(f"{field} must be a positive finite number") from exc
        if not math.isfinite(parsed) or parsed <= 0:
            raise BingxApiError(f"{field} must be a positive finite number")
        return parsed

    @staticmethod
    def _tp_order_price(order: Dict[str, Any]) -> float:
        return BingxAdapter._f(
            order.get("takeProfitPrice")
            or order.get("triggerPrice")
            or order.get("stopPrice")
            or order.get("price"),
            0.0,
        )

    def _is_open_take_profit_order(self, order: Dict[str, Any]) -> bool:
        if not isinstance(order, dict):
            return False
        if not self._is_take_profit_like_type(order.get("type")):
            return False
        state = order.get("state")
        try:
            if int(state) in {3, 4, 5}:
                return False
        except (TypeError, ValueError):
            pass
        status = str(order.get("status") or order.get("state_name") or "").upper()
        if status in {"FILLED", "CANCELED", "CANCELLED", "REJECTED", "EXPIRED", "FAILED"}:
            return False
        return True

    def _tp_order_matches_side(self, order: Dict[str, Any], side: str) -> bool:
        wanted = str(side or "").lower()
        row_side = str(order.get("side") or order.get("positionSide") or "").lower()
        return row_side == wanted

    def _stop_order_price(self, order: Dict[str, Any]) -> float:
        return BingxAdapter._f(
            order.get("stopLossPrice")
            or order.get("triggerPrice")
            or order.get("stopPrice")
            or order.get("price"),
            0.0,
        )

    def _is_open_stop_order(self, order: Dict[str, Any]) -> bool:
        if not isinstance(order, dict):
            return False
        if not self._is_stop_like_type(order.get("type")):
            return False
        state = order.get("state")
        try:
            if int(state) in {3, 4, 5}:
                return False
        except (TypeError, ValueError):
            pass
        status = str(order.get("status") or order.get("state_name") or "").upper()
        if status in {"FILLED", "CANCELED", "CANCELLED", "REJECTED", "EXPIRED", "FAILED"}:
            return False
        return True

    def _stop_order_matches_side(self, order: Dict[str, Any], side: str) -> bool:
        wanted = str(side or "").lower()
        row_side = str(order.get("side") or order.get("positionSide") or "").lower()
        return row_side == wanted

    async def _confirm_protective_write_by_exact_order_detail(
        self,
        *,
        symbol: str,
        side: str,
        order_id: str,
        kind: str,
        expected_price: float,
        expected_qty: float,
        price_tolerance: float,
        qty_tolerance: float,
    ) -> tuple[Dict[str, Any] | None, str, BaseException | None]:
        """Confirm a protective write through the exact order-detail endpoint.

        This is a bounded fallback used only when repeated ``openOrders``
        read-back could not be trusted.  It never adopts by price: the exact ID
        must be the ID returned by the POST, and symbol/type/side/price/quantity
        must all match.  Unknown opaque statuses are intentionally insufficient
        here because the detail endpoint is not an open-set endpoint.
        """
        exact_id = clean_exchange_id(order_id)
        symbol_norm = _normalize_symbol(symbol)
        kind_l = str(kind or "").strip().lower()
        endpoint = "/openApi/swap/v2/trade/order"
        if not exact_id:
            return None, "POST response did not include an exact orderId", None
        try:
            expected_price_f = float(expected_price)
            expected_qty_f = float(expected_qty)
            price_tolerance_f = float(price_tolerance)
            qty_tolerance_f = float(qty_tolerance)
        except (TypeError, ValueError, OverflowError) as exc:
            return None, "invalid expected protective order numeric values", exc
        if (
            not math.isfinite(expected_price_f)
            or expected_price_f <= 0
            or not math.isfinite(expected_qty_f)
            or expected_qty_f <= 0
            or not math.isfinite(price_tolerance_f)
            or price_tolerance_f < 0
            or not math.isfinite(qty_tolerance_f)
            or qty_tolerance_f < 0
        ):
            return None, "invalid expected protective order numeric values", None
        try:
            data = await self._request(
                "GET",
                endpoint,
                params={"symbol": _to_bingx_symbol(symbol_norm), "orderId": exact_id},
                auth=True,
            )
            payload = self._order_payload_from_response(data)
            returned_id = self._order_id_from_response(payload)
            if returned_id != exact_id:
                return None, f"exact order detail id mismatch returned={returned_id or 'missing'}", None
            row_symbol = self._strict_contract_symbol(
                self._symbol_from_order_response(payload),
                endpoint=endpoint,
                field="symbol",
                row_index=0,
            )
            if row_symbol != symbol_norm:
                return None, f"exact order detail symbol mismatch returned={row_symbol}", None
            order_type = str(payload.get("type") or "").strip().upper()
            if order_type not in self._KNOWN_ORDER_TYPES:
                return None, f"exact order detail type is missing or unknown: {order_type or 'missing'}", None
            if kind_l == "tp":
                if not self._is_take_profit_like_type(order_type):
                    return None, f"exact order detail is not TP-like: {order_type}", None
            elif kind_l in {"sl", "stop"}:
                if not self._is_stop_like_type(order_type):
                    return None, f"exact order detail is not STOP-like: {order_type}", None
            else:
                return None, f"unsupported protective confirmation kind={kind_l}", None

            (
                status_text,
                status_recognized,
                status_state,
                status_terminal,
                status_fully_filled,
            ) = self._validated_order_status_from_payload(
                payload,
                endpoint=endpoint,
                row_index=0,
                allow_opaque=False,
            )
            if not status_recognized:
                return None, f"exact order detail has opaque status={status_text}", None

            raw_side = str(payload.get("side") or "").strip().upper()
            if raw_side not in {"BUY", "SELL"}:
                return None, "exact order detail side is missing or invalid", None
            raw_position_side = str(payload.get("positionSide") or "").strip().upper()
            if raw_position_side and raw_position_side not in {"BOTH", "LONG", "SHORT"}:
                return None, f"exact order detail positionSide is invalid: {raw_position_side}", None

            normalized = self._normalize_order_row(payload)
            normalized["orderId"] = exact_id
            normalized["id"] = exact_id
            normalized["symbol"] = row_symbol
            normalized["type"] = order_type
            normalized["exchangeStatus"] = status_text
            normalized["statusRecognized"] = True
            normalized["_open_orders_status_opaque_active"] = False
            normalized["state"] = int(status_state)
            normalized["state_name"] = status_text
            normalized["status"] = status_text
            normalized["terminal"] = bool(status_terminal)
            normalized["fully_filled"] = bool(status_fully_filled)

            if bool(status_terminal) or int(status_state) in {3, 4, 5}:
                return None, f"exact order detail is terminal status={status_text}", None
            matches_side = (
                self._tp_order_matches_side(normalized, side)
                if kind_l == "tp"
                else self._stop_order_matches_side(normalized, side)
            )
            if not matches_side:
                return None, f"exact order detail side mismatch returned={normalized.get('side') or raw_position_side}", None

            actual_price = (
                self._tp_order_price(normalized)
                if kind_l == "tp"
                else self._stop_order_price(normalized)
            )
            if actual_price <= 0 or abs(actual_price - expected_price_f) > price_tolerance_f:
                return None, f"exact order detail price mismatch returned={actual_price:.12g}", None

            actual_qty = max(
                0.0,
                self._f(
                    normalized.get("qty")
                    or normalized.get("quantity")
                    or normalized.get("origQty"),
                    0.0,
                ),
            )
            if kind_l == "tp":
                if actual_qty <= 0 or abs(actual_qty - expected_qty_f) > qty_tolerance_f:
                    return None, f"exact TP detail qty mismatch returned={actual_qty:.12g}", None
            elif actual_qty > 0 and actual_qty + qty_tolerance_f < expected_qty_f:
                return None, f"exact STOP detail qty under coverage returned={actual_qty:.12g}", None
            return normalized, "", None
        except BingxExchangeRejected as exc:
            # Permission failures must retain their exact structured identity so
            # the signal executor can persist the durable code=100004 quarantine.
            if str(getattr(exc, "error_code", "") or "").strip() == "100004":
                raise
            return None, f"{type(exc).__name__}: {exc}"[:500], exc
        except Exception as exc:
            return None, f"{type(exc).__name__}: {exc}"[:500], exc

    async def fetch_protective_order_identity_detail(
        self,
        *,
        symbol: str,
        order_id: str,
        kind: str = "tp",
        side: str = "",
        expected_price: float = 0.0,
        expected_qty: float = 0.0,
    ) -> Dict[str, Any] | None:
        """Read one exact protective order and return only durable identity facts.

        This read-only recovery endpoint is intentionally stricter than a price
        lookup.  The requested exact id must be present in the returned payload;
        symbol, protective type and optional side/price/qty facts must agree.
        Terminal rows are allowed because the method is used after a TP plan has
        disappeared from ``openOrders`` and BingX exposes only its delegated
        child in history.
        """

        exact_id = clean_exchange_id(order_id)
        if not exact_id:
            return None
        symbol_norm = _normalize_symbol(symbol)
        kind_l = str(kind or "tp").strip().lower()
        if kind_l not in {"tp", "sl", "stop"}:
            raise BingxResponseIntegrityError(
                endpoint="/openApi/swap/v2/trade/order",
                reason=f"unsupported protective identity kind={kind_l}",
            )
        endpoint = "/openApi/swap/v2/trade/order"
        data = await self._request(
            "GET",
            endpoint,
            params={"symbol": _to_bingx_symbol(symbol_norm), "orderId": exact_id},
            auth=True,
        )
        payload = self._order_payload_from_response(data)
        if not isinstance(payload, dict) or not payload:
            raise BingxResponseIntegrityError(
                endpoint=endpoint,
                reason="exact protective order detail is empty or malformed",
            )
        row_symbol = self._strict_contract_symbol(
            self._symbol_from_order_response(payload),
            endpoint=endpoint,
            field="symbol",
            row_index=0,
        )
        if row_symbol != symbol_norm:
            raise BingxResponseIntegrityError(
                endpoint=endpoint,
                reason=f"exact protective order symbol mismatch returned={row_symbol}",
            )
        order_type = str(payload.get("type") or "").strip().upper()
        if order_type not in self._KNOWN_ORDER_TYPES:
            raise BingxResponseIntegrityError(
                endpoint=endpoint,
                reason=f"exact protective order type is missing or unknown: {order_type or 'missing'}",
            )
        if kind_l == "tp" and not self._is_take_profit_like_type(order_type):
            raw_reduce_only = (
                payload.get("reduceOnly") is True
                or str(payload.get("reduceOnly") or "").strip().lower()
                in {"true", "1"}
            )
            if order_type not in {"MARKET", "LIMIT"} or not raw_reduce_only:
                raise BingxResponseIntegrityError(
                    endpoint=endpoint,
                    reason=f"exact protective order is not TP-like or reduce-only child: {order_type}",
                )
        if kind_l in {"sl", "stop"} and not self._is_stop_like_type(order_type):
            raise BingxResponseIntegrityError(
                endpoint=endpoint,
                reason=f"exact protective order is not STOP-like: {order_type}",
            )

        (
            status_text,
            status_recognized,
            status_state,
            status_terminal,
            status_fully_filled,
        ) = self._validated_order_status_from_payload(
            payload,
            endpoint=endpoint,
            row_index=0,
            allow_opaque=False,
        )
        if not status_recognized:
            raise BingxResponseIntegrityError(
                endpoint=endpoint,
                reason=f"exact protective order has opaque status={status_text}",
            )

        normalized = self._normalize_order_row(payload)
        normalized.update(
            {
                "symbol": row_symbol,
                "type": order_type,
                "exchangeStatus": status_text,
                "statusRecognized": True,
                "state": int(status_state),
                "state_name": status_text,
                "status": status_text,
                "terminal": bool(status_terminal),
                "fully_filled": bool(status_fully_filled),
            }
        )
        snapshot = self._protective_identity_snapshot(
            normalized,
            payload,
            data,
            queried_order_id=exact_id,
        )
        identity_ids = {
            clean_exchange_id(value)
            for value in snapshot.get("identity_ids") or []
            if clean_exchange_id(value)
        }
        if exact_id not in identity_ids:
            raise BingxResponseIntegrityError(
                endpoint=endpoint,
                reason=(
                    "exact protective order detail did not retain the queried id "
                    f"queried={exact_id}"
                ),
            )

        wanted_side = str(side or "").strip().lower()
        row_side = str(normalized.get("side") or normalized.get("positionSide") or "").strip().lower()
        if wanted_side and row_side and row_side != wanted_side:
            raise BingxResponseIntegrityError(
                endpoint=endpoint,
                reason=f"exact protective order side mismatch returned={row_side}",
            )

        expected_price_f = self._f(expected_price, 0.0)
        # A delegated MARKET/LIMIT child may expose its execution/average price in
        # ``price``.  Slippage makes that value legitimately differ from the TP
        # trigger, so recovery must never reject an exact plan->child bridge on
        # execution price.  Compare only explicit protective trigger fields.
        actual_trigger_price = self._f(
            normalized.get("takeProfitPrice")
            or normalized.get("stopLossPrice")
            or normalized.get("triggerPrice")
            or normalized.get("stopPrice"),
            0.0,
        )
        if expected_price_f > 0 and actual_trigger_price > 0:
            tolerance = max(abs(expected_price_f) * 1e-9, 1e-12)
            if abs(actual_trigger_price - expected_price_f) > tolerance:
                raise BingxResponseIntegrityError(
                    endpoint=endpoint,
                    reason=(
                        "exact protective order trigger price mismatch "
                        f"returned={actual_trigger_price:.12g}"
                    ),
                )

        expected_qty_f = self._f(expected_qty, 0.0)
        actual_qty = max(
            0.0,
            self._f(
                normalized.get("origQty")
                or normalized.get("quantity")
                or normalized.get("qty"),
                0.0,
            ),
        )
        if expected_qty_f > 0 and actual_qty > 0:
            tolerance = max(abs(expected_qty_f) * 1e-6, 1e-10)
            if abs(actual_qty - expected_qty_f) > tolerance:
                raise BingxResponseIntegrityError(
                    endpoint=endpoint,
                    reason=f"exact protective order qty mismatch returned={actual_qty:.12g}",
                )
        return snapshot

    async def _create_stop_loss_under_lock(
        self,
        *,
        symbol: str,
        side: str,
        requested_qty: float,
        normalized_qty: float,
        stop_price: float,
        price_tick: float,
        qty_step: float,
        client_id: str | None,
        position_id: int | str | None = None,
        adopt_existing: bool = True,
        owned_order_ids: Iterable[str] | None = None,
    ) -> Dict[str, Any]:
        positions = await self.fetch_open_positions(symbol, side)
        wanted_pos = clean_exchange_id(position_id)
        owned_ids = {
            clean_exchange_id(value)
            for value in (owned_order_ids or [])
            if clean_exchange_id(value)
        }
        if wanted_pos:
            exact_positions = [
                p for p in positions or []
                if clean_exchange_id((p or {}).get("positionId")) == wanted_pos
            ]
            if not exact_positions:
                raise BingxApiError(
                    f"BingX live positionId {wanted_pos} was not found for {symbol} {side.upper()}; "
                    "strict STOP write aborted"
                )
            position_qty = sum(float(p.get("size") or 0.0) for p in exact_positions)
        else:
            position_qty = sum(float(p.get("size") or 0.0) for p in positions)
        if position_qty <= 0:
            raise BingxApiError(
                f"BingX has no live {symbol} {side.upper()} position; STOP write aborted"
            )

        price_tolerance = max(float(price_tick or 0.0) * 0.51, abs(stop_price) * 1e-9, 1e-12)
        qty_tolerance = max(
            float(qty_step or 0.0) * 0.51,
            abs(normalized_qty) * 1e-9,
            abs(position_qty) * 1e-9,
            1e-12,
        )

        open_algo = await self.fetch_open_algo_orders(symbol)
        same_side_all_stops = [
            o for o in open_algo
            if _normalize_symbol(o.get("symbol")) == symbol
            and self._is_open_stop_order(o)
            and self._stop_order_matches_side(o, side)
        ]
        pre_write_ids = {
            clean_exchange_id(o.get("orderId") or o.get("stopPlanOrderId"))
            for o in same_side_all_stops
            if clean_exchange_id(o.get("orderId") or o.get("stopPlanOrderId"))
        }
        if wanted_pos:
            def _stop_row_id(row: Dict[str, Any]) -> str:
                return clean_exchange_id(row.get("orderId") or row.get("stopPlanOrderId"))

            if not adopt_existing:
                ambiguous_unscoped = [
                    o for o in same_side_all_stops
                    if not clean_exchange_id(o.get("positionId"))
                    and _stop_row_id(o) not in owned_ids
                ]
                if ambiguous_unscoped:
                    raise BingxApiError(
                        f"BingX returned {len(ambiguous_unscoped)} same-side STOP order(s) without positionId for {symbol} "
                        f"{side.upper()}; strict STOP write will not adopt or bypass possible manual protection"
                    )
            if adopt_existing:
                same_side_stops = [
                    o for o in same_side_all_stops
                    if clean_exchange_id(o.get("positionId")) == wanted_pos
                    or not clean_exchange_id(o.get("positionId"))
                ]
            else:
                same_side_stops = [
                    o for o in same_side_all_stops
                    if clean_exchange_id(o.get("positionId")) == wanted_pos
                    or (not clean_exchange_id(o.get("positionId")) and _stop_row_id(o) in owned_ids)
                ]
        else:
            same_side_stops = same_side_all_stops
        matching = [o for o in same_side_stops if abs(self._stop_order_price(o) - stop_price) <= price_tolerance]

        if len(matching) > 1:
            raise BingxApiError(
                f"BingX found {len(matching)} open STOP orders at {stop_price} for {symbol} {side.upper()}; "
                "duplicate same-price STOP requires cleanup"
            )
        if matching:
            if not adopt_existing:
                existing_ids = sorted({
                    clean_exchange_id(o.get("orderId") or o.get("stopPlanOrderId"))
                    for o in matching
                    if clean_exchange_id(o.get("orderId") or o.get("stopPlanOrderId"))
                })
                raise BingxApiError(
                    f"BingX already has matching STOP at {stop_price} for {symbol} {side.upper()} "
                    f"ids={existing_ids or ['unknown']}; strict STOP write refused to adopt an existing order"
                )
            existing = dict(matching[0])
            existing_qty = max(0.0, self._f(existing.get("qty") or existing.get("quantity") or existing.get("origQty"), 0.0))
            minimum_qty = min(position_qty, normalized_qty) if normalized_qty > 0 else position_qty
            if existing_qty > 0 and existing_qty + qty_tolerance < minimum_qty:
                raise BingxApiError(
                    f"BingX existing STOP {stop_price} for {symbol} has qty {existing_qty:.12g}, "
                    f"expected at least {minimum_qty:.12g}; STOP coverage mismatch"
                )
            if not clean_exchange_id(existing.get("orderId") or existing.get("stopPlanOrderId")):
                raise BingxApiError(
                    f"BingX existing STOP {stop_price} for {symbol} is visible without exact orderId; cannot adopt safely"
                )
            existing["_idempotent_existing"] = True
            existing["_stop_open_confirmed"] = True
            existing["_confirmed_order_id"] = clean_exchange_id(existing.get("orderId") or existing.get("stopPlanOrderId"))
            existing["_normalized_quantity"] = existing_qty or normalized_qty
            existing["_requested_quantity"] = requested_qty
            return existing

        conflicting = [o for o in same_side_stops if abs(self._stop_order_price(o) - stop_price) > price_tolerance]
        if conflicting:
            conflicting_unknown = [
                o for o in conflicting
                if clean_exchange_id(o.get("orderId") or o.get("stopPlanOrderId")) not in owned_ids
            ]
            if conflicting_unknown:
                raise BingxApiError(
                    f"BingX found {len(conflicting_unknown)} existing STOP order(s) for {symbol} {side.upper()} at a different price; "
                    "refusing to create a second protective STOP without manual cleanup"
                )

        safe_qty = min(normalized_qty, position_qty) if position_qty > 0 else normalized_qty
        if qty_step > 0:
            safe_qty = self._floor_to_step(safe_qty, qty_step)
        if safe_qty <= 0:
            raise BingxApiError(
                f"BingX STOP qty after rounding is zero for {symbol} {side.upper()}"
            )

        result = await self.create_position_tpsl(
            symbol=symbol,
            side=side,
            qty=safe_qty,
            price=stop_price,
            kind="sl",
            client_id=client_id,
            position_id=position_id,
            min_acceptable_qty=max(0.0, safe_qty - qty_tolerance),
        )
        result.setdefault("_requested_quantity", requested_qty)
        submitted_safe_qty = float(result.get("_submitted_quantity") or safe_qty)
        if abs(submitted_safe_qty - normalized_qty) > qty_tolerance:
            result["_stop_quantity_capped"] = True

        returned_id = clean_exchange_id(result.get("orderId") or result.get("stopPlanOrderId"))
        confirmed: Dict[str, Any] | None = None
        last_error = ""
        last_confirmation_exc: BaseException | None = None
        for attempt in range(12):
            if attempt:
                await asyncio.sleep(min(2.0, 0.25 * attempt))
            try:
                rows = await self.fetch_open_algo_orders(symbol)
            except Exception as exc:
                if (
                    isinstance(exc, BingxExchangeRejected)
                    and str(getattr(exc, "error_code", "") or "").strip() == "100004"
                ):
                    raise
                last_confirmation_exc = exc
                last_error = f"{type(exc).__name__}: {exc}"
                log.warning("BingX STOP confirmation read failed for %s stop=%s: %s", symbol, stop_price, exc)
                continue
            for order in rows:
                if not isinstance(order, dict):
                    continue
                if _normalize_symbol(order.get("symbol")) != symbol:
                    continue
                if not self._is_open_stop_order(order):
                    continue
                if not self._stop_order_matches_side(order, side):
                    continue
                row_id = clean_exchange_id(order.get("orderId") or order.get("stopPlanOrderId"))
                if returned_id:
                    if not row_id or row_id != returned_id:
                        continue
                elif not row_id or row_id in pre_write_ids:
                    continue
                if wanted_pos:
                    row_pos = clean_exchange_id(order.get("positionId"))
                    if row_pos and row_pos != wanted_pos:
                        continue
                    if not row_pos and not (returned_id and row_id == returned_id):
                        continue
                if abs(self._stop_order_price(order) - stop_price) > price_tolerance:
                    continue
                confirmed_qty = max(0.0, self._f(order.get("qty") or order.get("quantity") or order.get("origQty"), 0.0))
                if confirmed_qty > 0 and confirmed_qty + qty_tolerance < submitted_safe_qty:
                    last_error = f"matching STOP qty {confirmed_qty:.12g} < expected {submitted_safe_qty:.12g}"
                    continue
                if not row_id:
                    last_error = "matching STOP row has no exact orderId"
                    continue
                confirmed = dict(order)
                break
            if confirmed is not None:
                break

        confirmation_source = "open_orders_exact"
        if confirmed is None and returned_id:
            confirmed, detail_error, detail_exc = await self._confirm_protective_write_by_exact_order_detail(
                symbol=symbol,
                side=side,
                order_id=returned_id,
                kind="sl",
                expected_price=stop_price,
                expected_qty=submitted_safe_qty,
                price_tolerance=price_tolerance,
                qty_tolerance=qty_tolerance,
            )
            if confirmed is not None:
                confirmation_source = "exact_order_detail"
            elif detail_error:
                last_error = f"{last_error}; exact detail: {detail_error}" if last_error else f"exact detail: {detail_error}"
            if detail_exc is not None:
                last_confirmation_exc = detail_exc

        if confirmed is None:
            ambiguous = BingxNetworkAmbiguousError(
                f"BingX STOP write for {symbol} stop {stop_price} was not confirmed in openOrders or by exact order detail; "
                f"success was not recorded. Last confirmation error: {last_error or 'order not visible'}"
            )
            if last_confirmation_exc is not None:
                raise ambiguous from last_confirmation_exc
            raise ambiguous

        confirmed_id = clean_exchange_id(confirmed.get("orderId") or confirmed.get("stopPlanOrderId"))
        result["_stop_open_confirmed"] = True
        result["_stop_confirmation_source"] = confirmation_source
        result["_confirmed_order_id"] = confirmed_id
        result["_confirmed_stop_plan_id"] = confirmed_id
        result["_confirmed_stop_price"] = self._stop_order_price(confirmed) or stop_price
        result["_normalized_quantity"] = max(0.0, self._f(confirmed.get("qty") or confirmed.get("quantity") or confirmed.get("origQty"), submitted_safe_qty))
        result["_confirmed_side"] = side
        result["_confirmed_symbol"] = symbol
        return result

    async def _create_take_profit_under_lock(
        self,
        *,
        symbol: str,
        side: str,
        requested_qty: float,
        normalized_qty: float,
        target: float,
        price_tick: float,
        qty_step: float,
        client_id: str | None,
        position_id: int | str | None = None,
        adopt_existing: bool = True,
        owned_order_ids: Iterable[str] | None = None,
    ) -> Dict[str, Any]:
        positions = await self.fetch_open_positions(symbol, side)
        wanted_pos = clean_exchange_id(position_id)
        owned_ids = {
            clean_exchange_id(value)
            for value in (owned_order_ids or [])
            if clean_exchange_id(value)
        }
        if wanted_pos:
            exact_positions = [
                p for p in positions or []
                if clean_exchange_id((p or {}).get("positionId")) == wanted_pos
            ]
            if not exact_positions:
                raise BingxTpOwnershipError(
                    f"BingX live positionId {wanted_pos} was not found for {symbol} {side.upper()}; "
                    "strict TP write aborted"
                )
            position_qty_d = sum(
                (self._qty_decimal((p or {}).get("size") or 0) for p in exact_positions),
                Decimal("0"),
            )
        else:
            position_qty_d = sum(
                (self._qty_decimal((p or {}).get("size") or 0) for p in positions or []),
                Decimal("0"),
            )
        position_qty = float(position_qty_d)
        if position_qty_d <= 0:
            raise BingxTpCoverageError(
                f"BingX has no live {symbol} {side.upper()} position; TP write aborted"
            )

        price_tolerance = max(float(price_tick or 0.0) * 0.51, abs(target) * 1e-9, 1e-12)
        qty_tolerance = max(
            float(qty_step or 0.0) * 1e-6,
            abs(normalized_qty) * 1e-9,
            abs(position_qty) * 1e-9,
            1e-12,
        )

        open_algo = await self.fetch_open_algo_orders(symbol)
        same_side_all_tps = [
            o for o in open_algo
            if _normalize_symbol(o.get("symbol")) == symbol
            and self._is_open_take_profit_order(o)
            and self._tp_order_matches_side(o, side)
        ]
        pre_write_ids = {
            clean_exchange_id(o.get("orderId") or o.get("stopPlanOrderId"))
            for o in same_side_all_tps
            if clean_exchange_id(o.get("orderId") or o.get("stopPlanOrderId"))
        }
        if wanted_pos:
            def _tp_row_id(row: Dict[str, Any]) -> str:
                return clean_exchange_id(row.get("orderId") or row.get("stopPlanOrderId"))

            ambiguous_unscoped = [
                o for o in same_side_all_tps
                if not clean_exchange_id(o.get("positionId"))
                and _tp_row_id(o) not in owned_ids
            ]
            if ambiguous_unscoped:
                raise BingxTpOwnershipError(
                    f"BingX returned {len(ambiguous_unscoped)} same-side TP order(s) without positionId for {symbol} "
                    f"{side.upper()}; strict write will not adopt or bypass possible manual TP"
                )
            same_side_tps = [
                o for o in same_side_all_tps
                if clean_exchange_id(o.get("positionId")) == wanted_pos
                or (not clean_exchange_id(o.get("positionId")) and _tp_row_id(o) in owned_ids)
            ]
        else:
            same_side_tps = same_side_all_tps
        aggregate_tp_qty_d = sum(
            (
                self._qty_decimal(
                    o.get("qty") or o.get("quantity") or o.get("origQty") or 0
                )
                for o in same_side_tps
            ),
            Decimal("0"),
        )
        aggregate_tp_qty = float(aggregate_tp_qty_d)
        matching = [o for o in same_side_tps if abs(self._tp_order_price(o) - target) <= price_tolerance]

        if aggregate_tp_qty_d > position_qty_d + Decimal(str(qty_tolerance)):
            raise BingxTpCoverageError(
                f"BingX open TP qty {aggregate_tp_qty:.12g} exceeds live position "
                f"{position_qty:.12g} for {symbol} {side.upper()}; manual cleanup required"
            )
        if len(matching) > 1:
            raise BingxTpCoverageError(
                f"BingX found {len(matching)} open TP orders at {target} for {symbol} {side.upper()}; "
                "duplicate same-price TP requires cleanup"
            )
        if matching:
            if not adopt_existing:
                existing_ids = sorted({
                    clean_exchange_id(o.get("orderId") or o.get("stopPlanOrderId"))
                    for o in matching
                    if clean_exchange_id(o.get("orderId") or o.get("stopPlanOrderId"))
                })
                raise BingxTpOwnershipError(
                    f"BingX already has matching TP at {target} for {symbol} {side.upper()} "
                    f"ids={existing_ids or ['unknown']}; strict write refused to adopt an existing order"
                )
            existing = dict(matching[0])
            existing_qty = max(0.0, self._f(existing.get("qty") or existing.get("quantity") or existing.get("origQty"), 0.0))
            if abs(existing_qty - normalized_qty) > qty_tolerance:
                raise BingxTpCoverageError(
                    f"BingX existing TP {target} for {symbol} has qty {existing_qty:.12g}, "
                    f"expected {normalized_qty:.12g}; same-price quantity mismatch"
                )
            if not clean_exchange_id(existing.get("orderId") or existing.get("stopPlanOrderId")):
                raise BingxTpCoverageError(
                    f"BingX existing TP {target} for {symbol} is visible without exact orderId; cannot adopt safely"
                )
            existing["_idempotent_existing"] = True
            existing["_tp_open_confirmed"] = True
            visible_id = clean_exchange_id(
                existing.get("orderId") or existing.get("stopPlanOrderId")
            )
            verification = self._protective_identity_snapshot(
                existing,
                queried_order_id=visible_id,
            )
            confirmed_plan_id, confirmed_order_id = (
                self._confirmed_protective_identity_pair(
                    verification,
                    fallback_id=visible_id,
                )
            )
            existing["_confirmed_order_id"] = confirmed_order_id
            existing["_confirmed_stop_plan_id"] = confirmed_plan_id
            existing["_normalized_quantity"] = existing_qty
            existing["_requested_quantity"] = requested_qty
            existing["verification"] = verification
            return existing

        available_qty_d = max(Decimal("0"), position_qty_d - aggregate_tp_qty_d)
        available_qty = float(available_qty_d)
        if available_qty_d <= Decimal(str(qty_tolerance)):
            raise BingxTpCoverageError(
                f"BingX TP {target} for {symbol} was not created: existing TP qty "
                f"{aggregate_tp_qty:.12g} already covers live position {position_qty:.12g}"
            )
        normalized_qty_d = self._qty_decimal(normalized_qty)
        safe_qty_d = min(normalized_qty_d, available_qty_d)
        safe_qty = float(safe_qty_d)
        if qty_step > 0:
            safe_qty = self._floor_to_step(safe_qty_d, qty_step)
        if safe_qty <= 0:
            raise BingxTpCoverageError(
                f"BingX remaining uncovered qty {available_qty:.12g} is below qty step {qty_step:.12g}"
            )

        result = await self.create_position_tpsl(
            symbol=symbol,
            side=side,
            qty=safe_qty,
            price=target,
            kind="tp",
            client_id=client_id,
            min_acceptable_qty=max(0.0, safe_qty - qty_tolerance),
        )
        result.setdefault("_requested_quantity", requested_qty)
        submitted_safe_qty = float(result.get("_submitted_quantity") or safe_qty)
        if abs(submitted_safe_qty - normalized_qty) > qty_tolerance:
            result["_tp_quantity_capped"] = True
        returned_id = clean_exchange_id(result.get("orderId") or result.get("stopPlanOrderId"))

        confirmed: Dict[str, Any] | None = None
        last_error = ""
        last_confirmation_exc: BaseException | None = None
        for attempt in range(12):
            if attempt:
                await asyncio.sleep(min(2.0, 0.25 * attempt))
            try:
                rows = await self.fetch_open_algo_orders(symbol)
            except Exception as exc:
                if (
                    isinstance(exc, BingxExchangeRejected)
                    and str(getattr(exc, "error_code", "") or "").strip() == "100004"
                ):
                    raise
                last_confirmation_exc = exc
                last_error = f"{type(exc).__name__}: {exc}"
                log.warning("BingX TP confirmation read failed for %s target=%s: %s", symbol, target, exc)
                continue
            for order in rows:
                if not isinstance(order, dict):
                    continue
                if _normalize_symbol(order.get("symbol")) != symbol:
                    continue
                if not self._is_open_take_profit_order(order):
                    continue
                if not self._tp_order_matches_side(order, side):
                    continue
                row_id = clean_exchange_id(order.get("orderId") or order.get("stopPlanOrderId"))
                if returned_id:
                    if not row_id or row_id != returned_id:
                        continue
                elif not row_id or row_id in pre_write_ids:
                    continue
                if wanted_pos:
                    row_pos = clean_exchange_id(order.get("positionId"))
                    if row_pos and row_pos != wanted_pos:
                        continue
                    if not row_pos and not (returned_id and row_id == returned_id):
                        # A position-less TP can confirm this write only when the
                        # exchange returned the same exact order id from the POST.
                        # Otherwise it could be a manual TP created concurrently.
                        continue
                if abs(self._tp_order_price(order) - target) > price_tolerance:
                    continue
                confirmed_qty = max(0.0, self._f(order.get("qty") or order.get("quantity") or order.get("origQty"), 0.0))
                if abs(confirmed_qty - submitted_safe_qty) > qty_tolerance:
                    last_error = f"matching TP qty {confirmed_qty:.12g} != expected {submitted_safe_qty:.12g}"
                    continue
                if not clean_exchange_id(order.get("orderId") or order.get("stopPlanOrderId")):
                    last_error = "matching TP row has no exact orderId"
                    continue
                confirmed = dict(order)
                break
            if confirmed is not None:
                break

        confirmation_source = "open_orders_exact"
        if confirmed is None and returned_id:
            confirmed, detail_error, detail_exc = await self._confirm_protective_write_by_exact_order_detail(
                symbol=symbol,
                side=side,
                order_id=returned_id,
                kind="tp",
                expected_price=target,
                expected_qty=submitted_safe_qty,
                price_tolerance=price_tolerance,
                qty_tolerance=qty_tolerance,
            )
            if confirmed is not None:
                confirmation_source = "exact_order_detail"
            elif detail_error:
                last_error = f"{last_error}; exact detail: {detail_error}" if last_error else f"exact detail: {detail_error}"
            if detail_exc is not None:
                last_confirmation_exc = detail_exc

        if confirmed is None:
            ambiguous = BingxNetworkAmbiguousError(
                f"BingX TP write for {symbol} target {target} was not confirmed in openOrders or by exact order detail; "
                f"success was not recorded. Last confirmation error: {last_error or 'order not visible'}"
            )
            if last_confirmation_exc is not None:
                raise ambiguous from last_confirmation_exc
            raise ambiguous

        confirmed_id = clean_exchange_id(
            confirmed.get("orderId") or confirmed.get("stopPlanOrderId")
        )

        # Final topology check for the eventual-consistency race: a TP that was
        # absent during preflight can appear immediately after our exact write,
        # leaving two identical reduce-only orders at one target.  Roll back only
        # the exact TP created by this call.  Never cancel the pre-existing/late
        # order, because it may be manual or owned by another recovery path.
        fresh_rows: list[dict[str, Any]] | None = None
        if confirmation_source == "open_orders_exact":
            try:
                fresh_rows = list(await self.fetch_open_algo_orders(symbol) or [])
                result["_tp_post_write_topology_check"] = "completed"
            except Exception as topology_exc:
                result["_tp_post_write_topology_check"] = "unavailable"
                result["_tp_post_write_topology_error"] = (
                    f"{type(topology_exc).__name__}: {topology_exc}"[:500]
                )
                log.warning(
                    "BingX TP final topology read unavailable for %s target=%s: %s",
                    symbol,
                    target,
                    topology_exc,
                )
        else:
            result["_tp_post_write_topology_check"] = "skipped_exact_detail_confirmation"

        fresh_matching: list[dict[str, Any]] = []
        for order in fresh_rows or []:
            if not isinstance(order, dict):
                continue
            if _normalize_symbol(order.get("symbol")) != symbol:
                continue
            if not self._is_open_take_profit_order(order):
                continue
            if not self._tp_order_matches_side(order, side):
                continue
            row_pos = clean_exchange_id(order.get("positionId"))
            if wanted_pos and row_pos and row_pos != wanted_pos:
                continue
            if abs(self._tp_order_price(order) - target) > price_tolerance:
                continue
            row_qty = max(
                0.0,
                self._f(
                    order.get("qty")
                    or order.get("quantity")
                    or order.get("origQty"),
                    0.0,
                ),
            )
            if row_qty > 0 and abs(row_qty - submitted_safe_qty) > qty_tolerance:
                continue
            fresh_matching.append(order)
        fresh_matching_ids = sorted(
            {
                clean_exchange_id(order.get("orderId") or order.get("stopPlanOrderId"))
                for order in fresh_matching
                if clean_exchange_id(order.get("orderId") or order.get("stopPlanOrderId"))
            }
        )
        if len(fresh_matching_ids) > 1 and confirmed_id in fresh_matching_ids:
            retained_ids = [value for value in fresh_matching_ids if value != confirmed_id]
            cleanup_error = ""
            try:
                await self.cancel_conditional_orders_exact([confirmed_id], symbol=symbol)
                verify_rows = list(await self.fetch_open_algo_orders(symbol) or [])
                remaining_ids: set[str] = set()
                for order in verify_rows:
                    if not isinstance(order, dict):
                        continue
                    if _normalize_symbol(order.get("symbol")) != symbol:
                        continue
                    if not self._is_open_take_profit_order(order):
                        continue
                    if not self._tp_order_matches_side(order, side):
                        continue
                    row_pos = clean_exchange_id(order.get("positionId"))
                    if wanted_pos and row_pos and row_pos != wanted_pos:
                        continue
                    if abs(self._tp_order_price(order) - target) > price_tolerance:
                        continue
                    row_qty = max(
                        0.0,
                        self._f(
                            order.get("qty")
                            or order.get("quantity")
                            or order.get("origQty"),
                            0.0,
                        ),
                    )
                    if row_qty > 0 and abs(row_qty - submitted_safe_qty) > qty_tolerance:
                        continue
                    row_id = clean_exchange_id(
                        order.get("orderId") or order.get("stopPlanOrderId")
                    )
                    if row_id:
                        remaining_ids.add(row_id)
                if confirmed_id in remaining_ids or not remaining_ids:
                    raise RuntimeError(
                        "exact TP rollback did not leave another matching TP live"
                    )
                raise BingxTpOwnershipError(
                    f"BingX exposed a late duplicate TP at {target} for {symbol} {side.upper()}; "
                    f"the new exact order {confirmed_id} was rolled back and retained ids={sorted(remaining_ids)}; "
                    "execution requires ownership reconciliation before more TP writes"
                )
            except BingxTpOwnershipError:
                raise
            except Exception as exc:
                cleanup_error = f"{type(exc).__name__}: {exc}"
            raise BingxTpCoverageError(
                f"BingX exposed duplicate TP ids={fresh_matching_ids} at {target} for {symbol} {side.upper()}; "
                f"exact rollback of new id {confirmed_id} was not proven: {cleanup_error or 'unknown error'}"
            )

        verification = self._protective_identity_snapshot(
            confirmed,
            result,
            queried_order_id=returned_id or confirmed_id,
        )
        confirmed_plan_id, confirmed_order_id = (
            self._confirmed_protective_identity_pair(
                verification,
                fallback_id=confirmed_id or returned_id,
            )
        )
        result["_tp_open_confirmed"] = True
        result["_tp_confirmation_source"] = confirmation_source
        result["_confirmed_order_id"] = confirmed_order_id
        result["_confirmed_stop_plan_id"] = confirmed_plan_id
        result["_confirmed_take_profit_price"] = self._tp_order_price(confirmed) or target
        result["_normalized_quantity"] = max(0.0, self._f(confirmed.get("qty") or confirmed.get("quantity") or confirmed.get("origQty"), submitted_safe_qty))
        result["_confirmed_side"] = side
        result["_confirmed_symbol"] = symbol
        result["verification"] = verification
        return result

    async def normalize_position_tpsl_request(self, *, symbol: str, side: str, qty: float, price: float, kind: str) -> Dict[str, Any]:
        info = await self.instrument_info(symbol)
        p = self._price_round(price, info.price_tick, side=side, kind=kind)
        q = max(0.0, self._floor_to_step(float(qty), info.qty_step)) if info.qty_step > 0 else float(qty)
        return {"symbol": info.symbol, "side": str(side).lower(), "qty": q, "price": p, "kind": str(kind).lower(), "price_tick": info.price_tick, "qty_step": info.qty_step}

    async def create_position_tpsl(self, *, symbol: str, side: str, qty: float, price: float, kind: str, client_id: str | None = None, position_id: int | str | None = None, min_acceptable_qty: float | None = None) -> Dict[str, Any]:
        await self._ensure_hedge_mode()
        n = await self.normalize_position_tpsl_request(symbol=symbol, side=side, qty=qty, price=price, kind=kind)
        kind_l = str(kind).lower()
        typ = "STOP_MARKET" if kind_l in {"sl", "stop", "stop_loss"} else "TAKE_PROFIT_MARKET"
        params = {
            "symbol": _to_bingx_symbol(symbol),
            "side": _side_close(side),
            "positionSide": _position_side(side),
            "type": typ,
            "quantity": self._fmt_num(n["qty"]),
            "stopPrice": self._fmt_num(n["price"], n["price_tick"]),
            "workingType": "MARK_PRICE",
        }
        # By default we keep trigger TP/SL clientOrderID OFF: BingX documents
        # clientOrderID for regular MARKET/LIMIT, while trigger-order support must
        # be verified by a controlled VST/tiny-test.  The opt-in flag gives us a
        # safe parity path without risking production rejects by default.
        protective_client_order_id = ""
        try:
            from app.config import get_settings as _get_settings
            if bool(getattr(_get_settings(), "BINGX_PROTECTIVE_CLIENT_ORDER_ID_ENABLED", False)) and client_id:
                protective_client_order_id = self._client_id("abx-tpsl", client_id)
                params["clientOrderID"] = protective_client_order_id
        except Exception:
            protective_client_order_id = ""
        open_tpsl = await self.fetch_open_algo_orders(symbol)
        same_side_tpsl = [
            o for o in open_tpsl
            if _normalize_symbol(o.get("symbol")) == _normalize_symbol(symbol)
            and str(o.get("side") or "").lower() == str(side).lower()
            and self._is_conditional_close_type(o.get("type"))
        ]
        if len(same_side_tpsl) >= 20:
            raise BingxApiError(
                f"BingX TP/SL limit reached for {_normalize_symbol(symbol)} {str(side).upper()}: "
                f"{len(same_side_tpsl)} active protective orders; cleanup required before creating a new one"
            )
        data, quantity_retry_audit = await self._post_trade_order_with_quantity_retry(
            params=params,
            symbol=symbol,
            context=f"tpsl:{typ}",
            min_acceptable_qty=min_acceptable_qty,
        )
        submitted_qty = float((quantity_retry_audit or {}).get("submitted_quantity") or n["qty"])
        if quantity_retry_audit:
            n = dict(n)
            n["qty"] = submitted_qty
        order_id = self._order_id_from_response(data)
        out = {
            "success": True,
            "code": 0,
            "data": data,
            "order": data,
            "normalized": n,
            "orderId": order_id,
            "orderID": order_id,
            "stopPlanOrderId": order_id,
            "clientOrderID": self._client_order_id_from_response(data) or protective_client_order_id,
            "clientOrderId": self._client_order_id_from_response(data) or protective_client_order_id,
            "_protective_client_order_id_enabled": bool(protective_client_order_id),
            "_exchange": "bingx",
            "_submitted_quantity": submitted_qty,
        }
        if quantity_retry_audit:
            out["_quantity_retry"] = quantity_retry_audit
        return out

    async def set_position_stop_loss(self, *, symbol: str, side: str, qty: float, stop: float, client_id: str | None = None, position_id: int | str | None = None, adopt_existing: bool = True, owned_order_ids: Iterable[str] | None = None) -> Dict[str, Any]:
        """Create/adopt a protective STOP with MEXC-style read-back confirmation."""
        symbol_norm = _normalize_symbol(symbol)
        side_l = str(side or "").strip().lower()
        if side_l not in {"long", "short"}:
            raise BingxApiError(f"Unsupported BingX STOP side: {side!r}")
        requested_qty = self._require_positive_finite(qty, field="BingX STOP qty")
        requested_stop = self._require_positive_finite(stop, field="BingX STOP price")

        await self._ensure_hedge_mode()
        info = await self.instrument_info(symbol_norm)
        n = await self.normalize_position_tpsl_request(
            symbol=symbol_norm, side=side_l, qty=requested_qty, price=requested_stop, kind="sl"
        )
        stop_price = float(n["price"])
        normalized_qty = float(n["qty"])
        if normalized_qty <= 0:
            raise BingxApiError(
                f"BingX STOP qty after rounding is zero for {symbol_norm} {side_l}; STOP write aborted"
            )

        lock_seed = f"{self.api_key}|{symbol_norm}|{side_l.upper()}"
        lock_key = hashlib.sha256(lock_seed.encode("utf-8")).hexdigest()
        lock = await _get_stop_write_lock(lock_key)

        async with lock:
            from app.database import db as _db

            async with _db.distributed_advisory_lock(f"bingx-stop:{lock_key}"):
                return await self._create_stop_loss_under_lock(
                    symbol=symbol_norm,
                    side=side_l,
                    requested_qty=requested_qty,
                    normalized_qty=normalized_qty,
                    stop_price=stop_price,
                    price_tick=float(n.get("price_tick") or info.price_tick or 0.0),
                    qty_step=float(n.get("qty_step") or info.qty_step or 0.0),
                    client_id=client_id,
                    position_id=position_id,
                    adopt_existing=adopt_existing,
                    owned_order_ids=owned_order_ids,
                )

    async def emergency_close_market(self, *, symbol: str, side: str, qty: float, client_id: str | None = None, position_id: int | str | None = None, open_type: int | None = None) -> Dict[str, Any]:
        await self._ensure_hedge_mode()
        info = await self.instrument_info(symbol)
        params: Dict[str, Any] = {
            "symbol": _to_bingx_symbol(symbol),
            "side": _side_close(side),
            "positionSide": _position_side(side),
            "type": "MARKET",
            "quantity": self._fmt_num(qty, info.qty_step),
            "clientOrderID": self._attempt_client_id("abx-close", client_id),
        }
        if position_id:
            params["positionId"] = str(position_id)
        data, quantity_retry_audit = await self._post_trade_order_with_quantity_retry(
            params=params,
            symbol=symbol,
            context="emergency_close",
        )
        submitted_qty = float((quantity_retry_audit or {}).get("submitted_quantity") or qty)
        order_id = self._order_id_from_response(data)
        out = {
            "success": True,
            "code": 0,
            "data": data,
            "symbol": _to_bingx_symbol(symbol),
            "orderId": order_id,
            "orderID": order_id,
            "clientOrderId": self._client_order_id_from_response(data) or params["clientOrderID"],
            "clientOrderID": self._client_order_id_from_response(data) or params["clientOrderID"],
            "_exchange": "bingx",
            "_submitted_quantity": submitted_qty,
        }
        if quantity_retry_audit:
            out["_quantity_retry"] = quantity_retry_audit
        return out

    async def emergency_close_market_confirmed(self, *, symbol: str, side: str, qty: float, client_id: str | None = None, position_id: int | str | None = None, open_type: int | None = None) -> Dict[str, Any]:
        """Submit emergency close and prove the live position decreased.

        A BingX MARKET close response is only an acknowledgement.  MEXC parity
        requires before/after live-position readback before callers can mark a
        rollback/manual close as successful.
        """
        requested_qty = self._require_positive_finite(qty, field="BingX emergency close qty")
        info = await self.instrument_info(symbol)
        normalized_qty = self._floor_to_step(requested_qty, info.qty_step) if info.qty_step > 0 else requested_qty
        if normalized_qty <= 0:
            raise BingxApiError(f"Emergency close qty {requested_qty} below BingX qty step for {symbol}")

        before = await self.fetch_open_positions(symbol, side.upper())
        before_qty = sum(float(p.get("size") or 0.0) for p in before)
        if before_qty <= 0:
            return {
                "order": {},
                "confirmed": True,
                "already_flat": True,
                "before_qty": before_qty,
                "after_qty": 0.0,
                "closed_qty": 0.0,
                "requested_qty": requested_qty,
                "_exchange": "bingx",
            }
        close_qty = min(normalized_qty, before_qty)
        order = await self.emergency_close_market(symbol=symbol, side=side, qty=close_qty, client_id=client_id, position_id=position_id, open_type=open_type)

        qty_tolerance = max(float(info.qty_step or 0.0) * 0.51, abs(close_qty) * 1e-9, 1e-12)
        target_after = max(0.0, before_qty - close_qty)
        after_qty = before_qty
        after_rows: list[dict[str, Any]] = []
        confirmed = False
        for attempt in range(10):
            if attempt:
                await asyncio.sleep(min(2.0, 0.25 * attempt))
            after_rows = await self.fetch_open_positions(symbol, side.upper())
            after_qty = sum(float(p.get("size") or 0.0) for p in after_rows)
            if after_qty <= target_after + qty_tolerance or after_qty < before_qty - qty_tolerance:
                confirmed = True
                break
        return {
            "order": order,
            "confirmed": bool(confirmed),
            "before_qty": before_qty,
            "after_qty": after_qty,
            "closed_qty": max(0.0, before_qty - after_qty),
            "requested_qty": requested_qty,
            "submitted_qty": close_qty,
            "target_after_qty": target_after,
            "after_positions": after_rows,
            "_exchange": "bingx",
        }

    @classmethod
    def _normalize_price_row(cls, row: Dict[str, Any]) -> tuple[str, Dict[str, float]]:
        sym = _normalize_symbol(row.get("symbol") or row.get("s"))
        last = cls._f(row.get("lastPrice") or row.get("price") or row.get("last") or row.get("close"), 0.0)
        mark = cls._f(row.get("markPrice") or row.get("mark_price") or row.get("mark") or row.get("fairPrice") or row.get("fair"), 0.0)
        index = cls._f(row.get("indexPrice") or row.get("index_price") or row.get("index"), 0.0)
        return sym, {
            "last": last,
            "fair": mark or last or index,
            "mark": mark or last or index,
            "index": index or mark or last,
            "bid": cls._f(row.get("bidPrice") or row.get("bid"), 0.0),
            "ask": cls._f(row.get("askPrice") or row.get("ask"), 0.0),
        }

    async def _fetch_premium_index_map(self, symbols: List[str]) -> Dict[str, Dict[str, float]]:
        wanted = {_normalize_symbol(s) for s in symbols}
        params: Dict[str, Any] = {}
        if len(wanted) == 1:
            params["symbol"] = _to_bingx_symbol(next(iter(wanted)))
        try:
            data = await self._request("GET", "/openApi/swap/v2/quote/premiumIndex", params=params or None)
        except Exception as exc:
            log.debug("BingX premiumIndex unavailable; falling back to ticker price fields: %s", exc)
            return {}
        out: dict[str, dict[str, float]] = {}
        for row in self._rows(data):
            sym, prices = self._normalize_price_row(row)
            if sym and (not wanted or sym in wanted):
                out[sym] = prices
        return out

    async def fetch_market_prices_bulk(
        self,
        symbols: List[str],
        *,
        include_unrequested: bool = False,
    ) -> Dict[str, Dict[str, float]]:
        """Fetch the public all-ticker payload once.

        Normal callers keep the historical requested-symbol filter. The public
        monitor may opt into the already-downloaded unrequested ticker rows so
        its process-local snapshot can serve slow full-reconcile BE pre-reads
        without issuing one ticker+premium pair per dormant symbol.
        """
        data = await self._request("GET", "/openApi/swap/v2/quote/ticker")
        out: dict[str, dict[str, float]] = {}
        wanted = {_normalize_symbol(s) for s in symbols}
        for row in self._rows(data):
            sym, prices = self._normalize_price_row(row)
            if not sym:
                continue
            if wanted and sym not in wanted and not include_unrequested:
                continue
            out[sym] = prices
        premium = await self._fetch_premium_index_map(symbols)
        for sym, p in premium.items():
            if sym in out:
                out[sym].update({k: v for k, v in p.items() if v > 0 or k in {"fair", "mark", "index"}})
            else:
                out[sym] = p
        return out

    async def fetch_price_tick_map(self, symbols: List[str]) -> Dict[str, float]:
        """Return public BingX price ticks for the requested symbols in one call.

        The market-event re-arm gate needs the real exchange tick size so that
        one-tick quote noise cannot look like a genuine retreat from ENTRY/TP/STOP.
        Missing or malformed rows are omitted and the caller keeps its safe
        percentage-based fallback.
        """
        wanted = {_normalize_symbol(s) for s in symbols if _normalize_symbol(s)}
        if not wanted:
            return {}
        data = await self._request("GET", "/openApi/swap/v2/quote/contracts")
        out: dict[str, float] = {}
        for row in self._rows(data):
            symbol = _normalize_symbol(row.get("symbol"))
            if symbol not in wanted:
                continue
            explicit_tick = self._f(
                row.get("priceUnit") or row.get("tickSize") or row.get("priceTick"),
                0.0,
            )
            if explicit_tick > 0:
                out[symbol] = float(explicit_tick)
                continue
            try:
                precision = int(self._f(row.get("pricePrecision"), -1))
            except (TypeError, ValueError, OverflowError):
                precision = -1
            if 0 <= precision <= 18:
                out[symbol] = float(10 ** (-precision))
        return out

    async def fetch_stop_only_fair_map(self, symbols: List[str]) -> Dict[str, bool]:
        # The bot submits BingX STOP/TP with workingType=MARK_PRICE, so event
        # detection must prefer mark/fair price for protective triggers.
        return {_normalize_symbol(s): True for s in symbols}

    async def fetch_market_prices(self, symbol: str) -> Dict[str, float]:
        data = await self._request("GET", "/openApi/swap/v2/quote/ticker", params={"symbol": _to_bingx_symbol(symbol)})
        rows = self._rows(data)
        if not rows:
            raise BingxResponseIntegrityError(
                endpoint="/openApi/swap/v2/quote/ticker",
                reason=f"empty ticker response for {_normalize_symbol(symbol)}",
            )
        row = self._select_requested_symbol_row(
            rows,
            requested_symbol=symbol,
            endpoint="/openApi/swap/v2/quote/ticker",
        )
        _, prices = self._normalize_price_row(row)
        premium = await self._fetch_premium_index_map([symbol])
        p = premium.get(_normalize_symbol(symbol))
        if p:
            prices.update({k: v for k, v in p.items() if v > 0 or k in {"fair", "mark", "index"}})
        return prices

    async def fetch_last_price(self, symbol: str) -> float:
        return float((await self.fetch_market_prices(symbol)).get("last") or 0.0)

    async def fetch_price(self, symbol: str) -> float:
        return await self.fetch_last_price(symbol)

    async def verify_api(self) -> bool:
        await self.fetch_balance_details()
        return True


# Backward-compatible class alias for code that instantiates MexcAdapter directly
# in the BingX-only build (for example old handlers before exchange_factory cleanup).
MexcAdapter = BingxAdapter
