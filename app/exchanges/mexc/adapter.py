"""MEXC Perpetual Futures (USDT-M, /api/v1/private/*) adapter.

API: https://www.mexc.com/api-docs/futures/

Implements the project adapter API surface so the rest of the project
doesn't care which exchange is in use.  Implements the same set of methods,
same exception types, and same dict shapes for positions/orders.

Key MEXC specifics:
  - Symbol format: ``BTC_USDT`` (with underscore).  We accept any of
    ``BTCUSDT`` / ``BTC-USDT`` / ``BTC/USDT`` / ``BTC_USDT`` and normalise.
  - Auth: HMAC-SHA256 of ``accessKey + timestamp + paramString``.
    Headers carry: ``ApiKey``, ``Request-Time`` (ms), ``Signature``,
    optional ``Recv-Window`` (seconds, max 60).
  - No passphrase.
  - GET/DELETE: sign sorted query string concatenated with ``&``.
  - POST: sign the raw JSON body string (no dict sorting).
  - Position mode is hedge (positionMode=1) — we enforce this on first write.
  - Place order uses numeric ``side`` codes:
        1 = open long, 2 = close short, 3 = open short, 4 = close long
    We translate from semantic side='long'|'short' + open/close intent.
  - Attached TP/SL on entry order: MEXC supports ``stopLossPrice`` and
    ``takeProfitPrice`` on ``/order/create`` — atomic with the entry.
  - Standalone TP/SL on a position: POST ``/stoporder/place`` and requires
    the live ``positionId`` returned by ``open_positions``.
  - MEXC ``vol`` is contracts, while the shared risk engine works in base
    asset units.  The adapter converts both directions using ``contractSize``.
"""

from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
import logging
import math
import re
import time
import urllib.parse
import uuid
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation, ROUND_DOWN, ROUND_UP
from typing import Any, Dict, List, Optional

import httpx

from app.services.exchange_identity import clean_exchange_id

log = logging.getLogger(__name__)


_SHARED_HTTP_CLIENT: httpx.AsyncClient | None = None

# Process-wide TP write locks. A single user can be represented by several adapter
# instances (signal executor, LIMIT catch-up, recovery and BE monitor). Serialising
# by API key + symbol + position side prevents two coroutines from checking the same empty TP set
# and then both creating the same order. The exchange-side preflight below remains
# the durable protection across restarts.
_TP_WRITE_LOCKS: Dict[str, asyncio.Lock] = {}
_TP_WRITE_LOCKS_META = asyncio.Lock()
_MAX_TP_WRITE_LOCKS = 2000


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


def _get_shared_http_client() -> httpx.AsyncClient:
    """Return one process-wide keep-alive client for all MEXC adapters.

    Authentication headers and signatures are still built per request, so no
    user credentials are stored on the shared client. Reusing the transport
    only preserves TCP/TLS connections and connection-pool state.
    """
    global _SHARED_HTTP_CLIENT
    if _SHARED_HTTP_CLIENT is None or _SHARED_HTTP_CLIENT.is_closed:
        from app.config import get_settings

        settings = get_settings()
        max_connections = max(4, int(settings.MEXC_HTTP_MAX_CONNECTIONS or 40))
        max_keepalive = max(
            2,
            min(
                max_connections, int(settings.MEXC_HTTP_MAX_KEEPALIVE_CONNECTIONS or 20)
            ),
        )
        keepalive_expiry = max(
            5.0, float(settings.MEXC_HTTP_KEEPALIVE_EXPIRY_SEC or 30)
        )
        _SHARED_HTTP_CLIENT = httpx.AsyncClient(
            base_url=MexcAdapter.PROD_BASE_URL,
            headers={"User-Agent": "antilud-vip-core/mexc"},
            limits=httpx.Limits(
                max_connections=max_connections,
                max_keepalive_connections=max_keepalive,
                keepalive_expiry=keepalive_expiry,
            ),
            http2=False,
        )
    return _SHARED_HTTP_CLIENT


async def close_shared_http_client() -> None:
    """Close the process-wide MEXC transport on graceful shutdown."""
    global _SHARED_HTTP_CLIENT
    client = _SHARED_HTTP_CLIENT
    _SHARED_HTTP_CLIENT = None
    if client is not None and not client.is_closed:
        await client.aclose()


# --------------------------------------------------------------------------- #
# Data types and exceptions
# --------------------------------------------------------------------------- #


@dataclass(frozen=True)
class InstrumentInfo:
    symbol: str
    min_qty: float = 1.0  # MEXC trades in contracts (integer vol), default minVol=1
    qty_step: float = 1.0  # MEXC vol step is usually 1
    price_tick: float = 0.0001
    min_notional: float = 1.0
    max_leverage: int = 0
    contract_size: float = (
        1.0  # how many base units per 1 contract; needed to convert qty
    )
    taker_fee_rate: float = 0.0  # decimal rate, e.g. 0.0008 = 0.08%
    stop_only_fair: bool = False  # contract requires fair-price TP/SL triggers


class MexcApiError(RuntimeError):
    pass


class MexcNetworkAmbiguousError(MexcApiError):
    """A private write outcome is unknown; the exchange may have accepted it.

    Ambiguity is not limited to socket timeouts. It also includes transport
    failures, task cancellation during the dispatched request, server/gateway
    5xx responses and successful HTTP responses whose body cannot prove the
    result. Callers must reconcile the exact order/position state before
    retrying any POST/DELETE operation.
    """


class MexcExchangeRejected(MexcApiError):
    """MEXC returned a deterministic HTTP/JSON rejection with a code."""

    def __init__(
        self,
        *,
        http_status: int,
        error_code: Any,
        error_message: str,
        response_audit: Dict[str, Any] | None = None,
    ) -> None:
        self.http_status = int(http_status)
        try:
            self.error_code = int(error_code)
        except (TypeError, ValueError, OverflowError):
            self.error_code = error_code
        self.error_message = str(error_message or "").strip()
        self.retryable: bool | None = None
        self.response_audit = dict(response_audit or {})
        super().__init__(
            f"MEXC HTTP {self.http_status} code={self.error_code}: "
            f"{self.error_message or 'exchange rejected the request'}"
        )


class MexcOrderCancelRejected(MexcApiError):
    """MEXC accepted the batch request but rejected one exact order cancel.

    ``POST /private/order/cancel`` can return outer ``success=true, code=0``
    while the per-order row in ``data[]`` carries a non-zero ``errorCode``.
    Treating the outer envelope as success caused repeated cancel writes and
    left stale LIMIT entries alive.  The structured fields let the lifecycle
    reconcile exact order state without parsing an exception string.
    """

    def __init__(
        self,
        *,
        order_id: str,
        error_code: int | None,
        error_message: str,
        retryable: bool,
        response_audit: Dict[str, Any] | None = None,
    ) -> None:
        self.order_id = clean_exchange_id(order_id)
        self.error_code = error_code
        self.error_message = str(error_message or "").strip()
        self.retryable = bool(retryable)
        self.response_audit = dict(response_audit or {})
        super().__init__(
            "MEXC exact order cancellation rejected "
            f"order_id={self.order_id or 'unknown'} "
            f"errorCode={self.error_code}: "
            f"{self.error_message or 'no error message'}"
        )


class MexcOrderCancelUnconfirmed(MexcApiError):
    """The exact cancel response cannot prove one per-order outcome.

    The outer MEXC envelope may still be successful while the exact ``data``
    row is missing, duplicated or malformed.  The raw-but-bounded audit payload
    is carried to the lifecycle so it can be persisted and shown to the user
    without logging API credentials or request signatures.
    """

    def __init__(
        self,
        *,
        order_id: str,
        error_message: str,
        response_audit: Dict[str, Any] | None = None,
    ) -> None:
        self.order_id = clean_exchange_id(order_id)
        self.error_code: int | None = None
        self.error_message = str(error_message or "").strip()
        self.retryable: bool | None = None
        self.response_audit = dict(response_audit or {})
        super().__init__(
            "MEXC exact order cancellation could not be confirmed "
            f"order_id={self.order_id or 'unknown'}: "
            f"{self.error_message or 'missing exact per-order result'}"
        )


class MexcTpCoverageError(MexcApiError):
    """A new TP cannot be placed without over-covering the live position.

    This is a fail-closed condition, not a successful idempotent write. Callers
    must keep the execution in a recovery/manual state instead of recording the
    missing signal target as created.
    """


class MexcMarketProtectionError(MexcApiError):
    """MARKET entry was accepted, but post-fill STOP protection failed.

    The exception carries enough structured context for the executor to report
    whether the emergency rollback was confirmed instead of incorrectly saying
    that no exchange write happened.
    """

    def __init__(
        self,
        message: str,
        *,
        emergency_close_confirmed: bool,
        entry_order: Optional[Dict[str, Any]] = None,
        protection_order: Optional[Dict[str, Any]] = None,
        emergency_close_order: Optional[Dict[str, Any]] = None,
        position_id: int | str | None = None,
        opened_qty: float = 0.0,
        actual_entry: float = 0.0,
    ) -> None:
        super().__init__(message)
        self.emergency_close_confirmed = bool(emergency_close_confirmed)
        self.entry_order = entry_order
        self.protection_order = protection_order
        self.emergency_close_order = emergency_close_order
        self.position_id = position_id
        self.opened_qty = float(opened_qty or 0.0)
        self.actual_entry = float(actual_entry or 0.0)


class MexcSymbolNotSupported(MexcApiError):
    pass


# --------------------------------------------------------------------------- #
# Symbol normalisation helpers
# --------------------------------------------------------------------------- #


_MEXC_CONTRACT_SUFFIXES = ("-PERP", "_PERP", "-SWAP", "_SWAP", "_UMCBL")


def _normalize_contract_symbol(value: Any) -> str:
    """Normalise incoming form to internal BTCUSDT (uppercase, no separators).

    Internal storage uses BTCUSDT; MEXC wire format ``BTC_USDT`` is rebuilt
    via ``_to_mexc_symbol`` only when calling the exchange.
    """
    if value is None:
        return ""
    s = str(value).strip().upper()
    for suffix in _MEXC_CONTRACT_SUFFIXES:
        if s.endswith(suffix):
            s = s[: -len(suffix)]
    s = s.replace("/", "").replace(":", "").replace("-", "").replace("_", "")
    return s


def _to_mexc_symbol(symbol: str) -> str:
    """Convert BTCUSDT (internal) -> BTC_USDT (MEXC wire format)."""
    s = _normalize_contract_symbol(symbol)
    if not s:
        return s
    for quote in ("USDT", "USDC", "USD"):
        if s.endswith(quote) and len(s) > len(quote):
            return f"{s[: -len(quote)]}_{quote}"
    return s


def _ctx_symbol(
    params: Optional[Dict[str, Any]], body: Any
) -> str:
    for src in (params or {}, body or {}):
        if isinstance(src, dict) and "symbol" in src and src["symbol"]:
            return _normalize_contract_symbol(src["symbol"])
    return ""


def _looks_symbol_unavailable(data: Any) -> str:
    """Return reason string when MEXC rejects a symbol, "" otherwise.

    Outcomes:
      - "invalid_symbol" — pair does NOT exist on MEXC.
      - "no_permission" — pair exists but is closed for API trading
        (apiAllowed=false, KYC missing, region restricted, etc).
      - "kyc_required" — explicit KYC error.
      - "" — not a symbol-availability problem.
    """
    if not isinstance(data, dict):
        return ""
    # MEXC futures uses string error codes sometimes
    code = str(data.get("code") or "").strip()
    msg_raw = str(data.get("message") or data.get("msg") or data)
    msg = msg_raw.lower()

    # Common MEXC futures error codes
    if code in {"1001", "1002"} and ("contract" in msg or "symbol" in msg):
        return "invalid_symbol"
    if "contract not exist" in msg or "contract does not exist" in msg:
        return "invalid_symbol"
    if "symbol not exist" in msg or "invalid symbol" in msg:
        return "invalid_symbol"
    if "unsupported contract" in msg or "unknown contract" in msg:
        return "invalid_symbol"

    if "kyc" in msg:
        return "kyc_required"
    if "identity" in msg and "verif" in msg:
        return "kyc_required"

    if "not allowed" in msg and ("api" in msg or "trade" in msg):
        return "no_permission"
    if "permission" in msg and (
        "symbol" in msg or "contract" in msg or "trading" in msg
    ):
        return "no_permission"
    if "trading restricted" in msg or "trading suspended" in msg:
        return "no_permission"
    if "api trading is not enabled" in msg or "api trading not allowed" in msg:
        return "no_permission"
    if "region" in msg and "restrict" in msg:
        return "no_permission"
    return ""


# --------------------------------------------------------------------------- #
# Adapter
# --------------------------------------------------------------------------- #


class MexcAdapter:
    """MEXC Perpetual Futures (USDT-M) REST adapter.

    Public + private endpoints used:
      Public (no auth):
          GET  /api/v1/contract/detail/country       (instrument info, optional ?symbol=)
          GET  /api/v1/contract/index_price/{symbol} (index price)
          GET  /api/v1/contract/fair_price/{symbol}  (mark / fair price)

      Private (HMAC-SHA256 signed; headers ApiKey/Request-Time/Signature):
          GET  /api/v1/private/account/assets
          GET  /api/v1/private/account/asset/{currency}
          GET  /api/v1/private/position/open_positions
          POST /api/v1/private/position/change_leverage
          GET  /api/v1/private/position/leverage
          POST /api/v1/private/position/change_position_mode
          GET  /api/v1/private/position/position_mode
          POST /api/v1/private/order/create             (entry + attached SL/TP)
          POST /api/v1/private/order/cancel
          POST /api/v1/private/order/cancel_all
          GET  /api/v1/private/order/list/open_orders
          POST /api/v1/private/stoporder/place             (standalone TP/SL)
          POST /api/v1/private/stoporder/cancel
          POST /api/v1/private/stoporder/cancel_all
          GET  /api/v1/private/stoporder/open_orders

    Safety rules (MEXC safety rules):
      - selected safe leverage is set before order sizing;
      - risk is calculated from entry-stop distance, never from balance*leverage;
      - STOP/TP close existing position (reduce-only by construction in MEXC);
      - emergency close goes through close-side market order.
    """

    PROD_BASE_URL = "https://api.mexc.com"
    # MEXC has no public testnet for futures; the env flag is kept for API
    # symmetry only and currently maps to the same URL.
    DEMO_BASE_URL = "https://api.mexc.com"

    # MEXC numeric side codes
    _SIDE_OPEN_LONG = 1
    _SIDE_CLOSE_SHORT = 2
    _SIDE_OPEN_SHORT = 3
    _SIDE_CLOSE_LONG = 4

    # MEXC order type codes (subset we need)
    _TYPE_LIMIT = 1
    _TYPE_POST_ONLY = 2
    _TYPE_IOC = 3
    _TYPE_FOK = 4
    _TYPE_MARKET = 5
    _TYPE_CONVERT_MARKET = 6  # convert market to current price

    # MEXC open type
    _OPEN_TYPE_ISOLATED = 1
    _OPEN_TYPE_CROSS = 2

    # MEXC position mode
    _POS_MODE_HEDGE = 1
    _POS_MODE_ONE_WAY = 2

    # MEXC trigger / stop kind for stoporder
    _TRIGGER_TYPE_LAST = 1  # last price
    _TRIGGER_TYPE_FAIR = 2  # fair / mark price (recommended for SL)

    def __init__(
        self,
        api_key: str,
        api_secret: str,
        passphrase: str = "",  # unused for MEXC, kept for adapter compatibility
        *,
        testnet: bool = False,
        timeout_ms: int = 15000,
    ) -> None:
        self._api_key = (api_key or "").strip()
        self._api_secret = (api_secret or "").strip()
        # passphrase intentionally ignored — MEXC does not use it.
        del passphrase
        self._base_url = self.DEMO_BASE_URL if testnet else self.PROD_BASE_URL
        timeout_sec = max(1.0, float(timeout_ms) / 1000.0)
        self._timeout = httpx.Timeout(timeout_sec, connect=min(5.0, timeout_sec))
        # Cached metadata
        self._instrument_cache: Dict[str, InstrumentInfo] = {}
        self._api_symbols_cache: Optional[set[str]] = None
        self._api_symbols_cache_ts: float = 0.0
        # Track whether we've already enforced hedge mode in this session.
        self._hedge_mode_ensured: bool = False
        # The current MEXC place-order endpoint requires leverage when opening.
        # Cache the effective leverage/open type selected by the shared executor.
        self._position_leverage: Dict[tuple[str, str], int] = {}
        self._position_open_type: Dict[tuple[str, str], int] = {}

    # --- HTTP plumbing -----------------------------------------------------

    async def close(self) -> None:
        # Adapters share one process-wide keep-alive transport. Individual
        # adapter lifetimes must not close connections used by other users or
        # monitor tasks; app.main closes the shared transport on shutdown.
        return None

    def _http(self) -> httpx.AsyncClient:
        return _get_shared_http_client()

    @staticmethod
    def _sorted_query(params: Dict[str, Any]) -> str:
        """Build sorted query string for GET/DELETE signing & URL.

        MEXC requires GET/DELETE business params sorted in dictionary order,
        concatenated with ``&``. Booleans serialised as ``true``/``false``.
        Null values excluded entirely.
        """
        if not params:
            return ""
        items: List[tuple[str, str]] = []
        for k in sorted(params.keys()):
            v = params[k]
            if v is None:
                continue
            if isinstance(v, bool):
                items.append((k, "true" if v else "false"))
            elif isinstance(v, float):
                items.append((k, format(v, "f").rstrip("0").rstrip(".") or "0"))
            else:
                items.append((k, str(v)))
        return urllib.parse.urlencode(items, quote_via=urllib.parse.quote, safe=",")

    @staticmethod
    def _post_body_str(body: Any) -> str:
        """JSON-encode POST body. No sorting; MEXC signs the raw body string."""
        if not body:
            return ""
        # MEXC documents that POST params are signed as JSON without sorting,
        # using camelCase keys.  Some bulk-cancel endpoints require a bare JSON
        # list, so preserve list payloads instead of coercing everything to dict.
        if isinstance(body, dict):
            clean: Any = {k: v for k, v in body.items() if v is not None}
        elif isinstance(body, list):
            clean = [
                {k: v for k, v in item.items() if v is not None}
                if isinstance(item, dict)
                else item
                for item in body
            ]
        else:
            clean = body
        return json.dumps(clean, separators=(",", ":"), ensure_ascii=False)

    def _sign(self, payload: str, timestamp_ms: str) -> str:
        """MEXC signature = HMAC-SHA256(accessKey + timestamp + paramString)."""
        target = (self._api_key + timestamp_ms + payload).encode("utf-8")
        return hmac.new(
            self._api_secret.encode("utf-8"),
            target,
            hashlib.sha256,
        ).hexdigest()

    def _headers(
        self,
        auth: bool,
        *,
        signature: str = "",
        timestamp_ms: str = "",
        recv_window_sec: int = 0,
    ) -> Dict[str, str]:
        headers: Dict[str, str] = {"Content-Type": "application/json"}
        if auth:
            headers["ApiKey"] = self._api_key
            headers["Request-Time"] = timestamp_ms
            headers["Signature"] = signature
            if recv_window_sec > 0:
                # max 60 according to docs; recommended <= 30
                headers["Recv-Window"] = str(min(60, max(1, recv_window_sec)))
        return headers

    async def _request(
        self,
        method: str,
        path: str,
        *,
        params: Optional[Dict[str, Any]] = None,
        body: Any = None,
        auth: bool = False,
        recv_window_sec: int = 10,
    ) -> Dict[str, Any]:
        params = dict(params or {})
        if body is None:
            body = {}
        method_up = method.upper()

        # Acquire the process-wide workload permit *before* signing. Waiting
        # after computing Request-Time could make a valid request expire in the
        # queue. Protection writes receive higher priority in the governor.
        from app.services.workload_manager import (
            MexcWorkloadTimeout,
            govern_mexc_request,
        )

        try:
            async with govern_mexc_request(
                api_key=self._api_key,
                auth=auth,
                method=method_up,
                path=path,
                body=body if isinstance(body, dict) else {},
            ) as workload_metrics:
                timestamp_ms = str(int(time.time() * 1000))

                # Choose payload to sign based on method.
                if method_up in ("GET", "DELETE"):
                    sign_payload = self._sorted_query(params)
                    request_body_bytes: bytes = b""
                    url = path + ("?" + sign_payload if sign_payload else "")
                else:
                    # POST: sign the exact raw JSON body sent on the wire.
                    sign_payload = self._post_body_str(body)
                    request_body_bytes = sign_payload.encode("utf-8")
                    url = path

                signature = self._sign(sign_payload, timestamp_ms) if auth else ""
                headers = self._headers(
                    auth,
                    signature=signature,
                    timestamp_ms=timestamp_ms,
                    recv_window_sec=recv_window_sec,
                )

                client = self._http()
                request_started = time.monotonic()
                try:
                    if method_up == "GET":
                        resp = await client.get(
                            url, headers=headers, timeout=self._timeout
                        )
                    elif method_up == "DELETE":
                        resp = await client.request(
                            "DELETE", url, headers=headers, timeout=self._timeout
                        )
                    elif method_up == "POST":
                        resp = await client.post(
                            url,
                            headers=headers,
                            content=request_body_bytes,
                            timeout=self._timeout,
                        )
                    else:
                        raise MexcApiError(f"Unsupported HTTP method: {method}")
                except asyncio.CancelledError as exc:
                    # Once an awaited private write has been dispatched, task
                    # cancellation cannot prove that MEXC did not accept it.
                    # Reclassify it so lifecycle callers reconcile exact state
                    # instead of blindly repeating the write after restart.
                    if method_up in ("POST", "DELETE"):
                        raise MexcNetworkAmbiguousError(
                            f"MEXC {method_up} {path} task cancelled during write; "
                            "state unknown"
                        ) from exc
                    raise
                except httpx.TransportError as exc:
                    # Fail closed for every write-side transport failure. A
                    # ReadError, WriteError or RemoteProtocolError may be raised
                    # after MEXC already accepted the request, exactly like a
                    # timeout. Treating it as a harmless failed write can create
                    # a duplicate entry/STOP/TP on retry or leave a live position
                    # outside local state.
                    if method_up in ("POST", "DELETE"):
                        raise MexcNetworkAmbiguousError(
                            f"MEXC {method_up} {path} transport failed; state unknown: "
                            f"{type(exc).__name__}: {exc}"
                        ) from exc
                    raise MexcApiError(
                        f"MEXC {method_up} {path} network error: "
                        f"{type(exc).__name__}: {exc}"
                    ) from exc
                except httpx.HTTPError as exc:
                    # Future httpx versions may raise a non-transport error after
                    # a write was dispatched. Preserve the same fail-closed
                    # outcome instead of assuming the exchange rejected it.
                    if method_up in ("POST", "DELETE"):
                        raise MexcNetworkAmbiguousError(
                            f"MEXC {method_up} {path} HTTP client failure; "
                            f"state unknown: {type(exc).__name__}: {exc}"
                        ) from exc
                    raise MexcApiError(
                        f"MEXC {method_up} {path} HTTP error: "
                        f"{type(exc).__name__}: {exc}"
                    ) from exc
        except MexcWorkloadTimeout as exc:
            # No HTTP request was sent, therefore this is never an ambiguous
            # exchange write. Callers may safely retry/reconcile as configured.
            raise MexcApiError(str(exc)) from exc

        elapsed_ms = int((time.monotonic() - request_started) * 1000)
        workload_wait_ms = int(workload_metrics.get("wait_ms") or 0)
        if elapsed_ms >= 1500 or workload_wait_ms >= 1000:
            # Never log query/body/auth headers: they can contain account context.
            log.warning(
                "slow MEXC request method=%s path=%s status=%s network_ms=%s workload_wait_ms=%s priority=%s",
                method_up,
                path,
                resp.status_code,
                elapsed_ms,
                workload_wait_ms,
                workload_metrics.get("priority"),
            )
        else:
            log.debug(
                "MEXC request method=%s path=%s status=%s network_ms=%s workload_wait_ms=%s",
                method_up,
                path,
                resp.status_code,
                elapsed_ms,
                workload_wait_ms,
            )

        is_write = method_up in ("POST", "DELETE")
        response_cannot_prove_rejection = resp.status_code == 408 or resp.status_code >= 500

        # A gateway/server failure after a private write cannot prove that the
        # matching engine rejected the operation. Do this before interpreting
        # MEXC's JSON envelope: retrying a 5xx write as a deterministic failure
        # can duplicate an entry, STOP, TP or cancellation.
        if is_write and response_cannot_prove_rejection:
            raise MexcNetworkAmbiguousError(
                f"MEXC {method_up} {path} returned HTTP {resp.status_code}; "
                "write state unknown"
            )

        # Parse body. A successful write without a valid MEXC JSON envelope is
        # also ambiguous because HTTP success alone does not identify the exact
        # order/plan that was accepted.
        try:
            data = resp.json()
        except ValueError as exc:
            text = resp.text
            if is_write and 200 <= resp.status_code < 300:
                raise MexcNetworkAmbiguousError(
                    f"MEXC {method_up} {path} returned non-JSON HTTP "
                    f"{resp.status_code}; write state unknown"
                ) from exc
            if resp.status_code >= 500:
                raise MexcApiError(
                    f"MEXC HTTP {resp.status_code}: {text[:300]}"
                ) from exc
            unavail = _looks_symbol_unavailable({"code": "", "message": text})
            if unavail:
                self._raise_symbol_unavailable(
                    unavail, params, body, resp.status_code, text
                )
            raise MexcApiError(
                f"MEXC HTTP {resp.status_code}: {text[:300]}"
            ) from exc

        if not isinstance(data, dict):
            if is_write and 200 <= resp.status_code < 300:
                raise MexcNetworkAmbiguousError(
                    f"MEXC {method_up} {path} returned unexpected HTTP "
                    f"{resp.status_code} payload; write state unknown"
                )
            raise MexcApiError(f"MEXC {path}: unexpected payload: {data!r}")

        # MEXC futures: success when {"success": true, "code": 0} or code==0.
        code = data.get("code")
        success_flag = data.get("success")
        if (
            resp.status_code >= 400
            or (code is not None and str(code) != "0")
            or success_flag is False
        ):
            unavail = _looks_symbol_unavailable(data)
            if unavail:
                self._raise_symbol_unavailable(
                    unavail, params, body, resp.status_code, data
                )
            msg = data.get("message") or data.get("msg") or str(data)
            msg_l = str(msg).lower()
            if "api key" in msg_l and (
                "expir" in msg_l or "invalid" in msg_l or "not exist" in msg_l
            ):
                raise MexcApiError(
                    "🔐 MEXC API ключ недействителен или истёк (TTL=90 дней без IP-whitelist).\n\n"
                    "Что сделать:\n"
                    "1) Создай новый API ключ на MEXC (Profile → API Management)\n"
                    "2) Включи разрешение <b>Futures Trading</b> (KYC обязателен)\n"
                    "3) Передай боту: <code>/api mexc API_KEY API_SECRET</code>"
                )
            raise MexcExchangeRejected(
                http_status=resp.status_code,
                error_code=code,
                error_message=str(msg),
                response_audit={
                    "outer": {
                        "success": self._bounded_audit_value(success_flag),
                        "code": self._bounded_audit_value(code),
                        "message": self._bounded_audit_value(msg),
                    },
                    "data": self._bounded_audit_value(data.get("data")),
                },
            )
        return data

    def _raise_symbol_unavailable(
        self,
        reason: str,
        params: Optional[Dict[str, Any]],
        body: Any,
        status_code: int,
        data: Any,
    ) -> None:
        symbol = _ctx_symbol(params, body)
        if reason == "kyc_required":
            raise MexcSymbolNotSupported(
                f"🔐 MEXC требует пройти KYC для торговли фьючерсами через API.\n\n"
                f"<b>Что сделать:</b>\n"
                f"1) Зайди на MEXC → Profile → Identity Verification\n"
                f"2) Пройди KYC (документ + селфи)\n"
                f"3) После одобрения создай новый API ключ с правом Futures Trading\n"
                f"4) Передай боту: <code>/api mexc API_KEY API_SECRET</code>\n\n"
                f"<i>Технические детали: HTTP {status_code}, {data}</i>"
            )
        if reason == "no_permission":
            raise MexcSymbolNotSupported(
                f"🔐 MEXC отказывает в торговле парой {symbol}\n\n"
                f"Пара существует на MEXC, но твой API-ключ не может с ней работать.\n\n"
                f"<b>Что делать:</b>\n\n"
                f"1️⃣ <b>Пересоздай API ключ</b>:\n"
                f"   • MEXC → API Management → Create API Key\n"
                f"   • Включи разрешения <b>Futures Trading</b> (требует KYC)\n"
                f"   • НЕ ограничивай пары в настройках ключа\n"
                f"   • Передай новые ключи боту через <code>/api mexc</code>\n\n"
                f"2️⃣ Проверь регион: MEXC ограничивает торговлю в некоторых странах\n\n"
                f"3️⃣ Запусти диагностику: напиши боту <code>проверь {symbol}</code>\n\n"
                f"<i>Технические детали: HTTP {status_code}, {data}</i>"
            )
        # invalid_symbol
        raise MexcSymbolNotSupported(
            f"⏭ {symbol} не найдена на MEXC Futures.\n"
            f"Возможно пара называется иначе или ещё не залистена.\n\n"
            f"Ответ MEXC: HTTP {status_code}: {data}"
        )

    # --- Number formatting helpers -----------------------------------------

    @staticmethod
    def _decimal_clean(value: Any) -> Decimal:
        if value is None:
            return Decimal("0")
        if isinstance(value, Decimal):
            return value if value.is_finite() else Decimal("0")
        try:
            parsed = Decimal(str(value))
            return parsed if parsed.is_finite() else Decimal("0")
        except (InvalidOperation, ValueError):
            return Decimal("0")

    @staticmethod
    def _fmt_num(value: float, step: float | None = None) -> str:
        """Format number without scientific notation, optionally rounded to step.

        Trailing zeros after the decimal point are stripped for clean output
        (consistent with the shared service contract).
        """
        d = MexcAdapter._decimal_clean(value)
        if step is not None and step > 0:
            step_dec = MexcAdapter._decimal_clean(step)
            if step_dec > 0:
                quant = step_dec.as_tuple().exponent
                if quant < 0:
                    fmt = f"{{:.{-quant}f}}"
                    s = fmt.format(float(d))
                    if "." in s:
                        s = s.rstrip("0").rstrip(".")
                    return s or "0"
        s = format(float(d), "f")
        if "." in s:
            s = s.rstrip("0").rstrip(".")
        return s or "0"

    @staticmethod
    def _floor_qty_to_step(qty: float, step: float) -> float:
        """Decimal-precise floor to avoid float-precision errors (0.236 / 0.001
        = 235.999... → 235 → 0.235 instead of 0.236)."""
        if step <= 0:
            return float(qty)
        d = Decimal(str(qty))
        q = Decimal(str(step))
        floored = (d / q).to_integral_value(rounding=ROUND_DOWN) * q
        return float(floored)

    @staticmethod
    def _ceil_to_step(value: float, step: float) -> float:
        if step <= 0:
            return float(value)
        d = Decimal(str(value))
        q = Decimal(str(step))
        ceiled = (d / q).to_integral_value(rounding=ROUND_UP) * q
        return float(ceiled)

    @staticmethod
    def _round_price_for_field(
        value: float, step: float, *, side: str, field: str
    ) -> float:
        """Round price toward a side-safe direction.

        For both SL and TP triggers we round to whichever side of the tick
        stays inside the conservative envelope for the given trade side.
        """
        if step <= 0:
            return float(value)
        side_u = (side or "").upper()
        if side_u in ("LONG", "BUY"):
            return MexcAdapter._floor_qty_to_step(float(value), step)
        return MexcAdapter._ceil_to_step(float(value), step)

    # --- Common helpers ----------------------------------------------------

    @staticmethod
    def _client_id(prefix: str, fixed: str | None = None) -> str:
        if fixed in (None, ""):
            return (prefix + uuid.uuid4().hex)[:32]
        cleaned = clean_exchange_id(fixed)
        if not cleaned:
            raise MexcApiError("Некорректный external/client order id")
        return cleaned[:32]

    @staticmethod
    def _validated_exchange_id(value: Any, *, field: str) -> str:
        cleaned = clean_exchange_id(value)
        if not cleaned:
            raise MexcApiError(f"{field} отсутствует или повреждён")
        return cleaned

    @staticmethod
    def _wire_exchange_id(value: Any, *, field: str) -> int | str:
        cleaned = MexcAdapter._validated_exchange_id(value, field=field)
        return int(cleaned) if cleaned.isascii() and cleaned.isdigit() else cleaned

    @staticmethod
    def _rows(data: Any) -> List[Dict[str, Any]]:
        """Return list of dicts from a MEXC response payload.

        MEXC responses are ``{success, code, data: ...}`` where data may be
        a list, a single dict, or a wrapper containing a list under 'data'
        or 'orderList' etc.
        """
        if data is None:
            return []
        if isinstance(data, list):
            return [r for r in data if isinstance(r, dict)]
        if isinstance(data, dict):
            payload = data.get("data", data)
            if isinstance(payload, list):
                return [r for r in payload if isinstance(r, dict)]
            if isinstance(payload, dict):
                for key in (
                    "list",
                    "positions",
                    "orders",
                    "orderList",
                    "result",
                    "balance",
                    "balances",
                ):
                    inner = payload.get(key)
                    if isinstance(inner, list):
                        return [r for r in inner if isinstance(r, dict)]
                return [payload]
        return []

    @staticmethod
    def _float_value(value: Any, default: float = 0.0) -> float:
        try:
            if value is None or value == "" or isinstance(value, bool):
                return default
            parsed = float(value)
            return parsed if math.isfinite(parsed) else default
        except (TypeError, ValueError, OverflowError):
            return default

    @staticmethod
    def _require_positive_finite(value: Any, *, field: str) -> float:
        if isinstance(value, bool):
            raise MexcApiError(f"{field} должен быть положительным конечным числом")
        try:
            parsed = float(value)
        except (TypeError, ValueError, OverflowError) as exc:
            raise MexcApiError(
                f"{field} должен быть положительным конечным числом"
            ) from exc
        if not math.isfinite(parsed) or parsed <= 0:
            raise MexcApiError(f"{field} должен быть положительным конечным числом")
        return parsed

    @staticmethod
    def _validated_trade_side(side: Any) -> str:
        normalized = str(side or "").strip().lower()
        if normalized not in {"long", "short"}:
            raise MexcApiError(f"Неподдерживаемое направление позиции: {side!r}")
        return normalized

    @staticmethod
    def _validated_write_symbol(symbol: Any) -> str:
        normalized = _normalize_contract_symbol(symbol)
        if not re.fullmatch(r"[A-Z0-9]{1,30}USDT", normalized):
            raise MexcApiError(f"Некорректный MEXC USDT-M символ: {symbol!r}")
        return normalized

    # --- Encoding side -----------------------------------------------------

    @staticmethod
    def _side_code_open(position_side: str) -> int:
        """Return MEXC numeric side code for opening a position."""
        return (
            MexcAdapter._SIDE_OPEN_LONG
            if position_side.upper() == "LONG"
            else MexcAdapter._SIDE_OPEN_SHORT
        )

    @staticmethod
    def _side_code_close(position_side: str) -> int:
        """Return MEXC numeric side code for closing a position."""
        return (
            MexcAdapter._SIDE_CLOSE_LONG
            if position_side.upper() == "LONG"
            else MexcAdapter._SIDE_CLOSE_SHORT
        )

    # --- Position mode (hedge) — set once per session ---------------------

    async def _ensure_hedge_mode(self) -> None:
        """Make sure account is in hedge position mode.

        We use hedge mode so LONG and SHORT positions can coexist (and so
        position-side logic matches the shared service contract).  Called lazily
        before the first write to avoid touching account state on read-only
        usage.  Safe to call repeatedly: MEXC returns code=0 if already set.
        """
        if self._hedge_mode_ensured:
            return
        try:
            cur = await self._request(
                "GET",
                "/api/v1/private/position/position_mode",
                auth=True,
            )
            payload = cur.get("data") if isinstance(cur, dict) else None
            if isinstance(payload, dict):
                mode = self._float_value(payload.get("positionMode"), 0)
            elif isinstance(payload, list) and payload and isinstance(payload[0], dict):
                mode = self._float_value(payload[0].get("positionMode"), 0)
            else:
                mode = self._float_value(payload, 0)
            if int(mode) == self._POS_MODE_HEDGE:
                self._hedge_mode_ensured = True
                return
        except MexcApiError as exc:
            log.debug("MEXC position_mode read failed (will try to set): %s", exc)

        try:
            await self._request(
                "POST",
                "/api/v1/private/position/change_position_mode",
                body={"positionMode": self._POS_MODE_HEDGE},
                auth=True,
            )
            self._hedge_mode_ensured = True
        except MexcNetworkAmbiguousError:
            # Do not downgrade an unknown account-setting write into a proven
            # rejection. The caller must re-read mode before attempting any
            # subsequent private order write.
            raise
        except MexcApiError as exc:
            text = str(exc).lower()
            if "already" in text or "same" in text or "no need" in text:
                self._hedge_mode_ensured = True
                return
            # Fail closed. Continuing with hedge-side order fields while the
            # account is still one-way can create rejected or misclassified orders.
            raise MexcApiError(
                "MEXC не удалось переключить в hedge mode; новая сделка остановлена: "
                f"{exc}"
            ) from exc

    # --- Account / balance -------------------------------------------------

    async def fetch_balance_details(self) -> Dict[str, float]:
        """Return normalized MEXC USDT futures balance details.

        MEXC exposes ``equity``, ``cashBalance`` and ``availableBalance``.
        Risk sizing follows the project risk-sizing convention and uses
        total equity, while the UI can still display wallet/available values.
        """
        data = await self._request(
            "GET",
            "/api/v1/private/account/assets",
            auth=True,
        )
        account_row: Optional[Dict[str, Any]] = None
        for row in self._rows(data):
            currency = str(row.get("currency") or row.get("asset") or "").upper()
            if currency in {"USDT", "SUSDT"}:
                account_row = row
                break
        if account_row is None:
            raise MexcApiError(f"MEXC balance response has no USDT row: {data}")

        def _first_numeric(keys: tuple[str, ...], default: float) -> float:
            for key in keys:
                value = account_row.get(key)
                if value is not None and value != "":
                    return self._float_value(value, default)
            return default

        equity = _first_numeric(("equity", "balance", "cashBalance"), 0.0)
        wallet = _first_numeric(("cashBalance", "balance"), equity)
        available = _first_numeric(
            ("availableBalance", "availableOpen", "availableCash"),
            0.0,
        )
        return {
            "total_equity": equity,
            "total_wallet_balance": wallet,
            "available_balance": available,
            "coin_equity": equity,
            "coin_wallet_balance": wallet,
            "USDT": available,
        }

    async def fetch_balance_usdt(self) -> float:
        details = await self.fetch_balance_details()
        if "total_equity" in details:
            # Zero equity must stay zero; truthiness fallback can dangerously
            # report wallet balance while unrealised PnL has consumed equity.
            return max(0.0, self._float_value(details.get("total_equity"), 0.0))
        if "total_wallet_balance" in details:
            return max(
                0.0, self._float_value(details.get("total_wallet_balance"), 0.0)
            )
        return max(0.0, self._float_value(details.get("available_balance"), 0.0))

    # --- Positions / orders read -------------------------------------------

    async def _instrument_info_or_default(self, symbol: str) -> InstrumentInfo:
        """Best-effort metadata for read-normalisation.

        Read paths must not disappear merely because the public metadata endpoint
        is temporarily unavailable.  Writes still call ``instrument_info`` and
        fail closed when a contract cannot be verified.
        """
        norm = _normalize_contract_symbol(symbol)
        cached = self._instrument_cache.get(norm)
        if cached is not None:
            return cached
        try:
            # Public contract metadata is account-independent. Share it across
            # adapters and coalesce concurrent cold reads so a positions screen
            # with many symbols does not repeat sequential detail requests.
            from app.services.ttl_cache import get_instrument_info_cache

            info = await get_instrument_info_cache().get_or_fetch(
                ("mexc", norm),
                lambda: self.instrument_info(norm),
            )
            self._instrument_cache[norm] = info
            return info
        except Exception as exc:  # noqa: BLE001 - reads remain best-effort
            log.warning("MEXC metadata unavailable for %s during read: %s", norm, exc)
            return InstrumentInfo(norm)

    @staticmethod
    def _contract_value_to_base(vol: Any, contract_size: Any) -> float:
        """Multiply native contract volume by contract size using Decimal."""
        contracts = max(Decimal("0"), MexcAdapter._decimal_clean(vol))
        size = max(Decimal("1e-30"), MexcAdapter._decimal_clean(contract_size or 1.0))
        return float(contracts * size)

    @staticmethod
    def _contracts_to_base(vol: float, info: InstrumentInfo) -> float:
        """Convert contracts to base quantity without binary-float lot loss."""
        # MEXC reports contract volume in native vol units. Decimal
        # multiplication preserves the exact base-asset lot before the final
        # API-facing float conversion (e.g. 6662 * 0.000001 = 0.006662).
        return MexcAdapter._contract_value_to_base(vol, info.contract_size)

    @staticmethod
    def _base_to_contracts(qty: float, info: InstrumentInfo) -> tuple[float, float]:
        """Convert shared base-asset qty to MEXC contract ``vol`` exactly.

        ``InstrumentInfo.qty_step`` is deliberately exposed in base units to the
        shared risk/TP engine. MEXC wire payloads require contracts, so the whole
        division/floor path stays in Decimal. Performing ``float(qty) /
        contractSize`` first can turn exactly 493 contracts into
        ``492.99999999999994`` and silently remove one contract.
        """
        qty_d = max(Decimal("0"), MexcAdapter._decimal_clean(qty))
        contract_size_d = MexcAdapter._decimal_clean(info.contract_size or 1.0)
        if contract_size_d <= 0:
            raise MexcApiError(
                f"Некорректный contractSize={info.contract_size} для {info.symbol}"
            )
        if info.qty_step > 0:
            contract_step_d = (
                MexcAdapter._decimal_clean(info.qty_step) / contract_size_d
            )
        else:
            contract_step_d = Decimal("1")
        if contract_step_d <= 0:
            contract_step_d = Decimal("1")
        raw_contracts = qty_d / contract_size_d
        contracts_d = (raw_contracts / contract_step_d).to_integral_value(
            rounding=ROUND_DOWN
        ) * contract_step_d
        return float(contracts_d), float(contract_step_d)

    async def fetch_open_positions(
        self,
        symbol: Optional[str] = None,
        side: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        params: Dict[str, Any] = {}
        if symbol:
            params["symbol"] = _to_mexc_symbol(symbol)
        data = await self._request(
            "GET",
            "/api/v1/private/position/open_positions",
            params=params,
            auth=True,
        )

        candidates: list[tuple[Dict[str, Any], str, float, str]] = []
        target_symbol = _normalize_contract_symbol(symbol) if symbol else ""
        target_side = str(side or "").lower()
        for row in self._rows(data):
            pos_type = row.get("positionType")
            if pos_type is not None:
                try:
                    pos_type_i = 0 if isinstance(pos_type, bool) else int(pos_type)
                except (TypeError, ValueError):
                    pos_type_i = 0
                if pos_type_i == 1:
                    pos_side = "LONG"
                elif pos_type_i == 2:
                    pos_side = "SHORT"
                else:
                    log.warning(
                        "MEXC ignored open-position row with unknown positionType=%r: %r",
                        pos_type,
                        row,
                    )
                    continue
            else:
                ps = str(row.get("positionSide") or row.get("side") or "").upper()
                if ps not in {"LONG", "SHORT"}:
                    log.warning(
                        "MEXC ignored open-position row without a valid side: %r", row
                    )
                    continue
                pos_side = ps

            hold_contracts = self._float_value(
                row.get("holdVol")
                or row.get("size")
                or row.get("availableSize")
                or row.get("positionAmt")
                or row.get("vol"),
                0.0,
            )
            if hold_contracts <= 0:
                continue
            sym = _normalize_contract_symbol(row.get("symbol"))
            if target_symbol and sym != target_symbol:
                continue
            if target_side and pos_side.lower() != target_side:
                continue
            candidates.append((row, pos_side, hold_contracts, sym))

        unique_symbols = sorted({item[3] for item in candidates if item[3]})
        if unique_symbols:
            await asyncio.gather(
                *[self._instrument_info_or_default(sym) for sym in unique_symbols]
            )

        out: List[Dict[str, Any]] = []
        for row, pos_side, hold_contracts, sym in candidates:
            info = self._instrument_cache.get(sym) or InstrumentInfo(sym)
            frozen_contracts = self._float_value(row.get("frozenVol"), 0.0)
            available_contracts = self._float_value(
                row.get("holdAvailVol") or row.get("availableSize"),
                max(0.0, hold_contracts - max(0.0, frozen_contracts)),
            )
            out.append(
                {
                    "symbol": sym,
                    "side": pos_side.lower(),
                    "positionSide": pos_side,
                    "size": self._contracts_to_base(hold_contracts, info),
                    "availableSize": self._contracts_to_base(
                        max(0.0, available_contracts), info
                    ),
                    "contracts": hold_contracts,
                    "positionId": clean_exchange_id(
                        row.get("positionId") or row.get("id")
                    ),
                    "openType": int(self._float_value(row.get("openType"), 0)),
                    "entryPrice": self._float_value(
                        row.get("holdAvgPrice")
                        or row.get("openAvgPrice")
                        or row.get("entryPrice")
                        or row.get("avgPrice"),
                        0.0,
                    ),
                    "breakEvenPrice": self._float_value(row.get("breakEvenPrice"), 0.0),
                    "leverage": int(self._float_value(row.get("leverage"), 0)),
                    "raw": row,
                }
            )
        return out

    async def _position_for_side(
        self,
        symbol: str,
        side: str,
        *,
        attempts: int = 4,
        delay_sec: float = 0.35,
    ) -> Optional[Dict[str, Any]]:
        """Find the live position, allowing a short post-MARKET propagation delay."""
        side_l = str(side or "").lower()
        for attempt in range(max(1, attempts)):
            positions = await self.fetch_open_positions(symbol, side_l)
            for pos in positions:
                if str(pos.get("side") or "").lower() == side_l:
                    return pos
            if attempt + 1 < attempts:
                await asyncio.sleep(delay_sec)
        return None

    @staticmethod
    def _response_data_dict(response: Any) -> Dict[str, Any]:
        if isinstance(response, dict):
            data = response.get("data")
            if isinstance(data, dict):
                return data
            return response
        return {}

    @classmethod
    def _extract_order_id(cls, response: Any) -> str:
        payload = cls._response_data_dict(response)
        value = payload.get("orderId") or payload.get("id")
        return clean_exchange_id(value)

    @staticmethod
    def _require_exact_order_detail_id(
        detail: Dict[str, Any],
        expected_order_id: str,
        *,
        context: str,
    ) -> str:
        returned_order_id = clean_exchange_id(
            detail.get("orderId") or detail.get("id")
        )
        if not returned_order_id:
            raise MexcApiError(
                f"MEXC {context} response has no orderId identity for exact "
                f"request {expected_order_id}"
            )
        if returned_order_id != expected_order_id:
            raise MexcApiError(
                f"MEXC {context} identity mismatch: requested "
                f"orderId={expected_order_id}, returned orderId={returned_order_id}"
            )
        return returned_order_id

    async def fetch_entry_order_fill_status(
        self,
        *,
        symbol: str,
        order_response: Dict[str, Any],
    ) -> Dict[str, Any]:
        """Return authoritative LIMIT/MARKET entry fill state in base units.

        A visible position is not proof that a LIMIT order finished: MEXC can
        expose a partial position while the remaining entry quantity is still
        open.  TP planning must wait for a terminal order state and use the
        final ``dealVol`` from this endpoint.
        """
        order_id = self._extract_order_id(order_response)
        external_oid = clean_exchange_id(
            order_response.get("_entry_external_oid")
            or order_response.get("externalOid")
            or self._response_data_dict(order_response).get("externalOid")
        )
        if order_id:
            response = await self._request(
                "GET",
                f"/api/v1/private/order/get/{order_id}",
                auth=True,
            )
        elif external_oid:
            wire = _to_mexc_symbol(symbol)
            response = await self._request(
                "GET",
                f"/api/v1/private/order/external/{wire}/{external_oid}",
                auth=True,
            )
        else:
            raise MexcApiError("MEXC entry response has no orderId/externalOid")
        detail = self._response_data_dict(response)
        if not detail:
            raise MexcApiError(
                f"MEXC returned empty order detail for {order_id or external_oid}"
            )

        returned_order_id = ""
        if order_id:
            returned_order_id = self._require_exact_order_detail_id(
                detail,
                order_id,
                context="order/get",
            )
        returned_external_oid = clean_exchange_id(
            detail.get("externalOid")
            or detail.get("clientOrderId")
            or detail.get("clientId")
        )
        returned_symbol = _normalize_contract_symbol(detail.get("symbol"))
        expected_symbol = _normalize_contract_symbol(symbol)
        if not order_id:
            if not returned_external_oid:
                raise MexcApiError(
                    "MEXC external-order response has no externalOid identity for "
                    f"exact request {external_oid}"
                )
            if returned_external_oid != external_oid:
                raise MexcApiError(
                    "MEXC external-order identity mismatch: requested "
                    f"externalOid={external_oid}, returned externalOid={returned_external_oid}"
                )
            if returned_symbol and returned_symbol != expected_symbol:
                raise MexcApiError(
                    "MEXC external-order symbol mismatch: requested "
                    f"symbol={expected_symbol}, returned symbol={returned_symbol}"
                )

        info = await self.instrument_info(symbol)
        state = int(self._float_value(detail.get("state"), 0.0))
        deal_contracts = self._float_value(detail.get("dealVol"), 0.0)
        order_contracts = self._float_value(detail.get("vol"), 0.0)
        filled_qty = (
            self._contracts_to_base(deal_contracts, info) if deal_contracts > 0 else 0.0
        )
        requested_qty = (
            self._contracts_to_base(order_contracts, info)
            if order_contracts > 0
            else 0.0
        )
        qty_tolerance = max(float(info.qty_step or 0.0) * 0.51, 1e-12)
        terminal = state in {3, 4, 5}
        fully_filled = state == 3 or (
            requested_qty > 0 and filled_qty + qty_tolerance >= requested_qty
        )
        resolved_order_id = returned_order_id
        return {
            "order_id": resolved_order_id,
            "state": state,
            "terminal": terminal,
            "fully_filled": fully_filled,
            "filled_qty": float(filled_qty),
            "requested_qty": float(requested_qty),
            "avg_fill_price": self._float_value(
                detail.get("dealAvgPrice") or detail.get("price"), 0.0
            ),
            "position_id": clean_exchange_id(detail.get("positionId")),
            "external_oid": clean_exchange_id(detail.get("externalOid")),
            "qty_step": float(info.qty_step or 0.0),
            "detail": detail,
        }

    async def _wait_order_fill(
        self,
        order_response: Dict[str, Any],
        info: InstrumentInfo,
        *,
        attempts: int = 16,
        delay_sec: float = 0.25,
    ) -> tuple[Dict[str, Any], float, float]:
        """Wait until a MARKET order has a confirmed fill.

        Returns ``(order_detail, filled_base_qty, average_fill_price)``.  MEXC
        returns only an order id from ``/order/create``; using the official order
        detail endpoint avoids guessing fill quantity from a possibly pre-existing
        aggregated position.
        """
        order_id = self._extract_order_id(order_response)
        if not order_id:
            raise MexcApiError("MEXC MARKET order response has no orderId")

        last: Dict[str, Any] = {}
        last_error: Exception | None = None
        for attempt in range(max(1, attempts)):
            try:
                response = await self._request(
                    "GET",
                    f"/api/v1/private/order/get/{order_id}",
                    auth=True,
                )
                detail = self._response_data_dict(response)
                if detail:
                    self._require_exact_order_detail_id(
                        detail,
                        order_id,
                        context="MARKET order/get",
                    )
                    last = detail
                state = int(self._float_value(detail.get("state"), 0))
                deal_contracts = self._float_value(detail.get("dealVol"), 0.0)
                if state == 3 and deal_contracts > 0:
                    filled_qty = self._contracts_to_base(deal_contracts, info)
                    avg_price = self._float_value(
                        detail.get("dealAvgPrice") or detail.get("price"),
                        0.0,
                    )
                    return detail, filled_qty, avg_price
                if state in (4, 5) and deal_contracts <= 0:
                    raise MexcApiError(
                        f"MEXC MARKET order {order_id} finished without a fill (state={state})"
                    )
            except MexcApiError as exc:
                # Terminal no-fill states are definitive; transient read errors are not.
                if "finished without a fill" in str(exc):
                    raise
                last_error = exc
            if attempt + 1 < attempts:
                await asyncio.sleep(delay_sec)

        deal_contracts = self._float_value(last.get("dealVol"), 0.0)
        state = int(self._float_value(last.get("state"), 0))
        raise MexcApiError(
            f"MEXC did not confirm terminal MARKET fill for order {order_id} within "
            f"{max(1, attempts) * delay_sec:.2f}s; state={state}, "
            f"reported dealVol={deal_contracts}, last read error: {last_error}"
        )

    async def _wait_order_fill_by_external_id(
        self,
        *,
        symbol: str,
        external_oid: str,
        info: InstrumentInfo,
        attempts: int = 16,
        delay_sec: float = 0.25,
    ) -> tuple[Dict[str, Any], float, float]:
        """Resolve an ambiguous MARKET write by its unique external order id."""
        wire = urllib.parse.quote(_to_mexc_symbol(symbol), safe="_")
        oid = urllib.parse.quote(str(external_oid), safe="")
        last_error: Exception | None = None
        for attempt in range(max(1, attempts)):
            try:
                response = await self._request(
                    "GET",
                    f"/api/v1/private/order/external/{wire}/{oid}",
                    auth=True,
                )
                detail = self._response_data_dict(response)
                if detail.get("orderId"):
                    state = int(self._float_value(detail.get("state"), 0))
                    deal_contracts = self._float_value(detail.get("dealVol"), 0.0)
                    if state == 3 and deal_contracts > 0:
                        return (
                            detail,
                            self._contracts_to_base(deal_contracts, info),
                            self._float_value(
                                detail.get("dealAvgPrice") or detail.get("price"),
                                0.0,
                            ),
                        )
                    # The order exists but its terminal state has not propagated yet.
                    return await self._wait_order_fill(response, info)
            except Exception as exc:
                last_error = exc
            if attempt + 1 < attempts:
                await asyncio.sleep(delay_sec)
        raise MexcApiError(
            f"MEXC could not resolve external order {external_oid} after an ambiguous write: "
            f"{last_error}"
        )

    async def _wait_order_by_external_id(
        self,
        *,
        symbol: str,
        external_oid: str,
        attempts: int = 8,
        delay_sec: float = 0.25,
    ) -> Dict[str, Any]:
        """Resolve an ambiguous regular-order write without requiring a fill.

        LIMIT entries can remain open for hours, so the MARKET helper above is
        intentionally too strict for them: it waits for a terminal fill.  For
        an ambiguous LIMIT ``POST /order/create`` it is sufficient to prove
        that the unique ``externalOid`` exists and recover its exact ``orderId``.
        The normal pending-limit monitor will then own the subsequent fill and
        cancellation lifecycle.
        """

        wire = urllib.parse.quote(_to_mexc_symbol(symbol), safe="_")
        oid = urllib.parse.quote(str(external_oid), safe="")
        last_error: Exception | None = None
        last_detail: Dict[str, Any] = {}
        for attempt in range(max(1, attempts)):
            try:
                response = await self._request(
                    "GET",
                    f"/api/v1/private/order/external/{wire}/{oid}",
                    auth=True,
                )
                detail = self._response_data_dict(response)
                if detail:
                    last_detail = detail
                order_id = clean_exchange_id(detail.get("orderId") or detail.get("id"))
                if order_id:
                    return detail
            except Exception as exc:  # noqa: BLE001 - bounded authoritative re-read
                last_error = exc
            if attempt + 1 < attempts:
                await asyncio.sleep(delay_sec)
        raise MexcApiError(
            "MEXC could not resolve the regular order after an ambiguous write "
            f"by externalOid={external_oid}; last_detail={last_detail!r}; "
            f"last_error={last_error}"
        )

    def entry_order_id(self, order_response: Dict[str, Any]) -> str:
        """Return the saved regular-order id without exposing private parsing details."""
        return str(self._extract_order_id(order_response or {}) or "")

    @staticmethod
    def _cancel_row_for_order(response: Dict[str, Any], order_id: str) -> Dict[str, Any]:
        """Return the exact per-order cancel result from MEXC ``data[]``.

        The endpoint is a batch API even when one order id is supplied.  Missing
        or ambiguous rows are not success because the caller cannot prove which
        order the envelope refers to.
        """
        data = response.get("data") if isinstance(response, dict) else None
        rows: list[Dict[str, Any]] = []
        if isinstance(data, list):
            rows = [row for row in data if isinstance(row, dict)]
        elif isinstance(data, dict):
            rows = [data]
        wanted = clean_exchange_id(order_id)
        exact = [
            row
            for row in rows
            if clean_exchange_id(row.get("orderId") or row.get("id")) == wanted
        ]
        if len(exact) == 1:
            return exact[0]
        if len(rows) == 1 and not clean_exchange_id(
            rows[0].get("orderId") or rows[0].get("id")
        ):
            # Some MEXC deployments omit orderId for a single-item batch.  It is
            # still unambiguous only because exactly one id was requested.
            return rows[0]
        raise MexcApiError(
            "MEXC exact cancel response has no unique per-order result "
            f"for order_id={wanted or 'unknown'}; rows={len(rows)}"
        )

    @staticmethod
    def _cancel_error_code(row: Dict[str, Any]) -> int | None:
        raw = row.get("errorCode")
        if raw in (None, ""):
            # A few responses use ``code`` inside the per-order object.
            raw = row.get("code")
        try:
            return int(raw) if raw not in (None, "") and not isinstance(raw, bool) else None
        except (TypeError, ValueError, OverflowError):
            return None


    @staticmethod
    def _bounded_audit_value(value: Any, *, depth: int = 0) -> Any:
        """Return a JSON-safe bounded copy of one exchange response value."""
        if depth >= 4:
            return "<max-depth>"
        if value is None or isinstance(value, (bool, int)):
            return value
        if isinstance(value, float):
            return value if math.isfinite(value) else str(value)
        if isinstance(value, str):
            return value[:1000]
        if isinstance(value, dict):
            result: Dict[str, Any] = {}
            for index, (key, item) in enumerate(value.items()):
                if index >= 40:
                    result["<truncated>"] = len(value) - 40
                    break
                result[str(key)[:120]] = MexcAdapter._bounded_audit_value(
                    item, depth=depth + 1
                )
            return result
        if isinstance(value, (list, tuple)):
            rows = [
                MexcAdapter._bounded_audit_value(item, depth=depth + 1)
                for item in list(value)[:40]
            ]
            if len(value) > 40:
                rows.append({"<truncated>": len(value) - 40})
            return rows
        return str(value)[:1000]


    @classmethod
    def _cancel_response_audit(
        cls,
        response: Dict[str, Any] | None,
        *,
        order_id: str,
        row: Dict[str, Any] | None = None,
    ) -> Dict[str, Any]:
        source = response if isinstance(response, dict) else {}
        return {
            "version": 1,
            "order_id": clean_exchange_id(order_id),
            "outer": {
                "success": cls._bounded_audit_value(source.get("success")),
                "code": cls._bounded_audit_value(source.get("code")),
                "message": cls._bounded_audit_value(
                    source.get("message") or source.get("msg")
                ),
            },
            "per_order": cls._bounded_audit_value(row)
            if isinstance(row, dict)
            else None,
            "data": cls._bounded_audit_value(source.get("data")),
            "data_shape": (
                "list"
                if isinstance(source.get("data"), list)
                else "dict"
                if isinstance(source.get("data"), dict)
                else type(source.get("data")).__name__
            ),
            "data_count": (
                len(source.get("data"))
                if isinstance(source.get("data"), list)
                else 1
                if isinstance(source.get("data"), dict)
                else 0
            ),
        }

    async def cancel_entry_order(
        self, order_response: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Cancel one exact regular entry order by its saved MEXC order id.

        Unlike symbol-wide cancel_all, this cannot remove an unrelated entry on
        the same contract.  Both the outer envelope and the exact ``data[]`` row
        are validated.  A non-zero per-order ``errorCode`` is never reported as
        success.
        """
        order_id = self._extract_order_id(order_response or {})
        if not order_id:
            raise MexcApiError("MEXC entry response has no orderId for exact cancel")
        try:
            typed_id: int | str = int(str(order_id))
        except (TypeError, ValueError):
            typed_id = str(order_id)
        response = await self._request(
            "POST",
            "/api/v1/private/order/cancel",
            body=[typed_id],
            auth=True,
        )
        try:
            row = self._cancel_row_for_order(response, order_id)
        except MexcApiError as exc:
            audit = self._cancel_response_audit(response, order_id=order_id)
            log.warning(
                "MEXC exact cancel response unconfirmed order_id=%s audit=%s",
                order_id,
                json.dumps(audit, ensure_ascii=False, sort_keys=True),
            )
            raise MexcOrderCancelUnconfirmed(
                order_id=order_id,
                error_message=str(exc),
                response_audit=audit,
            ) from exc
        error_code = self._cancel_error_code(row)
        audit = self._cancel_response_audit(response, order_id=order_id, row=row)
        # Official successful rows use errorCode=0.  Missing/invalid codes are
        # ambiguous and must not be silently accepted.
        if error_code is None:
            log.warning(
                "MEXC exact cancel response missing errorCode order_id=%s audit=%s",
                order_id,
                json.dumps(audit, ensure_ascii=False, sort_keys=True),
            )
            raise MexcOrderCancelUnconfirmed(
                order_id=order_id,
                error_message=(
                    "MEXC exact cancel response has no valid per-order errorCode "
                    f"for order_id={order_id}"
                ),
                response_audit=audit,
            )
        if error_code != 0:
            message = str(
                row.get("errorMsg") or row.get("message") or row.get("msg") or ""
            ).strip()
            # 2040 = order not found, 2041 = current state cannot be cancelled.
            # Repeating the same write cannot fix either state; callers should
            # perform authoritative reads and wait for/manual-confirm terminality.
            retryable = error_code not in {2040, 2041}
            raise MexcOrderCancelRejected(
                order_id=order_id,
                error_code=error_code,
                error_message=message,
                retryable=retryable,
                response_audit=audit,
            )
        result = dict(response)
        result["_exact_cancel_result"] = {
            "order_id": order_id,
            "error_code": 0,
            "error_message": str(row.get("errorMsg") or "").strip(),
            "response_audit": audit,
        }
        log.info(
            "MEXC exact cancel accepted order_id=%s audit=%s",
            order_id,
            json.dumps(audit, ensure_ascii=False, sort_keys=True),
        )
        return result

    async def cancel_regular_orders_exact(
        self, order_ids: list[str | int]
    ) -> Dict[str, Any]:
        """Cancel only explicitly identified regular orders.

        MEXC accepts up to 50 regular order ids per request. Every supplied
        non-empty identifier must be a strict scalar exchange id, and every
        requested id must have one unambiguous ``data[]`` result with
        ``errorCode=0``. The outer ``success/code`` envelope alone is not proof
        that any individual order was cancelled.
        """

        cleaned: list[int | str] = []
        cleaned_text: list[str] = []
        seen: set[str] = set()
        for raw in order_ids or []:
            if raw in (None, ""):
                continue
            text = clean_exchange_id(raw)
            if not text:
                raise MexcApiError("MEXC exact regular cancel received a damaged order id")
            if text in seen:
                continue
            seen.add(text)
            cleaned_text.append(text)
            cleaned.append(int(text) if text.isascii() and text.isdigit() else text)
        if not cleaned:
            return {"success": True, "code": 0, "data": [], "_exact_cancel_results": []}
        if len(cleaned) > 50:
            raise MexcApiError("MEXC exact regular cancel supports at most 50 order ids")
        response = await self._request(
            "POST",
            "/api/v1/private/order/cancel",
            body=cleaned,
            auth=True,
        )
        data = response.get("data") if isinstance(response, dict) else None
        rows = [row for row in data if isinstance(row, dict)] if isinstance(data, list) else ([data] if isinstance(data, dict) else [])
        if len(cleaned_text) > 1 and any(
            not clean_exchange_id(row.get("orderId") or row.get("id")) for row in rows
        ):
            raise MexcApiError(
                "MEXC batch cancel returned an unidentified per-order row for multiple exact ids"
            )
        exact_results: list[dict[str, Any]] = []
        for order_id in cleaned_text:
            row = self._cancel_row_for_order(response, order_id)
            error_code = self._cancel_error_code(row)
            if error_code is None:
                raise MexcApiError(
                    "MEXC exact batch cancel response has no valid per-order errorCode "
                    f"for order_id={order_id}"
                )
            message = str(row.get("errorMsg") or row.get("message") or row.get("msg") or "").strip()
            if error_code != 0:
                raise MexcOrderCancelRejected(
                    order_id=order_id,
                    error_code=error_code,
                    error_message=message,
                    retryable=error_code not in {2040, 2041},
                )
            exact_results.append(
                {"order_id": order_id, "error_code": 0, "error_message": message}
            )
        result = dict(response)
        result["_exact_cancel_results"] = exact_results
        return result

    async def cancel_conditional_orders_exact(
        self, stop_order_ids: list[str | int]
    ) -> Dict[str, Any]:
        """Cancel only explicitly identified MEXC TP/SL plan orders.

        The official futures endpoint expects a JSON list of objects containing
        ``stopPlanOrderId``.  No symbol-wide fallback is permitted here.
        """

        payload: list[dict[str, int | str]] = []
        cleaned_text: list[str] = []
        seen: set[str] = set()
        for raw in stop_order_ids or []:
            if raw in (None, ""):
                continue
            text = clean_exchange_id(raw)
            if not text:
                raise MexcApiError("MEXC exact conditional cancel received a damaged stop-plan id")
            if text in seen:
                continue
            seen.add(text)
            cleaned_text.append(text)
            typed: int | str = int(text) if text.isascii() and text.isdigit() else text
            payload.append({"stopPlanOrderId": typed})
        if not payload:
            return {"success": True, "code": 0, "data": []}
        if len(payload) > 50:
            raise MexcApiError("MEXC exact conditional cancel supports at most 50 order ids")
        response = await self._request(
            "POST",
            "/api/v1/private/stoporder/cancel",
            body=payload,
            auth=True,
        )
        data = response.get("data") if isinstance(response, dict) else None
        rows = (
            [row for row in data if isinstance(row, dict)]
            if isinstance(data, list)
            else [data]
            if isinstance(data, dict)
            else []
        )
        exact_results: list[dict[str, Any]] = []
        for order_id in cleaned_text:
            exact = [
                row
                for row in rows
                if clean_exchange_id(
                    row.get("stopPlanOrderId")
                    or row.get("stopOrderId")
                    or row.get("orderId")
                    or row.get("id")
                )
                == order_id
            ]
            if len(exact) == 1:
                row = exact[0]
            elif len(cleaned_text) == 1 and len(rows) == 1 and not clean_exchange_id(
                rows[0].get("stopPlanOrderId")
                or rows[0].get("stopOrderId")
                or rows[0].get("orderId")
                or rows[0].get("id")
            ):
                # Some deployments omit the id for a one-item batch. The result
                # remains attributable only because one exact id was requested.
                row = rows[0]
            else:
                audit = self._cancel_response_audit(
                    response, order_id=order_id
                )
                raise MexcOrderCancelUnconfirmed(
                    order_id=order_id,
                    error_message=(
                        "MEXC conditional cancel response has no unique per-order result"
                    ),
                    response_audit=audit,
                )
            error_code = self._cancel_error_code(row)
            audit = self._cancel_response_audit(
                response, order_id=order_id, row=row
            )
            if error_code is None:
                raise MexcOrderCancelUnconfirmed(
                    order_id=order_id,
                    error_message=(
                        "MEXC conditional cancel response has no valid per-order errorCode"
                    ),
                    response_audit=audit,
                )
            message = str(
                row.get("errorMsg") or row.get("message") or row.get("msg") or ""
            ).strip()
            if error_code != 0:
                raise MexcOrderCancelRejected(
                    order_id=order_id,
                    error_code=error_code,
                    error_message=message,
                    retryable=error_code not in {2040, 2041, 5002},
                    response_audit=audit,
                )
            exact_results.append(
                {
                    "order_id": order_id,
                    "error_code": 0,
                    "error_message": message,
                    "response_audit": audit,
                }
            )
        result = dict(response)
        result["_exact_cancel_results"] = exact_results
        return result

    async def _cancel_regular_order_best_effort(
        self, order_id: str
    ) -> Optional[Dict[str, Any]]:
        cleaned_id = clean_exchange_id(order_id)
        if not cleaned_id:
            log.warning("MEXC best-effort cancel skipped damaged order id")
            return None
        typed_id: int | str = (
            int(cleaned_id)
            if cleaned_id.isascii() and cleaned_id.isdigit()
            else cleaned_id
        )
        try:
            return await self._request(
                "POST",
                "/api/v1/private/order/cancel",
                body=[typed_id],
                auth=True,
            )
        except Exception as exc:
            log.warning("MEXC cancel unresolved order %s failed: %s", order_id, exc)
            return None

    async def _confirm_position_stop(
        self,
        *,
        symbol: str,
        side: str,
        position_id: int | str,
        stop: float,
        qty: float,
        info: InstrumentInfo,
        attempts: int = 8,
        delay_sec: float = 0.25,
    ) -> Optional[Dict[str, Any]]:
        target_id = clean_exchange_id(position_id)
        if not target_id:
            return None
        side_l = str(side or "").lower()
        qty_tolerance = max(float(info.qty_step or 0.0) * 0.51, 1e-12)
        price_tolerance = max(float(info.price_tick or 0.0) * 0.51, 1e-12)
        for attempt in range(max(1, attempts)):
            try:
                rows = await self.fetch_open_algo_orders(symbol)
                for row in rows:
                    if clean_exchange_id(row.get("positionId")) != target_id:
                        continue
                    if side_l and str(row.get("side") or "").lower() != side_l:
                        continue
                    stop_price = self._float_value(row.get("stopLossPrice"), 0.0)
                    row_qty = self._float_value(row.get("qty"), 0.0)
                    if (
                        stop_price > 0
                        and abs(stop_price - float(stop)) <= price_tolerance
                        and row_qty + qty_tolerance >= float(qty)
                    ):
                        return row
            except (
                Exception
            ) as exc:  # read retries resolve transient propagation/API errors
                log.warning(
                    "MEXC STOP confirmation read failed for %s: %s", symbol, exc
                )
            if attempt + 1 < attempts:
                await asyncio.sleep(delay_sec)
        return None

    async def _position_size_for_side(self, symbol: str, side: str) -> float:
        positions = await self.fetch_open_positions(symbol, side)
        total = 0.0
        for pos in positions:
            value = self._float_value(pos.get("size"), 0.0)
            if value > 0:
                total += value
        return total

    async def _confirm_position_not_above(
        self,
        *,
        symbol: str,
        side: str,
        maximum_qty: float,
        info: InstrumentInfo,
        attempts: int = 8,
        delay_sec: float = 0.25,
    ) -> bool:
        tolerance = max(float(info.qty_step or 0.0) * 0.51, 1e-12)
        for attempt in range(max(1, attempts)):
            try:
                current = await self._position_size_for_side(symbol, side)
                if current <= float(maximum_qty) + tolerance:
                    return True
            except Exception as exc:
                log.warning(
                    "MEXC rollback position confirmation failed for %s: %s", symbol, exc
                )
            if attempt + 1 < attempts:
                await asyncio.sleep(delay_sec)
        return False

    async def fetch_open_orders(
        self,
        symbol: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """Return regular (non-conditional) current orders."""
        data = await self._request(
            "GET",
            "/api/v1/private/order/list/open_orders",
            params={"page_num": 1, "page_size": 100},
            auth=True,
        )
        target = _normalize_contract_symbol(symbol) if symbol else ""
        rows = [
            row
            for row in self._rows(data)
            if not target or _normalize_contract_symbol(row.get("symbol")) == target
        ]
        unique_symbols = sorted(
            {
                _normalize_contract_symbol(row.get("symbol"))
                for row in rows
                if _normalize_contract_symbol(row.get("symbol"))
            }
        )
        if unique_symbols:
            await asyncio.gather(
                *[self._instrument_info_or_default(sym) for sym in unique_symbols]
            )
        return [self._normalize_order_row(row) for row in rows]

    async def fetch_open_algo_orders(
        self,
        symbol: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """Return current position TP/SL orders.

        Errors are propagated. Returning an empty list on API failure can make
        protection monitors believe that a STOP is absent.
        """
        params: Dict[str, Any] = {}
        if symbol:
            params["symbol"] = _to_mexc_symbol(symbol)
        data = await self._request(
            "GET",
            "/api/v1/private/stoporder/open_orders",
            params=params,
            auth=True,
        )
        rows = list(self._rows(data))
        unique_symbols = sorted(
            {
                _normalize_contract_symbol(row.get("symbol"))
                for row in rows
                if _normalize_contract_symbol(row.get("symbol"))
            }
        )
        if unique_symbols:
            await asyncio.gather(
                *[self._instrument_info_or_default(sym) for sym in unique_symbols]
            )
        return [self._normalize_order_row(row) for row in rows]

    async def fetch_position_tpsl_history(
        self,
        symbol: str,
        *,
        is_finished: int | None = 1,
        start_time_ms: int | None = None,
        end_time_ms: int | None = None,
        page_size: int = 100,
        max_pages: int = 5,
    ) -> List[Dict[str, Any]]:
        """Return MEXC position TP/SL history in normalized base units.

        ``/stoporder/open_orders`` only shows still-open protection. Once a TP
        executes it disappears from that endpoint, so monitoring must use the
        durable stop-order list to confirm ``state=3`` and ``realityVol``.
        This method is intentionally read-only and paginated because a user may
        have many historical TP/SL rows for the same contract.
        """
        target = _normalize_contract_symbol(symbol)
        if not target:
            return []
        info = await self._instrument_info_or_default(target)
        size = max(1, min(100, int(page_size or 100)))
        pages = max(1, min(10, int(max_pages or 1)))
        out: List[Dict[str, Any]] = []
        seen: set[tuple[Any, ...]] = set()

        for page in range(1, pages + 1):
            params: Dict[str, Any] = {
                "symbol": _to_mexc_symbol(target),
                "page_num": page,
                "page_size": size,
            }
            if is_finished is not None:
                params["is_finished"] = int(is_finished)
            if start_time_ms is not None and int(start_time_ms) > 0:
                params["start_time"] = int(start_time_ms)
            if end_time_ms is not None and int(end_time_ms) > 0:
                params["end_time"] = int(end_time_ms)

            response = await self._request(
                "GET",
                "/api/v1/private/stoporder/list/orders",
                params=params,
                auth=True,
            )
            page_rows = list(self._rows(response))
            raw_rows = [
                row
                for row in page_rows
                if _normalize_contract_symbol(row.get("symbol")) == target
            ]

            for row in raw_rows:
                contracts = self._float_value(row.get("vol"), 0.0)
                reality_contracts = self._float_value(
                    row.get("realityVol") or row.get("reality_vol"), 0.0
                )
                state = int(self._float_value(row.get("state"), 0.0))
                trigger_side = int(self._float_value(row.get("triggerSide"), 0.0))
                take_profit_price = self._float_value(row.get("takeProfitPrice"), 0.0)
                stop_loss_price = self._float_value(row.get("stopLossPrice"), 0.0)
                stop_order_id = clean_exchange_id(row.get("id"))
                delegated_order_id = clean_exchange_id(row.get("orderId"))
                place_order_id = clean_exchange_id(row.get("placeOrderId"))
                if stop_order_id or delegated_order_id or place_order_id:
                    dedupe_key = (
                        "ids",
                        stop_order_id,
                        delegated_order_id,
                        place_order_id,
                    )
                else:
                    # Defensive fallback for malformed/legacy rows that omit all
                    # order ids. Treat distinct position/price/time/volume rows as
                    # distinct instead of collapsing every such row into one.
                    dedupe_key = (
                        "fallback",
                        clean_exchange_id(row.get("positionId")),
                        str(row.get("positionType") or ""),
                        str(row.get("takeProfitPrice") or ""),
                        str(row.get("stopLossPrice") or ""),
                        str(row.get("updateTime") or row.get("createTime") or ""),
                        str(row.get("vol") or ""),
                        str(row.get("realityVol") or row.get("reality_vol") or ""),
                    )
                if dedupe_key in seen:
                    continue
                seen.add(dedupe_key)
                position_type = int(self._float_value(row.get("positionType"), 0.0))
                position_side = (
                    "LONG"
                    if position_type == 1
                    else "SHORT"
                    if position_type == 2
                    else ""
                )
                tp_executed = bool(
                    state == 3
                    and take_profit_price > 0
                    and stop_loss_price <= 0
                    and trigger_side in {0, 1}
                )
                out.append(
                    {
                        "stopOrderId": stop_order_id,
                        "orderId": delegated_order_id,
                        "placeOrderId": place_order_id,
                        "symbol": target,
                        "positionId": clean_exchange_id(row.get("positionId")),
                        "positionSide": position_side,
                        "state": state,
                        "triggerSide": trigger_side,
                        "isFinished": int(
                            self._float_value(row.get("isFinished"), 0.0)
                        ),
                        "takeProfitPrice": take_profit_price,
                        "stopLossPrice": stop_loss_price,
                        "qty": self._contracts_to_base(contracts, info)
                        if contracts > 0
                        else 0.0,
                        "filledQty": self._contracts_to_base(reality_contracts, info)
                        if reality_contracts > 0
                        else 0.0,
                        "contracts": contracts,
                        "filledContracts": reality_contracts,
                        "tpExecuted": tp_executed,
                        "errorCode": int(self._float_value(row.get("errorCode"), 0.0)),
                        "createTime": row.get("createTime"),
                        "updateTime": row.get("updateTime"),
                        "raw": row,
                    }
                )

            # Pagination must be based on the endpoint page length, not the
            # post-filtered symbol length. Some MEXC responses have returned
            # mixed rows despite a symbol parameter.
            if len(page_rows) < size:
                break

        return out

    def _normalize_order_row(self, row: Dict[str, Any]) -> Dict[str, Any]:
        # MEXC regular orders use numeric side 1/2/3/4. TP/SL rows instead
        # carry positionType (1 long, 2 short).
        side_code = row.get("side")
        position_side = ""
        is_close = False
        if side_code is not None:
            try:
                code_i = int(side_code)
            except (TypeError, ValueError):
                code_i = 0
            if code_i == self._SIDE_OPEN_LONG:
                position_side = "LONG"
            elif code_i == self._SIDE_CLOSE_LONG:
                position_side = "LONG"
                is_close = True
            elif code_i == self._SIDE_OPEN_SHORT:
                position_side = "SHORT"
            elif code_i == self._SIDE_CLOSE_SHORT:
                position_side = "SHORT"
                is_close = True
        elif row.get("positionType") is not None:
            try:
                pt = int(row.get("positionType"))
            except (TypeError, ValueError):
                pt = 0
            position_side = "LONG" if pt == 1 else "SHORT" if pt == 2 else ""
            is_close = bool(position_side)

        stop_loss_price = self._float_value(row.get("stopLossPrice"), 0.0)
        take_profit_price = self._float_value(row.get("takeProfitPrice"), 0.0)
        trigger_price = self._float_value(
            row.get("triggerPrice")
            or row.get("triggerprice")
            or row.get("stopPrice")
            or stop_loss_price
            or take_profit_price,
            0.0,
        )
        raw_type = row.get("orderType") or row.get("type") or ""
        if stop_loss_price > 0 and take_profit_price <= 0:
            normalized_type = "STOP_MARKET"
        elif take_profit_price > 0 and stop_loss_price <= 0:
            normalized_type = "TAKE_PROFIT_MARKET"
        elif stop_loss_price > 0 or take_profit_price > 0:
            normalized_type = "TPSL"
        else:
            normalized_type = str(raw_type).upper()

        sym = _normalize_contract_symbol(row.get("symbol"))
        info = self._instrument_cache.get(sym) or InstrumentInfo(sym)
        contracts = self._float_value(row.get("vol") or row.get("quantity"), 0.0)
        qty_base = self._contracts_to_base(contracts, info)

        # MEXC TP/SL responses expose two different identifiers:
        # - ``id`` / ``stopPlanOrderId`` is the stop-plan id accepted by
        #   POST /api/v1/private/stoporder/cancel;
        # - ``orderId`` may be a delegated regular order created only after the
        #   trigger fires.  Sending that delegated id to stoporder/cancel is
        #   unsafe and can make an exact cleanup silently miss the live plan.
        is_stop_plan = bool(
            row.get("positionType") is not None
            or stop_loss_price > 0
            or take_profit_price > 0
            or row.get("stopPlanOrderId") is not None
        )
        stop_plan_order_id = (
            clean_exchange_id(row.get("stopPlanOrderId") or row.get("id"))
            if is_stop_plan
            else ""
        )
        delegated_order_id = clean_exchange_id(row.get("orderId"))
        place_order_id = clean_exchange_id(row.get("placeOrderId"))
        return {
            # Keep ``orderId`` for compatibility with older readers, but expose
            # the exact plan identity separately and make it the preferred value
            # for TP/SL rows.
            "orderId": delegated_order_id or stop_plan_order_id,
            "stopPlanOrderId": stop_plan_order_id,
            "stopOrderId": stop_plan_order_id,
            "delegatedOrderId": delegated_order_id,
            "placeOrderId": place_order_id,
            "clientOrderId": clean_exchange_id(
                row.get("externalOid") or row.get("clientOrderId")
            ),
            "symbol": sym,
            "side": position_side.lower()
            if position_side
            else str(row.get("side") or "").lower(),
            "positionSide": position_side,
            "positionId": clean_exchange_id(row.get("positionId")),
            "type": normalized_type,
            "qty": qty_base,
            "contracts": contracts,
            "price": self._float_value(row.get("price"), 0.0),
            "triggerPrice": trigger_price,
            "stopPrice": trigger_price,
            "stopLossPrice": stop_loss_price,
            "takeProfitPrice": take_profit_price,
            "status": str(row.get("state") or row.get("status") or "").upper(),
            "reduceOnly": bool(row.get("reduceOnly")) or is_close,
            "raw": row,
        }

    # --- Cancel ------------------------------------------------------------

    async def cancel_all_open_orders(self, symbol: str | None = None) -> Dict[str, Any]:
        await self._ensure_hedge_mode()
        body: Dict[str, Any] = {}
        if symbol:
            body["symbol"] = _to_mexc_symbol(symbol)
        return await self._request(
            "POST",
            "/api/v1/private/order/cancel_all",
            body=body,
            auth=True,
        )

    async def cancel_all_conditional_orders(
        self,
        symbol: str | None = None,
        *,
        position_id: int | str | None = None,
    ) -> Dict[str, Any]:
        """Cancel position TP/SL plan orders.

        MEXC supports either ``positionId`` or ``symbol``. Prefer ``positionId``
        whenever it is known so a BE replacement cannot remove protection from
        another position on the same contract.
        """
        await self._ensure_hedge_mode()
        body: Dict[str, Any] = {}
        if position_id not in (None, ""):
            body["positionId"] = self._wire_exchange_id(
                position_id, field="MEXC positionId"
            )
        elif symbol:
            body["symbol"] = _to_mexc_symbol(symbol)
        else:
            raise MexcApiError(
                "cancel_all_conditional_orders requires position_id or symbol"
            )
        try:
            return await self._request(
                "POST",
                "/api/v1/private/stoporder/cancel_all",
                body=body,
                auth=True,
            )
        except MexcApiError as exc:
            # If no plan orders exist, MEXC sometimes errors; that's fine.
            txt = str(exc).lower()
            if "no order" in txt or "empty" in txt or "not exist" in txt:
                return {"success": True, "code": 0}
            raise

    # --- Public market data ------------------------------------------------

    async def fetch_api_trading_symbols(self, *, force: bool = False) -> set[str]:
        if not force and self._api_symbols_cache is not None:
            if time.time() - self._api_symbols_cache_ts < 3600:
                return set(self._api_symbols_cache)
        data = await self._request("GET", "/api/v1/contract/detail/country")
        symbols: set[str] = set()
        for row in self._rows(data):
            state = str(row.get("state") or "").lower()
            # 0 = Live trading, others restricted
            api_allowed = row.get("apiAllowed")
            if api_allowed is False:
                continue
            if state and state not in ("0", "live", "online", "enable"):
                continue
            sym = _normalize_contract_symbol(row.get("symbol"))
            if sym:
                symbols.add(sym)
        self._api_symbols_cache = symbols
        self._api_symbols_cache_ts = time.time()
        return set(symbols)

    async def instrument_info(self, symbol: str) -> InstrumentInfo:
        norm = _normalize_contract_symbol(symbol)
        cached = self._instrument_cache.get(norm)
        if cached is not None:
            return cached
        wire = _to_mexc_symbol(symbol)
        data = await self._request(
            "GET",
            "/api/v1/contract/detail/country",
            params={"symbol": wire},
        )
        rows = self._rows(data)
        if not rows:
            raise MexcSymbolNotSupported(
                f"{norm} не найдена на MEXC (contract/detail вернул пустой ответ)."
            )
        chosen: Optional[Dict[str, Any]] = None
        for r in rows:
            if _normalize_contract_symbol(r.get("symbol")) == norm:
                chosen = r
                break
        if chosen is None:
            raise MexcSymbolNotSupported(
                f"{norm} не найдена среди {len(rows)} контрактов MEXC."
            )
        # Refuse to trade if API is disabled for this pair
        if chosen.get("apiAllowed") is False:
            raise MexcSymbolNotSupported(
                f"🔐 Пара {norm} не разрешена для API-торговли на MEXC "
                "(apiAllowed=false). Это контракт только для UI-торговли."
            )
        info = self._extract_instrument_info(norm, chosen)
        self._instrument_cache[norm] = info
        return info

    @staticmethod
    def _extract_instrument_info(
        norm_symbol: str, row: Dict[str, Any]
    ) -> InstrumentInfo:
        # MEXC contract/detail fields:
        #   contractSize, priceScale, volScale, amountScale, priceUnit, volUnit,
        #   minVol, maxVol, maxLeverage, countryConfigContractMaxLeverage,
        #   apiAllowed
        contract_size_d = MexcAdapter._decimal_clean(row.get("contractSize"))
        if contract_size_d <= 0:
            contract_size_d = Decimal("1")

        # MEXC publishes volUnit/minVol in contracts. The shared risk engine
        # and every other adapter use base-asset units, so expose converted
        # quantities here and convert back to contracts only at write time.
        contract_step_d = MexcAdapter._decimal_clean(row.get("volUnit"))
        if contract_step_d <= 0:
            vol_scale = int(MexcAdapter._float_value(row.get("volScale"), 0))
            contract_step_d = (
                Decimal("1").scaleb(-vol_scale) if vol_scale > 0 else Decimal("1")
            )

        min_contracts_d = MexcAdapter._decimal_clean(row.get("minVol"))
        if min_contracts_d <= 0:
            min_contracts_d = contract_step_d
        qty_step = contract_step_d * contract_size_d
        min_qty = min_contracts_d * contract_size_d
        contract_size = float(contract_size_d)

        price_tick = MexcAdapter._float_value(row.get("priceUnit"), 0.0)
        if price_tick <= 0:
            price_scale = int(MexcAdapter._float_value(row.get("priceScale"), 4))
            price_tick = 10 ** (-price_scale) if price_scale >= 0 else 0.0001

        global_max_leverage = int(MexcAdapter._float_value(row.get("maxLeverage"), 0))
        country_max_leverage = int(
            MexcAdapter._float_value(row.get("countryConfigContractMaxLeverage"), 0)
        )
        if global_max_leverage > 0 and country_max_leverage > 0:
            max_leverage = min(global_max_leverage, country_max_leverage)
        elif country_max_leverage > 0:
            max_leverage = country_max_leverage
        else:
            max_leverage = global_max_leverage

        taker_fee_rate = MexcAdapter._float_value(row.get("takerFeeRate"), 0.0)
        stop_only_fair_raw = row.get("stopOnlyFair")
        stop_only_fair = stop_only_fair_raw is True or str(
            stop_only_fair_raw or ""
        ).strip().lower() in {"1", "true", "yes", "on"}

        return InstrumentInfo(
            symbol=norm_symbol,
            min_qty=float(min_qty),
            qty_step=float(qty_step),
            price_tick=float(price_tick),
            min_notional=0.0,  # MEXC enforces minVol in contracts
            max_leverage=int(max_leverage),
            contract_size=float(contract_size),
            taker_fee_rate=float(taker_fee_rate),
            stop_only_fair=bool(stop_only_fair),
        )

    # --- Leverage ----------------------------------------------------------

    async def set_margin_and_max_leverage(
        self,
        symbol: str,
        max_leverage: int,
        margin_mode: str = "cross",
        side: str = "long",
    ) -> int:
        """Set margin mode and leverage; return effective leverage.

        MEXC accepts both openType (1=isolated, 2=cross) and leverage in the
        same call (/position/change_leverage).  We set it for the requested
        position side and re-read to verify.
        """
        symbol = self._validated_write_symbol(symbol)
        side_l = self._validated_trade_side(side)
        margin_mode_l = str(margin_mode or "").strip().lower()
        if margin_mode_l not in {"cross", "isolated"}:
            raise MexcApiError(f"Неподдерживаемый режим маржи: {margin_mode!r}")
        if isinstance(max_leverage, bool):
            raise MexcApiError("Плечо должно быть положительным целым числом")
        try:
            leverage_value = float(max_leverage)
        except (TypeError, ValueError, OverflowError) as exc:
            raise MexcApiError(
                "Плечо должно быть положительным целым числом"
            ) from exc
        if (
            not math.isfinite(leverage_value)
            or leverage_value <= 0
            or leverage_value != int(leverage_value)
        ):
            raise MexcApiError("Плечо должно быть положительным целым числом")

        await self._ensure_hedge_mode()
        wire = _to_mexc_symbol(symbol)
        is_long = side_l == "long"
        open_type = (
            self._OPEN_TYPE_CROSS
            if margin_mode_l == "cross"
            else self._OPEN_TYPE_ISOLATED
        )
        target_lev = int(leverage_value)
        cache_key = (_normalize_contract_symbol(symbol), "long" if is_long else "short")

        # MEXC requires positionType for hedge mode change_leverage
        position_type = 1 if is_long else 2

        body = {
            "symbol": wire,
            "leverage": target_lev,
            "openType": open_type,
            "positionType": position_type,
        }
        change_error: MexcApiError | None = None
        try:
            await self._request(
                "POST",
                "/api/v1/private/position/change_leverage",
                body=body,
                auth=True,
            )
        except MexcApiError as exc:
            txt = str(exc).lower()
            if not ("no need" in txt or "same" in txt or "already" in txt):
                change_error = exc

        # Verify only the requested hedge side. The endpoint can return both
        # LONG and SHORT rows; accepting the first row can cache the opposite
        # side's leverage and later submit an entry with the wrong settings.
        try:
            verify = await self._request(
                "GET",
                "/api/v1/private/position/leverage",
                params={"symbol": wire},
                auth=True,
            )
            rows = self._rows(verify)
            candidates: list[Dict[str, Any]] = []
            undirected: list[Dict[str, Any]] = []
            for row in rows:
                row_side = ""
                raw_position_type = row.get("positionType")
                if raw_position_type is not None:
                    try:
                        position_type_value = int(raw_position_type)
                    except (TypeError, ValueError):
                        position_type_value = 0
                    row_side = (
                        "long"
                        if position_type_value == 1
                        else "short"
                        if position_type_value == 2
                        else ""
                    )
                if not row_side:
                    side_text = str(
                        row.get("positionSide") or row.get("holdSide") or ""
                    ).strip().lower()
                    if side_text in {"long", "short"}:
                        row_side = side_text
                if row_side == side_l:
                    candidates.append(row)
                elif not row_side:
                    undirected.append(row)

            # A single legacy response row without a side discriminator is
            # unambiguous. Multiple undirected rows are not.
            if not candidates and len(rows) == 1 and len(undirected) == 1:
                candidates = undirected

            for row in candidates:
                row_open_type = int(self._float_value(row.get("openType"), 0))
                if row_open_type in {1, 2} and row_open_type != open_type:
                    continue
                lev_value = self._float_value(row.get("leverage"), 0.0)
                if lev_value > 0 and float(lev_value).is_integer():
                    lev = int(lev_value)
                    self._position_open_type[cache_key] = (
                        row_open_type if row_open_type in {1, 2} else open_type
                    )
                    self._position_leverage[cache_key] = lev
                    return lev
        except Exception as exc:  # noqa: BLE001 - verification may be unavailable
            log.debug("MEXC leverage verify failed for %s: %s", symbol, exc)

        if change_error is not None:
            if isinstance(change_error, MexcNetworkAmbiguousError):
                # The verification read did not prove the requested setting.
                # Preserve ambiguity so the caller cannot treat this as a clean
                # rejection and immediately proceed with another write sequence.
                raise change_error
            raise MexcApiError(
                f"MEXC не подтвердила установку плеча {target_lev}x для {symbol}: "
                f"{change_error}"
            ) from change_error

        # The write was accepted but the optional read endpoint was unavailable
        # or ambiguous. Cache only now, after a successful change request.
        self._position_open_type[cache_key] = open_type
        self._position_leverage[cache_key] = target_lev
        return target_lev

    # --- Order creation ----------------------------------------------------

    async def _write_with_qty_step_retry(
        self,
        path: str,
        *,
        body: Dict[str, Any],
        symbol_for_step: Optional[str] = None,
        contract_size: float = 1.0,
        normalized_base_qty: float | None = None,
        native_qty_step: float | None = None,
    ) -> Dict[str, Any]:
        """POST with one precision-aware retry.

        MEXC returns/accepts ``vol`` in contracts. Metadata attached to the
        result is converted back to base units so the shared TP sizing and DB
        stay aligned with the real submitted position quantity.
        """
        del symbol_for_step  # retained in signature for backward compatibility

        def annotate(
            result: Dict[str, Any], base_qty: float | None, contract_step: float | None
        ) -> Dict[str, Any]:
            out = dict(result)
            if base_qty is not None and base_qty > 0:
                out["_normalized_quantity"] = float(base_qty)
            if contract_step is not None and contract_step > 0:
                out["_required_qty_step"] = self._contract_value_to_base(
                    contract_step, contract_size
                )
            return out

        try:
            result = await self._request("POST", path, body=body, auth=True)
            return annotate(result, normalized_base_qty, native_qty_step)
        except MexcNetworkAmbiguousError:
            raise
        except MexcSymbolNotSupported:
            raise
        except MexcApiError as exc:
            txt = str(exc)
            req_step = self._extract_required_qty_step(txt)
            qty_key = "vol" if "vol" in body else "quantity"
            if req_step and qty_key in body:
                old_qty = float(body[qty_key])
                new_qty = self._floor_qty_to_step(old_qty, req_step)
                if new_qty > 0 and new_qty != old_qty:
                    log.info(
                        "MEXC retry %s with %s %s -> %s (contract step=%s)",
                        path,
                        qty_key,
                        old_qty,
                        new_qty,
                        req_step,
                    )
                    body[qty_key] = self._fmt_num(new_qty, req_step)
                    result = await self._request("POST", path, body=body, auth=True)
                    return annotate(
                        result,
                        self._contract_value_to_base(new_qty, contract_size),
                        req_step,
                    )
            raise

    @staticmethod
    def _extract_required_qty_step(error_text: str) -> float | None:
        patterns = (
            r"precision[^0-9]+([0-9]+(?:\.[0-9]+)?)",
            r"(?:vol|quantity)[^0-9]+(?:step|unit)[^0-9]+([0-9]+(?:\.[0-9]+)?)",
        )
        for pattern in patterns:
            m = re.search(pattern, error_text, flags=re.IGNORECASE)
            if m:
                try:
                    return float(m.group(1))
                except ValueError:
                    continue
        return None

    async def create_entry_order_with_attached_stop(
        self,
        *,
        symbol: str,
        side: str,
        qty: float,
        entry: float,
        stop: float,
        order_type: str = "limit",
        take_profit: float | None = None,
        client_id: str | None = None,
    ) -> Dict[str, Any]:
        """Create an entry and guarantee STOP protection.

        LIMIT orders keep MEXC's native attached STOP because the entry price is
        concrete.  MARKET orders intentionally do *not* send ``stopLossPrice``
        together with ``price=0``: live MEXC rejects that combination with 5003.
        Instead the flow is fail-closed:

        1. snapshot the existing same-side position;
        2. submit MARKET without attached TP/SL fields;
        3. confirm fill via ``/order/get/{orderId}``;
        4. place and read-confirm a position STOP using the returned positionId;
        5. if STOP cannot be confirmed, close exactly the newly filled quantity
           and raise ``MexcMarketProtectionError``.
        """
        symbol = self._validated_write_symbol(symbol)
        side_l = self._validated_trade_side(side)
        order_type_l = str(order_type or "").strip().lower()
        if order_type_l not in {"limit", "market"}:
            raise MexcApiError(f"Неподдерживаемый тип входа: {order_type!r}")
        qty = self._require_positive_finite(qty, field="Entry qty")
        stop = self._require_positive_finite(stop, field="STOP price")
        if order_type_l == "limit":
            entry = self._require_positive_finite(entry, field="LIMIT entry price")
        elif not math.isfinite(float(entry or 0.0)):
            raise MexcApiError("MARKET reference entry должен быть конечным числом")
        if take_profit is not None:
            take_profit = self._require_positive_finite(
                take_profit, field="Take-profit price"
            )

        await self._ensure_hedge_mode()
        wire = _to_mexc_symbol(symbol)
        position_side = "LONG" if side_l == "long" else "SHORT"
        side_code = self._side_code_open(position_side)
        order_kind = (
            self._TYPE_MARKET if order_type_l == "market" else self._TYPE_LIMIT
        )

        info = await self.instrument_info(symbol)
        contracts, contract_step = self._base_to_contracts(qty, info)
        if contracts <= 0:
            raise MexcApiError(
                f"Размер позиции {qty} меньше минимального контрактного шага для {symbol}"
            )
        normalized_base_qty = self._contracts_to_base(contracts, info)
        cache_key = (_normalize_contract_symbol(symbol), side_l)
        leverage = int(self._position_leverage.get(cache_key) or info.max_leverage or 1)
        open_type = int(
            self._position_open_type.get(cache_key) or self._OPEN_TYPE_CROSS
        )

        # A baseline is required to prove that emergency rollback removed only the
        # quantity opened by this attempt. If the read fails, no MARKET write occurs.
        before_qty = 0.0
        if order_kind == self._TYPE_MARKET:
            before_qty = await self._position_size_for_side(symbol, side_l)

        body: Dict[str, Any] = {
            "symbol": wire,
            "side": side_code,
            "openType": open_type,
            "type": order_kind,
            "vol": self._fmt_num(contracts, contract_step),
            "leverage": max(1, leverage),
            "positionMode": self._POS_MODE_HEDGE,
            "externalOid": self._client_id("ent_", client_id),
        }

        stop_rounded = self._round_price_for_field(
            float(stop),
            info.price_tick,
            side=position_side,
            field="stopLoss",
        )

        if order_kind == self._TYPE_LIMIT:
            price_rounded = self._round_price_for_field(
                float(entry),
                info.price_tick,
                side=position_side,
                field="price",
            )
            body["price"] = self._fmt_num(price_rounded, info.price_tick)
            body["lossTrend"] = self._TRIGGER_TYPE_FAIR
            body["profitTrend"] = (
                self._TRIGGER_TYPE_FAIR
                if info.stop_only_fair
                else self._TRIGGER_TYPE_LAST
            )
            body["priceProtect"] = 0
            body["stopLossPrice"] = self._fmt_num(stop_rounded, info.price_tick)
            if take_profit is not None:
                tp_rounded = self._round_price_for_field(
                    float(take_profit),
                    info.price_tick,
                    side=position_side,
                    field="takeProfit",
                )
                body["takeProfitPrice"] = self._fmt_num(tp_rounded, info.price_tick)
            try:
                result = await self._write_with_qty_step_retry(
                    "/api/v1/private/order/create",
                    body=body,
                    contract_size=info.contract_size,
                    normalized_base_qty=normalized_base_qty,
                    native_qty_step=contract_step,
                )
            except MexcNetworkAmbiguousError as write_exc:
                external_oid = clean_exchange_id(body.get("externalOid"))
                try:
                    detail = await self._wait_order_by_external_id(
                        symbol=symbol,
                        external_oid=external_oid,
                    )
                except Exception as resolve_exc:
                    # Do not retry the create blindly and do not discard the only
                    # durable identity.  Persist a pending execution keyed by the
                    # immutable externalOid; the regular LIMIT reconciliation loop
                    # will keep querying the exact client id and will either recover
                    # the order/fill or close the row after repeated authoritative
                    # absence checks.
                    result = {
                        "success": False,
                        "code": 0,
                        "data": {"externalOid": external_oid},
                        "externalOid": external_oid,
                        "clientOrderId": external_oid,
                        "_write_ambiguous_unresolved": True,
                        "_write_ambiguous_reason": str(write_exc),
                        "_write_ambiguous_resolve_error": (
                            f"{type(resolve_exc).__name__}: {resolve_exc}"
                        ),
                        "_normalized_quantity": float(normalized_base_qty),
                        "_required_qty_step": self._contract_value_to_base(
                            contract_step, info.contract_size
                        ),
                    }
                    result["_entry_external_oid"] = external_oid
                    return result
                order_id = clean_exchange_id(detail.get("orderId") or detail.get("id"))
                if not order_id:
                    result = {
                        "success": False,
                        "code": 0,
                        "data": {"externalOid": external_oid},
                        "externalOid": external_oid,
                        "clientOrderId": external_oid,
                        "_write_ambiguous_unresolved": True,
                        "_write_ambiguous_reason": str(write_exc),
                        "_write_ambiguous_order_detail": detail,
                        "_normalized_quantity": float(normalized_base_qty),
                        "_required_qty_step": self._contract_value_to_base(
                            contract_step, info.contract_size
                        ),
                    }
                    result["_entry_external_oid"] = external_oid
                    return result
                result = {
                    "success": True,
                    "code": 0,
                    "data": {
                        "orderId": order_id,
                        "externalOid": external_oid,
                    },
                    "_write_ambiguous_resolved": True,
                    "_write_ambiguous_reason": str(write_exc),
                    "_write_ambiguous_order_detail": detail,
                    "_normalized_quantity": float(normalized_base_qty),
                    "_required_qty_step": self._contract_value_to_base(
                        contract_step, info.contract_size
                    ),
                }
            result = dict(result)
            result["_entry_external_oid"] = clean_exchange_id(
                body.get("externalOid")
            )
            return result

        # MARKET: price is required by MEXC, but attached STOP fields are omitted.
        body["price"] = "0"
        preconfirmed_fill: tuple[Dict[str, Any], float, float] | None = None
        try:
            entry_order = await self._write_with_qty_step_retry(
                "/api/v1/private/order/create",
                body=body,
                contract_size=info.contract_size,
                normalized_base_qty=normalized_base_qty,
                native_qty_step=contract_step,
            )
        except MexcNetworkAmbiguousError as write_exc:
            # Never let the shared ambiguous handler close a pre-existing same-side
            # position. Resolve this exact write by externalOid first.
            try:
                preconfirmed_fill = await self._wait_order_fill_by_external_id(
                    symbol=symbol,
                    external_oid=clean_exchange_id(body["externalOid"]),
                    info=info,
                )
                detail = preconfirmed_fill[0]
                entry_order = {
                    "success": True,
                    "code": 0,
                    "data": {"orderId": clean_exchange_id(detail.get("orderId"))},
                    "_write_ambiguous_resolved": True,
                    "_write_ambiguous_reason": str(write_exc),
                }
            except Exception as resolve_exc:
                # One final safe observation: only the positive delta above the
                # baseline can belong to this attempt. Do not close old exposure.
                try:
                    observed_position = await self._position_for_side(
                        symbol,
                        side_l,
                        attempts=8,
                        delay_sec=0.25,
                    )
                except Exception as observe_exc:
                    raise MexcMarketProtectionError(
                        "КРИТИЧЕСКИ: результат MEXC MARKET write неизвестен, "
                        "externalOid не удалось найти, а состояние позиции не удалось "
                        "прочитать. Проверьте MEXC вручную до отправки нового сигнала. "
                        f"Write: {write_exc}; resolve: {resolve_exc}; position read: {observe_exc}",
                        emergency_close_confirmed=False,
                    ) from write_exc
                observed_qty = (
                    float(observed_position.get("size") or 0.0)
                    if observed_position
                    else 0.0
                )
                delta_qty = max(0.0, observed_qty - before_qty)
                if delta_qty <= max(float(info.qty_step or 0.0) * 0.51, 1e-12):
                    raise MexcMarketProtectionError(
                        "КРИТИЧЕСКИ: результат MEXC MARKET write неизвестен и не удалось "
                        "подтвердить его по externalOid. Новая позиция пока не обнаружена, "
                        "но позднее исполнение исключить нельзя. Проверьте MEXC вручную. "
                        f"Write: {write_exc}; resolve: {resolve_exc}",
                        emergency_close_confirmed=False,
                    ) from write_exc
                synthetic = {
                    "positionId": observed_position.get("positionId"),
                    "dealVol": delta_qty / max(float(info.contract_size), 1e-30),
                    "dealAvgPrice": observed_position.get("entryPrice"),
                    "openType": observed_position.get("openType") or open_type,
                    "state": 3,
                }
                preconfirmed_fill = (
                    synthetic,
                    delta_qty,
                    self._float_value(observed_position.get("entryPrice"), 0.0),
                )
                entry_order = {
                    "success": True,
                    "code": 0,
                    "data": {"orderId": ""},
                    "_write_ambiguous_position_delta_resolved": True,
                    "_write_ambiguous_reason": str(write_exc),
                }

        order_detail: Dict[str, Any] = {}
        filled_qty = 0.0
        actual_entry = 0.0
        position_id: int | str | None = None
        protection_order: Optional[Dict[str, Any]] = None
        stop_error: Exception | None = None

        try:
            if preconfirmed_fill is not None:
                order_detail, filled_qty, actual_entry = preconfirmed_fill
            else:
                order_detail, filled_qty, actual_entry = await self._wait_order_fill(
                    entry_order, info
                )
            raw_position_id = clean_exchange_id(order_detail.get("positionId"))
            if raw_position_id:
                position_id = raw_position_id
            if position_id is None:
                position = await self._position_for_side(
                    symbol, side_l, attempts=8, delay_sec=0.25
                )
                if position:
                    raw_position_id = clean_exchange_id(position.get("positionId"))
                    if raw_position_id:
                        position_id = raw_position_id
            if position_id is None:
                raise MexcApiError(
                    "MEXC confirmed MARKET fill but returned no positionId"
                )
            if actual_entry > 0:
                if side_l == "long" and stop_rounded >= actual_entry:
                    raise MexcApiError(
                        f"MEXC MARKET LONG filled at {actual_entry}, but STOP {stop_rounded} "
                        "is no longer below the real entry"
                    )
                if side_l == "short" and stop_rounded <= actual_entry:
                    raise MexcApiError(
                        f"MEXC MARKET SHORT filled at {actual_entry}, but STOP {stop_rounded} "
                        "is no longer above the real entry"
                    )

            protection_order = await self.create_position_tpsl(
                symbol=symbol,
                side=side_l,
                qty=filled_qty,
                price=stop_rounded,
                kind="sl",
                position_id=position_id,
            )
            confirmed_stop = await self._confirm_position_stop(
                symbol=symbol,
                side=side_l,
                position_id=position_id,
                stop=stop_rounded,
                qty=filled_qty,
                info=info,
            )
            if not confirmed_stop:
                raise MexcApiError(
                    "MEXC accepted the post-fill STOP request but the STOP did not "
                    "appear in current TP/SL orders"
                )

            result = dict(entry_order)
            result["_normalized_quantity"] = float(filled_qty)
            result["_required_qty_step"] = self._contract_value_to_base(
                contract_step, info.contract_size
            )
            result["_market_order_detail"] = order_detail
            result["_post_fill_stop"] = {
                "confirmed": True,
                "positionId": position_id,
                "stop": stop_rounded,
                "qty": filled_qty,
                "order": protection_order,
                "verification": confirmed_stop,
            }
            if actual_entry > 0:
                # Shared executor understands this field and displays the real fill.
                result["avgFillPrice"] = actual_entry
            return result
        except Exception as exc:
            if (
                isinstance(exc, MexcNetworkAmbiguousError)
                and position_id is not None
                and filled_qty > 0
            ):
                confirmed_stop = await self._confirm_position_stop(
                    symbol=symbol,
                    side=side_l,
                    position_id=position_id,
                    stop=stop_rounded,
                    qty=filled_qty,
                    info=info,
                )
                if confirmed_stop:
                    result = dict(entry_order)
                    result["_normalized_quantity"] = float(filled_qty)
                    result["_required_qty_step"] = self._contract_value_to_base(
                        contract_step, info.contract_size
                    )
                    result["_market_order_detail"] = order_detail
                    result["_post_fill_stop"] = {
                        "confirmed": True,
                        "write_ambiguous_resolved": True,
                        "positionId": position_id,
                        "stop": stop_rounded,
                        "qty": filled_qty,
                        "order": protection_order,
                        "verification": confirmed_stop,
                    }
                    if actual_entry > 0:
                        result["avgFillPrice"] = actual_entry
                    return result
            stop_error = exc

        # STOP was not confirmed after a known MARKET fill. Roll back exactly the
        # newly filled quantity. If fill detail itself was not confirmed, use the
        # observed position delta as the only safe fallback.
        if filled_qty <= 0:
            order_id = self._extract_order_id(entry_order)
            no_fill_confirmed = False
            if order_id:
                await self._cancel_regular_order_best_effort(order_id)
                try:
                    response = await self._request(
                        "GET",
                        f"/api/v1/private/order/get/{order_id}",
                        auth=True,
                    )
                    detail = self._response_data_dict(response)
                    self._require_exact_order_detail_id(
                        detail,
                        order_id,
                        context="final order/get",
                    )
                    deal_contracts = self._float_value(detail.get("dealVol"), 0.0)
                    state = int(self._float_value(detail.get("state"), 0))
                    if deal_contracts > 0:
                        order_detail = detail
                        filled_qty = self._contracts_to_base(deal_contracts, info)
                        actual_entry = self._float_value(
                            detail.get("dealAvgPrice") or detail.get("price"),
                            0.0,
                        )
                        raw_position_id = clean_exchange_id(detail.get("positionId"))
                        if raw_position_id:
                            position_id = raw_position_id
                    elif state in (4, 5):
                        no_fill_confirmed = True
                except Exception as detail_exc:
                    stop_error = MexcApiError(
                        f"{stop_error}; final order read failed: {detail_exc}"
                    )
            if no_fill_confirmed:
                raise MexcApiError(
                    f"MEXC MARKET order {order_id} was canceled/invalid with zero fill; no position opened"
                )
            try:
                current_qty = await self._position_size_for_side(symbol, side_l)
                observed_delta = max(0.0, current_qty - before_qty)
                if filled_qty <= 0:
                    filled_qty = observed_delta
                position = await self._position_for_side(
                    symbol, side_l, attempts=2, delay_sec=0.25
                )
                if position:
                    raw_position_id = clean_exchange_id(position.get("positionId"))
                    if raw_position_id:
                        position_id = raw_position_id
                    if actual_entry <= 0:
                        actual_entry = self._float_value(
                            position.get("entryPrice"), 0.0
                        )
            except Exception as observe_exc:
                stop_error = MexcApiError(
                    f"{stop_error}; position delta lookup failed: {observe_exc}"
                )

        close_order: Optional[Dict[str, Any]] = None
        close_confirmed = False
        close_error: Exception | None = None
        if filled_qty > 0:
            try:
                close_order = await self.emergency_close_market(
                    symbol=symbol,
                    side=side_l,
                    qty=filled_qty,
                    client_id=self._client_id("rb_", client_id),
                    position_id=position_id,
                    open_type=open_type,
                )
                try:
                    _, closed_qty, _ = await self._wait_order_fill(close_order, info)
                    tolerance = max(float(info.qty_step or 0.0) * 0.51, 1e-12)
                    close_confirmed = closed_qty + tolerance >= filled_qty
                except Exception as detail_exc:
                    close_error = detail_exc
                # Position-size confirmation also resolves an ambiguous close response.
                if not close_confirmed:
                    close_confirmed = await self._confirm_position_not_above(
                        symbol=symbol,
                        side=side_l,
                        maximum_qty=before_qty,
                        info=info,
                    )
            except Exception as exc:
                close_error = exc
                close_confirmed = await self._confirm_position_not_above(
                    symbol=symbol,
                    side=side_l,
                    maximum_qty=before_qty,
                    info=info,
                )

        if close_confirmed:
            message = (
                "MEXC MARKET-вход был исполнен, но STOP не удалось подтвердить. "
                "Бот аварийно закрыл весь новый объём и подтвердил возврат позиции "
                f"к исходному размеру. Причина STOP: {stop_error}"
            )
        else:
            message = (
                "КРИТИЧЕСКИ: MEXC MARKET-вход мог быть исполнен, STOP не подтверждён, "
                "а аварийное закрытие нового объёма не удалось подтвердить. "
                "Немедленно проверь позицию на бирже вручную. "
                f"Причина STOP: {stop_error}; причина rollback: {close_error}"
            )
        raise MexcMarketProtectionError(
            message,
            emergency_close_confirmed=close_confirmed,
            entry_order=entry_order,
            protection_order=protection_order,
            emergency_close_order=close_order,
            position_id=position_id,
            opened_qty=filled_qty,
            actual_entry=actual_entry,
        )

    @staticmethod
    def _conditional_order_id(order: Dict[str, Any]) -> str:
        raw = order.get("raw") if isinstance(order.get("raw"), dict) else {}
        return clean_exchange_id(
            order.get("stopPlanOrderId")
            or order.get("stopOrderId")
            or raw.get("stopPlanOrderId")
            or raw.get("id")
        )

    @staticmethod
    def _is_open_take_profit_order(order: Dict[str, Any]) -> bool:
        typ = str(order.get("type") or "").upper()
        tp_price = MexcAdapter._float_value(
            order.get("takeProfitPrice")
            or order.get("triggerPrice")
            or order.get("stopPrice"),
            0.0,
        )
        return tp_price > 0 and (
            "TAKE_PROFIT" in typ
            or typ in {"TP", "TPSL"}
            or MexcAdapter._float_value(order.get("takeProfitPrice"), 0.0) > 0
        )

    @staticmethod
    def _tp_order_matches_side(order: Dict[str, Any], position_side: str) -> bool:
        wanted = str(position_side or "").upper()
        row_position_side = str(order.get("positionSide") or "").upper()
        if row_position_side:
            return row_position_side == wanted
        row_side = str(order.get("side") or "").upper()
        if not row_side:
            return True
        # Normalised MEXC stop orders expose LONG/SHORT. Raw/generic adapters may
        # expose the close order direction SELL for long and BUY for short.
        accepted = {wanted}
        accepted.add("SELL" if wanted == "LONG" else "BUY")
        return row_side in accepted

    async def create_take_profit(
        self,
        *,
        symbol: str,
        side: str,
        qty: float,
        price: float,
        client_id: str | None = None,
    ) -> Dict[str, Any]:
        from app.services.workload_manager import (
            PRIORITY_TP,
            mexc_request_context,
        )

        with mexc_request_context(
            priority=PRIORITY_TP,
            label="take_profit",
        ):
            return await self._create_take_profit_impl(
                symbol=symbol,
                side=side,
                qty=qty,
                price=price,
                client_id=client_id,
            )

    async def _create_take_profit_impl(
        self,
        *,
        symbol: str,
        side: str,
        qty: float,
        price: float,
        client_id: str | None = None,
    ) -> Dict[str, Any]:
        """Create one TP idempotently.

        MEXC's position TP/SL endpoint has no client order id. Therefore a retry
        after a restart, timeout or DB write failure can otherwise submit the same
        target again. Before every write we read the live position TP set and:

        * return the existing TP when the same position + side + target is present;
        * never let aggregate open TP quantity exceed the live position quantity;
        * serialise TP writes for the same account/position inside this process.

        This is the central guard used by MARKET entry, LIMIT catch-up, recovery
        and break-even TP recreation.
        """
        symbol = self._validated_write_symbol(symbol)
        requested_qty = self._require_positive_finite(qty, field="TP qty")
        price = self._require_positive_finite(price, field="TP price")
        side_l = self._validated_trade_side(side)

        await self._ensure_hedge_mode()
        position_side = "LONG" if side_l == "long" else "SHORT"
        info = await self.instrument_info(symbol)
        # Lock before resolving the live position. MARKET execution originally
        # launches TP writes concurrently; resolving positionId outside the lock
        # would burst the private position endpoint and still leave a race.
        lock_seed = (
            f"{self._api_key}|{_normalize_contract_symbol(symbol)}|{position_side}"
        )
        lock_key = hashlib.sha256(lock_seed.encode("utf-8")).hexdigest()
        lock = await _get_tp_write_lock(lock_key)

        async with lock:
            # PostgreSQL advisory lock also protects the short old/new Railway
            # deployment overlap. Import lazily to keep the adapter module free of
            # a top-level database dependency.
            from app.database import db as _db

            async with _db.distributed_advisory_lock(f"mexc-tp:{lock_key}"):
                return await self._create_take_profit_under_lock(
                    symbol=symbol,
                    side=side,
                    requested_qty=requested_qty,
                    price=price,
                    client_id=client_id,
                    position_side=position_side,
                    info=info,
                )

    async def _create_take_profit_under_lock(
        self,
        *,
        symbol: str,
        side: str,
        requested_qty: float,
        price: float,
        client_id: str | None,
        position_side: str,
        info: InstrumentInfo,
    ) -> Dict[str, Any]:
        # Resolve the current live position only after both local and distributed
        # locks are held.
        position = await self._position_for_side(symbol, position_side.lower())
        if not position or not clean_exchange_id(position.get("positionId")):
            raise MexcApiError(
                f"MEXC не вернула positionId для {symbol} {position_side}; "
                "TP не отправлен, чтобы не создать неподтверждённый ордер."
            )
        position_id = self._validated_exchange_id(
            position.get("positionId"), field="MEXC positionId"
        )
        position_qty = self._float_value(position.get("size"), 0.0)
        if position_qty <= 0:
            raise MexcApiError(
                f"MEXC вернула нулевой размер позиции для {symbol} {position_side}; "
                "TP write aborted for safety."
            )
        target = self._round_price_for_field(
            float(price), info.price_tick, side=position_side, field="tpPrice"
        )
        price_tolerance = max(
            float(info.price_tick or 0.0) * 0.51, abs(target) * 1e-9, 1e-12
        )
        # ``qty_step`` is already expressed in base-asset units by
        # ``_extract_instrument_info``. Multiplying it by contractSize again
        # makes the tolerance wrong for every contractSize != 1.
        qty_step = max(float(info.qty_step or 0.0), 0.0)
        # Existing and requested TP quantities must match by an executable lot,
        # not merely within half a lot. A 0.51-step tolerance can accept a
        # corrupted/manual half-lot quantity as idempotent and hide uncovered
        # or over-covered exposure. Keep only a microscopic float/API noise
        # allowance here; broader half-step tolerances remain appropriate only
        # for fill-state comparisons elsewhere.
        qty_tolerance = max(
            qty_step * 1e-6,
            abs(requested_qty) * 1e-9,
            abs(position_qty) * 1e-9,
            1e-12,
        )

        open_algo = await self.fetch_open_algo_orders(symbol)
        position_tps: List[Dict[str, Any]] = []
        unidentified_same_side_tps: List[Dict[str, Any]] = []
        for order in open_algo:
            if not isinstance(order, dict) or not self._is_open_take_profit_order(order):
                continue
            if not self._tp_order_matches_side(order, position_side):
                continue
            row_pid = clean_exchange_id(order.get("positionId"))
            if not row_pid:
                unidentified_same_side_tps.append(order)
                continue
            if row_pid != position_id:
                continue
            position_tps.append(order)
        if unidentified_same_side_tps:
            raise MexcTpCoverageError(
                f"Found {len(unidentified_same_side_tps)} open TP order(s) for "
                f"{_normalize_contract_symbol(symbol)} {position_side} without an exact "
                "positionId. Bot will not adopt or add TP until ownership is resolved."
            )

        aggregate_tp_qty = sum(
            max(0.0, self._float_value(order.get("qty"), 0.0)) for order in position_tps
        )

        matching_orders: List[Dict[str, Any]] = []
        for existing in position_tps:
            existing_target = self._float_value(
                existing.get("takeProfitPrice")
                or existing.get("triggerPrice")
                or existing.get("stopPrice"),
                0.0,
            )
            if existing_target > 0 and abs(existing_target - target) <= price_tolerance:
                matching_orders.append(existing)

        # An already over-covered position or duplicate same-price targets must
        # never be reported as an idempotent success. That would hide stale TP
        # duplicates and let the execution move to ``protected`` while the live
        # exchange state is unsafe.
        if aggregate_tp_qty > position_qty + qty_tolerance:
            raise MexcTpCoverageError(
                f"Open TP quantity {aggregate_tp_qty:.12g} exceeds live position "
                f"{position_qty:.12g} for {_normalize_contract_symbol(symbol)}. "
                "Existing duplicate/stale TP orders require manual cleanup."
            )
        if len(matching_orders) > 1:
            raise MexcTpCoverageError(
                f"Found {len(matching_orders)} open TP orders at target {target} for "
                f"{_normalize_contract_symbol(symbol)} position {position_id}. "
                "Duplicate same-price TP orders require manual cleanup."
            )

        matching_existing = matching_orders[0] if matching_orders else None
        matching_existing_qty = (
            max(0.0, self._float_value(matching_existing.get("qty"), 0.0))
            if matching_existing is not None
            else 0.0
        )

        # A matching target is idempotent only when its live quantity matches
        # the planned slice. MEXC updates same-price quantities asynchronously;
        # trying to "repair" a mismatched quantity and immediately placing the
        # next target can over-allocate the position before the update is visible.
        # Fail closed instead and require cleanup of stale/manual TP quantities.
        if matching_existing is not None:
            matching_existing_id = self._conditional_order_id(matching_existing)
            if not matching_existing_id:
                raise MexcTpCoverageError(
                    f"Existing TP {target} for {_normalize_contract_symbol(symbol)} has no "
                    "exact stop-plan id; it cannot be adopted as bot-owned."
                )
            if abs(matching_existing_qty - requested_qty) > qty_tolerance:
                raise MexcTpCoverageError(
                    f"Existing TP {target} for {_normalize_contract_symbol(symbol)} has "
                    f"qty {matching_existing_qty:.12g}, expected {requested_qty:.12g}. "
                    "Same-price quantity mismatch is not auto-modified because MEXC "
                    "applies that update asynchronously."
                )
            result = dict(matching_existing)
            result["_idempotent_existing"] = True
            result["_existing_quantity"] = matching_existing_qty
            result["_normalized_quantity"] = matching_existing_qty
            result["_requested_quantity"] = requested_qty
            log.warning(
                "MEXC TP duplicate suppressed: %s %s position=%s target=%s existing_order=%s",
                _normalize_contract_symbol(symbol),
                position_side,
                position_id,
                target,
                matching_existing.get("orderId"),
            )
            return result
        else:
            available_qty = max(0.0, position_qty - aggregate_tp_qty)
            if available_qty <= qty_tolerance:
                # Never report a missing signal target as successfully created.
                # v1.6.2 returned a synthetic success here, causing callers to
                # journal nonexistent TPs and mark the trade protected.
                raise MexcTpCoverageError(
                    f"TP {target} for {_normalize_contract_symbol(symbol)} was not created: "
                    f"existing TP quantity {aggregate_tp_qty:.12g} already covers live "
                    f"position {position_qty:.12g}. Manual/stale TP conflict must be checked."
                )
            safe_qty = min(requested_qty, available_qty)
            safe_qty = self._floor_qty_to_step(safe_qty, qty_step)
            if safe_qty <= 0:
                raise MexcTpCoverageError(
                    f"TP {target} for {_normalize_contract_symbol(symbol)} was not created: "
                    f"remaining uncovered quantity {available_qty:.12g} is below qty step "
                    f"{qty_step:.12g}."
                )

        if safe_qty + qty_tolerance < requested_qty:
            log.warning(
                "MEXC TP qty capped to remaining uncovered position: requested=%s safe=%s "
                "existing=%s position=%s %s %s",
                requested_qty,
                safe_qty,
                aggregate_tp_qty,
                position_qty,
                _normalize_contract_symbol(symbol),
                position_side,
            )

        result = await self.create_position_tpsl(
            symbol=symbol,
            side=side,
            qty=safe_qty,
            price=target,
            kind="tp",
            client_id=client_id,
            position_id=position_id,
        )
        if not isinstance(result, dict):
            raise MexcNetworkAmbiguousError(
                f"MEXC returned an invalid TP response for {_normalize_contract_symbol(symbol)} "
                f"target {target}; write outcome must be verified before continuing."
            )
        result.setdefault("_requested_quantity", requested_qty)
        if safe_qty != requested_qty:
            result["_tp_quantity_capped"] = True
            result["_normalized_quantity"] = min(
                self._float_value(result.get("_normalized_quantity"), safe_qty),
                safe_qty,
            )

        # Keep the advisory lock until the accepted TP is visible in MEXC's
        # open stop-order list. This closes the old/new deployment race where
        # process B could acquire the lock after process A's POST but before the
        # read endpoint reflected the new order. Never return a successful TP to
        # callers until the exact target and quantity are confirmed live.
        confirmed: Dict[str, Any] | None = None
        last_confirmation_error = ""
        # MEXC documents that same-price TP quantity updates are asynchronous,
        # and in live traffic a newly accepted TP can take several seconds to
        # appear in the open TP/SL list. Keep the distributed lock until a
        # longer confirmation window expires so another worker cannot submit a
        # duplicate while the read model is still catching up.
        for attempt in range(12):
            if attempt:
                await asyncio.sleep(min(2.0, 0.25 * attempt))
            try:
                rows = await self.fetch_open_algo_orders(symbol)
            except Exception as exc:
                last_confirmation_error = f"{type(exc).__name__}: {exc}"
                log.warning(
                    "MEXC TP confirmation read failed for %s target=%s: %s",
                    _normalize_contract_symbol(symbol),
                    target,
                    exc,
                )
                continue
            for order in rows:
                if not isinstance(order, dict) or not self._is_open_take_profit_order(
                    order
                ):
                    continue
                if clean_exchange_id(order.get("positionId")) != position_id:
                    continue
                if not self._tp_order_matches_side(order, position_side):
                    continue
                existing_target = self._float_value(
                    order.get("takeProfitPrice")
                    or order.get("triggerPrice")
                    or order.get("stopPrice"),
                    0.0,
                )
                confirmed_qty = self._float_value(order.get("qty"), 0.0)
                if (
                    existing_target > 0
                    and abs(existing_target - target) <= price_tolerance
                    and abs(confirmed_qty - safe_qty) <= qty_tolerance
                ):
                    if not self._conditional_order_id(order):
                        last_confirmation_error = "matching TP row has no exact stop-plan id"
                        continue
                    confirmed = order
                    break
            if confirmed is not None:
                break

        if confirmed is None:
            raise MexcNetworkAmbiguousError(
                f"MEXC TP write for {_normalize_contract_symbol(symbol)} target {target} "
                f"was not confirmed in open TP/SL orders; no success was recorded. "
                f"Last confirmation error: {last_confirmation_error or 'order not visible'}"
            )

        confirmed_stop_plan_id = self._conditional_order_id(confirmed)
        if not confirmed_stop_plan_id:
            raise MexcNetworkAmbiguousError(
                f"MEXC TP for {_normalize_contract_symbol(symbol)} target {target} is visible "
                "without an exact stop-plan id; success was not recorded."
            )
        result["_tp_open_confirmed"] = True
        result["_confirmed_stop_plan_id"] = confirmed_stop_plan_id
        # Compatibility alias for existing snapshots. New code must prefer
        # ``_confirmed_stop_plan_id`` for conditional-order cancellation.
        result["_confirmed_order_id"] = confirmed_stop_plan_id
        result["_confirmed_position_id"] = clean_exchange_id(
            confirmed.get("positionId")
        )
        result["_confirmed_take_profit_price"] = self._float_value(
            confirmed.get("takeProfitPrice") or confirmed.get("triggerPrice") or target,
            target,
        )
        result["_normalized_quantity"] = self._float_value(
            confirmed.get("qty"), safe_qty
        )
        return result

    @staticmethod
    def _normalized_position_tpsl_values(
        *,
        info: InstrumentInfo,
        side_l: str,
        qty: float,
        price: float,
        is_tp: bool,
    ) -> Dict[str, float]:
        """Return the exact price/quantity that MEXC will receive for TP/SL.

        Services which persist a durable pre-write intent must use these values,
        not the unrounded strategy calculation.  Otherwise a valid conditional
        accepted at the exchange tick can fail exact post-write confirmation.
        """

        contracts, contract_step = MexcAdapter._base_to_contracts(qty, info)
        if contracts <= 0:
            raise MexcApiError(
                f"TP/SL qty {qty} меньше минимального контрактного шага для {info.symbol}"
            )
        position_side = "LONG" if side_l == "long" else "SHORT"
        price_rounded = MexcAdapter._round_price_for_field(
            float(price),
            info.price_tick,
            side=position_side,
            field="tpPrice" if is_tp else "stopLoss",
        )
        normalized_qty = MexcAdapter._contracts_to_base(contracts, info)
        if not math.isfinite(price_rounded) or price_rounded <= 0:
            raise MexcApiError("TP/SL price после округления MEXC недопустима")
        if not math.isfinite(normalized_qty) or normalized_qty <= 0:
            raise MexcApiError("TP/SL qty после округления MEXC недопустим")
        return {
            "price": float(price_rounded),
            "qty": float(normalized_qty),
            "contracts": float(contracts),
            "contract_step": float(contract_step),
            "price_tick": float(info.price_tick or 0.0),
            "qty_step": float(info.qty_step or 0.0),
        }

    async def normalize_position_tpsl_request(
        self,
        *,
        symbol: str,
        side: str,
        qty: float,
        price: float,
        kind: str,
    ) -> Dict[str, float]:
        """Prepare the exact exchange-normalized TP/SL values without a write."""

        symbol = self._validated_write_symbol(symbol)
        side_l = self._validated_trade_side(side)
        qty = self._require_positive_finite(qty, field="TP/SL qty")
        price = self._require_positive_finite(price, field="TP/SL price")
        kind_l = str(kind or "").strip().lower()
        tp_kinds = {"tp", "take_profit", "takeprofit", "tp_market", "take-profit"}
        sl_kinds = {"sl", "stop", "stop_loss", "stoploss", "stop-loss"}
        if kind_l not in tp_kinds | sl_kinds:
            raise MexcApiError(f"Неподдерживаемый тип TP/SL: {kind!r}")
        info = await self.instrument_info(symbol)
        return self._normalized_position_tpsl_values(
            info=info,
            side_l=side_l,
            qty=qty,
            price=price,
            is_tp=kind_l in tp_kinds,
        )

    @staticmethod
    def _stop_plan_id_from_write_response(response: Any) -> str:
        """Extract the exact conditional plan id from stoporder/place."""

        if not isinstance(response, dict):
            return ""
        data = response.get("data")
        data_dict = data if isinstance(data, dict) else {}
        for value in (
            response.get("_confirmed_stop_plan_id"),
            response.get("stopPlanOrderId"),
            response.get("stopOrderId"),
            data_dict.get("stopPlanOrderId"),
            data_dict.get("stopOrderId"),
            data_dict.get("id"),
            data if not isinstance(data, (dict, list)) else None,
        ):
            cleaned = clean_exchange_id(value)
            if cleaned:
                return cleaned
        return ""

    async def create_position_tpsl(
        self,
        *,
        symbol: str,
        side: str,
        qty: float,
        price: float,
        kind: str,
        client_id: str | None = None,
        position_id: int | str | None = None,
    ) -> Dict[str, Any]:
        from app.services.workload_manager import (
            PRIORITY_STOP,
            PRIORITY_TP,
            mexc_request_context,
        )

        kind_l = str(kind or "").lower()
        is_tp = kind_l in {
            "tp",
            "take_profit",
            "takeprofit",
            "tp_market",
            "take-profit",
        }
        with mexc_request_context(
            priority=PRIORITY_TP if is_tp else PRIORITY_STOP,
            label="take_profit" if is_tp else "stop_protection",
        ):
            return await self._create_position_tpsl_impl(
                symbol=symbol,
                side=side,
                qty=qty,
                price=price,
                kind=kind,
                client_id=client_id,
                position_id=position_id,
            )

    async def _create_position_tpsl_impl(
        self,
        *,
        symbol: str,
        side: str,
        qty: float,
        price: float,
        kind: str,
        client_id: str | None = None,
        position_id: int | str | None = None,
    ) -> Dict[str, Any]:
        """Create a standalone reduce-only TP or SL on an existing position.

        Current MEXC API requires ``positionId`` and contract ``vol`` on
        ``POST /api/v1/private/stoporder/place``.  We resolve the live position
        after fill rather than submitting the removed legacy endpoint/fields.
        """
        del client_id  # Current TP/SL-by-position endpoint has no externalOid.
        symbol = self._validated_write_symbol(symbol)
        side_l = self._validated_trade_side(side)
        qty = self._require_positive_finite(qty, field="TP/SL qty")
        price = self._require_positive_finite(price, field="TP/SL price")
        kind_l = str(kind or "").strip().lower()
        tp_kinds = {"tp", "take_profit", "takeprofit", "tp_market", "take-profit"}
        sl_kinds = {"sl", "stop", "stop_loss", "stoploss", "stop-loss"}
        if kind_l not in tp_kinds | sl_kinds:
            raise MexcApiError(f"Неподдерживаемый тип TP/SL: {kind!r}")

        await self._ensure_hedge_mode()
        position_side = "LONG" if side_l == "long" else "SHORT"
        info = await self.instrument_info(symbol)
        normalized = self._normalized_position_tpsl_values(
            info=info,
            side_l=side_l,
            qty=qty,
            price=price,
            is_tp=kind_l in tp_kinds,
        )
        contracts = float(normalized["contracts"])
        contract_step = float(normalized["contract_step"])
        normalized_qty = float(normalized["qty"])
        price_rounded = float(normalized["price"])

        if position_id is None:
            position = await self._position_for_side(symbol, position_side.lower())
            if not position:
                raise MexcApiError(
                    f"MEXC не вернула positionId для {symbol} {position_side}; "
                    "TP/SL не отправлен, чтобы не создать неподтверждённую защиту."
                )
            position_id = position.get("positionId")
        position_id = self._wire_exchange_id(
            position_id, field="MEXC positionId"
        )

        is_tp = kind_l in tp_kinds

        body: Dict[str, Any] = {
            "positionId": position_id,
            "vol": self._fmt_num(contracts, contract_step),
            "lossTrend": self._TRIGGER_TYPE_FAIR,
            "profitTrend": (
                self._TRIGGER_TYPE_FAIR
                if info.stop_only_fair
                else self._TRIGGER_TYPE_LAST
            ),
            "priceProtect": 0,
            "profitLossVolType": "SAME",
            "volType": 1,
            "takeProfitReverse": 2,
            "stopLossReverse": 2,
            "takeProfitType": 0,
            "takeProfitOrderPrice": 0,
            "stopLossType": 0,
            "stopLossOrderPrice": 0,
        }
        if is_tp:
            body["takeProfitPrice"] = self._fmt_num(price_rounded, info.price_tick)
        else:
            body["stopLossPrice"] = self._fmt_num(price_rounded, info.price_tick)

        result = await self._write_with_qty_step_retry(
            "/api/v1/private/stoporder/place",
            body=body,
            contract_size=info.contract_size,
            normalized_base_qty=normalized_qty,
            native_qty_step=contract_step,
        )
        out = dict(result)
        out["_normalized_price"] = float(price_rounded)
        out.setdefault("_normalized_quantity", float(normalized_qty))
        out["_confirmed_position_id"] = clean_exchange_id(position_id)
        confirmed_plan_id = self._stop_plan_id_from_write_response(out)
        if confirmed_plan_id:
            out["_confirmed_stop_plan_id"] = confirmed_plan_id
        return out

    async def set_position_stop_loss(
        self,
        *,
        symbol: str,
        side: str,
        qty: float,
        stop: float,
        client_id: str | None = None,
        position_id: int | str | None = None,
    ) -> Dict[str, Any]:
        return await self.create_position_tpsl(
            symbol=symbol,
            side=side,
            qty=qty,
            price=stop,
            kind="sl",
            client_id=client_id,
            position_id=position_id,
        )

    async def emergency_close_market(
        self,
        *,
        symbol: str,
        side: str,
        qty: float,
        client_id: str | None = None,
        position_id: int | str | None = None,
        open_type: int | None = None,
    ) -> Dict[str, Any]:
        from app.services.workload_manager import (
            PRIORITY_EMERGENCY,
            mexc_request_context,
        )

        with mexc_request_context(
            priority=PRIORITY_EMERGENCY,
            label="emergency_close",
        ):
            return await self._emergency_close_market_impl(
                symbol=symbol,
                side=side,
                qty=qty,
                client_id=client_id,
                position_id=position_id,
                open_type=open_type,
            )

    async def _emergency_close_market_impl(
        self,
        *,
        symbol: str,
        side: str,
        qty: float,
        client_id: str | None = None,
        position_id: int | str | None = None,
        open_type: int | None = None,
    ) -> Dict[str, Any]:
        symbol = self._validated_write_symbol(symbol)
        side_l = self._validated_trade_side(side)
        qty = self._require_positive_finite(qty, field="Emergency close qty")

        await self._ensure_hedge_mode()
        wire = _to_mexc_symbol(symbol)
        position_side = "LONG" if side_l == "long" else "SHORT"
        side_code = self._side_code_close(position_side)
        info = await self.instrument_info(symbol)
        contracts, contract_step = self._base_to_contracts(qty, info)
        if contracts <= 0:
            raise MexcApiError(
                f"Close qty {qty} меньше минимального контрактного шага для {symbol}"
            )

        cache_key = (_normalize_contract_symbol(symbol), position_side.lower())
        resolved_open_type = int(
            open_type
            or self._position_open_type.get(cache_key)
            or self._OPEN_TYPE_CROSS
        )
        position: Optional[Dict[str, Any]] = None
        try:
            position = await self._position_for_side(
                symbol, position_side.lower(), attempts=1
            )
        except Exception as exc:  # emergency close must still be attempted
            log.warning("MEXC emergency position lookup failed for %s: %s", symbol, exc)
        if (
            position
            and int(position.get("openType") or 0) in (1, 2)
            and open_type is None
        ):
            resolved_open_type = int(position["openType"])

        body: Dict[str, Any] = {
            "symbol": wire,
            "side": side_code,
            "openType": resolved_open_type,
            "type": self._TYPE_MARKET,
            "price": "0",
            "vol": self._fmt_num(contracts, contract_step),
            "positionMode": self._POS_MODE_HEDGE,
            "externalOid": self._client_id("emer_", client_id),
        }
        resolved_position_id = (
            position_id if position_id not in (None, "") else (
                position.get("positionId") if position else None
            )
        )
        if position_id not in (None, "") and not clean_exchange_id(position_id):
            raise MexcApiError("MEXC positionId отсутствует или повреждён")
        if resolved_position_id not in (None, ""):
            body["positionId"] = self._wire_exchange_id(
                resolved_position_id, field="MEXC positionId"
            )

        return await self._write_with_qty_step_retry(
            "/api/v1/private/order/create",
            body=body,
            contract_size=info.contract_size,
            normalized_base_qty=self._contracts_to_base(contracts, info),
            native_qty_step=contract_step,
        )

    async def emergency_close_market_confirmed(
        self,
        *,
        symbol: str,
        side: str,
        qty: float,
        client_id: str | None = None,
        position_id: int | str | None = None,
        open_type: int | None = None,
    ) -> Dict[str, Any]:
        from app.services.workload_manager import (
            PRIORITY_EMERGENCY,
            mexc_request_context,
        )

        with mexc_request_context(
            priority=PRIORITY_EMERGENCY,
            label="emergency_close_confirm",
        ):
            return await self._emergency_close_market_confirmed_impl(
                symbol=symbol,
                side=side,
                qty=qty,
                client_id=client_id,
                position_id=position_id,
                open_type=open_type,
            )

    async def _emergency_close_market_confirmed_impl(
        self,
        *,
        symbol: str,
        side: str,
        qty: float,
        client_id: str | None = None,
        position_id: int | str | None = None,
        open_type: int | None = None,
    ) -> Dict[str, Any]:
        """Close a position slice and do not report success until it is proven.

        ``/order/create`` only acknowledges the write.  A timeout can also happen
        after MEXC accepted the order.  This helper resolves the deterministic
        ``externalOid``, waits for the fill and independently confirms that the
        live position decreased by the submitted quantity.  Callers must use this
        method whenever their database state would become ``closed`` or a TP
        market-close would be journalled as completed.
        """
        requested_qty = self._require_positive_finite(
            qty, field=f"Close qty for {symbol}"
        )

        info = await self.instrument_info(symbol)
        contracts, contract_step = self._base_to_contracts(requested_qty, info)
        normalized_qty = self._contracts_to_base(contracts, info)
        if normalized_qty <= 0:
            raise MexcApiError(
                f"Close qty {requested_qty} меньше минимального контрактного шага для {symbol}"
            )

        before_qty: float | None = None
        try:
            before_qty = await self._position_size_for_side(symbol, side)
        except Exception as exc:
            log.warning("MEXC pre-close position read failed for %s: %s", symbol, exc)

        order: Dict[str, Any] | None = None
        write_error: Exception | None = None
        try:
            order = await self.emergency_close_market(
                symbol=symbol,
                side=side,
                qty=normalized_qty,
                client_id=client_id,
                position_id=position_id,
                open_type=open_type,
            )
        except MexcNetworkAmbiguousError as exc:
            write_error = exc
            external_oid = self._client_id("emer_", client_id)
            try:
                detail, filled_qty, _ = await self._wait_order_fill_by_external_id(
                    symbol=symbol,
                    external_oid=external_oid,
                    info=info,
                )
                order = dict(detail)
                order["_normalized_quantity"] = filled_qty
                order["_resolved_after_ambiguous_write"] = True
            except Exception as resolve_exc:
                write_error = MexcNetworkAmbiguousError(
                    f"{exc}; externalOid resolution failed: {resolve_exc}"
                )

        fill_confirmed = False
        filled_qty = 0.0
        avg_fill_price = 0.0
        fill_error: Exception | None = None
        if order is not None:
            try:
                _detail, filled_qty, avg_fill_price = await self._wait_order_fill(
                    order, info
                )
                fill_confirmed = (
                    filled_qty + max(float(info.qty_step or 0.0) * 0.51, 1e-12)
                    >= normalized_qty
                )
            except Exception as exc:
                fill_error = exc

        position_confirmed = False
        maximum_remaining: float | None = None
        if before_qty is not None:
            maximum_remaining = max(0.0, float(before_qty) - float(normalized_qty))
            position_confirmed = await self._confirm_position_not_above(
                symbol=symbol,
                side=side,
                maximum_qty=maximum_remaining,
                info=info,
            )

        if not fill_confirmed and not position_confirmed:
            raise MexcNetworkAmbiguousError(
                f"MEXC emergency close for {_normalize_contract_symbol(symbol)} was not confirmed; "
                f"requested={normalized_qty:.12g}, before={before_qty}, "
                f"expected_remaining<={maximum_remaining}, write_error={write_error}, "
                f"fill_error={fill_error}. Check the live position manually before retrying."
            )

        result = dict(order or {})
        result["_close_confirmed"] = True
        result["_close_confirmed_via"] = (
            "order_fill+position"
            if fill_confirmed and position_confirmed
            else "order_fill"
            if fill_confirmed
            else "position_size"
        )
        result["_requested_quantity"] = requested_qty
        result["_normalized_quantity"] = normalized_qty
        result["_position_before"] = before_qty
        result["_maximum_remaining"] = maximum_remaining
        result["_filled_quantity"] = filled_qty
        result["_avg_fill_price"] = float(avg_fill_price or 0.0)
        if write_error is not None:
            result["_write_ambiguity"] = str(write_error)
        return result

    # --- Price feeds -------------------------------------------------------

    @staticmethod
    def _ticker_prices(row: Dict[str, Any]) -> Dict[str, float]:
        return {
            "last": MexcAdapter._float_value(row.get("lastPrice"), 0.0),
            "fair": MexcAdapter._float_value(row.get("fairPrice"), 0.0),
            "index": MexcAdapter._float_value(row.get("indexPrice"), 0.0),
        }

    async def fetch_market_prices_bulk(
        self,
        symbols: List[str] | set[str] | tuple[str, ...] | None = None,
    ) -> Dict[str, Dict[str, float]]:
        """Fetch all requested ticker prices with one public MEXC request.

        The ticker endpoint allows an omitted symbol and then returns the full
        contract list.  One batch call per cycle stays below the public endpoint
        limit even when several symbols are active.
        """
        wanted = {
            _normalize_contract_symbol(symbol)
            for symbol in (symbols or [])
            if _normalize_contract_symbol(symbol)
        }
        data = await self._request("GET", "/api/v1/contract/ticker")
        out: Dict[str, Dict[str, float]] = {}
        for row in self._rows(data):
            symbol = _normalize_contract_symbol(row.get("symbol"))
            if not symbol or (wanted and symbol not in wanted):
                continue
            prices = self._ticker_prices(row)
            if any(value > 0 for value in prices.values()):
                out[symbol] = prices
        if wanted and not out:
            raise MexcApiError(
                f"MEXC bulk ticker has no requested prices: {sorted(wanted)}"
            )
        return out

    async def fetch_stop_only_fair_map(
        self,
        symbols: List[str] | set[str] | tuple[str, ...] | None = None,
    ) -> Dict[str, bool]:
        """Return per-contract TP/SL fair-price restriction in one request."""
        wanted = {
            _normalize_contract_symbol(symbol)
            for symbol in (symbols or [])
            if _normalize_contract_symbol(symbol)
        }
        data = await self._request("GET", "/api/v1/contract/detail/country")
        out: Dict[str, bool] = {}
        for row in self._rows(data):
            symbol = _normalize_contract_symbol(row.get("symbol"))
            if not symbol or (wanted and symbol not in wanted):
                continue
            raw = row.get("stopOnlyFair")
            out[symbol] = bool(
                raw is True
                or str(raw or "").strip().lower() in {"1", "true", "yes", "on"}
            )
        return out

    async def fetch_market_prices(self, symbol: str) -> Dict[str, float]:
        """Return MEXC latest/fair/index prices from one ticker request.

        TP orders in this project trigger on latest price, while STOP orders
        trigger on fair price.  Keeping both values avoids false or delayed
        event-driven verification caused by using one reference for everything.
        """
        wire = _to_mexc_symbol(symbol)
        data = await self._request(
            "GET",
            "/api/v1/contract/ticker",
            params={"symbol": wire},
        )
        payload: Any = data.get("data") if isinstance(data, dict) else None
        row: Dict[str, Any] = {}
        if isinstance(payload, dict):
            row = payload
        elif isinstance(payload, list):
            wanted = _normalize_contract_symbol(symbol)
            for item in payload:
                if (
                    isinstance(item, dict)
                    and _normalize_contract_symbol(item.get("symbol")) == wanted
                ):
                    row = item
                    break
        prices = self._ticker_prices(row)
        if not any(value > 0 for value in prices.values()):
            raise MexcApiError(f"MEXC ticker has no usable price for {symbol}: {data}")
        return prices

    async def fetch_last_price(self, symbol: str) -> float:
        # MARKET sizing, LIMIT-entry events and TP triggers follow latest price.
        # The ticker call also returns fair/index values and is the cheapest
        # account-independent source for the event-driven monitor.
        try:
            prices = await self.fetch_market_prices(symbol)
            for key in ("last", "fair", "index"):
                if float(prices.get(key) or 0.0) > 0:
                    return float(prices[key])
        except MexcApiError as exc:
            log.debug("MEXC ticker failed for %s: %s", symbol, exc)

        wire = _to_mexc_symbol(symbol)
        # Conservative compatibility fallback for temporary ticker issues.
        for path, field in (
            (f"/api/v1/contract/fair_price/{wire}", "fairPrice"),
            (f"/api/v1/contract/index_price/{wire}", "indexPrice"),
        ):
            try:
                data = await self._request("GET", path)
                payload = data.get("data") if isinstance(data, dict) else None
                if isinstance(payload, dict):
                    price = self._float_value(
                        payload.get(field) or payload.get("price"), 0.0
                    )
                    if price > 0:
                        return price
            except MexcApiError as exc:
                log.debug(
                    "MEXC price fallback failed path=%s symbol=%s: %s",
                    path,
                    symbol,
                    exc,
                )

        raise MexcApiError(f"MEXC: цена для {symbol} не найдена")

    async def fetch_price(self, symbol: str) -> float:
        return await self.fetch_last_price(symbol)

    async def verify_api(self) -> bool:
        """Quick check that the API key is valid (fetch balance).

        Returns True on success.  On failure raises MexcApiError with the
        actual MEXC response so the caller can show a useful message
        ("API key expired", "KYC required", etc.) instead of just "rejected".
        """
        await self.fetch_balance_details()
        return True


__all__ = [
    "MexcAdapter",
    "MexcApiError",
    "MexcNetworkAmbiguousError",
    "MexcExchangeRejected",
    "MexcTpCoverageError",
    "MexcMarketProtectionError",
    "MexcSymbolNotSupported",
    "InstrumentInfo",
    "close_shared_http_client",
]
