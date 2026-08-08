"""Fail-open shadow ingress for source-signal analytics.

Stage ``g5b3g1`` extends the shadow store with passive public-price tracking.
It never calls BingX itself, mutates executions, acquires execution locks, or
calculates strategy performance on the price hot path. Telegram handlers and
price tracking use ``put_nowait`` so an analytics outage or full queue can never
delay or reject a trade.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import math
import re
import time
import uuid
from dataclasses import dataclass, replace
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from typing import Any, Iterable

from app.config import get_settings
from app.database.db import connect, is_postgres, monitor_db_workload
from app.services.bingx_contract_aliases import bingx_1000_alias
from app.services.models import Signal
from app.services.signal_parser import parse_vip_signal
from app.services.signal_analytics_tracker import (
    AnalyticsTrackingTransition,
    acknowledge_signal_analytics_recovery_quarantine,
    acknowledge_signal_analytics_transitions,
    quarantine_signal_analytics_transitions,
    observe_signal_analytics_prices,
    refresh_signal_analytics_registry,
    signal_analytics_recovery_quarantine_ids,
    signal_analytics_tracker_stats,
    signal_analytics_tracking_symbols,
    write_signal_analytics_recovery_marks,
    write_signal_analytics_transitions,
)
from app.services.statistics_gap_recovery import (
    recover_statistics_restart_gap_once,
    requeue_statistics_restart_gap_after_tracker_refresh_failure,
)
from app.services.statistics_linkage import (
    assign_active_period_to_ingest_events,
    reconcile_statistics_linkage,
)
from app.services.statistics_quality_gate import refresh_statistics_quality_gates

log = logging.getLogger(__name__)

_MAX_RAW_TEXT_CHARS = 8_192
_MAX_SOURCE_TITLE_CHARS = 256
_MAX_STRATEGY_CHARS = 96
_MAX_TIMEFRAME_CHARS = 16
_MAX_SIGNAL_ID_CHARS = 128
_MAX_SOURCE_FORMAT_CHARS = 128
_RATE_LIMIT_BACKOFF_MAX_SEC = 30.0
_INT64_MIN = -(2**63)
_INT64_MAX = 2**63 - 1

_TIMEFRAME_RE = re.compile(r"(?i)(?<![A-Z0-9])(\d{1,3}\s*[MHDW])(?![A-Z0-9])")
_HEADER_STRATEGY_RE = re.compile(
    r"(?im)^\s*[^\n]*#\s*[A-Z0-9]{1,30}USDT\b[^\n|]*\|\s*([^\n]{1,160})$"
)
_LEVERAGE_RE = re.compile(r"(?i)\bleverage\s*:\s*(\d{1,3})\s*x\b")
_BE_INSTRUCTION_RE = re.compile(
    r"(?is)(?:after\s+(?:reaching\s+)?(?:the\s+)?"
    r"(?P<english>first|second|third|[123](?:st|nd|rd)?)\s+target|"
    r"after\s+tp\s*(?P<tp>[123])|"
    r"после[^\n]{0,80}(?:tp|тп)\s*(?P<ru_tp>[123])|"
    r"после[^\n]{0,80}(?P<russian>перв(?:ой|ого)|втор(?:ой|ого)|"
    r"треть(?:ей|его))\s+цел)"
    r"[^\n]{0,160}(?:break[ -]?even|breakeven|безубыт|б/?у)"
)


def _extract_be_trigger_tp_index(raw_text: str) -> int:
    match = _BE_INSTRUCTION_RE.search(str(raw_text or ""))
    if not match:
        return 0
    for group in ("tp", "ru_tp"):
        value = match.groupdict().get(group)
        if value:
            return int(value)
    word = str(
        match.groupdict().get("english")
        or match.groupdict().get("russian")
        or ""
    ).lower()
    if word.startswith(("first", "1", "перв")):
        return 1
    if word.startswith(("second", "2", "втор")):
        return 2
    if word.startswith(("third", "3", "трет")):
        return 3
    return 0



@dataclass(frozen=True, slots=True)
class AnalyticsSignalEnvelope:
    """Primitive, immutable payload admitted to the bounded shadow queue."""

    ingest_event_id: str
    content_fingerprint: str
    source_chat_id: int
    source_message_id: int | None
    source_title: str | None
    sender_chat_id: int | None
    sender_chat_title: str | None
    published_at: datetime
    signal_id_text: str | None
    symbol: str
    side: str
    order_type: str
    source_format: str
    entry_reference: str
    stop_price: str
    targets: tuple[str, ...]
    target_percents: tuple[str, ...]
    raw_text: str


@dataclass(frozen=True, slots=True)
class NormalizedAnalyticsSignal:
    ingest_event_id: str
    dedup_key: str
    content_fingerprint: str
    source_chat_id: int
    source_message_id: int | None
    source_title: str | None
    sender_chat_id: int | None
    sender_chat_title: str | None
    published_at: datetime
    signal_id_text: str | None
    symbol: str
    side: str
    order_type: str
    source_format: str
    timeframe: str | None
    strategy: str | None
    source_leverage: int | None
    entry_low: str
    entry_high: str
    entry_reference: str
    stop_price: str
    targets_json: str
    target_percents_json: str
    raw_text: str
    dedup_window_seconds: int
    initial_status: str
    tracking_started_at: datetime | None
    expiry_at: datetime | None
    be_trigger_tp_index: int


@dataclass(frozen=True, slots=True)
class AnalyticsLinkageRequest:
    """Primitive non-blocking request handled by the analytics DB worker."""

    kind: str
    entity_id: int


def _bounded_text(value: Any, limit: int) -> str | None:
    # PostgreSQL text parameters reject NUL bytes. Telegram normally does not
    # deliver them, but analytics must never let one malformed trusted message
    # poison an entire retrying batch. Replace them before queue admission.
    text = str(value or "").replace("\x00", " ").strip()
    if not text:
        return None
    return text[: max(1, int(limit))]


def _int64(value: Any, *, field: str, optional: bool = False) -> int | None:
    if value is None and optional:
        return None
    if isinstance(value, bool):
        raise ValueError(f"{field} must be an int64")
    try:
        numeric = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise ValueError(f"{field} must be an int64") from exc
    if not numeric.is_finite() or numeric != numeric.to_integral_value():
        raise ValueError(f"{field} must be an int64")
    parsed = int(numeric)
    if parsed < _INT64_MIN or parsed > _INT64_MAX:
        raise ValueError(f"{field} is outside int64 range")
    return parsed


def _decimal_text(value: Any, *, positive: bool = True) -> str:
    if isinstance(value, bool):
        raise ValueError("boolean is not a signal price")
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise ValueError("signal price is not decimal") from exc
    if not parsed.is_finite() or (positive and parsed <= 0):
        raise ValueError("signal price must be finite and positive")
    # Fixed-point storage avoids binary-float JSON and scientific-notation
    # inconsistencies while preserving very small tokens such as FLOKI.
    rendered = format(parsed, "f")
    if "." in rendered:
        rendered = rendered.rstrip("0").rstrip(".")
    return rendered or "0"


def _utc_datetime(value: Any) -> datetime:
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, (int, float)) and math.isfinite(float(value)):
        parsed = datetime.fromtimestamp(float(value), tz=timezone.utc)
    else:
        parsed = datetime.now(timezone.utc)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def build_analytics_signal_envelope(
    signal: Signal,
    *,
    content_fingerprint: str,
    source_chat_id: int,
    source_message_id: int | None,
    source_title: str | None,
    sender_chat_id: int | None,
    sender_chat_title: str | None,
    published_at: datetime | None,
) -> AnalyticsSignalEnvelope:
    """Copy a parsed signal into a bounded primitive payload.

    This function performs no I/O and does not reparse Telegram text.  The
    optional metadata extraction is intentionally deferred to the DB worker.
    """

    fingerprint = str(content_fingerprint or "").strip().lower()
    if not re.fullmatch(r"[0-9a-f]{32,128}", fingerprint):
        raise ValueError("content_fingerprint must be a hexadecimal digest")

    symbol = str(signal.symbol or "").strip().upper()
    side = str(getattr(signal.side, "value", signal.side) or "").strip().lower()
    order_type = str(signal.order_type or "LIMIT").strip().upper()
    if not symbol or side not in {"long", "short"} or order_type not in {
        "LIMIT",
        "MARKET",
    }:
        raise ValueError("invalid normalized signal identity")

    normalized_source_chat_id = int(
        _int64(source_chat_id, field="source_chat_id", optional=False)
    )
    normalized_source_message_id = _int64(
        source_message_id, field="source_message_id", optional=True
    )
    if normalized_source_message_id is not None:
        # Telegram may redeliver the same update after reconnect/redeploy. A
        # deterministic observation identity prevents that transport replay
        # from inflating duplicate_count, while two separately posted copies
        # still have different message IDs and remain measurable duplicates.
        ingest_event_id = hashlib.sha256(
            (
                f"telegram|{normalized_source_chat_id}|"
                f"{normalized_source_message_id}|{fingerprint}"
            ).encode("utf-8")
        ).hexdigest()
    else:
        ingest_event_id = uuid.uuid4().hex

    raw_text = str(signal.raw_text or "").replace("\x00", " ")[:_MAX_RAW_TEXT_CHARS]
    return AnalyticsSignalEnvelope(
        ingest_event_id=ingest_event_id,
        content_fingerprint=fingerprint,
        source_chat_id=normalized_source_chat_id,
        source_message_id=normalized_source_message_id,
        source_title=_bounded_text(source_title, _MAX_SOURCE_TITLE_CHARS),
        sender_chat_id=_int64(sender_chat_id, field="sender_chat_id", optional=True),
        sender_chat_title=_bounded_text(
            sender_chat_title, _MAX_SOURCE_TITLE_CHARS
        ),
        published_at=_utc_datetime(published_at),
        signal_id_text=_bounded_text(signal.signal_id, _MAX_SIGNAL_ID_CHARS),
        symbol=symbol,
        side=side,
        order_type=order_type,
        source_format=(
            _bounded_text(signal.source_format, _MAX_SOURCE_FORMAT_CHARS)
            or "unknown"
        ),
        entry_reference=_decimal_text(signal.entry, positive=(order_type != "MARKET")),
        stop_price=_decimal_text(signal.stop),
        targets=tuple(_decimal_text(value) for value in signal.targets),
        target_percents=tuple(
            _decimal_text(value) for value in (signal.target_percents or [])
        ),
        raw_text=raw_text,
    )


def _extract_shadow_metadata(
    envelope: AnalyticsSignalEnvelope,
) -> tuple[str, str, str | None, str | None, int | None, int]:
    entry_reference = Decimal(envelope.entry_reference)
    entry_low = entry_reference
    entry_high = entry_reference
    raw = envelope.raw_text

    # Reuse the already hardened parser only in the low-priority DB worker.  A
    # metadata parse failure leaves the executable midpoint as both zone edges;
    # it can never affect the original trading Signal.
    if raw:
        try:
            parsed = parse_vip_signal(raw)
            low = parsed.entry_zone_low
            high = parsed.entry_zone_high
            if parsed.ok and low is not None and high is not None:
                low_d = Decimal(str(low))
                high_d = Decimal(str(high))
                parsed_symbol = str(
                    getattr(getattr(parsed, "plan", None), "symbol", "") or ""
                ).upper()
                if bingx_1000_alias(parsed_symbol) == envelope.symbol:
                    low_d *= Decimal("1000")
                    high_d *= Decimal("1000")
                if low_d.is_finite() and high_d.is_finite() and 0 < low_d <= high_d:
                    entry_low, entry_high = low_d, high_d
        except Exception:
            # Shadow enrichment is best-effort; core prices remain available.
            pass

    timeframe = None
    match = _TIMEFRAME_RE.search(raw)
    if match:
        timeframe = re.sub(r"\s+", "", match.group(1)).upper()[:_MAX_TIMEFRAME_CHARS]

    strategy = None
    match = _HEADER_STRATEGY_RE.search(raw)
    if match:
        strategy = _bounded_text(match.group(1), _MAX_STRATEGY_CHARS)

    source_leverage = None
    match = _LEVERAGE_RE.search(raw)
    if match:
        parsed_leverage = int(match.group(1))
        if 1 <= parsed_leverage <= 500:
            source_leverage = parsed_leverage

    be_trigger_tp_index = _extract_be_trigger_tp_index(raw)
    allow_zero_entry = envelope.order_type == "MARKET"
    return (
        _decimal_text(entry_low, positive=not allow_zero_entry),
        _decimal_text(entry_high, positive=not allow_zero_entry),
        timeframe,
        strategy,
        source_leverage,
        be_trigger_tp_index,
    )


def normalize_analytics_signal(
    envelope: AnalyticsSignalEnvelope,
    *,
    dedup_window_hours: int,
    tracking_enabled: bool = False,
    expiry_hours: int = 24,
) -> NormalizedAnalyticsSignal:
    published = _utc_datetime(envelope.published_at)
    window_seconds = max(3600, int(dedup_window_hours) * 3600)
    # Each transport observation gets a deterministic candidate key. The DB
    # writer then resolves it against a true sliding time window. Keeping the
    # candidate observation-specific avoids fixed-bucket boundary splits and
    # prevents config-window changes from colliding with an unrelated old row.
    dedup_material = (
        f"{envelope.source_chat_id}|{envelope.content_fingerprint}|"
        f"{envelope.ingest_event_id}"
    ).encode("utf-8")
    dedup_key = hashlib.sha256(dedup_material).hexdigest()

    (
        entry_low,
        entry_high,
        timeframe,
        strategy,
        leverage,
        be_trigger_tp_index,
    ) = _extract_shadow_metadata(envelope)
    tracking_started_at = datetime.now(timezone.utc) if tracking_enabled else None
    expiry_at = (
        published + timedelta(hours=max(1, int(expiry_hours)))
        if tracking_enabled
        else None
    )
    return NormalizedAnalyticsSignal(
        ingest_event_id=envelope.ingest_event_id,
        dedup_key=dedup_key,
        content_fingerprint=envelope.content_fingerprint,
        source_chat_id=envelope.source_chat_id,
        source_message_id=envelope.source_message_id,
        source_title=envelope.source_title,
        sender_chat_id=envelope.sender_chat_id,
        sender_chat_title=envelope.sender_chat_title,
        published_at=published,
        signal_id_text=envelope.signal_id_text,
        symbol=envelope.symbol,
        side=envelope.side,
        order_type=envelope.order_type,
        source_format=envelope.source_format,
        timeframe=timeframe,
        strategy=strategy,
        source_leverage=leverage,
        entry_low=entry_low,
        entry_high=entry_high,
        entry_reference=envelope.entry_reference,
        stop_price=envelope.stop_price,
        targets_json=json.dumps(list(envelope.targets), separators=(",", ":")),
        target_percents_json=json.dumps(
            list(envelope.target_percents), separators=(",", ":")
        ),
        raw_text=envelope.raw_text,
        dedup_window_seconds=window_seconds,
        initial_status="waiting_entry" if tracking_enabled else "shadow_received",
        tracking_started_at=tracking_started_at,
        expiry_at=expiry_at,
        be_trigger_tp_index=be_trigger_tp_index,
    )


_PG_RESOLVE_DEDUP = """
SELECT dedup_key
FROM signal_analytics_signals
WHERE source_chat_id=$1
  AND content_fingerprint=$2
  AND $3 >= last_seen_at - ($4 * INTERVAL '1 second')
  AND $3 <= published_at + ($4 * INTERVAL '1 second')
ORDER BY CASE
           WHEN $3 < published_at
             THEN EXTRACT(EPOCH FROM (published_at - $3))
           WHEN $3 > last_seen_at
             THEN EXTRACT(EPOCH FROM ($3 - last_seen_at))
           ELSE 0
         END ASC,
         published_at ASC, id ASC
LIMIT 1
FOR UPDATE
"""

_SQLITE_RESOLVE_DEDUP = """
SELECT dedup_key
FROM signal_analytics_signals
WHERE source_chat_id=?
  AND content_fingerprint=?
  AND julianday(?) >= julianday(last_seen_at) - (? / 86400.0)
  AND julianday(?) <= julianday(published_at) + (? / 86400.0)
ORDER BY CASE
           WHEN julianday(?) < julianday(published_at)
             THEN (julianday(published_at) - julianday(?)) * 86400.0
           WHEN julianday(?) > julianday(last_seen_at)
             THEN (julianday(?) - julianday(last_seen_at)) * 86400.0
           ELSE 0
         END ASC,
         published_at ASC, id ASC
LIMIT 1
"""


_PG_UPSERT = """
INSERT INTO signal_analytics_signals(
  ingest_event_id,dedup_key,content_fingerprint,
  source_chat_id,first_source_message_id,last_source_message_id,source_title,
  sender_chat_id,sender_chat_title,published_at,last_seen_at,
  signal_id_text,symbol,side,order_type,source_format,timeframe,strategy,
  source_leverage,entry_low,entry_high,entry_reference,stop_price,
  targets_json,target_percents_json,target_percents_source,raw_text,status,duplicate_count,
  tracking_started_at,expiry_at,be_trigger_tp_index,state_version,
  created_at,updated_at
) VALUES(
  $1,$2,$3,$4,$5,$5,$6,$7,$8,$9,$9,$10,$11,$12,$13,$14,$15,$16,$17,
  $18,$19,$20,$21,$22,$23,$24,$25,$26,1,$27,$28,$29,0,NOW(),NOW()
)
ON CONFLICT(dedup_key) DO UPDATE SET
  duplicate_count = CASE
    WHEN signal_analytics_signals.ingest_event_id = EXCLUDED.ingest_event_id
      THEN signal_analytics_signals.duplicate_count
    ELSE signal_analytics_signals.duplicate_count + 1
  END,
  ingest_event_id = CASE
    WHEN EXCLUDED.last_seen_at > signal_analytics_signals.last_seen_at
      OR (
        EXCLUDED.last_seen_at = signal_analytics_signals.last_seen_at
        AND EXCLUDED.last_source_message_id IS NOT NULL
        AND (
          signal_analytics_signals.last_source_message_id IS NULL
          OR EXCLUDED.last_source_message_id >= signal_analytics_signals.last_source_message_id
        )
      )
      THEN EXCLUDED.ingest_event_id
    ELSE signal_analytics_signals.ingest_event_id
  END,
  first_source_message_id = CASE
    WHEN EXCLUDED.published_at < signal_analytics_signals.published_at
      OR (
        EXCLUDED.published_at = signal_analytics_signals.published_at
        AND EXCLUDED.first_source_message_id IS NOT NULL
        AND (
          signal_analytics_signals.first_source_message_id IS NULL
          OR EXCLUDED.first_source_message_id < signal_analytics_signals.first_source_message_id
        )
      )
      THEN COALESCE(
        EXCLUDED.first_source_message_id,
        signal_analytics_signals.first_source_message_id
      )
    ELSE signal_analytics_signals.first_source_message_id
  END,
  last_source_message_id = CASE
    WHEN EXCLUDED.last_seen_at > signal_analytics_signals.last_seen_at
      OR (
        EXCLUDED.last_seen_at = signal_analytics_signals.last_seen_at
        AND EXCLUDED.last_source_message_id IS NOT NULL
        AND (
          signal_analytics_signals.last_source_message_id IS NULL
          OR EXCLUDED.last_source_message_id >= signal_analytics_signals.last_source_message_id
        )
      )
      THEN COALESCE(
        EXCLUDED.last_source_message_id,
        signal_analytics_signals.last_source_message_id
      )
    ELSE signal_analytics_signals.last_source_message_id
  END,
  published_at = LEAST(
    signal_analytics_signals.published_at,
    EXCLUDED.published_at
  ),
  last_seen_at = GREATEST(
    signal_analytics_signals.last_seen_at,
    EXCLUDED.last_seen_at
  ),
  expiry_at = CASE
    WHEN signal_analytics_signals.expiry_at IS NULL THEN EXCLUDED.expiry_at
    WHEN EXCLUDED.expiry_at IS NULL THEN signal_analytics_signals.expiry_at
    ELSE LEAST(signal_analytics_signals.expiry_at, EXCLUDED.expiry_at)
  END,
  updated_at = NOW()
RETURNING id,duplicate_count
"""

_SQLITE_UPSERT = """
INSERT INTO signal_analytics_signals(
  ingest_event_id,dedup_key,content_fingerprint,
  source_chat_id,first_source_message_id,last_source_message_id,source_title,
  sender_chat_id,sender_chat_title,published_at,last_seen_at,
  signal_id_text,symbol,side,order_type,source_format,timeframe,strategy,
  source_leverage,entry_low,entry_high,entry_reference,stop_price,
  targets_json,target_percents_json,target_percents_source,raw_text,status,duplicate_count,
  tracking_started_at,expiry_at,be_trigger_tp_index,state_version,
  created_at,updated_at
) VALUES(
  ?1,?2,?3,?4,?5,?5,?6,?7,?8,?9,?9,?10,?11,?12,?13,?14,?15,?16,?17,
  ?18,?19,?20,?21,?22,?23,?24,?25,?26,1,?27,?28,?29,0,
  CURRENT_TIMESTAMP,CURRENT_TIMESTAMP
)
ON CONFLICT(dedup_key) DO UPDATE SET
  duplicate_count = CASE
    WHEN signal_analytics_signals.ingest_event_id = excluded.ingest_event_id
      THEN signal_analytics_signals.duplicate_count
    ELSE signal_analytics_signals.duplicate_count + 1
  END,
  ingest_event_id = CASE
    WHEN excluded.last_seen_at > signal_analytics_signals.last_seen_at
      OR (
        excluded.last_seen_at = signal_analytics_signals.last_seen_at
        AND excluded.last_source_message_id IS NOT NULL
        AND (
          signal_analytics_signals.last_source_message_id IS NULL
          OR excluded.last_source_message_id >= signal_analytics_signals.last_source_message_id
        )
      )
      THEN excluded.ingest_event_id
    ELSE signal_analytics_signals.ingest_event_id
  END,
  first_source_message_id = CASE
    WHEN excluded.published_at < signal_analytics_signals.published_at
      OR (
        excluded.published_at = signal_analytics_signals.published_at
        AND excluded.first_source_message_id IS NOT NULL
        AND (
          signal_analytics_signals.first_source_message_id IS NULL
          OR excluded.first_source_message_id < signal_analytics_signals.first_source_message_id
        )
      )
      THEN COALESCE(
        excluded.first_source_message_id,
        signal_analytics_signals.first_source_message_id
      )
    ELSE signal_analytics_signals.first_source_message_id
  END,
  last_source_message_id = CASE
    WHEN excluded.last_seen_at > signal_analytics_signals.last_seen_at
      OR (
        excluded.last_seen_at = signal_analytics_signals.last_seen_at
        AND excluded.last_source_message_id IS NOT NULL
        AND (
          signal_analytics_signals.last_source_message_id IS NULL
          OR excluded.last_source_message_id >= signal_analytics_signals.last_source_message_id
        )
      )
      THEN COALESCE(
        excluded.last_source_message_id,
        signal_analytics_signals.last_source_message_id
      )
    ELSE signal_analytics_signals.last_source_message_id
  END,
  published_at = CASE
    WHEN signal_analytics_signals.published_at <= excluded.published_at
      THEN signal_analytics_signals.published_at
    ELSE excluded.published_at
  END,
  last_seen_at = CASE
    WHEN signal_analytics_signals.last_seen_at >= excluded.last_seen_at
      THEN signal_analytics_signals.last_seen_at
    ELSE excluded.last_seen_at
  END,
  expiry_at = CASE
    WHEN signal_analytics_signals.expiry_at IS NULL THEN excluded.expiry_at
    WHEN excluded.expiry_at IS NULL THEN signal_analytics_signals.expiry_at
    WHEN signal_analytics_signals.expiry_at <= excluded.expiry_at
      THEN signal_analytics_signals.expiry_at
    ELSE excluded.expiry_at
  END,
  updated_at = CURRENT_TIMESTAMP
"""

_PG_OBSERVATION_INSERT = """
INSERT INTO signal_analytics_observations(
  ingest_event_id,dedup_key,source_message_id,observed_at,created_at
) VALUES($1,$2,$3,$4,NOW())
ON CONFLICT(ingest_event_id) DO NOTHING
RETURNING ingest_event_id
"""

_SQLITE_OBSERVATION_INSERT = """
INSERT OR IGNORE INTO signal_analytics_observations(
  ingest_event_id,dedup_key,source_message_id,observed_at,created_at
) VALUES(?,?,?,?,CURRENT_TIMESTAMP)
"""


def _target_percents_source(value: Any) -> str:
    """Classify only a non-empty JSON list as source-provided allocation."""
    try:
        parsed = json.loads(str(value or "[]"))
    except (TypeError, ValueError, json.JSONDecodeError):
        return "empty"
    return "source_signal" if isinstance(parsed, list) and bool(parsed) else "empty"


def _record_values(record: NormalizedAnalyticsSignal) -> tuple[Any, ...]:
    published = record.published_at
    published_value: Any = published if is_postgres() else published.isoformat()
    # PostgreSQL NUMERIC receives Decimal; SQLite stores canonical decimal text
    # in NUMERIC-affinity columns without a binary-float conversion in Python.
    decimals: Iterable[Any]
    if is_postgres():
        decimals = (
            Decimal(record.entry_low),
            Decimal(record.entry_high),
            Decimal(record.entry_reference),
            Decimal(record.stop_price),
        )
    else:
        decimals = (
            record.entry_low,
            record.entry_high,
            record.entry_reference,
            record.stop_price,
        )
    entry_low, entry_high, entry_reference, stop_price = decimals
    return (
        record.ingest_event_id,
        record.dedup_key,
        record.content_fingerprint,
        record.source_chat_id,
        record.source_message_id,
        record.source_title,
        record.sender_chat_id,
        record.sender_chat_title,
        published_value,
        record.signal_id_text,
        record.symbol,
        record.side,
        record.order_type,
        record.source_format,
        record.timeframe,
        record.strategy,
        record.source_leverage,
        entry_low,
        entry_high,
        entry_reference,
        stop_price,
        record.targets_json,
        record.target_percents_json,
        _target_percents_source(record.target_percents_json),
        record.raw_text,
        record.initial_status,
        (
            record.tracking_started_at
            if is_postgres()
            else record.tracking_started_at.isoformat()
            if record.tracking_started_at is not None
            else None
        ),
        (
            record.expiry_at
            if is_postgres()
            else record.expiry_at.isoformat()
            if record.expiry_at is not None
            else None
        ),
        record.be_trigger_tp_index,
    )


async def _resolve_dedup_record(
    conn: Any, record: NormalizedAnalyticsSignal
) -> NormalizedAnalyticsSignal:
    """Resolve a true sliding dedup window instead of a fixed time bucket.

    The observation-specific key remains a deterministic candidate for first
    insertion. Before writing, the serialized worker looks for the nearest row
    with the same source/fingerprint inside the configured window. This prevents
    copies posted seconds apart on opposite bucket boundaries from becoming two
    analytics signals. The interval check also requires the complete first-to-last
    group span to stay within the configured window, so a chain of periodic reposts
    cannot extend one analytics signal forever.
    """

    if is_postgres():
        row = await conn.fetchrow(
            _PG_RESOLVE_DEDUP,
            record.source_chat_id,
            record.content_fingerprint,
            record.published_at,
            record.dedup_window_seconds,
        )
        resolved = str(row["dedup_key"] or "") if row is not None else ""
    else:
        published = record.published_at.isoformat()
        cursor = await conn.execute(
            _SQLITE_RESOLVE_DEDUP,
            (
                record.source_chat_id,
                record.content_fingerprint,
                published,
                record.dedup_window_seconds,
                published,
                record.dedup_window_seconds,
                published,
                published,
                published,
                published,
            ),
        )
        row = await cursor.fetchone()
        resolved = str(row[0] or "") if row is not None else ""
    if not resolved or resolved == record.dedup_key:
        return record
    return replace(record, dedup_key=resolved)


async def _write_normalized_batch(
    records: list[NormalizedAnalyticsSignal],
) -> tuple[int, int]:
    """Persist one batch and return ``(new_rows, duplicate_observations)``."""

    if not records:
        return 0, 0
    new_rows = 0
    duplicates = 0
    async with connect() as conn:
        if is_postgres():
            async with conn.transaction():
                for source_record in records:
                    record = await _resolve_dedup_record(conn, source_record)
                    observation = await conn.fetchrow(
                        _PG_OBSERVATION_INSERT,
                        record.ingest_event_id,
                        record.dedup_key,
                        record.source_message_id,
                        record.published_at,
                    )
                    # A committed batch can be retried when the client loses the
                    # COMMIT response. The observation PK makes that replay a
                    # true no-op even if another duplicate was stored meanwhile.
                    if observation is None:
                        continue
                    existing = await conn.fetchrow(
                        "SELECT ingest_event_id,duplicate_count "
                        "FROM signal_analytics_signals WHERE dedup_key=$1",
                        record.dedup_key,
                    )
                    await conn.fetchrow(_PG_UPSERT, *_record_values(record))
                    if existing is None:
                        new_rows += 1
                    elif str(existing["ingest_event_id"] or "") != record.ingest_event_id:
                        duplicates += 1
            return new_rows, duplicates

        await conn.execute("BEGIN")
        try:
            for source_record in records:
                record = await _resolve_dedup_record(conn, source_record)
                await conn.execute(
                    _SQLITE_OBSERVATION_INSERT,
                    (
                        record.ingest_event_id,
                        record.dedup_key,
                        record.source_message_id,
                        record.published_at.isoformat(),
                    ),
                )
                cursor = await conn.execute("SELECT changes()")
                observation_inserted = int((await cursor.fetchone())[0] or 0) == 1
                if not observation_inserted:
                    continue
                cursor = await conn.execute(
                    "SELECT ingest_event_id,duplicate_count "
                    "FROM signal_analytics_signals WHERE dedup_key=?",
                    (record.dedup_key,),
                )
                existing = await cursor.fetchone()
                await conn.execute(_SQLITE_UPSERT, _record_values(record))
                if existing is None:
                    new_rows += 1
                elif str(existing[0] or "") != record.ingest_event_id:
                    duplicates += 1
            await conn.commit()
        except BaseException:
            await conn.rollback()
            raise
    return new_rows, duplicates


class SignalAnalyticsShadowDispatcher:
    """Single bounded, fail-open writer for signal snapshots and level events."""

    def __init__(self) -> None:
        settings = get_settings()
        self.worker_count = max(1, int(settings.SIGNAL_ANALYTICS_DB_WORKERS))
        self.queue_max = max(1, int(settings.SIGNAL_ANALYTICS_QUEUE_MAX))
        self.batch_size = max(1, int(settings.SIGNAL_ANALYTICS_BATCH_SIZE))
        self.flush_seconds = max(0.1, float(settings.SIGNAL_ANALYTICS_FLUSH_SECONDS))
        self.shutdown_timeout = max(
            1.0, float(settings.SIGNAL_ANALYTICS_SHUTDOWN_TIMEOUT_SECONDS)
        )
        self.dedup_window_hours = max(
            1, int(settings.SIGNAL_ANALYTICS_DEDUP_WINDOW_HOURS)
        )
        self.expiry_hours = max(
            1, int(settings.SIGNAL_ANALYTICS_DEFAULT_EXPIRY_HOURS)
        )
        self.summary_interval = max(
            15.0, float(settings.SIGNAL_ANALYTICS_SUMMARY_INTERVAL_SEC)
        )
        reserve_candidate = max(1, min(self.batch_size, self.queue_max // 4))
        self.tracking_reserve = min(max(0, self.queue_max - 1), reserve_candidate)
        self._queue: asyncio.Queue[
            AnalyticsSignalEnvelope | AnalyticsTrackingTransition | AnalyticsLinkageRequest
        ] = asyncio.Queue(maxsize=self.queue_max)
        self._worker_task: asyncio.Task[None] | None = None
        self._start_lock = asyncio.Lock()
        self._stopping = False
        self._last_summary_at = time.monotonic()
        self._received = 0
        self._queued = 0
        self._dropped = 0
        self._invalid = 0
        self._written = 0
        self._duplicates = 0
        self._batches = 0
        self._db_errors = 0
        self._tracking_received = 0
        self._tracking_queued = 0
        self._tracking_dropped = 0
        self._tracking_applied = 0
        self._tracking_stale = 0
        self._tracking_events_written = 0
        self._tracking_refresh_errors = 0
        self._tracking_recovery_marked = 0
        self._tracking_recovery_mark_errors = 0
        self._linkage_received = 0
        self._linkage_queued = 0
        self._linkage_dropped = 0
        self._linkage_groups = 0
        self._linkage_group_conflicts = 0
        self._linkage_executions = 0
        self._linkage_execution_conflicts = 0
        self._linkage_missing = 0
        self._linkage_errors = 0
        self._periods_assigned = 0
        self._last_linkage_backlog_at = 0.0
        self._last_gap_recovery_at = 0.0
        self._gap_recovery_exact = 0
        self._gap_recovery_ambiguous = 0
        self._gap_recovery_retries = 0
        self._gap_recovery_unavailable = 0
        self._gap_recovery_errors = 0

    @staticmethod
    def _enabled() -> bool:
        settings = get_settings()
        analytics_enabled = bool(settings.SIGNAL_ANALYTICS_ENABLED) and bool(
            settings.SIGNAL_ANALYTICS_INGRESS_ENABLED
            or settings.SIGNAL_ANALYTICS_TRACKING_ENABLED
        )
        # Period assignment rides on real analytics ingress, and restart
        # recovery rides on real analytics tracking. Neither flag alone should
        # start an otherwise idle long-lived DB worker. Exact linkage has its
        # own durable backlog and is the only independent statistics workload.
        statistics_enabled = bool(settings.STATISTICS_LINKAGE_ENABLED)
        return analytics_enabled or statistics_enabled

    async def start(self) -> None:
        settings = get_settings()
        if not self._enabled():
            log.info(
                "Signal analytics/statistics worker disabled enabled=%s ingress=%s "
                "tracking=%s linkage=%s recovery=%s periods=%s",
                bool(settings.SIGNAL_ANALYTICS_ENABLED),
                bool(settings.SIGNAL_ANALYTICS_INGRESS_ENABLED),
                bool(settings.SIGNAL_ANALYTICS_TRACKING_ENABLED),
                bool(settings.STATISTICS_LINKAGE_ENABLED),
                bool(settings.STATISTICS_RECOVERY_ENABLED),
                bool(settings.STATISTICS_PERIODS_ENABLED),
            )
            return
        if self._worker_task is not None or self._stopping:
            return
        async with self._start_lock:
            if self._worker_task is not None or self._stopping:
                return
            if bool(settings.SIGNAL_ANALYTICS_TRACKING_ENABLED):
                if not bool(settings.EVENT_DRIVEN_MONITOR_ENABLED):
                    # Tracking consumes the existing market-price event loop.
                    # Keep analytics fail-open and start the writer/reports, but
                    # state clearly that no new ENTRY/TP/SL transitions can be
                    # observed until the public loop is enabled.
                    log.warning(
                        "SIGNAL_ANALYTICS_TRACKING_NO_PRICE_LOOP "
                        "EVENT_DRIVEN_MONITOR_ENABLED=false action=tracking_paused"
                    )
                # Startup refresh happens after init_db(). It is read-only and
                # outside the public-price hot path.
                async with monitor_db_workload(stage="analytics"):
                    await refresh_signal_analytics_registry()
            self._worker_task = asyncio.create_task(
                self._worker_with_db_admission(), name="signal-analytics-writer"
            )
            log.info(
                "Signal analytics dispatcher started workers=%s queue_max=%s "
                "batch_size=%s flush_sec=%.2f dedup_window_hours=%s "
                "tracking=%s expiry_hours=%s tracking_reserve=%s linkage=%s recovery=%s",
                self.worker_count,
                self.queue_max,
                self.batch_size,
                self.flush_seconds,
                self.dedup_window_hours,
                bool(settings.SIGNAL_ANALYTICS_TRACKING_ENABLED),
                self.expiry_hours,
                self.tracking_reserve,
                bool(settings.STATISTICS_LINKAGE_ENABLED),
                bool(settings.STATISTICS_RECOVERY_ENABLED),
            )

    def submit(self, envelope: AnalyticsSignalEnvelope) -> bool:
        self._received += 1
        if self._stopping:
            self._dropped += 1
            return False
        settings = get_settings()
        if not (
            bool(settings.SIGNAL_ANALYTICS_ENABLED)
            and bool(settings.SIGNAL_ANALYTICS_INGRESS_ENABLED)
        ):
            return False
        ingress_limit = self.queue_max - self.tracking_reserve
        if self.tracking_reserve and self._queue.qsize() >= ingress_limit:
            self._dropped += 1
            log.error(
                "SIGNAL_ANALYTICS_QUEUE_RESERVED kind=signal symbol=%s "
                "source_chat_id=%s depth=%s ingress_limit=%s max=%s",
                envelope.symbol,
                envelope.source_chat_id,
                self._queue.qsize(),
                ingress_limit,
                self.queue_max,
            )
            return False
        try:
            self._queue.put_nowait(envelope)
        except asyncio.QueueFull:
            self._dropped += 1
            log.error(
                "SIGNAL_ANALYTICS_QUEUE_FULL kind=signal symbol=%s "
                "source_chat_id=%s depth=%s max=%s",
                envelope.symbol,
                envelope.source_chat_id,
                self._queue.qsize(),
                self.queue_max,
            )
            return False
        self._queued += 1
        return True

    def submit_transition(self, transition: AnalyticsTrackingTransition) -> bool:
        self._tracking_received += 1
        if self._stopping:
            self._tracking_dropped += 1
            return False
        settings = get_settings()
        if not (
            bool(settings.SIGNAL_ANALYTICS_ENABLED)
            and bool(settings.SIGNAL_ANALYTICS_TRACKING_ENABLED)
        ):
            return False
        try:
            self._queue.put_nowait(transition)
        except asyncio.QueueFull:
            self._tracking_dropped += 1
            log.error(
                "SIGNAL_ANALYTICS_QUEUE_FULL kind=tracking signal_id=%s "
                "status=%s depth=%s max=%s",
                transition.signal_id,
                transition.status,
                self._queue.qsize(),
                self.queue_max,
            )
            return False
        self._tracking_queued += 1
        return True

    def submit_linkage(self, request: AnalyticsLinkageRequest) -> bool:
        self._linkage_received += 1
        if self._stopping:
            self._linkage_dropped += 1
            return False
        settings = get_settings()
        if not bool(settings.STATISTICS_LINKAGE_ENABLED):
            return False
        try:
            entity_id = int(request.entity_id)
        except (TypeError, ValueError, OverflowError):
            self._linkage_dropped += 1
            return False
        if entity_id <= 0 or request.kind not in {"group", "execution"}:
            self._linkage_dropped += 1
            return False
        # Linkage is reconstructable from durable trade_groups/executions and
        # therefore must not consume the queue reserve kept for irreversible
        # price transitions. A dropped linkage request is recovered by the
        # periodic backlog scan; a dropped TP/STOP transition is not.
        linkage_limit = self.queue_max - self.tracking_reserve
        if self.tracking_reserve and self._queue.qsize() >= linkage_limit:
            self._linkage_dropped += 1
            log.warning(
                "STATISTICS_LINKAGE_QUEUE_RESERVED kind=%s entity_id=%s "
                "depth=%s linkage_limit=%s max=%s action=durable_backlog_scan",
                request.kind,
                entity_id,
                self._queue.qsize(),
                linkage_limit,
                self.queue_max,
            )
            return False
        try:
            self._queue.put_nowait(request)
        except asyncio.QueueFull:
            self._linkage_dropped += 1
            log.error(
                "STATISTICS_LINKAGE_QUEUE_FULL kind=%s entity_id=%s depth=%s max=%s "
                "action=durable_backlog_scan",
                request.kind,
                entity_id,
                self._queue.qsize(),
                self.queue_max,
            )
            return False
        self._linkage_queued += 1
        return True

    def observe_prices(
        self, prices: dict[str, Any], *, observed_at: datetime
    ) -> dict[str, int]:
        """Synchronously evaluate already-fetched public prices.

        No await, SQL or network request is performed here. Durable transitions
        are admitted with put_nowait and dropped fail-open if the analytics
        queue is full.
        """

        return observe_signal_analytics_prices(
            prices,
            observed_at=observed_at,
            sink=self.submit_transition,
        )

    async def _collect_batch(
        self,
    ) -> list[
        AnalyticsSignalEnvelope | AnalyticsTrackingTransition | AnalyticsLinkageRequest
    ]:
        try:
            first = await asyncio.wait_for(
                self._queue.get(), timeout=self.summary_interval
            )
        except asyncio.TimeoutError:
            return []
        batch: list[
            AnalyticsSignalEnvelope | AnalyticsTrackingTransition | AnalyticsLinkageRequest
        ] = [first]
        deadline = time.monotonic() + self.flush_seconds
        while len(batch) < self.batch_size:
            try:
                batch.append(self._queue.get_nowait())
                continue
            except asyncio.QueueEmpty:
                pass
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break
            try:
                batch.append(await asyncio.wait_for(self._queue.get(), remaining))
            except asyncio.TimeoutError:
                break
        return batch

    async def _write_signal_batch(
        self, envelopes: list[AnalyticsSignalEnvelope]
    ) -> bool:
        if not envelopes:
            return False
        settings = get_settings()
        normalized: list[NormalizedAnalyticsSignal] = []
        for envelope in envelopes:
            try:
                normalized.append(
                    normalize_analytics_signal(
                        envelope,
                        dedup_window_hours=self.dedup_window_hours,
                        tracking_enabled=bool(
                            settings.SIGNAL_ANALYTICS_TRACKING_ENABLED
                        ),
                        expiry_hours=self.expiry_hours,
                    )
                )
            except Exception as exc:
                self._invalid += 1
                log.warning(
                    "SIGNAL_ANALYTICS_INVALID_SHADOW symbol=%s "
                    "source_chat_id=%s error=%s",
                    envelope.symbol,
                    envelope.source_chat_id,
                    f"{type(exc).__name__}: {exc}",
                )
        if not normalized:
            return False

        backoff = 1.0
        while True:
            try:
                inserted, duplicates = await _write_normalized_batch(normalized)
                self._written += inserted
                self._duplicates += duplicates
                self._batches += 1
                try:
                    self._periods_assigned += await assign_active_period_to_ingest_events(
                        item.ingest_event_id for item in normalized
                    )
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    self._linkage_errors += 1
                    log.exception(
                        "STATISTICS_PERIOD_ASSIGN_FAILED_FAIL_OPEN rows=%s error=%s",
                        len(normalized),
                        f"{type(exc).__name__}: {exc}",
                    )
                break
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self._db_errors += 1
                log.exception(
                    "SIGNAL_ANALYTICS_DB_BATCH_FAILED kind=signal rows=%s "
                    "retry_sec=%.1f error=%s",
                    len(normalized),
                    backoff,
                    f"{type(exc).__name__}: {exc}",
                )
                await asyncio.sleep(backoff)
                backoff = min(_RATE_LIMIT_BACKOFF_MAX_SEC, backoff * 2.0)

        return bool(settings.SIGNAL_ANALYTICS_TRACKING_ENABLED)

    async def _write_transition_batch(
        self, transitions: list[AnalyticsTrackingTransition]
    ) -> bool:
        if not transitions:
            return False
        backoff = 1.0
        while True:
            try:
                applied, stale, events = await write_signal_analytics_transitions(
                    transitions
                )
                acknowledge_signal_analytics_transitions(transitions)
                self._tracking_applied += applied
                self._tracking_stale += stale
                self._tracking_events_written += events
                self._batches += 1
                break
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self._db_errors += 1
                log.exception(
                    "SIGNAL_ANALYTICS_DB_BATCH_FAILED kind=tracking rows=%s "
                    "retry_sec=%.1f error=%s",
                    len(transitions),
                    backoff,
                    f"{type(exc).__name__}: {exc}",
                )
                await asyncio.sleep(backoff)
                backoff = min(_RATE_LIMIT_BACKOFF_MAX_SEC, backoff * 2.0)

        if bool(get_settings().STATISTICS_QUALITY_ENABLED):
            try:
                await refresh_statistics_quality_gates(
                    signal_ids={int(item.signal_id) for item in transitions},
                    limit=max(100, min(1_000, self.batch_size * 10)),
                )
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                log.warning(
                    "STATISTICS_QUALITY_GATE_TRANSITION_REFRESH_FAILED_FAIL_OPEN "
                    "signals=%s error=%s",
                    len({int(item.signal_id) for item in transitions}),
                    f"{type(exc).__name__}: {exc}",
                )
        return bool(stale)

    async def _write_linkage_batch(
        self, requests: list[AnalyticsLinkageRequest]
    ) -> None:
        if not requests:
            return
        group_ids = {
            int(item.entity_id) for item in requests if item.kind == "group"
        }
        execution_ids = {
            int(item.entity_id) for item in requests if item.kind == "execution"
        }
        try:
            result = await reconcile_statistics_linkage(
                group_ids=group_ids, execution_ids=execution_ids
            )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            self._linkage_errors += 1
            self._linkage_dropped += len(requests)
            log.exception(
                "STATISTICS_LINKAGE_BATCH_FAILED_FAIL_OPEN groups=%s executions=%s "
                "error=%s action=durable_backlog_scan",
                len(group_ids),
                len(execution_ids),
                f"{type(exc).__name__}: {exc}",
            )
            return
        self._linkage_groups += result.groups_linked
        self._linkage_group_conflicts += result.groups_conflicted
        self._linkage_executions += result.executions_projected
        self._linkage_execution_conflicts += result.executions_conflicted
        self._linkage_missing += result.missing
        self._periods_assigned += result.periods_assigned
        if bool(get_settings().STATISTICS_QUALITY_ENABLED):
            try:
                await refresh_statistics_quality_gates(
                    execution_ids=execution_ids,
                    include_backlog=bool(result.executions_projected and not execution_ids),
                    limit=max(100, min(1_000, self.batch_size * 10)),
                )
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self._linkage_errors += 1
                log.exception(
                    "STATISTICS_QUALITY_GATE_REFRESH_FAILED_FAIL_OPEN executions=%s error=%s",
                    len(execution_ids),
                    f"{type(exc).__name__}: {exc}",
                )
        self._batches += 1

    async def _reconcile_linkage_backlog_if_due(self, *, force: bool = False) -> None:
        settings = get_settings()
        if not bool(settings.STATISTICS_LINKAGE_ENABLED):
            return
        now = time.monotonic()
        interval = max(30.0, self.summary_interval)
        if not force and now - self._last_linkage_backlog_at < interval:
            return
        # The durable scan is strictly lower priority than queued analytics
        # transitions. Do not hold the single writer while fresh work is waiting.
        if not force and not self._queue.empty():
            return
        self._last_linkage_backlog_at = now
        try:
            result = await reconcile_statistics_linkage(
                include_backlog=True,
                backlog_limit=max(100, min(1_000, self.batch_size * 10)),
            )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            self._linkage_errors += 1
            log.exception(
                "STATISTICS_LINKAGE_BACKLOG_FAILED_FAIL_OPEN error=%s",
                f"{type(exc).__name__}: {exc}",
            )
            return
        self._linkage_groups += result.groups_linked
        self._linkage_group_conflicts += result.groups_conflicted
        self._linkage_executions += result.executions_projected
        self._linkage_execution_conflicts += result.executions_conflicted
        self._linkage_missing += result.missing
        self._periods_assigned += result.periods_assigned
        if bool(get_settings().STATISTICS_QUALITY_ENABLED):
            try:
                await refresh_statistics_quality_gates(
                    include_backlog=True,
                    limit=max(100, min(1_000, self.batch_size * 10)),
                )
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self._linkage_errors += 1
                log.exception(
                    "STATISTICS_QUALITY_GATE_BACKLOG_FAILED_FAIL_OPEN error=%s",
                    f"{type(exc).__name__}: {exc}",
                )
        if force or any(
            (
                result.groups_linked,
                result.groups_conflicted,
                result.executions_projected,
                result.executions_conflicted,
            )
        ):
            log.info(
                "STATISTICS_LINKAGE_BACKLOG forced=%s groups=%s group_conflicts=%s "
                "executions=%s execution_conflicts=%s missing=%s",
                int(bool(force)),
                result.groups_linked,
                result.groups_conflicted,
                result.executions_projected,
                result.executions_conflicted,
                result.missing,
            )

    async def _recover_restart_gaps_if_due(self, *, force: bool = False) -> None:
        settings = get_settings()
        if not bool(settings.STATISTICS_RECOVERY_ENABLED):
            return
        if not bool(settings.SIGNAL_ANALYTICS_TRACKING_ENABLED):
            return
        now = time.monotonic()
        interval = max(30.0, float(settings.STATISTICS_GAP_RECOVERY_INTERVAL_SEC))
        if not force and now - self._last_gap_recovery_at < interval:
            return
        # The analytics writer queue always wins at admission. Recovery starts
        # only while the queue is empty; new rows remain buffered if they arrive
        # during the bounded read-only request. No trading path uses this worker.
        if not force and not self._queue.empty():
            return
        self._last_gap_recovery_at = now
        batch = max(1, min(5, int(settings.STATISTICS_GAP_RECOVERY_BATCH_SIZE)))
        for _ in range(batch):
            try:
                outcome = await recover_statistics_restart_gap_once()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self._gap_recovery_errors += 1
                log.exception(
                    "STATISTICS_RESTART_GAP_WORKER_FAILED error=%s",
                    f"{type(exc).__name__}: {exc}",
                )
                break
            if outcome is None:
                break
            if outcome.action == "recovered_exact":
                self._gap_recovery_exact += 1
                # Re-admit each exact live row before processing another gap.
                # Waiting until the end of a configurable batch would create a
                # fresh blind tail for the first recovered row.
                admitted = await self._refresh_tracker_after_batch(
                    reason="restart_gap_recovery"
                )
                if not admitted:
                    try:
                        requeued = await (
                            requeue_statistics_restart_gap_after_tracker_refresh_failure(
                                outcome.signal_id,
                                error=(
                                    "signal analytics tracker refresh failed "
                                    "after exact replay"
                                ),
                            )
                        )
                    except asyncio.CancelledError:
                        raise
                    except Exception:
                        # Never terminate the only analytics writer because the
                        # fail-closed handoff marker itself hit a DB outage. A
                        # later normal registry refresh or process restart can
                        # still load the durable exact row.
                        self._gap_recovery_errors += 1
                        log.exception(
                            "STATISTICS_RESTART_GAP_ADMISSION_REQUEUE_FAILED "
                            "signal_id=%s",
                            outcome.signal_id,
                        )
                        requeued = False
                    if requeued:
                        self._gap_recovery_retries += 1
                        log.warning(
                            "STATISTICS_RESTART_GAP_ADMISSION_REQUEUED signal_id=%s",
                            outcome.signal_id,
                        )
            elif outcome.action == "recovered_ambiguous":
                self._gap_recovery_ambiguous += 1
            elif outcome.action == "recovery_unavailable":
                self._gap_recovery_unavailable += 1
            elif outcome.action == "recovery_rescheduled":
                self._gap_recovery_retries += 1

    async def _refresh_tracker_after_batch(self, *, reason: str) -> bool:
        try:
            await refresh_signal_analytics_registry()
            return True
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            self._tracking_refresh_errors += 1
            log.exception(
                "SIGNAL_ANALYTICS_TRACKER_REFRESH_FAILED reason=%s error=%s",
                reason,
                f"{type(exc).__name__}: {exc}",
            )
            return False

    async def _flush_tracking_recovery_marks(self) -> None:
        ids = signal_analytics_recovery_quarantine_ids(
            limit=max(100, self.batch_size * 10)
        )
        if not ids:
            return
        try:
            await write_signal_analytics_recovery_marks(ids)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            self._tracking_recovery_mark_errors += 1
            log.exception(
                "SIGNAL_ANALYTICS_RECOVERY_MARK_FAILED rows=%s error=%s",
                len(ids),
                f"{type(exc).__name__}: {exc}",
            )
            return
        acknowledge_signal_analytics_recovery_quarantine(ids)
        self._tracking_recovery_marked += len(ids)
        log.warning(
            "SIGNAL_ANALYTICS_RECOVERY_MARKED rows=%s action=tracking_paused",
            len(ids),
        )

    async def _worker_with_db_admission(self) -> None:
        # Reuse the monitor DB admission budget so this optional observer can
        # never consume every PostgreSQL pool slot needed by Telegram/trading.
        async with monitor_db_workload(stage="analytics"):
            await self._worker()

    async def _worker(self) -> None:
        await self._reconcile_linkage_backlog_if_due(force=True)
        while True:
            try:
                batch = await self._collect_batch()
            except asyncio.CancelledError:
                return
            if not batch:
                await self._flush_tracking_recovery_marks()
                await self._recover_restart_gaps_if_due()
                await self._reconcile_linkage_backlog_if_due()
                self._maybe_log_summary()
                continue

            envelopes = [
                item for item in batch if isinstance(item, AnalyticsSignalEnvelope)
            ]
            transitions = [
                item
                for item in batch
                if isinstance(item, AnalyticsTrackingTransition)
            ]
            linkage_requests = [
                item for item in batch if isinstance(item, AnalyticsLinkageRequest)
            ]
            try:
                refresh_for_ingress = await self._write_signal_batch(envelopes)
                refresh_for_stale = await self._write_transition_batch(transitions)
                # Exact linkage is reconstructable and lower priority than
                # irreversible ENTRY/TP/STOP/BE tracking transitions.
                await self._write_linkage_batch(linkage_requests)
                if refresh_for_ingress or refresh_for_stale:
                    # Refresh only after both durable sub-batches. Refreshing in
                    # between could overwrite optimistic in-memory transitions
                    # with the old DB version before they are persisted.
                    await self._refresh_tracker_after_batch(
                        reason=(
                            "ingress_and_stale"
                            if refresh_for_ingress and refresh_for_stale
                            else "ingress"
                            if refresh_for_ingress
                            else "stale_transition"
                        )
                    )
            except asyncio.CancelledError:
                for _ in batch:
                    self._queue.task_done()
                raise
            except Exception:
                # Both write helpers retry normal DB failures indefinitely. This
                # boundary prevents an unexpected programming error from killing
                # the only analytics worker and keeps trading fail-open.
                self._db_errors += 1
                self._dropped += len(envelopes)
                self._tracking_dropped += len(transitions)
                self._linkage_dropped += len(linkage_requests)
                log.exception(
                    "SIGNAL_ANALYTICS_WORKER_BATCH_UNEXPECTED rows=%s", len(batch)
                )
                if transitions:
                    # An observed transition that failed unexpectedly cannot be
                    # replayed from a later last-price sample without possibly
                    # changing its outcome. Quarantine instead of reloading the
                    # stale ACTIVE state.
                    quarantine_signal_analytics_transitions(
                        transitions, reason="unexpected_transition_batch_failure"
                    )

            await self._flush_tracking_recovery_marks()
            await self._recover_restart_gaps_if_due()
            await self._reconcile_linkage_backlog_if_due()
            for _ in batch:
                self._queue.task_done()
            self._maybe_log_summary()

    def _maybe_log_summary(self, *, force: bool = False) -> None:
        now = time.monotonic()
        if not force and now - self._last_summary_at < self.summary_interval:
            return
        self._last_summary_at = now
        stats = self.stats()
        log.info(
            "SIGNAL_ANALYTICS_SUMMARY enabled=%s ingress=%s tracking=%s "
            "received=%s queued=%s written=%s duplicates=%s dropped=%s "
            "invalid=%s tracking_received=%s tracking_queued=%s "
            "tracking_applied=%s tracking_stale=%s tracking_events=%s "
            "tracking_dropped=%s refresh_errors=%s recovery_marked=%s "
            "recovery_mark_errors=%s linkage_groups=%s linkage_group_conflicts=%s "
            "linkage_executions=%s linkage_execution_conflicts=%s linkage_missing=%s "
            "linkage_dropped=%s linkage_errors=%s periods_assigned=%s "
            "gap_exact=%s gap_ambiguous=%s gap_retries=%s "
            "gap_unavailable=%s gap_errors=%s "
            "batches=%s db_errors=%s queue_depth=%s queue_max=%s "
            "tracking_reserve=%s active_rows=%s symbols=%s",
            stats["enabled"],
            stats["ingress_enabled"],
            stats["tracking_enabled"],
            stats["received"],
            stats["queued_total"],
            stats["written"],
            stats["duplicates"],
            stats["dropped"],
            stats["invalid"],
            stats["tracking_received"],
            stats["tracking_queued_total"],
            stats["tracking_applied"],
            stats["tracking_stale"],
            stats["tracking_events_written"],
            stats["tracking_dropped"],
            stats["tracking_refresh_errors"],
            stats["tracking_recovery_marked"],
            stats["tracking_recovery_mark_errors"],
            stats["linkage_groups"],
            stats["linkage_group_conflicts"],
            stats["linkage_executions"],
            stats["linkage_execution_conflicts"],
            stats["linkage_missing"],
            stats["linkage_dropped"],
            stats["linkage_errors"],
            stats["periods_assigned"],
            stats["gap_recovery_exact"],
            stats["gap_recovery_ambiguous"],
            stats["gap_recovery_retries"],
            stats["gap_recovery_unavailable"],
            stats["gap_recovery_errors"],
            stats["batches"],
            stats["db_errors"],
            stats["queued"],
            stats["queue_max"],
            stats["tracking_reserve"],
            stats.get("tracking_active_rows", 0),
            stats.get("tracking_symbols", 0),
        )

    def stats(self) -> dict[str, int]:
        settings = get_settings()
        worker_active = int(
            self._worker_task is not None and not self._worker_task.done()
        )
        result = {
            "enabled": int(bool(settings.SIGNAL_ANALYTICS_ENABLED)),
            "ingress_enabled": int(
                bool(settings.SIGNAL_ANALYTICS_INGRESS_ENABLED)
            ),
            "tracking_enabled": int(
                bool(settings.SIGNAL_ANALYTICS_TRACKING_ENABLED)
            ),
            "price_loop_enabled": int(bool(settings.EVENT_DRIVEN_MONITOR_ENABLED)),
            "reports_enabled": int(
                bool(settings.SIGNAL_ANALYTICS_REPORTS_ENABLED)
            ),
            "statistics_periods_enabled": int(
                bool(settings.STATISTICS_PERIODS_ENABLED)
            ),
            "statistics_linkage_enabled": int(
                bool(settings.STATISTICS_LINKAGE_ENABLED)
            ),
            "statistics_recovery_enabled": int(
                bool(settings.STATISTICS_RECOVERY_ENABLED)
            ),
            "worker": worker_active,
            "workers": worker_active,
            "configured_workers": self.worker_count,
            "received": self._received,
            "queued_total": self._queued,
            "written": self._written,
            "duplicates": self._duplicates,
            "dropped": self._dropped,
            "invalid": self._invalid,
            "tracking_received": self._tracking_received,
            "tracking_queued_total": self._tracking_queued,
            "tracking_dropped": self._tracking_dropped,
            "tracking_applied": self._tracking_applied,
            "tracking_stale": self._tracking_stale,
            "tracking_events_written": self._tracking_events_written,
            "tracking_refresh_errors": self._tracking_refresh_errors,
            "tracking_recovery_marked": self._tracking_recovery_marked,
            "tracking_recovery_mark_errors": self._tracking_recovery_mark_errors,
            "linkage_received": self._linkage_received,
            "linkage_queued_total": self._linkage_queued,
            "linkage_dropped": self._linkage_dropped,
            "linkage_groups": self._linkage_groups,
            "linkage_group_conflicts": self._linkage_group_conflicts,
            "linkage_executions": self._linkage_executions,
            "linkage_execution_conflicts": self._linkage_execution_conflicts,
            "linkage_missing": self._linkage_missing,
            "linkage_errors": self._linkage_errors,
            "periods_assigned": self._periods_assigned,
            "gap_recovery_exact": self._gap_recovery_exact,
            "gap_recovery_ambiguous": self._gap_recovery_ambiguous,
            "gap_recovery_retries": self._gap_recovery_retries,
            "gap_recovery_unavailable": self._gap_recovery_unavailable,
            "gap_recovery_errors": self._gap_recovery_errors,
            "batches": self._batches,
            "db_errors": self._db_errors,
            "queued": self._queue.qsize(),
            "queue_max": self.queue_max,
            "tracking_reserve": self.tracking_reserve,
            "stopping": int(self._stopping),
        }
        result.update(signal_analytics_tracker_stats())
        return result

    def _drain_pending(self) -> int:
        drained = 0
        while True:
            try:
                item = self._queue.get_nowait()
            except asyncio.QueueEmpty:
                break
            else:
                self._queue.task_done()
                if isinstance(item, AnalyticsTrackingTransition):
                    self._tracking_dropped += 1
                    quarantine_signal_analytics_transitions(
                        (item,), reason="shutdown_queue_drain"
                    )
                elif isinstance(item, AnalyticsLinkageRequest):
                    self._linkage_dropped += 1
                else:
                    self._dropped += 1
                drained += 1
        return drained

    async def stop(self) -> None:
        self._stopping = True
        task = self._worker_task
        if task is not None:
            try:
                await asyncio.wait_for(
                    self._queue.join(), timeout=self.shutdown_timeout
                )
            except asyncio.TimeoutError:
                log.warning(
                    "Signal analytics shutdown grace expired queued=%s timeout=%.1fs",
                    self._queue.qsize(),
                    self.shutdown_timeout,
                )
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
            self._worker_task = None
        self._drain_pending()
        self._maybe_log_summary(force=True)
        log.info("Signal analytics dispatcher stopped")


_DISPATCHER: SignalAnalyticsShadowDispatcher | None = None


def get_signal_analytics_dispatcher() -> SignalAnalyticsShadowDispatcher:
    global _DISPATCHER
    if _DISPATCHER is None:
        _DISPATCHER = SignalAnalyticsShadowDispatcher()
    return _DISPATCHER


def signal_analytics_dispatcher_stats() -> dict[str, int]:
    dispatcher = _DISPATCHER
    if dispatcher is None:
        settings = get_settings()
        result = {
            "enabled": int(bool(settings.SIGNAL_ANALYTICS_ENABLED)),
            "ingress_enabled": int(
                bool(settings.SIGNAL_ANALYTICS_INGRESS_ENABLED)
            ),
            "tracking_enabled": int(
                bool(settings.SIGNAL_ANALYTICS_TRACKING_ENABLED)
            ),
            "price_loop_enabled": int(bool(settings.EVENT_DRIVEN_MONITOR_ENABLED)),
            "reports_enabled": int(
                bool(settings.SIGNAL_ANALYTICS_REPORTS_ENABLED)
            ),
            "statistics_periods_enabled": int(
                bool(settings.STATISTICS_PERIODS_ENABLED)
            ),
            "statistics_linkage_enabled": int(
                bool(settings.STATISTICS_LINKAGE_ENABLED)
            ),
            "statistics_recovery_enabled": int(
                bool(settings.STATISTICS_RECOVERY_ENABLED)
            ),
            "worker": 0,
            "workers": 0,
            "configured_workers": 1,
            "received": 0,
            "queued_total": 0,
            "written": 0,
            "duplicates": 0,
            "dropped": 0,
            "invalid": 0,
            "tracking_received": 0,
            "tracking_queued_total": 0,
            "tracking_dropped": 0,
            "tracking_applied": 0,
            "tracking_stale": 0,
            "tracking_events_written": 0,
            "tracking_refresh_errors": 0,
            "tracking_recovery_marked": 0,
            "tracking_recovery_mark_errors": 0,
            "linkage_received": 0,
            "linkage_queued_total": 0,
            "linkage_dropped": 0,
            "linkage_groups": 0,
            "linkage_group_conflicts": 0,
            "linkage_executions": 0,
            "linkage_execution_conflicts": 0,
            "linkage_missing": 0,
            "linkage_errors": 0,
            "periods_assigned": 0,
            "batches": 0,
            "db_errors": 0,
            "queued": 0,
            "queue_max": 0,
            "stopping": 0,
        }
        result.update(signal_analytics_tracker_stats())
        return result
    return dispatcher.stats()


def get_signal_analytics_tracking_symbols() -> tuple[str, ...]:
    """Return symbols that need the existing public ticker snapshot."""

    return signal_analytics_tracking_symbols()


def submit_statistics_trade_group_linkage(trade_group_id: int | None) -> bool:
    """Fail-open, non-blocking exact group-link request."""

    try:
        if not bool(get_settings().STATISTICS_LINKAGE_ENABLED):
            return False
        entity_id = int(trade_group_id or 0)
        if entity_id <= 0:
            return False
        return get_signal_analytics_dispatcher().submit_linkage(
            AnalyticsLinkageRequest(kind="group", entity_id=entity_id)
        )
    except Exception as exc:
        # Statistics must never change trade-group creation/fan-out behavior.
        log.exception(
            "STATISTICS_GROUP_LINKAGE_SUBMIT_FAILED_FAIL_OPEN group_id=%s error=%s",
            trade_group_id,
            f"{type(exc).__name__}: {exc}",
        )
        return False


def submit_statistics_execution_linkage(execution_id: int | None) -> bool:
    """Fail-open, non-blocking execution-projection request."""

    try:
        settings = get_settings()
        if not (
            bool(settings.STATISTICS_LINKAGE_ENABLED)
            and bool(settings.STATISTICS_EXECUTION_RESULTS_ENABLED)
        ):
            return False
        entity_id = int(execution_id or 0)
        if entity_id <= 0:
            return False
        return get_signal_analytics_dispatcher().submit_linkage(
            AnalyticsLinkageRequest(kind="execution", entity_id=entity_id)
        )
    except Exception as exc:
        # Execution persistence/ENTRY must not fail because optional statistics
        # could not enqueue a reconstructable durable linkage request.
        log.exception(
            "STATISTICS_EXECUTION_LINKAGE_SUBMIT_FAILED_FAIL_OPEN "
            "execution_id=%s error=%s",
            execution_id,
            f"{type(exc).__name__}: {exc}",
        )
        return False


def submit_signal_analytics_price_snapshot(
    prices: dict[str, Any], *, observed_at: datetime | None = None
) -> dict[str, int]:
    """Non-blocking, fail-open public-price fanout used by the monitor."""

    settings = get_settings()
    if not (
        bool(settings.SIGNAL_ANALYTICS_ENABLED)
        and bool(settings.SIGNAL_ANALYTICS_TRACKING_ENABLED)
    ):
        return {"signals_checked": 0, "transitions": 0, "events": 0}
    try:
        return get_signal_analytics_dispatcher().observe_prices(
            prices,
            observed_at=_utc_datetime(observed_at),
        )
    except Exception as exc:
        log.exception(
            "SIGNAL_ANALYTICS_PRICE_FAIL_OPEN symbols=%s error=%s",
            len(prices),
            f"{type(exc).__name__}: {exc}",
        )
        return {"signals_checked": 0, "transitions": 0, "events": 0}


def submit_signal_analytics_shadow(
    signal: Signal,
    *,
    content_fingerprint: str,
    source_chat_id: int,
    source_message_id: int | None,
    source_title: str | None,
    sender_chat_id: int | None,
    sender_chat_title: str | None,
    published_at: datetime | None,
) -> bool:
    """Non-blocking, fail-open admission used by the Telegram handler."""

    settings = get_settings()
    if not (
        bool(settings.SIGNAL_ANALYTICS_ENABLED)
        and bool(settings.SIGNAL_ANALYTICS_INGRESS_ENABLED)
    ):
        return False
    try:
        envelope = build_analytics_signal_envelope(
            signal,
            content_fingerprint=content_fingerprint,
            source_chat_id=source_chat_id,
            source_message_id=source_message_id,
            source_title=source_title,
            sender_chat_id=sender_chat_id,
            sender_chat_title=sender_chat_title,
            published_at=published_at,
        )
        return get_signal_analytics_dispatcher().submit(envelope)
    except Exception as exc:
        log.exception(
            "SIGNAL_ANALYTICS_INGRESS_FAIL_OPEN symbol=%s source_chat_id=%s error=%s",
            str(getattr(signal, "symbol", "") or "").upper(),
            source_chat_id,
            f"{type(exc).__name__}: {exc}",
        )
        return False


async def start_signal_analytics_dispatcher() -> bool:
    try:
        await get_signal_analytics_dispatcher().start()
        return True
    except Exception as exc:
        log.exception(
            "SIGNAL_ANALYTICS_START_FAILED_FAIL_OPEN error=%s",
            f"{type(exc).__name__}: {exc}",
        )
        return False


async def stop_signal_analytics_dispatcher() -> bool:
    global _DISPATCHER
    dispatcher = _DISPATCHER
    _DISPATCHER = None
    if dispatcher is None:
        return True
    try:
        await dispatcher.stop()
        return True
    except Exception as exc:
        log.exception(
            "SIGNAL_ANALYTICS_STOP_FAILED_FAIL_OPEN error=%s",
            f"{type(exc).__name__}: {exc}",
        )
        return False
