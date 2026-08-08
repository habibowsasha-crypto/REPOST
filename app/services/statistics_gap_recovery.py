"""Conservative read-only reconstruction of future source-signal restart gaps.

The trading bot already tracks source-level ENTRY/TP/STOP/BE transitions from
public prices. A Railway process restart creates an interval with no samples.
This module replays only that bounded interval from validated BingX 1-minute
candles. It is intentionally fail-closed:

- one durable leased signal is processed at a time;
- no authenticated API and no trading write is used;
- a candle that can contain two conflicting event orders becomes AMBIGUOUS;
- incomplete candle coverage is retried, never interpreted as "nothing happened";
- old ``forward_resumed`` rows cannot be repaired because their post-gap state
  was already mutated without a saved pre-gap snapshot; they remain excluded.
"""

from __future__ import annotations

import inspect
import json
import logging
import math
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from typing import Any, Awaitable, Callable, Iterable, Mapping, Sequence

from app.config import get_settings
from app.database import db
from app.exchanges.bingx.adapter import BingxAdapter

log = logging.getLogger(__name__)

_RECOVERABLE_STATUSES = {"waiting_entry", "active"}
_TERMINAL_STATUSES = {
    "completed_tp",
    "completed_be",
    "completed_stop",
    "expired_not_entered",
    "ambiguous",
}
_CANDLE_MS = 60_000


@dataclass(frozen=True, slots=True)
class GapCandle:
    open_time: datetime
    close_time: datetime
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal


@dataclass(frozen=True, slots=True)
class GapEvent:
    event_key: str
    event_type: str
    level_index: int
    observed_at: datetime
    observed_price: Decimal


@dataclass(frozen=True, slots=True)
class GapReplayResult:
    status: str
    zone_touched_at: datetime | None
    activated_at: datetime | None
    activated_price: Decimal | None
    max_tp_index: int
    be_armed_at: datetime | None
    completed_at: datetime | None
    terminal_reason: str | None
    ambiguous_reason: str | None
    last_observed_at: datetime
    last_observed_price: Decimal
    events: tuple[GapEvent, ...]
    exact: bool


@dataclass(frozen=True, slots=True)
class GapRecoveryOutcome:
    signal_id: int
    action: str
    recovery_status: str
    error: str | None = None
    events: int = 0


AdapterLoader = Callable[[], Awaitable[BingxAdapter] | BingxAdapter]


class PermanentGapRecoveryError(ValueError):
    """The stored signal/gap cannot be reconstructed by any later retry."""


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _utc(value: datetime) -> datetime:
    return (
        value.replace(tzinfo=timezone.utc)
        if value.tzinfo is None
        else value.astimezone(timezone.utc)
    )


def _dt(value: Any) -> datetime | None:
    if value in (None, ""):
        return None
    if isinstance(value, datetime):
        return _utc(value)
    try:
        return _utc(datetime.fromisoformat(str(value).replace("Z", "+00:00")))
    except (TypeError, ValueError):
        return None


def _latest_dt(*values: Any) -> datetime | None:
    parsed = [item for item in (_dt(value) for value in values) if item is not None]
    return max(parsed) if parsed else None


def _decimal(value: Any, *, positive: bool = True) -> Decimal | None:
    if value in (None, "") or isinstance(value, bool):
        return None
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return None
    if not parsed.is_finite() or (positive and parsed <= 0):
        return None
    return parsed


def _targets(value: Any) -> tuple[Decimal, ...]:
    try:
        raw = json.loads(str(value or "[]"))
    except (TypeError, ValueError, json.JSONDecodeError):
        return ()
    if not isinstance(raw, list) or not 1 <= len(raw) <= 20:
        return ()
    result: list[Decimal] = []
    for item in raw:
        parsed = _decimal(item)
        if parsed is None:
            return ()
        result.append(parsed)
    return tuple(result)


def _db_dt(value: datetime | None) -> Any:
    if value is None:
        return None
    normalized = _utc(value)
    return normalized if db.is_postgres() else normalized.isoformat()


def _db_decimal(value: Decimal | None) -> Any:
    if value is None:
        return None
    return value if db.is_postgres() else format(value, "f")


def _safe_error(value: Any, *, limit: int = 700) -> str:
    return str(value or "unknown").replace("\x00", " ")[:limit]


def normalize_gap_candles(rows: Sequence[Mapping[str, Any]]) -> tuple[GapCandle, ...]:
    result: list[GapCandle] = []
    seen: set[int] = set()
    for index, row in enumerate(rows):
        try:
            open_ms = int(row.get("openTime"))
            close_ms = int(row.get("closeTime") or (open_ms + _CANDLE_MS))
        except (TypeError, ValueError, OverflowError) as exc:
            raise ValueError(f"kline[{index}] timestamp invalid") from exc
        if open_ms in seen:
            raise ValueError(f"duplicate kline {open_ms}")
        seen.add(open_ms)
        if close_ms <= open_ms or close_ms - open_ms != _CANDLE_MS:
            raise ValueError(f"kline[{index}] interval is not exactly 1m")
        open_price = _decimal(row.get("open"))
        high = _decimal(row.get("high"))
        low = _decimal(row.get("low"))
        close = _decimal(row.get("close"))
        if None in {open_price, high, low, close}:
            raise ValueError(f"kline[{index}] OHLC invalid")
        assert open_price is not None and high is not None and low is not None and close is not None
        if high < max(open_price, close) or low > min(open_price, close) or high < low:
            raise ValueError(f"kline[{index}] OHLC bounds invalid")
        result.append(
            GapCandle(
                open_time=datetime.fromtimestamp(open_ms / 1000, tz=timezone.utc),
                close_time=datetime.fromtimestamp(close_ms / 1000, tz=timezone.utc),
                open=open_price,
                high=high,
                low=low,
                close=close,
            )
        )
    result.sort(key=lambda item: item.open_time)
    return tuple(result)


def validate_gap_coverage(
    candles: Sequence[GapCandle], *, start: datetime, end: datetime
) -> None:
    start = _utc(start)
    end = _utc(end)
    if end <= start:
        return
    if not candles:
        raise ValueError("kline coverage empty")
    floor_start = datetime.fromtimestamp(
        math.floor(start.timestamp() / 60) * 60, tz=timezone.utc
    )
    required_end = datetime.fromtimestamp(
        math.ceil(end.timestamp() / 60) * 60, tz=timezone.utc
    )
    if candles[0].open_time > floor_start:
        raise ValueError("kline coverage starts after gap")
    previous = candles[0]
    for current in candles[1:]:
        if current.open_time != previous.close_time:
            raise ValueError("kline coverage contains a missing minute")
        previous = current
    if candles[-1].close_time < required_end:
        raise ValueError("kline coverage ends before gap")


def _touch(side: str, *, high: Decimal, low: Decimal, level: Decimal, favorable: bool) -> bool:
    if favorable:
        return high >= level if side == "long" else low <= level
    return low <= level if side == "long" else high >= level


def _range_intersects(low: Decimal, high: Decimal, left: Decimal, right: Decimal) -> bool:
    return low <= max(left, right) and high >= min(left, right)


def _event(
    key: str,
    kind: str,
    level: int,
    when: datetime,
    price: Decimal,
) -> GapEvent:
    return GapEvent(key, kind, int(level), _utc(when), price)


def replay_restart_gap(
    row: Mapping[str, Any],
    candles: Sequence[GapCandle],
    *,
    gap_start: datetime,
    gap_end: datetime,
) -> GapReplayResult:
    """Replay one gap; conflicting intraminute order becomes AMBIGUOUS."""

    signal_id = int(row.get("id") or 0)
    status = str(row.get("status") or "").strip().lower()
    side = str(row.get("side") or "").strip().lower()
    order_type = str(row.get("order_type") or "").strip().upper()
    allow_zero_entry = order_type == "MARKET"
    entry_low = _decimal(row.get("entry_low"), positive=not allow_zero_entry)
    entry_high = _decimal(row.get("entry_high"), positive=not allow_zero_entry)
    entry_reference = _decimal(
        row.get("entry_reference"), positive=not allow_zero_entry
    )
    activated_price = _decimal(row.get("activated_price"))
    stop = _decimal(row.get("stop_price"))
    targets = _targets(row.get("targets_json"))
    if (
        signal_id <= 0
        or status not in _RECOVERABLE_STATUSES
        or side not in {"long", "short"}
        or order_type not in {"LIMIT", "MARKET"}
        or None in {entry_low, entry_high, entry_reference, stop}
        or (
            order_type == "MARKET"
            and any(value is not None and value < 0 for value in (entry_low, entry_high, entry_reference))
        )
        or not targets
    ):
        raise PermanentGapRecoveryError("signal recovery state is invalid")
    assert entry_low is not None and entry_high is not None and entry_reference is not None and stop is not None

    if entry_low > entry_high:
        raise PermanentGapRecoveryError("signal entry range is inverted")
    if order_type == "LIMIT" and not entry_low <= entry_reference <= entry_high:
        raise PermanentGapRecoveryError("signal entry reference is outside its range")
    waiting_market = status == "waiting_entry" and order_type == "MARKET"
    if not waiting_market:
        effective_entry = activated_price if status == "active" else entry_reference
        if effective_entry is None or effective_entry <= 0:
            raise PermanentGapRecoveryError("signal effective entry is invalid")
        if side == "long":
            if stop >= effective_entry or any(
                target <= effective_entry for target in targets
            ):
                raise PermanentGapRecoveryError(
                    "long signal price geometry is invalid"
                )
            if any(right <= left for left, right in zip(targets, targets[1:])):
                raise PermanentGapRecoveryError(
                    "long targets are not strictly ascending"
                )
        else:
            if stop <= effective_entry or any(
                target >= effective_entry for target in targets
            ):
                raise PermanentGapRecoveryError(
                    "short signal price geometry is invalid"
                )
            if any(right >= left for left, right in zip(targets, targets[1:])):
                raise PermanentGapRecoveryError(
                    "short targets are not strictly descending"
                )

    zone_touched_at = _dt(row.get("zone_touched_at"))
    activated_at = _dt(row.get("activated_at"))
    be_armed_at = _dt(row.get("be_armed_at"))
    completed_at = _dt(row.get("completed_at"))
    terminal_reason = str(row.get("terminal_reason") or "").strip() or None
    max_tp = max(0, min(len(targets), int(row.get("max_tp_index") or 0)))
    be_trigger = max(0, min(len(targets), int(row.get("be_trigger_tp_index") or 0)))
    expiry_at = _dt(row.get("expiry_at"))
    last_observed_at = _dt(row.get("last_observed_at")) or _utc(gap_start)
    last_observed_price = _decimal(row.get("last_observed_price")) or entry_reference
    events: list[GapEvent] = []

    if status == "active" and (activated_price is None or activated_price <= 0):
        raise ValueError("active signal has no activated price")
    if status == "waiting_entry" and order_type == "MARKET":
        # MARKET activation happens at a concrete sampled quote. Without a saved
        # quote at the gap boundary, a 1m candle cannot prove its fill price.
        when = min(_utc(gap_end), candles[0].close_time if candles else _utc(gap_end))
        return GapReplayResult(
            status="ambiguous",
            zone_touched_at=zone_touched_at,
            activated_at=None,
            activated_price=None,
            max_tp_index=0,
            be_armed_at=None,
            completed_at=when,
            terminal_reason="ambiguous",
            ambiguous_reason="restart_gap_market_entry_price_unknown",
            last_observed_at=when,
            last_observed_price=(candles[0].close if candles else last_observed_price),
            events=(_event("AMBIGUOUS", "AMBIGUOUS", 0, when, candles[0].close if candles else last_observed_price),),
            exact=False,
        )

    for candle in candles:
        if candle.close_time <= gap_start:
            continue
        if candle.open_time >= gap_end:
            break
        observed_at = min(candle.close_time, gap_end)

        # A 1-minute OHLC row cannot reveal which part of its range happened
        # before ``gap_start`` or after ``gap_end``. Never apply a transition
        # from a boundary-partial candle. We may only prove "no transition"
        # when the *entire* candle range misses every currently relevant level;
        # otherwise the row is explicitly AMBIGUOUS instead of fabricating an
        # intraminute order.
        boundary_partial = (
            candle.open_time < gap_start or candle.close_time > gap_end
        )
        if boundary_partial:
            if status == "waiting_entry":
                segment_start = max(candle.open_time, gap_start)
                segment_end = min(candle.close_time, gap_end)
                entry_touched = candle.low <= entry_reference <= candle.high
                zone_may_change = (
                    zone_touched_at is None
                    and _range_intersects(
                        candle.low, candle.high, entry_low, entry_high
                    )
                )
                if expiry_at is not None and expiry_at <= segment_start:
                    status = "expired_not_entered"
                    completed_at = expiry_at
                    terminal_reason = "expired_not_entered"
                    events.append(
                        _event(
                            "EXPIRED", "EXPIRED", 0, expiry_at, last_observed_price
                        )
                    )
                    break
                expiry_inside = (
                    expiry_at is not None
                    and segment_start < expiry_at <= segment_end
                )
                if entry_touched or zone_may_change:
                    reason = (
                        "restart_gap_boundary_entry_vs_expiry_unknown"
                        if expiry_inside
                        else "restart_gap_boundary_entry_timing_unknown"
                    )
                    return GapReplayResult(
                        status="ambiguous",
                        zone_touched_at=zone_touched_at,
                        activated_at=None,
                        activated_price=None,
                        max_tp_index=0,
                        be_armed_at=None,
                        completed_at=observed_at,
                        terminal_reason="ambiguous",
                        ambiguous_reason=reason,
                        last_observed_at=last_observed_at,
                        last_observed_price=last_observed_price,
                        events=tuple(
                            events
                            + [
                                _event(
                                    "AMBIGUOUS",
                                    "AMBIGUOUS",
                                    0,
                                    observed_at,
                                    last_observed_price,
                                )
                            ]
                        ),
                        exact=False,
                    )
                if expiry_inside:
                    status = "expired_not_entered"
                    completed_at = expiry_at
                    terminal_reason = "expired_not_entered"
                    events.append(
                        _event(
                            "EXPIRED", "EXPIRED", 0, expiry_at, last_observed_price
                        )
                    )
                    break
                continue

            if status == "active":
                next_target_touched = any(
                    _touch(
                        side,
                        high=candle.high,
                        low=candle.low,
                        level=targets[index],
                        favorable=True,
                    )
                    for index in range(max_tp, len(targets))
                )
                be_is_armed = be_trigger > 0 and max_tp >= be_trigger
                adverse_level = (activated_price or entry_reference) if be_is_armed else stop
                protection_touched = _touch(
                    side,
                    high=candle.high,
                    low=candle.low,
                    level=adverse_level,
                    favorable=False,
                )
                if next_target_touched or protection_touched:
                    return GapReplayResult(
                        status="ambiguous",
                        zone_touched_at=zone_touched_at,
                        activated_at=activated_at,
                        activated_price=activated_price,
                        max_tp_index=max_tp,
                        be_armed_at=be_armed_at,
                        completed_at=observed_at,
                        terminal_reason="ambiguous",
                        ambiguous_reason="restart_gap_boundary_transition_timing_unknown",
                        last_observed_at=last_observed_at,
                        last_observed_price=last_observed_price,
                        events=tuple(
                            events
                            + [
                                _event(
                                    "AMBIGUOUS",
                                    "AMBIGUOUS",
                                    max_tp,
                                    observed_at,
                                    last_observed_price,
                                )
                            ]
                        ),
                        exact=False,
                    )
                continue

        last_observed_at = observed_at
        last_observed_price = candle.close

        if status == "waiting_entry":
            entry_touched = candle.low <= entry_reference <= candle.high
            zone_touched = _range_intersects(
                candle.low, candle.high, entry_low, entry_high
            )
            if zone_touched and zone_touched_at is None:
                zone_touched_at = observed_at
                events.append(
                    _event("ENTRY_ZONE", "ENTRY_ZONE", 0, observed_at, candle.close)
                )

            expiry_inside = (
                expiry_at is not None
                and candle.open_time < expiry_at <= candle.close_time
                and expiry_at <= gap_end
            )
            if expiry_inside and entry_touched:
                return GapReplayResult(
                    status="ambiguous",
                    zone_touched_at=zone_touched_at,
                    activated_at=None,
                    activated_price=None,
                    max_tp_index=0,
                    be_armed_at=None,
                    completed_at=expiry_at,
                    terminal_reason="ambiguous",
                    ambiguous_reason="restart_gap_entry_vs_expiry_intraminute_order_unknown",
                    last_observed_at=observed_at,
                    last_observed_price=candle.close,
                    events=tuple(events + [_event("AMBIGUOUS", "AMBIGUOUS", 0, observed_at, candle.close)]),
                    exact=False,
                )
            if expiry_at is not None and expiry_at <= candle.open_time and expiry_at <= gap_end:
                status = "expired_not_entered"
                completed_at = expiry_at
                terminal_reason = "expired_not_entered"
                events.append(_event("EXPIRED", "EXPIRED", 0, expiry_at, candle.open))
                break
            if not entry_touched:
                continue

            target_touched = _touch(
                side, high=candle.high, low=candle.low, level=targets[0], favorable=True
            )
            stop_touched = _touch(
                side, high=candle.high, low=candle.low, level=stop, favorable=False
            )
            if target_touched or stop_touched:
                return GapReplayResult(
                    status="ambiguous",
                    zone_touched_at=zone_touched_at or observed_at,
                    activated_at=None,
                    activated_price=None,
                    max_tp_index=0,
                    be_armed_at=None,
                    completed_at=observed_at,
                    terminal_reason="ambiguous",
                    ambiguous_reason="restart_gap_entry_terminal_intraminute_order_unknown",
                    last_observed_at=observed_at,
                    last_observed_price=candle.close,
                    events=tuple(events + [_event("AMBIGUOUS", "AMBIGUOUS", 0, observed_at, candle.close)]),
                    exact=False,
                )
            status = "active"
            activated_at = observed_at
            activated_price = entry_reference
            if zone_touched_at is None:
                zone_touched_at = observed_at
                events.append(
                    _event("ENTRY_ZONE", "ENTRY_ZONE", 0, observed_at, entry_reference)
                )
            events.append(_event("ENTRY", "ENTRY", 0, observed_at, entry_reference))
            continue

        if status != "active":
            break

        entry = activated_price or entry_reference
        touched_targets: list[int] = []
        next_index = max_tp
        while next_index < len(targets) and _touch(
            side,
            high=candle.high,
            low=candle.low,
            level=targets[next_index],
            favorable=True,
        ):
            touched_targets.append(next_index + 1)
            next_index += 1

        be_was_armed = be_trigger > 0 and max_tp >= be_trigger
        be_will_arm = be_trigger > 0 and next_index >= be_trigger
        adverse_level = entry if (be_was_armed or be_will_arm) else stop
        adverse_touched = _touch(
            side,
            high=candle.high,
            low=candle.low,
            level=adverse_level,
            favorable=False,
        )
        if touched_targets and adverse_touched:
            return GapReplayResult(
                status="ambiguous",
                zone_touched_at=zone_touched_at,
                activated_at=activated_at,
                activated_price=activated_price,
                max_tp_index=max_tp,
                be_armed_at=be_armed_at,
                completed_at=observed_at,
                terminal_reason="ambiguous",
                ambiguous_reason="restart_gap_tp_vs_protection_intraminute_order_unknown",
                last_observed_at=observed_at,
                last_observed_price=candle.close,
                events=tuple(events + [_event("AMBIGUOUS", "AMBIGUOUS", max_tp, observed_at, candle.close)]),
                exact=False,
            )

        if touched_targets:
            for level_index in touched_targets:
                max_tp = level_index
                events.append(
                    _event(f"TP{level_index}", "TP", level_index, observed_at, candle.close)
                )
                if be_trigger > 0 and max_tp >= be_trigger and be_armed_at is None:
                    be_armed_at = observed_at
                    events.append(
                        _event("BE_ARMED", "BE_ARMED", be_trigger, observed_at, candle.close)
                    )
            if max_tp >= len(targets):
                status = "completed_tp"
                completed_at = observed_at
                terminal_reason = "all_targets"
                break
            continue

        if adverse_touched:
            if be_was_armed:
                status = "completed_be"
                completed_at = observed_at
                terminal_reason = f"be_after_tp{max_tp}"
                events.append(
                    _event("BREAKEVEN", "BREAKEVEN", max_tp, observed_at, candle.close)
                )
            else:
                status = "completed_stop"
                completed_at = observed_at
                terminal_reason = "stop_no_tp" if max_tp == 0 else f"stop_after_tp{max_tp}"
                events.append(_event("STOP", "STOP", max_tp, observed_at, candle.close))
            break

    if status == "waiting_entry" and expiry_at is not None and expiry_at <= gap_end:
        status = "expired_not_entered"
        completed_at = expiry_at
        terminal_reason = "expired_not_entered"
        events.append(_event("EXPIRED", "EXPIRED", 0, expiry_at, last_observed_price))

    return GapReplayResult(
        status=status,
        zone_touched_at=zone_touched_at,
        activated_at=activated_at,
        activated_price=activated_price,
        max_tp_index=max_tp,
        be_armed_at=be_armed_at,
        completed_at=completed_at,
        terminal_reason=terminal_reason,
        ambiguous_reason=None,
        last_observed_at=last_observed_at,
        last_observed_price=last_observed_price,
        events=tuple(events),
        exact=True,
    )


async def _claim_due_signal(*, now: datetime) -> dict[str, Any] | None:
    settings = get_settings()
    if not bool(settings.STATISTICS_RECOVERY_ENABLED):
        return None
    lease = uuid.uuid4().hex
    stale_before = now - timedelta(minutes=10)
    async with db.connect() as conn:
        if db.is_postgres():
            async with conn.transaction():
                row = await conn.fetchrow(
                    """
                    SELECT *
                    FROM signal_analytics_signals
                    WHERE needs_recovery=1
                      AND status IN ('waiting_entry','active')
                      AND recovery_method='bingx_1m_ohlc_conservative'
                      AND (
                        (recovery_status IN ('pending','retry')
                         AND COALESCE(recovery_next_attempt_at,NOW()) <= $1)
                        OR (recovery_status='processing'
                            AND recovery_processing_started_at < $2)
                      )
                    ORDER BY COALESCE(recovery_next_attempt_at,recovery_started_at,updated_at),id
                    FOR UPDATE SKIP LOCKED
                    LIMIT 1
                    """,
                    now,
                    stale_before,
                )
                if not row:
                    return None
                signal_id = int(row["id"])
                await conn.execute(
                    """
                    UPDATE signal_analytics_signals
                    SET recovery_status='processing',
                        recovery_attempts=COALESCE(recovery_attempts,0)+1,
                        recovery_processing_started_at=$1,
                        recovery_lease_token=$2,
                        recovery_last_error=NULL,
                        updated_at=NOW()
                    WHERE id=$3
                    """,
                    now,
                    lease,
                    signal_id,
                )
                return dict(
                    await conn.fetchrow(
                        "SELECT * FROM signal_analytics_signals WHERE id=$1", signal_id
                    )
                )
        await conn.execute("BEGIN IMMEDIATE")
        try:
            cursor = await conn.execute(
                """
                SELECT *
                FROM signal_analytics_signals
                WHERE needs_recovery=1
                  AND status IN ('waiting_entry','active')
                  AND recovery_method='bingx_1m_ohlc_conservative'
                  AND (
                    (recovery_status IN ('pending','retry')
                     AND julianday(COALESCE(recovery_next_attempt_at,CURRENT_TIMESTAMP)) <= julianday(?))
                    OR (recovery_status='processing'
                        AND julianday(recovery_processing_started_at) < julianday(?))
                  )
                ORDER BY COALESCE(recovery_next_attempt_at,recovery_started_at,updated_at),id
                LIMIT 1
                """,
                (
                    now.isoformat(),
                    stale_before.isoformat(),
                ),
            )
            row = await cursor.fetchone()
            if not row:
                await conn.commit()
                return None
            signal_id = int(row["id"])
            await conn.execute(
                """
                UPDATE signal_analytics_signals
                SET recovery_status='processing',
                    recovery_attempts=COALESCE(recovery_attempts,0)+1,
                    recovery_processing_started_at=?,
                    recovery_lease_token=?,
                    recovery_last_error=NULL,
                    updated_at=CURRENT_TIMESTAMP
                WHERE id=?
                """,
                (now.isoformat(), lease, signal_id),
            )
            cursor = await conn.execute(
                "SELECT * FROM signal_analytics_signals WHERE id=?", (signal_id,)
            )
            claimed = dict(await cursor.fetchone())
            await conn.commit()
            return claimed
        except BaseException:
            await conn.rollback()
            raise


async def _reschedule(
    row: Mapping[str, Any], *, now: datetime, error: str, permanent: bool = False
) -> GapRecoveryOutcome:
    signal_id = int(row["id"])
    lease = str(row.get("recovery_lease_token") or "")
    attempts = int(row.get("recovery_attempts") or 0)
    settings = get_settings()
    max_attempts = max(1, int(settings.STATISTICS_GAP_RECOVERY_MAX_ATTEMPTS))
    if permanent or attempts >= max_attempts:
        status = "unavailable"
        next_at = now
        action = "recovery_unavailable"
    else:
        status = "retry"
        base = max(60.0, float(settings.STATISTICS_GAP_RECOVERY_RETRY_SEC))
        raw_delay = min(3600.0, base * (2 ** max(0, attempts - 1)))
        # Deterministic 90-110% jitter prevents several Railway replicas from
        # retrying different rows on the same second without making tests flaky.
        jitter_slot = (signal_id * 1103515245 + attempts * 12345) % 1000
        delay = min(3600.0, raw_delay * (0.9 + jitter_slot / 5000.0))
        next_at = now + timedelta(seconds=delay)
        action = "recovery_rescheduled"
    async with db.connect() as conn:
        if db.is_postgres():
            result = await conn.execute(
                """
                UPDATE signal_analytics_signals
                SET recovery_status=$1,recovery_next_attempt_at=$2,
                    recovery_processing_started_at=NULL,recovery_lease_token=NULL,
                    recovery_last_error=$3,
                    recovery_completed_at=CASE WHEN $1='unavailable' THEN $4 ELSE NULL END,
                    recovery_confidence='none',
                    data_quality_status='recovery_required',
                    data_quality_reason=CASE WHEN $1='unavailable'
                      THEN 'restart_gap_recovery_unavailable'
                      ELSE 'restart_gap_recovery_retry' END,
                    updated_at=NOW()
                WHERE id=$5 AND recovery_lease_token=$6
                """,
                status,
                next_at,
                _safe_error(error),
                now,
                signal_id,
                lease,
            )
            saved = str(result).endswith(" 1")
        else:
            cursor = await conn.execute(
                """
                UPDATE signal_analytics_signals
                SET recovery_status=?,recovery_next_attempt_at=?,
                    recovery_processing_started_at=NULL,recovery_lease_token=NULL,
                    recovery_last_error=?,
                    recovery_completed_at=CASE WHEN ?='unavailable' THEN ? ELSE NULL END,
                    recovery_confidence='none',
                    data_quality_status='recovery_required',
                    data_quality_reason=CASE WHEN ?='unavailable'
                      THEN 'restart_gap_recovery_unavailable'
                      ELSE 'restart_gap_recovery_retry' END,
                    updated_at=CURRENT_TIMESTAMP
                WHERE id=? AND recovery_lease_token=?
                """,
                (
                    status,
                    next_at.isoformat(),
                    _safe_error(error),
                    status,
                    now.isoformat(),
                    status,
                    signal_id,
                    lease,
                ),
            )
            await conn.commit()
            saved = int(getattr(cursor, "rowcount", 0) or 0) == 1
    return GapRecoveryOutcome(
        signal_id=signal_id,
        action=action if saved else "recovery_lease_lost",
        recovery_status=status if saved else "lease_lost",
        error=_safe_error(error),
    )


async def _finalize(
    row: Mapping[str, Any],
    *,
    result: GapReplayResult,
    now: datetime,
    recovery_cursor_at: datetime,
) -> GapRecoveryOutcome:
    signal_id = int(row["id"])
    lease = str(row.get("recovery_lease_token") or "")
    expected_version = int(row.get("state_version") or 0)
    recovery_status = "recovered_exact" if result.exact else "ambiguous"
    needs_recovery = 0 if result.exact else 1
    quality = "partial" if result.exact else "ambiguous"
    quality_reason = (
        "restart_gap_recovered_exact_1m_ohlc"
        if result.exact
        else str(result.ambiguous_reason or "restart_gap_recovery_ambiguous")
    )
    async with db.connect() as conn:
        if db.is_postgres():
            async with conn.transaction():
                update = await conn.execute(
                    """
                    UPDATE signal_analytics_signals
                    SET status=$1,zone_touched_at=$2,activated_at=$3,activated_price=$4,
                        max_tp_index=$5,be_armed_at=$6,completed_at=$7,
                        terminal_reason=$8,ambiguous_reason=$9,
                        last_observed_at=$10,last_observed_price=$11,
                        state_version=state_version+1,
                        needs_recovery=$12,recovery_status=$13,
                        recovery_method='bingx_1m_ohlc_conservative',
                        recovery_completed_at=$14,recovery_confidence=$15,
                        recovery_cursor_at=$16,
                        recovery_next_attempt_at=NULL,recovery_processing_started_at=NULL,
                        recovery_lease_token=NULL,recovery_last_error=NULL,
                        data_quality_status=$17,data_quality_reason=$18,updated_at=NOW()
                    WHERE id=$19 AND state_version=$20 AND recovery_lease_token=$21
                    """,
                    result.status,
                    _db_dt(result.zone_touched_at),
                    _db_dt(result.activated_at),
                    _db_decimal(result.activated_price),
                    result.max_tp_index,
                    _db_dt(result.be_armed_at),
                    _db_dt(result.completed_at),
                    result.terminal_reason,
                    result.ambiguous_reason,
                    _db_dt(result.last_observed_at),
                    _db_decimal(result.last_observed_price),
                    needs_recovery,
                    recovery_status,
                    now,
                    "high" if result.exact else "none",
                    _db_dt(recovery_cursor_at),
                    quality,
                    quality_reason,
                    signal_id,
                    expected_version,
                    lease,
                )
                if not str(update).endswith(" 1"):
                    return GapRecoveryOutcome(signal_id, "recovery_lease_lost", "lease_lost")
                inserted = 0
                for event in result.events:
                    db_result = await conn.execute(
                        """
                        INSERT INTO signal_analytics_level_events(
                          signal_id,event_key,event_type,level_index,
                          observed_at,observed_price,created_at
                        ) VALUES($1,$2,$3,$4,$5,$6,NOW())
                        ON CONFLICT(signal_id,event_key) DO NOTHING
                        """,
                        signal_id,
                        event.event_key,
                        event.event_type,
                        event.level_index,
                        event.observed_at,
                        event.observed_price,
                    )
                    inserted += int(str(db_result).endswith(" 1"))
        else:
            await conn.execute("BEGIN IMMEDIATE")
            try:
                cursor = await conn.execute(
                    """
                    UPDATE signal_analytics_signals
                    SET status=?,zone_touched_at=?,activated_at=?,activated_price=?,
                        max_tp_index=?,be_armed_at=?,completed_at=?,
                        terminal_reason=?,ambiguous_reason=?,
                        last_observed_at=?,last_observed_price=?,
                        state_version=state_version+1,
                        needs_recovery=?,recovery_status=?,
                        recovery_method='bingx_1m_ohlc_conservative',
                        recovery_completed_at=?,recovery_confidence=?,recovery_cursor_at=?,
                        recovery_next_attempt_at=NULL,recovery_processing_started_at=NULL,
                        recovery_lease_token=NULL,recovery_last_error=NULL,
                        data_quality_status=?,data_quality_reason=?,updated_at=CURRENT_TIMESTAMP
                    WHERE id=? AND state_version=? AND recovery_lease_token=?
                    """,
                    (
                        result.status,
                        _db_dt(result.zone_touched_at),
                        _db_dt(result.activated_at),
                        _db_decimal(result.activated_price),
                        result.max_tp_index,
                        _db_dt(result.be_armed_at),
                        _db_dt(result.completed_at),
                        result.terminal_reason,
                        result.ambiguous_reason,
                        _db_dt(result.last_observed_at),
                        _db_decimal(result.last_observed_price),
                        needs_recovery,
                        recovery_status,
                        now.isoformat(),
                        "high" if result.exact else "none",
                        _db_dt(recovery_cursor_at),
                        quality,
                        quality_reason,
                        signal_id,
                        expected_version,
                        lease,
                    ),
                )
                if int(getattr(cursor, "rowcount", 0) or 0) != 1:
                    await conn.rollback()
                    return GapRecoveryOutcome(signal_id, "recovery_lease_lost", "lease_lost")
                inserted = 0
                for event in result.events:
                    await conn.execute(
                        """
                        INSERT OR IGNORE INTO signal_analytics_level_events(
                          signal_id,event_key,event_type,level_index,
                          observed_at,observed_price,created_at
                        ) VALUES(?,?,?,?,?,?,CURRENT_TIMESTAMP)
                        """,
                        (
                            signal_id,
                            event.event_key,
                            event.event_type,
                            event.level_index,
                            event.observed_at.isoformat(),
                            format(event.observed_price, "f"),
                        ),
                    )
                    changes = await conn.execute("SELECT changes()")
                    inserted += int((await changes.fetchone())[0] or 0)
                await conn.commit()
            except BaseException:
                await conn.rollback()
                raise
    return GapRecoveryOutcome(
        signal_id=signal_id,
        action="recovered_exact" if result.exact else "recovered_ambiguous",
        recovery_status=recovery_status,
        events=inserted,
        error=result.ambiguous_reason,
    )


async def requeue_statistics_restart_gap_after_tracker_refresh_failure(
    signal_id: int,
    *,
    error: str,
    now: datetime | None = None,
) -> bool:
    """Return an exactly replayed live row to recovery if registry admission fails.

    Clearing ``needs_recovery`` is safe only when the in-memory tracker is
    refreshed immediately. A DB/read failure during that handoff would create a
    second blind interval. Re-queue only still-live exact rows and start the new
    gap at the already durable ``recovery_cursor_at``. Terminal rows need no
    live admission and are intentionally left complete.
    """

    current = _utc(now or _utc_now())
    retry_at = current + timedelta(
        seconds=max(60.0, float(get_settings().STATISTICS_GAP_RECOVERY_RETRY_SEC))
    )
    safe_error = _safe_error(error)
    async with db.connect() as conn:
        if db.is_postgres():
            result = await conn.execute(
                """
                UPDATE signal_analytics_signals
                SET needs_recovery=1,recovery_status='retry',
                    recovery_method='bingx_1m_ohlc_conservative',
                    recovery_started_at=$1,recovery_completed_at=NULL,
                    recovery_confidence='none',recovery_attempts=0,
                    recovery_next_attempt_at=$2,
                    recovery_processing_started_at=NULL,recovery_lease_token=NULL,
                    recovery_last_error=$3,
                    data_quality_status='recovery_required',
                    data_quality_reason='restart_gap_tracker_admission_failed',
                    updated_at=NOW()
                WHERE id=$4
                  AND status IN ('waiting_entry','active')
                  AND COALESCE(needs_recovery,0)=0
                  AND recovery_status='recovered_exact'
                  AND recovery_cursor_at IS NOT NULL
                """,
                current,
                retry_at,
                safe_error,
                int(signal_id),
            )
            return str(result).endswith(" 1")
        cursor = await conn.execute(
            """
            UPDATE signal_analytics_signals
            SET needs_recovery=1,recovery_status='retry',
                recovery_method='bingx_1m_ohlc_conservative',
                recovery_started_at=?,recovery_completed_at=NULL,
                recovery_confidence='none',recovery_attempts=0,
                recovery_next_attempt_at=?,
                recovery_processing_started_at=NULL,recovery_lease_token=NULL,
                recovery_last_error=?,
                data_quality_status='recovery_required',
                data_quality_reason='restart_gap_tracker_admission_failed',
                updated_at=CURRENT_TIMESTAMP
            WHERE id=?
              AND status IN ('waiting_entry','active')
              AND COALESCE(needs_recovery,0)=0
              AND recovery_status='recovered_exact'
              AND recovery_cursor_at IS NOT NULL
            """,
            (current.isoformat(), retry_at.isoformat(), safe_error, int(signal_id)),
        )
        await conn.commit()
        return int(getattr(cursor, "rowcount", 0) or 0) == 1


async def _default_adapter_loader() -> BingxAdapter:
    return BingxAdapter("", "")


async def recover_statistics_restart_gap_once(
    *,
    adapter_loader: AdapterLoader | None = None,
    now: datetime | None = None,
) -> GapRecoveryOutcome | None:
    """Claim and recover at most one gap outside all trading hot paths."""

    current = _utc(now or _utc_now())
    row = await _claim_due_signal(now=current)
    if not row:
        return None
    signal_id = int(row["id"])
    try:
        gap_start = _latest_dt(
            row.get("recovery_cursor_at"),
            row.get("last_observed_at"),
            row.get("tracking_started_at"),
            row.get("published_at"),
        )
        # Recover through the current request snapshot, not merely through the
        # process-start marker. The row is deliberately absent from the live
        # tracker while recovery is pending; stopping at process start would
        # create a second blind interval before the registry is reloaded.
        gap_end = current
        if gap_start is None or gap_end < gap_start:
            raise PermanentGapRecoveryError("restart gap boundaries are invalid")
        max_hours = max(1, int(get_settings().STATISTICS_GAP_RECOVERY_MAX_HOURS))
        detected_gap_end = _dt(row.get("recovery_started_at"))
        if detected_gap_end is None or detected_gap_end < gap_start:
            raise PermanentGapRecoveryError("restart gap detection boundary is invalid")
        # Apply the configured cap to the downtime known at startup. A bounded
        # worker/API delay after detection must not make an otherwise eligible
        # 24-hour gap permanently unrecoverable; the replay still catches up to
        # ``current`` before the row is re-admitted to live tracking.
        if detected_gap_end - gap_start > timedelta(hours=max_hours):
            raise PermanentGapRecoveryError(
                "restart gap exceeds configured recovery window"
            )
        if gap_end == gap_start:
            synthetic_close = _decimal(row.get("last_observed_price")) or _decimal(
                row.get("entry_reference")
            )
            if synthetic_close is None:
                raise PermanentGapRecoveryError(
                    "zero-length recovery has no baseline price"
                )
            replay = GapReplayResult(
                status=str(row.get("status")),
                zone_touched_at=_dt(row.get("zone_touched_at")),
                activated_at=_dt(row.get("activated_at")),
                activated_price=_decimal(row.get("activated_price")),
                max_tp_index=int(row.get("max_tp_index") or 0),
                be_armed_at=_dt(row.get("be_armed_at")),
                completed_at=_dt(row.get("completed_at")),
                terminal_reason=str(row.get("terminal_reason") or "") or None,
                ambiguous_reason=None,
                last_observed_at=gap_end,
                last_observed_price=synthetic_close,
                events=(),
                exact=True,
            )
            return await _finalize(
                row,
                result=replay,
                now=current,
                recovery_cursor_at=gap_end,
            )

        start_ms = math.floor(gap_start.timestamp() / 60) * _CANDLE_MS
        end_ms = math.ceil(gap_end.timestamp() / 60) * _CANDLE_MS
        loader = adapter_loader or _default_adapter_loader
        loaded = loader()
        adapter = await loaded if inspect.isawaitable(loaded) else loaded
        if not hasattr(adapter, "fetch_public_klines"):
            raise PermanentGapRecoveryError(
                "statistics gap adapter has no public kline reader"
            )
        raw: list[Mapping[str, Any]] = []
        cursor_ms = int(start_ms)
        try:
            # A 24-hour gap can span 1441 calendar minutes when both boundaries
            # are partial. Fetch in non-overlapping chunks instead of silently
            # making the configured 24h limit unrecoverable at that edge.
            while cursor_ms < int(end_ms):
                remaining = math.ceil((int(end_ms) - cursor_ms) / _CANDLE_MS)
                chunk_limit = max(1, min(1440, remaining))
                chunk_end_exclusive = min(
                    int(end_ms), cursor_ms + chunk_limit * _CANDLE_MS
                )
                rows = await adapter.fetch_public_klines(
                    symbol=str(row.get("symbol")),
                    start_time_ms=cursor_ms,
                    # BingX endTime is inclusive. Keep it inside the final
                    # requested candle so a boundary candle cannot displace the
                    # first row when the exchange applies ``limit`` from the end.
                    end_time_ms=chunk_end_exclusive - 1,
                    interval="1m",
                    limit=chunk_limit,
                )
                raw.extend(rows)
                cursor_ms = chunk_end_exclusive
        finally:
            close = getattr(adapter, "close", None)
            if callable(close):
                try:
                    maybe = close()
                    if inspect.isawaitable(maybe):
                        await maybe
                except Exception:
                    log.warning(
                        "STATISTICS_RESTART_GAP_ADAPTER_CLOSE_FAILED signal_id=%s",
                        signal_id,
                        exc_info=True,
                    )
        candles = normalize_gap_candles(raw)
        validate_gap_coverage(candles, start=gap_start, end=gap_end)
        replay = replay_restart_gap(
            row,
            candles,
            gap_start=gap_start,
            gap_end=gap_end,
        )
        outcome = await _finalize(
            row,
            result=replay,
            now=current,
            recovery_cursor_at=gap_end,
        )
        log.info(
            "STATISTICS_RESTART_GAP_RECOVERY signal_id=%s action=%s status=%s events=%s",
            signal_id,
            outcome.action,
            outcome.recovery_status,
            outcome.events,
        )
        return outcome
    except Exception as exc:
        log.warning(
            "STATISTICS_RESTART_GAP_RECOVERY_RETRY signal_id=%s error=%s detail=%s",
            signal_id,
            type(exc).__name__,
            _safe_error(exc, limit=300),
        )
        return await _reschedule(
            row,
            now=current,
            error=f"{type(exc).__name__}: {_safe_error(exc)}",
            permanent=isinstance(exc, PermanentGapRecoveryError),
        )
