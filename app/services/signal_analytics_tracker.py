"""In-memory source-signal level tracking with durable, idempotent transitions.

The tracker is deliberately passive:
- it consumes public prices already fetched by the event-driven monitor;
- it never calls BingX itself;
- it never mutates trade executions, STOP, TP, BE or risk slots;
- it performs no SQL on the public-price hot path;
- all durable writes are returned as transition objects for the existing single
  analytics DB writer.

Stage g5b3g1 tracks the source plan using the parsed midpoint entry. A source
instruction to move the remainder to breakeven after TP1 is recognized during
shadow ingress and persisted as ``be_trigger_tp_index=1``. Step g5b3g14 adds an
optional forward-only restart mode. g35 replaces that behavior for future gaps
with a separate leased, conservative 1-minute candle replay. Rows are loaded
again only after an exact replay clears ``needs_recovery``; historical
``forward_resumed`` rows remain excluded because no pre-gap snapshot exists.
This module never invents intrabar order.
"""

from __future__ import annotations

import heapq
import json
import logging
from dataclasses import dataclass, replace
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any, Callable, Iterable

from app.config import get_settings
from app.database.db import connect, is_postgres

log = logging.getLogger(__name__)
_PROCESS_STARTED_AT = datetime.now(timezone.utc)

_TRACKABLE_STATUSES = {"waiting_entry", "active"}
_TERMINAL_STATUSES = {
    "completed_tp",
    "completed_be",
    "completed_stop",
    "expired_not_entered",
    "ambiguous",
}


def _utc(value: Any) -> datetime:
    if isinstance(value, datetime):
        parsed = value
    else:
        parsed = datetime.now(timezone.utc)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _dt(value: Any) -> datetime | None:
    if value in (None, ""):
        return None
    if isinstance(value, datetime):
        return _utc(value)
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except (TypeError, ValueError):
        return None
    return _utc(parsed)


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


def _decimal_db(value: Decimal | None) -> Any:
    if value is None:
        return None
    return value if is_postgres() else format(value, "f")


def _datetime_db(value: datetime | None) -> Any:
    if value is None:
        return None
    normalized = _utc(value)
    return normalized if is_postgres() else normalized.isoformat()


def _targets(value: Any) -> tuple[Decimal, ...]:
    try:
        raw = json.loads(str(value or "[]"))
    except (TypeError, ValueError, json.JSONDecodeError):
        return ()
    if not isinstance(raw, list):
        return ()
    result: list[Decimal] = []
    for item in raw[:20]:
        parsed = _decimal(item)
        if parsed is None:
            return ()
        result.append(parsed)
    return tuple(result)


@dataclass(frozen=True, slots=True)
class AnalyticsLevelEvent:
    event_key: str
    event_type: str
    level_index: int
    observed_at: datetime
    observed_price: Decimal


@dataclass(frozen=True, slots=True)
class AnalyticsTrackingTransition:
    signal_id: int
    expected_version: int
    new_version: int
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
    events: tuple[AnalyticsLevelEvent, ...]


@dataclass(frozen=True, slots=True)
class TrackedAnalyticsSignal:
    id: int
    symbol: str
    side: str
    order_type: str
    entry_low: Decimal
    entry_high: Decimal
    entry_reference: Decimal
    activated_price: Decimal | None
    stop_price: Decimal
    targets: tuple[Decimal, ...]
    be_trigger_tp_index: int
    status: str
    expiry_at: datetime | None
    zone_touched_at: datetime | None
    activated_at: datetime | None
    max_tp_index: int
    be_armed_at: datetime | None
    completed_at: datetime | None
    terminal_reason: str | None
    ambiguous_reason: str | None
    state_version: int
    last_observed_at: datetime | None
    last_observed_price: Decimal | None
    recovery_status: str = "not_required"

    @property
    def entry_price(self) -> Decimal | None:
        if self.order_type == "MARKET":
            return self.activated_price
        return self.entry_reference


TransitionSink = Callable[[AnalyticsTrackingTransition], bool]


class SignalAnalyticsRegistry:
    """Bounded process-local registry indexed by symbol."""

    def __init__(self) -> None:
        self._by_id: dict[int, TrackedAnalyticsSignal] = {}
        # Accepted transitions are optimistic until the single DB writer
        # acknowledges them. Keeping a separate pending map prevents a refresh
        # that started before the COMMIT from resurrecting an old ACTIVE row,
        # especially after an optimistic terminal TP/BE/STOP transition has
        # already been removed from the symbol index.
        self._pending_by_id: dict[int, TrackedAnalyticsSignal] = {}
        self._ids_by_symbol: dict[str, set[int]] = {}
        self._expiry_heap: list[tuple[float, int, int]] = []
        self._started = False
        self._refreshes = 0
        self._loaded = 0
        self._invalid_rows = 0
        self._price_snapshots = 0
        self._price_checks = 0
        self._transitions_queued = 0
        self._transitions_dropped = 0
        self._events_queued = 0
        self._last_refresh_at: datetime | None = None
        self._restart_gaps_quarantined = 0
        self._restart_gaps_forward_resumed = 0
        self._restart_gaps_recovery_pending = 0
        self._restart_baseline_complete_ids: set[int] = set()
        self._restart_baseline_ambiguous = 0
        self._out_of_order_snapshots = 0
        self._capacity_overflow = 0
        # A transition that was observed but could not be admitted to the
        # bounded writer queue must never be forgotten and followed by a later
        # contradictory outcome. Quarantine it until a durable recovery marker
        # is written by the low-priority analytics worker.
        self._recovery_quarantine_ids: set[int] = set()
        self._tracking_quarantined = 0
        self._capacity_rows_quarantined = 0

    def clear(self, *, preserve_recovery_quarantine: bool = False) -> None:
        self._by_id.clear()
        self._pending_by_id.clear()
        self._ids_by_symbol.clear()
        self._expiry_heap.clear()
        if not preserve_recovery_quarantine:
            self._recovery_quarantine_ids.clear()
        self._restart_baseline_complete_ids.clear()
        self._started = False

    def _index(self, row: TrackedAnalyticsSignal) -> None:
        self._by_id[row.id] = row
        self._ids_by_symbol.setdefault(row.symbol, set()).add(row.id)
        if row.status == "waiting_entry" and row.expiry_at is not None:
            heapq.heappush(
                self._expiry_heap,
                (row.expiry_at.timestamp(), row.id, row.state_version),
            )

    def _remove(self, signal_id: int) -> None:
        old = self._by_id.pop(int(signal_id), None)
        if old is None:
            return
        ids = self._ids_by_symbol.get(old.symbol)
        if ids is not None:
            ids.discard(old.id)
            if not ids:
                self._ids_by_symbol.pop(old.symbol, None)

    def _replace(self, row: TrackedAnalyticsSignal) -> None:
        self._remove(row.id)
        if row.status in _TRACKABLE_STATUSES:
            self._index(row)

    def symbols(self) -> tuple[str, ...]:
        if not self._started:
            return ()
        return tuple(sorted(self._ids_by_symbol))

    def stats(self) -> dict[str, int]:
        waiting = sum(1 for row in self._by_id.values() if row.status == "waiting_entry")
        active = sum(1 for row in self._by_id.values() if row.status == "active")
        return {
            "tracker_started": int(self._started),
            "tracking_active_rows": len(self._by_id),
            "tracking_pending_writes": len(self._pending_by_id),
            "tracking_waiting_rows": waiting,
            "tracking_open_rows": active,
            "tracking_symbols": len(self._ids_by_symbol),
            "tracking_refreshes": self._refreshes,
            "tracking_loaded": self._loaded,
            "tracking_invalid_rows": self._invalid_rows,
            "tracking_price_snapshots": self._price_snapshots,
            "tracking_price_checks": self._price_checks,
            "tracking_transitions_queued": self._transitions_queued,
            "tracking_transitions_dropped": self._transitions_dropped,
            "tracking_events_queued": self._events_queued,
            "tracking_restart_gaps_quarantined": self._restart_gaps_quarantined,
            "tracking_restart_gaps_forward_resumed": (
                self._restart_gaps_forward_resumed
            ),
            "tracking_restart_gaps_recovery_pending": (
                self._restart_gaps_recovery_pending
            ),
            "tracking_restart_baselines": len(self._restart_baseline_complete_ids),
            "tracking_restart_baseline_ambiguous": self._restart_baseline_ambiguous,
            "tracking_out_of_order_snapshots": self._out_of_order_snapshots,
            "tracking_capacity_overflow": self._capacity_overflow,
            "tracking_capacity_rows_quarantined": self._capacity_rows_quarantined,
            "tracking_quarantined": self._tracking_quarantined,
            "tracking_recovery_marks_pending": len(self._recovery_quarantine_ids),
        }

    @staticmethod
    def _merge_refreshed_row(
        previous: TrackedAnalyticsSignal | None,
        parsed: TrackedAnalyticsSignal,
    ) -> TrackedAnalyticsSignal:
        if previous is None:
            return parsed
        if previous.state_version > parsed.state_version:
            # A public-price transition may be admitted while refresh is awaiting
            # PostgreSQL. Keep the optimistic state until the serialized writer
            # persists it; reverting could miss the next BE/STOP crossing.
            return previous
        if (
            previous.state_version == parsed.state_version
            and previous.last_observed_at is not None
            and (
                parsed.last_observed_at is None
                or previous.last_observed_at > parsed.last_observed_at
            )
        ):
            # Ordinary non-event ticks are deliberately process-local. Preserve
            # their chronology across a refresh caused by an unrelated ingress.
            return replace(
                parsed,
                last_observed_at=previous.last_observed_at,
                last_observed_price=previous.last_observed_price,
            )
        return parsed

    def _quarantine_for_recovery(self, signal_id: int, *, reason: str) -> bool:
        """Stop observing one signal after a lost/non-durable transition.

        Continuing from the old state could turn an observed TP into a later
        false ``stop_no_tp``. The marker is persisted asynchronously outside the
        public-price hot path.
        """

        normalized_id = int(signal_id)
        added = normalized_id not in self._recovery_quarantine_ids
        self._recovery_quarantine_ids.add(normalized_id)
        self._pending_by_id.pop(normalized_id, None)
        self._remove(normalized_id)
        if added:
            self._tracking_quarantined += 1
            log.error(
                "SIGNAL_ANALYTICS_TRACKING_QUARANTINED signal_id=%s reason=%s "
                "action=needs_recovery",
                normalized_id,
                reason,
            )
        return added

    def quarantine_transitions(
        self,
        transitions: Iterable[AnalyticsTrackingTransition],
        *,
        reason: str,
    ) -> int:
        ids = {int(item.signal_id) for item in transitions}
        return sum(
            int(self._quarantine_for_recovery(signal_id, reason=reason))
            for signal_id in ids
        )

    def recovery_quarantine_ids(self, *, limit: int = 1000) -> tuple[int, ...]:
        bounded = max(1, int(limit))
        return tuple(sorted(self._recovery_quarantine_ids)[:bounded])

    def acknowledge_recovery_quarantine(self, signal_ids: Iterable[int]) -> int:
        removed = 0
        for signal_id in signal_ids:
            normalized_id = int(signal_id)
            if normalized_id in self._recovery_quarantine_ids:
                self._recovery_quarantine_ids.discard(normalized_id)
                removed += 1
        return removed

    def acknowledge_transitions(
        self, transitions: Iterable[AnalyticsTrackingTransition]
    ) -> int:
        """Drop optimistic markers that are now durable or superseded.

        A newer transition can be admitted while an older batch is awaiting the
        database. In that case the newer pending version must remain.
        """

        acknowledged: dict[int, int] = {}
        for transition in transitions:
            acknowledged[transition.signal_id] = max(
                acknowledged.get(transition.signal_id, 0),
                transition.new_version,
            )
        removed = 0
        for signal_id, version in acknowledged.items():
            pending = self._pending_by_id.get(signal_id)
            if pending is not None and pending.state_version <= version:
                self._pending_by_id.pop(signal_id, None)
                removed += 1
        return removed

    def reject_transitions(
        self, transitions: Iterable[AnalyticsTrackingTransition]
    ) -> int:
        """Discard matching optimistic states after an unexpected writer failure.

        Normal database failures are retried and never call this path. This is a
        last-resort consistency guard for a programming/non-DB failure at the
        worker boundary: remove only the rejected version, then let the caller
        reload the durable row. A newer admitted transition is preserved.
        """

        rejected: dict[int, int] = {}
        for transition in transitions:
            rejected[transition.signal_id] = max(
                rejected.get(transition.signal_id, 0), transition.new_version
            )
        removed = 0
        for signal_id, version in rejected.items():
            pending = self._pending_by_id.get(signal_id)
            if pending is not None and pending.state_version <= version:
                self._pending_by_id.pop(signal_id, None)
                removed += 1
            current = self._by_id.get(signal_id)
            if current is not None and current.state_version <= version:
                self._remove(signal_id)
        return removed

    async def _quarantine_restart_gaps(self) -> int:
        """Classify pre-process open signals without inventing gap history.

        With step-4 recovery disabled, the existing fail-closed behavior is
        preserved: old WAITING/ACTIVE rows are marked ``needs_recovery`` and are
        not loaded into the process registry.

        With ``STATISTICS_RECOVERY_ENABLED`` enabled, newly detected gaps are
        moved to a leased ``pending`` queue. The low-priority recovery worker
        replays validated BingX 1-minute candles conservatively before the row
        is loaded again. Historical ``forward_resumed`` rows from older builds
        remain excluded because their pre-gap state snapshot no longer exists.
        """

        settings = get_settings()
        recovery_enabled = bool(settings.STATISTICS_RECOVERY_ENABLED)
        if bool(settings.SIGNAL_ANALYTICS_RECOVERY_ENABLED) and not recovery_enabled:
            log.warning(
                "SIGNAL_ANALYTICS_RECOVERY_LEGACY_FLAG_RESERVED "
                "action=restart_gap_quarantine "
                "required_flag=STATISTICS_RECOVERY_ENABLED"
            )

        if is_postgres():
            if recovery_enabled:
                query = """
                UPDATE signal_analytics_signals
                SET needs_recovery=1,
                    recovery_status='pending',
                    recovery_method='bingx_1m_ohlc_conservative',
                    recovery_started_at=NOW(),
                    recovery_completed_at=NULL,
                    recovery_confidence='none',
                    recovery_attempts=0,
                    recovery_next_attempt_at=NOW(),
                    recovery_processing_started_at=NULL,
                    recovery_lease_token=NULL,
                    recovery_last_error=NULL,
                    recovery_cursor_at=COALESCE(
                      recovery_cursor_at,last_observed_at,tracking_started_at,published_at
                    ),
                    data_quality_status='recovery_required',
                    data_quality_reason='restart_gap_pending_candle_replay',
                    updated_at=NOW()
                WHERE status IN ('waiting_entry','active')
                  AND (tracking_started_at IS NULL OR tracking_started_at < $1)
                  AND COALESCE(needs_recovery,0)=0
                """
            else:
                query = """
                UPDATE signal_analytics_signals
                SET needs_recovery=1,
                    recovery_status='pending',
                    recovery_method='none',
                    recovery_started_at=COALESCE(recovery_started_at,NOW()),
                    recovery_completed_at=NULL,
                    recovery_confidence='none',
                    recovery_cursor_at=COALESCE(
                      recovery_cursor_at,last_observed_at,tracking_started_at,published_at
                    ),
                    data_quality_status='recovery_required',
                    data_quality_reason='restart_gap_recovery_disabled',
                    updated_at=NOW()
                WHERE status IN ('waiting_entry','active')
                  AND (tracking_started_at IS NULL OR tracking_started_at < $1)
                  AND (
                    COALESCE(needs_recovery,0)=0
                    OR recovery_status='forward_resumed'
                  )
                """
        else:
            if recovery_enabled:
                query = """
                UPDATE signal_analytics_signals
                SET needs_recovery=1,
                    recovery_status='pending',
                    recovery_method='bingx_1m_ohlc_conservative',
                    recovery_started_at=CURRENT_TIMESTAMP,
                    recovery_completed_at=NULL,
                    recovery_confidence='none',
                    recovery_attempts=0,
                    recovery_next_attempt_at=CURRENT_TIMESTAMP,
                    recovery_processing_started_at=NULL,
                    recovery_lease_token=NULL,
                    recovery_last_error=NULL,
                    recovery_cursor_at=COALESCE(
                      recovery_cursor_at,last_observed_at,tracking_started_at,published_at
                    ),
                    data_quality_status='recovery_required',
                    data_quality_reason='restart_gap_pending_candle_replay',
                    updated_at=CURRENT_TIMESTAMP
                WHERE status IN ('waiting_entry','active')
                  AND (tracking_started_at IS NULL OR tracking_started_at < ?)
                  AND COALESCE(needs_recovery,0)=0
                """
            else:
                query = """
                UPDATE signal_analytics_signals
                SET needs_recovery=1,
                    recovery_status='pending',
                    recovery_method='none',
                    recovery_started_at=COALESCE(
                      recovery_started_at,CURRENT_TIMESTAMP
                    ),
                    recovery_completed_at=NULL,
                    recovery_confidence='none',
                    recovery_cursor_at=COALESCE(
                      recovery_cursor_at,last_observed_at,tracking_started_at,published_at
                    ),
                    data_quality_status='recovery_required',
                    data_quality_reason='restart_gap_recovery_disabled',
                    updated_at=CURRENT_TIMESTAMP
                WHERE status IN ('waiting_entry','active')
                  AND (tracking_started_at IS NULL OR tracking_started_at < ?)
                  AND (
                    COALESCE(needs_recovery,0)=0
                    OR recovery_status='forward_resumed'
                  )
                """

        async with connect() as conn:
            if is_postgres():
                result = await conn.execute(query, _PROCESS_STARTED_AT)
                try:
                    count = int(str(result).rsplit(" ", 1)[-1])
                except (TypeError, ValueError):
                    count = 0
            else:
                cursor = await conn.execute(query, (_PROCESS_STARTED_AT.isoformat(),))
                count = max(0, int(getattr(cursor, "rowcount", 0) or 0))
                await conn.commit()

        if count:
            if recovery_enabled:
                self._restart_gaps_recovery_pending += count
                log.warning(
                    "SIGNAL_ANALYTICS_RESTART_GAP_PENDING rows=%s "
                    "action=conservative_1m_candle_replay",
                    count,
                )
            else:
                self._restart_gaps_quarantined += count
                log.warning(
                    "SIGNAL_ANALYTICS_RESTART_GAP_QUARANTINED rows=%s "
                    "reason=recovery_disabled",
                    count,
                )
        return count


    async def _quarantine_capacity_overflow(self, limit: int) -> int:
        """Durably exclude rows that cannot fit in the bounded registry."""

        bounded = max(1, int(limit))
        # Historical ``forward_resumed`` rows were mutated by older builds
        # without a saved pre-gap snapshot. Loading them again would continue a
        # statistically corrupted timeline. Only rows with no unresolved gap,
        # including rows completed by the new exact candle replay, are eligible.
        eligible = "COALESCE(needs_recovery,0)=0"
        async with connect() as conn:
            if is_postgres():
                rows = await conn.fetch(
                    f"""
                    WITH keep AS (
                      SELECT id
                      FROM signal_analytics_signals
                      WHERE status IN ('waiting_entry','active')
                        AND {eligible}
                      ORDER BY published_at DESC,id DESC
                      LIMIT $1
                    ), overflow AS (
                      SELECT id
                      FROM signal_analytics_signals
                      WHERE status IN ('waiting_entry','active')
                        AND {eligible}
                        AND id NOT IN (SELECT id FROM keep)
                    )
                    UPDATE signal_analytics_signals AS target
                    SET needs_recovery=1,
                        recovery_status='unavailable',
                        recovery_method='none',
                        recovery_completed_at=NOW(),
                        recovery_confidence='none',
                        data_quality_status='recovery_required',
                        data_quality_reason='tracker_capacity_overflow',
                        updated_at=NOW()
                    FROM overflow
                    WHERE target.id=overflow.id
                    RETURNING target.id
                    """,
                    bounded,
                )
                count = len(rows)
            else:
                cursor = await conn.execute(
                    f"""
                    SELECT id
                    FROM signal_analytics_signals
                    WHERE status IN ('waiting_entry','active')
                      AND {eligible}
                    ORDER BY published_at DESC,id DESC
                    LIMIT -1 OFFSET ?
                    """,
                    (bounded,),
                )
                ids = [int(row[0]) for row in await cursor.fetchall()]
                for start in range(0, len(ids), 400):
                    chunk = ids[start : start + 400]
                    placeholders = ",".join("?" for _ in chunk)
                    await conn.execute(
                        f"""
                        UPDATE signal_analytics_signals
                        SET needs_recovery=1,
                            recovery_status='unavailable',
                            recovery_method='none',
                            recovery_completed_at=CURRENT_TIMESTAMP,
                            recovery_confidence='none',
                            data_quality_status='recovery_required',
                            data_quality_reason='tracker_capacity_overflow',
                            updated_at=CURRENT_TIMESTAMP
                        WHERE status IN ('waiting_entry','active')
                          AND {eligible}
                          AND id IN ({placeholders})
                        """,
                        tuple(chunk),
                    )
                await conn.commit()
                count = len(ids)
        if count:
            self._capacity_overflow += count
            self._capacity_rows_quarantined += count
            log.error(
                "SIGNAL_ANALYTICS_TRACKER_CAPACITY_QUARANTINED rows=%s limit=%s "
                "action=needs_recovery",
                count,
                bounded,
            )
        return count

    async def refresh(self) -> int:
        settings = get_settings()
        if not (
            bool(settings.SIGNAL_ANALYTICS_ENABLED)
            and bool(settings.SIGNAL_ANALYTICS_TRACKING_ENABLED)
        ):
            # A runtime feature-flag change must not erase a transition that
            # was already observed but is still awaiting a durable recovery
            # marker. The worker will flush it when analytics is re-enabled.
            self.clear(preserve_recovery_quarantine=True)
            return 0

        if not self._started:
            await self._quarantine_restart_gaps()

        limit = max(100, int(settings.SIGNAL_ANALYTICS_MAX_ACTIVE))
        eligible = "COALESCE(needs_recovery,0)=0"
        query_pg = f"""
        SELECT id,symbol,side,order_type,entry_low,entry_high,entry_reference,
               activated_price,stop_price,targets_json,be_trigger_tp_index,status,
               expiry_at,zone_touched_at,activated_at,max_tp_index,be_armed_at,
               completed_at,terminal_reason,ambiguous_reason,state_version,
               last_observed_at,last_observed_price,recovery_status
        FROM signal_analytics_signals
        WHERE status IN ('waiting_entry','active')
          AND {eligible}
        ORDER BY published_at DESC,id DESC
        LIMIT $1
        """
        query_sqlite = query_pg.replace("LIMIT $1", "LIMIT ?")
        fetch_limit = limit + 1

        async def _fetch_rows() -> list[Any]:
            async with connect() as conn:
                if is_postgres():
                    return list(await conn.fetch(query_pg, fetch_limit))
                cursor = await conn.execute(query_sqlite, (fetch_limit,))
                return list(await cursor.fetchall())

        rows = await _fetch_rows()
        if len(rows) > limit:
            await self._quarantine_capacity_overflow(limit)
            rows = await _fetch_rows()

        overflow = max(0, len(rows) - limit)
        if overflow:
            # A concurrent insert can race the quarantine/refetch. Keep the hot
            # registry bounded now; the next refresh will durably quarantine the
            # residual row instead of silently growing memory.
            rows = rows[:limit]
            self._capacity_overflow += overflow
            log.error(
                "SIGNAL_ANALYTICS_TRACKER_CAPACITY_EXCEEDED loaded=%s limit=%s "
                "overflow_at_least=%s action=concurrent_rows_skipped",
                len(rows),
                limit,
                overflow,
            )

        new_rows: dict[int, TrackedAnalyticsSignal] = {}
        invalid = 0
        for source in rows:
            row = dict(source)
            try:
                parsed = self._parse_row(row)
            except Exception:
                invalid += 1
                log.exception(
                    "SIGNAL_ANALYTICS_TRACKER_INVALID_ROW signal_id=%s symbol=%s",
                    row.get("id"),
                    row.get("symbol"),
                )
                continue
            if parsed.id in self._recovery_quarantine_ids:
                # A queue/worker loss was observed in this process but its
                # durable marker may still be awaiting the writer. Never reload
                # the old state in the meantime.
                continue
            previous = self._pending_by_id.get(parsed.id) or self._by_id.get(parsed.id)
            parsed = self._merge_refreshed_row(previous, parsed)
            new_rows[parsed.id] = parsed

        self._by_id.clear()
        self._ids_by_symbol.clear()
        self._expiry_heap.clear()
        for parsed in new_rows.values():
            if parsed.status in _TRACKABLE_STATUSES:
                self._index(parsed)
        self._started = True
        self._refreshes += 1
        self._loaded = len(new_rows)
        self._invalid_rows += invalid
        self._last_refresh_at = datetime.now(timezone.utc)
        log.info(
            "SIGNAL_ANALYTICS_TRACKER_REFRESH active=%s symbols=%s invalid=%s limit=%s",
            len(self._by_id),
            len(self._ids_by_symbol),
            invalid,
            limit,
        )
        return len(new_rows)

    @staticmethod
    def _parse_row(row: dict[str, Any]) -> TrackedAnalyticsSignal:
        signal_id = int(row.get("id") or 0)
        symbol = str(row.get("symbol") or "").upper().strip()
        side = str(row.get("side") or "").lower().strip()
        order_type = str(row.get("order_type") or "LIMIT").upper().strip()
        entry_low = _decimal(row.get("entry_low"), positive=(order_type != "MARKET"))
        entry_high = _decimal(row.get("entry_high"), positive=(order_type != "MARKET"))
        entry_reference = _decimal(
            row.get("entry_reference"), positive=(order_type != "MARKET")
        )
        activated_price = _decimal(row.get("activated_price"))
        stop_price = _decimal(row.get("stop_price"))
        targets = _targets(row.get("targets_json"))
        status = str(row.get("status") or "").strip().lower()
        if (
            signal_id <= 0
            or not symbol
            or side not in {"long", "short"}
            or order_type not in {"LIMIT", "MARKET"}
            or entry_low is None
            or entry_high is None
            or entry_reference is None
            or stop_price is None
            or not targets
            or status not in _TRACKABLE_STATUSES
        ):
            raise ValueError("invalid analytics tracking row")
        if entry_low > entry_high:
            raise ValueError("entry range is reversed")
        if order_type == "LIMIT":
            if not (entry_low <= entry_reference <= entry_high):
                raise ValueError("entry reference is outside the entry range")
            if side == "long":
                if stop_price >= entry_reference:
                    raise ValueError("LONG stop is not below entry")
                if any(target <= entry_reference for target in targets):
                    raise ValueError("LONG targets are not above entry")
                if any(a >= b for a, b in zip(targets, targets[1:])):
                    raise ValueError("LONG targets are not strictly increasing")
            else:
                if stop_price <= entry_reference:
                    raise ValueError("SHORT stop is not above entry")
                if any(target >= entry_reference for target in targets):
                    raise ValueError("SHORT targets are not below entry")
                if any(a <= b for a, b in zip(targets, targets[1:])):
                    raise ValueError("SHORT targets are not strictly decreasing")
        be_trigger = max(0, min(len(targets), int(row.get("be_trigger_tp_index") or 0)))
        max_tp = max(0, min(len(targets), int(row.get("max_tp_index") or 0)))
        return TrackedAnalyticsSignal(
            id=signal_id,
            symbol=symbol,
            side=side,
            order_type=order_type,
            entry_low=entry_low,
            entry_high=entry_high,
            entry_reference=entry_reference,
            activated_price=activated_price,
            stop_price=stop_price,
            targets=targets,
            be_trigger_tp_index=be_trigger,
            status=status,
            expiry_at=_dt(row.get("expiry_at")),
            zone_touched_at=_dt(row.get("zone_touched_at")),
            activated_at=_dt(row.get("activated_at")),
            max_tp_index=max_tp,
            be_armed_at=_dt(row.get("be_armed_at")),
            completed_at=_dt(row.get("completed_at")),
            terminal_reason=(str(row.get("terminal_reason") or "").strip() or None),
            ambiguous_reason=(str(row.get("ambiguous_reason") or "").strip() or None),
            state_version=max(0, int(row.get("state_version") or 0)),
            last_observed_at=_dt(row.get("last_observed_at")),
            last_observed_price=_decimal(row.get("last_observed_price")),
            recovery_status=(
                str(row.get("recovery_status") or "not_required").strip().lower()
                or "not_required"
            ),
        )

    @staticmethod
    def _price_from_row(row: Any) -> Decimal | None:
        if isinstance(row, dict):
            return _decimal(row.get("last"))
        return _decimal(row)

    @staticmethod
    def _zone_touched(
        state: TrackedAnalyticsSignal, previous: Decimal | None, current: Decimal
    ) -> bool:
        if state.entry_low <= current <= state.entry_high:
            return True
        if previous is None:
            return False
        low = min(previous, current)
        high = max(previous, current)
        return low <= state.entry_high and high >= state.entry_low

    @staticmethod
    def _entry_reached(
        state: TrackedAnalyticsSignal, previous: Decimal | None, current: Decimal
    ) -> bool:
        if state.order_type == "MARKET":
            return True
        reference = state.entry_reference
        # A LIMIT at the parsed midpoint is marketable whenever the first
        # sampled quote is at or through that price on the fill side. The caller
        # still quarantines a first quote already beyond STOP/TP as ambiguous,
        # because the intrapoll path before observation is unknown.
        if previous is None:
            return current <= reference if state.side == "long" else current >= reference
        if state.side == "long":
            return previous > reference >= current or current == reference
        return previous < reference <= current or current == reference

    @staticmethod
    def _tp_reached(side: str, price: Decimal, target: Decimal) -> bool:
        return price >= target if side == "long" else price <= target

    @staticmethod
    def _stop_reached(side: str, price: Decimal, stop: Decimal) -> bool:
        return price <= stop if side == "long" else price >= stop

    @staticmethod
    def _be_reached(side: str, price: Decimal, entry: Decimal) -> bool:
        return price <= entry if side == "long" else price >= entry

    @staticmethod
    def _event(
        key: str,
        event_type: str,
        level_index: int,
        observed_at: datetime,
        price: Decimal,
    ) -> AnalyticsLevelEvent:
        return AnalyticsLevelEvent(
            event_key=key,
            event_type=event_type,
            level_index=int(level_index),
            observed_at=observed_at,
            observed_price=price,
        )

    def _waiting_transition(
        self,
        state: TrackedAnalyticsSignal,
        current: Decimal,
        observed_at: datetime,
    ) -> AnalyticsTrackingTransition | None:
        previous = state.last_observed_price
        events: list[AnalyticsLevelEvent] = []
        zone_touched_at = state.zone_touched_at
        if zone_touched_at is None and self._zone_touched(state, previous, current):
            zone_touched_at = observed_at
            events.append(self._event("ENTRY_ZONE", "ENTRY_ZONE", 0, observed_at, current))

        if self._entry_reached(state, previous, current):
            if previous is None:
                first_target = state.targets[0]
                outside_plan = self._stop_reached(
                    state.side, current, state.stop_price
                ) or self._tp_reached(state.side, current, first_target)
                if outside_plan:
                    reason_prefix = (
                        "market" if state.order_type == "MARKET" else "limit"
                    )
                    return AnalyticsTrackingTransition(
                        signal_id=state.id,
                        expected_version=state.state_version,
                        new_version=state.state_version + 1,
                        status="ambiguous",
                        zone_touched_at=zone_touched_at,
                        activated_at=None,
                        activated_price=None,
                        max_tp_index=0,
                        be_armed_at=None,
                        completed_at=observed_at,
                        terminal_reason="ambiguous",
                        ambiguous_reason=f"{reason_prefix}_first_price_outside_plan",
                        last_observed_at=observed_at,
                        last_observed_price=current,
                        events=(
                            self._event(
                                "AMBIGUOUS",
                                "AMBIGUOUS",
                                0,
                                observed_at,
                                current,
                            ),
                        ),
                    )
            activated_price = (
                current if state.order_type == "MARKET" else state.entry_reference
            )
            if state.order_type == "LIMIT" and zone_touched_at is None:
                # A first marketable quote can cross the midpoint without being
                # sampled inside the entry range. ENTRY implies that the range
                # was crossed, so persist both facts atomically.
                zone_touched_at = observed_at
                events.append(
                    self._event("ENTRY_ZONE", "ENTRY_ZONE", 0, observed_at, current)
                )
            elif state.order_type == "MARKET":
                # MARKET has no meaningful entry zone.
                zone_touched_at = None
            events.append(self._event("ENTRY", "ENTRY", 0, observed_at, activated_price))
            immediate_stop = (
                state.order_type == "LIMIT"
                and previous is not None
                and self._stop_reached(state.side, current, state.stop_price)
            )
            if immediate_stop:
                # Any continuous path from the previous quote through the LIMIT
                # midpoint to a quote beyond STOP must cross ENTRY before STOP.
                # Persist both events atomically instead of waiting for a later
                # tick that could rebound and hide the stop.
                events.append(self._event("STOP", "STOP", 0, observed_at, current))
            return AnalyticsTrackingTransition(
                signal_id=state.id,
                expected_version=state.state_version,
                new_version=state.state_version + 1,
                status="completed_stop" if immediate_stop else "active",
                zone_touched_at=zone_touched_at,
                activated_at=observed_at,
                activated_price=activated_price,
                max_tp_index=0,
                be_armed_at=None,
                completed_at=observed_at if immediate_stop else None,
                terminal_reason="stop_no_tp" if immediate_stop else None,
                ambiguous_reason=None,
                last_observed_at=observed_at,
                last_observed_price=current,
                events=tuple(events),
            )

        if events:
            return AnalyticsTrackingTransition(
                signal_id=state.id,
                expected_version=state.state_version,
                new_version=state.state_version + 1,
                status="waiting_entry",
                zone_touched_at=zone_touched_at,
                activated_at=None,
                activated_price=None,
                max_tp_index=0,
                be_armed_at=None,
                completed_at=None,
                terminal_reason=None,
                ambiguous_reason=None,
                last_observed_at=observed_at,
                last_observed_price=current,
                events=tuple(events),
            )
        return None

    def _active_transition(
        self,
        state: TrackedAnalyticsSignal,
        current: Decimal,
        observed_at: datetime,
    ) -> AnalyticsTrackingTransition | None:
        entry = state.entry_price
        if entry is None or entry <= 0:
            return AnalyticsTrackingTransition(
                signal_id=state.id,
                expected_version=state.state_version,
                new_version=state.state_version + 1,
                status="ambiguous",
                zone_touched_at=state.zone_touched_at,
                activated_at=state.activated_at,
                activated_price=state.activated_price,
                max_tp_index=state.max_tp_index,
                be_armed_at=state.be_armed_at,
                completed_at=observed_at,
                terminal_reason="ambiguous",
                ambiguous_reason="missing_activated_price",
                last_observed_at=observed_at,
                last_observed_price=current,
                events=(
                    self._event("AMBIGUOUS", "AMBIGUOUS", 0, observed_at, current),
                ),
            )

        events: list[AnalyticsLevelEvent] = []
        max_tp = state.max_tp_index
        be_armed_at = state.be_armed_at
        while max_tp < len(state.targets):
            target = state.targets[max_tp]
            if not self._tp_reached(state.side, current, target):
                break
            max_tp += 1
            events.append(
                self._event(f"TP{max_tp}", "TP", max_tp, observed_at, current)
            )
            if (
                state.be_trigger_tp_index > 0
                and max_tp >= state.be_trigger_tp_index
                and be_armed_at is None
            ):
                be_armed_at = observed_at
                events.append(
                    self._event(
                        "BE_ARMED",
                        "BE_ARMED",
                        state.be_trigger_tp_index,
                        observed_at,
                        current,
                    )
                )

        if max_tp >= len(state.targets):
            return AnalyticsTrackingTransition(
                signal_id=state.id,
                expected_version=state.state_version,
                new_version=state.state_version + 1,
                status="completed_tp",
                zone_touched_at=state.zone_touched_at,
                activated_at=state.activated_at,
                activated_price=state.activated_price,
                max_tp_index=max_tp,
                be_armed_at=be_armed_at,
                completed_at=observed_at,
                terminal_reason="all_targets",
                ambiguous_reason=None,
                last_observed_at=observed_at,
                last_observed_price=current,
                events=tuple(events),
            )

        if (
            state.be_trigger_tp_index > 0
            and max_tp >= state.be_trigger_tp_index
            and self._be_reached(state.side, current, entry)
        ):
            events.append(
                self._event("BREAKEVEN", "BREAKEVEN", max_tp, observed_at, current)
            )
            return AnalyticsTrackingTransition(
                signal_id=state.id,
                expected_version=state.state_version,
                new_version=state.state_version + 1,
                status="completed_be",
                zone_touched_at=state.zone_touched_at,
                activated_at=state.activated_at,
                activated_price=state.activated_price,
                max_tp_index=max_tp,
                be_armed_at=be_armed_at or observed_at,
                completed_at=observed_at,
                terminal_reason=f"be_after_tp{max_tp}",
                ambiguous_reason=None,
                last_observed_at=observed_at,
                last_observed_price=current,
                events=tuple(events),
            )

        if self._stop_reached(state.side, current, state.stop_price):
            reason = "stop_no_tp" if max_tp == 0 else f"stop_after_tp{max_tp}"
            events.append(self._event("STOP", "STOP", max_tp, observed_at, current))
            return AnalyticsTrackingTransition(
                signal_id=state.id,
                expected_version=state.state_version,
                new_version=state.state_version + 1,
                status="completed_stop",
                zone_touched_at=state.zone_touched_at,
                activated_at=state.activated_at,
                activated_price=state.activated_price,
                max_tp_index=max_tp,
                be_armed_at=be_armed_at,
                completed_at=observed_at,
                terminal_reason=reason,
                ambiguous_reason=None,
                last_observed_at=observed_at,
                last_observed_price=current,
                events=tuple(events),
            )

        if events:
            return AnalyticsTrackingTransition(
                signal_id=state.id,
                expected_version=state.state_version,
                new_version=state.state_version + 1,
                status="active",
                zone_touched_at=state.zone_touched_at,
                activated_at=state.activated_at,
                activated_price=state.activated_price,
                max_tp_index=max_tp,
                be_armed_at=be_armed_at,
                completed_at=None,
                terminal_reason=None,
                ambiguous_reason=None,
                last_observed_at=observed_at,
                last_observed_price=current,
                events=tuple(events),
            )
        return None

    def _restart_baseline_transition(
        self,
        state: TrackedAnalyticsSignal,
        current: Decimal,
        observed_at: datetime,
    ) -> AnalyticsTrackingTransition | None:
        """Establish a post-restart baseline without replaying the downtime gap.

        The first quote after ``forward_resumed`` is not treated as a continuous
        move from the pre-restart quote. If that quote is already at/through a
        level whose order could have been reached during the unknown gap, the
        outcome is terminally ambiguous. Otherwise the quote becomes a purely
        process-local baseline and only later observations may create events.
        """

        unsafe = False
        if state.status == "waiting_entry":
            unsafe = (
                state.order_type == "MARKET"
                or self._entry_reached(state, None, current)
                or self._stop_reached(state.side, current, state.stop_price)
                or any(
                    self._tp_reached(state.side, current, target)
                    for target in state.targets
                )
            )
        else:
            next_targets = state.targets[state.max_tp_index :]
            unsafe = (
                self._stop_reached(state.side, current, state.stop_price)
                or any(
                    self._tp_reached(state.side, current, target)
                    for target in next_targets
                )
                or (
                    state.be_trigger_tp_index > 0
                    and state.max_tp_index >= state.be_trigger_tp_index
                    and state.entry_price is not None
                    and self._be_reached(state.side, current, state.entry_price)
                )
            )
        if not unsafe:
            return None
        self._restart_baseline_ambiguous += 1
        return AnalyticsTrackingTransition(
            signal_id=state.id,
            expected_version=state.state_version,
            new_version=state.state_version + 1,
            status="ambiguous",
            zone_touched_at=state.zone_touched_at,
            activated_at=state.activated_at,
            activated_price=state.activated_price,
            max_tp_index=state.max_tp_index,
            be_armed_at=state.be_armed_at,
            completed_at=observed_at,
            terminal_reason="ambiguous",
            ambiguous_reason="restart_gap_first_quote_outside_known_state",
            last_observed_at=observed_at,
            last_observed_price=current,
            events=(
                self._event("AMBIGUOUS", "AMBIGUOUS", state.max_tp_index, observed_at, current),
            ),
        )

    def _apply_in_memory(
        self, state: TrackedAnalyticsSignal, transition: AnalyticsTrackingTransition
    ) -> None:
        updated = replace(
            state,
            status=transition.status,
            zone_touched_at=transition.zone_touched_at,
            activated_at=transition.activated_at,
            activated_price=transition.activated_price,
            max_tp_index=transition.max_tp_index,
            be_armed_at=transition.be_armed_at,
            completed_at=transition.completed_at,
            terminal_reason=transition.terminal_reason,
            ambiguous_reason=transition.ambiguous_reason,
            state_version=transition.new_version,
            last_observed_at=transition.last_observed_at,
            last_observed_price=transition.last_observed_price,
        )
        self._pending_by_id[updated.id] = updated
        self._replace(updated)

    def _expire_due(self, observed_at: datetime, sink: TransitionSink) -> None:
        now_ts = observed_at.timestamp()
        while self._expiry_heap and self._expiry_heap[0][0] <= now_ts:
            _, signal_id, version = heapq.heappop(self._expiry_heap)
            state = self._by_id.get(signal_id)
            if (
                state is None
                or state.status != "waiting_entry"
                or state.state_version != version
                or state.expiry_at is None
                or state.expiry_at > observed_at
            ):
                continue
            price = state.last_observed_price or state.entry_reference
            restart_gap_expiry = (
                state.recovery_status == "forward_resumed"
                and state.id not in self._restart_baseline_complete_ids
            )
            if restart_gap_expiry:
                self._restart_baseline_complete_ids.add(state.id)
                self._restart_baseline_ambiguous += 1
            transition = AnalyticsTrackingTransition(
                signal_id=state.id,
                expected_version=state.state_version,
                new_version=state.state_version + 1,
                status="ambiguous" if restart_gap_expiry else "expired_not_entered",
                zone_touched_at=state.zone_touched_at,
                activated_at=None,
                activated_price=None,
                max_tp_index=0,
                be_armed_at=None,
                completed_at=observed_at,
                terminal_reason="ambiguous" if restart_gap_expiry else "expired_not_entered",
                ambiguous_reason=(
                    "restart_gap_expiry_order_unknown"
                    if restart_gap_expiry
                    else None
                ),
                last_observed_at=observed_at,
                last_observed_price=price,
                events=(
                    self._event(
                        "AMBIGUOUS" if restart_gap_expiry else "EXPIRED",
                        "AMBIGUOUS" if restart_gap_expiry else "EXPIRED",
                        0,
                        observed_at,
                        price,
                    ),
                ),
            )
            if sink(transition):
                self._transitions_queued += 1
                self._events_queued += 1
                self._apply_in_memory(state, transition)
            else:
                self._transitions_dropped += 1
                self._quarantine_for_recovery(
                    state.id, reason="expiry_transition_queue_rejected"
                )
                break

    def observe_prices(
        self,
        prices: dict[str, Any],
        *,
        observed_at: datetime,
        sink: TransitionSink,
    ) -> dict[str, int]:
        if not self._started:
            return {"signals_checked": 0, "transitions": 0, "events": 0}
        now = _utc(observed_at)
        self._price_snapshots += 1
        self._expire_due(now, sink)
        checked = 0
        transitions = 0
        events = 0
        for symbol, source_price in prices.items():
            normalized_symbol = str(symbol or "").upper()
            ids = tuple(self._ids_by_symbol.get(normalized_symbol, ()))
            if not ids:
                continue
            current = self._price_from_row(source_price)
            if current is None:
                continue
            for signal_id in ids:
                state = self._by_id.get(signal_id)
                if state is None:
                    continue
                if state.last_observed_at is not None and now <= state.last_observed_at:
                    # The public loop is sequential, but wall-clock adjustments or
                    # adversarial callers must not move analytics chronology
                    # backwards or replay the same sampled quote as a new event.
                    self._out_of_order_snapshots += 1
                    continue
                checked += 1
                if (
                    state.recovery_status == "forward_resumed"
                    and state.id not in self._restart_baseline_complete_ids
                ):
                    self._restart_baseline_complete_ids.add(state.id)
                    transition = self._restart_baseline_transition(state, current, now)
                    if transition is None:
                        # The first post-restart quote is only a baseline. Do not
                        # emit ENTRY_ZONE/ENTRY/TP/STOP/BE from the unknown gap.
                        self._by_id[state.id] = replace(
                            state,
                            last_observed_at=now,
                            last_observed_price=current,
                        )
                        continue
                else:
                    transition = (
                        self._waiting_transition(state, current, now)
                        if state.status == "waiting_entry"
                        else self._active_transition(state, current, now)
                    )
                if transition is None:
                    # Process-local continuity only; no SQL is emitted for ordinary
                    # price ticks that do not cross a level.
                    self._by_id[state.id] = replace(
                        state,
                        last_observed_at=now,
                        last_observed_price=current,
                    )
                    continue
                if sink(transition):
                    transitions += 1
                    events += len(transition.events)
                    self._transitions_queued += 1
                    self._events_queued += len(transition.events)
                    self._apply_in_memory(state, transition)
                else:
                    self._transitions_dropped += 1
                    self._quarantine_for_recovery(
                        state.id, reason="price_transition_queue_rejected"
                    )
        self._price_checks += checked
        return {
            "signals_checked": checked,
            "transitions": transitions,
            "events": events,
        }


_REGISTRY = SignalAnalyticsRegistry()


def get_signal_analytics_registry() -> SignalAnalyticsRegistry:
    return _REGISTRY


async def refresh_signal_analytics_registry() -> int:
    return await _REGISTRY.refresh()


def signal_analytics_tracking_symbols() -> tuple[str, ...]:
    settings = get_settings()
    if not (
        bool(settings.SIGNAL_ANALYTICS_ENABLED)
        and bool(settings.SIGNAL_ANALYTICS_TRACKING_ENABLED)
    ):
        return ()
    return _REGISTRY.symbols()


def signal_analytics_tracker_stats() -> dict[str, int]:
    return _REGISTRY.stats()


def acknowledge_signal_analytics_transitions(
    transitions: Iterable[AnalyticsTrackingTransition],
) -> int:
    """Acknowledge transitions after their database batch committed."""

    return _REGISTRY.acknowledge_transitions(transitions)


def reject_signal_analytics_transitions(
    transitions: Iterable[AnalyticsTrackingTransition],
) -> int:
    """Rollback matching optimistic states before durable-state refresh."""

    return _REGISTRY.reject_transitions(transitions)


def quarantine_signal_analytics_transitions(
    transitions: Iterable[AnalyticsTrackingTransition],
    *,
    reason: str,
) -> int:
    return _REGISTRY.quarantine_transitions(transitions, reason=reason)


def signal_analytics_recovery_quarantine_ids(
    *, limit: int = 1000
) -> tuple[int, ...]:
    return _REGISTRY.recovery_quarantine_ids(limit=limit)


def acknowledge_signal_analytics_recovery_quarantine(
    signal_ids: Iterable[int],
) -> int:
    return _REGISTRY.acknowledge_recovery_quarantine(signal_ids)


async def write_signal_analytics_recovery_marks(
    signal_ids: Iterable[int],
) -> int:
    """Persist fail-closed analytics recovery markers outside the hot path."""

    ids = sorted({int(signal_id) for signal_id in signal_ids if int(signal_id) > 0})
    if not ids:
        return 0
    async with connect() as conn:
        if is_postgres():
            rows = await conn.fetch(
                """
                UPDATE signal_analytics_signals
                SET needs_recovery=1,
                    recovery_status='pending',
                    recovery_method='none',
                    recovery_started_at=COALESCE(recovery_started_at,NOW()),
                    recovery_completed_at=NULL,
                    recovery_confidence='none',
                    recovery_cursor_at=COALESCE(
                      recovery_cursor_at,last_observed_at,tracking_started_at,published_at
                    ),
                    data_quality_status='recovery_required',
                    data_quality_reason='price_transition_queue_rejected',
                    updated_at=NOW()
                WHERE id=ANY($1::bigint[])
                  AND status IN ('waiting_entry','active')
                  AND (
                    COALESCE(needs_recovery,0)=0
                    OR recovery_status='forward_resumed'
                  )
                RETURNING id
                """,
                ids,
            )
            return len(rows)
        updated = 0
        for start in range(0, len(ids), 400):
            chunk = ids[start : start + 400]
            placeholders = ",".join("?" for _ in chunk)
            cursor = await conn.execute(
                f"""
                UPDATE signal_analytics_signals
                SET needs_recovery=1,
                    recovery_status='pending',
                    recovery_method='none',
                    recovery_started_at=COALESCE(
                      recovery_started_at,CURRENT_TIMESTAMP
                    ),
                    recovery_completed_at=NULL,
                    recovery_confidence='none',
                    recovery_cursor_at=COALESCE(
                      recovery_cursor_at,last_observed_at,tracking_started_at,published_at
                    ),
                    data_quality_status='recovery_required',
                    data_quality_reason='price_transition_queue_rejected',
                    updated_at=CURRENT_TIMESTAMP
                WHERE id IN ({placeholders})
                  AND status IN ('waiting_entry','active')
                  AND (
                    COALESCE(needs_recovery,0)=0
                    OR recovery_status='forward_resumed'
                  )
                """,
                tuple(chunk),
            )
            updated += max(0, int(cursor.rowcount or 0))
        await conn.commit()
        return updated


def observe_signal_analytics_prices(
    prices: dict[str, Any],
    *,
    observed_at: datetime,
    sink: TransitionSink,
) -> dict[str, int]:
    settings = get_settings()
    if not (
        bool(settings.SIGNAL_ANALYTICS_ENABLED)
        and bool(settings.SIGNAL_ANALYTICS_TRACKING_ENABLED)
    ):
        return {"signals_checked": 0, "transitions": 0, "events": 0}
    return _REGISTRY.observe_prices(prices, observed_at=observed_at, sink=sink)


_PG_UPDATE_STATE = """
UPDATE signal_analytics_signals SET
  status=$3,
  zone_touched_at=$4,
  activated_at=$5,
  activated_price=$6,
  max_tp_index=$7,
  be_armed_at=$8,
  completed_at=$9,
  terminal_reason=$10,
  ambiguous_reason=$11,
  last_observed_at=$12,
  last_observed_price=$13,
  state_version=$14,
  updated_at=NOW()
WHERE id=$1 AND state_version=$2
"""

_SQLITE_UPDATE_STATE = """
UPDATE signal_analytics_signals SET
  status=?,
  zone_touched_at=?,
  activated_at=?,
  activated_price=?,
  max_tp_index=?,
  be_armed_at=?,
  completed_at=?,
  terminal_reason=?,
  ambiguous_reason=?,
  last_observed_at=?,
  last_observed_price=?,
  state_version=?,
  updated_at=CURRENT_TIMESTAMP
WHERE id=? AND state_version=?
"""

_PG_INSERT_EVENT = """
INSERT INTO signal_analytics_level_events(
  signal_id,event_key,event_type,level_index,observed_at,observed_price,created_at
) VALUES($1,$2,$3,$4,$5,$6,NOW())
ON CONFLICT(signal_id,event_key) DO NOTHING
"""

_SQLITE_INSERT_EVENT = """
INSERT OR IGNORE INTO signal_analytics_level_events(
  signal_id,event_key,event_type,level_index,observed_at,observed_price,created_at
) VALUES(?,?,?,?,?,?,CURRENT_TIMESTAMP)
"""


def _transition_update_values(
    transition: AnalyticsTrackingTransition,
) -> tuple[Any, ...]:
    if is_postgres():
        return (
            transition.signal_id,
            transition.expected_version,
            transition.status,
            _datetime_db(transition.zone_touched_at),
            _datetime_db(transition.activated_at),
            _decimal_db(transition.activated_price),
            transition.max_tp_index,
            _datetime_db(transition.be_armed_at),
            _datetime_db(transition.completed_at),
            transition.terminal_reason,
            transition.ambiguous_reason,
            _datetime_db(transition.last_observed_at),
            _decimal_db(transition.last_observed_price),
            transition.new_version,
        )
    return (
        transition.status,
        _datetime_db(transition.zone_touched_at),
        _datetime_db(transition.activated_at),
        _decimal_db(transition.activated_price),
        transition.max_tp_index,
        _datetime_db(transition.be_armed_at),
        _datetime_db(transition.completed_at),
        transition.terminal_reason,
        transition.ambiguous_reason,
        _datetime_db(transition.last_observed_at),
        _decimal_db(transition.last_observed_price),
        transition.new_version,
        transition.signal_id,
        transition.expected_version,
    )


async def write_signal_analytics_transitions(
    transitions: Iterable[AnalyticsTrackingTransition],
) -> tuple[int, int, int]:
    """Persist transition batches.

    Returns ``(applied, stale_or_replayed, events_inserted)``.  A lost COMMIT
    response is safe: the retry sees the advanced state_version and idempotent
    event keys, then the caller refreshes the registry from durable state.
    """

    rows = list(transitions)
    if not rows:
        return 0, 0, 0
    applied = 0
    stale = 0
    inserted_events = 0
    async with connect() as conn:
        if is_postgres():
            async with conn.transaction():
                for transition in rows:
                    result = await conn.execute(
                        _PG_UPDATE_STATE, *_transition_update_values(transition)
                    )
                    if not str(result).endswith(" 1"):
                        stale += 1
                        continue
                    applied += 1
                    for event in transition.events:
                        result = await conn.execute(
                            _PG_INSERT_EVENT,
                            transition.signal_id,
                            event.event_key,
                            event.event_type,
                            event.level_index,
                            _datetime_db(event.observed_at),
                            _decimal_db(event.observed_price),
                        )
                        if str(result).endswith(" 1"):
                            inserted_events += 1
            return applied, stale, inserted_events

        await conn.execute("BEGIN")
        try:
            for transition in rows:
                cursor = await conn.execute(
                    _SQLITE_UPDATE_STATE, _transition_update_values(transition)
                )
                if int(cursor.rowcount or 0) != 1:
                    stale += 1
                    continue
                applied += 1
                for event in transition.events:
                    await conn.execute(
                        _SQLITE_INSERT_EVENT,
                        (
                            transition.signal_id,
                            event.event_key,
                            event.event_type,
                            event.level_index,
                            _datetime_db(event.observed_at),
                            _decimal_db(event.observed_price),
                        ),
                    )
                    cursor = await conn.execute("SELECT changes()")
                    inserted_events += int((await cursor.fetchone())[0] or 0)
            await conn.commit()
        except BaseException:
            await conn.rollback()
            raise
    return applied, stale, inserted_events
