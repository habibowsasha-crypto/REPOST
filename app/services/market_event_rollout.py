from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from typing import Any, Awaitable, Callable

from app.config import get_settings
from app.database import db
from app.services.durable_notifications import send_or_enqueue
from app.services.notification_style import ensure_visual_card

log = logging.getLogger(__name__)
NotifyFn = Callable[..., Awaitable[object] | object]
_MIGRATION_VERSION = 1

# G67: production logs proved that the last legacy TP4 event of the configured
# canary group can remain in the old admin watch forever while the global
# MARKET_EVENT rollout intentionally stays in shadow.  This rescue is narrower
# than rollout: it may only take an already-observed ``shadow`` TP row of the
# configured target group after a very large legacy attempt history and prepare
# exactly one existing finite FINAL observation.  It never creates/cancels an
# exchange order and it never enables terminal logic for any other group.
G67_LEGACY_TARGET_RESCUE_REASON = "g67_legacy_target_one_final_check"
_G67_LEGACY_TARGET_RESCUE_MIN_ATTEMPTS = 50
_G67_LEGACY_TARGET_RESCUE_SCAN_LIMIT = 25


@dataclass(frozen=True)
class MarketEventRolloutResult:
    stage: str
    candidates: int
    shadowed: int
    prepared: int
    skipped: int
    target_group_id: int


def rollout_target_group(stage: str, configured_group_id: int) -> int | None:
    normalized = str(stage or "off").strip().lower()
    if normalized == "group_1541":
        return int(configured_group_id or 1541)
    return None


def market_event_stage_allows_group(
    stage: str, group_id: int, configured_group_id: int
) -> bool:
    """Return whether terminal/manual rollout logic may mutate one group.

    ``group_1541`` is a true single-group canary, not merely a migration-row
    selector.  ``global`` is the only stage that may run the finite terminal
    state machine for every group.  ``off`` and ``shadow`` remain observation
    only even when a stale Railway flag is accidentally left enabled.
    """

    normalized = str(stage or "off").strip().lower()
    if normalized == "global":
        return True
    if normalized == "group_1541":
        return int(group_id or 0) == int(configured_group_id or 1541)
    return False


def g67_prepared_target_event_allowed(settings: Any, event: dict[str, Any]) -> bool:
    """Allow finite terminal review only for the exact G67 rescue row.

    Generic ``shadow`` remains observation-only.  The exception is a row that
    G67 itself has already prepared after the configured target group exceeded
    the legacy stuck threshold.  The row identity is durable through
    ``migration_state`` + ``migration_reason`` and therefore cannot broaden to
    another group after restart.
    """

    stage = str(getattr(settings, "MARKET_EVENT_ROLLOUT_STAGE", "off") or "off").strip().lower()
    if stage != "shadow":
        return False
    target = int(getattr(settings, "MARKET_EVENT_MIGRATION_TARGET_GROUP_ID", 1541) or 1541)
    if int(event.get("trade_group_id") or 0) != target:
        return False
    if str(event.get("event_type") or "").strip().upper() != "TP":
        return False
    if str(event.get("migration_state") or "none").strip().lower() != "prepared":
        return False
    if str(event.get("migration_reason") or "").strip() != G67_LEGACY_TARGET_RESCUE_REASON:
        return False
    return str(event.get("phase") or "").strip().upper() == "FINAL_CHECK_PENDING"


async def run_g67_legacy_target_rescue_once() -> bool:
    """Prepare at most one already-shadowed legacy target event for FINAL.

    This is a one-row DB scheduling repair.  It performs no BingX call and no
    exchange write.  The normal verifier then performs the same exact read path
    it already used for the stuck event; unresolved evidence ends in
    MANUAL_REVIEW, which disables further automatic rearming.
    """

    settings = get_settings()
    stage = str(settings.MARKET_EVENT_ROLLOUT_STAGE or "off").strip().lower()
    if stage != "shadow":
        return False
    target = int(settings.MARKET_EVENT_MIGRATION_TARGET_GROUP_ID or 1541)
    if target <= 0:
        return False
    minimum = max(
        _G67_LEGACY_TARGET_RESCUE_MIN_ATTEMPTS,
        int(settings.MARKET_EVENT_MIGRATION_MIN_ATTEMPTS or 1),
    )
    rows = await db.market_event_migration_candidates(
        min_attempts=minimum,
        limit=_G67_LEGACY_TARGET_RESCUE_SCAN_LIMIT,
        target_group_id=target,
        include_shadow=True,
    )
    candidate = next(
        (
            row
            for row in rows
            if str(row.get("migration_state") or "none").strip().lower() == "shadow"
            and int(row.get("trade_group_id") or 0) == target
            and str(row.get("event_type") or "").strip().upper() == "TP"
        ),
        None,
    )
    if candidate is None:
        return False

    event_id = int(candidate.get("id") or 0)
    if event_id <= 0:
        return False
    changed = await db.prepare_market_event_final_migration(
        event_id,
        max_fast_attempts=int(settings.MARKET_EVENT_MAX_FAST_ATTEMPTS),
        max_deep_attempts=int(settings.MARKET_EVENT_MAX_DEEP_ATTEMPTS),
        migration_version=_MIGRATION_VERSION,
        reason=G67_LEGACY_TARGET_RESCUE_REASON,
    )
    if changed:
        log.warning(
            "G67_LEGACY_TARGET_RESCUE_PREPARED event_id=%s group_id=%s event=%s attempts=%s action=one_final_exact_check",
            event_id,
            target,
            str(candidate.get("event_key") or candidate.get("event_type") or "unknown"),
            int(candidate.get("attempts") or 0),
        )
    return bool(changed)


def _prepared_message(row: dict[str, Any], *, stage: str) -> str:
    return ensure_visual_card(
        "🧪 MARKET EVENT · ФИНАЛЬНАЯ ПРОВЕРКА ПОДГОТОВЛЕНА\n"
        f"Event ID: {int(row.get('id') or 0)}\n"
        f"Группа: {int(row.get('trade_group_id') or 0)}\n"
        f"Событие: {str(row.get('event_key') or row.get('event_type') or 'unknown')}\n"
        f"Старых попыток: {int(row.get('attempts') or 0)}\n"
        f"Причина: {str(row.get('stuck_reason') or row.get('last_error') or 'pending_limit_no_transition')[:500]}\n"
        f"Rollout: {stage}\n"
        "Действие: будет выполнена ровно одна свежая FINAL-сверка. "
        "При отсутствии точного доказательства событие перейдёт в MANUAL_REVIEW.\n"
        "Торговые ордера этой миграцией не изменяются."
    )


async def _notify_prepared(
    notify: NotifyFn | None,
    row: dict[str, Any],
    *,
    stage: str,
) -> None:
    settings = get_settings()
    event_id = int(row.get("id") or 0)
    group_id = int(row.get("trade_group_id") or 0)
    text = _prepared_message(row, stage=stage)
    for admin_id in sorted({int(value) for value in settings.admin_ids if int(value) > 0}):
        await send_or_enqueue(
            notify,
            admin_id,
            text,
            source="market_event_migration",
            event_key=f"market-event-migration:{event_id}:prepared",
            dedup_key_override=f"market-event-migration:{event_id}:prepared:{admin_id}",
        )
    log.warning(
        "MARKET_EVENT_MIGRATION_PREPARED event_id=%s group_id=%s stage=%s attempts=%s",
        event_id,
        group_id,
        stage,
        int(row.get("attempts") or 0),
    )


async def run_market_event_rollout_once(
    notify: NotifyFn | None = None,
) -> MarketEventRolloutResult:
    """Run one bounded rollout iteration.

    off: no reads/writes;
    shadow: mark eligible rows for visibility only, never touching queue state;
    group_1541: prepare at most one target event for one FINAL observation;
    global: prepare a bounded batch.
    """

    settings = get_settings()
    stage = str(settings.MARKET_EVENT_ROLLOUT_STAGE or "off").strip().lower()
    target = rollout_target_group(stage, settings.MARKET_EVENT_MIGRATION_TARGET_GROUP_ID)
    target_value = int(target or 0)
    if stage == "off":
        return MarketEventRolloutResult(stage, 0, 0, 0, 0, target_value)

    batch_size = 1 if stage == "group_1541" else int(settings.MARKET_EVENT_MIGRATION_BATCH_SIZE)
    rows = await db.market_event_migration_candidates(
        min_attempts=int(settings.MARKET_EVENT_MIGRATION_MIN_ATTEMPTS),
        limit=batch_size,
        target_group_id=target,
        include_shadow=stage != "shadow",
    )
    if stage == "shadow":
        shadowed = 0
        for row in rows:
            changed = await db.mark_market_event_migration_shadow(
                int(row.get("id") or 0),
                migration_version=_MIGRATION_VERSION,
                reason="g42_shadow_candidate_pending_limit_no_transition",
            )
            shadowed += int(bool(changed))
        snapshot = await db.market_event_rollout_snapshot(
            int(settings.MARKET_EVENT_MIGRATION_TARGET_GROUP_ID)
        )
        log.info(
            "MARKET_EVENT_ROLLOUT_SHADOW candidates=%s shadowed=%s snapshot=%s",
            len(rows),
            shadowed,
            snapshot,
        )
        return MarketEventRolloutResult(
            stage, len(rows), shadowed, 0, len(rows) - shadowed, target_value
        )

    if stage not in {"group_1541", "global"} or not bool(settings.MARKET_EVENT_MIGRATION_ENABLED):
        log.warning(
            "MARKET_EVENT_ROLLOUT_DISABLED stage=%s migration_enabled=%s",
            stage,
            int(bool(settings.MARKET_EVENT_MIGRATION_ENABLED)),
        )
        return MarketEventRolloutResult(stage, len(rows), 0, 0, len(rows), target_value)

    prepared = 0
    for row in rows:
        changed = await db.prepare_market_event_final_migration(
            int(row.get("id") or 0),
            max_fast_attempts=int(settings.MARKET_EVENT_MAX_FAST_ATTEMPTS),
            max_deep_attempts=int(settings.MARKET_EVENT_MAX_DEEP_ATTEMPTS),
            migration_version=_MIGRATION_VERSION,
            reason=(
                "g42_group_1541_final_check"
                if stage == "group_1541"
                else "g42_global_legacy_stuck_final_check"
            ),
        )
        if not changed:
            continue
        prepared += 1
        try:
            await _notify_prepared(notify, row, stage=stage)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            # State is already durable; the durable notification layer retries.
            log.warning(
                "MARKET_EVENT_MIGRATION_NOTIFY_DEFERRED event_id=%s error=%s",
                int(row.get("id") or 0),
                f"{type(exc).__name__}: {exc}",
            )
    return MarketEventRolloutResult(
        stage, len(rows), 0, prepared, len(rows) - prepared, target_value
    )


async def market_event_rollout_loop(notify: NotifyFn | None = None) -> None:
    settings = get_settings()
    interval = max(5.0, float(settings.MARKET_EVENT_MIGRATION_INTERVAL_SEC))
    while True:
        try:
            # G67 rescue is deliberately separate from the generic shadow
            # rollout contract.  A busy/leased row is simply retried on the next
            # rollout cycle until the exact legacy target row can be prepared.
            await run_g67_legacy_target_rescue_once()
            result = await run_market_event_rollout_once(notify=notify)
            if result.stage != "off" and (result.candidates or result.prepared or result.shadowed):
                log.info(
                    "MARKET_EVENT_ROLLOUT_CYCLE stage=%s candidates=%s shadowed=%s "
                    "prepared=%s skipped=%s target_group_id=%s",
                    result.stage,
                    result.candidates,
                    result.shadowed,
                    result.prepared,
                    result.skipped,
                    result.target_group_id,
                )
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("MARKET_EVENT_ROLLOUT_CYCLE_FAILED")
        await asyncio.sleep(interval)
