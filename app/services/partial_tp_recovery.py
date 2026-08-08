from __future__ import annotations

import math
import asyncio
import json
import logging
from typing import Any, Awaitable, Callable

from app.config import get_settings
from app.services.monitor_diagnostics import record_stage_rows
from app.services.ttl_cache import get_api_key_cache, get_user_settings_cache
from app.database import db
from app.services.exchange_factory import build_adapter, exchange_title
from app.services.notification_style import card, fmt_price, fmt_qty
from app.services.durable_notifications import (
    send_or_enqueue,
    set_notification_event_key,
)
from app.services.async_utils import StaleExecutionPass, null_async_context
from app.services.transient_errors import (
    is_transient_exchange_error,
    record_transient_error,
    should_notify_transient,
    transient_retry_exhausted,
)
from app.services.tp_qty import order_normalized_qty
from app.services.tp_plan_snapshot import (
    get_snapshot,
    snapshot_items,
    snapshot_plan_map,
    snapshot_target_map,
    snapshot_total_qty,
)
from app.services.tp_ambiguous_recheck import find_tp_order_after_ambiguous_write
from app.services.tp_execution_ledger import (
    canonicalize_tp_ledger,
    tp_ledger_repair_metadata,
)
from app.exchanges.bingx.adapter import BingxTpCoverageError as BingxTpCoverageError
from app.services.exchange_identity import clean_exchange_id

log = logging.getLogger(__name__)

NotifyFn = Callable[[int, str], Awaitable[object] | object]
_LOOP_LOCK = asyncio.Lock()
_SCAN_CURSOR = 0


def _f(value: Any, default: float = 0.0) -> float:
    """Parse a finite non-negative exchange scalar without repairing corruption."""
    try:
        if value in (None, "") or isinstance(value, bool):
            return default
        parsed = float(value)
        return parsed if math.isfinite(parsed) and parsed >= 0 else default
    except (TypeError, ValueError, OverflowError):
        return default


def _signed_f(value: Any, default: float = 0.0) -> float:
    """Parse a stored scalar without turning corrupted negatives positive."""
    try:
        if value in (None, "") or isinstance(value, bool):
            return default
        parsed = float(value)
        return parsed if math.isfinite(parsed) else default
    except (TypeError, ValueError, OverflowError):
        return default


def _position_size(pos: dict[str, Any]) -> float:
    for key in ("size", "availableSize", "positionAmt", "qty", "total"):
        val = _f(pos.get(key), 0.0)
        if val > 0:
            return val
    return 0.0


def _total_position_size(positions: list[dict[str, Any]]) -> float:
    return sum(_position_size(p) for p in positions)


def _unique_position_id(positions: list[dict[str, Any]]) -> str:
    ids = {
        clean_exchange_id(position.get("positionId"))
        for position in positions or []
        if isinstance(position, dict) and clean_exchange_id(position.get("positionId"))
    }
    return next(iter(ids)) if len(ids) == 1 else ""


def _position_entry_price(pos: dict[str, Any]) -> float:
    for key in (
        "avgFillPrice",
        "entryPrice",
        "avgEntryPrice",
        "positionAvgPrice",
        "avgEntry",
        "openPrice",
        "avgPrice",
    ):
        val = _f(pos.get(key), 0.0)
        if val > 0:
            return val
    return 0.0


def _crossed_tp(side: str, current: float, tp: float) -> bool:
    return current >= tp if side.lower() == "long" else current <= tp


def _breached_stop(side: str, current: float, stop: float) -> bool:
    return current <= stop if side.lower() == "long" else current >= stop


async def _notify(
    notify: NotifyFn | None,
    user_id: int,
    text: str,
    *,
    event_key: str | None = None,
) -> bool:
    return await send_or_enqueue(
        notify,
        user_id,
        text,
        source="partial_tp_recovery",
        event_key=event_key,
    )


TP_DONE_ACTION_TYPES = {
    "tp_order",
    "tp_created",
    "tp_created_after_fill",
    "tp_catchup_market_close",
    "tp_recovered_order",
    "tp_recovered_market_close",
    # Ambiguous writes are intentionally NOT counted as done. A TP order write
    # is resolved by the adapter's live target/position dedup guard on retry. An
    # ambiguous market close is escalated to manual review below because blindly
    # closing the same slice again could double-take profit.
    "manual_tp_confirmed",
}


def _legacy_fake_tp_success(item: Any) -> bool:
    """Detect v1.6.2 synthetic TP rows that never represented a live order."""
    if not isinstance(item, dict):
        return False
    candidates = [item]
    for key in ("order", "result"):
        nested = item.get(key)
        if isinstance(nested, dict):
            candidates.append(nested)
    return any(
        str(candidate.get("type") or "").upper() == "TAKE_PROFIT_SKIPPED_COVERED"
        or bool(candidate.get("_idempotent_coverage_full"))
        for candidate in candidates
    )


def _item_tp_index(item: Any) -> int:
    if not isinstance(item, dict):
        return 0
    try:
        return int(item.get("tp_index") or item.get("index") or 0)
    except Exception:
        return 0


def _done_tp_indices(payload: dict[str, Any]) -> set[int]:
    done: set[int] = set()

    # Main MARKET TP list. Old builds may store plain exchange order objects, so
    # tp_index presence is enough here: this list means the reduceOnly TP was
    # already submitted or at least attempted and journaled.
    for item in payload.get("tp") or []:
        idx = _item_tp_index(item)
        if idx > 0 and not _legacy_fake_tp_success(item):
            done.add(idx)

    # Action logs from LIMIT catch-up / recovery / future manual override.
    # Completed recovery is stored as {"completed": true, "actions": [...]},
    # while older rows store the action list directly. Support both shapes.
    for section in ("post_fill", "recovery", "manual"):
        entries = payload.get(section) or []
        if isinstance(entries, dict):
            entries = entries.get("actions") or []
        if not isinstance(entries, list):
            continue
        for item in entries:
            idx = _item_tp_index(item)
            typ = str(item.get("type") or "") if isinstance(item, dict) else ""
            if (
                idx > 0
                and typ in TP_DONE_ACTION_TYPES
                and not _legacy_fake_tp_success(item)
            ):
                done.add(idx)

    return done


def _fmt_actions(actions: list[dict[str, Any]]) -> str:
    lines: list[str] = []
    for a in actions:
        typ = a.get("type")
        if typ == "tp_recovered_order":
            lines.append(
                f"🎯 TP{a.get('tp_index')} восстановлен: {a.get('tp')} | qty {a.get('qty')}"
            )
        elif typ == "tp_recovered_market_close":
            lines.append(
                f"⚡ TP{a.get('tp_index')} уже пройден - закрыта доля market reduceOnly: qty {a.get('qty')}"
            )
        elif typ == "stop_recovery_market_close":
            lines.append(
                f"🚨 STOP пробит - позиция закрыта market reduceOnly: qty {a.get('qty')}"
            )
        elif typ == "closed_on_exchange":
            lines.append("ℹ️ Позиция уже закрыта на бирже")
        elif typ == "recovery_error":
            lines.append(f"⚠️ Ошибка восстановления: {a.get('error')}")
        elif typ == "transient_error":
            lines.append(
                f"⏳ Временная ошибка биржи, бот повторит позже: {a.get('error')}"
            )
        elif typ == "transient_give_up":
            lines.append(
                f"🛑 Лимит повторов временной ошибки исчерпан, нужна ручная проверка: {a.get('error')}"
            )
        elif typ == "tp_write_ambiguous":
            lines.append(
                f"⚠️ TP{a.get('tp_index')} под вопросом: биржа вернула временную ошибку во время создания TP. Проверь ордер вручную."
            )
        elif typ == "tp_market_close_ambiguous":
            lines.append(
                f"⚠️ TP{a.get('tp_index')} market-close под вопросом: биржа вернула временную ошибку. Проверь позицию вручную."
            )
    return "\n".join(lines) if lines else "действий нет"


async def process_partial_tp_recovery_once(
    notify: NotifyFn | None = None,
    *,
    rows_override: list[dict[str, Any]] | None = None,
    market_prices: dict[str, float] | None = None,
    shared_adapter_cache: dict[tuple[int, str], Any] | None = None,
    market_event_exchange_context: Any | None = None,
) -> int:
    """Retry incomplete TP protection for partial_error executions.

    This handles MARKET entries where TP1 was created but TP2/TP3 failed, and
    LIMIT catch-up rows that failed after partial post-fill actions. The bot will
    not re-open the signal because dedup remains marked; instead it tries to add
    missing reduceOnly TP protection for the still-open position.
    """
    use_global_lock = rows_override is None
    if use_global_lock and _LOOP_LOCK.locked():
        record_stage_rows(selected=0, scanned=0, lock_skipped=1, source="global_lock")
        return 0
    async with _LOOP_LOCK if use_global_lock else null_async_context():
        global _SCAN_CURSOR
        if rows_override is not None:
            rows = list(rows_override)
        else:
            rows = await db.partial_error_executions(limit=50, after_id=_SCAN_CURSOR)
            if not rows and _SCAN_CURSOR:
                _SCAN_CURSOR = 0
                rows = await db.partial_error_executions(limit=50, after_id=0)
            if rows:
                _SCAN_CURSOR = max(int(row.get("id") or 0) for row in rows)
        record_stage_rows(
            selected=len(rows),
            scanned=len(rows),
            source="override" if rows_override is not None else "database",
        )
        if not rows:
            return 0
        processed = 0
        owns_adapter_cache = shared_adapter_cache is None
        adapter_cache: dict[tuple[int, str], Any] = (
            {} if shared_adapter_cache is None else shared_adapter_cache
        )
        try:
            for original_row in rows:
                execution_id = int(original_row.get("id") or 0)
                if not execution_id:
                    continue
                set_notification_event_key(f"execution:{execution_id}")
                async with db.execution_lock(execution_id) as lock_acquired:
                    if lock_acquired is False:
                        log.warning(
                            "PARTIAL_TP_LOCK_DEFERRED execution_id=%s stage=%s",
                            execution_id, db.monitor_workload_stage(),
                        )
                        continue
                    row = await db.get_execution_by_id(execution_id) or original_row
                    current_status = str(row.get("status") or "")
                    if current_status not in {"partial_error", "partial_unrecoverable"}:
                        continue

                    # v1.6.18: track this iteration's last-known-good status so
                    # every write below refuses if a concurrent worker (or an
                    # old/new process briefly overlapping a Railway redeploy)
                    # already moved this row to a different status.
                    _known_status = current_status

                    async def _write_status(new_status, reason, patch=None):
                        nonlocal _known_status
                        ok = await db.update_execution_status_merge(
                            execution_id,
                            new_status,
                            reason,
                            patch,
                            expected_status=_known_status,
                            write_flow_audit_stage="partial_tp_recovery",
                            write_flow_audit_status=new_status,
                        )
                        if ok:
                            _known_status = new_status
                        else:
                            log.info(
                                "partial_tp_recovery: abort stale execution pass execution_id=%s "
                                "attempted=%s expected=%s",
                                execution_id,
                                new_status,
                                _known_status,
                            )
                            raise StaleExecutionPass(
                                source="partial_tp_recovery",
                                execution_id=execution_id,
                                expected_status=_known_status,
                                attempted_status=new_status,
                            )
                        return True

                    user_id = int(row.get("user_id") or 0)
                    symbol = str(row.get("symbol") or "").upper()
                    side = str(row.get("side") or "").lower()
                    stop = _signed_f(row.get("stop"), 0.0)
                    intended_qty = _signed_f(row.get("qty"), 0.0)
                    invalid_fields: list[str] = []
                    if not user_id:
                        invalid_fields.append("user_id")
                    if not symbol:
                        invalid_fields.append("symbol")
                    if side not in {"long", "short"}:
                        invalid_fields.append("side")
                    if stop <= 0:
                        invalid_fields.append("stop")
                    if intended_qty <= 0:
                        invalid_fields.append("qty")
                    if invalid_fields:
                        await _write_status(
                            "manual_required",
                            "partial TP recovery blocked: corrupted execution fields "
                            + ", ".join(invalid_fields),
                            {
                                "recovery": {
                                    "completed": False,
                                    "manual_required": True,
                                    "reason": "invalid_execution_fields",
                                    "invalid_fields": invalid_fields,
                                }
                            },
                        )
                        processed += 1
                        continue

                    # Legacy targets_json/tp_distribution_json are not recovery
                    # authority after v1.6.4. The immutable snapshot below is.
                    # Requiring both copies to parse allowed a damaged obsolete
                    # column to block an otherwise valid protected trade forever.

                    try:
                        payload = json.loads(row.get("exchange_order_ids_json") or "{}")
                    except Exception:
                        payload = {}
                    _, ledger_changed, repaired_indices = canonicalize_tp_ledger(
                        payload
                    )
                    if ledger_changed:
                        await db.merge_execution_metadata(
                            execution_id,
                            {
                                "tp": payload.get("tp") or [],
                                "tp_ledger_v1": tp_ledger_repair_metadata(
                                    repaired_indices,
                                    source="partial_tp_recovery.startup_repair",
                                ),
                            },
                            write_flow_audit_stage="partial_tp_recovery_ledger_repair",
                            write_flow_audit_status=current_status,
                        )
                    if (
                        (payload.get("recovery") or {})
                        and isinstance(payload.get("recovery"), dict)
                        and payload["recovery"].get("completed")
                    ):
                        continue

                    unresolved_market_close = []
                    for section in ("post_fill", "recovery", "manual"):
                        entries = payload.get(section) or []
                        if isinstance(entries, dict):
                            entries = entries.get("actions") or []
                        if not isinstance(entries, list):
                            continue
                        unresolved_market_close.extend(
                            item
                            for item in entries
                            if isinstance(item, dict)
                            and str(item.get("type") or "")
                            == "tp_market_close_ambiguous"
                        )
                    if unresolved_market_close:
                        last = unresolved_market_close[-1]
                        await _write_status(
                            "manual_required",
                            "ambiguous TP market-close requires manual position check",
                            {
                                "recovery": {
                                    "completed": False,
                                    "manual_required": True,
                                    "reason": "ambiguous_tp_market_close",
                                    "tp_index": int(last.get("tp_index") or 0),
                                }
                            },
                        )
                        await _notify(
                            notify,
                            user_id,
                            f"🚨 Нужна ручная проверка {symbol}\n"
                            f"Результат market-закрытия TP{int(last.get('tp_index') or 0)} "
                            "не удалось подтвердить. Бот не будет повторно закрывать ту же долю, "
                            "чтобы не зафиксировать лишний объём.",
                        )
                        processed += 1
                        continue

                    snapshot = get_snapshot(payload)
                    if snapshot is None:
                        # v1.6.4 never recalculates a live trade from current
                        # Railway settings or from a changed position size. A
                        # missing immutable plan means the row was created by an
                        # older build or the DB journal is incomplete. Fail closed.
                        await _write_status(
                            "manual_required",
                            "partial TP recovery blocked: immutable TP plan snapshot is missing",
                            {
                                "recovery": {
                                    "completed": False,
                                    "manual_required": True,
                                    "reason": "missing_tp_plan_snapshot",
                                }
                            },
                        )
                        await _notify(
                            notify,
                            user_id,
                            f"🚨 Нужна ручная проверка {symbol}\n"
                            "В базе нет зафиксированного TP-плана этой сделки. "
                            "Бот не будет пересчитывать цели по текущему объёму или новым настройкам, "
                            "чтобы не создать дубли и лишнее закрытие.",
                        )
                        processed += 1
                        continue

                    plan_items = snapshot_items(snapshot)
                    tp_qty_by_index = snapshot_plan_map(snapshot)
                    tp_target_by_index = snapshot_target_map(snapshot)
                    managed_total = snapshot_total_qty(snapshot)
                    done = _done_tp_indices(payload)
                    missing = [
                        int(item["tp_index"])
                        for item in plan_items
                        if int(item["tp_index"]) not in done
                    ]
                    if not missing:
                        await _write_status(
                            "protected",
                            "partial_error восстановлен: все TP зафиксированного плана уже учтены",
                            {
                                "recovery": {
                                    "completed": True,
                                    "note": "all_snapshot_tp_already_present",
                                }
                            },
                        )
                        processed += 1
                        continue

                    user_settings = await get_user_settings_cache().get_or_fetch(
                        (user_id, "settings"), lambda: db.get_user_settings(user_id)
                    )
                    exchange = str(
                        payload.get("exchange")
                        or user_settings.exchange
                        or get_settings().safe_default_exchange
                    ).lower()
                    api_row = await get_api_key_cache().get_or_fetch(
                        (user_id, "api", exchange),
                        lambda: db.get_api_key(user_id, exchange),
                    )
                    if not api_row:
                        await _write_status(
                            "partial_error",
                            f"partial TP recovery postponed: no API for {exchange_title(exchange)}",
                            {
                                "recovery": {
                                    "last_error": f"no API for {exchange_title(exchange)}"
                                }
                            },
                        )
                        continue

                    cache_key = (user_id, exchange)
                    adapter = adapter_cache.get(cache_key)
                    if adapter is None:
                        adapter = build_adapter(api_row)
                        if market_event_exchange_context is not None:
                            adapter = market_event_exchange_context.wrap_adapter(
                                adapter, cache_key
                            )
                        adapter_cache[cache_key] = adapter

                    saved_recovery = payload.get("recovery") or []
                    if isinstance(saved_recovery, dict):
                        saved_recovery = saved_recovery.get("actions") or []
                    actions: list[dict[str, Any]] = [
                        dict(item) for item in saved_recovery if isinstance(item, dict)
                    ]

                    def recovery_patch(
                        *,
                        completed: bool = False,
                        extra: dict[str, Any] | None = None,
                        source: str = "partial_tp_recovery",
                    ) -> dict[str, Any]:
                        recovery_value: Any = (
                            {"completed": True, "actions": actions}
                            if completed
                            else actions
                        )
                        payload["recovery"] = recovery_value
                        _, ledger_changed, repaired_indices = canonicalize_tp_ledger(
                            payload
                        )
                        patch: dict[str, Any] = {"recovery": recovery_value}
                        if ledger_changed:
                            patch["tp"] = payload.get("tp") or []
                            patch["tp_ledger_v1"] = tp_ledger_repair_metadata(
                                repaired_indices, source=source
                            )
                        if extra:
                            patch.update(extra)
                        return patch

                    async def persist_transient(area: str, exc: Exception) -> int:
                        attempts = record_transient_error(
                            payload, f"recovery_{area}", exc
                        )
                        exhausted = transient_retry_exhausted(
                            attempts,
                            max_retries=get_settings().EXCHANGE_TRANSIENT_ERROR_MAX_RETRIES,
                        )
                        typ = "transient_give_up" if exhausted else "transient_error"
                        actions.append(
                            {
                                "type": typ,
                                "area": area,
                                "error": f"{type(exc).__name__}: {exc}",
                                "attempt": attempts,
                            }
                        )
                        status = "manual_required" if exhausted else "partial_error"
                        reason = (
                            f"Retry cap exhausted in partial TP recovery ({area}), manual check required after {attempts} attempts: {type(exc).__name__}: {exc}"
                            if exhausted
                            else f"Retryable exchange error in partial TP recovery ({area}), attempt {attempts}: {type(exc).__name__}: {exc}"
                        )
                        await _write_status(
                            status,
                            reason,
                            recovery_patch(
                                extra={
                                    "transient_errors": payload.get("transient_errors")
                                },
                                source="partial_tp_recovery.transient",
                            ),
                        )
                        return attempts

                    try:
                        positions = await adapter.fetch_open_positions(
                            symbol, side.upper()
                        )
                    except Exception as exc:
                        if is_transient_exchange_error(exc):
                            attempts = await persist_transient("positions", exc)
                            if should_notify_transient(
                                attempts,
                                every=get_settings().EXCHANGE_TRANSIENT_ERROR_NOTIFY_EVERY,
                            ):
                                await _notify(
                                    notify,
                                    user_id,
                                    f"⏳ Временная ошибка биржи при восстановлении TP {symbol}\n{type(exc).__name__}: {str(exc)[:300]}\nПопытка: {attempts}. Бот повторит позже.",
                                )
                            processed += 1
                            continue
                        await _write_status(
                            "manual_required",
                            f"partial TP recovery cannot verify live position: {type(exc).__name__}: {exc}",
                            {
                                "recovery": {
                                    "completed": False,
                                    "manual_required": True,
                                    "last_error": f"{type(exc).__name__}: {exc}",
                                }
                            },
                        )
                        await _notify(
                            notify,
                            user_id,
                            f"🚨 Не удалось проверить позицию {symbol} для восстановления TP\n"
                            f"{type(exc).__name__}: {str(exc)[:400]}\n"
                            "Строка оставлена под ручным контролем, остальные сделки продолжают проверяться.",
                        )
                        processed += 1
                        continue
                    qty_now = min(_total_position_size(positions), managed_total)
                    recovery_position_id = _unique_position_id(positions)
                    if not recovery_position_id:
                        await _write_status(
                            "manual_required",
                            "partial TP recovery blocked: exact BingX positionId is missing or ambiguous",
                            {
                                "recovery": {
                                    "completed": False,
                                    "manual_required": True,
                                    "reason": "position_id_missing_or_ambiguous",
                                }
                            },
                        )
                        await _notify(
                            notify,
                            user_id,
                            f"🚨 Нужна ручная проверка {symbol}\n"
                            "BingX не вернула единственный точный positionId. "
                            "Бот не будет восстанавливать TP/закрывать доли без строгой привязки к позиции.",
                        )
                        processed += 1
                        continue
                    if qty_now <= max(managed_total * 0.005, 1e-10):
                        actions.append({"type": "closed_on_exchange"})
                        await _write_status(
                            "closed_on_exchange",
                            "partial_error recovery: position already closed on exchange",
                            {"recovery": actions},
                        )
                        await _notify(
                            notify,
                            user_id,
                            f"ℹ️ Восстановление TP {symbol}\nПозиция уже закрыта на бирже.",
                        )
                        processed += 1
                        continue

                    if positions:
                        actual_entry = _position_entry_price(positions[0])
                        if actual_entry > 0:
                            payload["actual_entry"] = actual_entry

                    try:
                        current = float(
                            (market_prices or {}).get(symbol)
                            or await adapter.fetch_last_price(symbol)
                        )
                    except Exception as exc:
                        current = None
                        actions.append(
                            {
                                "type": "price_unavailable",
                                "error": f"{type(exc).__name__}: {exc}",
                            }
                        )
                    if current is not None and _breached_stop(side, current, stop):
                        async with db.symbol_action_lock(user_id, symbol):
                            res = await adapter.emergency_close_market_confirmed(
                                symbol=symbol,
                                side=side,
                                qty=qty_now,
                                client_id=f"rec-sl-{execution_id}",
                            )
                        actions.append(
                            {
                                "type": "stop_recovery_market_close",
                                "qty": qty_now,
                                "current": current,
                                "stop": stop,
                                "result": res,
                            }
                        )
                        await _write_status(
                            "closed_stop_catchup",
                            "partial_error recovery: STOP already breached, closed reduceOnly market",
                            {
                                "recovery": actions,
                                "actual_entry": payload.get("actual_entry"),
                            },
                        )
                        await _notify(
                            notify,
                            user_id,
                            f"🚨 Восстановление TP {symbol}\nЦена уже за STOP. Позиция закрыта market reduceOnly.\n{_fmt_actions(actions)}",
                        )
                        processed += 1
                        continue

                    # Recovery must reuse the exact immutable plan locked after
                    # the terminal entry fill. It must never rebuild from current
                    # Railway settings, the original intended quantity, or the
                    # position size observed at recovery time.
                    if not any(
                        str(item.get("type") or "") == "recovery_tp_plan"
                        for item in actions
                    ):
                        actions.append(
                            {
                                "type": "recovery_tp_plan",
                                "mode": str(snapshot.get("mode") or ""),
                                "snapshot_locked": True,
                                "plan": [
                                    {
                                        "tp_idx": int(item["tp_index"]),
                                        "price": float(item["price"]),
                                        "qty": float(item["qty"]),
                                    }
                                    for item in plan_items
                                ],
                            }
                        )

                    remaining_cap = qty_now
                    failed = False
                    for idx in missing:
                        target = float(tp_target_by_index.get(idx, 0.0))
                        if target <= 0:
                            actions.append(
                                {
                                    "type": "recovery_error",
                                    "tp_index": idx,
                                    "error": "immutable TP snapshot has no valid target",
                                }
                            )
                            await _write_status(
                                "manual_required",
                                f"immutable TP snapshot is invalid for TP{idx}",
                                {"recovery": actions},
                            )
                            await _notify(
                                notify,
                                user_id,
                                f"🚨 Нужна ручная проверка {symbol}\n"
                                f"В зафиксированном плане отсутствует корректная цена TP{idx}. "
                                "Бот не будет создавать ордер по пересчитанным данным.",
                            )
                            processed += 1
                            failed = True
                            break
                        # Use the original exact TP slice.  The old code treated
                        # the last *missing* TP as the last TP of the whole plan
                        # and could market-close the entire remaining position
                        # when only one middle target was missing.
                        planned_qty = min(
                            remaining_cap,
                            float(tp_qty_by_index.get(idx, 0.0)),
                        )
                        if planned_qty <= 0:
                            continue
                        operation = ""
                        async with db.symbol_action_lock(user_id, symbol):
                            try:
                                # Fresh safety check before every exchange action.
                                positions_now = await adapter.fetch_open_positions(
                                    symbol, side.upper()
                                )
                                qty_now = min(
                                    _total_position_size(positions_now), managed_total
                                )
                                if qty_now <= max(managed_total * 0.005, 1e-10):
                                    actions.append(
                                        {"type": "closed_on_exchange", "tp_index": idx}
                                    )
                                    await _write_status(
                                        "closed_on_exchange",
                                        "partial_error recovery: position disappeared before TP recovery",
                                        {"recovery": actions},
                                    )
                                    await _notify(
                                        notify,
                                        user_id,
                                        f"ℹ️ Восстановление TP {symbol}\nПозиция закрылась до восстановления TP{idx}.",
                                    )
                                    processed += 1
                                    failed = True
                                    break
                                try:
                                    current = float(
                                        (market_prices or {}).get(symbol)
                                        or await adapter.fetch_last_price(symbol)
                                    )
                                except Exception as exc:
                                    current = None
                                    if not any(
                                        a.get("type") == "price_unavailable"
                                        for a in actions
                                    ):
                                        actions.append(
                                            {
                                                "type": "price_unavailable",
                                                "error": f"{type(exc).__name__}: {exc}",
                                            }
                                        )
                                if current is not None and _breached_stop(
                                    side, current, stop
                                ):
                                    res = (
                                        await adapter.emergency_close_market_confirmed(
                                            symbol=symbol,
                                            side=side,
                                            qty=qty_now,
                                            client_id=f"rec-sl-{execution_id}-{idx}",
                                            position_id=recovery_position_id,
                                        )
                                    )
                                    actions.append(
                                        {
                                            "type": "stop_recovery_market_close",
                                            "qty": qty_now,
                                            "current": current,
                                            "stop": stop,
                                            "result": res,
                                        }
                                    )
                                    await _write_status(
                                        "closed_stop_catchup",
                                        "partial_error recovery: STOP breached during recovery, closed reduceOnly market",
                                        {"recovery": actions},
                                    )
                                    await _notify(
                                        notify,
                                        user_id,
                                        f"🚨 Восстановление TP {symbol}\nSTOP пробит во время восстановления.\n{_fmt_actions(actions)}",
                                    )
                                    processed += 1
                                    failed = True
                                    break
                                planned_qty = min(planned_qty, qty_now)
                                if current is not None and _crossed_tp(
                                    side, current, target
                                ):
                                    operation = "tp_market_close"
                                    res = (
                                        await adapter.emergency_close_market_confirmed(
                                            symbol=symbol,
                                            side=side,
                                            qty=planned_qty,
                                            client_id=f"rec-tp{idx}-{execution_id}",
                                            position_id=recovery_position_id,
                                        )
                                    )
                                    actual_tp_qty = order_normalized_qty(
                                        res, planned_qty
                                    )
                                    actions.append(
                                        {
                                            "type": "tp_recovered_market_close",
                                            "tp_index": idx,
                                            "tp": target,
                                            "qty": actual_tp_qty,
                                            "planned_qty": planned_qty,
                                            "current": current,
                                            "result": res,
                                        }
                                    )
                                else:
                                    operation = "tp_order_create"
                                    res = await adapter.create_take_profit(
                                        symbol=symbol,
                                        side=side,
                                        qty=planned_qty,
                                        price=target,
                                        client_id=f"rec-tp{idx}-{execution_id}",
                                        position_id=recovery_position_id,
                                        adopt_existing=False,
                                    )
                                    actual_tp_qty = order_normalized_qty(
                                        res, planned_qty
                                    )
                                    actions.append(
                                        {
                                            "type": "tp_recovered_order",
                                            "tp_index": idx,
                                            "tp": target,
                                            "qty": actual_tp_qty,
                                            "planned_qty": planned_qty,
                                            "current": current,
                                            "result": res,
                                        }
                                    )
                                remaining_cap = max(0.0, remaining_cap - actual_tp_qty)
                                await _write_status(
                                    "partial_error",
                                    f"partial TP recovery: TP{idx} recovered",
                                    recovery_patch(
                                        extra={
                                            "actual_entry": payload.get("actual_entry")
                                        },
                                        source="partial_tp_recovery.tp_recovered",
                                    ),
                                )
                            except Exception as exc:
                                # PERMANENT errors that retry cannot fix:
                                #   * qty after step-rounding is 0 (e.g. JUPUSDT
                                #     with step=10 and remaining_cap=9.54)
                                #   * remaining position is below minVol
                                # If we keep retrying these, the monitor spams the
                                # user every PROTECTION_POLL_SECONDS forever. Mark
                                # the execution as 'partial_unrecoverable' and stop
                                # re-attempting; if the remainder is small enough
                                # to be untradeable, suggest manual close.
                                exc_text = str(exc).lower()
                                permanent_qty_dust = (
                                    "округление дало 0" in exc_text
                                    or "less than" in exc_text
                                    or "ниже minvol" in exc_text
                                    or (
                                        "меньше шага" in exc_text
                                        and "tp/sl qty" in exc_text
                                    )
                                    or (
                                        "меньше minvol" in exc_text
                                        and "tp/sl qty" in exc_text
                                    )
                                )
                                if permanent_qty_dust:
                                    # Close the *live remaining position quantity*.
                                    # v1.6.2 accidentally used ``current`` here, which
                                    # is the market PRICE, as the close quantity. On a
                                    # low-priced coin this could submit the wrong size;
                                    # on an expensive coin it could request an enormous
                                    # reduce-only quantity. The confirmed close helper
                                    # also prevents a mere API acknowledgement from
                                    # being journalled as a completed close.
                                    dust_qty = max(0.0, float(qty_now))
                                    dust_closed = False
                                    dust_close_error: str | None = None
                                    if dust_qty <= 0:
                                        dust_closed = True
                                    else:
                                        try:
                                            log.info(
                                                "Attempting confirmed dust emergency_close for %s qty=%s",
                                                symbol,
                                                dust_qty,
                                            )
                                            await adapter.emergency_close_market_confirmed(
                                                symbol=symbol,
                                                side=side,
                                                qty=dust_qty,
                                                client_id=f"dust-{execution_id}",
                                            )
                                            dust_closed = True
                                        except Exception as close_exc:
                                            dust_close_error = f"{type(close_exc).__name__}: {close_exc}"

                                    if dust_closed:
                                        actions.append(
                                            {
                                                "type": "tp_dust_auto_closed",
                                                "tp_index": idx,
                                                "qty": dust_qty,
                                                "confirmed": True,
                                            }
                                        )
                                        await _write_status(
                                            "closed_on_exchange",
                                            f"dust remainder ({dust_qty}) confirmed closed market reduceOnly on TP{idx}",
                                            {
                                                "recovery": actions,
                                                "tp_error": {
                                                    "tp_index": idx,
                                                    "permanent": True,
                                                    "auto_closed": True,
                                                    "close_confirmed": True,
                                                },
                                            },
                                        )
                                        await _notify(
                                            notify,
                                            user_id,
                                            f"✅ Закрыл остаток {symbol} автоматически\n"
                                            f"Живой остаток позиции ({dust_qty}) был подтверждённо "
                                            f"закрыт market reduce-only.\n\n"
                                            f"Далее бот проверит и очистит остаточные TP/STOP.",
                                        )
                                        processed += 1
                                        failed = True
                                        break

                                    # A close that was rejected or not confirmed must
                                    # remain visible to the lifecycle guard. Do not use
                                    # the old terminal partial_unrecoverable status.
                                    actions.append(
                                        {
                                            "type": "tp_unrecoverable_dust",
                                            "tp_index": idx,
                                            "qty": dust_qty,
                                            "current_price": current,
                                            "error": f"{type(exc).__name__}: {exc}",
                                            "auto_close_error": dust_close_error,
                                        }
                                    )
                                    await _write_status(
                                        "manual_required",
                                        f"partial TP recovery dust: TP{idx} qty < step and confirmed close failed",
                                        {
                                            "recovery": actions,
                                            "tp_error": {
                                                "tp_index": idx,
                                                "permanent": True,
                                                "reason": "qty_below_step_or_minvol",
                                                "error": f"{type(exc).__name__}: {exc}",
                                                "auto_close_error": dust_close_error,
                                                "manual_required": True,
                                            },
                                        },
                                    )
                                    await _notify(
                                        notify,
                                        user_id,
                                        f"⚠️ Не могу восстановить TP {symbol}\n"
                                        f"Остаток позиции: {dust_qty}. TP поставить нельзя, "
                                        f"а закрытие market reduce-only не было подтверждено: "
                                        f"<i>{dust_close_error or 'неизвестная причина'}</i>\n\n"
                                        f"<b>Что делать:</b> проверь позицию, STOP и TP на BingX вручную.",
                                    )
                                    processed += 1
                                    failed = True
                                    break

                                if isinstance(exc, BingxTpCoverageError):
                                    actions.append(
                                        {
                                            "type": "tp_coverage_conflict",
                                            "tp_index": idx,
                                            "tp": target,
                                            "error": f"{type(exc).__name__}: {exc}",
                                        }
                                    )
                                    await _write_status(
                                        "manual_required",
                                        f"partial TP recovery coverage conflict on TP{idx}: {exc}",
                                        {
                                            "recovery": actions,
                                            "tp_error": {
                                                "tp_index": idx,
                                                "coverage_conflict": True,
                                                "error": f"{type(exc).__name__}: {exc}",
                                            },
                                        },
                                    )
                                    await _notify(
                                        notify,
                                        user_id,
                                        f"🚨 Конфликт TP на BingX {symbol}\n"
                                        f"TP{idx} не был создан, чтобы не превысить объём позиции.\n"
                                        f"Проверь существующие TP вручную.\n"
                                        f"{type(exc).__name__}: {str(exc)[:400]}",
                                    )
                                elif is_transient_exchange_error(exc):
                                    # Read errors are safe to retry. Write errors are ambiguous: the request
                                    # may have reached the exchange even if the response failed. Avoid creating
                                    # duplicate partial TP orders blindly on the next recovery pass.
                                    if operation in {
                                        "tp_order_create",
                                        "tp_market_close",
                                    }:
                                        if operation == "tp_order_create":
                                            client_id = f"rec-tp{idx}-{execution_id}"
                                            found = await find_tp_order_after_ambiguous_write(
                                                adapter,
                                                symbol=symbol,
                                                side=side,
                                                tp_index=idx,
                                                target=float(target),
                                                qty=float(planned_qty),
                                                client_id=client_id,
                                                position_id=recovery_position_id,
                                            )
                                            if found:
                                                actual_tp_qty = order_normalized_qty(
                                                    found, planned_qty
                                                )
                                                actions.append(
                                                    {
                                                        "type": "tp_recovered_order",
                                                        "tp_index": idx,
                                                        "tp": target,
                                                        "qty": actual_tp_qty,
                                                        "planned_qty": planned_qty,
                                                        "current": current,
                                                        "result": found,
                                                        "verified_after_ambiguous": True,
                                                    }
                                                )
                                                remaining_cap = max(
                                                    0.0, remaining_cap - actual_tp_qty
                                                )
                                                await _write_status(
                                                    "partial_error",
                                                    f"partial TP recovery: TP{idx} verified on BingX after ambiguous write; no duplicate created",
                                                    recovery_patch(
                                                        extra={
                                                            "actual_entry": payload.get(
                                                                "actual_entry"
                                                            )
                                                        },
                                                        source="partial_tp_recovery.ambiguous_verified",
                                                    ),
                                                )
                                                await _notify(
                                                    notify,
                                                    user_id,
                                                    f"✅ Восстановление TP {symbol}\nTP{idx} был под вопросом, но бот нашёл ордер на BingX и не создал дубль.\n{_fmt_actions(actions)}",
                                                )
                                                continue
                                        typ = (
                                            "tp_write_ambiguous"
                                            if operation == "tp_order_create"
                                            else "tp_market_close_ambiguous"
                                        )
                                        actions.append(
                                            {
                                                "type": typ,
                                                "tp_index": idx,
                                                "tp": target,
                                                "qty": planned_qty,
                                                "current": current,
                                                "error": f"{type(exc).__name__}: {exc}",
                                            }
                                        )
                                        await _write_status(
                                            "partial_error",
                                            f"partial TP recovery ambiguous write on TP{idx}: {type(exc).__name__}: {exc}",
                                            {
                                                "recovery": actions,
                                                "tp_error": {
                                                    "tp_index": idx,
                                                    "ambiguous": True,
                                                    "error": f"{type(exc).__name__}: {exc}",
                                                },
                                            },
                                        )
                                        await _notify(
                                            notify,
                                            user_id,
                                            f"⚠️ Восстановление TP под вопросом {symbol}\n"
                                            f"TP{idx}: биржа вернула временную ошибку во время действия с ордером.\n"
                                            f"Бот проверил open orders/algo orders и не нашёл подтверждённый TP. Авто-дубль не создаю, чтобы не закрыть лишний объём. Проверь ордера вручную.\n"
                                            f"{_fmt_actions(actions)}",
                                        )
                                    else:
                                        attempts = await persist_transient(
                                            f"tp{idx}", exc
                                        )
                                        if should_notify_transient(
                                            attempts,
                                            every=get_settings().EXCHANGE_TRANSIENT_ERROR_NOTIFY_EVERY,
                                        ):
                                            await _notify(
                                                notify,
                                                user_id,
                                                f"⏳ Временная ошибка биржи при восстановлении TP {symbol}\n"
                                                f"TP{idx}: {type(exc).__name__}: {str(exc)[:300]}\n"
                                                f"Попытка: {attempts}. Бот повторит позже.\n"
                                                f"{_fmt_actions(actions)}",
                                            )
                                else:
                                    actions.append(
                                        {
                                            "type": "recovery_error",
                                            "tp_index": idx,
                                            "error": f"{type(exc).__name__}: {exc}",
                                        }
                                    )
                                    await _write_status(
                                        "partial_error",
                                        f"partial TP recovery failed on TP{idx}: {type(exc).__name__}: {exc}",
                                        {
                                            "recovery": actions,
                                            "tp_error": {
                                                "tp_index": idx,
                                                "error": f"{type(exc).__name__}: {exc}",
                                            },
                                        },
                                    )
                                    await _notify(
                                        notify,
                                        user_id,
                                        f"⚠️ Восстановление TP частично не удалось {symbol}\nОшибка на TP{idx}: {type(exc).__name__}: {str(exc)[:300]}\n{_fmt_actions(actions)}",
                                    )
                                processed += 1
                                failed = True
                                break
                    if failed:
                        continue
                    await _write_status(
                        "protected",
                        "partial_error восстановлен: недостающие TP обработаны",
                        recovery_patch(
                            completed=True,
                            extra={"actual_entry": payload.get("actual_entry")},
                            source="partial_tp_recovery.completed",
                        ),
                    )
                    await _notify(
                        notify,
                        user_id,
                        card(
                            "🛠 <b>TP ВОССТАНОВЛЕНЫ</b>",
                            symbol=symbol,
                            side=side,
                            blocks=(
                                [
                                    f"📦 <b>Текущий объём:</b> {fmt_qty(qty_now)}",
                                    f"🛡 <b>STOP:</b> {fmt_price(stop)}",
                                    f"🎯 <b>Обработано недостающих целей:</b> {len(missing)}",
                                ],
                                [
                                    "✅ Недостающая TP-защита восстановлена",
                                    "🔒 Позиция снова полностью защищена",
                                ],
                                [_fmt_actions(actions)],
                            ),
                        ),
                    )
                    processed += 1
            return processed
        except StaleExecutionPass as stale:
            log.info(
                "%s: stale monitor batch stopped safely execution_id=%s expected=%s attempted=%s",
                stale.source,
                stale.execution_id,
                stale.expected_status,
                stale.attempted_status,
            )
            return processed
        finally:
            set_notification_event_key("")
            if owns_adapter_cache:
                for adapter in adapter_cache.values():
                    try:
                        await adapter.close()
                    except Exception:
                        pass


async def partial_tp_recovery_loop(notify: NotifyFn | None = None) -> None:
    settings = get_settings()
    interval = max(10, int(getattr(settings, "MONITOR_ACTIVE_INTERVAL_SEC", 15) or 15))
    while True:
        try:
            await process_partial_tp_recovery_once(notify=notify)
        except Exception:
            log.exception("partial_tp_recovery_loop iteration failed")
        await asyncio.sleep(interval)
