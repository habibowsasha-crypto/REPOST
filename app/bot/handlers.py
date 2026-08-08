from __future__ import annotations

import html
from contextvars import ContextVar
from functools import wraps
import json
import math
import re
import secrets
from pathlib import Path
from weakref import WeakValueDictionary
from datetime import datetime, timezone

from app import __version__
from app.config import get_settings
from app.database import db
from app.services.encryption import encrypt_text
from app.services.exchange_factory import build_adapter, exchange_title
from app.services.exchange_identity import clean_exchange_id
from app.exchanges.bingx.adapter import BingxAdapter as BingxAdapter
from app.services.models import ExecutionResult, Signal, TpMode, UserMode
from app.services.trade_notification_policy import mandatory_trade_warning_payload
from app.services.notifications import (
    admin_batch_summary,
    admin_decimal_normalization_preview_message,
    is_suppressible_trade_skip_notification,
    user_result_message,
    system_api_connected_message,
    system_api_disabled_message,
    system_api_error_message,
    system_mode_message,
    system_risk_message,
    system_tp_message,
    system_be_message,
    whitelist_preview_message,
)
from app.services.notification_style import (
    ensure_visual_card,
    fmt_price,
    fmt_qty,
    premium_arrow_lines,
)
from app.services.dashboard import summarize_dashboard
from app.services.subscriber_stats import summarize_subscribers
from app.services.ttl_cache import (
    get_api_key_cache,
    get_dashboard_cache,
    get_user_settings_cache,
)
from app.services.risk_engine import (
    validate_risk_percent,
    validate_daily_risk_limit_percent,
    validate_max_portfolio_risk_percent,
    validate_max_open_trades,
)
from app.services.limit_policy import (
    PRESETS as LIMIT_POLICY_PRESETS,
    RUNTIME_KEY as LIMIT_POLICY_RUNTIME_KEY,
    preset_label as limit_preset_label,
    read_policy as read_limit_policy,
    threshold_index as limit_threshold_index,
    tp_mode_label as limit_tp_mode_label,
)
from app.services.limit_tp_catchup import (
    PendingEntryCancelDisposition,
    _cancel_opening_order_remainder_confirmed,
    _cancel_pending_entry_confirmed,
    _limit_cancel_control,
    _limit_cancel_pending_record,
    _terminal_no_fill_classification,
)
from app.services.telegram_delivery import mark_private_chat_ready
from app.services.admin_only_mode import admin_only_enabled, configured_admin_ids
from app.services.execution_dispatcher import get_trade_dispatcher
from app.services.execution_metrics import execution_metrics_snapshot
from app.services.workload_manager import bingx_workload_stats
from app.services.notification_dispatcher import (
    probe_queued_private_chat,
    send_queued_private_message,
)
from app.services.durable_notifications import send_or_enqueue
from app.services.signal_executor import execute_signal_for_user, background_tp_stats
from app.services.event_driven_monitor import PRICE_STREAM_DEGRADED
from app.services.market_event_rollout import market_event_stage_allows_group
from app.services.manual_position_actions import (
    close_position_fully,
    force_position_break_even,
    managed_execution_for_position,
)
from app.services.be_recovery_admin import (
    inspect_existing_be_recovery,
    execute_admin_existing_be_cleanup,
)
from app.services.signal_parser import parse_signal, signal_hash
from app.services.signal_analytics_ingress import (
    submit_signal_analytics_shadow,
    submit_statistics_trade_group_linkage,
)
from app.services.statistics_periods import (
    cancel_statistics_reset,
    confirm_statistics_reset,
    create_statistics_reset_request,
)
from app.services.statistics_reports_v2 import (
    export_statistics_period_zip,
    format_statistics_all_report,
    format_statistics_financial_report,
    format_statistics_period_report,
    format_statistics_periods_report,
    format_statistics_technical_report,
    format_statistics_quality_report,
)
from app.services.statistics_recovery import (
    format_statistics_recovery_report,
    get_statistics_recovery_candidate,
    request_statistics_recovery,
)
from app.services.bingx_contract_aliases import canonicalize_bingx_1000_signal
from app.services.price_anomaly import detect_signal_price_anomaly
from app.services.signal_decimal_normalizer import decimal_normalization_preview_payload
from app.services.terms import TERMS_FILENAME, TERMS_VERSION, terms_bytes, terms_hash

import asyncio
import logging
import time

try:
    from aiogram import Router, F
    from aiogram.filters import Command
    from aiogram.fsm.context import FSMContext
    from aiogram.fsm.state import State, StatesGroup
    from aiogram.types import CallbackQuery, Message, BufferedInputFile
    from app.bot.keyboards import (
        active_limit_cancel_confirm_menu,
        active_limit_detail_menu,
        active_limit_result_menu,
        active_limits_list_menu,
        exchange_connected_menu,
        exchange_need_api_menu,
        be_apply_confirm_menu,
        limit_apply_confirm_menu,
        limit_ttl_cancel_menu,
        main_menu,
        parse_callback_owner,
        position_action_result_menu,
        position_be_confirm_menu,
        position_close_confirm_menu,
        position_detail_menu,
        positions_list_menu,
        start_welcome_menu,
        subscribers_admin_menu,
        signal_analytics_admin_menu,
        statistics_technical_admin_menu,
        statistics_reset_confirm_menu,
        statistics_recovery_confirm_menu,
        terms_accept_menu,
    )
except Exception:  # pragma: no cover
    Router = None
    StatesGroup = object  # type: ignore

    class State:  # type: ignore
        pass


class ApiSetup(StatesGroup):
    """Multi-step interactive /api setup.

    State storage holds the BingX API_KEY and API_SECRET. Each user message
    containing a secret is deleted from chat history immediately when Telegram
    permits. We never log the secrets themselves.
    """

    waiting_key = State()
    waiting_secret = State()
    waiting_passphrase = State()


class LimitTtlSetup(StatesGroup):
    """One-message editor for the user's stale LIMIT lifetime."""

    waiting_hours = State()


class WhitelistAdd(StatesGroup):
    """FSM for admin's "➕ Добавить юзера" button in white-list menu.

    State waiting_uid: admin pastes the target user's telegram_id.  After
    the id is validated we show the standard per-user card so the admin
    can grant or revoke BingX trading access.
    """

    waiting_uid = State()


log = logging.getLogger(__name__)
router = Router() if Router else None


async def _send_trade_result_notification(
    bot, user_id: int, text: str
) -> tuple[bool, str]:
    """Deliver a trade result through the independent Telegram queue."""
    outcome = await send_queued_private_message(
        bot,
        int(user_id),
        text,
        parse_mode="HTML",
        attempts=3,
        log_context="trade notification",
    )
    return outcome.delivered, outcome.error or outcome.code


async def _send_signal_batch_summary_to_source(message: Message, text: str) -> bool:
    """Return only the aggregate VIP signal card to its source chat.

    G68 intentionally separates destinations: per-trade lifecycle cards remain
    private user/admin notifications, while ``VIP-СИГНАЛ ОБРАБОТАН`` is posted
    back to the trusted source chat that produced the signal. A Telegram send
    failure is informational and must never change an already completed trade
    dispatch result.
    """
    chat = getattr(message, "chat", None)
    chat_id = getattr(chat, "id", None)
    chat_type = str(getattr(chat, "type", "") or "")
    if chat_id is None or chat_type not in {
        "private",
        "group",
        "supergroup",
        "channel",
    }:
        log.warning(
            "signal batch summary source delivery skipped chat_id=%s chat_type=%s",
            chat_id,
            chat_type or "unknown",
        )
        return False
    try:
        await message.answer(text, parse_mode="HTML")
        log.info(
            "signal batch summary delivered to source chat chat_id=%s chat_type=%s admin_only=%s",
            chat_id,
            chat_type,
            admin_only_enabled(),
        )
        return True
    except Exception as exc:
        log.warning(
            "signal batch summary source delivery failed chat_id=%s chat_type=%s error=%s",
            chat_id,
            chat_type,
            f"{type(exc).__name__}: {exc}",
        )
        return False


async def _notify_admins_signal_price_anomaly(
    bot,
    signal: Signal,
    results: list[ExecutionResult],
) -> dict[int, bool]:
    """Send one identical blocked-signal card to every configured admin.

    An admin who already received the same card as the affected trading user is
    not sent a duplicate. Failed user delivery is retried through the admin path.
    """
    anomaly_result = next(
        (
            result
            for result in results
            if result.payload.get("signal_price_anomaly") is True
        ),
        None,
    )
    if anomaly_result is None:
        return {}

    admin_ids = {int(uid) for uid in get_settings().admin_ids}
    delivered_as_user = {
        int(result.user_id)
        for result in results
        if int(result.user_id) in admin_ids
        and result.payload.get("signal_price_anomaly") is True
        and result.payload.get("notification_delivered") is True
    }
    has_admin_preview = (
        anomaly_result.payload.get("decimal_normalization_preview") is True
    )
    # The decimal-normalization card is intentionally admin-only.  Even if the
    # admin already received the generic blocked-signal user card, still send
    # the private diagnostic card once through the admin path.
    pending = sorted(
        admin_ids if has_admin_preview else (admin_ids - delivered_as_user)
    )
    if not pending:
        return {uid: True for uid in delivered_as_user}

    text = (
        admin_decimal_normalization_preview_message(signal, anomaly_result)
        if has_admin_preview
        else user_result_message(signal, anomaly_result)
    )

    async def _send(uid: int) -> tuple[int, bool]:
        outcome = await send_queued_private_message(
            bot,
            uid,
            text,
            parse_mode="HTML",
            attempts=3,
            log_context="admin signal price anomaly",
        )
        if not outcome.delivered:
            log.warning(
                "admin anomaly notification failed uid=%s symbol=%s code=%s error=%s",
                uid,
                str(signal.symbol).upper(),
                outcome.code,
                outcome.error,
            )
        return uid, bool(outcome.delivered)

    sent = await asyncio.gather(*[_send(uid) for uid in pending])
    result_map = {uid: ok for uid, ok in sent}
    if not has_admin_preview:
        result_map.update({uid: True for uid in delivered_as_user})
    return result_map


async def _notify_admins_api_quarantine(
    bot,
    signal: Signal,
    results: list[ExecutionResult],
) -> dict[int, bool]:
    pending_by_user: dict[int, ExecutionResult] = {}
    for result in results:
        if (
            result.payload.get("api_permission_quarantine") is True
            and result.payload.get("api_quarantine_active") is True
            and result.payload.get("api_quarantine_admin_notification_pending") is True
        ):
            pending_by_user.setdefault(int(result.user_id), result)
    if not pending_by_user:
        return {}
    admin_ids = sorted({int(uid) for uid in get_settings().admin_ids})
    if not admin_ids:
        return {}

    delivered_by_user: dict[int, bool] = {}
    for affected_uid, result in pending_by_user.items():
        incident_token = str(result.payload.get("api_quarantine_incident_token") or "")[
            :128
        ]
        claim_acquired = False
        claim_failed = False
        if incident_token:
            try:
                claim_acquired = await db.claim_api_key_quarantine_notification(
                    affected_uid,
                    "admin",
                    incident_token,
                    exchange="bingx",
                )
            except Exception:
                # The notification is safety-relevant. Fail open on a temporary
                # claim error, but do not pretend the durable anti-spam claim exists.
                claim_failed = True
                log.exception(
                    "failed to claim admin API quarantine notification uid=%s",
                    affected_uid,
                )
            if not claim_acquired and not claim_failed:
                # Another concurrent signal already owns delivery for this exact
                # incident. Do not send a duplicate admin card.
                delivered_by_user[affected_uid] = False
                continue

        code = html.escape(
            str(result.payload.get("api_quarantine_error_code") or "100004")
        )
        endpoint = html.escape(
            str(result.payload.get("api_quarantine_endpoint") or "не указан")[:200]
        )
        text = ensure_visual_card(
            "🔐 <b>API-КЛЮЧ ПОМЕЩЁН В КАРАНТИН</b>\n"
            "━━━━━━━━━━━━━━━━━━━━\n\n"
            f"👤 <b>User ID:</b> <code>{affected_uid}</code>\n"
            f"🪙 <b>Сигнал:</b> {html.escape(str(signal.symbol).upper())}\n"
            f"🧾 <b>Код BingX:</b> <code>{code}</code>\n"
            f"🔗 <b>Endpoint:</b> <code>{endpoint}</code>\n\n"
            "🛡 Новые торговые попытки этим ключом остановлены.\n"
            "🔧 Пользователь должен включить Read и Futures Trading, "
            "затем заново подключить API в боте."
        )

        async def _send(admin_uid: int) -> bool:
            try:
                outcome = await send_queued_private_message(
                    bot,
                    admin_uid,
                    text,
                    parse_mode="HTML",
                    attempts=3,
                    log_context="admin api quarantine",
                )
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                log.exception(
                    "admin API quarantine notification crashed admin_uid=%s affected_uid=%s error=%s",
                    admin_uid,
                    affected_uid,
                    type(exc).__name__,
                )
                return False
            if not outcome.delivered:
                log.warning(
                    "admin API quarantine notification failed admin_uid=%s affected_uid=%s code=%s error=%s",
                    admin_uid,
                    affected_uid,
                    outcome.code,
                    outcome.error,
                )
            return bool(outcome.delivered)

        sent = await asyncio.gather(
            *[_send(uid) for uid in admin_ids], return_exceptions=True
        )
        delivered = any(item is True for item in sent)
        delivered_by_user[affected_uid] = delivered
        if delivered:
            try:
                marked = await db.mark_api_key_quarantine_notified(
                    affected_uid,
                    "admin",
                    exchange="bingx",
                    incident_token=incident_token,
                )
                if marked:
                    result.payload["api_quarantine_admin_notification_pending"] = False
                else:
                    log.warning(
                        "admin API quarantine delivery belongs to stale incident uid=%s",
                        affected_uid,
                    )
            except Exception:
                log.exception(
                    "failed to persist admin API quarantine notification uid=%s",
                    affected_uid,
                )
        elif claim_acquired:
            try:
                await db.release_api_key_quarantine_notification_claim(
                    affected_uid,
                    "admin",
                    incident_token,
                    exchange="bingx",
                )
            except Exception:
                log.exception(
                    "failed to release admin API quarantine claim uid=%s",
                    affected_uid,
                )
    return delivered_by_user

    # Bound callback ownership prevents one group member from using another
    # member's inline menu. The stable payload remains available to legacy handler
    # parsing through a task-local context variable.


_CALLBACK_PAYLOAD: ContextVar[str | None] = ContextVar(
    "antilud_callback_payload", default=None
)


def _callback_payload(call: CallbackQuery) -> str:
    current = _CALLBACK_PAYLOAD.get()
    if current is not None:
        return current
    return parse_callback_owner(getattr(call, "data", None))[1]


async def _authorize_callback_owner(call: CallbackQuery) -> str | None:
    owner_id, payload = parse_callback_owner(getattr(call, "data", None))
    actor_id = int(getattr(getattr(call, "from_user", None), "id", 0) or 0)
    if owner_id is not None and owner_id != actor_id:
        await call.answer(
            "⚠️ Это меню другого пользователя. Отправьте «меню», чтобы открыть своё.",
            show_alert=True,
        )
        return None

    chat_type = str(
        getattr(getattr(getattr(call, "message", None), "chat", None), "type", "") or ""
    ).lower()
    if owner_id is None and chat_type in {"group", "supergroup", "channel"}:
        await call.answer(
            "⚠️ Это старое меню без привязки. Отправьте «меню», чтобы открыть защищённое меню.",
            show_alert=True,
        )
        return None
    return payload


def _is_stale_callback_query_error(exc: BaseException) -> bool:
    """Recognize Telegram's harmless expired callback-answer error exactly."""

    if type(exc).__name__ != "TelegramBadRequest":
        return False
    text = str(exc).lower()
    return (
        "query is too old" in text
        or "response timeout expired" in text
        or "query id is invalid" in text
    )


def _log_stale_callback_suppressed(call: CallbackQuery) -> None:
    uid = int(getattr(getattr(call, "from_user", None), "id", 0) or 0)
    raw_payload = str(getattr(call, "data", "") or "")
    action = raw_payload.split(":", 1)[0][:32] or "unknown"
    log.info(
        "STALE_CALLBACK_QUERY_SUPPRESSED uid=%s action=%s",
        uid,
        action,
    )


def _owner_guarded_callback(handler):
    @wraps(handler)
    async def wrapped(call: CallbackQuery, *args, **kwargs):
        token = None
        try:
            payload = await _authorize_callback_owner(call)
            if payload is None:
                return None
            token = _CALLBACK_PAYLOAD.set(payload)
            return await handler(call, *args, **kwargs)
        except Exception as exc:
            # A callback received immediately after a Railway redeploy can be
            # older than Telegram's answer window.  The action itself is not a
            # trading failure and must not produce an aiogram traceback.  Only
            # this exact TelegramBadRequest is suppressed; every other error is
            # still propagated to the normal error middleware.
            if _is_stale_callback_query_error(exc):
                _log_stale_callback_suppressed(call)
                return None
            raise
        finally:
            if token is not None:
                _CALLBACK_PAYLOAD.reset(token)

    return wrapped

    # Per-user guard for inline menu callbacks. A rapid sequence of taps must not
    # start duplicate BingX requests or send duplicate menu cards. The guard is
    # deliberately process-local: it protects one running bot replica, while
    # Telegram polling itself must still run with a single replica/token.


_menu_callback_busy: set[int] = set()
_menu_callback_last_done: dict[tuple[int, str], float] = {}
_MENU_REPEAT_COOLDOWN_SEC = 0.8

# One-time confirmation snapshots for applying a new LIMIT policy to already
# pending orders.  The token binds the button to exactly what the user previewed
# and to the exact execution ids visible at that moment.  A Railway restart
# invalidates old buttons, which is safer than applying stale settings.
_limit_apply_tokens: dict[str, dict[str, object]] = {}
_LIMIT_APPLY_TOKEN_TTL_SEC = 300.0

# One-time confirmation snapshots for applying the currently selected BE
# trigger to executions that already existed before the user changed the menu.
# The snapshot binds the confirmation to the exact user, trigger and execution
# ids shown in preview. Railway restart intentionally invalidates old buttons.
_be_apply_tokens: dict[str, dict[str, object]] = {}
_BE_APPLY_TOKEN_TTL_SEC = 300.0

# One-time admin confirmation snapshots for g7a exact old-STOP cleanup.
# The token binds the destructive command to the exact live topology, exact
# execution and exact old STOP ids shown in the read-only preview. A Railway
# restart invalidates all tokens, which is safer than accepting stale approval.
_be_recovery_admin_tokens: dict[str, dict[str, object]] = {}
_BE_RECOVERY_ADMIN_TOKEN_TTL_SEC = 300.0

# Profile buttons can be tapped rapidly or from several still-visible Telegram
# messages.  Serialize the complete save+render transaction per user so the
# final checkmark cannot disagree with the value actually stored in the DB.
_LIMIT_POLICY_MENU_LOCKS: WeakValueDictionary[int, asyncio.Lock] = WeakValueDictionary()
_BE_POLICY_MENU_LOCKS: WeakValueDictionary[int, asyncio.Lock] = WeakValueDictionary()
_SKIP_NOTIFICATION_MENU_LOCKS: WeakValueDictionary[int, asyncio.Lock] = WeakValueDictionary()


def _limit_policy_menu_lock(user_id: int) -> asyncio.Lock:
    """Return one live lock per user without unsafe bounded-cache eviction.

    A bounded OrderedDict could evict an unlocked lock while another task had
    already obtained a reference but had not awaited it yet. A new callback
    could then create a second lock for the same user. Weak references remove
    idle locks automatically while every waiter/owner keeps the shared lock
    alive through its local strong reference.
    """
    uid = int(user_id)
    lock = _LIMIT_POLICY_MENU_LOCKS.get(uid)
    if lock is None:
        lock = asyncio.Lock()
        _LIMIT_POLICY_MENU_LOCKS[uid] = lock
    return lock


def _be_policy_menu_lock(user_id: int) -> asyncio.Lock:
    """Serialize BE setting preview/confirm operations for one user."""
    uid = int(user_id)
    lock = _BE_POLICY_MENU_LOCKS.get(uid)
    if lock is None:
        lock = asyncio.Lock()
        _BE_POLICY_MENU_LOCKS[uid] = lock
    return lock


def _skip_notification_menu_lock(user_id: int) -> asyncio.Lock:
    """Serialize skip-notification save, read-back and render per user."""
    uid = int(user_id)
    lock = _SKIP_NOTIFICATION_MENU_LOCKS.get(uid)
    if lock is None:
        lock = asyncio.Lock()
        _SKIP_NOTIFICATION_MENU_LOCKS[uid] = lock
    return lock


async def _clear_legacy_limit_ttl_state(state: FSMContext | None) -> bool:
    """Cancel the custom-TTL FSM when navigation or a preset supersedes it.

    The historical helper name is retained for compatibility. After a user
    leaves the custom-hours prompt, their next ordinary message must not be
    consumed as a surprise TTL value.
    """
    if state is None:
        return False
    try:
        current = await state.get_state()
        expected = getattr(LimitTtlSetup.waiting_hours, "state", None)
        if current and expected and current == expected:
            await state.clear()
            return True
    except Exception as exc:
        log.debug("Could not clear legacy LIMIT TTL FSM state: %s", exc)
    return False


def _prune_limit_apply_tokens(now: float | None = None) -> None:
    current = time.monotonic() if now is None else float(now)
    stale = [
        token
        for token, payload in _limit_apply_tokens.items()
        if current - float(payload.get("created_monotonic") or 0.0)
        > _LIMIT_APPLY_TOKEN_TTL_SEC
    ]
    for token in stale:
        _limit_apply_tokens.pop(token, None)


def _create_limit_apply_token(
    *,
    user_id: int,
    ttl_hours: int,
    tp_mode: str,
    preset: str,
    execution_ids: list[int],
) -> str:
    _prune_limit_apply_tokens()
    for old_token, payload in list(_limit_apply_tokens.items()):
        if int(payload.get("user_id") or 0) == int(user_id):
            _limit_apply_tokens.pop(old_token, None)
            # 12 hex chars keep Telegram callback_data comfortably below 64 bytes.
    token = secrets.token_hex(6)
    _limit_apply_tokens[token] = {
        "user_id": int(user_id),
        "ttl_hours": int(ttl_hours),
        "tp_mode": str(tp_mode),
        "preset": str(preset),
        "execution_ids": [int(x) for x in execution_ids if int(x) > 0],
        "created_monotonic": time.monotonic(),
    }
    return token


def _consume_limit_apply_token(token: str, user_id: int) -> dict[str, object] | None:
    _prune_limit_apply_tokens()
    normalized = str(token or "")
    payload = _limit_apply_tokens.get(normalized)
    if not payload or int(payload.get("user_id") or 0) != int(user_id):
        return None
        # Consume only after ownership validation. A foreign/forwarded callback must
        # not invalidate the real user's still-valid confirmation token.
    _limit_apply_tokens.pop(normalized, None)
    return payload


def _prune_be_apply_tokens(now: float | None = None) -> None:
    current = time.monotonic() if now is None else float(now)
    stale = [
        token
        for token, payload in _be_apply_tokens.items()
        if current - float(payload.get("created_monotonic") or 0.0)
        > _BE_APPLY_TOKEN_TTL_SEC
    ]
    for token in stale:
        _be_apply_tokens.pop(token, None)


def _create_be_apply_token(
    *, user_id: int, trigger_tp_index: int, execution_ids: list[int]
) -> str:
    _prune_be_apply_tokens()
    for old_token, payload in list(_be_apply_tokens.items()):
        if int(payload.get("user_id") or 0) == int(user_id):
            _be_apply_tokens.pop(old_token, None)
    token = secrets.token_hex(6)
    _be_apply_tokens[token] = {
        "user_id": int(user_id),
        "trigger_tp_index": int(trigger_tp_index),
        "execution_ids": [int(x) for x in execution_ids if int(x) > 0],
        "created_monotonic": time.monotonic(),
    }
    return token


def _invalidate_be_apply_tokens_for_user(user_id: int) -> None:
    for token, payload in list(_be_apply_tokens.items()):
        if int(payload.get("user_id") or 0) == int(user_id):
            _be_apply_tokens.pop(token, None)


def _consume_be_apply_token(token: str, user_id: int) -> dict[str, object] | None:
    _prune_be_apply_tokens()
    normalized = str(token or "")
    payload = _be_apply_tokens.get(normalized)
    if not payload or int(payload.get("user_id") or 0) != int(user_id):
        return None
    _be_apply_tokens.pop(normalized, None)
    return payload


def _prune_be_recovery_admin_tokens(now: float | None = None) -> None:
    current = time.monotonic() if now is None else float(now)
    stale = [
        token
        for token, payload in _be_recovery_admin_tokens.items()
        if current - float(payload.get("created_monotonic") or 0.0)
        > _BE_RECOVERY_ADMIN_TOKEN_TTL_SEC
    ]
    for token in stale:
        _be_recovery_admin_tokens.pop(token, None)


def _create_be_recovery_admin_token(
    *, admin_user_id: int, inspection: dict[str, object]
) -> str:
    _prune_be_recovery_admin_tokens()
    for old_token, payload in list(_be_recovery_admin_tokens.items()):
        if int(payload.get("admin_user_id") or 0) == int(admin_user_id):
            _be_recovery_admin_tokens.pop(old_token, None)
    token = secrets.token_hex(6)
    _be_recovery_admin_tokens[token] = {
        "admin_user_id": int(admin_user_id),
        "execution_id": int(inspection.get("execution_id") or 0),
        "topology_fingerprint": str(
            inspection.get("topology_fingerprint") or ""
        ),
        "allowed_old_stop_ids": sorted(
            {
                clean_exchange_id(value)
                for value in list(inspection.get("allowed_old_stop_ids") or [])
                if clean_exchange_id(value)
            }
        ),
        "created_monotonic": time.monotonic(),
    }
    return token


def _consume_be_recovery_admin_token(
    token: str, *, admin_user_id: int, selected_old_stop_ids: set[str]
) -> dict[str, object] | None:
    _prune_be_recovery_admin_tokens()
    normalized = str(token or "").strip()
    payload = _be_recovery_admin_tokens.get(normalized)
    selected = {clean_exchange_id(value) for value in selected_old_stop_ids}
    selected.discard("")
    if not payload or int(payload.get("admin_user_id") or 0) != int(
        admin_user_id
    ):
        return None
    allowed = {
        clean_exchange_id(value)
        for value in list(payload.get("allowed_old_stop_ids") or [])
    }
    allowed.discard("")
    if not selected or selected != allowed:
        return None
    _be_recovery_admin_tokens.pop(normalized, None)
    return payload


def _try_begin_menu_callback(
    user_id: int, action: str, now: float | None = None
) -> tuple[bool, str]:
    """Acquire the per-user menu guard without waiting.

    Returns ``(False, reason)`` for an already-running request or for an
    accidental repeat immediately after a completed callback. This keeps
    Telegram taps responsive and avoids duplicate exchange/database work.
    """
    current = time.monotonic() if now is None else float(now)
    if user_id in _menu_callback_busy:
        return False, "busy"
    last_done = _menu_callback_last_done.get((user_id, action))
    if last_done is not None and current - last_done < _MENU_REPEAT_COOLDOWN_SEC:
        return False, "cooldown"
    _menu_callback_busy.add(user_id)
    return True, ""


def _finish_menu_callback(user_id: int, action: str, now: float | None = None) -> None:
    current = time.monotonic() if now is None else float(now)
    _menu_callback_busy.discard(user_id)
    _menu_callback_last_done[(user_id, action)] = current

    # Bound process memory for long-running public bots. Only recent click
    # timestamps are useful; discard stale entries opportunistically.
    if len(_menu_callback_last_done) > 2000:
        cutoff = current - 60.0
        stale = [key for key, ts in _menu_callback_last_done.items() if ts < cutoff]
        for key in stale:
            _menu_callback_last_done.pop(key, None)


def _is_admin(user_id: int | None) -> bool:
    return user_id is not None and user_id in get_settings().admin_ids


async def _cached_api_key(user_id: int, exchange: str = "bingx"):
    exchange = str(exchange or "bingx").lower().strip()
    return await get_api_key_cache().get_or_fetch(
        (int(user_id), "api", exchange),
        lambda: db.get_api_key(int(user_id), exchange),
    )


async def _cached_user_settings(user_id: int):
    return await get_user_settings_cache().get_or_fetch(
        (int(user_id), "settings"),
        lambda: db.get_user_settings(int(user_id)),
    )


def _home_menu(user_id: int | None):
    """Render the home menu with admin-only buttons enabled when applicable.

    Use this everywhere we'd write ``main_menu()`` (no args).  For specific
    sub-sections (mode, exchanges, risk, tp, be) keep calling ``main_menu(section)``
    directly — those screens don't show admin shortcuts.
    """
    return main_menu("home", is_admin=_is_admin(user_id), owner_id=user_id)


async def _signal_analytics_text(user_id: int | None) -> str:
    if not _is_admin(user_id):
        return "⛔ <b>Раздел доступен только администратору.</b>"
    settings = get_settings()
    if bool(settings.STATS_V2_REPORTS_ENABLED):
        if not bool(settings.STATISTICS_PERIODS_ENABLED):
            return (
                "⏸ <b>Периодические отчёты включены не полностью</b>\n\n"
                "Для statistics-v2 требуется:\n"
                "<code>STATISTICS_PERIODS_ENABLED=true</code>\n"
                "<code>STATS_V2_REPORTS_ENABLED=true</code>\n\n"
                "Торговля продолжает работать независимо от отчётов."
            )
        try:
            return await format_statistics_period_report(user_id=user_id)
        except LookupError:
            return (
                "⚠️ <b>Активный период статистики не найден.</b>\n\n"
                "Проверьте миграцию statistics-v2. Физическое удаление истории не выполняется."
            )
        except Exception as exc:
            log.exception(
                "STATISTICS_V2_REPORT_FAILED user_id=%s error=%s",
                user_id,
                f"{type(exc).__name__}: {exc}",
            )
            return (
                "⚠️ <b>Не удалось получить statistics-v2 отчёт</b>\n\n"
                "Торговое ядро продолжает работать. Проверьте Railway-логи "
                "по метке <code>STATISTICS_V2_REPORT_FAILED</code>."
            )
    return (
        "⏸ <b>Account-scoped statistics-v2 выключена</b>\n\n"
        "Старый общий отчёт намеренно не показывается, потому что он смешивает "
        "исполнения разных BingX-аккаунтов.\n\n"
        "Для статистики только моего аккаунта требуется "
        "<code>STATS_V2_REPORTS_ENABLED=true</code>. "
        "Торговля продолжает работать независимо от отчётов."
    )


def _stats_period_argument(message: Message, command: str) -> int | None:
    text = str(getattr(message, "text", "") or "").strip()
    if not text:
        return None
    parts = text.split(maxsplit=1)
    if len(parts) < 2:
        return None
    raw = parts[1].strip()
    if not raw:
        return None
    try:
        value = int(raw)
    except (TypeError, ValueError):
        raise ValueError(f"Использование: /{command} ID")
    if value <= 0:
        raise ValueError(f"Использование: /{command} ID")
    return value


async def _send_signal_analytics_export(
    message: Message,
    user_id: int | None,
    *,
    period_id: int | None = None,
) -> bool:
    if not _is_admin(user_id):
        await message.answer("⛔ Раздел доступен только администратору.")
        return False
    settings = get_settings()
    if bool(settings.STATS_V2_REPORTS_ENABLED):
        if not bool(settings.STATISTICS_PERIODS_ENABLED):
            await message.answer(
                "⏸ Для period export включите STATISTICS_PERIODS_ENABLED=true."
            )
            return False
        try:
            export = await export_statistics_period_zip(
                period_id=period_id,
                admin_export=True,
                user_id=int(user_id),
            )
            caption_lines = [
                "📦 <b>Экспорт статистики периода</b>",
                f"Период: <b>#{export.period_id}</b> — <b>{html.escape(export.period_name)}</b>",
                "Область: только мой BingX аккаунт.",
                "Внутри: связанные signals, events, executions, fills, funding, quality, simulations и metadata.",
                "Формат CSV: UTF-8 BOM, formula-injection protection.",
            ]
            if export.truncated:
                caption_lines.append(
                    "⚠️ Экспорт ограничен безопасным row limit; metadata содержит точные totals."
                )
            await message.answer_document(
                BufferedInputFile(export.payload, filename=export.filename),
                caption="\n".join(caption_lines),
            )
            return True
        except LookupError:
            await message.answer("⚠️ Указанный период статистики не найден.")
            return False
        except Exception as exc:
            log.exception(
                "STATISTICS_V2_EXPORT_FAILED user_id=%s period_id=%s error=%s",
                user_id,
                period_id,
                f"{type(exc).__name__}: {exc}",
            )
            await message.answer(
                "⚠️ Не удалось сформировать ZIP/CSV. Торговля продолжает работать."
            )
            return False
    await message.answer(
        "⏸ Account-scoped export доступен только при "
        "STATS_V2_REPORTS_ENABLED=true. Старый общий CSV намеренно отключён, "
        "чтобы не смешивать другие BingX-аккаунты с моей статистикой."
    )
    return False


async def _send_statistics_reset_preview(
    message: Message,
    user_id: int | None,
    *,
    reason: str | None = None,
) -> bool:
    if not _is_admin(user_id):
        await message.answer("⛔ Раздел доступен только администратору.")
        return False
    settings = get_settings()
    if not bool(settings.STATISTICS_PERIODS_ENABLED):
        await message.answer("⏸ STATISTICS_PERIODS_ENABLED=false.")
        return False
    if not bool(settings.STATS_RESET_ENABLED):
        await message.answer(
            "⏸ Безопасный reset выключен. Включите STATS_RESET_ENABLED=true только на контролируемом этапе."
        )
        return False
    try:
        request = await create_statistics_reset_request(
            actor_user_id=int(user_id),
            reason=reason,
        )
    except Exception as exc:
        log.exception(
            "STATISTICS_RESET_PREVIEW_FAILED user_id=%s error=%s",
            user_id,
            f"{type(exc).__name__}: {exc}",
        )
        await message.answer(
            "⚠️ Не удалось подготовить подтверждение. Никакие данные не изменены."
        )
        return False
    await message.answer(
        "\n".join(
            [
                "<b>⚠️ СОЗДАНИЕ НОВОГО ПЕРИОДА</b>",
                "",
                f"Текущий период: <b>#{request.active_period_id}</b> — <b>{html.escape(request.active_period_name)}</b>",
                f"Причина: <b>{html.escape(request.reason)}</b>",
                "",
                "После подтверждения текущий период будет закрыт, а новый станет active.",
                "Старые signals, executions, fills, funding и quality-аудит <b>не удаляются</b>.",
                "Уже активные старые сигналы продолжат завершаться в старом периоде.",
                "",
                "Кнопка одноразовая и действует 10 минут.",
            ]
        ),
        reply_markup=statistics_reset_confirm_menu(
            request.request_id,
            request.token,
            owner_id=user_id,
        ),
    )
    return True

def _sender_id(message: Message) -> int | None:
    user = getattr(message, "from_user", None)
    return getattr(user, "id", None) if user is not None else None


def _sender_username(message: Message) -> str | None:
    user = getattr(message, "from_user", None)
    return getattr(user, "username", None) if user is not None else None


def _sender_chat_identity(message: Message) -> dict[str, str | int | None]:
    sender_chat = getattr(message, "sender_chat", None)
    if sender_chat is None:
        return {"id": None, "title": None, "username": None}
    return {
        "id": getattr(sender_chat, "id", None),
        "title": getattr(sender_chat, "title", None),
        "username": getattr(sender_chat, "username", None),
    }


def _vip_signal_source_allowed(message: Message) -> tuple[bool, str]:
    """Return whether a parsed VIP signal is allowed to execute.

    Commands remain available everywhere. This guard applies only after text was
    parsed as a signal. It prevents accidental executions from DMs or regular
    group users and allows only posts sent on behalf of the configured channel.
    """
    settings = get_settings()
    chat_type = getattr(getattr(message, "chat", None), "type", "") or ""

    # ADMIN EXCEPTION: a configured administrator may submit a personal
    # signal only from the bot's private chat. In groups/supergroups the same
    # trusted-source and sender_chat rules apply to admins as to everyone else,
    # preventing an accidental paste in an unrelated group from opening trades
    # for all AUTO users.
    sender_user_id = getattr(getattr(message, "from_user", None), "id", None)
    if chat_type == "private" and _is_admin(sender_user_id):
        return True, ""

    if chat_type == "private" and not settings.VIP_EXECUTE_PRIVATE_SIGNALS:
        return (
            False,
            "VIP-сигналы из личных сообщений запрещены. Бот открывает сделки только из разрешённой группы/канала.",
        )

    if settings.VIP_ONLY_GROUP_SIGNALS and chat_type not in {"group", "supergroup"}:
        return (
            False,
            "VIP-сигнал пропущен: бот принимает сделки только из группы/супергруппы.",
        )

    if (
        settings.VIP_REQUIRE_TRUSTED_SOURCE
        and message.chat.id not in settings.allowed_source_chat_ids
    ):
        return (
            False,
            "VIP-сигнал пропущен: этот чат не входит в VIP_ALLOWED_SOURCE_CHAT_IDS.",
        )

    if settings.VIP_REQUIRE_SENDER_CHAT:
        ident = _sender_chat_identity(message)
        sender_id = ident.get("id")
        title = str(ident.get("title") or "").strip()
        username = str(ident.get("username") or "").strip().lstrip("@")
        if not (sender_id or title or username):
            return (
                False,
                "VIP-сигнал пропущен: сообщение отправлено обычным пользователем, а не от лица разрешённого канала.",
            )

        allowed = settings.allowed_sender_chat_titles
        if not allowed:
            return False, (
                "VIP-сигнал пропущен: включён VIP_REQUIRE_SENDER_CHAT, "
                "но VIP_ALLOWED_SENDER_CHAT_TITLES не задан. Укажи разрешённый канал, например: Торгаш."
            )

        candidates = {
            title.lower(),
            username.lower(),
            f"@{username.lower()}" if username else "",
        }
        candidates.discard("")
        # Also allow numeric channel id in the same env list for precision.
        if sender_id is not None:
            candidates.add(str(sender_id))
        if not any(a in candidates for a in allowed):
            shown = title or (f"@{username}" if username else str(sender_id))
            return (
                False,
                f"VIP-сигнал пропущен: источник '{shown}' не разрешён. Разрешено: {', '.join(allowed)}",
            )

    return True, ""


def _message_age_seconds(message: Message) -> float | None:
    msg_dt = getattr(message, "date", None)
    if not msg_dt:
        return None
    try:
        if msg_dt.tzinfo is None:
            msg_dt = msg_dt.replace(tzinfo=timezone.utc)
        return max(
            0.0,
            (
                datetime.now(timezone.utc) - msg_dt.astimezone(timezone.utc)
            ).total_seconds(),
        )
    except Exception:
        return None


def _is_stale_vip_signal_message(message: Message) -> bool:
    limit = int(get_settings().VIP_MAX_SIGNAL_AGE_SECONDS or 0)
    if limit <= 0:
        return False
    age = _message_age_seconds(message)
    return age is not None and age > limit


def _enabled_exchanges_text() -> str:
    return "BingX"


def _first_present_finite(
    payload: dict[str, object], keys: tuple[str, ...], default: float = 0.0
) -> float:
    """Return the first present numeric key while preserving an explicit zero."""
    for key in keys:
        if key not in payload:
            continue
        try:
            value = float(payload.get(key) or 0.0)
        except (TypeError, ValueError, OverflowError):
            return default
        return value if math.isfinite(value) else default
    return default


def _fmt_percent_value(value: float | int | None) -> str:
    try:
        number = float(value or 0.0)
    except Exception:
        number = 0.0
    text = f"{number:.1f}".rstrip("0").rstrip(".")
    return text or "0"


def _fmt_risk_usage_percent(value: float | int | None) -> str:
    """Render live aggregate risk without hiding small non-zero exposure."""
    try:
        number = max(0.0, float(value or 0.0))
    except Exception:
        number = 0.0
    text = f"{number:.2f}".rstrip("0").rstrip(".")
    return text or "0"


def _risk_usage_label(used: float, limit: float) -> str:
    used_text = _fmt_risk_usage_percent(used)
    if float(limit or 0.0) <= 0:
        return f"{used_text}% • без лимита"
    return f"{used_text}% из {_fmt_percent_value(limit)}%"


def _risk_slots_label(active: int, limit: int) -> str:
    if int(limit or 0) <= 0:
        return f"{max(0, int(active or 0))} • без лимита"
    return f"{max(0, int(active or 0))} из {int(limit)}"


def _mode_label(mode: UserMode) -> str:
    if mode == UserMode.AUTO:
        return "🟢 Авто"
    if mode == UserMode.OFF:
        return "⏸ Выключено"
    return "👁 Просмотр"


def _tp_mode_label(mode: TpMode) -> str:
    return {
        TpMode.SMART: "🧠 Умная",
        TpMode.BELL: "🔔 Колокол",
        TpMode.EARLY_FIXATION: "🛡️ Ранняя фиксация",
        TpMode.ACCELERATION: "🚀 Разгон",
        TpMode.EQUAL: "⚖️ Равными долями",
        TpMode.MANUAL: "✍️ Ручная",
    }.get(mode, "🧠 Умная")


def _status_text_from_settings(
    user_settings,
    *,
    whitelisted: bool | None = None,
    api_connected: bool | None = None,
    risk_state: dict | None = None,
) -> str:
    """Render the compact account status card.

    Keep this formatter synchronous and side-effect free so both the text
    command and the inline callback use exactly the same output. Optional
    live checks are rendered as ``not checked`` instead of crashing the menu.
    """
    mode = getattr(user_settings, "mode", UserMode.PREVIEW)
    if not isinstance(mode, UserMode):
        try:
            mode = UserMode(str(mode))
        except Exception:
            mode = UserMode.PREVIEW

    tp_mode = getattr(user_settings, "tp_mode", TpMode.BELL)
    if not isinstance(tp_mode, TpMode):
        try:
            tp_mode = TpMode(str(tp_mode))
        except Exception:
            tp_mode = TpMode.BELL

    tp_limit = str(getattr(user_settings, "tp_limit", "all") or "all").lower()
    tp_limit_label = "первые 3" if tp_limit == "3" else "все из сигнала"

    be_index = int(getattr(user_settings, "be_trigger_tp_index", 0) or 0)
    be_enabled = bool(getattr(user_settings, "be_after_tp1_enabled", be_index > 0))
    be_label = (
        f"после TP{be_index}" if be_enabled and be_index in {1, 2, 3} else "выключено"
    )

    if whitelisted is True:
        whitelist_label = "✅ разрешён"
    elif whitelisted is False:
        whitelist_label = "👁 только просмотр"
    else:
        whitelist_label = "⚪ не проверен"

    if api_connected is True:
        api_label = "🟢 подключён"
    elif api_connected is False:
        api_label = "🔴 не подключён"
    else:
        api_label = "⚪ не проверен"

    signal_percents = (
        "✅ использовать"
        if bool(getattr(user_settings, "use_signal_tp_percents", False))
        else "🚫 использовать схему"
    )
    be_slot_enabled = bool(getattr(user_settings, "exclude_be_trades_from_risk", False))
    be_slot = "✅ да" if be_slot_enabled else "❌ нет"

    live_risk = risk_state if isinstance(risk_state, dict) else None
    max_open_trades = int(getattr(user_settings, "max_open_trades", 0) or 0)
    max_portfolio_risk = float(
        getattr(user_settings, "max_portfolio_risk_percent", 0.0) or 0.0
    )
    daily_risk_limit = float(
        getattr(user_settings, "daily_risk_limit_percent", 0.0) or 0.0
    )
    if live_risk is None:
        portfolio_usage_label = "⚪ не рассчитан"
        daily_usage_label = "⚪ не рассчитан"
        slots_usage_label = "⚪ не рассчитаны"
        be_released_label = "⚪ не рассчитано"
    else:
        portfolio_usage_label = _risk_usage_label(
            float(live_risk.get("active_risk_percent") or 0.0),
            max_portfolio_risk,
        )
        daily_usage_label = _risk_usage_label(
            float(live_risk.get("daily_risk_percent") or 0.0),
            daily_risk_limit,
        )
        slots_usage_label = _risk_slots_label(
            int(live_risk.get("active_count") or 0), max_open_trades
        )
        be_count = int(live_risk.get("be_released_count") or 0)
        if be_slot_enabled:
            be_count_caption = "Освобождено после Б/У"
            be_released_label = f"{be_count}"
        else:
            be_count_caption = "Сделок в Б/У"
            be_released_label = f"{be_count} • риск учитывается"
    if live_risk is None:
        be_count_caption = (
            "Освобождено после Б/У" if be_slot_enabled else "Сделок в Б/У"
        )

    limit_ttl = int(getattr(user_settings, "limit_ttl_hours", 24) or 0)
    limit_ttl_label = "без ограничения" if limit_ttl <= 0 else f"{limit_ttl}ч"
    limit_tp_label = limit_tp_mode_label(
        str(getattr(user_settings, "limit_tp_invalidation_mode", "half") or "half")
    )

    return "\n".join(
        [
            "📊 <b>СТАТУС БОТА</b>",
            "━━━━━━━━━━━━━━━━━━",
            "",
            "🏦 BingX Futures",
            f"🤖 Режим → <b>{html.escape(str(_mode_label(mode)).replace('🟢 ', '').replace('🟡 ', '').replace('👁 ', '').replace('⏸ ', ''))}</b>",
            f"🔑 API → <b>{api_label.replace('🟢 ', '').replace('🔴 ', '').replace('⚪ ', '')}</b>",
            "",
            *premium_arrow_lines(
                (
                    ("🆔 Аккаунт", int(getattr(user_settings, "telegram_id", 0) or 0)),
                    ("🛡 Доступ", whitelist_label),
                    ("⚙️ Риск", f"{_fmt_percent_value(getattr(user_settings, 'risk_per_trade_percent', 0))}% на сделку"),
                    ("📊 Портфель", portfolio_usage_label),
                    ("📂 Риск-слоты", slots_usage_label),
                    ("⚖️ Б/У", be_label),
                    ("⏳ LIMIT", f"{limit_ttl_label}, {html.escape(limit_tp_label)}"),
                )
            ),
            "",
            "🎯 <b>Управление сделкой</b>",
            "━━━━━━━━━━━━━━━━━━",
            *premium_arrow_lines(
                (
                    ("🎯 Тейки", tp_limit_label),
                    ("🔔 Схема", _tp_mode_label(tp_mode)),
                    ("🚫 % из сигнала", signal_percents),
                    ("✅ Б/У освобождает слот", be_slot),
                    ("📊 " + be_count_caption, be_released_label),
                )
            ),
            "",
            "🛡 <b>Риск-контроль</b>",
            "━━━━━━━━━━━━━━━━━━",
            *premium_arrow_lines(
                (
                    ("📉 Дневной риск", daily_usage_label),
                    ("📌 Расчёт", "активные сделки и LIMIT в учёте бота"),
                )
            ),
            "",
            f"📦 Версия → <b>{html.escape(__version__)}</b>",
        ]
    )


async def _status_text(user_id: int) -> str:
    """Load status dependencies concurrently and never fail on optional checks."""
    is_admin = _is_admin(user_id)
    await db.ensure_user(user_id, None, is_admin)
    user_settings = await _cached_user_settings(user_id)

    api_task = _cached_api_key(user_id, "bingx")
    whitelist_task = (
        asyncio.sleep(0, result=True)
        if is_admin
        else db.is_user_whitelisted(user_id, "bingx")
    )
    risk_task = db.user_risk_state(user_id, settings_row=user_settings)
    api_result, whitelist_result, risk_result = await asyncio.gather(
        api_task,
        whitelist_task,
        risk_task,
        return_exceptions=True,
    )

    return _status_text_from_settings(
        user_settings,
        api_connected=None if isinstance(api_result, Exception) else bool(api_result),
        whitelisted=(
            None if isinstance(whitelist_result, Exception) else bool(whitelist_result)
        ),
        risk_state=None if isinstance(risk_result, Exception) else risk_result,
    )


async def _menu_text(user_id: int, username: str | None = None) -> str:
    """Build the informative user dashboard shown by ``Меню``.

    v1.5.12 keeps the first render accurate while avoiding serial PostgreSQL
    round-trips. Independent reads run concurrently and the expensive
    execution aggregates are cached for five seconds.
    """
    started = time.monotonic()
    is_admin = _is_admin(user_id)
    await db.ensure_user(user_id, username, is_admin)
    settings = get_settings()
    user_settings = await _cached_user_settings(user_id)

    async def load_dashboard_summary():
        rows = await db.user_dashboard_executions(user_id, days=30)
        return summarize_dashboard(rows)

    async def load_risk_state():
        return await db.user_risk_state(user_id, settings_row=user_settings)

    dashboard_cache = get_dashboard_cache()
    dashboard_task = dashboard_cache.get_or_fetch(
        (int(user_id), "dashboard", 30),
        load_dashboard_summary,
    )
    risk_task = dashboard_cache.get_or_fetch(
        (int(user_id), "risk"),
        load_risk_state,
    )
    api_task = _cached_api_key(user_id, "bingx")
    whitelist_task = (
        asyncio.sleep(0, result=True)
        if is_admin
        else db.is_user_whitelisted(user_id, "bingx")
    )
    api_result, whitelist_result, dashboard_result, risk_result = await asyncio.gather(
        api_task,
        whitelist_task,
        dashboard_task,
        risk_task,
        return_exceptions=True,
    )

    api_connected: bool | None = (
        None if isinstance(api_result, Exception) else bool(api_result)
    )
    whitelisted: bool | None = (
        True
        if is_admin
        else None if isinstance(whitelist_result, Exception) else bool(whitelist_result)
    )

    if isinstance(dashboard_result, Exception):
        log.warning(
            "home dashboard aggregation failed for uid=%s: %s",
            user_id,
            dashboard_result,
        )
        dashboard = None
    else:
        dashboard = dashboard_result if isinstance(dashboard_result, dict) else None

    if isinstance(risk_result, Exception):
        log.warning(
            "home risk aggregation failed for uid=%s: %s",
            user_id,
            risk_result,
        )
        risk_state = None
    else:
        risk_state = risk_result if isinstance(risk_result, dict) else None

    mode = user_settings.mode
    trade_label = _mode_label(mode)
    if mode == UserMode.AUTO and api_connected is False:
        trade_label = "🟡 Авто - нет API"
    elif mode == UserMode.AUTO and whitelisted is False:
        trade_label = "👁 Авто - нет White-list"

    access_label = (
        "✅ разрешён"
        if whitelisted is True
        else "👁 только просмотр" if whitelisted is False else "⚪ не проверен"
    )
    api_label = (
        "🟢 ключ сохранён"
        if api_connected is True
        else "🔴 не подключён" if api_connected is False else "⚪ не проверен"
    )
    tp_limit_label = (
        "первые 3" if str(user_settings.tp_limit) == "3" else "все из сигнала"
    )
    be_index = int(getattr(user_settings, "be_trigger_tp_index", 0) or 0)
    be_label = f"после TP{be_index}" if be_index in {1, 2, 3} else "выключено"
    if risk_state is None:
        slots_usage_label = "⚪ не рассчитаны"
    else:
        slots_usage_label = _risk_slots_label(
            int(risk_state.get("active_count") or 0),
            int(user_settings.max_open_trades or 0),
        )

    stats_lines: list[str]
    if dashboard is None:
        stats_lines = [
            "📈 <b>СТАТИСТИКА • 30 ДНЕЙ</b>",
            "⚪ Не рассчитана → <b>временно недоступен агрегат сделок</b>",
        ]
    else:
        known = int(dashboard.get("known_closed") or 0)
    if dashboard is not None and known:
        wr = dashboard.get("winrate")
        clean_wr = dashboard.get("clean_winrate")
        stats_lines = [
            "📈 <b>СТАТИСТИКА • 30 ДНЕЙ</b>",
            "<i>Б/У считается победой в основном Winrate</i>",
            *premium_arrow_lines(
                (
                    ("📊 Учтено сделок", known),
                    ("🟢 В плюс", int(dashboard.get('wins') or 0)),
                    ("🛡 В Б/У", int(dashboard.get('breakevens') or 0)),
                    ("🔴 В минус", int(dashboard.get('losses') or 0)),
                    ("🏆 Winrate с Б/У", f"{float(wr):.1f}%"),
                    ("💰 Чистый WR", f"{float(clean_wr):.1f}%"),
                )
            ),
        ]
    elif dashboard is not None:
        stats_lines = [
            "📈 <b>СТАТИСТИКА • 30 ДНЕЙ</b>",
            "<i>Б/У считается победой в основном Winrate</i>",
            "📊 Сделки → <b>пока нет закрытых сделок с определённым результатом</b>",
        ]
    unknown = int(dashboard.get("unknown") or 0) if dashboard is not None else 0
    if dashboard is not None and unknown:
        stats_lines.append(f"⚪ Не определено: <b>{unknown}</b> • не входит в WR")

    if dashboard is None:
        active_positions_label = "⚪"
        protected_positions_label = "⚪"
        pending_limits_label = "⚪"
        protection_state = "⚪ не рассчитано"
    else:
        active_positions = int(dashboard.get("active_positions") or 0)
        protected_positions = int(dashboard.get("protected_positions") or 0)
        unprotected = int(dashboard.get("unprotected_positions") or 0)
        manual_review = int(dashboard.get("manual_review") or 0)
        pending_limits = int(dashboard.get("pending_limits") or 0)
        active_positions_label = str(active_positions)
        protected_positions_label = str(protected_positions)
        pending_limits_label = str(pending_limits)
    if dashboard is not None and manual_review:
        protection_state = f"🔴 ручная проверка: {manual_review}"
    elif dashboard is not None and unprotected:
        protection_state = f"🔴 без подтверждённого STOP: {unprotected}"
    elif dashboard is not None and active_positions:
        protection_state = "🟢 всё защищено по данным бота"
    elif dashboard is not None:
        protection_state = "🔵 активных позиций нет"

    activity_label = f"Сделки: {active_positions_label}  ·  Лимитки: {pending_limits_label}"
    monitor_label = (
        f"Рыночные события: каждые {settings.MARKET_PRICE_POLL_INTERVAL_SEC:g} сек"
        if settings.EVENT_DRIVEN_MONITOR_ENABLED
        else f"Проверка позиций: каждые {settings.MONITOR_ACTIVE_INTERVAL_SEC} сек"
    )
    critical_label = (
        f"Критическая сверка: {settings.MONITOR_CRITICAL_INTERVAL_SEC} сек · полная: {settings.MONITOR_FULL_RECONCILE_INTERVAL_SEC} сек"
        if settings.EVENT_DRIVEN_MONITOR_ENABLED
        else ""
    )

    lines = [
        "🤖 <b>BingXProfitBot</b>",
        "━━━━━━━━━━━━━━━━━━",
        "",
        "🏦 BingX Futures",
        f"🤖 Режим → <b>{trade_label.replace('🟢 ', '').replace('🟡 ', '').replace('👁 ', '').replace('⏸ ', '')}</b>",
        "🛡 Защита сделок → <b>включена</b>",
        "",
        *premium_arrow_lines(
            (
                ("🔑 API", api_label),
                ("🛡 Доступ", access_label),
                ("⚙️ Риск", f"{_fmt_percent_value(user_settings.risk_per_trade_percent)}% на сделку"),
                ("📊 Риск-слоты", slots_usage_label),
                ("🎯 Тейки", f"{tp_limit_label} · {_tp_mode_label(user_settings.tp_mode)}"),
                ("⚖️ Б/У", be_label),
                ("📂 Активность", activity_label),
            )
        ),
        "",
        "📈 <b>Статистика · 30 дней</b>",
        "━━━━━━━━━━━━━━━━━━",
        *stats_lines[1:],
        *premium_arrow_lines((("📦 Версия", html.escape(__version__)),)),
        "",
        "🔒 <b>Защита · по данным бота</b>",
        "━━━━━━━━━━━━━━━━━━",
        *premium_arrow_lines(
            (
                ("📂 Открытые позиции", active_positions_label),
                ("🛡 Со STOP", protected_positions_label),
                ("⏳ LIMIT ожидают входа", pending_limits_label),
                ("📌 Состояние", protection_state),
            )
        ),
        "",
        *premium_arrow_lines(
            (
                ("⚡ Монитор", html.escape(monitor_label)),
                *(([("🛡 Сверка", html.escape(critical_label))] if critical_label else [])),
            )
        ),
        "",
        "Выберите действие 👇",
    ]
    elapsed_ms = (time.monotonic() - started) * 1000.0
    if elapsed_ms >= 1500.0:
        log.warning("slow home menu uid=%s duration_ms=%.0f", user_id, elapsed_ms)
    else:
        log.debug("home menu uid=%s duration_ms=%.0f", user_id, elapsed_ms)
    return "\n".join(lines)


_MENU_RENDER_TIMEOUT_SEC = 5.0


def _menu_fallback_text() -> str:
    return (
        "🤖 <b>BingXProfitBot</b>\n"
        "━━━━━━━━━━━━━━━━━━\n\n"
        "⚠️ Подробная статистика временно загружается дольше обычного.\n"
        "Основные кнопки доступны, торговый монитор продолжает работу.\n\n"
        f"📦 Версия → <b>{html.escape(__version__)}</b>\n\n"
        "Повторите «Меню» через несколько секунд для полного отчёта."
    )


async def _menu_text_bounded(user_id: int, username: str | None = None) -> str:
    """Never let PostgreSQL contention silence the Telegram Menu command."""

    try:
        return await asyncio.wait_for(
            _menu_text(user_id, username),
            timeout=_MENU_RENDER_TIMEOUT_SEC,
        )
    except asyncio.TimeoutError:
        log.error(
            "MENU_RENDER_TIMEOUT uid=%s timeout_sec=%.1f; fast fallback sent",
            user_id,
            _MENU_RENDER_TIMEOUT_SEC,
        )
        return _menu_fallback_text()
    except Exception:
        log.exception("MENU_RENDER_FAILED uid=%s; fast fallback sent", user_id)
        return _menu_fallback_text()


def _algo_orders_for_position(
    algo_orders: list[dict], position: dict
) -> list[dict]:
    """Return visible TP/SL rows for a live BingX position.

    Prefer exact ``positionId`` when BingX provides it, but do not hide real
    protective orders simply because BingX omitted ``positionId`` from the
    openOrders row.  In live v1.6.57 this made the menu show ``STOP: not found``
    and ``TP: 0`` even when STOP/TP orders existed on the exchange.

    Fallback is read-only and symbol+side scoped.  It never cancels or modifies
    orders; it only makes the UI and protection diagnostics reflect BingX rows
    that are already open.
    """
    pid = clean_exchange_id(position.get("positionId"))
    symbol = str(position.get("symbol") or "").upper()
    side = str(position.get("side") or "").lower()
    exact: list[dict] = []
    fallback: list[dict] = []
    for order in algo_orders or []:
        if not isinstance(order, dict):
            continue
        if str(order.get("symbol") or "").upper() != symbol:
            continue
        order_type = str(order.get("type") or "").upper()
        looks_protective = (
            "STOP" in order_type
            or "TAKE_PROFIT" in order_type
            or order_type.startswith("TRIGGER")
            or _first_present_finite(order, ("stopLossPrice", "takeProfitPrice", "triggerPrice", "stopPrice")) > 0
        )
        if not looks_protective:
            continue
        order_pid = clean_exchange_id(order.get("positionId"))
        position_side = str(order.get("positionSide") or "").lower()
        if position_side and position_side != side:
            continue
        if pid and order_pid:
            if order_pid == pid:
                exact.append(order)
            continue
        # BingX often omits positionId on open TP/SL rows.  If side and symbol
        # match, showing this protection is safer than a false red "not found".
        if position_side == side or not position_side:
            fallback.append(order)
    return exact or fallback


async def _positions_view(
    user_id: int, username: str | None = None
) -> tuple[str, list[dict]]:
    """Read actual BingX positions and return both text and selectable rows."""
    await db.ensure_user(user_id, username, _is_admin(user_id))
    api_row = await _cached_api_key(user_id, "bingx")
    if not api_row:
        return (
            "📂 <b>ОТКРЫТЫЕ СДЕЛКИ</b>\n"
            "━━━━━━━━━━━━━━━━━━\n\n"
            "🏦 BingX Futures\n\n"
            "🔴 BingX API не подключён.\n"
            "Подключите ключ через раздел «🏦 BingX API».",
            [],
        )

    adapter = None
    algo_error: Exception | None = None
    try:
        adapter = build_adapter(api_row)
        positions_result, algo_result = await asyncio.gather(
            adapter.fetch_open_positions(),
            adapter.fetch_open_algo_orders(),
            return_exceptions=True,
        )
        if isinstance(positions_result, Exception):
            raise positions_result
        positions = list(positions_result or [])
        if isinstance(algo_result, Exception):
            algo_orders = []
            algo_error = algo_result
        else:
            algo_orders = list(algo_result or [])
    except Exception as exc:
        return (
            "📂 <b>ОТКРЫТЫЕ СДЕЛКИ</b>\n"
            "━━━━━━━━━━━━━━━━━━\n\n"
            "🏦 BingX Futures\n\n"
            "🔴 Не удалось получить позиции BingX.\n"
            f"⚠️ Причина: {html.escape(str(exc)[:500])}",
            [],
        )
    finally:
        if adapter is not None:
            try:
                await adapter.close()
            except Exception:
                pass

    if not positions:
        return (
            "📂 <b>ОТКРЫТЫЕ СДЕЛКИ</b>\n"
            "━━━━━━━━━━━━━━━━━━\n\n"
            "🏦 BingX Futures\n\n"
            "🔵 Открытых позиций нет.\n"
            "🛡 Проверять защиту сейчас не требуется.",
            [],
        )

    lines = [
        "📂 <b>ОТКРЫТЫЕ СДЕЛКИ</b>",
        "━━━━━━━━━━━━━━━━━━",
        "",
        "🏦 BingX Futures",
        f"📊 Всего активных: <b>{len(positions)}</b>",
        "",
        "Выберите позицию для управления 👇",
    ]
    selectable: list[dict] = []
    for index, pos in enumerate(positions[:15], 1):
        position_id = clean_exchange_id(pos.get("positionId"))
        if not position_id:
            continue
        symbol = str(pos.get("symbol") or "?").upper()
        side = str(pos.get("side") or "").lower()
        side_label = (
            "📈 LONG"
            if side == "long"
            else "📉 SHORT" if side == "short" else "⚠️ UNKNOWN"
        )
        matching = _algo_orders_for_position(algo_orders, pos)
        stop_orders = [
            order
            for order in matching
            if "STOP" in str(order.get("type") or "").upper()
            or _first_present_finite(order, ("stopLossPrice",)) > 0
        ]
        tp_orders = [
            order
            for order in matching
            if "TAKE_PROFIT" in str(order.get("type") or "").upper()
            or _first_present_finite(order, ("takeProfitPrice",)) > 0
        ]
        stop_price = 0.0
        if stop_orders:
            stop_price = _first_present_finite(
                stop_orders[0],
                ("stopLossPrice", "triggerPrice", "stopPrice"),
            )
        row = dict(pos)
        row["active_tp_count"] = len(tp_orders)
        row["stop_price"] = stop_price
        row["protection_unknown"] = bool(algo_error)
        selectable.append(row)
        lines.extend(
            [
                "",
                f"<b>{index}. {html.escape(symbol)} · {side_label}</b>",
                *premium_arrow_lines(
                    (
                        ("💵 Вход", fmt_price(_first_present_finite(pos, ("entryPrice",)))),
                        ("📦 Объём", fmt_qty(_first_present_finite(pos, ("size",)))),
                        ("⚙️ Плечо", f"{int(max(0.0, _first_present_finite(pos, ('leverage',))))}x"),
                        ("🛡 STOP", ('⚠️ не удалось проверить' if algo_error else ('✅ ' + fmt_price(stop_price) if stop_price > 0 else '❌ не найден'))),
                        ("🎯 Активных TP", '⚠️ неизвестно' if algo_error else len(tp_orders)),
                    )
                ),
            ]
        )
    if len(positions) > 15:
        lines.extend(["", f"➕ Ещё позиций: <b>{len(positions)-15}</b>"])
    if algo_error:
        lines.extend(
            [
                "",
                "🟡 Позиции прочитаны, но BingX не отдала список TP/SL.",
                f"❌ {html.escape(str(algo_error)[:300])}",
            ]
        )
    lines.extend(["", "🏦 Данные получены напрямую с BingX."])
    return "\n".join(lines), selectable


async def _positions_text(user_id: int, username: str | None = None) -> str:
    text, _rows = await _positions_view(user_id, username)
    return text


async def _position_detail_view(
    user_id: int,
    position_id: str,
    username: str | None = None,
) -> tuple[str, dict | None, bool]:
    """Return a fresh exact position card and whether forced BE is available."""
    await db.ensure_user(user_id, username, _is_admin(user_id))
    api_row = await _cached_api_key(user_id, "bingx")
    if not api_row:
        return (
            "🔴 <b>BingX API НЕ ПОДКЛЮЧЁН</b>\n"
            "━━━━━━━━━━━━━━━━━━━━\n"
            "Подключите API через главное меню.",
            None,
            False,
        )
    wanted_position_id = clean_exchange_id(position_id)
    if not wanted_position_id:
        return (
            "🔴 <b>НЕКОРРЕКТНЫЙ ID ПОЗИЦИИ</b>\n"
            "━━━━━━━━━━━━━━━━━━━━\n"
            "Обновите список открытых позиций.",
            None,
            False,
        )
    adapter = None
    try:
        adapter = build_adapter(api_row)
        positions_result, algo_result = await asyncio.gather(
            adapter.fetch_open_positions(),
            adapter.fetch_open_algo_orders(),
            return_exceptions=True,
        )
        if isinstance(positions_result, Exception):
            raise positions_result
        position = next(
            (
                row
                for row in list(positions_result or [])
                if clean_exchange_id(row.get("positionId")) == wanted_position_id
            ),
            None,
        )
        if position is None:
            return (
                "ℹ️ <b>ПОЗИЦИЯ УЖЕ ЗАКРЫТА ИЛИ НЕ НАЙДЕНА</b>\n"
                "━━━━━━━━━━━━━━━━━━━━\n"
                "Обновите список открытых позиций.",
                None,
                False,
            )
        algo_error = algo_result if isinstance(algo_result, Exception) else None
        algo_orders = [] if algo_error else list(algo_result or [])
    except Exception as exc:
        return (
            "🔴 <b>НЕ УДАЛОСЬ ОБНОВИТЬ ПОЗИЦИЮ</b>\n"
            "━━━━━━━━━━━━━━━━━━━━\n"
            f"❌ {html.escape(str(exc)[:500])}",
            None,
            False,
        )
    finally:
        if adapter is not None:
            try:
                await adapter.close()
            except Exception:
                pass

    symbol = str(position.get("symbol") or "?").upper()
    side = str(position.get("side") or "").lower()
    side_label = (
        "📈 LONG" if side == "long" else "📉 SHORT" if side == "short" else "⚠️ UNKNOWN"
    )
    matching = _algo_orders_for_position(algo_orders, position)
    stop_orders = [
        order
        for order in matching
        if "STOP" in str(order.get("type") or "").upper()
        or _first_present_finite(order, ("stopLossPrice",)) > 0
    ]
    tp_orders = [
        order
        for order in matching
        if "TAKE_PROFIT" in str(order.get("type") or "").upper()
        or _first_present_finite(order, ("takeProfitPrice",)) > 0
    ]
    stop_price = 0.0
    if stop_orders:
        stop_price = _first_present_finite(
            stop_orders[0],
            ("stopLossPrice", "triggerPrice", "stopPrice"),
        )

    managed_row, ambiguous = await managed_execution_for_position(user_id, position)
    be_moved = False
    be_stop = 0.0
    if managed_row is not None:
        try:
            payload = json.loads(managed_row.get("exchange_order_ids_json") or "{}")
        except Exception:
            payload = {}
        be_state = payload.get("be") if isinstance(payload.get("be"), dict) else {}
        be_moved = bool(be_state.get("moved"))
        be_stop = _first_present_finite(be_state, ("stop",))
    managed_status = str((managed_row or {}).get("status") or "")
    allow_force_be = bool(
        managed_row is not None
        and not ambiguous
        and managed_status in {"opened", "protected", "partial_error", "manual_required"}
        and not be_moved
    )
    management_label = (
        "🔒 уже в Б/У"
        if be_moved
        else (
            "✅ управляется ботом"
            if managed_row is not None and not ambiguous
            else (
                "⚠️ связь с исполнением неоднозначна"
                if ambiguous
                else "ℹ️ ручная/внешняя позиция"
            )
        )
    )

    metric_rows = [
        ("💵 Вход", fmt_price(_first_present_finite(position, ("entryPrice",)))),
        ("📦 Объём", fmt_qty(_first_present_finite(position, ("size",)))),
        ("⚙️ Плечо", f"{int(max(0.0, _first_present_finite(position, ('leverage',))))}x"),
        ("🛡 Stop-Loss", ('⚠️ не удалось проверить' if algo_error else ('✅ ' + fmt_price(stop_price) if stop_price > 0 else '❌ не найден'))),
        ("🎯 Активных TP", '⚠️ неизвестно' if algo_error else len(tp_orders)),
        ("🤖 Управление", management_label),
    ]
    if be_moved and be_stop > 0:
        metric_rows.append(("⚖️ Уровень Б/У", fmt_price(be_stop)))
    lines = [
        "📂 <b>СДЕЛКА</b>",
        "━━━━━━━━━━━━━━━━━━",
        "",
        f"🪙 <b>{html.escape(symbol)}</b>  ·  {side_label}",
        "🏦 BingX Futures",
        "",
        *premium_arrow_lines(metric_rows),
        "",
        "🔴 Закрытие выполняется MARKET-ордером на весь актуальный объём.",
    ]
    return "\n".join(lines), position, allow_force_be


def _position_close_result_text(result: dict) -> str:
    state = str(result.get("state") or "error")
    symbol = html.escape(str(result.get("symbol") or "ПОЗИЦИЯ"))
    if state == "closed":
        cleanup_verified = bool(result.get("cleanup_verified"))
        execution_id = int(result.get("execution_id") or 0)
        lines = [
            "✅ <b>ПОЗИЦИЯ ЗАКРЫТА ПОЛНОСТЬЮ</b>",
            "━━━━━━━━━━━━━━━━━━━━",
            f"🪙 <b>{symbol}</b> • {html.escape(str(result.get('side')or '').upper())}",
            f"📦 Закрытый объём: <b>{fmt_qty(float(result.get('qty')or 0.0))}</b>",
        ]
        if (
            bool(result.get("exit_price_confirmed"))
            and float(result.get("exit_price") or 0.0) > 0
        ):
            lines.append(
                f"💵 Цена исполнения: <b>{fmt_price(float(result.get('exit_price')or 0.0))}</b>"
            )
        else:
            lines.append("💵 Цена исполнения: <b>не подтверждена BingX</b>")
        lines.extend(
            [
                "",
                "✅ MARKET-закрытие подтверждено BingX",
                (
                    "✅ Связанные STOP и TP удалены точно по сохранённым ID"
                    if cleanup_verified
                    else "⚠️ Точная очистка связанных STOP/TP не подтверждена полностью"
                ),
            ]
        )
        if execution_id <= 0:
            lines.extend(
                [
                    "",
                    "ℹ️ Позиция не была связана с исполнением бота.",
                    "Чужие и неопознанные ордера бот не удалял.",
                ]
            )
        elif not bool(result.get("db_saved", True)):
            lines.extend(
                [
                    "",
                    "⚠️ Позиция закрыта на бирже, но запись БД изменилась параллельно.",
                    "Проверьте раздел «Позиции» и conditional orders.",
                ]
            )
        return "\n".join(lines)
    if state == "already_closed":
        return (
            "ℹ️ <b>ПОЗИЦИЯ УЖЕ ЗАКРЫТА</b>\n"
            "━━━━━━━━━━━━━━━━━━━━\n"
            "Повторный MARKET-ордер не отправлялся."
        )
    if state == "pending_entry_unconfirmed":
        return (
            "⚠️ <b>ПОЗИЦИЯ НЕ ЗАКРЫТА</b>\n"
            "━━━━━━━━━━━━━━━━━━━━\n"
            f"🪙 <b>{symbol}</b>\n\n"
            "У этой позиции остался LIMIT-вход. Бот не смог точно подтвердить "
            "отмену его остатка, поэтому MARKET-закрытие не отправлялось.\n\n"
            f"Причина: <code>{html.escape(str(result.get('reason')or state)[:500])}</code>\n"
            "Проверьте LIMIT и позицию на BingX."
        )
    if state == "pending_entry_ambiguous":
        return (
            "🔒 <b>ЗАКРЫТИЕ ЗАБЛОКИРОВАНО</b>\n"
            "━━━━━━━━━━━━━━━━━━━━\n"
            f"🪙 <b>{symbol}</b>\n\n"
            "Рядом с позицией есть активный LIMIT-вход, но бот не смог "
            "доказать, относится ли он к этой позиции. MARKET-закрытие не "
            "отправлялось, чтобы остаток LIMIT не открыл позицию повторно.\n\n"
            f"Причина: <code>{html.escape(str(result.get('reason')or state)[:500])}</code>\n"
            "Сначала проверьте или отмените LIMIT в разделе «Лимитки»."
        )
    if state == "unconfirmed":
        return (
            "⚠️ <b>ЗАКРЫТИЕ НЕ ПОДТВЕРЖДЕНО</b>\n"
            "━━━━━━━━━━━━━━━━━━━━\n"
            f"🪙 <b>{symbol}</b>\n"
            f"📦 Остаток: <b>{fmt_qty(float(result.get('remaining_qty')or 0.0))}</b>\n\n"
            "Бот не будет повторять MARKET-закрытие вслепую. Проверьте BingX."
        )
    if state in {"ambiguous_execution", "stale_identity"}:
        return (
            "🔒 <b>ЗАКРЫТИЕ ЗАБЛОКИРОВАНО</b>\n"
            "━━━━━━━━━━━━━━━━━━━━\n"
            "Бот не смог однозначно связать позицию с одной сделкой.\n"
            "Биржевые действия не выполнялись."
        )
    if state == "api_missing":
        return "🔴 <b>BingX API НЕ ПОДКЛЮЧЁН</b>"
    return (
        "🔴 <b>НЕ УДАЛОСЬ ЗАКРЫТЬ ПОЗИЦИЮ</b>\n"
        "━━━━━━━━━━━━━━━━━━━━\n"
        f"Причина: <code>{html.escape(str(result.get('reason')or state)[:500])}</code>\n\n"
        "Проверьте позицию на BingX перед повторной попыткой."
    )


def _position_be_result_text(result: dict) -> str:
    state = str(result.get("state") or "error")
    symbol = html.escape(str(result.get("symbol") or "ПОЗИЦИЯ"))
    if state == "moved":
        return (
            "🔒 <b>ПОЗИЦИЯ ПЕРЕВЕДЕНА В Б/У</b>\n"
            "━━━━━━━━━━━━━━━━━━━━\n"
            f"🪙 <b>{symbol}</b> • {html.escape(str(result.get('side')or '').upper())}\n"
            f"🔒 Новый STOP: <b>{fmt_price(float(result.get('stop')or 0.0))}</b>\n"
            f"📦 Остаток позиции: <b>{fmt_qty(float(result.get('qty')or 0.0))}</b>\n\n"
            "✅ Новый STOP подтверждён\n"
            "✅ Автоматический Б/У позже не создаст дубль"
        )
    if state == "already_be":
        return (
            "ℹ️ <b>ПОЗИЦИЯ УЖЕ В Б/У</b>\n"
            "━━━━━━━━━━━━━━━━━━━━\n"
            f"🔒 Текущий уровень: <b>{fmt_price(float(result.get('stop')or 0.0))}</b>\n\n"
            "Повторный STOP не создавался."
        )
    if state == "market_not_safe":
        return (
            "⚠️ <b>Б/У ПОКА НЕ УСТАНОВЛЕН</b>\n"
            "━━━━━━━━━━━━━━━━━━━━\n"
            f"🪙 <b>{symbol}</b>\n"
            f"📊 Последняя цена: <b>{fmt_price(float(result.get('market_price')or 0.0))}</b>\n"
            f"⚖️ Fair price: <b>{fmt_price(float(result.get('fair_price')or 0.0))}</b>\n"
            f"🔒 Расчётный Б/У: <b>{fmt_price(float(result.get('stop')or 0.0))}</b>\n\n"
            "Последняя или fair price ещё не ушла достаточно далеко. "
            "Такой STOP мог бы сработать сразу."
        )
    if state == "qty_coverage_waiting":
        return (
            "⏳ <b>Б/У ОТЛОЖЕН</b>\n"
            "━━━━━━━━━━━━━━━━━━━━\n"
            f"🪙 <b>{symbol}</b>\n"
            f"🔒 Расчётный Б/У: <b>{fmt_price(float(result.get('stop')or 0.0))}</b>\n"
            f"📦 STOP qty: <b>{fmt_qty(float(result.get('qty')or 0.0))}</b>\n"
            f"📦 Остаток позиции: <b>{fmt_qty(float(result.get('position_qty')or 0.0))}</b>\n"
            f"⚠️ Без покрытия: <b>{fmt_qty(float(result.get('uncovered_qty')or 0.0))}</b>\n\n"
            "Старый STOP оставлен активным. Бот повторит перенос позже."
        )
    if state == "api_missing":
        return (
            "🔴 <b>BingX API НЕ ПОДКЛЮЧЁН</b>\n"
            "━━━━━━━━━━━━━━━━━━━━\n"
            "Принудительный Б/У не выполнялся."
        )
    if state == "unmanaged":
        return (
            "🔒 <b>ПРИНУДИТЕЛЬНЫЙ Б/У НЕДОСТУПЕН</b>\n"
            "━━━━━━━━━━━━━━━━━━━━\n"
            "Позиция не связана с точным исполнением бота.\n"
            "Бот не будет изменять неопознанные STOP и TP."
        )
    if state in {
        "snapshot_missing",
        "tp_ledger_conflict",
        "position_id_missing",
        "verification_failed",
        "error",
    }:
        return (
            "🔴 <b>Б/У НЕ УСТАНОВЛЕН</b>\n"
            "━━━━━━━━━━━━━━━━━━━━\n"
            f"🪙 <b>{symbol}</b>\n\n"
            f"Причина: <code>{html.escape(str(result.get('reason')or state)[:500])}</code>\n\n"
            "Проверьте STOP и TP на BingX вручную."
        )
    if state in {"already_closed", "stale", "not_eligible"}:
        return (
            "ℹ️ <b>СОСТОЯНИЕ ПОЗИЦИИ УЖЕ ИЗМЕНИЛОСЬ</b>\n"
            "━━━━━━━━━━━━━━━━━━━━\n"
            "Обновите раздел «Позиции». Биржевые действия повторно не выполнялись."
        )
    return (
        "⚠️ <b>Б/У ПОКА НЕ ПОДТВЕРЖДЁН</b>\n"
        "━━━━━━━━━━━━━━━━━━━━\n"
        f"Причина: <code>{html.escape(str(result.get('reason')or state)[:500])}</code>\n\n"
        "Обновите позицию и проверьте BingX."
    )


async def _subscribers_text(user_id: int) -> str:
    """Render a fresh, read-only subscriber summary for configured admins."""
    if not _is_admin(user_id):
        return "⛔ Раздел «Подписчики» доступен только администратору."

    try:
        rows = await db.list_users_with_exchanges()
    except Exception as exc:
        log.exception("failed to load subscriber summary")
        return (
            "🔴 <b>ПОДПИСЧИКИ ВРЕМЕННО НЕДОСТУПНЫ</b>\n"
            "━━━━━━━━━━━━━━━━━━━━\n"
            f"Причина: <code>{html.escape(type(exc).__name__)}</code>\n\n"
            "Торговая логика не изменялась. Попробуйте обновить раздел позже."
        )
    stats = summarize_subscribers(rows)
    state_rows = [
        ("🔑 BingX API подключён", stats["api_connected"]),
        ("🛡 White-list BingX", stats["whitelisted"]),
        ("💰 Реальная торговля AUTO", stats["mode_auto"]),
        ("👁 Просмотр без сделок", stats["mode_preview"]),
        ("⏸ Торговля выключена", stats["mode_off"]),
    ]
    if stats["mode_unknown"]:
        state_rows.append(("⚠️ Неизвестный режим", stats["mode_unknown"]))
    return "\n".join(
        [
            "👥 <b>ПОДПИСЧИКИ</b>",
            "━━━━━━━━━━━━━━━━━━━━",
            "",
            *premium_arrow_lines(
                (
                    ("📦 Всего аккаунтов в базе", stats["total_accounts"]),
                    ("👤 Подписчиков", stats["subscribers"]),
                    ("👑 Администраторов", stats["admins"]),
                )
            ),
            "",
            "📡 <b>СОСТОЯНИЕ ПОДПИСЧИКОВ</b>",
            "━━━━━━━━━━━━━━━━━━━━",
            *premium_arrow_lines(state_rows),
            "",
            *premium_arrow_lines(
                (
                    ("📨 Получат общий сигнал", stats["routed_for_execution"]),
                    ("✅ Готовы к попытке открытия", stats["ready_for_attempt"]),
                    ("⚠️ AUTO без API", stats["auto_without_api"]),
                    ("⚠️ AUTO без White-list", stats["auto_without_whitelist"]),
                )
            ),
            "",
            "<i>«Готовы» означает: AUTO + White-list + активный BingX API. "
            "Перед каждой сделкой бот отдельно проверяет риск, баланс, активные "
            "позиции, дубликаты и состояние BingX, поэтому фактически открытых "
            "сделок может быть меньше.</i>",
        ]
    )


def _fmt_diag_ms(value: int | float | None) -> str:
    try:
        ms = max(0, int(float(value or 0)))
    except Exception:
        ms = 0
    if ms <= 0:
        return "н/д"
    if ms >= 1000:
        return f"{ms / 1000:.1f} сек"
    return f"{ms} мс"


async def _diagnostics_text(user_id: int) -> str:
    if not _is_admin(user_id):
        return "⛔ Диагностика доступна только администратору."
    settings = get_settings()
    api_connected = bool(await _cached_api_key(user_id, "bingx"))
    try:
        rows = await db.user_dashboard_executions(user_id, days=30)
        dashboard = summarize_dashboard(rows)
    except Exception:
        dashboard = {"active_positions": 0, "manual_review": 0}
    expected_workers = (
        4
        if settings.EVENT_DRIVEN_MONITOR_ENABLED
        else (2 if int(settings.MONITOR_WORKERS or 1) >= 2 else 1)
    )
    dispatcher_stats = get_trade_dispatcher().stats()
    try:
        workload_stats = await bingx_workload_stats()
    except Exception:
        workload_stats = {"active": 0, "queued": 0, "peak_active": 0, "limit": 0}
    execution_metrics = execution_metrics_snapshot()
    bg_tp_stats = background_tp_stats()
    entry_to_stop = execution_metrics.get("entry_to_stop_ms") or {}
    full_trade = execution_metrics.get("full_trade_ms") or {}
    workload_wait = execution_metrics.get("workload_wait_ms") or {}
    trade_order_wait = execution_metrics.get("workload_trade_order_wait_ms") or {}
    profile20_label = (
        "🟢 включён"
        if int(settings.TRADE_EXECUTION_WORKERS or 0) >= 20
        else f"🟡 workers {int(settings.TRADE_EXECUTION_WORKERS or 0)}/20"
    )
    diag_rows = [
        ("📦 Версия", html.escape(__version__)),
        ("🎯 Профиль 20 юзеров", profile20_label),
        ("🧾 Trade-order bucket", f"{settings.BINGX_TRADE_ORDER_REQUESTS_PER_SECOND:g}/сек, burst {settings.BINGX_TRADE_ORDER_BURST_LIMIT}"),
        ("👤 Per-user bucket", f"{settings.BINGX_PER_USER_REQUESTS_PER_SECOND:g}/сек, burst {settings.BINGX_PER_USER_BURST_LIMIT}"),
        ("🗄 База", db.storage_backend().upper()),
        ("🔑 BingX API", "🟢 сохранён" if api_connected else "🔴 отсутствует"),
        ("⚙️ Фоновые службы", f"{expected_workers} настроено"),
        ("📂 Позиции по данным бота", int(dashboard.get("active_positions") or 0)),
        ("🚨 Требуют проверки", int(dashboard.get("manual_review") or 0)),
        ("🧵 Trade workers", f"{dispatcher_stats.get('active', 0)}/{dispatcher_stats.get('workers', 0)} активны, очередь {dispatcher_stats.get('queued', 0)}"),
        ("🎯 TP фон", f"active {bg_tp_stats.get('active', 0)}, queue {bg_tp_stats.get('tracked', 0)}, recovered {bg_tp_stats.get('recovered', 0)}, done {bg_tp_stats.get('completed', 0)}"),
        ("📈 Пик workers", dispatcher_stats.get("peak_active", 0)),
        ("🚦 BingX governor", f"{workload_stats.get('active', 0)}/{workload_stats.get('limit', 0)} active, очередь {workload_stats.get('queued', 0)}"),
        ("🛡 p95 entry→STOP", _fmt_diag_ms(entry_to_stop.get("p95_ms"))),
        ("🎯 p95 полный цикл", _fmt_diag_ms(full_trade.get("p95_ms"))),
        ("⏱ p95 BingX wait", _fmt_diag_ms(workload_wait.get("p95_ms"))),
        ("🧾 p95 trade-order wait", _fmt_diag_ms(trade_order_wait.get("p95_ms"))),
    ]
    if settings.EVENT_DRIVEN_MONITOR_ENABLED:
        diag_rows.extend(
            [
                ("⚡ Общая цена BingX", f"{settings.MARKET_PRICE_POLL_INTERVAL_SEC:g} сек"),
                ("🛡 Критическая сверка", f"{settings.MONITOR_CRITICAL_INTERVAL_SEC} сек"),
                ("🔄 Полная сверка", f"{settings.MONITOR_FULL_RECONCILE_INTERVAL_SEC} сек"),
                ("📡 Публичная цена", "🟡 резервная сверка" if PRICE_STREAM_DEGRADED.is_set() else "🟢 работает"),
            ]
        )
    else:
        diag_rows.append(("⚙️ Интервал контроля", f"{settings.MONITOR_ACTIVE_INTERVAL_SEC} сек"))
    return "\n".join(
        [
            "🧰 <b>ДИАГНОСТИКА БОТА</b>",
            "━━━━━━━━━━━━━━━━━━━━",
            "",
            *premium_arrow_lines(diag_rows),
            "",
            "🧵 Фоновые службы - это общий монитор цены, проверка событий, "
            "критическая и полная сверка аккаунтов. Trade workers и p95-метрики "
            "показывают реальное узкое место перед оптимизацией на 20 пользователей. "
            "v1.0.6a/6b освобождают trade worker после подтверждённого STOP: "
            "MARKET TP ставятся короткой фоновой задачей, v1.0.6b добавляет "
            "durable recovery после redeploy, а BingX write-запросы по-прежнему "
            "проходят через priority governor.",
        ]
    )

_VISIBLE_LIMIT_PRESETS = ("fast", "tp2", "balanced", "long")


def _skip_notifications_section_text(enabled: bool) -> str:
    status = "🔔 <b>Включены</b>" if enabled else "🔕 <b>Выключены</b>"
    return "\n".join(
        [
            "🔔 <b>УВЕДОМЛЕНИЯ О ПРОПУСКАХ</b>",
            "━━━━━━━━━━━━━━━━━━",
            "",
            f"Текущий статус: {status}",
            "",
            "Эта настройка управляет только обычной карточкой",
            "<b>«⏭ СИГНАЛ ПРОПУЩЕН»</b>.",
            "",
            "Ошибки открытия, недоступные пары, ценовые аномалии,",
            "защита позиции и ручная проверка всегда отправляются.",
            "Техническая причина пропуска остаётся в логах.",
        ]
    )


async def _mode_menu_view(user_id: int):
    settings_row = await _cached_user_settings(int(user_id))
    enabled = bool(
        getattr(settings_row, "skip_trade_notifications_enabled", False)
    )
    return (
        _section_text(
            "mode", skip_trade_notifications_enabled=enabled
        ),
        main_menu(
            "mode",
            skip_trade_notifications_enabled=enabled,
            owner_id=int(user_id),
        ),
    )


async def _skip_notifications_menu_view(user_id: int):
    settings_row = await _cached_user_settings(int(user_id))
    enabled = bool(
        getattr(settings_row, "skip_trade_notifications_enabled", False)
    )
    return (
        _skip_notifications_section_text(enabled),
        main_menu(
            "skip_notifications",
            skip_trade_notifications_enabled=enabled,
            owner_id=int(user_id),
        ),
    )


async def _finalize_user_api_quarantine_notification(
    user_id: int,
    result: ExecutionResult,
    *,
    delivered: bool,
    quarantine_card_rendered: bool,
) -> None:
    """Commit or release one claimed user quarantine notification."""

    if (
        result.status != "skipped"
        or result.payload.get("api_permission_quarantine") is not True
        or result.payload.get("api_quarantine_active") is not True
        or result.payload.get("api_quarantine_user_notification_pending") is not True
    ):
        return
    incident_token = str(result.payload.get("api_quarantine_incident_token") or "")[
        :128
    ]
    claim_acquired = bool(
        result.payload.get("api_quarantine_user_claim_acquired") is True
    )
    if delivered and quarantine_card_rendered:
        try:
            marked = await db.mark_api_key_quarantine_notified(
                int(user_id),
                "user",
                exchange="bingx",
                incident_token=incident_token,
            )
            if marked:
                result.payload["api_quarantine_user_notification_pending"] = False
            else:
                log.warning(
                    "user API quarantine delivery belongs to stale incident uid=%s",
                    int(user_id),
                )
        except Exception:
            log.exception(
                "failed to persist user API quarantine notification uid=%s",
                int(user_id),
            )
        return
    if claim_acquired and incident_token:
        try:
            await db.release_api_key_quarantine_notification_claim(
                int(user_id),
                "user",
                incident_token,
                exchange="bingx",
            )
        except Exception:
            log.exception(
                "failed to release user API quarantine claim uid=%s",
                int(user_id),
            )


async def _trade_result_notification_allowed(
    user_id: int, result: ExecutionResult, *, symbol: str = ""
) -> bool:
    """Apply the per-user preference before rendering or queueing Telegram text."""

    if (
        result.status == "skipped"
        and result.payload.get("api_permission_quarantine") is True
        and result.payload.get("api_quarantine_active") is True
    ):
        if result.payload.get("api_quarantine_user_notification_pending") is not True:
            result.payload["notification_suppressed"] = True
            result.payload["notification_suppression_kind"] = (
                "api_quarantine_already_notified"
            )
            log.info(
                "API_QUARANTINE_NOTIFICATION_SUPPRESSED user_id=%s symbol=%s",
                int(user_id),
                str(symbol or result.payload.get("symbol") or "").upper(),
            )
            return False

        incident_token = str(result.payload.get("api_quarantine_incident_token") or "")[
            :128
        ]
        if not incident_token:
            # Legacy active rows are migrated at startup. Fail open if an old or
            # externally-created row still lacks a token, so the safety card is
            # not lost merely because anti-spam identity is unavailable.
            result.payload["api_quarantine_notification_claim_legacy"] = True
            return True
        try:
            claimed = await db.claim_api_key_quarantine_notification(
                int(user_id),
                "user",
                incident_token,
                exchange="bingx",
            )
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception(
                "failed to claim user API quarantine notification uid=%s",
                int(user_id),
            )
            result.payload["api_quarantine_notification_claim_failed"] = True
            return True
        if claimed:
            result.payload["api_quarantine_user_claim_acquired"] = True
            return True
        result.payload["notification_suppressed"] = True
        result.payload["notification_suppression_kind"] = (
            "api_quarantine_notification_claimed_elsewhere"
        )
        log.info(
            "API_QUARANTINE_NOTIFICATION_CLAIM_SUPPRESSED user_id=%s symbol=%s",
            int(user_id),
            str(symbol or result.payload.get("symbol") or "").upper(),
        )
        return False

    if not is_suppressible_trade_skip_notification(result):
        return True
    try:
        settings_row = await _cached_user_settings(int(user_id))
        enabled = bool(getattr(settings_row, "skip_trade_notifications_enabled", False))
    except asyncio.CancelledError:
        raise
    except Exception as exc:
        # This is an optional card whose database default is OFF. A transient
        # preference read failure must not create a notification storm. Safety
        # messages never enter this branch and therefore still fail open.
        enabled = False
        log.warning(
            "skip notification preference unavailable; optional card suppressed "
            "uid=%s error=%s",
            int(user_id),
            f"{type(exc).__name__}: {exc}",
        )
    if enabled:
        return True
    result.payload["notification_suppressed"] = True
    result.payload["notification_suppression_kind"] = "optional_trade_skip"
    log.info(
        "SKIP_NOTIFICATION_SUPPRESSED user_id=%s symbol=%s skip_kind=%s reason=%s",
        int(user_id),
        str(symbol or result.payload.get("symbol") or "").upper(),
        str(result.payload.get("trade_skip_kind") or "unknown"),
        str(result.reason or "")[:160],
    )
    return False


def _resolved_limit_preset(settings_row) -> str:
    """Map legacy custom settings onto a visible profile when they match exactly."""
    saved = (
        str(getattr(settings_row, "limit_policy_preset", "balanced") or "balanced")
        .strip()
        .lower()
    )
    ttl = int(getattr(settings_row, "limit_ttl_hours", 24) or 0)
    mode = (
        str(getattr(settings_row, "limit_tp_invalidation_mode", "half") or "half")
        .strip()
        .lower()
    )
    if saved in _VISIBLE_LIMIT_PRESETS and LIMIT_POLICY_PRESETS.get(saved) == (
        ttl,
        mode,
    ):
        return saved
    for preset in _VISIBLE_LIMIT_PRESETS:
        if LIMIT_POLICY_PRESETS.get(preset) == (ttl, mode):
            return preset
    return saved


async def _limit_menu_markup(user_id: int):
    """Render LIMIT profiles plus the bounded custom TTL flow."""
    settings_row = await db.get_user_settings(int(user_id))
    return main_menu(
        "limits",
        selected_limit_preset=_resolved_limit_preset(settings_row),
        owner_id=int(user_id),
    )


async def _limit_settings_text(user_id: int) -> str:
    settings_row = await db.get_user_settings(int(user_id))
    pending = await db.pending_limit_executions_for_user(int(user_id), limit=500)
    ttl = int(getattr(settings_row, "limit_ttl_hours", 24) or 0)
    mode = str(getattr(settings_row, "limit_tp_invalidation_mode", "half") or "half")
    preset = _resolved_limit_preset(settings_row)
    ttl_text = "без ограничения" if ttl <= 0 else f"{ttl} ч"
    return "\n".join(
        [
            "⏳ <b>АКТУАЛЬНОСТЬ LIMIT</b>",
            "━━━━━━━━━━━━━━━━━━",
            "",
            *premium_arrow_lines(
                (
                    ("⚙️ Текущий режим", html.escape(limit_preset_label(preset))),
                    ("⏱ Максимальное ожидание", ttl_text),
                    ("🎯 Отмена после движения", html.escape(limit_tp_mode_label(mode))),
                    ("🛡 Пробой STOP до входа", "отмена всегда включена"),
                    ("📋 Активных LIMIT", len(pending)),
                )
            ),
            "",
            "Выбери готовый режим или задай свой срок. Настройка применяется только к новым LIMIT.",
            "Свой срок меняет только TTL и сохраняет текущее правило отмены после движения цены.",
            "Для уже стоящих ордеров используй кнопку «Применить текущий режим».",
        ]
    )


_BE_APPLY_ELIGIBLE_STATUSES = {
    "pending_limit",
    "opened",
    "protected",
    "partial_error",
}


def _normalize_execution_be_trigger(value) -> int | None:
    """Return a valid frozen execution trigger, or None for legacy/malformed."""
    if value in (None, "") or isinstance(value, bool):
        return None
    try:
        parsed = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    if not math.isfinite(parsed) or not parsed.is_integer():
        return None
    trigger = int(parsed)
    return trigger if trigger in {0, 1, 2, 3} else None


def _be_apply_row_decision(row: dict, target_trigger: int) -> tuple[str, int | None]:
    """Classify one execution without touching exchange or database state."""
    status = str(row.get("status") or "").strip().lower()
    payload = _parse_execution_payload(row)
    be_state = payload.get("be") if isinstance(payload.get("be"), dict) else {}
    current_trigger = _normalize_execution_be_trigger(
        payload.get("be_trigger_tp_index")
    )

    if status not in _BE_APPLY_ELIGIBLE_STATUSES:
        if status in {"manual_required", "partial_unrecoverable"}:
            return "manual_review", current_trigger
        if status == "opening_intent":
            return "opening_in_progress", current_trigger
        return "inactive", current_trigger
    if bool(be_state.get("moved")):
        return "already_moved", current_trigger
    if bool(be_state.get("manual_required")):
        return "manual_review", current_trigger
    if bool(be_state.get("manual_requested")):
        return "write_in_progress", current_trigger
    if any(
        (
            bool(be_state.get("replacement_in_progress")),
            bool(be_state.get("replacement_write_intent_v1")),
            bool(be_state.get("replacement_stop_id")),
            bool(be_state.get("replacement_stop")),
            bool(be_state.get("cleanup_cancel_intent_v1")),
        )
    ):
        return "write_in_progress", current_trigger
    if current_trigger == int(target_trigger):
        return "already_current", current_trigger
    if status == "pending_limit":
        return "eligible_limit", current_trigger
    return "eligible_position", current_trigger


def _be_trigger_label(trigger: int) -> str:
    return "выключено" if int(trigger) <= 0 else f"после TP{int(trigger)}"


async def _be_apply_preview_text(
    user_id: int, *, trigger_tp_index: int
) -> tuple[str, list[int]]:
    rows = await db.active_position_executions_for_user(int(user_id), limit=500)
    counts = {
        "eligible_position": 0,
        "eligible_limit": 0,
        "already_current": 0,
        "already_moved": 0,
        "manual_review": 0,
        "write_in_progress": 0,
        "opening_in_progress": 0,
        "inactive": 0,
    }
    execution_ids: list[int] = []
    for row in rows:
        decision, _old_trigger = _be_apply_row_decision(row, trigger_tp_index)
        counts[decision] = counts.get(decision, 0) + 1
        if decision in {"eligible_position", "eligible_limit"}:
            execution_id = int(row.get("id") or 0)
            if execution_id > 0:
                execution_ids.append(execution_id)

    skipped_ambiguous = (
        counts["manual_review"]
        + counts["write_in_progress"]
        + counts["opening_in_progress"]
    )
    lines = [
        "♻️ <b>ПРИМЕНИТЬ Б/У К ТЕКУЩИМ СДЕЛКАМ</b>",
        "━━━━━━━━━━━━━━━━━━━━",
        "",
        *premium_arrow_lines(
            (
                ("⚖️ Новая настройка", _be_trigger_label(trigger_tp_index)),
                ("📈 Открытых позиций", counts["eligible_position"]),
                ("⏳ Ожидающих LIMIT", counts["eligible_limit"]),
            )
        ),
        "",
        "Не будут изменены:",
        *premium_arrow_lines(
            (
                ("✅ Уже используют эту настройку", counts["already_current"]),
                ("🔒 Уже переведены в Б/У", counts["already_moved"]),
                ("🚨 Ручная проверка / операция идёт", skipped_ambiguous),
            )
        ),
        "",
        "Меняется только сохранённый уровень автоматического Б/У. "
        "STOP, TP, объём, вход и риск напрямую не изменяются.",
        "",
        "⚠️ Если выбранный TP уже подтверждён, обычный монитор может "
        "безопасно перенести STOP на ближайшем цикле.",
    ]
    return "\n".join(lines), execution_ids


async def _apply_be_trigger_to_current(
    user_id: int,
    *,
    trigger_tp_index: int,
    execution_ids: list[int],
) -> dict[str, int]:
    """Rewrite only frozen BE trigger snapshots after explicit confirmation."""
    allowed_ids = {int(x) for x in execution_ids if int(x) > 0}
    result = {
        "changed": 0,
        "positions": 0,
        "limits": 0,
        "skipped": 0,
    }
    updated_at = datetime.now(timezone.utc).isoformat()
    for execution_id in sorted(allowed_ids):
        async with db.execution_lock(execution_id):
            latest = await db.get_execution_by_id(execution_id)
            if not latest or int(latest.get("user_id") or 0) != int(user_id):
                result["skipped"] += 1
                continue
            decision, old_trigger = _be_apply_row_decision(
                latest, int(trigger_tp_index)
            )
            if decision not in {"eligible_position", "eligible_limit"}:
                result["skipped"] += 1
                continue
            status = str(latest.get("status") or "")
            patch = {
                "be_trigger_tp_index": int(trigger_tp_index),
                "be_trigger_policy_update_v1": {
                    "source": "telegram_apply_current",
                    "previous_trigger_tp_index": old_trigger,
                    "new_trigger_tp_index": int(trigger_tp_index),
                    "updated_at": updated_at,
                },
                # A wait/skip decision belongs to the old trigger. Clearing only
                # that retry metadata lets the normal BE monitor re-evaluate the
                # new policy from fresh position/TP evidence. Exchange orders are
                # never written by this UI operation.
                "be": {
                    "waiting_market_safe": False,
                    "waiting_retry": False,
                    "waiting_reason": None,
                    "waiting_since": None,
                    "waiting_last_checked_at": None,
                    "waiting_next_retry_after": None,
                    "waiting_backoff_sec": None,
                    "waiting_rate_limited": False,
                    "waiting_trigger_tp_index": None,
                    "waiting_trigger_ordinal": None,
                    "waiting_old_stop_ids": [],
                    "waiting_target_stop": None,
                    "waiting_qty": None,
                    "waiting_market_safety": None,
                    "waiting_bypass_event_tp_index": None,
                    "waiting_bypass_trigger_tp_index": None,
                    "waiting_bypass_waiting_trigger_tp_index": None,
                    "waiting_bypass_consumed_event_tp_index": None,
                    "skipped": None,
                    "trigger_tp_index": None,
                    "trigger_ordinal": None,
                },
            }
            saved = await db.merge_execution_metadata(
                execution_id,
                patch,
                expected_status=status,
                write_flow_audit_stage="be_apply_current_policy",
                write_flow_audit_status=status,
            )
            if not saved:
                result["skipped"] += 1
                continue
            result["changed"] += 1
            if decision == "eligible_limit":
                result["limits"] += 1
            else:
                result["positions"] += 1
    return result


async def _active_limits_text(user_id: int, rows: list[dict] | None = None) -> str:
    rows = (
        list(rows)
        if rows is not None
        else await db.pending_limit_executions_for_user(int(user_id), limit=100)
    )
    if not rows:
        return (
            "⏳ <b>ЛИМИТНЫЕ СДЕЛКИ</b>\n"
            "━━━━━━━━━━━━━━━━━━\n\n"
            "Активных LIMIT-ордеров нет."
        )
    lines = [
        "⏳ <b>ЛИМИТНЫЕ СДЕЛКИ</b>",
        "━━━━━━━━━━━━━━━━━━",
        f"Всего: <b>{len(rows)}</b>",
        "",
        "Выберите нужный ордер ниже 👇",
        "",
    ]
    now = datetime.now(timezone.utc)
    for row in rows[:15]:
        try:
            targets = json.loads(row.get("targets_json") or "[]")
        except (TypeError, ValueError):
            targets = []
        try:
            payload = json.loads(row.get("exchange_order_ids_json") or "{}")
        except (TypeError, ValueError):
            payload = {}
        policy = read_limit_policy(payload, targets=targets)
        created_raw = str(row.get("created_at") or "")
        age_text = "?"
        try:
            created = datetime.fromisoformat(created_raw.replace("Z", "+00:00"))
            if created.tzinfo is None:
                created = created.replace(tzinfo=timezone.utc)
            age_hours = max(0.0, (now - created).total_seconds() / 3600.0)
            age_text = f"{age_hours:.1f}ч"
        except Exception:
            pass
        ttl = int(policy.get("ttl_hours") or 0)
        ttl_text = "∞" if ttl <= 0 else f"{ttl}ч"
        mode_text = limit_tp_mode_label(str(policy.get("tp_mode") or "last"))
        if (
            str(policy.get("tp_mode") or "") == "tp2"
            and int(policy.get("tp_threshold_index") or 0) == 0
        ):
            mode_text += " (TP2 отсутствует - правило отключено)"
        lines.extend(
            [
                f"🪙 <b>{html.escape(str(row.get('symbol') or ''))}</b> · {html.escape(str(row.get('side') or '').upper())}",
                *premium_arrow_lines(
                    (
                        ("💵 Вход", html.escape(str(row.get('entry') or '—'))),
                        ("⏱ Возраст", age_text),
                        ("⚙️ Политика", f"TTL {ttl_text}, {html.escape(mode_text)}"),
                    )
                ),
                "",
            ]
        )
    if len(rows) > 15:
        lines.append(f"…ещё {len(rows)-15}")
    return "\n".join(lines)


def _parse_execution_payload(row: dict) -> dict:
    try:
        payload = json.loads(row.get("exchange_order_ids_json") or "{}")
    except (TypeError, ValueError):
        payload = {}
    return payload if isinstance(payload, dict) else {}


def _compact_limit_fill_status(status: dict | None) -> dict:
    source = status if isinstance(status, dict) else {}
    return {
        key: source.get(key)
        for key in (
            "order_id",
            "state",
            "terminal",
            "fully_filled",
            "filled_qty",
            "requested_qty",
            "avg_fill_price",
            "position_id",
            "external_oid",
            "qty_step",
        )
    }


def _active_limit_detail_text(row: dict) -> str:
    payload = _parse_execution_payload(row)
    try:
        targets = json.loads(row.get("targets_json") or "[]")
    except (TypeError, ValueError):
        targets = []
    if not isinstance(targets, list):
        targets = []
    policy = read_limit_policy(payload, targets=targets)
    ttl = int(policy.get("ttl_hours") or 0)
    ttl_label = "без ограничения" if ttl <= 0 else f"{ttl} ч"
    mode_label = limit_tp_mode_label(str(policy.get("tp_mode") or "last"))
    created_label = html.escape(
        str(row.get("created_at") or "—")[:19].replace("T", " ")
    )
    lines = [
        "🔵 <b>АКТИВНЫЙ LIMIT</b>",
        "━━━━━━━━━━━━━━━━━━━━",
        f"🪙 <b>{html.escape(str(row.get('symbol') or ''))}</b> • {html.escape(str(row.get('side') or '').upper())}",
        "",
        *premium_arrow_lines(
            (
                ("💵 Вход", html.escape(str(row.get("entry") or "—"))),
                ("📦 Объём", html.escape(str(row.get("qty") or "—"))),
                ("⚙️ Плечо", f"{html.escape(str(row.get('leverage') or '—'))}x"),
                ("🛡 STOP", html.escape(str(row.get("stop") or "—"))),
                ("🎯 Целей", len(targets)),
                ("⏳ Срок", html.escape(ttl_label)),
                ("📈 Отмена по движению", html.escape(mode_label)),
                ("🕒 Создан", created_label),
                ("🧾 Исполнение", f"<code>#{int(row.get('id') or 0)}</code>"),
            )
        ),
        "",
        "Можно отменить только этот конкретный LIMIT вашего BingX-аккаунта.",
    ]
    return "\n".join(lines)

def _manual_limit_cancel_audit(
    *,
    user_id: int,
    result: str,
    reason: str,
    entry_disposition: str = "",
    remainder_disposition: str = "",
) -> dict:
    return {
        "version": 1,
        "requested_by_user_id": int(user_id),
        "requested_at": datetime.now(timezone.utc).isoformat(),
        "result": str(result),
        "reason": str(reason)[:1000],
        "entry_disposition": str(entry_disposition),
        "remainder_disposition": str(remainder_disposition),
    }


async def _cancel_user_pending_limit_impl(user_id: int, execution_id: int) -> dict:
    """Cancel one exact LIMIT owned by the requesting user.

    The existing LIMIT reconciliation helpers are reused so this menu action
    never falls back to symbol-wide cancellation. A partial/full fill race is
    preserved as a live execution and handed back to the normal monitor.
    """
    uid = int(user_id)
    eid = int(execution_id)
    if uid <= 0 or eid <= 0:
        return {"state": "not_found", "execution_id": eid}

    adapter = None
    async with db.execution_lock(eid):
        row = await db.get_execution_by_id(eid)
        if not row or int(row.get("user_id") or 0) != uid:
            return {"state": "not_found", "execution_id": eid}
        if str(row.get("status") or "") != "pending_limit":
            return {
                "state": "not_pending",
                "execution_id": eid,
                "symbol": str(row.get("symbol") or ""),
                "status": str(row.get("status") or ""),
            }

        api_row = await _cached_api_key(uid, "bingx")
        if not api_row:
            return {
                "state": "api_missing",
                "execution_id": eid,
                "symbol": str(row.get("symbol") or ""),
            }

        symbol = str(row.get("symbol") or "").upper()
        side = str(row.get("side") or "").lower()
        payload = _parse_execution_payload(row)
        entry_order = (
            payload.get("entry") if isinstance(payload.get("entry"), dict) else {}
        )
        cancel_pending = (
            dict(payload.get("limit_cancel_pending"))
            if isinstance(payload.get("limit_cancel_pending"), dict)
            else {}
        )
        cancel_control = _limit_cancel_control(
            payload,
            policy_reason=str(cancel_pending.get("reason") or "manual_user"),
        )
        try:
            adapter = build_adapter(api_row)
            other_rows = await db.other_active_symbol_executions(
                uid, symbol, eid, limit=1
            )
            result = await _cancel_pending_entry_confirmed(
                adapter,
                user_id=uid,
                execution_id=eid,
                symbol=symbol,
                side=side,
                entry_order=entry_order,
                other_active_execution=bool(other_rows),
                attempts=(6 if cancel_control["allow_write"] else 2),
                allow_write=bool(cancel_control["allow_write"]),
                write_suppressed_exhausted=bool(cancel_control["exhausted"]),
                policy_reason=str(cancel_pending.get("reason") or "manual_user"),
            )
            disposition = result.disposition
            state_code = 0
            if isinstance(result.order_status, dict):
                try:
                    state_code = int(result.order_status.get("state") or 0)
                except (TypeError, ValueError):
                    state_code = 0

            if disposition == PendingEntryCancelDisposition.CANCELED_NO_FILL or (
                disposition == PendingEntryCancelDisposition.TERMINAL_NO_FILL
                and state_code == 4
            ):
                audit = _manual_limit_cancel_audit(
                    user_id=uid,
                    result="canceled_no_fill",
                    reason=result.reason,
                    entry_disposition=disposition.value,
                )
                saved = await db.update_execution_status_merge(
                    eid,
                    "canceled_external",
                    "LIMIT entry canceled by its owner from Telegram; exact BingX order reached terminal state with zero fill",
                    {
                        "manual_limit_cancel_v1": audit,
                        "limit_fill_status": _compact_limit_fill_status(
                            result.order_status
                        ),
                        "limit_cancel_pending": None,
                        "limit_cancel_confirmed": {
                            "reason": "manual_user",
                            "confirmed_at": datetime.now(timezone.utc).isoformat(),
                            "cancel_response_v1": (
                                result.cancel_response_audit
                                or cancel_pending.get("cancel_response_v1")
                            ),
                            "confirmation_v1": (
                                result.confirmation_audit
                                or cancel_pending.get("confirmation_v1")
                            ),
                        },
                    },
                    expected_status="pending_limit",
                )
                if saved:
                    cleanup = await db.finish_stale_market_events_if_group_inactive(eid)
                    if int(cleanup.get("finished") or 0) > 0:
                        log.info(
                            "G69_MARKET_EVENT_EAGER_TERMINAL execution_id=%s group_id=%s "
                            "status=canceled_external finished=%s reason=no_active_executions",
                            eid,
                            int(cleanup.get("trade_group_id") or 0),
                            int(cleanup.get("finished") or 0),
                        )
                if not saved:
                    return {
                        "state": "stale",
                        "execution_id": eid,
                        "symbol": symbol,
                    }
                return {
                    "state": "canceled",
                    "execution_id": eid,
                    "symbol": symbol,
                    "side": side,
                }

            if disposition in {
                PendingEntryCancelDisposition.FILLED,
                PendingEntryCancelDisposition.POSITION_EXISTS,
            }:
                # One user click may never produce two cancel writes. If the
                # first exact attempt was already dispatched, or the durable
                # backoff/ceiling suppresses writes, leave the remainder to the
                # normal post-fill monitor and persist the exact read outcome.
                if result.write_attempted or not cancel_control["allow_write"]:
                    updated_pending = _limit_cancel_pending_record(
                        previous=cancel_pending,
                        policy_reason=str(
                            cancel_pending.get("reason") or "manual_user"
                        ),
                        result=result,
                    )
                    saved = await db.update_execution_status_merge(
                        eid,
                        "pending_limit",
                        "Manual LIMIT cancel detected fill/position; no second cancel write was sent and post-fill reconciliation must continue",
                        {
                            "manual_limit_cancel_v1": _manual_limit_cancel_audit(
                                user_id=uid,
                                result="fill_or_position_read_only",
                                reason=result.reason,
                                entry_disposition=disposition.value,
                            ),
                            "limit_fill_status": _compact_limit_fill_status(
                                result.order_status
                            ),
                            "limit_cancel_pending": updated_pending,
                            "limit_cancel_race": {
                                "policy_reason": str(
                                    cancel_pending.get("reason") or "manual_user"
                                ),
                                "disposition": disposition.value,
                                "cancel_response_v1": (
                                    result.cancel_response_audit
                                    or cancel_pending.get("cancel_response_v1")
                                ),
                                "confirmation_v1": (
                                    result.confirmation_audit
                                    or cancel_pending.get("confirmation_v1")
                                ),
                                "detected_at": datetime.now(timezone.utc).isoformat(),
                            },
                        },
                        expected_status="pending_limit",
                    )
                    return {
                        "state": "filled" if saved else "stale",
                        "execution_id": eid,
                        "symbol": symbol,
                        "side": side,
                        "reason": result.reason,
                    }

                async with db.symbol_action_lock(uid, symbol):
                    remainder = await _cancel_opening_order_remainder_confirmed(
                        adapter,
                        symbol=symbol,
                        side=side,
                        entry_order=entry_order,
                    )
                status_payload = (
                    remainder.order_status
                    if isinstance(remainder.order_status, dict)
                    else result.order_status
                )
                if remainder.disposition == PendingEntryCancelDisposition.FILLED:
                    compact = _compact_limit_fill_status(status_payload)
                    requested = float(compact.get("requested_qty") or 0.0)
                    filled = float(compact.get("filled_qty") or 0.0)
                    fully_filled = bool(compact.get("fully_filled")) or (
                        requested > 0 and filled + 1e-12 >= requested
                    )
                    audit = _manual_limit_cancel_audit(
                        user_id=uid,
                        result=(
                            "already_fully_filled"
                            if fully_filled
                            else "remainder_canceled_after_partial_fill"
                        ),
                        reason=remainder.reason,
                        entry_disposition=disposition.value,
                        remainder_disposition=remainder.disposition.value,
                    )
                    saved = await db.update_execution_status_merge(
                        eid,
                        "pending_limit",
                        (
                            "Manual LIMIT cancel found a fill/position; the exact opening remainder is terminal and normal post-fill reconciliation must continue"
                        ),
                        {
                            "manual_limit_cancel_v1": audit,
                            "limit_fill_status": compact,
                            "limit_cancel_pending": None,
                            "limit_cancel_confirmed": {
                                "reason": "manual_user_partial_fill",
                                "confirmed_at": datetime.now(timezone.utc).isoformat(),
                                "cancel_response_v1": (
                                    remainder.cancel_response_audit
                                    or cancel_pending.get("cancel_response_v1")
                                ),
                                "confirmation_v1": (
                                    remainder.confirmation_audit
                                    or cancel_pending.get("confirmation_v1")
                                ),
                            },
                        },
                        expected_status="pending_limit",
                    )
                    if not saved:
                        return {
                            "state": "stale",
                            "execution_id": eid,
                            "symbol": symbol,
                        }
                    return {
                        "state": "filled" if fully_filled else "partial",
                        "execution_id": eid,
                        "symbol": symbol,
                        "side": side,
                        "filled_qty": filled,
                        "requested_qty": requested,
                    }

                updated_pending = _limit_cancel_pending_record(
                    previous=cancel_pending,
                    policy_reason=str(cancel_pending.get("reason") or "manual_user"),
                    result=remainder,
                )
                audit = _manual_limit_cancel_audit(
                    user_id=uid,
                    result="remainder_unconfirmed",
                    reason=remainder.reason,
                    entry_disposition=disposition.value,
                    remainder_disposition=remainder.disposition.value,
                )
                saved = await db.merge_execution_metadata(
                    eid,
                    {
                        "manual_limit_cancel_v1": audit,
                        "limit_fill_status": _compact_limit_fill_status(status_payload),
                        "limit_cancel_pending": updated_pending,
                    },
                    expected_status="pending_limit",
                )
                return {
                    "state": "unconfirmed" if saved else "stale",
                    "execution_id": eid,
                    "symbol": symbol,
                    "reason": remainder.reason,
                }

            if disposition == PendingEntryCancelDisposition.TERMINAL_NO_FILL:
                terminal_status = "error"
                terminal_reason = f"LIMIT entry reached terminal BingX state={state_code} without fill during manual cancel"
                audit = _manual_limit_cancel_audit(
                    user_id=uid,
                    result="terminal_no_fill_not_canceled",
                    reason=result.reason,
                    entry_disposition=disposition.value,
                )
                saved = await db.update_execution_status_merge(
                    eid,
                    terminal_status,
                    terminal_reason,
                    {
                        "manual_limit_cancel_v1": audit,
                        "limit_fill_status": _compact_limit_fill_status(
                            result.order_status
                        ),
                        "limit_cancel_pending": None,
                        "limit_cancel_confirmed": {
                            "reason": "manual_user_terminal",
                            "confirmed_at": datetime.now(timezone.utc).isoformat(),
                            "cancel_response_v1": (
                                result.cancel_response_audit
                                or cancel_pending.get("cancel_response_v1")
                            ),
                            "confirmation_v1": (
                                result.confirmation_audit
                                or cancel_pending.get("confirmation_v1")
                            ),
                        },
                    },
                    expected_status="pending_limit",
                )
                return {
                    "state": "terminal" if saved else "stale",
                    "execution_id": eid,
                    "symbol": symbol,
                    "reason": result.reason,
                }

            updated_pending = _limit_cancel_pending_record(
                previous=cancel_pending,
                policy_reason=str(cancel_pending.get("reason") or "manual_user"),
                result=result,
            )
            audit = _manual_limit_cancel_audit(
                user_id=uid,
                result="cancel_unconfirmed",
                reason=result.reason,
                entry_disposition=disposition.value,
            )
            saved = await db.merge_execution_metadata(
                eid,
                {
                    "manual_limit_cancel_v1": audit,
                    "limit_cancel_pending": updated_pending,
                },
                expected_status="pending_limit",
            )
            return {
                "state": "unconfirmed" if saved else "stale",
                "execution_id": eid,
                "symbol": symbol,
                "reason": result.reason,
            }
        except Exception as exc:
            log.exception(
                "manual LIMIT cancellation failed uid=%s execution=%s symbol=%s",
                uid,
                eid,
                symbol,
            )
            return {
                "state": "error",
                "execution_id": eid,
                "symbol": symbol,
                "reason": f"{type(exc).__name__}: {exc}",
            }
        finally:
            if adapter is not None:
                try:
                    await adapter.close()
                except Exception:
                    pass


async def _cancel_user_pending_limit(user_id: int, execution_id: int) -> dict:
    """Never let a lock/database failure escape the destructive callback."""
    try:
        return await _cancel_user_pending_limit_impl(user_id, execution_id)
    except Exception as exc:
        log.exception(
            "manual LIMIT cancellation wrapper failed uid=%s execution=%s",
            user_id,
            execution_id,
        )
        return {
            "state": "error",
            "execution_id": int(execution_id or 0),
            "reason": f"{type(exc).__name__}: {exc}",
        }


async def _recheck_user_pending_limit_impl(user_id: int, execution_id: int) -> dict:
    """Read-only exact LIMIT reconciliation used by the Telegram safety button."""

    uid = int(user_id)
    eid = int(execution_id)
    if uid <= 0 or eid <= 0:
        return {"state": "not_found", "execution_id": eid}

    adapter = None
    async with db.execution_lock(eid):
        row = await db.get_execution_by_id(eid)
        if not row or int(row.get("user_id") or 0) != uid:
            return {"state": "not_found", "execution_id": eid}
        if str(row.get("status") or "") != "pending_limit":
            return {
                "state": "not_pending",
                "execution_id": eid,
                "symbol": str(row.get("symbol") or ""),
                "status": str(row.get("status") or ""),
            }

        api_row = await _cached_api_key(uid, "bingx")
        if not api_row:
            return {
                "state": "api_missing",
                "execution_id": eid,
                "symbol": str(row.get("symbol") or ""),
            }

        symbol = str(row.get("symbol") or "").upper()
        side = str(row.get("side") or "").lower()
        payload = _parse_execution_payload(row)
        entry_order = (
            payload.get("entry") if isinstance(payload.get("entry"), dict) else {}
        )
        pending = (
            dict(payload.get("limit_cancel_pending"))
            if isinstance(payload.get("limit_cancel_pending"), dict)
            else {}
        )
        exhausted = bool(pending.get("exhausted"))
        try:
            adapter = build_adapter(api_row)
            other_rows = await db.other_active_symbol_executions(
                uid, symbol, eid, limit=1
            )
            result = await _cancel_pending_entry_confirmed(
                adapter,
                user_id=uid,
                execution_id=eid,
                symbol=symbol,
                side=side,
                entry_order=entry_order,
                other_active_execution=bool(other_rows),
                attempts=4,
                allow_write=False,
                write_suppressed_exhausted=exhausted,
                policy_reason=str(pending.get("reason") or "manual_recheck"),
            )
            disposition = result.disposition

            if disposition in {
                PendingEntryCancelDisposition.CANCELED_NO_FILL,
                PendingEntryCancelDisposition.TERMINAL_NO_FILL,
            }:
                policy_reason = str(pending.get("reason") or "").strip().lower()
                terminal_status, terminal_reason, _title = (
                    _terminal_no_fill_classification(
                        (
                            result.order_status
                            if isinstance(result.order_status, dict)
                            else {}
                        ),
                        policy_reason=policy_reason,
                    )
                )
                saved = await db.update_execution_status_merge(
                    eid,
                    terminal_status,
                    terminal_reason,
                    {
                        "limit_cancel_pending": None,
                        "limit_fill_status": _compact_limit_fill_status(
                            result.order_status
                        ),
                        "limit_cancel_confirmed": {
                            "reason": policy_reason or "manual_recheck",
                            "confirmed_at": datetime.now(timezone.utc).isoformat(),
                            "cancel_response_v1": (
                                result.cancel_response_audit
                                or pending.get("cancel_response_v1")
                            ),
                            "confirmation_v1": (
                                result.confirmation_audit
                                or pending.get("confirmation_v1")
                            ),
                        },
                        "limit_cancel_recheck_v1": {
                            "checked_at": datetime.now(timezone.utc).isoformat(),
                            "requested_by_user_id": uid,
                            "result": disposition.value,
                            "reason": result.reason,
                            "confirmation_v1": result.confirmation_audit,
                        },
                    },
                    expected_status="pending_limit",
                )
                return {
                    "state": (
                        "canceled"
                        if saved and terminal_status.startswith("canceled")
                        else "terminal" if saved else "stale"
                    ),
                    "execution_id": eid,
                    "symbol": symbol,
                    "reason": result.reason,
                }

            if disposition in {
                PendingEntryCancelDisposition.FILLED,
                PendingEntryCancelDisposition.POSITION_EXISTS,
            }:
                saved = await db.update_execution_status_merge(
                    eid,
                    "pending_limit",
                    "Read-only Telegram recheck detected fill/position; post-fill reconciliation must continue",
                    {
                        "limit_cancel_pending": None,
                        "limit_fill_status": _compact_limit_fill_status(
                            result.order_status
                        ),
                        "limit_cancel_race": {
                            "policy_reason": str(pending.get("reason") or ""),
                            "disposition": disposition.value,
                            "cancel_response_v1": (
                                result.cancel_response_audit
                                or pending.get("cancel_response_v1")
                            ),
                            "confirmation_v1": (
                                result.confirmation_audit
                                or pending.get("confirmation_v1")
                            ),
                            "detected_at": datetime.now(timezone.utc).isoformat(),
                        },
                        "limit_cancel_recheck_v1": {
                            "checked_at": datetime.now(timezone.utc).isoformat(),
                            "requested_by_user_id": uid,
                            "result": disposition.value,
                            "reason": result.reason,
                            "confirmation_v1": result.confirmation_audit,
                        },
                    },
                    expected_status="pending_limit",
                )
                return {
                    "state": "filled" if saved else "stale",
                    "execution_id": eid,
                    "symbol": symbol,
                    "reason": result.reason,
                }

            recheck_patch = {
                "limit_cancel_recheck_v1": {
                    "checked_at": datetime.now(timezone.utc).isoformat(),
                    "requested_by_user_id": uid,
                    "result": disposition.value,
                    "reason": result.reason,
                    "confirmation_v1": result.confirmation_audit,
                },
            }
            if pending:
                updated_pending = _limit_cancel_pending_record(
                    previous=pending,
                    policy_reason=str(pending.get("reason") or "manual_recheck"),
                    result=result,
                )
                recheck_patch["limit_cancel_pending"] = updated_pending
            else:
                # The detail menu exposes the same safe button even before an
                # invalidation condition exists. A pure status check must not
                # manufacture a cancellation incident or consume retry state.
                updated_pending = {}
            saved = await db.merge_execution_metadata(
                eid,
                recheck_patch,
                expected_status="pending_limit",
            )
            return {
                "state": "active" if saved else "stale",
                "execution_id": eid,
                "symbol": symbol,
                "reason": result.reason,
                "attempts": int(updated_pending.get("write_attempts") or 0),
                "exhausted": bool(updated_pending.get("exhausted")),
                "cancel_pending": bool(pending),
            }
        except Exception as exc:
            log.exception(
                "read-only LIMIT recheck failed uid=%s execution=%s symbol=%s",
                uid,
                eid,
                symbol,
            )
            return {
                "state": "error",
                "execution_id": eid,
                "symbol": symbol,
                "reason": f"{type(exc).__name__}: {exc}",
            }
        finally:
            if adapter is not None:
                try:
                    await adapter.close()
                except Exception:
                    pass


async def _recheck_user_pending_limit(user_id: int, execution_id: int) -> dict:
    try:
        return await _recheck_user_pending_limit_impl(user_id, execution_id)
    except Exception as exc:
        log.exception(
            "read-only LIMIT recheck wrapper failed uid=%s execution=%s",
            user_id,
            execution_id,
        )
        return {
            "state": "error",
            "execution_id": int(execution_id or 0),
            "reason": f"{type(exc).__name__}: {exc}",
        }


def _limit_recheck_result_text(result: dict) -> str:
    state = str(result.get("state") or "error")
    symbol = html.escape(str(result.get("symbol") or "LIMIT"))
    eid = int(result.get("execution_id") or 0)
    reason = html.escape(str(result.get("reason") or "Нет подробностей."))
    if state == "canceled":
        return (
            "✅ <b>ОТМЕНА ПОДТВЕРЖДЕНА</b>\n"
            "━━━━━━━━━━━━━━━━━━━━\n"
            f"🪙 <b>{symbol}</b> • <code>#{eid}</code>\n\n"
            "BingX теперь подтверждает конечное состояние ордера без исполнения.\n"
            "Новый cancel-запрос не отправлялся."
        )
    if state == "filled":
        return (
            "🟠 <b>ОБНАРУЖЕНО ИСПОЛНЕНИЕ</b>\n"
            "━━━━━━━━━━━━━━━━━━━━\n"
            f"🪙 <b>{symbol}</b> • <code>#{eid}</code>\n\n"
            f"{reason}\n"
            "Бот передал сделку обычному контуру сопровождения позиции."
        )
    if state == "active":
        attempts = int(result.get("attempts") or 0)
        exhausted = bool(result.get("exhausted"))
        cancel_pending = bool(result.get("cancel_pending"))
        tail = (
            "Лимит автоматических cancel-запросов исчерпан; дальнейшая работа только read-only."
            if exhausted
            else (
                "Следующий cancel возможен только после durable-паузы."
                if cancel_pending
                else "Условия автоматического удаления пока нет; выполнена только проверка состояния."
            )
        )
        attempts_line = (
            f"Записей cancel: <b>{attempts}/3</b>\n" if cancel_pending else ""
        )
        return (
            "⚠️ <b>LIMIT ЕЩЁ НЕ ПОДТВЕРЖДЁН ОТМЕНЁННЫМ</b>\n"
            "━━━━━━━━━━━━━━━━━━━━\n"
            f"🪙 <b>{symbol}</b> • <code>#{eid}</code>\n\n"
            f"{reason}\n"
            f"{attempts_line}"
            f"{tail}\n\n"
            "Кнопка выполнила только чтение order/get, open_orders, fill и позиции."
        )
    if state == "not_pending":
        return (
            "ℹ️ <b>LIMIT УЖЕ НЕ АКТИВЕН В УЧЁТЕ БОТА</b>\n"
            "━━━━━━━━━━━━━━━━━━━━\n"
            f"🪙 <b>{symbol}</b> • <code>#{eid}</code>"
        )
    if state == "terminal":
        return (
            "⚠️ <b>LIMIT ЗАВЕРШЁН BingX БЕЗ ИСПОЛНЕНИЯ</b>\n"
            "━━━━━━━━━━━━━━━━━━━━\n"
            f"🪙 <b>{symbol}</b> • <code>#{eid}</code>\n\n"
            f"{reason}\n"
            "Новый cancel-запрос не отправлялся; состояние сохранено для проверки."
        )
    if state == "api_missing":
        return "🔴 <b>BingX API НЕ ПОДКЛЮЧЁН</b>\nПерепроверка невозможна."
    if state in {"not_found", "stale"}:
        return "ℹ️ <b>СОСТОЯНИЕ УЖЕ ИЗМЕНИЛОСЬ</b>\nОбновите список активных LIMIT."
    return (
        "🔴 <b>ОШИБКА ПЕРЕПРОВЕРКИ LIMIT</b>\n"
        "━━━━━━━━━━━━━━━━━━━━\n"
        f"🪙 <b>{symbol}</b> • <code>#{eid}</code>\n\n{reason}"
    )


def _manual_limit_cancel_result_text(result: dict) -> str:
    state = str(result.get("state") or "error")
    symbol = html.escape(str(result.get("symbol") or "LIMIT"))
    eid = int(result.get("execution_id") or 0)
    if state == "canceled":
        return (
            "✅ <b>LIMIT-ОРДЕР ОТМЕНЁН</b>\n"
            "━━━━━━━━━━━━━━━━━━━━\n"
            f"🪙 <b>{symbol}</b>\n"
            f"🧾 Исполнение: <code>#{eid}</code>\n\n"
            "Отменён только выбранный ордер на вашем BingX-аккаунте.\n"
            "Позиция по нему не была открыта."
        )
    if state == "partial":
        return (
            "⚠️ <b>LIMIT ИСПОЛНИЛСЯ ЧАСТИЧНО</b>\n"
            "━━━━━━━━━━━━━━━━━━━━\n"
            f"🪙 <b>{symbol}</b>\n"
            f"📦 Исполнено: <b>{html.escape(str(result.get('filled_qty')or '—'))}</b>\n\n"
            "Оставшаяся часть входа отменена точечно.\n"
            "Исполненная позиция не закрыта и продолжит сопровождаться ботом."
        )
    if state == "filled":
        return (
            "ℹ️ <b>LIMIT УЖЕ ИСПОЛНЕН</b>\n"
            "━━━━━━━━━━━━━━━━━━━━\n"
            f"🪙 <b>{symbol}</b>\n\n"
            "Ордер успел полностью исполниться до отмены.\n"
            "Открытая позиция не удалена. Перейдите в раздел «Позиции»."
        )
    if state in {"not_pending", "stale"}:
        return (
            "ℹ️ <b>СОСТОЯНИЕ УЖЕ ИЗМЕНИЛОСЬ</b>\n"
            "━━━━━━━━━━━━━━━━━━━━\n"
            f"🪙 <b>{symbol}</b>\n\n"
            "LIMIT уже исполнен, отменён или обрабатывается монитором.\n"
            "Обновите список активных лимиток."
        )
    if state == "not_found":
        return (
            "🔒 <b>LIMIT НЕДОСТУПЕН</b>\n"
            "━━━━━━━━━━━━━━━━━━━━\n"
            "Ордер не найден среди ваших активных лимиток.\n"
            "Чужие исполнения недоступны и не изменяются."
        )
    if state == "api_missing":
        return (
            "🔴 <b>BingX API НЕ ПОДКЛЮЧЁН</b>\n"
            "━━━━━━━━━━━━━━━━━━━━\n"
            "Отменить ордер через бота невозможно. Проверьте его вручную на BingX."
        )
    if state == "unconfirmed":
        return (
            "⚠️ <b>ОТМЕНА НЕ ПОДТВЕРЖДЕНА</b>\n"
            "━━━━━━━━━━━━━━━━━━━━\n"
            f"🪙 <b>{symbol}</b>\n\n"
            "Бот не получил достаточного подтверждения BingX и не пометил LIMIT отменённым.\n"
            "Не нажимайте повторно вслепую: монитор продолжит безопасную сверку."
        )
    if state == "terminal":
        return (
            "⚠️ <b>LIMIT ЗАВЕРШЁН BingX</b>\n"
            "━━━━━━━━━━━━━━━━━━━━\n"
            f"🪙 <b>{symbol}</b>\n\n"
            "Биржа уже перевела ордер в конечное состояние без исполнения.\n"
            "Запись сохранена для проверки."
        )
    return (
        "🔴 <b>НЕ УДАЛОСЬ ОТМЕНИТЬ LIMIT</b>\n"
        "━━━━━━━━━━━━━━━━━━━━\n"
        f"🪙 <b>{symbol}</b>\n\n"
        f"Причина: <code>{html.escape(str(result.get('reason')or 'неизвестная ошибка')[:500])}</code>\n\n"
        "Проверьте ордер на BingX."
    )


async def _limit_apply_preview_text(
    user_id: int,
    *,
    ttl_hours: int,
    tp_mode: str,
) -> tuple[str, list[int]]:
    """Dry-run a policy change against the exact currently pending rows."""
    rows = await db.pending_limit_executions_for_user(int(user_id), limit=500)
    now = datetime.now(timezone.utc)
    execution_ids: list[int] = []
    details: list[str] = []
    cancel_ttl = 0
    cancel_tp = 0
    keep = 0
    tp_rule_unavailable = 0
    listed_rows = 0

    for row in rows:
        execution_id = int(row.get("id") or 0)
        if execution_id > 0:
            execution_ids.append(execution_id)
        try:
            targets = [float(x) for x in json.loads(row.get("targets_json") or "[]")]
        except (TypeError, ValueError):
            targets = []
        try:
            payload = json.loads(row.get("exchange_order_ids_json") or "{}")
        except (TypeError, ValueError):
            payload = {}
        if not isinstance(payload, dict):
            payload = {}

        created_raw = str(row.get("created_at") or "")
        age_hours: float | None = None
        try:
            created = datetime.fromisoformat(created_raw.replace("Z", "+00:00"))
            if created.tzinfo is None:
                created = created.replace(tzinfo=timezone.utc)
            age_hours = max(0.0, (now - created).total_seconds() / 3600.0)
        except Exception:
            age_hours = None

        threshold = limit_threshold_index(tp_mode, len(targets))
        runtime = payload.get(LIMIT_POLICY_RUNTIME_KEY)
        if not isinstance(runtime, dict):
            runtime = {}
        try:
            max_tp_passed = max(0, int(runtime.get("max_tp_passed") or 0))
        except (TypeError, ValueError):
            max_tp_passed = 0

        reason = "🟢 останется активной"
        if ttl_hours > 0 and age_hours is not None and age_hours > ttl_hours:
            reason = f"🔴 отменится по TTL ({age_hours:.1f}ч > {ttl_hours}ч)"
            cancel_ttl += 1
        elif threshold > 0 and max_tp_passed >= threshold:
            reason = (
                f"🔴 отменится по сохранённому касанию TP{threshold} "
                f"(пройдено {max_tp_passed})"
            )
            cancel_tp += 1
        elif tp_mode == "tp2" and len(targets) < 2:
            reason = "🟡 TP2 отсутствует - для этой сделки действуют только TTL и STOP"
            tp_rule_unavailable += 1
            keep += 1
        else:
            keep += 1

        if listed_rows < 15:
            age_text = "?" if age_hours is None else f"{age_hours:.1f}ч"
            details.extend(
                [
                    f"🪙 <b>{html.escape(str(row.get('symbol')or ''))}</b> • "
                    f"{html.escape(str(row.get('side')or '').upper())}",
                    f"Стоит: {age_text} • {reason}",
                    "",
                ]
            )
            listed_rows += 1

    ttl_text = "без ограничения" if ttl_hours <= 0 else f"{ttl_hours} ч"
    lines = [
        "⚠️ <b>ПРИМЕНИТЬ К АКТИВНЫМ LIMIT?</b>",
        "━━━━━━━━━━━━━━━━━━━━",
        f"Активных ордеров в снимке: <b>{len(rows)}</b>",
        f"Новый срок: <b>{ttl_text}</b>",
        f"Новое правило: <b>{html.escape(limit_tp_mode_label(tp_mode))}</b>",
        "",
        f"Сразу по TTL: <b>{cancel_ttl}</b>",
        f"Сразу по сохранённым TP-касаниям: <b>{cancel_tp}</b>",
        f"Останутся активными: <b>{keep}</b>",
    ]
    if tp_rule_unavailable:
        lines.append(
            f"Без TP2 в плане: <b>{tp_rule_unavailable}</b> - TP-удаление для них отключится"
        )
    lines.extend(["", *details])
    if len(rows) > listed_rows:
        lines.append(f"…ещё {len(rows)-listed_rows} ордеров")
    lines.extend(
        [
            "",
            "Это безопасный dry-run по базе и уже сохранённым касаниям. "
            "Перед реальной отменой monitor заново проверит точный BingX orderId, "
            "dealVol, позицию и текущий STOP. Новые LIMIT, появившиеся после этого "
            "экрана, подтверждение не затронет.",
        ]
    )
    return "\n".join(lines), execution_ids


def _section_text(
    section: str, *, skip_trade_notifications_enabled: bool = False
) -> str:
    if section == "exchanges":
        return "\n".join(
            [
                "🔑 <b>BingX API</b>",
                "━━━━━━━━━━━━━━━━━━",
                "",
                "🏦 BingX Futures",
                "",
                *premium_arrow_lines(
                    (
                        ("🔐 Права", "чтение счёта + торговля"),
                        ("🚫 Вывод средств", "не включать"),
                        ("✅ Назначение", "открытие сделок, STOP, TP и Б/У"),
                    )
                ),
                "",
                "Если API уже подключён - его можно отключить и подключить новый.",
            ]
        )
    if section == "mode":
        return "\n".join(
            [
                "🟢 <b>РЕЖИМ РАБОТЫ</b>",
                "━━━━━━━━━━━━━━━━━━",
                "",
                *premium_arrow_lines(
                    (
                        ("🤖 Авто", "бот открывает сделки по VIP-сигналам"),
                        ("👁 Просмотр", "показывает, что открыл бы, без реального входа"),
                        ("⏸ Выкл", "новые сигналы игнорируются"),
                        (
                            "🔔 Уведомления о пропусках",
                            "включены"
                            if bool(skip_trade_notifications_enabled)
                            else "выключены",
                        ),
                    )
                ),
                "",
                "Уже открытые сделки продолжают сопровождаться по правилам защиты.",
                "Критические ошибки и ручная проверка всегда остаются включёнными.",
            ]
        )
    if section == "risk":
        return "\n".join(
            [
                "⚙️ <b>РИСК-МЕНЕДЖМЕНТ</b>",
                "━━━━━━━━━━━━━━━━━━",
                "",
                *premium_arrow_lines(
                    (
                        ("🧮 Рекомендуемый режим", "10 сделок по 1%"),
                        ("📊 Портфельный риск", "ограничивает общий риск открытых сделок"),
                        ("⚖️ Б/У", "может освобождать риск-слот после подтверждения STOP"),
                    )
                ),
                "",
                "Пример: депозит 1000 USDT, риск 1% = 10 USDT риска на сделку.",
            ]
        )
    if section == "tp":
        return "\n".join(
            [
                "🎯 <b>СХЕМА ТЕЙКОВ</b>",
                "━━━━━━━━━━━━━━━━━━",
                "",
                *premium_arrow_lines(
                    (
                        ("🎯 3 TP / Все TP", "ограничить количество целей или брать все"),
                        ("🧠 Умная", "больше объёма на ближних целях"),
                        ("🛡️ Ранняя фиксация", "70% / 15% / 10% / 5%, максимум 4 TP"),
                        ("🚀 Разгон", "10% / 65% / 20% / 5%, максимум 4 TP"),
                        ("🔔 Колокол", "больше объёма в середине цепочки"),
                        ("⚖️ Равные доли", "объём делится одинаково"),
                    )
                ),
                "",
                "Если в сигнале есть проценты и сумма = 100%, их можно использовать вместо схемы.",
            ]
        )
    if section == "limits":
        return "\n".join(
            [
                "⏳ <b>АКТУАЛЬНОСТЬ LIMIT</b>",
                "━━━━━━━━━━━━━━━━━━",
                "",
                *premium_arrow_lines(
                    (
                        ("⚡ Быстрый", "короткое ожидание входа"),
                        ("🎯 После TP2", "ждать до движения к TP2"),
                        ("⚖️ Стандартный", "баланс времени и движения"),
                        ("🛡 Долгий", "максимально терпеливый режим"),
                    )
                ),
                "",
                "Пробой STOP до входа всегда отменяет неисполненный LIMIT.",
            ]
        )
    if section == "be":
        return "\n".join(
            [
                "⚖️ <b>БЕЗУБЫТОК</b>",
                "━━━━━━━━━━━━━━━━━━",
                "",
                *premium_arrow_lines(
                    (
                        ("После TP1", "самый защитный режим"),
                        ("После TP2 / TP3", "больше пространства для сделки"),
                        ("Выкл", "STOP остаётся исходным"),
                    )
                ),
                "",
                "Когда условие выполнено, бот переносит STOP в зону Б/У и подтверждает его на бирже.",
                "Новая настройка автоматически действует только на новые сделки и новые LIMIT.",
                "Для уже существующих используй кнопку «♻️ Применить к текущим сделкам».",
            ]
        )
    return "🤖 BingXProfitBot"



def _help_text() -> str:
    return """🤖 <b>ANTILUD VIP CORE - команды</b>
━━━━━━━━━━━━━━━━━━━━

🏦 Биржа: <b>BingX</b>

<b>Основное:</b>
/start, /help, меню
/balance, баланс, б
/positions, позиции, сделки
vip статус

<b>VIP режим:</b>
vip авто / vip просмотр / vip выкл

<b>Риск:</b>
vip риск 1
vip дневной риск 10
vip макс сделки 5
vip портфель риск 6
vip риск режим бу10
vip бу риск вкл / vip бу риск выкл

<b>Тейки:</b>
vip тейки 3 / vip тейки все
vip тейки умная / колокол / равная
vip проценты сигнала вкл / выкл

<b>Безубыток:</b>
vip бу после 1 / 2 / 3
vip бу выкл

<b>BingX API:</b>
/api bingx BingX_API_KEY BingX_API_SECRET
/delete_api
/api_setup

<b>Диагностика:</b>
проверь BTCUSDT
/status
/memory
/market_event EVENT_ID — read-only диагностика события

<b>Админ:</b>
/users — все юзеры
/users wl — только white-list
/whitelist_add USER_ID
/whitelist_remove USER_ID
/whitelist_list

<b>Статистика v2:</b>
/stats — текущий период
/stats_periods — список периодов
/stats_period ID — конкретный период
/stats_all — сводка по периодам
/stats_tech [ID] — технические детали
/stats_financial [ID] — фактические финансы
/stats_quality [ID] — качество данных
/stats_recovery [ID] — recoverable findings
/stats_recovery_request AUDIT_ID — audit-запрос без автоисправления
/stats_export [ID] — ZIP с 8 файлами
/stats_reset [причина] — новый период без удаления истории

⚠️ API-сообщение безопаснее вводить через пошаговое меню.
"""


async def _safe_edit(message: Message, text: str, reply_markup=None) -> bool:
    """Edit the current menu card without duplicating identical content.

    Telegram raises ``Bad Request: message is not modified`` when the same
    inline button is pressed repeatedly and the rendered text did not change.
    That response is successful from the user's perspective and must be
    ignored. Only errors proving that the original message cannot be edited
    are allowed to fall back to a new message.
    """
    try:
        await message.edit_text(text, reply_markup=reply_markup)
        return True
    except Exception as exc:
        error_text = str(exc).lower()

        # Normal idempotent outcome: the requested section is already shown.
        if "message is not modified" in error_text:
            return True

            # Telegram cannot edit some old/service/deleted messages. In those
            # specific cases a new card is the only useful fallback.
        non_editable_markers = (
            "message can't be edited",
            "message can not be edited",
            "message to edit not found",
            "there is no text in the message to edit",
            "message identifier is not specified",
        )
        if any(marker in error_text for marker in non_editable_markers):
            await message.answer(text, reply_markup=reply_markup)
            return True

            # Do not silently turn every transient edit error into a duplicate
            # message. Log the failure so Railway diagnostics retain the cause.
        log.warning("Failed to edit menu message without safe fallback: %s", exc)
        return False


async def _safe_action_result(
    call: CallbackQuery,
    text: str,
    *,
    reply_markup=None,
    event_key: str,
) -> bool:
    """Show a destructive-action result and durably retry only on UI failure."""
    if await _safe_edit(call.message, text, reply_markup=reply_markup):
        return True
    try:
        await call.message.answer(text, reply_markup=reply_markup)
        return True
    except Exception as exc:
        log.warning("Failed to send action result in source chat: %s", exc)

    async def notify(user_id: int, payload: str):
        return await send_queued_private_message(
            call.bot,
            int(user_id),
            payload,
            log_context="manual action result",
        )

    return await send_or_enqueue(
        notify,
        int(call.from_user.id),
        text,
        source="manual_user_action",
        event_key=event_key,
    )


async def _send_terms_prompt(
    message: Message, user_id: int | None = None, username: str | None = None
) -> None:
    uid = user_id or message.from_user.id
    uname = username if username is not None else message.from_user.username
    await db.ensure_user(uid, uname, _is_admin(uid))
    current_hash = terms_hash()
    accepted = await db.has_accepted_terms(uid, TERMS_VERSION, current_hash)
    status = "✅ Уже принято" if accepted else "⚠️ Требуется подтверждение"
    text = (
        "📄 <b>Пользовательское соглашение ANTILUD</b>\n"
        "━━━━━━━━━━━━━━━━━━━━\n"
        f"Версия: <b>{TERMS_VERSION}</b>\n"
        f"Статус: <b>{status}</b>\n\n"
        "Перед подключением API нужно скачать файл, ознакомиться с условиями и принять их кнопкой ниже.\n\n"
        "Без принятия условий бот не будет сохранять API-ключи биржи."
    )
    try:
        await message.answer_document(
            BufferedInputFile(terms_bytes(), filename=TERMS_FILENAME),
            caption=text,
            reply_markup=terms_accept_menu(owner_id=uid),
        )
    except Exception:
        # Fallback for clients/chats where sending a document failed.
        await message.answer(
            text
            + "\n\n⚠️ Не удалось отправить TXT-файл. Попробуй команду /terms ещё раз.",
            reply_markup=terms_accept_menu(owner_id=uid),
        )


async def _terms_gate_ok(message: Message) -> bool:
    uid = message.from_user.id
    uname = message.from_user.username
    await db.ensure_user(uid, uname, _is_admin(uid))
    if await db.has_accepted_terms(uid, TERMS_VERSION, terms_hash()):
        return True
    await message.answer(
        "⚠️ <b>API не сохранён.</b>\n\n"
        "Перед подключением биржи нужно принять пользовательское соглашение и уведомление о рисках.\n"
        "Сейчас отправлю TXT-файл. Скачай, прочитай и нажми «✅ Принимаю условия».\n\n"
        "После подтверждения повтори команду /api.",
    )
    await _send_terms_prompt(message, uid, uname)
    return False


async def _show_status(message: Message, user_id: int | None = None) -> None:
    uid = user_id or message.from_user.id
    await message.answer(
        await _status_text(uid),
        reply_markup=_home_menu(message.from_user.id),
    )


async def _handle_memory_status(
    message: Message, user_id: int | None = None, username: str | None = None
) -> None:
    uid = user_id or message.from_user.id
    uname = username if username is not None else message.from_user.username
    await db.ensure_user(uid, uname, _is_admin(uid))
    saved = await db.list_user_api_exchanges(uid)
    saved_text = ", ".join(saved) if saved else "нет"
    settings = get_settings()
    backend = db.storage_backend().upper()
    hint = db.persistence_hint()

    text = (
        "🧠 Память бота / API\n"
        "━━━━━━━━━━━━━━━━━━━━\n"
        f"Хранилище: <b>{backend}</b>\n"
        f"Путь SQLite: <code>{settings.DATABASE_PATH}</code>\n"
        f"DATABASE_URL: {'✅ задан' if bool(settings.DATABASE_URL) else '❌ не задан'}\n"
        f"Биржа: <b>BingX</b>\n"
        f"Сохранённые API: <b>{saved_text}</b>\n\n"
        f"{hint}\n\n"
        "Важно: API сохраняются не в Telegram, а в БД. После редеплоя они останутся только при PostgreSQL или Railway Volume на /data. ENCRYPTION_KEY должен оставаться тем же самым."
    )
    await message.answer(text)


async def _balance_text(user_id: int, username: str | None = None) -> str:
    uid = user_id
    uname = username
    await db.ensure_user(uid, uname, _is_admin(uid))
    exchange = "bingx"

    if not get_settings().is_exchange_enabled(exchange):
        return "❌ В этой сборке доступна только BingX."

    api_row = await _cached_api_key(uid, "bingx")
    if not api_row:
        fmt = "/api bingx BingX_API_KEY BingX_API_SECRET"
        return (
            f"❌ API для {exchange.upper()} не найден.\n"
            f"Сначала добавь ключ:\n<code>{fmt}</code>"
        )

    adapter = None
    try:
        adapter = build_adapter(api_row)
        if hasattr(adapter, "fetch_balance_details"):
            details = await adapter.fetch_balance_details()
            balance = _first_present_finite(
                details,
                ("total_equity", "total_wallet_balance", "available_balance", "USDT"),
            )
        else:
            details = {}
            balance = await adapter.fetch_balance_usdt()
    except Exception as exc:
        return (
            f"❌ Не удалось получить баланс {exchange.upper()}.\n"
            f"Причина: {type(exc).__name__}: {str(exc)[:700]}"
        )
    finally:
        if adapter is not None:
            try:
                await adapter.close()
            except Exception:
                pass

    if details:
        equity = _first_present_finite(details, ("total_equity", "USDT"), balance)
        wallet = _first_present_finite(
            details, ("total_wallet_balance", "USDT"), equity
        )
        available = _first_present_finite(
            details, ("available_balance", "USDT"), wallet
        )
        return "\n".join(
            [
                "💰 <b>БАЛАНС</b>",
                "━━━━━━━━━━━━━━━━━━",
                "",
                f"🏦 {exchange_title(exchange)}",
                "",
                *premium_arrow_lines(
                    (
                        ("💎 Equity", f"{equity:.4f} USDT"),
                        ("👛 Wallet", f"{wallet:.4f} USDT"),
                        ("✅ Доступно", f"{available:.4f} USDT"),
                    )
                ),
            ]
        )
    return "\n".join(
        [
            "💰 <b>БАЛАНС</b>",
            "━━━━━━━━━━━━━━━━━━",
            "",
            f"🏦 {exchange_title(exchange)}",
            "",
            *premium_arrow_lines((("💎 USDT", f"{balance:.4f}"),)),
        ]
    )


async def _handle_balance(
    message: Message, user_id: int | None = None, username: str | None = None
) -> None:
    uid = user_id or message.from_user.id
    uname = username if username is not None else message.from_user.username
    await message.answer(await _balance_text(uid, uname))


async def _handle_tp_confirmed(message: Message) -> None:
    """Manual escape hatch for ambiguous TP writes.

    If the exchange returned a transient error during TP creation/market-close,
    the bot deliberately does not auto-repeat the write to avoid duplicate TP.
    User can inspect the exchange and confirm that TP_i is already handled.
    """
    uid = message.from_user.id
    parts = (message.text or "").split()
    if len(parts) < 3:
        await message.answer(
            "Формат:\n"
            "<code>/tp_confirmed EXECUTION_ID TP_INDEX</code>\n\n"
            "Пример:\n<code>/tp_confirmed 5 2</code>\n\n"
            "Используй только если ты вручную проверил на бирже, что TP действительно стоит или нужная доля уже закрыта."
        )
        return
    try:
        execution_id = int(parts[1])
        tp_index = int(parts[2])
    except Exception:
        await message.answer(
            "❌ execution_id и tp_index должны быть числами. Пример: <code>/tp_confirmed 5 2</code>"
        )
        return
    if execution_id <= 0 or tp_index <= 0 or tp_index > 20:
        await message.answer(
            "❌ Некорректный execution_id или tp_index. TP должен быть от 1 до 20."
        )
        return

    row = await db.get_execution_by_id(execution_id)
    if not row:
        await message.answer(f"❌ Сделка #{execution_id} не найдена в журнале бота.")
        return
    owner_id = int(row.get("user_id") or 0)
    if owner_id != uid and not _is_admin(uid):
        await message.answer(
            "❌ Это не твоя сделка. Подтвердить TP может только владелец сделки или админ."
        )
        return

    status = str(row.get("status") or "")
    if status not in {"partial_error", "manual_required", "protected", "opened"}:
        await message.answer(
            f"ℹ️ Сделка #{execution_id} сейчас в статусе <b>{status}</b>. Подтверждение TP не требуется."
        )
        return

    try:
        targets = json.loads(row.get("targets_json") or "[]")
    except Exception:
        targets = []
    if targets and tp_index > len(targets):
        await message.answer(
            f"❌ В сделке #{execution_id} всего {len(targets)} TP. TP{tp_index} не существует."
        )
        return

    action = {
        "type": "manual_tp_confirmed",
        "tp_index": tp_index,
        "confirmed_by": uid,
        "confirmed_at": datetime.now(timezone.utc).isoformat(),
        "note": "User confirmed that TP is already placed/handled on exchange after ambiguous write.",
    }
    # Return manual_required rows back to partial_error so recovery can re-check the remaining TP.
    next_status = "partial_error" if status == "manual_required" else status
    ok = await db.update_execution_status_merge(
        execution_id,
        next_status,
        f"Manual TP{tp_index} confirmation by user {uid}",
        {"manual": [action]},
        expected_status=status,
    )
    if not ok:
        await message.answer(
            f"⚠️ Сделка #{execution_id} уже изменила статус (сейчас проверяется монитором). "
            "Подтверждение не применено — открой /status ещё раз и повтори при необходимости."
        )
        return
    await message.answer(
        f"✅ TP{tp_index} по сделке #{execution_id} отмечен как подтверждённый вручную.\n\n"
        "Если были другие недостающие TP, recovery-монитор продолжит проверку. "
        "Если все TP уже учтены, статус будет восстановлен автоматически."
    )


async def _diag_pair(message: Message, user_id: int, symbol: str) -> None:
    """Диагностика пары на BingX."""
    exchange = "bingx"
    title = exchange_title(exchange)
    lines = [f"🔬 <b>Диагностика {symbol} на {title}</b>", ""]

    api_row = await _cached_api_key(user_id, exchange)
    if not api_row:
        lines.append(f"❌ API-ключ {title} не подключён")
        lines.append("Подключи API через меню → BingX API.")
        await message.answer("\n".join(lines), parse_mode="HTML")
        return

    try:
        adapter = build_adapter(api_row)
    except Exception as exc:
        lines.append(
            f"❌ Не удалось создать адаптер {title}: "
            f"<code>{html.escape(str(exc)[:200])}</code>"
        )
        await message.answer("\n".join(lines), parse_mode="HTML")
        return

    try:
        lines.append("1️⃣ <b>Проверка существования пары...</b>")
        try:
            info = await adapter.instrument_info(symbol)
            if info and getattr(info, "symbol", None):
                lines.append(
                    f"   ✅ Пара найдена. tick={getattr(info, 'price_tick', '?')}, "
                    f"step={getattr(info, 'qty_step', '?')}, "
                    f"max_lev={getattr(info, 'max_leverage', '?')}"
                )
            else:
                lines.append("   ⚠️ Пара возвращена пустой структурой")
        except Exception as exc:
            lines.append(
                f"   ❌ Пара НЕ найдена на {title}: "
                f"<code>{html.escape(type(exc).__name__ +': '+str(exc)[:180])}</code>"
            )
            lines.append("")
            lines.append("Возможно, пара называется иначе или недоступна через API.")
            await message.answer("\n".join(lines), parse_mode="HTML")
            return

        lines.append("")
        lines.append("2️⃣ <b>Проверка авторизации API...</b>")
        try:
            balance = await adapter.fetch_balance_usdt()
            lines.append(f"   ✅ API авторизация работает. Баланс: {balance:.2f} USDT")
        except Exception as exc:
            lines.append(
                f"   ❌ API не авторизован: "
                f"<code>{html.escape(str(exc)[:200])}</code>"
            )
            lines.append("")
            lines.append("Пересоздай API-ключ и подключи его через меню → BingX API.")
            await message.answer("\n".join(lines), parse_mode="HTML")
            return

        lines.append("")
        lines.append("3️⃣ <b>Проверка списка разрешённых пар...</b>")
        try:
            allowed = await adapter.fetch_api_trading_symbols()
            if symbol in allowed:
                lines.append(
                    f"   ✅ {symbol} доступна через API ({len(allowed)} пар всего)"
                )
            else:
                lines.append(
                    f"   ⚠️ {symbol} отсутствует в API-списке ({len(allowed)} пар всего)"
                )
                similar = ", ".join(s for s in allowed if symbol[:4] in s)[:200]
                if similar:
                    lines.append("   Похожие имена: " + similar)
        except Exception as exc:
            lines.append(
                f"   ⚠️ Не удалось получить список: "
                f"<code>{html.escape(str(exc)[:150])}</code>"
            )

        lines.append("")
        lines.append("4️⃣ <b>Проверка торгового READ-доступа...</b>")
        try:
            positions = await adapter.fetch_open_positions(symbol)
            lines.append(
                f"   ✅ READ доступ работает (открытых позиций: {len(positions)})"
            )
            lines.append(
                "   <i>WRITE-доступ подтверждается только реальным ордером.</i>"
            )
        except Exception as exc:
            lines.append(
                f"   ⚠️ Ошибка чтения: "
                f"<code>{html.escape(str(exc)[:180])}</code>"
            )

        lines.append("")
        lines.append("📋 <b>Итог:</b>")
        lines.append(
            "Если авторизация и READ-доступ прошли, подключение работает. "
            "Для автоторговли у ключа также должно быть разрешение на размещение ордеров."
        )
    finally:
        try:
            await adapter.close()
        except Exception:
            pass

    await message.answer("\n".join(lines), parse_mode="HTML")


async def _reconcile_pending_limits_for_menu(user_id: int, *, passes: int = 3) -> int:
    """Bounded safe refresh for the active LIMIT menu.

    This is read/safety reconciliation, not a broad cancel.  It lets old
    identityless BingX phantom LIMIT rows disappear from the menu after the
    exchange proves that no position and no plausible open entry order remain.
    """

    processed = 0
    try:
        from app.services.limit_tp_catchup import process_pending_limit_tp_catchup_once

        for _attempt in range(max(1, int(passes or 1))):
            rows = await db.pending_limit_executions_for_user(int(user_id), limit=100)
            if not rows:
                break
            processed += int(
                await process_pending_limit_tp_catchup_once(
                    notify=None,
                    rows_override=rows,
                )
                or 0
            )
    except Exception:
        log.exception("active LIMIT menu reconciliation failed uid=%s", user_id)
    return processed


async def _active_unlock_rows_summary(user_id: int, *, limit: int = 6) -> list[str]:
    """Return compact active execution labels for a safe unlock refusal card."""
    try:
        rows = await db.active_position_executions_for_user(int(user_id), limit=limit)
    except Exception:
        log.exception("failed to read active rows for unlock summary uid=%s", user_id)
        return []
    lines: list[str] = []
    for row in rows[:limit]:
        symbol = str(row.get("symbol") or "?").upper()
        side = str(row.get("side") or "?").upper()
        status = str(row.get("status") or "?")
        execution_id = int(row.get("id") or 0)
        lines.append(f"• #{execution_id} {symbol} {side} - {status}")
    return lines


async def _reconcile_before_unlock(user_id: int) -> dict[str, int | str]:
    """Run a bounded live reconciliation pass before deciding whether unlock is safe.

    ``разблок`` is often used after a live LIMIT bug/redeploy. Refusing solely
    because stale DB rows exist leaves the user stuck even when BingX already
    has no matching position/order. The command still must not hide real risk in
    the database, so it first lets the normal safety workers re-check exact
    BingX state and only then clears dedup when active exposure reaches zero.
    """
    result: dict[str, int | str] = {
        "pending_rows_seen": 0,
        "pending_processed": 0,
        "position_rows_seen": 0,
        "position_processed": 0,
        "errors": 0,
    }
    try:
        from app.services.limit_tp_catchup import process_pending_limit_tp_catchup_once
        from app.services.position_lifecycle_guard import process_position_lifecycle_guard_once

        # Three passes intentionally match the existing phantom-LIMIT threshold:
        # one Telegram command can now clear a LIMIT that is absent on BingX and
        # has no position after three exact confirmations, instead of requiring
        # the user to wait for background monitor cycles.
        for _attempt in range(3):
            pending_rows = await db.pending_limit_executions_for_user(int(user_id), limit=100)
            result["pending_rows_seen"] = max(
                int(result.get("pending_rows_seen") or 0), len(pending_rows)
            )
            if pending_rows:
                result["pending_processed"] = int(result.get("pending_processed") or 0) + int(
                    await process_pending_limit_tp_catchup_once(
                        notify=None,
                        rows_override=pending_rows,
                    )
                    or 0
                )

            active_rows = await db.active_position_executions_for_user(int(user_id), limit=100)
            position_rows = [
                row
                for row in active_rows
                if str(row.get("status") or "") != "pending_limit"
            ]
            result["position_rows_seen"] = max(
                int(result.get("position_rows_seen") or 0), len(position_rows)
            )
            if position_rows:
                result["position_processed"] = int(result.get("position_processed") or 0) + int(
                    await process_position_lifecycle_guard_once(
                        notify=None,
                        rows_override=position_rows,
                    )
                    or 0
                )

            state = await db.user_risk_state(int(user_id))
            if int(state.get("active_total_count") or 0) <= 0:
                break
            if not pending_rows and not position_rows:
                break
            await asyncio.sleep(0.05)
    except Exception as exc:
        result["errors"] = int(result.get("errors") or 0) + 1
        result["last_error"] = f"{type(exc).__name__}: {exc}"[:240]
        log.exception("pre-unlock reconciliation failed uid=%s", user_id)
    return result


async def _handle_vip_command(
    message: Message, text: str, user_id: int | None = None, username: str | None = None
) -> bool:
    low = text.lower().strip()
    uid = user_id or message.from_user.id
    uname = username if username is not None else message.from_user.username
    await db.ensure_user(uid, uname, _is_admin(uid))

    if low in {"vip", "вип", "vip статус", "вип статус", "vip status"}:
        await _show_status(message, uid)
        return True

        # Диагностика пары: "проверь BTCUSDT" / "check BTCUSDT" / "тест AGLDUSDT"
    m_diag = re.match(r"^(?:проверь|проверить|check|тест|test)\s+([a-zA-Z0-9]+)$", low)
    if m_diag:
        target_symbol = m_diag.group(1).upper()
        if not target_symbol.endswith("USDT"):
            target_symbol = target_symbol + "USDT"
        await _diag_pair(message, uid, target_symbol)
        return True

        # Whitelist commands — text aliases for admin only.
        # Examples:
        #   добавить 123456789       → /whitelist_add
        #   удалить 987654321        → /whitelist_remove
        #   показать всех            → /whitelist_list
        #   показать whitelist       → /whitelist_list
        # «добавить 123» — показать подтверждение доступа BingX
        # «добавить 123 bingx» — сразу дать доступ BingX
    m_wl_add = re.match(r"^(?:добавить|add)\s+([0-9]+)(?:\s+(bingx|all|все))?$", low)
    if m_wl_add:
        if not _is_admin(uid):
            await message.answer("⛔ Только админ может управлять white-list.")
            return True
        target_uid = int(m_wl_add.group(1))
        ex_choice = m_wl_add.group(2)
        await db.ensure_user(target_uid)
        if ex_choice is None:
            # Показать кнопку подтверждения доступа BingX
            from app.bot.keyboards import whitelist_add_exchange_picker

            picker = whitelist_add_exchange_picker(target_uid, owner_id=uid)
            if picker is None:
                # Fallback на всё если клавиатура недоступна
                await db.add_user_whitelist_exchange(target_uid, "all")
                await message.answer(
                    f"✅ Юзер <code>{target_uid}</code> добавлен в white-list (BingX).",
                    parse_mode="HTML",
                )
            else:
                await message.answer(
                    f"➕ Добавление в white-list юзера <code>{target_uid}</code>\n\n"
                    "Разрешить ему автоторговлю на BingX?",
                    parse_mode="HTML",
                    reply_markup=picker,
                )
            return True
        ex_code = "all" if ex_choice in ("all", "все") else ex_choice
        if ex_code != "all" and not get_settings().is_exchange_enabled(ex_code):
            await message.answer(f"❌ {ex_code.upper()} отключена в ENV")
            return True
        new_set = await db.add_user_whitelist_exchange(target_uid, ex_code)
        if "all" in new_set:
            grant_text = "BingX"
        else:
            grant_text = ", ".join(e.upper() for e in sorted(new_set))
        await message.answer(
            f"✅ Юзер <code>{target_uid}</code> добавлен в white-list.\n"
            f"Разрешено: <b>{grant_text}</b>",
            parse_mode="HTML",
        )
        return True

    m_wl_rm = re.match(
        r"^(?:удалить|remove|убрать)\s+([0-9]+)(?:\s+(bingx|all|все))?$", low
    )
    if m_wl_rm:
        if not _is_admin(uid):
            await message.answer("⛔ Только админ может управлять white-list.")
            return True
        target_uid = int(m_wl_rm.group(1))
        ex_choice = m_wl_rm.group(2)
        current = await db.get_user_whitelist_exchanges(target_uid)
        if not current:
            await message.answer(
                f"ℹ️ Юзер <code>{target_uid}</code> и так не в white-list.",
                parse_mode="HTML",
            )
            return True
        if ex_choice is None:
            from app.bot.keyboards import whitelist_remove_exchange_picker

            picker = whitelist_remove_exchange_picker(target_uid, current, owner_id=uid)
            if picker is None:
                await db.remove_user_whitelist_exchange(target_uid, "all")
                await message.answer(
                    f"🚫 Юзер <code>{target_uid}</code> полностью удалён из white-list.",
                    parse_mode="HTML",
                )
            else:
                cur_text = "BingX"
                await message.answer(
                    f"➖ Убрать из white-list юзера <code>{target_uid}</code>\n\n"
                    f"Сейчас разрешено: <b>{cur_text}</b>\n\n"
                    "Убрать доступ к автоторговле BingX?",
                    parse_mode="HTML",
                    reply_markup=picker,
                )
            return True
        ex_code = "all" if ex_choice in ("all", "все") else ex_choice
        new_set = await db.remove_user_whitelist_exchange(target_uid, ex_code)
        if not new_set:
            await message.answer(
                f"🚫 Юзер <code>{target_uid}</code> полностью удалён из white-list.\n"
                "Теперь только наблюдение (preview-only).",
                parse_mode="HTML",
            )
        else:
            remaining = ", ".join(e.upper() for e in sorted(new_set))
            await message.answer(
                f"➖ Убрали {ex_code.upper()}. Юзер <code>{target_uid}</code> остался "
                f"в white-list для: <b>{remaining}</b>",
                parse_mode="HTML",
            )
        return True

    if low in {
        "показать всех",
        "показать whitelist",
        "показать вайтлист",
        "список",
        "list",
        "list whitelist",
        "show whitelist",
    }:
        if not _is_admin(uid):
            await message.answer("⛔ Только админ.")
            return True
        users = await db.list_whitelisted_users()
        if not users:
            await message.answer(
                "📋 White-list пуст.\n\n"
                "Никто из юзеров не может открывать сделки сейчас.\n"
                "Добавь юзера через <code>добавить &lt;user_id&gt;</code> "
                "или <code>/whitelist_add &lt;user_id&gt;</code>",
                parse_mode="HTML",
            )
            return True
        lines = [f"📋 <b>White-list ({len(users)} юзеров):</b>", ""]
        for wl_uid in users:
            lines.append(f"  • <code>{wl_uid}</code>")
        lines.append("")
        lines.append("<i>Управление:</i>")
        lines.append("<code>добавить &lt;user_id&gt;</code>")
        lines.append("<code>удалить &lt;user_id&gt;</code>")
        await message.answer("\n".join(lines), parse_mode="HTML")
        return True
    m = re.match(r"^(?:vip|вип)\s+(?:биржа|exchange)\s+(bingx)$", low)
    if m:
        ex = m.group(1).lower()
        settings = get_settings()
        if not settings.is_exchange_enabled(ex):
            await message.answer(
                f"❌ Биржа {ex.upper()} отключена в ENV и недоступна"
            )
            return True
        api_row = await _cached_api_key(uid, ex)
        if not api_row:
            fmt = "/api bingx BingX_API_KEY BingX_API_SECRET"
            await message.answer(
                f"⚠️ API для {ex.upper()} ещё не подключён.\n"
                "Активную биржу не меняю, чтобы не сломать авто-режим.\n\n"
                f"Подключи API:\n<code>{fmt}</code>"
            )
            return True
        await db.set_user_setting(uid, "exchange", ex)
        # Tell user if they don't have whitelist for this exchange yet.
        wl_ok = await db.is_user_whitelisted(uid, ex)
        if wl_ok:
            await message.answer(f"✅ VIP биржа выбрана: {ex.upper()}")
        else:
            await message.answer(
                f"✅ VIP биржа выбрана: {ex.upper()}\n\n"
                f"⚠️ <b>Внимание:</b> у тебя нет white-list разрешения для {ex.upper()}.\n"
                "Бот будет показывать сигналы в preview-режиме, но реальные "
                "сделки на этой бирже открываться НЕ будут.\n\n"
                "Чтобы торговать — попроси админа добавить разрешение через "
                f"<code>/whitelist_add {uid} {ex}</code>",
                parse_mode="HTML",
            )
        return True

    if low in {"vip авто", "вип авто", "vip auto"}:
        await db.set_user_setting(uid, "mode", UserMode.AUTO.value)
        await message.answer(system_mode_message("auto"))
        return True
    if low in {"vip просмотр", "вип просмотр", "vip preview"}:
        await db.set_user_setting(uid, "mode", UserMode.PREVIEW.value)
        await message.answer(system_mode_message("preview"))
        return True
    if low in {"vip выкл", "вип выкл", "vip off"}:
        await db.set_user_setting(uid, "mode", UserMode.OFF.value)
        await message.answer(system_mode_message("off"))
        return True

    m = re.match(r"^(?:vip|вип)\s+риск\s+([0-9]+(?:[\.,][0-9]+)?)$", low)
    if m:
        val = float(m.group(1).replace(",", "."))
        try:
            validate_risk_percent(val)
        except ValueError as exc:
            await message.answer(f"❌ {exc}\nПример: vip риск 1")
            return True
        await db.set_user_setting(uid, "risk_per_trade_percent", val)
        await message.answer(
            system_risk_message(
                "Риск на сделку",
                f"{val}%",
                [
                    "🧾 Комиссия учитывается при расчёте объёма",
                    f"🛡 Плановый убыток при STOP: не более {val}%",
                ],
            )
        )
        return True

        # Короткая команда без префикса "vip": "риск 2" или "risk 2"
    m_short = re.match(r"^(?:риск|risk)\s+([0-9]+(?:[\.,][0-9]+)?)$", low)
    if m_short:
        val = float(m_short.group(1).replace(",", "."))
        try:
            validate_risk_percent(val)
        except ValueError as exc:
            await message.answer(f"❌ {exc}\nПример: риск 1.5")
            return True
        await db.set_user_setting(uid, "risk_per_trade_percent", val)
        await message.answer(
            system_risk_message(
                "Риск на сделку",
                f"{val}%",
                [
                    "🧾 Комиссия учитывается при расчёте объёма",
                    f"🛡 Плановый убыток при STOP: не более {val}%",
                ],
            )
        )
        return True

    for key, db_key, label, validator in [
        (
            "дневной риск",
            "daily_risk_limit_percent",
            "Дневной VIP-риск",
            validate_daily_risk_limit_percent,
        ),
        (
            "дневной лимит",
            "daily_risk_limit_percent",
            "Дневной VIP-риск",
            validate_daily_risk_limit_percent,
        ),
        (
            "макс сделки",
            "max_open_trades",
            "Лимит открытых VIP-сделок",
            validate_max_open_trades,
        ),
        (
            "открытые",
            "max_open_trades",
            "Лимит открытых VIP-сделок",
            validate_max_open_trades,
        ),
        (
            "портфель риск",
            "max_portfolio_risk_percent",
            "Портфельный VIP-риск",
            validate_max_portfolio_risk_percent,
        ),
    ]:
        m = re.match(rf"^(?:vip|вип)\s+{key}\s+([0-9]+(?:[\.,][0-9]+)?)$", low)
        if m:
            raw_value = m.group(1).replace(",", ".")
            try:
                val = validator(raw_value)
            except ValueError as exc:
                await message.answer(f"❌ {exc}\nПример: vip {key} 10")
                return True
            await db.set_user_setting(uid, db_key, val)
            await message.answer(system_risk_message(label, val))
            return True

    m = re.match(r"^(?:vip|вип)\s+(?:tp|тп)\s+(.+)$", low)
    if m:
        raw = m.group(1).strip()
        if raw in {"все", "всё", "all"}:
            val = "all"
        else:
            if not raw.isdigit():
                await message.answer(
                    "❌ TP лимит должен быть числом от 1 до 20 или словом: все"
                )
                return True
            n = max(1, min(int(raw), 20))
            val = str(n)
        await db.set_user_setting(uid, "tp_limit", val)
        await message.answer(
            system_tp_message(
                "Лимит целей", "все TP" if val == "all" else f"первые {val} TP"
            )
        )
        return True

    if low in {
        "vip фиксация умная",
        "vip фиксация от малого до великого",
        "vip фиксация smart",
        "vip фиксация лесенка",
        "vip растейкивание умная",
        "вип фиксация умная",
    }:
        await db.set_user_setting(uid, "tp_mode", TpMode.SMART.value)
        await message.answer(system_tp_message("Режим фиксации", "🧠 умная схема"))
        return True
    if low in {
        "vip фиксация ранняя",
        "vip ранняя фиксация",
        "vip фиксация early fixation",
        "vip фиксация early_fixation",
        "вип ранняя фиксация",
    }:
        await db.set_user_setting(uid, "tp_mode", TpMode.EARLY_FIXATION.value)
        await db.set_user_setting(uid, "be_trigger_tp_index", 1)
        await db.set_user_setting(uid, "be_after_tp1_enabled", 1)
        await message.answer(
            system_tp_message(
                "Режим фиксации",
                "🛡️ Ранняя фиксация",
                [
                    "TP1 70% • TP2 15% • TP3 10% • TP4 5%",
                    "После TP1 STOP переносится в Б/У",
                    "Используется максимум 4 цели",
                    "Применяется только к новым сделкам",
                ],
            )
        )
        return True
    if low in {
        "vip фиксация разгон",
        "vip разгон",
        "vip фиксация acceleration",
        "вип фиксация разгон",
        "вип разгон",
    }:
        await db.set_user_setting(uid, "tp_mode", TpMode.ACCELERATION.value)
        await message.answer(
            system_tp_message(
                "Режим фиксации",
                "🚀 Разгон",
                [
                    "TP1 10% • TP2 65% • TP3 20% • TP4 5%",
                    "Используется максимум 4 цели",
                    "Применяется только к новым сделкам",
                ],
            )
        )
        return True
    if low in {
        "vip фиксация долями",
        "vip фиксация равными",
        "vip фиксация equal",
        "вип фиксация долями",
    }:
        await db.set_user_setting(uid, "tp_mode", TpMode.EQUAL.value)
        await message.answer(system_tp_message("Режим фиксации", "⚖️ равными долями"))
        return True

    if low in {
        "vip тп сигнал вкл",
        "vip tp signal on",
        "vip tp percents on",
        "vip проценты сигнала вкл",
        "вип тп сигнал вкл",
    }:
        await db.set_user_setting(uid, "use_signal_tp_percents", 1)
        await message.answer(
            system_tp_message(
                "Проценты из сигнала",
                "включены",
                [
                    "✅ Используются при сумме процентов 100%",
                    "🔄 Иначе применяется выбранная схема",
                ],
            )
        )
        return True
    if low in {
        "vip тп сигнал выкл",
        "vip tp signal o",
        "vip tp percents o",
        "vip проценты сигнала выкл",
        "вип тп сигнал выкл",
    }:
        await db.set_user_setting(uid, "use_signal_tp_percents", 0)
        await message.answer(
            system_tp_message(
                "Проценты из сигнала",
                "выключены",
                ["🎯 Будет использоваться выбранная схема фиксации"],
            )
        )
        return True

    if low in {
        "vip бу выкл",
        "вип бу выкл",
        "vip be of",
        "vip бу off",
        "vip безубыток выкл",
    }:
        _invalidate_be_apply_tokens_for_user(uid)
        await db.set_user_setting(uid, "be_trigger_tp_index", 0)
        await db.set_user_setting(uid, "be_after_tp1_enabled", 0)
        await message.answer(
            system_be_message(0)
            + "\n\nℹ️ Для уже существующих сделок открой «Меню → Б/У» и нажми "
            "«♻️ Применить к текущим сделкам»."
        )
        return True

    m = re.match(
        r"^(?:vip|вип)\s+(?:бу|be|безубыток)\s+(?:после|after)\s*(?:tp|тп)?\s*([123])$",
        low,
    )
    if m:
        trigger = int(m.group(1))
        _invalidate_be_apply_tokens_for_user(uid)
        await db.set_user_setting(uid, "be_trigger_tp_index", trigger)
        await db.set_user_setting(uid, "be_after_tp1_enabled", 1)
        await message.answer(
            system_be_message(trigger)
            + "\n\nℹ️ Настройка действует на новые сделки. Для уже существующих "
            "открой «Меню → Б/У» и нажми «♻️ Применить к текущим сделкам»."
        )
        return True

    if re.match(
        r"^(?:vip|вип)\s+(?:бу|be|безубыток)\s+(?:после|after)\s*(?:tp|тп)?\s*[45]$",
        low,
    ):
        await message.answer(
            "❌ БУ после TP4 и TP5 удалено. Доступны только TP1, TP2, TP3 или БУ выкл."
        )
        return True

    if low in {
        "vip риск режим бу10",
        "вип риск режим бу10",
        "vip risk be10",
        "vip 10 сделок",
        "вип 10 сделок",
    }:
        await db.set_user_setting(uid, "risk_per_trade_percent", 1.0)
        await db.set_user_setting(uid, "max_open_trades", 10)
        await db.set_user_setting(uid, "max_portfolio_risk_percent", 10.0)
        await db.set_user_setting(uid, "daily_risk_limit_percent", 10.0)
        await db.set_user_setting(uid, "exclude_be_trades_from_risk", 1)
        await message.answer(
            system_risk_message(
                "Профиль",
                "10 сделок по 1%",
                [
                    "📦 Максимум риск-активных сделок: 10",
                    "📊 Портфельный риск: 10%",
                    "📅 Дневной риск: 10%",
                    "🛡 Подтверждённое Б/У освобождает риск-слот",
                ],
            )
        )
        return True

    if low in {
        "vip бу риск вкл",
        "вип бу риск вкл",
        "vip be risk on",
        "vip exclude be risk on",
    }:
        await db.set_user_setting(uid, "exclude_be_trades_from_risk", 1)
        await message.answer(
            system_risk_message(
                "Сделки в Б/У",
                "не считаются активным риском",
                ["🛡 Подтверждённое Б/У освобождает риск-слот"],
            )
        )
        return True

    if low in {
        "vip бу риск выкл",
        "вип бу риск выкл",
        "vip be risk off",
        "vip exclude be risk off",
    }:
        await db.set_user_setting(uid, "exclude_be_trades_from_risk", 0)
        await message.answer(
            system_risk_message(
                "Сделки в Б/У",
                "считаются активным риском",
                ["📦 Риск-слот остаётся занятым до полного закрытия"],
            )
        )
        return True

    if low in {
        "vip сброс дубликатов",
        "вип сброс дубликатов",
        "vip reset dedup",
        "vip clear dedup",
        "vip очистить дубликаты",
    }:
        if not _is_admin(uid):
            return True
        await db.clear_dedup()
        await message.answer(
            "✅ VIP-дубликаты сброшены. Можно повторно тестировать сигналы."
        )
        return True

    if low in {
        "разблок",
        "razblok",
        "vip разблок",
        "vip razblok",
        "разблокировать",
        "сброс лимитов",
        "vip сброс лимитов",
    }:
        # Safety: never hide real exchange exposure only in the database.
        # Before refusing, run bounded exact reconciliation so stale/fantom
        # pending rows from a redeploy or earlier BingX mismatch can be cleared
        # by the same safety workers that normally run in the background.
        state = await db.user_risk_state(uid)
        active_total = int(state.get("active_total_count") or 0)
        reconcile: dict[str, int | str] = {}
        if active_total > 0:
            reconcile = await _reconcile_before_unlock(uid)
            state = await db.user_risk_state(uid)
            active_total = int(state.get("active_total_count") or 0)

        if active_total > 0:
            summary_lines = await _active_unlock_rows_summary(uid)
            sync_lines = [
                "♻️ Перед отказом выполнена безопасная синхронизация с BingX:",
                f"├ LIMIT записей проверено: <b>{int(reconcile.get('pending_rows_seen') or 0)}</b>",
                f"├ LIMIT действий/очисток: <b>{int(reconcile.get('pending_processed') or 0)}</b>",
                f"├ Позиционных записей проверено: <b>{int(reconcile.get('position_rows_seen') or 0)}</b>",
                f"└ Позиционных действий/очисток: <b>{int(reconcile.get('position_processed') or 0)}</b>",
            ]
            if reconcile.get("errors"):
                sync_lines.append(
                    f"⚠️ Ошибка синхронизации: <code>{html.escape(str(reconcile.get('last_error') or 'unknown'))}</code>"
                )
            if summary_lines:
                sync_lines.append("")
                sync_lines.append("📌 Что всё ещё считается активным:")
                sync_lines.extend(html.escape(line) for line in summary_lines)
            await message.answer(
                "🚫 <b>РАЗБЛОКИРОВКА ОСТАНОВЛЕНА</b>\n"
                "━━━━━━━━━━━━━━━━━━━━\n"
                f"📂 Активных записей сделки: <b>{active_total}</b>\n\n"
                + "\n".join(sync_lines)
                + "\n\n"
                "Бот не будет скрывать их только в базе, пока на BingX могут "
                "оставаться позиции, STOP или TP. Если это фантомные LIMIT, "
                "повторите «разблок» после этой синхронизации или проверьте раздел «📂 Позиции»."
            )
            return True

        cleared = await db.clear_user_dedup(uid)
        try:
            from app.services.signal_executor import (
                _OPENING_REGISTRY,
                _OPENING_REGISTRY_LOCK,
            )

            with _OPENING_REGISTRY_LOCK:
                to_remove = {key for key in _OPENING_REGISTRY if key[0] == uid}
                _OPENING_REGISTRY -= to_remove
        except Exception:
            log.exception("failed to clear opening registry for uid=%s", uid)

        await message.answer(
            "🔓 <b>РАЗБЛОКИРОВКА ВЫПОЛНЕНА</b>\n"
            "━━━━━━━━━━━━━━━━━━━━\n"
            "✅ Активных сделок по данным бота нет\n"
            f"♻️ Удалено дубликатов пользователя: <b>{cleared}</b>\n\n"
            "Новые сигналы снова могут обрабатываться."
        )
        return True

    return False


WELCOME_TEXT = (
    "🤖 <b>Добро пожаловать в BingX WinBot</b>\n"
    "━━━━━━━━━━━━━━━━━━━━\n\n"
    "🛡 <b>Автоматическая торговля с контролем риска и защитой позиции</b>\n\n"
    "Бот сопровождает сделку от получения сигнала до полного закрытия:\n\n"
    "├ 🚀 открывает MARKET и LIMIT-позиции\n"
    "├ 📊 рассчитывает объём по заданному риску\n"
    "├ 🛡 устанавливает защитный STOP\n"
    "├ 🎯 распределяет TAKE PROFIT\n"
    "├ 🔒 переносит STOP в безубыток\n"
    "└ 🔔 уведомляет о каждом важном событии\n\n"
    "<b>Для начала работы:</b>\n\n"
    "1️⃣ Подключите API BingX\n"
    "2️⃣ Выберите режим торговли\n"
    "3️⃣ Настройте риск, TP и Б/У\n"
    "4️⃣ Проверьте статус аккаунта\n\n"
    "🔐 API-ключи сохраняются в зашифрованном виде.\n"
    "🚫 Разрешение на вывод средств не требуется.\n\n"
    "⚠️ Торговля фьючерсами связана с риском. Перед включением реальной "
    "торговли внимательно проверьте настройки.\n\n"
    "Нажмите кнопку ниже, чтобы открыть главное меню 👇"
)


async def _send_start_welcome(message: Message, user_id: int) -> None:
    """Send branded onboarding without making /start depend on the image file."""
    api_connected = bool(await _cached_api_key(user_id, "bingx"))
    keyboard = start_welcome_menu(api_connected=api_connected, owner_id=user_id)
    logo_path = Path(__file__).resolve().parents[2] / "assets" / "bingx_winbot_logo.png"

    if logo_path.is_file():
        try:
            logo_bytes = await asyncio.to_thread(logo_path.read_bytes)
            await message.answer_photo(
                BufferedInputFile(logo_bytes, filename="bingx_winbot_logo.png"),
                caption=WELCOME_TEXT,
                parse_mode="HTML",
                reply_markup=keyboard,
            )
            return
        except Exception as exc:
            log.warning("start welcome photo failed uid=%s: %s", user_id, exc)

    await message.answer(WELCOME_TEXT, parse_mode="HTML", reply_markup=keyboard)


if router:

    @router.message(Command("start"))
    async def cmd_start(message: Message):
        user_id = int(message.from_user.id)
        is_private = getattr(message.chat, "type", "") == "private"
        if is_private:
            mark_private_chat_ready(user_id)
        await db.ensure_user(user_id, message.from_user.username, _is_admin(user_id))

        if is_private:
            await _send_start_welcome(message, user_id)
            return

            # Keep group chats compact: the full onboarding and logo belong in DM.
        await message.answer(
            "🤖 <b>BingX WinBot</b>\n\n"
            "Для подключения API и получения личных уведомлений откройте бота "
            "в ЛС и отправьте /start.",
            parse_mode="HTML",
        )

    @router.message(Command("help"))
    async def cmd_help(message: Message):
        await message.answer(_help_text())

    @router.message(Command("terms"))
    async def cmd_terms(message: Message):
        await _send_terms_prompt(message)

    @router.message(Command("id"))
    async def cmd_id(message: Message):
        uid = message.from_user.id
        grants = await db.get_user_whitelist_exchanges(uid)
        if not grants:
            status = "👁 Preview-only (наблюдение)"
            details = (
                "Сейчас бот только показывает сигналы — реальные сделки "
                "не открываются. Отправь свой ID админу, чтобы он добавил "
                "тебя в white-list."
            )
        elif "all" in grants:
            status = "✅ White-list: <b>BingX</b>"
            details = "Бот может открывать сделки на BingX."
        else:
            ex_list = ", ".join(e.upper() for e in sorted(grants))
            status = f"✅ White-list: <b>{html.escape(ex_list)}</b>"
            details = f"Бот может открывать сделки на BingX. Разрешение: {html.escape(ex_list)}."
        admin_status = (
            "👑 Администратор: <b>да</b>"
            if _is_admin(uid)
            else "👤 Администратор: <b>нет</b>"
        )
        await message.answer(
            f"🆔 Твой ID: <code>{uid}</code>\n\n"
            f"{admin_status}\n"
            f"Статус: {status}\n\n"
            f"{details}",
            parse_mode="HTML",
        )

    @router.message(Command("api"))
    async def cmd_api(message: Message):
        settings = get_settings()
        parts = (message.text or "").split()
        if not await _terms_gate_ok(message):
            return

            # Supported forms:
            # /api bingx API_KEY API_SECRET
            # /api API_KEY API_SECRET
        if len(parts) >= 4 and parts[1].lower() == "bingx":
            api_key, api_secret = parts[2], parts[3]
        elif len(parts) >= 3:
            api_key, api_secret = parts[1], parts[2]
        else:
            await message.answer(
                "Формат для BingX:\n"
                "<code>/api bingx BingX_API_KEY BingX_API_SECRET</code>\n\n"
                "Безопаснее использовать кнопку «Подключить BingX API (по шагам)»."
            )
            return

        try:
            await message.delete()
        except Exception:
            pass

        api_key_enc = encrypt_text(api_key, settings.ENCRYPTION_KEY)
        api_secret_enc = encrypt_text(api_secret, settings.ENCRYPTION_KEY)
        verifying = await message.answer("⏳ Проверяю API на BingX…")
        adapter = None
        try:
            adapter = build_adapter(
                {
                    "exchange": "bingx",
                    "api_key_encrypted": api_key_enc,
                    "api_secret_encrypted": api_secret_enc,
                    "passphrase_encrypted": None,
                    "testnet": settings.BINGX_VST,
                }
            )
            ok = await adapter.verify_api()
        except Exception as exc:
            log.warning(
                "Direct BingX API verification rejected uid=%s: %s",
                message.from_user.id,
                type(exc).__name__,
            )
            try:
                await verifying.delete()
            except Exception:
                pass
            await message.answer(system_api_error_message(exc))
            return
        finally:
            if adapter is not None:
                try:
                    await adapter.close()
                except Exception:
                    pass

        try:
            await verifying.delete()
        except Exception:
            pass
        if not ok:
            await message.answer(system_api_error_message(rejected=True))
            return

        await db.save_api_key(
            message.from_user.id,
            api_key_enc,
            api_secret_enc,
            None,
            exchange="bingx",
            testnet=settings.BINGX_VST,
        )
        await db.set_user_setting(message.from_user.id, "exchange", "bingx")
        await message.answer(system_api_connected_message())

    @router.message(Command("balance"))
    async def cmd_balance(message: Message):
        await _handle_balance(message)

    @router.message(Command("positions"))
    async def cmd_positions(message: Message):
        text, rows = await _positions_view(
            message.from_user.id, message.from_user.username
        )
        await message.answer(
            text,
            reply_markup=positions_list_menu(rows, owner_id=message.from_user.id),
        )

    @router.message(Command("memory"))
    async def cmd_memory(message: Message):
        await _handle_memory_status(message)

    @router.message(Command("stats"))
    async def cmd_signal_analytics_stats(message: Message):
        user_id = _sender_id(message)
        await message.answer(
            await _signal_analytics_text(user_id),
            reply_markup=(
                signal_analytics_admin_menu(owner_id=user_id)
                if _is_admin(user_id)
                else _home_menu(user_id)
            ),
        )

    @router.message(Command("stats_export"))
    async def cmd_signal_analytics_export(message: Message):
        try:
            period_id = _stats_period_argument(message, "stats_export")
        except ValueError as exc:
            await message.answer(str(exc))
            return
        await _send_signal_analytics_export(
            message,
            _sender_id(message),
            period_id=period_id,
        )

    @router.message(Command("stats_periods"))
    async def cmd_statistics_periods(message: Message):
        user_id = _sender_id(message)
        if not _is_admin(user_id):
            await message.answer("⛔ Раздел доступен только администратору.")
            return
        if not bool(get_settings().STATS_V2_REPORTS_ENABLED):
            await message.answer("⏸ STATS_V2_REPORTS_ENABLED=false.")
            return
        try:
            text = await format_statistics_periods_report(user_id=user_id)
        except Exception as exc:
            log.exception("STATISTICS_PERIODS_REPORT_FAILED error=%s", type(exc).__name__)
            text = "⚠️ Не удалось загрузить список периодов."
        await message.answer(
            text,
            reply_markup=signal_analytics_admin_menu(owner_id=user_id),
        )

    @router.message(Command("stats_all"))
    async def cmd_statistics_all(message: Message):
        user_id = _sender_id(message)
        if not _is_admin(user_id):
            await message.answer("⛔ Раздел доступен только администратору.")
            return
        if not bool(get_settings().STATS_V2_REPORTS_ENABLED):
            await message.answer("⏸ STATS_V2_REPORTS_ENABLED=false.")
            return
        try:
            text = await format_statistics_all_report(user_id=user_id)
        except Exception as exc:
            log.exception("STATISTICS_ALL_REPORT_FAILED error=%s", type(exc).__name__)
            text = "⚠️ Не удалось сформировать сводку всех периодов."
        await message.answer(
            text,
            reply_markup=signal_analytics_admin_menu(owner_id=user_id),
        )

    @router.message(Command("stats_period"))
    async def cmd_statistics_period(message: Message):
        user_id = _sender_id(message)
        if not _is_admin(user_id):
            await message.answer("⛔ Раздел доступен только администратору.")
            return
        try:
            period_id = _stats_period_argument(message, "stats_period")
        except ValueError as exc:
            await message.answer(str(exc))
            return
        if period_id is None:
            await message.answer("Использование: <code>/stats_period ID</code>")
            return
        if not bool(get_settings().STATS_V2_REPORTS_ENABLED):
            await message.answer("⏸ STATS_V2_REPORTS_ENABLED=false.")
            return
        try:
            text = await format_statistics_period_report(period_id, user_id=user_id)
        except LookupError:
            text = "⚠️ Период не найден."
        except Exception as exc:
            log.exception("STATISTICS_PERIOD_REPORT_FAILED period_id=%s error=%s", period_id, type(exc).__name__)
            text = "⚠️ Не удалось сформировать отчёт периода."
        await message.answer(
            text,
            reply_markup=signal_analytics_admin_menu(owner_id=user_id),
        )

    @router.message(Command("stats_tech"))
    async def cmd_statistics_technical(message: Message):
        user_id = _sender_id(message)
        if not _is_admin(user_id):
            await message.answer("⛔ Раздел доступен только администратору.")
            return
        try:
            period_id = _stats_period_argument(message, "stats_tech")
        except ValueError as exc:
            await message.answer(str(exc))
            return
        if not bool(get_settings().STATS_V2_REPORTS_ENABLED):
            await message.answer("⏸ STATS_V2_REPORTS_ENABLED=false.")
            return
        try:
            text = await format_statistics_technical_report(period_id, user_id=user_id)
        except LookupError:
            text = "⚠️ Период не найден."
        except Exception as exc:
            log.exception(
                "STATISTICS_TECHNICAL_REPORT_FAILED period_id=%s error=%s",
                period_id,
                type(exc).__name__,
            )
            text = "⚠️ Не удалось сформировать технический отчёт."
        await message.answer(
            text,
            reply_markup=statistics_technical_admin_menu(owner_id=user_id),
        )

    @router.message(Command("stats_quality"))
    async def cmd_statistics_quality(message: Message):
        user_id = _sender_id(message)
        if not _is_admin(user_id):
            await message.answer("⛔ Раздел доступен только администратору.")
            return
        try:
            period_id = _stats_period_argument(message, "stats_quality")
        except ValueError as exc:
            await message.answer(str(exc))
            return
        if not bool(get_settings().STATS_V2_REPORTS_ENABLED):
            await message.answer("⏸ STATS_V2_REPORTS_ENABLED=false.")
            return
        try:
            text = await format_statistics_quality_report(period_id, user_id=user_id)
        except LookupError:
            text = "⚠️ Период не найден."
        except Exception as exc:
            log.exception("STATISTICS_QUALITY_REPORT_FAILED period_id=%s error=%s", period_id, type(exc).__name__)
            text = "⚠️ Не удалось сформировать quality-отчёт."
        await message.answer(
            text,
            reply_markup=signal_analytics_admin_menu(owner_id=user_id),
        )

    @router.message(Command("stats_financial"))
    async def cmd_statistics_financial(message: Message):
        user_id = _sender_id(message)
        if not _is_admin(user_id):
            await message.answer("⛔ Раздел доступен только администратору.")
            return
        try:
            period_id = _stats_period_argument(message, "stats_financial")
        except ValueError as exc:
            await message.answer(str(exc))
            return
        if not bool(get_settings().STATS_V2_REPORTS_ENABLED):
            await message.answer("⏸ STATS_V2_REPORTS_ENABLED=false.")
            return
        try:
            text = await format_statistics_financial_report(period_id, user_id=user_id)
        except LookupError:
            text = "⚠️ Период не найден."
        except Exception as exc:
            log.exception("STATISTICS_FINANCIAL_REPORT_FAILED period_id=%s error=%s", period_id, type(exc).__name__)
            text = "⚠️ Не удалось сформировать финансовый отчёт."
        await message.answer(
            text,
            reply_markup=signal_analytics_admin_menu(owner_id=user_id),
        )

    @router.message(Command("stats_recovery"))
    async def cmd_statistics_recovery(message: Message):
        user_id = _sender_id(message)
        if not _is_admin(user_id):
            await message.answer("⛔ Раздел доступен только администратору.")
            return
        if not bool(get_settings().STATS_V2_REPORTS_ENABLED):
            await message.answer("⏸ STATS_V2_REPORTS_ENABLED=false.")
            return
        try:
            period_id = _stats_period_argument(message, "stats_recovery")
            text = await format_statistics_recovery_report(period_id, user_id=user_id)
        except ValueError as exc:
            await message.answer(str(exc))
            return
        except Exception as exc:
            log.exception("STATISTICS_RECOVERY_REPORT_FAILED error=%s", type(exc).__name__)
            text = "⚠️ Не удалось загрузить recovery review."
        await message.answer(
            text,
            reply_markup=signal_analytics_admin_menu(owner_id=user_id),
        )

    @router.message(Command("stats_recovery_request"))
    async def cmd_statistics_recovery_request(message: Message):
        user_id = _sender_id(message)
        if not _is_admin(user_id):
            await message.answer("⛔ Раздел доступен только администратору.")
            return
        settings = get_settings()
        if not bool(settings.STATS_V2_REPORTS_ENABLED):
            await message.answer("⏸ STATS_V2_REPORTS_ENABLED=false.")
            return
        if not bool(settings.STATISTICS_QUALITY_ENABLED):
            await message.answer(
                "⏸ STATISTICS_QUALITY_ENABLED=false. Audit-запросы recovery выключены."
            )
            return
        try:
            audit_id = _stats_period_argument(message, "stats_recovery_request")
        except ValueError as exc:
            await message.answer(str(exc))
            return
        if audit_id is None:
            await message.answer("Использование: <code>/stats_recovery_request AUDIT_ID</code>")
            return
        candidate = await get_statistics_recovery_candidate(audit_id, user_id=user_id)
        if candidate is None:
            await message.answer("⚠️ Recoverable finding с таким AUDIT_ID не найден.")
            return
        await message.answer(
            "\n".join(
                [
                    "<b>🧯 RECOVERY REVIEW REQUEST</b>",
                    "",
                    f"Audit: <b>#{candidate.audit_id}</b>",
                    f"Entity: <b>{html.escape(candidate.entity_type)}</b> <code>{html.escape(candidate.entity_id)}</code>",
                    f"Issue: <code>{html.escape(candidate.issue_code)}</code>",
                    f"Причина: {html.escape(candidate.reason[:500] or '—')}",
                    "",
                    "Подтверждение создаст только append-only recovery_requested audit event.",
                    "Автоматического исправления истории или торговых данных не будет.",
                ]
            ),
            reply_markup=statistics_recovery_confirm_menu(
                candidate.audit_id, owner_id=user_id
            ),
        )

    @router.message(Command("stats_reset"))
    async def cmd_statistics_reset(message: Message):
        raw = str(getattr(message, "text", "") or "").strip().split(maxsplit=1)
        reason = raw[1].strip() if len(raw) > 1 else "manual_admin_reset"
        await _send_statistics_reset_preview(
            message,
            _sender_id(message),
            reason=reason,
        )

    @router.message(Command("tp_confirmed"))
    async def cmd_tp_confirmed(message: Message):
        await _handle_tp_confirmed(message)

        # ------------------------------------------------------------------
        # Multi-step /api setup wizard
        # ------------------------------------------------------------------
        # Goal: never make the user paste their key+secret(+passphrase) on a
        # single line.  Each secret arrives in its own message which is deleted
        # from the chat history immediately when Telegram allows.
        # State data stored in FSMContext:
        #   exchange: 'bingx'
        #   api_key:  str(set after step 1)
        #   api_secret: str(set after step 2)
        # waiting_passphrase kept only for stale FSM compatibility.
        # Cancel: /cancel command or the "❌ Отмена" inline button.

    async def _delete_message_safely(message: Message) -> None:
        try:
            await message.delete()
        except Exception:
            pass

    def _api_format_hint(ex: str = "bingx") -> str:
        return "/api bingx BingX_API_KEY BingX_API_SECRET"

    async def _start_bingx_api_wizard(
        message: Message, state: FSMContext, uid: int, uname: str | None
    ) -> None:
        from app.bot.keyboards import api_setup_cancel_menu

        await db.ensure_user(uid, uname, _is_admin(uid))
        if not await db.has_accepted_terms(uid, TERMS_VERSION, terms_hash()):
            await _send_terms_prompt(message, uid, uname)
            return
        await state.clear()
        await state.update_data(exchange="bingx", started_chat=message.chat.id)
        await state.set_state(ApiSetup.waiting_key)
        await message.answer(
            "🔑 <b>Подключение API для BingX</b>\n\n"
            "Шаг <b>1/2</b>\n\n"
            "Пришли <b>API KEY</b> следующим сообщением.\n\n"
            "⚠️ Я постараюсь удалить сообщение сразу после получения. "
            "Если Telegram не позволит - удали его вручную.",
            parse_mode="HTML",
            reply_markup=api_setup_cancel_menu(owner_id=uid),
        )

    @router.callback_query(F.data.startswith("api_setup_start:"))
    @_owner_guarded_callback
    async def cb_api_setup_start(call: CallbackQuery, state: FSMContext):
        ex = _callback_payload(call).split(":", 1)[1].lower().strip()
        if ex != "bingx":
            await call.answer("Эта сборка работает только с BingX", show_alert=True)
            return
        if not await db.has_accepted_terms(
            call.from_user.id, TERMS_VERSION, terms_hash()
        ):
            await call.answer("Сначала прими условия (/terms)", show_alert=True)
            await _send_terms_prompt(
                call.message, call.from_user.id, call.from_user.username
            )
            return
        await _start_bingx_api_wizard(
            call.message, state, call.from_user.id, call.from_user.username
        )
        await call.answer()

    @router.message(Command("api_setup"))
    async def cmd_api_setup(message: Message, state: FSMContext):
        await _start_bingx_api_wizard(
            message, state, message.from_user.id, message.from_user.username
        )

    @router.message(Command("cancel"))
    async def cmd_cancel(message: Message, state: FSMContext):
        current_state = await state.get_state()
        if current_state is None:
            await message.answer("Нечего отменять.")
            return
        ttl_state = getattr(LimitTtlSetup.waiting_hours, "state", None)
        if ttl_state and current_state == ttl_state:
            # v1.6.18: every other entry point that touches this exact FSM
            # state (menu buttons, presets, the "hours" reply handler itself)
            # goes through _limit_policy_menu_lock. /cancel previously did not,
            # so a /cancel racing with one of those (e.g. sent right after an
            # hours reply) could leave the hidden waiting_hours state active
            # after the user believed they had left it, letting an unrelated
            # later message be misread as a TTL value.
            uid = message.from_user.id
            async with _limit_policy_menu_lock(uid):
                cleared = await _clear_legacy_limit_ttl_state(state)
            if cleared:
                await message.answer("❌ Настройка срока LIMIT отменена.")
            else:
                # Another task already resolved this state first (e.g. the
                # hours reply was processed before this /cancel acquired the
                # lock) -- nothing is left to cancel.
                await message.answer("Нечего отменять.")
            return
        await state.clear()
        await message.answer("❌ Подключение API отменено. Никаких данных не сохранил.")

    @router.callback_query(
        lambda call: parse_callback_owner(call.data)[1] == "api_setup_cancel"
    )
    @_owner_guarded_callback
    async def cb_api_setup_cancel(call: CallbackQuery, state: FSMContext):
        await state.clear()
        try:
            await call.message.edit_text(
                "❌ Подключение API отменено.\nНикаких данных не сохранил."
            )
        except Exception:
            pass
        await call.answer("Отменено")

    @router.message(ApiSetup.waiting_key)
    async def fsm_api_key(message: Message, state: FSMContext):
        from app.bot.keyboards import api_setup_cancel_menu

        text = (message.text or "").strip()
        if not text or text.startswith("/") or len(text) < 8 or " " in text:
            if text:
                await _delete_message_safely(message)
            await message.answer(
                "⚠️ Это не похоже на API KEY. Пришли ключ одним сообщением без пробелов или /cancel.",
                reply_markup=api_setup_cancel_menu(owner_id=message.from_user.id),
            )
            return
        await state.update_data(api_key=text, exchange="bingx")
        await _delete_message_safely(message)
        await state.set_state(ApiSetup.waiting_secret)
        await message.answer(
            f"✅ KEY получен ({len(text)} символов).\n\n"
            "🔑 <b>Подключение API для BingX</b> - Шаг <b>2/2</b>\n\n"
            "Теперь пришли <b>API SECRET</b> следующим сообщением.",
            parse_mode="HTML",
            reply_markup=api_setup_cancel_menu(owner_id=message.from_user.id),
        )

    @router.message(ApiSetup.waiting_secret)
    async def fsm_api_secret(message: Message, state: FSMContext):
        from app.bot.keyboards import api_setup_cancel_menu

        text = (message.text or "").strip()
        if not text or text.startswith("/") or len(text) < 8 or " " in text:
            if text:
                await _delete_message_safely(message)
            await message.answer(
                "⚠️ Это не похоже на API SECRET. Пришли секрет одним сообщением без пробелов или /cancel.",
                reply_markup=api_setup_cancel_menu(owner_id=message.from_user.id),
            )
            return
        await state.update_data(api_secret=text, exchange="bingx")
        await _delete_message_safely(message)
        await _finalize_api_setup(message, state, passphrase="")

    @router.message(ApiSetup.waiting_passphrase)
    async def fsm_api_passphrase(message: Message, state: FSMContext):
        # Состояние оставлено только для совместимости со старыми незавершёнными FSM.
        await state.clear()
        await message.answer(
            "Эта сборка работает только с BingX. Запусти /api_setup заново."
        )

    async def _finalize_api_setup(
        message: Message, state: FSMContext, *, passphrase: str = ""
    ) -> None:
        data = await state.get_data()
        api_key = data.get("api_key") or ""
        api_secret = data.get("api_secret") or ""
        uid = message.from_user.id
        await state.clear()
        if not api_key or not api_secret:
            await message.answer(
                "❌ Часть данных потерялась. Запусти /api_setup заново."
            )
            return

        settings = get_settings()
        verifying = await message.answer("⏳ Проверяю ключи на BingX…")
        adapter = None
        try:
            api_key_enc = encrypt_text(api_key, settings.ENCRYPTION_KEY)
            api_secret_enc = encrypt_text(api_secret, settings.ENCRYPTION_KEY)
            adapter = build_adapter(
                {
                    "exchange": "bingx",
                    "api_key_encrypted": api_key_enc,
                    "api_secret_encrypted": api_secret_enc,
                    "passphrase_encrypted": None,
                    "testnet": settings.BINGX_VST,
                }
            )
            ok = await adapter.verify_api()
        except Exception as exc:
            log.warning(
                "BingX API wizard verification failed uid=%s: %s",
                uid,
                type(exc).__name__,
            )
            try:
                await verifying.delete()
            except Exception:
                pass
            await message.answer(system_api_error_message(exc))
            return
        finally:
            if adapter is not None:
                try:
                    await adapter.close()
                except Exception:
                    pass

        try:
            await verifying.delete()
        except Exception:
            pass
        if not ok:
            await message.answer(system_api_error_message(rejected=True))
            return

        await db.save_api_key(
            user_id=uid,
            api_key_enc=api_key_enc,
            api_secret_enc=api_secret_enc,
            passphrase_enc=None,
            exchange="bingx",
            testnet=settings.BINGX_VST,
        )
        await db.set_user_setting(uid, "exchange", "bingx")
        await message.answer(
            system_api_connected_message(), reply_markup=_home_menu(uid)
        )

    @router.message(Command("delete_api"))
    async def cmd_delete_api(message: Message):
        uid = message.from_user.id
        api_row = await _cached_api_key(uid, "bingx")
        if not api_row:
            await message.answer("ℹ️ BingX API уже не подключён или отключён.")
            return
        await db.disable_api_key(uid, "bingx")
        await db.set_user_setting(uid, "exchange", "bingx")
        await db.set_user_setting(uid, "mode", UserMode.PREVIEW.value)
        await message.answer(system_api_disabled_message())

    @router.callback_query(F.data.startswith("menu:"))
    @_owner_guarded_callback
    async def cb_menu(call: CallbackQuery, state: FSMContext):
        action = _callback_payload(call).split(":", 1)[1]
        user_id = call.from_user.id
        if action != "limits":
            async with _limit_policy_menu_lock(user_id):
                await _clear_legacy_limit_ttl_state(state)
        acquired, reason = _try_begin_menu_callback(user_id, action)
        if not acquired:
            if reason == "busy":
                await call.answer(
                    "⏳ Предыдущий раздел ещё загружается", show_alert=False
                )
            else:
                await call.answer("✅ Раздел уже открыт", show_alert=False)
            return

        try:
            # Acknowledge immediately so Telegram stops the loading spinner
            # while the dashboard is refreshed from PostgreSQL or BingX.
            await call.answer()
            if action == "home":
                await _safe_edit(
                    call.message,
                    await _menu_text_bounded(user_id, call.from_user.username),
                    reply_markup=_home_menu(user_id),
                )
            elif action == "mode":
                mode_text, mode_markup = await _mode_menu_view(user_id)
                await _safe_edit(
                    call.message,
                    mode_text,
                    reply_markup=mode_markup,
                )
            elif action == "skip_notifications":
                skip_text, skip_markup = await _skip_notifications_menu_view(
                    user_id
                )
                await _safe_edit(
                    call.message,
                    skip_text,
                    reply_markup=skip_markup,
                )
            elif action in {"exchanges", "risk", "tp", "be"}:
                await _safe_edit(
                    call.message,
                    _section_text(action),
                    reply_markup=main_menu(action, owner_id=user_id),
                )
            elif action == "limits":
                # Serialize with legacy LIMIT-policy callbacks. Otherwise an
                # old custom-TTL button can set a hidden FSM state immediately
                # after this menu has tried to clear it.
                async with _limit_policy_menu_lock(user_id):
                    await _clear_legacy_limit_ttl_state(state)
                    await _safe_edit(
                        call.message,
                        await _limit_settings_text(user_id),
                        reply_markup=await _limit_menu_markup(user_id),
                    )
            elif action == "status":
                await _safe_edit(
                    call.message,
                    await _status_text(user_id),
                    reply_markup=_home_menu(user_id),
                )
            elif action == "balance":
                await _safe_edit(
                    call.message,
                    await _balance_text(user_id, call.from_user.username),
                    reply_markup=_home_menu(user_id),
                )
            elif action == "positions":
                text, rows = await _positions_view(user_id, call.from_user.username)
                await _safe_edit(
                    call.message,
                    text,
                    reply_markup=positions_list_menu(rows, owner_id=user_id),
                )
            elif action == "subscribers":
                markup = (
                    subscribers_admin_menu(owner_id=user_id)
                    if _is_admin(user_id)
                    else _home_menu(user_id)
                )
                await _safe_edit(
                    call.message,
                    await _subscribers_text(user_id),
                    reply_markup=markup,
                )
            elif action == "analytics":
                markup = (
                    signal_analytics_admin_menu(owner_id=user_id)
                    if _is_admin(user_id)
                    else _home_menu(user_id)
                )
                await _safe_edit(
                    call.message,
                    await _signal_analytics_text(user_id),
                    reply_markup=markup,
                )
            elif action == "diagnostics":
                await _safe_edit(
                    call.message,
                    await _diagnostics_text(user_id),
                    reply_markup=_home_menu(user_id),
                )
            elif action == "help":
                await _safe_edit(
                    call.message, _help_text(), reply_markup=_home_menu(user_id)
                )
        finally:
            _finish_menu_callback(user_id, action)

    @router.callback_query(F.data.startswith("analytics:"))
    @_owner_guarded_callback
    async def cb_signal_analytics(call: CallbackQuery):
        action = _callback_payload(call).split(":", 1)[1].strip().lower()
        user_id = call.from_user.id
        if not _is_admin(user_id):
            await call.answer("Только для администратора", show_alert=True)
            return
        if action == "export":
            await call.answer("Формирую ZIP/CSV…")
            await _send_signal_analytics_export(call.message, user_id)
            return
        if action == "reset":
            await call.answer()
            await _send_statistics_reset_preview(
                call.message,
                user_id,
                reason="manual_admin_reset_from_menu",
            )
            return
        if not bool(get_settings().STATS_V2_REPORTS_ENABLED):
            await call.answer("STATS_V2_REPORTS_ENABLED=false", show_alert=True)
            return
        if action not in {"periods", "all", "technical", "financial", "quality", "recovery"}:
            await call.answer("Неизвестное действие", show_alert=True)
            return
        await call.answer()
        try:
            if action == "periods":
                text = await format_statistics_periods_report(user_id=user_id)
            elif action == "all":
                text = await format_statistics_all_report(user_id=user_id)
            elif action == "technical":
                text = await format_statistics_technical_report(user_id=user_id)
            elif action == "financial":
                text = await format_statistics_financial_report(user_id=user_id)
            elif action == "quality":
                text = await format_statistics_quality_report(user_id=user_id)
            elif action == "recovery":
                text = await format_statistics_recovery_report(user_id=user_id)
        except Exception as exc:
            log.exception(
                "STATISTICS_CALLBACK_REPORT_FAILED action=%s error=%s",
                action,
                type(exc).__name__,
            )
            text = "⚠️ Не удалось сформировать statistics-v2 отчёт."
        await _safe_edit(
            call.message,
            text,
            reply_markup=(
                statistics_technical_admin_menu(owner_id=user_id)
                if action in {"technical", "financial", "quality", "recovery"}
                else signal_analytics_admin_menu(owner_id=user_id)
            ),
        )

    @router.callback_query(F.data.startswith("statsrecovery:"))
    @_owner_guarded_callback
    async def cb_statistics_recovery(call: CallbackQuery):
        payload = _callback_payload(call).split(":")
        if len(payload) != 3 or payload[1] not in {"request", "cancel"}:
            await call.answer("Некорректный recovery callback", show_alert=True)
            return
        if not _is_admin(call.from_user.id):
            await call.answer("Только для администратора", show_alert=True)
            return
        try:
            audit_id = int(payload[2])
        except (TypeError, ValueError):
            await call.answer("Некорректный AUDIT_ID", show_alert=True)
            return
        if payload[1] == "cancel":
            await call.answer("Отменено")
            await _safe_edit(
                call.message,
                "❌ <b>Recovery review request отменён.</b>\n\nНикакие данные не изменены.",
                reply_markup=signal_analytics_admin_menu(owner_id=call.from_user.id),
            )
            return
        if not bool(get_settings().STATISTICS_QUALITY_ENABLED):
            await call.answer("STATISTICS_QUALITY_ENABLED=false", show_alert=True)
            return
        await call.answer("Записываю audit-запрос…")
        try:
            result = await request_statistics_recovery(
                audit_id=audit_id,
                actor_user_id=call.from_user.id,
                scope_user_id=call.from_user.id,
            )
        except Exception as exc:
            log.exception(
                "STATISTICS_RECOVERY_REQUEST_FAILED audit_id=%s error=%s",
                audit_id,
                type(exc).__name__,
            )
            text = "⚠️ Не удалось записать recovery audit request. Ничего не исправлялось."
        else:
            if result.status == "requested":
                text = (
                    "✅ <b>Recovery review запрошен.</b>\n\n"
                    "Создан только append-only audit event. Автоматического исправления нет."
                )
            elif result.status == "already_requested":
                text = "ℹ️ <b>Этот recovery review уже был запрошен.</b>"
            elif result.status == "not_found":
                text = "⚠️ Recoverable finding больше не найден."
            elif result.status == "disabled":
                text = "⏸ STATISTICS_QUALITY_ENABLED=false."
            else:
                text = "⚠️ Некорректный recovery request."
        await _safe_edit(
            call.message,
            text,
            reply_markup=signal_analytics_admin_menu(owner_id=call.from_user.id),
        )

    @router.callback_query(F.data.startswith("statsreset:"))
    @_owner_guarded_callback
    async def cb_statistics_reset(call: CallbackQuery):
        payload = _callback_payload(call).split(":")
        if len(payload) != 4 or payload[1] not in {"confirm", "cancel"}:
            await call.answer("Некорректное подтверждение", show_alert=True)
            return
        if not _is_admin(call.from_user.id):
            await call.answer("Только для администратора", show_alert=True)
            return
        action, raw_id, token = payload[1], payload[2], payload[3]
        try:
            request_id = int(raw_id)
        except (TypeError, ValueError):
            await call.answer("Некорректный request id", show_alert=True)
            return
        if action == "cancel":
            cancelled = await cancel_statistics_reset(
                request_id=request_id,
                token=token,
                actor_user_id=call.from_user.id,
            )
            await call.answer("Отменено" if cancelled else "Запрос уже неактивен")
            await _safe_edit(
                call.message,
                "❌ <b>Создание нового периода отменено.</b>\n\nНикакие данные не изменены.",
                reply_markup=signal_analytics_admin_menu(owner_id=call.from_user.id),
            )
            return
        await call.answer("Проверяю и создаю период…")
        try:
            result = await confirm_statistics_reset(
                request_id=request_id,
                token=token,
                actor_user_id=call.from_user.id,
            )
        except Exception as exc:
            log.exception(
                "STATISTICS_RESET_CONFIRM_FAILED request_id=%s error=%s",
                request_id,
                type(exc).__name__,
            )
            await _safe_edit(
                call.message,
                "⚠️ <b>Reset не выполнен.</b>\n\nНикакие строки статистики не удалялись. Проверьте Railway-логи.",
                reply_markup=signal_analytics_admin_menu(owner_id=call.from_user.id),
            )
            return
        if result.status in {"applied", "already_applied"}:
            label = "создан" if result.status == "applied" else "уже был создан"
            text = "\n".join(
                [
                    "✅ <b>НОВЫЙ ПЕРИОД СОЗДАН</b>",
                    "",
                    f"Старый период: <b>#{result.old_period_id}</b> — закрыт.",
                    f"Новый период: <b>#{result.new_period_id}</b> — <b>{html.escape(result.new_period_name or '')}</b> ({label}).",
                    "",
                    "История не удалялась. Новые сигналы будут относиться к новому active-периоду; уже сохранённые сигналы сохраняют прежний period_id.",
                ]
            )
        elif result.status == "stale":
            text = (
                "⚠️ <b>Подтверждение устарело.</b>\n\n"
                "Активный период уже изменился другим подтверждением. Второй период не создан."
            )
        elif result.status == "expired":
            text = "⏳ <b>Кнопка истекла.</b>\n\nЗапустите /stats_reset заново."
        elif result.status == "disabled":
            text = "⏸ <b>STATS_RESET_ENABLED=false.</b>"
        else:
            text = f"⚠️ Reset не выполнен: <code>{html.escape(result.status)}</code>."
        await _safe_edit(
            call.message,
            text,
            reply_markup=signal_analytics_admin_menu(owner_id=call.from_user.id),
        )

    @router.callback_query(F.data.startswith("limitpreset:"))
    @_owner_guarded_callback
    async def cb_limit_preset(call: CallbackQuery, state: FSMContext):
        preset = _callback_payload(call).split(":", 1)[1].strip().lower()
        if preset not in LIMIT_POLICY_PRESETS:
            await call.answer("Неизвестный режим", show_alert=True)
            return
        ttl, tp_mode = LIMIT_POLICY_PRESETS[preset]
        uid = call.from_user.id
        async with _limit_policy_menu_lock(uid):
            await _clear_legacy_limit_ttl_state(state)
            await db.ensure_user(uid, call.from_user.username, _is_admin(uid))
            await db.set_user_limit_policy(
                uid, ttl_hours=ttl, tp_mode=tp_mode, preset=preset
            )
            # Re-read the authoritative row after the write.  Never render a
            # hard-coded checkmark that can disagree with a concurrent click.
            text = "✅ <b>Режим лимиток сохранён</b>\n\n" + await _limit_settings_text(
                uid
            )
            markup = await _limit_menu_markup(uid)
            await _safe_edit(call.message, text, reply_markup=markup)
        await call.answer("Сохранено")

    @router.callback_query(F.data.startswith("limittp:"))
    @_owner_guarded_callback
    async def cb_limit_tp_mode(call: CallbackQuery, state: FSMContext):
        mode = _callback_payload(call).split(":", 1)[1].strip().lower()
        if mode not in {"none", "tp1", "tp2", "half", "last"}:
            await call.answer("Неизвестное правило TP", show_alert=True)
            return
        uid = call.from_user.id
        async with _limit_policy_menu_lock(uid):
            await _clear_legacy_limit_ttl_state(state)
            await db.ensure_user(uid, call.from_user.username, _is_admin(uid))
            current = await db.get_user_settings(uid)
            await db.set_user_limit_policy(
                uid,
                ttl_hours=int(getattr(current, "limit_ttl_hours", 24) or 0),
                tp_mode=mode,
                preset="custom",
            )
            await _safe_edit(
                call.message,
                "✅ <b>Правило движения сохранено</b>\n\n"
                + await _limit_settings_text(uid),
                reply_markup=await _limit_menu_markup(uid),
            )
        await call.answer("Сохранено")

    @router.callback_query(
        lambda call: parse_callback_owner(call.data)[1] == "limitttl:custom"
    )
    @_owner_guarded_callback
    async def cb_limit_custom_ttl(call: CallbackQuery, state: FSMContext):
        uid = call.from_user.id
        async with _limit_policy_menu_lock(uid):
            await state.clear()
            await state.set_state(LimitTtlSetup.waiting_hours)
        await call.message.answer(
            "🕒 <b>СВОЙ СРОК LIMIT</b>\n"
            "━━━━━━━━━━━━━━━━━━━━\n"
            "Отправь количество часов от <b>1 до 168</b>.\n\n"
            "Изменится только максимальное время ожидания.\n"
            "Текущее правило отмены после движения цены сохранится.\n\n"
            "Пример: <code>12</code>",
            reply_markup=limit_ttl_cancel_menu(owner_id=uid),
        )
        await call.answer("Жду число часов")

    @router.callback_query(
        lambda call: parse_callback_owner(call.data)[1] == "limitttl:cancel"
    )
    @_owner_guarded_callback
    async def cb_limit_custom_ttl_cancel(call: CallbackQuery, state: FSMContext):
        uid = call.from_user.id
        async with _limit_policy_menu_lock(uid):
            await state.clear()
            await _safe_edit(
                call.message,
                await _limit_settings_text(uid),
                reply_markup=await _limit_menu_markup(uid),
            )
        await call.answer("Отменено")

    @router.message(LimitTtlSetup.waiting_hours)
    async def fsm_limit_custom_ttl(message: Message, state: FSMContext):
        text = (message.text or "").strip()
        try:
            hours = int(text)
        except ValueError:
            await message.answer("❌ Отправь целое число часов от 1 до 168.")
            return
        if hours < 1 or hours > 168:
            await message.answer("❌ Допустимый срок: от 1 до 168 часов.")
            return
        uid = message.from_user.id
        async with _limit_policy_menu_lock(uid):
            # A preset/navigation callback may have cleared this custom TTL
            # FSM while this message handler was waiting for the lock.
            # Do not let an already-dispatched stale text overwrite the newer
            # profile selection.
            expected_state = getattr(LimitTtlSetup.waiting_hours, "state", None)
            if expected_state and await state.get_state() != expected_state:
                return
            await db.ensure_user(uid, message.from_user.username, _is_admin(uid))
            current = await db.get_user_settings(uid)
            await db.set_user_limit_policy(
                uid,
                ttl_hours=hours,
                tp_mode=str(
                    getattr(current, "limit_tp_invalidation_mode", "half") or "half"
                ),
                preset="custom",
            )
            await state.clear()
            result_text = (
                f"✅ Срок ожидания новых LIMIT: <b>{hours} ч</b>\n\n"
                + await _limit_settings_text(uid)
            )
            result_markup = await _limit_menu_markup(uid)
        await message.answer(result_text, reply_markup=result_markup)

    @router.callback_query(
        lambda call: parse_callback_owner(call.data)[1] == "position:list"
    )
    @_owner_guarded_callback
    async def cb_position_list(call: CallbackQuery):
        uid = call.from_user.id
        text, rows = await _positions_view(uid, call.from_user.username)
        await _safe_edit(
            call.message,
            text,
            reply_markup=positions_list_menu(rows, owner_id=uid),
        )
        await call.answer()

    @router.callback_query(
        lambda call: parse_callback_owner(call.data)[1].startswith("position:view:")
    )
    @_owner_guarded_callback
    async def cb_position_view(call: CallbackQuery):
        uid = call.from_user.id
        position_id = _callback_payload(call).split(":", 2)[-1].strip()
        if not position_id:
            await call.answer("Некорректная позиция", show_alert=True)
            return
        text, position, allow_force_be = await _position_detail_view(
            uid, position_id, call.from_user.username
        )
        markup = (
            position_detail_menu(
                position_id,
                owner_id=uid,
                allow_force_be=allow_force_be,
            )
            if position is not None
            else position_action_result_menu(owner_id=uid)
        )
        await _safe_edit(call.message, text, reply_markup=markup)
        await call.answer()

    @router.callback_query(
        lambda call: parse_callback_owner(call.data)[1].startswith("position:close:")
    )
    @_owner_guarded_callback
    async def cb_position_close_preview(call: CallbackQuery):
        uid = call.from_user.id
        position_id = _callback_payload(call).split(":", 2)[-1].strip()
        text, position, _allow_force_be = await _position_detail_view(
            uid, position_id, call.from_user.username
        )
        if position is None:
            await _safe_edit(
                call.message,
                text,
                reply_markup=position_action_result_menu(owner_id=uid),
            )
            await call.answer("Позиция уже недоступна", show_alert=True)
            return
        symbol = html.escape(str(position.get("symbol") or ""))
        side = html.escape(str(position.get("side") or "").upper())
        await _safe_edit(
            call.message,
            "⚠️ <b>ПОДТВЕРДИТЕ ПОЛНОЕ ЗАКРЫТИЕ</b>\n"
            "━━━━━━━━━━━━━━━━━━━━\n"
            f"🪙 <b>{symbol}</b> • {side}\n"
            f"📦 Будет закрыто: <b>{fmt_qty(_first_present_finite(position, ('size',)))}</b>\n\n"
            "Бот повторно получит актуальный объём и закроет всю позицию MARKET-ордером.\n"
            "Цена исполнения может отличаться от текущей. Частичного закрытия нет.",
            reply_markup=position_close_confirm_menu(position_id, owner_id=uid),
        )
        await call.answer()

    @router.callback_query(
        lambda call: parse_callback_owner(call.data)[1].startswith(
            "position:closeconfirm:"
        )
    )
    @_owner_guarded_callback
    async def cb_position_close_confirm(call: CallbackQuery):
        uid = call.from_user.id
        position_id = _callback_payload(call).split(":", 2)[-1].strip()
        try:
            await call.answer("Закрываю всю позицию и проверяю BingX…")
        except Exception:
            pass
        result = await close_position_fully(uid, position_id)
        await _safe_action_result(
            call,
            _position_close_result_text(result),
            reply_markup=position_action_result_menu(owner_id=uid),
            event_key=f"manual-close:{uid}:{position_id}:{getattr(call, 'id', '')}",
        )

    @router.callback_query(
        lambda call: parse_callback_owner(call.data)[1].startswith("position:be:")
    )
    @_owner_guarded_callback
    async def cb_position_be_preview(call: CallbackQuery):
        uid = call.from_user.id
        position_id = _callback_payload(call).split(":", 2)[-1].strip()
        text, position, allow_force_be = await _position_detail_view(
            uid, position_id, call.from_user.username
        )
        if position is None:
            await _safe_edit(
                call.message,
                text,
                reply_markup=position_action_result_menu(owner_id=uid),
            )
            await call.answer("Позиция уже недоступна", show_alert=True)
            return
        if not allow_force_be:
            await call.answer(
                "Б/У уже установлен, позиция не связана с точной сделкой бота или статус не позволяет перенос.",
                show_alert=True,
            )
            return
        symbol = html.escape(str(position.get("symbol") or ""))
        side = html.escape(str(position.get("side") or "").upper())
        await _safe_edit(
            call.message,
            "⚠️ <b>ПРИНУДИТЕЛЬНЫЙ Б/У</b>\n"
            "━━━━━━━━━━━━━━━━━━━━\n"
            f"🪙 <b>{symbol}</b> • {side}\n"
            f"💵 Вход: <b>{fmt_price(_first_present_finite(position, ('entryPrice',)))}</b>\n"
            f"📦 Остаток: <b>{fmt_qty(_first_present_finite(position, ('size',)))}</b>\n\n"
            "Бот немедленно рассчитает Б/У с учётом комиссионного буфера и безопасно заменит только STOP. TP на BingX не удаляются и не пересоздаются.\n\n"
            "Если текущая цена слишком близко к входу, операция будет отклонена, чтобы STOP не сработал сразу.",
            reply_markup=position_be_confirm_menu(position_id, owner_id=uid),
        )
        await call.answer()

    @router.callback_query(
        lambda call: parse_callback_owner(call.data)[1].startswith(
            "position:beconfirm:"
        )
    )
    @_owner_guarded_callback
    async def cb_position_be_confirm(call: CallbackQuery):
        uid = call.from_user.id
        position_id = _callback_payload(call).split(":", 2)[-1].strip()
        try:
            await call.answer("Переношу STOP в Б/У и проверяю защиту…")
        except Exception:
            pass
        result = await force_position_break_even(uid, position_id)
        await _safe_action_result(
            call,
            _position_be_result_text(result),
            reply_markup=position_action_result_menu(owner_id=uid),
            event_key=f"manual-be:{uid}:{position_id}:{getattr(call, 'id', '')}",
        )

    @router.callback_query(
        lambda call: parse_callback_owner(call.data)[1] == "limitactive:list"
    )
    @_owner_guarded_callback
    async def cb_limit_active_list(call: CallbackQuery, state: FSMContext):
        uid = call.from_user.id
        async with _limit_policy_menu_lock(uid):
            await _clear_legacy_limit_ttl_state(state)
            await _reconcile_pending_limits_for_menu(uid, passes=3)
            rows = await db.pending_limit_executions_for_user(uid, limit=100)
            await _safe_edit(
                call.message,
                await _active_limits_text(uid, rows),
                reply_markup=active_limits_list_menu(rows, owner_id=uid),
            )
        await call.answer()

    @router.callback_query(
        lambda call: parse_callback_owner(call.data)[1].startswith("limitactive:view:")
    )
    @_owner_guarded_callback
    async def cb_limit_active_view(call: CallbackQuery, state: FSMContext):
        uid = call.from_user.id
        try:
            execution_id = int(_callback_payload(call).rsplit(":", 1)[-1])
        except (TypeError, ValueError):
            await call.answer("Некорректный LIMIT", show_alert=True)
            return
        async with _limit_policy_menu_lock(uid):
            await _clear_legacy_limit_ttl_state(state)
            row = await db.get_execution_by_id(execution_id)
            if (
                not row
                or int(row.get("user_id") or 0) != uid
                or str(row.get("status") or "") != "pending_limit"
            ):
                await call.answer(
                    "LIMIT уже недоступен или принадлежит другому пользователю.",
                    show_alert=True,
                )
                return
            await _safe_edit(
                call.message,
                _active_limit_detail_text(row),
                reply_markup=active_limit_detail_menu(execution_id, owner_id=uid),
            )
        await call.answer()

    @router.callback_query(
        lambda call: parse_callback_owner(call.data)[1].startswith(
            "limitactive:recheck:"
        )
    )
    @_owner_guarded_callback
    async def cb_limit_active_recheck(call: CallbackQuery, state: FSMContext):
        uid = call.from_user.id
        try:
            execution_id = int(_callback_payload(call).rsplit(":", 1)[-1])
        except (TypeError, ValueError):
            await call.answer("Некорректный LIMIT", show_alert=True)
            return
        async with _limit_policy_menu_lock(uid):
            await _clear_legacy_limit_ttl_state(state)
        try:
            await call.answer("Безопасно перепроверяю без нового cancel…")
        except Exception:
            pass
        result = await _recheck_user_pending_limit(uid, execution_id)
        await _safe_action_result(
            call,
            _limit_recheck_result_text(result),
            reply_markup=active_limit_result_menu(owner_id=uid),
            event_key=(
                f"limit-readonly-recheck:{uid}:{execution_id}:{getattr(call, 'id', '')}"
            ),
        )

    @router.callback_query(
        lambda call: parse_callback_owner(call.data)[1].startswith(
            "limitactive:cancel:"
        )
    )
    @_owner_guarded_callback
    async def cb_limit_active_cancel_preview(call: CallbackQuery, state: FSMContext):
        uid = call.from_user.id
        try:
            execution_id = int(_callback_payload(call).rsplit(":", 1)[-1])
        except (TypeError, ValueError):
            await call.answer("Некорректный LIMIT", show_alert=True)
            return
        async with _limit_policy_menu_lock(uid):
            await _clear_legacy_limit_ttl_state(state)
            row = await db.get_execution_by_id(execution_id)
            if (
                not row
                or int(row.get("user_id") or 0) != uid
                or str(row.get("status") or "") != "pending_limit"
            ):
                await call.answer(
                    "LIMIT уже недоступен или принадлежит другому пользователю.",
                    show_alert=True,
                )
                return
            await _safe_edit(
                call.message,
                "⚠️ <b>ПОДТВЕРДИТЕ ОТМЕНУ</b>\n"
                "━━━━━━━━━━━━━━━━━━━━\n"
                f"🪙 <b>{html.escape(str(row.get('symbol')or ''))}</b> • "
                f"{html.escape(str(row.get('side')or '').upper())}\n"
                f"💵 Вход: <b>{html.escape(str(row.get('entry')or '—'))}</b>\n"
                f"📦 Объём: <b>{html.escape(str(row.get('qty')or '—'))}</b>\n\n"
                "Бот отменит только этот точный LIMIT на вашем аккаунте.\n"
                "Если вход успел исполниться частично, открытая часть позиции не будет закрыта.",
                reply_markup=active_limit_cancel_confirm_menu(
                    execution_id, owner_id=uid
                ),
            )
        await call.answer()

    @router.callback_query(
        lambda call: parse_callback_owner(call.data)[1].startswith(
            "limitactive:confirm:"
        )
    )
    @_owner_guarded_callback
    async def cb_limit_active_cancel_confirm(call: CallbackQuery, state: FSMContext):
        uid = call.from_user.id
        try:
            execution_id = int(_callback_payload(call).rsplit(":", 1)[-1])
        except (TypeError, ValueError):
            await call.answer("Некорректный LIMIT", show_alert=True)
            return
        async with _limit_policy_menu_lock(uid):
            await _clear_legacy_limit_ttl_state(state)
        try:
            await call.answer("Проверяю и отменяю точный LIMIT…")
        except Exception:
            pass
        result = await _cancel_user_pending_limit(uid, execution_id)
        await _safe_action_result(
            call,
            _manual_limit_cancel_result_text(result),
            reply_markup=active_limit_result_menu(owner_id=uid),
            event_key=(
                f"manual-limit-cancel:{uid}:{execution_id}:"
                f"{getattr(call, 'id', '')}"
            ),
        )

    @router.callback_query(
        lambda call: parse_callback_owner(call.data)[1] == "limitapply:preview"
    )
    @_owner_guarded_callback
    async def cb_limit_apply_preview(call: CallbackQuery, state: FSMContext):
        uid = call.from_user.id
        async with _limit_policy_menu_lock(uid):
            await _clear_legacy_limit_ttl_state(state)
            # Bind preview and confirmation token to one authoritative settings
            # snapshot. A rapid profile click cannot produce a token for a
            # different policy than the one shown on screen.
            settings_row = await db.get_user_settings(uid)
            ttl = int(getattr(settings_row, "limit_ttl_hours", 24) or 0)
            mode = str(
                getattr(settings_row, "limit_tp_invalidation_mode", "half") or "half"
            )
            preset = _resolved_limit_preset(settings_row)
            preview_text, execution_ids = await _limit_apply_preview_text(
                uid,
                ttl_hours=ttl,
                tp_mode=mode,
            )
            if not execution_ids:
                await call.answer("Активных лимиток нет", show_alert=True)
                return
            token = _create_limit_apply_token(
                user_id=uid,
                ttl_hours=ttl,
                tp_mode=mode,
                preset=preset,
                execution_ids=execution_ids,
            )
            await _safe_edit(
                call.message,
                preview_text,
                reply_markup=limit_apply_confirm_menu(token, owner_id=uid),
            )
        await call.answer()

    @router.callback_query(F.data.startswith("limitapply:confirm:"))
    @_owner_guarded_callback
    async def cb_limit_apply_confirm(call: CallbackQuery, state: FSMContext):
        uid = call.from_user.id
        async with _limit_policy_menu_lock(uid):
            await _clear_legacy_limit_ttl_state(state)
            token = _callback_payload(call).rsplit(":", 1)[-1].strip()
            snapshot = _consume_limit_apply_token(token, uid)
            if not snapshot:
                await call.answer(
                    "Подтверждение устарело или уже использовано. Открой preview заново.",
                    show_alert=True,
                )
                return
            changed = await db.apply_limit_policy_to_pending(
                uid,
                ttl_hours=int(snapshot.get("ttl_hours") or 0),
                tp_mode=str(snapshot.get("tp_mode") or "last"),
                preset=str(snapshot.get("preset") or "custom"),
                execution_ids=[
                    int(x) for x in list(snapshot.get("execution_ids") or [])
                ],
            )
            await _safe_edit(
                call.message,
                f"✅ Новая политика применена к <b>{changed}</b> активным LIMIT.\n\n"
                "Monitor безопасно проверит каждую лимитку на следующем цикле.\n\n"
                + await _limit_settings_text(uid),
                reply_markup=await _limit_menu_markup(uid),
            )
        await call.answer("Применено")

    @router.callback_query(
        lambda call: parse_callback_owner(call.data)[1] == "limitapply:confirm"
    )
    @_owner_guarded_callback
    async def cb_limit_apply_confirm_legacy(call: CallbackQuery, state: FSMContext):
        async with _limit_policy_menu_lock(call.from_user.id):
            await _clear_legacy_limit_ttl_state(state)
        await call.answer(
            "Эта кнопка устарела. Открой раздел лимиток и создай новое подтверждение.",
            show_alert=True,
        )

    @router.callback_query(
        lambda call: parse_callback_owner(call.data)[1] == "terms:show"
    )
    @_owner_guarded_callback
    async def cb_terms_show(call: CallbackQuery):
        await _send_terms_prompt(
            call.message, call.from_user.id, call.from_user.username
        )
        await call.answer("Соглашение отправлено")

    @router.callback_query(
        lambda call: parse_callback_owner(call.data)[1] == "terms:accept"
    )
    @_owner_guarded_callback
    async def cb_terms_accept(call: CallbackQuery):
        uid = call.from_user.id
        uname = call.from_user.username
        accepted_text = (
            "Пользователь нажал кнопку подтверждения в Telegram: "
            "Я прочитал(а), понял(а) и принимаю Пользовательское соглашение, "
            "уведомление о рисках и ограничение ответственности ANTILUD VIP CORE."
        )
        await db.accept_terms(uid, uname, TERMS_VERSION, terms_hash(), accepted_text)
        _api_block = "<code>/api bingx API_KEY API_SECRET</code>"
        await _safe_edit(
            call.message,
            (
                "✅ <b>Условия приняты.</b>\n"
                "━━━━━━━━━━━━━━━━━━━━\n"
                f"Версия: <b>{TERMS_VERSION}</b>\n"
                f"Hash: <code>{terms_hash()[:16]}...</code>\n\n"
                "Теперь можно подключить BingX API:\n"
                f"{_api_block}"
            ),
            reply_markup=_home_menu(call.from_user.id),
        )
        await call.answer("Условия приняты")

    @router.callback_query(F.data.startswith("vip:"))
    @_owner_guarded_callback
    async def cb_vip(call: CallbackQuery):
        raw = _callback_payload(call).split(":", 1)[1]
        uid = call.from_user.id
        await db.ensure_user(uid, call.from_user.username, _is_admin(uid))
        if raw == "auto":
            await db.set_user_setting(uid, "mode", UserMode.AUTO.value)
            prefix = "✅ VIP-режим: <b>Авто</b>\n\n"
        elif raw == "preview":
            await db.set_user_setting(uid, "mode", UserMode.PREVIEW.value)
            prefix = "👁 VIP-режим: <b>Просмотр</b>\n\n"
        elif raw in {"off", "o"}:
            await db.set_user_setting(uid, "mode", UserMode.OFF.value)
            prefix = "⏸ VIP-режим: <b>Выкл</b>\n\n"
        else:
            prefix = ""
        mode_text, mode_markup = await _mode_menu_view(uid)
        await _safe_edit(
            call.message,
            prefix + mode_text,
            reply_markup=mode_markup,
        )
        await call.answer("Сохранено")

    @router.callback_query(F.data.startswith("skipnotify:"))
    @_owner_guarded_callback
    async def cb_skip_trade_notifications(call: CallbackQuery):
        raw = _callback_payload(call).split(":", 1)[1].strip().lower()
        if raw not in {"on", "off"}:
            await call.answer("Неизвестная настройка", show_alert=True)
            return
        uid = int(call.from_user.id)
        requested_enabled = raw == "on"
        lock = _skip_notification_menu_lock(uid)
        async with lock:
            await db.ensure_user(uid, call.from_user.username, _is_admin(uid))
            await db.set_user_setting(
                uid, "skip_trade_notifications_enabled", requested_enabled
            )
            # Render only the value freshly read from durable storage. This keeps
            # rapidly tapped callbacks and old Telegram buttons consistent with DB.
            saved_row = await db.get_user_settings(uid)
            saved_enabled = bool(
                getattr(saved_row, "skip_trade_notifications_enabled", False)
            )
            await _safe_edit(
                call.message,
                "✅ <b>Настройка сохранена</b>\n\n"
                + _skip_notifications_section_text(saved_enabled),
                reply_markup=main_menu(
                    "skip_notifications",
                    skip_trade_notifications_enabled=saved_enabled,
                    owner_id=uid,
                ),
            )
        await call.answer("Включено" if saved_enabled else "Выключено")


    @router.callback_query(F.data.startswith("exchange:"))
    @_owner_guarded_callback
    async def cb_exchange(call: CallbackQuery):
        ex = _callback_payload(call).split(":", 1)[1].lower().strip()
        uid = call.from_user.id
        uname = call.from_user.username
        await db.ensure_user(uid, uname, _is_admin(uid))
        settings = get_settings()
        if not settings.is_exchange_enabled(ex):
            await _safe_edit(
                call.message,
                f"❌ Биржа {ex.upper()} отключена в ENV и недоступна",
                reply_markup=main_menu("exchanges", owner_id=uid),
            )
            await call.answer()
            return

        api_row = await _cached_api_key(uid, ex)
        title = exchange_title(ex)
        if api_row:
            await db.set_user_setting(uid, "exchange", ex)
            await _safe_edit(
                call.message,
                f"✅ BingX выбрана для торговли.\n"
                f"🔐 API для {title} уже подключён.\n\n"
                "Чтобы отключить BingX API - нажми кнопку ниже.",
                reply_markup=exchange_connected_menu(ex, owner_id=uid),
            )
        else:
            fmt = "/api bingx BingX_API_KEY BingX_API_SECRET"
            terms_ok = await db.has_accepted_terms(uid, TERMS_VERSION, terms_hash())
            if not terms_ok:
                await _safe_edit(
                    call.message,
                    f"⚠️ API для <b>{title}</b> ещё не подключён.\n"
                    "Перед подключением API нужно принять пользовательское соглашение и уведомление о рисках.\n"
                    "Нажми кнопку ниже, скачай TXT-файл и подтверди условия.\n\n"
                    "После подтверждения повтори команду подключения:\n"
                    f"<code>{fmt}</code>",
                    reply_markup=exchange_need_api_menu(
                        ex, terms_required=True, owner_id=uid
                    ),
                )
                await _send_terms_prompt(call.message, uid, uname)
                await call.answer("Сначала нужно принять условия", show_alert=True)
                return
            await _safe_edit(
                call.message,
                f"⚠️ API для <b>{title}</b> ещё не подключён.\n"
                "🔑 <b>Нажми кнопку ниже</b> чтобы подключить API по шагам "
                "(безопаснее чем вводить всё одной командой), "
                "либо отправь вручную:\n"
                f"<code>{fmt}</code>",
                reply_markup=exchange_need_api_menu(ex, owner_id=uid),
            )
        await call.answer()

    @router.callback_query(F.data.startswith("api_disconnect:"))
    @_owner_guarded_callback
    async def cb_api_disconnect(call: CallbackQuery):
        ex = _callback_payload(call).split(":", 1)[1].lower().strip()
        uid = call.from_user.id
        settings = get_settings()
        if ex != "bingx":
            await call.answer("Неизвестная биржа", show_alert=True)
            return
        if not settings.is_exchange_enabled(ex):
            await _safe_edit(
                call.message,
                f"❌ Биржа {ex.upper()} уже отключена в ENV.",
                reply_markup=main_menu("exchanges", owner_id=uid),
            )
            await call.answer()
            return
        title = exchange_title(ex)
        api_row = await _cached_api_key(uid, ex)
        if not api_row:
            await _safe_edit(
                call.message,
                f"ℹ️ API {title} уже не подключён или уже отключён.\n\n"
                "Ничего удалять не пришлось.",
                reply_markup=main_menu("exchanges", owner_id=uid),
            )
            await call.answer()
            return

        user_settings = await _cached_user_settings(uid)
        was_current = (user_settings.exchange or "").lower() == ex
        await db.disable_api_key(uid, ex)
        if was_current:
            await db.set_user_setting(uid, "mode", UserMode.PREVIEW.value)
            await _safe_edit(
                call.message,
                f"🔌 API {title} отключён для твоего аккаунта.\n\n"
                "⚠️ Эта биржа была активной, поэтому я перевёл VIP-режим в <b>просмотр</b>.\n"
                "Так бот не будет пытаться открывать сделки без API.\n\n"
                "Чтобы снова торговать через эту биржу - добавь API и включи режим Авто.",
                reply_markup=main_menu("exchanges", owner_id=uid),
            )
        else:
            await _safe_edit(
                call.message,
                f"🔌 API {title} отключён для твоего аккаунта.\n\n"
                "Режим не изменялся.",
                reply_markup=main_menu("exchanges", owner_id=uid),
            )
        await call.answer()

    @router.callback_query(F.data.startswith("riskpreset:"))
    @_owner_guarded_callback
    async def cb_risk_preset(call: CallbackQuery):
        raw = _callback_payload(call).split(":", 1)[1]
        uid = call.from_user.id
        await db.ensure_user(uid, call.from_user.username, _is_admin(uid))
        if raw == "be10":
            await db.set_user_setting(uid, "risk_per_trade_percent", 1.0)
            await db.set_user_setting(uid, "max_open_trades", 10)
            await db.set_user_setting(uid, "max_portfolio_risk_percent", 10.0)
            await db.set_user_setting(uid, "daily_risk_limit_percent", 10.0)
            await db.set_user_setting(uid, "exclude_be_trades_from_risk", 1)
            prefix = (
                "✅ <b>Риск-режим включён</b>\n"
                "10 сделок по 1%, БУ освобождает риск-слот.\n\n"
            )
        else:
            prefix = ""
        await _safe_edit(
            call.message,
            prefix + _section_text("risk"),
            reply_markup=main_menu("risk", owner_id=uid),
        )
        await call.answer("Сохранено")

    @router.callback_query(F.data.startswith("riskbe:"))
    @_owner_guarded_callback
    async def cb_risk_be(call: CallbackQuery):
        raw = _callback_payload(call).split(":", 1)[1]
        uid = call.from_user.id
        await db.ensure_user(uid, call.from_user.username, _is_admin(uid))
        enabled = 1 if raw == "on" else 0
        await db.set_user_setting(uid, "exclude_be_trades_from_risk", enabled)
        prefix = (
            "✅ <b>БУ освобождает риск-слот</b>\n\n"
            if enabled
            else "🚫 <b>БУ считается активным риском</b>\n\n"
        )
        await _safe_edit(
            call.message,
            prefix + _section_text("risk"),
            reply_markup=main_menu("risk", owner_id=uid),
        )
        await call.answer("Сохранено")

    @router.callback_query(F.data.startswith("tp:"))
    @_owner_guarded_callback
    async def cb_tp(call: CallbackQuery):
        mode = _callback_payload(call).split(":", 1)[1]
        uid = call.from_user.id
        await db.ensure_user(uid, call.from_user.username, _is_admin(uid))
        if mode == "smart":
            await db.set_user_setting(uid, "tp_mode", TpMode.SMART.value)
            prefix = "✅ TP режим: <b>умная фиксация</b>\n\n"
        elif mode == "equal":
            await db.set_user_setting(uid, "tp_mode", TpMode.EQUAL.value)
            prefix = "✅ TP режим: <b>равными долями</b>\n\n"
        elif mode == "bell":
            await db.set_user_setting(uid, "tp_mode", TpMode.BELL.value)
            prefix = "✅ TP режим: <b>🔔 колокол</b> (меньше на крайних, больше в центре)\n\n"
        elif mode == "early_fixation":
            await db.set_user_setting(uid, "tp_mode", TpMode.EARLY_FIXATION.value)
            await db.set_user_setting(uid, "be_trigger_tp_index", 1)
            await db.set_user_setting(uid, "be_after_tp1_enabled", 1)
            prefix = (
                "✅ TP режим: <b>🛡️ Ранняя фиксация</b>\n"
                "TP1 70% • TP2 15% • TP3 10% • TP4 5%\n"
                "После TP1 STOP переносится в Б/У.\n"
                "Применяется только к новым сделкам.\n\n"
            )
        elif mode == "acceleration":
            await db.set_user_setting(uid, "tp_mode", TpMode.ACCELERATION.value)
            prefix = (
                "✅ TP режим: <b>🚀 Разгон</b>\n"
                "TP1 10% • TP2 65% • TP3 20% • TP4 5%\n"
                "Применяется только к новым сделкам.\n\n"
            )
        else:
            await call.answer("Неизвестный режим", show_alert=True)
            return
        await _safe_edit(
            call.message,
            prefix + _section_text("tp"),
            reply_markup=main_menu("tp", owner_id=uid),
        )
        await call.answer("Сохранено")

    @router.callback_query(F.data.startswith("tpsignal:"))
    @_owner_guarded_callback
    async def cb_tp_signal(call: CallbackQuery):
        value = _callback_payload(call).split(":", 1)[1]
        uid = call.from_user.id
        await db.ensure_user(uid, call.from_user.username, _is_admin(uid))
        enabled = 1 if value == "on" else 0
        await db.set_user_setting(uid, "use_signal_tp_percents", enabled)
        prefix = (
            "✅ <b>TP-проценты из сигнала включены</b>\n\n"
            if enabled
            else "🚫 <b>TP-проценты из сигнала выключены</b>\n\n"
        )
        await _safe_edit(
            call.message,
            prefix + _section_text("tp"),
            reply_markup=main_menu("tp", owner_id=uid),
        )
        await call.answer("Сохранено")

    @router.callback_query(F.data.startswith("tplimit:"))
    @_owner_guarded_callback
    async def cb_tp_limit(call: CallbackQuery):
        value = _callback_payload(call).split(":", 1)[1]
        uid = call.from_user.id
        await db.ensure_user(uid, call.from_user.username, _is_admin(uid))
        if value == "all":
            val = "all"
        elif value.isdigit():
            val = str(max(1, min(int(value), 20)))
        else:
            await call.answer("Некорректный TP лимит", show_alert=True)
            return
        await db.set_user_setting(uid, "tp_limit", val)
        label = "все TP" if val == "all" else f"первые {val} TP"
        await _safe_edit(
            call.message,
            f"✅ TP лимит: <b>{label}</b>\n\n" + _section_text("tp"),
            reply_markup=main_menu("tp", owner_id=uid),
        )
        await call.answer("Сохранено")

    @router.callback_query(F.data.startswith("be:"))
    @_owner_guarded_callback
    async def cb_be(call: CallbackQuery):
        value = _callback_payload(call).split(":", 1)[1]
        uid = call.from_user.id
        await db.ensure_user(uid, call.from_user.username, _is_admin(uid))
        _invalidate_be_apply_tokens_for_user(uid)
        if value == "off":
            await db.set_user_setting(uid, "be_trigger_tp_index", 0)
            await db.set_user_setting(uid, "be_after_tp1_enabled", 0)
            prefix = (
                "⛔ Умное БУ: <b>выключено</b>\n"
                "ℹ️ Новая настройка действует только на новые сделки.\n\n"
            )
        else:
            if value not in {"1", "2", "3"}:
                await call.answer("Доступны только TP1, TP2 или TP3", show_alert=True)
                return
            trigger = int(value)
            await db.set_user_setting(uid, "be_trigger_tp_index", trigger)
            await db.set_user_setting(uid, "be_after_tp1_enabled", 1)
            prefix = (
                f"✅ Умное БУ: <b>после TP{trigger}</b>\n"
                "ℹ️ Новая настройка действует только на новые сделки.\n\n"
            )
        await _safe_edit(
            call.message,
            prefix + _section_text("be"),
            reply_markup=main_menu("be", owner_id=uid),
        )
        await call.answer("Сохранено")

    @router.callback_query(
        lambda call: parse_callback_owner(call.data)[1] == "beapply:preview"
    )
    @_owner_guarded_callback
    async def cb_be_apply_preview(call: CallbackQuery):
        uid = call.from_user.id
        async with _be_policy_menu_lock(uid):
            settings_row = await db.get_user_settings(uid)
            trigger = _normalize_execution_be_trigger(
                getattr(settings_row, "be_trigger_tp_index", 0)
            )
            if trigger is None:
                await call.answer(
                    "Текущая настройка Б/У повреждена. Выбери TP1, TP2, TP3 или выкл.",
                    show_alert=True,
                )
                return
            preview_text, execution_ids = await _be_apply_preview_text(
                uid, trigger_tp_index=trigger
            )
            if not execution_ids:
                await call.answer(
                    "Нет текущих сделок, которым нужно применить эту настройку.",
                    show_alert=True,
                )
                return
            token = _create_be_apply_token(
                user_id=uid,
                trigger_tp_index=trigger,
                execution_ids=execution_ids,
            )
            await _safe_edit(
                call.message,
                preview_text,
                reply_markup=be_apply_confirm_menu(token, owner_id=uid),
            )
        await call.answer()

    @router.callback_query(F.data.startswith("beapply:confirm:"))
    @_owner_guarded_callback
    async def cb_be_apply_confirm(call: CallbackQuery):
        uid = call.from_user.id
        async with _be_policy_menu_lock(uid):
            token = _callback_payload(call).rsplit(":", 1)[-1].strip()
            snapshot = _consume_be_apply_token(token, uid)
            if not snapshot:
                await call.answer(
                    "Подтверждение устарело или уже использовано. Открой preview заново.",
                    show_alert=True,
                )
                return
            trigger = int(snapshot.get("trigger_tp_index") or 0)
            if trigger not in {0, 1, 2, 3}:
                await call.answer("Некорректная настройка Б/У", show_alert=True)
                return
            current_settings = await db.get_user_settings(uid)
            current_trigger = _normalize_execution_be_trigger(
                getattr(current_settings, "be_trigger_tp_index", 0)
            )
            if current_trigger != trigger:
                await call.answer(
                    "Настройка Б/У изменилась после preview. Открой подтверждение заново.",
                    show_alert=True,
                )
                return
            result = await _apply_be_trigger_to_current(
                uid,
                trigger_tp_index=trigger,
                execution_ids=[
                    int(x) for x in list(snapshot.get("execution_ids") or [])
                ],
            )
            await _safe_edit(
                call.message,
                "✅ <b>НАСТРОЙКА Б/У ПРИМЕНЕНА</b>\n"
                "━━━━━━━━━━━━━━━━━━━━\n\n"
                + "\n".join(
                    premium_arrow_lines(
                        (
                            ("⚖️ Режим", _be_trigger_label(trigger)),
                            ("📈 Открытых позиций обновлено", result["positions"]),
                            ("⏳ Ожидающих LIMIT обновлено", result["limits"]),
                            ("↪️ Пропущено после повторной проверки", result["skipped"]),
                        )
                    )
                )
                + "\n\nБот не менял ордера напрямую. Обычный монитор безопасно "
                "перепроверит TP, позицию и STOP на ближайшем цикле.\n"
                "Strict ownership ручных или неизвестных STOP не обходится.",
                reply_markup=main_menu("be", owner_id=uid),
            )
        await call.answer("Применено")

    @router.callback_query(
        lambda call: parse_callback_owner(call.data)[1] == "beapply:confirm"
    )
    @_owner_guarded_callback
    async def cb_be_apply_confirm_legacy(call: CallbackQuery):
        await call.answer(
            "Эта кнопка устарела. Открой раздел Б/У и создай новое подтверждение.",
            show_alert=True,
        )

    @router.message(Command("market_event"))
    async def cmd_market_event(message: Message):
        """Read-only diagnostic card for one durable market event."""
        if not _is_admin(message.from_user.id):
            await message.answer("⛔ Только админ может смотреть market_event.")
            return
        parts = (message.text or "").split()
        if len(parts) != 2:
            await message.answer(
                "Использование: <code>/market_event EVENT_ID</code>",
                parse_mode="HTML",
            )
            return
        try:
            event_id = int(parts[1])
        except ValueError:
            await message.answer("❌ event_id должен быть числом")
            return
        snapshot = await db.market_event_diagnostic(event_id)
        if not snapshot:
            await message.answer("🟡 Market event не найден.")
            return
        raw_keys = str(snapshot.get("coalesced_event_keys") or "").strip()
        try:
            parsed_keys = json.loads(raw_keys) if raw_keys else []
        except (TypeError, ValueError, json.JSONDecodeError):
            parsed_keys = []
        levels = ", ".join(str(value) for value in parsed_keys) or str(
            snapshot.get("event_key") or snapshot.get("event_type") or "unknown"
        )
        statuses = ", ".join(snapshot.get("execution_statuses") or []) or "unknown"
        lines = [
            "🔎 <b>MARKET EVENT · READ ONLY</b>",
            f"Event ID: <code>{event_id}</code>",
            f"Группа: <code>{int(snapshot.get('trade_group_id') or 0)}</code>",
            f"Инструмент: <b>{html.escape(str(snapshot.get('group_symbol') or 'unknown'))}</b>",
            f"Сторона: <b>{html.escape(str(snapshot.get('group_side') or 'unknown'))}</b>",
            f"Уровни: <code>{html.escape(levels)}</code>",
            f"Статус события: <code>{html.escape(str(snapshot.get('status') or 'unknown'))}</code>",
            f"Линия: <code>{html.escape(str(snapshot.get('watch_lane') or 'critical'))}</code>",
            f"Outcome: <code>{html.escape(str(snapshot.get('outcome_kind') or ''))}</code>",
            f"Причина: <code>{html.escape(str(snapshot.get('stuck_reason') or snapshot.get('last_error') or ''))}</code>",
            f"Попыток: <code>{int(snapshot.get('attempts') or 0)}</code>",
            f"Аккаунтов: <code>{int(snapshot.get('user_count') or 0)}</code>",
            f"Execution статусы: <code>{html.escape(statuses)}</code>",
            f"Escalated UTC: <code>{html.escape(str(snapshot.get('escalated_at') or '—'))}</code>",
            f"Stuck UTC: <code>{html.escape(str(snapshot.get('stuck_started_at') or '—'))}</code>",
            f"Следующая проверка: <code>{html.escape(str(snapshot.get('next_attempt_at') or '—'))}</code>",
            "",
            "🚫 Команда ничего не меняет в БД и на BingX.",
        ]
        await message.answer("\n".join(lines), parse_mode="HTML")


    async def _market_event_admin_card(event_id: int) -> str:
        snapshot = await db.market_event_diagnostic(int(event_id))
        if not snapshot:
            return "🟡 Market event не найден."
        history = await db.market_event_manual_action_history(int(event_id), limit=5)
        raw_keys = str(snapshot.get("coalesced_event_keys") or "").strip()
        try:
            parsed_keys = json.loads(raw_keys) if raw_keys else []
        except (TypeError, ValueError, json.JSONDecodeError):
            parsed_keys = []
        levels = ", ".join(str(value) for value in parsed_keys) or str(
            snapshot.get("event_key") or snapshot.get("event_type") or "unknown"
        )
        lines = [
            "🔎 <b>MARKET EVENT · EVIDENCE</b>",
            f"Event ID: <code>{int(event_id)}</code>",
            f"Группа: <code>{int(snapshot.get('trade_group_id') or 0)}</code>",
            f"Инструмент: <b>{html.escape(str(snapshot.get('group_symbol') or 'unknown'))}</b>",
            f"Событие: <code>{html.escape(levels)}</code>",
            f"Статус / фаза: <code>{html.escape(str(snapshot.get('status') or ''))} / {html.escape(str(snapshot.get('phase') or ''))}</code>",
            f"Попытки: <code>{int(snapshot.get('attempts') or 0)} · fast={int(snapshot.get('fast_attempts') or 0)} · deep={int(snapshot.get('deep_attempts') or 0)} · final={int(snapshot.get('final_attempts') or 0)}</code>",
            f"Outcome: <code>{html.escape(str(snapshot.get('outcome_kind') or ''))}</code>",
            f"Terminal: <code>{html.escape(str(snapshot.get('terminal_outcome') or '—'))}</code>",
            f"Причина: <code>{html.escape(str(snapshot.get('terminal_reason') or snapshot.get('stuck_reason') or snapshot.get('last_error') or '—'))}</code>",
            f"Автоматизация: <code>{int(snapshot.get('automation_enabled') if snapshot.get('automation_enabled') is not None else 1)}</code>",
            f"Миграция: <code>{html.escape(str(snapshot.get('migration_state') or 'none'))} v{int(snapshot.get('migration_version') or 0)}</code>",
            f"Ручное решение: <code>{html.escape(str(snapshot.get('manual_resolution') or '—'))}</code>",
            f"Fingerprint: <code>{html.escape(str(snapshot.get('evidence_fingerprint') or '—'))}</code>",
        ]
        if history:
            lines.extend(["", "<b>Последние ручные действия:</b>"])
            for item in history:
                lines.append(
                    "• <code>{}</code> · admin <code>{}</code> · {}".format(
                        html.escape(str(item.get("action") or "unknown")),
                        int(item.get("admin_user_id") or 0),
                        html.escape(str(item.get("created_at") or "")),
                    )
                )
        lines.extend(
            [
                "",
                "🚫 Ручное решение не изменяет ордера BingX и остаётся исключённым из статистики.",
            ]
        )
        return "\n".join(lines)

    def _market_event_manual_actions_enabled() -> bool:
        settings = get_settings()
        return bool(
            settings.MARKET_EVENT_TERMINAL_REVIEW_ENABLED
            and settings.MARKET_EVENT_MANUAL_RESOLUTION_ENABLED
        )

    async def _market_event_manual_action_allowed(event_id: int) -> bool:
        settings = get_settings()
        event = await db.market_event_diagnostic(int(event_id))
        if not event:
            return False
        return market_event_stage_allows_group(
            str(settings.MARKET_EVENT_ROLLOUT_STAGE),
            int(event.get("trade_group_id") or 0),
            int(settings.MARKET_EVENT_MIGRATION_TARGET_GROUP_ID),
        )

    @router.message(Command("market_events_rollout"))
    async def cmd_market_events_rollout(message: Message):
        if not _is_admin(message.from_user.id):
            await message.answer("⛔ Только админ может смотреть rollout.")
            return
        settings = get_settings()
        target_group_id = int(settings.MARKET_EVENT_MIGRATION_TARGET_GROUP_ID)
        snapshot = await db.market_event_rollout_snapshot(target_group_id)
        await message.answer(
            "🧭 <b>MARKET EVENT ROLLOUT</b>\n"
            f"Стадия: <code>{html.escape(str(settings.MARKET_EVENT_ROLLOUT_STAGE))}</code>\n"
            f"Migration enabled: <code>{int(bool(settings.MARKET_EVENT_MIGRATION_ENABLED))}</code>\n"
            f"Terminal review: <code>{int(bool(settings.MARKET_EVENT_TERMINAL_REVIEW_ENABLED))}</code>\n"
            f"Read coalescing: <code>{int(bool(settings.MARKET_EVENT_READ_COALESCING_ENABLED))}</code>\n"
            f"Target group: <code>{target_group_id}</code>\n"
            f"Глобально none / shadow / prepared / completed: <code>{int(snapshot.get('none_count') or 0)} / {int(snapshot.get('shadow_count') or 0)} / {int(snapshot.get('prepared_count') or 0)} / {int(snapshot.get('completed_count') or 0)}</code>\n"
            f"MANUAL_REVIEW глобально: <code>{int(snapshot.get('manual_review_count') or 0)}</code>\n"
            f"Target shadow / prepared / completed / MANUAL_REVIEW: <code>{int(snapshot.get('target_shadow_count') or 0)} / {int(snapshot.get('target_prepared_count') or 0)} / {int(snapshot.get('target_completed_count') or 0)} / {int(snapshot.get('target_manual_review_count') or 0)}</code>\n"
            f"Строк target group: <code>{int(snapshot.get('target_group_rows') or 0)}</code>",
            parse_mode="HTML",
        )

    @router.message(Command("market_event_retry"))
    async def cmd_market_event_retry(message: Message):
        if not _is_admin(message.from_user.id):
            await message.answer("⛔ Только админ может повторять FINAL-проверку.")
            return
        if not _market_event_manual_actions_enabled():
            await message.answer("⏸ Ручные действия MARKET EVENT выключены настройками Railway.")
            return
        parts = (message.text or "").split(maxsplit=2)
        if len(parts) < 2 or not parts[1].isdigit():
            await message.answer("Использование: <code>/market_event_retry EVENT_ID [комментарий]</code>", parse_mode="HTML")
            return
        event_id = int(parts[1])
        if not await _market_event_manual_action_allowed(event_id):
            await message.answer("⛔ Событие вне разрешённой rollout-группы.")
            return
        settings = get_settings()
        changed = await db.retry_market_event_manual_review(
            event_id,
            admin_user_id=message.from_user.id,
            max_fast_attempts=int(settings.MARKET_EVENT_MAX_FAST_ATTEMPTS),
            max_deep_attempts=int(settings.MARKET_EVENT_MAX_DEEP_ATTEMPTS),
            comment=parts[2] if len(parts) > 2 else "admin_final_recheck",
        )
        await message.answer(
            "✅ Назначена одна новая FINAL-проверка." if changed else "🟡 Событие не найдено либо уже не находится в MANUAL_REVIEW."
        )

    @router.message(Command("market_event_resolve"))
    async def cmd_market_event_resolve(message: Message):
        if not _is_admin(message.from_user.id):
            await message.answer("⛔ Только админ может фиксировать ручной результат.")
            return
        if not _market_event_manual_actions_enabled():
            await message.answer("⏸ Ручные действия MARKET EVENT выключены настройками Railway.")
            return
        parts = (message.text or "").split(maxsplit=3)
        if len(parts) < 3 or not parts[1].isdigit():
            await message.answer(
                "Использование: <code>/market_event_resolve EVENT_ID tp_filled|tp_not_filled|entry_not_filled|unknown [комментарий]</code>",
                parse_mode="HTML",
            )
            return
        event_id = int(parts[1])
        if not await _market_event_manual_action_allowed(event_id):
            await message.answer("⛔ Событие вне разрешённой rollout-группы.")
            return
        aliases = {
            "tp_filled": "tp_filled_manual",
            "tp_not_filled": "tp_not_filled_manual",
            "entry_not_filled": "entry_never_filled_manual",
            "unknown": "unknown_manual",
        }
        action = aliases.get(parts[2].strip().lower())
        if not action:
            await message.answer("❌ Неизвестное действие.")
            return
        changed = await db.resolve_market_event_manual_review(
            event_id,
            admin_user_id=message.from_user.id,
            action=action,
            comment=parts[3] if len(parts) > 3 else "",
        )
        if changed:
            snapshot = await db.market_event_diagnostic(event_id)
            if snapshot:
                await db.mark_market_event_statistics_manual_review(
                    int(snapshot.get("trade_group_id") or 0),
                    reason="market_event_manual_resolution_not_exchange_evidence",
                )
        await message.answer(
            "✅ Ручное административное решение сохранено. Ордера не изменялись; статистика остаётся в карантине."
            if changed else "🟡 Событие не найдено либо уже не находится в MANUAL_REVIEW."
        )

    @router.callback_query(F.data.startswith("mer:"))
    async def cb_market_event_manual_review(call: CallbackQuery):
        if not _is_admin(call.from_user.id):
            await call.answer("Только для администратора", show_alert=True)
            return
        payload = _callback_payload(call)
        parts = payload.split(":")
        if len(parts) != 3 or not parts[2].isdigit():
            await call.answer("Некорректная команда", show_alert=True)
            return
        action, event_id = parts[1], int(parts[2])
        if action == "view":
            await call.message.answer(await _market_event_admin_card(event_id), parse_mode="HTML")
            await call.answer()
            return
        if not _market_event_manual_actions_enabled():
            await call.answer("Ручные действия выключены настройками Railway", show_alert=True)
            return
        if not await _market_event_manual_action_allowed(event_id):
            await call.answer("Событие вне разрешённой rollout-группы", show_alert=True)
            return
        if action == "retry":
            settings = get_settings()
            changed = await db.retry_market_event_manual_review(
                event_id,
                admin_user_id=call.from_user.id,
                max_fast_attempts=int(settings.MARKET_EVENT_MAX_FAST_ATTEMPTS),
                max_deep_attempts=int(settings.MARKET_EVENT_MAX_DEEP_ATTEMPTS),
                comment="telegram_button_final_recheck",
            )
            await call.answer("FINAL-проверка назначена" if changed else "Состояние уже изменилось", show_alert=True)
            return
        mapping = {
            "tpfill": "tp_filled_manual",
            "tpno": "tp_not_filled_manual",
            "entryno": "entry_never_filled_manual",
        }
        resolution = mapping.get(action)
        if not resolution:
            await call.answer("Неизвестное действие", show_alert=True)
            return
        changed = await db.resolve_market_event_manual_review(
            event_id,
            admin_user_id=call.from_user.id,
            action=resolution,
            comment=f"telegram_button:{action}",
        )
        if changed:
            snapshot = await db.market_event_diagnostic(event_id)
            if snapshot:
                await db.mark_market_event_statistics_manual_review(
                    int(snapshot.get("trade_group_id") or 0),
                    reason="market_event_manual_resolution_not_exchange_evidence",
                )
        await call.answer(
            "Решение сохранено; ордера не менялись" if changed else "Состояние уже изменилось",
            show_alert=True,
        )

    @router.message(Command("be_recovery"))
    async def cmd_be_recovery(message: Message):
        """Fresh read-only g7a diagnostic for one recovery execution."""
        if not _is_admin(message.from_user.id):
            await message.answer("⛔ Только админ может проверять recovery.")
            return
        parts = (message.text or "").split()
        if len(parts) != 2:
            await message.answer(
                "Использование: <code>/be_recovery EXECUTION_ID</code>",
                parse_mode="HTML",
            )
            return
        try:
            execution_id = int(parts[1])
        except ValueError:
            await message.answer("❌ execution_id должен быть числом")
            return
        result = await inspect_existing_be_recovery(execution_id)
        state = str(result.get("state") or "error")
        if state not in {"ready", "blocked"}:
            await message.answer(
                "🟡 <b>Recovery не готов к exact cleanup</b>\n"
                f"Execution: <code>{execution_id}</code>\n"
                f"Состояние: <code>{html.escape(state)}</code>\n"
                f"Причина: <code>{html.escape(str(result.get('reason') or ''))}</code>",
                parse_mode="HTML",
            )
            return

        lines = [
            "🛡 <b>Fresh recovery diagnostics g7a</b>",
            f"Execution: <code>{execution_id}</code>",
            f"Symbol: <b>{html.escape(str(result.get('symbol') or ''))}</b>",
            f"Side: <b>{html.escape(str(result.get('side') or ''))}</b>",
            f"Position ID: <code>{html.escape(str(result.get('position_id') or ''))}</code>",
            f"Position qty: <code>{html.escape(str(result.get('position_qty') or 0))}</code>",
            f"Reason: <code>{html.escape(str(result.get('reason') or ''))}</code>",
            f"Topology: <code>{html.escape(str(result.get('topology_fingerprint') or ''))}</code>",
            "",
            "<b>Live STOP:</b>",
        ]
        for item in list(result.get("live_stops") or []):
            if not isinstance(item, dict):
                continue
            lines.append(
                "• <code>{}</code> | price={} | qty={} | {}".format(
                    html.escape(str(item.get("order_id") or "idless")),
                    html.escape(str(item.get("price") or 0)),
                    html.escape(str(item.get("qty"))),
                    html.escape(str(item.get("ownership") or "unknown")),
                )
            )
        lines.extend(
            [
                "",
                f"Replacement: <code>{html.escape(str(result.get('replacement_stop_id') or 'не доказан'))}</code>",
                f"TP fingerprint: <code>{html.escape(str(result.get('tp_fingerprint') or ''))}</code>",
                "🚫 Эта команда ничего на бирже не меняет.",
            ]
        )
        if state == "ready":
            token = _create_be_recovery_admin_token(
                admin_user_id=message.from_user.id, inspection=result
            )
            old_ids = ",".join(
                str(value) for value in list(result.get("allowed_old_stop_ids") or [])
            )
            lines.extend(
                [
                    "",
                    "⚠️ <b>Для exact cleanup скопируй команду полностью:</b>",
                    f"<code>/be_cleanup {token} {html.escape(old_ids)}</code>",
                    "Токен действует 5 минут и привязан к этой топологии.",
                ]
            )
        else:
            lines.extend(
                [
                    "",
                    "⛔ Exact cleanup сейчас запрещён: полная топология не доказана.",
                ]
            )
        await message.answer("\n".join(lines), parse_mode="HTML")

    @router.message(Command("be_cleanup"))
    async def cmd_be_cleanup(message: Message):
        """Execute one token-bound exact old-STOP cleanup after fresh re-proof."""
        if not _is_admin(message.from_user.id):
            await message.answer("⛔ Только админ может выполнять exact cleanup.")
            return
        parts = (message.text or "").split()
        if len(parts) != 3:
            await message.answer(
                "Использование: <code>/be_cleanup TOKEN OLD_STOP_ID[,OLD_STOP_ID]</code>",
                parse_mode="HTML",
            )
            return
        selected_ids = {
            clean_exchange_id(value)
            for value in parts[2].replace(";", ",").split(",")
        }
        selected_ids.discard("")
        snapshot = _consume_be_recovery_admin_token(
            parts[1],
            admin_user_id=message.from_user.id,
            selected_old_stop_ids=selected_ids,
        )
        if not snapshot:
            await message.answer(
                "❌ Токен устарел, принадлежит другому админу или STOP ID не совпадают. "
                "Сначала снова выполни <code>/be_recovery EXECUTION_ID</code>.",
                parse_mode="HTML",
            )
            return
        result = await execute_admin_existing_be_cleanup(
            execution_id=int(snapshot.get("execution_id") or 0),
            selected_old_stop_ids=selected_ids,
            expected_topology_fingerprint=str(
                snapshot.get("topology_fingerprint") or ""
            ),
            admin_user_id=message.from_user.id,
        )
        state = str(result.get("state") or "error")
        if state == "success":
            await message.answer(
                "✅ <b>Exact cleanup подтверждён</b>\n"
                f"Execution: <code>{int(result.get('execution_id') or 0)}</code>\n"
                f"Удалены старые STOP: <code>{html.escape(','.join(result.get('cancelled_old_stop_ids') or []))}</code>\n"
                f"Оставлен BE STOP: <code>{html.escape(str(result.get('replacement_stop_id') or ''))}</code>\n"
                "TP fingerprint не изменился.",
                parse_mode="HTML",
            )
            return
        await message.answer(
            "🟡 <b>Exact cleanup не завершён</b>\n"
            f"Состояние: <code>{html.escape(state)}</code>\n"
            f"Execution: <code>{int(result.get('execution_id') or 0)}</code>\n"
            "Никаких дополнительных STOP бот не создавал. Перед новой попыткой снова выполни read-only проверку.",
            parse_mode="HTML",
        )

    @router.message(Command("whitelist_add"))
    async def cmd_whitelist_add(message: Message):
        """Add user to whitelist. Two forms:

        /whitelist_add USER_ID                — grant BingX access
        /whitelist_add USER_ID bingx           — дать доступ BingX

        /whitelist_add USER_ID all            — legacy alias for BingX access
        """
        if not _is_admin(message.from_user.id):
            await message.answer("⛔ Только админ может управлять white-list.")
            return
        parts = (message.text or "").split()
        if len(parts) < 2:
            await message.answer(
                "Использование:\n"
                "<code>/whitelist_add USER_ID</code> — дать доступ BingX\n"
                "<code>/whitelist_add USER_ID bingx</code> — дать доступ BingX\n"
                "<code>/whitelist_add USER_ID all</code> — старый алиас для BingX\n\n"
                "Чтобы узнать ID юзера — попроси его написать <code>/id</code> боту.",
                parse_mode="HTML",
            )
            return
        try:
            target_uid = int(parts[1])
        except ValueError:
            await message.answer("❌ user_id должен быть числом")
            return
        await db.ensure_user(target_uid)

        # Form 1: only user_id provided → show BingX confirmation button
        if len(parts) == 2:
            from app.bot.keyboards import whitelist_add_exchange_picker

            picker = whitelist_add_exchange_picker(
                target_uid, owner_id=message.from_user.id
            )
            if picker is None:
                # Keyboard unavailable — grant the only supported BingX access
                await db.add_user_whitelist_exchange(target_uid, "all")
                await message.answer(
                    f"✅ Юзер <code>{target_uid}</code> добавлен в white-list (BingX).",
                    parse_mode="HTML",
                )
                return
            await message.answer(
                f"➕ Добавление в white-list юзера <code>{target_uid}</code>\n\n"
                "Разрешить ему автоторговлю на BingX?",
                parse_mode="HTML",
                reply_markup=picker,
            )
            return

            # Form 2: user_id + exchange list provided → apply directly
        raw_exchanges = parts[2].lower().replace(";", ",")
        wanted = [e.strip() for e in raw_exchanges.split(",") if e.strip()]
        if not wanted:
            await message.answer(
                "❌ Использование: /whitelist_add USER_ID или /whitelist_add USER_ID bingx"
            )
            return
        settings = get_settings()
        granted: list[str] = []
        rejected: list[str] = []
        for ex in wanted:
            if ex == "all":
                await db.add_user_whitelist_exchange(target_uid, "all")
                granted = ["BingX"]
                break
            if ex != "bingx":
                rejected.append(ex)
                continue
            if not settings.is_exchange_enabled(ex):
                rejected.append(f"{ex} (отключена в ENV)")
                continue
            await db.add_user_whitelist_exchange(target_uid, ex)
            granted.append(ex.upper())
        lines = [f"✅ Юзер <code>{target_uid}</code> добавлен в white-list."]
        if granted:
            lines.append(f"Разрешено: <b>{', '.join(granted)}</b>")
        if rejected:
            lines.append(f"⚠️ Пропущено: {', '.join(rejected)}")
        await message.answer("\n".join(lines), parse_mode="HTML")

    @router.callback_query(F.data.startswith("wladd:"))
    @_owner_guarded_callback
    async def cb_wladd(call):
        """Handle the BingX access confirmation button."""
        if not _is_admin(call.from_user.id):
            await call.answer("⛔ Только админ", show_alert=True)
            return
        try:
            _, uid_str, choice = _callback_payload(call).split(":", 2)
            target_uid = int(uid_str)
        except (ValueError, AttributeError):
            await call.answer("Ошибка callback", show_alert=True)
            return
        choice = (choice or "").lower().strip()
        if choice == "cancel":
            try:
                await call.message.edit_text(
                    f"❌ Отменено. Юзер <code>{target_uid}</code> не добавлен в white-list.",
                    parse_mode="HTML",
                )
            except Exception:
                pass
            await call.answer("Отменено")
            return
        if choice not in ("bingx", "all"):
            await call.answer("Неизвестный выбор", show_alert=True)
            return
        if choice != "all" and not get_settings().is_exchange_enabled(choice):
            await call.answer(f"{choice.upper()} отключена в ENV", show_alert=True)
            return
        new_set = await db.add_user_whitelist_exchange(target_uid, choice)
        if "all" in new_set:
            grant_text = "BingX"
        else:
            grant_text = ", ".join(ex.upper() for ex in sorted(new_set)) or "—"
        try:
            await call.message.edit_text(
                f"✅ Юзер <code>{target_uid}</code> добавлен в white-list.\n"
                f"Разрешено торговать: <b>{grant_text}</b>",
                parse_mode="HTML",
            )
        except Exception:
            pass
        await call.answer(f"Добавлен: {grant_text}")

    @router.message(Command("whitelist_remove"))
    async def cmd_whitelist_remove(message: Message):
        """Remove user from whitelist. Two forms:

        /whitelist_remove USER_ID            — show BingX revoke button
        /whitelist_remove USER_ID bingx       — убрать доступ BingX
        /whitelist_remove USER_ID all        — revoke everything (preview-only)
        """
        if not _is_admin(message.from_user.id):
            await message.answer("⛔ Только админ может управлять white-list.")
            return
        parts = (message.text or "").split()
        if len(parts) < 2:
            await message.answer(
                "Использование:\n"
                "<code>/whitelist_remove USER_ID</code> — покажу что убрать\n"
                "<code>/whitelist_remove USER_ID bingx</code> — убрать BingX\n"
                "<code>/whitelist_remove USER_ID all</code> — убрать всё",
                parse_mode="HTML",
            )
            return
        try:
            target_uid = int(parts[1])
        except ValueError:
            await message.answer("❌ user_id должен быть числом")
            return
        current = await db.get_user_whitelist_exchanges(target_uid)
        if not current:
            await message.answer(
                f"ℹ️ Юзер <code>{target_uid}</code> и так не в white-list.",
                parse_mode="HTML",
            )
            return

            # Form 1: only user_id → show BingX revoke button
        if len(parts) == 2:
            from app.bot.keyboards import whitelist_remove_exchange_picker

            picker = whitelist_remove_exchange_picker(
                target_uid, current, owner_id=message.from_user.id
            )
            if picker is None:
                await db.remove_user_whitelist_exchange(target_uid, "all")
                await message.answer(
                    f"🚫 Юзер <code>{target_uid}</code> удалён из white-list.",
                    parse_mode="HTML",
                )
                return
            if "all" in current:
                cur_text = "BingX"
            else:
                cur_text = ", ".join(ex.upper() for ex in sorted(current))
            await message.answer(
                f"➖ Убрать из white-list юзера <code>{target_uid}</code>\n\n"
                f"Сейчас разрешено: <b>{cur_text}</b>\n\n"
                "Убрать доступ к автоторговле BingX?",
                parse_mode="HTML",
                reply_markup=picker,
            )
            return

            # Form 2: explicit exchange
        ex = parts[2].lower().strip()
        if ex not in ("bingx", "all"):
            await message.answer("❌ Допустимо: bingx / all")
            return
        new_set = await db.remove_user_whitelist_exchange(target_uid, ex)
        if not new_set:
            await message.answer(
                f"🚫 Юзер <code>{target_uid}</code> полностью удалён из white-list.\n"
                "Теперь только наблюдение (preview-only).",
                parse_mode="HTML",
            )
        else:
            remaining = ", ".join(e.upper() for e in sorted(new_set))
            await message.answer(
                f"➖ Убрали {ex.upper()}. Юзер <code>{target_uid}</code> остался "
                f"в white-list для: <b>{remaining}</b>",
                parse_mode="HTML",
            )

    @router.callback_query(F.data.startswith("wlrm:"))
    @_owner_guarded_callback
    async def cb_wlrm(call):
        """Handle exchange picker from /whitelist_remove."""
        if not _is_admin(call.from_user.id):
            await call.answer("⛔ Только админ", show_alert=True)
            return
        try:
            _, uid_str, choice = _callback_payload(call).split(":", 2)
            target_uid = int(uid_str)
        except (ValueError, AttributeError):
            await call.answer("Ошибка callback", show_alert=True)
            return
        choice = (choice or "").lower().strip()
        if choice == "cancel":
            try:
                await call.message.edit_text(
                    f"❌ Отменено. White-list юзера <code>{target_uid}</code> не изменён.",
                    parse_mode="HTML",
                )
            except Exception:
                pass
            await call.answer("Отменено")
            return
        if choice not in ("bingx", "all"):
            await call.answer("Неизвестный выбор", show_alert=True)
            return
        new_set = await db.remove_user_whitelist_exchange(target_uid, choice)
        if not new_set:
            text = (
                f"🚫 Юзер <code>{target_uid}</code> полностью удалён из white-list.\n"
                "Теперь только наблюдение (preview-only)."
            )
        else:
            remaining = ", ".join(e.upper() for e in sorted(new_set))
            text = (
                f"➖ Убрали {choice.upper()}. Юзер <code>{target_uid}</code> "
                f"остался в white-list для: <b>{remaining}</b>"
            )
        try:
            await call.message.edit_text(text, parse_mode="HTML")
        except Exception:
            pass
        await call.answer("Готово")

        # ------------------------------------------------------------------
        # Admin White-list management UI (menu → "🛡 White-list (админ)")
        # ------------------------------------------------------------------
        # Callbacks:
        #   wl_menu:<page>            — paginated list of all registered users
        #   wl_user:<uid>             — drill into per-user card
        #   wl_grant:<uid>:<ex|all>   — grant exchange to user, then refresh card
        #   wl_revoke:<uid>:<ex|all>  — revoke exchange from user, then refresh card
        #   wl_add_prompt             — start FSM to add a user by ID

    def _format_user_card_text(u: dict) -> str:
        """Build the per-user info text shown above the action keyboard."""
        uname = html.escape(u.get("username") or "—")
        connected = u.get("connected_exchanges") or []
        connected_str = (
            ", ".join(html.escape(e.upper()) for e in connected) or "<i>нет API</i>"
        )
        grants = u.get("whitelist_exchanges") or set()
        if "all" in grants:
            wl_str = "BingX ✅"
        elif grants:
            wl_str = ", ".join(html.escape(e.upper()) for e in sorted(grants))
        else:
            wl_str = "—"
        mode = html.escape(str(u.get("mode") or "preview"))
        admin_badge = " 👑 (админ)" if u.get("is_admin") else ""
        uid = u.get("telegram_id")
        return (
            f"<b>👤 Юзер</b> <code>{uid}</code>{admin_badge}\n"
            f"Имя: @{uname}\n"
            f"Режим: <b>{mode}</b>\n"
            f"Биржа: <b>BingX</b>\n"
            f"BingX API: {connected_str}\n"
            f"White-list: <b>{wl_str}</b>"
        )

    @router.callback_query(F.data.startswith("wl_menu:"))
    @_owner_guarded_callback
    async def cb_wl_menu(call: CallbackQuery, state: FSMContext):
        if not _is_admin(call.from_user.id):
            await call.answer("⛔ Только админ", show_alert=True)
            return
            # If admin was in the "add user" FSM, clear it on navigation back
        try:
            await state.clear()
        except Exception:
            pass
        try:
            page = max(0, int(_callback_payload(call).split(":", 1)[1]))
        except (ValueError, IndexError):
            page = 0
        from app.bot.keyboards import wl_users_list_keyboard

        users = await db.list_users_with_exchanges()
        if not users:
            text = "📋 Никого нет в базе ещё. Юзеры появятся когда напишут /start."
        else:
            wl_count = sum(1 for u in users if u.get("whitelist_exchanges"))
            per_page = 8
            total_pages = max(1, (len(users) + per_page - 1) // per_page)
            current_page = min(page, total_pages - 1) + 1
            text = (
                f"🛡 <b>White-list — управление</b>\n\n"
                f"Всего юзеров: <b>{len(users)}</b>\n"
                f"В whitelist:   <b>{wl_count}</b>\n\n"
                f"Страница {current_page}/{total_pages}\n"
                f"Кликни на юзера чтобы открыть карточку."
            )
        try:
            await call.message.edit_text(
                text,
                parse_mode="HTML",
                reply_markup=wl_users_list_keyboard(
                    users, page, owner_id=call.from_user.id
                ),
            )
        except Exception:
            # Sometimes edit fails (message too old, identical content) — fall back
            await call.message.answer(
                text,
                parse_mode="HTML",
                reply_markup=wl_users_list_keyboard(
                    users, page, owner_id=call.from_user.id
                ),
            )
        await call.answer()

    async def _render_user_card(call: CallbackQuery, target_uid: int) -> None:
        """Shared helper: redraw the per-user white-list card."""
        from app.bot.keyboards import wl_user_card_keyboard

        users = await db.list_users_with_exchanges()
        target = next((u for u in users if u["telegram_id"] == target_uid), None)
        if target is None:
            # User not yet registered (no /start) — show a stub card
            grants = await db.get_user_whitelist_exchanges(target_uid)
            target = {
                "telegram_id": target_uid,
                "username": None,
                "is_admin": False,
                "mode": "preview",
                "active_exchange": "—",
                "connected_exchanges": [],
                "whitelist_exchanges": grants,
            }
        enabled = get_settings().enabled_exchanges
        text = _format_user_card_text(target)
        kb = wl_user_card_keyboard(
            target_uid,
            target.get("whitelist_exchanges") or set(),
            enabled,
            owner_id=call.from_user.id,
        )
        try:
            await call.message.edit_text(text, parse_mode="HTML", reply_markup=kb)
        except Exception:
            await call.message.answer(text, parse_mode="HTML", reply_markup=kb)

    @router.callback_query(F.data.startswith("wl_user:"))
    @_owner_guarded_callback
    async def cb_wl_user(call: CallbackQuery):
        if not _is_admin(call.from_user.id):
            await call.answer("⛔ Только админ", show_alert=True)
            return
        try:
            target_uid = int(_callback_payload(call).split(":", 1)[1])
        except (ValueError, IndexError):
            await call.answer("Bad target", show_alert=True)
            return
        await _render_user_card(call, target_uid)
        await call.answer()

    @router.callback_query(F.data.startswith("wl_grant:"))
    @_owner_guarded_callback
    async def cb_wl_grant(call: CallbackQuery):
        if not _is_admin(call.from_user.id):
            await call.answer("⛔ Только админ", show_alert=True)
            return
        try:
            _, uid_str, ex = _callback_payload(call).split(":", 2)
            target_uid = int(uid_str)
        except (ValueError, AttributeError):
            await call.answer("Bad payload", show_alert=True)
            return
        ex = (ex or "").lower().strip()
        if ex != "all" and not get_settings().is_exchange_enabled(ex):
            await call.answer(f"{ex.upper()} отключена в ENV", show_alert=True)
            return
        await db.ensure_user(target_uid)
        new_grants = await db.add_user_whitelist_exchange(target_uid, ex)
        await call.answer(
            "Дано: "
            + (
                "все"
                if "all" in new_grants
                else ", ".join(sorted(e.upper() for e in new_grants))
            ),
        )
        await _render_user_card(call, target_uid)

    @router.callback_query(F.data.startswith("wl_revoke:"))
    @_owner_guarded_callback
    async def cb_wl_revoke(call: CallbackQuery):
        if not _is_admin(call.from_user.id):
            await call.answer("⛔ Только админ", show_alert=True)
            return
        try:
            _, uid_str, ex = _callback_payload(call).split(":", 2)
            target_uid = int(uid_str)
        except (ValueError, AttributeError):
            await call.answer("Bad payload", show_alert=True)
            return
        ex = (ex or "").lower().strip()
        new_grants = await db.remove_user_whitelist_exchange(target_uid, ex)
        if not new_grants:
            await call.answer("Полностью удалён из white-list")
        else:
            await call.answer(
                "Осталось: " + ", ".join(sorted(e.upper() for e in new_grants))
            )
        await _render_user_card(call, target_uid)

    @router.callback_query(
        lambda call: parse_callback_owner(call.data)[1] == "wl_add_prompt"
    )
    @_owner_guarded_callback
    async def cb_wl_add_prompt(call: CallbackQuery, state: FSMContext):
        if not _is_admin(call.from_user.id):
            await call.answer("⛔ Только админ", show_alert=True)
            return
        await state.clear()
        await state.set_state(WhitelistAdd.waiting_uid)
        try:
            await call.message.edit_text(
                "➕ <b>Добавить юзера в white-list</b>\n\n"
                "Пришли <b>telegram_id</b> юзера (число) следующим сообщением.\n\n"
                "Чтобы юзер узнал свой ID — попроси его написать <code>/id</code> боту.\n\n"
                "Отмена: /cancel",
                parse_mode="HTML",
            )
        except Exception:
            await call.message.answer(
                "➕ Пришли telegram_id юзера. Отмена: /cancel",
                parse_mode="HTML",
            )
        await call.answer()

    @router.message(WhitelistAdd.waiting_uid)
    async def fsm_wl_uid(message: Message, state: FSMContext):
        if not _is_admin(message.from_user.id):
            await state.clear()
            await message.answer("⛔ Только админ.")
            return
        text = (message.text or "").strip()
        if text.startswith("/"):
            await state.clear()
            await message.answer("❌ Отменено.")
            return
        try:
            target_uid = int(text)
            if target_uid <= 0:
                raise ValueError
        except ValueError:
            await message.answer(
                "❌ Это не похоже на telegram_id (нужно положительное число).\n"
                "Попробуй ещё раз или /cancel."
            )
            return
        await state.clear()
        await db.ensure_user(target_uid)
        # Open the per-user card so the admin can grant BingX access
        from app.bot.keyboards import wl_user_card_keyboard

        users = await db.list_users_with_exchanges()
        target = next((u for u in users if u["telegram_id"] == target_uid), None)
        if target is None:
            target = {
                "telegram_id": target_uid,
                "username": None,
                "is_admin": False,
                "mode": "preview",
                "active_exchange": "—",
                "connected_exchanges": [],
                "whitelist_exchanges": await db.get_user_whitelist_exchanges(
                    target_uid
                ),
            }
        enabled = get_settings().enabled_exchanges
        await message.answer(
            "✅ Юзер найден. Управление доступом BingX:\n\n"
            + _format_user_card_text(target),
            parse_mode="HTML",
            reply_markup=wl_user_card_keyboard(
                target_uid,
                target.get("whitelist_exchanges") or set(),
                enabled,
                owner_id=message.from_user.id,
            ),
        )

    @router.message(Command("whitelist_list"))
    async def cmd_whitelist_list(message: Message):
        if not _is_admin(message.from_user.id):
            await message.answer("⛔ Только админ.")
            return
        users = await db.list_users_with_exchanges()
        wl_users = [u for u in users if u.get("whitelist_exchanges")]
        if not wl_users:
            await message.answer(
                "📋 White-list пуст.\n\n"
                "Никто из юзеров не может открывать сделки сейчас. "
                "Добавь юзера через <code>/whitelist_add USER_ID</code>",
                parse_mode="HTML",
            )
            return
        lines = [f"📋 <b>White-list ({len(wl_users)} юзеров):</b>", ""]
        for u in wl_users:
            grants = u["whitelist_exchanges"]
            if "all" in grants:
                wl_text = "все"
            else:
                wl_text = ", ".join(e.upper() for e in sorted(grants))
            uname = html.escape(u["username"] or "")
            uname_part = f" @{uname}" if uname else ""
            lines.append(
                f"  • <code>{u ['telegram_id']}</code>{uname_part} — {wl_text}"
            )
        lines.append("")
        lines.append("Управление: /whitelist_add /whitelist_remove")
        await message.answer("\n".join(lines), parse_mode="HTML")

    @router.message(Command("users"))
    async def cmd_users(message: Message):
        """Список всех юзеров и состояние BingX API (для админа).

        Использование:
          /users               — все юзеры
          /users bingx          — пользователи BingX
          /users wl            — только из white-list
        """
        if not _is_admin(message.from_user.id):
            await message.answer("⛔ Только админ.")
            return

        parts = (message.text or "").split()
        filter_kind = parts[1].lower().strip() if len(parts) > 1 else ""

        try:
            users = await db.list_users_with_exchanges()
        except Exception as exc:
            await message.answer(f"❌ Ошибка чтения БД: {exc}")
            return

        if not users:
            await message.answer("📋 Нет ни одного юзера в БД.")
            return

            # Применяем фильтр
        if filter_kind == "bingx":
            users = [u for u in users if u["active_exchange"] == filter_kind]
            filter_label = f" (биржа: {filter_kind.upper()})"
        elif filter_kind in ("wl", "whitelist", "white-list"):
            users = [u for u in users if u["whitelisted"]]
            filter_label = " (white-list)"
        else:
            filter_label = ""

        if not users:
            await message.answer(f"📋 Под фильтр '{filter_kind}' никто не подошёл.")
            return

            # Считаем пользователей BingX
        by_exchange: dict[str, int] = {}
        wl_count = 0
        auto_count = 0
        for u in users:
            by_exchange[u["active_exchange"]] = (
                by_exchange.get(u["active_exchange"], 0) + 1
            )
            if u["whitelisted"]:
                wl_count += 1
            if u["mode"] == "auto":
                auto_count += 1

                # Формируем сообщение (бьём на куски если >40 юзеров — Telegram message limit 4096)
        chunks: list[list[str]] = [[]]
        header = [
            f"👥 <b>Все юзеры бота{filter_label}: {len(users)}</b>",
            "",
            f"🏦 BingX: {len(users)}",
            f"✅ White-list: {wl_count} / {len(users)}",
            f"🤖 В авто-режиме: {auto_count} / {len(users)}",
            "",
            "━━━━━━━━━━━━━━━━━━━━━",
            "",
        ]
        chunks[0].extend(header)

        for u in users:
            uid = u["telegram_id"]
            # Telegram usernames are restricted to [a-zA-Z0-9_], but we still
            # escape defensively in case a malicious / bot-generated username
            # contains HTML-special chars and would break the message parser
            # (or worse, smuggle markup through parse_mode=HTML).
            raw_username = u["username"]
            username = html.escape(raw_username) if raw_username else "—"
            active = html.escape(u["active_exchange"].upper())
            connected = u["connected_exchanges"]

            # Статусные эмодзи
            wl_emoji = "✅" if u["whitelisted"] else "👁"
            admin_emoji = " 👑" if u["is_admin"] else ""
            mode_short = {"auto": "🤖", "preview": "👁", "off": "⏸"}.get(u["mode"], "?")

            # Состояние BingX API
            connected_str = ""
            if connected:
                marks = []
                for ex in connected:
                    ex_safe = html.escape(str(ex).upper())
                    if ex == u["active_exchange"]:
                        marks.append(f"<b>{ex_safe}</b>★")
                    else:
                        marks.append(ex_safe)
                connected_str = ", ".join(marks)
            else:
                connected_str = "<i>нет API</i>"

                # BingX white-list access
            wl_grants = u.get("whitelist_exchanges") or set()
            if "all" in wl_grants:
                wl_str = "все"
            elif wl_grants:
                wl_str = ", ".join(html.escape(e.upper()) for e in sorted(wl_grants))
            else:
                wl_str = "—"

            line = (
                f"{wl_emoji}{mode_short} <code>{uid}</code>{admin_emoji} "
                f"@{username if username !='—'else '—'}\n"
                f"     Активна: {active}  •  API: {connected_str}\n"
                f"     WL: {wl_str}"
            )

            # Проверяем размер последнего chunk — Telegram limit ≈ 4096 chars
            current_size = sum(len(s) for s in chunks[-1])
            if current_size + len(line) > 3500:
                chunks.append([])
            chunks[-1].append(line)

            # Легенда в первом chunk
        chunks[0].append("")
        chunks[0].append(
            "<i>Легенда: ✅=whitelist 👁=preview-only 👑=админ "
            "🤖=auto ⏸=off  API=BingX API  WL=доступ к автоторговле BingX</i>"
        )

        # Отправляем
        for i, chunk in enumerate(chunks):
            if i == 0:
                text = "\n".join(chunk)
            else:
                text = f"<b>Часть {i +1}/{len(chunks)}</b>\n\n" + "\n".join(chunk)
            await message.answer(text, parse_mode="HTML")

    @router.message()
    async def all_messages(message: Message):
        if getattr(message.chat, "type", "") == "private" and getattr(
            message, "from_user", None
        ):
            mark_private_chat_ready(message.from_user.id)
        text = message.text or message.caption or ""
        if not text:
            return
        sender_id = _sender_id(message)
        sender_username = _sender_username(message)
        source_feed_only = admin_only_enabled() and not _is_admin(sender_id)
        low_text = text.lower().strip()
        if not source_feed_only and low_text in {"меню", "menu"}:
            await message.answer(
                await _menu_text_bounded(message.from_user.id, message.from_user.username),
                reply_markup=_home_menu(message.from_user.id),
            )
            return
        if not source_feed_only and low_text in {"статистика", "аналитика", "статистика сигналов"}:
            user_id = _sender_id(message)
            await message.answer(
                await _signal_analytics_text(user_id),
                reply_markup=(
                    signal_analytics_admin_menu(owner_id=user_id)
                    if _is_admin(user_id)
                    else _home_menu(user_id)
                ),
            )
            return
        if not source_feed_only and low_text in {"скачать статистику", "выгрузить статистику"}:
            await _send_signal_analytics_export(message, _sender_id(message))
            return
        if not source_feed_only and low_text in {"соглашение", "риски", "terms", "условия"}:
            if sender_id is not None:
                await _send_terms_prompt(message, sender_id, sender_username)
            return
        if not source_feed_only and low_text in {"баланс", "б", "balance"}:
            if sender_id is not None:
                await _handle_balance(message, sender_id, sender_username)
            return
        if not source_feed_only and low_text in {"позиции", "позиция", "сделки", "открытые", "positions"}:
            if sender_id is not None:
                positions_text, position_rows = await _positions_view(
                    sender_id, sender_username
                )
                await message.answer(
                    positions_text,
                    reply_markup=positions_list_menu(position_rows, owner_id=sender_id),
                )
            return
            # VIP setting commands are user-specific. Channel posts may not have from_user;
            # do not let such posts crash the router before signal parsing/stale guard.
        if (
            not source_feed_only
            and sender_id is not None
            and await _handle_vip_command(message, text, sender_id, sender_username)
        ):
            return

            # Critical safety: do not execute stale Telegram updates after redeploy/offline.
            # Telegram can deliver queued messages after bot restart; old VIP signals must not
            # be opened later when market context is already different. Commands above still work.
        if _is_stale_vip_signal_message(message):
            if get_settings().VIP_DEBUG_LOGS and (
                _is_admin(sender_id) or message.chat.type == "private"
            ):
                age = _message_age_seconds(message)
                await message.answer(
                    ensure_visual_card(
                        "⏳ VIP-сигнал пропущен\n"
                        f"🕒 Возраст сообщения: ~{int(age or 0)} сек\n"
                        "🔄 Отправьте сигнал заново, если он ещё актуален."
                    )
                )
            return

        sig = None
        try:
            sig = parse_signal(text)
        except Exception as exc:
            # Валидировать строго, но не спамить на обычные посты.
            if get_settings().VIP_DEBUG_LOGS and not admin_only_enabled():
                await message.answer(
                    ensure_visual_card(f"⚠️ VIP-сигнал не принят\n❌ Причина: {exc}")
                )
            return
        if not sig:
            return

        original_signal_symbol = str(sig.symbol or "").upper()
        sig = canonicalize_bingx_1000_signal(sig)
        if str(sig.symbol).upper() != original_signal_symbol:
            log.info(
                "BINGX_1000_CONTRACT_ALIAS_APPLIED original=%s canonical=%s multiplier=1000",
                original_signal_symbol,
                str(sig.symbol).upper(),
            )

        allowed, source_reason = _vip_signal_source_allowed(message)
        if not allowed:
            if (
                get_settings().VIP_DEBUG_LOGS
                and not admin_only_enabled()
                and (
                    _is_admin(sender_id)
                    or getattr(message.chat, "type", "") != "private"
                )
            ):
                await message.answer(
                    ensure_visual_card(f"⏭ Сигнал пропущен\n{source_reason}")
                )
            return

            # Monotonic timestamp used by the process-wide queue. It is deliberately
            # taken only after parsing/source validation so ordinary chat messages do
            # not affect load metrics.
        signal_received_monotonic = time.monotonic()

        # Determine recipients based on signal source:
        # - DM from admin → open only for that admin (manual personal trade)
        # - Group/channel signal → open for whitelisted auto-mode users only
        #   Non-whitelisted users (preview mode or auto-without-whitelist) get
        #   preview-only notifications without trades being opened.
        chat_type = getattr(getattr(message, "chat", None), "type", "") or ""
        if chat_type == "private" and _is_admin(sender_id):
            recipients = [sender_id]
            preview_only_users: list[int] = []
            log.info("Admin DM signal: opening only for admin uid=%s", sender_id)
        else:
            recipients = await db.auto_recipients()
            preview_only_users = await db.preview_recipients()
            if admin_only_enabled():
                admin_set = configured_admin_ids()
                recipients = [uid for uid in recipients if int(uid) in admin_set]
                preview_only_users = []
                log.info(
                    "ADMIN_ONLY_SIGNAL_FANOUT symbol=%s eligible_admins=%s",
                    str(sig.symbol).upper(),
                    len(recipients),
                )

        # Claim the parsed signal once for the whole fan-out before any shared
        # market read, trade-group creation or account execution.  The reserved
        # dedup recipient ``0`` cannot collide with real Telegram user IDs and
        # reuses the already-atomic PostgreSQL/SQLite claim path.  The signal
        # hash includes the source chat and all trading levels, so a repeated
        # symbol with different ENTRY/STOP/TP remains a new signal.
        ingress_signal_hash = signal_hash(sig, message.chat.id)
        ingress_source_message_id = getattr(message, "message_id", None)
        ingress_claim_token = (
            f"ingress:{message.chat.id}:{ingress_source_message_id}:"
            f"{secrets.token_urlsafe(12)}"
        )
        try:
            ingress_claimed = await db.claim_duplicate(
                ingress_signal_hash,
                message.chat.id,
                ingress_claim_token,
                0,
            )
        except Exception as exc:
            # Do not turn a dedup-storage outage into silent loss of every
            # signal. Existing per-user atomic claims and same-symbol guards
            # remain the final protection; make the degraded state explicit.
            ingress_claimed = True
            log.exception(
                "SIGNAL_INGRESS_CLAIM_FAILED_FAIL_OPEN chat_id=%s message_id=%s symbol=%s error=%s",
                message.chat.id,
                ingress_source_message_id,
                str(sig.symbol).upper(),
                f"{type(exc).__name__}: {exc}",
            )
        source_chat = getattr(message, "chat", None)
        sender_chat = getattr(message, "sender_chat", None)

        def _queue_analytics_observation(*, duplicate: bool) -> None:
            # g5b3g analytics ingress: one synchronous put_nowait after trusted
            # source validation. Exact duplicate Telegram messages are also
            # observed so duplicate_count remains measurable, but they still
            # return before any public/private BingX request or trade action.
            analytics_queued = submit_signal_analytics_shadow(
                sig,
                content_fingerprint=ingress_signal_hash,
                source_chat_id=int(message.chat.id),
                source_message_id=ingress_source_message_id,
                source_title=(
                    getattr(source_chat, "title", None)
                    or getattr(source_chat, "full_name", None)
                    or getattr(source_chat, "username", None)
                ),
                sender_chat_id=getattr(sender_chat, "id", None),
                sender_chat_title=(
                    getattr(sender_chat, "title", None)
                    or getattr(sender_chat, "username", None)
                ),
                published_at=getattr(message, "date", None),
            )
            if analytics_queued:
                log.info(
                    "SIGNAL_ANALYTICS_SHADOW_QUEUED chat_id=%s message_id=%s "
                    "symbol=%s fingerprint=%s duplicate=%s",
                    message.chat.id,
                    ingress_source_message_id,
                    str(sig.symbol).upper(),
                    ingress_signal_hash[:16],
                    int(duplicate),
                )

        if not ingress_claimed:
            _queue_analytics_observation(duplicate=True)
            log.warning(
                "SIGNAL_INGRESS_DUPLICATE_SUPPRESSED chat_id=%s message_id=%s signal_id=%s symbol=%s fingerprint=%s",
                message.chat.id,
                ingress_source_message_id,
                str(getattr(sig, "signal_id", "") or "-"),
                str(sig.symbol).upper(),
                ingress_signal_hash[:16],
            )
            return
        log.info(
            "SIGNAL_INGRESS_CLAIMED chat_id=%s message_id=%s signal_id=%s symbol=%s fingerprint=%s",
            message.chat.id,
            ingress_source_message_id,
            str(getattr(sig, "signal_id", "") or "-"),
            str(sig.symbol).upper(),
            ingress_signal_hash[:16],
        )
        _queue_analytics_observation(duplicate=False)

            # Obtain one public BingX price for the whole signal fan-out. It is used
            # both for MARKET sizing and for the fail-safe decimal-shift/anomalous
            # LIMIT price guard. Every account still confirms its own actual fill.
        shared_market_entry_hint = 0.0
        if recipients or preview_only_users or get_settings().admin_ids:
            try:
                _public_adapter = BingxAdapter(
                    "",
                    "",
                    testnet=bool(get_settings().BINGX_VST),
                    timeout_ms=int(get_settings().BINGX_REQUEST_TIMEOUT_SECONDS) * 1000,
                )
                shared_market_entry_hint = float(
                    await _public_adapter.fetch_last_price(sig.symbol)
                )
                log.info(
                    "shared signal market price symbol=%s price=%s recipients=%s",
                    str(sig.symbol).upper(),
                    shared_market_entry_hint,
                    len(recipients),
                )
            except Exception as exc:
                # Safe fallback: each account executor may request the public
                # price through the shared short TTL cache. If all public reads
                # fail, BingX's own price-band validation remains the fallback.
                log.warning(
                    "shared signal market price unavailable symbol=%s error=%s",
                    str(sig.symbol).upper(),
                    f"{type(exc).__name__}: {exc}",
                )
                shared_market_entry_hint = 0.0

                # Signal-wide fail-safe: reject an obvious decimal-shift/extreme price
                # mismatch before creating a trade group, reading private account state
                # or sending any private BingX write. The per-account executor repeats the
                # same guard as defense in depth for alternate/direct call paths.
        signal_price_anomaly = detect_signal_price_anomaly(
            getattr(sig, "entry", 0.0),
            shared_market_entry_hint,
            max_price_ratio=float(get_settings().MAX_SIGNAL_ENTRY_PRICE_RATIO or 0.0),
        )
        anomaly_payload: dict[str, object] = {}
        if signal_price_anomaly is not None:
            anomaly_payload = signal_price_anomaly.as_payload()
            settings_now = get_settings()
            anomaly_payload.update(
                decimal_normalization_preview_payload(
                    sig,
                    shared_market_entry_hint,
                    enabled=bool(
                        settings_now.SIGNAL_DECIMAL_NORMALIZATION_PREVIEW_ENABLED
                    ),
                    max_deviation_after_percent=float(
                        settings_now.SIGNAL_DECIMAL_NORMALIZATION_MAX_DEVIATION_PERCENT
                        or 0.0
                    ),
                    max_power=int(
                        settings_now.SIGNAL_DECIMAL_NORMALIZATION_MAX_POWER or 0
                    ),
                )
            )
            all_notify_ids = sorted(
                {
                    *[int(uid) for uid in recipients],
                    *[int(uid) for uid in preview_only_users],
                    *[int(uid) for uid in get_settings().admin_ids],
                }
            )
            anomaly_hash = ingress_signal_hash
            claim_tokens = {
                uid: f"{sig.signal_id or 'signal'}#anomaly:{secrets.token_urlsafe(12)}"
                for uid in all_notify_ids
            }
            claimed = await asyncio.gather(
                *(
                    db.claim_duplicate(
                        anomaly_hash,
                        message.chat.id,
                        claim_tokens[uid],
                        uid,
                    )
                    for uid in all_notify_ids
                ),
                return_exceptions=True,
            )
            notify_ids: list[int] = []
            for uid, claim in zip(all_notify_ids, claimed, strict=True):
                if claim is True:
                    notify_ids.append(uid)
                elif isinstance(claim, asyncio.CancelledError):
                    raise claim
                elif isinstance(claim, BaseException):
                    # A storage failure must not hide a safety warning. The
                    # trade remains blocked; only duplicate suppression degrades.
                    log.error(
                        "blocked signal dedup claim failed uid=%s symbol=%s error=%s",
                        uid,
                        str(sig.symbol).upper(),
                        f"{type(claim).__name__}: {claim}",
                    )
                    notify_ids.append(uid)

            if not notify_ids:
                log.info(
                    "duplicate signal-wide price anomaly suppressed symbol=%s recipients=%s",
                    str(sig.symbol).upper(),
                    len(all_notify_ids),
                )
                return

            anomaly_results = [
                ExecutionResult(
                    uid,
                    "skipped",
                    "аномальное расхождение цены сигнала и текущей цены BingX",
                    mandatory_trade_warning_payload(
                        "signal_price_anomaly",
                        {
                            "exchange": "bingx",
                            **anomaly_payload,
                        },
                    ),
                )
                for uid in notify_ids
            ]

            async def _notify_blocked_price(uid: int, result: ExecutionResult) -> None:
                try:
                    is_admin_recipient = int(uid) in {
                        int(x) for x in get_settings().admin_ids
                    }
                    if (
                        is_admin_recipient
                        and result.payload.get("decimal_normalization_preview") is True
                    ):
                        text = admin_decimal_normalization_preview_message(sig, result)
                    else:
                        text = user_result_message(sig, result)
                    delivered, delivery_error = await _send_trade_result_notification(
                        message.bot, uid, text
                    )
                    result.payload["notification_delivered"] = bool(delivered)
                    if delivery_error:
                        result.payload["notification_error"] = delivery_error
                except Exception as exc:
                    result.payload["notification_delivered"] = False
                    result.payload["notification_error"] = (
                        f"{type(exc).__name__}: {exc}"
                    )
                    log.exception(
                        "blocked price notification failed uid=%s symbol=%s",
                        uid,
                        str(sig.symbol).upper(),
                    )
                if result.payload.get("notification_delivered") is not True:
                    try:
                        await db.release_duplicate(
                            anomaly_hash,
                            uid,
                            expected_signal_id=claim_tokens[uid],
                        )
                    except Exception:
                        log.exception(
                            "failed to release undelivered anomaly dedup uid=%s symbol=%s",
                            uid,
                            str(sig.symbol).upper(),
                        )

            await asyncio.gather(
                *(
                    _notify_blocked_price(uid, result)
                    for uid, result in zip(notify_ids, anomaly_results, strict=True)
                ),
                return_exceptions=True,
            )
            log.warning(
                "signal-wide price anomaly blocked before BingX write; signal-wide price anomaly blocked before BingX write symbol=%s entry=%s current=%s ratio=%.6f notified=%s",
                str(sig.symbol).upper(),
                signal_price_anomaly.signal_entry,
                signal_price_anomaly.current_price,
                signal_price_anomaly.price_ratio,
                len(notify_ids),
            )
            anomaly_summary = admin_batch_summary(sig, anomaly_results)
            if admin_only_enabled():
                await asyncio.gather(
                    *[
                        send_queued_private_message(
                            message.bot,
                            admin_id,
                            anomaly_summary,
                            parse_mode="HTML",
                            attempts=2,
                            log_context="admin anomaly summary",
                        )
                        for admin_id in sorted(configured_admin_ids())
                    ],
                    return_exceptions=True,
                )
            elif _is_admin(sender_id) or message.chat.type in {
                "private",
                "group",
                "supergroup",
                "channel",
            }:
                await message.answer(anomaly_summary)
            return

            # Create one durable common trading plan for every user execution of
            # this signal. The public BingX price monitor watches this plan once per
            # symbol, while every account remains independently verified.
        trade_group_id: int | None = None
        if recipients:
            try:
                trade_group_id = await db.create_trade_group(
                    signal_hash=ingress_signal_hash,
                    symbol=sig.symbol,
                    side=sig.side.value,
                    entry_type=str(getattr(sig, "order_type", "LIMIT") or "LIMIT"),
                    planned_entry=float(sig.entry or 0.0),
                    stop_price=float(sig.stop),
                    targets_json=json.dumps(list(sig.targets), ensure_ascii=False),
                    source_chat_id=message.chat.id,
                    source_message_id=getattr(message, "message_id", None),
                )
                log.info(
                    "trade group created group_id=%s symbol=%s recipients=%s",
                    trade_group_id,
                    str(sig.symbol).upper(),
                    len(recipients),
                )
                submit_statistics_trade_group_linkage(trade_group_id)
            except Exception:
                # Group monitoring is an acceleration layer. Fail open to the
                # proven per-account execution path; periodic private-account
                # reconciliation still protects the trade.
                log.exception("failed to create trade group for %s", sig.symbol)
                trade_group_id = None

                # All signals share one process-wide dispatcher.  v1.6.5 created a
                # separate Semaphore(10) for every Telegram signal, so three simultaneous
                # signals could start 30 account executions. The dispatcher keeps the
                # configured limit global and rejects stale/overflowed entries safely.
        dispatcher = get_trade_dispatcher()

        async def _open_for_user(uid: int):
            # For group/channel auto-signals, never open a live position when the
            # bot cannot reach the user in private. The probe itself uses the
            # independent Telegram queue and cannot occupy a BingX trade worker.
            if not (chat_type == "private" and _is_admin(sender_id)):
                dm = await probe_queued_private_chat(message.bot, uid)
                if not dm.delivered:
                    result = ExecutionResult(
                        uid,
                        "skipped",
                        "ЛС бота недоступны: откройте бота и нажмите /start",
                        mandatory_trade_warning_payload(
                            "dm_unavailable",
                            {
                                "dm_unavailable": True,
                                "notification_delivered": False,
                                "notification_error": dm.error or dm.code,
                            },
                        ),
                    )
                    log.warning(
                        "signal skipped before execution uid=%s symbol=%s reason=dm_unavailable code=%s",
                        uid,
                        sig.symbol,
                        dm.code,
                    )
                    return result

            async def _execute(dispatch_context: dict):
                return await execute_signal_for_user(
                    sig,
                    uid,
                    source_chat_id=message.chat.id,
                    trade_group_id=trade_group_id,
                    market_entry_hint=shared_market_entry_hint,
                    dispatch_context=dispatch_context,
                )

            result = await dispatcher.submit(
                _execute,
                user_id=uid,
                symbol=sig.symbol,
                signal_received_at=signal_received_monotonic,
                entry_type=str(getattr(sig, "order_type", "LIMIT") or "LIMIT"),
            )
            # The dispatcher adds final queue/execution/total timings only after
            # execute_signal_for_user() returns. Persist that completed snapshot
            # without changing the confirmed trade result. Preflight skips have
            # no execution row and simply return False.
            try:
                await db.merge_latest_execution_metadata(
                    ingress_signal_hash,
                    uid,
                    {"dispatch": dict(result.payload.get("dispatch") or {})},
                )
            except Exception:
                log.exception(
                    "failed to persist final dispatch metrics uid=%s symbol=%s",
                    uid,
                    sig.symbol,
                )

                # Return the confirmed execution immediately. Telegram delivery is
                # deliberately performed only after the common trade group has been
                # finalized, so event-driven monitoring never waits for a slow or
                # flood-limited Telegram send.
            return result

        raw_results = await asyncio.gather(
            *[_open_for_user(uid) for uid in recipients],
            return_exceptions=True,
        )
        # Wait for every follower task before publishing the group. An unexpected
        # exception in one account must not finalize the group while slower
        # account executions are still being linked to it.
        results = []
        for uid, item in zip(recipients, raw_results, strict=True):
            if isinstance(item, BaseException):
                log.error(
                    "unexpected follower task failure uid=%s symbol=%s: %s",
                    uid,
                    sig.symbol,
                    item,
                    exc_info=(type(item), item, item.__traceback__),
                )
                results.append(
                    ExecutionResult(uid, "error", f"internal task error: {item}")
                )
            else:
                results.append(item)

        if trade_group_id is not None:
            try:
                final_group_status = await db.finalize_trade_group(trade_group_id)
                log.info(
                    "trade group finalized group_id=%s status=%s recipients=%s",
                    trade_group_id,
                    final_group_status or "missing",
                    len(recipients),
                )
                # Re-submit after the full fan-out so every durable execution row
                # can be projected even if its earlier queue item was dropped.
                submit_statistics_trade_group_linkage(trade_group_id)
            except Exception:
                # The 30-second private-account reconcile remains active even
                # if publishing the acceleration group fails. A stale building
                # group is recovered automatically after restart/timeout.
                log.exception("failed to finalize trade group id=%s", trade_group_id)

                # Deliver user results only after the group is active. This keeps
                # Telegram completely outside the monitoring activation barrier.

        async def _notify_execution_result(uid: int, result):
            if not await _trade_result_notification_allowed(
                uid, result, symbol=sig.symbol
            ):
                return
            quarantine_card_rendered = False
            try:
                notification_text = user_result_message(sig, result)
                quarantine_card_rendered = bool(
                    result.status == "skipped"
                    and result.payload.get("api_permission_quarantine") is True
                    and result.payload.get("api_quarantine_active") is True
                )
            except Exception as exc:
                log.exception(
                    "Failed to render trade notification uid=%s status=%s: %s",
                    uid,
                    getattr(result, "status", "unknown"),
                    exc,
                )
                notification_text = ensure_visual_card(
                    "🚨 Уведомление о сделке не удалось сформировать\n"
                    f"Пара: {sig.symbol}\n"
                    f"Статус исполнения: {getattr(result, 'status', 'unknown')}\n"
                    "Проверьте позицию и защитные ордера на BingX."
                )
            delivered, delivery_error = await _send_trade_result_notification(
                message.bot, uid, notification_text
            )
            result.payload["notification_delivered"] = bool(delivered)
            if delivery_error:
                result.payload["notification_error"] = delivery_error
            await _finalize_user_api_quarantine_notification(
                int(uid),
                result,
                delivered=bool(delivered),
                quarantine_card_rendered=quarantine_card_rendered,
            )

        await asyncio.gather(
            *[
                _notify_execution_result(uid, result)
                for uid, result in zip(recipients, results, strict=True)
            ],
            return_exceptions=True,
        )

        # The same explicit safety card is also delivered to every configured
        # administrator. One signal produces one admin card, not one per user.
        await _notify_admins_signal_price_anomaly(message.bot, sig, list(results))
        await _notify_admins_api_quarantine(message.bot, sig, list(results))

        # Send preview-only signal text to non-whitelisted users (they see the
        # signal but no trade is opened on their account).
        if preview_only_users:
            preview_text = whitelist_preview_message(sig)

            async def _notify_preview(uid: int):
                outcome = await send_queued_private_message(
                    message.bot,
                    uid,
                    preview_text,
                    parse_mode="HTML",
                    attempts=2,
                    log_context="preview notification",
                )
                if not outcome.delivered:
                    if outcome.code in {
                        "dm_forbidden",
                        "recipient_is_bot",
                        "invalid_recipient",
                        "cached_unavailable",
                    }:
                        log.info(
                            "preview notification skipped uid=%s code=%s",
                            uid,
                            outcome.code,
                        )
                    else:
                        log.warning(
                            "preview notification skipped uid=%s code=%s",
                            uid,
                            outcome.code,
                        )

            await asyncio.gather(
                *[_notify_preview(uid) for uid in preview_only_users],
                return_exceptions=True,
            )

        summary_text = admin_batch_summary(sig, list(results))
        await _send_signal_batch_summary_to_source(message, summary_text)
