"""Dialog funnel after First DM: promo, apology, optional link help, Q&A and opt-out."""

from __future__ import annotations

import asyncio
import datetime as dt
import inspect
import math
import random
import weakref
from typing import Set, Tuple

from loguru import logger

from config import DIALOG_FLOW_VARIANT
from telethon.errors import (
    FloodWaitError,
    PeerFloodError,
    UserIsBlockedError,
    UserPrivacyRestrictedError,
    ChatWriteForbiddenError,
    PeerIdInvalidError,
)
from telethon.tl.types import InputPeerUser

from services import account_auth
from services import ai_dialog
from services import dialog_inbox
from services import dialog_store as store
from services import dialog_delivery
from services import first_dm_delivery
from services import monitor as monitor_svc
from services import opt_out as opt_out_svc
from services import pacing
from services import phrases as phrases_svc
from services import telegram_history

# Prevent parallel handling of the same dialog.
_inflight: Set[Tuple[int, int]] = set()
_dialog_locks: weakref.WeakValueDictionary[Tuple[int, int], asyncio.Lock] = (
    weakref.WeakValueDictionary()
)


class DeliveryPendingError(RuntimeError):
    """Telegram delivery is ambiguous and must be reconciled before retry."""


def _now() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)


def _delay_reply() -> float:
    from services import runtime as runtime_svc

    lo, hi = runtime_svc.get_ai_reply_delay_range()
    return float(random.randint(lo, hi))


def _auto_link_delay() -> int:
    """Return the configured delay between automatic funnel messages."""
    from services import runtime as runtime_svc

    lo, hi = runtime_svc.get_auto_link_delay_range()
    return random.randint(lo, hi)


def _reserved_automatic_slots(stage: str) -> int:
    """Reserve mandatory automatic slots inside the five-message cap."""
    if DIALOG_FLOW_VARIANT in {2, 3}:
        return 1 if stage == store.STAGE_PROMO_SENT else 0
    if stage == store.STAGE_PROMO_SENT:
        return 2
    if stage == store.STAGE_APOLOGY_SENT:
        return 1
    return 0


async def _cleanup_disabled_account(account_user_id: int) -> None:
    try:
        await monitor_svc.maybe_disconnect_inactive_account(account_user_id)
    except Exception as exc:
        logger.debug("dialog account cleanup failed account={}: {}", account_user_id, exc)


def _link_retry_at() -> str:
    return (_now() + dt.timedelta(minutes=15)).isoformat()


def _account_cooldown_seconds(account_user_id: int) -> float:
    """Return the cold-outreach cooldown used only by First DM work."""
    return pacing.account_cooldown_seconds(int(account_user_id))


def _retry_at_for_account(account_user_id: int, fallback_seconds: int) -> str:
    remaining = _account_cooldown_seconds(account_user_id)
    delay = max(1, math.ceil(remaining) + 1) if remaining > 0 else max(1, int(fallback_seconds))
    return (_now() + dt.timedelta(seconds=delay)).isoformat()


_FOLLOWUP_PEERFLOOD_RETRY_SECONDS = 6 * 60 * 60
_DIALOG_PEERFLOOD_RETRY_SECONDS = 60


def _global_pre_reply_blocked(
    account_user_id: int,
    target_user_id: int,
    prepared_row: dict | None = None,
) -> bool:
    """Block autonomous pre-reply delivery while global First DM is paused."""
    from services import runtime as runtime_svc

    if not runtime_svc.is_worker_state_initialized():
        return False
    if runtime_svc.is_worker_enabled():
        return False
    row = prepared_row or {}
    if row.get("source_inbox_id") is not None:
        return False
    return not store.has_incoming_reply(target_user_id, account_user_id)


async def on_first_dm_sent(target_user_id: int, account_user_id: int, text: str) -> None:
    store.create_after_first_dm(target_user_id, account_user_id, text)


async def handle_incoming_private(
    account_user_id: int,
    target_user_id: int,
    text: str,
    *,
    telegram_message_id: int | None = None,
    received_at: str | None = None,
    content_kind: str = "text",
) -> None:
    """Persist and sequentially process any private reaction for one dialog.

    Text, voice notes, stickers, GIFs, photos, videos and emoji-only messages enter
    the same durable inbox. Non-text content is never transcribed or interpreted.
    """
    text = (text or "").strip()
    content_kind = str(content_kind or "text")
    if not text:
        if content_kind == "text":
            return
        text = "[нетекстовая реакция]"

    key = (int(account_user_id), int(target_user_id))
    dialog = store.get_dialog(target_user_id)
    if not dialog:
        return
    if int(dialog.get("account_user_id") or 0) != int(account_user_id):
        return
    if dialog.get("stage") == store.STAGE_CLOSED:
        return

    # Only textual stop requests can trigger the immediate safety stop. Every voice,
    # sticker, emoji and other non-text reaction is approved as neutral or positive.
    hard_stop = (
        content_kind == "text"
        and not ai_dialog.is_emoji_only(text)
        and ai_dialog.is_hard_stop(text)
    )
    row_id = dialog_inbox.enqueue(
        account_user_id,
        target_user_id,
        text,
        telegram_message_id=telegram_message_id,
        received_at=received_at,
        is_hard_stop=hard_stop,
        content_kind=content_kind,
    )
    if row_id is None:
        return

    if hard_stop:
        # Persist opt-out immediately so an already-running reply cannot continue.
        opt_out_svc.add(target_user_id, "user_stop")
        dialog_inbox.ignore_pending_for_target(
            target_user_id,
            "superseded_by_hard_stop",
            include_hard_stop=False,
        )

    lock = _dialog_locks.setdefault(key, asyncio.Lock())
    async with lock:
        _inflight.add(key)
        try:
            await _drain_dialog_inbox(account_user_id, target_user_id)
        finally:
            _inflight.discard(key)


async def _drain_dialog_inbox(account_user_id: int, target_user_id: int) -> None:
    """Drain all saved messages for one dialog in deterministic order."""
    while True:
        # PeerFlood and local account cooldowns protect only cold outreach. A real
        # incoming reply must be processed immediately by the account that owns the
        # dialog. Telegram delivery errors are handled per outbox action below.
        row = dialog_inbox.claim_next(account_user_id, target_user_id)
        if row is None:
            return
        row_id = int(row["id"])
        text = str(row.get("text") or "").strip()
        content_kind = str(row.get("content_kind") or "text")
        is_hard_stop = bool(int(row.get("is_hard_stop") or 0))
        try:
            history_already_appended = bool(int(row.get("history_appended") or 0))
            if not history_already_appended:
                store.append_history(target_user_id, "user", text)
                dialog_inbox.mark_history_appended(row_id)
                history_already_appended = True
            if is_hard_stop:
                await _process_hard_stop(
                    account_user_id,
                    target_user_id,
                    text,
                    history_already_appended=history_already_appended,
                    source_inbox_id=row_id,
                )
                dialog_inbox.mark_done(row_id)
                dialog_inbox.ignore_pending_for_target(
                    target_user_id,
                    "ignored_after_hard_stop",
                    include_hard_stop=True,
                )
                return

            if opt_out_svc.is_opted_out(target_user_id):
                dialog_inbox.mark_ignored(row_id, "target_opted_out")
                continue

            body_kwargs = {"history_already_appended": history_already_appended}
            body_signature = inspect.signature(_handle_incoming_private_body)
            accepts_kwargs = any(
                parameter.kind is inspect.Parameter.VAR_KEYWORD
                for parameter in body_signature.parameters.values()
            )
            if "source_inbox_id" in body_signature.parameters or accepts_kwargs:
                body_kwargs["source_inbox_id"] = row_id
            if "content_kind" in body_signature.parameters or accepts_kwargs:
                body_kwargs["content_kind"] = content_kind
            await _handle_incoming_private_body(
                account_user_id,
                target_user_id,
                text,
                **body_kwargs,
            )
            dialog_inbox.mark_done(row_id)
        except DeliveryPendingError as exc:
            dialog_inbox.requeue(row_id, str(exc))
            logger.info(
                "Incoming dialog delivery deferred account={} target={} inbox_id={} reason={}",
                account_user_id,
                target_user_id,
                row_id,
                exc,
            )
            return
        except asyncio.CancelledError:
            if is_hard_stop:
                dialog_inbox.requeue(row_id, "hard_stop_worker_cancelled")
            elif opt_out_svc.is_opted_out(target_user_id):
                dialog_inbox.mark_ignored(row_id, "cancelled_after_opt_out")
            else:
                dialog_inbox.requeue(row_id, "worker_cancelled")
            raise
        except Exception as exc:
            dialog_inbox.requeue(row_id, f"{type(exc).__name__}: {exc}")
            logger.exception(
                "Incoming dialog processing failed account={} target={} inbox_id={}: {}",
                account_user_id,
                target_user_id,
                row_id,
                exc,
            )
            return


async def _process_hard_stop(
    account_user_id: int,
    target_user_id: int,
    text: str,
    *,
    history_already_appended: bool = False,
    source_inbox_id: int | None = None,
) -> None:
    """Send one terminal reply and permanently stop every account from contacting user."""
    if not history_already_appended:
        store.append_history(target_user_id, "user", text)
    dialog = store.get_dialog(target_user_id) or {}
    outgoing = int(dialog.get("outgoing_count") or 0)
    if outgoing >= store.MAX_OUTGOING:
        store.set_stage(target_user_id, store.STAGE_CLOSED, clear_auto_link=True)
        store.mark_contact_completed(target_user_id)
        await _cleanup_disabled_account(account_user_id)
        return

    history = dialog.get("history") or []
    category = await ai_dialog.classify_user_message(
        history,
        text=text,
        content_kind="text",
    )
    if category not in {
        ai_dialog.CATEGORY_STOP_REQUEST,
        ai_dialog.CATEGORY_AGGRESSIVE_REFUSAL,
    }:
        category = ai_dialog.local_category(text)
        if category not in {
            ai_dialog.CATEGORY_STOP_REQUEST,
            ai_dialog.CATEGORY_AGGRESSIVE_REFUSAL,
        }:
            category = ai_dialog.CATEGORY_STOP_REQUEST

    reply = await ai_dialog.generate_terminal_reply(history, category=category)
    result = await _deliver_inbox_message(
        account_user_id,
        target_user_id,
        reply,
        message_kind=dialog_delivery.KIND_STOP_CLOSE,
        source_inbox_id=source_inbox_id,
        transition={
            "stage": store.STAGE_CLOSED,
            "bump_outgoing": True,
            "clear_auto_link": True,
            "append_history": True,
            "mark_contact_completed": True,
        },
        allow_opt_out=True,
    )
    if result != "sent":
        logger.warning(
            "Terminal stop reply failed target={} account={} category={}",
            target_user_id,
            account_user_id,
            category,
        )
        store.set_stage(target_user_id, store.STAGE_CLOSED, clear_auto_link=True)
        store.mark_contact_completed(target_user_id)
    opt_out_svc.add(target_user_id, category)
    logger.info("Opt-out target={} reason={}", target_user_id, category)
    await _cleanup_disabled_account(account_user_id)


async def recover_pending_incoming_messages(
    limit: int = 100,
    *,
    reset_stale_processing: bool = False,
    stale_after_seconds: int = 300,
) -> int:
    """Resume durable incoming messages left pending by a restart."""
    if reset_stale_processing:
        reset = dialog_inbox.reset_stale_processing(
            older_than_seconds=stale_after_seconds
        )
        if reset:
            logger.warning("Recovered {} stale incoming message(s)", reset)
    processed_dialogs = 0
    for account_user_id, target_user_id in dialog_inbox.list_pending_dialogs(limit=limit):
        if monitor_svc.get_client(account_user_id) is None:
            continue
        key = (account_user_id, target_user_id)
        lock = _dialog_locks.setdefault(key, asyncio.Lock())
        async with lock:
            _inflight.add(key)
            try:
                await _drain_dialog_inbox(account_user_id, target_user_id)
                processed_dialogs += 1
            finally:
                _inflight.discard(key)
    return processed_dialogs


async def _handle_incoming_private_body(
    account_user_id: int,
    target_user_id: int,
    text: str,
    *,
    history_already_appended: bool = False,
    source_inbox_id: int | None = None,
    content_kind: str = "text",
) -> None:
    dialog = store.get_dialog(target_user_id)
    if not dialog:
        return
    if int(dialog.get("account_user_id") or 0) != int(account_user_id):
        return
    if dialog.get("stage") == store.STAGE_CLOSED:
        return
    if dialog.get("stage") == store.STAGE_FIRST_DM_SENDING:
        # A real incoming reaction proves the prepared First DM reached Telegram.
        if not first_dm_delivery.confirm_from_incoming(target_user_id, account_user_id):
            logger.error(
                "Cannot confirm provisional First DM from incoming reply account={} target={}",
                account_user_id,
                target_user_id,
            )
            return
        dialog = store.get_dialog(target_user_id)
        if not dialog:
            return

    # Reconcile every prepared delivery before creating a new response.
    for prepared_action in dialog_delivery.list_prepared_for_target(target_user_id):
        reconciled = await _reconcile_prepared_action(prepared_action)
        if not reconciled:
            return
        dialog = store.get_dialog(target_user_id)
        if not dialog:
            return
        if dialog.get("stage") == store.STAGE_CLOSED and not bool(
            int(prepared_action.get("allow_opt_out") or 0)
        ):
            return

    if opt_out_svc.is_opted_out(target_user_id):
        store.close_for_opt_out(target_user_id)
        await _cleanup_disabled_account(account_user_id)
        return

    if not history_already_appended:
        store.append_history(target_user_id, "user", text)
        dialog = store.get_dialog(target_user_id) or dialog

    outgoing = int(dialog.get("outgoing_count") or 0)
    if outgoing >= store.MAX_OUTGOING:
        store.set_stage(target_user_id, store.STAGE_CLOSED, clear_auto_link=True)
        store.mark_contact_completed(target_user_id)
        await _cleanup_disabled_account(account_user_id)
        return

    history = (store.get_dialog(target_user_id) or {}).get("history") or []
    category = await ai_dialog.classify_user_message(
        history,
        text=text,
        content_kind=content_kind,
    )

    if category in {
        ai_dialog.CATEGORY_STOP_REQUEST,
        ai_dialog.CATEGORY_AGGRESSIVE_REFUSAL,
    }:
        opt_out_svc.add(target_user_id, category)
        await _process_hard_stop(
            account_user_id,
            target_user_id,
            text,
            history_already_appended=True,
            source_inbox_id=source_inbox_id,
        )
        return

    stage = str(dialog.get("stage") or "")
    first_reply_stages = {
        store.STAGE_WAITING_REPLY,
        store.STAGE_FOLLOWUP_SENT,
        store.STAGE_ENGAGED,
        store.STAGE_EXPLAINED,
    }

    if stage in first_reply_stages:
        # The approved funnel moves directly from an allowed First DM response to one
        # complete promo with the exact link. A calm refusal is allowed here and uses
        # its own soft wording. Only stop requests and aggressive refusals were handled
        # above as terminal. Generation plus prepare is serialized so two accounts
        # cannot send the same fresh wording concurrently.
        async with phrases_svc.generation_lock(phrases_svc.KIND_PROMO):
            await asyncio.sleep(_delay_reply())
            history = (store.get_dialog(target_user_id) or {}).get("history") or []
            promo = await ai_dialog.generate_promo(
                history,
                category=category,
                content_kind=content_kind,
            )
            apology_at = (
                _now() + dt.timedelta(seconds=_auto_link_delay())
            ).isoformat()
            result = await _deliver_inbox_message(
                account_user_id,
                target_user_id,
                promo,
                message_kind=dialog_delivery.KIND_PROMO,
                source_inbox_id=source_inbox_id,
                transition={
                    "stage": store.STAGE_PROMO_SENT,
                    "bump_outgoing": True,
                    "link_sent": True,
                    "auto_link_at": apology_at,
                    "append_history": True,
                },
            )
        if result == "sent":
            logger.info(
                "Promo sent target={} account={} category={} apology_due={}",
                target_user_id,
                account_user_id,
                category,
                apology_at,
            )
        return

    followup_stages = {
        store.STAGE_PROMO_SENT,
        store.STAGE_APOLOGY_SENT,
        store.STAGE_LINK_HELP_SENT,
        store.STAGE_LINK_SENT,
    }
    if stage not in followup_stages:
        logger.warning(
            "Unsupported dialog stage target={} account={} stage={}",
            target_user_id,
            account_user_id,
            stage,
        )
        return

    if (
        DIALOG_FLOW_VARIANT in {2, 3}
        and ai_dialog.is_link_open_problem(text, content_kind)
    ):
        reserved_slots = _reserved_automatic_slots(stage)
        if outgoing >= store.MAX_OUTGOING - reserved_slots:
            logger.info(
                "Detailed link help skipped by message budget "
                "target={} account={} stage={} outgoing={} reserved={}",
                target_user_id,
                account_user_id,
                stage,
                outgoing,
                reserved_slots,
            )
            return
        await asyncio.sleep(_delay_reply())
        history = (store.get_dialog(target_user_id) or {}).get("history") or []
        help_text = await ai_dialog.generate_link_open_help(history)
        keep_scheduled = stage == store.STAGE_PROMO_SENT and bool(
            dialog.get("auto_link_at")
        )
        next_stage = (
            store.STAGE_LINK_HELP_SENT
            if stage == store.STAGE_APOLOGY_SENT and not keep_scheduled
            else stage
        )
        result = await _deliver_inbox_message(
            account_user_id,
            target_user_id,
            help_text,
            message_kind=dialog_delivery.KIND_LINK_HELP,
            source_inbox_id=source_inbox_id,
            transition={
                "stage": next_stage,
                "bump_outgoing": True,
                "link_sent": True,
                "clear_auto_link": not keep_scheduled,
                "append_history": True,
            },
        )
        if result == "sent":
            logger.info(
                "Detailed link help sent after real user problem "
                "target={} account={} stage={}",
                target_user_id,
                account_user_id,
                stage,
            )
        return

    if category == ai_dialog.CATEGORY_SOFT_REFUSAL:
        # A calm refusal is not an opt-out and must not cancel scheduled work.
        logger.info(
            "Calm refusal keeps funnel active target={} account={} stage={}",
            target_user_id,
            account_user_id,
            stage,
        )
        return

    reserved_slots = _reserved_automatic_slots(stage)
    if outgoing >= store.MAX_OUTGOING - reserved_slots:
        logger.info(
            "Dialog reply skipped to preserve mandatory automatic steps "
            "target={} account={} stage={} outgoing={} reserved={}",
            target_user_id,
            account_user_id,
            stage,
            outgoing,
            reserved_slots,
        )
        return

    await asyncio.sleep(_delay_reply())
    history = (store.get_dialog(target_user_id) or {}).get("history") or []
    include_link = category == ai_dialog.CATEGORY_LINK_REQUEST
    reply = await ai_dialog.generate_qna_reply(
        history,
        category=category,
        content_kind=content_kind,
        include_link=include_link,
    )
    keep_scheduled = stage in {
        store.STAGE_PROMO_SENT,
        store.STAGE_APOLOGY_SENT,
    } and bool(dialog.get("auto_link_at"))
    result = await _deliver_inbox_message(
        account_user_id,
        target_user_id,
        reply,
        message_kind=dialog_delivery.KIND_QNA,
        source_inbox_id=source_inbox_id,
        transition={
            "stage": stage,
            "bump_outgoing": True,
            "link_sent": True,
            "clear_auto_link": not keep_scheduled,
            "append_history": True,
        },
    )
    if result != "sent":
        return

    current = store.get_dialog(target_user_id) or {}
    if int(current.get("outgoing_count") or 0) >= store.MAX_OUTGOING:
        store.set_stage(target_user_id, store.STAGE_CLOSED, clear_auto_link=True)
        store.mark_contact_completed(target_user_id)
        await _cleanup_disabled_account(account_user_id)


async def _deliver_inbox_message(
    account_user_id: int,
    target_user_id: int,
    text: str,
    *,
    message_kind: str,
    source_inbox_id: int | None,
    transition: dict,
    allow_opt_out: bool = False,
) -> str:
    """Crash-safe delivery for one message produced from a durable inbox row."""
    if source_inbox_id is None:
        raise ValueError("source_inbox_id is required for durable dialog delivery")
    action_key = dialog_delivery.inbox_action_key(message_kind, source_inbox_id)
    existing = dialog_delivery.get(target_user_id, action_key)
    if existing and str(existing.get("status")) == dialog_delivery.STATUS_SENT:
        return "sent"
    if existing and str(existing.get("status")) == dialog_delivery.STATUS_PREPARED:
        reconciled = await _reconcile_prepared_action(existing)
        if not reconciled:
            raise DeliveryPendingError(action_key)
        existing = dialog_delivery.get(target_user_id, action_key)
        if existing and str(existing.get("status")) == dialog_delivery.STATUS_SENT:
            return "sent"
    if existing and str(existing.get("status")) == dialog_delivery.STATUS_FAILED:
        retry_at = pacing._parse_iso(existing.get("recovery_next_at"))
        if retry_at is not None and retry_at > _now():
            raise DeliveryPendingError(action_key)

    prepared = dialog_delivery.prepare(
        target_user_id,
        account_user_id,
        action_key,
        text,
        message_kind=message_kind,
        transition=transition,
        source_inbox_id=source_inbox_id,
        allow_opt_out=allow_opt_out,
    )
    if not prepared:
        existing = dialog_delivery.get(target_user_id, action_key)
        if existing and str(existing.get("status")) == dialog_delivery.STATUS_SENT:
            return "sent"
        if existing and str(existing.get("status")) == dialog_delivery.STATUS_PREPARED:
            raise DeliveryPendingError(action_key)
        if existing and str(existing.get("status")) == dialog_delivery.STATUS_FAILED:
            retry_at = pacing._parse_iso(existing.get("recovery_next_at"))
            if retry_at is not None and retry_at > _now():
                raise DeliveryPendingError(action_key)
        return "failed"

    result = await _send_prepared_action(
        account_user_id, target_user_id, action_key, text
    )
    if result in {"ambiguous", "retry", "cooldown"}:
        raise DeliveryPendingError(action_key)
    return result


def _normalize_message_text(value: str | None) -> str:
    return telegram_history.normalize_text(value)


def _dialog_entity_user_id(entity) -> int | None:
    for attr in ("user_id", "id"):
        value = getattr(entity, attr, None)
        if value is None:
            continue
        try:
            return int(value)
        except (TypeError, ValueError):
            continue
    return None


def _load_dialog_identity(target_user_id: int) -> dict:
    from db.schema import get_connection

    row = get_connection().execute(
        """
        SELECT username, access_hash, source_account_user_id
          FROM audience WHERE user_id=?
        """,
        (int(target_user_id),),
    ).fetchone()
    return dict(row) if row else {}


async def _resolve_dialog_entity(client, account: int, target: int, identity: dict):
    """Resolve an owned dialog without trusting a recycled username."""
    username = str(identity.get("username") or "").strip().lstrip("@")
    if username:
        try:
            entity = await client.get_input_entity(username)
        except (ValueError, TypeError, LookupError, PeerIdInvalidError):
            entity = None
        if entity is not None and _dialog_entity_user_id(entity) == target:
            return entity

    try:
        return await client.get_input_entity(target)
    except (ValueError, TypeError, LookupError, PeerIdInvalidError):
        pass

    access_hash = identity.get("access_hash")
    source_account = identity.get("source_account_user_id")
    if (
        access_hash is not None
        and source_account is not None
        and int(source_account) == int(account)
    ):
        return InputPeerUser(target, int(access_hash))
    return None


async def _send_prepared_action(
    account_user_id: int,
    target_user_id: int,
    action_kind: str,
    text: str,
) -> str:
    """Return sent | failed | ambiguous after a durable prepare."""
    prepared_row = dialog_delivery.get(target_user_id, action_kind) or {}
    message_kind = str(
        prepared_row.get("message_kind") or str(action_kind).split(":", 1)[0]
    )
    if message_kind == dialog_delivery.KIND_FOLLOWUP and store.has_incoming_reply(
        target_user_id, account_user_id
    ):
        dialog_delivery.mark_failed(
            target_user_id, action_kind, "incoming_reply_exists"
        )
        store.set_stage(
            target_user_id, store.STAGE_WAITING_REPLY, clear_auto_link=True
        )
        logger.info(
            "Silence follow-up cancelled after durable incoming reply "
            "account={} target={}",
            account_user_id,
            target_user_id,
        )
        return "blocked"
    if _global_pre_reply_blocked(
        account_user_id, target_user_id, prepared_row
    ):
        dialog_delivery.mark_failed(
            target_user_id, action_kind, "global_pause_pre_reply_blocked"
        )
        logger.info(
            "Pre-reply autonomous delivery blocked by global pause "
            "kind={} account={} target={}",
            message_kind,
            account_user_id,
            target_user_id,
        )
        return "blocked"
    original_text = str(prepared_row.get("text") or text or "")
    clean_text = ai_dialog.sanitize_post_first_dm_text(original_text)
    if not clean_text:
        dialog_delivery.mark_failed(
            target_user_id, action_kind, "empty_after_repeated_greeting_guard"
        )
        logger.error(
            "Post-First-DM message blocked because only a greeting remained "
            "kind={} account={} target={}",
            prepared_row.get("message_kind") or action_kind,
            account_user_id,
            target_user_id,
        )
        return "failed"
    if clean_text != original_text:
        if not dialog_delivery.replace_prepared_text(
            target_user_id, action_kind, clean_text
        ):
            logger.error(
                "Cannot persist repeated-greeting cleanup before send "
                "kind={} account={} target={}",
                prepared_row.get("message_kind") or action_kind,
                account_user_id,
                target_user_id,
            )
            return "retry"
        logger.warning(
            "Removed repeated greeting before post-First-DM delivery "
            "kind={} account={} target={}",
            prepared_row.get("message_kind") or action_kind,
            account_user_id,
            target_user_id,
        )
    text = clean_text
    allow_opt_out = bool(int(prepared_row.get("allow_opt_out") or 0))
    if opt_out_svc.is_opted_out(target_user_id) and not allow_opt_out:
        dialog_delivery.mark_failed(target_user_id, action_kind, "target_opted_out")
        store.close_for_opt_out(target_user_id)
        return "failed"
    # Do not apply the cold-outreach account cooldown to an established dialog.
    # The same account must answer a real incoming message without cross-account
    # transfer. Telegram errors are persisted as per-action retry state.
    client = monitor_svc.get_client(account_user_id)
    if client is None or not client.is_connected():
        dialog_delivery.mark_failed(target_user_id, action_kind, "client_unavailable")
        return "retry"
    try:
        identity = _load_dialog_identity(target_user_id)
        entity = await _resolve_dialog_entity(
            client, account_user_id, target_user_id, identity
        )
        if entity is None:
            dialog_delivery.mark_failed(
                target_user_id, action_kind, "entity_unavailable_before_send"
            )
            store.set_stage(target_user_id, store.STAGE_CLOSED, clear_auto_link=True)
            store.mark_contact_completed(target_user_id)
            logger.warning(
                "Dialog closed because Telegram entity is unavailable before send "
                "kind={} account={} target={}",
                action_kind,
                account_user_id,
                target_user_id,
            )
            await _cleanup_disabled_account(account_user_id)
            return "failed"
        if opt_out_svc.is_opted_out(target_user_id) and not allow_opt_out:
            dialog_delivery.mark_failed(target_user_id, action_kind, "target_opted_out")
            store.close_for_opt_out(target_user_id)
            return "failed"
        message = await client.send_message(entity, text)
        sent_at = getattr(message, "date", None)
        if sent_at is not None and sent_at.tzinfo is None:
            sent_at = sent_at.replace(tzinfo=dt.timezone.utc)
        committed = dialog_delivery.commit_sent(
            target_user_id,
            action_kind,
            telegram_message_id=getattr(message, "id", None),
            sent_at=(sent_at or _now()).isoformat(),
        )
        if not committed:
            logger.error(
                "Telegram accepted scheduled message but DB commit failed kind={} "
                "account={} target={}",
                action_kind,
                account_user_id,
                target_user_id,
            )
            return "ambiguous"
        await _cleanup_disabled_account(account_user_id)
        return "sent"
    except FloodWaitError as exc:
        seconds = int(getattr(exc, "seconds", 60) or 60)
        # Keep the account cooldown for First DM, but retry only this dialog action
        # after Telegram's explicit wait. Other real dialogs are not globally held.
        pacing.apply_floodwait(account_user_id, seconds)
        dialog_delivery.mark_failed_with_backoff(
            target_user_id,
            action_kind,
            f"FloodWait:{seconds}",
            delay_seconds=seconds,
        )
        return "retry"
    except PeerFloodError:
        try:
            from services import spambot as spambot_svc

            await spambot_svc.on_peer_flood(
                account_user_id, source="dialog"
            )
        except Exception as sp_exc:
            logger.exception("SpamBot from scheduled dialog: {}", sp_exc)
            pacing.set_paused(account_user_id, "PeerFlood", paused=True)
        if message_kind == dialog_delivery.KIND_FOLLOWUP:
            retry_at = dialog_delivery.mark_failed_with_backoff(
                target_user_id,
                action_kind,
                "PeerFlood",
                delay_seconds=_FOLLOWUP_PEERFLOOD_RETRY_SECONDS,
            )
            logger.warning(
                "Silence follow-up held after PeerFlood account={} target={} retry_at={}",
                account_user_id,
                target_user_id,
                retry_at,
            )
            return "peerflood"
        dialog_delivery.mark_failed_with_backoff(
            target_user_id,
            action_kind,
            "PeerFlood",
            delay_seconds=_DIALOG_PEERFLOOD_RETRY_SECONDS,
        )
        return "retry"
    except (UserPrivacyRestrictedError, UserIsBlockedError, ChatWriteForbiddenError):
        dialog_delivery.mark_failed(target_user_id, action_kind, "privacy_or_blocked")
        store.set_stage(target_user_id, store.STAGE_CLOSED, clear_auto_link=True)
        store.mark_contact_completed(target_user_id)
        await _cleanup_disabled_account(account_user_id)
        return "failed"
    except Exception as exc:
        if account_auth.is_auth_loss_error(exc):
            dialog_delivery.mark_failed(
                target_user_id, action_kind, "authorization_lost"
            )
            await account_auth.register_auth_loss(
                account_user_id, exc, notify=True
            )
            await monitor_svc.disconnect_account(
                account_user_id, cancel_tasks=True
            )
            logger.warning(
                "Dialog send stopped because authorization was lost "
                "kind={} account={} target={}",
                action_kind,
                account_user_id,
                target_user_id,
            )
            return "retry"
        # Telegram may have accepted the message even when the network response was
        # lost. Keep PREPARED and reconcile against real chat history before retry.
        logger.exception(
            "Scheduled send ambiguous kind={} account={} target={}: {}",
            action_kind,
            account_user_id,
            target_user_id,
            exc,
        )
        return "ambiguous"


_DIALOG_RECOVERY_NO_ENTITY_MAX_ATTEMPTS = 3
_DIALOG_RECOVERY_GENERIC_MAX_ATTEMPTS = 6
_DIALOG_RECOVERY_NO_ENTITY_DELAY_SECONDS = 900
_DIALOG_RECOVERY_GENERIC_DELAY_SECONDS = 900


def _defer_or_abandon_dialog_recovery(
    row: dict,
    *,
    reason: str,
    max_attempts: int,
    delay_seconds: int,
) -> bool:
    target = int(row["target_user_id"])
    action_key = str(row["action_kind"])
    account = int(row["account_user_id"])
    kind = str(row.get("message_kind") or action_key.split(":", 1)[0])
    attempts = dialog_delivery.defer_recovery(
        target, action_key, reason, delay_seconds=delay_seconds
    )
    if attempts < max_attempts:
        logger.warning(
            "Dialog recovery deferred kind={} account={} target={} reason={} "
            "attempt={}/{} retry_sec={}",
            kind,
            account,
            target,
            reason,
            attempts,
            max_attempts,
            delay_seconds,
        )
        return False

    final_reason = f"recovery_exhausted:{reason}:{attempts}"
    dialog_delivery.abandon_recovery(target, action_key, final_reason)
    logger.error(
        "Dialog recovery abandoned safely kind={} account={} target={} "
        "reason={} attempts={}",
        kind,
        account,
        target,
        reason,
        attempts,
    )
    return True


async def _reconcile_prepared_action(row: dict) -> bool:
    target = int(row["target_user_id"])
    account = int(row["account_user_id"])
    action_key = str(row["action_kind"])
    kind = str(row.get("message_kind") or action_key.split(":", 1)[0])
    client = monitor_svc.get_client(account)
    if client is None or not client.is_connected():
        return _defer_or_abandon_dialog_recovery(
            row,
            reason="client_unavailable",
            max_attempts=_DIALOG_RECOVERY_GENERIC_MAX_ATTEMPTS,
            delay_seconds=300,
        )
    try:
        entity = await _resolve_dialog_entity(client, account, target, row)
        if entity is None:
            return _defer_or_abandon_dialog_recovery(
                row,
                reason="entity_unavailable",
                max_attempts=_DIALOG_RECOVERY_NO_ENTITY_MAX_ATTEMPTS,
                delay_seconds=_DIALOG_RECOVERY_NO_ENTITY_DELAY_SECONDS,
            )
        prepared_at = pacing._parse_iso(row.get("prepared_at"))
        lower_bound = prepared_at - dt.timedelta(minutes=2) if prepared_at else None
        found = await telegram_history.find_outgoing_text_since(
            client,
            entity,
            str(row.get("text") or ""),
            since=lower_bound,
        )
    except Exception as exc:
        if isinstance(exc, FloodWaitError):
            seconds = int(getattr(exc, "seconds", 60) or 60)
            pacing.apply_floodwait(account, seconds)
            return _defer_or_abandon_dialog_recovery(
                row,
                reason=f"FloodWait:{seconds}",
                max_attempts=_DIALOG_RECOVERY_GENERIC_MAX_ATTEMPTS,
                delay_seconds=max(900, seconds),
            )
        if isinstance(exc, PeerFloodError):
            try:
                from services import spambot as spambot_svc

                await spambot_svc.on_peer_flood(
                    account, source="dialog_recovery"
                )
            except Exception as sp_exc:
                logger.warning(
                    "SpamBot from dialog recovery failed account={} error_type={}",
                    account,
                    type(sp_exc).__name__,
                )
                pacing.set_paused(account, "PeerFlood", paused=True)
            return _defer_or_abandon_dialog_recovery(
                row,
                reason="PeerFlood",
                max_attempts=_DIALOG_RECOVERY_GENERIC_MAX_ATTEMPTS,
                delay_seconds=_DIALOG_PEERFLOOD_RETRY_SECONDS,
            )
        if account_auth.is_auth_loss_error(exc):
            await account_auth.register_auth_loss(account, exc, notify=True)
            await monitor_svc.disconnect_account(account, cancel_tasks=True)
            dialog_delivery.defer_recovery(
                target, action_key, "authorization_lost", delay_seconds=21600
            )
            logger.warning(
                "Dialog recovery paused because authorization was lost "
                "kind={} account={} target={} retry_sec=21600",
                kind,
                account,
                target,
            )
            return False
        expected_entity_miss = isinstance(
            exc, (ValueError, TypeError, LookupError, PeerIdInvalidError)
        )
        if expected_entity_miss:
            return _defer_or_abandon_dialog_recovery(
                row,
                reason=type(exc).__name__,
                max_attempts=_DIALOG_RECOVERY_NO_ENTITY_MAX_ATTEMPTS,
                delay_seconds=_DIALOG_RECOVERY_NO_ENTITY_DELAY_SECONDS,
            )
        logger.warning(
            "Cannot inspect Telegram history kind={} account={} target={} "
            "error_type={}",
            kind,
            account,
            target,
            type(exc).__name__,
        )
        logger.opt(exception=exc).debug(
            "Dialog recovery diagnostic kind={} account={} target={}",
            kind,
            account,
            target,
        )
        return _defer_or_abandon_dialog_recovery(
            row,
            reason=type(exc).__name__,
            max_attempts=_DIALOG_RECOVERY_GENERIC_MAX_ATTEMPTS,
            delay_seconds=_DIALOG_RECOVERY_GENERIC_DELAY_SECONDS,
        )

    if found is not None:
        committed = dialog_delivery.commit_sent(
            target,
            action_key,
            telegram_message_id=getattr(found, "id", None),
            sent_at=(getattr(found, "date", None) or _now()).isoformat(),
        )
        if not committed:
            return False
        if kind == dialog_delivery.KIND_SMOOTH_APOLOGY:
            repaired = store.get_dialog(target) or {}
            if DIALOG_FLOW_VARIANT in {2, 3}:
                if repaired.get("stage") == store.STAGE_APOLOGY_SENT:
                    store.set_stage(
                        target,
                        store.STAGE_APOLOGY_SENT,
                        clear_auto_link=True,
                    )
            elif (
                repaired.get("stage") == store.STAGE_APOLOGY_SENT
                and not repaired.get("auto_link_at")
                and int(repaired.get("outgoing_count") or 0) < store.MAX_OUTGOING
            ):
                store.set_stage(
                    target,
                    store.STAGE_APOLOGY_SENT,
                    auto_link_at=(
                        _now() + dt.timedelta(seconds=_auto_link_delay())
                    ).isoformat(),
                )
        await _cleanup_disabled_account(account)
        logger.warning(
            "Recovered dialog message from Telegram kind={} account={} target={} msg_id={}",
            kind,
            account,
            target,
            getattr(found, "id", None),
        )
        return True

    dialog_delivery.mark_failed(target, action_key, "telegram_history_no_match")
    if action_key == dialog_delivery.KIND_AUTO_LINK:
        store.set_stage(
            target,
            store.STAGE_EXPLAINED,
            auto_link_at=(_now() + dt.timedelta(minutes=5)).isoformat(),
        )
    elif action_key == dialog_delivery.KIND_SMOOTH_APOLOGY:
        store.set_stage(
            target,
            store.STAGE_PROMO_SENT,
            auto_link_at=(_now() + dt.timedelta(seconds=60)).isoformat(),
        )
    elif action_key == dialog_delivery.KIND_LINK_HELP:
        if DIALOG_FLOW_VARIANT in {2, 3}:
            store.set_stage(
                target,
                store.STAGE_APOLOGY_SENT,
                clear_auto_link=True,
            )
        else:
            store.set_stage(
                target,
                store.STAGE_APOLOGY_SENT,
                auto_link_at=(_now() + dt.timedelta(seconds=60)).isoformat(),
            )
    elif action_key == dialog_delivery.KIND_FOLLOWUP:
        store.set_stage(
            target,
            store.STAGE_WAITING_REPLY,
            auto_link_at=(_now() + dt.timedelta(hours=2)).isoformat(),
        )
    logger.warning(
        "Prepared dialog message not found; marked retryable kind={} account={} target={}",
        kind,
        account,
        target,
    )
    return True


async def recover_ambiguous_dialog_messages() -> int:
    recovered = 0
    rows = dialog_delivery.list_stale_prepared(older_than_seconds=90, limit=100)
    for row in rows:
        account = int(row["account_user_id"])
        target = int(row["target_user_id"])
        if _global_pre_reply_blocked(account, target, row):
            logger.info(
                "Prepared pre-reply action held by global pause kind={} "
                "account={} target={}",
                row.get("message_kind") or row.get("action_kind"),
                account,
                target,
            )
            continue
        if await _reconcile_prepared_action(row):
            recovered += 1
    return recovered


async def recover_ambiguous_scheduled_messages() -> int:
    return await recover_ambiguous_dialog_messages()


async def process_due_auto_links(limit: int = 25) -> int:
    """Send due promo compatibility steps, apologies and link-help instructions.

    The historical function name is retained for the main loop. Variant 1 uses
    promo -> apology -> detailed link help. Variants 2 and 3 stop after the apology
    and send detailed help only after a real user problem. Existing dialogs continue
    even when new First DM sending is paused.
    """
    # Existing dialogs continue only after a durable incoming reply.
    due = store.list_due_auto_links(limit=max(1, int(limit)))
    sent_count = 0
    for due_row in due:
        target = int(due_row["target_user_id"])
        account = int(due_row["account_user_id"])
        if opt_out_svc.is_opted_out(target):
            store.close_for_opt_out(target)
            await _cleanup_disabled_account(account)
            continue

        key = (account, target)
        lock = _dialog_locks.setdefault(key, asyncio.Lock())
        if lock.locked():
            continue
        async with lock:
            _inflight.add(key)
            try:
                current = store.get_dialog(target)
                if not current or current.get("stage") == store.STAGE_CLOSED:
                    continue
                if _global_pre_reply_blocked(account, target, current):
                    logger.warning(
                        "Scheduled post-reply action held without durable incoming proof "
                        "account={} target={} stage={}",
                        account,
                        target,
                        current.get("stage"),
                    )
                    continue
                # These scheduled steps belong to a dialog that already has durable
                # incoming proof. First DM cooldowns must not stop the conversation.
                if dialog_inbox.has_pending(account, target):
                    continue
                outgoing = int(current.get("outgoing_count") or 0)
                if outgoing >= store.MAX_OUTGOING:
                    store.set_stage(target, store.STAGE_CLOSED, clear_auto_link=True)
                    store.mark_contact_completed(target)
                    await _cleanup_disabled_account(account)
                    continue

                stage = str(current.get("stage") or "")
                history = current.get("history") or []
                if stage == store.STAGE_PROMO_SENT and int(current.get("link_sent") or 0):
                    remaining = store.MAX_OUTGOING - outgoing
                    if DIALOG_FLOW_VARIANT in {2, 3}:
                        if remaining >= 1:
                            action_kind = dialog_delivery.KIND_SMOOTH_APOLOGY
                        else:
                            continue
                    elif remaining >= 2:
                        action_kind = dialog_delivery.KIND_SMOOTH_APOLOGY
                    elif remaining == 1:
                        # Repair old dialogs where extra Q&A consumed the apology slot.
                        # In variant 1 the mandatory opening instruction gets priority.
                        action_kind = dialog_delivery.KIND_LINK_HELP
                        logger.warning(
                            "Skipping apology to preserve link help for legacy budget "
                            "target={} account={} outgoing={}",
                            target,
                            account,
                            outgoing,
                        )
                    else:
                        continue
                elif stage == store.STAGE_APOLOGY_SENT and int(current.get("link_sent") or 0):
                    if DIALOG_FLOW_VARIANT in {2, 3}:
                        store.set_stage(
                            target,
                            store.STAGE_APOLOGY_SENT,
                            clear_auto_link=True,
                        )
                        logger.info(
                            "Automatic detailed link help disabled by dialog variant "
                            "target={} account={} variant={}",
                            target,
                            account,
                            DIALOG_FLOW_VARIANT,
                        )
                        continue
                    action_kind = dialog_delivery.KIND_LINK_HELP
                elif stage == store.STAGE_EXPLAINED and not int(current.get("link_sent") or 0):
                    action_kind = dialog_delivery.KIND_AUTO_LINK
                else:
                    continue

                existing = dialog_delivery.get(target, action_kind)
                existing_status = str((existing or {}).get("status") or "")
                if existing_status == dialog_delivery.STATUS_PREPARED:
                    logger.debug(
                        "Scheduled action awaits reconciliation target={} account={} "
                        "stage={} action={}",
                        target,
                        account,
                        stage,
                        action_kind,
                    )
                    continue
                if existing_status == dialog_delivery.STATUS_SENT:
                    if action_kind == dialog_delivery.KIND_SMOOTH_APOLOGY:
                        if DIALOG_FLOW_VARIANT in {2, 3}:
                            store.set_stage(
                                target,
                                store.STAGE_APOLOGY_SENT,
                                link_sent=True,
                                clear_auto_link=True,
                            )
                        else:
                            store.set_stage(
                                target,
                                store.STAGE_APOLOGY_SENT,
                                link_sent=True,
                                auto_link_at=(
                                    _now() + dt.timedelta(seconds=_auto_link_delay())
                                ).isoformat(),
                            )
                    elif action_kind == dialog_delivery.KIND_LINK_HELP:
                        store.set_stage(
                            target,
                            store.STAGE_LINK_HELP_SENT,
                            link_sent=True,
                            clear_auto_link=True,
                        )
                    else:
                        store.set_stage(
                            target,
                            store.STAGE_PROMO_SENT,
                            link_sent=True,
                            auto_link_at=(
                                _now() + dt.timedelta(seconds=_auto_link_delay())
                            ).isoformat(),
                        )
                    logger.warning(
                        "Repaired stale dialog state from sent outbox target={} account={} "
                        "old_stage={} action={}",
                        target,
                        account,
                        stage,
                        action_kind,
                    )
                    continue

                if action_kind == dialog_delivery.KIND_SMOOTH_APOLOGY:
                    text = await ai_dialog.generate_smoothing_apology(history)
                    transition = {
                        "stage": store.STAGE_APOLOGY_SENT,
                        "bump_outgoing": True,
                        "link_sent": True,
                        "append_history": True,
                    }
                    if DIALOG_FLOW_VARIANT in {2, 3}:
                        transition["clear_auto_link"] = True
                    else:
                        transition["auto_link_at"] = (
                            _now() + dt.timedelta(seconds=_auto_link_delay())
                        ).isoformat()
                    message_kind = dialog_delivery.KIND_SMOOTH_APOLOGY
                    retry_stage = store.STAGE_PROMO_SENT
                    retry_delay = 60
                elif action_kind == dialog_delivery.KIND_LINK_HELP:
                    text = await ai_dialog.generate_link_open_help(history)
                    transition = {
                        "stage": store.STAGE_LINK_HELP_SENT,
                        "bump_outgoing": True,
                        "link_sent": True,
                        "clear_auto_link": True,
                        "append_history": True,
                    }
                    if stage == store.STAGE_PROMO_SENT:
                        transition["allow_skip_apology"] = True
                    message_kind = dialog_delivery.KIND_LINK_HELP
                    retry_stage = stage
                    retry_delay = 60
                else:
                    text = await ai_dialog.generate_promo(
                        history,
                        category=ai_dialog.CATEGORY_NORMAL,
                    )
                    transition = {
                        "stage": store.STAGE_PROMO_SENT,
                        "bump_outgoing": True,
                        "link_sent": True,
                        "auto_link_at": (
                            _now() + dt.timedelta(seconds=_auto_link_delay())
                        ).isoformat(),
                        "append_history": True,
                    }
                    message_kind = dialog_delivery.KIND_PROMO
                    retry_stage = store.STAGE_EXPLAINED
                    retry_delay = 300

                if not dialog_delivery.prepare(
                    target,
                    account,
                    action_kind,
                    text,
                    message_kind=message_kind,
                    transition=transition,
                ):
                    latest_outbox = dialog_delivery.get(target, action_kind) or {}
                    store.set_stage(
                        target,
                        stage,
                        auto_link_at=(
                            _now() + dt.timedelta(seconds=retry_delay)
                        ).isoformat(),
                    )
                    logger.warning(
                        "Scheduled prepare rejected target={} account={} stage={} action={} "
                        "outbox_status={} retry_sec={}",
                        target,
                        account,
                        stage,
                        action_kind,
                        latest_outbox.get("status") or "missing",
                        retry_delay,
                    )
                    continue

                result = await _send_prepared_action(account, target, action_kind, text)
                if result == "sent":
                    sent_count += 1
                    if action_kind == dialog_delivery.KIND_AUTO_LINK:
                        logger.info("Legacy promo sent target={} account={}", target, account)
                    elif action_kind == dialog_delivery.KIND_SMOOTH_APOLOGY:
                        logger.info("Smoothing apology sent target={} account={}", target, account)
                    else:
                        logger.info("Link help sent target={} account={}", target, account)
                    latest = store.get_dialog(target) or {}
                    if int(latest.get("outgoing_count") or 0) >= store.MAX_OUTGOING:
                        store.set_stage(target, store.STAGE_CLOSED, clear_auto_link=True)
                        store.mark_contact_completed(target)
                        await _cleanup_disabled_account(account)
                elif result in {"failed", "retry", "cooldown"}:
                    latest = store.get_dialog(target)
                    if latest and latest.get("stage") != store.STAGE_CLOSED:
                        store.set_stage(
                            target,
                            retry_stage,
                            auto_link_at=(
                                _now() + dt.timedelta(seconds=retry_delay)
                            ).isoformat(),
                        )
            except ai_dialog.ChannelLinkNotConfiguredError as exc:
                logger.error("Scheduled promo blocked by CHANNEL_LINK: {}", exc)
                current = store.get_dialog(target)
                if current and current.get("stage") == store.STAGE_EXPLAINED:
                    store.set_stage(target, store.STAGE_EXPLAINED, auto_link_at=_link_retry_at())
            finally:
                _inflight.discard(key)
    return sent_count

async def process_due_followups(limit: int = 50) -> int:
    """Send one crash-safe silence follow-up after the First DM.

    The global First DM switch also controls every autonomous pre-reply touch.
    Real incoming dialogs are processed by the durable inbox path instead.
    """
    from services import runtime as runtime_svc

    if not runtime_svc.is_worker_enabled():
        return 0

    due = store.list_due_followups(limit=max(1, int(limit)))
    n = 0
    for d in due:
        target = int(d["target_user_id"])
        account = int(d["account_user_id"])
        outgoing = int(d.get("outgoing_count") or 0)
        if store.has_incoming_reply(target, account):
            store.set_stage(
                target, store.STAGE_WAITING_REPLY, clear_auto_link=True
            )
            logger.info(
                "Silence follow-up cancelled because incoming reply already exists "
                "account={} target={}",
                account,
                target,
            )
            continue
        if opt_out_svc.is_opted_out(target):
            store.close_for_opt_out(target)
            await _cleanup_disabled_account(account)
            continue
        if outgoing >= store.MAX_OUTGOING:
            store.set_stage(target, store.STAGE_CLOSED, clear_auto_link=True)
            store.mark_contact_completed(target)
            await _cleanup_disabled_account(account)
            continue
        cooldown = _account_cooldown_seconds(account)
        if cooldown > 0:
            store.set_stage(
                target,
                store.STAGE_WAITING_REPLY,
                auto_link_at=_retry_at_for_account(account, 120),
            )
            continue
        key = (account, target)
        lock = _dialog_locks.setdefault(key, asyncio.Lock())
        if lock.locked():
            continue
        async with lock:
            _inflight.add(key)
            try:
                if not runtime_svc.is_worker_enabled():
                    return n
                text = ai_dialog.followup_silence_text()
                if not dialog_delivery.prepare(
                    target, account, dialog_delivery.KIND_FOLLOWUP, text
                ):
                    retry_at = dialog_delivery.retry_not_before(
                        target, dialog_delivery.KIND_FOLLOWUP
                    )
                    if retry_at:
                        store.set_stage(
                            target,
                            store.STAGE_WAITING_REPLY,
                            auto_link_at=retry_at,
                        )
                    continue
                result = await _send_prepared_action(
                    account, target, dialog_delivery.KIND_FOLLOWUP, text
                )
                if result == "sent":
                    n += 1
                    logger.info(
                        "Silence follow-up sent target={} account={}", target, account
                    )
                elif result == "peerflood":
                    retry_at = dialog_delivery.retry_not_before(
                        target, dialog_delivery.KIND_FOLLOWUP
                    ) or (
                        _now()
                        + dt.timedelta(seconds=_FOLLOWUP_PEERFLOOD_RETRY_SECONDS)
                    ).isoformat()
                    current = store.get_dialog(target)
                    if current and current.get("stage") != store.STAGE_CLOSED:
                        store.set_stage(
                            target,
                            store.STAGE_WAITING_REPLY,
                            auto_link_at=retry_at,
                        )
                elif result in {"failed", "retry", "cooldown"}:
                    current = store.get_dialog(target)
                    if current and current.get("stage") != store.STAGE_CLOSED:
                        fallback = 7200 if result == "failed" else 120
                        store.set_stage(
                            target,
                            store.STAGE_WAITING_REPLY,
                            auto_link_at=_retry_at_for_account(account, fallback),
                        )
            finally:
                _inflight.discard(key)
    return n


async def _send_and_close(
    account_user_id: int,
    target_user_id: int,
    text: str,
    *,
    opt_out: bool,
    reason: str,
    source_inbox_id: int | None,
) -> None:
    await asyncio.sleep(min(5.0, _delay_reply()))
    result = await _deliver_inbox_message(
        account_user_id,
        target_user_id,
        text,
        message_kind=dialog_delivery.KIND_CLOSE,
        source_inbox_id=source_inbox_id,
        transition={
            "stage": store.STAGE_CLOSED,
            "bump_outgoing": True,
            "clear_auto_link": True,
            "append_history": True,
            "mark_contact_completed": True,
        },
    )
    if result != "sent":
        logger.warning(
            "Close message failed target={} account={} reason={}",
            target_user_id,
            account_user_id,
            reason,
        )
        store.set_stage(target_user_id, store.STAGE_CLOSED, clear_auto_link=True)
        store.mark_contact_completed(target_user_id)
    if opt_out:
        opt_out_svc.add(target_user_id, reason)
        logger.info("Opt-out target={} reason={}", target_user_id, reason)
    await _cleanup_disabled_account(account_user_id)
