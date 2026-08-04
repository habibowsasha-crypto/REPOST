"""Dialog funnel after First DM: promo, apology, Q&A and opt-out."""

from __future__ import annotations

import asyncio
import datetime as dt
import inspect
import random
import weakref
from typing import Set, Tuple

from loguru import logger
from telethon.errors import (
    FloodWaitError,
    PeerFloodError,
    UserIsBlockedError,
    UserPrivacyRestrictedError,
    ChatWriteForbiddenError,
)

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
    """Legacy helper name: now returns the smoothing-apology delay."""
    from services import runtime as runtime_svc

    lo, hi = runtime_svc.get_auto_link_delay_range()
    return random.randint(lo, hi)


async def _cleanup_disabled_account(account_user_id: int) -> None:
    try:
        await monitor_svc.maybe_disconnect_inactive_account(account_user_id)
    except Exception as exc:
        logger.debug("dialog account cleanup failed account={}: {}", account_user_id, exc)


def _link_retry_at() -> str:
    return (_now() + dt.timedelta(minutes=15)).isoformat()


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
            "link_sent": category == ai_dialog.CATEGORY_STOP_REQUEST,
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
        # The approved funnel always moves directly from First DM response to one
        # complete promo with the exact link. No extra engage question is inserted.
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
            phrases_svc.remember(phrases_svc.KIND_PROMO, promo)
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

    if category == ai_dialog.CATEGORY_SOFT_REFUSAL:
        reply = ai_dialog.soft_close_text()
        result = await _deliver_inbox_message(
            account_user_id,
            target_user_id,
            reply,
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
            store.set_stage(target_user_id, store.STAGE_CLOSED, clear_auto_link=True)
            store.mark_contact_completed(target_user_id)
        await _cleanup_disabled_account(account_user_id)
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
    result = await _deliver_inbox_message(
        account_user_id,
        target_user_id,
        reply,
        message_kind=dialog_delivery.KIND_QNA,
        source_inbox_id=source_inbox_id,
        transition={
            "stage": store.STAGE_APOLOGY_SENT,
            "bump_outgoing": True,
            "link_sent": True,
            "clear_auto_link": True,
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
        return "failed"

    result = await _send_prepared_action(
        account_user_id, target_user_id, action_key, text
    )
    if result in {"ambiguous", "retry"}:
        raise DeliveryPendingError(action_key)
    return result


def _normalize_message_text(value: str | None) -> str:
    return telegram_history.normalize_text(value)


async def _send_prepared_action(
    account_user_id: int,
    target_user_id: int,
    action_kind: str,
    text: str,
) -> str:
    """Return sent | failed | ambiguous after a durable prepare."""
    prepared_row = dialog_delivery.get(target_user_id, action_kind) or {}
    allow_opt_out = bool(int(prepared_row.get("allow_opt_out") or 0))
    if opt_out_svc.is_opted_out(target_user_id) and not allow_opt_out:
        dialog_delivery.mark_failed(target_user_id, action_kind, "target_opted_out")
        store.close_for_opt_out(target_user_id)
        return "failed"
    client = monitor_svc.get_client(account_user_id)
    if client is None or not client.is_connected():
        dialog_delivery.mark_failed(target_user_id, action_kind, "client_unavailable")
        return "retry"
    try:
        entity = await client.get_input_entity(int(target_user_id))
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
        pacing.apply_floodwait(account_user_id, seconds)
        dialog_delivery.mark_failed(target_user_id, action_kind, f"FloodWait:{seconds}")
        return "retry"
    except PeerFloodError:
        try:
            from services import spambot as spambot_svc

            await spambot_svc.on_peer_flood(account_user_id)
        except Exception as sp_exc:
            logger.exception("SpamBot from scheduled dialog: {}", sp_exc)
            pacing.set_paused(account_user_id, "PeerFlood", paused=True)
        dialog_delivery.mark_failed(target_user_id, action_kind, "PeerFlood")
        return "retry"
    except (UserPrivacyRestrictedError, UserIsBlockedError, ChatWriteForbiddenError):
        dialog_delivery.mark_failed(target_user_id, action_kind, "privacy_or_blocked")
        store.set_stage(target_user_id, store.STAGE_CLOSED, clear_auto_link=True)
        store.mark_contact_completed(target_user_id)
        await _cleanup_disabled_account(account_user_id)
        return "failed"
    except Exception as exc:
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


async def _reconcile_prepared_action(row: dict) -> bool:
    target = int(row["target_user_id"])
    account = int(row["account_user_id"])
    action_key = str(row["action_kind"])
    kind = str(row.get("message_kind") or action_key.split(":", 1)[0])
    client = monitor_svc.get_client(account)
    if client is None or not client.is_connected():
        return False
    try:
        entity = await client.get_input_entity(target)
        prepared_at = pacing._parse_iso(row.get("prepared_at"))
        lower_bound = prepared_at - dt.timedelta(minutes=2) if prepared_at else None
        found = await telegram_history.find_outgoing_text_since(
            client,
            entity,
            str(row.get("text") or ""),
            since=lower_bound,
        )
    except Exception as exc:
        logger.exception(
            "Cannot inspect Telegram history kind={} account={} target={}: {}",
            kind,
            account,
            target,
            exc,
        )
        return False

    if found is not None:
        committed = dialog_delivery.commit_sent(
            target,
            action_key,
            telegram_message_id=getattr(found, "id", None),
            sent_at=(getattr(found, "date", None) or _now()).isoformat(),
        )
        if not committed:
            return False
        if kind in {
            dialog_delivery.KIND_PROMO,
            dialog_delivery.KIND_AUTO_LINK,
            dialog_delivery.KIND_DIRECT_LINK,
        }:
            phrases_svc.remember(phrases_svc.KIND_PROMO, str(row.get("text") or ""))
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
        if await _reconcile_prepared_action(row):
            recovered += 1
    return recovered


async def recover_ambiguous_scheduled_messages() -> int:
    return await recover_ambiguous_dialog_messages()


async def process_due_auto_links() -> int:
    """Send due smoothing apologies and recover legacy pre-v1.0.55 link steps.

    The historical function name is retained for the main loop. New dialogs already
    received the link in message 2, so their due action is message 3: one short
    apology after the approved 5-60 second delay.
    """
    # Main-menu pause stops only new First DM. Existing dialogs continue.
    due = store.list_due_auto_links()
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
                # A user reply that arrived before the deadline always wins over
                # the automatic apology, even if its worker has not started yet.
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
                    text = await ai_dialog.generate_smoothing_apology(history)
                    action_kind = dialog_delivery.KIND_SMOOTH_APOLOGY
                    transition = {
                        "stage": store.STAGE_APOLOGY_SENT,
                        "bump_outgoing": True,
                        "link_sent": True,
                        "clear_auto_link": True,
                        "append_history": True,
                    }
                elif stage == store.STAGE_EXPLAINED and not int(current.get("link_sent") or 0):
                    # Upgrade compatibility: complete an old explain-only dialog by sending
                    # the new full promo with the exact link, then schedule its apology.
                    text = await ai_dialog.generate_promo(
                        history,
                        category=ai_dialog.CATEGORY_NORMAL,
                    )
                    action_kind = dialog_delivery.KIND_AUTO_LINK
                    transition = {
                        "stage": store.STAGE_PROMO_SENT,
                        "bump_outgoing": True,
                        "link_sent": True,
                        "auto_link_at": (
                            _now() + dt.timedelta(seconds=_auto_link_delay())
                        ).isoformat(),
                        "append_history": True,
                    }
                else:
                    continue

                if not dialog_delivery.prepare(
                    target,
                    account,
                    action_kind,
                    text,
                    message_kind=(
                        dialog_delivery.KIND_SMOOTH_APOLOGY
                        if action_kind == dialog_delivery.KIND_SMOOTH_APOLOGY
                        else dialog_delivery.KIND_PROMO
                    ),
                    transition=transition,
                ):
                    continue
                result = await _send_prepared_action(account, target, action_kind, text)
                if result == "sent":
                    sent_count += 1
                    if action_kind == dialog_delivery.KIND_AUTO_LINK:
                        phrases_svc.remember(phrases_svc.KIND_PROMO, text)
                        logger.info("Legacy promo sent target={} account={}", target, account)
                    else:
                        logger.info("Smoothing apology sent target={} account={}", target, account)
                    latest = store.get_dialog(target) or {}
                    if int(latest.get("outgoing_count") or 0) >= store.MAX_OUTGOING:
                        store.set_stage(target, store.STAGE_CLOSED, clear_auto_link=True)
                        store.mark_contact_completed(target)
                        await _cleanup_disabled_account(account)
                elif result == "failed":
                    latest = store.get_dialog(target)
                    if latest and latest.get("stage") != store.STAGE_CLOSED:
                        retry_stage = (
                            store.STAGE_PROMO_SENT
                            if action_kind == dialog_delivery.KIND_SMOOTH_APOLOGY
                            else store.STAGE_EXPLAINED
                        )
                        retry_delay = 60 if action_kind == dialog_delivery.KIND_SMOOTH_APOLOGY else 300
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


async def process_due_followups() -> int:
    """Send one crash-safe silence follow-up after the First DM."""
    due = store.list_due_followups()
    n = 0
    for d in due:
        target = int(d["target_user_id"])
        account = int(d["account_user_id"])
        outgoing = int(d.get("outgoing_count") or 0)
        if opt_out_svc.is_opted_out(target):
            store.close_for_opt_out(target)
            await _cleanup_disabled_account(account)
            continue
        if outgoing >= store.MAX_OUTGOING:
            store.set_stage(target, store.STAGE_CLOSED, clear_auto_link=True)
            store.mark_contact_completed(target)
            await _cleanup_disabled_account(account)
            continue
        key = (account, target)
        lock = _dialog_locks.setdefault(key, asyncio.Lock())
        if lock.locked():
            continue
        async with lock:
            _inflight.add(key)
            try:
                text = ai_dialog.followup_silence_text()
                if not dialog_delivery.prepare(
                    target, account, dialog_delivery.KIND_FOLLOWUP, text
                ):
                    continue
                result = await _send_prepared_action(
                    account, target, dialog_delivery.KIND_FOLLOWUP, text
                )
                if result == "sent":
                    n += 1
                    logger.info("Silence follow-up sent target={} account={}", target, account)
                elif result == "failed":
                    current = store.get_dialog(target)
                    if current and current.get("stage") != store.STAGE_CLOSED:
                        store.set_stage(
                            target,
                            store.STAGE_WAITING_REPLY,
                            auto_link_at=(_now() + dt.timedelta(hours=2)).isoformat(),
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
