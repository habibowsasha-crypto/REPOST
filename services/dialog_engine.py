"""Dialog funnel after first DM: explain → auto-link → stop/opt-out."""

from __future__ import annotations

import asyncio
import datetime as dt
import random
from typing import Set, Tuple

from loguru import logger
from telethon.errors import (
    FloodWaitError,
    PeerFloodError,
    UserIsBlockedError,
    UserPrivacyRestrictedError,
    ChatWriteForbiddenError,
)

from config import (
    AI_AUTO_LINK_DELAY_MAX,
    AI_AUTO_LINK_DELAY_MIN,
    AI_REPLY_DELAY_MAX,
    AI_REPLY_DELAY_MIN,
)
from services import ai_dialog
from services import dialog_store as store
from services import monitor as monitor_svc
from services import opt_out as opt_out_svc
from services import pacing
from services import phrases as phrases_svc

# Prevent parallel handling of the same dialog.
_inflight: Set[Tuple[int, int]] = set()


def _now() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)


def _delay_reply() -> float:
    from services import runtime as runtime_svc

    lo, hi = runtime_svc.get_ai_reply_delay_range()
    return float(random.randint(lo, hi))


def _auto_link_delay() -> int:
    from services import runtime as runtime_svc

    lo, hi = runtime_svc.get_auto_link_delay_range()
    return random.randint(lo, hi)


async def on_first_dm_sent(target_user_id: int, account_user_id: int, text: str) -> None:
    store.create_after_first_dm(target_user_id, account_user_id, text)


async def handle_incoming_private(
    account_user_id: int, target_user_id: int, text: str
) -> None:
    """Process user reply in private chat for an in-progress dialog."""
    text = (text or "").strip()
    if not text:
        return

    key = (int(account_user_id), int(target_user_id))
    if key in _inflight:
        logger.debug("Dialog already in-flight for {}", key)
        return
    _inflight.add(key)
    try:
        await _handle_incoming_private_body(account_user_id, target_user_id, text)
    finally:
        _inflight.discard(key)


async def _handle_incoming_private_body(
    account_user_id: int, target_user_id: int, text: str
) -> None:
    dialog = store.get_dialog(target_user_id)
    if not dialog:
        return
    if int(dialog.get("account_user_id") or 0) != int(account_user_id):
        return
    if dialog.get("stage") == store.STAGE_CLOSED:
        return
    if opt_out_svc.is_opted_out(target_user_id):
        store.set_stage(target_user_id, store.STAGE_CLOSED, clear_auto_link=True)
        store.mark_contact_completed(target_user_id)
        return

    store.append_history(target_user_id, "user", text)

    if ai_dialog.is_hard_stop(text):
        await _send_and_close(
            account_user_id,
            target_user_id,
            ai_dialog.apology_text(),
            opt_out=True,
            reason="user_stop",
        )
        return

    # Pause: no funnel replies / auto-link while worker is off.
    # Group monitoring still fills the queue independently.
    from services import runtime as runtime_svc

    if not runtime_svc.is_worker_enabled():
        logger.info(
            "Dialog paused (worker off) target={} account={}",
            target_user_id,
            account_user_id,
        )
        return

    stage = dialog.get("stage")
    outgoing = int(dialog.get("outgoing_count") or 0)

    if stage in (store.STAGE_WAITING_REPLY, store.STAGE_FOLLOWUP_SENT):
        # Cancel silence follow-up immediately so it cannot race with explain.
        store.set_stage(target_user_id, stage, clear_auto_link=True)
        if ai_dialog.is_soft_decline(text):
            await _send_and_close(
                account_user_id,
                target_user_id,
                ai_dialog.soft_close_text(),
                opt_out=False,
                reason="soft_no",
            )
            return
        await asyncio.sleep(_delay_reply())
        history = (store.get_dialog(target_user_id) or {}).get("history") or []
        explain = await ai_dialog.generate_explain(history)
        ok = await _send_private(account_user_id, target_user_id, explain)
        if not ok:
            # Follow-up timer was cleared above; restore so silence path still works.
            if stage == store.STAGE_WAITING_REPLY:
                later = (
                    _now() + dt.timedelta(hours=store.FOLLOWUP_DELAY_HOURS)
                ).isoformat()
                store.set_stage(
                    target_user_id, store.STAGE_WAITING_REPLY, auto_link_at=later
                )
            return
        phrases_svc.remember(phrases_svc.KIND_EXPLAIN, explain)
        store.append_history(target_user_id, "assistant", explain)
        auto_at = (_now() + dt.timedelta(seconds=_auto_link_delay())).isoformat()
        store.set_stage(
            target_user_id,
            store.STAGE_EXPLAINED,
            bump_outgoing=True,
            auto_link_at=auto_at,
        )
        return

    if stage == store.STAGE_EXPLAINED:
        if ai_dialog.is_soft_decline(text):
            await _send_and_close(
                account_user_id,
                target_user_id,
                ai_dialog.soft_close_text(),
                opt_out=False,
                reason="soft_no",
            )
            return
        await asyncio.sleep(_delay_reply())
        history = (store.get_dialog(target_user_id) or {}).get("history") or []
        link_already = bool(dialog.get("link_sent"))
        reply = await ai_dialog.generate_contextual_reply(
            history, include_link=not link_already
        )
        from config import CHANNEL_LINK

        # Ensure link is in the text BEFORE send when we still need it.
        if (not link_already) and CHANNEL_LINK and CHANNEL_LINK not in (reply or ""):
            reply = f"{(reply or '').rstrip()}\n{CHANNEL_LINK}"

        ok = await _send_private(account_user_id, target_user_id, reply)
        if not ok:
            # Keep stage explained; re-schedule auto-link.
            later = (_now() + dt.timedelta(seconds=_auto_link_delay())).isoformat()
            if not link_already:
                store.set_stage(
                    target_user_id,
                    store.STAGE_EXPLAINED,
                    auto_link_at=later,
                )
            return

        link_now = bool(CHANNEL_LINK and CHANNEL_LINK in (reply or ""))
        store.append_history(target_user_id, "assistant", reply)

        if link_now:
            store.set_stage(
                target_user_id,
                store.STAGE_LINK_SENT,
                bump_outgoing=True,
                link_sent=True,
                clear_auto_link=True,
            )
            phrases_svc.remember(phrases_svc.KIND_LINK, reply)
        else:
            # No link in this turn — keep explained and RE-schedule auto-link.
            later = (_now() + dt.timedelta(seconds=_auto_link_delay())).isoformat()
            store.set_stage(
                target_user_id,
                store.STAGE_EXPLAINED,
                bump_outgoing=True,
                auto_link_at=later,
            )
        d2 = store.get_dialog(target_user_id)
        if d2 and int(d2.get("outgoing_count") or 0) >= store.MAX_OUTGOING:
            store.set_stage(target_user_id, store.STAGE_CLOSED)
            store.mark_contact_completed(target_user_id)
        return

    if stage == store.STAGE_LINK_SENT:
        # Budget: first + explain + link (+ optional close/apology).
        # After link — no more pitch, only short close.
        if ai_dialog.is_soft_decline(text):
            await _send_and_close(
                account_user_id,
                target_user_id,
                ai_dialog.soft_close_text(),
                opt_out=False,
                reason="soft_no",
            )
            return
        # Any further chat: one short goodbye, then closed (uses last slot).
        await _send_and_close(
            account_user_id,
            target_user_id,
            random.choice(
                [
                    "Если что - ссылка выше. Больше не отвлекаю.",
                    "Ок, глянь по ссылке если интересно. Больше не пишу.",
                    "На связи не буду, ссылка уже выше. Хорошего дня.",
                ]
            ),
            opt_out=False,
            reason="funnel_done",
        )
        return


async def process_due_auto_links() -> int:
    """Send link messages for explained dialogs past auto_link_at."""
    from services import runtime as runtime_svc

    if not runtime_svc.is_worker_enabled():
        return 0
    due = store.list_due_auto_links()
    n = 0
    for d in due:
        target = int(d["target_user_id"])
        account = int(d["account_user_id"])
        if int(d.get("link_sent") or 0):
            continue
        key = (account, target)
        if key in _inflight:
            continue
        _inflight.add(key)
        try:
            history = d.get("history") or []
            text = await ai_dialog.generate_link_wrap(history)
            ok = await _send_private(account, target, text)
            if not ok:
                later = (_now() + dt.timedelta(minutes=5)).isoformat()
                store.set_stage(target, store.STAGE_EXPLAINED, auto_link_at=later)
                continue
            phrases_svc.remember(phrases_svc.KIND_LINK, text)
            store.append_history(target, "assistant", text)
            store.set_stage(
                target,
                store.STAGE_LINK_SENT,
                bump_outgoing=True,
                link_sent=True,
                clear_auto_link=True,
            )
            n += 1
            logger.info("Auto-link sent target={} account={}", target, account)
        finally:
            _inflight.discard(key)
    return n


async def process_due_followups() -> int:
    """After ~24h silence on first DM: one soft apology, no channel/link."""
    from services import runtime as runtime_svc

    if not runtime_svc.is_worker_enabled():
        return 0
    due = store.list_due_followups()
    n = 0
    for d in due:
        target = int(d["target_user_id"])
        account = int(d["account_user_id"])
        outgoing = int(d.get("outgoing_count") or 0)
        if outgoing >= store.MAX_OUTGOING:
            store.set_stage(target, store.STAGE_CLOSED, clear_auto_link=True)
            store.mark_contact_completed(target)
            continue
        key = (account, target)
        if key in _inflight:
            continue
        _inflight.add(key)
        try:
            text = ai_dialog.followup_silence_text()
            ok = await _send_private(account, target, text)
            if not ok:
                # retry later (+2h)
                later = (_now() + dt.timedelta(hours=2)).isoformat()
                store.set_stage(
                    target, store.STAGE_WAITING_REPLY, auto_link_at=later
                )
                continue
            store.append_history(target, "assistant", text)
            store.set_stage(
                target,
                store.STAGE_FOLLOWUP_SENT,
                bump_outgoing=True,
                clear_auto_link=True,
            )
            n += 1
            logger.info("Silence follow-up sent target={} account={}", target, account)
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
) -> None:
    await asyncio.sleep(min(5.0, _delay_reply()))
    ok = await _send_private(account_user_id, target_user_id, text)
    if ok:
        store.append_history(target_user_id, "assistant", text)
    else:
        logger.warning(
            "Close message failed target={} account={} reason={}",
            target_user_id,
            account_user_id,
            reason,
        )
    # Always stop our side; hard-stop still opts out even if apology failed to send.
    store.set_stage(
        target_user_id,
        store.STAGE_CLOSED,
        bump_outgoing=bool(ok),
        clear_auto_link=True,
    )
    store.mark_contact_completed(target_user_id)
    if opt_out:
        opt_out_svc.add(target_user_id, reason)
        logger.info("Opt-out target={} reason={}", target_user_id, reason)


async def _send_private(account_user_id: int, target_user_id: int, text: str) -> bool:
    client = monitor_svc.get_client(account_user_id)
    if client is None or not client.is_connected():
        logger.warning(
            "No client for dialog account={} target={}", account_user_id, target_user_id
        )
        return False
    try:
        entity = await client.get_input_entity(int(target_user_id))
        await client.send_message(entity, text)
        return True
    except FloodWaitError as exc:
        seconds = int(getattr(exc, "seconds", 60) or 60)
        logger.warning(
            "FloodWait dialog account={} target={} sec={}",
            account_user_id,
            target_user_id,
            seconds,
        )
        pacing.apply_floodwait(account_user_id, seconds)
        return False
    except PeerFloodError:
        logger.warning(
            "PeerFlood dialog account={} target={}", account_user_id, target_user_id
        )
        try:
            from services import spambot as spambot_svc

            await spambot_svc.on_peer_flood(account_user_id)
        except Exception as sp_exc:
            logger.exception("SpamBot from dialog: {}", sp_exc)
            pacing.set_paused(account_user_id, "PeerFlood", paused=True)
        return False
    except (UserPrivacyRestrictedError, UserIsBlockedError, ChatWriteForbiddenError):
        logger.info(
            "Cannot write dialog target={} account={}", target_user_id, account_user_id
        )
        store.set_stage(target_user_id, store.STAGE_CLOSED, clear_auto_link=True)
        store.mark_contact_completed(target_user_id)
        return False
    except Exception as exc:
        logger.exception(
            "Dialog send failed account={} target={}: {}",
            account_user_id,
            target_user_id,
            exc,
        )
        return False
