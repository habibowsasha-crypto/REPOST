"""First-DM dispatcher: pending lead × preferred ready account."""

from __future__ import annotations

import asyncio
import random
from typing import Any, Optional

from loguru import logger
from telethon.errors import (
    FloodWaitError,
    PeerFloodError,
    UserPrivacyRestrictedError,
    UserIsBlockedError,
    InputUserDeactivatedError,
    ChatWriteForbiddenError,
    UserBannedInChannelError,
)

from services import accounts as accounts_svc
from services import chats as chats_svc
from services import monitor as monitor_svc
from services import opt_out as opt_out_svc
from services import pacing
from services import phrases as phrases_svc
from services import queue as queue_svc
from services import runtime
from services.ai_first_dm import generate_first_dm

_worker_task: Optional[asyncio.Task] = None
_stop_event: Optional[asyncio.Event] = None
_recent_texts: list[str] = []


def is_worker_running() -> bool:
    return _worker_task is not None and not _worker_task.done()


def worker_status() -> dict[str, Any]:
    return {
        "enabled": runtime.is_worker_enabled(),
        "loop_running": is_worker_running(),
        "global_wait_sec": round(pacing.seconds_until_global_ready(), 1),
    }


async def start_worker() -> None:
    global _worker_task, _stop_event
    runtime.set_worker_enabled(True)
    if is_worker_running():
        return
    _stop_event = asyncio.Event()
    _worker_task = asyncio.create_task(_worker_loop(), name="dm-dispatcher")
    logger.info("Dispatcher worker started")


async def stop_worker() -> None:
    global _worker_task, _stop_event
    runtime.set_worker_enabled(False)
    if _stop_event is not None:
        _stop_event.set()
    if _worker_task is not None:
        try:
            await asyncio.wait_for(_worker_task, timeout=15)
        except Exception:
            _worker_task.cancel()
        _worker_task = None
    _stop_event = None
    logger.info("Dispatcher worker stopped")


async def ensure_worker_from_runtime() -> None:
    if runtime.is_worker_enabled() and not is_worker_running():
        await start_worker()


async def _worker_loop() -> None:
    assert _stop_event is not None
    logger.info("Dispatcher loop running")
    ticks = 0
    while not _stop_event.is_set():
        if not runtime.is_worker_enabled():
            break
        try:
            did = await _tick()
        except Exception as exc:
            logger.exception("Dispatcher tick error: {}", exc)
            did = False
        ticks += 1
        if ticks % 6 == 0:
            try:
                n = queue_svc.release_stale_claims(older_than_seconds=900)
                if n:
                    logger.warning("Released {} stale claimed leads", n)
            except Exception as sc_exc:
                logger.exception("stale claims release: {}", sc_exc)
        timeout = 2.0 if did else 5.0
        try:
            await asyncio.wait_for(_stop_event.wait(), timeout=timeout)
            break
        except asyncio.TimeoutError:
            continue
    logger.info("Dispatcher loop exit")


def _list_ready_accounts() -> list[dict[str, Any]]:
    ready: list[dict[str, Any]] = []
    for acc in accounts_svc.list_accounts():
        if not acc.get("participates"):
            continue
        ok, _reason = pacing.account_is_send_ready(acc)
        if not ok:
            continue
        client = monitor_svc.get_client(int(acc["user_id"]))
        if client is None or not client.is_connected():
            continue
        ready.append(acc)
    return ready


def _order_accounts_for_lead(
    lead: dict[str, Any], ready: list[dict[str, Any]]
) -> list[dict[str, Any]]:
    """Prefer source account, then accounts watching source chat, then others."""
    source = lead.get("source_account_user_id")
    source_chat = lead.get("source_chat_id")
    preferred: list[dict[str, Any]] = []
    same_chat: list[dict[str, Any]] = []
    others: list[dict[str, Any]] = []
    for acc in ready:
        uid = int(acc["user_id"])
        if source is not None and uid == int(source):
            preferred.append(acc)
        elif source_chat is not None and chats_svc.is_chat_watchable(
            uid, int(source_chat)
        ):
            same_chat.append(acc)
        else:
            others.append(acc)
    random.shuffle(same_chat)
    random.shuffle(others)
    return preferred + same_chat + others


async def _tick() -> bool:
    if not pacing.global_ready():
        return False
    ready = _list_ready_accounts()
    if not ready:
        return False

    # Temporary claim by any ready account, then reassign to preferred sender.
    lead = queue_svc.claim_random_pending(int(ready[0]["user_id"]))
    if not lead:
        return False
    target_id = int(lead["target_user_id"])

    if opt_out_svc.is_opted_out(target_id):
        queue_svc.cancel_lead(target_id, "opt_out")
        return False

    ordered = _order_accounts_for_lead(lead, ready)
    if not ordered:
        queue_svc.release_claim(target_id, as_pending=True)
        return False

    # One AI/local text per tick — reuse across account fallbacks.
    text = await generate_first_dm()

    for acc in ordered:
        account_id = int(acc["user_id"])
        ok, _reason = pacing.account_is_send_ready(acc)
        if not ok:
            continue
        client = monitor_svc.get_client(account_id)
        if client is None:
            continue

        # Previous attempt may have released the claim (PeerFlood/error).
        if not queue_svc.ensure_claim(target_id, account_id):
            return True  # lead already sent/cancelled

        result = await _send_first_dm(client, account_id, lead, text)
        if result == "sent":
            phrases_svc.remember(phrases_svc.KIND_FIRST_DM, text)
            _remember_text(text)
            pacing.mark_global_sent()
            return True
        if result == "flood":
            # Account-level cooldown only; try next ready account.
            continue
        if result == "peerflood":
            # Account paused; try next ready account for this lead.
            continue
        if result in {"privacy", "invalid"}:
            return True  # lead closed
        if result == "no_entity":
            # Try next account that may resolve entity.
            continue
        if result == "error":
            # Soft error: try next account via ensure_claim.
            continue
    # Nobody could send — release if still claimed
    queue_svc.release_claim(target_id, as_pending=True)
    return False


def _target_label(lead: dict[str, Any]) -> str:
    return queue_svc.format_target_label(lead)


def _account_label(account_id: int) -> str:
    acc = accounts_svc.get_account(int(account_id))
    if not acc:
        return f"id {account_id}"
    return accounts_svc.format_account_label(acc, include_id=False)


def _notify_first_dm(account_id: int, lead: dict[str, Any], text: str) -> str:
    """Pretty admin card for a successful first DM."""
    from_label = _account_label(account_id)
    to_label = _target_label(lead)
    snippet = (text or "").replace("\n", " ").strip()
    if len(snippet) > 80:
        snippet = snippet[:77].rstrip() + "…"
    lines = [
        "💌  **First DM**",
        "━━━━━━━━━━━━",
        f"📤  **{from_label}**",
        f"📥  **{to_label}**",
        "━━━━━━━━━━━━",
        f"💬  _{snippet}_",
    ]
    return "\n".join(lines)


async def _notify_admins_first_dm(account_id: int, lead: dict[str, Any], text: str) -> None:
    try:
        from services.spambot import notify_admins

        await notify_admins(_notify_first_dm(account_id, lead, text))
    except Exception as exc:
        logger.debug("first dm notify failed: {}", exc)


def _remember_text(text: str) -> None:
    _recent_texts.append(text)
    if len(_recent_texts) > 50:
        del _recent_texts[:-50]


async def _send_first_dm(
    client,
    account_id: int,
    lead: dict[str, Any],
    text: str,
) -> str:
    target_id = int(lead["target_user_id"])
    username = (lead.get("username") or "").strip().lstrip("@")

    try:
        entity = None
        try:
            entity = await client.get_input_entity(target_id)
        except Exception:
            if username:
                entity = await client.get_input_entity(username)
            else:
                attempts = queue_svc.bump_send_attempts(target_id)
                logger.warning(
                    "No entity for target={} account={} attempts={}",
                    target_id,
                    account_id,
                    attempts,
                )
                if attempts >= queue_svc.MAX_SEND_ATTEMPTS:
                    queue_svc.cancel_lead(target_id, "no_entity")
                    return "no_entity"
                queue_svc.release_claim(target_id, as_pending=True)
                return "no_entity"

        # Anti-duplicate: contact before network send.
        queue_svc.mark_sending(target_id, account_id)

        await client.send_message(entity, text)
        queue_svc.mark_sent(target_id, account_id)
        pacing.record_successful_send(account_id)
        try:
            from services import audience as audience_svc

            audience_svc.record_first_dm(
                target_id,
                username=lead.get("username"),
                first_name=lead.get("first_name"),
                last_name=lead.get("last_name"),
                source_chat_id=lead.get("source_chat_id"),
            )
        except Exception as a_exc:
            logger.exception("audience record after first DM: {}", a_exc)
        try:
            from services import dialog_engine

            await dialog_engine.on_first_dm_sent(target_id, account_id, text)
        except Exception as d_exc:
            logger.exception("dialog create after first DM: {}", d_exc)
        logger.info(
            "First DM sent account={} target={} text={!r}",
            account_id,
            target_id,
            text[:40],
        )
        await _notify_admins_first_dm(account_id, lead, text)
        return "sent"

    except FloodWaitError as exc:
        seconds = int(getattr(exc, "seconds", 60) or 60)
        logger.warning(
            "FloodWait account={} target={} seconds={}",
            account_id,
            target_id,
            seconds,
        )
        pacing.apply_floodwait(account_id, seconds)
        queue_svc.clear_sending_contact(target_id)
        queue_svc.release_claim(target_id, as_pending=True)
        return "flood"

    except PeerFloodError:
        logger.warning("PeerFlood account={} target={}", account_id, target_id)
        queue_svc.clear_sending_contact(target_id)
        queue_svc.release_claim(target_id, as_pending=True)
        try:
            from services import spambot as spambot_svc

            await spambot_svc.on_peer_flood(account_id)
        except Exception as sp_exc:
            logger.exception("SpamBot on_peer_flood failed: {}", sp_exc)
            pacing.set_paused(account_id, "PeerFlood", paused=True)
        return "peerflood"

    except (UserPrivacyRestrictedError, UserIsBlockedError, ChatWriteForbiddenError):
        logger.info("Cannot write target={} from account={}", target_id, account_id)
        queue_svc.clear_sending_contact(target_id)
        queue_svc.cancel_lead(target_id, "privacy_or_blocked")
        return "privacy"

    except (InputUserDeactivatedError, UserBannedInChannelError, ValueError):
        logger.info("Invalid target={} from account={}", target_id, account_id)
        queue_svc.clear_sending_contact(target_id)
        queue_svc.cancel_lead(target_id, "invalid_target")
        return "invalid"

    except Exception as exc:
        logger.exception(
            "Send failed account={} target={}: {}", account_id, target_id, exc
        )
        queue_svc.clear_sending_contact(target_id)
        attempts = queue_svc.bump_send_attempts(target_id)
        if attempts >= queue_svc.MAX_SEND_ATTEMPTS:
            queue_svc.cancel_lead(target_id, "max_attempts")
            return "invalid"
        queue_svc.release_claim(target_id, as_pending=True)
        return "error"
