"""First-DM dispatcher: pending lead × preferred ready account."""

from __future__ import annotations

import asyncio
import datetime as dt
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
from telethon.tl.types import InputPeerUser

from services import accounts as accounts_svc
from services import first_dm_delivery
from services import chats as chats_svc
from services import monitor as monitor_svc
from services import opt_out as opt_out_svc
from services import pacing
from services import phrases as phrases_svc
from services import queue as queue_svc
from services import runtime
from services import telegram_history
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
        except asyncio.TimeoutError:
            logger.warning("Dispatcher stop timed out; cancelling worker")
            _worker_task.cancel()
            await asyncio.gather(_worker_task, return_exceptions=True)
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
        if ticks % 3 == 0:
            try:
                recovered = await recover_ambiguous_first_dms()
                if recovered:
                    logger.warning("Reconciled {} ambiguous First-DM delivery(s)", recovered)
            except Exception as recovery_exc:
                logger.exception("First-DM recovery loop failed: {}", recovery_exc)
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
    """Try the account that discovered the user first, then random fallbacks.

    The source account is the most likely to have the correct Telegram entity/access hash.
    """
    if not ready:
        return []
    pool = list(ready)
    random.shuffle(pool)
    source = lead.get("source_account_user_id")
    if source is None:
        return pool
    source_id = int(source)
    for i, acc in enumerate(pool):
        if int(acc["user_id"]) == source_id:
            pool.insert(0, pool.pop(i))
            break
    return pool


def _participating_sender_ids() -> set[int]:
    """Accounts that may legitimately send a future First DM for this queue."""
    return {
        int(acc["user_id"])
        for acc in accounts_svc.list_accounts()
        if acc.get("participates") and acc.get("session_string")
    }


def _untried_ready_accounts(
    lead: dict[str, Any], ready: list[dict[str, Any]]
) -> list[dict[str, Any]]:
    attempted = queue_svc.failed_account_ids(
        int(lead["target_user_id"]), "no_entity"
    )
    return [
        acc
        for acc in _order_accounts_for_lead(lead, ready)
        if int(acc["user_id"]) not in attempted
    ]


def _finish_or_defer_unresolvable(target_id: int, lead: dict[str, Any]) -> bool:
    """Finish only after every participating account proved it cannot resolve target."""
    if queue_svc.identity_snapshot_changed(target_id, lead):
        queue_svc.clear_account_failures(target_id)
        queue_svc.defer_claim(
            target_id, seconds=1, reason="telegram_identity_updated_during_attempt"
        )
        logger.info("Lead entity evidence refreshed during attempt target={}", target_id)
        return False
    attempted = queue_svc.failed_account_ids(target_id, "no_entity")
    possible = _participating_sender_ids()
    if possible and possible.issubset(attempted):
        reason = f"no_entity_all_accounts:{','.join(map(str, sorted(attempted)))}"
        queue_svc.mark_terminal_failure(
            target_id, "no_entity_all_accounts", reason
        )
        logger.warning(
            "Lead terminal: no Telegram entity target={} checked_accounts={}",
            target_id,
            sorted(attempted),
        )
        return True

    waiting = sorted(possible - attempted)
    queue_svc.defer_claim(
        target_id,
        seconds=60,
        reason=f"waiting_untried_accounts:{','.join(map(str, waiting))}",
    )
    logger.debug(
        "Lead deferred: target={} waiting_untried_accounts={}", target_id, waiting
    )
    return False


async def _attempt_lead_across_accounts(
    lead: dict[str, Any], accounts: list[dict[str, Any]], text: str
) -> bool:
    target_id = int(lead["target_user_id"])
    had_retryable_error = False
    had_account_cooldown = False
    for acc in accounts:
        account_id = int(acc["user_id"])
        fresh = accounts_svc.get_account(account_id) or acc
        ok, _reason = pacing.account_is_send_ready(fresh)
        if not ok:
            continue
        client = monitor_svc.get_client(account_id)
        if client is None or not client.is_connected():
            continue

        if not queue_svc.ensure_claim(target_id, account_id):
            return True

        result = await _send_first_dm(client, account_id, lead, text)
        if result == "sent":
            phrases_svc.remember(phrases_svc.KIND_FIRST_DM, text)
            _remember_text(text)
            pacing.mark_global_sent()
            return True
        if result in {"flood", "peerflood"}:
            had_account_cooldown = True
            continue
        if result == "error":
            had_retryable_error = True
            continue
        if result in {"privacy", "invalid", "terminal"}:
            return True
        if result == "no_entity":
            queue_svc.record_account_failure(
                target_id, account_id, "no_entity", "Telegram entity unavailable"
            )
            continue
        if result == "ambiguous":
            return True

    if had_retryable_error:
        last_error = queue_svc.get_last_error(target_id)
        attempts = queue_svc.bump_send_attempts(target_id)
        if attempts >= queue_svc.MAX_SEND_ATTEMPTS:
            detail = (
                f"{last_error} | retryable rounds exhausted:{attempts}"
                if last_error else f"retryable rounds exhausted:{attempts}"
            )
            queue_svc.mark_terminal_failure(
                target_id, "max_transient_attempts", detail
            )
            logger.error(
                "Lead terminal after retryable rounds target={} attempts={}",
                target_id, attempts,
            )
            return True
        queue_svc.defer_claim(
            target_id, seconds=60,
            reason=f"retryable_round:{attempts}/{queue_svc.MAX_SEND_ATTEMPTS}",
        )
        return False
    if had_account_cooldown:
        queue_svc.defer_claim(
            target_id, seconds=60, reason="sender_account_cooldown"
        )
        return False
    return _finish_or_defer_unresolvable(target_id, lead)


async def _tick() -> bool:
    if not pacing.global_ready():
        return False
    ready = _list_ready_accounts()
    if not ready:
        return False

    claimer = random.choice(ready)
    lead = queue_svc.claim_random_pending(int(claimer["user_id"]))
    if not lead:
        return False
    target_id = int(lead["target_user_id"])

    if opt_out_svc.is_opted_out(target_id):
        queue_svc.cancel_lead(target_id, "opt_out")
        return True

    ordered = _untried_ready_accounts(lead, ready)
    if not ordered:
        return _finish_or_defer_unresolvable(target_id, lead)

    # Generate once and reuse for account fallbacks. No generation occurs while
    # the lead merely waits for an unavailable, not-yet-tried account.
    text = await generate_first_dm()
    return await _attempt_lead_across_accounts(lead, ordered, text)


def _target_label(lead: dict[str, Any]) -> str:
    return queue_svc.format_target_label(lead)


def _account_label(account_id: int) -> str:
    acc = accounts_svc.get_account(int(account_id))
    if not acc:
        return f"id {account_id}"
    return accounts_svc.format_account_label(acc, include_id=False)


def _notify_first_dm(account_id: int, lead: dict[str, Any], text: str) -> str:
    """Unified admin notification for the only routine event: successful First DM."""
    from_label = _account_label(account_id)
    to_label = _target_label(lead)
    source = queue_svc.source_label(lead)
    now_local = dt.datetime.now(dt.timezone(dt.timedelta(hours=3))).strftime("%H:%M")
    today = queue_svc.count_first_dm_today()
    total = queue_svc.count_first_dm_total()
    return "\n".join(
        [
            "📨 **FIRST DM ОТПРАВЛЕН**",
            "━━━━━━━━━━━━━━━━━━",
            "",
            f"👤 Аккаунт: **{from_label}**",
            f"🎯 Пользователь: **{to_label}**",
            f"📍 Источник: **{source}**",
            f"🕒 Время: **{now_local} МСК**",
            "",
            f"📬 Сегодня: **{today}**",
            f"📊 Всего: **{total}**",
        ]
    )


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


async def _resolve_target_entity(client, account_id: int, lead: dict[str, Any]):
    """Resolve target on the source account first, then by cached entity/username."""
    target_id = int(lead["target_user_id"])
    username = (lead.get("username") or "").strip().lstrip("@")
    access_hash = lead.get("access_hash")
    source_account_id = lead.get("source_account_user_id")

    if (
        access_hash is not None
        and source_account_id is not None
        and int(source_account_id) == int(account_id)
    ):
        return InputPeerUser(target_id, int(access_hash))

    try:
        return await client.get_input_entity(target_id)
    except (ValueError, TypeError, LookupError) as exc:
        logger.debug(
            "Entity by id unavailable target={} account={}: {}",
            target_id,
            account_id,
            type(exc).__name__,
        )

    if username:
        try:
            return await client.get_input_entity(username)
        except (ValueError, TypeError, LookupError) as exc:
            logger.debug(
                "Entity by username unavailable target={} account={}: {}",
                target_id,
                account_id,
                type(exc).__name__,
            )
    return None


def _normalize_message_text(value: str | None) -> str:
    return (value or "").replace("\r\n", "\n").strip()


async def recover_ambiguous_first_dms() -> int:
    """Reconcile prepared First DMs after a crash/network ambiguity.

    The bot checks the real Telegram conversation before any retry. Exact matching
    outgoing text confirms delivery; absence of evidence safely returns the lead
    to pending. If Telegram cannot be checked, state remains prepared and no
    duplicate is sent.
    """
    recovered = 0
    rows = first_dm_delivery.list_stale_prepared(older_than_seconds=90, limit=20)
    for row in rows:
        target_id = int(row["target_user_id"])
        account_id = int(row["account_user_id"])
        client = monitor_svc.get_client(account_id)
        if client is None or not client.is_connected():
            logger.warning(
                "Prepared First DM awaits reconciliation: no client account={} target={}",
                account_id,
                target_id,
            )
            continue

        entity = await _resolve_target_entity(client, account_id, row)
        if entity is None:
            logger.warning(
                "Prepared First DM awaits reconciliation: no entity account={} target={}",
                account_id,
                target_id,
            )
            continue

        prepared_at = pacing._parse_iso(row.get("prepared_at"))
        lower_bound = (
            prepared_at - dt.timedelta(minutes=2)
            if prepared_at is not None
            else None
        )
        try:
            found = await telegram_history.find_outgoing_text_since(
                client,
                entity,
                str(row.get("text") or ""),
                since=lower_bound,
            )
        except Exception as exc:
            logger.exception(
                "Cannot inspect Telegram history account={} target={}: {}",
                account_id,
                target_id,
                exc,
            )
            continue

        if found is not None:
            first_dm_delivery.commit_sent(
                target_id,
                telegram_message_id=getattr(found, "id", None),
                sent_at=(getattr(found, "date", None) or pacing._now()).isoformat(),
            )
            try:
                pacing.record_successful_send(account_id)
                pacing.mark_global_sent()
            except Exception as exc:
                logger.exception(
                    "Recovered First DM accounting failed account={} target={}: {}",
                    account_id,
                    target_id,
                    exc,
                )
            try:
                from services import audience as audience_svc

                audience_svc.record_first_dm(
                    target_id,
                    username=row.get("username"),
                    first_name=row.get("first_name"),
                    last_name=row.get("last_name"),
                    access_hash=row.get("access_hash"),
                    source_chat_id=row.get("source_chat_id"),
                    source_account_user_id=account_id,
                )
            except Exception as exc:
                logger.exception(
                    "Recovered First DM audience record failed target={}: {}",
                    target_id,
                    exc,
                )
            recovered += 1
            logger.warning(
                "Recovered delivered First DM from Telegram account={} target={} msg_id={}",
                account_id,
                target_id,
                getattr(found, "id", None),
            )
        else:
            first_dm_delivery.rollback(target_id, "telegram_history_no_match", as_pending=True)
            recovered += 1
            logger.warning(
                "Prepared First DM not found in Telegram; returned to queue account={} target={}",
                account_id,
                target_id,
            )
    return recovered


async def _send_first_dm(
    client,
    account_id: int,
    lead: dict[str, Any],
    text: str,
) -> str:
    target_id = int(lead["target_user_id"])
    prepared = False

    try:
        entity = await _resolve_target_entity(client, account_id, lead)
        if entity is None:
            logger.warning(
                "No entity for target={} account={} source_account={}",
                target_id,
                account_id,
                lead.get("source_account_user_id"),
            )
            return "no_entity"

        prepared = first_dm_delivery.prepare(target_id, account_id, text)
        if not prepared:
            logger.error(
                "First DM prepare rejected account={} target={}; network send blocked",
                account_id,
                target_id,
            )
            queue_svc.set_last_error(target_id, "first_dm_prepare_rejected")
            queue_svc.defer_claim(
                target_id, seconds=60, reason="first_dm_prepare_rejected"
            )
            return "error"

        sent_message = await client.send_message(entity, text)
        committed = first_dm_delivery.commit_sent(
            target_id,
            telegram_message_id=getattr(sent_message, "id", None),
            sent_at=(getattr(sent_message, "date", None) or pacing._now()).isoformat(),
        )
        if not committed:
            logger.critical(
                "Telegram accepted First DM but SQLite commit is pending account={} target={}",
                account_id,
                target_id,
            )
            return "ambiguous"

        pacing.record_successful_send(account_id)
        try:
            from services import audience as audience_svc

            audience_svc.record_first_dm(
                target_id,
                username=lead.get("username"),
                first_name=lead.get("first_name"),
                last_name=lead.get("last_name"),
                access_hash=lead.get("access_hash"),
                source_chat_id=lead.get("source_chat_id"),
                source_account_user_id=account_id,
            )
        except Exception as exc:
            logger.exception("Audience record after First DM failed: {}", exc)
        logger.info(
            "First DM sent account={} target={} msg_id={} text={!r}",
            account_id,
            target_id,
            getattr(sent_message, "id", None),
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
        if prepared:
            first_dm_delivery.rollback(target_id, f"FloodWait:{seconds}", as_pending=True)
        else:
            queue_svc.release_claim(target_id, as_pending=True)
        pacing.apply_floodwait(account_id, seconds)
        return "flood"

    except PeerFloodError:
        logger.warning("PeerFlood account={} target={}", account_id, target_id)
        if prepared:
            first_dm_delivery.rollback(target_id, "PeerFlood", as_pending=True)
        else:
            queue_svc.release_claim(target_id, as_pending=True)
        try:
            from services import spambot as spambot_svc

            await spambot_svc.on_peer_flood(account_id)
        except Exception as exc:
            logger.exception("SpamBot on_peer_flood failed: {}", exc)
            pacing.set_paused(account_id, "PeerFlood", paused=True)
        return "peerflood"

    except (UserPrivacyRestrictedError, UserIsBlockedError, ChatWriteForbiddenError):
        logger.info("Cannot write target={} from account={}", target_id, account_id)
        if prepared:
            first_dm_delivery.rollback(target_id, "privacy_or_blocked", as_pending=False)
        queue_svc.cancel_lead(target_id, "privacy_or_blocked")
        return "privacy"

    except (InputUserDeactivatedError, UserBannedInChannelError, ValueError):
        logger.info("Invalid target={} from account={}", target_id, account_id)
        if prepared:
            first_dm_delivery.rollback(target_id, "invalid_target", as_pending=False)
        queue_svc.cancel_lead(target_id, "invalid_target")
        return "invalid"

    except Exception as exc:
        logger.exception(
            "First DM send became ambiguous account={} target={}: {}",
            account_id,
            target_id,
            exc,
        )
        if prepared:
            # Do not retry through another account: Telegram may have accepted it.
            return "ambiguous"
        detail = f"{type(exc).__name__}: {exc}"
        queue_svc.set_last_error(target_id, detail)
        queue_svc.release_claim(target_id, as_pending=True)
        logger.warning(
            "Retryable pre-send error target={} account={} error={}",
            target_id, account_id, detail
        )
        return "error"
