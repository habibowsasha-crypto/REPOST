"""First-DM dispatcher: pending lead × preferred ready account."""

from __future__ import annotations

import asyncio
import datetime as dt
import random
import re
from typing import Any, Optional

from loguru import logger
from telethon.errors import (
    ChatWriteForbiddenError,
    FloodWaitError,
    InputUserDeactivatedError,
    PeerFloodError,
    PeerIdInvalidError,
    UserBannedInChannelError,
    UserIsBlockedError,
    UserPrivacyRestrictedError,
)
from telethon.tl.types import InputPeerUser

from services import account_auth
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

_PAID_MESSAGE_REQUIRED_RE = re.compile(r"ALLOW_PAYMENT_REQUIRED(?:_\d+)?", re.IGNORECASE)

# Legacy prepared rows must not stay in the dashboard forever. Three failed
# resolution/history checks are enough before the exact account attempt is
# rolled back and the shared queue may try another eligible account.
_UNRESOLVABLE_RECOVERY_MAX_ATTEMPTS = 3
_UNRESOLVABLE_RECOVERY_RETRY_SECONDS = 300


def _is_paid_message_required(exc: BaseException) -> bool:
    """Recognize Telegram paid-message restriction without relying on one SDK class."""
    values = [str(exc)]
    for attr in ("message", "rpc_error_name", "error_message"):
        value = getattr(exc, attr, None)
        if value is not None:
            values.append(str(value))
    return any(_PAID_MESSAGE_REQUIRED_RE.search(value or "") for value in values)


def _safe_error_name(exc: BaseException) -> str:
    return type(exc).__name__



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
            logger.opt(exception=exc).error(
                "Dispatcher tick failed error_type={}", _safe_error_name(exc)
            )
            did = False
        ticks += 1
        if ticks % 3 == 0:
            try:
                recovered = await recover_ambiguous_first_dms()
                if recovered:
                    logger.warning("Reconciled {} ambiguous First-DM delivery(s)", recovered)
            except Exception as recovery_exc:
                logger.opt(exception=recovery_exc).error(
                    "First-DM recovery loop failed error_type={}",
                    _safe_error_name(recovery_exc),
                )
        if ticks % 6 == 0:
            try:
                n = queue_svc.release_stale_claims(older_than_seconds=900)
                if n:
                    logger.warning("Released {} stale claimed leads", n)
            except Exception as sc_exc:
                logger.opt(exception=sc_exc).error(
                    "Stale claims release failed error_type={}",
                    _safe_error_name(sc_exc),
                )
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
        if not acc.get("participates") or accounts_svc.is_reauth_required(acc):
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
        if (
            acc.get("participates")
            and acc.get("session_string")
            and not accounts_svc.is_reauth_required(acc)
        )
    }


def _peerflood_lead_retry_seconds(acc: dict[str, Any] | None = None) -> int:
    """Back off the lead after PeerFlood instead of probing other accounts.

    Reuse the already approved per-account First-DM interval. The queue row gets
    its own eligible_at, so a different lead may still be processed safely.
    """
    return pacing.random_account_interval_seconds(acc)


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
    lead: dict[str, Any],
    accounts: list[dict[str, Any]],
    text: str | None = None,
    *,
    enforce_global_pause: bool = False,
) -> bool:
    """Try ready accounts in the existing order.

    Production dispatch passes ``text=None`` so Telegram entity resolution happens
    before the first AI call. A provided text is retained as a compatibility path
    for tests and recovery helpers that already own a generated First DM.
    """
    target_id = int(lead["target_user_id"])
    generated_text = text
    lazy_generation = generated_text is None
    had_retryable_error = False
    had_account_cooldown = False

    for acc in accounts:
        if enforce_global_pause and not runtime.is_worker_enabled():
            queue_svc.release_claim(target_id, as_pending=True)
            logger.info(
                "First DM attempt stopped because global pause became active "
                "target={}",
                target_id,
            )
            return False
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

        entity = None
        if lazy_generation:
            try:
                entity = await _resolve_target_entity(client, account_id, lead)
            except FloodWaitError as exc:
                seconds = int(getattr(exc, "seconds", 60) or 60)
                queue_svc.release_claim(target_id, as_pending=True)
                pacing.apply_floodwait(account_id, seconds)
                had_account_cooldown = True
                logger.warning(
                    "Entity resolution FloodWait account={} target={} seconds={}",
                    account_id,
                    target_id,
                    seconds,
                )
                continue
            except PeerFloodError:
                try:
                    from services import spambot as spambot_svc

                    await spambot_svc.on_peer_flood(account_id)
                except Exception as exc:
                    logger.error(
                        "SpamBot on_peer_flood failed account={} error_type={}",
                        account_id,
                        _safe_error_name(exc),
                    )
                    pacing.set_paused(account_id, "PeerFlood", paused=True)
                retry_seconds = _peerflood_lead_retry_seconds(fresh)
                queue_svc.defer_claim(
                    target_id,
                    seconds=retry_seconds,
                    reason=f"peerflood_entity_account:{account_id}",
                )
                logger.warning(
                    "Lead deferred after PeerFlood; no cross-account probe "
                    "target={} account={} retry_sec={}",
                    target_id,
                    account_id,
                    retry_seconds,
                )
                return False
            except PeerIdInvalidError:
                queue_svc.record_account_failure(
                    target_id,
                    account_id,
                    "no_entity",
                    "peer_id_invalid_during_entity_resolution",
                )
                queue_svc.release_claim(target_id, as_pending=True)
                continue
            except (UserPrivacyRestrictedError, UserIsBlockedError, ChatWriteForbiddenError):
                queue_svc.cancel_lead(target_id, "privacy_or_blocked")
                return True
            except (InputUserDeactivatedError, UserBannedInChannelError, ValueError):
                queue_svc.cancel_lead(target_id, "invalid_target")
                return True
            except Exception as exc:
                if _is_paid_message_required(exc):
                    queue_svc.mark_terminal_failure(
                        target_id,
                        "paid_message_required",
                        "paid_message_required",
                    )
                    logger.info(
                        "Paid message required target={} account={} phase=entity",
                        target_id,
                        account_id,
                    )
                    return True
                if account_auth.is_auth_loss_error(exc):
                    queue_svc.release_claim(target_id, as_pending=True)
                    await account_auth.register_auth_loss(account_id, exc, notify=True)
                    await monitor_svc.disconnect_account(account_id, cancel_tasks=True)
                    had_account_cooldown = True
                    continue
                detail = f"{_safe_error_name(exc)} during entity resolution"
                queue_svc.set_last_error(target_id, detail)
                queue_svc.release_claim(target_id, as_pending=True)
                logger.error(
                    "Retryable entity resolution error target={} account={} error_type={}",
                    target_id,
                    account_id,
                    _safe_error_name(exc),
                )
                had_retryable_error = True
                continue

            if entity is None:
                queue_svc.record_account_failure(
                    target_id,
                    account_id,
                    "no_entity",
                    "Telegram entity unavailable",
                )
                queue_svc.release_claim(target_id, as_pending=True)
                continue

            if generated_text is None:
                try:
                    # Keep generation and durable prepare inside one process-local
                    # critical section. Otherwise two concurrent dispatch calls could
                    # read the same last-20 window and choose the same fresh wording.
                    async with phrases_svc.generation_lock(phrases_svc.KIND_FIRST_DM):
                        generated_text = await generate_first_dm()
                        if enforce_global_pause and not runtime.is_worker_enabled():
                            queue_svc.release_claim(target_id, as_pending=True)
                            logger.info(
                                "Generated First DM discarded because global pause "
                                "became active target={}",
                                target_id,
                            )
                            return False
                        result = await _send_first_dm(
                            client,
                            account_id,
                            lead,
                            generated_text,
                            entity=entity,
                        )
                except Exception as exc:
                    detail = f"{_safe_error_name(exc)} during First DM generation"
                    queue_svc.set_last_error(target_id, detail)
                    queue_svc.release_claim(target_id, as_pending=True)
                    logger.error(
                        "First DM generation failed target={} error_type={}",
                        target_id,
                        _safe_error_name(exc),
                    )
                    had_retryable_error = True
                    break
            else:
                if enforce_global_pause and not runtime.is_worker_enabled():
                    queue_svc.release_claim(target_id, as_pending=True)
                    return False
                result = await _send_first_dm(
                    client,
                    account_id,
                    lead,
                    generated_text,
                    entity=entity,
                )
        else:
            if generated_text is None:
                continue
            if enforce_global_pause and not runtime.is_worker_enabled():
                queue_svc.release_claim(target_id, as_pending=True)
                return False
            result = await _send_first_dm(
                client,
                account_id,
                lead,
                generated_text,
                entity=entity,
            )
        if result == "sent":
            _remember_text(generated_text)
            pacing.mark_global_sent()
            return True
        if result == "peerflood":
            retry_seconds = _peerflood_lead_retry_seconds(fresh)
            queue_svc.defer_claim(
                target_id,
                seconds=retry_seconds,
                reason=f"peerflood_send_account:{account_id}",
            )
            logger.warning(
                "Lead deferred after PeerFlood send; no cross-account probe "
                "target={} account={} retry_sec={}",
                target_id,
                account_id,
                retry_seconds,
            )
            return False
        if result in {"flood", "auth_lost"}:
            had_account_cooldown = True
            continue
        if result == "error":
            had_retryable_error = True
            continue
        if result in {"privacy", "invalid", "terminal", "paid_message_required"}:
            return True
        if result in {"no_entity", "peer_invalid"}:
            detail = (
                "PeerIdInvalid during SendMessageRequest"
                if result == "peer_invalid"
                else "Telegram entity unavailable"
            )
            queue_svc.record_account_failure(
                target_id, account_id, "no_entity", detail
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
                if last_error
                else f"retryable rounds exhausted:{attempts}"
            )
            queue_svc.mark_terminal_failure(
                target_id, "max_transient_attempts", detail
            )
            logger.error(
                "Lead terminal after retryable rounds target={} attempts={}",
                target_id,
                attempts,
            )
            return True
        queue_svc.defer_claim(
            target_id,
            seconds=60,
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
    if not runtime.is_worker_enabled():
        return False
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

    if not runtime.is_worker_enabled():
        queue_svc.release_claim(target_id, as_pending=True)
        logger.info(
            "First DM claim released because global pause became active target={}",
            target_id,
        )
        return False

    if opt_out_svc.is_opted_out(target_id):
        queue_svc.cancel_lead(target_id, "opt_out")
        return True

    ordered = _untried_ready_accounts(lead, ready)
    if not ordered:
        return _finish_or_defer_unresolvable(target_id, lead)

    # Entity resolution is performed inside the account round. AI is called only
    # after at least one ready account has a real Telegram entity.
    return await _attempt_lead_across_accounts(
        lead, ordered, enforce_global_pause=True
    )


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
            "💬 **Отправленный First DM:**",
            text,
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


_USERNAME_LOOKUP_MISS_ERRORS = {
    "UsernameInvalidError",
    "UsernameNotOccupiedError",
}


def _entity_user_id(entity: Any) -> int | None:
    """Return a Telegram user id from common input/entity objects when available."""
    for attr in ("user_id", "id"):
        value = getattr(entity, attr, None)
        if value is None:
            continue
        try:
            return int(value)
        except (TypeError, ValueError):
            continue
    return None


def _is_entity_lookup_miss(exc: BaseException) -> bool:
    """Classify exact lookup misses while preserving real Telegram failures."""
    return isinstance(exc, (ValueError, TypeError, LookupError, PeerIdInvalidError)) or (
        type(exc).__name__ in _USERNAME_LOOKUP_MISS_ERRORS
    )


async def _resolve_target_entity(client, account_id: int, lead: dict[str, Any]):
    """Resolve by username first, then account cache, then source access hash.

    A username result is accepted only when Telegram returns the same numeric user id.
    This prevents a changed or recycled username from routing a First DM to another user.
    """
    target_id = int(lead["target_user_id"])
    username = (lead.get("username") or "").strip().lstrip("@")
    access_hash = lead.get("access_hash")
    source_account_id = lead.get("source_account_user_id")

    if username:
        try:
            entity = await client.get_input_entity(username)
        except Exception as exc:
            if not _is_entity_lookup_miss(exc):
                raise
            logger.debug(
                "Entity by username unavailable target={} account={} error_type={}",
                target_id,
                account_id,
                type(exc).__name__,
            )
        else:
            resolved_user_id = _entity_user_id(entity)
            if resolved_user_id == target_id:
                return entity
            logger.warning(
                "Username entity mismatch target={} resolved_target={} account={}",
                target_id,
                resolved_user_id,
                account_id,
            )

    try:
        return await client.get_input_entity(target_id)
    except Exception as exc:
        if not _is_entity_lookup_miss(exc):
            raise
        logger.debug(
            "Entity by id unavailable target={} account={} error_type={}",
            target_id,
            account_id,
            type(exc).__name__,
        )

    if (
        access_hash is not None
        and source_account_id is not None
        and int(source_account_id) == int(account_id)
    ):
        return InputPeerUser(target_id, int(access_hash))

    return None


def _normalize_message_text(value: str | None) -> str:
    return (value or "").replace("\r\n", "\n").strip()


def _defer_or_release_unresolvable_recovery(
    row: dict[str, Any],
    *,
    reason: str,
) -> bool:
    """Return True when a legacy prepared row was released from recovery."""
    target_id = int(row["target_user_id"])
    account_id = int(row["account_user_id"])
    attempts = first_dm_delivery.defer_recovery(
        target_id,
        reason,
        delay_seconds=_UNRESOLVABLE_RECOVERY_RETRY_SECONDS,
    )
    if attempts < _UNRESOLVABLE_RECOVERY_MAX_ATTEMPTS:
        logger.warning(
            "Prepared First DM recovery deferred account={} target={} reason={} "
            "attempt={}/{} retry_sec={}",
            account_id,
            target_id,
            reason,
            attempts,
            _UNRESOLVABLE_RECOVERY_MAX_ATTEMPTS,
            _UNRESOLVABLE_RECOVERY_RETRY_SECONDS,
        )
        return False

    queue_svc.record_account_failure(
        target_id,
        account_id,
        "no_entity",
        f"legacy_recovery_exhausted:{reason}",
    )
    first_dm_delivery.rollback(
        target_id,
        f"legacy_recovery_exhausted:{reason}",
        as_pending=True,
    )
    logger.error(
        "Prepared First DM recovery exhausted; returned to shared queue "
        "account={} target={} reason={} attempts={}",
        account_id,
        target_id,
        reason,
        attempts,
    )
    return True


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
            attempts = first_dm_delivery.defer_recovery(
                target_id,
                "client_unavailable",
                delay_seconds=300,
            )
            logger.warning(
                "Prepared First DM recovery deferred account={} target={} reason=no_client "
                "attempt={} retry_sec=300",
                account_id,
                target_id,
                attempts,
            )
            continue

        try:
            entity = await _resolve_target_entity(client, account_id, row)
        except PeerIdInvalidError:
            if _defer_or_release_unresolvable_recovery(
                row, reason="peer_id_invalid_entity_resolution"
            ):
                recovered += 1
            continue
        if entity is None:
            if _defer_or_release_unresolvable_recovery(
                row, reason="entity_unavailable"
            ):
                recovered += 1
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
            if account_auth.is_auth_loss_error(exc):
                await account_auth.register_auth_loss(account_id, exc, notify=True)
                await monitor_svc.disconnect_account(account_id, cancel_tasks=True)
                attempts = first_dm_delivery.defer_recovery(
                    target_id,
                    f"authorization_lost: {exc}",
                    delay_seconds=21600,
                )
                logger.warning(
                    "Prepared First DM recovery paused for authorization account={} "
                    "target={} attempt={} retry_sec=21600",
                    account_id,
                    target_id,
                    attempts,
                )
                continue
            peer_invalid = isinstance(exc, PeerIdInvalidError) or (
                type(exc).__name__ == "PeerIdInvalidError"
            )
            if peer_invalid:
                if _defer_or_release_unresolvable_recovery(
                    row, reason="peer_id_invalid_history"
                ):
                    recovered += 1
                continue
            reason = type(exc).__name__
            attempts = first_dm_delivery.defer_recovery(
                target_id,
                reason,
                delay_seconds=900,
            )
            logger.warning(
                "Prepared First DM recovery deferred account={} target={} reason={} "
                "attempt={} retry_sec=900",
                account_id,
                target_id,
                reason,
                attempts,
            )
            logger.opt(exception=exc).debug(
                "Prepared First DM recovery diagnostic account={} target={}",
                account_id,
                target_id,
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
    *,
    entity=None,
) -> str:
    target_id = int(lead["target_user_id"])
    prepared = False

    try:
        if entity is None:
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

    except PeerIdInvalidError:
        logger.info(
            "PeerIdInvalid during First DM send account={} target={}",
            account_id,
            target_id,
        )
        if prepared:
            first_dm_delivery.rollback(
                target_id,
                "peer_id_invalid_send",
                as_pending=True,
            )
        else:
            queue_svc.release_claim(target_id, as_pending=True)
        return "peer_invalid"

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
        if _is_paid_message_required(exc):
            logger.info(
                "Paid message required target={} account={} phase=send",
                target_id,
                account_id,
            )
            if prepared:
                first_dm_delivery.rollback(
                    target_id,
                    "paid_message_required",
                    as_pending=False,
                )
            else:
                queue_svc.release_claim(target_id, as_pending=False)
            queue_svc.mark_terminal_failure(
                target_id,
                "paid_message_required",
                "paid_message_required",
            )
            return "paid_message_required"
        if account_auth.is_auth_loss_error(exc):
            logger.warning(
                "First DM stopped because account authorization was lost "
                "account={} target={} error={}",
                account_id,
                target_id,
                type(exc).__name__,
            )
            if prepared:
                first_dm_delivery.rollback(
                    target_id, "authorization_lost", as_pending=True
                )
            else:
                queue_svc.release_claim(target_id, as_pending=True)
            await account_auth.register_auth_loss(account_id, exc, notify=True)
            await monitor_svc.disconnect_account(account_id, cancel_tasks=True)
            return "auth_lost"
        logger.opt(exception=exc).error(
            "First DM send became ambiguous account={} target={} error_type={}",
            account_id,
            target_id,
            _safe_error_name(exc),
        )
        if prepared:
            # Do not retry through another account: Telegram may have accepted it.
            return "ambiguous"
        detail = f"{_safe_error_name(exc)}: pre-send failure"
        queue_svc.set_last_error(target_id, detail)
        queue_svc.release_claim(target_id, as_pending=True)
        logger.warning(
            "Retryable pre-send error target={} account={} error={}",
            target_id, account_id, detail
        )
        return "error"
