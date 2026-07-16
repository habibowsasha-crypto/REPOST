"""DM Autoposter: watched-chat triggers and safe persistent first-DM dispatch."""

from __future__ import annotations

import asyncio
import datetime
from types import SimpleNamespace
from typing import Optional

from loguru import logger
from telethon import TelegramClient, events
from telethon.errors import (
    FloodWaitError,
    InputUserDeactivatedError,
    PeerFloodError,
    UserIsBlockedError,
    UserPrivacyRestrictedError,
)
from telethon.sessions import StringSession
from telethon.tl.custom import Button
from telethon.tl.types import (
    InputPeerChannel,
    InputPeerChat,
    InputPeerUser,
    PeerChannel,
    PeerChat,
    PeerUser,
    User,
)

from config import (
    ADMIN_ID_LIST,
    API_HASH,
    API_ID,
    MEDIA_DIR,
    New_Message,
    Query,
    bot,
    callback_message,
    callback_query,
    conn,
)
from services.account_profiles import (
    format_account_label,
    refresh_stale_account_profiles,
    save_account_profile,
)
from services.admin_state import clear_admin_interaction_state, is_command_event
from services.ai_dialog_service import handle_private_incoming, record_first_dm
from services.dm_contact_analytics import (
    is_completed_contact,
    is_contact_in_progress,
    record_first_dm as record_contact_first_dm,
    record_source_seen,
    register_completed_queue_purger,
    release_first_dm_claim,
    try_claim_first_dm,
)
from services.dm_opt_out import is_opted_out, register_queue_purger
from services.dm_task_cleanup import (
    count_active_dm_tasks,
    count_inactive_dm_tasks,
    delete_inactive_dm_tasks,
)
from services.dm_task_queue import (
    account_gate_wait_seconds,
    cancel_row,
    cancel_target_globally,
    count_all_pending,
    count_pending,
    claim_pending,
    clear_task_pending,
    earliest_due_at,
    enqueue_pending,
    finalize_sent,
    get_account_dispatch_state,
    get_global_first_dm_state,
    get_due_pending,
    mark_account_send_completed,
    mark_sending,
    mark_uncertain,
    is_global_first_dm_paused,
    MAX_DELAY_SECONDS,
    pause_account,
    pause_all_first_dms,
    prepare_tasks_for_deletion,
    reassign_task_pending_to_active_sources,
    recover_stale_queue,
    release_pending_claim,
    resume_all_first_dms,
    schedule_retry,
    set_account_cooldown,
)
from services.first_message import choose_first_dm_text, is_random_first_dm_enabled
from services.menu_ui import render_menu
from utils.database.database import create_dm_tables
from utils.telegram import gid_key

# Admin setup state.
dm_setup_state: dict = {}

# One monitor/client per task remains in stage B. Stage C will consolidate them
# into one account-level Telethon runtime without changing this queue contract.
dm_monitor_clients: dict[int, TelegramClient] = {}
dm_monitor_tasks: dict[int, asyncio.Task] = {}
dm_account_dispatcher_tasks: dict[int, asyncio.Task] = {}

# Compatibility-only alias for old imports. The live first-DM queue is now SQLite.
dm_send_queues: dict[int, tuple] = {}
dm_source_chat_titles: dict[tuple[int, int], str] = {}
dm_source_chat_ids: dict[tuple[int, int], int] = {}

_task_operation_locks: dict[int, tuple[asyncio.AbstractEventLoop, asyncio.Lock]] = {}


def get_dm_task_operation_lock(task_id: int) -> asyncio.Lock:
    """Return a task lock bound to the current event loop.

    Railway uses one loop, while tests and controlled restarts may create a new
    loop in the same process. Replacing a stale loop-bound lock avoids a false
    RuntimeError without weakening synchronization inside the active loop.
    """
    task_id = int(task_id)
    loop = asyncio.get_running_loop()
    current = _task_operation_locks.get(task_id)
    if current is None or current[0] is not loop:
        lock = asyncio.Lock()
        _task_operation_locks[task_id] = (loop, lock)
        return lock
    return current[1]


def _now_iso() -> str:
    return datetime.datetime.now(datetime.timezone.utc).isoformat()


def _minutes_since(iso_ts: str) -> float:
    try:
        parsed = datetime.datetime.fromisoformat(iso_ts)
    except (TypeError, ValueError):
        return float("inf")
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=datetime.timezone.utc)
    return max(
        0.0,
        (
            datetime.datetime.now(datetime.timezone.utc)
            - parsed.astimezone(datetime.timezone.utc)
        ).total_seconds()
        / 60,
    )


def _is_blacklisted(task_id: int, target_user_id: int) -> bool:
    row = conn.execute(
        """
        SELECT sent_at FROM dm_sent_log
         WHERE dm_task_id=? AND target_user_id=? AND status='privacy'
         ORDER BY sent_at DESC LIMIT 1
        """,
        (int(task_id), int(target_user_id)),
    ).fetchone()
    return bool(row and _minutes_since(row[0]) < 24 * 60)


def _log_event(task_id: int, target_user_id: int, status: str) -> None:
    with conn:
        conn.execute(
            """
            INSERT INTO dm_sent_log(dm_task_id, target_user_id, sent_at, status)
            VALUES (?, ?, ?, ?)
            """,
            (int(task_id), int(target_user_id), _now_iso(), str(status)),
        )


def _safe_log_event(task_id: int, target_user_id: int, status: str) -> None:
    try:
        _log_event(task_id, target_user_id, status)
    except Exception as exc:
        logger.error(
            f"[DM {task_id}] failed to persist status={status} user={target_user_id}: {exc}"
        )


def _get_task(task_id: int) -> Optional[dict]:
    row = conn.execute(
        """
        SELECT t.id, t.admin_id, t.user_id,
               COALESCE(s.session_string, t.session_string),
               t.post_text, t.photo_url, t.interval_minutes, t.is_active,
               t.delay_min, t.delay_max
          FROM dm_tasks AS t
          LEFT JOIN sessions AS s ON s.user_id=t.user_id
         WHERE t.id=?
        """,
        (int(task_id),),
    ).fetchone()
    if not row:
        return None
    keys = (
        "id",
        "admin_id",
        "user_id",
        "session_string",
        "post_text",
        "photo_url",
        "interval_minutes",
        "is_active",
        "delay_min",
        "delay_max",
    )
    return dict(zip(keys, row))


def _get_watched_chats(task_id: int) -> list[int]:
    rows = conn.execute(
        "SELECT chat_id FROM dm_watched_chats WHERE dm_task_id=? ORDER BY id",
        (int(task_id),),
    ).fetchall()
    return [int(row[0]) for row in rows]


async def _resolve_watched_chats(
    client: TelegramClient, account_user_id: int, chat_ids: list[int]
) -> list:
    if not chat_ids:
        return []
    placeholders = ",".join("?" for _ in chat_ids)
    discovered_rows = conn.execute(
        f"""
        SELECT group_id, access_hash, peer_type, is_available
          FROM discovered_groups
         WHERE user_id=? AND group_id IN ({placeholders})
        """,
        (int(account_user_id), *chat_ids),
    ).fetchall()
    legacy_rows = conn.execute(
        f"""
        SELECT group_id, group_username FROM groups
         WHERE user_id=? AND group_id IN ({placeholders})
        """,
        (int(account_user_id), *chat_ids),
    ).fetchall()
    discovered = {int(row[0]): row[1:] for row in discovered_rows}
    identifiers = {int(row[0]): row[1] for row in legacy_rows}
    resolved: list = []
    seen: set[int] = set()
    for raw_group_id in chat_ids:
        group_id = gid_key(raw_group_id)
        row = discovered.get(group_id)
        peer = None
        if row is not None:
            access_hash, peer_type, available = row
            if not available:
                continue
            if peer_type == "channel" and access_hash is not None:
                peer = InputPeerChannel(group_id, int(access_hash))
            elif peer_type == "chat":
                peer = InputPeerChat(group_id)
        if peer is None:
            candidates = []
            if identifiers.get(group_id):
                candidates.append(identifiers[group_id])
            candidates.extend((PeerChannel(group_id), PeerChat(group_id)))
            for candidate in candidates:
                try:
                    peer = await client.get_input_entity(candidate)
                    break
                except Exception:
                    continue
        if peer is None:
            logger.warning(f"[DM] cannot restore watched peer chat={group_id}")
            continue
        key = int(getattr(peer, "channel_id", getattr(peer, "chat_id", group_id)))
        if key in seen:
            continue
        seen.add(key)
        resolved.append(peer)
    return resolved


def _purge_opted_out_user(target_user_id: int) -> int:
    target_user_id = int(target_user_id)
    removed = cancel_target_globally(target_user_id, "global_opt_out")
    for task_id, queue in list(dm_send_queues.items()):
        try:
            kept = [item for item in queue if int(item[0]) != target_user_id]
            removed += len(queue) - len(kept)
            queue.clear()
            queue.extend(kept)
        except (AttributeError, TypeError):
            continue
    for key in [key for key in dm_source_chat_titles if key[1] == target_user_id]:
        dm_source_chat_titles.pop(key, None)
    for key in [key for key in dm_source_chat_ids if key[1] == target_user_id]:
        dm_source_chat_ids.pop(key, None)
    return removed


def _purge_completed_user(target_user_id: int) -> int:
    """Remove a globally completed user from every account and task queue."""
    target_user_id = int(target_user_id)
    removed = cancel_target_globally(target_user_id, "global_completed_contact")
    for task_id, queue in list(dm_send_queues.items()):
        try:
            kept = [item for item in queue if int(item[0]) != target_user_id]
            removed += len(queue) - len(kept)
            queue.clear()
            queue.extend(kept)
        except (AttributeError, TypeError):
            continue
        dm_source_chat_titles.pop((int(task_id), target_user_id), None)
        dm_source_chat_ids.pop((int(task_id), target_user_id), None)
    return removed


register_queue_purger(_purge_opted_out_user)
register_completed_queue_purger(_purge_completed_user)


async def _resolve_pending_target(client: TelegramClient, row: dict):
    target_id = int(row["target_user_id"])
    access_hash = row.get("target_access_hash")
    if access_hash is not None:
        return InputPeerUser(target_id, int(access_hash))
    username = (row.get("target_username") or "").strip().lstrip("@")
    candidates = [f"@{username}"] if username else []
    candidates.append(PeerUser(target_id))
    for candidate in candidates:
        try:
            return await client.get_input_entity(candidate)
        except (FloodWaitError, PeerFloodError):
            raise
        except Exception:
            continue
    raise ValueError(f"cannot resolve Telegram peer for user={target_id}")


def _target_snapshot(row: dict) -> SimpleNamespace:
    return SimpleNamespace(
        id=int(row["target_user_id"]),
        username=row.get("target_username"),
        first_name=row.get("target_first_name"),
        last_name=row.get("target_last_name"),
        access_hash=row.get("target_access_hash"),
    )


def _unresolved_backoff(attempts: int) -> int:
    schedule = (60, 300, 900, 3600)
    return schedule[min(max(int(attempts), 0), len(schedule) - 1)]


async def _send_pending_row(row: dict) -> str:
    row_id = int(row["id"])
    task_id = int(row["dm_task_id"])
    account_user_id = int(row["account_user_id"])
    target_id = int(row["target_user_id"])
    task_lock = get_dm_task_operation_lock(task_id)

    if is_global_first_dm_paused():
        return "global_paused"

    async with task_lock:
        task = _get_task(task_id)
        if not task or not task["is_active"]:
            # A stopped task keeps its queue. get_due_pending() will ignore it
            # until the task is explicitly started again.
            return "task_inactive"

        queue_claim = claim_pending(row_id)
        if not queue_claim:
            return "lost_race"
        if is_global_first_dm_paused():
            release_pending_claim(row_id, queue_claim, "global_pause_after_claim")
            return "global_paused"

        client = dm_monitor_clients.get(task_id)
        if client is None or not client.is_connected():
            schedule_retry(
                row_id,
                seconds=30,
                error="task_client_not_ready",
                status="retry_wait",
                claim_token=queue_claim,
            )
            return "retry"

        if is_opted_out(target_id):
            cancel_row(row_id, "global_opt_out", claim_token=queue_claim)
            return "cancelled"
        if is_completed_contact(account_user_id, target_id):
            cancel_row(row_id, "completed_contact", claim_token=queue_claim)
            return "cancelled"
        if is_contact_in_progress(account_user_id, target_id):
            cancel_row(row_id, "dialog_in_progress", claim_token=queue_claim)
            return "cancelled"
        if _is_blacklisted(task_id, target_id):
            cancel_row(row_id, "privacy_blacklist", claim_token=queue_claim)
            return "cancelled"

        first_claim = try_claim_first_dm(
            account_user_id=account_user_id,
            target_user_id=target_id,
            dm_task_id=task_id,
            source_chat_id=row.get("source_chat_id"),
        )
        if not first_claim:
            cancel_row(row_id, "first_dm_claim_unavailable", claim_token=queue_claim)
            return "cancelled"

        try:
            target_peer = await _resolve_pending_target(client, row)
        except FloodWaitError as exc:
            wait_seconds = max(1, int(exc.seconds))
            release_first_dm_claim(account_user_id, target_id, first_claim)
            schedule_retry(
                row_id,
                seconds=wait_seconds,
                error=f"peer_resolve_flood_wait_{wait_seconds}",
                status="retry_wait",
                claim_token=queue_claim,
            )
            set_account_cooldown(account_user_id, wait_seconds, "FloodWait")
            logger.warning(
                f"[DM account {account_user_id}] FloodWait {wait_seconds}s while resolving peer"
            )
            return "flood_wait"
        except PeerFloodError:
            release_first_dm_claim(account_user_id, target_id, first_claim)
            schedule_retry(
                row_id,
                seconds=60,
                error="peer_resolve_peer_flood_manual_resume_required",
                status="retry_wait",
                claim_token=queue_claim,
            )
            pause_account(account_user_id, "PeerFlood: ручное возобновление")
            logger.error(
                f"[DM account {account_user_id}] PeerFlood while resolving peer; paused"
            )
            return "peer_flood"
        except Exception as exc:
            delay = _unresolved_backoff(int(row.get("resolve_attempts") or 0))
            release_first_dm_claim(account_user_id, target_id, first_claim)
            schedule_retry(
                row_id,
                seconds=delay,
                error=f"peer_resolve_failed: {exc}",
                status="unresolved_peer",
                claim_token=queue_claim,
            )
            logger.warning(
                f"[DM {task_id}] unresolved peer user={target_id}; retry in {delay}s"
            )
            return "unresolved"

        outgoing_text = (
            choose_first_dm_text(task["post_text"] or "")
            or (task["post_text"] or "Привет 👋")
        )
        if is_global_first_dm_paused():
            release_first_dm_claim(account_user_id, target_id, first_claim)
            release_pending_claim(row_id, queue_claim, "global_pause_before_sending")
            return "global_paused"
        if not mark_sending(row_id, queue_claim):
            release_first_dm_claim(account_user_id, target_id, first_claim)
            return "lost_race"

        if is_global_first_dm_paused():
            release_first_dm_claim(account_user_id, target_id, first_claim)
            release_pending_claim(row_id, queue_claim, "global_pause_final_guard")
            return "global_paused"

        # Final global guard immediately before the Telegram request. Another
        # account may have completed the dialog while this peer was resolving.
        if is_completed_contact(account_user_id, target_id):
            release_first_dm_claim(account_user_id, target_id, first_claim)
            cancel_row(row_id, "global_completed_contact", claim_token=queue_claim)
            return "cancelled"

        partial_delivery = False
        try:
            if task["photo_url"]:
                if outgoing_text and len(outgoing_text) <= 1024:
                    await client.send_file(
                        target_peer, task["photo_url"], caption=outgoing_text
                    )
                else:
                    await client.send_file(target_peer, task["photo_url"])
                    partial_delivery = True
                    if outgoing_text:
                        await client.send_message(target_peer, outgoing_text)
            else:
                await client.send_message(target_peer, outgoing_text)
        except UserPrivacyRestrictedError:
            if partial_delivery:
                mark_uncertain(row_id, "photo_delivered_text_privacy_error")
                mark_account_send_completed(account_user_id)
                _safe_log_event(task_id, target_id, "uncertain")
                return "uncertain"
            release_first_dm_claim(account_user_id, target_id, first_claim)
            _safe_log_event(task_id, target_id, "privacy")
            cancel_row(row_id, "privacy_restricted", claim_token=queue_claim)
            return "known_failure"
        except (UserIsBlockedError, InputUserDeactivatedError):
            if partial_delivery:
                mark_uncertain(row_id, "photo_delivered_text_blocked_error")
                mark_account_send_completed(account_user_id)
                _safe_log_event(task_id, target_id, "uncertain")
                return "uncertain"
            release_first_dm_claim(account_user_id, target_id, first_claim)
            _safe_log_event(task_id, target_id, "blocked")
            cancel_row(row_id, "blocked_or_deactivated", claim_token=queue_claim)
            return "known_failure"
        except FloodWaitError as exc:
            wait_seconds = max(1, int(exc.seconds))
            set_account_cooldown(account_user_id, wait_seconds, "FloodWait")
            if partial_delivery:
                mark_uncertain(row_id, f"photo_delivered_then_flood_wait_{wait_seconds}")
                mark_account_send_completed(account_user_id)
                _safe_log_event(task_id, target_id, "uncertain")
                logger.warning(
                    f"[DM account {account_user_id}] partial first DM then FloodWait {wait_seconds}s"
                )
                return "uncertain"
            release_first_dm_claim(account_user_id, target_id, first_claim)
            schedule_retry(
                row_id,
                seconds=wait_seconds,
                error=f"flood_wait_{wait_seconds}",
                status="retry_wait",
                claim_token=queue_claim,
            )
            logger.warning(
                f"[DM account {account_user_id}] FloodWait {wait_seconds}s; all first DMs paused"
            )
            return "flood_wait"
        except PeerFloodError:
            pause_account(account_user_id, "PeerFlood: ручное возобновление")
            if partial_delivery:
                mark_uncertain(row_id, "photo_delivered_then_peer_flood")
                mark_account_send_completed(account_user_id)
                _safe_log_event(task_id, target_id, "uncertain")
                logger.error(
                    f"[DM account {account_user_id}] partial first DM then PeerFlood; paused"
                )
                return "uncertain"
            release_first_dm_claim(account_user_id, target_id, first_claim)
            schedule_retry(
                row_id,
                seconds=60,
                error="peer_flood_manual_resume_required",
                status="retry_wait",
                claim_token=queue_claim,
            )
            logger.error(
                f"[DM account {account_user_id}] PeerFlood; first DMs paused until admin resume"
            )
            return "peer_flood"
        except asyncio.CancelledError:
            mark_uncertain(row_id, "dispatcher_cancelled_during_send")
            # The request may already have reached Telegram. Preserve account pacing
            # across a graceful shutdown/restart as an additional safety margin.
            mark_account_send_completed(account_user_id)
            logger.error(
                f"[DM {task_id}] cancelled during Telegram send user={target_id}; marked uncertain"
            )
            raise
        except Exception as exc:
            # A network/transport exception can happen after Telegram accepted the
            # request. Do not blindly retry and risk a duplicate first DM.
            mark_uncertain(row_id, f"unknown_send_result: {type(exc).__name__}: {exc}")
            mark_account_send_completed(account_user_id)
            _safe_log_event(task_id, target_id, "error")
            logger.exception(
                f"[DM {task_id}] uncertain first-DM result user={target_id}: {exc}"
            )
            return "uncertain"

        try:
            queue_finalized = finalize_sent(row_id, queue_claim)
        except Exception as exc:
            queue_finalized = False
            logger.exception(
                f"[DM {task_id}] Telegram accepted first DM but queue finalization failed "
                f"user={target_id}: {exc}"
            )
        if not queue_finalized:
            mark_uncertain(row_id, "telegram_accepted_queue_finalize_failed")
        _safe_log_event(task_id, target_id, "sent")
        contact_cycle_id: Optional[int] = None
        try:
            contact_cycle_id = record_contact_first_dm(
                dm_task_id=task_id,
                account_user_id=account_user_id,
                target_user_id=target_id,
                source_chat_id=row.get("source_chat_id"),
                source_chat_title=row.get("source_chat_title"),
                claim_token=first_claim,
            )
        except Exception as exc:
            logger.exception(
                f"[DM {task_id}] delivered but contact cycle save failed user={target_id}: {exc}"
            )
        try:
            record_first_dm(
                dm_task_id=task_id,
                account_user_id=account_user_id,
                target=_target_snapshot(row),
                text=outgoing_text,
                source_chat_id=row.get("source_chat_id"),
                source_chat_title=row.get("source_chat_title"),
                contact_cycle_id=contact_cycle_id,
            )
        except Exception as exc:
            logger.exception(
                f"[DM {task_id}] delivered but AI dialog save failed user={target_id}: {exc}"
            )
        pacing = mark_account_send_completed(account_user_id)
        logger.info(
            f"[DM {task_id}] first DM delivered user={target_id}; account pacing={pacing}s"
        )
        return "sent"


def _account_has_active_tasks(account_user_id: int) -> bool:
    row = conn.execute(
        "SELECT 1 FROM dm_tasks WHERE user_id=? AND is_active=1 LIMIT 1",
        (int(account_user_id),),
    ).fetchone()
    return bool(row)


async def _account_dispatch_loop(account_user_id: int) -> None:
    account_user_id = int(account_user_id)
    logger.info(f"[DM dispatcher] account={account_user_id} started")
    try:
        while True:
            if is_global_first_dm_paused():
                await asyncio.sleep(2)
                continue
            state = get_account_dispatch_state(account_user_id)
            if state.is_paused:
                await asyncio.sleep(5)
                continue

            gate_wait = account_gate_wait_seconds(account_user_id)
            if gate_wait is None:
                await asyncio.sleep(5)
                continue
            if gate_wait > 0:
                await asyncio.sleep(min(max(gate_wait, 0.25), 30.0))
                continue

            row = get_due_pending(account_user_id)
            if row is None:
                earliest = earliest_due_at(account_user_id)
                if earliest is None:
                    if not _account_has_active_tasks(account_user_id):
                        logger.info(
                            f"[DM dispatcher] account={account_user_id} has no active tasks; exiting"
                        )
                        return
                    await asyncio.sleep(2)
                else:
                    wait = max(
                        0.25,
                        (
                            earliest
                            - datetime.datetime.now(datetime.timezone.utc)
                        ).total_seconds(),
                    )
                    await asyncio.sleep(min(wait, 30.0))
                continue

            await _send_pending_row(row)
            await asyncio.sleep(0.25)
    except asyncio.CancelledError:
        logger.info(f"[DM dispatcher] account={account_user_id} stopped")
        raise
    except Exception as exc:
        logger.exception(
            f"[DM dispatcher] account={account_user_id} crashed: {exc}"
        )
    finally:
        current = dm_account_dispatcher_tasks.get(account_user_id)
        if current is asyncio.current_task():
            dm_account_dispatcher_tasks.pop(account_user_id, None)


def ensure_account_dispatcher(account_user_id: int) -> None:
    account_user_id = int(account_user_id)
    existing = dm_account_dispatcher_tasks.get(account_user_id)
    if existing and not existing.done():
        return
    dm_account_dispatcher_tasks[account_user_id] = bot.loop.create_task(
        _account_dispatch_loop(account_user_id),
        name=f"dm-account-dispatch-{account_user_id}",
    )


def _detect_private_message_kind(event) -> str | None:
    """Return a coarse media kind for a private message without reading it.

    Maxim does not inspect media contents. The value only prevents an empty
    caption from silently ending the AI flow and lets the service treat the
    message as a neutral reaction.
    """
    message = getattr(event, "message", None)
    if message is None or getattr(message, "media", None) is None:
        return None

    checks = (
        ("sticker", "sticker"),
        ("gif", "gif"),
        ("photo", "photo"),
        ("voice", "voice"),
        ("video", "video"),
        ("audio", "audio"),
        ("poll", "poll"),
        ("contact", "contact"),
        ("geo", "location"),
        ("venue", "location"),
        ("document", "document"),
    )
    for attribute, kind in checks:
        try:
            if getattr(message, attribute, None):
                return kind
        except Exception:
            continue
    return "media"


async def _monitor_loop(task_id: int) -> None:
    task = _get_task(task_id)
    if not task or not task["is_active"] or not task.get("session_string"):
        return
    client = TelegramClient(StringSession(task["session_string"]), API_ID, API_HASH)
    try:
        await client.connect()
        if not await client.is_user_authorized():
            logger.error(f"[DM task {task_id}] session is not authorized")
            return
        try:
            save_account_profile(await client.get_me())
        except Exception as exc:
            logger.warning(f"[DM task {task_id}] profile refresh failed: {exc}")

        watched_ids = _get_watched_chats(task_id)
        watched = await _resolve_watched_chats(client, task["user_id"], watched_ids)
        if not watched:
            logger.warning(f"[DM task {task_id}] no available watched chats")
            return

        dm_monitor_clients[task_id] = client
        ensure_account_dispatcher(task["user_id"])
        logger.info(f"[DM task {task_id}] monitoring {len(watched)} chats")

        @client.on(events.NewMessage(incoming=True))
        async def on_private_message(event):
            if not event.is_private:
                return
            try:
                sender = await event.get_sender()
            except Exception:
                return
            if not isinstance(sender, User) or sender.bot or sender.is_self:
                return
            await handle_private_incoming(
                dm_task_id=task_id,
                account_user_id=task["user_id"],
                client=client,
                sender=sender,
                text=event.raw_text or "",
                message_id=getattr(event, "id", None),
                media_kind=_detect_private_message_kind(event),
            )

        @client.on(events.NewMessage(chats=watched, incoming=True))
        async def on_chat_message(event):
            if not event.is_group and not event.is_channel:
                return
            try:
                sender = await event.get_sender()
            except Exception:
                return
            if not isinstance(sender, User) or sender.bot or sender.is_self:
                return

            current_task = _get_task(task_id)
            if not current_task or not current_task["is_active"]:
                return
            target_id = int(sender.id)
            source_chat_id = None
            source_chat_title = None
            try:
                source_chat = await event.get_chat()
                raw_chat_id = getattr(source_chat, "id", None)
                source_chat_id = int(raw_chat_id) if raw_chat_id is not None else None
                source_chat_title = getattr(source_chat, "title", None)
            except Exception as exc:
                logger.debug(
                    f"[DM {task_id}] source chat unavailable user={target_id}: {exc}"
                )

            if source_chat_id is not None:
                try:
                    record_source_seen(
                        account_user_id=current_task["user_id"],
                        target_user_id=target_id,
                        source_chat_id=source_chat_id,
                        source_chat_title=(
                            str(source_chat_title) if source_chat_title else None
                        ),
                    )
                except Exception as exc:
                    logger.error(
                        f"[DM {task_id}] source analytics failed user={target_id}: {exc}"
                    )

            if is_opted_out(target_id):
                return
            if is_completed_contact(current_task["user_id"], target_id):
                return
            if is_contact_in_progress(current_task["user_id"], target_id):
                return
            if _is_blacklisted(task_id, target_id):
                return

            created, pending_id = enqueue_pending(
                dm_task_id=task_id,
                account_user_id=current_task["user_id"],
                target_user_id=target_id,
                target_access_hash=getattr(sender, "access_hash", None),
                target_username=getattr(sender, "username", None),
                target_first_name=getattr(sender, "first_name", None),
                target_last_name=getattr(sender, "last_name", None),
                source_chat_id=source_chat_id,
                source_chat_title=(
                    str(source_chat_title) if source_chat_title else None
                ),
                delay_min=int(current_task.get("delay_min") or 0),
                delay_max=int(current_task.get("delay_max") or 0),
            )
            ensure_account_dispatcher(current_task["user_id"])
            logger.debug(
                f"[DM {task_id}] queue user={target_id} pending={pending_id} created={created}"
            )

        while True:
            current_task = _get_task(task_id)
            if not current_task or not current_task["is_active"]:
                break
            if not client.is_connected():
                await client.connect()
            ensure_account_dispatcher(task["user_id"])
            await asyncio.sleep(15)
    except asyncio.CancelledError:
        logger.info(f"[DM task {task_id}] monitor cancelled")
        raise
    except Exception as exc:
        logger.exception(f"[DM task {task_id}] monitor crashed: {exc}")
    finally:
        dm_monitor_clients.pop(task_id, None)
        try:
            await client.disconnect()
        except Exception:
            pass
        current = dm_monitor_tasks.get(task_id)
        if current is asyncio.current_task():
            dm_monitor_tasks.pop(task_id, None)


def _launch_monitor(task_id: int) -> None:
    task_id = int(task_id)
    existing = dm_monitor_tasks.get(task_id)
    if existing and not existing.done():
        return
    dm_monitor_tasks[task_id] = bot.loop.create_task(
        _monitor_loop(task_id), name=f"dm-monitor-{task_id}"
    )


async def stop_dm_task_runtime(task_id: int, *, preserve_queue: bool = True) -> bool:
    task_id = int(task_id)
    lock = get_dm_task_operation_lock(task_id)
    async with lock:
        with conn:
            cursor = conn.execute(
                "UPDATE dm_tasks SET is_active=0 WHERE id=?", (task_id,)
            )
        if int(cursor.rowcount or 0) != 1:
            return False
        if preserve_queue:
            reassign_task_pending_to_active_sources(task_id)
        else:
            clear_task_pending(task_id, "task_stopped_and_queue_cleared")
        monitor = dm_monitor_tasks.get(task_id)
        if monitor and not monitor.done():
            monitor.cancel()
            await asyncio.gather(monitor, return_exceptions=True)
    return True


async def start_dm_task_runtime(task_id: int) -> bool:
    task_id = int(task_id)
    lock = get_dm_task_operation_lock(task_id)
    async with lock:
        task = _get_task(task_id)
        if not task or not task.get("session_string") or not _get_watched_chats(task_id):
            return False
        with conn:
            conn.execute("UPDATE dm_tasks SET is_active=1 WHERE id=?", (task_id,))
    _launch_monitor(task_id)
    return True


async def restart_dm_task_runtime(task_id: int) -> bool:
    task_id = int(task_id)
    lock = get_dm_task_operation_lock(task_id)
    async with lock:
        task = _get_task(task_id)
        if not task:
            return False
        monitor = dm_monitor_tasks.get(task_id)
        if monitor and not monitor.done():
            monitor.cancel()
            await asyncio.gather(monitor, return_exceptions=True)
        current = _get_task(task_id)
        if current and current["is_active"]:
            _launch_monitor(task_id)
    return True


async def delete_dm_task_runtime(task_id: int) -> bool:
    task_id = int(task_id)
    if not _get_task(task_id):
        return False
    await stop_dm_task_runtime(task_id, preserve_queue=True)
    lock = get_dm_task_operation_lock(task_id)
    async with lock:
        prepare_tasks_for_deletion([task_id])
        with conn:
            conn.execute("DELETE FROM dm_watched_chats WHERE dm_task_id=?", (task_id,))
            cursor = conn.execute("DELETE FROM dm_tasks WHERE id=?", (task_id,))
    return int(cursor.rowcount or 0) == 1


async def restore_dm_tasks() -> None:
    create_dm_tables()
    recovery = recover_stale_queue()
    if recovery["claimed_recovered"] or recovery["sending_uncertain"]:
        logger.warning(f"[DM restore] queue recovery: {recovery}")
    rows = conn.execute(
        "SELECT id, user_id FROM dm_tasks WHERE is_active=1 ORDER BY id"
    ).fetchall()
    logger.info(f"[DM restore] active tasks: {len(rows)}")
    for task_id, _account_user_id in rows:
        _launch_monitor(int(task_id))

# ══════════════════════════════════════════════════════════════════════════════
# UI настройки
# ══════════════════════════════════════════════════════════════════════════════

@bot.on(New_Message(pattern=r"^/dm_post(?:@\w+)?$"))
async def cmd_dm_post(event: callback_message) -> None:
    if event.sender_id not in ADMIN_ID_LIST:
        return
    cursor = conn.cursor()
    cursor.execute("SELECT user_id, session_string FROM sessions ORDER BY user_id")
    sessions = cursor.fetchall()
    cursor.close()
    if not sessions:
        await render_menu(event, "⚠ Нет добавленных аккаунтов. Сначала добавьте через /start.", buttons=[[Button.inline("🏠 Главное меню", b"menu_home")]])
        return

    active_clients: dict[int, TelegramClient] = {}
    for task_id, client in list(dm_monitor_clients.items()):
        task = _get_task(task_id)
        if not task or not client.is_connected():
            continue
        active_clients.setdefault(int(task["user_id"]), client)

    try:
        await refresh_stale_account_profiles(
            [(int(uid), str(session_string)) for uid, session_string in sessions],
            active_clients=active_clients,
        )
    except asyncio.CancelledError:
        raise
    except Exception as exc:
        # Telegram profile refresh is display-only. A temporary failure must not
        # prevent the administrator from opening the DM setup menu.
        logger.warning(f"Не удалось обновить подписи аккаунтов для DM-меню: {exc}")

    buttons = [
        [
            Button.inline(
                f"👤 {format_account_label(int(uid), include_id=True, max_length=42)}",
                f"dm_acc_{uid}".encode(),
            )
        ]
        for uid, _session_string in sessions
    ]
    buttons.append([Button.inline("🏠 Главное меню", b"menu_home")])
    await render_menu(event, "📩 **DM Автопостер**\n\nВыберите аккаунт:", buttons=buttons)


@bot.on(Query(data=lambda d: d.decode().startswith("dm_acc_")))
async def dm_pick_account(event: callback_query) -> None:
    if event.sender_id not in ADMIN_ID_LIST:
        return
    admin_id = event.sender_id
    await clear_admin_interaction_state(admin_id)
    user_id = int(event.data.decode().split("_")[2])
    cursor = conn.cursor()
    cursor.execute("SELECT session_string FROM sessions WHERE user_id = ?", (user_id,))
    row = cursor.fetchone()
    cursor.execute(
        """
        SELECT
            g.group_id,
            COALESCE(d.title, d.username, g.group_username, CAST(g.group_id AS TEXT))
        FROM groups AS g
        LEFT JOIN discovered_groups AS d
          ON d.user_id = g.user_id AND d.group_id = g.group_id
        WHERE g.user_id = ?
          AND COALESCE(d.is_available, 1) = 1
        ORDER BY lower(COALESCE(d.title, d.username, g.group_username, CAST(g.group_id AS TEXT)))
        """,
        (user_id,),
    )
    groups = cursor.fetchall()
    cursor.close()
    if not row:
        await render_menu(event, "⚠ Сессия не найдена.")
        return
    if not groups:
        await render_menu(event, "⚠ Нет доступных групп. Откройте аккаунт и нажмите «Найти группы аккаунта».", buttons=[[Button.inline("🔎 Найти группы", f"sync_groups_{user_id}".encode()), Button.inline("🏠 Меню", b"menu_home")]])
        return
    dm_setup_state[admin_id] = {
        "step": "pick_chats",
        "user_id": user_id,
        "session_string": row[0],
        "selected_chats": [],
        "all_groups": groups,
    }
    await render_menu(event, 
        "📋 **Выберите чаты для мониторинга:**",
        buttons=_build_chat_buttons(groups, []),
    )
    await event.answer()


def _build_chat_buttons(groups, selected):
    buttons = [
        [Button.inline(
            f"{'✅' if gid in selected else '☐'} {uname or str(gid)}",
            f"dm_tog_{gid}".encode(),
        )]
        for gid, uname in groups
    ]
    buttons.append([Button.inline("▶️ Готово", b"dm_chats_done")])
    return buttons


@bot.on(Query(data=lambda d: d.decode().startswith("dm_tog_")))
async def dm_toggle_chat(event: callback_query) -> None:
    if event.sender_id not in ADMIN_ID_LIST:
        return
    admin_id = event.sender_id
    st = dm_setup_state.get(admin_id)
    if not st or st["step"] != "pick_chats":
        await event.answer("Начните заново через /dm_post")
        return
    chat_id = int(event.data.decode().split("_")[2])
    sel = st["selected_chats"]
    if chat_id in sel:
        sel.remove(chat_id)
    else:
        sel.append(chat_id)
    await event.edit("📋 **Выберите чаты для мониторинга:**",
                     buttons=_build_chat_buttons(st["all_groups"], sel))
    await event.answer()


@bot.on(Query(data=b"dm_chats_done"))
async def dm_chats_done(event: callback_query) -> None:
    if event.sender_id not in ADMIN_ID_LIST:
        return
    admin_id = event.sender_id
    st = dm_setup_state.get(admin_id)
    if not st or st["step"] != "pick_chats":
        await event.answer("Начните заново через /dm_post")
        return
    if not st["selected_chats"]:
        await event.answer("⚠ Выберите хотя бы один чат!", alert=True)
        return
    if is_random_first_dm_enabled():
        st["post_text"] = ""
        st["step"] = "delay_min"
        await render_menu(
            event,
            f"✅ Выбрано чатов: {len(st['selected_chats'])}\n\n"
            "🎲 Случайное первое сообщение включено. Ручной текст вводить не нужно.\n\n"
            "⏱ **Минимальная задержка после сообщения пользователя** (секунды):\n"
            "_(например: `30`)_",
        )
    else:
        st["step"] = "text"
        await render_menu(
            event,
            f"✅ Выбрано чатов: {len(st['selected_chats'])}\n\n"
            "📝 Введите текст первого сообщения для ЛС:",
        )
    await event.answer()


@bot.on(New_Message(func=lambda e: e.sender_id in dm_setup_state and
                    not is_command_event(e) and
                    dm_setup_state[e.sender_id].get("step") in
                    ("text", "delay_min", "delay_max", "photo")))
async def dm_dialog(event: callback_message) -> None:
    admin_id = event.sender_id
    st = dm_setup_state[admin_id]

    if st["step"] == "text":
        st["post_text"] = event.raw_text.strip()
        st["step"] = "delay_min"
        await event.respond(
            "⏱ **Минимальная задержка после сообщения пользователя** (секунды):\n"
            "_(например: `30`)_"
        )
        return

    if st["step"] == "delay_min":
        try:
            val = int(event.raw_text.strip())
            if val < 0 or val > MAX_DELAY_SECONDS:
                raise ValueError
        except ValueError:
            await event.respond("⚠ Введите число от 0 до 2 592 000 секунд (30 дней).")
            return
        st["delay_min"] = val
        st["step"] = "delay_max"
        await event.respond(
            "⏱ **Максимальная задержка после сообщения пользователя** (секунды):\n"
            "_(может быть равна минимальной, например: `90`)_"
        )
        return

    if st["step"] == "delay_max":
        try:
            val = int(event.raw_text.strip())
            if val < st["delay_min"] or val > MAX_DELAY_SECONDS:
                raise ValueError
        except ValueError:
            await event.respond(
                f"⚠ Значение должно быть от {st['delay_min']} до 2 592 000 секунд."
            )
            return
        st["delay_max"] = val
        st["step"] = "photo"
        buttons = [
            [Button.inline("📸 Прикрепить фото", b"dm_photo_yes")],
            [Button.inline("❌ Без фото", b"dm_photo_no")],
        ]
        await event.respond("Хотите прикрепить фото?", buttons=buttons)
        return

    if st["step"] == "photo":
        if event.photo:
            photo_path = await event.download_media(file=MEDIA_DIR)
            st["photo_url"] = photo_path
            await _save_and_launch(event, admin_id, st)
        else:
            await event.respond("⚠ Отправьте фото или нажмите «Без фото».")


@bot.on(Query(data=b"dm_photo_yes"))
async def dm_photo_yes(event: callback_query) -> None:
    if event.sender_id not in ADMIN_ID_LIST:
        return
    admin_id = event.sender_id
    st = dm_setup_state.get(admin_id)
    if not st:
        return
    st["step"] = "photo"
    await render_menu(event, "📸 Отправьте фото:")
    await event.answer()


@bot.on(Query(data=b"dm_photo_no"))
async def dm_photo_no(event: callback_query) -> None:
    if event.sender_id not in ADMIN_ID_LIST:
        return
    admin_id = event.sender_id
    st = dm_setup_state.get(admin_id)
    if not st:
        return
    st["photo_url"] = None
    await _save_and_launch(event, admin_id, st)
    await event.answer()


async def _save_and_launch(event, admin_id: int, st: dict) -> None:
    cursor = conn.cursor()
    cursor.execute(
        """INSERT INTO dm_tasks
           (admin_id, user_id, session_string, post_text, photo_url,
            interval_minutes, is_active, created_at, delay_min, delay_max)
           VALUES (?,?,?,?,?,?,1,?,?,?)""",
        (
            admin_id, st["user_id"], st["session_string"],
            st["post_text"], st.get("photo_url"),
            0, _now_iso(),
            st.get("delay_min", 30), st.get("delay_max", 90),
        ),
    )
    task_id = cursor.lastrowid
    for chat_id in st["selected_chats"]:
        cursor.execute(
            "INSERT OR IGNORE INTO dm_watched_chats (dm_task_id, chat_id) VALUES (?,?)",
            (task_id, chat_id),
        )
    conn.commit()
    cursor.close()
    dm_setup_state.pop(admin_id, None)
    _launch_monitor(task_id)

    await render_menu(event, 
        f"🚀 **DM-задача #{task_id} запущена!**\n\n"
        f"👥 Чатов: {len(st['selected_chats'])}\n"
        f"⏱ Задержка после сообщения: {st.get('delay_min', 30)}–{st.get('delay_max', 90)} сек\n"
        "🧭 Пауза между фактическими первыми DM настраивается отдельно для аккаунта.\n"
        f"📸 Фото: {'да' if st.get('photo_url') else 'нет'}\n\n"
        f"🔒 Закрытый ЛС → blacklist на 24ч\n\n"
        f"/dm_list — список | /dm_stop {task_id} — стоп"
    )


@bot.on(New_Message(pattern=r"^/dm_list(?:@\w+)?$"))
async def cmd_dm_list(event: callback_message) -> None:
    if event.sender_id not in ADMIN_ID_LIST:
        return
    rows = conn.execute(
        """
        SELECT id, user_id, is_active, created_at, delay_min, delay_max,
               (SELECT COUNT(*) FROM dm_watched_chats WHERE dm_task_id=dm_tasks.id),
               (SELECT COUNT(*) FROM dm_sent_log WHERE dm_task_id=dm_tasks.id AND status='sent'),
               (SELECT COUNT(*) FROM dm_sent_log WHERE dm_task_id=dm_tasks.id AND status='privacy')
          FROM dm_tasks ORDER BY id DESC
        """
    ).fetchall()
    if not rows:
        await render_menu(
            event,
            "📭 Нет DM-задач.",
            buttons=[[Button.inline("🏠 Главное меню", b"menu_home")]],
        )
        return

    inactive_count = count_inactive_dm_tasks(conn)
    global_pause = get_global_first_dm_state()
    lines = ["📋 **DM-задачи:**"]
    if global_pause.is_paused:
        lines.append(
            "⏸ **ГЛОБАЛЬНАЯ ПАУЗА ПЕРВЫХ DM**\n"
            "Новые первые сообщения не отправляются. Пользователи продолжают добавляться в очередь."
        )
    lines.append(f"🧹 Неактуальных задач: **{inactive_count}**")
    buttons = []
    for task_id, account_id, active, created, low, high, chats, sent, privacy in rows:
        running_task = dm_monitor_tasks.get(int(task_id))
        running = bool(running_task and not running_task.done())
        if active and global_pause.is_paused:
            status = "⏸ глобальная пауза"
            status_icon = "⏸"
        elif active and running:
            status = "🟢 активна"
            status_icon = "🟢"
        elif active:
            status = "🟡 ожидает запуска"
            status_icon = "🟡"
        else:
            status = "🔴 остановлена"
            status_icon = "🔴"
        queue_size = count_pending(int(task_id))
        account_label = format_account_label(
            int(account_id), include_id=True, max_length=44
        )
        lines.append(
            f"**#{task_id}** | {account_label} | {status}\n"
            f"Чатов: {chats} | ✅ отправлено: {sent} | 🔒 закрытых ЛС: {privacy}\n"
            f"Задержка после сообщения: {int(low or 0)}–{int(high or 0)}с\n"
            f"В очереди: {queue_size} чел. | Создана: {(created or '')[:16]}"
        )
        buttons.append(
            [
                Button.inline(
                    f"⚙️ #{task_id} • {account_label} • {status_icon}",
                    f"dm_task_{task_id}".encode(),
                )
            ]
        )
    global_control_button = (
        [Button.inline("▶️ Возобновить первые DM", b"dm_global_resume_ask")]
        if global_pause.is_paused
        else [Button.inline("⏸ Пауза всех первых DM", b"dm_global_pause")]
    )
    buttons.extend(
        [
            global_control_button,
            [
                Button.inline(
                    f"🧹 Очистить неактуальные ({inactive_count})",
                    b"menu_dm_cleanup",
                )
            ],
            [Button.inline("🔄 Обновить список", b"menu_dm_list")],
            [Button.inline("🏠 Главное меню", b"menu_home")],
        ]
    )
    await render_menu(event, "\n\n".join(lines), buttons=buttons)


async def _show_dm_cleanup_confirmation(event) -> None:
    """Show the same cleanup confirmation for callback and slash command."""
    inactive_count = count_inactive_dm_tasks(conn)

    if inactive_count <= 0:
        await render_menu(
            event,
            "📭 Неактуальных DM-задач для очистки нет.",
            buttons=[
                [Button.inline("📋 Вернуться к DM-задачам", b"menu_dm_list")],
                [Button.inline("🏠 Главное меню", b"menu_home")],
            ],
        )
        return

    await render_menu(
        event,
        f"🧹 **Очистить неактуальные DM-задачи?**\n\n"
        f"Будут удалены только остановленные задачи: **{inactive_count}**.\n"
        "Активные задачи, история AI-диалогов и аккаунты не затрагиваются.",
        buttons=[
            [Button.inline("✅ Да, очистить", b"menu_dm_cleanup_confirm")],
            [Button.inline("❌ Отмена", b"menu_dm_list")],
        ],
    )


@bot.on(Query(data=b"menu_dm_cleanup"))
async def menu_dm_cleanup(event: callback_query) -> None:
    """Ask for confirmation before removing stopped DM tasks."""
    if event.sender_id not in ADMIN_ID_LIST:
        await event.answer("Недоступно", alert=True)
        return

    await _show_dm_cleanup_confirmation(event)
    await event.answer()


@bot.on(New_Message(pattern=r"^/dm_cleanup(?:@\w+)?$"))
async def cmd_dm_cleanup(event: callback_message) -> None:
    """Slash-command fallback for opening the stopped-task cleanup dialog."""
    if event.sender_id not in ADMIN_ID_LIST:
        return
    await _show_dm_cleanup_confirmation(event)


@bot.on(Query(data=b"menu_dm_cleanup_confirm"))
async def menu_dm_cleanup_confirm(event: callback_query) -> None:
    if event.sender_id not in ADMIN_ID_LIST:
        await event.answer("Недоступно", alert=True)
        return

    task_ids = delete_inactive_dm_tasks(conn)
    for task_id in task_ids:
        monitor = dm_monitor_tasks.get(int(task_id))
        if monitor and not monitor.done():
            monitor.cancel()
            await asyncio.gather(monitor, return_exceptions=True)
        stale_client = dm_monitor_clients.pop(int(task_id), None)
        if stale_client is not None:
            try:
                await stale_client.disconnect()
            except Exception as exc:
                logger.warning(
                    f"Не удалось отключить клиент удалённой DM-задачи #{task_id}: {exc}"
                )

    await render_menu(
        event,
        f"✅ Неактуальные DM-задачи очищены.\n\n"
        f"Удалено: **{len(task_ids)}**\n"
        f"Активных задач осталось: **{count_active_dm_tasks(conn)}**\n\n"
        "История отправок, завершённые контакты и opt-out сохранены.",
        buttons=[
            [Button.inline("📋 Открыть DM-задачи", b"menu_dm_list")],
            [Button.inline("🏠 Главное меню", b"menu_home")],
        ],
    )
    await event.answer("Очищено")


@bot.on(New_Message(pattern=r"^/dm_stop(?:@\w+)?(?:\s+(\d+))?$"))
async def cmd_dm_stop(event: callback_message) -> None:
    if event.sender_id not in ADMIN_ID_LIST:
        return
    match = event.pattern_match.group(1)
    if not match:
        await event.respond("Использование: /dm_stop <id>\nСписок: /dm_list")
        return
    task_id = int(match)
    if await stop_dm_task_runtime(task_id, preserve_queue=True):
        await event.respond(
            f"⏸ Задача #{task_id} остановлена. Очередь сохранена."
        )
    else:
        await event.respond(f"⚠ Задача #{task_id} не найдена.")


# ══════════════════════════════════════════════════════════════════════════════
# Главное меню — callback-обёртки над DM-командами
# ══════════════════════════════════════════════════════════════════════════════

@bot.on(Query(data=b"menu_dm_post"))
async def menu_dm_post(event: callback_query) -> None:
    if event.sender_id not in ADMIN_ID_LIST:
        await event.answer("Недоступно", alert=True)
        return
    from services.admin_state import clear_admin_interaction_state

    await clear_admin_interaction_state(event.sender_id)
    await cmd_dm_post(event)
    await event.answer()


@bot.on(Query(data=b"menu_dm_list"))
async def menu_dm_list(event: callback_query) -> None:
    if event.sender_id not in ADMIN_ID_LIST:
        await event.answer("Недоступно", alert=True)
        return
    await cmd_dm_list(event)
    await event.answer()


@bot.on(Query(data=b"dm_global_pause"))
async def dm_global_pause(event: callback_query) -> None:
    if event.sender_id not in ADMIN_ID_LIST:
        await event.answer("Недоступно", alert=True)
        return
    pause_all_first_dms(event.sender_id)
    active_tasks = count_active_dm_tasks(conn)
    pending = count_all_pending()
    await render_menu(
        event,
        "⏸ **Первые DM приостановлены**\n\n"
        f"Активных задач: **{active_tasks}**\n"
        f"Людей в очереди: **{pending}**\n\n"
        "Ни один новый первый DM не будет отправлен, пока администратор не возобновит работу.\n"
        "Пользователи продолжат добавляться в очередь, а начатые диалоги Максима продолжат работать.",
        buttons=[
            [Button.inline("📋 К DM-задачам", b"menu_dm_list")],
            [Button.inline("🏠 Главное меню", b"menu_home")],
        ],
    )
    await event.answer("Первые DM поставлены на паузу")


@bot.on(Query(data=b"dm_global_resume_ask"))
async def dm_global_resume_ask(event: callback_query) -> None:
    if event.sender_id not in ADMIN_ID_LIST:
        await event.answer("Недоступно", alert=True)
        return
    state = get_global_first_dm_state()
    if not state.is_paused:
        await event.answer("Глобальная пауза уже выключена", alert=True)
        await cmd_dm_list(event)
        return
    active_tasks = count_active_dm_tasks(conn)
    pending = count_all_pending()
    await render_menu(
        event,
        "▶️ **Возобновить первые DM?**\n\n"
        f"Активных задач: **{active_tasks}**\n"
        f"В очереди: **{pending}** пользователей\n\n"
        "Отправка продолжится постепенно по задержкам задач, паузам аккаунтов и ограничениям Telegram.",
        buttons=[
            [Button.inline("✅ Возобновить", b"dm_global_resume_yes")],
            [Button.inline("❌ Отмена", b"menu_dm_list")],
        ],
    )
    await event.answer()


@bot.on(Query(data=b"dm_global_resume_yes"))
async def dm_global_resume_yes(event: callback_query) -> None:
    if event.sender_id not in ADMIN_ID_LIST:
        await event.answer("Недоступно", alert=True)
        return
    resume_all_first_dms(event.sender_id)
    accounts = conn.execute(
        "SELECT DISTINCT user_id FROM dm_tasks WHERE is_active=1"
    ).fetchall()
    for (account_user_id,) in accounts:
        ensure_account_dispatcher(int(account_user_id))
    await render_menu(
        event,
        "▶️ **Первые DM возобновлены**\n\n"
        "Очередь будет обрабатываться постепенно по прежним настройкам каждой задачи и аккаунта.",
        buttons=[
            [Button.inline("📋 К DM-задачам", b"menu_dm_list")],
            [Button.inline("🏠 Главное меню", b"menu_home")],
        ],
    )
    await event.answer("Первые DM возобновлены")


@bot.on(Query(data=b"menu_dm_stop"))
async def menu_dm_stop(event: callback_query) -> None:
    if event.sender_id not in ADMIN_ID_LIST:
        await event.answer("Недоступно", alert=True)
        return

    cursor = conn.cursor()
    cursor.execute(
        "SELECT id, user_id FROM dm_tasks WHERE is_active = 1 ORDER BY id DESC LIMIT 50"
    )
    rows = cursor.fetchall()
    cursor.close()

    if not rows:
        await render_menu(event, "📭 Активных DM-задач нет.", buttons=[[Button.inline("🏠 Главное меню", b"menu_home")]])
        await event.answer()
        return

    buttons = [
        [
            Button.inline(
                f"⏸ #{task_id} | {format_account_label(int(user_id), include_id=True, max_length=40)}",
                f"menu_dm_stop_{task_id}".encode(),
            )
        ]
        for task_id, user_id in rows
    ]
    buttons.append([Button.inline("🏠 Главное меню", b"menu_home")])
    await render_menu(event, "🛑 Выберите DM-задачу для остановки:", buttons=buttons)
    await event.answer()


@bot.on(Query(data=lambda d: d.decode().startswith("menu_dm_stop_")))
async def menu_dm_stop_selected(event: callback_query) -> None:
    if event.sender_id not in ADMIN_ID_LIST:
        await event.answer("Недоступно", alert=True)
        return
    try:
        task_id = int(event.data.decode().rsplit("_", 1)[1])
    except (ValueError, IndexError):
        await event.answer("Некорректный ID задачи", alert=True)
        return
    if not await stop_dm_task_runtime(task_id, preserve_queue=True):
        await event.answer("Задача уже остановлена или не найдена", alert=True)
        return
    await render_menu(
        event,
        f"⏸ DM-задача #{task_id} остановлена. Очередь сохранена.",
        buttons=[[Button.inline("📋 К DM-задачам", b"menu_dm_list")]],
    )
    await event.answer("Остановлено")

