"""Live group monitoring plus connected Telegram clients for account work."""

from __future__ import annotations

import asyncio
import time
from typing import Any, Optional

from loguru import logger
from telethon import TelegramClient, events
from telethon.errors import FloodWaitError
from telethon.sessions import StringSession
from telethon.tl.types import User

from config import API_HASH, API_ID
from services import account_auth
from services import accounts as accounts_svc
from services import chats as chats_svc
from services import queue as queue_svc

# account_user_id -> TelegramClient
_clients: dict[int, TelegramClient] = {}
_started = False
_lock = asyncio.Lock()
_bg_tasks: set[asyncio.Task] = set()
_bg_tasks_by_account: dict[int, set[asyncio.Task]] = {}
_entity_sync_tasks: dict[int, asyncio.Task] = {}
_last_auth_health_check = 0.0
_AUTH_HEALTH_INTERVAL_SECONDS = 60.0
_ENTITY_SYNC_HISTORY_LIMIT = 100
_ENTITY_SYNC_MIN_INTERVAL_SECONDS = 86400
_ENTITY_SYNC_CHAT_DELAY_SECONDS = 1.0


def _track_dialog_task(account_user_id: int, task: asyncio.Task) -> None:
    uid = int(account_user_id)
    _bg_tasks.add(task)
    _bg_tasks_by_account.setdefault(uid, set()).add(task)

    def _done(done_task: asyncio.Task) -> None:
        _bg_tasks.discard(done_task)
        bucket = _bg_tasks_by_account.get(uid)
        if bucket is not None:
            bucket.discard(done_task)
            if not bucket:
                _bg_tasks_by_account.pop(uid, None)
        if done_task.cancelled():
            return
        try:
            exc = done_task.exception()
        except asyncio.CancelledError:
            return
        if exc is not None:
            logger.opt(exception=exc).error(
                "Background dialog task crashed account={} task={}",
                uid,
                done_task.get_name(),
            )

    task.add_done_callback(_done)


async def _cancel_dialog_tasks(account_user_id: int | None = None) -> int:
    if account_user_id is None:
        tasks = list(_bg_tasks)
    else:
        tasks = list(_bg_tasks_by_account.get(int(account_user_id), set()))
    current = asyncio.current_task()
    tasks = [task for task in tasks if task is not current and not task.done()]
    for task in tasks:
        task.cancel()
    if tasks:
        await asyncio.gather(*tasks, return_exceptions=True)
    return len(tasks)


def is_running() -> bool:
    return _started


def connected_account_ids() -> list[int]:
    return sorted(_clients.keys())


def get_client(account_user_id: int) -> Optional[TelegramClient]:
    """Return connected Telethon client for account, if monitor holds it."""
    return _clients.get(int(account_user_id))


async def start_monitor() -> None:
    """Connect every account needed for groups, First DM or active dialogs."""
    global _started
    async with _lock:
        await _sync_clients_unlocked()
        _started = True
        logger.info(
            "Monitor started for {} account(s): {}",
            len(_clients),
            list(_clients.keys()),
        )
    _schedule_all_entity_syncs()
    pending_alerts = await account_auth.notify_pending_reauth_required()
    if pending_alerts:
        logger.warning(
            "Delivered {} pending account authorization alert(s)", pending_alerts
        )


async def stop_monitor() -> None:
    global _started
    cancelled = await _cancel_dialog_tasks()
    if cancelled:
        logger.info("Cancelled {} background dialog task(s) on monitor stop", cancelled)
    _bg_tasks.clear()
    _bg_tasks_by_account.clear()
    _entity_sync_tasks.clear()
    async with _lock:
        for uid, client in list(_clients.items()):
            await _safe_disconnect(client)
            _clients.pop(uid, None)
        _started = False
        logger.info("Monitor stopped")


async def refresh_monitor() -> None:
    """Re-read DB and reconnect clients after First DM or chat changes."""
    global _started
    async with _lock:
        await _sync_clients_unlocked()
        _started = True
        logger.info(
            "Monitor refreshed, active accounts: {} connected={}",
            list(_clients.keys()),
            list(_clients.keys()),
        )
    _schedule_all_entity_syncs()


def _schedule_all_entity_syncs() -> None:
    for uid, client in list(_clients.items()):
        _schedule_entity_sync(uid, client)


def _schedule_entity_sync(account_user_id: int, client: TelegramClient) -> None:
    uid = int(account_user_id)
    existing = _entity_sync_tasks.get(uid)
    if existing is not None and not existing.done():
        return
    if not chats_svc.list_watchable_ids(uid):
        return
    if not queue_svc.targets_missing_account_entity(uid, limit=1):
        return
    task = asyncio.create_task(
        _sync_recent_entity_evidence(uid, client),
        name=f"entity-sync-{uid}",
    )
    _entity_sync_tasks[uid] = task
    _track_dialog_task(uid, task)

    def _clear(done_task: asyncio.Task) -> None:
        if _entity_sync_tasks.get(uid) is done_task:
            _entity_sync_tasks.pop(uid, None)

    task.add_done_callback(_clear)


async def _sync_recent_entity_evidence(
    account_user_id: int,
    client: TelegramClient,
) -> int:
    """Backfill entity evidence only for leads already present in the queue.

    The scan is bounded, persisted per account/chat and never creates new leads
    from old history. This makes a newly added sender useful without repeatedly
    searching usernames through Telegram.
    """
    uid = int(account_user_id)
    targets = queue_svc.targets_missing_account_entity(uid)
    if not targets:
        return 0
    matched = 0
    scanned_chats = 0
    for chat_id in sorted(chats_svc.list_watchable_ids(uid)):
        if not targets:
            break
        if not chats_svc.entity_sync_due(
            uid,
            chat_id,
            min_interval_seconds=_ENTITY_SYNC_MIN_INTERVAL_SECONDS,
        ):
            continue
        try:
            async for message in client.iter_messages(
                int(chat_id),
                limit=_ENTITY_SYNC_HISTORY_LIMIT,
            ):
                sender = getattr(message, "sender", None)
                if sender is None:
                    sender = await message.get_sender()
                if sender is None or not isinstance(sender, User):
                    continue
                if getattr(sender, "bot", False) or getattr(sender, "is_self", False):
                    continue
                target_id = int(sender.id)
                if target_id not in targets:
                    continue
                access_hash = getattr(sender, "access_hash", None)
                if access_hash is None:
                    continue
                queue_svc.record_account_entity(
                    target_user_id=target_id,
                    account_user_id=uid,
                    access_hash=int(access_hash),
                    username=getattr(sender, "username", None),
                    source_chat_id=int(chat_id),
                    reopen_no_entity=True,
                )
                targets.discard(target_id)
                matched += 1
            chats_svc.mark_entity_sync(uid, chat_id, success=True)
            scanned_chats += 1
        except FloodWaitError as exc:
            seconds = int(getattr(exc, "seconds", 60) or 60)
            chats_svc.mark_entity_sync(
                uid,
                chat_id,
                success=False,
                error=f"FloodWait:{seconds}",
                retry_seconds=seconds,
            )
            from services import pacing

            pacing.apply_floodwait(uid, seconds)
            logger.warning(
                "Entity history sync FloodWait account={} chat={} seconds={}",
                uid,
                chat_id,
                seconds,
            )
            break
        except Exception as exc:
            if account_auth.is_auth_loss_error(exc):
                await account_auth.register_auth_loss(uid, exc, notify=True)
                await disconnect_account(uid, cancel_tasks=True)
                break
            chats_svc.mark_entity_sync(
                uid,
                chat_id,
                success=False,
                error=type(exc).__name__,
                retry_seconds=3600,
            )
            logger.warning(
                "Entity history sync failed account={} chat={} error_type={}",
                uid,
                chat_id,
                type(exc).__name__,
            )
        if _ENTITY_SYNC_CHAT_DELAY_SECONDS > 0:
            await asyncio.sleep(_ENTITY_SYNC_CHAT_DELAY_SECONDS)
    if scanned_chats or matched:
        logger.info(
            "Entity history sync complete account={} chats={} matched_leads={}",
            uid,
            scanned_chats,
            matched,
        )
    return matched


async def _sync_clients_unlocked() -> None:
    """
    Connect every authorized account needed for at least one function.

    A client is required when the account has First DM enabled, owns an open
    dialog, or has at least one watchable group. Therefore selecting groups
    automatically keeps group monitoring online even while First DM is
    manually disabled. Group events still pass through is_chat_watchable.
    """
    from services import dialog_store as dialog_store_svc

    wanted: set[int] = set()
    for acc in accounts_svc.list_accounts():
        uid = int(acc["user_id"])
        if accounts_svc.is_reauth_required(acc):
            continue
        needs_first_dm_client = bool(acc.get("participates"))
        needs_dialog_client = dialog_store_svc.has_open_for_account(uid)
        needs_group_monitor = bool(chats_svc.list_watchable_ids(uid))
        if not (needs_first_dm_client or needs_dialog_client or needs_group_monitor):
            continue
        if not acc.get("session_string"):
            continue
        wanted.add(uid)

    for uid in list(_clients.keys()):
        if uid not in wanted:
            await _safe_disconnect(_clients.pop(uid))
            logger.info("Monitor: disconnected account {}", uid)

    for uid in wanted:
        if uid in _clients:
            continue
        acc = accounts_svc.get_account(uid)
        if not acc:
            continue
        client = TelegramClient(
            StringSession(acc["session_string"]), API_ID, API_HASH
        )
        try:
            await client.connect()
            if not await client.is_user_authorized():
                logger.warning("Monitor: account {} session not authorized", uid)
                await account_auth.register_auth_loss(
                    uid, "session_not_authorized", notify=True
                )
                await _safe_disconnect(client)
                continue
            account_auth.mark_authorized(uid)
            _register_handler(client, uid)
            _clients[uid] = client
            n_chats = len(chats_svc.list_watchable_ids(uid))
            logger.info(
                "Monitor: account {} connected (watchable chats={})",
                uid,
                n_chats,
            )
        except Exception as exc:
            if account_auth.is_auth_loss_error(exc):
                logger.warning(
                    "Monitor: account {} authorization lost during connect: {}",
                    uid,
                    type(exc).__name__,
                )
                await account_auth.register_auth_loss(uid, exc, notify=True)
            else:
                logger.exception("Monitor: failed to start account {}: {}", uid, exc)
            await _safe_disconnect(client)


def _register_handler(client: TelegramClient, account_user_id: int) -> None:
    @client.on(events.NewMessage(incoming=True))
    async def _on_message(event: events.NewMessage.Event) -> None:
        try:
            if event.is_private:
                await _handle_private(account_user_id, event)
            elif event.is_group or event.is_channel:
                logger.debug(
                    "Group msg account={} chat_id={} is_group={} is_channel={}",
                    account_user_id,
                    int(event.chat_id),
                    bool(event.is_group),
                    bool(event.is_channel),
                )
                await _handle_group(account_user_id, event)
        except Exception as exc:
            if account_auth.is_auth_loss_error(exc):
                logger.warning(
                    "Monitor handler detected authorization loss account={} error={}",
                    account_user_id,
                    type(exc).__name__,
                )
                await account_auth.register_auth_loss(
                    account_user_id, exc, notify=True
                )
                await disconnect_account(account_user_id, cancel_tasks=True)
                return
            logger.exception(
                "Monitor handler error account={}: {}", account_user_id, exc
            )


async def _handle_group(
    account_user_id: int, event: events.NewMessage.Event
) -> None:
    if event.out:
        return

    # First DM participation controls only outreach. Group monitoring is
    # controlled by the account's watchable chat selection.
    acc = accounts_svc.get_account(account_user_id)
    if not acc or accounts_svc.is_reauth_required(acc):
        return

    chat_id = int(event.chat_id)
    if not chats_svc.is_chat_watchable(account_user_id, chat_id):
        logger.debug(
            "Skip chat {} for account {} (not watchable / mode filter)",
            chat_id,
            account_user_id,
        )
        return

    sender = await event.get_sender()
    if sender is None or not isinstance(sender, User):
        logger.debug(
            "Skip non-user sender in chat {} account={}", chat_id, account_user_id
        )
        return
    if getattr(sender, "bot", False) or getattr(sender, "is_self", False):
        return

    target_id = int(sender.id)
    if target_id == int(account_user_id):
        return

    action = queue_svc.upsert_from_activity(
        target_user_id=target_id,
        username=getattr(sender, "username", None),
        first_name=getattr(sender, "first_name", None),
        last_name=getattr(sender, "last_name", None),
        access_hash=getattr(sender, "access_hash", None),
        source_chat_id=chat_id,
        source_account_user_id=account_user_id,
    )
    if action == "created":
        logger.debug(
            "Lead created target={} from chat={} via account={}",
            target_id,
            chat_id,
            account_user_id,
        )
    elif action == "refreshed":
        logger.debug(
            "Lead refreshed target={} via account={} chat={}",
            target_id,
            account_user_id,
            chat_id,
        )
    else:
        logger.debug(
            "Lead skip target={} action={} account={} chat={}",
            target_id,
            action,
            account_user_id,
            chat_id,
        )


def _private_content_kind(event: events.NewMessage.Event, text: str) -> str:
    """Classify private content without interpreting its meaning."""
    message = getattr(event, "message", None)
    if getattr(message, "voice", False):
        return "voice"
    if getattr(message, "sticker", False):
        return "sticker"
    if getattr(message, "gif", False):
        return "gif"
    if getattr(message, "photo", None) is not None:
        return "photo"
    if getattr(message, "video", False):
        return "video"
    if getattr(message, "document", None) is not None and not text.strip():
        return "document"
    if text.strip():
        from services import ai_dialog

        if ai_dialog.is_emoji_only(text):
            return "emoji"
        return "text"
    return "non_text"


async def _handle_private(
    account_user_id: int, event: events.NewMessage.Event
) -> None:
    """Route private replies into the dialog engine (non-blocking)."""
    if event.out:
        return
    sender = await event.get_sender()
    if sender is None or not isinstance(sender, User):
        return
    if getattr(sender, "bot", False):
        return
    target_id = int(sender.id)
    if target_id == int(account_user_id):
        return
    text = event.raw_text or ""
    content_kind = _private_content_kind(event, text)
    if not text.strip() and content_kind != "text":
        labels = {
            "voice": "[голосовое сообщение]",
            "sticker": "[стикер]",
            "gif": "[GIF]",
            "photo": "[фото]",
            "video": "[видео]",
            "document": "[файл]",
            "non_text": "[нетекстовая реакция]",
        }
        text = labels.get(content_kind, "[нетекстовая реакция]")
    message_id = getattr(event, "id", None)
    received_at = getattr(event, "date", None)
    if received_at is not None:
        try:
            received_at = received_at.isoformat()
        except AttributeError:
            received_at = None

    from services import dialog_engine

    # Schedule so long AI delays do not block this client's event loop.
    task = asyncio.create_task(
        dialog_engine.handle_incoming_private(
            account_user_id,
            target_id,
            text,
            telegram_message_id=message_id,
            received_at=received_at,
            content_kind=content_kind,
        ),
        name=f"dialog-{account_user_id}-{target_id}",
    )
    _track_dialog_task(account_user_id, task)


async def disconnect_account(account_user_id: int, *, cancel_tasks: bool = True) -> None:
    """Remove one account from active memory immediately."""
    uid = int(account_user_id)
    if cancel_tasks:
        cancelled = await _cancel_dialog_tasks(uid)
        if cancelled:
            logger.info("Cancelled {} dialog task(s) for account {}", cancelled, uid)
    async with _lock:
        client = _clients.pop(uid, None)
        await _safe_disconnect(client)
    logger.info("Monitor: account {} removed from active memory", uid)


async def check_authorization_health(*, force: bool = False) -> int:
    """Periodically verify connected sessions and quarantine revoked accounts."""
    global _last_auth_health_check
    now = time.monotonic()
    if not force and now - _last_auth_health_check < _AUTH_HEALTH_INTERVAL_SECONDS:
        return 0
    _last_auth_health_check = now

    lost = 0
    for uid, client in list(_clients.items()):
        try:
            authorized = bool(await client.is_user_authorized())
        except Exception as exc:
            if not account_auth.is_auth_loss_error(exc):
                logger.warning(
                    "Authorization health check temporary failure account={} error={}",
                    uid,
                    type(exc).__name__,
                )
                continue
            authorized = False
            reason: str | BaseException = exc
        else:
            reason = "session_not_authorized"

        if authorized:
            account_auth.mark_authorized(uid)
            continue

        await account_auth.register_auth_loss(uid, reason, notify=True)
        await disconnect_account(uid, cancel_tasks=True)
        lost += 1
        logger.warning("Authorization health check quarantined account={}", uid)
    return lost


async def maybe_disconnect_inactive_account(account_user_id: int) -> bool:
    """Disconnect only when no First DM, dialogs or selected groups need a client."""
    uid = int(account_user_id)
    acc = accounts_svc.get_account(uid)
    if not acc or acc.get("participates"):
        return False
    if chats_svc.list_watchable_ids(uid):
        return False
    from services import dialog_store as dialog_store_svc

    if dialog_store_svc.has_open_for_account(uid):
        return False
    async with _lock:
        client = _clients.pop(uid, None)
        await _safe_disconnect(client)
    if client is not None:
        logger.info("Monitor: disabled account {} finished dialogs and disconnected", uid)
        return True
    return False


async def _safe_disconnect(client: Optional[TelegramClient]) -> None:
    if client is None:
        return
    try:
        if client.is_connected():
            await client.disconnect()
    except Exception as exc:
        logger.debug("disconnect: {}", exc)


def monitor_status() -> dict[str, Any]:
    return {
        "running": _started,
        "connected_accounts": connected_account_ids(),
        "connected_count": len(_clients),
    }
