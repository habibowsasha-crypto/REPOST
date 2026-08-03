"""Live monitoring of participating accounts' chats + connected clients for send."""

from __future__ import annotations

import asyncio
from typing import Any, Optional

from loguru import logger
from telethon import TelegramClient, events
from telethon.sessions import StringSession
from telethon.tl.types import User

from config import API_HASH, API_ID
from services import accounts as accounts_svc
from services import chats as chats_svc
from services import queue as queue_svc

# account_user_id -> TelegramClient
_clients: dict[int, TelegramClient] = {}
_started = False
_lock = asyncio.Lock()
_bg_tasks: set[asyncio.Task] = set()


def is_running() -> bool:
    return _started


def connected_account_ids() -> list[int]:
    return sorted(_clients.keys())


def get_client(account_user_id: int) -> Optional[TelegramClient]:
    """Return connected Telethon client for account, if monitor holds it."""
    return _clients.get(int(account_user_id))


async def start_monitor() -> None:
    """Connect all participates=on accounts and attach message handlers."""
    global _started
    async with _lock:
        await _sync_clients_unlocked()
        _started = True
        logger.info(
            "Monitor started for {} account(s): {}",
            len(_clients),
            list(_clients.keys()),
        )


async def stop_monitor() -> None:
    global _started
    async with _lock:
        for uid, client in list(_clients.items()):
            await _safe_disconnect(client)
            _clients.pop(uid, None)
        _started = False
        logger.info("Monitor stopped")


async def refresh_monitor() -> None:
    """Re-read DB and reconnect clients (call after participates / chat changes)."""
    global _started
    async with _lock:
        await _sync_clients_unlocked()
        _started = True
        logger.info(
            "Monitor refreshed, active accounts: {} connected={}",
            list(_clients.keys()),
            list(_clients.keys()),
        )


async def _sync_clients_unlocked() -> None:
    """
    Connect every participates=on account that has a session.

    Watchable chats are NOT required to stay connected: without a client
    the dispatcher cannot send first DMs. Group monitoring still filters
    by is_chat_watchable at event time.
    """
    wanted: set[int] = set()
    for acc in accounts_svc.list_accounts():
        if not acc.get("participates"):
            continue
        if not acc.get("session_string"):
            continue
        wanted.add(int(acc["user_id"]))

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
                await _safe_disconnect(client)
                continue
            _register_handler(client, uid)
            _clients[uid] = client
            n_chats = len(chats_svc.list_watchable_ids(uid))
            logger.info(
                "Monitor: account {} connected (watchable chats={})",
                uid,
                n_chats,
            )
        except Exception as exc:
            logger.exception("Monitor: failed to start account {}: {}", uid, exc)
            await _safe_disconnect(client)


def _register_handler(client: TelegramClient, account_user_id: int) -> None:
    @client.on(events.NewMessage(incoming=True))
    async def _on_message(event: events.NewMessage.Event) -> None:
        try:
            if event.is_private:
                await _handle_private(account_user_id, event)
            elif event.is_group or event.is_channel:
                logger.info(
                    "Group msg account={} chat_id={} is_group={} is_channel={}",
                    account_user_id,
                    int(event.chat_id),
                    bool(event.is_group),
                    bool(event.is_channel),
                )
                await _handle_group(account_user_id, event)
        except Exception as exc:
            logger.exception(
                "Monitor handler error account={}: {}", account_user_id, exc
            )


async def _handle_group(
    account_user_id: int, event: events.NewMessage.Event
) -> None:
    if event.out:
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
        source_chat_id=chat_id,
        source_account_user_id=account_user_id,
    )
    if action == "created":
        logger.info(
            "Lead created target={} from chat={} via account={}",
            target_id,
            chat_id,
            account_user_id,
        )
    elif action == "refreshed":
        logger.info(
            "Lead refreshed target={} via account={} chat={}",
            target_id,
            account_user_id,
            chat_id,
        )
    else:
        logger.info(
            "Lead skip target={} action={} account={} chat={}",
            target_id,
            action,
            account_user_id,
            chat_id,
        )


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

    from services import dialog_engine

    # Schedule so long AI delays do not block this client's event loop.
    task = asyncio.create_task(
        dialog_engine.handle_incoming_private(account_user_id, target_id, text),
        name=f"dialog-{account_user_id}-{target_id}",
    )
    _bg_tasks.add(task)
    task.add_done_callback(_bg_tasks.discard)


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
