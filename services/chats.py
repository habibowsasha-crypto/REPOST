"""Chat discovery and selection for user accounts."""

from __future__ import annotations

import datetime as dt
from typing import Any, Optional

from loguru import logger
from telethon import TelegramClient, utils
from telethon.sessions import StringSession
from telethon.tl.types import Channel, Chat

from config import API_HASH, API_ID
from db.schema import db_lock, get_connection
from services import accounts as accounts_svc

CHAT_MODE_MANUAL = "manual"
CHAT_MODE_ALL = "all_with_exclusions"
VALID_CHAT_MODES = {CHAT_MODE_MANUAL, CHAT_MODE_ALL}
PAGE_SIZE = 12


def _now_iso() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat()


def get_chat_mode(account_user_id: int) -> str:
    acc = accounts_svc.get_account(account_user_id)
    if not acc:
        return CHAT_MODE_MANUAL
    mode = str(acc.get("chat_mode") or CHAT_MODE_MANUAL)
    return mode if mode in VALID_CHAT_MODES else CHAT_MODE_MANUAL


def set_chat_mode(account_user_id: int, mode: str) -> bool:
    if mode not in VALID_CHAT_MODES:
        return False
    conn = get_connection()
    with db_lock(), conn:
        cur = conn.execute(
            """
            UPDATE accounts
               SET chat_mode=?, updated_at=?
             WHERE user_id=?
            """,
            (mode, _now_iso(), int(account_user_id)),
        )
        return int(cur.rowcount or 0) == 1


def list_discovered(account_user_id: int) -> list[dict[str, Any]]:
    conn = get_connection()
    rows = conn.execute(
        """
        SELECT account_user_id, chat_id, title, username, peer_type, updated_at
          FROM account_discovered_chats
         WHERE account_user_id=?
         ORDER BY lower(COALESCE(title, username, CAST(chat_id AS TEXT)))
        """,
        (int(account_user_id),),
    ).fetchall()
    return [dict(r) for r in rows]


def list_selected_ids(account_user_id: int) -> set[int]:
    conn = get_connection()
    rows = conn.execute(
        """
        SELECT chat_id FROM account_selected_chats
         WHERE account_user_id=?
        """,
        (int(account_user_id),),
    ).fetchall()
    return {int(r[0]) for r in rows}


def list_excluded_ids(account_user_id: int) -> set[int]:
    conn = get_connection()
    rows = conn.execute(
        """
        SELECT chat_id FROM account_excluded_chats
         WHERE account_user_id=?
        """,
        (int(account_user_id),),
    ).fetchall()
    return {int(r[0]) for r in rows}


def toggle_selected(account_user_id: int, chat_id: int) -> bool:
    """Toggle manual selection. Returns True if now selected."""
    account_user_id = int(account_user_id)
    chat_id = int(chat_id)
    conn = get_connection()
    with db_lock(), conn:
        exists = conn.execute(
            """
            SELECT 1 FROM account_selected_chats
             WHERE account_user_id=? AND chat_id=?
            """,
            (account_user_id, chat_id),
        ).fetchone()
        if exists:
            conn.execute(
                """
                DELETE FROM account_selected_chats
                 WHERE account_user_id=? AND chat_id=?
                """,
                (account_user_id, chat_id),
            )
            return False
        conn.execute(
            """
            INSERT OR IGNORE INTO account_selected_chats (account_user_id, chat_id)
            VALUES (?, ?)
            """,
            (account_user_id, chat_id),
        )
        return True


def toggle_excluded(account_user_id: int, chat_id: int) -> bool:
    """Toggle exclusion in all-mode. Returns True if now excluded."""
    account_user_id = int(account_user_id)
    chat_id = int(chat_id)
    conn = get_connection()
    with db_lock(), conn:
        exists = conn.execute(
            """
            SELECT 1 FROM account_excluded_chats
             WHERE account_user_id=? AND chat_id=?
            """,
            (account_user_id, chat_id),
        ).fetchone()
        if exists:
            conn.execute(
                """
                DELETE FROM account_excluded_chats
                 WHERE account_user_id=? AND chat_id=?
                """,
                (account_user_id, chat_id),
            )
            return False
        conn.execute(
            """
            INSERT OR IGNORE INTO account_excluded_chats (account_user_id, chat_id)
            VALUES (?, ?)
            """,
            (account_user_id, chat_id),
        )
        return True


def count_watchable(account_user_id: int) -> int:
    """How many chats would be monitored under current mode."""
    mode = get_chat_mode(account_user_id)
    discovered = list_discovered(account_user_id)
    if mode == CHAT_MODE_MANUAL:
        selected = list_selected_ids(account_user_id)
        return sum(1 for d in discovered if int(d["chat_id"]) in selected)
    excluded = list_excluded_ids(account_user_id)
    return sum(1 for d in discovered if int(d["chat_id"]) not in excluded)


def chat_title(row: dict[str, Any]) -> str:
    title = (row.get("title") or "").strip()
    username = (row.get("username") or "").strip().lstrip("@")
    if title:
        return title
    if username:
        return f"@{username}"
    return str(row.get("chat_id"))


def short_title(row: dict[str, Any], limit: int = 28) -> str:
    text = " ".join(chat_title(row).split())
    if len(text) <= limit:
        return text
    return text[: limit - 1].rstrip() + "…"


async def refresh_discovered_chats(account_user_id: int) -> int:
    """
    Connect with saved session and refresh group/supergroup list.
    Returns number of discovered chats.
    """
    acc = accounts_svc.get_account(account_user_id)
    if not acc or not acc.get("session_string"):
        raise RuntimeError("account_not_found")

    client = TelegramClient(
        StringSession(acc["session_string"]), API_ID, API_HASH
    )
    found: list[tuple[int, Optional[str], Optional[str], str]] = []
    try:
        await client.connect()
        if not await client.is_user_authorized():
            raise RuntimeError("session_not_authorized")

        async for dialog in client.iter_dialogs():
            entity = dialog.entity
            peer_type = None
            chat_id = None
            title = getattr(dialog, "name", None) or getattr(entity, "title", None)
            username = getattr(entity, "username", None)

            if isinstance(entity, Chat):
                peer_type = "chat"
                # Peer id matches event.chat_id in NewMessage handlers.
                chat_id = int(utils.get_peer_id(entity))
            elif isinstance(entity, Channel):
                # Megagroups only (broadcast channels have no normal chat activity).
                if getattr(entity, "megagroup", False) or getattr(entity, "gigagroup", False):
                    peer_type = "channel"
                    chat_id = int(utils.get_peer_id(entity))
                else:
                    continue
            else:
                continue

            if chat_id is None:
                continue
            found.append(
                (
                    chat_id,
                    str(title).strip() if title else None,
                    str(username).strip() if username else None,
                    peer_type,
                )
            )
    finally:
        try:
            await client.disconnect()
        except Exception as exc:
            logger.debug("Temporary chat client disconnect failed: {}", exc)

    now = _now_iso()
    conn = get_connection()
    with db_lock(), conn:
        conn.execute(
            "DELETE FROM account_discovered_chats WHERE account_user_id=?",
            (int(account_user_id),),
        )
        conn.executemany(
            """
            INSERT INTO account_discovered_chats (
                account_user_id, chat_id, title, username, peer_type, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            [
                (int(account_user_id), chat_id, title, username, peer_type, now)
                for chat_id, title, username, peer_type in found
            ],
        )
        # Drop selections/exclusions that no longer exist.
        valid_ids = {chat_id for chat_id, _, _, _ in found}
        for table in ("account_selected_chats", "account_excluded_chats"):
            rows = conn.execute(
                f"SELECT chat_id FROM {table} WHERE account_user_id=?",
                (int(account_user_id),),
            ).fetchall()
            for (cid,) in rows:
                if int(cid) not in valid_ids:
                    conn.execute(
                        f"DELETE FROM {table} WHERE account_user_id=? AND chat_id=?",
                        (int(account_user_id), int(cid)),
                    )

    logger.info(
        "Refreshed chats for account {}: {} groups",
        account_user_id,
        len(found),
    )
    return len(found)


def is_chat_watchable(account_user_id: int, chat_id: int) -> bool:
    """Return True if this chat should be monitored for the account."""
    chat_id = int(chat_id)
    mode = get_chat_mode(account_user_id)
    if mode == CHAT_MODE_MANUAL:
        # Manual: only explicitly selected chats (must be in selection table).
        return chat_id in list_selected_ids(account_user_id)
    # all_with_exclusions: any group message except excluded.
    # Do NOT require prior discovery - newly joined groups still collect leads.
    return chat_id not in list_excluded_ids(account_user_id)


def list_watchable_ids(account_user_id: int) -> set[int]:
    mode = get_chat_mode(account_user_id)
    discovered = list_discovered(account_user_id)
    if mode == CHAT_MODE_MANUAL:
        selected = list_selected_ids(account_user_id)
        return {int(d["chat_id"]) for d in discovered if int(d["chat_id"]) in selected}
    excluded = list_excluded_ids(account_user_id)
    return {int(d["chat_id"]) for d in discovered if int(d["chat_id"]) not in excluded}
