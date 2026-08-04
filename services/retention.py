"""Telegram and SQLite retention for completed DM conversations."""

from __future__ import annotations

import datetime as dt
from typing import Any

from loguru import logger
from telethon import TelegramClient
from telethon.sessions import StringSession
from telethon.tl.types import InputPeerUser

from config import API_HASH, API_ID, LOCAL_DIALOG_TEXT_RETENTION_DAYS
from db.schema import db_lock, get_connection
from services import monitor as monitor_svc


def _now() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)


def _now_iso() -> str:
    return _now().isoformat()


def _parse_iso(value: str | None) -> dt.datetime | None:
    if not value:
        return None
    try:
        parsed = dt.datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=dt.timezone.utc)
        return parsed.astimezone(dt.timezone.utc)
    except (TypeError, ValueError):
        return None


def retention_stats() -> dict[str, int]:
    conn = get_connection()
    now = _now_iso()
    tg = conn.execute(
        """
        SELECT COUNT(*) AS c FROM dialogs
         WHERE telegram_deleted_at IS NULL
           AND telegram_delete_at IS NOT NULL
        """
    ).fetchone()
    tg_due = conn.execute(
        """
        SELECT COUNT(*) AS c FROM dialogs
         WHERE telegram_deleted_at IS NULL
           AND telegram_delete_at IS NOT NULL
           AND COALESCE(telegram_delete_next_attempt_at, telegram_delete_at) <= ?
        """,
        (now,),
    ).fetchone()
    local = conn.execute(
        """
        SELECT COUNT(*) AS c FROM dialogs
         WHERE history_purged_at IS NULL AND history_purge_at IS NOT NULL
        """
    ).fetchone()
    return {
        "telegram_pending": int(tg["c"] if tg else 0),
        "telegram_due": int(tg_due["c"] if tg_due else 0),
        "local_pending": int(local["c"] if local else 0),
    }



def count_pending_for_account(account_user_id: int) -> int:
    """Dialogs whose future Telegram cleanup still needs this account session."""
    conn = get_connection()
    row = conn.execute(
        """
        SELECT COUNT(*) AS c FROM dialogs
         WHERE account_user_id=?
           AND telegram_deleted_at IS NULL
           AND telegram_delete_at IS NOT NULL
        """,
        (int(account_user_id),),
    ).fetchone()
    return int(row["c"] if row else 0)

def _list_due_telegram(limit: int = 10) -> list[dict[str, Any]]:
    conn = get_connection()
    rows = conn.execute(
        """
        SELECT d.target_user_id, d.account_user_id, d.first_dm_at,
               d.first_dm_message_id, d.telegram_delete_at,
               d.telegram_delete_attempts, a.session_string,
               au.username, au.access_hash,
               o.text AS first_dm_text, o.sent_at AS outbox_sent_at,
               o.telegram_message_id AS outbox_message_id
          FROM dialogs d
          LEFT JOIN accounts a ON a.user_id=d.account_user_id
          LEFT JOIN audience au ON au.user_id=d.target_user_id
          LEFT JOIN first_dm_outbox o ON o.target_user_id=d.target_user_id
         WHERE d.telegram_deleted_at IS NULL
           AND d.telegram_delete_at IS NOT NULL
           AND COALESCE(d.telegram_delete_next_attempt_at, d.telegram_delete_at) <= ?
         ORDER BY COALESCE(d.telegram_delete_next_attempt_at, d.telegram_delete_at) ASC
         LIMIT ?
        """,
        (_now_iso(), int(limit)),
    ).fetchall()
    return [dict(r) for r in rows]


def _schedule_retry(target_user_id: int, error: str) -> None:
    conn = get_connection()
    with db_lock(), conn:
        row = conn.execute(
            "SELECT telegram_delete_attempts FROM dialogs WHERE target_user_id=?",
            (int(target_user_id),),
        ).fetchone()
        attempts = int(row["telegram_delete_attempts"] or 0) + 1 if row else 1
        minutes = min(24 * 60, 15 * (2 ** min(attempts - 1, 6)))
        next_at = (_now() + dt.timedelta(minutes=minutes)).isoformat()
        conn.execute(
            """
            UPDATE dialogs
               SET telegram_delete_attempts=?, telegram_delete_last_error=?,
                   telegram_delete_next_attempt_at=?, updated_at=?
             WHERE target_user_id=?
            """,
            (attempts, str(error)[:500], next_at, _now_iso(), int(target_user_id)),
        )


def _mark_telegram_deleted(target_user_id: int) -> None:
    now = _now_iso()
    conn = get_connection()
    with db_lock(), conn:
        conn.execute(
            """
            UPDATE dialogs
               SET telegram_deleted_at=?, telegram_delete_next_attempt_at=NULL,
                   telegram_delete_last_error=NULL, stage='closed', auto_link_at=NULL,
                   updated_at=?
             WHERE target_user_id=?
            """,
            (now, now, int(target_user_id)),
        )
        conn.execute(
            """
            UPDATE contacts SET status='completed', updated_at=?
             WHERE target_user_id=?
            """,
            (now, int(target_user_id)),
        )
        conn.execute(
            "DELETE FROM dialog_outbox WHERE target_user_id=?",
            (int(target_user_id),),
        )


async def _resolve_entity(client, row: dict[str, Any]):
    target = int(row["target_user_id"])
    access_hash = row.get("access_hash")
    username = str(row.get("username") or "").strip().lstrip("@")
    if access_hash is not None:
        try:
            return await client.get_input_entity(InputPeerUser(target, int(access_hash)))
        except Exception as exc:
            logger.debug("retention access_hash resolve failed target={}: {}", target, exc)
    if username:
        try:
            return await client.get_input_entity(username)
        except Exception as exc:
            logger.debug("retention username resolve failed target={}: {}", target, exc)
    return await client.get_input_entity(target)


async def _find_first_dm_id(client, entity, row: dict[str, Any]) -> int | None:
    direct = row.get("first_dm_message_id") or row.get("outbox_message_id")
    if direct:
        return int(direct)
    expected = str(row.get("first_dm_text") or "").replace("\r\n", "\n").strip()
    if not expected:
        return None
    sent_at = _parse_iso(row.get("outbox_sent_at") or row.get("first_dm_at"))
    lower = sent_at - dt.timedelta(minutes=2) if sent_at else None
    messages = await client.get_messages(entity, limit=100)
    for message in messages or []:
        if not bool(getattr(message, "out", False)):
            continue
        text = str(getattr(message, "message", "") or "").replace("\r\n", "\n").strip()
        if text != expected:
            continue
        msg_date = getattr(message, "date", None)
        if msg_date is not None and msg_date.tzinfo is None:
            msg_date = msg_date.replace(tzinfo=dt.timezone.utc)
        if lower is not None and msg_date is not None and msg_date < lower:
            continue
        msg_id = getattr(message, "id", None)
        if msg_id:
            conn = get_connection()
            with db_lock(), conn:
                conn.execute(
                    "UPDATE dialogs SET first_dm_message_id=? WHERE target_user_id=?",
                    (int(msg_id), int(row["target_user_id"])),
                )
            return int(msg_id)
    return None


async def _collect_message_ids(client, entity, first_message_id: int) -> list[int]:
    ids: list[int] = []
    async for message in client.iter_messages(entity, min_id=max(0, first_message_id - 1)):
        msg_id = getattr(message, "id", None)
        if msg_id is not None and int(msg_id) >= int(first_message_id):
            ids.append(int(msg_id))
    return sorted(set(ids))


async def _delete_batches(client, entity, message_ids: list[int]) -> None:
    for start in range(0, len(message_ids), 100):
        batch = message_ids[start : start + 100]
        if batch:
            await client.delete_messages(entity, batch, revoke=True)


async def _temporary_client(session_string: str):
    client = TelegramClient(StringSession(session_string), API_ID, API_HASH)
    await client.connect()
    authorized = await client.is_user_authorized()
    if not authorized:
        await client.disconnect()
        raise RuntimeError("account_session_not_authorized")
    return client


async def process_due_telegram_deletions(limit: int = 10) -> int:
    """Delete private-chat messages from First DM onward for both sides."""
    processed = 0
    for row in _list_due_telegram(limit=limit):
        target = int(row["target_user_id"])
        account = int(row["account_user_id"])
        client = monitor_svc.get_client(account)
        temporary = False
        try:
            if client is None or not client.is_connected():
                session = str(row.get("session_string") or "")
                if not session:
                    raise RuntimeError("account_removed_or_session_missing")
                client = await _temporary_client(session)
                temporary = True
            entity = await _resolve_entity(client, row)
            first_id = await _find_first_dm_id(client, entity, row)
            if not first_id:
                raise RuntimeError("first_dm_message_id_not_found")
            message_ids = await _collect_message_ids(client, entity, first_id)
            if message_ids:
                await _delete_batches(client, entity, message_ids)
            _mark_telegram_deleted(target)
            processed += 1
            logger.info(
                "Telegram dialog deleted for both sides account={} target={} messages={}",
                account,
                target,
                len(message_ids),
            )
        except Exception as exc:
            _schedule_retry(target, f"{type(exc).__name__}: {exc}")
            logger.exception(
                "Telegram retention failed account={} target={}: {}", account, target, exc
            )
        finally:
            if temporary and client is not None:
                try:
                    await client.disconnect()
                except Exception as exc:
                    logger.debug("temporary retention client disconnect failed: {}", exc)
    return processed


def process_due_local_history_purge(limit: int = 100) -> int:
    """Erase message texts after retention while preserving metadata/statistics."""
    now_dt = _now()
    now = now_dt.isoformat()
    phrase_cutoff = (
        now_dt - dt.timedelta(days=LOCAL_DIALOG_TEXT_RETENTION_DAYS)
    ).isoformat()
    conn = get_connection()
    with db_lock(), conn:
        rows = conn.execute(
            """
            SELECT target_user_id FROM dialogs
             WHERE history_purged_at IS NULL
               AND history_purge_at IS NOT NULL
               AND history_purge_at <= ?
             ORDER BY history_purge_at ASC
             LIMIT ?
            """,
            (now, int(limit)),
        ).fetchall()
        targets = [int(r["target_user_id"]) for r in rows]
        for target in targets:
            conn.execute(
                """
                UPDATE dialogs
                   SET history_json='[]', history_purged_at=?, updated_at=?
                 WHERE target_user_id=?
                """,
                (now, now, target),
            )
            conn.execute(
                "UPDATE first_dm_outbox SET text='' WHERE target_user_id=?",
                (target,),
            )
            conn.execute(
                "UPDATE dialog_outbox SET text='' WHERE target_user_id=?",
                (target,),
            )
        phrase_cur = conn.execute(
            "DELETE FROM sent_phrases WHERE created_at <= ?",
            (phrase_cutoff,),
        )
        phrases_deleted = int(phrase_cur.rowcount or 0)
    if targets or phrases_deleted:
        logger.info(
            "Purged local text: dialogs={} old_phrase_rows={}",
            len(targets),
            phrases_deleted,
        )
    return len(targets)
