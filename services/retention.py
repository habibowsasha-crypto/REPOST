"""Telegram and SQLite retention for current and archived DM attempts."""

from __future__ import annotations

import datetime as dt
import json
from typing import Any

from loguru import logger
from telethon import TelegramClient
from telethon.sessions import StringSession
from telethon.tl.types import InputPeerUser

from config import API_HASH, API_ID, LOCAL_DIALOG_TEXT_RETENTION_DAYS
from db.schema import db_lock, get_connection
from services import account_auth
from services import monitor as monitor_svc
from services import telegram_history


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


def _count(sql: str, params: tuple[Any, ...] = ()) -> int:
    row = get_connection().execute(sql, params).fetchone()
    return int(row["c"] if row else 0)


def retention_stats() -> dict[str, int]:
    now = _now_iso()
    current_tg = _count(
        """SELECT COUNT(*) AS c FROM dialogs
             WHERE telegram_deleted_at IS NULL
               AND telegram_delete_abandoned_at IS NULL
               AND telegram_delete_at IS NOT NULL"""
    )
    archived_tg = _count(
        """SELECT COUNT(*) AS c FROM dialog_archives
             WHERE telegram_deleted_at IS NULL
               AND telegram_delete_abandoned_at IS NULL
               AND telegram_delete_at IS NOT NULL"""
    )
    current_due = _count(
        """SELECT COUNT(*) AS c FROM dialogs
             WHERE telegram_deleted_at IS NULL
               AND telegram_delete_abandoned_at IS NULL
               AND telegram_delete_at IS NOT NULL
               AND COALESCE(telegram_delete_next_attempt_at, telegram_delete_at) <= ?""",
        (now,),
    )
    archived_due = _count(
        """SELECT COUNT(*) AS c FROM dialog_archives
             WHERE telegram_deleted_at IS NULL
               AND telegram_delete_abandoned_at IS NULL
               AND telegram_delete_at IS NOT NULL
               AND COALESCE(telegram_delete_next_attempt_at, telegram_delete_at) <= ?""",
        (now,),
    )
    current_local = _count(
        """SELECT COUNT(*) AS c FROM dialogs
             WHERE history_purged_at IS NULL AND history_purge_at IS NOT NULL"""
    )
    archived_local = _count(
        """SELECT COUNT(*) AS c FROM dialog_archives
             WHERE history_purged_at IS NULL AND history_purge_at IS NOT NULL"""
    )
    abandoned = _count(
        """SELECT (
               (SELECT COUNT(*) FROM dialogs
                 WHERE telegram_deleted_at IS NULL
                   AND telegram_delete_abandoned_at IS NOT NULL)
             + (SELECT COUNT(*) FROM dialog_archives
                 WHERE telegram_deleted_at IS NULL
                   AND telegram_delete_abandoned_at IS NOT NULL)
           ) AS c"""
    )
    return {
        "telegram_pending": current_tg + archived_tg,
        "telegram_due": current_due + archived_due,
        "telegram_abandoned": abandoned,
        "local_pending": current_local + archived_local,
    }


def count_pending_for_account(account_user_id: int) -> int:
    """Retention jobs whose future Telegram cleanup needs this account."""
    uid = int(account_user_id)
    return _count(
        """SELECT (
               (SELECT COUNT(*) FROM dialogs
                 WHERE account_user_id=? AND telegram_deleted_at IS NULL
                   AND telegram_delete_abandoned_at IS NULL
                   AND telegram_delete_at IS NOT NULL)
             + (SELECT COUNT(*) FROM dialog_archives
                 WHERE account_user_id=? AND telegram_deleted_at IS NULL
                   AND telegram_delete_abandoned_at IS NULL
                   AND telegram_delete_at IS NOT NULL)
           ) AS c""",
        (uid, uid),
    )


def _list_due_telegram(limit: int = 10) -> list[dict[str, Any]]:
    conn = get_connection()
    now = _now_iso()
    current = conn.execute(
        """
        SELECT 'current' AS record_type, d.target_user_id AS record_id,
               d.target_user_id, d.account_user_id, d.first_dm_at,
               d.first_dm_message_id, d.telegram_delete_at,
               d.telegram_delete_attempts, NULL AS telegram_delete_until_message_id,
               NULL AS next_attempt_first_dm_at, a.session_string,
               au.username, au.access_hash, o.text AS first_dm_text,
               o.sent_at AS outbox_sent_at, o.telegram_message_id AS outbox_message_id,
               COALESCE(d.telegram_delete_next_attempt_at, d.telegram_delete_at) AS due_at
          FROM dialogs d
          LEFT JOIN accounts a ON a.user_id=d.account_user_id
          LEFT JOIN audience au ON au.user_id=d.target_user_id
          LEFT JOIN first_dm_outbox o ON o.target_user_id=d.target_user_id
         WHERE d.telegram_deleted_at IS NULL
           AND d.telegram_delete_abandoned_at IS NULL
           AND d.telegram_delete_at IS NOT NULL
           AND COALESCE(d.telegram_delete_next_attempt_at, d.telegram_delete_at) <= ?
        """,
        (now,),
    ).fetchall()
    archived = conn.execute(
        """
        SELECT 'archive' AS record_type, ar.id AS record_id,
               ar.target_user_id, ar.account_user_id, ar.first_dm_at,
               ar.first_dm_message_id, ar.telegram_delete_at,
               ar.telegram_delete_attempts, ar.telegram_delete_until_message_id,
               ar.next_attempt_first_dm_at, a.session_string,
               au.username, au.access_hash, ar.first_dm_text,
               ar.first_dm_sent_at AS outbox_sent_at,
               ar.first_dm_message_id AS outbox_message_id,
               COALESCE(ar.telegram_delete_next_attempt_at, ar.telegram_delete_at) AS due_at
          FROM dialog_archives ar
          LEFT JOIN accounts a ON a.user_id=ar.account_user_id
          LEFT JOIN audience au ON au.user_id=ar.target_user_id
         WHERE ar.telegram_deleted_at IS NULL
           AND ar.telegram_delete_abandoned_at IS NULL
           AND ar.telegram_delete_at IS NOT NULL
           AND COALESCE(ar.telegram_delete_next_attempt_at, ar.telegram_delete_at) <= ?
        """,
        (now,),
    ).fetchall()
    rows = [dict(row) for row in current] + [dict(row) for row in archived]
    rows.sort(key=lambda row: str(row.get("due_at") or ""))
    return rows[: int(limit)]


def _record_where(row: dict[str, Any]) -> tuple[str, str, int]:
    if str(row.get("record_type")) == "archive":
        return "dialog_archives", "id", int(row["record_id"])
    return "dialogs", "target_user_id", int(row["target_user_id"])


def _schedule_retry(row: dict[str, Any], error: str) -> None:
    table, key, value = _record_where(row)
    conn = get_connection()
    with db_lock(), conn:
        current = conn.execute(
            f"SELECT telegram_delete_attempts FROM {table} WHERE {key}=?", (value,)
        ).fetchone()
        attempts = int(current["telegram_delete_attempts"] or 0) + 1 if current else 1
        minutes = min(24 * 60, 15 * (2 ** min(attempts - 1, 6)))
        next_at = (_now() + dt.timedelta(minutes=minutes)).isoformat()
        conn.execute(
            f"""
            UPDATE {table}
               SET telegram_delete_attempts=?, telegram_delete_last_error=?,
                   telegram_delete_next_attempt_at=?
             WHERE {key}=?
            """,
            (attempts, str(error)[:500], next_at, value),
        )


def _mark_telegram_abandoned(row: dict[str, Any], reason: str) -> None:
    """Finish an impossible retention job instead of retrying forever."""
    now = _now_iso()
    table, key, value = _record_where(row)
    conn = get_connection()
    with db_lock(), conn:
        conn.execute(
            f"""
            UPDATE {table}
               SET telegram_delete_abandoned_at=?,
                   telegram_delete_next_attempt_at=NULL,
                   telegram_delete_last_error=?
             WHERE {key}=?
            """,
            (now, str(reason)[:500], value),
        )


def _mark_telegram_deleted(row: dict[str, Any]) -> None:
    now = _now_iso()
    table, key, value = _record_where(row)
    conn = get_connection()
    with db_lock(), conn:
        if table == "dialog_archives":
            conn.execute(
                """
                UPDATE dialog_archives
                   SET telegram_deleted_at=?, telegram_delete_next_attempt_at=NULL,
                       telegram_delete_last_error=NULL,
                       telegram_delete_abandoned_at=NULL
                 WHERE id=?
                """,
                (now, value),
            )
            return
        target = int(row["target_user_id"])
        conn.execute(
            """
            UPDATE dialogs
               SET telegram_deleted_at=?, telegram_delete_next_attempt_at=NULL,
                   telegram_delete_last_error=NULL,
                   telegram_delete_abandoned_at=NULL,
                   stage=CASE
                       WHEN stage IN ('first_dm_sending','waiting_reply','engaged','explained')
                       THEN 'closed' ELSE stage END,
                   auto_link_at=NULL,
                   lifecycle_completed_at=COALESCE(lifecycle_completed_at, ?)
             WHERE target_user_id=?
            """,
            (now, now, target),
        )
        conn.execute(
            "UPDATE contacts SET status='completed', updated_at=? WHERE target_user_id=?",
            (now, target),
        )
        conn.execute("DELETE FROM dialog_outbox WHERE target_user_id=?", (target,))


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
    message = await telegram_history.find_outgoing_text_since(
        client, entity, expected, since=lower
    )
    if message is None or not getattr(message, "id", None):
        return None
    msg_id = int(message.id)
    table, key, value = _record_where(row)
    conn = get_connection()
    with db_lock(), conn:
        conn.execute(
            f"UPDATE {table} SET first_dm_message_id=? WHERE {key}=?",
            (msg_id, value),
        )
    return msg_id


def _message_before_boundary(message: Any, row: dict[str, Any]) -> bool:
    upper = row.get("telegram_delete_until_message_id")
    msg_id = getattr(message, "id", None)
    # Telegram message IDs are the exact boundary when available.  Do not also
    # apply the timestamp fallback because synthetic/recovered dates can differ.
    if upper is not None:
        return msg_id is not None and int(msg_id) <= int(upper)
    boundary = _parse_iso(row.get("next_attempt_first_dm_at"))
    msg_date = getattr(message, "date", None)
    if boundary is not None and msg_date is not None:
        if msg_date.tzinfo is None:
            msg_date = msg_date.replace(tzinfo=dt.timezone.utc)
        if msg_date.astimezone(dt.timezone.utc) >= boundary:
            return False
    return True


async def _delete_attempt_messages(
    client, entity, first_message_id: int, row: dict[str, Any], *, batch_size: int = 100
) -> int:
    """Stream Telegram history and delete bounded chunks without loading it all."""
    batch: list[int] = []
    deleted = 0
    async for message in client.iter_messages(
        entity, min_id=max(0, int(first_message_id) - 1)
    ):
        msg_id = getattr(message, "id", None)
        if msg_id is None or int(msg_id) < int(first_message_id):
            continue
        if not _message_before_boundary(message, row):
            continue
        batch.append(int(msg_id))
        if len(batch) >= max(1, int(batch_size)):
            await client.delete_messages(entity, sorted(set(batch)), revoke=True)
            deleted += len(set(batch))
            batch.clear()
    if batch:
        await client.delete_messages(entity, sorted(set(batch)), revoke=True)
        deleted += len(set(batch))
    return deleted


async def _temporary_client(session_string: str):
    client = TelegramClient(StringSession(session_string), API_ID, API_HASH)
    await client.connect()
    if not await client.is_user_authorized():
        await client.disconnect()
        raise RuntimeError("account_session_not_authorized")
    return client


def _group_by_account(rows: list[dict[str, Any]]) -> list[tuple[int, list[dict[str, Any]]]]:
    grouped: dict[int, list[dict[str, Any]]] = {}
    order: list[int] = []
    for row in rows:
        account = int(row["account_user_id"])
        if account not in grouped:
            grouped[account] = []
            order.append(account)
        grouped[account].append(row)
    return [(account, grouped[account]) for account in order]


async def process_due_telegram_deletions(limit: int = 10) -> int:
    """Delete due attempts, reusing one Telegram connection per account."""
    processed = 0
    rows = _list_due_telegram(limit=limit)
    for account, account_rows in _group_by_account(rows):
        client = monitor_svc.get_client(account)
        temporary = False
        if client is None or not client.is_connected():
            session = next(
                (str(row.get("session_string") or "") for row in account_rows if row.get("session_string")),
                "",
            )
            if not session:
                for row in account_rows:
                    _mark_telegram_abandoned(row, "account_removed_or_session_missing")
                    logger.warning(
                        "Telegram retention abandoned account={} target={} source={}: session missing",
                        account,
                        int(row["target_user_id"]),
                        row.get("record_type"),
                    )
                continue
            try:
                client = await _temporary_client(session)
                temporary = True
            except Exception as exc:
                if (
                    account_auth.is_auth_loss_error(exc)
                    or "not_authorized" in str(exc).lower()
                ):
                    await account_auth.register_auth_loss(
                        account, exc, notify=True
                    )
                for row in account_rows:
                    _schedule_retry(row, f"{type(exc).__name__}: {exc}")
                logger.exception(
                    "Telegram retention account connect failed account={}: {}", account, exc
                )
                continue

        try:
            for row in account_rows:
                target = int(row["target_user_id"])
                try:
                    entity = await _resolve_entity(client, row)
                    first_id = await _find_first_dm_id(client, entity, row)
                    if not first_id:
                        raise RuntimeError("first_dm_message_id_not_found")
                    deleted = await _delete_attempt_messages(client, entity, first_id, row)
                    _mark_telegram_deleted(row)
                    processed += 1
                    logger.info(
                        "Telegram attempt deleted account={} target={} source={} messages={}",
                        account,
                        target,
                        row.get("record_type"),
                        deleted,
                    )
                except Exception as exc:
                    if account_auth.is_auth_loss_error(exc):
                        await account_auth.register_auth_loss(
                            account, exc, notify=True
                        )
                        await monitor_svc.disconnect_account(
                            account, cancel_tasks=True
                        )
                    _schedule_retry(row, f"{type(exc).__name__}: {exc}")
                    logger.exception(
                        "Telegram retention failed account={} target={} source={}: {}",
                        account,
                        target,
                        row.get("record_type"),
                        exc,
                    )
        finally:
            if temporary and client is not None:
                try:
                    await client.disconnect()
                except Exception as exc:
                    logger.debug("temporary retention client disconnect failed: {}", exc)
            try:
                await monitor_svc.maybe_disconnect_inactive_account(account)
            except Exception as exc:
                logger.debug("retention account cleanup failed account={}: {}", account, exc)
    return processed

def _load_json_list(raw: str | None) -> list[dict[str, Any]]:
    try:
        data = json.loads(raw or "[]")
    except (TypeError, json.JSONDecodeError):
        return []
    return [dict(item) for item in data if isinstance(item, dict)] if isinstance(data, list) else []


def _due_for(stamp: dt.datetime) -> dt.datetime:
    return stamp + dt.timedelta(days=LOCAL_DIALOG_TEXT_RETENTION_DAYS)


def _purge_history(
    raw: str | None, *, fallback_at: dt.datetime | None, cutoff: dt.datetime
) -> tuple[str, list[dt.datetime], int]:
    kept: list[dict[str, Any]] = []
    future_due: list[dt.datetime] = []
    removed = 0
    for item in _load_json_list(raw):
        text = str(item.get("text") or "")
        stamp = _parse_iso(item.get("at")) or fallback_at
        if text and stamp is not None and stamp <= cutoff:
            removed += 1
            continue
        if text and stamp is not None:
            future_due.append(_due_for(stamp))
        kept.append(item)
    return json.dumps(kept, ensure_ascii=False), future_due, removed


def _purge_snapshot_rows(
    raw: str | None,
    *,
    time_fields: tuple[str, ...],
    fallback_at: dt.datetime | None,
    cutoff: dt.datetime,
) -> tuple[str, list[dt.datetime], int]:
    rows = _load_json_list(raw)
    due: list[dt.datetime] = []
    removed = 0
    for item in rows:
        text = str(item.get("text") or "")
        stamp = None
        for field in time_fields:
            stamp = _parse_iso(item.get(field))
            if stamp is not None:
                break
        stamp = stamp or fallback_at
        if text and stamp is not None and stamp <= cutoff:
            item["text"] = ""
            removed += 1
        elif text and stamp is not None:
            due.append(_due_for(stamp))
    return json.dumps(rows, ensure_ascii=False), due, removed


def _next_due(dates: list[dt.datetime]) -> str | None:
    return min(dates).isoformat() if dates else None


def _purge_current_target(target: int, now_dt: dt.datetime, cutoff: dt.datetime) -> int:
    conn = get_connection()
    row = conn.execute("SELECT * FROM dialogs WHERE target_user_id=?", (target,)).fetchone()
    if not row:
        return 0
    fallback = _parse_iso(row["first_dm_at"])
    history_json, due, removed = _purge_history(
        row["history_json"], fallback_at=fallback, cutoff=cutoff
    )
    first = conn.execute(
        "SELECT text, prepared_at, sent_at FROM first_dm_outbox WHERE target_user_id=?",
        (target,),
    ).fetchone()
    if first and str(first["text"] or ""):
        stamp = _parse_iso(first["sent_at"] or first["prepared_at"]) or fallback
        if stamp is not None and stamp <= cutoff:
            conn.execute("UPDATE first_dm_outbox SET text='' WHERE target_user_id=?", (target,))
            removed += 1
        elif stamp is not None:
            due.append(_due_for(stamp))
    for table, stamp_expr in (
        ("dialog_outbox", "COALESCE(sent_at, prepared_at)"),
        ("dialog_inbox", "received_at"),
    ):
        rows = conn.execute(
            f"SELECT rowid AS rid, text, {stamp_expr} AS stamp FROM {table} WHERE target_user_id=?",
            (target,),
        ).fetchall()
        for item in rows:
            text = str(item["text"] or "")
            if not text:
                continue
            stamp = _parse_iso(item["stamp"]) or fallback
            if stamp is not None and stamp <= cutoff:
                conn.execute(f"UPDATE {table} SET text='' WHERE rowid=?", (int(item["rid"]),))
                removed += 1
            elif stamp is not None:
                due.append(_due_for(stamp))
    next_due = _next_due(due)
    conn.execute(
        """
        UPDATE dialogs
           SET history_json=?, history_purge_at=?, history_purged_at=?
         WHERE target_user_id=?
        """,
        (
            history_json,
            next_due,
            None if next_due else now_dt.isoformat(),
            target,
        ),
    )
    return removed


def _purge_archive(archive_id: int, now_dt: dt.datetime, cutoff: dt.datetime) -> int:
    conn = get_connection()
    row = conn.execute("SELECT * FROM dialog_archives WHERE id=?", (archive_id,)).fetchone()
    if not row:
        return 0
    fallback = _parse_iso(row["first_dm_at"] or row["first_dm_sent_at"])
    history_json, due, removed = _purge_history(
        row["history_json"], fallback_at=fallback, cutoff=cutoff
    )
    first_text = str(row["first_dm_text"] or "")
    first_stamp = _parse_iso(row["first_dm_sent_at"] or row["first_dm_prepared_at"]) or fallback
    if first_text and first_stamp is not None and first_stamp <= cutoff:
        first_text = ""
        removed += 1
    elif first_text and first_stamp is not None:
        due.append(_due_for(first_stamp))
    outbox_json, out_due, out_removed = _purge_snapshot_rows(
        row["dialog_outbox_json"],
        time_fields=("sent_at", "prepared_at"),
        fallback_at=fallback,
        cutoff=cutoff,
    )
    inbox_json, in_due, in_removed = _purge_snapshot_rows(
        row["dialog_inbox_json"],
        time_fields=("received_at",),
        fallback_at=fallback,
        cutoff=cutoff,
    )
    due.extend(out_due)
    due.extend(in_due)
    removed += out_removed + in_removed
    next_due = _next_due(due)
    conn.execute(
        """
        UPDATE dialog_archives
           SET history_json=?, first_dm_text=?, dialog_outbox_json=?,
               dialog_inbox_json=?, history_purge_at=?, history_purged_at=?
         WHERE id=?
        """,
        (
            history_json,
            first_text,
            outbox_json,
            inbox_json,
            next_due,
            None if next_due else now_dt.isoformat(),
            archive_id,
        ),
    )
    return removed


def process_due_local_history_purge(limit: int = 100) -> int:
    """Purge each message 180 days after its own timestamp, preserving metadata."""
    now_dt = _now()
    now = now_dt.isoformat()
    cutoff = now_dt - dt.timedelta(days=LOCAL_DIALOG_TEXT_RETENTION_DAYS)
    conn = get_connection()
    with db_lock(), conn:
        current = conn.execute(
            """SELECT 'current' AS kind, target_user_id AS rid, history_purge_at AS due
                 FROM dialogs
                WHERE history_purged_at IS NULL AND history_purge_at IS NOT NULL
                  AND history_purge_at <= ?""",
            (now,),
        ).fetchall()
        archived = conn.execute(
            """SELECT 'archive' AS kind, id AS rid, history_purge_at AS due
                 FROM dialog_archives
                WHERE history_purged_at IS NULL AND history_purge_at IS NOT NULL
                  AND history_purge_at <= ?""",
            (now,),
        ).fetchall()
        jobs = [dict(row) for row in current] + [dict(row) for row in archived]
        jobs.sort(key=lambda item: str(item.get("due") or ""))
        jobs = jobs[: int(limit)]
        removed_texts = 0
        for job in jobs:
            if job["kind"] == "archive":
                removed_texts += _purge_archive(int(job["rid"]), now_dt, cutoff)
            else:
                removed_texts += _purge_current_target(int(job["rid"]), now_dt, cutoff)
        phrase_cutoff = cutoff.isoformat()
        phrase_cur = conn.execute(
            "DELETE FROM sent_phrases WHERE created_at <= ?", (phrase_cutoff,)
        )
        phrases_deleted = int(phrase_cur.rowcount or 0)
    if jobs or phrases_deleted:
        logger.info(
            "Purged local text jobs={} text_fields={} old_phrase_rows={}",
            len(jobs),
            removed_texts,
            phrases_deleted,
        )
    return len(jobs)
