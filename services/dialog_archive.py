"""Immutable snapshots of previous dialog attempts.

The operational ``dialogs`` table keeps only the current attempt for a target.
Before an explicit requeue/import, the previous attempt is copied here so its
history, statistics and 30/180-day retention jobs survive independently.
"""

from __future__ import annotations

import datetime as dt
import json
import sqlite3
from typing import Any

from config import LOCAL_DIALOG_TEXT_RETENTION_DAYS
from db.schema import db_lock, get_connection


def _now_iso() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat()


def _rows_as_json(rows: list[sqlite3.Row]) -> str:
    return json.dumps([dict(row) for row in rows], ensure_ascii=False)


def archive_current_attempt(
    target_user_id: int,
    *,
    reason: str = "explicit_requeue",
    conn: sqlite3.Connection | None = None,
) -> int | None:
    """Copy the current attempt into ``dialog_archives`` without deleting it.

    The caller normally performs the deletes in the same transaction after this
    function returns.  ``conn`` can be supplied to keep the whole requeue atomic.
    """
    target = int(target_user_id)
    connection = conn or get_connection()
    own_transaction = conn is None

    def _archive() -> int | None:
        dialog = connection.execute(
            "SELECT * FROM dialogs WHERE target_user_id=?", (target,)
        ).fetchone()
        first = connection.execute(
            "SELECT * FROM first_dm_outbox WHERE target_user_id=?", (target,)
        ).fetchone()
        outbox = connection.execute(
            "SELECT * FROM dialog_outbox WHERE target_user_id=? ORDER BY prepared_at, action_kind",
            (target,),
        ).fetchall()
        inbox = connection.execute(
            "SELECT * FROM dialog_inbox WHERE target_user_id=? ORDER BY id",
            (target,),
        ).fetchall()
        if dialog is None and first is None and not outbox and not inbox:
            return None

        d = dict(dialog) if dialog is not None else {}
        f = dict(first) if first is not None else {}
        account = d.get("account_user_id") or f.get("account_user_id")
        if account is None:
            lead = connection.execute(
                "SELECT source_account_user_id, claimed_by_account FROM leads WHERE target_user_id=?",
                (target,),
            ).fetchone()
            if lead is not None:
                account = lead["claimed_by_account"] or lead["source_account_user_id"]
        account = int(account or 0)
        now = _now_iso()
        # Even a prepared-but-not-confirmed attempt can contain message text.
        # Give such text its own 180-day cleanup instead of retaining it forever.
        history_purge_at = d.get("history_purge_at")
        if not history_purge_at:
            raw_base = f.get("sent_at") or f.get("prepared_at") or d.get("updated_at") or now
            try:
                base = dt.datetime.fromisoformat(str(raw_base).replace("Z", "+00:00"))
                if base.tzinfo is None:
                    base = base.replace(tzinfo=dt.timezone.utc)
            except (TypeError, ValueError):
                base = dt.datetime.now(dt.timezone.utc)
            history_purge_at = (
                base + dt.timedelta(days=LOCAL_DIALOG_TEXT_RETENTION_DAYS)
            ).isoformat()
        cur = connection.execute(
            """
            INSERT INTO dialog_archives (
                target_user_id, account_user_id, stage, outgoing_count, link_sent,
                auto_link_at, history_json, original_updated_at, first_dm_at,
                first_dm_message_id, telegram_delete_at, telegram_deleted_at,
                telegram_delete_next_attempt_at, telegram_delete_attempts,
                telegram_delete_last_error, telegram_delete_until_message_id,
                next_attempt_first_dm_at, history_purge_at, history_purged_at,
                lifecycle_completed_at, last_message_at, telegram_delete_abandoned_at,
                first_dm_text, first_dm_prepared_at, first_dm_sent_at,
                first_dm_outbox_status, dialog_outbox_json, dialog_inbox_json,
                archived_reason, archived_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, NULL,
                      ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                target,
                account,
                str(d.get("stage") or "unknown"),
                int(d.get("outgoing_count") or 0),
                int(d.get("link_sent") or 0),
                d.get("auto_link_at"),
                str(d.get("history_json") or "[]"),
                d.get("updated_at") or f.get("updated_at") or now,
                d.get("first_dm_at") or f.get("sent_at"),
                d.get("first_dm_message_id") or f.get("telegram_message_id"),
                d.get("telegram_delete_at"),
                d.get("telegram_deleted_at"),
                d.get("telegram_delete_next_attempt_at"),
                int(d.get("telegram_delete_attempts") or 0),
                d.get("telegram_delete_last_error"),
                history_purge_at,
                d.get("history_purged_at"),
                d.get("lifecycle_completed_at"),
                d.get("last_message_at"),
                d.get("telegram_delete_abandoned_at"),
                str(f.get("text") or ""),
                f.get("prepared_at"),
                f.get("sent_at"),
                f.get("status"),
                _rows_as_json(outbox),
                _rows_as_json(inbox),
                str(reason),
                now,
            ),
        )
        return int(cur.lastrowid)

    if own_transaction:
        with db_lock(), connection:
            return _archive()
    return _archive()


def bound_previous_attempts(
    target_user_id: int,
    *,
    next_account_user_id: int,
    next_first_dm_message_id: int | None,
    next_first_dm_at: str,
    conn: sqlite3.Connection | None = None,
) -> int:
    """Prevent an older cleanup job from deleting a newer attempt.

    If Telegram returned a message ID, the older attempt is capped at the message
    immediately before it.  The timestamp is always stored as a fallback.
    """
    target = int(target_user_id)
    next_account = int(next_account_user_id)
    connection = conn or get_connection()
    own_transaction = conn is None
    upper = (
        max(0, int(next_first_dm_message_id) - 1)
        if next_first_dm_message_id is not None
        else None
    )

    def _update() -> int:
        cur = connection.execute(
            """
            UPDATE dialog_archives
               SET telegram_delete_until_message_id=COALESCE(
                       telegram_delete_until_message_id, ?
                   ),
                   next_attempt_first_dm_at=COALESCE(next_attempt_first_dm_at, ?)
             WHERE target_user_id=?
               AND account_user_id=?
               AND telegram_deleted_at IS NULL
               AND telegram_delete_abandoned_at IS NULL
               AND first_dm_at IS NOT NULL
               AND next_attempt_first_dm_at IS NULL
            """,
            (upper, str(next_first_dm_at), target, next_account),
        )
        return int(cur.rowcount or 0)

    if own_transaction:
        with db_lock(), connection:
            return _update()
    return _update()


def count_for_target(target_user_id: int) -> int:
    conn = get_connection()
    row = conn.execute(
        "SELECT COUNT(*) AS c FROM dialog_archives WHERE target_user_id=?",
        (int(target_user_id),),
    ).fetchone()
    return int(row["c"] if row else 0)


def list_for_target(target_user_id: int) -> list[dict[str, Any]]:
    conn = get_connection()
    rows = conn.execute(
        """
        SELECT * FROM dialog_archives
         WHERE target_user_id=?
         ORDER BY id ASC
        """,
        (int(target_user_id),),
    ).fetchall()
    return [dict(row) for row in rows]
