"""Durable per-dialog inbox for sequential private-message processing."""

from __future__ import annotations

import datetime as dt
from typing import Any, Optional

from config import LOCAL_DIALOG_TEXT_RETENTION_DAYS
from db.schema import db_lock, get_connection
from services import dialog_retention_policy

STATUS_PENDING = "pending"
STATUS_PROCESSING = "processing"
STATUS_DONE = "done"
STATUS_IGNORED = "ignored"


def _now_iso() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat()


def enqueue(
    account_user_id: int,
    target_user_id: int,
    text: str,
    *,
    telegram_message_id: int | None = None,
    received_at: str | None = None,
    is_hard_stop: bool = False,
    content_kind: str = "text",
) -> Optional[int]:
    """Persist an incoming message before any dialog processing.

    Returns the inbox row id. Duplicate Telegram message ids return the existing id.
    """
    account = int(account_user_id)
    target = int(target_user_id)
    msg_id = int(telegram_message_id) if telegram_message_id is not None else None
    now = _now_iso()
    received = str(received_at or now)
    conn = get_connection()
    with db_lock(), conn:
        if msg_id is not None:
            existing = conn.execute(
                """
                SELECT id FROM dialog_inbox
                 WHERE account_user_id=? AND target_user_id=?
                   AND telegram_message_id=?
                """,
                (account, target, msg_id),
            ).fetchone()
            if existing:
                return int(existing["id"])
        cur = conn.execute(
            """
            INSERT INTO dialog_inbox (
                account_user_id, target_user_id, telegram_message_id,
                text, is_hard_stop, status, received_at, updated_at, content_kind
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                account,
                target,
                msg_id,
                str(text),
                1 if is_hard_stop else 0,
                STATUS_PENDING,
                received,
                now,
                str(content_kind or "text"),
            ),
        )
        try:
            received_dt = dt.datetime.fromisoformat(received.replace("Z", "+00:00"))
            if received_dt.tzinfo is None:
                received_dt = received_dt.replace(tzinfo=dt.timezone.utc)
        except (TypeError, ValueError):
            received_dt = dt.datetime.now(dt.timezone.utc)
        purge_due = (
            received_dt + dt.timedelta(days=LOCAL_DIALOG_TEXT_RETENTION_DAYS)
        ).isoformat()
        conn.execute(
            """
            UPDATE dialogs
               SET history_purge_at=CASE
                       WHEN history_purge_at IS NULL OR history_purge_at>? THEN ?
                       ELSE history_purge_at
                   END,
                   history_purged_at=NULL
             WHERE target_user_id=?
            """,
            (purge_due, purge_due, target),
        )
        dialog_retention_policy.touch_dialog_activity(conn, target, received_dt.isoformat())
        return int(cur.lastrowid)


def claim_next(account_user_id: int, target_user_id: int) -> Optional[dict[str, Any]]:
    """Claim one message, prioritizing a direct refusal over normal queued text."""
    account = int(account_user_id)
    target = int(target_user_id)
    conn = get_connection()
    with db_lock(), conn:
        row = conn.execute(
            """
            SELECT id, account_user_id, target_user_id, telegram_message_id,
                   text, is_hard_stop, status, received_at, updated_at,
                   history_appended, content_kind
              FROM dialog_inbox
             WHERE account_user_id=? AND target_user_id=? AND status=?
             ORDER BY is_hard_stop DESC, id ASC
             LIMIT 1
            """,
            (account, target, STATUS_PENDING),
        ).fetchone()
        if not row:
            return None
        now = _now_iso()
        cur = conn.execute(
            """
            UPDATE dialog_inbox
               SET status=?, processing_started_at=?, updated_at=?, last_error=NULL
             WHERE id=? AND status=?
            """,
            (STATUS_PROCESSING, now, now, int(row["id"]), STATUS_PENDING),
        )
        if int(cur.rowcount or 0) != 1:
            return None
        data = dict(row)
        data["status"] = STATUS_PROCESSING
        data["processing_started_at"] = now
        return data


def mark_history_appended(row_id: int) -> None:
    conn = get_connection()
    with db_lock(), conn:
        conn.execute(
            """
            UPDATE dialog_inbox
               SET history_appended=1, updated_at=?
             WHERE id=?
            """,
            (_now_iso(), int(row_id)),
        )


def mark_done(row_id: int) -> None:
    _finish(row_id, STATUS_DONE, None)


def mark_ignored(row_id: int, reason: str) -> None:
    _finish(row_id, STATUS_IGNORED, reason)


def _finish(row_id: int, status: str, reason: str | None) -> None:
    now = _now_iso()
    conn = get_connection()
    with db_lock(), conn:
        conn.execute(
            """
            UPDATE dialog_inbox
               SET status=?, processed_at=?, updated_at=?, last_error=?
             WHERE id=?
            """,
            (status, now, now, reason, int(row_id)),
        )


def requeue(row_id: int, error: str | None = None) -> None:
    now = _now_iso()
    conn = get_connection()
    with db_lock(), conn:
        conn.execute(
            """
            UPDATE dialog_inbox
               SET status=?, processing_started_at=NULL, updated_at=?, last_error=?
             WHERE id=?
            """,
            (STATUS_PENDING, now, error, int(row_id)),
        )


def ignore_pending_for_target(
    target_user_id: int,
    reason: str,
    *,
    include_hard_stop: bool = False,
) -> int:
    now = _now_iso()
    conn = get_connection()
    clause = "" if include_hard_stop else " AND is_hard_stop=0"
    with db_lock(), conn:
        cur = conn.execute(
            f"""
            UPDATE dialog_inbox
               SET status=?, processed_at=?, updated_at=?, last_error=?
             WHERE target_user_id=? AND status=?{clause}
            """,
            (STATUS_IGNORED, now, now, reason, int(target_user_id), STATUS_PENDING),
        )
        return int(cur.rowcount or 0)


def has_pending(account_user_id: int, target_user_id: int) -> bool:
    conn = get_connection()
    row = conn.execute(
        """
        SELECT 1 FROM dialog_inbox
         WHERE account_user_id=? AND target_user_id=? AND status=?
         LIMIT 1
        """,
        (int(account_user_id), int(target_user_id), STATUS_PENDING),
    ).fetchone()
    return row is not None


def reset_stale_processing(older_than_seconds: int = 300) -> int:
    cutoff = (
        dt.datetime.now(dt.timezone.utc) - dt.timedelta(seconds=max(1, int(older_than_seconds)))
    ).isoformat()
    now = _now_iso()
    conn = get_connection()
    with db_lock(), conn:
        cur = conn.execute(
            """
            UPDATE dialog_inbox
               SET status=?, processing_started_at=NULL, updated_at=?,
                   last_error=COALESCE(last_error, 'stale_processing_recovered')
             WHERE status=?
               AND (processing_started_at IS NULL OR processing_started_at<=?)
            """,
            (STATUS_PENDING, now, STATUS_PROCESSING, cutoff),
        )
        return int(cur.rowcount or 0)


def list_pending_dialogs(limit: int = 100) -> list[tuple[int, int]]:
    conn = get_connection()
    rows = conn.execute(
        """
        SELECT account_user_id, target_user_id, MIN(id) AS first_id
          FROM dialog_inbox
         WHERE status=?
         GROUP BY account_user_id, target_user_id
         ORDER BY first_id ASC
         LIMIT ?
        """,
        (STATUS_PENDING, max(1, int(limit))),
    ).fetchall()
    return [(int(r["account_user_id"]), int(r["target_user_id"])) for r in rows]


def count_by_status(status: str) -> int:
    conn = get_connection()
    row = conn.execute(
        "SELECT COUNT(*) AS c FROM dialog_inbox WHERE status=?",
        (str(status),),
    ).fetchone()
    return int(row["c"] if row else 0)
