"""Central policy for Telegram dialog auto-deletion scheduling."""

from __future__ import annotations

import datetime as dt
import sqlite3

from config import (
    DIALOG_AUTO_DELETE_AFTER_DAYS,
    DIALOG_AUTO_DELETE_ENABLED,
    TELEGRAM_DIALOG_DELETE_DAYS,
)


def parse_iso(value: str | None) -> dt.datetime | None:
    if not value:
        return None
    try:
        parsed = dt.datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except (TypeError, ValueError):
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=dt.timezone.utc)
    return parsed.astimezone(dt.timezone.utc)


def delete_after_days() -> int:
    """Return the active Telegram cleanup interval."""
    if DIALOG_AUTO_DELETE_ENABLED:
        return max(1, int(DIALOG_AUTO_DELETE_AFTER_DAYS))
    return max(1, int(TELEGRAM_DIALOG_DELETE_DAYS))


def due_at(activity_at: str | dt.datetime) -> str:
    """Calculate the next Telegram deletion time from a real message timestamp."""
    if isinstance(activity_at, dt.datetime):
        stamp = activity_at
    else:
        stamp = parse_iso(str(activity_at))
        if stamp is None:
            stamp = dt.datetime.now(dt.timezone.utc)
    if stamp.tzinfo is None:
        stamp = stamp.replace(tzinfo=dt.timezone.utc)
    stamp = stamp.astimezone(dt.timezone.utc)
    return (stamp + dt.timedelta(days=delete_after_days())).isoformat()


def touch_dialog_activity(
    conn: sqlite3.Connection,
    target_user_id: int,
    activity_at: str,
) -> None:
    """Record one real Telegram message and safely move the deletion deadline.

    The update is performed in the caller's transaction so a durable inbox or
    outbox commit and its retention deadline cannot diverge after a crash.
    """
    target = int(target_user_id)
    due = due_at(activity_at)
    if DIALOG_AUTO_DELETE_ENABLED:
        conn.execute(
            """
            UPDATE dialogs
               SET last_message_at=CASE
                       WHEN last_message_at IS NULL OR julianday(last_message_at) < julianday(?)
                       THEN ? ELSE last_message_at
                   END,
                   telegram_delete_at=?,
                   telegram_deleted_at=NULL,
                   telegram_delete_next_attempt_at=NULL,
                   telegram_delete_attempts=0,
                   telegram_delete_last_error=NULL,
                   telegram_delete_abandoned_at=NULL
             WHERE target_user_id=?
            """,
            (activity_at, activity_at, due, target),
        )
        return
    conn.execute(
        """
        UPDATE dialogs
           SET last_message_at=CASE
                   WHEN last_message_at IS NULL OR julianday(last_message_at) < julianday(?)
                   THEN ? ELSE last_message_at
               END
         WHERE target_user_id=?
        """,
        (activity_at, activity_at, target),
    )
