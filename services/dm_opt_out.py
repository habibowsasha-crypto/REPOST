"""Persistent global opt-out for users who explicitly ask not to be contacted.

The registry is keyed only by Telegram user id, so it survives bot restarts,
DM-task recreation, watched-chat changes and sender-account changes.
"""

from __future__ import annotations

import datetime
from collections.abc import Callable
from typing import Any

from loguru import logger

from config import conn


QueuePurger = Callable[[int], int]
_queue_purgers: list[QueuePurger] = []
_table_ready = False


def _now_iso() -> str:
    return datetime.datetime.now(datetime.timezone.utc).isoformat()


def create_opt_out_table() -> None:
    global _table_ready
    if _table_ready:
        return
    cursor = conn.cursor()
    try:
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS dm_opt_out_users (
                target_user_id INTEGER PRIMARY KEY,
                reason TEXT NOT NULL DEFAULT 'explicit_stop',
                source_dialog_id INTEGER,
                source_account_user_id INTEGER,
                source_dm_task_id INTEGER,
                username TEXT,
                first_name TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            """
        )
        cursor.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_dm_opt_out_updated_at
            ON dm_opt_out_users(updated_at DESC)
            """
        )
        conn.commit()
        _table_ready = True
    finally:
        cursor.close()


def migrate_legacy_closed_dialogs() -> int:
    """Backfill opt-out ids from older AI rows already closed by explicit refusal."""
    create_opt_out_table()
    cursor = conn.cursor()
    try:
        table_exists = cursor.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='ai_dialogs'"
        ).fetchone()
        if not table_exists:
            return 0
        now = _now_iso()
        before = conn.total_changes
        cursor.execute(
            """
            INSERT OR IGNORE INTO dm_opt_out_users (
                target_user_id, reason, source_dialog_id, source_account_user_id,
                source_dm_task_id, username, first_name, created_at, updated_at
            )
            SELECT target_user_id,
                   COALESCE(NULLIF(stopped_reason, ''), 'legacy_closed_negative'),
                   id, account_user_id, dm_task_id, username, first_name,
                   COALESCE(created_at, ?), COALESCE(updated_at, created_at, ?)
            FROM ai_dialogs
            WHERE status = 'closed_negative'
            """,
            (now, now),
        )
        conn.commit()
        inserted = conn.total_changes - before
        if inserted:
            logger.info(f"[DM opt-out] migrated legacy closed dialogs: {inserted}")
        return int(inserted)
    finally:
        cursor.close()


def register_queue_purger(callback: QueuePurger) -> None:
    """Register a runtime queue cleanup callback once."""
    if callback not in _queue_purgers:
        _queue_purgers.append(callback)


def purge_user_from_registered_queues(target_user_id: int) -> int:
    removed = 0
    for callback in tuple(_queue_purgers):
        try:
            removed += max(0, int(callback(int(target_user_id)) or 0))
        except Exception as exc:
            logger.error(
                f"[DM opt-out] queue purge callback failed for user={target_user_id}: {exc}"
            )
    return removed


def is_opted_out(target_user_id: int) -> bool:
    create_opt_out_table()
    cursor = conn.cursor()
    try:
        row = cursor.execute(
            "SELECT 1 FROM dm_opt_out_users WHERE target_user_id = ? LIMIT 1",
            (int(target_user_id),),
        ).fetchone()
        return row is not None
    finally:
        cursor.close()


def add_opt_out(
    target_user_id: int,
    *,
    reason: str = "explicit_stop",
    source_dialog_id: int | None = None,
    source_account_user_id: int | None = None,
    source_dm_task_id: int | None = None,
    username: str | None = None,
    first_name: str | None = None,
) -> int:
    """Persist a global opt-out and immediately purge live first-DM queues.

    Returns the number of queued entries removed from running tasks.
    """
    create_opt_out_table()
    now = _now_iso()
    cursor = conn.cursor()
    try:
        cursor.execute(
            """
            INSERT INTO dm_opt_out_users (
                target_user_id, reason, source_dialog_id, source_account_user_id,
                source_dm_task_id, username, first_name, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(target_user_id) DO UPDATE SET
                reason = excluded.reason,
                source_dialog_id = COALESCE(excluded.source_dialog_id, dm_opt_out_users.source_dialog_id),
                source_account_user_id = COALESCE(excluded.source_account_user_id, dm_opt_out_users.source_account_user_id),
                source_dm_task_id = COALESCE(excluded.source_dm_task_id, dm_opt_out_users.source_dm_task_id),
                username = COALESCE(excluded.username, dm_opt_out_users.username),
                first_name = COALESCE(excluded.first_name, dm_opt_out_users.first_name),
                updated_at = excluded.updated_at
            """,
            (
                int(target_user_id),
                (reason or "explicit_stop").strip(),
                source_dialog_id,
                source_account_user_id,
                source_dm_task_id,
                (username or "").strip() or None,
                (first_name or "").strip() or None,
                now,
                now,
            ),
        )
        # Cancel any not-yet-delivered persistent first-DM reservations. The live
        # queue purger below handles in-memory entries for every connected account.
        if cursor.execute(
            "SELECT 1 FROM sqlite_master "
            "WHERE type='table' AND name='dm_first_dm_claims'"
        ).fetchone():
            cursor.execute(
                "DELETE FROM dm_first_dm_claims WHERE target_user_id = ?",
                (int(target_user_id),),
            )
        conn.commit()
    finally:
        cursor.close()

    removed = purge_user_from_registered_queues(int(target_user_id))
    logger.info(
        f"[DM opt-out] user={target_user_id} saved permanently; "
        f"reason={reason}; removed_from_queues={removed}"
    )
    return removed


def remove_opt_out(target_user_id: int) -> bool:
    create_opt_out_table()
    cursor = conn.cursor()
    try:
        cursor.execute(
            "DELETE FROM dm_opt_out_users WHERE target_user_id = ?",
            (int(target_user_id),),
        )
        affected = cursor.rowcount
        conn.commit()
        return affected > 0
    finally:
        cursor.close()


def list_opt_out_users(limit: int = 50) -> list[tuple[Any, ...]]:
    create_opt_out_table()
    safe_limit = min(max(int(limit), 1), 200)
    cursor = conn.cursor()
    try:
        return cursor.execute(
            """
            SELECT target_user_id, username, first_name, reason, created_at, updated_at
            FROM dm_opt_out_users
            ORDER BY updated_at DESC
            LIMIT ?
            """,
            (safe_limit,),
        ).fetchall()
    finally:
        cursor.close()


def opt_out_count() -> int:
    create_opt_out_table()
    cursor = conn.cursor()
    try:
        return int(cursor.execute("SELECT COUNT(*) FROM dm_opt_out_users").fetchone()[0] or 0)
    finally:
        cursor.close()
