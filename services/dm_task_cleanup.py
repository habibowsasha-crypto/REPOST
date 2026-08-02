from __future__ import annotations

import sqlite3
from typing import List

from services.dm_task_queue import prepare_tasks_for_deletion


def count_inactive_dm_tasks(conn: sqlite3.Connection) -> int:
    row = conn.execute("SELECT COUNT(*) FROM dm_tasks WHERE is_active=0").fetchone()
    return int(row[0] or 0) if row else 0


def count_active_dm_tasks(conn: sqlite3.Connection) -> int:
    row = conn.execute("SELECT COUNT(*) FROM dm_tasks WHERE is_active=1").fetchone()
    return int(row[0] or 0) if row else 0


def delete_inactive_dm_tasks(conn: sqlite3.Connection) -> List[int]:
    """Delete stopped task definitions without erasing contact history.

    Sent logs, AI dialogs, completed-contact records and opt-out data are kept.
    Sendable rows are reassigned to another active source task of the same
    account when possible, otherwise cancelled. ``uncertain_delivery`` rows
    remain as duplicate-delivery guards and are never silently reactivated.
    """
    task_ids = [
        int(row[0])
        for row in conn.execute(
            "SELECT id FROM dm_tasks WHERE is_active=0 ORDER BY id"
        ).fetchall()
    ]
    if not task_ids:
        return []
    placeholders = ",".join("?" for _ in task_ids)
    if conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name='dm_pending_queue'"
    ).fetchone():
        prepare_tasks_for_deletion(task_ids)
    with conn:
        conn.execute(
            f"DELETE FROM dm_watched_chats WHERE dm_task_id IN ({placeholders})",
            task_ids,
        )
        conn.execute(
            f"DELETE FROM dm_tasks WHERE id IN ({placeholders}) AND is_active=0",
            task_ids,
        )
    return task_ids
