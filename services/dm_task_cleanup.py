from __future__ import annotations

import sqlite3
from typing import List


def count_inactive_dm_tasks(conn: sqlite3.Connection) -> int:
    row = conn.execute(
        "SELECT COUNT(*) FROM dm_tasks WHERE is_active = 0"
    ).fetchone()
    return int(row[0]) if row else 0


def count_active_dm_tasks(conn: sqlite3.Connection) -> int:
    row = conn.execute(
        "SELECT COUNT(*) FROM dm_tasks WHERE is_active = 1"
    ).fetchone()
    return int(row[0]) if row else 0


def delete_inactive_dm_tasks(conn: sqlite3.Connection) -> List[int]:
    """Delete only stopped DM tasks and their task-local rows.

    Active tasks are protected twice: the IDs are selected only from
    ``is_active = 0`` rows and the final DELETE repeats the same predicate.
    """
    task_ids = [
        int(row[0])
        for row in conn.execute(
            "SELECT id FROM dm_tasks WHERE is_active = 0 ORDER BY id"
        ).fetchall()
    ]
    if not task_ids:
        return []

    placeholders = ",".join("?" for _ in task_ids)
    with conn:
        conn.execute(
            f"DELETE FROM dm_sent_log WHERE dm_task_id IN ({placeholders})",
            task_ids,
        )
        conn.execute(
            f"DELETE FROM dm_watched_chats WHERE dm_task_id IN ({placeholders})",
            task_ids,
        )
        conn.execute(
            f"DELETE FROM dm_tasks WHERE id IN ({placeholders}) AND is_active = 0",
            task_ids,
        )
    return task_ids
