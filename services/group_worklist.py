from __future__ import annotations

import asyncio

from loguru import logger

from config import conn


async def reconcile_dm_tasks_for_account(user_id: int) -> list[int]:
    """Make active DM tasks match the account's current working group list.

    The function removes watched-chat links that are no longer present in
    ``groups``.  Tasks with no remaining chats are stopped.  Running monitors are
    restarted only when their watched set changed.
    """
    cursor = conn.cursor()
    try:
        affected = cursor.execute(
            """
            SELECT DISTINCT t.id
            FROM dm_tasks AS t
            JOIN dm_watched_chats AS w ON w.dm_task_id = t.id
            LEFT JOIN groups AS g
              ON g.user_id = t.user_id AND g.group_id = w.chat_id
            WHERE t.user_id = ? AND t.is_active = 1 AND g.group_id IS NULL
            """,
            (user_id,),
        ).fetchall()

        task_ids = [int(row[0]) for row in affected]
        if not task_ids:
            return []

        with conn:
            for task_id in task_ids:
                conn.execute(
                    """
                    DELETE FROM dm_watched_chats
                    WHERE dm_task_id = ?
                      AND chat_id NOT IN (
                          SELECT group_id FROM groups WHERE user_id = ?
                      )
                    """,
                    (task_id, user_id),
                )
                remaining = int(
                    conn.execute(
                        "SELECT COUNT(*) FROM dm_watched_chats WHERE dm_task_id = ?",
                        (task_id,),
                    ).fetchone()[0]
                    or 0
                )
                if remaining == 0:
                    conn.execute(
                        "UPDATE dm_tasks SET is_active = 0 WHERE id = ?",
                        (task_id,),
                    )
    finally:
        cursor.close()

    # Lazy import avoids a module-registration cycle at startup.
    try:
        from handlers.dm.dm_handlers import _launch_monitor, dm_monitor_tasks

        for task_id in task_ids:
            running = dm_monitor_tasks.get(task_id)
            if running and not running.done():
                running.cancel()
                await asyncio.gather(running, return_exceptions=True)

            active = conn.execute(
                "SELECT is_active FROM dm_tasks WHERE id = ?", (task_id,)
            ).fetchone()
            if active and bool(active[0]):
                _launch_monitor(task_id)
    except Exception as exc:
        logger.warning(f"Не удалось обновить DM-мониторы после изменения групп: {exc}")

    return task_ids
