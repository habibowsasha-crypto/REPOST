from __future__ import annotations

import asyncio

from loguru import logger
from telethon import Button

from config import ADMIN_ID_LIST, Query, bot, callback_query, conn
from services.broadcast_runtime import stop_account_broadcast_jobs
from services.menu_ui import render_menu


@bot.on(Query(data=lambda data: data.decode().startswith("delete_account_")))
async def delete_account(event: callback_query) -> None:
    if event.sender_id not in ADMIN_ID_LIST:
        await event.answer("Недоступно", alert=True)
        return
    try:
        user_id = int(event.data.decode().rsplit("_", 1)[1])
    except (ValueError, IndexError):
        await event.answer("Некорректный ID аккаунта", alert=True)
        return

    cursor = conn.cursor()
    try:
        exists = cursor.execute(
            "SELECT 1 FROM sessions WHERE user_id = ?", (user_id,)
        ).fetchone()
        if not exists:
            await render_menu(
                event,
                "⚠ Этот аккаунт не найден в базе данных.",
                buttons=[[Button.inline("🏠 Главное меню", b"menu_home")]],
            )
            return

        task_rows = cursor.execute(
            "SELECT id FROM dm_tasks WHERE user_id = ? AND is_active = 1", (user_id,)
        ).fetchall()
        stop_account_broadcast_jobs(user_id)
        with conn:
            conn.execute(
                "UPDATE dm_tasks SET is_active = 0, session_string = '' WHERE user_id = ?",
                (user_id,),
            )
            conn.execute("UPDATE broadcasts SET is_active = 0 WHERE user_id = ?", (user_id,))
            # Cancel unfinished first-DM rows for this account only.
            # Global contact history / opt-out / completed registry stay intact.
            try:
                conn.execute(
                    """
                    UPDATE dm_pending_queue
                       SET status='cancelled',
                           last_error='account_deleted',
                           claim_token=NULL,
                           claimed_at=NULL,
                           updated_at=strftime('%Y-%m-%dT%H:%M:%f+00:00','now')
                     WHERE account_user_id=?
                       AND status IN (
                            'pending','claimed','sending','retry_wait',
                            'unresolved_peer','uncertain_delivery'
                       )
                    """,
                    (user_id,),
                )
            except Exception:
                pass
            try:
                conn.execute(
                    "DELETE FROM dm_account_dispatch WHERE account_user_id=?",
                    (user_id,),
                )
            except Exception:
                pass
            try:
                conn.execute(
                    """
                    UPDATE dm_unified_leads
                       SET preferred_account_user_id=CASE
                             WHEN preferred_account_user_id=? THEN NULL
                             ELSE preferred_account_user_id
                           END,
                           reserved_by_account_user_id=CASE
                             WHEN reserved_by_account_user_id=? THEN NULL
                             ELSE reserved_by_account_user_id
                           END,
                           status=CASE
                             WHEN status='reserved'
                              AND reserved_by_account_user_id=? THEN 'pending'
                             ELSE status
                           END,
                           reserve_token=CASE
                             WHEN reserved_by_account_user_id=? THEN NULL
                             ELSE reserve_token
                           END,
                           reserved_at=CASE
                             WHEN reserved_by_account_user_id=? THEN NULL
                             ELSE reserved_at
                           END,
                           updated_at=strftime('%Y-%m-%dT%H:%M:%f+00:00','now')
                     WHERE preferred_account_user_id=?
                        OR reserved_by_account_user_id=?
                        OR source_account_user_id=?
                    """,
                    (user_id, user_id, user_id, user_id, user_id, user_id, user_id, user_id),
                )
                conn.execute(
                    "DELETE FROM dm_unified_lead_accounts WHERE account_user_id=?",
                    (user_id,),
                )
            except Exception:
                pass
            conn.execute("DELETE FROM groups WHERE user_id = ?", (user_id,))
            conn.execute("DELETE FROM discovered_groups WHERE user_id = ?", (user_id,))
            conn.execute("DELETE FROM dm_spambot_monitor WHERE account_user_id = ?", (user_id,))
            conn.execute("DELETE FROM sessions WHERE user_id = ?", (user_id,))
    finally:
        cursor.close()

    # Stop in-memory monitors after the durable DB flag is already set to inactive.
    try:
        from handlers.dm.dm_handlers import (
            dm_account_dispatcher_tasks,
            dm_monitor_tasks,
        )

        cancelled = []
        for (task_id,) in task_rows:
            task = dm_monitor_tasks.get(task_id)
            if task and not task.done():
                task.cancel()
                cancelled.append(task)
        dispatcher = dm_account_dispatcher_tasks.get(int(user_id))
        if dispatcher and not dispatcher.done():
            dispatcher.cancel()
            cancelled.append(dispatcher)
        if cancelled:
            await asyncio.gather(*cancelled, return_exceptions=True)
    except Exception as exc:
        logger.debug(f"Не удалось остановить DM-runtime аккаунта {user_id}: {exc}")

    logger.info(f"Аккаунт id={user_id} удалён, его активные задачи остановлены")
    await render_menu(
        event,
        f"✅ Аккаунт id={user_id} удалён. Его активные задачи остановлены.",
        buttons=[
            [Button.inline("👤 Мои аккаунты", b"my_accounts")],
            [Button.inline("🏠 Главное меню", b"menu_home")],
        ],
    )
