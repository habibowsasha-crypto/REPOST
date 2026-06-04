"""
DM Autoposter — автопостер в личные сообщения.

Логика:
  1. Пользователь выбирает Telethon-аккаунт из своих сессий.
  2. Выбирает чаты/группы для мониторинга (из таблицы groups этого аккаунта).
  3. Вводит текст поста (+ опционально фото).
  4. Вводит интервал (минуты) — минимальная пауза между двумя DM одному и тому же человеку.
  5. Бот запускает Telethon-клиент, который слушает новые сообщения в выбранных чатах.
  6. При появлении нового сообщения от пользователя (не бота) — проверяет, не писали ли ему
     уже в рамках этой задачи за последние interval_minutes.  Если нет — ставит в очередь ЛС.
  7. Все активные DM-задачи восстанавливаются при перезапуске бота (вызов restore_dm_tasks).
"""

import asyncio
import datetime
from loguru import logger
from typing import Optional

from telethon import TelegramClient, events
from telethon.errors import (
    UserPrivacyRestrictedError,
    FloodWaitError,
    InputUserDeactivatedError,
    UserIsBlockedError,
)
from telethon.sessions import StringSession
from telethon.tl.types import User

from config import (
    API_ID, API_HASH,
    bot, conn,
    New_Message, Query,
    callback_query, callback_message,
    ADMIN_ID_LIST,
)
from utils.database.database import create_dm_tables

# ─── состояние диалога настройки ──────────────────────────────────────────────
dm_setup_state: dict = {}   # admin_id → {step, user_id, session_string, chat_ids, ...}

# ─── активные Telethon-клиенты мониторинга ────────────────────────────────────
# dm_task_id → TelegramClient
dm_monitor_clients: dict = {}


# ══════════════════════════════════════════════════════════════════════════════
# Вспомогательные функции
# ══════════════════════════════════════════════════════════════════════════════

def _now_iso() -> str:
    return datetime.datetime.utcnow().isoformat()


def _minutes_since(iso_ts: str) -> float:
    dt = datetime.datetime.fromisoformat(iso_ts)
    return (datetime.datetime.utcnow() - dt).total_seconds() / 60


def _already_sent_recently(task_id: int, target_user_id: int, interval_minutes: int) -> bool:
    """Возвращает True, если в рамках этой задачи уже писали этому пользователю
    менее interval_minutes назад."""
    cursor = conn.cursor()
    cursor.execute(
        """SELECT sent_at FROM dm_sent_log
           WHERE dm_task_id = ? AND target_user_id = ?
           ORDER BY sent_at DESC LIMIT 1""",
        (task_id, target_user_id),
    )
    row = cursor.fetchone()
    cursor.close()
    if not row:
        return False
    return _minutes_since(row[0]) < interval_minutes


def _log_sent(task_id: int, target_user_id: int) -> None:
    cursor = conn.cursor()
    cursor.execute(
        "INSERT INTO dm_sent_log (dm_task_id, target_user_id, sent_at) VALUES (?,?,?)",
        (task_id, target_user_id, _now_iso()),
    )
    conn.commit()
    cursor.close()


def _get_task(task_id: int) -> Optional[dict]:
    cursor = conn.cursor()
    cursor.execute(
        """SELECT id, admin_id, user_id, session_string, post_text, photo_url,
                  interval_minutes, is_active
           FROM dm_tasks WHERE id = ?""",
        (task_id,),
    )
    row = cursor.fetchone()
    cursor.close()
    if not row:
        return None
    keys = ["id", "admin_id", "user_id", "session_string", "post_text",
            "photo_url", "interval_minutes", "is_active"]
    return dict(zip(keys, row))


def _get_watched_chats(task_id: int) -> list:
    cursor = conn.cursor()
    cursor.execute("SELECT chat_id FROM dm_watched_chats WHERE dm_task_id = ?", (task_id,))
    rows = cursor.fetchall()
    cursor.close()
    return [r[0] for r in rows]


# ══════════════════════════════════════════════════════════════════════════════
# Ядро мониторинга: запуск Telethon-клиента
# ══════════════════════════════════════════════════════════════════════════════

async def _start_monitor(task_id: int) -> None:
    """Запускает Telethon-клиент для задачи task_id, навешивает хендлер новых сообщений."""
    task = _get_task(task_id)
    if not task or not task["is_active"]:
        return

    watched_chats = _get_watched_chats(task_id)
    if not watched_chats:
        logger.warning(f"[DM task {task_id}] нет чатов для мониторинга")
        return

    client = TelegramClient(StringSession(task["session_string"]), API_ID, API_HASH)
    await client.connect()

    if not await client.is_user_authorized():
        logger.error(f"[DM task {task_id}] сессия не авторизована")
        await client.disconnect()
        return

    dm_monitor_clients[task_id] = client
    logger.info(f"[DM task {task_id}] мониторинг запущен, чаты: {watched_chats}")

    @client.on(events.NewMessage(chats=watched_chats))
    async def on_chat_message(event):
        # Игнорируем: сервисные сообщения, каналы, самого себя
        sender = await event.get_sender()
        if not isinstance(sender, User):
            return
        if sender.bot or sender.is_self:
            return

        target_id = sender.id

        # Получаем свежие данные задачи (текст/интервал могут меняться)
        t = _get_task(task_id)
        if not t or not t["is_active"]:
            client.remove_event_handler(on_chat_message)
            return

        if _already_sent_recently(task_id, target_id, t["interval_minutes"]):
            logger.debug(f"[DM task {task_id}] пропуск {target_id} — недавно отправляли")
            return

        # Пишем в ЛС
        try:
            if t["photo_url"]:
                await client.send_file(target_id, t["photo_url"], caption=t["post_text"])
            else:
                await client.send_message(target_id, t["post_text"])
            _log_sent(task_id, target_id)
            logger.info(f"[DM task {task_id}] ЛС отправлено → {target_id}")
        except UserPrivacyRestrictedError:
            logger.debug(f"[DM task {task_id}] {target_id} закрыл ЛС (privacy)")
        except (InputUserDeactivatedError, UserIsBlockedError):
            logger.debug(f"[DM task {task_id}] {target_id} недоступен")
        except FloodWaitError as e:
            logger.warning(f"[DM task {task_id}] FloodWait {e.seconds}s")
            await asyncio.sleep(e.seconds)
        except Exception as exc:
            logger.error(f"[DM task {task_id}] ошибка отправки: {exc}")

    # Держим клиент живым, пока задача активна
    while True:
        await asyncio.sleep(30)
        t = _get_task(task_id)
        if not t or not t["is_active"]:
            logger.info(f"[DM task {task_id}] задача остановлена, отключаю клиент")
            await client.disconnect()
            dm_monitor_clients.pop(task_id, None)
            return
        if not client.is_connected():
            await client.connect()


async def restore_dm_tasks() -> None:
    """Вызывается при старте бота — восстанавливает все активные DM-задачи."""
    create_dm_tables()
    cursor = conn.cursor()
    cursor.execute("SELECT id FROM dm_tasks WHERE is_active = 1")
    rows = cursor.fetchall()
    cursor.close()
    logger.info(f"[DM restore] активных задач: {len(rows)}")
    for (task_id,) in rows:
        asyncio.ensure_future(_start_monitor(task_id))


# ══════════════════════════════════════════════════════════════════════════════
# Telegram-бот: UI настройки задачи
# ══════════════════════════════════════════════════════════════════════════════

from telethon.tl.custom import Button  # noqa: E402


# ── /dm_post — точка входа ────────────────────────────────────────────────────

@bot.on(New_Message(pattern=r"/dm_post"))
async def cmd_dm_post(event: callback_message) -> None:
    if event.sender_id not in ADMIN_ID_LIST:
        return

    # Получаем все сессии этого админа
    cursor = conn.cursor()
    cursor.execute("SELECT user_id FROM sessions")
    sessions = cursor.fetchall()
    cursor.close()

    if not sessions:
        await event.respond("⚠ Нет добавленных аккаунтов. Сначала добавьте аккаунт через /start.")
        return

    buttons = [
        [Button.inline(f"👤 Аккаунт #{uid}", f"dm_acc_{uid}".encode())]
        for (uid,) in sessions
    ]
    await event.respond("📩 **DM Автопостер**\n\nВыберите аккаунт для рассылки в ЛС:", buttons=buttons)


# ── Выбор аккаунта ────────────────────────────────────────────────────────────

@bot.on(Query(data=lambda d: d.decode().startswith("dm_acc_")))
async def dm_pick_account(event: callback_query) -> None:
    admin_id = event.sender_id
    user_id = int(event.data.decode().split("_")[2])

    # Подгружаем session_string
    cursor = conn.cursor()
    cursor.execute("SELECT session_string FROM sessions WHERE user_id = ?", (user_id,))
    row = cursor.fetchone()
    # Получаем список групп этого аккаунта
    cursor.execute("SELECT group_id, group_username FROM groups WHERE user_id = ?", (user_id,))
    groups = cursor.fetchall()
    cursor.close()

    if not row:
        await event.respond("⚠ Сессия не найдена.")
        return
    if not groups:
        await event.respond("⚠ У этого аккаунта нет добавленных групп. Сначала добавьте группы.")
        return

    dm_setup_state[admin_id] = {
        "step": "pick_chats",
        "user_id": user_id,
        "session_string": row[0],
        "selected_chats": [],
        "all_groups": groups,  # [(group_id, username), ...]
    }

    buttons = [
        [Button.inline(f"{'✅' if False else '☐'} {uname or gid}", f"dm_tog_{gid}".encode())]
        for gid, uname in groups
    ]
    buttons.append([Button.inline("✅ Готово — выбрать эти чаты", b"dm_chats_done")])
    await event.respond(
        "📋 **Выберите чаты для мониторинга**\n(нажмите для переключения, затем «Готово»):",
        buttons=buttons,
    )


# ── Переключение выбора чата ──────────────────────────────────────────────────

@bot.on(Query(data=lambda d: d.decode().startswith("dm_tog_")))
async def dm_toggle_chat(event: callback_query) -> None:
    admin_id = event.sender_id
    st = dm_setup_state.get(admin_id)
    if not st or st["step"] != "pick_chats":
        await event.answer("Сначала начните настройку через /dm_post")
        return

    chat_id = int(event.data.decode().split("_")[2])
    sel = st["selected_chats"]
    if chat_id in sel:
        sel.remove(chat_id)
    else:
        sel.append(chat_id)

    # Перерисовываем кнопки
    buttons = [
        [Button.inline(
            f"{'✅' if gid in sel else '☐'} {uname or gid}",
            f"dm_tog_{gid}".encode(),
        )]
        for gid, uname in st["all_groups"]
    ]
    buttons.append([Button.inline("✅ Готово — выбрать эти чаты", b"dm_chats_done")])
    await event.edit(
        "📋 **Выберите чаты для мониторинга**\n(нажмите для переключения, затем «Готово»):",
        buttons=buttons,
    )
    await event.answer()


# ── Подтверждение выбора чатов ────────────────────────────────────────────────

@bot.on(Query(data=b"dm_chats_done"))
async def dm_chats_done(event: callback_query) -> None:
    admin_id = event.sender_id
    st = dm_setup_state.get(admin_id)
    if not st or st["step"] != "pick_chats":
        await event.answer("Начните заново через /dm_post")
        return

    if not st["selected_chats"]:
        await event.answer("⚠ Выберите хотя бы один чат!", alert=True)
        return

    st["step"] = "text"
    await event.respond(
        f"✅ Выбрано чатов: {len(st['selected_chats'])}\n\n"
        "📝 Введите текст поста, который будет отправляться в ЛС:"
    )
    await event.answer()


# ── Диалог: текст → интервал → фото ──────────────────────────────────────────

@bot.on(New_Message(func=lambda e: e.sender_id in dm_setup_state and
                                   dm_setup_state[e.sender_id].get("step") in
                                   ("text", "interval", "photo")))
async def dm_dialog(event: callback_message) -> None:
    admin_id = event.sender_id
    st = dm_setup_state[admin_id]

    if st["step"] == "text":
        st["post_text"] = event.raw_text.strip()
        st["step"] = "interval"
        await event.respond(
            "⏱ Введите **интервал в минутах** — минимальная пауза перед повторной отправкой "
            "одному и тому же пользователю:"
        )
        return

    if st["step"] == "interval":
        try:
            minutes = int(event.raw_text.strip())
            if minutes <= 0:
                raise ValueError
        except ValueError:
            await event.respond("⚠ Введите положительное целое число.")
            return
        st["interval_minutes"] = minutes
        st["step"] = "photo"
        buttons = [
            [Button.inline("📸 Прикрепить фото", b"dm_photo_yes")],
            [Button.inline("❌ Без фото", b"dm_photo_no")],
        ]
        await event.respond("Хотите прикрепить фото к посту?", buttons=buttons)
        return

    if st["step"] == "photo":
        if event.photo:
            photo_path = await event.download_media()
            st["photo_url"] = photo_path
            await _save_and_launch(event, admin_id, st)
        else:
            await event.respond("⚠ Отправьте фото или нажмите «Без фото».")


@bot.on(Query(data=b"dm_photo_yes"))
async def dm_photo_yes(event: callback_query) -> None:
    admin_id = event.sender_id
    st = dm_setup_state.get(admin_id)
    if not st:
        return
    st["step"] = "photo"
    await event.respond("📸 Отправьте фото:")
    await event.answer()


@bot.on(Query(data=b"dm_photo_no"))
async def dm_photo_no(event: callback_query) -> None:
    admin_id = event.sender_id
    st = dm_setup_state.get(admin_id)
    if not st:
        return
    st["photo_url"] = None
    await _save_and_launch(event, admin_id, st)
    await event.answer()


# ── Сохранение задачи и запуск мониторинга ────────────────────────────────────

async def _save_and_launch(event, admin_id: int, st: dict) -> None:
    """Записывает задачу в БД и запускает Telethon-мониторинг."""
    cursor = conn.cursor()
    cursor.execute(
        """INSERT INTO dm_tasks
           (admin_id, user_id, session_string, post_text, photo_url, interval_minutes, is_active, created_at)
           VALUES (?,?,?,?,?,?,1,?)""",
        (
            admin_id,
            st["user_id"],
            st["session_string"],
            st["post_text"],
            st.get("photo_url"),
            st["interval_minutes"],
            _now_iso(),
        ),
    )
    task_id = cursor.lastrowid

    for chat_id in st["selected_chats"]:
        cursor.execute(
            "INSERT INTO dm_watched_chats (dm_task_id, chat_id) VALUES (?,?)",
            (task_id, chat_id),
        )
    conn.commit()
    cursor.close()

    dm_setup_state.pop(admin_id, None)

    # Запускаем мониторинг в фоне
    asyncio.ensure_future(_start_monitor(task_id))

    await event.respond(
        f"🚀 **DM-задача #{task_id} запущена!**\n\n"
        f"👥 Чатов для мониторинга: {len(st['selected_chats'])}\n"
        f"⏱ Интервал: {st['interval_minutes']} мин\n"
        f"📸 Фото: {'да' if st.get('photo_url') else 'нет'}\n\n"
        f"Бот будет писать в ЛС всем, кто напишет в выбранных чатах.\n"
        f"Управление: /dm_list — список задач, /dm_stop <id> — остановить."
    )


# ── /dm_list — список активных задач ─────────────────────────────────────────

@bot.on(New_Message(pattern=r"/dm_list"))
async def cmd_dm_list(event: callback_message) -> None:
    if event.sender_id not in ADMIN_ID_LIST:
        return

    cursor = conn.cursor()
    cursor.execute(
        """SELECT id, user_id, interval_minutes, is_active, created_at,
                  (SELECT COUNT(*) FROM dm_watched_chats WHERE dm_task_id = dm_tasks.id),
                  (SELECT COUNT(*) FROM dm_sent_log WHERE dm_task_id = dm_tasks.id)
           FROM dm_tasks ORDER BY id DESC"""
    )
    rows = cursor.fetchall()
    cursor.close()

    if not rows:
        await event.respond("📭 Нет DM-задач.")
        return

    lines = ["📋 **DM-задачи:**\n"]
    for tid, uid, interval, active, created, chats, sent in rows:
        status = "🟢 активна" if active else "🔴 остановлена"
        lines.append(
            f"• **Задача #{tid}** | акк: {uid} | {status}\n"
            f"  Чатов: {chats} | ЛС отправлено: {sent} | интервал: {interval} мин\n"
            f"  Создана: {created[:16] if created else '—'}"
        )
    await event.respond("\n".join(lines))


# ── /dm_stop <id> — остановить задачу ────────────────────────────────────────

@bot.on(New_Message(pattern=r"/dm_stop(?:\s+(\d+))?"))
async def cmd_dm_stop(event: callback_message) -> None:
    if event.sender_id not in ADMIN_ID_LIST:
        return

    match = event.pattern_match.group(1)
    if not match:
        await event.respond("Использование: /dm_stop <id задачи>\nСписок: /dm_list")
        return

    task_id = int(match)
    cursor = conn.cursor()
    cursor.execute("UPDATE dm_tasks SET is_active = 0 WHERE id = ?", (task_id,))
    affected = cursor.rowcount
    conn.commit()
    cursor.close()

    if not affected:
        await event.respond(f"⚠ Задача #{task_id} не найдена.")
        return

    # Клиент остановится сам через цикл в _start_monitor
    await event.respond(f"⛔ Задача #{task_id} остановлена.")
