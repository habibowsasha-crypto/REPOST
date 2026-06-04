"""
DM Autoposter — автопостер в личные сообщения.

Фичи:
  - Случайная задержка между отправками (мин-макс секунд)
  - Рандомная очередь (перемешивание pending-пользователей)
  - Закрытый ЛС → пользователь в blacklist на 24ч, не спамим повторно
  - Интервал повтора одному человеку (уже был)
"""

import asyncio
import datetime
import random
from collections import deque
from loguru import logger
from typing import Optional

from telethon import TelegramClient, events
from telethon.errors import (
    UserPrivacyRestrictedError,
    FloodWaitError,
    InputUserDeactivatedError,
    UserIsBlockedError,
    PeerFloodError,
)
from telethon.sessions import StringSession
from telethon.tl.custom import Button
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
dm_setup_state: dict = {}

# ─── активные клиенты и задачи ────────────────────────────────────────────────
dm_monitor_clients: dict = {}   # task_id → TelegramClient
dm_monitor_tasks: dict = {}     # task_id → asyncio.Task

# ─── очереди отправки (рандомайзер) ───────────────────────────────────────────
# task_id → deque of (target_user_id, sender_obj)
dm_send_queues: dict = {}


# ══════════════════════════════════════════════════════════════════════════════
# Вспомогательные функции
# ══════════════════════════════════════════════════════════════════════════════

def _now_iso() -> str:
    return datetime.datetime.utcnow().isoformat()


def _minutes_since(iso_ts: str) -> float:
    dt = datetime.datetime.fromisoformat(iso_ts)
    return (datetime.datetime.utcnow() - dt).total_seconds() / 60


def _already_sent_recently(task_id: int, target_user_id: int, interval_minutes: int) -> bool:
    """Проверяет интервал повтора одному человеку."""
    cursor = conn.cursor()
    cursor.execute(
        """SELECT sent_at FROM dm_sent_log
           WHERE dm_task_id = ? AND target_user_id = ? AND status = 'sent'
           ORDER BY sent_at DESC LIMIT 1""",
        (task_id, target_user_id),
    )
    row = cursor.fetchone()
    cursor.close()
    if not row:
        return False
    return _minutes_since(row[0]) < interval_minutes


def _is_blacklisted(task_id: int, target_user_id: int) -> bool:
    """Проверяет, в blacklist ли пользователь (закрыл ЛС < 24ч назад)."""
    cursor = conn.cursor()
    cursor.execute(
        """SELECT sent_at FROM dm_sent_log
           WHERE dm_task_id = ? AND target_user_id = ? AND status = 'privacy'
           ORDER BY sent_at DESC LIMIT 1""",
        (task_id, target_user_id),
    )
    row = cursor.fetchone()
    cursor.close()
    if not row:
        return False
    # Блокируем на 24 часа
    return _minutes_since(row[0]) < 60 * 24


def _log_event(task_id: int, target_user_id: int, status: str) -> None:
    """status: 'sent' | 'privacy' | 'blocked' | 'error'"""
    cursor = conn.cursor()
    cursor.execute(
        "INSERT INTO dm_sent_log (dm_task_id, target_user_id, sent_at, status) VALUES (?,?,?,?)",
        (task_id, target_user_id, _now_iso(), status),
    )
    conn.commit()
    cursor.close()


def _get_task(task_id: int) -> Optional[dict]:
    cursor = conn.cursor()
    cursor.execute(
        """SELECT id, admin_id, user_id, session_string, post_text, photo_url,
                  interval_minutes, is_active, delay_min, delay_max
           FROM dm_tasks WHERE id = ?""",
        (task_id,),
    )
    row = cursor.fetchone()
    cursor.close()
    if not row:
        return None
    keys = ["id", "admin_id", "user_id", "session_string", "post_text",
            "photo_url", "interval_minutes", "is_active", "delay_min", "delay_max"]
    return dict(zip(keys, row))


def _get_watched_chats(task_id: int) -> list:
    cursor = conn.cursor()
    cursor.execute("SELECT chat_id FROM dm_watched_chats WHERE dm_task_id = ?", (task_id,))
    rows = cursor.fetchall()
    cursor.close()
    return [r[0] for r in rows]


# ══════════════════════════════════════════════════════════════════════════════
# Воркер отправки (рандомная очередь + интервал)
# ══════════════════════════════════════════════════════════════════════════════

async def _send_worker(task_id: int, client: TelegramClient) -> None:
    """
    Отдельная корутина — разгребает очередь dm_send_queues[task_id].
    Между каждой отправкой — случайная пауза delay_min..delay_max секунд.
    Очередь периодически перемешивается для рандомного порядка.
    """
    queue = dm_send_queues.setdefault(task_id, deque())

    while True:
        t = _get_task(task_id)
        if not t or not t["is_active"]:
            return

        if not queue:
            await asyncio.sleep(2)
            continue

        # Перемешиваем очередь перед каждой серией
        items = list(queue)
        random.shuffle(items)
        queue.clear()
        queue.extend(items)

        target_id, sender = queue.popleft()

        # Повторная проверка перед отправкой
        if _is_blacklisted(task_id, target_id):
            logger.debug(f"[DM {task_id}] {target_id} в blacklist, пропуск")
            continue
        if _already_sent_recently(task_id, target_id, t["interval_minutes"]):
            logger.debug(f"[DM {task_id}] {target_id} недавно получал, пропуск")
            continue

        # Отправка
        try:
            if t["photo_url"]:
                await client.send_file(target_id, t["photo_url"], caption=t["post_text"])
            else:
                await client.send_message(target_id, t["post_text"])

            _log_event(task_id, target_id, "sent")
            uname = getattr(sender, "username", None)
            logger.info(f"[DM {task_id}] ✅ ЛС → {target_id} (@{uname or '?'})")

        except UserPrivacyRestrictedError:
            _log_event(task_id, target_id, "privacy")
            logger.info(f"[DM {task_id}] 🔒 {target_id} закрыл ЛС — в blacklist на 24ч")

        except (UserIsBlockedError, InputUserDeactivatedError):
            _log_event(task_id, target_id, "blocked")
            logger.debug(f"[DM {task_id}] ⛔ {target_id} заблокировал или деактивирован")

        except PeerFloodError:
            logger.warning(f"[DM {task_id}] 🌊 PeerFlood — пауза 10 мин")
            await asyncio.sleep(600)
            queue.appendleft((target_id, sender))  # вернуть в очередь
            continue

        except FloodWaitError as e:
            logger.warning(f"[DM {task_id}] ⏳ FloodWait {e.seconds}s")
            await asyncio.sleep(e.seconds)
            queue.appendleft((target_id, sender))
            continue

        except Exception as exc:
            _log_event(task_id, target_id, "error")
            logger.error(f"[DM {task_id}] ❌ ошибка отправки {target_id}: {exc}")

        # Случайная задержка между отправками
        delay_min = t.get("delay_min") or 30
        delay_max = t.get("delay_max") or 90
        delay = random.randint(int(delay_min), int(delay_max))
        logger.debug(f"[DM {task_id}] пауза {delay}s перед следующей отправкой")
        await asyncio.sleep(delay)


# ══════════════════════════════════════════════════════════════════════════════
# Ядро мониторинга
# ══════════════════════════════════════════════════════════════════════════════

async def _monitor_loop(task_id: int) -> None:
    task = _get_task(task_id)
    if not task or not task["is_active"]:
        return

    watched_chats = _get_watched_chats(task_id)
    if not watched_chats:
        logger.warning(f"[DM task {task_id}] нет чатов для мониторинга")
        return

    logger.info(f"[DM task {task_id}] запуск мониторинга, чаты: {watched_chats}")

    client = TelegramClient(StringSession(task["session_string"]), API_ID, API_HASH)

    try:
        await client.connect()
        if not await client.is_user_authorized():
            logger.error(f"[DM task {task_id}] сессия не авторизована")
            return

        dm_monitor_clients[task_id] = client
        dm_send_queues[task_id] = deque()

        # Запускаем воркер отправки
        worker = asyncio.ensure_future(_send_worker(task_id, client))

        @client.on(events.NewMessage(chats=watched_chats, incoming=True))
        async def on_chat_message(event):
            if not event.is_group and not event.is_channel:
                return
            try:
                sender = await event.get_sender()
            except Exception:
                return
            if not isinstance(sender, User):
                return
            if sender.bot or sender.is_self:
                return

            target_id = sender.id
            t = _get_task(task_id)
            if not t or not t["is_active"]:
                return

            if _is_blacklisted(task_id, target_id):
                return
            if _already_sent_recently(task_id, target_id, t["interval_minutes"]):
                return

            # Проверяем, не в очереди ли уже
            queue = dm_send_queues.get(task_id, deque())
            if any(uid == target_id for uid, _ in queue):
                return

            queue.append((target_id, sender))
            logger.debug(f"[DM {task_id}] добавлен в очередь: {target_id}, размер очереди: {len(queue)}")

        # Держим клиент живым
        while True:
            t = _get_task(task_id)
            if not t or not t["is_active"]:
                logger.info(f"[DM task {task_id}] задача остановлена")
                worker.cancel()
                break
            if not client.is_connected():
                logger.warning(f"[DM task {task_id}] реконнект...")
                await client.connect()
            await asyncio.sleep(15)

    except asyncio.CancelledError:
        logger.info(f"[DM task {task_id}] отменена")
    except Exception as e:
        logger.error(f"[DM task {task_id}] критическая ошибка: {e}")
    finally:
        try:
            await client.disconnect()
        except Exception:
            pass
        dm_monitor_clients.pop(task_id, None)
        dm_monitor_tasks.pop(task_id, None)
        dm_send_queues.pop(task_id, None)


def _launch_monitor(task_id: int) -> None:
    loop = bot.loop
    task = loop.create_task(_monitor_loop(task_id))
    dm_monitor_tasks[task_id] = task


async def restore_dm_tasks() -> None:
    create_dm_tables()
    cursor = conn.cursor()
    cursor.execute("SELECT id FROM dm_tasks WHERE is_active = 1")
    rows = cursor.fetchall()
    cursor.close()
    logger.info(f"[DM restore] активных задач: {len(rows)}")
    for (task_id,) in rows:
        _launch_monitor(task_id)


# ══════════════════════════════════════════════════════════════════════════════
# UI настройки
# ══════════════════════════════════════════════════════════════════════════════

@bot.on(New_Message(pattern=r"/dm_post"))
async def cmd_dm_post(event: callback_message) -> None:
    if event.sender_id not in ADMIN_ID_LIST:
        return
    cursor = conn.cursor()
    cursor.execute("SELECT user_id FROM sessions")
    sessions = cursor.fetchall()
    cursor.close()
    if not sessions:
        await event.respond("⚠ Нет добавленных аккаунтов. Сначала добавьте через /start.")
        return
    buttons = [
        [Button.inline(f"👤 Аккаунт #{uid}", f"dm_acc_{uid}".encode())]
        for (uid,) in sessions
    ]
    await event.respond("📩 **DM Автопостер**\n\nВыберите аккаунт:", buttons=buttons)


@bot.on(Query(data=lambda d: d.decode().startswith("dm_acc_")))
async def dm_pick_account(event: callback_query) -> None:
    admin_id = event.sender_id
    user_id = int(event.data.decode().split("_")[2])
    cursor = conn.cursor()
    cursor.execute("SELECT session_string FROM sessions WHERE user_id = ?", (user_id,))
    row = cursor.fetchone()
    cursor.execute("SELECT group_id, group_username FROM groups WHERE user_id = ?", (user_id,))
    groups = cursor.fetchall()
    cursor.close()
    if not row:
        await event.respond("⚠ Сессия не найдена.")
        return
    if not groups:
        await event.respond("⚠ Нет групп. Добавьте группы через меню.")
        return
    dm_setup_state[admin_id] = {
        "step": "pick_chats",
        "user_id": user_id,
        "session_string": row[0],
        "selected_chats": [],
        "all_groups": groups,
    }
    await event.respond(
        "📋 **Выберите чаты для мониторинга:**",
        buttons=_build_chat_buttons(groups, []),
    )
    await event.answer()


def _build_chat_buttons(groups, selected):
    buttons = [
        [Button.inline(
            f"{'✅' if gid in selected else '☐'} {uname or str(gid)}",
            f"dm_tog_{gid}".encode(),
        )]
        for gid, uname in groups
    ]
    buttons.append([Button.inline("▶️ Готово", b"dm_chats_done")])
    return buttons


@bot.on(Query(data=lambda d: d.decode().startswith("dm_tog_")))
async def dm_toggle_chat(event: callback_query) -> None:
    admin_id = event.sender_id
    st = dm_setup_state.get(admin_id)
    if not st or st["step"] != "pick_chats":
        await event.answer("Начните заново через /dm_post")
        return
    chat_id = int(event.data.decode().split("_")[2])
    sel = st["selected_chats"]
    if chat_id in sel:
        sel.remove(chat_id)
    else:
        sel.append(chat_id)
    await event.edit("📋 **Выберите чаты для мониторинга:**",
                     buttons=_build_chat_buttons(st["all_groups"], sel))
    await event.answer()


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
        "📝 Введите текст сообщения для ЛС:"
    )
    await event.answer()


@bot.on(New_Message(func=lambda e: e.sender_id in dm_setup_state and
                    dm_setup_state[e.sender_id].get("step") in
                    ("text", "interval", "delay_min", "delay_max", "photo")))
async def dm_dialog(event: callback_message) -> None:
    admin_id = event.sender_id
    st = dm_setup_state[admin_id]

    if st["step"] == "text":
        st["post_text"] = event.raw_text.strip()
        st["step"] = "interval"
        await event.respond(
            "⏱ **Интервал повтора** (минуты) — через сколько минут можно снова написать "
            "одному и тому же человеку:\n_(например: `60`)_"
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
        st["step"] = "delay_min"
        await event.respond(
            "🎲 **Минимальная задержка** между отправками разным людям (секунды):\n"
            "_(например: `30`)_"
        )
        return

    if st["step"] == "delay_min":
        try:
            val = int(event.raw_text.strip())
            if val < 5:
                raise ValueError
        except ValueError:
            await event.respond("⚠ Минимум 5 секунд.")
            return
        st["delay_min"] = val
        st["step"] = "delay_max"
        await event.respond(
            "🎲 **Максимальная задержка** между отправками (секунды):\n"
            "_(должна быть больше минимальной, например: `90`)_"
        )
        return

    if st["step"] == "delay_max":
        try:
            val = int(event.raw_text.strip())
            if val <= st["delay_min"]:
                raise ValueError
        except ValueError:
            await event.respond(f"⚠ Должно быть больше {st['delay_min']}.")
            return
        st["delay_max"] = val
        st["step"] = "photo"
        buttons = [
            [Button.inline("📸 Прикрепить фото", b"dm_photo_yes")],
            [Button.inline("❌ Без фото", b"dm_photo_no")],
        ]
        await event.respond("Хотите прикрепить фото?", buttons=buttons)
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


async def _save_and_launch(event, admin_id: int, st: dict) -> None:
    cursor = conn.cursor()
    cursor.execute(
        """INSERT INTO dm_tasks
           (admin_id, user_id, session_string, post_text, photo_url,
            interval_minutes, is_active, created_at, delay_min, delay_max)
           VALUES (?,?,?,?,?,?,1,?,?,?)""",
        (
            admin_id, st["user_id"], st["session_string"],
            st["post_text"], st.get("photo_url"),
            st["interval_minutes"], _now_iso(),
            st.get("delay_min", 30), st.get("delay_max", 90),
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
    _launch_monitor(task_id)

    await event.respond(
        f"🚀 **DM-задача #{task_id} запущена!**\n\n"
        f"👥 Чатов: {len(st['selected_chats'])}\n"
        f"⏱ Интервал повтора: {st['interval_minutes']} мин\n"
        f"🎲 Задержка: {st.get('delay_min', 30)}–{st.get('delay_max', 90)} сек (рандом)\n"
        f"📸 Фото: {'да' if st.get('photo_url') else 'нет'}\n\n"
        f"🔒 Закрытый ЛС → blacklist на 24ч\n\n"
        f"/dm_list — список | /dm_stop {task_id} — стоп"
    )


@bot.on(New_Message(pattern=r"/dm_list"))
async def cmd_dm_list(event: callback_message) -> None:
    if event.sender_id not in ADMIN_ID_LIST:
        return
    cursor = conn.cursor()
    cursor.execute(
        """SELECT id, user_id, interval_minutes, is_active, created_at, delay_min, delay_max,
                  (SELECT COUNT(*) FROM dm_watched_chats WHERE dm_task_id = dm_tasks.id),
                  (SELECT COUNT(*) FROM dm_sent_log WHERE dm_task_id = dm_tasks.id AND status='sent'),
                  (SELECT COUNT(*) FROM dm_sent_log WHERE dm_task_id = dm_tasks.id AND status='privacy')
           FROM dm_tasks ORDER BY id DESC"""
    )
    rows = cursor.fetchall()
    cursor.close()
    if not rows:
        await event.respond("📭 Нет DM-задач.")
        return
    lines = ["📋 **DM-задачи:**\n"]
    for tid, uid, interval, active, created, dmin, dmax, chats, sent, blocked in rows:
        running = tid in dm_monitor_tasks and not dm_monitor_tasks[tid].done()
        queue_size = len(dm_send_queues.get(tid, []))
        status = "🟢 активна" if active and running else ("🟡 не запущена" if active else "🔴 остановлена")
        lines.append(
            f"**#{tid}** | акк: {uid} | {status}\n"
            f"  Чатов: {chats} | ✅ отправлено: {sent} | 🔒 закрытых ЛС: {blocked}\n"
            f"  Интервал повтора: {interval} мин | Задержка: {dmin}–{dmax}с\n"
            f"  В очереди сейчас: {queue_size} чел.\n"
            f"  Создана: {(created or '')[:16]}"
        )
    await event.respond("\n\n".join(lines))


@bot.on(New_Message(pattern=r"/dm_stop(?:\s+(\d+))?"))
async def cmd_dm_stop(event: callback_message) -> None:
    if event.sender_id not in ADMIN_ID_LIST:
        return
    match = event.pattern_match.group(1)
    if not match:
        await event.respond("Использование: /dm_stop <id>\nСписок: /dm_list")
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
    t = dm_monitor_tasks.get(task_id)
    if t and not t.done():
        t.cancel()
    await event.respond(f"⛔ Задача #{task_id} остановлена.")
