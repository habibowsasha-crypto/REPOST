from services.menu_ui import render_menu
from services.admin_state import clear_admin_interaction_state, is_command_event
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
from telethon.tl.types import InputPeerChannel, InputPeerChat, PeerChannel, PeerChat, User

from config import (
    API_ID, API_HASH,
    bot, conn,
    New_Message, Query,
    callback_query, callback_message,
    ADMIN_ID_LIST, MEDIA_DIR,
)
from utils.database.database import create_dm_tables
from utils.telegram import gid_key
from services.first_message import choose_first_dm_text, is_random_first_dm_enabled
from services.ai_dialog_service import handle_private_incoming, record_first_dm
from services.dm_task_cleanup import (
    count_active_dm_tasks,
    count_inactive_dm_tasks,
    delete_inactive_dm_tasks,
)

# ─── состояние диалога настройки ──────────────────────────────────────────────
dm_setup_state: dict = {}

# ─── активные клиенты и задачи ────────────────────────────────────────────────
dm_monitor_clients: dict = {}   # task_id → TelegramClient
dm_monitor_tasks: dict = {}     # task_id → asyncio.Task

# ─── очереди отправки (рандомайзер) ───────────────────────────────────────────
# task_id → deque of (target_user_id, sender_obj, source_chat_title)
dm_send_queues: dict = {}


# ══════════════════════════════════════════════════════════════════════════════
# Вспомогательные функции
# ══════════════════════════════════════════════════════════════════════════════

def _now_iso() -> str:
    return datetime.datetime.utcnow().isoformat()


def _minutes_since(iso_ts: str) -> float:
    try:
        dt = datetime.datetime.fromisoformat(iso_ts)
    except (TypeError, ValueError):
        logger.warning(f"Некорректная дата в DM-логе: {iso_ts!r}")
        return float("inf")
    if dt.tzinfo is not None:
        now = datetime.datetime.now(datetime.timezone.utc)
        dt = dt.astimezone(datetime.timezone.utc)
    else:
        now = datetime.datetime.utcnow()
    return max(0.0, (now - dt).total_seconds() / 60)


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


async def _resolve_watched_chats(
    client: TelegramClient, user_id: int, chat_ids: list[int]
) -> list:
    """Resolve every available working group, including ordinary memberships.

    New databases use saved access_hash values. Older rows are resolved through
    the connected account session so tasks created before this patch keep working.
    """
    if not chat_ids:
        return []

    placeholders = ",".join("?" for _ in chat_ids)
    cursor = conn.cursor()
    try:
        discovered_rows = cursor.execute(
            f"""
            SELECT group_id, access_hash, peer_type, is_available
            FROM discovered_groups
            WHERE user_id = ? AND group_id IN ({placeholders})
            """,
            (user_id, *chat_ids),
        ).fetchall()
        legacy_rows = cursor.execute(
            f"""
            SELECT group_id, group_username
            FROM groups
            WHERE user_id = ? AND group_id IN ({placeholders})
            """,
            (user_id, *chat_ids),
        ).fetchall()
    finally:
        cursor.close()

    discovered = {int(row[0]): row[1:] for row in discovered_rows}
    identifiers = {int(group_id): identifier for group_id, identifier in legacy_rows}
    resolved = []
    seen_ids: set[int] = set()

    for raw_group_id in chat_ids:
        group_id = gid_key(raw_group_id)
        row = discovered.get(group_id)
        peer = None
        if row is not None:
            access_hash, peer_type, is_available = row
            if not is_available:
                logger.warning(f"Пропускаю недоступную группу {group_id}")
                continue
            if peer_type == "channel" and access_hash is not None:
                peer = InputPeerChannel(group_id, int(access_hash))
            elif peer_type == "chat":
                peer = InputPeerChat(group_id)

        if peer is None:
            identifier = identifiers.get(group_id)
            candidates = []
            if identifier:
                candidates.append(identifier)
            candidates.extend((PeerChannel(group_id), PeerChat(group_id)))
            for candidate in candidates:
                try:
                    peer = await client.get_input_entity(candidate)
                    break
                except Exception:
                    continue

        if peer is None:
            logger.warning(f"Не удалось восстановить Telegram peer для группы {group_id}")
            continue

        peer_key = int(getattr(peer, "channel_id", getattr(peer, "chat_id", group_id)))
        if peer_key in seen_ids:
            continue
        seen_ids.add(peer_key)
        resolved.append(peer)

    return resolved


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

        target_id, sender, source_chat_title = queue.popleft()

        # Повторная проверка перед отправкой
        if _is_blacklisted(task_id, target_id):
            logger.debug(f"[DM {task_id}] {target_id} в blacklist, пропуск")
            continue
        if _already_sent_recently(task_id, target_id, t["interval_minutes"]):
            logger.debug(f"[DM {task_id}] {target_id} недавно получал, пропуск")
            continue

        # Отправка
        try:
            outgoing_text = choose_first_dm_text(t["post_text"] or "") or (t["post_text"] or "Привет 👋")
            if t["photo_url"]:
                # Telegram ограничивает caption до 1024 символов
                if outgoing_text and len(outgoing_text) <= 1024:
                    await client.send_file(sender, t["photo_url"], caption=outgoing_text)
                else:
                    # Текст слишком длинный — фото и текст отдельно
                    await client.send_file(sender, t["photo_url"])
                    if outgoing_text:
                        await client.send_message(sender, outgoing_text)
            else:
                await client.send_message(sender, outgoing_text)

            _log_event(task_id, target_id, "sent")
            try:
                record_first_dm(
                    dm_task_id=task_id,
                    account_user_id=t["user_id"],
                    target=sender,
                    text=outgoing_text,
                    source_chat_title=source_chat_title,
                )
            except Exception as exc:
                # The Telegram message was already delivered. AI history failure must
                # not turn a successful send into a false delivery error.
                logger.error(f"[DM {task_id}] не удалось сохранить AI-диалог {target_id}: {exc}")
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
            queue.appendleft((target_id, sender, source_chat_title))  # вернуть в очередь
            continue

        except FloodWaitError as e:
            logger.warning(f"[DM {task_id}] ⏳ FloodWait {e.seconds}s")
            await asyncio.sleep(e.seconds)
            queue.appendleft((target_id, sender, source_chat_title))
            continue

        except Exception as exc:
            _log_event(task_id, target_id, "error")
            logger.error(f"[DM {task_id}] ❌ ошибка отправки {target_id}: {exc}")

        # Случайная задержка между отправками
        try:
            delay_min = max(0, int(t.get("delay_min") or 30))
            delay_max = max(delay_min, int(t.get("delay_max") or 90))
        except (TypeError, ValueError):
            delay_min, delay_max = 30, 90
        delay = random.randint(delay_min, delay_max)
        logger.debug(f"[DM {task_id}] пауза {delay}s перед следующей отправкой")
        await asyncio.sleep(delay)


# ══════════════════════════════════════════════════════════════════════════════
# Ядро мониторинга
# ══════════════════════════════════════════════════════════════════════════════

async def _monitor_loop(task_id: int) -> None:
    task = _get_task(task_id)
    if not task or not task["is_active"]:
        return

    client = TelegramClient(StringSession(task["session_string"]), API_ID, API_HASH)
    worker: Optional[asyncio.Task] = None

    try:
        await client.connect()
        if not await client.is_user_authorized():
            logger.error(f"[DM task {task_id}] сессия не авторизована")
            return

        watched_chat_ids = _get_watched_chats(task_id)
        watched_chats = await _resolve_watched_chats(client, task["user_id"], watched_chat_ids)
        if not watched_chats:
            logger.warning(f"[DM task {task_id}] нет доступных групп для мониторинга")
            return

        logger.info(f"[DM task {task_id}] запуск мониторинга, групп: {len(watched_chats)}")

        dm_monitor_clients[task_id] = client
        dm_send_queues[task_id] = deque()

        # Запускаем воркер отправки
        worker = asyncio.create_task(_send_worker(task_id, client), name=f"dm-send-{task_id}")

        @client.on(events.NewMessage(incoming=True))
        async def on_private_message(event):
            # AI-слой отвечает только на входящие личные сообщения от людей,
            # которым уже был отправлен первый DM и для которых есть active dialog.
            if not event.is_private:
                return
            try:
                sender = await event.get_sender()
            except Exception:
                return
            if not isinstance(sender, User):
                return
            if sender.bot or sender.is_self:
                return
            await handle_private_incoming(
                dm_task_id=task_id,
                account_user_id=task["user_id"],
                client=client,
                sender=sender,
                text=event.raw_text or "",
                message_id=getattr(event, "id", None),
            )

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
            if any(item[0] == target_id for item in queue):
                return

            source_chat_title = None
            try:
                source_chat = await event.get_chat()
                source_chat_title = getattr(source_chat, "title", None)
            except Exception as exc:
                logger.debug(
                    f"[DM {task_id}] не удалось получить название исходного чата "
                    f"для user={target_id}: {exc}"
                )

            queue.append((target_id, sender, source_chat_title))
            logger.debug(f"[DM {task_id}] добавлен в очередь: {target_id}, размер очереди: {len(queue)}")

        # Держим клиент живым
        while True:
            t = _get_task(task_id)
            if not t or not t["is_active"]:
                logger.info(f"[DM task {task_id}] задача остановлена")
                if worker and not worker.done():
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
        if worker and not worker.done():
            worker.cancel()
            try:
                await worker
            except asyncio.CancelledError:
                pass
            except Exception as exc:
                logger.debug(f"[DM task {task_id}] ошибка завершения worker: {exc}")
        try:
            await client.disconnect()
        except Exception as exc:
            logger.debug(f"[DM task {task_id}] ошибка отключения клиента: {exc}")
        dm_monitor_clients.pop(task_id, None)
        current = dm_monitor_tasks.get(task_id)
        if current is asyncio.current_task():
            dm_monitor_tasks.pop(task_id, None)
        dm_send_queues.pop(task_id, None)


def _launch_monitor(task_id: int) -> None:
    existing = dm_monitor_tasks.get(task_id)
    if existing and not existing.done():
        logger.debug(f"[DM task {task_id}] монитор уже запущен")
        return
    task = bot.loop.create_task(_monitor_loop(task_id), name=f"dm-monitor-{task_id}")
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

@bot.on(New_Message(pattern=r"^/dm_post(?:@\w+)?$"))
async def cmd_dm_post(event: callback_message) -> None:
    if event.sender_id not in ADMIN_ID_LIST:
        return
    cursor = conn.cursor()
    cursor.execute("SELECT user_id FROM sessions")
    sessions = cursor.fetchall()
    cursor.close()
    if not sessions:
        await render_menu(event, "⚠ Нет добавленных аккаунтов. Сначала добавьте через /start.", buttons=[[Button.inline("🏠 Главное меню", b"menu_home")]])
        return
    buttons = [
        [Button.inline(f"👤 Аккаунт #{uid}", f"dm_acc_{uid}".encode())]
        for (uid,) in sessions
    ]
    buttons.append([Button.inline("🏠 Главное меню", b"menu_home")])
    await render_menu(event, "📩 **DM Автопостер**\n\nВыберите аккаунт:", buttons=buttons)


@bot.on(Query(data=lambda d: d.decode().startswith("dm_acc_")))
async def dm_pick_account(event: callback_query) -> None:
    if event.sender_id not in ADMIN_ID_LIST:
        return
    admin_id = event.sender_id
    await clear_admin_interaction_state(admin_id)
    user_id = int(event.data.decode().split("_")[2])
    cursor = conn.cursor()
    cursor.execute("SELECT session_string FROM sessions WHERE user_id = ?", (user_id,))
    row = cursor.fetchone()
    cursor.execute(
        """
        SELECT
            g.group_id,
            COALESCE(d.title, d.username, g.group_username, CAST(g.group_id AS TEXT))
        FROM groups AS g
        LEFT JOIN discovered_groups AS d
          ON d.user_id = g.user_id AND d.group_id = g.group_id
        WHERE g.user_id = ?
          AND COALESCE(d.is_available, 1) = 1
        ORDER BY lower(COALESCE(d.title, d.username, g.group_username, CAST(g.group_id AS TEXT)))
        """,
        (user_id,),
    )
    groups = cursor.fetchall()
    cursor.close()
    if not row:
        await render_menu(event, "⚠ Сессия не найдена.")
        return
    if not groups:
        await render_menu(event, "⚠ Нет доступных групп. Откройте аккаунт и нажмите «Найти группы аккаунта».", buttons=[[Button.inline("🔎 Найти группы", f"sync_groups_{user_id}".encode()), Button.inline("🏠 Меню", b"menu_home")]])
        return
    dm_setup_state[admin_id] = {
        "step": "pick_chats",
        "user_id": user_id,
        "session_string": row[0],
        "selected_chats": [],
        "all_groups": groups,
    }
    await render_menu(event, 
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
    if event.sender_id not in ADMIN_ID_LIST:
        return
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
    if event.sender_id not in ADMIN_ID_LIST:
        return
    admin_id = event.sender_id
    st = dm_setup_state.get(admin_id)
    if not st or st["step"] != "pick_chats":
        await event.answer("Начните заново через /dm_post")
        return
    if not st["selected_chats"]:
        await event.answer("⚠ Выберите хотя бы один чат!", alert=True)
        return
    if is_random_first_dm_enabled():
        # В случайном режиме ручной текст не нужен: при каждой отправке
        # choose_first_dm_text() выберет один из встроенных/пользовательских шаблонов.
        st["post_text"] = ""
        st["step"] = "interval"
        await render_menu(
            event,
            f"✅ Выбрано чатов: {len(st['selected_chats'])}\n\n"
            "🎲 Случайное первое сообщение включено. Ручной текст вводить не нужно.\n\n"
            "⏱ **Интервал повтора** (минуты) — через сколько минут можно снова написать "
            "одному и тому же человеку:\n_(например: `60`)_",
        )
    else:
        st["step"] = "text"
        await render_menu(
            event,
            f"✅ Выбрано чатов: {len(st['selected_chats'])}\n\n"
            "📝 Введите текст сообщения для ЛС:",
        )
    await event.answer()


@bot.on(New_Message(func=lambda e: e.sender_id in dm_setup_state and
                    not is_command_event(e) and
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
            photo_path = await event.download_media(file=MEDIA_DIR)
            st["photo_url"] = photo_path
            await _save_and_launch(event, admin_id, st)
        else:
            await event.respond("⚠ Отправьте фото или нажмите «Без фото».")


@bot.on(Query(data=b"dm_photo_yes"))
async def dm_photo_yes(event: callback_query) -> None:
    if event.sender_id not in ADMIN_ID_LIST:
        return
    admin_id = event.sender_id
    st = dm_setup_state.get(admin_id)
    if not st:
        return
    st["step"] = "photo"
    await render_menu(event, "📸 Отправьте фото:")
    await event.answer()


@bot.on(Query(data=b"dm_photo_no"))
async def dm_photo_no(event: callback_query) -> None:
    if event.sender_id not in ADMIN_ID_LIST:
        return
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
            "INSERT OR IGNORE INTO dm_watched_chats (dm_task_id, chat_id) VALUES (?,?)",
            (task_id, chat_id),
        )
    conn.commit()
    cursor.close()
    dm_setup_state.pop(admin_id, None)
    _launch_monitor(task_id)

    await render_menu(event, 
        f"🚀 **DM-задача #{task_id} запущена!**\n\n"
        f"👥 Чатов: {len(st['selected_chats'])}\n"
        f"⏱ Интервал повтора: {st['interval_minutes']} мин\n"
        f"🎲 Задержка: {st.get('delay_min', 30)}–{st.get('delay_max', 90)} сек (рандом)\n"
        f"📸 Фото: {'да' if st.get('photo_url') else 'нет'}\n\n"
        f"🔒 Закрытый ЛС → blacklist на 24ч\n\n"
        f"/dm_list — список | /dm_stop {task_id} — стоп"
    )


@bot.on(New_Message(pattern=r"^/dm_list(?:@\w+)?$"))
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
        await render_menu(event, "📭 Нет DM-задач.", buttons=[[Button.inline("🏠 Главное меню", b"menu_home")]])
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
    # Считаем напрямую из БД и всегда показываем кнопку очистки.
    # Так кнопка не зависит от типа значения is_active в уже существующей базе
    # и остаётся видимой даже когда неактуальных задач временно нет.
    inactive_count = count_inactive_dm_tasks(conn)
    lines.insert(1, f"🧹 Неактуальных задач: **{inactive_count}**")
    buttons = [
        [
            Button.inline(
                f"🧹 Очистить неактуальные ({inactive_count})",
                b"menu_dm_cleanup",
            )
        ],
        [Button.inline("🔄 Обновить список", b"menu_dm_list")],
        [Button.inline("🏠 Главное меню", b"menu_home")],
    ]
    await render_menu(event, "\n\n".join(lines), buttons=buttons)


async def _show_dm_cleanup_confirmation(event) -> None:
    """Show the same cleanup confirmation for callback and slash command."""
    inactive_count = count_inactive_dm_tasks(conn)

    if inactive_count <= 0:
        await render_menu(
            event,
            "📭 Неактуальных DM-задач для очистки нет.",
            buttons=[
                [Button.inline("📋 Вернуться к DM-задачам", b"menu_dm_list")],
                [Button.inline("🏠 Главное меню", b"menu_home")],
            ],
        )
        return

    await render_menu(
        event,
        f"🧹 **Очистить неактуальные DM-задачи?**\n\n"
        f"Будут удалены только остановленные задачи: **{inactive_count}**.\n"
        "Активные задачи, история AI-диалогов и аккаунты не затрагиваются.",
        buttons=[
            [Button.inline("✅ Да, очистить", b"menu_dm_cleanup_confirm")],
            [Button.inline("❌ Отмена", b"menu_dm_list")],
        ],
    )


@bot.on(Query(data=b"menu_dm_cleanup"))
async def menu_dm_cleanup(event: callback_query) -> None:
    """Ask for confirmation before removing stopped DM tasks."""
    if event.sender_id not in ADMIN_ID_LIST:
        await event.answer("Недоступно", alert=True)
        return

    await _show_dm_cleanup_confirmation(event)
    await event.answer()


@bot.on(New_Message(pattern=r"^/dm_cleanup(?:@\w+)?$"))
async def cmd_dm_cleanup(event: callback_message) -> None:
    """Slash-command fallback for opening the stopped-task cleanup dialog."""
    if event.sender_id not in ADMIN_ID_LIST:
        return
    await _show_dm_cleanup_confirmation(event)


@bot.on(Query(data=b"menu_dm_cleanup_confirm"))
async def menu_dm_cleanup_confirm(event: callback_query) -> None:
    """Delete only stopped DM tasks and their task-local technical rows."""
    if event.sender_id not in ADMIN_ID_LIST:
        await event.answer("Недоступно", alert=True)
        return

    task_ids = delete_inactive_dm_tasks(conn)
    deleted_count = len(task_ids)

    # Remove only stale in-memory references belonging to deleted tasks.
    for task_id in task_ids:
        running_task = dm_monitor_tasks.pop(task_id, None)
        if running_task and not running_task.done():
            running_task.cancel()
        stale_client = dm_monitor_clients.pop(task_id, None)
        if stale_client is not None:
            try:
                await stale_client.disconnect()
            except Exception as exc:
                logger.warning(
                    f"Не удалось отключить клиент удалённой DM-задачи #{task_id}: {exc}"
                )
        dm_send_queues.pop(task_id, None)

    active_count = count_active_dm_tasks(conn)

    await render_menu(
        event,
        f"✅ Неактуальные DM-задачи очищены.\n\n"
        f"Удалено: **{deleted_count}**\n"
        f"Активных задач осталось: **{active_count}**",
        buttons=[
            [Button.inline("📋 Открыть DM-задачи", b"menu_dm_list")],
            [Button.inline("🏠 Главное меню", b"menu_home")],
        ],
    )
    await event.answer("Очищено")


@bot.on(New_Message(pattern=r"^/dm_stop(?:@\w+)?(?:\s+(\d+))?$"))
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


# ══════════════════════════════════════════════════════════════════════════════
# Главное меню — callback-обёртки над DM-командами
# ══════════════════════════════════════════════════════════════════════════════

@bot.on(Query(data=b"menu_dm_post"))
async def menu_dm_post(event: callback_query) -> None:
    if event.sender_id not in ADMIN_ID_LIST:
        await event.answer("Недоступно", alert=True)
        return
    from services.admin_state import clear_admin_interaction_state

    await clear_admin_interaction_state(event.sender_id)
    await cmd_dm_post(event)
    await event.answer()


@bot.on(Query(data=b"menu_dm_list"))
async def menu_dm_list(event: callback_query) -> None:
    if event.sender_id not in ADMIN_ID_LIST:
        await event.answer("Недоступно", alert=True)
        return
    await cmd_dm_list(event)
    await event.answer()


@bot.on(Query(data=b"menu_dm_stop"))
async def menu_dm_stop(event: callback_query) -> None:
    if event.sender_id not in ADMIN_ID_LIST:
        await event.answer("Недоступно", alert=True)
        return

    cursor = conn.cursor()
    cursor.execute(
        "SELECT id, user_id FROM dm_tasks WHERE is_active = 1 ORDER BY id DESC LIMIT 50"
    )
    rows = cursor.fetchall()
    cursor.close()

    if not rows:
        await render_menu(event, "📭 Активных DM-задач нет.", buttons=[[Button.inline("🏠 Главное меню", b"menu_home")]])
        await event.answer()
        return

    buttons = [
        [Button.inline(f"⛔ Остановить #{task_id} | аккаунт {user_id}", f"menu_dm_stop_{task_id}".encode())]
        for task_id, user_id in rows
    ]
    buttons.append([Button.inline("🏠 Главное меню", b"menu_home")])
    await render_menu(event, "🛑 Выберите DM-задачу для остановки:", buttons=buttons)
    await event.answer()


@bot.on(Query(data=lambda d: d.decode().startswith("menu_dm_stop_")))
async def menu_dm_stop_selected(event: callback_query) -> None:
    if event.sender_id not in ADMIN_ID_LIST:
        await event.answer("Недоступно", alert=True)
        return

    try:
        task_id = int(event.data.decode().rsplit("_", 1)[1])
    except (ValueError, IndexError):
        await event.answer("Некорректный ID задачи", alert=True)
        return

    cursor = conn.cursor()
    cursor.execute("UPDATE dm_tasks SET is_active = 0 WHERE id = ? AND is_active = 1", (task_id,))
    affected = cursor.rowcount
    conn.commit()
    cursor.close()

    if not affected:
        await event.answer("Задача уже остановлена или не найдена", alert=True)
        return

    running_task = dm_monitor_tasks.get(task_id)
    if running_task and not running_task.done():
        running_task.cancel()

    await render_menu(event, f"⛔ DM-задача #{task_id} остановлена.", buttons=[[Button.inline("🏠 Главное меню", b"menu_home")]])
    await event.answer("Остановлено")
