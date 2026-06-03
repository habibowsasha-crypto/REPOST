import asyncio
import os
import sys
from datetime import datetime, timedelta
from math import ceil
from typing import Dict, Any, Optional, List

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger
from loguru import logger
from telethon import TelegramClient, events, Button
from telethon.errors import (
    FloodWaitError,
    SessionPasswordNeededError,
    PhoneCodeExpiredError,
    PhoneCodeInvalidError,
    ChatWriteForbiddenError,
    ChatAdminRequiredError,
    SlowModeWaitError,
)
from telethon.sessions import StringSession
from telethon.tl.types import Channel, Chat, PeerChannel, PeerChat

from config import settings
import db

logger.remove()
logger.add(sys.stderr, level=settings.log_level, format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level}</level> | {message}", colorize=True)
logger.add("logs/bot.log", level="DEBUG", rotation="10 MB", retention="14 days", compression="zip")

bot = TelegramClient("bot", settings.api_id, settings.api_hash)
scheduler = AsyncIOScheduler()
states: Dict[int, Dict[str, Any]] = {}
login_clients: Dict[int, TelegramClient] = {}


def is_admin(user_id: int) -> bool:
    return int(user_id) in settings.admin_id_list


async def guard(event) -> bool:
    if is_admin(event.sender_id):
        return True
    try:
        if isinstance(event, events.CallbackQuery.Event):
            await event.answer("⛔ Доступ запрещён", alert=True)
        else:
            await event.respond("⛔ Доступ запрещён")
    except Exception:
        pass
    return False


def main_menu_buttons():
    return [
        [Button.inline("➕ Добавить аккаунт", b"add_account"), Button.inline("👤 Аккаунты", b"accounts")],
        [Button.inline("📢 Новая рассылка", b"pick_account_broadcast")],
        [Button.inline("⏹ Остановить активные", b"stop_all")],
        [Button.inline("🕘 История", b"history"), Button.inline("⚙️ Настройки", b"settings")],
    ]


def account_buttons(account_user_id: int):
    return [
        [Button.inline("📥 Импортировать чаты", f"import_chats:{account_user_id}".encode())],
        [Button.inline("✅ Разрешённые чаты", f"chats:{account_user_id}:0".encode())],
        [Button.inline("➕ Добавить чат вручную", f"manual_chat:{account_user_id}".encode())],
        [Button.inline("📢 Новая рассылка", f"new_broadcast:{account_user_id}".encode())],
        [Button.inline("🗑 Удалить аккаунт", f"delete_account:{account_user_id}".encode())],
        [Button.inline("◀️ Назад", b"accounts")],
    ]


def short(text: Optional[str], limit: int = 260) -> str:
    text = text or ""
    text = text.strip()
    return text if len(text) <= limit else text[: limit - 3] + "..."


def get_account_title(row) -> str:
    username = f"@{row['username']}" if row["username"] else "без username"
    return f"{row['display_name']} ({username}) | ID {row['user_id']}"


def chat_title(row) -> str:
    allowed = "✅" if row["allowed"] else "⚪"
    uname = f" @{row['username']}" if row["username"] else ""
    return f"{allowed} {row['title']}{uname}"


def rate_limit_ok(target_count: int, interval_minutes: int) -> Optional[str]:
    if interval_minutes < settings.min_interval_minutes:
        return f"Минимальный интервал по SAFE_MODE: {settings.min_interval_minutes} мин."
    if target_count <= 0:
        return "Нет разрешённых чатов для рассылки."
    messages_per_hour = target_count * (60 / interval_minutes)
    if messages_per_hour > settings.max_messages_per_hour_per_account:
        min_interval = ceil(target_count * 60 / settings.max_messages_per_hour_per_account)
        return (
            f"Слишком частая рассылка: получится ~{messages_per_hour:.1f} сообщений/час.\n"
            f"Лимит: {settings.max_messages_per_hour_per_account} сообщений/час на аккаунт.\n"
            f"Для {target_count} чатов нужен интервал минимум {min_interval} мин."
        )
    return None


async def get_entity_for_chat(client: TelegramClient, chat_row):
    if chat_row["username"]:
        try:
            return await client.get_entity(f"@{chat_row['username']}")
        except Exception as exc:
            logger.debug(f"Не получилось получить entity по username @{chat_row['username']}: {exc}")
    chat_id = int(chat_row["chat_id"])
    try:
        return await client.get_entity(chat_id)
    except Exception:
        pass
    try:
        return await client.get_entity(PeerChannel(chat_id))
    except Exception:
        pass
    try:
        return await client.get_entity(PeerChat(chat_id))
    except Exception:
        pass
    return None


async def user_client(account_user_id: int) -> TelegramClient:
    account = db.get_account(account_user_id)
    if not account:
        raise RuntimeError("Аккаунт не найден")
    client = TelegramClient(StringSession(account["session_string"]), settings.api_id, settings.api_hash)
    await client.connect()
    if not await client.is_user_authorized():
        await client.disconnect()
        raise RuntimeError("Сессия аккаунта не авторизована")
    return client


async def send_to_targets(broadcast_id: int) -> Dict[str, int]:
    broadcast = db.get_broadcast(broadcast_id)
    if not broadcast:
        return {"ok": 0, "fail": 0}
    targets = db.get_broadcast_targets(broadcast_id)
    if not targets:
        db.mark_broadcast_status(broadcast_id, "stopped", "Нет целей рассылки")
        return {"ok": 0, "fail": 0}

    result = {"ok": 0, "fail": 0}
    client = await user_client(int(broadcast["account_user_id"]))
    try:
        for index, target in enumerate(targets, start=1):
            current = db.get_broadcast(broadcast_id)
            if not current or current["status"] != "active":
                break
            try:
                entity = await get_entity_for_chat(client, target)
                if entity is None:
                    raise RuntimeError("Telegram entity не найден. Аккаунт должен состоять в этом чате/канале.")

                text = broadcast["text"] or ""
                media_path = broadcast["media_path"]
                if media_path and os.path.exists(media_path):
                    await client.send_file(entity, media_path, caption=text or None)
                else:
                    await client.send_message(entity, text)

                db.add_history(broadcast_id, int(broadcast["account_user_id"]), int(target["chat_id"]), target["title"], "ok", short(text, 120))
                db.touch_broadcast_sent(broadcast_id)
                result["ok"] += 1
                logger.info(f"✅ Отправлено: broadcast={broadcast_id}, chat={target['title']}")
            except (ChatWriteForbiddenError, ChatAdminRequiredError) as exc:
                msg = f"Нет прав писать в чат: {exc}"
                db.add_history(broadcast_id, int(broadcast["account_user_id"]), int(target["chat_id"]), target["title"], "fail", short(broadcast["text"], 120), msg)
                result["fail"] += 1
                logger.warning(f"⚠️ {msg}")
            except (FloodWaitError, SlowModeWaitError) as exc:
                wait_seconds = int(getattr(exc, "seconds", 60))
                msg = f"Telegram попросил ждать {wait_seconds} сек. Рассылка остановлена безопасно."
                db.add_history(broadcast_id, int(broadcast["account_user_id"]), int(target["chat_id"]), target["title"], "fail", short(broadcast["text"], 120), msg)
                db.mark_broadcast_status(broadcast_id, "stopped", msg)
                job = scheduler.get_job(f"broadcast:{broadcast_id}")
                if job:
                    job.remove()
                result["fail"] += 1
                logger.warning(msg)
                break
            except Exception as exc:
                msg = f"Ошибка отправки: {type(exc).__name__}: {exc}"
                db.add_history(broadcast_id, int(broadcast["account_user_id"]), int(target["chat_id"]), target["title"], "fail", short(broadcast["text"], 120), msg)
                result["fail"] += 1
                logger.error(msg)

            if index < len(targets):
                await asyncio.sleep(max(settings.send_delay_seconds, 1))
    finally:
        await client.disconnect()
    return result


async def run_broadcast_job(broadcast_id: int) -> None:
    broadcast = db.get_broadcast(broadcast_id)
    if not broadcast or broadcast["status"] != "active":
        return
    result = await send_to_targets(broadcast_id)
    logger.info(f"Broadcast {broadcast_id}: ok={result['ok']}, fail={result['fail']}")
    if broadcast["mode"] == "once":
        db.mark_broadcast_status(broadcast_id, "completed")
        job = scheduler.get_job(f"broadcast:{broadcast_id}")
        if job:
            job.remove()


def schedule_recurring(broadcast_id: int, interval_minutes: int, first_run_seconds: int = 15) -> None:
    scheduler.add_job(
        run_broadcast_job,
        IntervalTrigger(minutes=interval_minutes),
        args=[broadcast_id],
        id=f"broadcast:{broadcast_id}",
        next_run_time=datetime.now() + timedelta(seconds=first_run_seconds),
        replace_existing=True,
        max_instances=1,
        coalesce=True,
    )


async def restore_active_jobs() -> None:
    for row in db.active_broadcasts():
        interval = int(row["interval_minutes"] or settings.min_interval_minutes)
        schedule_recurring(int(row["id"]), interval, first_run_seconds=30)
        logger.info(f"♻️ Восстановлена активная рассылка #{row['id']} с интервалом {interval} мин")


@bot.on(events.NewMessage(pattern=r"^/start$"))
async def start(event):
    if not await guard(event):
        return
    states.pop(event.sender_id, None)
    await event.respond(
        "👋 **Telegram Broadcast Manager**\n\n"
        "Режим: SAFE. Рассылка доступна только по разрешённым чатам, которые ты сам добавил в белый список.",
        buttons=main_menu_buttons(),
    )


@bot.on(events.NewMessage(pattern=r"^/cancel$"))
async def cancel(event):
    if not await guard(event):
        return
    states.pop(event.sender_id, None)
    client = login_clients.pop(event.sender_id, None)
    if client:
        await client.disconnect()
    await event.respond("✅ Действие отменено.", buttons=main_menu_buttons())


@bot.on(events.CallbackQuery(data=b"settings"))
async def settings_handler(event):
    if not await guard(event):
        return
    text = (
        "⚙️ **Текущие SAFE-настройки**\n\n"
        f"SAFE_MODE: `{settings.safe_mode}`\n"
        f"Минимальный интервал: `{settings.min_interval_minutes}` мин\n"
        f"Пауза между чатами: `{settings.send_delay_seconds}` сек\n"
        f"Макс. целей в одной рассылке: `{settings.max_targets_per_broadcast}`\n"
        f"Макс. сообщений/час на аккаунт: `{settings.max_messages_per_hour_per_account}`\n"
        f"База: `{settings.db_path}`\n\n"
        "Важно: эта версия не делает авто-вступления, не пишет в лички и не рассылает по неразрешённым чатам."
    )
    await event.respond(text, buttons=[[Button.inline("◀️ Меню", b"menu")]])


@bot.on(events.CallbackQuery(data=b"menu"))
async def menu_handler(event):
    if not await guard(event):
        return
    states.pop(event.sender_id, None)
    await event.respond("Главное меню:", buttons=main_menu_buttons())


@bot.on(events.CallbackQuery(data=b"add_account"))
async def add_account(event):
    if not await guard(event):
        return
    states[event.sender_id] = {"action": "login_phone"}
    await event.respond(
        "📲 Отправь номер Telegram-аккаунта в формате `+79999999999`.\n\n"
        "Лучше использовать отдельный рабочий аккаунт, а не основной.\n"
        "Отмена: /cancel"
    )


@bot.on(events.CallbackQuery(data=b"accounts"))
async def accounts_list(event):
    if not await guard(event):
        return
    rows = db.get_accounts()
    if not rows:
        await event.respond("Пока нет подключённых аккаунтов.", buttons=[[Button.inline("➕ Добавить аккаунт", b"add_account")], [Button.inline("◀️ Меню", b"menu")]])
        return
    buttons = [[Button.inline(get_account_title(row)[:60], f"account:{row['user_id']}".encode())] for row in rows]
    buttons.append([Button.inline("◀️ Меню", b"menu")])
    await event.respond("👤 **Подключённые аккаунты:**", buttons=buttons)


@bot.on(events.CallbackQuery(pattern=rb"^account:(\d+)$"))
async def account_menu(event):
    if not await guard(event):
        return
    account_user_id = int(event.pattern_match.group(1))
    row = db.get_account(account_user_id)
    if not row:
        await event.respond("Аккаунт не найден.")
        return
    allowed_count = len(db.list_chats(account_user_id, allowed=1))
    total_count = len(db.list_chats(account_user_id))
    await event.respond(
        f"👤 **Аккаунт**\n{get_account_title(row)}\n\n"
        f"Чатов в базе: `{total_count}`\n"
        f"Разрешено для рассылки: `{allowed_count}`",
        buttons=account_buttons(account_user_id),
    )


@bot.on(events.CallbackQuery(pattern=rb"^delete_account:(\d+)$"))
async def delete_account_confirm(event):
    if not await guard(event):
        return
    account_user_id = int(event.pattern_match.group(1))
    await event.respond(
        "⚠️ Удалить аккаунт из базы? Сессия, чаты и рассылки этого аккаунта будут удалены.",
        buttons=[
            [Button.inline("🗑 Да, удалить", f"delete_account_yes:{account_user_id}".encode())],
            [Button.inline("◀️ Назад", f"account:{account_user_id}".encode())],
        ],
    )


@bot.on(events.CallbackQuery(pattern=rb"^delete_account_yes:(\d+)$"))
async def delete_account_yes(event):
    if not await guard(event):
        return
    account_user_id = int(event.pattern_match.group(1))
    db.delete_account(account_user_id)
    await event.respond("✅ Аккаунт удалён.", buttons=main_menu_buttons())


@bot.on(events.CallbackQuery(pattern=rb"^import_chats:(\d+)$"))
async def import_chats(event):
    if not await guard(event):
        return
    account_user_id = int(event.pattern_match.group(1))
    await event.respond("⏳ Импортирую чаты аккаунта. Они будут добавлены как **выключенные**. Потом ты сам включишь нужные в белый список.")
    imported = 0
    skipped = 0
    client = await user_client(account_user_id)
    try:
        dialogs = await client.get_dialogs(limit=settings.max_import_chats)
        for dialog in dialogs:
            ent = dialog.entity
            if not isinstance(ent, (Channel, Chat)):
                skipped += 1
                continue
            if getattr(ent, "bot", False):
                skipped += 1
                continue

            chat_type = "group"
            if isinstance(ent, Channel):
                if ent.broadcast:
                    chat_type = "channel"
                elif ent.megagroup:
                    chat_type = "supergroup"
                else:
                    skipped += 1
                    continue

            title = getattr(ent, "title", dialog.name or str(ent.id)) or str(ent.id)
            username = getattr(ent, "username", None)
            db.upsert_chat(account_user_id, int(ent.id), title, username, chat_type, allowed=0)
            imported += 1
    finally:
        await client.disconnect()

    await event.respond(
        f"✅ Импорт завершён.\n\nДобавлено/обновлено: `{imported}`\nПропущено: `{skipped}`\n\n"
        "Теперь зайди в список чатов и включи только свои/разрешённые точки.",
        buttons=[[Button.inline("✅ Открыть чаты", f"chats:{account_user_id}:0".encode())], [Button.inline("◀️ Аккаунт", f"account:{account_user_id}".encode())]],
    )


@bot.on(events.CallbackQuery(pattern=rb"^manual_chat:(\d+)$"))
async def manual_chat(event):
    if not await guard(event):
        return
    account_user_id = int(event.pattern_match.group(1))
    states[event.sender_id] = {"action": "manual_chat", "account_user_id": account_user_id}
    await event.respond(
        "➕ Отправь @username чата/канала или его ID.\n\n"
        "Аккаунт-отправитель должен уже состоять в этом чате или иметь право постинга в канале.\n"
        "Этот чат сразу попадёт в белый список.\n\nОтмена: /cancel"
    )


async def send_chats_page(event, account_user_id: int, page: int) -> None:
    rows = db.list_chats(account_user_id)
    if not rows:
        await event.respond("Чатов пока нет. Импортируй чаты или добавь вручную.", buttons=[[Button.inline("📥 Импорт", f"import_chats:{account_user_id}".encode())], [Button.inline("◀️ Аккаунт", f"account:{account_user_id}".encode())]])
        return
    per_page = 8
    total_pages = max(1, ceil(len(rows) / per_page))
    page = max(0, min(page, total_pages - 1))
    chunk = rows[page * per_page : (page + 1) * per_page]
    buttons = []
    for row in chunk:
        buttons.append([Button.inline(chat_title(row)[:60], f"toggle_chat:{account_user_id}:{row['id']}:{page}".encode())])
    nav = []
    if page > 0:
        nav.append(Button.inline("⬅️", f"chats:{account_user_id}:{page-1}".encode()))
    nav.append(Button.inline(f"{page+1}/{total_pages}", b"noop"))
    if page < total_pages - 1:
        nav.append(Button.inline("➡️", f"chats:{account_user_id}:{page+1}".encode()))
    buttons.append(nav)
    buttons.append([Button.inline("◀️ Аккаунт", f"account:{account_user_id}".encode())])
    await event.respond(
        "✅ Нажми на чат, чтобы включить/выключить его для рассылки.\n"
        "Белый список - только чаты с ✅.",
        buttons=buttons,
    )


@bot.on(events.CallbackQuery(pattern=rb"^chats:(\d+):(\d+)$"))
async def chats_handler(event):
    if not await guard(event):
        return
    account_user_id = int(event.pattern_match.group(1))
    page = int(event.pattern_match.group(2))
    await send_chats_page(event, account_user_id, page)


@bot.on(events.CallbackQuery(data=b"noop"))
async def noop(event):
    if is_admin(event.sender_id):
        await event.answer("Навигация")


@bot.on(events.CallbackQuery(pattern=rb"^toggle_chat:(\d+):(\d+):(\d+)$"))
async def toggle_chat(event):
    if not await guard(event):
        return
    account_user_id = int(event.pattern_match.group(1))
    chat_db_id = int(event.pattern_match.group(2))
    page = int(event.pattern_match.group(3))
    row = db.get_chat(chat_db_id)
    if not row:
        await event.respond("Чат не найден.")
        return
    new_allowed = 0 if row["allowed"] else 1
    db.set_chat_allowed(chat_db_id, new_allowed)
    await event.answer("Включено" if new_allowed else "Выключено")
    await send_chats_page(event, account_user_id, page)


@bot.on(events.CallbackQuery(data=b"pick_account_broadcast"))
async def pick_account_broadcast(event):
    if not await guard(event):
        return
    rows = db.get_accounts()
    if not rows:
        await event.respond("Сначала добавь аккаунт-отправитель.", buttons=[[Button.inline("➕ Добавить аккаунт", b"add_account")]])
        return
    buttons = [[Button.inline(get_account_title(row)[:60], f"new_broadcast:{row['user_id']}".encode())] for row in rows]
    buttons.append([Button.inline("◀️ Меню", b"menu")])
    await event.respond("Выбери аккаунт для рассылки:", buttons=buttons)


@bot.on(events.CallbackQuery(pattern=rb"^new_broadcast:(\d+)$"))
async def new_broadcast(event):
    if not await guard(event):
        return
    account_user_id = int(event.pattern_match.group(1))
    targets = db.list_chats(account_user_id, allowed=1)
    if not targets:
        await event.respond(
            "❌ Нет разрешённых чатов. Сначала включи нужные чаты в белый список.",
            buttons=[[Button.inline("✅ Открыть чаты", f"chats:{account_user_id}:0".encode())], [Button.inline("◀️ Аккаунт", f"account:{account_user_id}".encode())]],
        )
        return
    if len(targets) > settings.max_targets_per_broadcast:
        await event.respond(
            f"❌ Сейчас разрешено {len(targets)} чатов, лимит одной рассылки: {settings.max_targets_per_broadcast}.\n"
            "Выключи лишние чаты или подними лимит осознанно в Variables.",
            buttons=[[Button.inline("✅ Открыть чаты", f"chats:{account_user_id}:0".encode())]],
        )
        return
    states[event.sender_id] = {"action": "broadcast_content", "account_user_id": account_user_id}
    await event.respond(
        "📝 Отправь текст рассылки.\n\n"
        "Можно отправить фото с подписью - тогда уйдёт фото + текст.\n"
        "Перед запуском я покажу превью и попрошу подтверждение.\n\nОтмена: /cancel"
    )


async def show_broadcast_preview(admin_id: int, event) -> None:
    st = states[admin_id]
    account_user_id = st["account_user_id"]
    targets = db.list_chats(account_user_id, allowed=1)
    target_lines = "\n".join([f"- {row['title']}" for row in targets[:15]])
    if len(targets) > 15:
        target_lines += f"\n...ещё {len(targets)-15}"
    content_type = "текст"
    if st.get("media_path") and st.get("text"):
        content_type = "фото + текст"
    elif st.get("media_path"):
        content_type = "только фото"

    text = (
        "📨 **Превью рассылки**\n\n"
        f"Тип: `{content_type}`\n"
        f"Целей: `{len(targets)}`\n\n"
        f"**Текст:**\n{short(st.get('text'), 900) or 'без текста'}\n\n"
        f"**Куда отправляем:**\n{target_lines}\n\n"
        "Выбери режим:"
    )
    buttons = [
        [Button.inline("🧪 Показать превью здесь", b"preview_here")],
        [Button.inline("📨 Разовая рассылка", b"broadcast_once")],
        [Button.inline("🔁 Повторяющаяся", b"broadcast_recurring")],
        [Button.inline("❌ Отменить", b"menu")],
    ]
    await event.respond(text, buttons=buttons)


@bot.on(events.CallbackQuery(data=b"preview_here"))
async def preview_here(event):
    if not await guard(event):
        return
    st = states.get(event.sender_id)
    if not st or st.get("action") not in {"broadcast_mode", "broadcast_interval"}:
        await event.respond("Сессия превью истекла. Начни заново.")
        return
    text = st.get("text") or ""
    media_path = st.get("media_path")
    if media_path and os.path.exists(media_path):
        await bot.send_file(event.sender_id, media_path, caption=text or None)
    else:
        await event.respond(text or "[только медиа отсутствует]")


@bot.on(events.CallbackQuery(data=b"broadcast_once"))
async def broadcast_once(event):
    if not await guard(event):
        return
    st = states.get(event.sender_id)
    if not st or st.get("action") != "broadcast_mode":
        await event.respond("Сессия рассылки истекла. Начни заново.")
        return
    st["mode"] = "once"
    await event.respond(
        "⚠️ Подтвердить **разовую** отправку по белому списку?",
        buttons=[[Button.inline("✅ Отправить один раз", b"confirm_broadcast")], [Button.inline("❌ Отмена", b"menu")]],
    )


@bot.on(events.CallbackQuery(data=b"broadcast_recurring"))
async def broadcast_recurring(event):
    if not await guard(event):
        return
    st = states.get(event.sender_id)
    if not st or st.get("action") != "broadcast_mode":
        await event.respond("Сессия рассылки истекла. Начни заново.")
        return
    st["action"] = "broadcast_interval"
    st["mode"] = "recurring"
    await event.respond(
        f"⏱ Укажи интервал в минутах. Минимум: `{settings.min_interval_minutes}` мин.\n\n"
        "Пример: `120`\nОтмена: /cancel"
    )


@bot.on(events.CallbackQuery(data=b"confirm_broadcast"))
async def confirm_broadcast(event):
    if not await guard(event):
        return
    st = states.get(event.sender_id)
    if not st or st.get("mode") not in {"once", "recurring"}:
        await event.respond("Сессия рассылки истекла. Начни заново.")
        return

    account_user_id = st["account_user_id"]
    targets = db.list_chats(account_user_id, allowed=1)
    if len(targets) > settings.max_targets_per_broadcast:
        await event.respond("❌ Превышен лимит целей. Рассылка не запущена.")
        return
    interval = st.get("interval_minutes")
    if st["mode"] == "recurring":
        issue = rate_limit_ok(len(targets), int(interval))
        if issue:
            await event.respond(f"❌ {issue}")
            return

    broadcast_id = db.create_broadcast(
        account_user_id=account_user_id,
        text=st.get("text"),
        media_path=st.get("media_path"),
        mode=st["mode"],
        interval_minutes=interval,
        chat_db_ids=[int(row["id"]) for row in targets],
        status="active",
    )

    if st["mode"] == "recurring":
        schedule_recurring(broadcast_id, int(interval), first_run_seconds=15)
        await event.respond(f"✅ Повторяющаяся рассылка #{broadcast_id} запущена. Первая отправка через ~15 секунд, далее каждые {interval} мин.", buttons=main_menu_buttons())
    else:
        await event.respond(f"🚀 Разовая рассылка #{broadcast_id} запущена. Отправляю по белому списку...")
        await run_broadcast_job(broadcast_id)
        await event.respond("✅ Разовая рассылка завершена. Проверь историю.", buttons=main_menu_buttons())
    states.pop(event.sender_id, None)


@bot.on(events.CallbackQuery(data=b"stop_all"))
async def stop_all(event):
    if not await guard(event):
        return
    stopped = 0
    for job in scheduler.get_jobs():
        if job.id.startswith("broadcast:"):
            broadcast_id = int(job.id.split(":", 1)[1])
            db.mark_broadcast_status(broadcast_id, "stopped", "Остановлено админом")
            job.remove()
            stopped += 1
    # На всякий случай останавливаем активные в БД, даже если job не восстановился
    for row in db.active_broadcasts():
        db.mark_broadcast_status(int(row["id"]), "stopped", "Остановлено админом")
        stopped += 1
    await event.respond(f"⛔ Остановлено активных рассылок: {stopped}", buttons=main_menu_buttons())


@bot.on(events.CallbackQuery(data=b"history"))
async def history(event):
    if not await guard(event):
        return
    rows = db.latest_history(15)
    if not rows:
        await event.respond("История пока пустая.", buttons=main_menu_buttons())
        return
    lines = ["🕘 **Последние отправки:**\n"]
    for row in rows:
        status = "✅" if row["status"] == "ok" else "❌"
        err = f"\nОшибка: `{short(row['error_reason'], 140)}`" if row["error_reason"] else ""
        lines.append(f"{status} `{row['sent_at']}` | {row['chat_title']}\n{short(row['message_preview'], 160)}{err}\n")
    await event.respond("\n".join(lines), buttons=main_menu_buttons())


@bot.on(events.NewMessage(func=lambda e: e.sender_id in states))
async def state_router(event):
    if not await guard(event):
        return
    if event.raw_text and event.raw_text.strip() in {"/start", "/cancel"}:
        return
    st = states.get(event.sender_id)
    if not st:
        return
    action = st.get("action")

    if action == "login_phone":
        phone = event.raw_text.strip()
        if not phone.startswith("+") or not phone[1:].replace(" ", "").isdigit():
            await event.respond("❌ Номер должен быть в формате `+79999999999`. Попробуй ещё раз или /cancel")
            return
        client = TelegramClient(StringSession(), settings.api_id, settings.api_hash)
        await client.connect()
        try:
            await client.send_code_request(phone)
            login_clients[event.sender_id] = client
            states[event.sender_id] = {"action": "login_code", "phone": phone}
            await event.respond("✅ Код отправлен в Telegram. Отправь код сюда одним сообщением.\nОтмена: /cancel")
        except FloodWaitError as exc:
            await client.disconnect()
            states.pop(event.sender_id, None)
            await event.respond(f"⚠️ Telegram просит подождать {exc.seconds} сек. Попробуй позже.", buttons=main_menu_buttons())
        except Exception as exc:
            await client.disconnect()
            states.pop(event.sender_id, None)
            await event.respond(f"❌ Ошибка отправки кода: {exc}", buttons=main_menu_buttons())
        return

    if action == "login_code":
        code = event.raw_text.replace(" ", "").strip()
        client = login_clients.get(event.sender_id)
        if not client:
            states.pop(event.sender_id, None)
            await event.respond("Сессия авторизации потеряна. Начни заново.")
            return
        try:
            await client.sign_in(st["phone"], code)
            me = await client.get_me()
            session_string = client.session.save()
            db.upsert_account(me.id, st["phone"], me.first_name or "Без имени", me.username, session_string)
            await client.disconnect()
            login_clients.pop(event.sender_id, None)
            states.pop(event.sender_id, None)
            await event.respond(f"✅ Аккаунт добавлен: {me.first_name or me.id}", buttons=main_menu_buttons())
        except SessionPasswordNeededError:
            states[event.sender_id] = {"action": "login_password", "phone": st["phone"]}
            await event.respond("🔐 На аккаунте включён 2FA. Отправь пароль.\nОтмена: /cancel")
        except PhoneCodeExpiredError:
            await event.respond("⏰ Код истёк. Нажми /cancel и добавь аккаунт заново.")
        except PhoneCodeInvalidError:
            await event.respond("❌ Неверный код. Проверь и отправь ещё раз.")
        except Exception as exc:
            await event.respond(f"❌ Ошибка входа: {exc}. Нажми /cancel и попробуй заново.")
        return

    if action == "login_password":
        password = event.raw_text.strip()
        client = login_clients.get(event.sender_id)
        if not client:
            states.pop(event.sender_id, None)
            await event.respond("Сессия авторизации потеряна. Начни заново.")
            return
        try:
            await client.sign_in(password=password)
            me = await client.get_me()
            session_string = client.session.save()
            db.upsert_account(me.id, st["phone"], me.first_name or "Без имени", me.username, session_string)
            await client.disconnect()
            login_clients.pop(event.sender_id, None)
            states.pop(event.sender_id, None)
            await event.respond(f"✅ Аккаунт добавлен: {me.first_name or me.id}", buttons=main_menu_buttons())
        except Exception as exc:
            await event.respond(f"❌ Ошибка 2FA: {exc}. Пароль можно отправить ещё раз или /cancel")
        return

    if action == "manual_chat":
        identifier = event.raw_text.strip()
        account_user_id = int(st["account_user_id"])
        client = await user_client(account_user_id)
        try:
            entity = await client.get_entity(int(identifier) if identifier.lstrip("-").isdigit() else identifier)
            if not isinstance(entity, (Channel, Chat)):
                await event.respond("❌ Это не группа и не канал. Отправь @username/ID группы или канала.")
                return
            chat_type = "group"
            if isinstance(entity, Channel):
                chat_type = "channel" if entity.broadcast else "supergroup"
            username = getattr(entity, "username", None)
            title = getattr(entity, "title", str(entity.id))
            db.upsert_chat(account_user_id, int(entity.id), title, username, chat_type, allowed=1)
            states.pop(event.sender_id, None)
            await event.respond(f"✅ Чат добавлен в белый список: {title}", buttons=[[Button.inline("✅ Открыть чаты", f"chats:{account_user_id}:0".encode())], [Button.inline("◀️ Аккаунт", f"account:{account_user_id}".encode())]])
        except Exception as exc:
            await event.respond(f"❌ Не получилось добавить чат: {exc}\nПроверь, что аккаунт состоит в этом чате/канале.")
        finally:
            await client.disconnect()
        return

    if action == "broadcast_content":
        account_user_id = int(st["account_user_id"])
        text = event.raw_text or ""
        media_path = None
        if event.media:
            try:
                media_path = await event.download_media(file=settings.media_dir)
            except Exception as exc:
                await event.respond(f"❌ Не получилось скачать медиа: {exc}")
                return
        if not text and not media_path:
            await event.respond("❌ Нужно отправить текст или фото с подписью.")
            return
        states[event.sender_id] = {
            "action": "broadcast_mode",
            "account_user_id": account_user_id,
            "text": text,
            "media_path": media_path,
        }
        await show_broadcast_preview(event.sender_id, event)
        return

    if action == "broadcast_interval":
        raw = event.raw_text.strip()
        try:
            interval = int(raw)
        except ValueError:
            await event.respond("❌ Нужно число минут. Например: `120`")
            return
        targets = db.list_chats(int(st["account_user_id"]), allowed=1)
        issue = rate_limit_ok(len(targets), interval)
        if issue:
            await event.respond(f"❌ {issue}\n\nВведи другой интервал или /cancel")
            return
        st["interval_minutes"] = interval
        await event.respond(
            f"⚠️ Подтвердить **повторяющуюся** рассылку каждые `{interval}` мин по `{len(targets)}` чатам?",
            buttons=[[Button.inline("✅ Запустить", b"confirm_broadcast")], [Button.inline("❌ Отмена", b"menu")]],
        )
        return


async def main() -> None:
    logger.info("Инициализация базы")
    db.init_db()
    logger.info("Запуск бота")
    await bot.start(bot_token=settings.bot_token)
    scheduler.start()
    await restore_active_jobs()
    me = await bot.get_me()
    logger.info(f"Бот запущен: @{me.username}")
    await bot.run_until_disconnected()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    finally:
        if scheduler.running:
            scheduler.shutdown(wait=False)
