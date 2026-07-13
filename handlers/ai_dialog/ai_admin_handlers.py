from __future__ import annotations
from services.menu_ui import render_menu

import os
import tempfile

from telethon import Button

from config import ADMIN_ID_LIST, New_Message, Query, bot, callback_message
from services.ai_dialog_service import (
    ai_stats,
    export_dialogs_text,
    recent_dialogs,
    resume_dialog_by_user,
    stop_dialog_by_user,
)
from services.first_message import get_templates_preview, reload_first_dm_templates_cache


@bot.on(New_Message(pattern=r"^/ai_status(?:@\w+)?$"))
async def cmd_ai_status(event: callback_message) -> None:
    if event.sender_id not in ADMIN_ID_LIST:
        return
    s = ai_stats()
    await render_menu(event, 
        "🤖 **AI DM статус**\n\n"
        f"AI включён: {'да' if s['enabled'] else 'нет'}\n"
        f"Dry-run: {'да' if s['dry_run'] else 'нет'}\n"
        f"Модель: `{s['model']}`\n"
        f"Диалогов всего: {s['total_dialogs']}\n"
        f"Активных диалогов: {s['active_dialogs']}\n"
        f"Диалогов сегодня: {s['dialogs_today']}"
        + (f" / лимит {s['daily_dialog_limit']}" if s.get('daily_dialog_limit') else "")
        + "\n"
        f"Сообщений сегодня: {s['messages_today']}\n\n"
        "Команды: /ai_dialogs, /ai_stop USER_ID, /ai_resume USER_ID, /ai_export"
    )


@bot.on(New_Message(pattern=r"^/ai_dialogs(?:@\w+)?(?:\s+(\d+))?$"))
async def cmd_ai_dialogs(event: callback_message) -> None:
    if event.sender_id not in ADMIN_ID_LIST:
        return
    raw_limit = event.pattern_match.group(1)
    limit = min(max(int(raw_limit or 10), 1), 30)
    rows = recent_dialogs(limit=limit)
    if not rows:
        await render_menu(event, "📭 AI-диалогов пока нет.")
        return
    lines = ["📋 **Последние AI-диалоги:**\n"]
    for uid, username, first_name, stage, status, count, updated in rows:
        who = f"@{username}" if username else (first_name or str(uid))
        lines.append(
            f"`{uid}` | {who}\n"
            f"стадия: `{stage}` | статус: `{status}` | AI-ответов: {count}\n"
            f"обновлён: {(updated or '')[:19]}"
        )
    await render_menu(event, "\n\n".join(lines))


@bot.on(New_Message(pattern=r"^/ai_stop(?:@\w+)?(?:\s+(\d+))?$"))
async def cmd_ai_stop(event: callback_message) -> None:
    if event.sender_id not in ADMIN_ID_LIST:
        return
    raw = event.pattern_match.group(1)
    if not raw:
        await event.respond("Использование: `/ai_stop USER_ID`")
        return
    ok = stop_dialog_by_user(int(raw))
    await event.respond("⛔ AI остановлен для пользователя." if ok else "⚠ Активный диалог не найден.")


@bot.on(New_Message(pattern=r"^/ai_resume(?:@\w+)?(?:\s+(\d+))?$"))
async def cmd_ai_resume(event: callback_message) -> None:
    if event.sender_id not in ADMIN_ID_LIST:
        return
    raw = event.pattern_match.group(1)
    if not raw:
        await event.respond("Использование: `/ai_resume USER_ID`")
        return
    ok = resume_dialog_by_user(int(raw))
    await event.respond("✅ AI снова активен для пользователя." if ok else "⚠ Диалог не найден.")


@bot.on(New_Message(pattern=r"^/ai_export(?:@\w+)?$"))
async def cmd_ai_export(event: callback_message) -> None:
    if event.sender_id not in ADMIN_ID_LIST:
        return
    text = export_dialogs_text(limit=500)
    fd, path = tempfile.mkstemp(prefix="ai_dialogs_", suffix=".txt")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            f.write(text)
        await event.respond("📤 Экспорт AI-диалогов:", file=path)
    finally:
        try:
            os.remove(path)
        except Exception:
            pass


@bot.on(New_Message(pattern=r"^/first_dm_templates(?:@\w+)?$"))
async def cmd_first_dm_templates(event: callback_message) -> None:
    if event.sender_id not in ADMIN_ID_LIST:
        return
    reload_first_dm_templates_cache()
    templates = get_templates_preview(limit=30)
    if not templates:
        await event.respond("⚠ Шаблоны первых сообщений не найдены.")
        return
    lines = ["💬 **Первые DM-шаблоны:**\n"]
    for i, item in enumerate(templates, start=1):
        lines.append(f"{i}. {item}")
    await event.respond("\n".join(lines))


# Главное меню — callback-обёртки над AI-командами.
@bot.on(Query(data=b"menu_ai_status"))
async def menu_ai_status(event):
    if event.sender_id not in ADMIN_ID_LIST:
        await event.answer("Недоступно", alert=True)
        return
    s = ai_stats()
    await render_menu(
        event,
        "🤖 **AI DM статус**\n\n"
        f"AI включён: {'да' if s['enabled'] else 'нет'}\n"
        f"Dry-run: {'да' if s['dry_run'] else 'нет'}\n"
        f"Модель: `{s['model']}`\n"
        f"Диалогов всего: {s['total_dialogs']}\n"
        f"Активных диалогов: {s['active_dialogs']}\n"
        f"Диалогов сегодня: {s['dialogs_today']}"
        + (f" / лимит {s['daily_dialog_limit']}" if s.get('daily_dialog_limit') else "")
        + "\n"
        f"Сообщений сегодня: {s['messages_today']}",
        buttons=[[Button.inline("🏠 Главное меню", b"menu_home")]],
    )
    await event.answer()


@bot.on(Query(data=b"menu_ai_dialogs"))
async def menu_ai_dialogs(event):
    if event.sender_id not in ADMIN_ID_LIST:
        await event.answer("Недоступно", alert=True)
        return
    rows = recent_dialogs(limit=10)
    if not rows:
        await render_menu(event, "📭 AI-диалогов пока нет.", buttons=[[Button.inline("🏠 Главное меню", b"menu_home")]])
        await event.answer()
        return

    lines = ["📋 **Последние AI-диалоги:**\n"]
    for uid, username, first_name, stage, status, count, updated in rows:
        who = f"@{username}" if username else (first_name or str(uid))
        lines.append(
            f"`{uid}` | {who}\n"
            f"стадия: `{stage}` | статус: `{status}` | AI-ответов: {count}\n"
            f"обновлён: {(updated or '')[:19]}"
        )
    await render_menu(event, "\n\n".join(lines), buttons=[[Button.inline("🏠 Главное меню", b"menu_home")]])
    await event.answer()


@bot.on(Query(data=b"menu_first_dm_templates"))
async def menu_first_dm_templates(event):
    if event.sender_id not in ADMIN_ID_LIST:
        await event.answer("Недоступно", alert=True)
        return
    reload_first_dm_templates_cache()
    templates = get_templates_preview(limit=30)
    if not templates:
        await render_menu(event, "⚠ Шаблоны первых сообщений не найдены.", buttons=[[Button.inline("🏠 Главное меню", b"menu_home")]])
        await event.answer()
        return
    lines = ["💬 **Первые DM-шаблоны:**\n"]
    for i, item in enumerate(templates, start=1):
        lines.append(f"{i}. {item}")
    await render_menu(event, "\n".join(lines), buttons=[[Button.inline("🏠 Главное меню", b"menu_home")]])
    await event.answer()
