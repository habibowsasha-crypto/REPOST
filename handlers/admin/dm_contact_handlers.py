from __future__ import annotations

import html
import math
from typing import Optional

from telethon import Button

from config import ADMIN_ID_LIST, Query, bot, callback_query
from services.dm_contact_analytics import (
    chat_rows,
    chat_stats,
    clear_completed_for_chat,
    dialog_timeout_settings,
    overall_stats,
)
from services.menu_ui import render_menu

_PAGE_SIZE = 8


def _safe_callback_int(data: bytes, prefix: str) -> Optional[int]:
    try:
        value = data.decode("utf-8", errors="strict")
    except UnicodeDecodeError:
        return None
    if not value.startswith(prefix):
        return None
    try:
        return int(value[len(prefix) :])
    except (TypeError, ValueError):
        return None


def _short_button_title(title: str, limit: int = 34) -> str:
    compact = " ".join((title or "").replace("\n", " ").split()).strip() or "Без названия"
    return compact if len(compact) <= limit else compact[: limit - 1].rstrip() + "…"


def _stats_text(stats: dict[str, int]) -> str:
    timeouts = dialog_timeout_settings()
    return (
        "📊 <b>Контакты и диалоги</b>\n\n"
        f"👀 Уникальных людей замечено в чатах: <b>{stats['seen_recipients']}</b>\n"
        f"👤 Уникальных получателей первого DM: <b>{stats['unique_recipients']}</b>\n"
        f"✉️ Первых DM отправлено всего: <b>{stats['first_dms']}</b>\n"
        f"💬 Ответили хотя бы раз: <b>{stats['replied']}</b>\n"
        f"🔗 Диалогов со ссылкой: <b>{stats['link_sent']}</b>\n"
        f"✅ Уникальных людей с завершённым диалогом: "
        f"<b>{stats['completed_unique_people']}</b>\n"
        f"👥 Завершённых пар «аккаунт + пользователь»: "
        f"<b>{stats['completed_account_contacts']}</b>\n"
        f"🧾 Завершённых циклов за всё время: <b>{stats['completed']}</b>\n"
        f"🔒 Сейчас защищены от повторного DM: <b>{stats['blocked_records']}</b>\n"
        f"🚫 В постоянном списке «не писать»: <b>{stats['opted_out']}</b>\n"
        f"⏳ Активных диалогов: <b>{stats['active']}</b>\n"
        f"🕓 Ожидают первого ответа: <b>{stats['waiting']}</b>\n"
        f"😶 Закрыто без ответа через {timeouts['before_link_hours']} ч.: "
        f"<b>{stats['abandoned']}</b>\n\n"
        "Ниже — чаты, в которых подключённые аккаунты замечали пользователей."
    )


def _chat_buttons(page: int) -> tuple[list[list[Button]], int, int]:
    rows = chat_rows()
    pages = max(1, math.ceil(len(rows) / _PAGE_SIZE))
    page = max(0, min(int(page), pages - 1))
    start = page * _PAGE_SIZE
    selected = rows[start : start + _PAGE_SIZE]

    buttons: list[list[Button]] = []
    for chat_id, title, first_dm_count, seen_count in selected:
        label = (
            f"💬 {_short_button_title(title)} · DM {first_dm_count} · 👀 {seen_count}"
        )
        buttons.append(
            [Button.inline(label, f"dmstats_chat_{int(chat_id)}".encode("utf-8"))]
        )

    if pages > 1:
        nav: list[Button] = []
        if page > 0:
            nav.append(Button.inline("⬅️", f"dmstats_page_{page - 1}".encode("utf-8")))
        nav.append(Button.inline(f"{page + 1}/{pages}", f"dmstats_page_{page}".encode("utf-8")))
        if page + 1 < pages:
            nav.append(Button.inline("➡️", f"dmstats_page_{page + 1}".encode("utf-8")))
        buttons.append(nav)

    buttons.append(
        [
            Button.inline("🔄 Обновить", f"dmstats_page_{page}".encode("utf-8")),
            Button.inline("🏠 Главное меню", b"menu_home"),
        ]
    )
    return buttons, page, pages


async def _show_contacts_page(event: callback_query, page: int = 0) -> None:
    buttons, normalized_page, pages = _chat_buttons(page)
    text = _stats_text(overall_stats())
    if pages > 1:
        text += f"\n\nСтраница чатов: <b>{normalized_page + 1}/{pages}</b>"
    await render_menu(event, text, buttons=buttons, parse_mode="html")


@bot.on(Query(data=b"menu_dm_contacts"))
async def dm_contacts_menu(event: callback_query) -> None:
    if event.sender_id not in ADMIN_ID_LIST:
        await event.answer("Недоступно", alert=True)
        return
    await _show_contacts_page(event, 0)
    await event.answer()


@bot.on(Query(data=lambda d: d.decode(errors="ignore").startswith("dmstats_page_")))
async def dm_contacts_page(event: callback_query) -> None:
    if event.sender_id not in ADMIN_ID_LIST:
        await event.answer("Недоступно", alert=True)
        return
    page = _safe_callback_int(event.data, "dmstats_page_")
    if page is None:
        await event.answer("Некорректная страница", alert=True)
        return
    await _show_contacts_page(event, page)
    await event.answer()


@bot.on(Query(data=lambda d: d.decode(errors="ignore").startswith("dmstats_chat_")))
async def dm_contacts_chat(event: callback_query) -> None:
    if event.sender_id not in ADMIN_ID_LIST:
        await event.answer("Недоступно", alert=True)
        return
    chat_id = _safe_callback_int(event.data, "dmstats_chat_")
    if chat_id is None:
        await event.answer("Некорректный ID чата", alert=True)
        return

    stats = chat_stats(chat_id)
    timeouts = dialog_timeout_settings()
    title = html.escape(str(stats["title"]))
    text = (
        f"📊 <b>{title}</b>\n\n"
        f"👀 Уникальных людей замечено в чате: <b>{stats['seen_recipients']}</b>\n"
        f"👤 Получили первый DM именно из этого чата: "
        f"<b>{stats['unique_recipients']}</b>\n"
        f"✉️ Первых DM всего: <b>{stats['first_dms']}</b>\n"
        f"💬 Ответили: <b>{stats['replied']}</b>\n"
        f"🔗 Получили ссылку: <b>{stats['link_sent']}</b>\n"
        f"✅ Уникальных людей с завершённым диалогом: "
        f"<b>{stats['completed_unique_people']}</b>\n"
        f"👥 Завершённых пар «аккаунт + пользователь»: "
        f"<b>{stats['completed_account_contacts']}</b>\n"
        f"🧾 Завершённых циклов за всё время: <b>{stats['completed']}</b>\n"
        f"🚫 Попросили больше не писать: <b>{stats['opted_out']}</b>\n"
        f"⏳ Активно: <b>{stats['active']}</b>\n"
        f"🕓 Ожидают ответа: <b>{stats['waiting']}</b>\n"
        f"😶 Закрыто без ответа через {timeouts['before_link_hours']} ч.: "
        f"<b>{stats['abandoned']}</b>\n\n"
        f"🔒 Сейчас запрещают повторный первый DM: "
        f"<b>{stats['blocked_records']}</b>"
    )
    buttons = [
        [
            Button.inline(
                "🧹 Разрешить повторный контакт",
                f"dmstats_clear_ask_{chat_id}".encode("utf-8"),
            )
        ],
        [
            Button.inline("◀️ К списку чатов", b"menu_dm_contacts"),
            Button.inline("🏠 Главное меню", b"menu_home"),
        ],
    ]
    await render_menu(event, text, buttons=buttons, parse_mode="html")
    await event.answer()


@bot.on(Query(data=lambda d: d.decode(errors="ignore").startswith("dmstats_clear_ask_")))
async def dm_contacts_clear_ask(event: callback_query) -> None:
    if event.sender_id not in ADMIN_ID_LIST:
        await event.answer("Недоступно", alert=True)
        return
    chat_id = _safe_callback_int(event.data, "dmstats_clear_ask_")
    if chat_id is None:
        await event.answer("Некорректный ID чата", alert=True)
        return

    stats = chat_stats(chat_id)
    title = html.escape(str(stats["title"]))
    text = (
        f"⚠️ <b>Разрешить повторный контакт: {title}?</b>\n\n"
        f"Будут удалены <b>{stats['blocked_records']}</b> действующих записей "
        "о завершённых контактах, которые сейчас привязаны к этому чату.\n\n"
        "После очистки бот никому сразу не напишет и старую очередь не восстановит. "
        "Пользователь сможет получить новый первый DM от того же аккаунта только "
        "после своего нового сообщения в любом чате, который отслеживает этот аккаунт.\n\n"
        "Постоянный список «не писать» не изменится. Историческая статистика тоже сохранится."
    )
    buttons = [
        [
            Button.inline(
                "✅ Подтвердить",
                f"dmstats_clear_yes_{chat_id}".encode("utf-8"),
            ),
            Button.inline(
                "❌ Отмена", f"dmstats_chat_{chat_id}".encode("utf-8")
            ),
        ]
    ]
    await render_menu(event, text, buttons=buttons, parse_mode="html")
    await event.answer()


@bot.on(Query(data=lambda d: d.decode(errors="ignore").startswith("dmstats_clear_yes_")))
async def dm_contacts_clear_yes(event: callback_query) -> None:
    if event.sender_id not in ADMIN_ID_LIST:
        await event.answer("Недоступно", alert=True)
        return
    chat_id = _safe_callback_int(event.data, "dmstats_clear_yes_")
    if chat_id is None:
        await event.answer("Некорректный ID чата", alert=True)
        return

    affected = clear_completed_for_chat(chat_id)
    stats = chat_stats(chat_id)
    title = html.escape(str(stats["title"]))
    text = (
        f"✅ Готово. Снята защита от повторного контакта для "
        f"<b>{affected}</b> записей из чата <b>{title}</b>.\n\n"
        "Никому не отправлено сообщение. Новый первый DM возможен только после "
        "нового сообщения пользователя в отслеживаемом чате. Постоянный opt-out не изменён."
    )
    buttons = [
        [
            Button.inline(
                "◀️ К статистике чата",
                f"dmstats_chat_{chat_id}".encode("utf-8"),
            ),
            Button.inline("🏠 Меню", b"menu_home"),
        ]
    ]
    await render_menu(event, text, buttons=buttons, parse_mode="html")
    await event.answer("Готово")
