"""Admin dialog overview without per-message notifications."""

from __future__ import annotations

import datetime as dt

from telethon import events

from config import bot, is_admin
from services import accounts as accounts_svc
from services import dialog_store as dialog_store_svc
from services.menu_ui import render_menu
from services.ui import DENIED, back_home_row, btn, join, screen


_STAGE_LABELS = {
    dialog_store_svc.STAGE_FIRST_DM_SENDING: "готовится First DM",
    dialog_store_svc.STAGE_WAITING_REPLY: "ждём ответа",
    dialog_store_svc.STAGE_FOLLOWUP_SENT: "follow-up отправлен",
    dialog_store_svc.STAGE_ENGAGED: "диалог начат",
    dialog_store_svc.STAGE_EXPLAINED: "ждёт ссылки",
    dialog_store_svc.STAGE_LINK_SENT: "ссылка отправлена",
    dialog_store_svc.STAGE_PROMO_SENT: "реклама отправлена",
    dialog_store_svc.STAGE_APOLOGY_SENT: "извинение отправлено",
    dialog_store_svc.STAGE_LINK_HELP_SENT: "инструкция отправлена",
    dialog_store_svc.STAGE_CLOSED: "завершён",
}


def _target_label(row: dict) -> str:
    username = str(row.get("username") or "").strip().lstrip("@")
    if username:
        return f"@{username}"
    name = " ".join(
        part for part in (
            str(row.get("first_name") or "").strip(),
            str(row.get("last_name") or "").strip(),
        ) if part
    )
    return name or f"id {int(row['target_user_id'])}"


def _date(value: str | None) -> str:
    if not value:
        return "-"
    try:
        parsed = dt.datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=dt.timezone.utc)
        parsed = parsed.astimezone(dt.timezone(dt.timedelta(hours=3)))
        return parsed.strftime("%d.%m · %H:%M МСК")
    except (TypeError, ValueError):
        return str(value)[:16].replace("T", " · ")


def _row_line(row: dict) -> str:
    target = _target_label(row)
    account = accounts_svc.get_account(int(row["account_user_id"]))
    account_label = (
        accounts_svc.format_account_label(account, include_id=False)
        if account
        else f"id {int(row['account_user_id'])}"
    )
    stage = _STAGE_LABELS.get(str(row.get("stage")), str(row.get("stage")))
    stamp = row.get("lifecycle_completed_at") or row.get("updated_at")
    return (
        f"• **{target}** · {stage}\n"
        f"  └ {account_label} · {_date(stamp)}"
    )


def _summary_text(*, show_closed: bool = False) -> str:
    active = dialog_store_svc.count_active()
    waiting = dialog_store_svc.count_by_stage(
        dialog_store_svc.STAGE_WAITING_REPLY,
    )
    link_wait = dialog_store_svc.count_by_stage(dialog_store_svc.STAGE_EXPLAINED)
    closed_today = dialog_store_svc.count_closed_today()
    rows = (
        dialog_store_svc.list_recent_closed(limit=10)
        if show_closed
        else dialog_store_svc.list_recent(active_only=True, limit=10)
    )
    listing = "\n\n".join(_row_line(row) for row in rows) or "Записей пока нет."
    title = "Завершённые диалоги" if show_closed else "Диалоги"
    return screen(
        "💬",
        title,
        join(
            f"├ Активные: **{active}**",
            f"├ Ждут ответа: **{waiting}**",
            f"├ Ждут ссылки: **{link_wait}**",
            f"└ Завершено сегодня: **{closed_today}**",
        ),
        "**Последние записи**",
        listing,
        "Продолжение диалогов не присылается уведомлениями и доступно здесь.",
    )


def _buttons(*, show_closed: bool = False):
    switch = (
        btn("🟢 АКТИВНЫЕ", b"menu_dialogs")
        if show_closed
        else btn("✅ ЗАВЕРШЁННЫЕ", b"dialogs_closed")
    )
    return [
        [switch, btn("🚫 ОТКАЗЫ", b"menu_optout")],
        [btn("🔄 ОБНОВИТЬ", b"dialogs_closed" if show_closed else b"menu_dialogs")],
        back_home_row(),
    ]


@bot.on(events.CallbackQuery(data=b"menu_dialogs"))
async def cb_dialogs(event: events.CallbackQuery.Event) -> None:
    if not is_admin(event.sender_id):
        await event.answer(DENIED, alert=True)
        return
    await render_menu(event, _summary_text(show_closed=False), _buttons(show_closed=False))
    await event.answer()


@bot.on(events.CallbackQuery(data=b"dialogs_closed"))
async def cb_dialogs_closed(event: events.CallbackQuery.Event) -> None:
    if not is_admin(event.sender_id):
        await event.answer(DENIED, alert=True)
        return
    await render_menu(event, _summary_text(show_closed=True), _buttons(show_closed=True))
    await event.answer()
