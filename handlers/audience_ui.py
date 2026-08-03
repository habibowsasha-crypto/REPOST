"""Audience (crypto base) admin UI."""

from __future__ import annotations

from telethon import events

from config import bot, is_admin
from services import audience as audience_svc
from services.admin_state import clear_state, get_state, set_state
from services.menu_ui import render_menu
from services.ui import (
    DENIED,
    back_home_row,
    back_row,
    btn,
    join,
    kv,
    notice,
    screen,
    section,
)


def _audience_text() -> str:
    total = audience_svc.count()
    recent = audience_svc.list_recent(12)
    if recent:
        body = join(*[audience_svc.format_line(r) for r in recent])
    else:
        body = "Пока пусто. База пополняется после first DM."
    return screen(
        "📁",
        "База (крипто-аудитория)",
        kv("Всего", str(total)),
        section("Последние", body),
        join(
            "Авто: каждый, кому ушёл first DM.",
            "Загрузка: id → очередь (opt-out пропускается).",
        ),
    )


def _audience_buttons():
    return [
        [btn("🔄 Обновить", b"menu_audience")],
        [btn("📥 Выгрузить", b"aud_export")],
        [btn("📤 Загрузить id", b"aud_import")],
        back_home_row(),
    ]


@bot.on(events.CallbackQuery(data=b"menu_audience"))
async def cb_menu_audience(event: events.CallbackQuery.Event) -> None:
    if not is_admin(event.sender_id):
        await event.answer(DENIED, alert=True)
        return
    await render_menu(event, _audience_text(), _audience_buttons())
    await event.answer()


@bot.on(events.CallbackQuery(data=b"aud_export"))
async def cb_aud_export(event: events.CallbackQuery.Event) -> None:
    if not is_admin(event.sender_id):
        await event.answer(DENIED, alert=True)
        return
    lines = audience_svc.export_lines(only_with_dm=True)
    total = max(0, len(lines) - 1)
    if total == 0:
        await event.answer("База пуста", alert=True)
        return
    # Telegram message limit ~4096; chunk if needed
    chunk: list[str] = []
    size = 0
    files = 0
    for line in lines:
        if size + len(line) + 1 > 3500 and chunk:
            files += 1
            await event.respond(
                f"**Выгрузка {files}** ({len(chunk)} строк)\n\n"
                + "```\n"
                + "\n".join(chunk)
                + "\n```"
            )
            chunk = []
            size = 0
        chunk.append(line)
        size += len(line) + 1
    if chunk:
        files += 1
        await event.respond(
            f"**Выгрузка** · {total} id (с first DM)\n\n"
            + "```\n"
            + "\n".join(chunk)
            + "\n```"
        )
    await event.answer(f"Выгружено: {total}")


@bot.on(events.CallbackQuery(data=b"aud_import"))
async def cb_aud_import(event: events.CallbackQuery.Event) -> None:
    if not is_admin(event.sender_id):
        await event.answer(DENIED, alert=True)
        return
    set_state(event.sender_id, flow="audience", step="import_ids")
    text = screen(
        "📤",
        "Загрузить в очередь",
        "Пришли список **user id** (по одному в строке) или CSV:",
        "`user_id,username,first_dm_at`",
        "Opt-out и уже sent/in_progress не ставятся в pending.",
        "Отмена: /cancel",
    )
    await render_menu(
        event,
        text,
        [back_row(b"menu_audience"), back_home_row()],
    )
    await event.answer()


def _is_audience_flow(event) -> bool:
    if not event.is_private:
        return False
    if not is_admin(event.sender_id):
        return False
    st = get_state(int(event.sender_id)) or {}
    return st.get("flow") == "audience"


@bot.on(events.NewMessage(func=_is_audience_flow))
async def on_audience_import(event: events.NewMessage.Event) -> None:
    st = get_state(event.sender_id) or {}
    if st.get("step") != "import_ids":
        return
    raw = (event.raw_text or "").strip()
    if raw.startswith("/"):
        return
    ids = audience_svc.parse_ids_from_text(raw)
    if not ids:
        await event.respond(notice("warn", "Не нашёл ни одного user id. /cancel — отмена."))
        return
    clear_state(event.sender_id)
    stats = audience_svc.import_user_ids(ids, source="import")
    await event.respond(
        screen(
            "📁",
            "Импорт базы",
            join(
                kv("Распознано id", str(len(ids))),
                kv("В базу", str(stats["added_or_touch"])),
                kv("В очередь", str(stats["queued"])),
                kv("Opt-out пропуск", str(stats["skipped_opt_out"])),
                kv("Уже заняты / skip", str(stats["skipped_queue"])),
                kv("Битые", str(stats["skipped_invalid"])),
            ),
            "Дальше: главная → **Запустить**, если воркер на паузе.",
        ),
        buttons=[back_home_row()],
    )
