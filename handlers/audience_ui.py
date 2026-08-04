"""Audience (crypto base) admin UI with downloadable CSV import/export."""

from __future__ import annotations

import datetime as dt
import io

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

MAX_IMPORT_BYTES = 10 * 1024 * 1024


def _audience_text() -> str:
    total = audience_svc.count()
    recent = audience_svc.list_recent(12)
    if recent:
        body = join(*[audience_svc.format_line(r) for r in recent])
    else:
        body = "Пока пусто. База пополняется после First DM или импорта."
    return screen(
        "📁",
        "База людей",
        f"👥 Всего записей: **{total}**",
        section("Последние записи", body),
        join(
            "📥 Скачать: подробный UTF-8 CSV",
            "📤 Загрузить: CSV, TXT или список ID",
            "🚫 Люди из «Не писать» всегда пропускаются",
        ),
    )


def _audience_buttons():
    return [
        [btn("🔄 ОБНОВИТЬ", b"menu_audience")],
        [btn("📥 СКАЧАТЬ CSV", b"aud_export")],
        [btn("📤 ЗАГРУЗИТЬ CSV / TXT", b"aud_import")],
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
    total = len(audience_svc.export_rows(only_with_dm=False))
    if total == 0:
        await event.answer("База пуста", alert=True)
        return

    payload = audience_svc.export_csv_bytes(only_with_dm=False)
    stamp = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    file_obj = io.BytesIO(payload)
    file_obj.name = f"channel_dm_audience_{stamp}.csv"
    await bot.send_file(
        event.chat_id,
        file_obj,
        caption=(
            "📤 **ЭКСПОРТ БАЗЫ ГОТОВ**\n"
            "━━━━━━━━━━━━━━━━━━\n\n"
            f"👥 Записей: **{total}**\n"
            "📄 Формат: **UTF-8 CSV**\n\n"
            "Этот же файл можно загрузить обратно через раздел «База людей»."
        ),
    )
    await event.answer(f"Выгружено: {total}")


@bot.on(events.CallbackQuery(data=b"aud_import"))
async def cb_aud_import(event: events.CallbackQuery.Event) -> None:
    if not is_admin(event.sender_id):
        await event.answer(DENIED, alert=True)
        return
    set_state(event.sender_id, flow="audience", step="import_file")
    text = screen(
        "📤",
        "Загрузить базу",
        "Пришли **CSV/TXT-файл** из выгрузки бота или вставь данные текстом.",
        "Поддерживается подробный CSV: `user_id, username, access_hash, ...`",
        "Также можно прислать обычные user ID по одному или несколько в строке.",
        "Импорт сохраняет username, access_hash и исходный аккаунт, если они есть.",
        "По текущему правилу проекта импортированные записи повторно ставятся в очередь; opt-out пропускается.",
        "Отмена: /cancel",
    )
    await render_menu(event, text, [back_row(b"menu_audience"), back_home_row()])
    await event.answer()


def _is_audience_flow(event) -> bool:
    if not event.is_private:
        return False
    if not is_admin(event.sender_id):
        return False
    st = get_state(int(event.sender_id)) or {}
    return st.get("flow") == "audience"


def _decode_import_bytes(payload: bytes) -> str:
    for encoding in ("utf-8-sig", "utf-8", "cp1251"):
        try:
            return payload.decode(encoding)
        except UnicodeDecodeError:
            continue
    raise ValueError("unsupported_encoding")


@bot.on(events.NewMessage(func=_is_audience_flow))
async def on_audience_import(event: events.NewMessage.Event) -> None:
    st = get_state(event.sender_id) or {}
    if st.get("step") not in {"import_file", "import_ids"}:
        return

    raw = (event.raw_text or "").strip()
    if raw.startswith("/"):
        return

    if event.message.file:
        size = int(getattr(event.message.file, "size", 0) or 0)
        if size > MAX_IMPORT_BYTES:
            await event.respond(notice("warn", "Файл больше 10 МБ. Раздели его на части."))
            return
        payload = await event.message.download_media(file=bytes)
        if not isinstance(payload, (bytes, bytearray)):
            await event.respond(notice("warn", "Не удалось прочитать файл. Пришли CSV или TXT."))
            return
        try:
            raw = _decode_import_bytes(bytes(payload))
        except ValueError:
            await event.respond(notice("warn", "Неизвестная кодировка. Нужен UTF-8 CSV/TXT."))
            return

    records = audience_svc.parse_import_text(raw)
    if not records:
        await event.respond(notice("warn", "Не нашёл корректных user ID. /cancel - отмена."))
        return

    clear_state(event.sender_id)
    stats = audience_svc.import_records(records, source="import")
    await event.respond(
        screen(
            "📁",
            "Импорт базы завершён",
            join(
                kv("Распознано записей", str(stats["recognized"])),
                kv("Сохранено в базу", str(stats["added_or_touch"])),
                kv("Поставлено в очередь", str(stats["queued"])),
                kv("С username", str(stats["with_username"])),
                kv("С access_hash", str(stats["with_access_hash"])),
                kv("С исходным аккаунтом", str(stats["with_source_account"])),
                kv("Opt-out пропущено", str(stats["skipped_opt_out"])),
                kv("Не поставлено в очередь", str(stats["skipped_queue"])),
                kv("Повреждённых строк", str(stats["skipped_invalid"])),
            ),
            "Новые First DM начнут отправляться только когда рассылка включена в главном меню.",
        ),
        buttons=[back_home_row()],
    )
