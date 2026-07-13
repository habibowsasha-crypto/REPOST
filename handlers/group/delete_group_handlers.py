from telethon import Button

from services.menu_ui import render_menu
from utils.telegram import gid_key
from services.admin_state import clear_admin_interaction_state, is_command_event
from config import (
    user_sessions_deleting, callback_query, callback_message,
    Query, New_Message, bot, conn,
)


@bot.on(Query(data=b"delete_group"))
async def handle_delete_group(event: callback_query) -> None:
    await clear_admin_interaction_state(event.sender_id)
    user_sessions_deleting[event.sender_id] = {"step": "awaiting_group_username"}
    await render_menu(
        event,
        "📲 Введите @username или ID записи, которую нужно удалить из общего каталога:\n\n"
        "Пример: @mygroup или -1001234567890",
        buttons=[[Button.inline("🏠 Главное меню", b"menu_home")]],
    )


def is_awaiting_group_deletion(event):
    if is_command_event(event):
        return False
    state = user_sessions_deleting.get(event.sender_id)
    return bool(state and state.get("step") == "awaiting_group_username")


@bot.on(New_Message(func=is_awaiting_group_deletion))
async def handle_user_input(event: callback_message) -> None:
    group_input = event.text.strip()
    if not (group_input.startswith("@") or group_input.isdigit() or group_input.startswith("-")):
        await event.respond("⚠ Введите корректный @username или числовой ID.")
        return

    cursor = conn.cursor()
    try:
        if group_input.startswith("@"):
            cursor.execute("DELETE FROM pre_groups WHERE group_username = ?", (group_input,))
        else:
            try:
                raw_group_id = int(group_input)
                group_id = gid_key(raw_group_id)
            except ValueError:
                await event.respond("⚠ Введите корректный числовой ID.")
                return
            cursor.execute(
                "DELETE FROM pre_groups WHERE group_id IN (?, ?, ?)",
                (raw_group_id, abs(raw_group_id), group_id),
            )

        affected = cursor.rowcount
        conn.commit()
        user_sessions_deleting.pop(event.sender_id, None)
        if affected:
            text = "✅ Запись удалена из общего каталога."
        else:
            text = "⚠ Такая запись в общем каталоге не найдена."
        await event.respond(text, buttons=[[Button.inline("🏠 Главное меню", b"menu_home")]])
    finally:
        cursor.close()
