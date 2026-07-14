from sqlite3 import IntegrityError

from telethon import Button

from services.menu_ui import render_menu
from utils.telegram import gid_key
from services.admin_state import clear_admin_interaction_state, is_command_event
from config import callback_query, callback_message, user_sessions, New_Message, Query, bot, conn


@bot.on(Query(data=b"add_groups"))
async def manage_groups(event: callback_query) -> None:
    await clear_admin_interaction_state(event.sender_id)
    user_sessions[event.sender_id] = {"step": "awaiting_group_username"}
    await render_menu(
        event,
        "📲 Напишите @username группы или ID группы, чтобы добавить её в общий каталог.\n\n"
        "Для закрытых групп удобнее открыть нужный аккаунт и нажать «Найти группы аккаунта».",
        buttons=[[Button.inline("🏠 Главное меню", b"menu_home")]],
    )


def _awaiting_group_input(event) -> bool:
    if is_command_event(event):
        return False
    state = user_sessions.get(event.sender_id)
    return bool(state and state.get("step") == "awaiting_group_username")


@bot.on(New_Message(func=_awaiting_group_input))
async def handle_group_input(event: callback_message) -> None:
    user_sessions.pop(event.sender_id, None)
    group_identifier: str = event.text.strip()
    cursor = conn.cursor()
    try:
        if group_identifier.startswith("@") and " " not in group_identifier:
            try:
                entity = await bot.get_entity(group_identifier)
                group_id = int(entity.id)
                cursor.execute(
                    "INSERT INTO pre_groups (group_username, group_id) VALUES (?, ?)",
                    (group_identifier, group_id),
                )
                conn.commit()
                await event.respond(
                    f"✅ Группа {group_identifier} добавлена в общий каталог.\n\n"
                    "Она не привязывается к Telegram-аккаунтам автоматически. "
                    "Откройте «Мои аккаунты» → аккаунт → «Найти группы аккаунта».",
                    buttons=[[Button.inline("🏠 Главное меню", b"menu_home")]],
                )
            except IntegrityError:
                await event.respond(
                    "⚠ Эта группа уже есть в общем каталоге.",
                    buttons=[[Button.inline("🏠 Главное меню", b"menu_home")]],
                )
            except Exception as exc:
                await event.respond(
                    f"⚠ Не удалось проверить публичную группу: {exc}",
                    buttons=[[Button.inline("🏠 Главное меню", b"menu_home")]],
                )
            return

        try:
            group_id = gid_key(group_identifier)
        except ValueError:
            await event.respond(
                "⚠ Неправильный формат. Введите @username группы или числовой ID.",
                buttons=[[Button.inline("🏠 Главное меню", b"menu_home")]],
            )
            return

        try:
            cursor.execute(
                "INSERT INTO pre_groups (group_username, group_id) VALUES (?, ?)",
                (str(group_id), group_id),
            )
            conn.commit()
            await event.respond(
                f"✅ ID {group_id} добавлен в общий каталог.\n\n"
                "Для закрытой группы привязка к аккаунту выполняется через "
                "«Мои аккаунты» → аккаунт → «Найти группы аккаунта».",
                buttons=[[Button.inline("🏠 Главное меню", b"menu_home")]],
            )
        except IntegrityError:
            await event.respond(
                "⚠ Эта группа уже есть в общем каталоге.",
                buttons=[[Button.inline("🏠 Главное меню", b"menu_home")]],
            )
    finally:
        cursor.close()
