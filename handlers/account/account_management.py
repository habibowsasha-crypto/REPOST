from __future__ import annotations

from loguru import logger
from telethon import Button, TelegramClient
from telethon.sessions import StringSession

from config import API_HASH, API_ID, Query, bot, callback_query, conn
from services.menu_ui import render_menu
from utils.telegram import broadcast_status_emoji, get_active_broadcast_groups


@bot.on(Query(data=b"my_accounts"))
async def my_accounts(event: callback_query) -> None:
    cursor = conn.cursor()
    try:
        sessions = cursor.execute(
            "SELECT user_id, session_string FROM sessions ORDER BY user_id"
        ).fetchall()
    finally:
        cursor.close()

    if not sessions:
        await render_menu(
            event,
            "❌ У вас нет добавленных аккаунтов",
            buttons=[
                [Button.inline("➕ Добавить аккаунт", b"add_account")],
                [Button.inline("🏠 Главное меню", b"menu_home")],
            ],
        )
        return

    buttons = []
    for user_id, session_string in sessions:
        client = TelegramClient(StringSession(session_string), API_ID, API_HASH)
        label = f"Аккаунт {user_id}"
        try:
            await client.connect()
            me = await client.get_me()
            label = me.first_name or me.username or label
        except Exception as exc:
            logger.warning(f"Не удалось загрузить аккаунт {user_id}: {exc}")
            label = f"⚠ Аккаунт {user_id}"
        finally:
            try:
                await client.disconnect()
            except Exception:
                pass
        buttons.append([Button.inline(f"👤 {label}", f"account_info_{user_id}".encode())])

    buttons.append([Button.inline("🏠 Главное меню", b"menu_home")])
    await render_menu(event, "📱 **Список ваших аккаунтов:**", buttons=buttons)


@bot.on(Query(data=lambda data: data.decode().startswith("account_info_")))
async def handle_account_button(event: callback_query) -> None:
    try:
        user_id = int(event.data.decode().rsplit("_", 1)[1])
    except (ValueError, IndexError):
        await event.answer("Некорректный ID аккаунта", alert=True)
        return

    cursor = conn.cursor()
    try:
        row = cursor.execute(
            "SELECT session_string FROM sessions WHERE user_id = ?", (user_id,)
        ).fetchone()
        group_rows = cursor.execute(
            """
            SELECT g.group_id, COALESCE(d.title, g.group_username), COALESCE(d.is_available, 1)
            FROM groups AS g
            LEFT JOIN discovered_groups AS d
              ON d.user_id = g.user_id AND d.group_id = g.group_id
            WHERE g.user_id = ?
            ORDER BY lower(COALESCE(d.title, g.group_username))
            """,
            (user_id,),
        ).fetchall()
    finally:
        cursor.close()

    if not row:
        await render_menu(
            event,
            "⚠ Не удалось найти аккаунт.",
            buttons=[[Button.inline("🏠 Главное меню", b"menu_home")]],
        )
        return

    client = TelegramClient(StringSession(row[0]), API_ID, API_HASH)
    try:
        await client.connect()
        if not await client.is_user_authorized():
            await render_menu(
                event,
                "⚠ Сессия аккаунта больше не авторизована.",
                buttons=[
                    [Button.inline("❌ Удалить аккаунт", f"delete_account_{user_id}".encode())],
                    [Button.inline("🏠 Главное меню", b"menu_home")],
                ],
            )
            return

        me = await client.get_me()
        name = me.first_name or me.username or "Без имени"
        phone = me.phone or "Не указан"
        active_gids = set(get_active_broadcast_groups(user_id))

        lines = []
        for group_id, title, available in group_rows:
            icon = broadcast_status_emoji(user_id, int(group_id))
            suffix = " ⚠ недоступна" if not available else ""
            lines.append(f"{icon} {title}{suffix}")
        group_list = "\n".join(lines) if lines else "Рабочих групп пока нет."
        mass_active = "🟢 ВКЛ" if active_gids else "🔴 ВЫКЛ"

        buttons = [
            [Button.inline("🔎 Найти группы аккаунта", f"sync_groups_{user_id}".encode())],
            [Button.inline("📋 Найденные группы", f"discovered_groups_{user_id}_0".encode())],
            [Button.inline("📋 Рабочий список групп", f"groups_{user_id}".encode())],
            [
                Button.inline("🚀 Начать рассылку во все чаты", f"broadcastAll_{user_id}".encode()),
                Button.inline("❌ Остановить общую рассылку", f"StopBroadcastAll_{user_id}".encode()),
            ],
            [Button.inline("❌ Удалить этот аккаунт", f"delete_account_{user_id}".encode())],
            [Button.inline("🏠 Главное меню", b"menu_home")],
        ]
        await render_menu(
            event,
            f"📢 **Меню аккаунта {name}:**\n"
            f"🚀 **Массовая рассылка:** {mass_active}\n\n"
            f"📌 **Имя:** {name}\n"
            f"📞 **Номер:** `+{phone}`\n\n"
            f"📝 **Рабочие группы:**\n{group_list}",
            buttons=buttons,
        )
    except Exception as exc:
        logger.exception(f"Ошибка открытия аккаунта {user_id}: {exc}")
        await render_menu(
            event,
            f"⚠ Не удалось открыть аккаунт: {exc}",
            buttons=[[Button.inline("🏠 Главное меню", b"menu_home")]],
        )
    finally:
        try:
            await client.disconnect()
        except Exception:
            pass
