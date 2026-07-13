from __future__ import annotations

import os

from loguru import logger
from telethon import Button, TelegramClient
from telethon.sessions import StringSession
from telethon.tl.functions.channels import GetFullChannelRequest
from telethon.tl.functions.messages import GetFullChatRequest
from telethon.tl.types import Channel, Chat, InputPeerChannel, InputPeerChat

from config import API_HASH, API_ID, Query, bot, callback_query, conn
from services.menu_ui import render_menu
from utils.telegram import broadcast_status_emoji, get_entity_by_id, gid_key


async def _resolve_group_entity(
    client: TelegramClient,
    user_id: int,
    group_id: int,
    identifier: str,
):
    cursor = conn.cursor()
    try:
        row = cursor.execute(
            """
            SELECT access_hash, peer_type
            FROM discovered_groups
            WHERE user_id = ? AND group_id = ?
            """,
            (user_id, group_id),
        ).fetchone()
    finally:
        cursor.close()

    candidates = []
    if row:
        access_hash, peer_type = row
        if peer_type == "channel" and access_hash is not None:
            candidates.append(InputPeerChannel(int(group_id), int(access_hash)))
        elif peer_type == "chat":
            candidates.append(InputPeerChat(int(group_id)))
    if identifier:
        candidates.append(identifier)

    for candidate in candidates:
        try:
            return await client.get_entity(candidate)
        except Exception:
            continue
    return await get_entity_by_id(client, int(group_id), user_id=user_id, identifier=identifier)


@bot.on(Query(data=lambda data: data.decode().startswith("listOfgroups_")))
async def handle_groups_list(event: callback_query) -> None:
    try:
        user_id = int(event.data.decode().rsplit("_", 1)[1])
    except (ValueError, IndexError):
        await event.answer("Некорректный ID аккаунта", alert=True)
        return

    cursor = conn.cursor()
    try:
        session_row = cursor.execute(
            "SELECT session_string FROM sessions WHERE user_id = ?", (user_id,)
        ).fetchone()
        groups = cursor.execute(
            """
            SELECT g.group_id, g.group_username, COALESCE(d.title, g.group_username)
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

    if not session_row:
        await render_menu(event, "⚠ Не удалось найти аккаунт.")
        return
    if not groups:
        await render_menu(
            event,
            "📭 У аккаунта нет рабочих групп.",
            buttons=[
                [Button.inline("🔎 Найти группы аккаунта", f"sync_groups_{user_id}".encode())],
                [Button.inline("◀️ К аккаунту", f"account_info_{user_id}".encode())],
            ],
        )
        return

    buttons = []
    for group_id, _identifier, title in groups:
        buttons.append(
            [
                Button.inline(
                    f"{broadcast_status_emoji(user_id, int(group_id))} {title}",
                    f"groupInfo_{user_id}_{gid_key(group_id)}".encode(),
                )
            ]
        )
    buttons.extend(
        [
            [Button.inline("◀️ К аккаунту", f"account_info_{user_id}".encode())],
            [Button.inline("🏠 Главное меню", b"menu_home")],
        ]
    )
    await render_menu(event, "📋 **Рабочие группы аккаунта:**", buttons=buttons)


@bot.on(Query(data=lambda data: data.decode().startswith("groupInfo_")))
async def group_info(event: callback_query) -> None:
    try:
        _, user_id_raw, group_id_raw = event.data.decode().split("_", 2)
        user_id = int(user_id_raw)
        group_id = int(group_id_raw)
    except (ValueError, IndexError):
        await event.answer("Некорректные данные группы", alert=True)
        return

    cursor = conn.cursor()
    try:
        session_row = cursor.execute(
            "SELECT session_string FROM sessions WHERE user_id = ?", (user_id,)
        ).fetchone()
        group_row = cursor.execute(
            """
            SELECT g.group_username, COALESCE(d.title, g.group_username)
            FROM groups AS g
            LEFT JOIN discovered_groups AS d
              ON d.user_id = g.user_id AND d.group_id = g.group_id
            WHERE g.user_id = ? AND g.group_id = ?
            """,
            (user_id, group_id),
        ).fetchone()
        broadcast_row = cursor.execute(
            """
            SELECT broadcast_text, interval_minutes, is_active, photo_url
            FROM broadcasts
            WHERE user_id = ? AND group_id = ?
            ORDER BY rowid DESC LIMIT 1
            """,
            (user_id, gid_key(group_id)),
        ).fetchone()
    finally:
        cursor.close()

    if not session_row:
        await render_menu(event, "⚠ Не найдена сессия этого аккаунта.")
        return
    if not group_row:
        await render_menu(event, "⚠ Группа не найдена в рабочем списке.")
        return

    identifier, saved_title = group_row
    client = TelegramClient(StringSession(session_row[0]), API_ID, API_HASH)
    try:
        await client.connect()
        entity = await _resolve_group_entity(client, user_id, group_id, identifier)
        if entity is None:
            raise RuntimeError("Telegram entity не найден")

        broadcast_text = (
            broadcast_row[0] if broadcast_row and broadcast_row[0] else "Не установлен"
        )
        interval = (
            f"{broadcast_row[1]} мин."
            if broadcast_row and broadcast_row[1]
            else "Не установлен"
        )
        photo_url = broadcast_row[3] if broadcast_row and len(broadcast_row) > 3 else None
        photo_info = f"Фото: {os.path.basename(photo_url)}" if photo_url else "Фото отсутствует"
        status = broadcast_status_emoji(user_id, group_id)

        group_title = getattr(entity, "title", None) or saved_title or identifier
        username = getattr(entity, "username", None)
        username_display = f"@{username}" if username else "Нет юзернейма"
        members_count = getattr(entity, "participants_count", None)
        if members_count is None:
            try:
                if isinstance(entity, Channel):
                    full = await client(GetFullChannelRequest(entity))
                    members_count = getattr(full.full_chat, "participants_count", "Неизвестно")
                elif isinstance(entity, Chat):
                    full = await client(GetFullChatRequest(entity.id))
                    members_count = getattr(full.full_chat, "participants_count", "Неизвестно")
            except Exception as exc:
                logger.debug(f"Не удалось получить количество участников {group_id}: {exc}")
                members_count = "Неизвестно"
        if members_count is None:
            members_count = "Неизвестно"

        if isinstance(entity, Channel):
            group_type = "Канал" if entity.broadcast else "Супергруппа"
        elif isinstance(entity, Chat):
            group_type = "Группа"
        else:
            group_type = "Неизвестный тип"

        info_text = (
            "📊 **Информация о группе**\n\n"
            f"👥 **Название**: {group_title}\n"
            f"🔖 **Юзернейм**: {username_display}\n"
            f"👤 **Участников**: {members_count}\n"
            f"📝 **Тип**: {group_type}\n"
            f"🆔 **ID**: {group_id}\n\n"
            f"📬 **Статус рассылки**: {status}\n"
            f"⏱ **Интервал**: {interval}\n"
            f"📝 **Текст рассылки**:\n"
            f"{broadcast_text[:100] + '...' if len(broadcast_text) > 100 else broadcast_text}\n"
            f"🖼 **{photo_info}**"
        )
        buttons = [
            [Button.inline("📝 Текст и интервал рассылки", f"BroadcastTextInterval_{user_id}_{group_id}".encode())],
            [Button.inline("▶️ Начать/возобновить рассылку", f"StartResumeBroadcast_{user_id}_{group_id}".encode())],
            [Button.inline("⏹ Остановить рассылку", f"StopAccountBroadcast_{user_id}_{group_id}".encode())],
            [Button.inline("❌ Удалить из рабочего списка", f"DeleteGroup_{user_id}_{group_id}".encode())],
            [Button.inline("◀️ Назад к группам", f"groups_{user_id}".encode())],
            [Button.inline("🏠 Главное меню", b"menu_home")],
        ]
        await render_menu(event, info_text, buttons=buttons)
    except Exception as exc:
        logger.exception(f"Ошибка получения информации о группе {group_id}: {exc}")
        await render_menu(
            event,
            f"⚠ Не удалось получить информацию о группе: {exc}",
            buttons=[
                [Button.inline("◀️ Назад", f"groups_{user_id}".encode())],
                [Button.inline("🏠 Главное меню", b"menu_home")],
            ],
        )
    finally:
        try:
            await client.disconnect()
        except Exception:
            pass
