from __future__ import annotations

from datetime import datetime, timezone

from loguru import logger
from telethon import Button, TelegramClient
from telethon.sessions import StringSession
from telethon.tl.functions.channels import JoinChannelRequest
from telethon.tl.types import Channel, Chat

from config import API_HASH, API_ID, Query, bot, callback_query, conn
from services.group_worklist import reconcile_dm_tasks_for_account
from services.menu_ui import render_menu
from utils.telegram import broadcast_status_emoji, gid_key, get_entity_by_id


@bot.on(
    Query(
        data=lambda d: d.decode().startswith("account_")
        and d.decode().split("_", 1)[1].isdigit()
    )
)
async def account_menu(event: callback_query) -> None:
    try:
        user_id = int(event.data.decode().split("_", 1)[1])
    except (ValueError, IndexError):
        await event.answer("Некорректный ID аккаунта", alert=True)
        return

    buttons = [
        [Button.inline("🔎 Найти группы аккаунта", f"sync_groups_{user_id}".encode())],
        [Button.inline("📋 Найденные группы", f"discovered_groups_{user_id}_0".encode())],
        [Button.inline("📋 Рабочий список групп", f"groups_{user_id}".encode())],
        [Button.inline("📢 Запустить рассылку во все группы", f"broadcastAll_{user_id}".encode())],
        [Button.inline("❌ Остановить общую рассылку", f"StopBroadcastAll_{user_id}".encode())],
        [Button.inline("◀️ Назад", b"my_accounts")],
        [Button.inline("🏠 Главное меню", b"menu_home")],
    ]
    await render_menu(event, "📱 **Меню аккаунта**\n\nВыберите действие:", buttons=buttons)


@bot.on(Query(data=b"my_groups"))
async def my_groups(event: callback_query) -> None:
    cursor = conn.cursor()
    try:
        catalog = cursor.execute(
            "SELECT group_id, group_username FROM pre_groups ORDER BY lower(group_username)"
        ).fetchall()
        working_count = int(cursor.execute("SELECT COUNT(*) FROM groups").fetchone()[0] or 0)
    finally:
        cursor.close()

    buttons = []
    if not catalog:
        message = (
            "📭 **Общий каталог групп пуст.**\n\n"
            "Добавьте публичную группу по @username/ID либо найдите группы через карточку аккаунта."
        )
    else:
        lines = ["👥 **Общий каталог групп:**", ""]
        for group_id, group_username in catalog:
            lines.append(f"• {group_username} (`{group_id}`)")
        lines.extend(
            [
                "",
                f"Записей в каталоге: {len(catalog)}",
                f"Рабочих привязок к аккаунтам: {working_count}",
            ]
        )
        message = "\n".join(lines)
        buttons.extend(
            [
                [Button.inline("➕ Добавить все аккаунты в каталог", b"add_all_accounts_to_groups")],
                [Button.inline("❌ Удалить из каталога", b"delete_group")],
            ]
        )

    buttons.extend(
        [
            [Button.inline("➕ Добавить в каталог", b"add_groups")],
            [Button.inline("👤 Открыть аккаунты", b"my_accounts")],
            [Button.inline("🏠 Главное меню", b"menu_home")],
        ]
    )
    await render_menu(event, message, buttons=buttons)


async def _resolve_catalog_entity(client: TelegramClient, user_id: int, group_id: int, identifier: str):
    if identifier.startswith("@"):
        try:
            await client(JoinChannelRequest(identifier))
        except Exception as exc:
            # Already joined, invite-only, or no permission. Resolution below decides
            # whether the account actually has access.
            logger.debug(f"Не удалось выполнить JoinChannelRequest {identifier}: {exc}")
        try:
            return await client.get_entity(identifier)
        except Exception:
            pass
    return await get_entity_by_id(client, group_id, user_id=user_id, identifier=identifier)


@bot.on(Query(data=b"add_all_accounts_to_groups"))
async def add_all_accounts_to_groups(event: callback_query) -> None:
    cursor = conn.cursor()
    try:
        accounts = cursor.execute(
            "SELECT user_id, session_string FROM sessions ORDER BY user_id"
        ).fetchall()
        catalog = cursor.execute(
            "SELECT group_id, group_username FROM pre_groups ORDER BY group_username"
        ).fetchall()
    finally:
        cursor.close()

    if not accounts:
        await render_menu(event, "❌ Нет добавленных аккаунтов.")
        return
    if not catalog:
        await render_menu(event, "❌ Общий каталог групп пуст.")
        return

    added = failed = 0
    for user_id, session_string in accounts:
        client = TelegramClient(StringSession(session_string), API_ID, API_HASH)
        try:
            await client.connect()
            if not await client.is_user_authorized():
                failed += len(catalog)
                continue

            for group_id, identifier in catalog:
                try:
                    entity = await _resolve_catalog_entity(client, user_id, group_id, identifier)
                    if not isinstance(entity, (Channel, Chat)):
                        failed += 1
                        continue
                    if isinstance(entity, Channel) and not (
                        getattr(entity, "megagroup", False)
                        or getattr(entity, "gigagroup", False)
                    ):
                        failed += 1
                        continue

                    actual_id = int(entity.id)
                    raw_username = getattr(entity, "username", None)
                    username = f"@{raw_username}" if raw_username else None
                    title = getattr(entity, "title", None) or identifier or str(actual_id)
                    peer_type = "channel" if isinstance(entity, Channel) else "chat"
                    access_hash = getattr(entity, "access_hash", None)
                    is_admin = int(bool(getattr(entity, "admin_rights", None)))
                    is_creator = int(bool(getattr(entity, "creator", False)))
                    now = datetime.now(timezone.utc).isoformat()
                    working_identifier = username or str(actual_id)

                    with conn:
                        conn.execute(
                            """
                            INSERT INTO discovered_groups (
                                user_id, group_id, title, username, access_hash, peer_type,
                                is_admin, is_creator, is_available, last_seen_at
                            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 1, ?)
                            ON CONFLICT(user_id, group_id) DO UPDATE SET
                                title=excluded.title,
                                username=excluded.username,
                                access_hash=excluded.access_hash,
                                peer_type=excluded.peer_type,
                                is_admin=excluded.is_admin,
                                is_creator=excluded.is_creator,
                                is_available=1,
                                last_seen_at=excluded.last_seen_at
                            """,
                            (
                                user_id,
                                actual_id,
                                title,
                                username,
                                access_hash,
                                peer_type,
                                is_admin,
                                is_creator,
                                now,
                            ),
                        )
                        conn.execute(
                            "UPDATE discovered_groups SET is_enabled = 1 WHERE user_id = ? AND group_id = ?",
                            (user_id, actual_id),
                        )
                        conn.execute(
                            """
                            INSERT INTO groups (user_id, group_id, group_username)
                            VALUES (?, ?, ?)
                            ON CONFLICT(user_id, group_id) DO UPDATE SET
                                group_username=excluded.group_username
                            """,
                            (user_id, actual_id, working_identifier),
                        )
                    added += 1
                except Exception as exc:
                    failed += 1
                    logger.warning(
                        f"Не удалось добавить группу {identifier} аккаунту {user_id}: {exc}"
                    )
        finally:
            await client.disconnect()

    await render_menu(
        event,
        f"✅ Готово. Рабочих привязок добавлено/обновлено: {added}. Ошибок: {failed}.",
        buttons=[
            [Button.inline("👤 Мои аккаунты", b"my_accounts")],
            [Button.inline("🏠 Главное меню", b"menu_home")],
        ],
    )


@bot.on(Query(data=lambda data: data.decode().startswith("add_all_groups_")))
async def add_all_groups_to_account(event: callback_query) -> None:
    """Compatibility callback for older bot messages."""
    from handlers.group.group_discovery_handlers import sync_groups

    await sync_groups(event)


@bot.on(Query(data=lambda d: d.decode().startswith("groups_")))
async def groups_list(event: callback_query) -> None:
    try:
        user_id = int(event.data.decode().split("_", 1)[1])
    except (ValueError, IndexError):
        await event.answer("Некорректный ID аккаунта", alert=True)
        return

    cursor = conn.cursor()
    try:
        session_row = cursor.execute(
            "SELECT session_string FROM sessions WHERE user_id = ?", (user_id,)
        ).fetchone()
        rows = cursor.execute(
            """
            SELECT
                g.group_id,
                g.group_username,
                d.title,
                COALESCE(d.is_available, 1)
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
        await render_menu(event, "⚠ Не найдена сессия этого аккаунта.")
        return
    if not rows:
        await render_menu(
            event,
            "📭 Рабочий список групп пуст.",
            buttons=[
                [Button.inline("🔎 Найти группы аккаунта", f"sync_groups_{user_id}".encode())],
                [Button.inline("◀️ К аккаунту", f"account_info_{user_id}".encode())],
                [Button.inline("🏠 Главное меню", b"menu_home")],
            ],
        )
        return

    buttons = []
    for group_id, identifier, title, available in rows:
        status = broadcast_status_emoji(user_id, int(group_id))
        availability = "" if available else " ⚠"
        display = title or identifier or str(group_id)
        buttons.append(
            [
                Button.inline(
                    f"{status} {display}{availability}",
                    f"groupInfo_{user_id}_{gid_key(group_id)}".encode(),
                )
            ]
        )
    buttons.extend(
        [
            [Button.inline("◀️ Назад", f"account_info_{user_id}".encode())],
            [Button.inline("🏠 Главное меню", b"menu_home")],
        ]
    )
    await render_menu(
        event,
        "📋 **Рабочий список групп:**\n\nВыберите группу:",
        buttons=buttons,
    )


@bot.on(Query(data=lambda data: data.decode().startswith("DeleteGroup_")))
async def remove_group_from_account(event: callback_query) -> None:
    """Remove one account/group working link and refresh affected DM monitors."""
    try:
        _, user_id_raw, group_id_raw = event.data.decode().split("_", 2)
        user_id = int(user_id_raw)
        group_id = gid_key(group_id_raw)
    except (ValueError, IndexError):
        await event.answer("Некорректные данные группы", alert=True)
        return

    with conn:
        conn.execute(
            "UPDATE discovered_groups SET is_enabled = 0 WHERE user_id = ? AND group_id = ?",
            (user_id, group_id),
        )
        conn.execute(
            "DELETE FROM groups WHERE user_id = ? AND group_id = ?",
            (user_id, group_id),
        )

    await reconcile_dm_tasks_for_account(user_id)
    await render_menu(
        event,
        "✅ Группа удалена из рабочего списка аккаунта.",
        buttons=[
            [Button.inline("◀️ К группам", f"groups_{user_id}".encode())],
            [Button.inline("🏠 Главное меню", b"menu_home")],
        ],
    )
