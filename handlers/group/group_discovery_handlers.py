from __future__ import annotations

from datetime import datetime, timezone
from math import ceil

from loguru import logger
from telethon import Button, TelegramClient
from telethon.sessions import StringSession
from telethon.tl.types import Channel, Chat

from config import ADMIN_ID_LIST, API_HASH, API_ID, Query, bot, callback_query, conn
from services.menu_ui import render_menu

_PAGE_SIZE = 8


def _is_group_entity(entity) -> bool:
    if isinstance(entity, Chat):
        return True
    if isinstance(entity, Channel):
        return bool(getattr(entity, "megagroup", False) or getattr(entity, "gigagroup", False))
    return False


def _managed_by_account(entity) -> tuple[bool, bool]:
    is_creator = bool(getattr(entity, "creator", False))
    is_admin = bool(getattr(entity, "admin_rights", None))
    return is_admin, is_creator


def _account_session(user_id: int):
    cursor = conn.cursor()
    try:
        return cursor.execute(
            "SELECT session_string FROM sessions WHERE user_id = ?", (user_id,)
        ).fetchone()
    finally:
        cursor.close()


async def _sync_groups(user_id: int) -> dict:
    row = _account_session(user_id)
    if not row:
        raise RuntimeError("Сессия аккаунта не найдена")

    client = TelegramClient(StringSession(row[0]), API_ID, API_HASH)
    now = datetime.now(timezone.utc).isoformat()
    total = managed = private_count = public_count = 0

    await client.connect()
    try:
        if not await client.is_user_authorized():
            raise RuntimeError("Аккаунт больше не авторизован")

        dialogs = await client.get_dialogs()
        cursor = conn.cursor()
        try:
            cursor.execute(
                "UPDATE discovered_groups SET is_available = 0 WHERE user_id = ?",
                (user_id,),
            )

            for dialog in dialogs:
                entity = dialog.entity
                if not _is_group_entity(entity):
                    continue

                group_id = int(entity.id)
                title = (getattr(dialog, "name", None) or getattr(entity, "title", None) or str(group_id)).strip()
                raw_username = getattr(entity, "username", None)
                username = f"@{raw_username}" if raw_username else None
                access_hash = getattr(entity, "access_hash", None)
                peer_type = "channel" if isinstance(entity, Channel) else "chat"
                is_admin, is_creator = _managed_by_account(entity)

                total += 1
                public_count += 1 if username else 0
                private_count += 0 if username else 1
                managed += 1 if (is_admin or is_creator) else 0

                cursor.execute(
                    """
                    INSERT INTO discovered_groups (
                        user_id, group_id, title, username, access_hash, peer_type,
                        is_admin, is_creator, is_available, last_seen_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 1, ?)
                    ON CONFLICT(user_id, group_id) DO UPDATE SET
                        title = excluded.title,
                        username = excluded.username,
                        access_hash = excluded.access_hash,
                        peer_type = excluded.peer_type,
                        is_admin = excluded.is_admin,
                        is_creator = excluded.is_creator,
                        is_available = 1,
                        last_seen_at = excluded.last_seen_at
                    """,
                    (
                        user_id,
                        group_id,
                        title,
                        username,
                        access_hash,
                        peer_type,
                        int(is_admin),
                        int(is_creator),
                        now,
                    ),
                )

                # Automatic activation is intentionally limited to groups managed
                # by this account. Other memberships remain visible for review only.
                if is_admin or is_creator:
                    identifier = username or str(group_id)
                    exists = cursor.execute(
                        "SELECT 1 FROM groups WHERE user_id = ? AND group_id = ? LIMIT 1",
                        (user_id, group_id),
                    ).fetchone()
                    if not exists:
                        cursor.execute(
                            "INSERT INTO groups (user_id, group_id, group_username) VALUES (?, ?, ?)",
                            (user_id, group_id, identifier),
                        )

            conn.commit()
        finally:
            cursor.close()
    finally:
        await client.disconnect()

    return {
        "total": total,
        "managed": managed,
        "private": private_count,
        "public": public_count,
    }


def _list_rows(user_id: int):
    cursor = conn.cursor()
    try:
        return cursor.execute(
            """
            SELECT group_id, title, username, is_admin, is_creator, is_available
            FROM discovered_groups
            WHERE user_id = ?
            ORDER BY is_available DESC, is_creator DESC, is_admin DESC, lower(title)
            """,
            (user_id,),
        ).fetchall()
    finally:
        cursor.close()


def _page_buttons(user_id: int, page: int, pages: int):
    nav = []
    if page > 0:
        nav.append(Button.inline("⬅️", f"discovered_groups_{user_id}_{page - 1}".encode()))
    nav.append(Button.inline(f"{page + 1}/{max(pages, 1)}", b"noop"))
    if page + 1 < pages:
        nav.append(Button.inline("➡️", f"discovered_groups_{user_id}_{page + 1}".encode()))

    return [
        nav,
        [Button.inline("🔄 Обновить", f"sync_groups_{user_id}".encode())],
        [Button.inline("◀️ К аккаунту", f"account_info_{user_id}".encode())],
        [Button.inline("🏠 Главное меню", b"menu_home")],
    ]


async def _render_discovered(event, user_id: int, page: int = 0, notice: str = "") -> None:
    rows = _list_rows(user_id)
    if not rows:
        text = (
            "🔎 **Группы аккаунта**\n\n"
            "Список ещё не синхронизирован. Нажмите «Обновить», чтобы получить "
            "группы и супергруппы из диалогов подключённого аккаунта."
        )
        buttons = [
            [Button.inline("🔄 Найти группы", f"sync_groups_{user_id}".encode())],
            [Button.inline("◀️ К аккаунту", f"account_info_{user_id}".encode())],
            [Button.inline("🏠 Главное меню", b"menu_home")],
        ]
        await render_menu(event, text, buttons=buttons)
        return

    pages = max(1, ceil(len(rows) / _PAGE_SIZE))
    page = min(max(page, 0), pages - 1)
    chunk = rows[page * _PAGE_SIZE : (page + 1) * _PAGE_SIZE]

    lines = ["🔎 **Группы аккаунта**", ""]
    if notice:
        lines.extend([notice, ""])
    lines.append("🛡 - аккаунт управляет группой; 👁 - только просмотр; ⚠ - недоступна")
    lines.append("")

    for _, title, username, is_admin, is_creator, available in chunk:
        if not available:
            icon = "⚠"
        elif is_creator or is_admin:
            icon = "🛡"
        else:
            icon = "👁"
        suffix = f" ({username})" if username else " (закрытая)"
        lines.append(f"{icon} {title}{suffix}")

    lines.extend([
        "",
        f"Найдено: {len(rows)}",
        "Автоматически добавляются в рабочий список только группы, которыми аккаунт управляет.",
    ])
    await render_menu(event, "\n".join(lines), buttons=_page_buttons(user_id, page, pages))


@bot.on(Query(data=lambda d: d.decode().startswith("sync_groups_")))
async def sync_groups(event: callback_query) -> None:
    if event.sender_id not in ADMIN_ID_LIST:
        await event.answer("Недоступно", alert=True)
        return
    user_id = int(event.data.decode().split("_")[-1])
    await event.answer()
    try:
        await render_menu(event, "⏳ Получаю список групп аккаунта...")
        stats = await _sync_groups(user_id)
        notice = (
            f"✅ Синхронизация завершена: {stats['total']} групп, "
            f"закрытых {stats['private']}, публичных {stats['public']}, "
            f"под управлением аккаунта {stats['managed']}."
        )
        await _render_discovered(event, user_id, 0, notice)
    except Exception as exc:
        logger.exception(f"Ошибка синхронизации групп аккаунта {user_id}: {exc}")
        await render_menu(
            event,
            f"⚠ Не удалось получить группы: {exc}",
            buttons=[
                [Button.inline("🔄 Повторить", f"sync_groups_{user_id}".encode())],
                [Button.inline("🏠 Главное меню", b"menu_home")],
            ],
        )


@bot.on(Query(data=lambda d: d.decode().startswith("discovered_groups_")))
async def discovered_groups(event: callback_query) -> None:
    if event.sender_id not in ADMIN_ID_LIST:
        await event.answer("Недоступно", alert=True)
        return
    parts = event.data.decode().split("_")
    user_id = int(parts[-2])
    page = int(parts[-1])
    await _render_discovered(event, user_id, page)
    await event.answer()


@bot.on(Query(data=b"noop"))
async def noop(event: callback_query) -> None:
    await event.answer()
