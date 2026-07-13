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
                title = (
                    getattr(dialog, "name", None)
                    or getattr(entity, "title", None)
                    or str(group_id)
                ).strip()
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

                if is_admin or is_creator:
                    identifier = username or str(group_id)
                    cursor.execute(
                        """
                        INSERT OR IGNORE INTO groups (user_id, group_id, group_username)
                        VALUES (?, ?, ?)
                        """,
                        (user_id, group_id, identifier),
                    )
                    cursor.execute(
                        """
                        UPDATE groups SET group_username = ?
                        WHERE user_id = ? AND group_id = ?
                        """,
                        (identifier, user_id, group_id),
                    )

            cursor.execute(
                """
                DELETE FROM groups
                WHERE user_id = ?
                  AND group_id IN (
                      SELECT group_id FROM discovered_groups
                      WHERE user_id = ?
                        AND (is_available = 0 OR (is_admin = 0 AND is_creator = 0))
                  )
                """,
                (user_id, user_id),
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
            SELECT
                d.group_id,
                d.title,
                d.username,
                d.is_admin,
                d.is_creator,
                d.is_available,
                EXISTS(
                    SELECT 1 FROM groups AS g
                    WHERE g.user_id = d.user_id AND g.group_id = d.group_id
                ) AS in_working_list
            FROM discovered_groups AS d
            WHERE d.user_id = ?
            ORDER BY
                d.is_available DESC,
                d.is_creator DESC,
                d.is_admin DESC,
                in_working_list DESC,
                lower(d.title)
            """,
            (user_id,),
        ).fetchall()
    finally:
        cursor.close()


def _page_buttons(user_id: int, page: int, pages: int, rows):
    buttons = []
    for group_id, title, _username, is_admin, is_creator, available, in_work in rows:
        if not available:
            icon = "⚠"
        elif in_work:
            icon = "✅"
        elif is_creator or is_admin:
            icon = "🛡"
        else:
            icon = "👁"
        short_title = title if len(title) <= 38 else title[:35] + "..."
        buttons.append([
            Button.inline(
                f"{icon} {short_title}",
                f"discovered_group_{user_id}_{group_id}_{page}".encode(),
            )
        ])

    nav = []
    if page > 0:
        nav.append(Button.inline("⬅️", f"discovered_groups_{user_id}_{page - 1}".encode()))
    nav.append(Button.inline(f"{page + 1}/{max(pages, 1)}", b"noop"))
    if page + 1 < pages:
        nav.append(Button.inline("➡️", f"discovered_groups_{user_id}_{page + 1}".encode()))
    buttons.append(nav)
    buttons.extend([
        [Button.inline("🔄 Обновить", f"sync_groups_{user_id}".encode())],
        [Button.inline("◀️ К аккаунту", f"account_info_{user_id}".encode())],
        [Button.inline("🏠 Главное меню", b"menu_home")],
    ])
    return buttons


async def _render_discovered(event, user_id: int, page: int = 0, notice: str = "") -> None:
    rows = _list_rows(user_id)
    if not rows:
        text = (
            "🔎 **Группы аккаунта**\n\n"
            "Список ещё не синхронизирован. Нажмите «Найти группы», чтобы получить "
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
    lines.extend([
        "✅ - в рабочем списке",
        "🛡 - аккаунт управляет группой",
        "👁 - только просмотр",
        "⚠ - группа недоступна",
        "",
        "Нажмите на группу, чтобы открыть подробности.",
        "В рабочий список можно включать только группы, которыми аккаунт управляет.",
        "",
        f"Найдено: {len(rows)}",
    ])
    await render_menu(
        event,
        "\n".join(lines),
        buttons=_page_buttons(user_id, page, pages, chunk),
    )


def _get_discovered_group(user_id: int, group_id: int):
    cursor = conn.cursor()
    try:
        return cursor.execute(
            """
            SELECT
                d.group_id, d.title, d.username, d.access_hash, d.peer_type,
                d.is_admin, d.is_creator, d.is_available, d.last_seen_at,
                EXISTS(
                    SELECT 1 FROM groups AS g
                    WHERE g.user_id = d.user_id AND g.group_id = d.group_id
                ) AS in_working_list
            FROM discovered_groups AS d
            WHERE d.user_id = ? AND d.group_id = ?
            """,
            (user_id, group_id),
        ).fetchone()
    finally:
        cursor.close()


async def _render_group_details(
    event,
    user_id: int,
    group_id: int,
    page: int = 0,
    notice: str = "",
) -> None:
    row = _get_discovered_group(user_id, group_id)
    if not row:
        await render_menu(
            event,
            "⚠ Группа не найдена в результатах синхронизации.",
            buttons=[
                [Button.inline("◀️ Назад", f"discovered_groups_{user_id}_{page}".encode())],
                [Button.inline("🏠 Главное меню", b"menu_home")],
            ],
        )
        return

    (
        _gid, title, username, access_hash, peer_type,
        is_admin, is_creator, available, last_seen_at, in_work,
    ) = row
    managed = bool(is_admin or is_creator)
    access_text = "владелец" if is_creator else ("администратор" if is_admin else "только участник")
    visibility = "публичная" if username else "закрытая"
    status = "доступна" if available else "недоступна"
    working = "да" if in_work else "нет"

    lines = ["👥 **Группа аккаунта**", ""]
    if notice:
        lines.extend([notice, ""])
    lines.extend([
        f"Название: **{title}**",
        f"Username: {username or 'нет'}",
        f"ID: `{group_id}`",
        f"Тип: {peer_type}, {visibility}",
        f"Доступ: {access_text}",
        f"Статус: {status}",
        f"В рабочем списке: {working}",
        f"Access hash: {'сохранён' if access_hash is not None else 'нет'}",
        f"Последняя синхронизация: {(last_seen_at or 'нет')[:19]}",
    ])

    buttons = []
    if available and managed:
        if in_work:
            buttons.append([
                Button.inline(
                    "➖ Убрать из рабочего списка",
                    f"work_group_remove_{user_id}_{group_id}_{page}".encode(),
                )
            ])
        else:
            buttons.append([
                Button.inline(
                    "➕ Добавить в рабочий список",
                    f"work_group_add_{user_id}_{group_id}_{page}".encode(),
                )
            ])
    elif available:
        lines.extend([
            "",
            "ℹ️ Эта группа доступна аккаунту только как участнику. Для автоматических "
            "действий она остаётся в режиме просмотра.",
        ])
        buttons.append([Button.inline("👁 Только просмотр", b"noop")])
    else:
        buttons.append([Button.inline("⚠ Недоступна", b"noop")])

    buttons.extend([
        [Button.inline("◀️ К найденным группам", f"discovered_groups_{user_id}_{page}".encode())],
        [Button.inline("◀️ К аккаунту", f"account_info_{user_id}".encode())],
        [Button.inline("🏠 Главное меню", b"menu_home")],
    ])
    await render_menu(event, "\n".join(lines), buttons=buttons)


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


@bot.on(Query(data=lambda d: d.decode().startswith("discovered_group_")))
async def discovered_group_details(event: callback_query) -> None:
    if event.sender_id not in ADMIN_ID_LIST:
        await event.answer("Недоступно", alert=True)
        return
    parts = event.data.decode().split("_")
    user_id = int(parts[-3])
    group_id = int(parts[-2])
    page = int(parts[-1])
    await _render_group_details(event, user_id, group_id, page)
    await event.answer()


@bot.on(Query(data=lambda d: d.decode().startswith("work_group_add_")))
async def add_working_group(event: callback_query) -> None:
    if event.sender_id not in ADMIN_ID_LIST:
        await event.answer("Недоступно", alert=True)
        return
    parts = event.data.decode().split("_")
    user_id = int(parts[-3])
    group_id = int(parts[-2])
    page = int(parts[-1])
    row = _get_discovered_group(user_id, group_id)
    if not row:
        await event.answer("Группа не найдена", alert=True)
        return
    _gid, _title, username, _hash, _type, is_admin, is_creator, available, _seen, _in_work = row
    if not available or not (is_admin or is_creator):
        await event.answer("Для этой группы доступен только просмотр", alert=True)
        return
    identifier = username or str(group_id)
    cursor = conn.cursor()
    try:
        cursor.execute(
            "INSERT OR IGNORE INTO groups (user_id, group_id, group_username) VALUES (?, ?, ?)",
            (user_id, group_id, identifier),
        )
        cursor.execute(
            "UPDATE groups SET group_username = ? WHERE user_id = ? AND group_id = ?",
            (identifier, user_id, group_id),
        )
        conn.commit()
    finally:
        cursor.close()
    await _render_group_details(event, user_id, group_id, page, "✅ Группа добавлена в рабочий список.")
    await event.answer()


@bot.on(Query(data=lambda d: d.decode().startswith("work_group_remove_")))
async def remove_working_group(event: callback_query) -> None:
    if event.sender_id not in ADMIN_ID_LIST:
        await event.answer("Недоступно", alert=True)
        return
    parts = event.data.decode().split("_")
    user_id = int(parts[-3])
    group_id = int(parts[-2])
    page = int(parts[-1])
    cursor = conn.cursor()
    try:
        cursor.execute(
            "DELETE FROM groups WHERE user_id = ? AND group_id = ?",
            (user_id, group_id),
        )
        conn.commit()
    finally:
        cursor.close()
    await _render_group_details(event, user_id, group_id, page, "✅ Группа убрана из рабочего списка.")
    await event.answer()


@bot.on(Query(data=b"noop"))
async def noop(event: callback_query) -> None:
    await event.answer()
