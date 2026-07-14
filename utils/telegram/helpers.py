from __future__ import annotations

from typing import List, Optional, Union

from loguru import logger
from telethon import TelegramClient
from telethon.tl.types import (
    Channel,
    Chat,
    InputPeerChannel,
    InputPeerChat,
    PeerChannel,
    PeerChat,
)

from config import conn


def normalize_group_id(value: int | str) -> int:
    """Return Telegram's canonical positive chat/channel id.

    Telethon entities expose positive ids (for example ``3659991044``), while
    Bot API-style supergroup ids are often written as ``-1003659991044``.  The
    old project used ``abs()`` and produced ``1003659991044``, which cannot be
    resolved as a Telethon channel.  This helper removes the Bot API ``-100``
    prefix and keeps ordinary chat ids intact.
    """
    number = int(value)
    absolute = abs(number)
    digits = str(absolute)

    # Bot API supergroup/channel id: -100<telethon channel id>.  Positive values
    # with the same prefix can exist in old databases after the former abs().
    if digits.startswith("100") and (number <= -1_000_000_000_000 or absolute >= 1_000_000_000_000):
        stripped = digits[3:]
        if stripped:
            return int(stripped)
    return absolute


def gid_key(value: int | str) -> int:
    """Compatibility alias used by the legacy broadcast code."""
    return normalize_group_id(value)


def broadcast_status_emoji(user_id: int, group_id: int) -> str:
    group_key = gid_key(group_id)
    return (
        "✅ Активна"
        if group_key in get_active_broadcast_groups(user_id)
        else "❌ Закончена или не начата"
    )


def get_active_broadcast_groups(user_id: int) -> List[int]:
    cursor = conn.cursor()
    try:
        rows = cursor.execute(
            "SELECT group_id FROM broadcasts WHERE is_active = ? AND user_id = ?",
            (True, user_id),
        ).fetchall()
        return [gid_key(row[0]) for row in rows]
    finally:
        cursor.close()


def create_broadcast_data(
    user_id: int,
    group_id: int,
    text: str,
    interval_minutes: int,
    photo_url: str | None = None,
) -> None:
    """Create or update one ordinary broadcast record."""
    group_id_key = gid_key(group_id)
    with conn:
        existing = conn.execute(
            "SELECT 1 FROM broadcasts WHERE user_id = ? AND group_id = ? LIMIT 1",
            (user_id, group_id_key),
        ).fetchone()
        if existing:
            conn.execute(
                """
                UPDATE broadcasts
                SET broadcast_text = ?, interval_minutes = ?, is_active = ?,
                    photo_url = ?, error_reason = NULL
                WHERE user_id = ? AND group_id = ?
                """,
                (text, interval_minutes, True, photo_url, user_id, group_id_key),
            )
        else:
            conn.execute(
                """
                INSERT INTO broadcasts
                    (user_id, group_id, broadcast_text, interval_minutes,
                     is_active, photo_url, error_reason)
                VALUES (?, ?, ?, ?, ?, ?, NULL)
                """,
                (user_id, group_id_key, text, interval_minutes, True, photo_url),
            )


def _discovered_peer_data(user_id: int, group_id: int):
    cursor = conn.cursor()
    try:
        return cursor.execute(
            """
            SELECT username, access_hash, peer_type, is_available
            FROM discovered_groups
            WHERE user_id = ? AND group_id = ?
            """,
            (user_id, gid_key(group_id)),
        ).fetchone()
    finally:
        cursor.close()


async def get_entity_by_id(
    client: TelegramClient,
    group_id: int,
    *,
    user_id: int | None = None,
    identifier: str | None = None,
) -> Optional[Union[Channel, Chat]]:
    """Resolve a group for public and private memberships.

    When ``user_id`` is supplied, the saved ``access_hash`` from group discovery
    is used.  This is essential for private supergroups after a Railway restart,
    because a ``StringSession`` does not preserve Telethon's entity cache.
    """
    group_key = gid_key(group_id)
    candidates: list[object] = []

    if user_id is not None:
        row = _discovered_peer_data(user_id, group_key)
        if row:
            username, access_hash, peer_type, is_available = row
            if not is_available:
                return None
            if username:
                candidates.append(username)
            if peer_type == "channel" and access_hash is not None:
                candidates.append(InputPeerChannel(group_key, int(access_hash)))
            elif peer_type == "chat":
                candidates.append(InputPeerChat(group_key))

    if identifier:
        candidates.append(identifier)

    # Public groups or entities already known by this live client may resolve by
    # PeerChannel/PeerChat.  Keep direct-id fallbacks for old database rows.
    candidates.extend(
        [
            PeerChannel(group_key),
            PeerChat(group_key),
            InputPeerChat(group_key),
            group_key,
        ]
    )

    seen: set[str] = set()
    for candidate in candidates:
        marker = repr(candidate)
        if marker in seen:
            continue
        seen.add(marker)
        try:
            entity = await client.get_entity(candidate)
            if isinstance(entity, (Channel, Chat)):
                return entity
        except Exception as exc:
            logger.debug(
                f"Не удалось получить entity группы {group_key} через {type(candidate).__name__}: {exc}"
            )

    logger.error(f"Не удалось получить entity для group_id={group_key}")
    return None
