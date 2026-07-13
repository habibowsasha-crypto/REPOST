from __future__ import annotations

from typing import Any

from loguru import logger

from config import (
    broadcast_all_state,
    broadcast_all_state_account,
    broadcast_all_text,
    broadcast_solo_state,
    code_waiting,
    password_waiting,
    phone_waiting,
    user_clients,
    user_sessions,
    user_sessions_deleting,
    user_states,
)


def is_command_event(event: Any) -> bool:
    """Return True when an incoming admin message is a slash command."""
    text = getattr(event, "raw_text", None) or getattr(event, "text", None) or ""
    return text.lstrip().startswith("/")


async def clear_admin_interaction_state(admin_id: int) -> int:
    """
    Clear all transient admin wizard states.

    This prevents a stale broadcast/group/account wizard from consuming unrelated
    slash commands as ordinary text or numeric input.
    """
    cleared = 0
    state_maps = (
        phone_waiting,
        code_waiting,
        password_waiting,
        broadcast_all_text,
        broadcast_all_state,
        broadcast_solo_state,
        broadcast_all_state_account,
        user_sessions,
        user_sessions_deleting,
        user_states,
    )

    for state_map in state_maps:
        if admin_id in state_map:
            state_map.pop(admin_id, None)
            cleared += 1

    # DM setup state lives in the DM module. Import lazily to avoid import cycles.
    try:
        from handlers.dm.dm_handlers import dm_setup_state

        if admin_id in dm_setup_state:
            dm_setup_state.pop(admin_id, None)
            cleared += 1
    except Exception as exc:  # pragma: no cover - best effort during startup
        logger.debug(f"Не удалось очистить dm_setup_state для {admin_id}: {exc}")

    # A pending Telegram login client is transient too. Disconnect it cleanly.
    client = user_clients.pop(admin_id, None)
    if client is not None:
        try:
            if client.is_connected():
                await client.disconnect()
        except Exception as exc:  # pragma: no cover - best effort cleanup
            logger.debug(f"Не удалось отключить временный клиент {admin_id}: {exc}")
        cleared += 1

    if cleared:
        logger.info(f"Сброшено временных состояний админа {admin_id}: {cleared}")
    return cleared
