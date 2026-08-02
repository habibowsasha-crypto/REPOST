from __future__ import annotations

from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State
from aiogram.types import CallbackQuery

from .ai_comments_states import is_ai_comments_state


async def activate_ai_comments_state(
    callback: CallbackQuery,
    state: FSMContext,
    target: State,
) -> bool:
    """Move only within the isolated AI Comments FSM namespace."""

    message = getattr(callback, "message", None)
    if message is None or not callable(getattr(message, "edit_text", None)):
        await callback.answer(
            "Сообщение больше недоступно. Откройте /menu заново.",
            show_alert=True,
        )
        return False
    current = await state.get_state()
    if current is not None and not is_ai_comments_state(current):
        await callback.answer(
            "Сначала завершите текущее действие или используйте /cancel.",
            show_alert=True,
        )
        return False
    await state.set_state(target)
    return True
