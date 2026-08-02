from __future__ import annotations

import html
import logging
import re
from collections.abc import Sequence

from aiogram import F, Router
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State
from aiogram.types import CallbackQuery, Message

from .ai_account_profiles_handlers import AIAccountProfilesHandlersMixin
from .ai_channel_memory_handlers import AIChannelMemoryHandlersMixin
from .ai_comment_generation_handlers import AICommentGenerationHandlersMixin
from .ai_dialogues_handlers import AIDialoguesHandlersMixin
from .ai_comments_keyboards import (
    AI_COMMENTS_CHANNEL_PAGE_SIZE,
    AI_COMMENTS_PLACEHOLDER_CALLBACKS,
    ai_comments_channel_keyboard,
    ai_comments_channel_list_keyboard,
    ai_comments_flag_confirm_keyboard,
    ai_comments_menu_keyboard,
    ai_comments_placeholder_keyboard,
    ai_comments_settings_keyboard,
)
from .ai_comments_repository import AI_COMMENTS_FLAG_KEYS
from .ai_comments_states import AICommentsUI
from .ai_comments_ui import activate_ai_comments_state
from .openai_gateway_handlers import OpenAIGatewayHandlersMixin
from .utils import truncate

logger = logging.getLogger("laika_bot.handlers.ai_comments")

MAX_AI_COMMENTS_PAGE = 1_000_000
MAX_DATABASE_ID = 9_223_372_036_854_775_807

AI_COMMENTS_PLACEHOLDER_SCREENS: dict[str, tuple[str, str, State]] = {
    "aic:history": (
        "💬 История комментариев",
        "История и reply-связи будут подключены на последующих шагах.",
        AICommentsUI.comment_history,
    ),
    "aic:stats": (
        "📊 Статистика AI Comments",
        "Usage, стоимость и очереди появятся на шаге 15.",
        AICommentsUI.statistics,
    ),
}


def parse_ai_comments_page_callback(data: str | None) -> int:
    if data == "aic:channels":
        return 0
    match = re.fullmatch(r"aic:channels:([0-9]+)", data or "")
    if match is None:
        raise ValueError("Некорректная страница")
    page = int(match.group(1))
    if page > MAX_AI_COMMENTS_PAGE:
        raise ValueError("Некорректная страница")
    return page


def parse_ai_comments_channel_callback(data: str | None) -> tuple[int, int]:
    match = re.fullmatch(r"aic:channel:([1-9][0-9]*):([0-9]+)", data or "")
    if match is None:
        raise ValueError("Некорректная кнопка канала")
    channel_id, page = int(match.group(1)), int(match.group(2))
    if channel_id > MAX_DATABASE_ID or page > MAX_AI_COMMENTS_PAGE:
        raise ValueError("Некорректная кнопка канала")
    return channel_id, page


def parse_ai_comments_flag_callback(data: str | None, *, action: str) -> bool:
    if action not in {"confirm", "set"}:
        raise ValueError("Некорректное действие настройки")
    match = re.fullmatch(rf"aic:flag:{action}:([01])", data or "")
    if match is None:
        raise ValueError("Некорректная кнопка настройки")
    return match.group(1) == "1"

AI_COMMENTS_FLAG_CODES = {"g": "ai_generation_enabled", "d": "ai_dialogues_enabled"}


def parse_ai_comments_named_flag_callback(
    data: str | None,
    *,
    action: str,
) -> tuple[str, str, bool]:
    if action not in {"confirm", "set"}:
        raise ValueError("Некорректное действие настройки")
    match = re.fullmatch(rf"aic:flag:{action}:([a-z]):([01])", data or "")
    if match is None or match.group(1) not in AI_COMMENTS_FLAG_CODES:
        raise ValueError("Некорректная кнопка настройки")
    code = match.group(1)
    return AI_COMMENTS_FLAG_CODES[code], code, match.group(2) == "1"


def parse_ai_comments_flag_target(
    data: str | None,
    *,
    action: str,
) -> tuple[str, str | None, bool]:
    try:
        return "ai_comments_enabled", None, parse_ai_comments_flag_callback(
            data, action=action
        )
    except ValueError:
        return parse_ai_comments_named_flag_callback(data, action=action)


def paginate_ai_comments_channels(
    channels: Sequence[object], page: int
) -> tuple[list[object], int, int]:
    total_pages = max(
        1,
        (len(channels) + AI_COMMENTS_CHANNEL_PAGE_SIZE - 1)
        // AI_COMMENTS_CHANNEL_PAGE_SIZE,
    )
    safe_page = min(max(0, page), total_pages - 1)
    start = safe_page * AI_COMMENTS_CHANNEL_PAGE_SIZE
    return (
        list(channels[start : start + AI_COMMENTS_CHANNEL_PAGE_SIZE]),
        safe_page,
        total_pages,
    )


def _status_label(value: bool) -> str:
    return "🟢 ВКЛ" if value else "🔴 ВЫКЛ"


class AICommentsHandlersMixin(
    AIDialoguesHandlersMixin,
    AICommentGenerationHandlersMixin,
    OpenAIGatewayHandlersMixin,
    AIChannelMemoryHandlersMixin,
    AIAccountProfilesHandlersMixin,
):
    """AI Comments memory, profiles and a fail-closed OpenAI DEV gateway."""

    def _register_ai_comments_handlers(self, router: Router) -> None:
        router.callback_query.register(self.ai_comments_menu, F.data == "aic:menu")
        router.callback_query.register(
            self.ai_comments_channels, F.data == "aic:channels"
        )
        router.callback_query.register(
            self.ai_comments_channels, F.data.startswith("aic:channels:")
        )
        router.callback_query.register(
            self.ai_comments_channel, F.data.startswith("aic:channel:")
        )
        self._register_ai_channel_memory_handlers(router)
        self._register_ai_account_profile_handlers(router)
        self._register_openai_gateway_handlers(router)
        self._register_ai_comment_generation_handlers(router)
        self._register_ai_dialogue_handlers(router)
        router.callback_query.register(
            self.ai_comments_settings, F.data == "aic:settings"
        )
        router.callback_query.register(
            self.ai_comments_flag_confirm,
            F.data.startswith("aic:flag:confirm:"),
        )
        router.callback_query.register(
            self.ai_comments_flag_set,
            F.data.startswith("aic:flag:set:"),
        )
        router.callback_query.register(
            self.ai_comments_placeholder,
            F.data.in_(AI_COMMENTS_PLACEHOLDER_CALLBACKS),
        )
        # This namespace-only fallback must remain last among aic handlers.
        router.callback_query.register(
            self.ai_comments_stale, F.data.startswith("aic:")
        )

    async def _read_ai_comments_flags_fail_closed(self) -> tuple[dict[str, bool], bool]:
        try:
            flags = await self.db.get_ai_comments_flags()
            return flags, False
        except Exception:  # noqa: BLE001
            logger.exception("AI Comments settings could not be read")
            return {key: False for key in AI_COMMENTS_FLAG_KEYS}, True

    async def _render_ai_comments_menu(self, message: Message) -> None:
        flags, settings_error = await self._read_ai_comments_flags_fail_closed()
        stored_enabled = flags["ai_comments_enabled"]
        deployment_enabled = bool(self.settings.ai_comments_enabled)
        effective_enabled = stored_enabled and deployment_enabled and not settings_error
        generation_stored = flags["ai_generation_enabled"]
        generation_deployment = bool(getattr(self.settings, "ai_generation_enabled", False))
        generation_effective = (
            effective_enabled
            and generation_stored
            and generation_deployment
            and self.openai_gateway.status.ready
        )
        dialogue_stored = flags["ai_dialogues_enabled"]
        dialogue_deployment = bool(getattr(self.settings, "ai_dialogues_enabled", False))
        dialogue_effective = (
            generation_effective and dialogue_stored and dialogue_deployment
        )

        warning = (
            "\n\n⚠️ Настройки БД временно недоступны. Модуль принудительно считается выключенным."
            if settings_error
            else ""
        )
        await self._safe_edit_text(
            message,
            "💬 <b>Комментарии</b>\n"
            "<i>Шаг 12 - конечные связанные диалоги без публикации</i>\n\n"
            "━━━━━━━━━━━━━━\n"
            f"💾 Настройка в БД: <b>{_status_label(stored_enabled)}</b>\n"
            f"🛡 Разрешение Railway: <b>{_status_label(deployment_enabled)}</b>\n"
            f"⚙️ Эффективный флаг: <b>{_status_label(effective_enabled)}</b>\n"
            "━━━━━━━━━━━━━━\n\n"
            f"✍️ Генерация черновика: <b>{_status_label(generation_effective)}</b>\n"
            f"🗣 Связанные диалоги: <b>{_status_label(dialogue_effective)}</b>\n\n"
            "Доступны одиночные черновики и конечные связанные диалоги из 2-5 профилей. "
            "Каждая реплика создаётся и принимается вручную. Публикация физически отсутствует."
            f"{warning}",
            reply_markup=ai_comments_menu_keyboard(),
        )

    async def _render_ai_comments_settings(self, message: Message) -> None:
        flags, settings_error = await self._read_ai_comments_flags_fail_closed()
        stored_enabled = flags["ai_comments_enabled"]
        deployment_enabled = bool(self.settings.ai_comments_enabled)
        effective_enabled = stored_enabled and deployment_enabled and not settings_error
        generation_stored = flags["ai_generation_enabled"]
        generation_deployment = bool(getattr(self.settings, "ai_generation_enabled", False))
        generation_effective = (
            effective_enabled
            and generation_stored
            and generation_deployment
            and self.openai_gateway.status.ready
        )
        dialogue_stored = flags["ai_dialogues_enabled"]
        dialogue_deployment = bool(getattr(self.settings, "ai_dialogues_enabled", False))
        dialogue_effective = generation_effective and dialogue_stored and dialogue_deployment
        locked_flags = (
            ("Публикация", flags["ai_publication_enabled"]),
        )
        locked_lines = "\n".join(
            f"- {label}: <b>{_status_label(value)}</b>"
            for label, value in locked_flags
        )
        error_line = (
            "\n\n⚠️ Чтение настроек завершилось ошибкой. Изменение заблокировано."
            if settings_error
            else ""
        )
        gateway = self.openai_gateway.status
        gateway_ready = gateway.ready
        gateway_label = "🟢 ГОТОВ" if gateway_ready else "🔴 ЗАБЛОКИРОВАН"
        gateway_key = "🟢 НАСТРОЕН" if gateway.key_configured else "🔴 НЕТ"
        gateway_railway = "🟢 ВКЛ" if gateway.railway_enabled else "🔴 ВЫКЛ"
        await self._safe_edit_text(
            message,
            "⚙️ <b>Настройки AI Comments</b>\n\n"
            f"Настройка администратора: <b>{_status_label(stored_enabled)}</b>\n"
            f"Railway kill switch: <b>{_status_label(deployment_enabled)}</b>\n"
            f"Эффективный флаг: <b>{_status_label(effective_enabled)}</b>\n\n"
            "🔌 <b>OpenAI Gateway</b>\n"
            f"Статус: <b>{gateway_label}</b>\n"
            f"Railway-разрешение: <b>{gateway_railway}</b>\n"
            f"API-ключ: <b>{gateway_key}</b>\n"
            f"Модель DEV-теста: <code>{html.escape(gateway.model)}</code>\n"
            f"SDK: <code>{html.escape(gateway.sdk_version)}</code>\n"
            f"Тайм-аут / повторы: <b>{gateway.timeout_seconds:g} сек / {gateway.max_retries}</b>\n\n"
            f"🗣 Диалоги: <b>{_status_label(dialogue_effective)}</b>\n"
            f"Сброс дневных лимитов: <b>00:00 {html.escape(getattr(self.settings, "ai_comments_timezone", "Europe/Moscow"))}</b>\n\n"
            "🔒 <b>Недоступно до шага 15</b>\n"
            f"{locked_lines}\n\n"
            "Флаги сохраняются в БД, а Railway-переменные остаются аварийными выключателями. "
            "На шаге 12 доступны конечные диалоги-черновики; публикация заблокирована."
            f"{error_line}",
            reply_markup=ai_comments_settings_keyboard(
                stored_enabled=stored_enabled,
                generation_stored_enabled=generation_stored,
                dialogue_stored_enabled=dialogue_stored,
                editable=not settings_error,
            ),
        )

    async def _show_ai_comments_stale(
        self,
        callback: CallbackQuery,
        state: FSMContext,
        *,
        notice: str = "Эта кнопка устарела. Меню обновлено.",
    ) -> None:
        if not await activate_ai_comments_state(callback, state, AICommentsUI.menu):
            return
        await state.update_data(ai_comments_flag_target=None)
        await self._render_ai_comments_menu(callback.message)
        await callback.answer(notice, show_alert=True)

    async def ai_comments_menu(
        self, callback: CallbackQuery, state: FSMContext
    ) -> None:
        if await self._deny_if_needed(callback):
            return
        if not await activate_ai_comments_state(callback, state, AICommentsUI.menu):
            return
        await state.update_data(ai_comments_flag_target=None)
        await self._render_ai_comments_menu(callback.message)
        await callback.answer()

    async def ai_comments_placeholder(
        self, callback: CallbackQuery, state: FSMContext
    ) -> None:
        if await self._deny_if_needed(callback):
            return
        screen = AI_COMMENTS_PLACEHOLDER_SCREENS.get(callback.data or "")
        if screen is None:
            await self._show_ai_comments_stale(callback, state)
            return
        title, description, target_state = screen
        if not await activate_ai_comments_state(callback, state, target_state):
            return
        await state.update_data(ai_comments_flag_target=None)
        await self._safe_edit_text(
            callback.message,
            f"{title}\n\n"
            "✅ Экран и безопасная навигация готовы.\n"
            f"{description}\n\n"
            "Сейчас этот раздел ничего не генерирует, не публикует и не "
            "обращается к внешним API.",
            reply_markup=ai_comments_placeholder_keyboard(),
        )
        await callback.answer()

    async def ai_comments_channels(
        self, callback: CallbackQuery, state: FSMContext
    ) -> None:
        if await self._deny_if_needed(callback):
            return
        try:
            requested_page = parse_ai_comments_page_callback(callback.data)
        except ValueError:
            await self._show_ai_comments_stale(callback, state)
            return
        if not await activate_ai_comments_state(callback, state, AICommentsUI.channels):
            return
        await state.update_data(ai_comments_flag_target=None)

        channels = await self.db.list_channels(kind="channel")
        page_items, safe_page, total_pages = paginate_ai_comments_channels(
            channels, requested_page
        )
        buttons = [
            (int(channel.id), truncate(channel.title, 48), bool(channel.is_active))
            for channel in page_items
        ]
        page_line = (
            f"\nСтраница: <b>{safe_page + 1}/{total_pages}</b>"
            if total_pages > 1
            else ""
        )
        await self._safe_edit_text(
            callback.message,
            "📡 <b>Выбор канала</b>\n\n"
            f"Каналов: <b>{len(channels)}</b>{page_line}\n\n"
            + (
                "Выберите канал для будущей настройки AI Comments 👇"
                if channels
                else "Каналов пока нет. Сначала добавьте канал в основном меню."
            ),
            reply_markup=ai_comments_channel_list_keyboard(
                buttons, page=safe_page, total_pages=total_pages
            ),
        )
        if safe_page != requested_page:
            await callback.answer("Список изменился, показана доступная страница.")
        else:
            await callback.answer()

    async def ai_comments_channel(
        self, callback: CallbackQuery, state: FSMContext
    ) -> None:
        if await self._deny_if_needed(callback):
            return
        try:
            channel_id, page = parse_ai_comments_channel_callback(callback.data)
        except ValueError:
            await self._show_ai_comments_stale(callback, state)
            return
        if not await activate_ai_comments_state(callback, state, AICommentsUI.channel):
            return
        await state.update_data(ai_comments_flag_target=None)
        channel = await self.db.get_channel(channel_id)
        if channel is None or channel.kind != "channel":
            await self._show_ai_comments_stale(
                callback,
                state,
                notice="Канал больше недоступен. Список обновлён.",
            )
            return
        try:
            profile = await self.db.ensure_ai_channel_profile(channel_id)
            memory = await self.db.get_ai_channel_memory(channel_id)
        except Exception:  # noqa: BLE001
            logger.exception(
                "AI channel memory profile could not be selected target=%s",
                channel_id,
            )
            await self._show_ai_comments_stale(
                callback,
                state,
                notice="Память канала не удалось подготовить. Ничего не изменено.",
            )
            return
        await state.update_data(
            ai_comments_selected_channel_id=channel_id,
            ai_comments_selected_channel_page=page,
            ai_comments_memory_field=None,
            ai_comments_scenario_complete_id=None,
        )
        await self._safe_edit_text(
            callback.message,
            "📡 <b>Канал выбран</b>\n\n"
            f"Название: <b>{html.escape(channel.title)}</b>\n"
            f"ID LikeBot: <code>{channel.id}</code>\n"
            f"Telegram ID: <code>{channel.telegram_channel_id}</code>\n"
            f"Статус: <b>{'активен' if channel.is_active else 'выключен'}</b>\n"
            f"Версия памяти: <b>{profile.profile_version}</b>\n"
            f"Постов сохранено: <b>{memory.stored_post_count}</b>\n\n"
            "Профиль создан отдельно для этого канала. Откройте публикацию и нажмите "
            "«✍️ Создать один черновик». Автоматической генерации и публикации нет.",
            reply_markup=ai_comments_channel_keyboard(page=page),
        )
        await callback.answer()

    async def ai_comments_settings(
        self, callback: CallbackQuery, state: FSMContext
    ) -> None:
        if await self._deny_if_needed(callback):
            return
        if not await activate_ai_comments_state(callback, state, AICommentsUI.settings):
            return
        await state.update_data(ai_comments_flag_target=None)
        await self._render_ai_comments_settings(callback.message)
        await callback.answer()

    async def ai_comments_flag_confirm(
        self, callback: CallbackQuery, state: FSMContext
    ) -> None:
        if await self._deny_if_needed(callback):
            return
        try:
            key, flag_code, target_enabled = parse_ai_comments_flag_target(
                callback.data, action="confirm"
            )
        except ValueError:
            await self._show_ai_comments_stale(callback, state)
            return
        if not await activate_ai_comments_state(callback, state, AICommentsUI.settings):
            return
        if key == "ai_comments_enabled":
            await state.update_data(ai_comments_flag_target=target_enabled)
        else:
            await state.update_data(
                ai_comments_flag_key=key,
                ai_comments_flag_code=flag_code,
                ai_comments_flag_target=target_enabled,
            )
        action = "включить" if target_enabled else "выключить"
        label = (
            "AI Comments"
            if key == "ai_comments_enabled"
            else "генерацию одного черновика"
        )
        await self._safe_edit_text(
            callback.message,
            "⚙️ <b>Подтверждение настройки</b>\n\n"
            f"Вы хотите <b>{action}</b> {label} в базе.\n\n"
            "Railway-переменная остаётся главным аварийным выключателем. "
            "Диалоги и публикация остаются недоступны.",
            reply_markup=ai_comments_flag_confirm_keyboard(
                target_enabled=target_enabled,
                flag_code=flag_code,
            ),
        )
        await callback.answer()

    async def ai_comments_flag_set(
        self, callback: CallbackQuery, state: FSMContext
    ) -> None:
        if await self._deny_if_needed(callback):
            return
        try:
            key, flag_code, target_enabled = parse_ai_comments_flag_target(
                callback.data, action="set"
            )
        except ValueError:
            await self._show_ai_comments_stale(callback, state)
            return
        if not await activate_ai_comments_state(callback, state, AICommentsUI.settings):
            return
        confirmation = await state.get_data()
        confirmed_key = confirmation.get(
            "ai_comments_flag_key",
            "ai_comments_enabled",
        )
        confirmed_code = confirmation.get("ai_comments_flag_code")
        if (
            confirmed_key != key
            or confirmed_code != flag_code
            or confirmation.get("ai_comments_flag_target") is not target_enabled
        ):
            await self._show_ai_comments_stale(
                callback,
                state,
                notice="Подтверждение устарело. Настройка не изменена.",
            )
            return
        try:
            async with self._ai_comments_settings_lock:
                result = await self.db.set_ai_comments_flag(
                    key,
                    target_enabled,
                    updated_by=int(callback.from_user.id),
                )
        except Exception:  # noqa: BLE001
            logger.exception("AI Comments feature flag could not be updated key=%s", key)
            await state.update_data(
                ai_comments_flag_key=None,
                ai_comments_flag_code=None,
                ai_comments_flag_target=None,
            )
            await self._render_ai_comments_settings(callback.message)
            await callback.answer(
                "Настройку не удалось сохранить. Модуль оставлен в безопасном состоянии.",
                show_alert=True,
            )
            return
        await state.update_data(
            ai_comments_flag_key=None,
            ai_comments_flag_code=None,
            ai_comments_flag_target=None,
        )
        await self._render_ai_comments_settings(callback.message)
        status = "включена" if target_enabled else "выключена"
        suffix = "" if result["changed"] else " (уже была сохранена)"
        await callback.answer(f"Настройка {status}{suffix}", show_alert=True)

    async def ai_comments_stale(
        self, callback: CallbackQuery, state: FSMContext
    ) -> None:
        if await self._deny_if_needed(callback):
            return
        await self._show_ai_comments_stale(callback, state)
