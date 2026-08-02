from __future__ import annotations

import html
import logging
import re
import uuid
from collections.abc import Mapping

from aiogram import F, Router
from aiogram.fsm.context import FSMContext
from aiogram.types import CallbackQuery

from .ai_channel_memory_handlers import selected_ai_channel_id
from .ai_comment_generation import AICommentGenerationError
from .ai_comments_keyboards import (
    ai_comments_menu_keyboard,
    ai_dialogue_cancel_confirm_keyboard,
    ai_dialogue_generation_confirm_keyboard,
    ai_dialogue_post_selection_keyboard,
    ai_dialogue_profile_selection_keyboard,
    ai_dialogue_review_keyboard,
    ai_dialogue_thread_keyboard,
    ai_dialogues_menu_keyboard,
)
from .ai_comments_states import AICommentsUI
from .ai_comments_ui import activate_ai_comments_state
from .ai_dialogues import (
    AI_DIALOGUE_PROMPT_VERSION,
    AI_DIALOGUE_SCHEMA_VERSION,
    AIDialogueError,
    AIDialogueThreadSnapshot,
    validate_dialogue_reply_output,
)
from .openai_gateway import OpenAIGatewayError
from .utils import truncate

logger = logging.getLogger("laika_bot.handlers.ai_dialogues")
MAX_DATABASE_ID = 9_223_372_036_854_775_807


def _safe(value: object, limit: int = 500) -> str:
    return truncate(html.escape(str(value or "")), limit)


def _state_int(data: Mapping[str, object], key: str) -> int | None:
    value = data.get(key)
    if (
        not isinstance(value, int)
        or isinstance(value, bool)
        or not 0 < value <= MAX_DATABASE_ID
    ):
        return None
    return value


def _callback_id(data: str | None, prefix: str) -> int:
    match = re.fullmatch(re.escape(prefix) + r"([1-9][0-9]*)", data or "")
    if match is None:
        raise ValueError("Некорректная кнопка")
    value = int(match.group(1))
    if value > MAX_DATABASE_ID:
        raise ValueError("Некорректная кнопка")
    return value


def _callback_pair(data: str | None, prefix: str) -> tuple[int, int]:
    match = re.fullmatch(
        re.escape(prefix) + r"([1-9][0-9]*):([1-9][0-9]*)", data or ""
    )
    if match is None:
        raise ValueError("Некорректная кнопка")
    left, right = map(int, match.groups())
    if left > MAX_DATABASE_ID or right > MAX_DATABASE_ID:
        raise ValueError("Некорректная кнопка")
    return left, right


def _status_label(value: str) -> str:
    return {
        "planned": "🟢 готов к следующей реплике",
        "generating": "🟡 генерация",
        "review": "🟠 ожидает проверки",
        "completed": "✅ завершён",
        "cancelled": "⛔ отменён",
    }.get(value, value)


def _thread_text(thread: AIDialogueThreadSnapshot, *, notice: str | None = None) -> str:
    lines = []
    for message in thread.messages:
        reply = (
            f" ↳ reply #{message.reply_to_local_message_id}"
            if message.reply_to_local_message_id is not None
            else " ↳ пост"
        )
        lines.append(
            f"{message.position}. <b>{_safe(message.account_profile_name or message.account_profile_id, 100)}</b>"
            f"{reply}\n{_safe(message.text, 420)}"
        )
    dialogue = "\n\n".join(lines) if lines else "<i>реплик пока нет</i>"
    notice_line = f"\n\n⚠️ {_safe(notice, 500)}" if notice else ""
    return (
        "🗣 <b>Связанный диалог</b>\n"
        "<i>Шаг 12: конечный план, только черновики</i>\n\n"
        f"Диалог: <code>#{thread.id}</code>\n"
        f"Публикация: <code>#{thread.telegram_message_id or 'удалена'}</code>\n"
        f"Статус: <b>{_status_label(thread.status)}</b>\n"
        f"Прогресс: <b>{thread.accepted_messages}/{thread.max_messages}</b>\n"
        f"Следующая позиция: <b>{thread.next_position}</b>\n"
        f"Интервалы будущей публикации: <b>{thread.min_interval_seconds}-{thread.max_interval_seconds}с</b>\n"
        f"Участников: <b>{len(thread.participant_profile_ids)}</b>\n\n"
        "<b>Одобренная ветка</b>\n"
        f"{dialogue}{notice_line}\n\n"
        "Автоматическая отправка в Telegram отсутствует. Каждая реплика создаётся "
        "отдельным подтверждённым запросом и принимается вручную."
    )


class AIDialoguesHandlersMixin:
    """Step 12 finite dialogue drafts; no publication worker."""

    def _register_ai_dialogue_handlers(self, router: Router) -> None:
        router.callback_query.register(self.ai_dialogues_menu, F.data == "aic:dialogue")
        router.callback_query.register(self.ai_dialogue_new, F.data == "aic:dlg:new")
        router.callback_query.register(
            self.ai_dialogue_select_post, F.data.startswith("aic:dlg:post:")
        )
        router.callback_query.register(
            self.ai_dialogue_toggle_profile, F.data.startswith("aic:dlg:profile:")
        )
        router.callback_query.register(
            self.ai_dialogue_set_max, F.data.startswith("aic:dlg:max:")
        )
        router.callback_query.register(self.ai_dialogue_create, F.data == "aic:dlg:create")
        router.callback_query.register(
            self.ai_dialogue_thread, F.data.startswith("aic:dlg:thread:")
        )
        router.callback_query.register(
            self.ai_dialogue_next, F.data.startswith("aic:dlg:next:")
        )
        router.callback_query.register(self.ai_dialogue_run, F.data == "aic:dlg:run")
        router.callback_query.register(
            self.ai_dialogue_review, F.data.startswith("aic:dlg:review:")
        )
        router.callback_query.register(
            self.ai_dialogue_accept, F.data.startswith("aic:dlg:accept:")
        )
        router.callback_query.register(
            self.ai_dialogue_reject, F.data.startswith("aic:dlg:reject:")
        )
        router.callback_query.register(
            self.ai_dialogue_cancel_confirm, F.data.startswith("aic:dlg:cancel:")
        )
        router.callback_query.register(
            self.ai_dialogue_cancel, F.data.startswith("aic:dlg:canceldo:")
        )

    async def _ai_dialogue_readiness(self) -> tuple[bool, list[str]]:
        reasons: list[str] = []
        try:
            flags = await self.db.get_ai_comments_flags()
        except Exception:  # noqa: BLE001
            logger.exception("AI dialogue flags could not be read")
            return False, ["Настройки AI Comments в БД недоступны"]
        if not flags.get("ai_comments_enabled", False):
            reasons.append("AI Comments выключен в настройках бота")
        if not flags.get("ai_generation_enabled", False):
            reasons.append("Генерация черновиков выключена в настройках бота")
        if not flags.get("ai_dialogues_enabled", False):
            reasons.append("Диалоги выключены в настройках бота")
        if not getattr(self.settings, "ai_comments_enabled", False):
            reasons.append("AI_COMMENTS_ENABLED=false в Railway")
        if not getattr(self.settings, "ai_generation_enabled", False):
            reasons.append("AI_GENERATION_ENABLED=false в Railway")
        if not getattr(self.settings, "ai_dialogues_enabled", False):
            reasons.append("AI_DIALOGUES_ENABLED=false в Railway")
        if not self.openai_gateway.status.ready:
            reasons.append("OpenAI Gateway выключен или не готов")
        return not reasons, reasons

    async def _show_dialogue_blocked(
        self, callback: CallbackQuery, state: FSMContext, reasons: list[str]
    ) -> None:
        await state.set_state(AICommentsUI.dialogue)
        body = "\n".join(f"- {_safe(item, 300)}" for item in reasons)
        await self._safe_edit_text(
            callback.message,
            "🔒 <b>Связанные диалоги заблокированы</b>\n\n"
            f"{body}\n\n"
            "Для теста шага 12 нужны общий модуль, генерация, диалоги и Gateway. "
            "Публикация должна оставаться выключенной.",
            reply_markup=ai_comments_menu_keyboard(),
        )
        await callback.answer("Диалоги пока заблокированы", show_alert=True)

    async def ai_dialogues_menu(self, callback: CallbackQuery, state: FSMContext) -> None:
        if await self._deny_if_needed(callback):
            return
        if not await activate_ai_comments_state(callback, state, AICommentsUI.dialogue):
            return
        channel_id = selected_ai_channel_id(await state.get_data())
        threads = await self.db.list_ai_dialogue_threads(channel_id=channel_id, limit=20)
        buttons = [
            (
                item.id,
                truncate(
                    f"{_status_label(item.status)} · пост #{item.telegram_message_id or '-'} · {item.accepted_messages}/{item.max_messages}",
                    58,
                ),
            )
            for item in threads
        ]
        await self._safe_edit_text(
            callback.message,
            "🗣 <b>Связанные диалоги</b>\n\n"
            f"Выбранный канал: <b>{'да' if channel_id is not None else 'нет'}</b>\n"
            f"Диалогов: <b>{len(threads)}</b>\n\n"
            "Диалог состоит из 2-5 профилей и 2-5 конечных реплик. "
            "Каждая следующая реплика отвечает на предыдущую и создаётся только вручную.",
            reply_markup=ai_dialogues_menu_keyboard(buttons, has_channel=channel_id is not None),
        )
        await callback.answer()

    async def ai_dialogue_new(self, callback: CallbackQuery, state: FSMContext) -> None:
        if await self._deny_if_needed(callback):
            return
        if not await activate_ai_comments_state(callback, state, AICommentsUI.dialogue_post):
            return
        channel_id = selected_ai_channel_id(await state.get_data())
        if channel_id is None:
            await callback.answer("Сначала выберите канал", show_alert=True)
            return
        memory = await self.db.get_ai_channel_memory(channel_id)
        posts = [item for item in memory.recent_posts if item.deleted_at is None]
        buttons = [
            (
                item.id,
                truncate(
                    f"#{item.telegram_message_id} · {(item.text or item.media_caption or '[медиа]')}",
                    58,
                ),
            )
            for item in posts[:10]
        ]
        await state.update_data(
            ai_dialogue_post_id=None,
            ai_dialogue_profile_ids=[],
            ai_dialogue_max_messages=int(self.settings.ai_dialogue_max_messages),
            ai_dialogue_thread_id=None,
            ai_dialogue_nonce=None,
        )
        await self._safe_edit_text(
            callback.message,
            "🗣 <b>Новый диалог</b>\n\nВыберите корневую публикацию:",
            reply_markup=ai_dialogue_post_selection_keyboard(buttons),
        )
        await callback.answer()

    async def _render_dialogue_profiles(
        self,
        callback: CallbackQuery,
        state: FSMContext,
        *,
        notice: str | None = None,
    ) -> None:
        data = await state.get_data()
        post_id = _state_int(data, "ai_dialogue_post_id")
        channel_id = selected_ai_channel_id(data)
        selected_raw = data.get("ai_dialogue_profile_ids", [])
        selected = [
            int(item)
            for item in selected_raw
            if isinstance(item, int) and not isinstance(item, bool) and item > 0
        ] if isinstance(selected_raw, list) else []
        max_messages = data.get("ai_dialogue_max_messages", 5)
        if not isinstance(max_messages, int) or isinstance(max_messages, bool):
            max_messages = 5
        if post_id is None or channel_id is None:
            await callback.answer("Форма устарела", show_alert=True)
            return
        post = await self.db.get_ai_channel_post(channel_id, post_id)
        if post is None or post.deleted_at is not None:
            await callback.answer("Публикация недоступна", show_alert=True)
            return
        profiles = [
            item
            for item in await self.db.list_ai_account_profiles()
            if item.enabled and not item.retired
        ]
        buttons = []
        for profile in profiles:
            eligibility = await self.db.get_ai_account_profile_eligibility(
                profile.id,
                timezone_name=getattr(self.settings, "ai_comments_timezone", "Europe/Moscow"),
                reply_context=False,
            )
            capacity = max(0, eligibility.daily_limit - eligibility.comments_today)
            label = truncate(
                f"{profile.name} · {eligibility.comments_today}/{eligibility.daily_limit} · +{profile.reply_bonus_slots} ответов",
                52,
            )
            buttons.append((profile.id, label, eligibility.allowed, capacity > 0))
        notice_line = f"\n\n⚠️ {_safe(notice, 400)}" if notice else ""
        await self._safe_edit_text(
            callback.message,
            "🗣 <b>Участники диалога</b>\n\n"
            f"Публикация: <code>#{post.telegram_message_id}</code>\n"
            f"Выбрано профилей: <b>{len(selected)}/5</b>\n"
            f"Реплик в плане: <b>{max_messages}</b>\n"
            f"Сброс дневных лимитов: <b>00:00 {html.escape(getattr(self.settings, "ai_comments_timezone", "Europe/Moscow"))}</b>\n\n"
            "Выберите 2-5 разных профилей. План идёт по кругу, каждая новая реплика "
            "отвечает на предыдущую. Бонусные слоты доступны только после реального "
            "входящего ответа на опубликованный комментарий и не используются для заранее созданного диалога."
            f"{notice_line}",
            reply_markup=ai_dialogue_profile_selection_keyboard(
                buttons,
                selected_ids=selected,
                max_messages=max_messages,
            ),
        )

    async def ai_dialogue_select_post(self, callback: CallbackQuery, state: FSMContext) -> None:
        if await self._deny_if_needed(callback):
            return
        try:
            post_id = _callback_id(callback.data, "aic:dlg:post:")
        except ValueError:
            await callback.answer("Кнопка устарела", show_alert=True)
            return
        if not await activate_ai_comments_state(callback, state, AICommentsUI.dialogue_profiles):
            return
        channel_id = selected_ai_channel_id(await state.get_data())
        if channel_id is None:
            await callback.answer("Сначала выберите канал", show_alert=True)
            return
        post = await self.db.get_ai_channel_post(channel_id, post_id)
        if post is None or post.deleted_at is not None:
            await callback.answer("Публикация недоступна", show_alert=True)
            return
        await state.update_data(
            ai_dialogue_post_id=post_id,
            ai_dialogue_profile_ids=[],
            ai_dialogue_max_messages=int(self.settings.ai_dialogue_max_messages),
        )
        await self._render_dialogue_profiles(callback, state)
        await callback.answer()

    async def ai_dialogue_toggle_profile(self, callback: CallbackQuery, state: FSMContext) -> None:
        if await self._deny_if_needed(callback):
            return
        try:
            profile_id = _callback_id(callback.data, "aic:dlg:profile:")
        except ValueError:
            await callback.answer("Кнопка устарела", show_alert=True)
            return
        data = await state.get_data()
        selected_raw = data.get("ai_dialogue_profile_ids", [])
        selected = [
            int(item)
            for item in selected_raw
            if isinstance(item, int) and not isinstance(item, bool) and item > 0
        ] if isinstance(selected_raw, list) else []
        notice = None
        if profile_id in selected:
            selected.remove(profile_id)
        elif len(selected) >= 5:
            notice = "Можно выбрать не больше пяти профилей."
        else:
            eligibility = await self.db.get_ai_account_profile_eligibility(
                profile_id,
                timezone_name=getattr(self.settings, "ai_comments_timezone", "Europe/Moscow"),
                reply_context=False,
            )
            if not eligibility.allowed:
                notice = f"Профиль недоступен: {eligibility.reason}"
            else:
                selected.append(profile_id)
        await state.update_data(ai_dialogue_profile_ids=selected)
        await self._render_dialogue_profiles(callback, state, notice=notice)
        await callback.answer()

    async def ai_dialogue_set_max(self, callback: CallbackQuery, state: FSMContext) -> None:
        if await self._deny_if_needed(callback):
            return
        match = re.fullmatch(r"aic:dlg:max:([2-5])", callback.data or "")
        if match is None:
            await callback.answer("Кнопка устарела", show_alert=True)
            return
        await state.update_data(ai_dialogue_max_messages=int(match.group(1)))
        await self._render_dialogue_profiles(callback, state)
        await callback.answer()

    async def ai_dialogue_create(self, callback: CallbackQuery, state: FSMContext) -> None:
        if await self._deny_if_needed(callback):
            return
        ready, reasons = await self._ai_dialogue_readiness()
        if not ready:
            await self._show_dialogue_blocked(callback, state, reasons)
            return
        data = await state.get_data()
        channel_id = selected_ai_channel_id(data)
        post_id = _state_int(data, "ai_dialogue_post_id")
        selected_raw = data.get("ai_dialogue_profile_ids", [])
        profile_ids = tuple(
            int(item)
            for item in selected_raw
            if isinstance(item, int) and not isinstance(item, bool) and item > 0
        ) if isinstance(selected_raw, list) else ()
        max_messages = data.get("ai_dialogue_max_messages", 5)
        if channel_id is None or post_id is None or not isinstance(max_messages, int):
            await callback.answer("Форма устарела", show_alert=True)
            return
        if not 2 <= len(profile_ids) <= 5:
            await self._render_dialogue_profiles(
                callback, state, notice="Нужно выбрать от двух до пяти профилей."
            )
            await callback.answer("Выберите 2-5 профилей", show_alert=True)
            return
        try:
            thread = await self.db.create_ai_dialogue_thread(
                channel_id=channel_id,
                post_id=post_id,
                profile_ids=profile_ids,
                max_messages=max_messages,
                min_interval_seconds=self.settings.ai_dialogue_min_interval_seconds,
                max_interval_seconds=self.settings.ai_dialogue_max_interval_seconds,
                expires_hours=self.settings.ai_dialogue_expires_hours,
                created_by_admin_id=callback.from_user.id,
                timezone_name=getattr(self.settings, "ai_comments_timezone", "Europe/Moscow"),
            )
        except (AIDialogueError, ValueError) as exc:
            await self._render_dialogue_profiles(callback, state, notice=str(exc))
            await callback.answer("План не создан", show_alert=True)
            return
        await state.set_state(AICommentsUI.dialogue_thread)
        await state.update_data(ai_dialogue_thread_id=thread.id)
        await self._safe_edit_text(
            callback.message,
            _thread_text(thread, notice="План создан. Сгенерируйте первую реплику."),
            reply_markup=ai_dialogue_thread_keyboard(
                thread_id=thread.id,
                status=thread.status,
                pending_draft_id=thread.pending_draft_id,
            ),
        )
        await callback.answer("План диалога создан")

    async def ai_dialogue_thread(self, callback: CallbackQuery, state: FSMContext) -> None:
        if await self._deny_if_needed(callback):
            return
        try:
            thread_id = _callback_id(callback.data, "aic:dlg:thread:")
        except ValueError:
            await callback.answer("Кнопка устарела", show_alert=True)
            return
        if not await activate_ai_comments_state(callback, state, AICommentsUI.dialogue_thread):
            return
        thread = await self.db.get_ai_dialogue_thread(thread_id)
        if thread is None:
            await callback.answer("Диалог не найден", show_alert=True)
            return
        await state.update_data(ai_dialogue_thread_id=thread_id)
        await self._safe_edit_text(
            callback.message,
            _thread_text(thread),
            reply_markup=ai_dialogue_thread_keyboard(
                thread_id=thread.id,
                status=thread.status,
                pending_draft_id=thread.pending_draft_id,
            ),
        )
        await callback.answer()

    async def ai_dialogue_next(self, callback: CallbackQuery, state: FSMContext) -> None:
        if await self._deny_if_needed(callback):
            return
        try:
            thread_id = _callback_id(callback.data, "aic:dlg:next:")
        except ValueError:
            await callback.answer("Кнопка устарела", show_alert=True)
            return
        ready, reasons = await self._ai_dialogue_readiness()
        if not ready:
            await self._show_dialogue_blocked(callback, state, reasons)
            return
        try:
            context = await self.db.build_ai_dialogue_reply_context(
                thread_id=thread_id,
                recent_post_limit=self.settings.ai_generation_recent_posts,
                knowledge_limit=self.settings.ai_generation_knowledge_chunks,
            )
        except (AIDialogueError, AICommentGenerationError, ValueError) as exc:
            thread = await self.db.get_ai_dialogue_thread(thread_id)
            if thread is not None:
                await self._safe_edit_text(
                    callback.message,
                    _thread_text(thread, notice=str(exc)),
                    reply_markup=ai_dialogue_thread_keyboard(
                        thread_id=thread.id,
                        status=thread.status,
                        pending_draft_id=thread.pending_draft_id,
                    ),
                )
            await callback.answer(str(exc), show_alert=True)
            return
        nonce = uuid.uuid4().hex
        await state.set_state(AICommentsUI.dialogue_generation_confirm)
        await state.update_data(
            ai_dialogue_thread_id=thread_id,
            ai_dialogue_position=context.position,
            ai_dialogue_post_revision=context.base.post.source_revision,
            ai_dialogue_post_hash=context.base.post.normalized_text_hash,
            ai_dialogue_nonce=nonce,
            ai_dialogue_confirmed=True,
        )
        target = "корневой пост" if context.position == 1 else f"реплика {context.position - 1}"
        await self._safe_edit_text(
            callback.message,
            "🗣 <b>Подтверждение следующей реплики</b>\n\n"
            f"Диалог: <code>#{thread_id}</code>\n"
            f"Позиция: <b>{context.position}/{context.thread.max_messages}</b>\n"
            f"Профиль: <b>{_safe(context.base.account_profile.name, 160)}</b>\n"
            f"Ответ на: <b>{target}</b>\n"
            f"Prompt/Schema: <code>{AI_DIALOGUE_PROMPT_VERSION}</code> / <code>{AI_DIALOGUE_SCHEMA_VERSION}</code>\n"
            f"Источников: <b>{len(context.base.sources)}</b>\n\n"
            "Будет выполнен один запрос. Реплика сохранится как черновик и потребует "
            "ручного принятия. Публикации нет.",
            reply_markup=ai_dialogue_generation_confirm_keyboard(thread_id=thread_id),
        )
        await callback.answer()

    async def ai_dialogue_run(self, callback: CallbackQuery, state: FSMContext) -> None:
        if await self._deny_if_needed(callback):
            return
        data = await state.get_data()
        thread_id = _state_int(data, "ai_dialogue_thread_id")
        nonce = data.get("ai_dialogue_nonce")
        if (
            thread_id is None
            or not isinstance(nonce, str)
            or data.get("ai_dialogue_confirmed") is not True
        ):
            await callback.answer("Подтверждение устарело", show_alert=True)
            return
        await state.update_data(ai_dialogue_confirmed=False)
        if self._ai_dialogue_generation_lock.locked():
            await callback.answer("Другой AI-запрос уже выполняется", show_alert=True)
            return
        ready, reasons = await self._ai_dialogue_readiness()
        if not ready:
            await self._show_dialogue_blocked(callback, state, reasons)
            return
        job_id: int | None = None
        context = None
        async with self._ai_dialogue_generation_lock:
            try:
                context = await self.db.build_ai_dialogue_reply_context(
                    thread_id=thread_id,
                    recent_post_limit=self.settings.ai_generation_recent_posts,
                    knowledge_limit=self.settings.ai_generation_knowledge_chunks,
                )
                job_id = await self.db.create_ai_dialogue_generation_job(
                    context=context,
                    requested_by_admin_id=callback.from_user.id,
                    request_nonce=nonce,
                )
                result = await self.openai_gateway.generate_dialogue_reply(
                    context,
                    max_output_tokens=self.settings.ai_generation_max_output_tokens,
                )
                validation = validate_dialogue_reply_output(context, result.output)
                draft_id = await self.db.complete_ai_dialogue_generation(
                    job_id=job_id,
                    context=context,
                    validation=validation,
                    model_name=result.model_name,
                    request_id_safe=result.request_id_safe,
                    input_tokens=result.input_tokens,
                    output_tokens=result.output_tokens,
                    cached_tokens=result.cached_tokens,
                    latency_ms=result.latency_ms,
                )
            except OpenAIGatewayError as exc:
                if job_id is not None and context is not None:
                    await self.db.fail_ai_dialogue_generation(
                        job_id=job_id,
                        thread_id=thread_id,
                        model_name=exc.result.model_name,
                        request_id_safe=exc.result.request_id_safe,
                        input_tokens=exc.result.input_tokens,
                        output_tokens=exc.result.output_tokens,
                        cached_tokens=exc.result.cached_tokens,
                        latency_ms=exc.result.latency_ms,
                        error_class=exc.result.error_class or "unknown",
                        safe_error=exc.safe_message,
                    )
                thread = await self.db.get_ai_dialogue_thread(thread_id)
                if thread is not None:
                    await self._safe_edit_text(
                        callback.message,
                        _thread_text(thread, notice=exc.safe_message),
                        reply_markup=ai_dialogue_thread_keyboard(
                            thread_id=thread.id,
                            status=thread.status,
                            pending_draft_id=thread.pending_draft_id,
                        ),
                    )
                await callback.answer("OpenAI вернул ошибку", show_alert=True)
                return
            except Exception as exc:  # noqa: BLE001
                logger.exception("Dialogue reply generation failed thread=%s", thread_id)
                if job_id is not None and context is not None:
                    await self.db.fail_ai_dialogue_generation(
                        job_id=job_id,
                        thread_id=thread_id,
                        model_name=self.openai_gateway.status.model,
                        request_id_safe=None,
                        input_tokens=0,
                        output_tokens=0,
                        cached_tokens=0,
                        latency_ms=0,
                        error_class="internal",
                        safe_error="Внутренняя ошибка генерации диалога",
                    )
                await callback.answer("Внутренняя ошибка генерации", show_alert=True)
                return
        draft = await self.db.get_ai_comment_draft(draft_id)
        thread = await self.db.get_ai_dialogue_thread(thread_id)
        if draft is None or thread is None:
            await callback.answer("Результат не найден", show_alert=True)
            return
        decision = str(draft.validation.get("decision", "rejected"))
        if draft.status == "pending_review" and draft.text:
            await state.set_state(AICommentsUI.dialogue_review)
            await self._safe_edit_text(
                callback.message,
                "✅ <b>Реплика создана</b>\n\n"
                f"Позиция: <b>{context.position}/{thread.max_messages}</b>\n"
                f"Профиль: <b>{_safe(context.base.account_profile.name, 160)}</b>\n\n"
                f"{_safe(draft.text, 1200)}\n\n"
                f"Confidence: <b>{draft.confidence}</b>\n"
                f"Статус: <code>{draft.status}</code>\n"
                "Примите реплику в план или отклоните. В Telegram она не отправляется.",
                reply_markup=ai_dialogue_review_keyboard(
                    thread_id=thread_id,
                    draft_id=draft_id,
                ),
            )
        else:
            reasons = draft.validation.get("errors") or []
            reason_text = "; ".join(str(item) for item in reasons) or str(
                draft.validation.get("skip_reason") or decision
            )
            await self._safe_edit_text(
                callback.message,
                _thread_text(thread, notice=f"Реплика не принята валидатором: {reason_text}"),
                reply_markup=ai_dialogue_thread_keyboard(
                    thread_id=thread.id,
                    status=thread.status,
                    pending_draft_id=thread.pending_draft_id,
                ),
            )
        await callback.answer("Генерация завершена")

    async def ai_dialogue_review(self, callback: CallbackQuery, state: FSMContext) -> None:
        if await self._deny_if_needed(callback):
            return
        try:
            thread_id, draft_id = _callback_pair(callback.data, "aic:dlg:review:")
        except ValueError:
            await callback.answer("Кнопка устарела", show_alert=True)
            return
        draft = await self.db.get_ai_comment_draft(draft_id)
        thread = await self.db.get_ai_dialogue_thread(thread_id)
        if (
            draft is None
            or thread is None
            or draft.status != "pending_review"
            or thread.pending_draft_id != draft_id
        ):
            await callback.answer("Черновик уже обработан", show_alert=True)
            return
        await state.set_state(AICommentsUI.dialogue_review)
        await self._safe_edit_text(
            callback.message,
            "🔎 <b>Проверка реплики диалога</b>\n\n"
            f"{_safe(draft.text, 1400)}\n\n"
            f"Тема: {_safe(draft.topic, 160)}\n"
            f"Confidence: <b>{draft.confidence}</b>\n"
            f"Prompt/Schema: <code>{draft.prompt_version}</code> / <code>{draft.schema_version}</code>\n\n"
            "Принятие добавит текст только в локальный план диалога. Публикации нет.",
            reply_markup=ai_dialogue_review_keyboard(thread_id=thread_id, draft_id=draft_id),
        )
        await callback.answer()

    async def ai_dialogue_accept(self, callback: CallbackQuery, state: FSMContext) -> None:
        if await self._deny_if_needed(callback):
            return
        try:
            thread_id, draft_id = _callback_pair(callback.data, "aic:dlg:accept:")
        except ValueError:
            await callback.answer("Кнопка устарела", show_alert=True)
            return
        try:
            thread = await self.db.accept_ai_dialogue_draft(
                thread_id=thread_id,
                draft_id=draft_id,
                reviewed_by=callback.from_user.id,
            )
        except AIDialogueError as exc:
            await callback.answer(str(exc), show_alert=True)
            return
        await state.set_state(AICommentsUI.dialogue_thread)
        await self._safe_edit_text(
            callback.message,
            _thread_text(
                thread,
                notice=(
                    "Диалог завершён. Все реплики остались черновиками."
                    if thread.status == "completed"
                    else "Реплика принята. Можно создать следующую."
                ),
            ),
            reply_markup=ai_dialogue_thread_keyboard(
                thread_id=thread.id,
                status=thread.status,
                pending_draft_id=thread.pending_draft_id,
            ),
        )
        await callback.answer("Реплика принята")

    async def ai_dialogue_reject(self, callback: CallbackQuery, state: FSMContext) -> None:
        if await self._deny_if_needed(callback):
            return
        try:
            thread_id, draft_id = _callback_pair(callback.data, "aic:dlg:reject:")
        except ValueError:
            await callback.answer("Кнопка устарела", show_alert=True)
            return
        try:
            thread = await self.db.reject_ai_dialogue_draft(
                thread_id=thread_id,
                draft_id=draft_id,
                reviewed_by=callback.from_user.id,
            )
        except AIDialogueError as exc:
            await callback.answer(str(exc), show_alert=True)
            return
        await state.set_state(AICommentsUI.dialogue_thread)
        await self._safe_edit_text(
            callback.message,
            _thread_text(thread, notice="Реплика отклонена. Позицию можно создать заново."),
            reply_markup=ai_dialogue_thread_keyboard(
                thread_id=thread.id,
                status=thread.status,
                pending_draft_id=thread.pending_draft_id,
            ),
        )
        await callback.answer("Реплика отклонена")

    async def ai_dialogue_cancel_confirm(
        self, callback: CallbackQuery, state: FSMContext
    ) -> None:
        if await self._deny_if_needed(callback):
            return
        try:
            thread_id = _callback_id(callback.data, "aic:dlg:cancel:")
        except ValueError:
            await callback.answer("Кнопка устарела", show_alert=True)
            return
        thread = await self.db.get_ai_dialogue_thread(thread_id)
        if thread is None or thread.status in {"completed", "cancelled"}:
            await callback.answer("Диалог уже завершён", show_alert=True)
            return
        await self._safe_edit_text(
            callback.message,
            "⛔ <b>Отменить диалог?</b>\n\n"
            f"Диалог: <code>#{thread_id}</code>\n"
            f"Принято реплик: <b>{thread.accepted_messages}/{thread.max_messages}</b>\n\n"
            "Неопубликованные данные останутся в аудите, но продолжить цепочку будет нельзя.",
            reply_markup=ai_dialogue_cancel_confirm_keyboard(thread_id=thread_id),
        )
        await callback.answer()

    async def ai_dialogue_cancel(self, callback: CallbackQuery, state: FSMContext) -> None:
        if await self._deny_if_needed(callback):
            return
        try:
            thread_id = _callback_id(callback.data, "aic:dlg:canceldo:")
        except ValueError:
            await callback.answer("Кнопка устарела", show_alert=True)
            return
        try:
            thread = await self.db.cancel_ai_dialogue_thread(thread_id)
        except AIDialogueError as exc:
            await callback.answer(str(exc), show_alert=True)
            return
        await state.set_state(AICommentsUI.dialogue_thread)
        await self._safe_edit_text(
            callback.message,
            _thread_text(thread, notice="Диалог отменён."),
            reply_markup=ai_dialogue_thread_keyboard(
                thread_id=thread.id,
                status=thread.status,
                pending_draft_id=thread.pending_draft_id,
            ),
        )
        await callback.answer("Диалог отменён")
