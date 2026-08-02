from __future__ import annotations

import html
import logging
import re
import uuid
from collections.abc import Mapping

from aiogram import F, Router
from aiogram.fsm.context import FSMContext
from aiogram.types import CallbackQuery, Message

from .ai_channel_memory_handlers import selected_ai_channel_id
from .ai_comment_generation import (
    AI_COMMENT_PROMPT_VERSION,
    AI_COMMENT_SCHEMA_VERSION,
    AICommentDraftSnapshot,
    AICommentGenerationError,
    validate_single_comment_output,
)
from .ai_comments_keyboards import (
    ai_comment_draft_keyboard,
    ai_comment_drafts_keyboard,
    ai_comment_generation_confirm_keyboard,
    ai_comment_generation_result_keyboard,
    ai_comment_generation_wait_keyboard,
    ai_comment_profile_selection_keyboard,
    ai_comments_menu_keyboard,
)
from .ai_comments_states import AICommentsUI
from .ai_comments_ui import activate_ai_comments_state
from .openai_gateway import OpenAIGatewayError
from .utils import truncate

logger = logging.getLogger("laika_bot.handlers.ai_comment_generation")
MAX_DATABASE_ID = 9_223_372_036_854_775_807


def parse_ai_comment_generation_start_callback(data: str | None) -> tuple[int, int]:
    match = re.fullmatch(r"aic:g:start:([1-9][0-9]*):([0-9]+)", data or "")
    if match is None:
        raise ValueError("Некорректная кнопка генерации")
    post_id, page = map(int, match.groups())
    if post_id > MAX_DATABASE_ID or page > 1_000_000:
        raise ValueError("Некорректная кнопка генерации")
    return post_id, page


def parse_ai_comment_generation_profile_callback(
    data: str | None,
) -> tuple[int, int, int]:
    match = re.fullmatch(
        r"aic:g:pr:([1-9][0-9]*):([1-9][0-9]*):([0-9]+)",
        data or "",
    )
    if match is None:
        raise ValueError("Некорректная кнопка профиля")
    post_id, profile_id, page = map(int, match.groups())
    if (
        post_id > MAX_DATABASE_ID
        or profile_id > MAX_DATABASE_ID
        or page > 1_000_000
    ):
        raise ValueError("Некорректная кнопка профиля")
    return post_id, profile_id, page


def parse_ai_comment_draft_callback(data: str | None) -> int:
    match = re.fullmatch(r"aic:d:([1-9][0-9]*)", data or "")
    if match is None:
        raise ValueError("Некорректная кнопка черновика")
    draft_id = int(match.group(1))
    if draft_id > MAX_DATABASE_ID:
        raise ValueError("Некорректная кнопка черновика")
    return draft_id


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


def _draft_decision(draft: AICommentDraftSnapshot) -> str:
    value = draft.validation.get("decision")
    return str(value) if value in {"draft", "skip", "rejected"} else "rejected"


class AICommentGenerationHandlersMixin:
    """Step 11: explicit single-draft generation with no publication path."""

    def _register_ai_comment_generation_handlers(self, router: Router) -> None:
        router.callback_query.register(
            self.ai_comment_generation_start,
            F.data.startswith("aic:g:start:"),
        )
        router.callback_query.register(
            self.ai_comment_generation_profile,
            F.data.startswith("aic:g:pr:"),
        )
        router.callback_query.register(
            self.ai_comment_generation_run,
            F.data == "aic:g:run",
        )
        router.callback_query.register(
            self.ai_comment_drafts,
            F.data == "aic:drafts",
        )
        router.callback_query.register(
            self.ai_comment_draft,
            F.data.startswith("aic:d:"),
        )

    async def _ai_generation_readiness(self) -> tuple[bool, list[str]]:
        reasons: list[str] = []
        try:
            flags = await self.db.get_ai_comments_flags()
        except Exception:  # noqa: BLE001
            logger.exception("AI generation flags could not be read")
            return False, ["Настройки AI Comments в БД недоступны"]
        if not flags.get("ai_comments_enabled", False):
            reasons.append("AI Comments выключен в настройках бота")
        if not flags.get("ai_generation_enabled", False):
            reasons.append("Генерация черновиков выключена в настройках бота")
        if not getattr(self.settings, "ai_comments_enabled", False):
            reasons.append("AI_COMMENTS_ENABLED=false в Railway")
        if not getattr(self.settings, "ai_generation_enabled", False):
            reasons.append("AI_GENERATION_ENABLED=false в Railway")
        if not self.openai_gateway.status.ready:
            reasons.append("OpenAI Gateway выключен или не готов")
        return not reasons, reasons

    async def _show_generation_blocked(
        self,
        callback: CallbackQuery,
        state: FSMContext,
        reasons: list[str],
    ) -> None:
        await state.set_state(AICommentsUI.draft_generation)
        body = "\n".join(f"- {_safe(reason, 300)}" for reason in reasons)
        await self._safe_edit_text(
            callback.message,
            "🔒 <b>Генерация черновика заблокирована</b>\n\n"
            f"{body}\n\n"
            "Для шага 11 должны быть включены только общий модуль, генерация "
            "черновика и OpenAI Gateway. Диалоги и публикация остаются выключенными.",
            reply_markup=ai_comments_menu_keyboard(),
        )
        await callback.answer("Генерация пока заблокирована", show_alert=True)

    async def ai_comment_generation_start(
        self,
        callback: CallbackQuery,
        state: FSMContext,
    ) -> None:
        if await self._deny_if_needed(callback):
            return
        try:
            post_id, page = parse_ai_comment_generation_start_callback(callback.data)
        except ValueError:
            await callback.answer("Кнопка устарела", show_alert=True)
            return
        if not await activate_ai_comments_state(
            callback,
            state,
            AICommentsUI.draft_generation,
        ):
            return
        channel_id = selected_ai_channel_id(await state.get_data())
        if channel_id is None:
            await self._safe_edit_text(
                callback.message,
                "✍️ <b>Один черновик</b>\n\nСначала выберите канал и публикацию.",
                reply_markup=ai_comments_menu_keyboard(),
            )
            await callback.answer("Сначала выберите канал", show_alert=True)
            return
        ready, reasons = await self._ai_generation_readiness()
        if not ready:
            await self._show_generation_blocked(callback, state, reasons)
            return
        post = await self.db.get_ai_channel_post(channel_id, post_id)
        if post is None or post.deleted_at is not None:
            await callback.answer("Публикация больше недоступна", show_alert=True)
            return
        profiles = tuple(
            profile
            for profile in await self.db.list_ai_account_profiles()
            if profile.enabled and not profile.retired
        )
        if not profiles:
            await self._safe_edit_text(
                callback.message,
                "✍️ <b>Один черновик</b>\n\n"
                "Нет включённых профилей аккаунтов. Откройте «🎭 Профили аккаунтов» "
                "и включите хотя бы один профиль.",
                reply_markup=ai_comments_menu_keyboard(),
            )
            await callback.answer("Нет доступных профилей", show_alert=True)
            return
        await state.update_data(
            ai_generation_post_id=post_id,
            ai_generation_page=page,
            ai_generation_profile_id=None,
            ai_generation_confirmed=False,
            ai_generation_nonce=None,
        )
        source = post.text or post.media_caption or "[медиа]"
        buttons = [
            (
                profile.id,
                truncate(
                    f"{profile.name} · {profile.account_display_name or profile.telegram_user_id or 'без аккаунта'}",
                    48,
                ),
                bool(
                    profile.account_is_active
                    and profile.account_status == "ready"
                    and profile.account_has_session
                ),
            )
            for profile in profiles
        ]
        await self._safe_edit_text(
            callback.message,
            "✍️ <b>Создание одного черновика</b>\n"
            "<i>Шаг 11: результат только сохраняется в БД</i>\n\n"
            f"Публикация: <code>#{post.telegram_message_id}</code> · rev {post.source_revision}\n"
            f"{_safe(source, 700)}\n\n"
            "Выберите профиль, от имени которого будет сформирован стиль. "
            "Статус аккаунта не приводит к публикации: на этом шаге отправка отсутствует.",
            reply_markup=ai_comment_profile_selection_keyboard(
                buttons,
                post_id=post_id,
                page=page,
            ),
        )
        await callback.answer()

    async def ai_comment_generation_profile(
        self,
        callback: CallbackQuery,
        state: FSMContext,
    ) -> None:
        if await self._deny_if_needed(callback):
            return
        try:
            post_id, profile_id, page = parse_ai_comment_generation_profile_callback(
                callback.data
            )
        except ValueError:
            await callback.answer("Кнопка устарела", show_alert=True)
            return
        if not await activate_ai_comments_state(
            callback,
            state,
            AICommentsUI.draft_generation_confirm,
        ):
            return
        data = await state.get_data()
        channel_id = selected_ai_channel_id(data)
        if channel_id is None or _state_int(data, "ai_generation_post_id") != post_id:
            await callback.answer("Выбор публикации устарел", show_alert=True)
            return
        ready, reasons = await self._ai_generation_readiness()
        if not ready:
            await self._show_generation_blocked(callback, state, reasons)
            return
        try:
            context = await self.db.build_ai_single_comment_context(
                channel_id=channel_id,
                post_id=post_id,
                account_profile_id=profile_id,
                recent_post_limit=self.settings.ai_generation_recent_posts,
                knowledge_limit=self.settings.ai_generation_knowledge_chunks,
            )
        except (AICommentGenerationError, ValueError) as exc:
            await callback.answer(str(exc), show_alert=True)
            return
        nonce = uuid.uuid4().hex
        await state.update_data(
            ai_generation_profile_id=profile_id,
            ai_generation_post_revision=context.post.source_revision,
            ai_generation_post_hash=context.post.normalized_text_hash,
            ai_generation_page=page,
            ai_generation_confirmed=True,
            ai_generation_nonce=nonce,
        )
        ref_lines = [
            f"- <code>{_safe(source.ref, 160)}</code> · {_safe(source.title, 120)}"
            for source in context.sources
        ]
        await self._safe_edit_text(
            callback.message,
            "✍️ <b>Подтверждение одного черновика</b>\n\n"
            f"Публикация: <code>#{context.post.telegram_message_id}</code> · rev {context.post.source_revision}\n"
            f"Профиль: <b>{_safe(context.account_profile.name, 160)}</b>\n"
            f"Длина: <b>{context.account_profile.min_length}-{context.account_profile.max_length}</b> символов\n"
            f"Модель: <code>{_safe(self.openai_gateway.status.model, 96)}</code>\n"
            f"Prompt: <code>{AI_COMMENT_PROMPT_VERSION}</code>\n"
            f"Schema: <code>{AI_COMMENT_SCHEMA_VERSION}</code>\n\n"
            "<b>Источники контекста</b>\n"
            + "\n".join(ref_lines)
            + "\n\nБудет выполнен один запрос. Результат пройдёт локальную проверку "
            "и сохранится как pending_review, skip или rejected. Публикации нет.",
            reply_markup=ai_comment_generation_confirm_keyboard(
                post_id=post_id,
                profile_id=profile_id,
                page=page,
            ),
        )
        await callback.answer()

    async def ai_comment_generation_run(
        self,
        callback: CallbackQuery,
        state: FSMContext,
    ) -> None:
        if await self._deny_if_needed(callback):
            return
        if not await activate_ai_comments_state(
            callback,
            state,
            AICommentsUI.draft_generation_confirm,
        ):
            return
        data = await state.get_data()
        channel_id = selected_ai_channel_id(data)
        post_id = _state_int(data, "ai_generation_post_id")
        profile_id = _state_int(data, "ai_generation_profile_id")
        page = data.get("ai_generation_page")
        nonce = data.get("ai_generation_nonce")
        if (
            channel_id is None
            or post_id is None
            or profile_id is None
            or not isinstance(page, int)
            or isinstance(page, bool)
            or not isinstance(nonce, str)
            or data.get("ai_generation_confirmed") is not True
        ):
            await callback.answer("Подтверждение устарело", show_alert=True)
            return
        await state.update_data(ai_generation_confirmed=False)
        if self._ai_comment_generation_lock.locked():
            await callback.answer("Другой черновик уже создаётся", show_alert=True)
            return
        ready, reasons = await self._ai_generation_readiness()
        if not ready:
            await self._show_generation_blocked(callback, state, reasons)
            return
        await callback.answer("Создаю и проверяю черновик…")
        await self._safe_edit_text(
            callback.message,
            "⏳ <b>Создаётся один черновик…</b>\n\n"
            "Контекст собирается заново, источник проверяется по ревизии и SHA-256. "
            "Ничего не публикуется.",
            reply_markup=ai_comment_generation_wait_keyboard(
                post_id=post_id,
                page=page,
            ),
        )

        async with self._ai_comment_generation_lock:
            job_id: int | None = None
            generated = None
            try:
                context = await self.db.build_ai_single_comment_context(
                    channel_id=channel_id,
                    post_id=post_id,
                    account_profile_id=profile_id,
                    recent_post_limit=self.settings.ai_generation_recent_posts,
                    knowledge_limit=self.settings.ai_generation_knowledge_chunks,
                )
                if (
                    context.post.source_revision
                    != data.get("ai_generation_post_revision")
                    or context.post.normalized_text_hash
                    != data.get("ai_generation_post_hash")
                ):
                    raise AICommentGenerationError(
                        "Публикация изменилась после подтверждения"
                    )
                job_id = await self.db.create_ai_single_generation_job(
                    context=context,
                    requested_by_admin_id=int(callback.from_user.id),
                    request_nonce=nonce,
                )
                generated = await self.openai_gateway.generate_single_comment(
                    context,
                    max_output_tokens=self.settings.ai_generation_max_output_tokens,
                )
                validation = validate_single_comment_output(
                    context,
                    generated.output,
                )
                draft = await self.db.complete_ai_single_generation(
                    job_id=job_id,
                    context=context,
                    validation=validation,
                    model_name=generated.model_name,
                    request_id_safe=generated.request_id_safe,
                    input_tokens=generated.input_tokens,
                    output_tokens=generated.output_tokens,
                    cached_tokens=generated.cached_tokens,
                    latency_ms=generated.latency_ms,
                )
            except OpenAIGatewayError as exc:
                if job_id is not None:
                    try:
                        await self.db.fail_ai_single_generation(
                            job_id=job_id,
                            model_name=exc.result.model_name,
                            request_id_safe=exc.result.request_id_safe,
                            input_tokens=exc.result.input_tokens,
                            output_tokens=exc.result.output_tokens,
                            cached_tokens=exc.result.cached_tokens,
                            latency_ms=exc.result.latency_ms,
                            error_class=exc.result.error_class or "unknown",
                            safe_error=exc.safe_message,
                        )
                    except Exception:  # noqa: BLE001
                        logger.exception("Failed generation usage could not be stored")
                await self._safe_edit_text(
                    callback.message,
                    "❌ <b>Черновик не создан</b>\n\n"
                    f"{_safe(exc.safe_message, 500)}\n\n"
                    "Основной LikeBot продолжает работать. Публикации не было.",
                    reply_markup=ai_comments_menu_keyboard(),
                )
                return
            except (AICommentGenerationError, ValueError) as exc:
                if job_id is not None:
                    try:
                        await self.db.fail_ai_single_generation(
                            job_id=job_id,
                            model_name=(
                                generated.model_name
                                if generated is not None
                                else self.openai_gateway.status.model
                            ),
                            request_id_safe=(
                                generated.request_id_safe
                                if generated is not None
                                else None
                            ),
                            input_tokens=(generated.input_tokens if generated is not None else 0),
                            output_tokens=(generated.output_tokens if generated is not None else 0),
                            cached_tokens=(generated.cached_tokens if generated is not None else 0),
                            latency_ms=(generated.latency_ms if generated is not None else 0),
                            error_class="local_validation",
                            safe_error=str(exc),
                        )
                    except Exception:  # noqa: BLE001
                        logger.exception("Local generation failure could not be stored")
                await self._safe_edit_text(
                    callback.message,
                    "❌ <b>Черновик не сохранён</b>\n\n"
                    f"{_safe(exc, 500)}\n\nПубликации не было.",
                    reply_markup=ai_comments_menu_keyboard(),
                )
                return
            except Exception as exc:  # noqa: BLE001
                logger.error(
                    "Unexpected single draft failure type=%s",
                    type(exc).__name__,
                )
                if job_id is not None:
                    try:
                        await self.db.fail_ai_single_generation(
                            job_id=job_id,
                            model_name=(
                                generated.model_name
                                if generated is not None
                                else self.openai_gateway.status.model
                            ),
                            request_id_safe=(
                                generated.request_id_safe
                                if generated is not None
                                else None
                            ),
                            input_tokens=(generated.input_tokens if generated is not None else 0),
                            output_tokens=(generated.output_tokens if generated is not None else 0),
                            cached_tokens=(generated.cached_tokens if generated is not None else 0),
                            latency_ms=(generated.latency_ms if generated is not None else 0),
                            error_class="unknown",
                            safe_error="Неизвестная безопасно скрытая ошибка",
                        )
                    except Exception:  # noqa: BLE001
                        logger.exception("Unknown generation failure could not be stored")
                await self._safe_edit_text(
                    callback.message,
                    "❌ <b>Неизвестная ошибка безопасно скрыта</b>\n\n"
                    "Основной LikeBot продолжает работать. Публикации не было.",
                    reply_markup=ai_comments_menu_keyboard(),
                )
                return

        await state.set_state(AICommentsUI.drafts)
        decision = _draft_decision(draft)
        refs = "\n".join(
            f"- <code>{_safe(ref, 160)}</code>" for ref in draft.knowledge_refs
        ) or "- <i>нет</i>"
        errors = draft.validation.get("errors")
        error_lines = (
            "\n".join(f"- {_safe(item, 250)}" for item in errors)
            if isinstance(errors, list) and errors
            else ""
        )
        if decision == "draft" and draft.text:
            title = "✅ Черновик создан и сохранён"
            body = (
                f"<b>Текст</b>\n{_safe(draft.text, 1_200)}\n\n"
                f"Тема: <b>{_safe(draft.topic, 128)}</b>\n"
                f"Confidence: <b>{draft.confidence}</b>\n"
                f"Статус: <b>{draft.status}</b>"
            )
        elif decision == "skip":
            title = "⏭ Модель выбрала skip"
            body = (
                f"Причина: {_safe(draft.validation.get('skip_reason'), 500)}\n\n"
                "Пустой комментарий не создан и ничего не опубликовано. "
                "Решение сохранено для измерения качества."
            )
        else:
            title = "⛔ Результат отклонён валидатором"
            body = (
                f"Причины:\n{error_lines or '- локальная проверка не пройдена'}\n\n"
                "Отклонённый текст не доступен для публикации. Решение сохранено для аудита."
            )
        await self._safe_edit_text(
            callback.message,
            f"{title}\n\n{body}\n\n"
            f"Модель: <code>{_safe(draft.model_name, 96)}</code>\n"
            f"Prompt/Schema: <code>{draft.prompt_version}</code> / <code>{draft.schema_version}</code>\n\n"
            f"<b>Provenance</b>\n{refs}\n\n"
            "Публикация на шаге 11 физически отсутствует.",
            reply_markup=ai_comment_generation_result_keyboard(
                draft_id=draft.id,
                post_id=post_id,
                page=page,
            ),
        )

    async def ai_comment_drafts(
        self,
        callback: CallbackQuery,
        state: FSMContext,
    ) -> None:
        if await self._deny_if_needed(callback):
            return
        if not await activate_ai_comments_state(callback, state, AICommentsUI.drafts):
            return
        channel_id = selected_ai_channel_id(await state.get_data())
        drafts = await self.db.list_ai_comment_drafts(
            channel_id=channel_id,
            limit=20,
        )
        lines: list[str] = []
        buttons: list[tuple[int, str]] = []
        for draft in drafts:
            decision = _draft_decision(draft)
            icon = {"draft": "✅", "skip": "⏭", "rejected": "⛔"}[decision]
            source = f"#{draft.telegram_message_id}" if draft.telegram_message_id else "без поста"
            label = truncate(
                f"{icon} {source} · {draft.account_profile_name or 'профиль'}",
                52,
            )
            buttons.append((draft.id, label))
            preview = draft.text or str(draft.validation.get("skip_reason") or "отклонён")
            lines.append(
                f"{icon} <b>#{draft.id}</b> · {source} · {_safe(draft.status, 40)}\n"
                f"{_safe(preview, 180)}"
            )
        body = "\n\n".join(lines) if lines else "<i>Записей пока нет.</i>"
        scope = "выбранного канала" if channel_id is not None else "всех каналов"
        await self._safe_edit_text(
            callback.message,
            "✅ <b>Черновики шага 11</b>\n\n"
            f"Показаны последние записи {scope}. Здесь есть draft, skip и rejected.\n\n"
            f"{body}\n\n"
            "Одобрение, редактирование и публикация будут реализованы на следующих этапах.",
            reply_markup=ai_comment_drafts_keyboard(buttons),
        )
        await callback.answer()

    async def ai_comment_draft(
        self,
        callback: CallbackQuery,
        state: FSMContext,
    ) -> None:
        if await self._deny_if_needed(callback):
            return
        try:
            draft_id = parse_ai_comment_draft_callback(callback.data)
        except ValueError:
            await callback.answer("Кнопка устарела", show_alert=True)
            return
        if not await activate_ai_comments_state(callback, state, AICommentsUI.drafts):
            return
        draft = await self.db.get_ai_comment_draft(draft_id)
        if draft is None:
            await callback.answer("Запись не найдена", show_alert=True)
            return
        decision = _draft_decision(draft)
        errors = draft.validation.get("errors")
        claims = draft.validation.get("factual_claims")
        error_lines = (
            "\n".join(f"- {_safe(item, 280)}" for item in errors)
            if isinstance(errors, list) and errors
            else "- нет"
        )
        claim_lines = []
        if isinstance(claims, list):
            for item in claims[:8]:
                if isinstance(item, dict):
                    claim_lines.append(
                        f"- {_safe(item.get('claim'), 220)}\n"
                        f"  <code>{_safe(item.get('source_ref'), 160)}</code> · “{_safe(item.get('evidence_quote'), 220)}”"
                    )
        refs = "\n".join(
            f"- <code>{_safe(ref, 160)}</code>" for ref in draft.knowledge_refs
        ) or "- нет"
        text = draft.text or "<i>текст отсутствует</i>"
        await self._safe_edit_text(
            callback.message,
            f"📄 <b>Запись #{draft.id}</b>\n\n"
            f"Решение: <b>{decision}</b>\n"
            f"Статус: <b>{_safe(draft.status, 40)}</b>\n"
            f"Публикация: <code>#{draft.telegram_message_id or '-'}</code> · rev {draft.source_post_revision}\n"
            f"Профиль: <b>{_safe(draft.account_profile_name, 160)}</b>\n"
            f"Модель: <code>{_safe(draft.model_name, 96)}</code>\n"
            f"Confidence: <b>{draft.confidence if draft.confidence is not None else '-'}</b>\n"
            f"Prompt/Schema: <code>{draft.prompt_version}</code> / <code>{draft.schema_version}</code>\n"
            f"Source SHA: <code>{draft.source_post_hash}</code>\n\n"
            f"<b>Текст</b>\n{_safe(text, 1_500) if draft.text else text}\n\n"
            f"<b>Provenance</b>\n{refs}\n\n"
            f"<b>Factual claims</b>\n{chr(10).join(claim_lines) if claim_lines else '- нет'}\n\n"
            f"<b>Ошибки валидатора</b>\n{error_lines}\n\n"
            "На шаге 11 эта запись не может быть отправлена в Telegram.",
            reply_markup=ai_comment_draft_keyboard(post_id=draft.post_id),
        )
        await callback.answer()
