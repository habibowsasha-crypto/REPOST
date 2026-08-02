from __future__ import annotations

import html
import logging

from aiogram import F, Router
from aiogram.fsm.context import FSMContext
from aiogram.types import CallbackQuery, Message

from .ai_comments_keyboards import (
    ai_comments_gateway_confirm_keyboard,
    ai_comments_gateway_keyboard,
)
from .ai_comments_states import AICommentsUI
from .ai_comments_ui import activate_ai_comments_state
from .openai_gateway import OpenAIGatewayError, OpenAIProbeResult

logger = logging.getLogger("laika_bot.handlers.openai_gateway")


class OpenAIGatewayHandlersMixin:
    """Explicit, admin-only OpenAI DEV connectivity probe for step 10."""

    def _register_openai_gateway_handlers(self, router: Router) -> None:
        router.callback_query.register(self.ai_comments_gateway, F.data == "aic:test")
        router.callback_query.register(
            self.ai_comments_gateway_confirm,
            F.data == "aic:gw:confirm",
        )
        router.callback_query.register(
            self.ai_comments_gateway_run,
            F.data == "aic:gw:run",
        )
        router.callback_query.register(
            self.ai_comments_gateway_cancel,
            F.data == "aic:gw:cancel",
        )

    async def _render_openai_gateway(self, message: Message, *, notice: str = "") -> None:
        gateway = self.openai_gateway
        status = gateway.status
        ready_label = "🟢 ГОТОВ" if status.ready else "🔴 ЗАБЛОКИРОВАН"
        key_label = "🟢 НАСТРОЕН" if status.key_configured else "🔴 НЕТ"
        railway_label = "🟢 ВКЛ" if status.railway_enabled else "🔴 ВЫКЛ"
        notice_block = f"\n\n{html.escape(notice)}" if notice else ""
        await self._safe_edit_text(
            message,
            "🧪 <b>Проверка OpenAI Gateway</b>\n"
            "<i>Шаг 10: только безопасное DEV-соединение</i>\n\n"
            "━━━━━━━━━━━━━━\n"
            f"Статус: <b>{ready_label}</b>\n"
            f"Railway-разрешение: <b>{railway_label}</b>\n"
            f"API-ключ: <b>{key_label}</b>\n"
            f"Модель теста: <code>{html.escape(status.model)}</code>\n"
            f"SDK: <code>{html.escape(status.sdk_version)}</code>\n"
            f"Тайм-аут: <b>{status.timeout_seconds:g} сек</b>\n"
            f"Повторов SDK: <b>{status.max_retries}</b>\n"
            "━━━━━━━━━━━━━━\n\n"
            "Проверка отправляет один фиксированный короткий запрос без постов, "
            "комментариев, Telegram ID, сессий и других пользовательских данных. "
            "Ответ не публикуется и не используется как комментарий."
            f"{notice_block}",
            reply_markup=ai_comments_gateway_keyboard(ready=status.ready),
        )

    async def ai_comments_gateway(
        self, callback: CallbackQuery, state: FSMContext
    ) -> None:
        if await self._deny_if_needed(callback):
            return
        if not await activate_ai_comments_state(callback, state, AICommentsUI.test_comment):
            return
        await state.update_data(ai_comments_gateway_probe_confirmed=False)
        await self._render_openai_gateway(callback.message)
        await callback.answer()

    async def ai_comments_gateway_confirm(
        self, callback: CallbackQuery, state: FSMContext
    ) -> None:
        if await self._deny_if_needed(callback):
            return
        if not await activate_ai_comments_state(callback, state, AICommentsUI.test_comment):
            return
        if not self.openai_gateway.status.ready:
            await state.update_data(ai_comments_gateway_probe_confirmed=False)
            await self._render_openai_gateway(
                callback.message,
                notice="⚠️ Сначала настрой OPENAI_API_KEY и включи OPENAI_GATEWAY_ENABLED в Railway.",
            )
            await callback.answer("Gateway пока заблокирован", show_alert=True)
            return
        await state.update_data(ai_comments_gateway_probe_confirmed=True)
        await self._safe_edit_text(
            callback.message,
            "🧪 <b>Подтверждение DEV-проверки</b>\n\n"
            "Будет выполнен ровно один короткий платный запрос к OpenAI. "
            "Telegram-посты, комментарии и секреты в него не передаются.\n\n"
            "Генерация комментариев и публикация останутся выключенными.",
            reply_markup=ai_comments_gateway_confirm_keyboard(),
        )
        await callback.answer()

    async def _record_openai_probe(self, result: OpenAIProbeResult) -> None:
        await self.db.record_ai_gateway_usage(
            model_name=result.model_name,
            request_id_safe=result.request_id_safe,
            input_tokens=result.input_tokens,
            output_tokens=result.output_tokens,
            cached_tokens=result.cached_tokens,
            latency_ms=result.latency_ms,
            success=result.success,
            error_class=result.error_class,
        )

    async def ai_comments_gateway_run(
        self, callback: CallbackQuery, state: FSMContext
    ) -> None:
        if await self._deny_if_needed(callback):
            return
        if not await activate_ai_comments_state(callback, state, AICommentsUI.test_comment):
            return
        data = await state.get_data()
        if data.get("ai_comments_gateway_probe_confirmed") is not True:
            await self._render_openai_gateway(
                callback.message,
                notice="⚠️ Подтверждение устарело. Запрос не выполнялся.",
            )
            await callback.answer("Сначала подтверди проверку", show_alert=True)
            return

        await state.update_data(ai_comments_gateway_probe_confirmed=False)
        if self._openai_gateway_test_lock.locked():
            await callback.answer("Проверка уже выполняется", show_alert=True)
            return

        await callback.answer("Проверяю соединение…")
        await self._safe_edit_text(
            callback.message,
            "🧪 <b>Проверка OpenAI выполняется…</b>\n\n"
            "Не нажимай кнопку повторно. Основные функции LikeBot продолжают работать.",
            reply_markup=ai_comments_gateway_keyboard(
                ready=self.openai_gateway.status.ready
            ),
        )

        async with self._openai_gateway_test_lock:
            try:
                result = await self.openai_gateway.run_dev_probe()
            except OpenAIGatewayError as exc:
                try:
                    await self._record_openai_probe(exc.result)
                except Exception as storage_exc:  # noqa: BLE001
                    logger.error(
                        "Failed OpenAI probe usage could not be stored type=%s",
                        type(storage_exc).__name__,
                    )
                await self._render_openai_gateway(
                    callback.message,
                    notice=f"❌ {exc.safe_message}",
                )
                return
            except Exception as unexpected_exc:  # noqa: BLE001
                logger.error(
                    "Unexpected OpenAI gateway handler failure type=%s",
                    type(unexpected_exc).__name__,
                )
                await self._render_openai_gateway(
                    callback.message,
                    notice="❌ Неизвестная ошибка была безопасно скрыта. Основной бот не остановлен.",
                )
                return

            try:
                await self._record_openai_probe(result)
            except Exception as storage_exc:  # noqa: BLE001
                logger.error(
                    "Successful OpenAI probe usage could not be stored type=%s",
                    type(storage_exc).__name__,
                )
                await self._render_openai_gateway(
                    callback.message,
                    notice=(
                        "⚠️ OpenAI ответил, но usage-аудит не сохранился в БД. "
                        "Проверка не считается полностью завершённой."
                    ),
                )
                return

        request_line = (
            f" Request ID: {html.escape(result.request_id_safe)}."
            if result.request_id_safe
            else ""
        )
        await self._render_openai_gateway(
            callback.message,
            notice=(
                "✅ Соединение подтверждено. "
                f"Задержка {result.latency_ms} мс, токены {result.input_tokens}/{result.output_tokens}."
                f"{request_line}"
            ),
        )

    async def ai_comments_gateway_cancel(
        self, callback: CallbackQuery, state: FSMContext
    ) -> None:
        if await self._deny_if_needed(callback):
            return
        if not await activate_ai_comments_state(callback, state, AICommentsUI.test_comment):
            return
        await state.update_data(ai_comments_gateway_probe_confirmed=False)
        await self._render_openai_gateway(callback.message)
        await callback.answer("Проверка отменена")
