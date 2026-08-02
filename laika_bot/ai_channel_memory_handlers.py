from __future__ import annotations

import asyncio
import html
import logging
import math
import re
from collections.abc import Mapping, Sequence

from aiogram import F, Router
from aiogram.fsm.context import FSMContext
from aiogram.types import CallbackQuery, Message

from .ai_channel_memory import (
    AI_CHANNEL_PROFILE_FIELD_CODES,
    AI_CHANNEL_PROFILE_FIELDS,
    MAX_DATABASE_ID,
    AIChannelMemoryConflictError,
    AIChannelMemoryError,
    AIChannelMemorySnapshot,
    AIChannelPostSnapshot,
    parse_ai_channel_profile_input,
)
from .ai_comments_keyboards import (
    ai_comments_memory_input_keyboard,
    ai_comments_memory_keyboard,
    ai_comments_menu_keyboard,
    ai_comments_post_keyboard,
    ai_comments_recent_posts_keyboard,
    ai_comments_scenario_complete_keyboard,
)
from .ai_comments_states import AICommentsUI
from .ai_comments_ui import activate_ai_comments_state
from .utils import truncate

logger = logging.getLogger("laika_bot.handlers.ai_channel_memory")

AI_COMMENTS_POST_PAGE_SIZE = 5
MAX_AI_COMMENTS_MEMORY_PAGE = 1_000_000


def parse_ai_channel_memory_edit_callback(
    data: str | None,
) -> tuple[str, int, int]:
    match = re.fullmatch(
        r"aic:m:e:([a-z]):([1-9][0-9]*):([1-9][0-9]*)",
        data or "",
    )
    if match is None:
        raise ValueError("Некорректная кнопка поля памяти")
    field = AI_CHANNEL_PROFILE_FIELD_CODES.get(match.group(1))
    channel_id = int(match.group(2))
    version = int(match.group(3))
    if field is None or channel_id > MAX_DATABASE_ID or version > MAX_DATABASE_ID:
        raise ValueError("Некорректная кнопка поля памяти")
    return field, channel_id, version


def parse_ai_channel_posts_page_callback(data: str | None) -> int:
    if data == "aic:posts":
        return 0
    match = re.fullmatch(r"aic:posts:([0-9]+)", data or "")
    if match is None:
        raise ValueError("Некорректная страница публикаций")
    page = int(match.group(1))
    if page > MAX_AI_COMMENTS_MEMORY_PAGE:
        raise ValueError("Некорректная страница публикаций")
    return page


def parse_ai_channel_post_callback(data: str | None) -> tuple[int, int]:
    match = re.fullmatch(r"aic:p:([1-9][0-9]*):([0-9]+)", data or "")
    if match is None:
        raise ValueError("Некорректная кнопка публикации")
    post_id, page = int(match.group(1)), int(match.group(2))
    if post_id > MAX_DATABASE_ID or page > MAX_AI_COMMENTS_MEMORY_PAGE:
        raise ValueError("Некорректная кнопка публикации")
    return post_id, page


def parse_ai_channel_post_link_callback(
    data: str | None,
) -> tuple[int, int, int]:
    match = re.fullmatch(
        r"aic:pl:([1-9][0-9]*):([1-9][0-9]*):([0-9]+)",
        data or "",
    )
    if match is None:
        raise ValueError("Некорректная кнопка сценария")
    post_id, scenario_id, page = map(int, match.groups())
    if (
        post_id > MAX_DATABASE_ID
        or scenario_id > MAX_DATABASE_ID
        or page > MAX_AI_COMMENTS_MEMORY_PAGE
    ):
        raise ValueError("Некорректная кнопка сценария")
    return post_id, scenario_id, page


def parse_ai_channel_scenario_callback(
    data: str | None,
    *,
    action: str,
) -> int:
    prefix = {"confirm": "scf", "apply": "sca"}.get(action)
    if prefix is None:
        raise ValueError("Некорректное действие сценария")
    match = re.fullmatch(rf"aic:{prefix}:([1-9][0-9]*)", data or "")
    if match is None:
        raise ValueError("Некорректная кнопка сценария")
    scenario_id = int(match.group(1))
    if scenario_id > MAX_DATABASE_ID:
        raise ValueError("Некорректная кнопка сценария")
    return scenario_id


def selected_ai_channel_id(data: Mapping[str, object]) -> int | None:
    value = data.get("ai_comments_selected_channel_id")
    if (
        not isinstance(value, int)
        or isinstance(value, bool)
        or not 0 < value <= MAX_DATABASE_ID
    ):
        return None
    return value


def paginate_ai_channel_posts(
    posts: Sequence[AIChannelPostSnapshot],
    page: int,
) -> tuple[list[AIChannelPostSnapshot], int, int]:
    total_pages = max(
        1,
        math.ceil(len(posts) / AI_COMMENTS_POST_PAGE_SIZE),
    )
    safe_page = min(max(0, page), total_pages - 1)
    start = safe_page * AI_COMMENTS_POST_PAGE_SIZE
    return (
        list(posts[start : start + AI_COMMENTS_POST_PAGE_SIZE]),
        safe_page,
        total_pages,
    )


def parse_ai_channel_scenario_input(
    raw: str | None,
) -> dict[str, object]:
    if raw is None:
        raise ValueError("Отправьте сценарий обычным текстом")
    lines = [line.strip() for line in raw.replace("\r\n", "\n").split("\n")]
    if len(lines) < 5:
        raise ValueError("Нужно заполнить пять строк по указанному шаблону")
    title, symbol, direction, opening_message_id = lines[:4]
    summary = "\n".join(lines[4:]).strip()
    if not title or title == "-":
        raise ValueError("Первая строка должна содержать название сценария")
    opening_id: int | None = None
    if opening_message_id and opening_message_id != "-":
        if not re.fullmatch(r"[1-9][0-9]*", opening_message_id):
            raise ValueError("ID публикации должен быть положительным числом или -")
        opening_id = int(opening_message_id)
        if opening_id > MAX_DATABASE_ID:
            raise ValueError("Некорректный ID публикации")
    return {
        "title": title,
        "symbol": None if symbol == "-" else symbol,
        "direction": None if direction == "-" else direction,
        "opened_by_telegram_message_id": opening_id,
        "factual_summary": None if summary == "-" else summary,
    }


def _escaped(value: object, *, limit: int = 120) -> str:
    if value is None or value == "" or value == ():
        return "<i>не задано</i>"
    if isinstance(value, (tuple, list)):
        text = ", ".join(str(item) for item in value)
    else:
        text = str(value)
    return truncate(html.escape(text), limit)


def _scenario_label(scenario) -> str:
    details = " ".join(
        value for value in (scenario.symbol, scenario.direction) if value
    )
    return truncate(f"{scenario.title} {details}".strip(), 42)


def ai_channel_memory_text(
    memory: AIChannelMemorySnapshot,
    *,
    notice: str | None = None,
) -> str:
    profile = memory.profile
    scenario_lines = [
        "- " + _escaped(_scenario_label(scenario), limit=80)
        for scenario in memory.active_scenarios[:5]
    ] or ["- <i>нет активных сценариев</i>"]
    post_lines = []
    for post in memory.recent_posts[:3]:
        source_text = post.text or post.media_caption or f"[{post.media_type or 'медиа'}]"
        post_lines.append(
            f"- <code>#{post.telegram_message_id}</code> "
            f"{_escaped(source_text, limit=100)} "
            f"<i>(rev {post.source_revision})</i>"
        )
    if not post_lines:
        post_lines.append("- <i>публикации ещё не загружены</i>")
    notice_line = f"\n⚠️ {_escaped(notice, limit=300)}\n" if notice else ""
    return (
        "🧠 <b>Память канала</b>\n\n"
        f"Канал: <b>{_escaped(profile.channel_title, limit=180)}</b>\n"
        f"Telegram ID: <code>{profile.telegram_channel_id}</code>\n"
        f"Версия профиля: <b>{profile.profile_version}</b>\n"
        f"Сохранено постов: <b>{memory.stored_post_count}</b> "
        f"(в окне {len(memory.recent_posts)}, удалённых {memory.deleted_post_count})\n"
        f"{notice_line}\n"
        "<b>Постоянный профиль</b>\n"
        f"- Описание: {_escaped(profile.description)}\n"
        f"- Тематика: {_escaped(profile.topic)}\n"
        f"- Целевая аудитория: {_escaped(profile.target_audience)}\n"
        f"- Язык: {_escaped(profile.language)}\n"
        f"- Стиль автора: {_escaped(profile.author_style)}\n"
        f"- Стиль аудитории: {_escaped(profile.audience_style)}\n"
        f"- Длина комментариев: {_escaped(profile.typical_comment_length)}\n"
        f"- Термины: {_escaped(profile.main_terms)}\n"
        f"- Методология: {_escaped(profile.methodology)}\n"
        f"- Разрешено: {_escaped(profile.allowed_topics)}\n"
        f"- Запрещено: {_escaped(profile.forbidden_topics)}\n"
        f"- Реклама: {_escaped(profile.advertising_restrictions)}\n"
        f"- Заметки: {_escaped(profile.admin_notes)}\n"
        f"- Summary: {_escaped(profile.memory_summary, limit=180)}\n\n"
        "<b>Активные сценарии</b>\n"
        + "\n".join(scenario_lines)
        + "\n\n<b>Последние публикации</b>\n"
        + "\n".join(post_lines)
    )


class AIChannelMemoryHandlersMixin:
    """Admin-only channel memory UI with no OpenAI or publication actions."""

    def _register_ai_channel_memory_handlers(self, router: Router) -> None:
        router.callback_query.register(
            self.ai_comments_memory,
            F.data == "aic:memory",
        )
        router.callback_query.register(
            self.ai_comments_memory_refresh,
            F.data == "aic:m:refresh",
        )
        router.callback_query.register(
            self.ai_comments_memory_edit,
            F.data.startswith("aic:m:e:"),
        )
        router.callback_query.register(
            self.ai_comments_posts,
            F.data == "aic:posts",
        )
        router.callback_query.register(
            self.ai_comments_posts,
            F.data.startswith("aic:posts:"),
        )
        router.callback_query.register(
            self.ai_comments_post_link,
            F.data.startswith("aic:pl:"),
        )
        router.callback_query.register(
            self.ai_comments_post,
            F.data.startswith("aic:p:"),
        )
        router.callback_query.register(
            self.ai_comments_scenario_new,
            F.data == "aic:s:new",
        )
        router.callback_query.register(
            self.ai_comments_scenario_complete_confirm,
            F.data.startswith("aic:scf:"),
        )
        router.callback_query.register(
            self.ai_comments_scenario_complete_apply,
            F.data.startswith("aic:sca:"),
        )
        router.message.register(
            self.ai_comments_memory_input,
            AICommentsUI.channel_memory_edit,
        )
        router.message.register(
            self.ai_comments_scenario_input,
            AICommentsUI.channel_scenario_edit,
        )

    def _ai_comments_memory_lock(self, channel_id: int) -> asyncio.Lock:
        locks = getattr(self, "_ai_comments_memory_locks", None)
        if locks is None:
            locks = {}
            self._ai_comments_memory_locks = locks
        lock = locks.get(channel_id)
        if lock is None:
            lock = asyncio.Lock()
            locks[channel_id] = lock
        return lock

    async def _selected_ai_channel_id(self, state: FSMContext) -> int | None:
        return selected_ai_channel_id(await state.get_data())

    async def _render_ai_channel_memory(
        self,
        message: Message,
        memory: AIChannelMemorySnapshot,
        *,
        notice: str | None = None,
    ) -> None:
        scenarios = [
            (scenario.id, _scenario_label(scenario))
            for scenario in memory.active_scenarios
        ]
        await self._safe_edit_text(
            message,
            ai_channel_memory_text(memory, notice=notice),
            reply_markup=ai_comments_memory_keyboard(
                channel_id=memory.profile.channel_id or 0,
                profile_version=memory.profile.profile_version,
                active_scenarios=scenarios,
            ),
        )

    async def _show_missing_ai_channel(
        self,
        callback: CallbackQuery,
        state: FSMContext,
    ) -> None:
        await state.set_state(AICommentsUI.menu)
        await self._safe_edit_text(
            callback.message,
            "🧠 <b>Память канала</b>\n\n"
            "Сначала выберите канал в разделе «📡 Выбрать канал».\n"
            "Чужая память не подставляется автоматически.",
            reply_markup=ai_comments_menu_keyboard(),
        )
        await callback.answer("Сначала выберите канал", show_alert=True)

    async def ai_comments_memory(
        self,
        callback: CallbackQuery,
        state: FSMContext,
    ) -> None:
        if await self._deny_if_needed(callback):
            return
        if not await activate_ai_comments_state(
            callback,
            state,
            AICommentsUI.channel_memory,
        ):
            return
        channel_id = await self._selected_ai_channel_id(state)
        if channel_id is None:
            await self._show_missing_ai_channel(callback, state)
            return
        try:
            memory = await self.db.get_ai_channel_memory(channel_id)
        except Exception:  # noqa: BLE001
            logger.exception("AI channel memory could not be read target=%s", channel_id)
            await self._show_ai_comments_stale(
                callback,
                state,
                notice="Память канала недоступна. Выберите канал заново.",
            )
            return
        await state.update_data(
            ai_comments_flag_target=None,
            ai_comments_memory_field=None,
            ai_comments_memory_expected_version=None,
            ai_comments_scenario_complete_id=None,
        )
        await self._render_ai_channel_memory(callback.message, memory)
        await callback.answer()

    async def ai_comments_memory_refresh(
        self,
        callback: CallbackQuery,
        state: FSMContext,
    ) -> None:
        if await self._deny_if_needed(callback):
            return
        if not await activate_ai_comments_state(
            callback,
            state,
            AICommentsUI.channel_memory,
        ):
            return
        channel_id = await self._selected_ai_channel_id(state)
        if channel_id is None:
            await self._show_missing_ai_channel(callback, state)
            return
        flags, settings_error = await self._read_ai_comments_flags_fail_closed()
        effective = (
            not settings_error
            and bool(self.settings.ai_comments_enabled)
            and bool(flags["ai_comments_enabled"])
        )
        if not effective:
            try:
                memory = await self.db.get_ai_channel_memory(channel_id)
            except Exception:  # noqa: BLE001
                logger.exception(
                    "Disabled AI channel memory could not be read target=%s",
                    channel_id,
                )
                await self._show_ai_comments_stale(
                    callback,
                    state,
                    notice="Память канала временно недоступна.",
                )
                return
            await self._render_ai_channel_memory(
                callback.message,
                memory,
                notice=(
                    "Сбор постов выключен. Включите AI Comments и Railway "
                    "kill switch; генерация и публикация останутся недоступны."
                ),
            )
            await callback.answer("Сбор постов выключен", show_alert=True)
            return
        await callback.answer("Обновляю последние публикации...")
        try:
            result = await self.jobs.sync_ai_channel_memory(channel_id)
            memory = await self.db.get_ai_channel_memory(channel_id)
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                "Manual AI channel memory refresh failed target=%s error=%s: %s",
                channel_id,
                type(exc).__name__,
                exc,
            )
            try:
                memory = await self.db.get_ai_channel_memory(channel_id)
            except Exception:  # noqa: BLE001
                logger.exception(
                    "AI channel memory could not be read after refresh failure target=%s",
                    channel_id,
                )
                await self._show_ai_comments_stale(
                    callback,
                    state,
                    notice="Обновление не выполнено, память временно недоступна.",
                )
                return
            await self._render_ai_channel_memory(
                callback.message,
                memory,
                notice=(
                    "Обновление не выполнено. Проверьте доступ подключённых "
                    "аккаунтов к каналу."
                ),
            )
            return
        await self._render_ai_channel_memory(
            callback.message,
            memory,
            notice=(
                f"Обработано {result['processed']}: новых {result['created']}, "
                f"изменено {result['updated']}, без изменений {result['unchanged']}."
            ),
        )

    async def ai_comments_memory_edit(
        self,
        callback: CallbackQuery,
        state: FSMContext,
    ) -> None:
        if await self._deny_if_needed(callback):
            return
        try:
            field, channel_id, expected_version = (
                parse_ai_channel_memory_edit_callback(callback.data)
            )
        except ValueError:
            await self._show_ai_comments_stale(callback, state)
            return
        if not await activate_ai_comments_state(
            callback,
            state,
            AICommentsUI.channel_memory_edit,
        ):
            return
        selected = await self._selected_ai_channel_id(state)
        if selected != channel_id:
            await self._show_ai_comments_stale(
                callback,
                state,
                notice="Канал был изменён. Старое поле не открыто.",
            )
            return
        try:
            profile = await self.db.get_ai_channel_profile(channel_id)
        except Exception:  # noqa: BLE001
            profile = None
        if profile is None or profile.profile_version != expected_version:
            await self._show_ai_comments_stale(
                callback,
                state,
                notice="Профиль уже обновлён. Откройте память заново.",
            )
            return
        spec = AI_CHANNEL_PROFILE_FIELDS[field]
        await state.update_data(
            ai_comments_memory_field=field,
            ai_comments_memory_channel_id=channel_id,
            ai_comments_memory_expected_version=expected_version,
        )
        await self._safe_edit_text(
            callback.message,
            f"{spec.label}\n\n"
            f"{html.escape(spec.prompt)}.\n\n"
            "Отправьте новое значение одним сообщением. "
            "Чтобы очистить поле, отправьте только <code>-</code>.\n\n"
            f"Версия профиля: <b>{expected_version}</b>",
            reply_markup=ai_comments_memory_input_keyboard(),
        )
        await callback.answer()

    async def ai_comments_memory_input(
        self,
        message: Message,
        state: FSMContext,
    ) -> None:
        if await self._deny_if_needed(message):
            return
        data = await state.get_data()
        field = data.get("ai_comments_memory_field")
        channel_id = data.get("ai_comments_memory_channel_id")
        expected_version = data.get("ai_comments_memory_expected_version")
        if (
            not isinstance(field, str)
            or field not in AI_CHANNEL_PROFILE_FIELDS
            or not isinstance(channel_id, int)
            or isinstance(channel_id, bool)
            or not isinstance(expected_version, int)
            or isinstance(expected_version, bool)
        ):
            await state.set_state(AICommentsUI.menu)
            await message.answer(
                "⚠️ Редактирование устарело. Откройте память канала заново.",
                reply_markup=ai_comments_menu_keyboard(),
            )
            return
        try:
            value = parse_ai_channel_profile_input(field, message.text)
            async with self._ai_comments_memory_lock(channel_id):
                await self.db.update_ai_channel_profile_field(
                    channel_id,
                    field,
                    value,
                    expected_version=expected_version,
                    updated_by=int(message.from_user.id),
                )
            memory = await self.db.get_ai_channel_memory(channel_id)
        except AIChannelMemoryConflictError as exc:
            await state.set_state(AICommentsUI.channel_memory)
            memory = await self.db.get_ai_channel_memory(channel_id)
            await message.answer(
                ai_channel_memory_text(memory, notice=str(exc)),
                reply_markup=ai_comments_memory_keyboard(
                    channel_id=channel_id,
                    profile_version=memory.profile.profile_version,
                    active_scenarios=[
                        (scenario.id, _scenario_label(scenario))
                        for scenario in memory.active_scenarios
                    ],
                ),
            )
            return
        except (AIChannelMemoryError, TypeError, ValueError) as exc:
            await message.answer(
                f"⚠️ {html.escape(str(exc))}\n\nПопробуйте ещё раз или отмените ввод.",
                reply_markup=ai_comments_memory_input_keyboard(),
            )
            return
        await state.set_state(AICommentsUI.channel_memory)
        await state.update_data(
            ai_comments_memory_field=None,
            ai_comments_memory_expected_version=None,
        )
        await message.answer(
            ai_channel_memory_text(memory, notice="Поле сохранено."),
            reply_markup=ai_comments_memory_keyboard(
                channel_id=channel_id,
                profile_version=memory.profile.profile_version,
                active_scenarios=[
                    (scenario.id, _scenario_label(scenario))
                    for scenario in memory.active_scenarios
                ],
            ),
        )

    async def ai_comments_posts(
        self,
        callback: CallbackQuery,
        state: FSMContext,
    ) -> None:
        if await self._deny_if_needed(callback):
            return
        try:
            requested_page = parse_ai_channel_posts_page_callback(callback.data)
        except ValueError:
            await self._show_ai_comments_stale(callback, state)
            return
        if not await activate_ai_comments_state(
            callback,
            state,
            AICommentsUI.recent_posts,
        ):
            return
        channel_id = await self._selected_ai_channel_id(state)
        if channel_id is None:
            await self._show_missing_ai_channel(callback, state)
            return
        try:
            memory = await self.db.get_ai_channel_memory(channel_id)
        except Exception:  # noqa: BLE001
            await self._show_ai_comments_stale(callback, state)
            return
        posts, safe_page, total_pages = paginate_ai_channel_posts(
            memory.recent_posts,
            requested_page,
        )
        lines = []
        for post in posts:
            source = post.text or post.media_caption or f"[{post.media_type or 'медиа'}]"
            scenario = (
                f" · сценарий {post.linked_scenario_id}"
                if post.linked_scenario_id is not None
                else ""
            )
            lines.append(
                f"<b>#{post.telegram_message_id}</b> · rev {post.source_revision}{scenario}\n"
                f"{_escaped(source, limit=360)}"
            )
        body = "\n\n".join(lines) if lines else "<i>Публикации ещё не загружены.</i>"
        await self._safe_edit_text(
            callback.message,
            "📚 <b>Последние публикации</b>\n\n"
            f"Канал: <b>{_escaped(memory.profile.channel_title, limit=180)}</b>\n"
            f"Оперативное окно: <b>{len(memory.recent_posts)}/10</b>\n"
            f"Страница: <b>{safe_page + 1}/{total_pages}</b>\n\n"
            f"{body}",
            reply_markup=ai_comments_recent_posts_keyboard(
                [
                    (
                        post.id,
                        post.telegram_message_id,
                        post.linked_scenario_id is not None,
                    )
                    for post in posts
                ],
                page=safe_page,
                total_pages=total_pages,
            ),
        )
        if safe_page != requested_page:
            await callback.answer("Список изменился, показана доступная страница.")
        else:
            await callback.answer()

    async def _render_ai_channel_post(
        self,
        message: Message,
        *,
        channel_id: int,
        post_id: int,
        page: int,
        notice: str | None = None,
    ) -> bool:
        post = await self.db.get_ai_channel_post(channel_id, post_id)
        if post is None:
            return False
        memory = await self.db.get_ai_channel_memory(channel_id)
        revision_loader = getattr(self.db, "list_ai_channel_post_revisions", None)
        revisions = (
            await revision_loader(channel_id, post_id, limit=5)
            if revision_loader is not None
            else ()
        )
        source = post.text or post.media_caption or f"[{post.media_type or 'медиа'}]"
        notice_line = f"\n✅ {_escaped(notice, limit=300)}\n" if notice else ""
        scenario_line = (
            f"<code>{post.linked_scenario_id}</code>"
            if post.linked_scenario_id is not None
            else "<i>не связан</i>"
        )
        revision_lines = [
            f"- rev {revision.source_revision} · "
            f"{html.escape(revision.revision_reason)} · "
            f"<code>{revision.normalized_text_hash[:12]}…</code> · "
            f"{_escaped(revision.text or revision.media_caption or '[медиа]', limit=90)}"
            for revision in revisions
        ] or ["- <i>история ревизий пока пуста</i>"]
        await self._safe_edit_text(
            message,
            "📄 <b>Публикация в памяти</b>\n\n"
            f"Канал: <b>{_escaped(memory.profile.channel_title, limit=180)}</b>\n"
            f"Telegram message ID: <code>{post.telegram_message_id}</code>\n"
            f"Ревизия источника: <b>{post.source_revision}</b>\n"
            f"Опубликовано: <code>{post.posted_at.isoformat()}Z</code>\n"
            f"Изменено: <code>{post.edited_at.isoformat() + 'Z' if post.edited_at else '-'}</code>\n"
            f"Тип: <b>{html.escape(post.media_type or 'текст')}</b>\n"
            f"Сценарий: {scenario_line}\n"
            f"SHA-256: <code>{post.normalized_text_hash}</code>\n"
            f"{notice_line}\n"
            f"<b>Фактический текст</b>\n{_escaped(source, limit=2_250)}\n\n"
            "<b>Неизменяемые ревизии</b>\n"
            + "\n".join(revision_lines),
            reply_markup=ai_comments_post_keyboard(
                post_id=post.id,
                page=page,
                active_scenarios=[
                    (scenario.id, _scenario_label(scenario))
                    for scenario in memory.active_scenarios
                ],
                linked_scenario_id=post.linked_scenario_id,
            ),
        )
        return True

    async def ai_comments_post(
        self,
        callback: CallbackQuery,
        state: FSMContext,
    ) -> None:
        if await self._deny_if_needed(callback):
            return
        try:
            post_id, page = parse_ai_channel_post_callback(callback.data)
        except ValueError:
            await self._show_ai_comments_stale(callback, state)
            return
        if not await activate_ai_comments_state(
            callback,
            state,
            AICommentsUI.recent_posts,
        ):
            return
        channel_id = await self._selected_ai_channel_id(state)
        if channel_id is None:
            await self._show_missing_ai_channel(callback, state)
            return
        try:
            shown = await self._render_ai_channel_post(
                callback.message,
                channel_id=channel_id,
                post_id=post_id,
                page=page,
            )
        except Exception:  # noqa: BLE001
            logger.exception(
                "AI channel post could not be read target=%s post=%s",
                channel_id,
                post_id,
            )
            shown = False
        if not shown:
            await self._show_ai_comments_stale(
                callback,
                state,
                notice="Публикация больше недоступна в выбранном канале.",
            )
            return
        await callback.answer()

    async def ai_comments_post_link(
        self,
        callback: CallbackQuery,
        state: FSMContext,
    ) -> None:
        if await self._deny_if_needed(callback):
            return
        try:
            post_id, scenario_id, page = parse_ai_channel_post_link_callback(
                callback.data
            )
        except ValueError:
            await self._show_ai_comments_stale(callback, state)
            return
        if not await activate_ai_comments_state(
            callback,
            state,
            AICommentsUI.recent_posts,
        ):
            return
        channel_id = await self._selected_ai_channel_id(state)
        if channel_id is None:
            await self._show_missing_ai_channel(callback, state)
            return
        try:
            async with self._ai_comments_memory_lock(channel_id):
                result = await self.db.link_ai_channel_post_to_scenario(
                    channel_id,
                    post_id,
                    scenario_id,
                )
            shown = await self._render_ai_channel_post(
                callback.message,
                channel_id=channel_id,
                post_id=post_id,
                page=page,
                notice=(
                    "Публикация связана со сценарием."
                    if result["changed"]
                    else "Связь уже была сохранена."
                ),
            )
        except (AIChannelMemoryError, ValueError) as exc:
            await callback.answer(str(exc), show_alert=True)
            return
        except Exception:  # noqa: BLE001
            logger.exception(
                "AI channel post scenario link failed target=%s post=%s scenario=%s",
                channel_id,
                post_id,
                scenario_id,
            )
            await self._show_ai_comments_stale(
                callback,
                state,
                notice="Связь не сохранена из-за временной ошибки.",
            )
            return
        if not shown:
            await self._show_ai_comments_stale(callback, state)
            return
        await callback.answer()

    async def ai_comments_scenario_new(
        self,
        callback: CallbackQuery,
        state: FSMContext,
    ) -> None:
        if await self._deny_if_needed(callback):
            return
        if not await activate_ai_comments_state(
            callback,
            state,
            AICommentsUI.channel_scenario_edit,
        ):
            return
        channel_id = await self._selected_ai_channel_id(state)
        if channel_id is None:
            await self._show_missing_ai_channel(callback, state)
            return
        await state.update_data(ai_comments_scenario_channel_id=channel_id)
        await self._safe_edit_text(
            callback.message,
            "🎯 <b>Новый активный сценарий</b>\n\n"
            "Отправьте пять строк:\n"
            "1. Название сценария\n"
            "2. SYMBOL или <code>-</code>\n"
            "3. LONG, SHORT или <code>-</code>\n"
            "4. Telegram ID открывающего поста или <code>-</code>\n"
            "5. Только фактическая сводка или <code>-</code>\n\n"
            "Пример:\n"
            "<code>BTC возврат в POI\nBTCUSDT\nLONG\n4581\n"
            "Автор ждёт подтверждение после возврата в зону</code>",
            reply_markup=ai_comments_memory_input_keyboard(),
        )
        await callback.answer()

    async def ai_comments_scenario_input(
        self,
        message: Message,
        state: FSMContext,
    ) -> None:
        if await self._deny_if_needed(message):
            return
        data = await state.get_data()
        channel_id = data.get("ai_comments_scenario_channel_id")
        if not isinstance(channel_id, int) or isinstance(channel_id, bool):
            await state.set_state(AICommentsUI.menu)
            await message.answer(
                "⚠️ Создание сценария устарело. Откройте память заново.",
                reply_markup=ai_comments_menu_keyboard(),
            )
            return
        try:
            values = parse_ai_channel_scenario_input(message.text)
            async with self._ai_comments_memory_lock(channel_id):
                await self.db.create_ai_channel_scenario(channel_id, **values)
            memory = await self.db.get_ai_channel_memory(channel_id)
        except (AIChannelMemoryError, TypeError, ValueError) as exc:
            await message.answer(
                f"⚠️ {html.escape(str(exc))}\n\nИсправьте пять строк или отмените ввод.",
                reply_markup=ai_comments_memory_input_keyboard(),
            )
            return
        await state.set_state(AICommentsUI.channel_memory)
        await state.update_data(ai_comments_scenario_channel_id=None)
        await message.answer(
            ai_channel_memory_text(memory, notice="Активный сценарий создан."),
            reply_markup=ai_comments_memory_keyboard(
                channel_id=channel_id,
                profile_version=memory.profile.profile_version,
                active_scenarios=[
                    (scenario.id, _scenario_label(scenario))
                    for scenario in memory.active_scenarios
                ],
            ),
        )

    async def ai_comments_scenario_complete_confirm(
        self,
        callback: CallbackQuery,
        state: FSMContext,
    ) -> None:
        if await self._deny_if_needed(callback):
            return
        try:
            scenario_id = parse_ai_channel_scenario_callback(
                callback.data,
                action="confirm",
            )
        except ValueError:
            await self._show_ai_comments_stale(callback, state)
            return
        if not await activate_ai_comments_state(
            callback,
            state,
            AICommentsUI.channel_scenario_confirm,
        ):
            return
        channel_id = await self._selected_ai_channel_id(state)
        if channel_id is None:
            await self._show_missing_ai_channel(callback, state)
            return
        try:
            memory = await self.db.get_ai_channel_memory(channel_id)
        except Exception:  # noqa: BLE001
            logger.exception(
                "AI channel scenario confirmation could not be read target=%s scenario=%s",
                channel_id,
                scenario_id,
            )
            await self._show_ai_comments_stale(
                callback,
                state,
                notice="Сценарий временно недоступен.",
            )
            return
        scenario = next(
            (item for item in memory.active_scenarios if item.id == scenario_id),
            None,
        )
        if scenario is None:
            await self._show_ai_comments_stale(
                callback,
                state,
                notice="Сценарий уже не активен.",
            )
            return
        await state.update_data(ai_comments_scenario_complete_id=scenario_id)
        await self._safe_edit_text(
            callback.message,
            "✅ <b>Завершить сценарий?</b>\n\n"
            f"{html.escape(_scenario_label(scenario))}\n\n"
            "Запись останется в аудите, но исчезнет из списка активных.",
            reply_markup=ai_comments_scenario_complete_keyboard(
                scenario_id=scenario_id
            ),
        )
        await callback.answer()

    async def ai_comments_scenario_complete_apply(
        self,
        callback: CallbackQuery,
        state: FSMContext,
    ) -> None:
        if await self._deny_if_needed(callback):
            return
        try:
            scenario_id = parse_ai_channel_scenario_callback(
                callback.data,
                action="apply",
            )
        except ValueError:
            await self._show_ai_comments_stale(callback, state)
            return
        if not await activate_ai_comments_state(
            callback,
            state,
            AICommentsUI.channel_scenario_confirm,
        ):
            return
        data = await state.get_data()
        channel_id = selected_ai_channel_id(data)
        if (
            channel_id is None
            or data.get("ai_comments_scenario_complete_id") != scenario_id
        ):
            await self._show_ai_comments_stale(
                callback,
                state,
                notice="Подтверждение сценария устарело.",
            )
            return
        try:
            async with self._ai_comments_memory_lock(channel_id):
                result = await self.db.complete_ai_channel_scenario(
                    channel_id,
                    scenario_id,
                )
            memory = await self.db.get_ai_channel_memory(channel_id)
        except (AIChannelMemoryError, ValueError) as exc:
            await callback.answer(str(exc), show_alert=True)
            return
        except Exception:  # noqa: BLE001
            logger.exception(
                "AI channel scenario completion failed target=%s scenario=%s",
                channel_id,
                scenario_id,
            )
            await self._show_ai_comments_stale(
                callback,
                state,
                notice="Сценарий не изменён из-за временной ошибки.",
            )
            return
        await state.set_state(AICommentsUI.channel_memory)
        await state.update_data(ai_comments_scenario_complete_id=None)
        await self._render_ai_channel_memory(
            callback.message,
            memory,
            notice=(
                "Сценарий завершён."
                if result["changed"]
                else "Сценарий уже был завершён."
            ),
        )
        await callback.answer()
