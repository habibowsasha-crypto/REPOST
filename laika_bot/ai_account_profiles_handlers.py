from __future__ import annotations

import asyncio
import html
import math
import re
from collections.abc import Mapping, Sequence
from decimal import Decimal, InvalidOperation

from aiogram import F, Router
from aiogram.fsm.context import FSMContext
from aiogram.types import CallbackQuery, Message

from .ai_account_profiles import (
    AI_ACCOUNT_KNOWLEDGE_LEVELS,
    AI_ACCOUNT_MISTAKE_LEVELS,
    AI_ACCOUNT_PROFILE_PAGE_SIZE,
    AI_ACCOUNT_PUNCTUATION_MODES,
    AI_ACCOUNT_ROLES,
    AI_ACCOUNT_UPPERCASE_MODES,
    AIAccountProfileEligibility,
    AIAccountProfileSnapshot,
    normalize_profile_list,
)
from .ai_account_profiles_keyboards import (
    ai_account_profile_confirm_keyboard,
    ai_account_profile_history_keyboard,
    ai_account_profile_input_keyboard,
    ai_account_profile_keyboard,
    ai_account_profiles_list_keyboard,
)
from .ai_comments_states import AICommentsUI
from .ai_comments_ui import activate_ai_comments_state
from .utils import truncate

MAX_AI_ACCOUNT_PROFILE_PAGE = 1_000_000
MAX_DATABASE_ID = 9_223_372_036_854_775_807

_EDIT_SECTIONS = frozenset({"b", "s", "l", "c"})
_KNOWLEDGE_ALIASES = {
    **{key: key for key in AI_ACCOUNT_KNOWLEDGE_LEVELS},
    **{value.casefold(): key for key, value in AI_ACCOUNT_KNOWLEDGE_LEVELS.items()},
}
_ROLE_ALIASES = {
    **{key: key for key in AI_ACCOUNT_ROLES},
    **{value.casefold(): key for key, value in AI_ACCOUNT_ROLES.items()},
}


def _bounded_id(value: str, *, field: str) -> int:
    parsed = int(value)
    if not 0 < parsed <= MAX_DATABASE_ID:
        raise ValueError(f"Некорректный {field}")
    return parsed


def parse_ai_account_profiles_page_callback(data: str | None) -> int:
    if data == "aic:profiles":
        return 0
    match = re.fullmatch(r"aic:profiles:([0-9]+)", data or "")
    if match is None:
        raise ValueError("Некорректная страница профилей")
    page = int(match.group(1))
    if page > MAX_AI_ACCOUNT_PROFILE_PAGE:
        raise ValueError("Некорректная страница профилей")
    return page


def parse_ai_account_profile_callback(data: str | None) -> tuple[int, int]:
    match = re.fullmatch(r"aic:pr:([1-9][0-9]*):([0-9]+)", data or "")
    if match is None:
        raise ValueError("Некорректная кнопка профиля")
    profile_id = _bounded_id(match.group(1), field="ID профиля")
    page = int(match.group(2))
    if page > MAX_AI_ACCOUNT_PROFILE_PAGE:
        raise ValueError("Некорректная страница профилей")
    return profile_id, page


def parse_ai_account_profile_edit_callback(
    data: str | None,
) -> tuple[str, int, int, int]:
    match = re.fullmatch(
        r"aic:pe:([bslc]):([1-9][0-9]*):([1-9][0-9]*):([0-9]+)",
        data or "",
    )
    if match is None:
        raise ValueError("Некорректная кнопка редактирования профиля")
    section = match.group(1)
    profile_id = _bounded_id(match.group(2), field="ID профиля")
    version = _bounded_id(match.group(3), field="версия профиля")
    page = int(match.group(4))
    if section not in _EDIT_SECTIONS or page > MAX_AI_ACCOUNT_PROFILE_PAGE:
        raise ValueError("Некорректная кнопка редактирования профиля")
    return section, profile_id, version, page


def parse_ai_account_profile_toggle_callback(
    data: str | None,
) -> tuple[int, int, bool, int]:
    match = re.fullmatch(
        r"aic:pt:([1-9][0-9]*):([1-9][0-9]*):([01]):([0-9]+)",
        data or "",
    )
    if match is None:
        raise ValueError("Некорректная кнопка состояния профиля")
    profile_id = _bounded_id(match.group(1), field="ID профиля")
    version = _bounded_id(match.group(2), field="версия профиля")
    page = int(match.group(4))
    if page > MAX_AI_ACCOUNT_PROFILE_PAGE:
        raise ValueError("Некорректная страница профилей")
    return profile_id, version, match.group(3) == "1", page


def parse_ai_account_profile_action_callback(
    data: str | None,
    *,
    action: str,
) -> tuple[int, int, int]:
    prefix = {
        "regenerate_confirm": "prc",
        "regenerate_apply": "pra",
        "retire_confirm": "pdc",
        "retire_apply": "pda",
    }.get(action)
    if prefix is None:
        raise ValueError("Некорректное действие профиля")
    match = re.fullmatch(
        rf"aic:{prefix}:([1-9][0-9]*):([1-9][0-9]*):([0-9]+)",
        data or "",
    )
    if match is None:
        raise ValueError("Некорректная кнопка действия профиля")
    profile_id = _bounded_id(match.group(1), field="ID профиля")
    version = _bounded_id(match.group(2), field="версия профиля")
    page = int(match.group(3))
    if page > MAX_AI_ACCOUNT_PROFILE_PAGE:
        raise ValueError("Некорректная страница профилей")
    return profile_id, version, page


def parse_ai_account_profile_history_callback(data: str | None) -> tuple[int, int]:
    match = re.fullmatch(r"aic:ph:([1-9][0-9]*):([0-9]+)", data or "")
    if match is None:
        raise ValueError("Некорректная кнопка истории профиля")
    profile_id = _bounded_id(match.group(1), field="ID профиля")
    page = int(match.group(2))
    if page > MAX_AI_ACCOUNT_PROFILE_PAGE:
        raise ValueError("Некорректная страница профилей")
    return profile_id, page


def paginate_ai_account_profiles(
    profiles: Sequence[AIAccountProfileSnapshot],
    page: int,
) -> tuple[list[AIAccountProfileSnapshot], int, int]:
    total_pages = max(1, math.ceil(len(profiles) / AI_ACCOUNT_PROFILE_PAGE_SIZE))
    safe_page = min(max(0, page), total_pages - 1)
    start = safe_page * AI_ACCOUNT_PROFILE_PAGE_SIZE
    return (
        list(profiles[start : start + AI_ACCOUNT_PROFILE_PAGE_SIZE]),
        safe_page,
        total_pages,
    )


def _lines(raw: str | None, *, expected: int) -> list[str]:
    if raw is None:
        raise ValueError("Отправьте данные обычным текстом")
    lines = [line.strip() for line in raw.replace("\r\n", "\n").split("\n")]
    if len(lines) != expected:
        raise ValueError(f"Нужно отправить ровно {expected} строк по шаблону")
    return lines


def _optional(value: str) -> str | None:
    return None if not value or value == "-" else value


def _alias(value: str, aliases: Mapping[str, str], *, field: str) -> str:
    result = aliases.get(value) or aliases.get(value.casefold())
    if result is None:
        raise ValueError(f"Некорректное значение поля «{field}»")
    return result


def parse_ai_account_profile_basic_input(raw: str | None) -> dict[str, object]:
    name, knowledge, role, tone, vocabulary = _lines(raw, expected=5)
    if not name or name == "-":
        raise ValueError("Название профиля обязательно")
    return {
        "name": name,
        "knowledge_level": _alias(
            knowledge,
            _KNOWLEDGE_ALIASES,
            field="уровень знаний",
        ),
        "role": _alias(role, _ROLE_ALIASES, field="роль"),
        "style": {
            "tone": _optional(tone),
            "vocabulary": _optional(vocabulary),
        },
    }


def parse_ai_account_profile_style_input(raw: str | None) -> dict[str, object]:
    uppercase, punctuation, mistakes, favorites, sentence_pattern = _lines(
        raw,
        expected=5,
    )
    if uppercase not in AI_ACCOUNT_UPPERCASE_MODES:
        raise ValueError("Заглавные: never, rare или sometimes")
    if punctuation not in AI_ACCOUNT_PUNCTUATION_MODES:
        raise ValueError("Пунктуация: minimal, loose или clean")
    if mistakes not in AI_ACCOUNT_MISTAKE_LEVELS:
        raise ValueError("Ошибки: none или light")
    return {
        "style": {
            "uppercase_mode": uppercase,
            "punctuation_mode": punctuation,
            "mistake_level": mistakes,
            "favorite_words": normalize_profile_list(
                () if favorites == "-" else favorites,
                field="Любимые слова",
                max_items=12,
                max_item_length=60,
            ),
            "sentence_pattern": _optional(sentence_pattern),
        }
    }


def _integer(value: str, *, field: str) -> int:
    if not re.fullmatch(r"[0-9]+", value):
        raise ValueError(f"{field} должен быть целым неотрицательным числом")
    return int(value)


def _percent(value: str, *, field: str) -> Decimal:
    try:
        result = Decimal(value.replace(",", "."))
    except InvalidOperation as exc:
        raise ValueError(f"{field} должен быть числом от 0 до 100") from exc
    if not result.is_finite() or result < 0 or result > 100:
        raise ValueError(f"{field} должен быть от 0 до 100")
    return result / Decimal(100)


def parse_ai_account_profile_limits_input(raw: str | None) -> dict[str, object]:
    values = [line.strip() for line in (raw or "").splitlines() if line.strip()]
    if len(values) not in {8, 9}:
        raise ValueError("Нужно отправить 9 строк (старый формат из 8 строк тоже поддерживается)")
    (
        min_length,
        max_length,
        emoji_rate,
        question_rate,
        reply_rate,
        disagreement_rate,
        daily_limit,
        cooldown_minutes,
        *bonus_values,
    ) = values
    reply_bonus_slots = bonus_values[0] if bonus_values else "3"
    return {
        "min_length": _integer(min_length, field="Минимальная длина"),
        "max_length": _integer(max_length, field="Максимальная длина"),
        "emoji_rate": _percent(emoji_rate, field="Частота эмодзи"),
        "question_rate": _percent(question_rate, field="Частота вопросов"),
        "reply_rate": _percent(reply_rate, field="Частота ответов"),
        "disagreement_rate": _percent(
            disagreement_rate,
            field="Частота возражений",
        ),
        "daily_limit": _integer(daily_limit, field="Дневной лимит"),
        "reply_bonus_slots": _integer(reply_bonus_slots, field="Бонусные слоты"),
        "cooldown_seconds": _integer(
            cooldown_minutes,
            field="Cooldown",
        )
        * 60,
    }


def parse_ai_account_profile_claims_input(raw: str | None) -> dict[str, object]:
    allowed, forbidden = _lines(raw, expected=2)
    return {
        "allowed_claims": normalize_profile_list(
            () if allowed == "-" else allowed,
            field="Разрешённые утверждения",
        ),
        "forbidden_claims": normalize_profile_list(
            () if forbidden == "-" else forbidden,
            field="Запрещённые утверждения",
        ),
    }


def parse_ai_account_profile_section_input(
    section: str,
    raw: str | None,
) -> dict[str, object]:
    parser = {
        "b": parse_ai_account_profile_basic_input,
        "s": parse_ai_account_profile_style_input,
        "l": parse_ai_account_profile_limits_input,
        "c": parse_ai_account_profile_claims_input,
    }.get(section)
    if parser is None:
        raise ValueError("Некорректный раздел профиля")
    return parser(raw)


def _escaped(value: object, *, limit: int = 180) -> str:
    if value is None or value == "" or value == ():
        return "<i>не задано</i>"
    if isinstance(value, (tuple, list)):
        text = ", ".join(str(item) for item in value)
    else:
        text = str(value)
    return truncate(html.escape(text), limit)


def _percent_text(value: Decimal) -> str:
    return f"{format((value * Decimal(100)).normalize(), 'f')}%"


def ai_account_profile_icon(profile: AIAccountProfileSnapshot) -> str:
    if profile.retired:
        return "🗄"
    if profile.account_id is None:
        return "⚪️"
    if not profile.enabled:
        return "🔴"
    if (
        not profile.account_is_active
        or profile.account_status != "ready"
        or not profile.account_has_session
    ):
        return "🟠"
    return "🟢"


def ai_account_profile_text(
    profile: AIAccountProfileSnapshot,
    eligibility: AIAccountProfileEligibility,
    *,
    notice: str | None = None,
) -> str:
    account_name = profile.account_display_name or "аккаунт удалён"
    username = f"@{profile.account_username}" if profile.account_username else "без username"
    notice_line = f"\n⚠️ {_escaped(notice, limit=300)}\n" if notice else ""
    return (
        "🎭 <b>Индивидуальный профиль</b>\n\n"
        f"Аккаунт: <b>{_escaped(account_name)}</b> ({_escaped(username)})\n"
        f"Telegram ID: <code>{profile.telegram_user_id or 'не сохранён'}</code>\n"
        f"Связь Account: <b>{profile.account_id or 'нет'}</b>\n"
        f"Core-статус: <b>{_escaped(profile.account_status or 'удалён')}</b>\n"
        f"Профиль: <b>{'ВКЛ' if profile.enabled else 'ВЫКЛ'}</b>"
        f"{' · архив' if profile.retired else ''}\n"
        f"Готовность: <b>{'🟢 да' if eligibility.allowed else '🔴 нет'}</b> — "
        f"{_escaped(eligibility.reason)}\n"
        f"Сегодня: <b>{eligibility.comments_today}/{eligibility.daily_limit}</b>"
        f" · бонус ответов: <b>{eligibility.reply_bonus_available}/{profile.reply_bonus_slots}</b>"
        f" · cooldown: <b>{eligibility.cooldown_remaining_seconds}с</b>"
        f"{notice_line}\n"
        "<b>Личность</b>\n"
        f"- Название: {_escaped(profile.name)}\n"
        f"- Preset: {_escaped(profile.preset_key)}\n"
        f"- Знания: {_escaped(AI_ACCOUNT_KNOWLEDGE_LEVELS.get(profile.knowledge_level, profile.knowledge_level))}\n"
        f"- Роль: {_escaped(AI_ACCOUNT_ROLES.get(profile.role, profile.role))}\n"
        f"- Тон: {_escaped(profile.tone)}\n"
        f"- Словарь: {_escaped(profile.vocabulary)}\n"
        f"- Манера фраз: {_escaped(profile.sentence_pattern)}\n"
        f"- Заглавные: {_escaped(AI_ACCOUNT_UPPERCASE_MODES.get(profile.uppercase_mode, profile.uppercase_mode))}\n"
        f"- Пунктуация: {_escaped(AI_ACCOUNT_PUNCTUATION_MODES.get(profile.punctuation_mode, profile.punctuation_mode))}\n"
        f"- Ошибки: {_escaped(AI_ACCOUNT_MISTAKE_LEVELS.get(profile.mistake_level, profile.mistake_level))}\n"
        f"- Любимые слова: {_escaped(profile.favorite_words)}\n\n"
        "<b>Ограничения</b>\n"
        f"- Длина: <b>{profile.min_length}-{profile.max_length}</b> символов\n"
        f"- Эмодзи: <b>{_percent_text(profile.emoji_rate)}</b>\n"
        f"- Вопросы: <b>{_percent_text(profile.question_rate)}</b>\n"
        f"- Ответы: <b>{_percent_text(profile.reply_rate)}</b>\n"
        f"- Возражения: <b>{_percent_text(profile.disagreement_rate)}</b>\n"
        f"- Лимит: <b>{profile.daily_limit}/день</b>\n"
        f"- Бонус за входящий ответ: <b>до {profile.reply_bonus_slots}/день</b>\n"
        f"- Сброс: <b>00:00 {eligibility.day_timezone}</b>\n"
        f"- Cooldown: <b>{profile.cooldown_seconds // 60} мин</b>\n"
        f"- Разрешено: {_escaped(profile.allowed_claims, limit=260)}\n"
        f"- Запрещено: {_escaped(profile.forbidden_claims, limit=320)}\n\n"
        f"Версия: <b>{profile.profile_version}</b> · генерация <b>{profile.generation}</b>"
    )


class AIAccountProfilesHandlersMixin:
    """Admin-only persona CRUD without OpenAI calls or Telegram publication."""

    def _register_ai_account_profile_handlers(self, router: Router) -> None:
        router.callback_query.register(
            self.ai_account_profiles,
            F.data == "aic:profiles",
        )
        router.callback_query.register(
            self.ai_account_profiles,
            F.data.startswith("aic:profiles:"),
        )
        router.callback_query.register(
            self.ai_account_profile,
            F.data.startswith("aic:pr:"),
        )
        router.callback_query.register(
            self.ai_account_profile_edit,
            F.data.startswith("aic:pe:"),
        )
        router.callback_query.register(
            self.ai_account_profile_toggle,
            F.data.startswith("aic:pt:"),
        )
        router.callback_query.register(
            self.ai_account_profile_regenerate_confirm,
            F.data.startswith("aic:prc:"),
        )
        router.callback_query.register(
            self.ai_account_profile_regenerate_apply,
            F.data.startswith("aic:pra:"),
        )
        router.callback_query.register(
            self.ai_account_profile_retire_confirm,
            F.data.startswith("aic:pdc:"),
        )
        router.callback_query.register(
            self.ai_account_profile_retire_apply,
            F.data.startswith("aic:pda:"),
        )
        router.callback_query.register(
            self.ai_account_profile_history,
            F.data.startswith("aic:ph:"),
        )
        router.message.register(
            self.ai_account_profile_input,
            AICommentsUI.account_profile_edit,
        )

    def _ai_account_profile_lock(self, profile_id: int) -> asyncio.Lock:
        locks = getattr(self, "_ai_account_profile_locks", None)
        if locks is None:
            locks = {}
            self._ai_account_profile_locks = locks
        lock = locks.get(profile_id)
        if lock is None:
            lock = asyncio.Lock()
            locks[profile_id] = lock
        return lock

    async def _render_ai_account_profiles(
        self,
        message: Message,
        *,
        requested_page: int,
        notice: str | None = None,
    ) -> int:
        sync = await self.db.sync_ai_account_profiles()
        profiles = await self.db.list_ai_account_profiles()
        page_items, safe_page, total_pages = paginate_ai_account_profiles(
            profiles,
            requested_page,
        )
        buttons = [
            (
                profile.id,
                truncate(
                    f"{profile.account_display_name or 'Удалённый аккаунт'} · {profile.name}",
                    48,
                ),
                ai_account_profile_icon(profile),
            )
            for profile in page_items
        ]
        page_line = (
            f"\nСтраница: <b>{safe_page + 1}/{total_pages}</b>"
            if total_pages > 1
            else ""
        )
        notice_line = f"\n⚠️ {_escaped(notice, limit=300)}\n" if notice else ""
        await self._safe_edit_text(
            message,
            "🎭 <b>Профили аккаунтов</b>\n\n"
            f"Подключённых аккаунтов: <b>{sync['accounts']}</b>\n"
            f"Сохранённых профилей: <b>{sync['profiles']}</b>{page_line}\n"
            f"Автоматически создано сейчас: <b>{sync['created']}</b>\n"
            f"Восстановлено связей: <b>{sync['reattached']}</b>"
            f"{notice_line}\n"
            "Новый аккаунт получает уникальную личность автоматически. Профили "
            "создаются выключенными и не могут ничего публиковать на этом шаге.",
            reply_markup=ai_account_profiles_list_keyboard(
                buttons,
                page=safe_page,
                total_pages=total_pages,
            ),
        )
        return safe_page

    async def _render_ai_account_profile(
        self,
        message: Message,
        profile: AIAccountProfileSnapshot,
        *,
        page: int,
        notice: str | None = None,
    ) -> None:
        eligibility = await self.db.get_ai_account_profile_eligibility(profile.id, timezone_name=getattr(self.settings, "ai_comments_timezone", "Europe/Moscow"))
        await self._safe_edit_text(
            message,
            ai_account_profile_text(profile, eligibility, notice=notice),
            reply_markup=ai_account_profile_keyboard(
                profile_id=profile.id,
                profile_version=profile.profile_version,
                page=page,
                enabled=profile.enabled,
                retired=profile.retired,
            ),
        )

    async def _show_ai_account_profile_stale(
        self,
        callback: CallbackQuery,
        state: FSMContext,
        *,
        page: int = 0,
        notice: str = "Профиль уже изменён или недоступен. Список обновлён.",
    ) -> None:
        await state.set_state(AICommentsUI.account_profiles)
        await state.update_data(
            ai_account_profile_id=None,
            ai_account_profile_version=None,
            ai_account_profile_section=None,
            ai_account_profile_action=None,
        )
        try:
            await self._render_ai_account_profiles(
                callback.message,
                requested_page=page,
                notice=notice,
            )
        except Exception:  # noqa: BLE001
            await self._show_ai_comments_stale(
                callback,
                state,
                notice="Профили временно недоступны. Ничего не изменено.",
            )
            return
        await callback.answer(notice, show_alert=True)

    async def ai_account_profiles(
        self,
        callback: CallbackQuery,
        state: FSMContext,
    ) -> None:
        if await self._deny_if_needed(callback):
            return
        try:
            requested_page = parse_ai_account_profiles_page_callback(callback.data)
        except ValueError:
            await self._show_ai_comments_stale(callback, state)
            return
        if not await activate_ai_comments_state(
            callback,
            state,
            AICommentsUI.account_profiles,
        ):
            return
        await state.update_data(
            ai_account_profile_id=None,
            ai_account_profile_version=None,
            ai_account_profile_section=None,
            ai_account_profile_action=None,
        )
        try:
            safe_page = await self._render_ai_account_profiles(
                callback.message,
                requested_page=requested_page,
            )
        except Exception:  # noqa: BLE001
            await self._show_ai_comments_stale(
                callback,
                state,
                notice="Профили аккаунтов временно недоступны. Ничего не изменено.",
            )
            return
        if safe_page != requested_page:
            await callback.answer("Список изменился, показана доступная страница.")
        else:
            await callback.answer()

    async def ai_account_profile(
        self,
        callback: CallbackQuery,
        state: FSMContext,
    ) -> None:
        if await self._deny_if_needed(callback):
            return
        try:
            profile_id, page = parse_ai_account_profile_callback(callback.data)
        except ValueError:
            await self._show_ai_comments_stale(callback, state)
            return
        if not await activate_ai_comments_state(
            callback,
            state,
            AICommentsUI.account_profile,
        ):
            return
        profile = await self.db.get_ai_account_profile(profile_id)
        if profile is None:
            await self._show_ai_account_profile_stale(callback, state, page=page)
            return
        await state.update_data(
            ai_account_profile_id=profile.id,
            ai_account_profile_version=profile.profile_version,
            ai_account_profile_page=page,
            ai_account_profile_section=None,
            ai_account_profile_action=None,
        )
        try:
            await self._render_ai_account_profile(
                callback.message,
                profile,
                page=page,
            )
        except Exception:  # noqa: BLE001
            await self._show_ai_account_profile_stale(
                callback,
                state,
                page=page,
                notice="Профиль не удалось прочитать. Ничего не изменено.",
            )
            return
        await callback.answer()

    async def ai_account_profile_edit(
        self,
        callback: CallbackQuery,
        state: FSMContext,
    ) -> None:
        if await self._deny_if_needed(callback):
            return
        try:
            section, profile_id, version, page = parse_ai_account_profile_edit_callback(
                callback.data
            )
        except ValueError:
            await self._show_ai_comments_stale(callback, state)
            return
        if not await activate_ai_comments_state(
            callback,
            state,
            AICommentsUI.account_profile_edit,
        ):
            return
        profile = await self.db.get_ai_account_profile(profile_id)
        if profile is None or profile.profile_version != version:
            await self._show_ai_account_profile_stale(callback, state, page=page)
            return
        prompts = {
            "b": (
                "✏️ <b>Основное</b>\n\nОтправьте ровно 5 строк:\n"
                "1. Название профиля\n2. Уровень: novice/basic/intermediate/advanced\n"
                "3. Роль: asks/answers/asks_entry/cautions/doubts\n"
                "4. Тон или -\n5. Словарный запас или -"
            ),
            "s": (
                "🗣 <b>Стиль</b>\n\nОтправьте ровно 5 строк:\n"
                "1. Заглавные: never/rare/sometimes\n"
                "2. Пунктуация: minimal/loose/clean\n"
                "3. Небольшие ошибки: none/light\n"
                "4. Любимые слова через запятую или -\n"
                "5. Манера фраз или -"
            ),
            "l": (
                "⏱ <b>Лимиты</b>\n\nОтправьте ровно 9 строк:\n"
                "1. Минимальная длина\n2. Максимальная длина\n"
                "3. Эмодзи, %\n4. Вопросы, %\n5. Ответы, %\n"
                "6. Возражения, %\n7. Комментариев в день\n8. Cooldown, минут\n"
                "9. Бонусных ответов в день (например 3)"
            ),
            "c": (
                "🛡 <b>Разрешения и запреты</b>\n\nОтправьте ровно 2 строки:\n"
                "1. Разрешённые утверждения через запятую или -\n"
                "2. Запрещённые утверждения через запятую или -"
            ),
        }
        await state.update_data(
            ai_account_profile_id=profile_id,
            ai_account_profile_version=version,
            ai_account_profile_page=page,
            ai_account_profile_section=section,
            ai_account_profile_action=None,
        )
        await self._safe_edit_text(
            callback.message,
            prompts[section]
            + f"\n\nТекущая версия: <b>{version}</b>. Старый экран после изменения не сработает.",
            reply_markup=ai_account_profile_input_keyboard(
                profile_id=profile_id,
                page=page,
            ),
        )
        await callback.answer()

    async def ai_account_profile_input(
        self,
        message: Message,
        state: FSMContext,
    ) -> None:
        if await self._deny_if_needed(message):
            return
        data = await state.get_data()
        profile_id = data.get("ai_account_profile_id")
        version = data.get("ai_account_profile_version")
        page = data.get("ai_account_profile_page", 0)
        section = data.get("ai_account_profile_section")
        if (
            not isinstance(profile_id, int)
            or isinstance(profile_id, bool)
            or not isinstance(version, int)
            or isinstance(version, bool)
            or not isinstance(page, int)
            or isinstance(page, bool)
            or not isinstance(section, str)
        ):
            await state.set_state(AICommentsUI.account_profiles)
            await message.answer("Форма устарела. Откройте профиль заново.")
            return
        try:
            patch = parse_ai_account_profile_section_input(section, message.text)
        except (TypeError, ValueError) as exc:
            await message.answer(
                f"⚠️ {html.escape(str(exc))}\n\nИсправьте данные и отправьте форму ещё раз.",
                reply_markup=ai_account_profile_input_keyboard(
                    profile_id=profile_id,
                    page=page,
                ),
            )
            return
        async with self._ai_account_profile_lock(profile_id):
            try:
                result = await self.db.update_ai_account_profile(
                    profile_id,
                    patch,
                    expected_version=version,
                    updated_by=message.from_user.id,
                )
            except Exception as exc:  # noqa: BLE001
                await state.set_state(AICommentsUI.account_profiles)
                await message.answer(
                    "⚠️ Профиль не сохранён: " + html.escape(str(exc))
                )
                return
        profile = result["profile"]
        await state.set_state(AICommentsUI.account_profile)
        await state.update_data(
            ai_account_profile_id=profile.id,
            ai_account_profile_version=profile.profile_version,
            ai_account_profile_section=None,
        )
        eligibility = await self.db.get_ai_account_profile_eligibility(profile.id, timezone_name=getattr(self.settings, "ai_comments_timezone", "Europe/Moscow"))
        await message.answer(
            ai_account_profile_text(
                profile,
                eligibility,
                notice=(
                    "Изменения сохранены."
                    if result["changed"]
                    else "Значение не изменилось."
                ),
            ),
            reply_markup=ai_account_profile_keyboard(
                profile_id=profile.id,
                profile_version=profile.profile_version,
                page=page,
                enabled=profile.enabled,
                retired=profile.retired,
            ),
        )

    async def ai_account_profile_toggle(
        self,
        callback: CallbackQuery,
        state: FSMContext,
    ) -> None:
        if await self._deny_if_needed(callback):
            return
        try:
            profile_id, version, enabled, page = parse_ai_account_profile_toggle_callback(
                callback.data
            )
        except ValueError:
            await self._show_ai_comments_stale(callback, state)
            return
        if not await activate_ai_comments_state(
            callback,
            state,
            AICommentsUI.account_profile,
        ):
            return
        async with self._ai_account_profile_lock(profile_id):
            try:
                result = await self.db.set_ai_account_profile_enabled(
                    profile_id,
                    enabled,
                    expected_version=version,
                    updated_by=callback.from_user.id,
                )
            except Exception:  # noqa: BLE001
                await self._show_ai_account_profile_stale(callback, state, page=page)
                return
        profile = result["profile"]
        await state.update_data(
            ai_account_profile_id=profile.id,
            ai_account_profile_version=profile.profile_version,
            ai_account_profile_action=None,
        )
        await self._render_ai_account_profile(
            callback.message,
            profile,
            page=page,
            notice="Профиль включён." if enabled else "Профиль выключен.",
        )
        await callback.answer("Состояние профиля сохранено")

    async def _profile_confirm(
        self,
        callback: CallbackQuery,
        state: FSMContext,
        *,
        action: str,
    ) -> None:
        parser_action = f"{action}_confirm"
        try:
            profile_id, version, page = parse_ai_account_profile_action_callback(
                callback.data,
                action=parser_action,
            )
        except ValueError:
            await self._show_ai_comments_stale(callback, state)
            return
        if not await activate_ai_comments_state(
            callback,
            state,
            AICommentsUI.account_profile_confirm,
        ):
            return
        profile = await self.db.get_ai_account_profile(profile_id)
        if profile is None or profile.profile_version != version:
            await self._show_ai_account_profile_stale(callback, state, page=page)
            return
        await state.update_data(
            ai_account_profile_id=profile_id,
            ai_account_profile_version=version,
            ai_account_profile_page=page,
            ai_account_profile_action=action,
        )
        if action == "regenerate":
            text = (
                "🔄 <b>Создать новый характер?</b>\n\n"
                "Текущий профиль останется в истории, а аккаунт получит новый "
                "уникальный preset и стиль. Действие не публикует комментарии."
            )
        else:
            text = (
                "🗄 <b>Архивировать профиль?</b>\n\n"
                "Профиль выключится, но Telegram ID, история и связь для будущего "
                "восстановления сохранятся. Физического удаления аудита не будет."
            )
        await self._safe_edit_text(
            callback.message,
            text,
            reply_markup=ai_account_profile_confirm_keyboard(
                action=action,
                profile_id=profile_id,
                profile_version=version,
                page=page,
            ),
        )
        await callback.answer()

    async def ai_account_profile_regenerate_confirm(
        self,
        callback: CallbackQuery,
        state: FSMContext,
    ) -> None:
        if await self._deny_if_needed(callback):
            return
        await self._profile_confirm(callback, state, action="regenerate")

    async def ai_account_profile_retire_confirm(
        self,
        callback: CallbackQuery,
        state: FSMContext,
    ) -> None:
        if await self._deny_if_needed(callback):
            return
        await self._profile_confirm(callback, state, action="retire")

    async def _profile_apply(
        self,
        callback: CallbackQuery,
        state: FSMContext,
        *,
        action: str,
    ) -> None:
        try:
            profile_id, version, page = parse_ai_account_profile_action_callback(
                callback.data,
                action=f"{action}_apply",
            )
        except ValueError:
            await self._show_ai_comments_stale(callback, state)
            return
        current_state = await state.get_state()
        data = await state.get_data()
        if (
            current_state != AICommentsUI.account_profile_confirm.state
            or data.get("ai_account_profile_action") != action
            or data.get("ai_account_profile_id") != profile_id
            or data.get("ai_account_profile_version") != version
            or data.get("ai_account_profile_page") != page
        ):
            await self._show_ai_account_profile_stale(
                callback,
                state,
                page=page,
                notice="Старое подтверждение не применено.",
            )
            return
        async with self._ai_account_profile_lock(profile_id):
            try:
                if action == "regenerate":
                    result = await self.db.regenerate_ai_account_profile(
                        profile_id,
                        expected_version=version,
                        updated_by=callback.from_user.id,
                    )
                else:
                    result = await self.db.retire_ai_account_profile(
                        profile_id,
                        expected_version=version,
                        updated_by=callback.from_user.id,
                    )
            except Exception:  # noqa: BLE001
                await self._show_ai_account_profile_stale(callback, state, page=page)
                return
        profile = result["profile"]
        await state.set_state(AICommentsUI.account_profile)
        await state.update_data(
            ai_account_profile_id=profile.id,
            ai_account_profile_version=profile.profile_version,
            ai_account_profile_action=None,
        )
        await self._render_ai_account_profile(
            callback.message,
            profile,
            page=page,
            notice=(
                "Новый характер создан."
                if action == "regenerate"
                else "Профиль архивирован и выключен."
            ),
        )
        await callback.answer("Готово")

    async def ai_account_profile_regenerate_apply(
        self,
        callback: CallbackQuery,
        state: FSMContext,
    ) -> None:
        if await self._deny_if_needed(callback):
            return
        await self._profile_apply(callback, state, action="regenerate")

    async def ai_account_profile_retire_apply(
        self,
        callback: CallbackQuery,
        state: FSMContext,
    ) -> None:
        if await self._deny_if_needed(callback):
            return
        await self._profile_apply(callback, state, action="retire")

    async def ai_account_profile_history(
        self,
        callback: CallbackQuery,
        state: FSMContext,
    ) -> None:
        if await self._deny_if_needed(callback):
            return
        try:
            profile_id, page = parse_ai_account_profile_history_callback(callback.data)
        except ValueError:
            await self._show_ai_comments_stale(callback, state)
            return
        if not await activate_ai_comments_state(
            callback,
            state,
            AICommentsUI.account_profile_history,
        ):
            return
        profile = await self.db.get_ai_account_profile(profile_id)
        if profile is None:
            await self._show_ai_account_profile_stale(callback, state, page=page)
            return
        revisions = await self.db.list_ai_account_profile_revisions(profile_id, limit=10)
        replies = await self.db.list_ai_account_reply_history(profile_id, limit=5)
        revision_lines = [
            f"- v{item.profile_version} · {_escaped(item.change_reason, limit=60)} · "
            f"<code>{item.snapshot_hash[:10]}</code>"
            for item in revisions
        ] or ["- <i>нет ревизий</i>"]
        reply_lines = [
            f"- {_escaped(item.text or '[без текста]', limit=160)}"
            for item in replies
        ] or ["- <i>опубликованных реплик ещё нет</i>"]
        await state.update_data(
            ai_account_profile_id=profile_id,
            ai_account_profile_version=profile.profile_version,
            ai_account_profile_page=page,
            ai_account_profile_action=None,
        )
        await self._safe_edit_text(
            callback.message,
            "📜 <b>История профиля</b>\n\n"
            f"Профиль: <b>{_escaped(profile.name)}</b>\n"
            f"Telegram ID: <code>{profile.telegram_user_id or 'нет'}</code>\n\n"
            "<b>Последние изменения</b>\n"
            + "\n".join(revision_lines)
            + "\n\n<b>Последние опубликованные реплики</b>\n"
            + "\n".join(reply_lines)
            + "\n\nНа шаге 9 реплики не создаются и не публикуются; экран уже читает "
            "фактическую историю для последующих этапов.",
            reply_markup=ai_account_profile_history_keyboard(
                profile_id=profile_id,
                page=page,
            ),
        )
        await callback.answer()
