from __future__ import annotations

import asyncio
import contextlib
import html
import io
import json
import logging
import re
from datetime import UTC, datetime

from aiogram import F, Router
from aiogram.exceptions import TelegramBadRequest
from aiogram.filters import Command, CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.types import BufferedInputFile, CallbackQuery, Message
from telethon import errors, functions

from . import __version__
from .analytics import (
    ANALYTICS_PERIODS,
    build_analytics_csv,
    parse_analytics_period_callback,
    render_analytics_overview,
    render_analytics_ranking,
)
from .backup import (
    MAX_BACKUP_BYTES,
    BackupIntegrityError,
    BackupValidationError,
    create_backup_bytes,
    payload_sha256,
    verify_backup_bytes,
)
from .channel_profiles import (
    CHANNEL_PROFILES,
    channel_profile_label,
    get_channel_profile,
    resolve_channel_profile,
)
from .config import Settings
from .db import AccountIdentityConflictError, Database
from .health import evaluate_account_health
from .keyboards import (
    account_actions,
    account_bulk_actions_keyboard,
    account_bulk_confirm_keyboard,
    account_email_confirm_keyboard,
    account_email_management_keyboard,
    account_health_detail_keyboard,
    account_health_list_keyboard,
    account_list_keyboard,
    account_login_code_prompt_keyboard,
    account_login_code_result_keyboard,
    account_management_keyboard,
    analytics_ranking_keyboard,
    autolike_actions,
    back_main,
    backup_event_keyboard,
    backup_history_keyboard,
    backup_menu_keyboard,
    backup_restore_cancel_keyboard,
    backup_restore_confirm_keyboard,
    backup_rollback_confirm_keyboard,
    channel_actions,
    channel_back_keyboard,
    channel_connect_all_confirm_keyboard,
    channel_connect_manual_keyboard,
    channel_connect_overview_keyboard,
    channel_connect_selected_confirm_keyboard,
    channel_copy_confirm_keyboard,
    channel_copy_targets_keyboard,
    channel_list_keyboard,
    channel_post_type_cancel_keyboard,
    channel_post_types_keyboard,
    channel_profile_confirm_keyboard,
    channel_profile_keyboard,
    channel_reaction_window_after_save_keyboard,
    channel_reaction_window_keyboard,
    channel_reactions_keyboard,
    channel_view_batch_keyboard,
    channel_views_confirm_keyboard,
    channel_views_manual_cancel_keyboard,
    channel_views_setup_keyboard,
    confirm_account_delete,
    confirm_account_email_delete,
    confirm_channel_delete,
    confirm_group_leave,
    confirm_problem_accounts_clear,
    delay_overview_keyboard,
    depth_menu,
    group_actions,
    group_list_keyboard,
    group_reactions_keyboard,
    login_code_actions,
    main_menu,
    membership_delay_keyboard,
    membership_delay_reschedule_keyboard,
    missing_email_account_list_keyboard,
    problem_account_actions,
    problem_account_list_keyboard,
    promotion_period_confirm_keyboard,
    promotion_period_keyboard,
    reaction_delay_keyboard,
    reaction_delay_reschedule_keyboard,
    reaction_limit_confirm_keyboard,
    reaction_limit_keyboard,
    reactions_overview_keyboard,
    settings_overview_keyboard,
    statistics_keyboard,
    system_health_keyboard,
    target_management_keyboard,
)
from .management import (
    ACCOUNT_FILTERS,
    TARGET_FILTERS,
    account_matches,
    normalize_management_query,
    parse_account_management_callback,
    parse_bulk_account_action,
    parse_copy_channel_callback,
    parse_target_management_callback,
    target_matches,
)
from .models import promotion_is_active, utcnow
from .services.jobs import JobService
from .services.telegram_accounts import (
    ACCOUNT_AUTH_FAILURES,
    ClientPool,
    LoginManager,
    SessionCipher,
)
from .states import (
    AccountAuth,
    AccountEmailEdit,
    AddChannel,
    ConfigurationRestore,
    ManagementSearch,
    SetChannelReactions,
    SetChannelReactionWindow,
    SetDelay,
    SetGroupReactions,
    SetManualViewAmount,
    SetMembershipDelay,
    SetPostReactionLimit,
    SetPostTypePercentage,
    SetPromotionPeriod,
    SetReactions,
)
from .utils import (
    display_account_name,
    format_reaction_weights,
    normalize_email_login,
    parse_channel_link,
    parse_weighted_reactions,
    truncate,
)

logger = logging.getLogger("laika_bot.handlers")

ACCOUNT_PAGE_SIZE = 12


async def _delete_sensitive_message(message: Message) -> None:
    """Best-effort removal of one-time codes and 2FA passwords from bot chat."""

    try:
        await message.delete()
    except Exception:  # noqa: BLE001
        # Never log message text or the secret value. Deletion may legitimately
        # fail because of Telegram permissions, age, or a stale update.
        logger.warning(
            "Sensitive administrator message could not be deleted message_id=%s",
            getattr(message, "message_id", None),
        )


async def _revoke_pending_login(login_manager: LoginManager, admin_id: int) -> bool:
    """Revoke an unpersisted login, with compatibility for older test doubles."""

    revoke = getattr(login_manager, "revoke_and_cancel", None)
    if revoke is not None:
        return bool(await revoke(admin_id))
    await login_manager.cancel(admin_id)
    return False


def _page_from_callback(data: str | None, prefix: str) -> int:
    if not data or not data.startswith(prefix):
        return 0
    suffix = data[len(prefix) :]
    if not suffix:
        return 0
    try:
        return max(0, int(suffix))
    except ValueError:
        return 0


def _paginate(items: list, page: int, *, page_size: int = ACCOUNT_PAGE_SIZE):
    total = len(items)
    total_pages = max(1, (total + page_size - 1) // page_size)
    safe_page = min(max(0, page), total_pages - 1)
    start = safe_page * page_size
    return items[start : start + page_size], safe_page, total_pages


def format_duration_range(minimum_seconds: int, maximum_seconds: int) -> str:
    def one(value: int) -> str:
        if value % 3600 == 0 and value >= 3600:
            hours = value // 3600
            return f"{hours} ч"
        if value % 60 == 0:
            return f"{value // 60} мин"
        return f"{value} сек"

    if minimum_seconds == maximum_seconds:
        return one(minimum_seconds)
    return f"{one(minimum_seconds)}–{one(maximum_seconds)}"


def parse_channel_reaction_window(text: str) -> tuple[int, int]:
    cleaned = (text or "").strip().lower().replace("—", "-").replace("–", "-")
    match = re.fullmatch(r"\s*(\d+)\s*-\s*(\d+)\s*(мин|минут|m|ч|час|часа|часов|h)?\s*", cleaned)
    if not match:
        raise ValueError("Введите период, например 20-90 или 1-3 ч")
    minimum, maximum = int(match.group(1)), int(match.group(2))
    unit = match.group(3) or "мин"
    multiplier = 3600 if unit in {"ч", "час", "часа", "часов", "h"} else 60
    minimum_seconds, maximum_seconds = minimum * multiplier, maximum * multiplier
    if minimum_seconds < 0 or maximum_seconds < minimum_seconds:
        raise ValueError("Минимум не может быть больше максимума")
    if maximum_seconds > 7 * 24 * 60 * 60:
        raise ValueError("Максимальный период — 7 дней")
    return minimum_seconds, maximum_seconds


def is_menu_text(message: Message) -> bool:
    return bool(message.text and message.text.strip().casefold() in {"меню", "menu"})


def parse_autolike_depth_callback(data: str) -> tuple[int, int]:
    parts = data.split(":")
    if len(parts) != 4 or parts[0:2] != ["autolike", "set_depth"]:
        raise ValueError("Некорректная кнопка глубины старых постов")
    return int(parts[2]), int(parts[3])


def parse_channel_profile_callback(data: str | None, action: str) -> tuple[int, str]:
    if action not in {"select", "apply"}:
        raise ValueError("Некорректное действие профиля")
    parts = (data or "").split(":")
    if len(parts) != 4 or parts[0:2] != ["channel", f"profile_{action}"]:
        raise ValueError("Некорректная кнопка профиля")
    try:
        channel_id = int(parts[2])
    except (TypeError, ValueError) as exc:
        raise ValueError("Некорректный ID канала") from exc
    if channel_id < 1:
        raise ValueError("Некорректный ID канала")
    profile_key = parts[3]
    get_channel_profile(profile_key)
    return channel_id, profile_key


def parse_configuration_event_callback(data: str | None, action: str) -> int:
    if action not in {"event", "rollback_confirm", "rollback_apply"}:
        raise ValueError("Некорректное действие истории")
    prefix = f"backup:{action}:"
    if not data or not data.startswith(prefix):
        raise ValueError("Некорректная кнопка истории")
    suffix = data[len(prefix) :]
    if not suffix or ":" in suffix:
        raise ValueError("Некорректная кнопка истории")
    try:
        event_id = int(suffix)
    except ValueError as exc:
        raise ValueError("Некорректный ID события") from exc
    if event_id < 1:
        raise ValueError("Некорректный ID события")
    return event_id


def parse_reaction_limit_input(text: str, maximum: int) -> int:
    try:
        value = int(text.strip())
    except (TypeError, ValueError) as exc:
        raise ValueError(f"Введите целое число от 1 до {maximum}") from exc
    if not 1 <= value <= maximum:
        raise ValueError(f"Введите число от 1 до {maximum}")
    return value


def parse_post_type_percentage(text: str) -> int:
    try:
        value = int(text.strip().rstrip("%"))
    except (TypeError, ValueError) as exc:
        raise ValueError("Введите целое число от 0 до 100") from exc
    if not 0 <= value <= 100:
        raise ValueError("Введите число от 0 до 100")
    return value


def parse_manual_view_amount(text: str, joined_count: int) -> tuple[str, int, int]:
    if joined_count <= 0:
        raise ValueError("Нет подписанных аккаунтов")
    normalized = text.strip()
    if normalized.endswith("%"):
        try:
            percent = int(normalized[:-1].strip())
        except (TypeError, ValueError) as exc:
            raise ValueError("Введите процент от 1% до 100% или число аккаунтов") from exc
        if not 1 <= percent <= 100:
            raise ValueError("Введите процент от 1% до 100%")
        count = max(1, joined_count * percent // 100)
        return "percent", percent, min(count, joined_count)
    try:
        count = int(normalized)
    except (TypeError, ValueError) as exc:
        raise ValueError(
            f"Введите число аккаунтов от 1 до {joined_count} или процент"
        ) from exc
    if not 1 <= count <= joined_count:
        raise ValueError(f"Введите число аккаунтов от 1 до {joined_count}")
    return "count", count, count


def parse_promotion_period_input(text: str) -> tuple[str, int | None]:
    normalized = text.strip().casefold()
    if normalized == "постоянный":
        return "permanent", None
    try:
        days = int(normalized)
    except (TypeError, ValueError) as exc:
        raise ValueError("Введите количество дней от 1 до 365 или слово «постоянный»") from exc
    if not 1 <= days <= 365:
        raise ValueError("Введите количество дней от 1 до 365")
    return "timed", days


def format_utc_datetime(value) -> str:
    return value.strftime("%d.%m.%Y, %H:%M") if value else "не задано"


def format_age_seconds(value: int | None) -> str:
    if value is None:
        return "ещё не запускался"
    if value < 10:
        return "только что"
    if value < 60:
        return f"{value} сек. назад"
    minutes = value // 60
    if minutes < 60:
        return f"{minutes} мин. назад"
    hours = minutes // 60
    return f"{hours} ч. назад"


def format_remaining_seconds(value: int) -> str:
    value = max(0, int(value))
    if value < 60:
        return f"{value} сек."
    minutes, seconds = divmod(value, 60)
    if minutes < 60:
        return f"{minutes} мин. {seconds} сек."
    hours, minutes = divmod(minutes, 60)
    return f"{hours} ч. {minutes} мин."

# Export private helpers too: split handler modules intentionally share one compatibility surface.
__all__ = [name for name in globals() if not name.startswith('__')]
