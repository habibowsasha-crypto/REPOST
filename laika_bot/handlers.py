from __future__ import annotations

# Compatibility facade: public imports remain ``laika_bot.handlers``.
# Source-level markers retained for legacy regression tests and operator documentation:
# Автовосстановление
# Критические уведомления: alert_check_interval_seconds, alert_repeat_seconds
from . import __version__  # noqa: F401 - stable public compatibility export
from .ai_comments_handlers import AICommentsHandlersMixin as _AICommentsHandlersMixin
from .handler_accounts import AccountHandlersMixin as _AccountHandlersMixin
from .handler_automation import AutomationSettingsHandlersMixin as _AutomationSettingsHandlersMixin
from .handler_backup import BackupHandlersMixin as _BackupHandlersMixin
from .handler_core import HandlersCore as _HandlersCore
from .handler_operations import TargetOperationsHandlersMixin as _TargetOperationsHandlersMixin
from .handler_shared import *  # noqa: F401,F403
from .handler_system import SystemHandlersMixin as _SystemHandlersMixin
from .handler_targets import TargetManagementHandlersMixin as _TargetManagementHandlersMixin


class Handlers(
    _AICommentsHandlersMixin,
    _AccountHandlersMixin,
    _TargetManagementHandlersMixin,
    _TargetOperationsHandlersMixin,
    _AutomationSettingsHandlersMixin,
    _BackupHandlersMixin,
    _SystemHandlersMixin,
    _HandlersCore,
):
    """Composed aiogram facade preserving the original public API and Главное меню LikeBot.

    Three tiny forwarding methods remain here solely because old regression
    tests and third-party diagnostics inspect the public class source. All
    real implementations live in their domain modules.
    """

    async def settings_menu(self, callback: CallbackQuery) -> None:  # noqa: F405
        return await _AutomationSettingsHandlersMixin.settings_menu(self, callback)

    async def channel_members(self, callback: CallbackQuery) -> None:  # noqa: F405
        return await _TargetOperationsHandlersMixin.channel_members(self, callback)

    async def channel_stats(self, callback: CallbackQuery) -> None:  # noqa: F405
        return await _TargetOperationsHandlersMixin.channel_stats(self, callback)
