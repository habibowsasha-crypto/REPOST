from __future__ import annotations

from .handler_shared import *  # noqa: F403
from .openai_gateway import OpenAIGateway


class HandlersCore:
    def __init__(
        self,
        settings: Settings,
        db: Database,
        cipher: SessionCipher,
        login_manager: LoginManager,
        pool: ClientPool,
        jobs: JobService,
        recovery: object | None = None,
        openai_gateway: OpenAIGateway | None = None,
    ) -> None:
        self.settings = settings
        self.db = db
        self.cipher = cipher
        self.login_manager = login_manager
        self.pool = pool
        self.jobs = jobs
        self.recovery = recovery
        self.openai_gateway = openai_gateway or OpenAIGateway(settings)
        self._owns_openai_gateway = openai_gateway is None
        self.router = Router(name="admin")
        # In-memory set of Telegram service message ids already shown to the admin.
        # Codes are never written to the database or to application logs.
        self._shown_login_message_ids: dict[int, set[int]] = {}
        self._login_code_hide_tasks: dict[tuple[int, int], asyncio.Task] = {}
        self._channel_profile_locks: dict[int, asyncio.Lock] = {}
        self._configuration_restore_lock = asyncio.Lock()
        self._account_management_queries: dict[int, str] = {}
        self._target_management_queries: dict[tuple[int, str], str] = {}
        self._account_bulk_lock = asyncio.Lock()
        self._channel_copy_lock = asyncio.Lock()
        self._ai_comments_settings_lock = asyncio.Lock()
        self._ai_comments_memory_locks: dict[int, asyncio.Lock] = {}
        self._openai_gateway_test_lock = asyncio.Lock()
        self._ai_comment_generation_lock = asyncio.Lock()
        self._ai_dialogue_generation_lock = asyncio.Lock()
        self._register()

    def _is_admin(self, user_id: int | None) -> bool:
        return user_id == self.settings.admin_id

    async def _deny_if_needed(self, event: Message | CallbackQuery) -> bool:
        user_id = event.from_user.id if event.from_user else None
        if self._is_admin(user_id):
            return False
        if isinstance(event, CallbackQuery):
            await event.answer("Нет доступа", show_alert=True)
        else:
            await event.answer("⛔ Доступ разрешён только администратору.")
        return True

    async def _safe_edit_text(self, message: Message, text: str, *, reply_markup) -> None:
        try:
            await message.edit_text(text, reply_markup=reply_markup)
        except TelegramBadRequest as exc:
            if "message is not modified" not in str(exc).casefold():
                raise

    async def noop(self, callback: CallbackQuery) -> None:
        if await self._deny_if_needed(callback):
            return
        await callback.answer()

    def _channel_profile_lock(self, channel_id: int) -> asyncio.Lock:
        lock = self._channel_profile_locks.get(channel_id)
        if lock is None:
            lock = asyncio.Lock()
            self._channel_profile_locks[channel_id] = lock
        return lock

    async def close(self) -> None:
        """Cancel transient UI tasks before the bot HTTP session is closed."""

        tasks = list(self._login_code_hide_tasks.values())
        self._login_code_hide_tasks.clear()
        for task in tasks:
            if not task.done():
                task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        if getattr(self, "_owns_openai_gateway", False):
            gateway = getattr(self, "openai_gateway", None)
            if gateway is not None:
                await gateway.close()

    def _register(self) -> None:
        r = self.router
        r.message.register(self.start, CommandStart())
        r.message.register(self.menu_command, Command("menu"))
        r.message.register(self.menu_command, is_menu_text)
        r.message.register(self.cancel, Command("cancel"))
        r.callback_query.register(self.main_callback, F.data == "main")
        r.callback_query.register(self.noop, F.data == "noop")
        self._register_ai_comments_handlers(r)

        r.callback_query.register(self.account_add, F.data == "account:add")
        r.callback_query.register(
            self.account_email_confirm,
            F.data == "account:email_confirm",
        )
        r.callback_query.register(self.account_email_change, F.data == "account:email_change")
        r.callback_query.register(self.account_resend_code, F.data == "account:resend_code")
        r.callback_query.register(self.account_restart_phone, F.data == "account:restart_phone")
        r.callback_query.register(self.account_list, F.data == "account:list")
        r.callback_query.register(
            self.account_list, F.data.startswith("account:list_page:")
        )
        r.callback_query.register(self.account_refresh, F.data == "account:refresh")
        r.callback_query.register(
            self.account_refresh, F.data.startswith("account:refresh_page:")
        )
        r.callback_query.register(
            self.account_missing_email_list,
            F.data == "account:email_missing",
        )
        r.callback_query.register(
            self.account_missing_email_list,
            F.data.startswith("account:email_missing_page:"),
        )
        r.callback_query.register(
            self.account_email_delete_confirm,
            F.data.startswith("account:email_delete_confirm:"),
        )
        r.callback_query.register(
            self.account_email_delete,
            F.data.startswith("account:email_delete:"),
        )
        r.callback_query.register(
            self.account_email_edit,
            F.data.startswith("account:email_edit:"),
        )
        r.callback_query.register(self.account_email_note, F.data.startswith("account:email_note:"))
        r.callback_query.register(self.account_email_view, F.data.startswith("account:email:"))
        r.callback_query.register(self.problem_account_list, F.data == "account:problems")
        r.callback_query.register(
            self.problem_account_list, F.data.startswith("account:problems_page:")
        )
        r.callback_query.register(self.problem_account_check_all, F.data == "account:problems_check")
        r.callback_query.register(
            self.problem_accounts_clear_confirm,
            F.data == "account:problems_clear_confirm",
        )
        r.callback_query.register(
            self.problem_accounts_clear,
            F.data == "account:problems_clear",
        )
        r.callback_query.register(
            self.problem_account_view, F.data.startswith("account:problem_view:")
        )
        r.callback_query.register(
            self.problem_account_check, F.data.startswith("account:problem_check:")
        )
        r.callback_query.register(self.account_reauth, F.data.startswith("account:reauth:"))
        r.callback_query.register(self.account_view, F.data.startswith("account:view:"))
        r.callback_query.register(self.account_toggle, F.data.startswith("account:toggle:"))
        r.callback_query.register(self.account_delete_confirm, F.data.startswith("account:delete_confirm:"))
        r.callback_query.register(self.account_delete, F.data.startswith("account:delete:"))
        # login_code_check must be registered before login_code: (prefix overlap)
        r.callback_query.register(self.account_login_code_check, F.data.startswith("account:login_code_check:"))
        r.callback_query.register(self.account_login_code_prompt, F.data.startswith("account:login_code:"))
        r.callback_query.register(self.account_session_check, F.data.startswith("account:session_check:"))
        r.callback_query.register(
            self.account_management_view, F.data.startswith("manage:a:")
        )
        r.callback_query.register(
            self.account_management_search, F.data.startswith("manage:as:")
        )
        r.callback_query.register(
            self.account_management_clear_search, F.data.startswith("manage:ac:")
        )
        r.callback_query.register(self.account_bulk_menu, F.data == "manage:ab")
        r.callback_query.register(
            self.account_bulk_prepare, F.data.startswith("manage:ab:")
        )
        r.callback_query.register(
            self.account_bulk_apply, F.data.startswith("manage:abc:")
        )
        r.message.register(self.account_phone, AccountAuth.phone)
        r.message.register(self.account_email, AccountAuth.email)
        r.message.register(self.account_code, AccountAuth.code)
        r.message.register(self.account_password, AccountAuth.password)
        r.message.register(self.account_email_address_input, AccountEmailEdit.address)
        r.message.register(self.account_email_note_input, AccountEmailEdit.note)
        r.message.register(self.account_management_search_input, ManagementSearch.accounts)

        r.callback_query.register(self.channel_add, F.data == "channel:add")
        r.callback_query.register(self.channel_list, F.data == "channel:list")
        r.callback_query.register(self.channel_view, F.data.startswith("channel:view:"))
        r.callback_query.register(
            self.target_management_view, F.data.startswith("manage:t:")
        )
        r.callback_query.register(
            self.target_management_search, F.data.startswith("manage:ts:")
        )
        r.callback_query.register(
            self.target_management_clear_search, F.data.startswith("manage:tc:")
        )
        r.callback_query.register(
            self.channel_copy_list, F.data.startswith("channel:copy:")
        )
        r.callback_query.register(
            self.channel_copy_confirm, F.data.startswith("channel:copyc:")
        )
        r.callback_query.register(
            self.channel_copy_apply, F.data.startswith("channel:copya:")
        )
        r.callback_query.register(
            self.channel_profile_select, F.data.startswith("channel:profile_select:")
        )
        r.callback_query.register(
            self.channel_profile_apply, F.data.startswith("channel:profile_apply:")
        )
        r.callback_query.register(
            self.channel_profile_view, F.data.startswith("channel:profile:")
        )
        r.callback_query.register(
            self.channel_reaction_window_view, F.data.startswith("channel:reaction_window:")
        )
        r.callback_query.register(
            self.channel_reaction_window_value, F.data.startswith("channel:reaction_window_value:")
        )
        r.callback_query.register(
            self.channel_reaction_window_edit, F.data.startswith("channel:reaction_window_edit:")
        )
        r.callback_query.register(
            self.channel_reaction_window_reschedule,
            F.data.startswith("channel:reaction_window_reschedule:"),
        )
        r.callback_query.register(self.channel_reactions_view, F.data.startswith("channel:reactions:"))
        r.callback_query.register(self.channel_reactions_edit, F.data.startswith("channel:reactions_edit:"))
        r.callback_query.register(self.channel_reactions_reset, F.data.startswith("channel:reactions_reset:"))
        r.callback_query.register(self.channel_post_types_view, F.data.startswith("channel:post_types:"))
        r.callback_query.register(
            self.channel_post_types_reset, F.data.startswith("channel:post_types_reset:")
        )
        r.callback_query.register(
            self.channel_post_type_edit, F.data.startswith("channel:post_type_edit:")
        )
        r.callback_query.register(self.channel_views_open, F.data.startswith("channel:views:"))
        r.callback_query.register(
            self.channel_views_posts, F.data.startswith("channel:views_posts:")
        )
        r.callback_query.register(
            self.channel_views_accounts, F.data.startswith("channel:views_accounts:")
        )
        r.callback_query.register(
            self.channel_views_manual, F.data.startswith("channel:views_manual:")
        )
        r.callback_query.register(
            self.channel_views_run, F.data.startswith("channel:views_run:")
        )
        r.callback_query.register(
            self.channel_views_batch, F.data.startswith("channel:views_batch:")
        )
        r.callback_query.register(
            self.channel_views_cancel, F.data.startswith("channel:views_cancel:")
        )
        r.callback_query.register(self.channel_members, F.data.startswith("channel:members:"))
        r.callback_query.register(self.channel_stats, F.data.startswith("channel:stats:"))
        r.callback_query.register(self.channel_connect_view, F.data.startswith("channel:connect:"))
        r.callback_query.register(self.channel_connect_refresh, F.data.startswith("channel:connect_refresh:"))
        r.callback_query.register(self.channel_connect_all, F.data.startswith("channel:connect_all:"))
        r.callback_query.register(self.channel_connect_run_all, F.data.startswith("channel:connect_run_all:"))
        r.callback_query.register(self.channel_connect_manual, F.data.startswith("channel:connect_manual:"))
        r.callback_query.register(self.channel_connect_toggle, F.data.startswith("channel:connect_toggle:"))
        r.callback_query.register(self.channel_connect_select_all, F.data.startswith("channel:connect_select_all:"))
        r.callback_query.register(self.channel_connect_clear, F.data.startswith("channel:connect_clear:"))
        r.callback_query.register(self.channel_connect_selected, F.data.startswith("channel:connect_selected:"))
        r.callback_query.register(self.channel_connect_run_selected, F.data.startswith("channel:connect_run_selected:"))
        r.callback_query.register(self.channel_delete_confirm, F.data.startswith("channel:delete_confirm:"))
        r.callback_query.register(self.channel_delete, F.data.startswith("channel:delete:"))

        r.callback_query.register(self.group_add, F.data == "group:add")
        r.callback_query.register(self.group_list, F.data == "group:list")
        r.callback_query.register(self.group_view, F.data.startswith("group:view:"))
        r.callback_query.register(self.group_reactions_view, F.data.startswith("group:reactions:"))
        r.callback_query.register(self.group_reactions_edit, F.data.startswith("group:reactions_edit:"))
        r.callback_query.register(self.group_reactions_reset, F.data.startswith("group:reactions_reset:"))
        r.callback_query.register(self.group_leave_confirm, F.data.startswith("group:leave_confirm:"))
        r.callback_query.register(self.group_leave, F.data.startswith("group:leave:"))
        r.message.register(self.channel_link, AddChannel.link)
        r.message.register(self.target_management_search_input, ManagementSearch.targets)
        r.message.register(self.channel_reactions_input, SetChannelReactions.reactions)
        r.message.register(self.channel_reaction_window_input, SetChannelReactionWindow.value)
        r.message.register(self.channel_post_type_input, SetPostTypePercentage.value)
        r.message.register(self.channel_views_manual_input, SetManualViewAmount.value)
        r.message.register(self.group_reactions_input, SetGroupReactions.reactions)

        r.callback_query.register(self.autolike_list, F.data == "autolike:list")
        r.callback_query.register(self.autolike_view, F.data.startswith("autolike:view:"))
        r.callback_query.register(self.autolike_toggle_new, F.data.startswith("autolike:toggle_new:"))
        r.callback_query.register(self.autolike_toggle_old, F.data.startswith("autolike:toggle_old:"))
        r.callback_query.register(self.autolike_depth_menu, F.data.startswith("autolike:depth_menu:"))
        r.callback_query.register(self.autolike_set_depth, F.data.startswith("autolike:set_depth:"))

        r.callback_query.register(self.autolike_limit_view, F.data.startswith("autolike:limit:"))
        r.callback_query.register(self.autolike_limit_value, F.data.startswith("autolike:limit_value:"))
        r.callback_query.register(self.autolike_limit_manual, F.data.startswith("autolike:limit_manual:"))
        r.callback_query.register(self.autolike_limit_apply, F.data.startswith("autolike:limit_apply:"))
        r.callback_query.register(self.autolike_limit_cancel, F.data.startswith("autolike:limit_cancel:"))
        r.message.register(self.autolike_limit_input, SetPostReactionLimit.value)

        r.callback_query.register(self.autolike_period_view, F.data.startswith("autolike:period:"))
        r.callback_query.register(self.autolike_period_edit, F.data.startswith("autolike:period_edit:"))
        r.callback_query.register(self.autolike_period_value, F.data.startswith("autolike:period_value:"))
        r.callback_query.register(self.autolike_period_apply, F.data.startswith("autolike:period_apply:"))
        r.callback_query.register(self.autolike_period_cancel, F.data.startswith("autolike:period_cancel:"))
        r.message.register(self.autolike_period_input, SetPromotionPeriod.value)

        r.callback_query.register(self.backup_menu, F.data == "backup:menu")
        r.callback_query.register(self.backup_export, F.data == "backup:export")
        r.callback_query.register(self.backup_restore_start, F.data == "backup:restore")
        r.callback_query.register(self.backup_restore_apply, F.data == "backup:restore_apply")
        r.callback_query.register(self.backup_cancel, F.data == "backup:cancel")
        r.callback_query.register(self.backup_history, F.data == "backup:history")
        r.callback_query.register(
            self.backup_event_view, F.data.startswith("backup:event:")
        )
        r.callback_query.register(
            self.backup_rollback_confirm,
            F.data.startswith("backup:rollback_confirm:"),
        )
        r.callback_query.register(
            self.backup_rollback_apply,
            F.data.startswith("backup:rollback_apply:"),
        )
        r.message.register(
            self.backup_restore_file, ConfigurationRestore.backup_file, F.document
        )
        r.message.register(
            self.backup_restore_invalid_file, ConfigurationRestore.backup_file
        )

        r.callback_query.register(self.settings_menu, F.data == "settings:menu")
        r.callback_query.register(self.reactions_menu, F.data == "settings:reactions")
        r.callback_query.register(self.reactions_edit, F.data == "settings:reactions:edit")
        r.message.register(self.reactions_input, SetReactions.reactions)
        r.callback_query.register(self.delay_menu, F.data == "settings:delay")
        r.callback_query.register(self.reaction_delay_view, F.data == "settings:delay:reactions")
        r.callback_query.register(self.delay_edit, F.data == "settings:delay:reactions:edit")
        # Compatibility with an old v1.0.5 button still visible in Telegram history.
        r.callback_query.register(self.delay_edit, F.data == "settings:delay:edit")
        r.message.register(self.delay_minimum, SetDelay.minimum)
        r.message.register(self.delay_maximum, SetDelay.maximum)
        r.callback_query.register(
            self.reaction_delay_reschedule,
            F.data == "settings:delay:reactions:reschedule",
        )
        r.callback_query.register(
            self.reaction_delay_keep_existing,
            F.data == "settings:delay:reactions:keep",
        )
        r.callback_query.register(self.membership_delay_view, F.data == "settings:delay:membership")
        r.callback_query.register(
            self.membership_delay_edit,
            F.data == "settings:delay:membership:edit",
        )
        r.message.register(self.membership_delay_minimum, SetMembershipDelay.minimum)
        r.message.register(self.membership_delay_maximum, SetMembershipDelay.maximum)
        r.callback_query.register(
            self.membership_delay_reschedule,
            F.data == "settings:delay:membership:reschedule",
        )
        r.callback_query.register(
            self.membership_delay_keep_existing,
            F.data == "settings:delay:membership:keep",
        )
        r.callback_query.register(self.system_refresh, F.data == "system:refresh")
        r.callback_query.register(self.system_statistics, F.data == "system:statistics")
        r.callback_query.register(
            self.analytics_period, F.data.startswith("analytics:period:")
        )
        r.callback_query.register(
            self.analytics_accounts, F.data.startswith("analytics:accounts:")
        )
        r.callback_query.register(
            self.analytics_targets, F.data.startswith("analytics:targets:")
        )
        r.callback_query.register(
            self.analytics_export, F.data.startswith("analytics:export:")
        )
        r.callback_query.register(self.system_accounts, F.data == "system:accounts")
        r.callback_query.register(
            self.system_accounts, F.data.startswith("system:accounts_page:")
        )
        r.callback_query.register(
            self.system_account_detail, F.data.startswith("system:account:")
        )
        r.callback_query.register(self.stats, F.data == "stats")

    async def _main_menu_text(self) -> str:
        stats = await self.db.stats()
        problem_line = (
            f"⚠️ Проблемные аккаунты: <b>{stats['problem_accounts']}</b>\n"
            if stats["problem_accounts"]
            else ""
        )
        return (
            "❤️ <b>Главное меню LikeBot</b>\n"
            "<i>Управление аккаунтами, каналами и реакциями</i>\n\n"
            "━━━━━━━━━━━━━━\n"
            "📌 <b>Текущий статус</b>\n"
            f"👥 Аккаунты: <b>{stats['active_accounts']}</b> активных / {stats['accounts']} всего\n"
            f"{problem_line}"
            f"📢 Каналы: <b>{stats['channels']}</b>  ·  Группы: <b>{stats['groups']}</b>\n"
            f"🧾 Очередь реакций: <b>{stats['reaction_pending']}</b>\n"
            "━━━━━━━━━━━━━━\n\n"
            "Выберите нужный раздел 👇"
        )

    async def start(self, message: Message, state: FSMContext) -> None:
        if await self._deny_if_needed(message):
            return
        await self.login_manager.cancel(message.from_user.id)
        await state.clear()
        await message.answer(await self._main_menu_text(), reply_markup=main_menu())

    async def menu_command(self, message: Message, state: FSMContext) -> None:
        if await self._deny_if_needed(message):
            return
        await self.login_manager.cancel(message.from_user.id)
        await state.clear()
        await message.answer(await self._main_menu_text(), reply_markup=main_menu())

    async def cancel(self, message: Message, state: FSMContext) -> None:
        if await self._deny_if_needed(message):
            return
        await self.login_manager.cancel(message.from_user.id)
        await state.clear()
        await message.answer("✅ Действие отменено.\n\n" + await self._main_menu_text(), reply_markup=main_menu())

    async def main_callback(self, callback: CallbackQuery, state: FSMContext) -> None:
        if await self._deny_if_needed(callback):
            return
        await self.login_manager.cancel(callback.from_user.id)
        await state.clear()
        await callback.message.edit_text(await self._main_menu_text(), reply_markup=main_menu())
        await callback.answer()
