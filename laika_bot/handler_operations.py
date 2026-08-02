from __future__ import annotations

from .handler_shared import *  # noqa: F403


class TargetOperationsHandlersMixin:
    @staticmethod
    def _manual_view_account_count(
        joined_count: int, selection_mode: str, selection_value: int
    ) -> int:
        if joined_count <= 0:
            return 0
        if selection_mode == "percent":
            return min(joined_count, max(1, joined_count * selection_value // 100))
        return min(joined_count, selection_value)

    async def _render_channel_views_setup(self, message: Message, channel_id: int) -> bool:
        channel = await self.db.get_channel(channel_id)
        if not channel or channel.kind != "channel" or not channel.is_active:
            return False
        membership = await self.db.membership_counts(channel.id)
        counts = await self.db.view_counts(channel.id)
        delay_min, delay_max = await self.db.get_delays(
            self.settings.default_reaction_delay_min_seconds,
            self.settings.default_reaction_delay_max_seconds,
        )
        await self._safe_edit_text(
            message,
            "👁 <b>Добавить просмотры</b>\n"
            f"<i>{html.escape(channel.title)}</i>\n\n"
            "━━━━━━━━━━━━━━\n"
            f"👥 Подписано аккаунтов: <b>{membership['joined']}</b>\n"
            f"⏳ Уже ожидают: <b>{counts['pending'] + counts['running']}</b>\n"
            f"✅ Выполнено: <b>{counts['done']}</b>\n"
            f"⏱ Между аккаунтами: <b>{delay_min}-{delay_max} сек.</b>\n"
            "━━━━━━━━━━━━━━\n\n"
            "Выберите, сколько последних публикаций обработать. "
            "Конкретный пост выбирать не нужно.",
            reply_markup=channel_views_setup_keyboard(channel.id),
        )
        return True

    async def _render_channel_views_confirmation(
        self,
        message: Message,
        *,
        channel_id: int,
        post_count: int,
        selection_mode: str,
        selection_value: int,
    ) -> bool:
        channel = await self.db.get_channel(channel_id)
        if not channel or channel.kind != "channel" or not channel.is_active:
            return False
        membership = await self.db.membership_counts(channel.id)
        accounts_per_post = self._manual_view_account_count(
            membership["joined"], selection_mode, selection_value
        )
        delay_min, delay_max = await self.db.get_delays(
            self.settings.default_reaction_delay_min_seconds,
            self.settings.default_reaction_delay_max_seconds,
        )
        selection_text = (
            f"{selection_value}% — до {accounts_per_post}"
            if selection_mode == "percent"
            else f"{accounts_per_post} аккаунтов"
        )
        await self._safe_edit_text(
            message,
            "👁 <b>Настройка просмотров</b>\n"
            f"<i>{html.escape(channel.title)}</i>\n\n"
            "━━━━━━━━━━━━━━\n"
            f"📚 Публикаций: <b>последние {post_count}</b>\n"
            f"👥 Подписано аккаунтов: <b>{membership['joined']}</b>\n"
            f"📊 На каждый пост: <b>{selection_text}</b>\n"
            f"📦 Максимум заданий: <b>до {post_count * accounts_per_post}</b>\n"
            f"⏱ Между аккаунтами: <b>{delay_min}-{delay_max} сек.</b>\n"
            "━━━━━━━━━━━━━━\n\n"
            "Аккаунты, для которых просмотр уже выполнен или запланирован вместе с реакцией, "
            "не получат повторное задание.",
            reply_markup=channel_views_confirm_keyboard(
                channel.id,
                post_count=post_count,
                selection_mode=selection_mode,
                selection_value=selection_value,
            ),
        )
        return True

    async def channel_views_open(self, callback: CallbackQuery, state: FSMContext) -> None:
        if await self._deny_if_needed(callback):
            return
        await state.clear()
        channel_id = int(callback.data.rsplit(":", 1)[1])
        if not await self._render_channel_views_setup(callback.message, channel_id):
            await callback.answer("Канал не найден", show_alert=True)
            return
        await callback.answer()

    async def channel_views_posts(self, callback: CallbackQuery, state: FSMContext) -> None:
        if await self._deny_if_needed(callback):
            return
        await state.clear()
        parts = callback.data.split(":")
        if len(parts) != 4:
            await callback.answer("Некорректная настройка", show_alert=True)
            return
        channel_id, post_count = int(parts[2]), int(parts[3])
        if post_count not in {5, 20, 50, 100}:
            await callback.answer("Некорректное число публикаций", show_alert=True)
            return
        if not await self._render_channel_views_confirmation(
            callback.message,
            channel_id=channel_id,
            post_count=post_count,
            selection_mode="percent",
            selection_value=100,
        ):
            await callback.answer("Канал не найден", show_alert=True)
            return
        await callback.answer()

    async def channel_views_accounts(self, callback: CallbackQuery, state: FSMContext) -> None:
        if await self._deny_if_needed(callback):
            return
        await state.clear()
        parts = callback.data.split(":")
        if len(parts) != 5:
            await callback.answer("Некорректная настройка", show_alert=True)
            return
        channel_id, post_count, percent = int(parts[2]), int(parts[3]), int(parts[4])
        if post_count not in {5, 20, 50, 100} or percent not in {25, 50, 75, 100}:
            await callback.answer("Некорректное значение", show_alert=True)
            return
        if not await self._render_channel_views_confirmation(
            callback.message,
            channel_id=channel_id,
            post_count=post_count,
            selection_mode="percent",
            selection_value=percent,
        ):
            await callback.answer("Канал не найден", show_alert=True)
            return
        await callback.answer(f"На каждый пост: {percent}%")

    async def channel_views_manual(self, callback: CallbackQuery, state: FSMContext) -> None:
        if await self._deny_if_needed(callback):
            return
        parts = callback.data.split(":")
        if len(parts) != 4:
            await callback.answer("Некорректная настройка", show_alert=True)
            return
        channel_id, post_count = int(parts[2]), int(parts[3])
        channel = await self.db.get_channel(channel_id)
        if not channel or channel.kind != "channel" or not channel.is_active:
            await callback.answer("Канал не найден", show_alert=True)
            return
        membership = await self.db.membership_counts(channel.id)
        if membership["joined"] <= 0:
            await callback.answer("Нет подписанных аккаунтов", show_alert=True)
            return
        await state.set_state(SetManualViewAmount.value)
        await state.update_data(channel_id=channel.id, post_count=post_count)
        await callback.message.answer(
            "✏️ <b>Количество аккаунтов на каждый пост</b>\n\n"
            f"Канал: <b>{html.escape(channel.title)}</b>\n"
            f"Подписано: <b>{membership['joined']}</b>\n"
            f"Публикаций: <b>последние {post_count}</b>\n\n"
            "Введите точное число аккаунтов или процент.\n"
            "Примеры: <code>6</code> или <code>50%</code>.",
            reply_markup=channel_views_manual_cancel_keyboard(channel.id, post_count),
        )
        await callback.answer()

    async def channel_views_manual_input(self, message: Message, state: FSMContext) -> None:
        if await self._deny_if_needed(message):
            return
        data = await state.get_data()
        channel_id = int(data.get("channel_id", 0))
        post_count = int(data.get("post_count", 0))
        channel = await self.db.get_channel(channel_id)
        if not channel or channel.kind != "channel" or not channel.is_active:
            await state.clear()
            await message.answer("❌ Канал не найден.", reply_markup=main_menu())
            return
        membership = await self.db.membership_counts(channel.id)
        try:
            selection_mode, selection_value, _count = parse_manual_view_amount(
                message.text or "", membership["joined"]
            )
        except ValueError as exc:
            await message.answer(f"❌ {html.escape(str(exc))}")
            return
        await state.clear()
        selection_count = self._manual_view_account_count(
            membership["joined"], selection_mode, selection_value
        )
        delay_min, delay_max = await self.db.get_delays(
            self.settings.default_reaction_delay_min_seconds,
            self.settings.default_reaction_delay_max_seconds,
        )
        selection_text = (
            f"{selection_value}% — до {selection_count}"
            if selection_mode == "percent"
            else f"{selection_count} аккаунтов"
        )
        await message.answer(
            "👁 <b>Настройка просмотров</b>\n"
            f"<i>{html.escape(channel.title)}</i>\n\n"
            "━━━━━━━━━━━━━━\n"
            f"📚 Публикаций: <b>последние {post_count}</b>\n"
            f"👥 Подписано аккаунтов: <b>{membership['joined']}</b>\n"
            f"📊 На каждый пост: <b>{selection_text}</b>\n"
            f"📦 Максимум заданий: <b>до {post_count * selection_count}</b>\n"
            f"⏱ Между аккаунтами: <b>{delay_min}-{delay_max} сек.</b>\n"
            "━━━━━━━━━━━━━━",
            reply_markup=channel_views_confirm_keyboard(
                channel.id,
                post_count=post_count,
                selection_mode=selection_mode,
                selection_value=selection_value,
            ),
        )

    async def channel_views_run(self, callback: CallbackQuery, state: FSMContext) -> None:
        if await self._deny_if_needed(callback):
            return
        await state.clear()
        parts = callback.data.split(":")
        if len(parts) != 6:
            await callback.answer("Некорректная задача", show_alert=True)
            return
        channel_id, post_count = int(parts[2]), int(parts[3])
        if parts[4] not in {"p", "c"}:
            await callback.answer("Некорректный режим", show_alert=True)
            return
        selection_mode = "percent" if parts[4] == "p" else "count"
        selection_value = int(parts[5])
        channel = await self.db.get_channel(channel_id)
        if not channel or channel.kind != "channel" or not channel.is_active:
            await callback.answer("Канал не найден", show_alert=True)
            return
        await callback.answer("Создаю очередь просмотров…")
        try:
            batch = await self.jobs.schedule_recent_views(
                channel,
                post_count=post_count,
                selection_mode=selection_mode,
                selection_value=selection_value,
            )
        except (ValueError, RuntimeError) as exc:
            await callback.message.edit_text(
                "❌ <b>Не удалось создать просмотры</b>\n\n"
                f"{html.escape(str(exc))}",
                reply_markup=channel_views_setup_keyboard(channel.id),
            )
            return
        await self._render_view_batch(callback.message, batch.id)

    async def _render_view_batch(self, message: Message, batch_id: int) -> bool:
        batch = await self.db.get_view_batch(batch_id)
        if batch is None:
            return False
        counts = await self.db.view_batch_counts(batch.id)
        channel = batch.channel
        active = counts["pending"] + counts["running"]
        status_labels = {
            "pending": "⏳ Ожидает",
            "running": "▶️ Выполняется",
            "done": "✅ Завершена",
            "cancelled": "🛑 Остановлена",
        }
        status = status_labels.get(batch.status, batch.status)
        await self._safe_edit_text(
            message,
            "👁 <b>Задача просмотров</b>\n"
            f"<i>{html.escape(channel.title)}</i>\n\n"
            "━━━━━━━━━━━━━━\n"
            f"🆔 Задача: <b>#{batch.id}</b>\n"
            f"📚 Публикаций: <b>{batch.posts_found}</b> из запрошенных {batch.requested_post_count}\n"
            f"👥 Аккаунтов на пост: <b>до {batch.accounts_per_post}</b>\n"
            f"📦 Создано заданий: <b>{batch.total_jobs}</b>\n"
            f"⏳ Ожидают: <b>{active}</b>\n"
            f"✅ Выполнено: <b>{counts['done']}</b>\n"
            f"⏭ Уже учтено или запланировано: <b>{batch.skipped_existing}</b>\n"
            f"❌ Ошибки: <b>{counts['failed']}</b>\n"
            f"🛑 Отменено: <b>{counts['cancelled']}</b>\n"
            f"📍 Статус: <b>{status}</b>\n"
            "━━━━━━━━━━━━━━\n\n"
            "Telegram может не увеличить публичный счётчик повторно, если аккаунт уже просматривал публикацию.",
            reply_markup=channel_view_batch_keyboard(
                batch.id, channel.id, can_cancel=counts["pending"] > 0
            ),
        )
        return True

    async def channel_views_batch(self, callback: CallbackQuery) -> None:
        if await self._deny_if_needed(callback):
            return
        batch_id = int(callback.data.rsplit(":", 1)[1])
        if not await self._render_view_batch(callback.message, batch_id):
            await callback.answer("Задача не найдена", show_alert=True)
            return
        await callback.answer("Статус обновлён")

    async def channel_views_cancel(self, callback: CallbackQuery) -> None:
        if await self._deny_if_needed(callback):
            return
        batch_id = int(callback.data.rsplit(":", 1)[1])
        batch = await self.db.get_view_batch(batch_id)
        if batch is None:
            await callback.answer("Задача не найдена", show_alert=True)
            return
        cancelled = await self.db.cancel_view_batch(batch.id)
        await self._render_view_batch(callback.message, batch.id)
        await callback.answer(f"Остановлено заданий: {cancelled}")

    async def channel_members(self, callback: CallbackQuery) -> None:
        if await self._deny_if_needed(callback):
            return
        channel_id = int(callback.data.rsplit(":", 1)[1])
        channel = await self.db.get_channel(channel_id)
        if not channel or channel.kind != "channel":
            await callback.answer("Канал не найден", show_alert=True)
            return
        joined_ids = await self.db.joined_account_ids(channel.id)
        accounts = []
        for account_id in joined_ids:
            account = await self.db.get_account(account_id)
            if account:
                accounts.append(account)
        lines = [
            f"{'🟢' if account.is_active else '⚫️'} {html.escape(display_account_name(account.display_name, account.username))}"
            for account in accounts
        ]
        await callback.message.edit_text(
            "👥 <b>Аккаунты канала</b>\n"
            f"<i>{html.escape(channel.title)}</i>\n\n"
            "━━━━━━━━━━━━━━\n"
            f"Подписаны: <b>{len(accounts)}</b>\n"
            "━━━━━━━━━━━━━━\n\n"
            + ("\n".join(lines) if lines else "Подписанных аккаунтов пока нет."),
            reply_markup=channel_back_keyboard(channel.id),
        )
        await callback.answer()

    async def channel_stats(self, callback: CallbackQuery) -> None:
        if await self._deny_if_needed(callback):
            return
        channel_id = int(callback.data.rsplit(":", 1)[1])
        channel = await self.db.get_channel(channel_id)
        if not channel or channel.kind != "channel":
            await callback.answer("Канал не найден", show_alert=True)
            return
        membership = await self.db.membership_counts(channel.id)
        queue = await self.db.reaction_counts(channel.id)
        views = await self.db.view_counts(channel.id)
        await callback.message.edit_text(
            "📊 <b>Статистика канала</b>\n"
            f"<i>{html.escape(channel.title)}</i>\n\n"
            "━━━━━━━━━━━━━━\n"
            "👥 <b>Подписки</b>\n"
            f"Подписаны: {membership['joined']}\n"
            f"Ожидают: {membership['pending']}\n"
            f"Ошибки: {membership['failed']}\n\n"
            "❤️ <b>Реакции</b>\n"
            f"Ожидают: {queue['pending']}\n"
            f"Готово: {queue['done']}\n"
            f"Ошибки: {queue['failed']}\n\n"
            "👁 <b>Просмотры</b>\n"
            f"Ожидают: {views['pending'] + views['running']}\n"
            f"Готово: {views['done']}\n"
            f"Ошибки: {views['failed']}\n"
            f"Отменено: {views['cancelled']}\n"
            "━━━━━━━━━━━━━━",
            reply_markup=channel_back_keyboard(channel.id),
        )
        await callback.answer()

    async def _render_channel_connect(self, message: Message, channel_id: int) -> bool:
        channel = await self.db.get_channel(channel_id)
        if not channel or channel.kind != "channel" or not channel.is_active:
            return False
        summary, _available = await self.db.channel_connect_state(
            channel.id, max_accounts=self.settings.max_accounts_per_channel
        )
        delay_min, delay_max = await self.db.get_membership_delays(
            self.settings.join_delay_min_seconds,
            self.settings.join_delay_max_seconds,
        )
        await self._safe_edit_text(
            message,
            "➕ <b>Подключение аккаунтов</b>\n"
            f"<i>{html.escape(channel.title)}</i>\n\n"
            "━━━━━━━━━━━━━━\n"
            f"👥 Всего активных: <b>{summary['total']}</b>\n"
            f"✅ Уже подписаны: <b>{summary['joined']}</b>\n"
            f"⏳ Уже в очереди: <b>{summary['pending']}</b>\n"
            f"➕ Доступно для подключения: <b>{summary['connectable']}</b>\n"
            f"⚠️ Ошибок прошлых попыток: <b>{summary['failed']}</b>\n"
            "━━━━━━━━━━━━━━\n"
            f"📢 Текущая задержка подписки: <b>{delay_min}-{delay_max} сек.</b>\n\n"
            "В очередь попадут только активные аккаунты, которые ещё не подписаны "
            "и не ожидают подключения. Повторные задания не создаются.",
            reply_markup=channel_connect_overview_keyboard(
                channel.id, connectable_count=summary["connectable"]
            ),
        )
        return True

    async def channel_connect_view(self, callback: CallbackQuery) -> None:
        if await self._deny_if_needed(callback):
            return
        channel_id = int(callback.data.rsplit(":", 1)[1])
        if not await self._render_channel_connect(callback.message, channel_id):
            await callback.answer("Канал не найден", show_alert=True)
            return
        await callback.answer()

    async def channel_connect_refresh(self, callback: CallbackQuery) -> None:
        if await self._deny_if_needed(callback):
            return
        channel_id = int(callback.data.rsplit(":", 1)[1])
        if not await self._render_channel_connect(callback.message, channel_id):
            await callback.answer("Канал не найден", show_alert=True)
            return
        await callback.answer("Состояние очереди обновлено")

    async def channel_connect_all(self, callback: CallbackQuery) -> None:
        if await self._deny_if_needed(callback):
            return
        channel_id = int(callback.data.rsplit(":", 1)[1])
        channel = await self.db.get_channel(channel_id)
        if not channel or channel.kind != "channel" or not channel.is_active:
            await callback.answer("Канал не найден", show_alert=True)
            return
        summary, _available = await self.db.channel_connect_state(
            channel.id, max_accounts=self.settings.max_accounts_per_channel
        )
        if summary["connectable"] <= 0:
            await self._render_channel_connect(callback.message, channel.id)
            await callback.answer("Недостающих аккаунтов нет")
            return
        delay_min, delay_max = await self.db.get_membership_delays(
            self.settings.join_delay_min_seconds,
            self.settings.join_delay_max_seconds,
        )
        queue_note = (
            "Новые задания будут добавлены после последнего ожидающего задания."
            if summary["pending"] > 0
            else "Первый аккаунт начнёт подключение сразу."
        )
        await callback.message.edit_text(
            "✅ <b>Подключить недостающие аккаунты?</b>\n\n"
            f"Канал: <b>{html.escape(channel.title)}</b>\n"
            f"Будет добавлено в очередь: <b>{summary['connectable']}</b>\n"
            f"Задержка подписки: <b>{delay_min}-{delay_max} сек.</b>\n\n"
            f"{queue_note} Каждый следующий аккаунт получит случайный интервал "
            f"{delay_min}-{delay_max} секунд.",
            reply_markup=channel_connect_all_confirm_keyboard(channel.id),
        )
        await callback.answer()

    async def channel_connect_run_all(self, callback: CallbackQuery) -> None:
        if await self._deny_if_needed(callback):
            return
        channel_id = int(callback.data.rsplit(":", 1)[1])
        channel = await self.db.get_channel(channel_id)
        if not channel or channel.kind != "channel" or not channel.is_active:
            await callback.answer("Канал не найден", show_alert=True)
            return
        result = await self.jobs.schedule_missing_channel_joins(channel)
        await self._render_channel_connect(callback.message, channel.id)
        await callback.answer(
            f"Добавлено в очередь: {result['scheduled']}",
            show_alert=result["scheduled"] == 0,
        )

    async def _render_channel_connect_manual(
        self, message: Message, state: FSMContext, channel_id: int, page: int
    ) -> bool:
        channel = await self.db.get_channel(channel_id)
        if not channel or channel.kind != "channel" or not channel.is_active:
            return False
        summary, available = await self.db.channel_connect_state(
            channel.id, max_accounts=self.settings.max_accounts_per_channel
        )
        data = await state.get_data()
        selected_ids = (
            set(int(value) for value in data.get("connect_selected_ids", []))
            if int(data.get("connect_channel_id", 0) or 0) == channel.id
            else set()
        )
        available_ids = {account.id for account in available}
        selected_ids.intersection_update(available_ids)
        await state.update_data(
            connect_channel_id=channel.id,
            connect_selected_ids=sorted(selected_ids),
        )
        account_rows = [
            (
                account.id,
                truncate(display_account_name(account.display_name, account.username), 42),
            )
            for account in available
        ]
        await self._safe_edit_text(
            message,
            "☑️ <b>Выбор аккаунтов</b>\n"
            f"<i>{html.escape(channel.title)}</i>\n\n"
            "━━━━━━━━━━━━━━\n"
            f"Доступно: <b>{summary['connectable']}</b>\n"
            f"Выбрано: <b>{len(selected_ids)}</b>\n"
            "━━━━━━━━━━━━━━\n\n"
            + (
                "Нажмите на аккаунты, которые нужно добавить в очередь."
                if available
                else "Недостающих аккаунтов больше нет."
            ),
            reply_markup=channel_connect_manual_keyboard(
                channel.id, account_rows, selected_ids, page=page
            ),
        )
        return True

    async def channel_connect_manual(self, callback: CallbackQuery, state: FSMContext) -> None:
        if await self._deny_if_needed(callback):
            return
        parts = callback.data.split(":")
        channel_id = int(parts[2])
        page = int(parts[3])
        if not await self._render_channel_connect_manual(callback.message, state, channel_id, page):
            await callback.answer("Канал не найден", show_alert=True)
            return
        await callback.answer()

    async def channel_connect_toggle(self, callback: CallbackQuery, state: FSMContext) -> None:
        if await self._deny_if_needed(callback):
            return
        parts = callback.data.split(":")
        channel_id, account_id, page = int(parts[2]), int(parts[3]), int(parts[4])
        summary, available = await self.db.channel_connect_state(
            channel_id, max_accounts=self.settings.max_accounts_per_channel
        )
        del summary
        available_ids = {account.id for account in available}
        data = await state.get_data()
        selected_ids = (
            set(int(value) for value in data.get("connect_selected_ids", []))
            if int(data.get("connect_channel_id", 0) or 0) == channel_id
            else set()
        )
        selected_ids.intersection_update(available_ids)
        if account_id in available_ids:
            if account_id in selected_ids:
                selected_ids.remove(account_id)
            else:
                selected_ids.add(account_id)
        await state.update_data(
            connect_channel_id=channel_id, connect_selected_ids=sorted(selected_ids)
        )
        await self._render_channel_connect_manual(callback.message, state, channel_id, page)
        await callback.answer()

    async def channel_connect_select_all(self, callback: CallbackQuery, state: FSMContext) -> None:
        if await self._deny_if_needed(callback):
            return
        parts = callback.data.split(":")
        channel_id, page = int(parts[2]), int(parts[3])
        _summary, available = await self.db.channel_connect_state(
            channel_id, max_accounts=self.settings.max_accounts_per_channel
        )
        await state.update_data(
            connect_channel_id=channel_id,
            connect_selected_ids=[account.id for account in available],
        )
        await self._render_channel_connect_manual(callback.message, state, channel_id, page)
        await callback.answer("Выбраны все доступные аккаунты")

    async def channel_connect_clear(self, callback: CallbackQuery, state: FSMContext) -> None:
        if await self._deny_if_needed(callback):
            return
        parts = callback.data.split(":")
        channel_id, page = int(parts[2]), int(parts[3])
        await state.update_data(connect_channel_id=channel_id, connect_selected_ids=[])
        await self._render_channel_connect_manual(callback.message, state, channel_id, page)
        await callback.answer("Выбор сброшен")

    async def channel_connect_selected(self, callback: CallbackQuery, state: FSMContext) -> None:
        if await self._deny_if_needed(callback):
            return
        channel_id = int(callback.data.rsplit(":", 1)[1])
        channel = await self.db.get_channel(channel_id)
        if not channel or channel.kind != "channel" or not channel.is_active:
            await callback.answer("Канал не найден", show_alert=True)
            return
        _summary, available = await self.db.channel_connect_state(
            channel.id, max_accounts=self.settings.max_accounts_per_channel
        )
        available_ids = {account.id for account in available}
        data = await state.get_data()
        selected_ids = (
            set(int(value) for value in data.get("connect_selected_ids", []))
            if int(data.get("connect_channel_id", 0) or 0) == channel.id
            else set()
        )
        selected_ids.intersection_update(available_ids)
        if not selected_ids:
            await callback.answer("Сначала выберите хотя бы один аккаунт", show_alert=True)
            return
        delay_min, delay_max = await self.db.get_membership_delays(
            self.settings.join_delay_min_seconds,
            self.settings.join_delay_max_seconds,
        )
        await state.update_data(
            connect_channel_id=channel.id, connect_selected_ids=sorted(selected_ids)
        )
        await callback.message.edit_text(
            "✅ <b>Подключить выбранные аккаунты?</b>\n\n"
            f"Канал: <b>{html.escape(channel.title)}</b>\n"
            f"Выбрано аккаунтов: <b>{len(selected_ids)}</b>\n"
            f"Задержка подписки: <b>{delay_min}-{delay_max} сек.</b>\n\n"
            "Перед запуском список будет проверен повторно. Уже подписанные "
            "и ожидающие аккаунты будут безопасно пропущены.",
            reply_markup=channel_connect_selected_confirm_keyboard(channel.id),
        )
        await callback.answer()

    async def channel_connect_run_selected(
        self, callback: CallbackQuery, state: FSMContext
    ) -> None:
        if await self._deny_if_needed(callback):
            return
        channel_id = int(callback.data.rsplit(":", 1)[1])
        channel = await self.db.get_channel(channel_id)
        if not channel or channel.kind != "channel" or not channel.is_active:
            await callback.answer("Канал не найден", show_alert=True)
            return
        data = await state.get_data()
        selected_ids = (
            [int(value) for value in data.get("connect_selected_ids", [])]
            if int(data.get("connect_channel_id", 0) or 0) == channel.id
            else []
        )
        if not selected_ids:
            await callback.answer("Выбранные аккаунты не найдены", show_alert=True)
            return
        result = await self.jobs.schedule_missing_channel_joins(
            channel, account_ids=selected_ids
        )
        await state.update_data(connect_channel_id=channel.id, connect_selected_ids=[])
        await self._render_channel_connect(callback.message, channel.id)
        await callback.answer(
            f"Добавлено: {result['scheduled']} · пропущено: {result['skipped']}",
            show_alert=result["scheduled"] == 0,
        )

    async def channel_delete_confirm(self, callback: CallbackQuery) -> None:
        if await self._deny_if_needed(callback):
            return
        channel_id = int(callback.data.rsplit(":", 1)[1])
        await callback.message.edit_text(
            "⚠️ Удалить канал, очередь подключений и все задания реакций?\n"
            "Аккаунты из самого Telegram-канала автоматически не выходят.",
            reply_markup=confirm_channel_delete(channel_id),
        )
        await callback.answer()

    async def channel_delete(self, callback: CallbackQuery) -> None:
        if await self._deny_if_needed(callback):
            return
        channel_id = int(callback.data.rsplit(":", 1)[1])
        await self.db.delete_channel(channel_id)
        self._channel_profile_locks.pop(channel_id, None)
        await self._render_channel_list(callback.message)
        await callback.answer("Канал удалён")

    async def group_list(self, callback: CallbackQuery) -> None:
        if await self._deny_if_needed(callback):
            return
        groups = await self.db.list_channels(active_only=True, kind="group")
        text = (
            "👥 <b>Группы</b>\n"
            "<i>Группы, к которым подключаются аккаунты</i>\n\n"
            "━━━━━━━━━━━━━━\n"
            f"🟢 Активны: <b>{len(groups)}</b>\n"
            "━━━━━━━━━━━━━━\n\n"
            + ("Выберите группу 👇" if groups else "Добавленных групп пока нет. Добавьте первую группу 👇")
        )
        await callback.message.edit_text(text, reply_markup=group_list_keyboard(groups))
        await callback.answer()

    async def group_view(self, callback: CallbackQuery) -> None:
        if await self._deny_if_needed(callback):
            return
        group_id = int(callback.data.rsplit(":", 1)[1])
        group = await self.db.get_channel(group_id)
        if not group or group.kind != "group" or not group.is_active:
            await callback.answer("Группа не найдена", show_alert=True)
            return
        counts = await self.db.membership_counts(group.id)
        error = html.escape(truncate(group.last_error)) if group.last_error else "нет"
        override = await self.db.get_channel_reaction_override_weights(group.id)
        effective = override or await self.db.get_reaction_weights()
        reaction_mode = "свои для этой группы" if override else "по умолчанию"
        await callback.message.edit_text(
            "👥 <b>Карточка группы</b>\n"
            f"<i>{html.escape(group.title)}</i>\n\n"
            "━━━━━━━━━━━━━━\n"
            f"🔗 Ссылка: {html.escape(group.link)}\n"
            f"😀 Реакции: <b>{html.escape(format_reaction_weights(effective))}</b>\n"
            f"🎛 Режим: <b>{reaction_mode}</b>\n"
            f"🟢 Подписаны: <b>{counts['joined']}</b>\n"
            f"⏳ В очереди: <b>{counts['pending']}</b>\n"
            f"⚠️ Ошибки: <b>{counts['failed']}</b>\n"
            f"📝 Последняя ошибка: <code>{error}</code>\n"
            "━━━━━━━━━━━━━━",
            reply_markup=group_actions(group.id),
        )
        await callback.answer()

    async def _render_group_reactions(self, message: Message, group_id: int) -> bool:
        group = await self.db.get_channel(group_id)
        if not group or group.kind != "group" or not group.is_active:
            return False
        override = await self.db.get_channel_reaction_override_weights(group.id)
        defaults = await self.db.get_reaction_weights()
        effective = override or defaults
        mode = "🟣 Свои реакции группы" if override else "🔵 Реакции по умолчанию"
        await message.edit_text(
            "😀 <b>Реакции группы</b>\n"
            f"<i>{html.escape(group.title)}</i>\n\n"
            "━━━━━━━━━━━━━━\n"
            f"Текущее соотношение: <b>{html.escape(format_reaction_weights(effective))}</b>\n"
            "Итого: <b>100%</b>\n"
            f"Режим: <b>{mode}</b>\n"
            f"По умолчанию: {html.escape(format_reaction_weights(defaults))}\n"
            "━━━━━━━━━━━━━━\n\n"
            "Для публикаций этой группы бот использует указанные здесь реакции и веса. "
            "Если отдельные настройки не заданы, применяются реакции по умолчанию.",
            reply_markup=group_reactions_keyboard(group.id, has_override=bool(override)),
        )
        return True

    async def group_reactions_view(self, callback: CallbackQuery, state: FSMContext) -> None:
        if await self._deny_if_needed(callback):
            return
        await state.clear()
        group_id = int(callback.data.rsplit(":", 1)[1])
        if not await self._render_group_reactions(callback.message, group_id):
            await callback.answer("Группа не найдена", show_alert=True)
            return
        await callback.answer()

    async def group_reactions_edit(self, callback: CallbackQuery, state: FSMContext) -> None:
        if await self._deny_if_needed(callback):
            return
        group_id = int(callback.data.rsplit(":", 1)[1])
        group = await self.db.get_channel(group_id)
        if not group or group.kind != "group" or not group.is_active:
            await callback.answer("Группа не найдена", show_alert=True)
            return
        current = await self.db.get_reaction_weights_for_channel(group)
        await state.set_state(SetGroupReactions.reactions)
        await state.update_data(group_id=group.id)
        await callback.message.edit_text(
            "✏️ <b>Реакции конкретной группы</b>\n\n"
            f"Группа: <b>{html.escape(group.title)}</b>\n"
            f"Сейчас: {html.escape(format_reaction_weights(current))}\n\n"
            "Укажите эмодзи и любые числовые веса. Бот сам пересчитает их в 100%.\n"
            "Пример: <code>👍60 ❤️20 🔥20</code>\n"
            "Или: <code>👍 6 ❤️ 2 🔥 2</code>\n"
            "Без чисел реакции будут равновероятными: <code>👍 ❤️ 🔥</code>\n\n"
            "Отмена: /cancel",
            reply_markup=back_main(),
        )
        await callback.answer()

    async def group_reactions_input(self, message: Message, state: FSMContext) -> None:
        if await self._deny_if_needed(message):
            return
        data = await state.get_data()
        group_id = int(data.get("group_id", 0))
        group = await self.db.get_channel(group_id)
        if not group or group.kind != "group" or not group.is_active:
            await state.clear()
            await message.answer("❌ Группа не найдена.", reply_markup=main_menu())
            return
        try:
            weights = parse_weighted_reactions(message.text or "")
        except ValueError as exc:
            await message.answer(f"❌ {html.escape(str(exc))}")
            return
        await self.db.set_channel_reaction_weights(group.id, weights)
        updated_pending = await self.db.refresh_pending_reactions(group.id, weights)
        await state.clear()
        await message.answer(
            "✅ <b>Реакции группы сохранены</b>\n\n"
            f"Группа: <b>{html.escape(group.title)}</b>\n"
            f"Соотношение: <b>{html.escape(format_reaction_weights(weights))}</b>\n"
            "Итого: <b>100%</b>\n"
            f"Обновлено ожидающих заданий: <b>{updated_pending}</b>\n\n"
            "Другие группы и каналы эти настройки не используют.",
            reply_markup=group_reactions_keyboard(group.id, has_override=True),
        )

    async def group_reactions_reset(self, callback: CallbackQuery, state: FSMContext) -> None:
        if await self._deny_if_needed(callback):
            return
        await state.clear()
        group_id = int(callback.data.rsplit(":", 1)[1])
        group = await self.db.get_channel(group_id)
        if not group or group.kind != "group" or not group.is_active:
            await callback.answer("Группа не найдена", show_alert=True)
            return
        await self.db.clear_channel_reactions(group.id)
        defaults = await self.db.get_reaction_weights()
        updated_pending = await self.db.refresh_pending_reactions(group.id, defaults)
        if not await self._render_group_reactions(callback.message, group.id):
            await callback.answer("Группа не найдена", show_alert=True)
            return
        await callback.answer(
            f"Включены реакции по умолчанию · обновлено заданий: {updated_pending}"
        )

    async def group_leave_confirm(self, callback: CallbackQuery) -> None:
        if await self._deny_if_needed(callback):
            return
        group_id = int(callback.data.rsplit(":", 1)[1])
        group = await self.db.get_channel(group_id)
        if not group or group.kind != "group" or not group.is_active:
            await callback.answer("Группа не найдена", show_alert=True)
            return
        await callback.message.edit_text(
            "⚠️ <b>Отписаться от группы?</b>\n\n"
            f"Группа: {html.escape(group.title)}\n\n"
            "Будет создана очередь выхода для всех сохранённых аккаунтов. Ожидающие реакции этой группы отменятся.",
            reply_markup=confirm_group_leave(group.id),
        )
        await callback.answer()

    async def group_leave(self, callback: CallbackQuery) -> None:
        if await self._deny_if_needed(callback):
            return
        group_id = int(callback.data.rsplit(":", 1)[1])
        group = await self.db.get_channel(group_id)
        if not group or group.kind != "group" or not group.is_active:
            await callback.answer("Группа не найдена", show_alert=True)
            return
        cancelled = await self.db.cancel_pending_reactions(group.id)
        scheduled = await self.jobs.schedule_channel_leaves(group)
        await self.db.deactivate_channel(group.id)
        await callback.answer("Очередь выхода создана", show_alert=True)
        groups = await self.db.list_channels(active_only=True, kind="group")
        await callback.message.edit_text(
            "✅ <b>Отписка запущена</b>\n\n"
            f"Группа: {html.escape(group.title)}\n"
            f"Аккаунтов в очереди: {scheduled}\n"
            f"Отменено реакций: {cancelled}\n\n"
            "Группа убрана из активного списка.",
            reply_markup=group_list_keyboard(groups),
        )
