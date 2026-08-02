from __future__ import annotations

from .handler_shared import *  # noqa: F403


class AutomationSettingsHandlersMixin:
    async def _render_autolike_view(self, message: Message, channel_id: int) -> bool:
        channel = await self.db.get_channel(channel_id)
        if not channel:
            return False
        if not promotion_is_active(channel) and (channel.new_posts_enabled or channel.old_posts_enabled):
            await self.db.close_expired_promotion(channel.id)
            channel = await self.db.get_channel(channel_id)
            if not channel:
                return False
        reaction_weights = await self.db.get_reaction_weights_for_channel(channel)
        reactions = format_reaction_weights(reaction_weights)
        reaction_scope = "свои для цели" if channel.reactions_json else "по умолчанию"
        profile_key = await self.db.get_channel_profile_key(
            channel, max_accounts_per_channel=self.settings.max_accounts_per_channel
        )
        profile_text = channel_profile_label(profile_key)
        delay_min, delay_max = await self.db.get_delays(
            self.settings.default_reaction_delay_min_seconds,
            self.settings.default_reaction_delay_max_seconds,
        )
        membership = await self.db.membership_counts(channel.id)
        queue = await self.db.reaction_counts(channel.id)
        new_status = "🟢 ВКЛ" if channel.new_posts_enabled else "🔴 ВЫКЛ"
        old_status = "🟢 ВКЛ" if channel.old_posts_enabled else "🔴 ВЫКЛ"
        limit_text = (
            f"{channel.max_reactions_per_post} из {membership['joined']}"
            if channel.max_reactions_per_post
            else f"все доступные ({membership['joined']})"
        )
        if channel.promotion_mode == "permanent":
            period_text = "♾ Постоянный"
        elif promotion_is_active(channel):
            period_text = f"до {format_utc_datetime(channel.promotion_until)}"
        else:
            period_text = "🔴 Завершён"
        channel_window_text = format_duration_range(
            channel.reaction_window_min_seconds,
            channel.reaction_window_max_seconds,
        )
        await message.edit_text(
            "❤️ <b>Авто лайк</b>\n"
            f"<i>{html.escape(channel.title)}</i>\n\n"
            "━━━━━━━━━━━━━━\n"
            f"🆕 Новые посты: <b>{new_status}</b>\n"
            f"🕘 Старые посты: <b>{old_status}</b>\n"
            f"📚 Глубина старых: <b>{channel.old_posts_depth}</b>\n"
            f"🎚 Профиль: <b>{profile_text}</b>\n"
            f"😀 Реакции: {html.escape(reactions)} <i>({reaction_scope})</i>\n"
            f"🎯 Лимит на пост: <b>{limit_text}</b>\n"
            f"🖼 С изображением: <b>{channel.image_post_reaction_percent}%</b>  ·  "
            f"📝 Без изображения: <b>{channel.no_image_post_reaction_percent}%</b>\n"
            f"⏳ Период раскрутки: <b>{period_text}</b>\n"
            f"⏱ Период канала: <b>{channel_window_text}</b>\n"
            f"🛡 Пауза аккаунта: <b>{delay_min}-{delay_max} сек.</b>\n"
            "━━━━━━━━━━━━━━\n"
            f"👥 Подписаны: <b>{membership['joined']}</b>  ·  в очереди: {membership['pending']}\n"
            f"🧾 Реакции: ожидают <b>{queue['pending']}</b>  ·  готово {queue['done']}  ·  ошибок {queue['failed']}",
            reply_markup=autolike_actions(channel, profile_text=profile_text),
        )
        return True

    async def autolike_list(self, callback: CallbackQuery) -> None:
        if await self._deny_if_needed(callback):
            return
        channels = await self.db.list_channels(active_only=True)
        await callback.message.edit_text(
            "❤️ <b>Авто лайк</b>\n"
            "<i>Настройка реакций на новые и старые публикации</i>\n\n"
            "━━━━━━━━━━━━━━\n"
            f"Доступно каналов и групп: <b>{len(channels)}</b>\n"
            "━━━━━━━━━━━━━━\n\n"
            + ("Выберите канал или группу 👇" if channels else "Сначала добавьте канал или группу."),
            reply_markup=channel_list_keyboard(channels, prefix="autolike:view"),
        )
        await callback.answer()

    async def autolike_view(self, callback: CallbackQuery) -> None:
        if await self._deny_if_needed(callback):
            return
        channel_id = int(callback.data.rsplit(":", 1)[1])
        if not await self._render_autolike_view(callback.message, channel_id):
            await callback.answer("Канал не найден", show_alert=True)
            return
        await callback.answer()

    async def autolike_toggle_new(self, callback: CallbackQuery) -> None:
        if await self._deny_if_needed(callback):
            return
        channel_id = int(callback.data.rsplit(":", 1)[1])
        channel = await self.db.get_channel(channel_id)
        if not channel:
            await callback.answer("Канал не найден", show_alert=True)
            return
        new_value = not channel.new_posts_enabled
        if new_value and not promotion_is_active(channel):
            await self.db.close_expired_promotion(channel.id)
            await self._render_autolike_view(callback.message, channel.id)
            await callback.answer(
                "Период раскрутки завершён. Откройте «ТФ» и задайте новый срок или режим «Постоянный».",
                show_alert=True,
            )
            return
        if new_value:
            await self.jobs.sync_last_seen_to_latest(channel)
            cancelled = 0
        else:
            cancelled = await self.db.cancel_pending_reactions(channel.id, source="new")
        await self.db.set_channel_flag(channel.id, "new_posts_enabled", new_value)
        await self._render_autolike_view(callback.message, channel.id)
        if new_value:
            await callback.answer("Новые посты включены")
        else:
            await callback.answer(f"Новые посты выключены. Отменено новых заданий: {cancelled}")

    async def autolike_toggle_old(self, callback: CallbackQuery) -> None:
        if await self._deny_if_needed(callback):
            return
        channel_id = int(callback.data.rsplit(":", 1)[1])
        channel = await self.db.get_channel(channel_id)
        if not channel:
            await callback.answer("Канал не найден", show_alert=True)
            return
        new_value = not channel.old_posts_enabled
        if new_value:
            if not promotion_is_active(channel):
                await self.db.close_expired_promotion(channel.id)
                await self._render_autolike_view(callback.message, channel.id)
                await callback.answer(
                    "Период раскрутки завершён. Откройте «ТФ» и задайте новый срок или режим «Постоянный».",
                    show_alert=True,
                )
                return
            membership = await self.db.membership_counts(channel.id)
            if membership["joined"] <= 0:
                await self._render_autolike_view(callback.message, channel.id)
                await callback.answer(
                    "Аккаунты ещё не подписаны. Дождитесь завершения очереди подключения и включите старые посты снова.",
                    show_alert=True,
                )
                return
            await self.db.set_channel_flag(channel.id, "old_posts_enabled", True)
            try:
                fresh_channel = await self.db.get_channel(channel.id)
                created = await self.jobs.enqueue_old_posts(fresh_channel)
            except Exception as exc:  # noqa: BLE001
                await self.db.set_channel_flag(channel.id, "old_posts_enabled", False)
                logger.exception("Не удалось создать очередь старых постов target=%s", channel.id)
                await self._render_autolike_view(callback.message, channel.id)
                await callback.answer(f"Не удалось создать очередь: {truncate(str(exc), 120)}", show_alert=True)
                return
            await self._render_autolike_view(callback.message, channel.id)
            text = (
                f"Старые посты включены. Создано заданий: {created}"
                if created
                else "Старые посты включены. Новых заданий нет: они уже были созданы или постов нет."
            )
            await callback.answer(text, show_alert=True)
            return

        await self.db.set_channel_flag(channel.id, "old_posts_enabled", False)
        cancelled = await self.db.cancel_pending_reactions(channel.id, source="old")
        await self._render_autolike_view(callback.message, channel.id)
        await callback.answer(f"Старые посты выключены. Отменено старых заданий: {cancelled}", show_alert=True)

    async def autolike_depth_menu(self, callback: CallbackQuery) -> None:
        if await self._deny_if_needed(callback):
            return
        channel_id = int(callback.data.rsplit(":", 1)[1])
        await callback.message.edit_text(
            "📚 Сколько последних старых постов добавить в очередь?",
            reply_markup=depth_menu(channel_id, self.settings.max_old_posts),
        )
        await callback.answer()

    async def autolike_set_depth(self, callback: CallbackQuery) -> None:
        if await self._deny_if_needed(callback):
            return
        try:
            channel_id, depth = parse_autolike_depth_callback(callback.data)
        except (TypeError, ValueError):
            await callback.answer("Некорректная кнопка глубины", show_alert=True)
            return
        if depth <= 0 or depth > self.settings.max_old_posts:
            await callback.answer("Превышен лимит", show_alert=True)
            return
        await self.db.set_channel_depth(channel_id, depth)
        channel = await self.db.get_channel(channel_id)
        created = 0
        if channel and channel.old_posts_enabled:
            try:
                created = await self.jobs.enqueue_old_posts(channel)
            except Exception:  # noqa: BLE001
                logger.exception("Не удалось расширить очередь старых постов target=%s", channel_id)
        if not await self._render_autolike_view(callback.message, channel_id):
            await callback.answer("Канал не найден", show_alert=True)
            return
        suffix = f" · добавлено заданий: {created}" if created else ""
        await callback.answer(f"Глубина: {depth}{suffix}")

    async def _render_reaction_limit(self, message: Message, channel_id: int) -> bool:
        channel = await self.db.get_channel(channel_id)
        if not channel or not channel.is_active:
            return False
        membership = await self.db.membership_counts(channel.id)
        current = str(channel.max_reactions_per_post) if channel.max_reactions_per_post else "Все аккаунты"
        await message.edit_text(
            "🎯 <b>Лимит реакций на пост</b>\n"
            f"<i>{html.escape(channel.title)}</i>\n\n"
            "━━━━━━━━━━━━━━\n"
            f"👥 Подписано аккаунтов: <b>{membership['joined']}</b>\n"
            f"🎯 Текущий лимит: <b>{current}</b>\n"
            "━━━━━━━━━━━━━━\n\n"
            "Лимит относится к каждой публикации отдельно. Для каждого поста бот случайно выбирает "
            "нужное количество аккаунтов и случайные реакции из набора канала.\n\n"
            "Уже поставленные реакции не удаляются.",
            reply_markup=reaction_limit_keyboard(channel.id, self.settings.max_accounts_per_channel),
        )
        return True

    async def autolike_limit_view(self, callback: CallbackQuery, state: FSMContext) -> None:
        if await self._deny_if_needed(callback):
            return
        await state.clear()
        channel_id = int(callback.data.rsplit(":", 1)[1])
        if not await self._render_reaction_limit(callback.message, channel_id):
            await callback.answer("Канал не найден", show_alert=True)
            return
        await callback.answer()

    async def _show_limit_confirmation(
        self, message: Message, state: FSMContext, channel_id: int, value: int | None
    ) -> bool:
        channel = await self.db.get_channel(channel_id)
        if not channel or not channel.is_active:
            return False
        current = str(channel.max_reactions_per_post) if channel.max_reactions_per_post else "Все аккаунты"
        new_value = str(value) if value is not None else "Все аккаунты"
        await state.update_data(limit_channel_id=channel_id, pending_limit=value if value is not None else "all")
        await message.edit_text(
            "🎯 <b>Новый лимит реакций</b>\n\n"
            f"Канал: <b>{html.escape(channel.title)}</b>\n"
            f"Текущее значение: <b>{current}</b>\n"
            f"Новое значение: <b>{new_value}</b>\n\n"
            "Настройка применится к новым публикациям, старым публикациям при включённом режиме "
            "и к ожидающим заданиям текущей очереди.\n\n"
            "Уже поставленные реакции не удаляются.",
            reply_markup=reaction_limit_confirm_keyboard(channel.id),
        )
        return True

    async def autolike_limit_value(self, callback: CallbackQuery, state: FSMContext) -> None:
        if await self._deny_if_needed(callback):
            return
        parts = callback.data.split(":")
        if len(parts) != 4:
            await callback.answer("Некорректная кнопка", show_alert=True)
            return
        channel_id = int(parts[2])
        value = None if parts[3] == "all" else parse_reaction_limit_input(
            parts[3], self.settings.max_accounts_per_channel
        )
        if not await self._show_limit_confirmation(callback.message, state, channel_id, value):
            await callback.answer("Канал не найден", show_alert=True)
            return
        await callback.answer()

    async def autolike_limit_manual(self, callback: CallbackQuery, state: FSMContext) -> None:
        if await self._deny_if_needed(callback):
            return
        channel_id = int(callback.data.rsplit(":", 1)[1])
        channel = await self.db.get_channel(channel_id)
        if not channel or not channel.is_active:
            await callback.answer("Канал не найден", show_alert=True)
            return
        await state.set_state(SetPostReactionLimit.value)
        await state.update_data(limit_channel_id=channel.id)
        await callback.message.edit_text(
            "✏️ <b>Введите лимит реакций на один пост</b>\n\n"
            f"Допустимое значение: от 1 до {self.settings.max_accounts_per_channel}.\n"
            "Для режима без ограничения вернитесь назад и выберите «Все аккаунты».\n\n"
            "Отмена: /cancel",
            reply_markup=back_main(),
        )
        await callback.answer()

    async def autolike_limit_input(self, message: Message, state: FSMContext) -> None:
        if await self._deny_if_needed(message):
            return
        data = await state.get_data()
        channel_id = int(data.get("limit_channel_id", 0))
        try:
            value = parse_reaction_limit_input(message.text or "", self.settings.max_accounts_per_channel)
        except ValueError as exc:
            await message.answer(f"❌ {html.escape(str(exc))}")
            return
        channel = await self.db.get_channel(channel_id)
        if not channel or not channel.is_active:
            await state.clear()
            await message.answer("❌ Канал не найден.", reply_markup=main_menu())
            return
        current = str(channel.max_reactions_per_post) if channel.max_reactions_per_post else "Все аккаунты"
        await state.update_data(pending_limit=value)
        await message.answer(
            "🎯 <b>Новый лимит реакций</b>\n\n"
            f"Канал: <b>{html.escape(channel.title)}</b>\n"
            f"Текущее значение: <b>{current}</b>\n"
            f"Новое значение: <b>{value}</b>\n\n"
            "После применения лишние ожидающие задания будут отменены, а недостающие созданы. "
            "Уже поставленные реакции останутся.",
            reply_markup=reaction_limit_confirm_keyboard(channel.id),
        )

    async def autolike_limit_apply(self, callback: CallbackQuery, state: FSMContext) -> None:
        if await self._deny_if_needed(callback):
            return
        channel_id = int(callback.data.rsplit(":", 1)[1])
        data = await state.get_data()
        if int(data.get("limit_channel_id", 0)) != channel_id or "pending_limit" not in data:
            await callback.answer("Сначала выберите новое значение", show_alert=True)
            return
        raw = data["pending_limit"]
        limit = None if raw == "all" else int(raw)
        await self.db.set_channel_reaction_limit(channel_id, limit)
        channel = await self.db.get_channel(channel_id)
        try:
            result = await self.jobs.apply_reaction_limit(channel)
            note = (
                f"Отменено лишних заданий: {result['cancelled']} · создано недостающих: {result['created']}"
            )
        except Exception as exc:  # noqa: BLE001
            logger.exception("Не удалось полностью применить лимит target=%s", channel_id)
            note = f"Лимит сохранён, но очередь обновилась не полностью: {truncate(str(exc), 100)}"
        await state.clear()
        await self._render_autolike_view(callback.message, channel_id)
        await callback.answer(note, show_alert=True)

    async def autolike_limit_cancel(self, callback: CallbackQuery, state: FSMContext) -> None:
        if await self._deny_if_needed(callback):
            return
        channel_id = int(callback.data.rsplit(":", 1)[1])
        await state.clear()
        if not await self._render_reaction_limit(callback.message, channel_id):
            await callback.answer("Канал не найден", show_alert=True)
            return
        await callback.answer("Изменение отменено")

    async def _render_promotion_period(self, message: Message, channel_id: int) -> bool:
        channel = await self.db.get_channel(channel_id)
        if not channel or not channel.is_active:
            return False
        if channel.promotion_mode == "permanent":
            period = "♾ Постоянный"
            start = format_utc_datetime(channel.promotion_started_at) if channel.promotion_started_at else "без даты"
            finish = "пока администратор не выключит авто лайк"
        else:
            period = "🟢 Активен" if promotion_is_active(channel) else "🔴 Завершён"
            start = format_utc_datetime(channel.promotion_started_at)
            finish = format_utc_datetime(channel.promotion_until)
        await message.edit_text(
            "📅 <b>Таймфрейм раскрутки</b>\n"
            f"<i>{html.escape(channel.title)}</i>\n\n"
            "━━━━━━━━━━━━━━\n"
            f"⏳ Режим: <b>{period}</b>\n"
            f"📆 Начало: <b>{start}</b>\n"
            f"🏁 Завершение: <b>{finish}</b>\n"
            "━━━━━━━━━━━━━━\n\n"
            "Количество дней относится ко всему каналу. В течение этого срока все включённые "
            "новые и старые посты получают реакции согласно лимиту, набору и задержке.\n\n"
            "Команда «постоянный» включает работу без срока - пока администратор сам не выключит режимы постов.",
            reply_markup=promotion_period_keyboard(
                channel.id, permanent=channel.promotion_mode == "permanent"
            ),
        )
        return True

    async def autolike_period_view(self, callback: CallbackQuery, state: FSMContext) -> None:
        if await self._deny_if_needed(callback):
            return
        await state.clear()
        channel_id = int(callback.data.rsplit(":", 1)[1])
        if not await self._render_promotion_period(callback.message, channel_id):
            await callback.answer("Канал не найден", show_alert=True)
            return
        await callback.answer()

    async def autolike_period_edit(self, callback: CallbackQuery, state: FSMContext) -> None:
        if await self._deny_if_needed(callback):
            return
        channel_id = int(callback.data.rsplit(":", 1)[1])
        channel = await self.db.get_channel(channel_id)
        if not channel or not channel.is_active:
            await callback.answer("Канал не найден", show_alert=True)
            return
        await state.set_state(SetPromotionPeriod.value)
        await state.update_data(period_channel_id=channel.id)
        await callback.message.edit_text(
            "⏳ <b>Период раскрутки</b>\n\n"
            "Введите количество дней от 1 до 365.\n\n"
            "Для работы без срока отправьте команду: <code>постоянный</code>\n\n"
            "Примеры: <code>30</code> или <code>постоянный</code>\n"
            "Отмена: /cancel",
            reply_markup=back_main(),
        )
        await callback.answer()

    async def _show_period_confirmation(
        self, message: Message, state: FSMContext, channel_id: int, mode: str, days: int | None
    ) -> bool:
        channel = await self.db.get_channel(channel_id)
        if not channel or not channel.is_active:
            return False
        current = (
            "Постоянный"
            if channel.promotion_mode == "permanent"
            else f"до {format_utc_datetime(channel.promotion_until)}"
        )
        new_value = "Постоянный" if mode == "permanent" else f"{days} дней"
        await state.update_data(
            period_channel_id=channel.id, pending_period_mode=mode, pending_period_days=days
        )
        await message.edit_text(
            "📅 <b>Новый период раскрутки</b>\n\n"
            f"Канал: <b>{html.escape(channel.title)}</b>\n"
            f"Текущий режим: <b>{current}</b>\n"
            f"Новый режим: <b>{new_value}</b>\n\n"
            + (
                "Реакции будут работать без ограничения по сроку, пока администратор не выключит новые или старые посты."
                if mode == "permanent"
                else "Отсчёт начнётся сразу после применения. После завершения срока новые задания перестанут создаваться, а ожидающие будут отменены."
            ),
            reply_markup=promotion_period_confirm_keyboard(channel.id),
        )
        return True

    async def autolike_period_value(self, callback: CallbackQuery, state: FSMContext) -> None:
        if await self._deny_if_needed(callback):
            return
        parts = callback.data.split(":")
        if len(parts) != 4 or parts[3] != "permanent":
            await callback.answer("Некорректная кнопка", show_alert=True)
            return
        channel_id = int(parts[2])
        if not await self._show_period_confirmation(
            callback.message, state, channel_id, "permanent", None
        ):
            await callback.answer("Канал не найден", show_alert=True)
            return
        await callback.answer()

    async def autolike_period_input(self, message: Message, state: FSMContext) -> None:
        if await self._deny_if_needed(message):
            return
        data = await state.get_data()
        channel_id = int(data.get("period_channel_id", 0))
        try:
            mode, days = parse_promotion_period_input(message.text or "")
        except ValueError as exc:
            await message.answer(f"❌ {html.escape(str(exc))}")
            return
        channel = await self.db.get_channel(channel_id)
        if not channel or not channel.is_active:
            await state.clear()
            await message.answer("❌ Канал не найден.", reply_markup=main_menu())
            return
        current = (
            "Постоянный"
            if channel.promotion_mode == "permanent"
            else f"до {format_utc_datetime(channel.promotion_until)}"
        )
        new_value = "Постоянный" if mode == "permanent" else f"{days} дней"
        await state.update_data(pending_period_mode=mode, pending_period_days=days)
        await message.answer(
            "📅 <b>Новый период раскрутки</b>\n\n"
            f"Канал: <b>{html.escape(channel.title)}</b>\n"
            f"Текущий режим: <b>{current}</b>\n"
            f"Новый режим: <b>{new_value}</b>\n\n"
            + (
                "Реакции будут работать без срока, пока администратор сам не выключит режимы постов."
                if mode == "permanent"
                else "Отсчёт начнётся после применения. Период относится ко всему каналу, а не к одному посту."
            ),
            reply_markup=promotion_period_confirm_keyboard(channel.id),
        )

    async def autolike_period_apply(self, callback: CallbackQuery, state: FSMContext) -> None:
        if await self._deny_if_needed(callback):
            return
        channel_id = int(callback.data.rsplit(":", 1)[1])
        data = await state.get_data()
        if int(data.get("period_channel_id", 0)) != channel_id or "pending_period_mode" not in data:
            await callback.answer("Сначала задайте новый период", show_alert=True)
            return
        mode = str(data["pending_period_mode"])
        days_raw = data.get("pending_period_days")
        days = None if mode == "permanent" else int(days_raw)
        channel = await self.db.set_channel_promotion_period(channel_id, days=days, now=utcnow())
        try:
            result = await self.jobs.apply_promotion_period(channel)
            note = (
                f"Период применён · отменено за пределами срока: {result['cancelled']} · создано: {result['created']}"
            )
        except Exception as exc:  # noqa: BLE001
            logger.exception("Не удалось полностью применить период target=%s", channel_id)
            note = f"Период сохранён, но очередь обновилась не полностью: {truncate(str(exc), 100)}"
        await state.clear()
        await self._render_autolike_view(callback.message, channel_id)
        await callback.answer(note, show_alert=True)

    async def autolike_period_cancel(self, callback: CallbackQuery, state: FSMContext) -> None:
        if await self._deny_if_needed(callback):
            return
        channel_id = int(callback.data.rsplit(":", 1)[1])
        await state.clear()
        if not await self._render_promotion_period(callback.message, channel_id):
            await callback.answer("Канал не найден", show_alert=True)
            return
        await callback.answer("Изменение отменено")

    async def settings_menu(self, callback: CallbackQuery) -> None:
        if await self._deny_if_needed(callback):
            return
        reactions = format_reaction_weights(await self.db.get_reaction_weights())
        reaction_min, reaction_max = await self.db.get_delays(
            self.settings.default_reaction_delay_min_seconds,
            self.settings.default_reaction_delay_max_seconds,
        )
        membership_min, membership_max = await self.db.get_membership_delays(
            self.settings.join_delay_min_seconds,
            self.settings.join_delay_max_seconds,
        )
        await callback.message.edit_text(
            "⚙️ <b>Настройки LikeBot</b>\n"
            "<i>Основные параметры автоматизации</i>\n\n"
            "━━━━━━━━━━━━━━\n"
            f"😀 Реакции по умолчанию: {html.escape(reactions)}\n"
            f"❤️ Задержка реакций: <b>{reaction_min}-{reaction_max} сек.</b>\n"
            f"📢 Задержка подписки: <b>{membership_min}-{membership_max} сек.</b>\n"
            f"📚 Максимум старых постов: <b>{self.settings.max_old_posts}</b>\n"
            f"🔄 Интервал мониторинга: <b>{self.settings.monitor_interval_seconds} сек.</b>\n"
            "━━━━━━━━━━━━━━\n\n"
            "Выберите настройку 👇",
            reply_markup=settings_overview_keyboard(),
        )
        await callback.answer()

    async def reactions_menu(self, callback: CallbackQuery, state: FSMContext) -> None:
        if await self._deny_if_needed(callback):
            return
        await state.clear()
        current = await self.db.get_reaction_weights()
        await callback.message.edit_text(
            "😀 <b>Реакции по умолчанию</b>\n"
            "<i>Базовое соотношение для каналов и групп без своих настроек</i>\n\n"
            "━━━━━━━━━━━━━━\n"
            f"Текущее соотношение: <b>{html.escape(format_reaction_weights(current))}</b>\n"
            "Итого: <b>100%</b>\n"
            "━━━━━━━━━━━━━━\n\n"
            "Для каждого задания бот случайно выбирает одну реакцию с учётом её веса. "
            "Индивидуальные настройки каналов и групп не перезаписываются.",
            reply_markup=reactions_overview_keyboard(),
        )
        await callback.answer()

    async def reactions_edit(self, callback: CallbackQuery, state: FSMContext) -> None:
        if await self._deny_if_needed(callback):
            return
        current = await self.db.get_reaction_weights()
        await state.set_state(SetReactions.reactions)
        await callback.message.edit_text(
            "✏️ <b>Реакции по умолчанию</b>\n\n"
            f"Сейчас: {html.escape(format_reaction_weights(current))}\n\n"
            "Укажите эмодзи и любые числовые веса. Бот сам пересчитает их в 100%.\n"
            "Пример: <code>👍60 ❤️20 🔥20</code>\n"
            "Или: <code>👍 6 ❤️ 2 🔥 2</code>\n"
            "Без чисел реакции будут равновероятными: <code>👍 ❤️ 🔥</code>\n\n"
            "Отмена: /cancel",
            reply_markup=back_main(),
        )
        await callback.answer()

    async def reactions_input(self, message: Message, state: FSMContext) -> None:
        if await self._deny_if_needed(message):
            return
        try:
            weights = parse_weighted_reactions(message.text or "")
        except ValueError as exc:
            await message.answer(f"❌ {html.escape(str(exc))}")
            return
        await self.db.set_reaction_weights(weights)
        updated_pending = await self.db.refresh_pending_default_reactions(weights)
        await state.clear()
        reaction_min, reaction_max = await self.db.get_delays(
            self.settings.default_reaction_delay_min_seconds,
            self.settings.default_reaction_delay_max_seconds,
        )
        await message.answer(
            "✅ <b>Реакции по умолчанию сохранены</b>\n\n"
            "━━━━━━━━━━━━━━\n"
            f"😀 Соотношение: <b>{html.escape(format_reaction_weights(weights))}</b>\n"
            "Итого: <b>100%</b>\n"
            f"❤️ Задержка реакций: {reaction_min}-{reaction_max} сек.\n"
            "━━━━━━━━━━━━━━\n"
            f"Обновлено ожидающих заданий: <b>{updated_pending}</b>\n\n"
            "Каналы и группы с собственными настройками не изменены.",
            reply_markup=settings_overview_keyboard(),
        )

    async def delay_menu(self, callback: CallbackQuery, state: FSMContext) -> None:
        if await self._deny_if_needed(callback):
            return
        await state.clear()
        reaction_min, reaction_max = await self.db.get_delays(
            self.settings.default_reaction_delay_min_seconds,
            self.settings.default_reaction_delay_max_seconds,
        )
        membership_min, membership_max = await self.db.get_membership_delays(
            self.settings.join_delay_min_seconds,
            self.settings.join_delay_max_seconds,
        )
        await callback.message.edit_text(
            "⏱ <b>Задержки</b>\n"
            "<i>Отдельные диапазоны для реакций и подписок</i>\n\n"
            "━━━━━━━━━━━━━━\n"
            f"❤️ Реакции: <b>{reaction_min}-{reaction_max} сек.</b>\n"
            f"📢 Подписка/отписка: <b>{membership_min}-{membership_max} сек.</b>\n"
            "━━━━━━━━━━━━━━\n\n"
            "Выберите, какой диапазон изменить 👇",
            reply_markup=delay_overview_keyboard(),
        )
        await callback.answer()

    async def reaction_delay_view(self, callback: CallbackQuery, state: FSMContext) -> None:
        if await self._deny_if_needed(callback):
            return
        await state.clear()
        minimum, maximum = await self.db.get_delays(
            self.settings.default_reaction_delay_min_seconds,
            self.settings.default_reaction_delay_max_seconds,
        )
        await callback.message.edit_text(
            "❤️ <b>Задержка реакций</b>\n"
            "<i>Время от обнаружения публикации до реакции аккаунта</i>\n\n"
            "━━━━━━━━━━━━━━\n"
            f"Минимум: <b>{minimum} сек.</b>\n"
            f"Максимум: <b>{maximum} сек.</b>\n"
            f"Диапазон: <b>{minimum}-{maximum} сек.</b>\n"
            "━━━━━━━━━━━━━━\n\n"
            "Для каждого задания выбирается время внутри этого диапазона.",
            reply_markup=reaction_delay_keyboard(),
        )
        await callback.answer()

    async def delay_edit(self, callback: CallbackQuery, state: FSMContext) -> None:
        if await self._deny_if_needed(callback):
            return
        minimum, maximum = await self.db.get_delays(
            self.settings.default_reaction_delay_min_seconds,
            self.settings.default_reaction_delay_max_seconds,
        )
        await state.set_state(SetDelay.minimum)
        await state.update_data(old_minimum=minimum, old_maximum=maximum)
        await callback.message.edit_text(
            "✏️ <b>Задержка реакций</b>\n\n"
            f"Текущий диапазон: <b>{minimum}-{maximum} сек.</b>\n\n"
            "Отправьте минимальную задержку в секундах.\n"
            "Пример: <code>60</code>\n\n"
            "Отмена: /cancel",
            reply_markup=back_main(),
        )
        await callback.answer()

    async def delay_minimum(self, message: Message, state: FSMContext) -> None:
        if await self._deny_if_needed(message):
            return
        try:
            minimum = int((message.text or "").strip())
        except ValueError:
            await message.answer("Нужно целое число секунд.")
            return
        if not 1 <= minimum <= 86400:
            await message.answer("Допустимый диапазон: 1-86400 секунд.")
            return
        await state.update_data(minimum=minimum)
        await state.set_state(SetDelay.maximum)
        await message.answer("Теперь отправьте максимальную задержку реакций. Пример: <code>1800</code>")

    async def delay_maximum(self, message: Message, state: FSMContext) -> None:
        if await self._deny_if_needed(message):
            return
        data = await state.get_data()
        minimum = int(data.get("minimum", 0))
        try:
            maximum = int((message.text or "").strip())
        except ValueError:
            await message.answer("Нужно целое число секунд.")
            return
        if maximum < minimum or maximum > 86400:
            await message.answer(f"Максимум должен быть не меньше {minimum} и не больше 86400.")
            return
        old_minimum = int(data.get("old_minimum", minimum))
        old_maximum = int(data.get("old_maximum", maximum))
        await self.db.set_delays(minimum, maximum)
        logger.info(
            "Reaction delay updated old=%s-%s new=%s-%s",
            old_minimum,
            old_maximum,
            minimum,
            maximum,
        )
        await state.clear()
        await message.answer(
            "✅ <b>Задержка реакций сохранена</b>\n\n"
            f"❤️ Новый диапазон: <b>{minimum}-{maximum} сек.</b>\n\n"
            "Применить новый диапазон к уже ожидающим реакциям?\n"
            "Повторные попытки после FloodWait и ошибок не будут переноситься.",
            reply_markup=reaction_delay_reschedule_keyboard(),
        )

    async def reaction_delay_reschedule(self, callback: CallbackQuery) -> None:
        if await self._deny_if_needed(callback):
            return
        minimum, maximum = await self.db.get_delays(
            self.settings.default_reaction_delay_min_seconds,
            self.settings.default_reaction_delay_max_seconds,
        )
        result = await self.db.reschedule_pending_reaction_jobs(minimum, maximum)
        logger.info(
            "Pending reaction jobs rescheduled count=%s skipped_retries=%s delay=%s-%s",
            result["rescheduled"],
            result["skipped_retries"],
            minimum,
            maximum,
        )
        await callback.message.edit_text(
            "✅ <b>Текущая очередь реакций пересчитана</b>\n\n"
            f"Диапазон: <b>{minimum}-{maximum} сек.</b>\n"
            f"Перенесено заданий: <b>{result['rescheduled']}</b>\n"
            f"Защитных повторов не изменено: <b>{result['skipped_retries']}</b>\n\n"
            "Каждая ожидающая реакция получила новую случайную задержку от текущего момента.",
            reply_markup=delay_overview_keyboard(),
        )
        await callback.answer("Очередь реакций пересчитана")

    async def reaction_delay_keep_existing(self, callback: CallbackQuery) -> None:
        if await self._deny_if_needed(callback):
            return
        minimum, maximum = await self.db.get_delays(
            self.settings.default_reaction_delay_min_seconds,
            self.settings.default_reaction_delay_max_seconds,
        )
        await callback.message.edit_text(
            "✅ <b>Новая задержка сохранена</b>\n\n"
            f"Диапазон <b>{minimum}-{maximum} сек.</b> применяется только к новым реакциям.\n"
            "Текущая очередь сохранила ранее рассчитанное время.",
            reply_markup=delay_overview_keyboard(),
        )
        await callback.answer()

    async def membership_delay_view(self, callback: CallbackQuery, state: FSMContext) -> None:
        if await self._deny_if_needed(callback):
            return
        await state.clear()
        minimum, maximum = await self.db.get_membership_delays(
            self.settings.join_delay_min_seconds,
            self.settings.join_delay_max_seconds,
        )
        await callback.message.edit_text(
            "📢 <b>Задержка подписки</b>\n"
            "<i>Интервал между аккаунтами при подписке и отписке</i>\n\n"
            "━━━━━━━━━━━━━━\n"
            f"Минимум: <b>{minimum} сек.</b>\n"
            f"Максимум: <b>{maximum} сек.</b>\n"
            f"Диапазон: <b>{minimum}-{maximum} сек.</b>\n"
            "━━━━━━━━━━━━━━\n\n"
            "Первый аккаунт обрабатывается сразу. Для каждого следующего аккаунта "
            "добавляется случайный интервал из этого диапазона.",
            reply_markup=membership_delay_keyboard(),
        )
        await callback.answer()

    async def membership_delay_edit(self, callback: CallbackQuery, state: FSMContext) -> None:
        if await self._deny_if_needed(callback):
            return
        minimum, maximum = await self.db.get_membership_delays(
            self.settings.join_delay_min_seconds,
            self.settings.join_delay_max_seconds,
        )
        await state.set_state(SetMembershipDelay.minimum)
        await state.update_data(old_minimum=minimum, old_maximum=maximum)
        await callback.message.edit_text(
            "✏️ <b>Задержка подписки</b>\n\n"
            f"Текущий диапазон: <b>{minimum}-{maximum} сек.</b>\n\n"
            "Отправьте минимальный интервал между аккаунтами в секундах.\n"
            "Пример: <code>300</code>\n\n"
            "Отмена: /cancel",
            reply_markup=back_main(),
        )
        await callback.answer()

    async def membership_delay_minimum(self, message: Message, state: FSMContext) -> None:
        if await self._deny_if_needed(message):
            return
        try:
            minimum = int((message.text or "").strip())
        except ValueError:
            await message.answer("Нужно целое число секунд.")
            return
        if not 1 <= minimum <= 86400:
            await message.answer("Допустимый диапазон: 1-86400 секунд.")
            return
        await state.update_data(minimum=minimum)
        await state.set_state(SetMembershipDelay.maximum)
        await message.answer("Теперь отправьте максимальный интервал подписки. Пример: <code>1800</code>")

    async def membership_delay_maximum(self, message: Message, state: FSMContext) -> None:
        if await self._deny_if_needed(message):
            return
        data = await state.get_data()
        minimum = int(data.get("minimum", 0))
        try:
            maximum = int((message.text or "").strip())
        except ValueError:
            await message.answer("Нужно целое число секунд.")
            return
        if maximum < minimum or maximum > 86400:
            await message.answer(f"Максимум должен быть не меньше {minimum} и не больше 86400.")
            return
        old_minimum = int(data.get("old_minimum", minimum))
        old_maximum = int(data.get("old_maximum", maximum))
        await self.db.set_membership_delays(minimum, maximum)
        logger.info(
            "Membership delay updated old=%s-%s new=%s-%s",
            old_minimum,
            old_maximum,
            minimum,
            maximum,
        )
        await state.clear()
        await message.answer(
            "✅ <b>Задержка подписки сохранена</b>\n\n"
            f"📢 Новый диапазон: <b>{minimum}-{maximum} сек.</b>\n\n"
            "Применить новый диапазон к уже ожидающим подпискам и отпискам?\n"
            "Повторные попытки после FloodWait и ошибок не будут переноситься.",
            reply_markup=membership_delay_reschedule_keyboard(),
        )

    async def membership_delay_reschedule(self, callback: CallbackQuery) -> None:
        if await self._deny_if_needed(callback):
            return
        minimum, maximum = await self.db.get_membership_delays(
            self.settings.join_delay_min_seconds,
            self.settings.join_delay_max_seconds,
        )
        result = await self.db.reschedule_pending_membership_jobs(minimum, maximum)
        logger.info(
            "Pending membership jobs rescheduled count=%s skipped_retries=%s delay=%s-%s",
            result["rescheduled"],
            result["skipped_retries"],
            minimum,
            maximum,
        )
        await callback.message.edit_text(
            "✅ <b>Текущая очередь подписок пересчитана</b>\n\n"
            f"Диапазон: <b>{minimum}-{maximum} сек.</b>\n"
            f"Перенесено заданий: <b>{result['rescheduled']}</b>\n"
            f"Защитных повторов не изменено: <b>{result['skipped_retries']}</b>\n\n"
            "Первое ожидающее задание запускается сразу. Каждое следующее получает "
            "накопительный случайный интервал из выбранного диапазона.",
            reply_markup=delay_overview_keyboard(),
        )
        await callback.answer("Очередь подписок пересчитана")

    async def membership_delay_keep_existing(self, callback: CallbackQuery) -> None:
        if await self._deny_if_needed(callback):
            return
        minimum, maximum = await self.db.get_membership_delays(
            self.settings.join_delay_min_seconds,
            self.settings.join_delay_max_seconds,
        )
        await callback.message.edit_text(
            "✅ <b>Новая задержка сохранена</b>\n\n"
            f"Диапазон <b>{minimum}-{maximum} сек.</b> применяется только к новым подпискам и отпискам.\n"
            "Текущая очередь сохранила ранее рассчитанное время.",
            reply_markup=delay_overview_keyboard(),
        )
        await callback.answer()
