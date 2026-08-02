from __future__ import annotations

from .handler_shared import *  # noqa: F403


class TargetManagementHandlersMixin:
    async def _render_channel_list(self, message: Message) -> None:
        channels = await self.db.list_channels(kind="channel")
        active = sum(1 for channel in channels if channel.is_active)
        text = (
            "📢 <b>Каналы</b>\n"
            "<i>Подписки и настройки авто лайка</i>\n\n"
            "━━━━━━━━━━━━━━\n"
            f"🟢 Активны: <b>{active}</b>\n"
            f"📦 Всего: <b>{len(channels)}</b>\n"
            "━━━━━━━━━━━━━━\n\n"
            + ("Выберите канал 👇" if channels else "Каналов пока нет. Добавьте первый канал 👇")
        )
        await message.edit_text(text, reply_markup=channel_list_keyboard(channels))

    async def channel_add(self, callback: CallbackQuery, state: FSMContext) -> None:
        if await self._deny_if_needed(callback):
            return
        accounts = await self.db.list_accounts(active_only=True)
        if not accounts:
            await callback.answer("Сначала добавьте хотя бы один аккаунт", show_alert=True)
            return
        await state.set_state(AddChannel.link)
        await state.update_data(expected_kind="channel")
        await callback.message.edit_text(
            "📢 <b>Добавление канала</b>\n\n"
            "Отправьте:\n"
            "• <code>@username</code>\n"
            "• публичную ссылку <code>https://t.me/username</code>\n"
            "• приватную пригласительную ссылку\n\n"
            "После проверки создаётся очередь подключения активных аккаунтов.\n"
            "Отмена: /cancel",
            reply_markup=back_main(),
        )
        await callback.answer()

    async def group_add(self, callback: CallbackQuery, state: FSMContext) -> None:
        if await self._deny_if_needed(callback):
            return
        accounts = await self.db.list_accounts(active_only=True)
        if not accounts:
            await callback.answer("Сначала добавьте хотя бы один аккаунт", show_alert=True)
            return
        await state.set_state(AddChannel.link)
        await state.update_data(expected_kind="group")
        await callback.message.edit_text(
            "👥 <b>Добавление группы</b>\n\n"
            "Отправьте публичную или приватную ссылку на Telegram-группу.\n"
            "После проверки создаётся очередь подключения аккаунтов.\n\n"
            "Отмена: /cancel",
            reply_markup=back_main(),
        )
        await callback.answer()

    async def channel_link(self, message: Message, state: FSMContext) -> None:
        if await self._deny_if_needed(message):
            return
        try:
            parsed = parse_channel_link(message.text or "")
        except ValueError as exc:
            await message.answer(f"❌ {html.escape(str(exc))}")
            return
        state_data = await state.get_data()
        expected_kind = state_data.get("expected_kind", "channel")
        accounts = await self.db.list_accounts(active_only=True)
        if not accounts:
            await state.clear()
            await message.answer("Нет активных аккаунтов.", reply_markup=main_menu())
            return
        target_data = None
        try:
            for probe in accounts:
                try:
                    async with self.pool.lock_for(probe.id):
                        try:
                            client = await self.pool.get(probe)
                            joined_for_probe = False
                            if parsed.kind == "private":
                                join_result = await self.pool.join_channel(client, parsed)
                                entity = join_result.entity
                                joined_for_probe = join_result.joined_now
                            else:
                                entity = await client.get_entity(parsed.value)
    
                            if not hasattr(entity, "broadcast") and not hasattr(
                                entity, "megagroup"
                            ):
                                raise ValueError("Ссылка ведёт не на канал или супергруппу")
                            actual_kind = (
                                "group"
                                if bool(getattr(entity, "megagroup", False))
                                else "channel"
                            )
                            if actual_kind != expected_kind:
                                if joined_for_probe:
                                    try:
                                        await client(
                                            functions.channels.LeaveChannelRequest(
                                                channel=entity
                                            )
                                        )
                                    except Exception:  # noqa: BLE001
                                        logger.exception(
                                            "Не удалось отменить пробное вступление"
                                        )
                                expected_label = (
                                    "группу" if expected_kind == "group" else "канал"
                                )
                                actual_label = (
                                    "группу" if actual_kind == "group" else "канал"
                                )
                                raise ValueError(
                                    f"Вы добавляете {expected_label}, "
                                    f"но ссылка ведёт на {actual_label}"
                                )
    
                            latest_messages = await client.get_messages(entity, limit=1)
                            last_seen = latest_messages[0].id if latest_messages else 0
                            target_data = {
                                "telegram_channel_id": int(entity.id),
                                "kind": actual_kind,
                                "title": getattr(entity, "title", None) or parsed.value,
                                "username": getattr(entity, "username", None),
                                "last_seen_message_id": last_seen,
                            }
                        except ACCOUNT_AUTH_FAILURES:
                            await self.pool.remove_unauthorized_account_while_locked(
                                probe.id, context="add-target-probe"
                            )
                            raise
                    break
                except ACCOUNT_AUTH_FAILURES:
                    continue
            if target_data is None:
                raise RuntimeError(
                    "Нет авторизованных аккаунтов для проверки ссылки. "
                    "Недействительные сессии перенесены в проблемные."
                )
            channel, created = await self.db.add_or_update_channel(
                telegram_channel_id=target_data["telegram_channel_id"],
                kind=target_data["kind"],
                title=target_data["title"],
                username=target_data["username"],
                link=parsed.canonical,
                invite_hash=parsed.invite_hash,
                last_seen_message_id=target_data["last_seen_message_id"],
            )
            scheduled = await self.jobs.schedule_channel_joins(channel)
        except ACCOUNT_AUTH_FAILURES:
            if "probe" in locals():
                await self.pool.remove_unauthorized_account(
                    probe.id, context="add-target-finalize"
                )
            await message.answer(
                "❌ Сессия проверочного аккаунта потеряла авторизацию и перенесена в проблемные. "
                "Отправьте ссылку ещё раз."
            )
            return
        except Exception as exc:  # noqa: BLE001
            logger.exception("Не удалось добавить канал или группу")
            await message.answer(f"❌ Не удалось добавить: <code>{html.escape(str(exc))}</code>")
            return
        await state.clear()
        label = "Группа" if channel.kind == "group" else "Канал"
        if channel.kind == "group":
            reply_markup = group_actions(channel.id)
        else:
            connect_summary, _available = await self.db.channel_connect_state(
                channel.id, max_accounts=self.settings.max_accounts_per_channel
            )
            reply_markup = channel_actions(
                channel.id,
                connectable_count=connect_summary["connectable"],
                pending_count=connect_summary["pending"],
            )
        await message.answer(
            f"✅ <b>{label} сохранён</b>\n\n"
            f"Название: {html.escape(channel.title)}\n"
            f"Username: @{html.escape(channel.username) if channel.username else '-'}\n"
            f"Очередь подключения: {scheduled}\n"
            f"Режим: {'новая запись' if created else 'обновлено'}\n\n"
            "Авто лайк по умолчанию выключен.",
            reply_markup=reply_markup,
        )

    async def _target_management_payload(
        self,
        *,
        user_id: int,
        kind: str,
        filter_key: str,
        page: int,
    ) -> tuple[str, object]:
        if kind not in {"channel", "group"} or filter_key not in TARGET_FILTERS:
            raise ValueError("Недопустимый фильтр целей")
        query = self._target_management_queries.get((user_id, kind))
        targets = await self.db.list_channels(kind=kind)
        filtered = [
            target
            for target in targets
            if target_matches(
                target, kind=kind, filter_key=filter_key, query=query
            )
        ]
        page_targets, safe_page, total_pages = _paginate(filtered, page)
        rows = [
            (
                target.id,
                truncate(target.title, 44),
                target.is_active,
                bool(target.last_error),
            )
            for target in page_targets
        ]
        kind_code = "c" if kind == "channel" else "g"
        title = "каналами" if kind == "channel" else "группами"
        active = sum(1 for target in targets if target.is_active)
        disabled = len(targets) - active
        errors = sum(1 for target in targets if target.last_error)
        query_line = (
            f"\n🔎 Поиск: <code>{html.escape(query)}</code>" if query else ""
        )
        page_line = (
            f"\n📄 Страница: <b>{safe_page + 1}/{total_pages}</b>"
            if len(filtered) > ACCOUNT_PAGE_SIZE
            else ""
        )
        text = (
            f"🔎 <b>Управление {title}</b>\n"
            f"Фильтр: <b>{TARGET_FILTERS[filter_key]}</b>"
            f"{query_line}\n\n"
            "━━━━━━━━━━━━━━\n"
            f"🟢 Активны: <b>{active}</b>  ·  ⚫️ выключены: <b>{disabled}</b>\n"
            f"⚠️ С ошибками: <b>{errors}</b>\n"
            f"📋 Найдено: <b>{len(filtered)}</b> из {len(targets)}"
            f"{page_line}\n"
            "━━━━━━━━━━━━━━\n\n"
            + ("Выберите цель 👇" if filtered else "По этому фильтру ничего не найдено.")
        )
        keyboard = target_management_keyboard(
            rows,
            kind_code=kind_code,
            filter_key=filter_key,
            query_active=bool(query),
            page=safe_page,
            total_pages=total_pages,
        )
        return text, keyboard

    async def target_management_view(
        self, callback: CallbackQuery, state: FSMContext
    ) -> None:
        if await self._deny_if_needed(callback):
            return
        try:
            kind, filter_key, page = parse_target_management_callback(callback.data)
        except ValueError as exc:
            await callback.answer(str(exc), show_alert=True)
            return
        await state.set_state(None)
        text, keyboard = await self._target_management_payload(
            user_id=callback.from_user.id,
            kind=kind,
            filter_key=filter_key,
            page=page,
        )
        await self._safe_edit_text(callback.message, text, reply_markup=keyboard)
        await callback.answer()

    async def target_management_search(
        self, callback: CallbackQuery, state: FSMContext
    ) -> None:
        if await self._deny_if_needed(callback):
            return
        parts = (callback.data or "").split(":")
        if (
            len(parts) != 4
            or parts[:2] != ["manage", "ts"]
            or parts[2] not in {"c", "g"}
            or parts[3] not in TARGET_FILTERS
        ):
            await callback.answer("Некорректная кнопка поиска", show_alert=True)
            return
        kind = "channel" if parts[2] == "c" else "group"
        await state.set_state(ManagementSearch.targets)
        await state.update_data(
            management_target_kind=kind,
            management_target_filter=parts[3],
        )
        await callback.message.edit_text(
            "🔎 <b>Поиск канала или группы</b>\n\n"
            "Отправьте название, @username или Telegram ID. Приватная ссылка и invite-hash "
            "не помещаются в поисковое состояние.\n\n"
            "Отмена: /cancel",
            reply_markup=back_main(),
        )
        await callback.answer()

    async def target_management_search_input(
        self, message: Message, state: FSMContext
    ) -> None:
        if await self._deny_if_needed(message):
            return
        data = await state.get_data()
        kind = str(data.get("management_target_kind") or "channel")
        filter_key = str(data.get("management_target_filter") or "all")
        if kind not in {"channel", "group"}:
            kind = "channel"
        if filter_key not in TARGET_FILTERS:
            filter_key = "all"
        try:
            query = normalize_management_query(message.text)
        except ValueError as exc:
            await message.answer(f"❌ {html.escape(str(exc))}")
            return
        self._target_management_queries[(message.from_user.id, kind)] = query
        await state.clear()
        text, keyboard = await self._target_management_payload(
            user_id=message.from_user.id,
            kind=kind,
            filter_key=filter_key,
            page=0,
        )
        await message.answer(text, reply_markup=keyboard)

    async def target_management_clear_search(
        self, callback: CallbackQuery, state: FSMContext
    ) -> None:
        if await self._deny_if_needed(callback):
            return
        parts = (callback.data or "").split(":")
        if (
            len(parts) != 4
            or parts[:2] != ["manage", "tc"]
            or parts[2] not in {"c", "g"}
            or parts[3] not in TARGET_FILTERS
        ):
            await callback.answer("Некорректная кнопка", show_alert=True)
            return
        kind = "channel" if parts[2] == "c" else "group"
        self._target_management_queries.pop((callback.from_user.id, kind), None)
        await state.set_state(None)
        text, keyboard = await self._target_management_payload(
            user_id=callback.from_user.id,
            kind=kind,
            filter_key=parts[3],
            page=0,
        )
        await self._safe_edit_text(callback.message, text, reply_markup=keyboard)
        await callback.answer("Поиск сброшен")

    async def channel_copy_list(self, callback: CallbackQuery) -> None:
        if await self._deny_if_needed(callback):
            return
        try:
            source_id, page = parse_copy_channel_callback(callback.data, action="list")
        except ValueError as exc:
            await callback.answer(str(exc), show_alert=True)
            return
        source = await self.db.get_channel(source_id)
        if source is None or source.kind != "channel":
            await callback.answer("Исходный канал не найден", show_alert=True)
            return
        candidates = [
            channel
            for channel in await self.db.list_channels(kind="channel")
            if channel.id != source.id
        ]
        page_items, safe_page, total_pages = _paginate(candidates, page)
        rows = [(item.id, truncate(item.title, 44)) for item in page_items]
        await callback.message.edit_text(
            "📋 <b>Копирование настроек канала</b>\n\n"
            f"Источник: <b>{html.escape(source.title)}</b>\n\n"
            "Выберите целевой канал. Будут скопированы реакции, режим новых/старых "
            "публикаций, глубина, лимит, проценты типов постов и период распределения.\n\n"
            "Не копируются ссылка, участники, очереди, история, ошибки, last_seen и временный "
            "срок раскрутки.",
            reply_markup=channel_copy_targets_keyboard(
                source.id, rows, page=safe_page, total_pages=total_pages
            ),
        )
        await callback.answer()

    async def channel_copy_confirm(self, callback: CallbackQuery) -> None:
        if await self._deny_if_needed(callback):
            return
        try:
            source_id, target_id = parse_copy_channel_callback(
                callback.data, action="confirm"
            )
        except ValueError as exc:
            await callback.answer(str(exc), show_alert=True)
            return
        source = await self.db.get_channel(source_id)
        target = await self.db.get_channel(target_id)
        if (
            source is None
            or target is None
            or source.kind != "channel"
            or target.kind != "channel"
        ):
            await callback.answer("Исходный или целевой канал не найден", show_alert=True)
            return
        await callback.message.edit_text(
            "⚠️ <b>Подтвердите копирование</b>\n\n"
            f"Источник: <b>{html.escape(source.title)}</b>\n"
            f"Получатель: <b>{html.escape(target.title)}</b>\n\n"
            "Текущие настройки получателя будут заменены. Его ссылка, история, подписки "
            "и временный срок раскрутки сохранятся.",
            reply_markup=channel_copy_confirm_keyboard(source.id, target.id),
        )
        await callback.answer()

    async def channel_copy_apply(self, callback: CallbackQuery) -> None:
        if await self._deny_if_needed(callback):
            return
        try:
            source_id, target_id = parse_copy_channel_callback(
                callback.data, action="apply"
            )
        except ValueError as exc:
            await callback.answer(str(exc), show_alert=True)
            return
        if self._channel_copy_lock.locked():
            await callback.answer("Копирование уже выполняется", show_alert=True)
            return
        await callback.answer("Копирую настройки…")
        queue_errors: list[str] = []
        details = {"reactions": 0, "rescheduled": 0, "cancelled": 0, "created": 0}
        async with self._channel_copy_lock:
            try:
                target = await self.db.copy_channel_settings(source_id, target_id)
            except ValueError as exc:
                await callback.message.edit_text(
                    f"❌ {html.escape(str(exc))}", reply_markup=back_main()
                )
                return
            except Exception:  # noqa: BLE001
                logger.exception(
                    "Копирование настроек канала завершилось ошибкой source=%s target=%s",
                    source_id,
                    target_id,
                )
                await callback.message.edit_text(
                    "❌ <b>Настройки не скопированы</b>\n\n"
                    "Подробности безопасно записаны в журнал приложения.",
                    reply_markup=back_main(),
                )
                return
            try:
                weights = await self.db.get_reaction_weights_for_channel(target)
                details["reactions"] = await self.db.refresh_pending_reactions(
                    target.id, weights
                )
            except Exception:  # noqa: BLE001
                logger.exception("Не удалось обновить реакции после копирования target=%s", target.id)
                queue_errors.append("реакции")
            try:
                result = await self.jobs.apply_reaction_limit(target)
                details["cancelled"] += int(result.get("cancelled", 0))
                details["created"] += int(result.get("created", 0))
            except Exception:  # noqa: BLE001
                logger.exception("Не удалось применить лимиты после копирования target=%s", target.id)
                queue_errors.append("лимиты")
            try:
                if not target.new_posts_enabled:
                    details["cancelled"] += await self.db.cancel_pending_reactions(
                        target.id, source="new"
                    )
                if not target.old_posts_enabled:
                    details["cancelled"] += await self.db.cancel_pending_reactions(
                        target.id, source="old"
                    )
                elif target.is_active and promotion_is_active(target):
                    details["created"] += await self.jobs.enqueue_old_posts(target)
            except Exception:  # noqa: BLE001
                logger.exception("Не удалось синхронизировать режим постов target=%s", target.id)
                queue_errors.append("режим постов")
            try:
                delay_min, delay_max = await self.db.get_delays(
                    self.settings.default_reaction_delay_min_seconds,
                    self.settings.default_reaction_delay_max_seconds,
                )
                result = await self.db.reschedule_pending_channel_reactions(
                    target.id,
                    minimum_seconds=target.reaction_window_min_seconds,
                    maximum_seconds=target.reaction_window_max_seconds,
                    account_delay_min_seconds=delay_min,
                    account_delay_max_seconds=delay_max,
                )
                details["rescheduled"] = int(result.get("updated", 0))
            except Exception:  # noqa: BLE001
                logger.exception("Не удалось перераспределить очередь target=%s", target.id)
                queue_errors.append("распределение")
        await self._render_channel_card(callback.message, target.id)
        suffix = (
            "\n⚠️ Настройки сохранены, но частично не обновлены: "
            + ", ".join(queue_errors)
            if queue_errors
            else ""
        )
        await callback.message.answer(
            "✅ <b>Настройки скопированы</b>\n\n"
            f"Обновлено реакций в очереди: <b>{details['reactions']}</b>\n"
            f"Перераспределено заданий: <b>{details['rescheduled']}</b>\n"
            f"Создано заданий: <b>{details['created']}</b>\n"
            f"Отменено несовместимых: <b>{details['cancelled']}</b>"
            f"{suffix}"
        )

    async def channel_list(self, callback: CallbackQuery) -> None:
        if await self._deny_if_needed(callback):
            return
        await self._render_channel_list(callback.message)
        await callback.answer()

    async def _render_channel_card(self, message: Message, channel_id: int) -> bool:
        channel = await self.db.get_channel(channel_id)
        if not channel or channel.kind != "channel":
            return False
        connect_summary, _available = await self.db.channel_connect_state(
            channel.id, max_accounts=self.settings.max_accounts_per_channel
        )
        queue = await self.db.reaction_counts(channel.id)
        views = await self.db.view_counts(channel.id)
        latest_view_batch = await self.db.latest_view_batch(channel.id)
        active_view_count = views["pending"] + views["running"]
        active_view_batch_id = (
            latest_view_batch.id
            if latest_view_batch and latest_view_batch.status in {"pending", "running"}
            else None
        )
        reaction_weights = await self.db.get_reaction_weights_for_channel(channel)
        reaction_mode = "свои для канала" if channel.reactions_json else "по умолчанию"
        profile_key = await self.db.get_channel_profile_key(
            channel, max_accounts_per_channel=self.settings.max_accounts_per_channel
        )
        profile_text = channel_profile_label(profile_key)
        status = "🟢 Активен" if channel.is_active else "⚫️ Выключен"
        error = html.escape(truncate(channel.last_error)) if channel.last_error else "нет"
        channel_window_text = format_duration_range(
            channel.reaction_window_min_seconds,
            channel.reaction_window_max_seconds,
        )
        new_mode = "ВКЛ" if channel.new_posts_enabled else "ВЫКЛ"
        old_mode = "ВКЛ" if channel.old_posts_enabled else "ВЫКЛ"
        await message.edit_text(
            "📢 <b>Карточка канала</b>\n"
            f"<i>{html.escape(channel.title)}</i>\n\n"
            "━━━━━━━━━━━━━━\n"
            f"🔌 Статус: <b>{status}</b>\n"
            f"🔗 Ссылка: {html.escape(channel.link)}\n"
            f"👥 Всего активных аккаунтов: <b>{connect_summary['total']}</b>\n"
            f"✅ Подписаны: <b>{connect_summary['joined']}</b>  ·  в очереди: {connect_summary['pending']}\n"
            f"➕ Не подключены: <b>{connect_summary['connectable']}</b>\n"
            f"🧾 Реакции: ожидают <b>{queue['pending'] + queue['running']}</b>  ·  готово {queue['done']}\n"
            f"👁 Просмотры: ожидают <b>{views['pending'] + views['running']}</b>  ·  готово {views['done']}\n"
            f"😀 Набор: <b>{html.escape(format_reaction_weights(reaction_weights))}</b>\n"
            f"🎛 Режим реакций: <b>{reaction_mode}</b>\n"
            f"🎚 Профиль: <b>{profile_text}</b>\n"
            f"⏱ Период реакций: <b>{channel_window_text}</b>\n"
            f"🖼 С изображением: <b>{channel.image_post_reaction_percent}%</b>  ·  "
            f"📝 Без изображения: <b>{channel.no_image_post_reaction_percent}%</b>\n"
            f"❤️ Новые: <b>{new_mode}</b>  ·  Старые: <b>{old_mode}</b>\n"
            f"⚠️ Последняя ошибка: <code>{error}</code>\n"
            "━━━━━━━━━━━━━━",
            reply_markup=channel_actions(
                channel.id,
                connectable_count=connect_summary["connectable"],
                pending_count=connect_summary["pending"],
                view_batch_id=active_view_batch_id,
                view_pending_count=active_view_count,
                profile_text=profile_text,
            ),
        )
        return True

    async def channel_view(self, callback: CallbackQuery) -> None:
        if await self._deny_if_needed(callback):
            return
        channel_id = int(callback.data.rsplit(":", 1)[1])
        if not await self._render_channel_card(callback.message, channel_id):
            await callback.answer("Канал не найден", show_alert=True)
            return
        await callback.answer()

    async def _render_channel_profile(
        self, message: Message, channel_id: int
    ) -> bool:
        channel = await self.db.get_channel(channel_id)
        if not channel or channel.kind != "channel" or not channel.is_active:
            return False
        current_key = await self.db.get_channel_profile_key(
            channel, max_accounts_per_channel=self.settings.max_accounts_per_channel
        )
        current_label = channel_profile_label(current_key)
        profile_lines: list[str] = []
        for definition in CHANNEL_PROFILES:
            profile = definition.resolve(self.settings.max_accounts_per_channel)
            coverage = (
                "все доступные аккаунты"
                if profile.max_reactions_per_post is None
                else f"до <b>{profile.max_reactions_per_post}</b> аккаунтов"
            )
            profile_lines.append(
                f"{profile.label}: {coverage} · "
                f"{format_duration_range(profile.reaction_window_min_seconds, profile.reaction_window_max_seconds)} · "
                f"🖼 {profile.image_post_reaction_percent}% · "
                f"📝 {profile.no_image_post_reaction_percent}%\n"
                f"<i>{html.escape(profile.description)}</i>"
            )
        await message.edit_text(
            "🎚 <b>Профили канала</b>\n"
            f"<i>{html.escape(channel.title)}</i>\n\n"
            f"Текущий профиль: <b>{current_label}</b>\n\n"
            + "\n".join(profile_lines)
            + "\n\nПрофиль меняет только лимит аккаунтов, период распределения "
            "и долю постов с реакцией/просмотром. Набор реакций, режим новых/старых "
            "постов, таймфрейм раскрутки и глобальная пауза аккаунта сохраняются.",
            reply_markup=channel_profile_keyboard(
                channel.id, current_profile_key=current_key
            ),
        )
        return True

    async def channel_profile_view(self, callback: CallbackQuery) -> None:
        if await self._deny_if_needed(callback):
            return
        try:
            channel_id = int(callback.data.rsplit(":", 1)[1])
        except (AttributeError, TypeError, ValueError):
            await callback.answer("Некорректная кнопка", show_alert=True)
            return
        if channel_id < 1:
            await callback.answer("Некорректная кнопка", show_alert=True)
            return
        if not await self._render_channel_profile(callback.message, channel_id):
            await callback.answer("Канал не найден", show_alert=True)
            return
        await callback.answer()

    async def channel_profile_select(self, callback: CallbackQuery) -> None:
        if await self._deny_if_needed(callback):
            return
        try:
            channel_id, profile_key = parse_channel_profile_callback(
                callback.data, "select"
            )
        except ValueError as exc:
            await callback.answer(str(exc), show_alert=True)
            return
        channel = await self.db.get_channel(channel_id)
        if not channel or channel.kind != "channel" or not channel.is_active:
            await callback.answer("Канал не найден", show_alert=True)
            return
        current_key = await self.db.get_channel_profile_key(
            channel, max_accounts_per_channel=self.settings.max_accounts_per_channel
        )
        profile = resolve_channel_profile(
            profile_key, self.settings.max_accounts_per_channel
        )
        limit = (
            "Все доступные аккаунты"
            if profile.max_reactions_per_post is None
            else str(profile.max_reactions_per_post)
        )
        profile_window_text = format_duration_range(
            profile.reaction_window_min_seconds,
            profile.reaction_window_max_seconds,
        )
        await callback.message.edit_text(
            "🎚 <b>Применить профиль</b>\n\n"
            f"Канал: <b>{html.escape(channel.title)}</b>\n"
            f"Сейчас: <b>{channel_profile_label(current_key)}</b>\n"
            f"Новый профиль: <b>{profile.label}</b>\n\n"
            f"🎯 Лимит на пост: <b>{limit}</b>\n"
            f"⏱ Период: <b>{profile_window_text}</b>\n"
            f"🖼 С изображением: <b>{profile.image_post_reaction_percent}%</b>\n"
            f"📝 Без изображения: <b>{profile.no_image_post_reaction_percent}%</b>\n\n"
            "Ожидающие задания без попыток будут приведены к новому лимиту и "
            "перераспределены по новому периоду. Уже выполненные реакции не меняются.",
            reply_markup=channel_profile_confirm_keyboard(channel.id, profile.key),
        )
        await callback.answer()

    async def channel_profile_apply(self, callback: CallbackQuery) -> None:
        if await self._deny_if_needed(callback):
            return
        try:
            channel_id, profile_key = parse_channel_profile_callback(
                callback.data, "apply"
            )
        except ValueError as exc:
            await callback.answer(str(exc), show_alert=True)
            return

        async with self._channel_profile_lock(channel_id):
            channel = await self.db.get_channel(channel_id)
            if not channel or channel.kind != "channel" or not channel.is_active:
                await callback.answer("Канал не найден", show_alert=True)
                return
            current_key = await self.db.get_channel_profile_key(
                channel,
                max_accounts_per_channel=self.settings.max_accounts_per_channel,
            )
            if current_key == profile_key:
                await self._render_channel_profile(callback.message, channel_id)
                await callback.answer("Этот профиль уже активен")
                return

            profile = resolve_channel_profile(
                profile_key, self.settings.max_accounts_per_channel
            )
            updated = await self.db.apply_channel_profile(channel_id, profile)
            if updated is None:
                await callback.answer("Канал не найден", show_alert=True)
                return

            notes: list[str] = []
            errors_list: list[str] = []
            try:
                result = await self.jobs.apply_post_type_percentages(updated)
                notes.append(
                    f"очередь: -{result['cancelled']} / +{result['created']}"
                )
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001
                logger.exception(
                    "Не удалось согласовать лимиты профиля target=%s", channel_id
                )
                errors_list.append(f"лимиты: {truncate(str(exc), 80)}")

            try:
                account_min, account_max = await self.db.get_delays(
                    self.settings.default_reaction_delay_min_seconds,
                    self.settings.default_reaction_delay_max_seconds,
                )
                timing = await self.db.reschedule_pending_channel_reactions(
                    channel_id,
                    minimum_seconds=updated.reaction_window_min_seconds,
                    maximum_seconds=updated.reaction_window_max_seconds,
                    account_delay_min_seconds=account_min,
                    account_delay_max_seconds=account_max,
                )
                notes.append(
                    f"пересчитано: {timing['updated']}"
                )
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001
                logger.exception(
                    "Не удалось перераспределить очередь профиля target=%s",
                    channel_id,
                )
                errors_list.append(f"период: {truncate(str(exc), 80)}")

            if not await self._render_channel_profile(callback.message, channel_id):
                await callback.answer("Профиль сохранён, но канал больше не найден", show_alert=True)
                return
            if errors_list:
                text = "Профиль сохранён; очередь обновилась частично: " + " · ".join(
                    errors_list
                )
            else:
                text = "Профиль применён · " + " · ".join(notes)
            await callback.answer(truncate(text, 190), show_alert=True)

    async def _render_channel_reaction_window(self, message: Message, channel_id: int) -> bool:
        channel = await self.db.get_channel(channel_id)
        if not channel or channel.kind != "channel" or not channel.is_active:
            return False
        account_min, account_max = await self.db.get_delays(
            self.settings.default_reaction_delay_min_seconds,
            self.settings.default_reaction_delay_max_seconds,
        )
        await message.edit_text(
            "⏱ <b>Период распределения реакций</b>\n"
            f"<i>{html.escape(channel.title)}</i>\n\n"
            "━━━━━━━━━━━━━━\n"
            f"Текущий период: <b>{format_duration_range(channel.reaction_window_min_seconds, channel.reaction_window_max_seconds)}</b>\n"
            f"Пауза одного аккаунта: <b>{account_min}–{account_max} сек.</b>\n"
            "━━━━━━━━━━━━━━\n\n"
            "Период задаёт, за какое время выбранные аккаунты распределят реакции на одну публикацию. "
            "Каждый пост получает отдельное окно. Пауза аккаунта дополнительно не даёт одному аккаунту "
            "выполнять задания слишком часто.\n\n"
            "Новые значения применяются к следующим заданиям. Для уже ожидающих используйте отдельную кнопку.",
            reply_markup=channel_reaction_window_keyboard(channel.id),
        )
        return True

    async def channel_reaction_window_view(self, callback: CallbackQuery, state: FSMContext) -> None:
        if await self._deny_if_needed(callback):
            return
        await state.clear()
        channel_id = int(callback.data.rsplit(":", 1)[1])
        if not await self._render_channel_reaction_window(callback.message, channel_id):
            await callback.answer("Канал не найден", show_alert=True)
            return
        await callback.answer()

    async def channel_reaction_window_value(self, callback: CallbackQuery, state: FSMContext) -> None:
        if await self._deny_if_needed(callback):
            return
        await state.clear()
        parts = callback.data.split(":")
        if len(parts) != 5:
            await callback.answer("Некорректная настройка", show_alert=True)
            return
        channel_id, minimum, maximum = int(parts[2]), int(parts[3]), int(parts[4])
        channel = await self.db.get_channel(channel_id)
        if not channel or channel.kind != "channel" or not channel.is_active:
            await callback.answer("Канал не найден", show_alert=True)
            return
        await self.db.set_channel_reaction_window(channel.id, minimum, maximum)
        await callback.message.edit_text(
            "✅ <b>Период реакций сохранён</b>\n\n"
            f"Канал: <b>{html.escape(channel.title)}</b>\n"
            f"Новый период: <b>{format_duration_range(minimum, maximum)}</b>\n\n"
            "Новые задания получат этот период. Ожидающую очередь можно пересчитать отдельно.",
            reply_markup=channel_reaction_window_after_save_keyboard(channel.id),
        )
        await callback.answer("Сохранено")

    async def channel_reaction_window_edit(self, callback: CallbackQuery, state: FSMContext) -> None:
        if await self._deny_if_needed(callback):
            return
        channel_id = int(callback.data.rsplit(":", 1)[1])
        channel = await self.db.get_channel(channel_id)
        if not channel or channel.kind != "channel" or not channel.is_active:
            await callback.answer("Канал не найден", show_alert=True)
            return
        await state.set_state(SetChannelReactionWindow.value)
        await state.update_data(channel_id=channel.id)
        await callback.message.edit_text(
            "✏️ <b>Свой период реакций</b>\n\n"
            f"Канал: <b>{html.escape(channel.title)}</b>\n"
            f"Сейчас: <b>{format_duration_range(channel.reaction_window_min_seconds, channel.reaction_window_max_seconds)}</b>\n\n"
            "Введите минимум и максимум. Без единицы используются минуты.\n"
            "Примеры: <code>20-90</code>, <code>1-3 ч</code>.\n\n"
            "Максимальный период — 7 дней. Отмена: /cancel",
            reply_markup=channel_back_keyboard(channel.id),
        )
        await callback.answer()

    async def channel_reaction_window_input(self, message: Message, state: FSMContext) -> None:
        if await self._deny_if_needed(message):
            return
        data = await state.get_data()
        channel_id = int(data.get("channel_id", 0))
        channel = await self.db.get_channel(channel_id)
        if not channel or channel.kind != "channel" or not channel.is_active:
            await state.clear()
            await message.answer("❌ Канал не найден.", reply_markup=main_menu())
            return
        try:
            minimum, maximum = parse_channel_reaction_window(message.text or "")
        except ValueError as exc:
            await message.answer(f"❌ {html.escape(str(exc))}")
            return
        await self.db.set_channel_reaction_window(channel.id, minimum, maximum)
        await state.clear()
        await message.answer(
            "✅ <b>Период реакций сохранён</b>\n\n"
            f"Канал: <b>{html.escape(channel.title)}</b>\n"
            f"Новый период: <b>{format_duration_range(minimum, maximum)}</b>\n\n"
            "Новые задания используют этот период. Ожидающие можно пересчитать кнопкой ниже.",
            reply_markup=channel_reaction_window_after_save_keyboard(channel.id),
        )

    async def channel_reaction_window_reschedule(self, callback: CallbackQuery) -> None:
        if await self._deny_if_needed(callback):
            return
        channel_id = int(callback.data.rsplit(":", 1)[1])
        channel = await self.db.get_channel(channel_id)
        if not channel or channel.kind != "channel" or not channel.is_active:
            await callback.answer("Канал не найден", show_alert=True)
            return
        account_min, account_max = await self.db.get_delays(
            self.settings.default_reaction_delay_min_seconds,
            self.settings.default_reaction_delay_max_seconds,
        )
        result = await self.db.reschedule_pending_channel_reactions(
            channel.id,
            minimum_seconds=channel.reaction_window_min_seconds,
            maximum_seconds=channel.reaction_window_max_seconds,
            account_delay_min_seconds=account_min,
            account_delay_max_seconds=account_max,
        )
        await self._render_channel_reaction_window(callback.message, channel.id)
        await callback.answer(
            f"Пересчитано: {result['updated']}; повторные попытки не тронуты: {result['skipped_retries']}",
            show_alert=True,
        )

    async def _render_channel_reactions(self, message: Message, channel_id: int) -> bool:
        channel = await self.db.get_channel(channel_id)
        if not channel or channel.kind != "channel" or not channel.is_active:
            return False
        override = await self.db.get_channel_reaction_override_weights(channel.id)
        defaults = await self.db.get_reaction_weights()
        effective = override or defaults
        mode = "🟣 Свои реакции канала" if override else "🔵 Реакции по умолчанию"
        await message.edit_text(
            "😀 <b>Реакции канала</b>\n"
            f"<i>{html.escape(channel.title)}</i>\n\n"
            "━━━━━━━━━━━━━━\n"
            f"Текущее соотношение: <b>{html.escape(format_reaction_weights(effective))}</b>\n"
            "Итого: <b>100%</b>\n"
            f"Режим: <b>{mode}</b>\n"
            f"По умолчанию: {html.escape(format_reaction_weights(defaults))}\n"
            "━━━━━━━━━━━━━━\n\n"
            "Эти реакции и их вероятность применяются только к выбранному каналу. "
            "Глобальные реакции из главного меню не меняются.",
            reply_markup=channel_reactions_keyboard(channel.id, has_override=bool(override)),
        )
        return True

    async def channel_reactions_view(self, callback: CallbackQuery, state: FSMContext) -> None:
        if await self._deny_if_needed(callback):
            return
        await state.clear()
        channel_id = int(callback.data.rsplit(":", 1)[1])
        if not await self._render_channel_reactions(callback.message, channel_id):
            await callback.answer("Канал не найден", show_alert=True)
            return
        await callback.answer()

    async def channel_reactions_edit(self, callback: CallbackQuery, state: FSMContext) -> None:
        if await self._deny_if_needed(callback):
            return
        channel_id = int(callback.data.rsplit(":", 1)[1])
        channel = await self.db.get_channel(channel_id)
        if not channel or channel.kind != "channel" or not channel.is_active:
            await callback.answer("Канал не найден", show_alert=True)
            return
        current = await self.db.get_reaction_weights_for_channel(channel)
        await state.set_state(SetChannelReactions.reactions)
        await state.update_data(channel_id=channel.id)
        await callback.message.edit_text(
            "✏️ <b>Реакции конкретного канала</b>\n\n"
            f"Канал: <b>{html.escape(channel.title)}</b>\n"
            f"Сейчас: {html.escape(format_reaction_weights(current))}\n\n"
            "Укажите эмодзи и любые числовые веса. Бот сам пересчитает их в 100%.\n"
            "Пример: <code>👍60 ❤️20 🔥20</code>\n"
            "Или: <code>👍 6 ❤️ 2 🔥 2</code>\n"
            "Без чисел реакции будут равновероятными: <code>👍 ❤️ 🔥</code>\n\n"
            "Отмена: /cancel",
            reply_markup=back_main(),
        )
        await callback.answer()

    async def channel_reactions_input(self, message: Message, state: FSMContext) -> None:
        if await self._deny_if_needed(message):
            return
        data = await state.get_data()
        channel_id = int(data.get("channel_id", 0))
        channel = await self.db.get_channel(channel_id)
        if not channel or channel.kind != "channel" or not channel.is_active:
            await state.clear()
            await message.answer("❌ Канал не найден.", reply_markup=main_menu())
            return
        try:
            weights = parse_weighted_reactions(message.text or "")
        except ValueError as exc:
            await message.answer(f"❌ {html.escape(str(exc))}")
            return
        await self.db.set_channel_reaction_weights(channel.id, weights)
        updated_pending = await self.db.refresh_pending_reactions(channel.id, weights)
        await state.clear()
        await message.answer(
            "✅ <b>Реакции канала сохранены</b>\n\n"
            f"Канал: <b>{html.escape(channel.title)}</b>\n"
            f"Соотношение: <b>{html.escape(format_reaction_weights(weights))}</b>\n"
            "Итого: <b>100%</b>\n"
            f"Обновлено ожидающих заданий: <b>{updated_pending}</b>\n\n"
            "Настройка действует только для этого канала.",
            reply_markup=channel_reactions_keyboard(channel.id, has_override=True),
        )

    async def channel_reactions_reset(self, callback: CallbackQuery, state: FSMContext) -> None:
        if await self._deny_if_needed(callback):
            return
        await state.clear()
        channel_id = int(callback.data.rsplit(":", 1)[1])
        channel = await self.db.get_channel(channel_id)
        if not channel or channel.kind != "channel" or not channel.is_active:
            await callback.answer("Канал не найден", show_alert=True)
            return
        await self.db.clear_channel_reactions(channel.id)
        defaults = await self.db.get_reaction_weights()
        updated_pending = await self.db.refresh_pending_reactions(channel.id, defaults)
        if not await self._render_channel_reactions(callback.message, channel.id):
            await callback.answer("Канал не найден", show_alert=True)
            return
        await callback.answer(
            f"Включены реакции по умолчанию · обновлено заданий: {updated_pending}"
        )

    async def _render_channel_post_types(self, message: Message, channel_id: int) -> bool:
        channel = await self.db.get_channel(channel_id)
        if not channel or channel.kind != "channel" or not channel.is_active:
            return False
        membership = await self.db.membership_counts(channel.id)
        configured_limit = channel.max_reactions_per_post
        base_text = str(configured_limit) if configured_limit else "все подписанные аккаунты"
        await message.edit_text(
            "🖼 <b>Типы постов</b>\n"
            f"<i>{html.escape(channel.title)}</i>\n\n"
            "━━━━━━━━━━━━━━\n"
            f"🖼 С изображением: <b>{channel.image_post_reaction_percent}%</b>\n"
            f"📝 Без изображения: <b>{channel.no_image_post_reaction_percent}%</b>\n"
            "━━━━━━━━━━━━━━\n"
            f"База расчёта: <b>{base_text}</b>.\n"
            f"Сейчас подписаны: <b>{membership['joined']}</b>.\n\n"
            "Процент определяет, какая часть доступных аккаунтов получит задание. "
            "Например, при 10 аккаунтах и 20% будет создано 2 реакции.\n\n"
            "Изображением считается фото, файл image/* или альбом, где есть хотя бы одно изображение.",
            reply_markup=channel_post_types_keyboard(
                channel.id,
                image_percent=channel.image_post_reaction_percent,
                no_image_percent=channel.no_image_post_reaction_percent,
            ),
        )
        return True

    async def channel_post_types_view(self, callback: CallbackQuery, state: FSMContext) -> None:
        if await self._deny_if_needed(callback):
            return
        await state.clear()
        channel_id = int(callback.data.rsplit(":", 1)[1])
        if not await self._render_channel_post_types(callback.message, channel_id):
            await callback.answer("Канал не найден", show_alert=True)
            return
        await callback.answer()

    async def channel_post_type_edit(self, callback: CallbackQuery, state: FSMContext) -> None:
        if await self._deny_if_needed(callback):
            return
        parts = callback.data.split(":")
        if len(parts) != 4 or parts[2] not in {"image", "no_image"}:
            await callback.answer("Некорректная настройка", show_alert=True)
            return
        post_type = parts[2]
        channel_id = int(parts[3])
        channel = await self.db.get_channel(channel_id)
        if not channel or channel.kind != "channel" or not channel.is_active:
            await callback.answer("Канал не найден", show_alert=True)
            return
        current = (
            channel.image_post_reaction_percent
            if post_type == "image"
            else channel.no_image_post_reaction_percent
        )
        label = "с изображением" if post_type == "image" else "без изображения"
        await state.set_state(SetPostTypePercentage.value)
        await state.update_data(channel_id=channel.id, post_type=post_type)
        await callback.message.answer(
            f"✏️ <b>Процент реакций для постов {label}</b>\n\n"
            f"Канал: <b>{html.escape(channel.title)}</b>\n"
            f"Сейчас: <b>{current}%</b>\n\n"
            "Введите целое число от 0 до 100.\n"
            "0% - реакции на этот тип постов не создаются.",
            reply_markup=channel_post_type_cancel_keyboard(channel.id),
        )
        await callback.answer()

    async def channel_post_type_input(self, message: Message, state: FSMContext) -> None:
        if await self._deny_if_needed(message):
            return
        data = await state.get_data()
        channel_id = int(data.get("channel_id", 0))
        post_type = str(data.get("post_type", ""))
        channel = await self.db.get_channel(channel_id)
        if not channel or channel.kind != "channel" or not channel.is_active:
            await state.clear()
            await message.answer("Канал не найден.", reply_markup=back_main())
            return
        try:
            percent = parse_post_type_percentage(message.text or "")
            await self.db.set_channel_post_type_percent(
                channel.id, post_type=post_type, percent=percent
            )
        except ValueError as exc:
            await message.answer(f"❌ {html.escape(str(exc))}")
            return
        await state.clear()
        updated = await self.db.get_channel(channel.id)
        result = await self.jobs.apply_post_type_percentages(updated)
        label = "с изображением" if post_type == "image" else "без изображения"
        await message.answer(
            "✅ <b>Процент обновлён</b>\n\n"
            f"Канал: <b>{html.escape(updated.title)}</b>\n"
            f"Посты {label}: <b>{percent}%</b>\n"
            f"Ожидающие задания сокращены: <b>{result['cancelled']}</b>\n"
            f"Дополнительные задания созданы: <b>{result['created']}</b>",
            reply_markup=channel_post_types_keyboard(
                updated.id,
                image_percent=updated.image_post_reaction_percent,
                no_image_percent=updated.no_image_post_reaction_percent,
            ),
        )

    async def channel_post_types_reset(
        self, callback: CallbackQuery, state: FSMContext
    ) -> None:
        if await self._deny_if_needed(callback):
            return
        await state.clear()
        channel_id = int(callback.data.rsplit(":", 1)[1])
        channel = await self.db.get_channel(channel_id)
        if not channel or channel.kind != "channel" or not channel.is_active:
            await callback.answer("Канал не найден", show_alert=True)
            return
        await self.db.set_channel_post_type_percents(
            channel.id, image_percent=100, no_image_percent=100
        )
        updated = await self.db.get_channel(channel.id)
        result = await self.jobs.apply_post_type_percentages(updated)
        await self._render_channel_post_types(callback.message, updated.id)
        await callback.answer(
            f"Оба типа: 100%. Создано: {result['created']}, сокращено: {result['cancelled']}"
        )
