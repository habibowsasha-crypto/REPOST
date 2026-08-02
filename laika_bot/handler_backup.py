from __future__ import annotations

from . import __version__
from .handler_shared import *  # noqa: F403


class BackupHandlersMixin:
    def _backup_runtime_kwargs(self) -> dict[str, int]:
        return {
            "default_reaction_min": self.settings.default_reaction_delay_min_seconds,
            "default_reaction_max": self.settings.default_reaction_delay_max_seconds,
            "default_membership_min": self.settings.join_delay_min_seconds,
            "default_membership_max": self.settings.join_delay_max_seconds,
            "max_accounts_per_channel": self.settings.max_accounts_per_channel,
            "max_old_posts": self.settings.max_old_posts,
        }

    async def backup_menu(self, callback: CallbackQuery, state: FSMContext) -> None:
        if await self._deny_if_needed(callback):
            return
        await state.clear()
        await callback.message.edit_text(
            "💾 <b>Резервные копии LikeBot</b>\n"
            "<i>Переносимая копия безопасных настроек</i>\n\n"
            "━━━━━━━━━━━━━━\n"
            "✅ Входит: реакции, задержки, настройки каналов и групп, "
            "маскированный список аккаунтов.\n"
            "🔒 Не входит: StringSession, коды, 2FA, токены, API-ключи, "
            "пароли, DATABASE_URL, приватные invite hash и очереди.\n"
            "━━━━━━━━━━━━━━\n\n"
            "Файл подписывается ключом LikeBot. Изменённый файл или копия от "
            "другого SESSION_ENCRYPTION_KEY будет отклонена. Подпись защищает "
            "целостность, но файл не шифруется — храните его закрыто.\n\n"
            "<b>Важно:</b> это резервная копия конфигурации, а не сырой дамп "
            "PostgreSQL. Полный backup базы нужно хранить средствами Railway.",
            reply_markup=backup_menu_keyboard(),
        )
        await callback.answer()

    async def backup_export(self, callback: CallbackQuery) -> None:
        if await self._deny_if_needed(callback):
            return
        await callback.answer("Готовлю резервную копию…")
        try:
            payload = await self.db.export_configuration_payload(
                **{
                    key: value
                    for key, value in self._backup_runtime_kwargs().items()
                    if key != "max_old_posts"
                }
            )
            raw = create_backup_bytes(
                payload,
                app_version=__version__,
                signing_secret=self.settings.session_encryption_key,
            )
            stamp = datetime.now(UTC).strftime("%Y%m%d_%H%M%S")
            filename = f"likebot_configuration_v{__version__}_{stamp}.json"
            await callback.message.answer_document(
                BufferedInputFile(raw, filename=filename),
                caption=(
                    "✅ <b>Резервная копия настроек готова</b>\n\n"
                    f"Аккаунтов в справочном списке: <b>{len(payload['accounts'])}</b>\n"
                    f"Каналов и групп: <b>{len(payload['targets'])}</b>\n"
                    f"SHA-256 содержимого: <code>{payload_sha256(payload)}</code>\n\n"
                    "Храните файл закрыто вместе с актуальным SESSION_ENCRYPTION_KEY. "
                    "Файл подписан, но не зашифрован; секретные сессии и пароли "
                    "в него не включены."
                ),
            )
            try:
                await self.db.record_configuration_export(payload, source_name=filename)
            except Exception:  # noqa: BLE001
                logger.exception("Не удалось записать событие экспорта конфигурации")
        except (BackupValidationError, ValueError) as exc:
            await callback.message.answer(
                f"❌ Не удалось создать копию: {html.escape(str(exc))}",
                reply_markup=backup_menu_keyboard(),
            )
        except Exception as exc:  # noqa: BLE001
            logger.exception("Ошибка экспорта конфигурации")
            await callback.message.answer(
                "❌ Не удалось создать резервную копию. "
                f"Ошибка: <code>{html.escape(truncate(str(exc), 180))}</code>",
                reply_markup=backup_menu_keyboard(),
            )

    async def backup_restore_start(
        self, callback: CallbackQuery, state: FSMContext
    ) -> None:
        if await self._deny_if_needed(callback):
            return
        await state.clear()
        await state.set_state(ConfigurationRestore.backup_file)
        await callback.message.edit_text(
            "📥 <b>Восстановление настроек</b>\n\n"
            "Отправьте JSON-файл, ранее созданный LikeBot.\n\n"
            "Будут восстановлены только настройки уже существующих каналов и "
            "групп, совпавших по Telegram ID и типу. Новые аккаунты, каналы, "
            "сессии и членство автоматически не создаются. Перед применением "
            "бот покажет точный предварительный отчёт.\n\n"
            f"Максимальный размер файла: <b>{MAX_BACKUP_BYTES // 1024} КБ</b>.",
            reply_markup=backup_restore_cancel_keyboard(),
        )
        await callback.answer()

    async def backup_restore_invalid_file(
        self, message: Message, state: FSMContext
    ) -> None:
        if await self._deny_if_needed(message):
            return
        await message.answer(
            "❌ Отправьте резервную копию как JSON-документ.",
            reply_markup=backup_restore_cancel_keyboard(),
        )

    async def backup_restore_file(
        self, message: Message, state: FSMContext
    ) -> None:
        if await self._deny_if_needed(message):
            return
        document = message.document
        if document is None:
            await self.backup_restore_invalid_file(message, state)
            return
        filename = str(document.file_name or "").strip()
        if not filename.casefold().endswith(".json"):
            await message.answer(
                "❌ Нужен JSON-файл резервной копии LikeBot.",
                reply_markup=backup_restore_cancel_keyboard(),
            )
            return
        if document.file_size is not None and int(document.file_size) > MAX_BACKUP_BYTES:
            await message.answer(
                f"❌ Файл больше {MAX_BACKUP_BYTES // 1024} КБ.",
                reply_markup=backup_restore_cancel_keyboard(),
            )
            return

        buffer = io.BytesIO()
        try:
            await asyncio.wait_for(
                message.bot.download(document, destination=buffer), timeout=30
            )
            raw = buffer.getvalue()
            verified = verify_backup_bytes(
                raw, signing_secret=self.settings.session_encryption_key
            )
            preview = await self.db.preview_configuration_restore(
                verified.payload, **self._backup_runtime_kwargs()
            )
        except asyncio.TimeoutError:
            await message.answer(
                "❌ Telegram не успел отдать файл за 30 секунд. Повторите загрузку.",
                reply_markup=backup_restore_cancel_keyboard(),
            )
            return
        except BackupIntegrityError as exc:
            await message.answer(
                "❌ <b>Подпись резервной копии не совпала</b>\n\n"
                f"{html.escape(str(exc))}",
                reply_markup=backup_restore_cancel_keyboard(),
            )
            return
        except (BackupValidationError, ValueError) as exc:
            await message.answer(
                f"❌ Некорректная резервная копия: {html.escape(str(exc))}",
                reply_markup=backup_restore_cancel_keyboard(),
            )
            return
        except Exception as exc:  # noqa: BLE001
            logger.exception("Ошибка чтения резервной копии")
            await message.answer(
                "❌ Не удалось проверить файл. "
                f"Ошибка: <code>{html.escape(truncate(str(exc), 180))}</code>",
                reply_markup=backup_restore_cancel_keyboard(),
            )
            return

        await state.update_data(
            configuration_backup_payload=verified.payload,
            configuration_backup_digest=payload_sha256(verified.payload),
            configuration_backup_source=filename[:255],
            configuration_backup_created_at=verified.created_at,
            configuration_backup_app_version=verified.app_version,
        )
        await state.set_state(ConfigurationRestore.confirmation)
        await message.answer(
            "🔎 <b>Предварительная проверка завершена</b>\n\n"
            "━━━━━━━━━━━━━━\n"
            f"Версия источника: <b>{html.escape(verified.app_version)}</b>\n"
            f"Создана: <b>{html.escape(verified.created_at)}</b>\n"
            f"Глобальные настройки изменятся: <b>{'да' if preview['global_changed'] else 'нет'}</b>\n"
            f"Совпавшие каналы/группы: <b>{preview['targets_matched']}</b>\n"
            f"Из них изменятся: <b>{preview['targets_changed']}</b>\n"
            f"Без изменений: <b>{preview['targets_unchanged']}</b>\n"
            f"Не найдены: <b>{preview['targets_missing']}</b>\n"
            f"Не совпал тип: <b>{preview['targets_kind_mismatch']}</b>\n"
            f"Аккаунты совпали: <b>{preview['accounts_matched']}</b> из {preview['accounts_total']}\n"
            "━━━━━━━━━━━━━━\n\n"
            "Список аккаунтов используется только для справки и не меняет "
            "сессии, статусы или авторизацию. Перед восстановлением автоматически "
            "будет сохранён безопасный снимок для отката.",
            reply_markup=backup_restore_confirm_keyboard(),
        )

    async def _reconcile_restored_configuration(
        self, result: dict[str, object]
    ) -> list[str]:
        errors: list[str] = []
        weights: dict[str, float] = {"👍": 1.0}
        try:
            weights = await self.db.get_reaction_weights()
            await self.db.refresh_pending_default_reactions(weights)
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001
            logger.exception("Не удалось обновить реакции после восстановления")
            errors.append(f"глобальные реакции: {truncate(str(exc), 90)}")

        try:
            reaction_min, reaction_max = await self.db.get_delays(
                self.settings.default_reaction_delay_min_seconds,
                self.settings.default_reaction_delay_max_seconds,
            )
            await self.db.reschedule_pending_reaction_jobs(
                reaction_min, reaction_max
            )
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001
            logger.exception("Не удалось пересчитать очередь реакций после восстановления")
            errors.append(f"задержка реакций: {truncate(str(exc), 90)}")
            reaction_min = self.settings.default_reaction_delay_min_seconds
            reaction_max = self.settings.default_reaction_delay_max_seconds

        try:
            membership_min, membership_max = await self.db.get_membership_delays(
                self.settings.join_delay_min_seconds, self.settings.join_delay_max_seconds
            )
            await self.db.reschedule_pending_membership_jobs(
                membership_min, membership_max
            )
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001
            logger.exception("Не удалось пересчитать очередь подписок после восстановления")
            errors.append(f"задержка подписок: {truncate(str(exc), 90)}")

        changed_ids = [int(value) for value in result.get("changed_channel_ids", [])]
        for channel_id in changed_ids:
            try:
                channel = await self.db.get_channel(channel_id)
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001
                logger.exception("Не удалось перечитать target=%s после восстановления", channel_id)
                errors.append(f"канал {channel_id}, чтение: {truncate(str(exc), 90)}")
                continue
            if channel is None or not channel.is_active:
                continue

            try:
                effective_weights = await self.db.get_reaction_weights_for_channel(channel)
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001
                logger.exception(
                    "Не удалось получить реакции target=%s после восстановления",
                    channel_id,
                )
                errors.append(
                    f"канал {channel_id}, реакции: {truncate(str(exc), 90)}"
                )
                effective_weights = weights

            operations = (
                (
                    "реакции",
                    lambda: self.db.refresh_pending_reactions(
                        channel.id, effective_weights
                    ),
                ),
                ("лимиты", lambda: self.jobs.apply_reaction_limit(channel)),
                ("период", lambda: self.jobs.apply_promotion_period(channel)),
                (
                    "окно",
                    lambda: self.db.reschedule_pending_channel_reactions(
                        channel.id,
                        minimum_seconds=channel.reaction_window_min_seconds,
                        maximum_seconds=channel.reaction_window_max_seconds,
                        account_delay_min_seconds=reaction_min,
                        account_delay_max_seconds=reaction_max,
                    ),
                ),
            )
            for operation_name, operation in operations:
                try:
                    await operation()
                except asyncio.CancelledError:
                    raise
                except Exception as exc:  # noqa: BLE001
                    logger.exception(
                        "Не удалось согласовать %s target=%s",
                        operation_name,
                        channel_id,
                    )
                    errors.append(
                        f"канал {channel_id}, {operation_name}: {truncate(str(exc), 90)}"
                    )
        return errors

    async def backup_restore_apply(
        self, callback: CallbackQuery, state: FSMContext
    ) -> None:
        if await self._deny_if_needed(callback):
            return
        await callback.answer("Восстанавливаю настройки…")
        async with self._configuration_restore_lock:
            data = await state.get_data()
            payload = data.get("configuration_backup_payload")
            digest = data.get("configuration_backup_digest")
            source_name = data.get("configuration_backup_source")
            try:
                payload_matches = (
                    isinstance(payload, dict) and payload_sha256(payload) == digest
                )
            except (BackupValidationError, TypeError, ValueError):
                payload_matches = False
            if not payload_matches:
                await state.clear()
                await callback.message.edit_text(
                    "❌ Данные подтверждения устарели или повреждены. Загрузите копию снова.",
                    reply_markup=backup_menu_keyboard(),
                )
                return
            try:
                result = await self.db.restore_configuration_payload(
                    payload,
                    source_name=str(source_name or "uploaded-backup.json"),
                    **self._backup_runtime_kwargs(),
                )
            except (BackupValidationError, ValueError) as exc:
                await callback.message.edit_text(
                    f"❌ Восстановление отклонено: {html.escape(str(exc))}",
                    reply_markup=backup_menu_keyboard(),
                )
                return
            except Exception as exc:  # noqa: BLE001
                logger.exception("Ошибка восстановления конфигурации")
                await callback.message.edit_text(
                    "❌ Восстановление не завершено. Транзакция настроек отменена. "
                    f"Ошибка: <code>{html.escape(truncate(str(exc), 180))}</code>",
                    reply_markup=backup_menu_keyboard(),
                )
                return
            try:
                reconcile_errors = await self._reconcile_restored_configuration(result)
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001
                logger.exception("Неожиданная ошибка согласования очереди после restore")
                reconcile_errors = [f"общая ошибка очереди: {truncate(str(exc), 90)}"]
            await state.clear()
            note = (
                "✅ <b>Настройки восстановлены</b>\n\n"
                "━━━━━━━━━━━━━━\n"
                f"Событие истории: <b>#{result['event_id']}</b>\n"
                f"Совпало каналов/групп: <b>{result['targets_matched']}</b>\n"
                f"Изменено: <b>{result['targets_changed']}</b>\n"
                f"Не найдено: <b>{result['targets_missing']}</b>\n"
                f"Не совпал тип: <b>{result['targets_kind_mismatch']}</b>\n"
                "━━━━━━━━━━━━━━"
            )
            if reconcile_errors:
                note += (
                    "\n\n⚠️ Настройки сохранены, но очередь обновилась не полностью:\n- "
                    + "\n- ".join(html.escape(item) for item in reconcile_errors[:8])
                )
            else:
                note += "\n\n🟢 Текущие очереди согласованы с восстановленными настройками."
            await callback.message.edit_text(note, reply_markup=backup_menu_keyboard())

    async def backup_cancel(
        self, callback: CallbackQuery, state: FSMContext
    ) -> None:
        if await self._deny_if_needed(callback):
            return
        await state.clear()
        await callback.message.edit_text(
            "✅ Операция с резервной копией отменена.",
            reply_markup=backup_menu_keyboard(),
        )
        await callback.answer()

    @staticmethod
    def _configuration_event_label(event: object) -> str:
        event_type = getattr(event, "event_type", "unknown")
        icons = {"export": "📤", "restore": "📥", "rollback": "↩️"}
        names = {"export": "Экспорт", "restore": "Восстановление", "rollback": "Откат"}
        created = getattr(event, "created_at", None)
        stamp = created.strftime("%d.%m %H:%M") if created is not None else "без даты"
        return f"{icons.get(event_type, '🧾')} #{getattr(event, 'id', '?')} · {names.get(event_type, event_type)} · {stamp}"[:64]

    async def backup_history(self, callback: CallbackQuery) -> None:
        if await self._deny_if_needed(callback):
            return
        events = await self.db.list_configuration_events(limit=12)
        labels = [(int(event.id), self._configuration_event_label(event)) for event in events]
        text = (
            "🕘 <b>История резервных операций</b>\n\n"
            "Для восстановлений и откатов хранится безопасный снимок состояния "
            "до изменения. Максимум — 30 последних событий.\n\n"
            + ("Выберите событие 👇" if events else "История пока пуста.")
        )
        await callback.message.edit_text(
            text, reply_markup=backup_history_keyboard(labels)
        )
        await callback.answer()

    async def backup_event_view(self, callback: CallbackQuery) -> None:
        if await self._deny_if_needed(callback):
            return
        try:
            event_id = parse_configuration_event_callback(callback.data, "event")
        except ValueError as exc:
            await callback.answer(str(exc), show_alert=True)
            return
        event = await self.db.get_configuration_event(event_id)
        if event is None:
            await callback.answer("Событие не найдено", show_alert=True)
            return
        try:
            summary = json.loads(event.summary_json)
        except (TypeError, json.JSONDecodeError):
            summary = {}
        type_name = {
            "export": "Экспорт",
            "restore": "Восстановление",
            "rollback": "Откат",
        }.get(event.event_type, event.event_type)
        source = html.escape(event.source_name or "не указан")
        created = event.created_at.strftime("%d.%m.%Y %H:%M:%S UTC")
        await callback.message.edit_text(
            "🧾 <b>Событие конфигурации</b>\n\n"
            "━━━━━━━━━━━━━━\n"
            f"ID: <b>#{event.id}</b>\n"
            f"Тип: <b>{html.escape(type_name)}</b>\n"
            f"Дата: <b>{created}</b>\n"
            f"Источник: <code>{source}</code>\n"
            f"Каналов/групп: <b>{summary.get('targets', summary.get('targets_total', '—'))}</b>\n"
            f"Изменено: <b>{summary.get('targets_changed', '—')}</b>\n"
            f"Не найдено: <b>{summary.get('targets_missing', '—')}</b>\n"
            "━━━━━━━━━━━━━━\n\n"
            + (
                "↩️ Доступен откат к состоянию, которое было до этой операции."
                if event.snapshot_json and event.snapshot_sha256
                else "Для этого события снимок отката не сохранялся."
            ),
            reply_markup=backup_event_keyboard(
                event.id,
                can_rollback=bool(event.snapshot_json and event.snapshot_sha256),
            ),
        )
        await callback.answer()

    async def backup_rollback_confirm(self, callback: CallbackQuery) -> None:
        if await self._deny_if_needed(callback):
            return
        try:
            event_id = parse_configuration_event_callback(
                callback.data, "rollback_confirm"
            )
        except ValueError as exc:
            await callback.answer(str(exc), show_alert=True)
            return
        event = await self.db.get_configuration_event(event_id)
        if event is None or not event.snapshot_json or not event.snapshot_sha256:
            await callback.answer("Снимок отката не найден", show_alert=True)
            return
        await callback.message.edit_text(
            "⚠️ <b>Подтвердите откат</b>\n\n"
            f"LikeBot восстановит состояние, которое было <b>до события #{event.id}</b>. "
            "Перед откатом текущее состояние также будет сохранено новым снимком, "
            "поэтому действие можно будет отменить обратным откатом.",
            reply_markup=backup_rollback_confirm_keyboard(event.id),
        )
        await callback.answer()

    async def backup_rollback_apply(self, callback: CallbackQuery) -> None:
        if await self._deny_if_needed(callback):
            return
        try:
            event_id = parse_configuration_event_callback(
                callback.data, "rollback_apply"
            )
        except ValueError as exc:
            await callback.answer(str(exc), show_alert=True)
            return
        await callback.answer("Выполняю откат…")
        async with self._configuration_restore_lock:
            try:
                result = await self.db.rollback_configuration_event(
                    event_id, **self._backup_runtime_kwargs()
                )
            except (BackupValidationError, ValueError) as exc:
                await callback.message.edit_text(
                    f"❌ Откат отклонён: {html.escape(str(exc))}",
                    reply_markup=backup_menu_keyboard(),
                )
                return
            except Exception as exc:  # noqa: BLE001
                logger.exception("Ошибка отката конфигурации event=%s", event_id)
                await callback.message.edit_text(
                    "❌ Откат не завершён. Транзакция отката отменена. "
                    f"Ошибка: <code>{html.escape(truncate(str(exc), 180))}</code>",
                    reply_markup=backup_menu_keyboard(),
                )
                return
            try:
                reconcile_errors = await self._reconcile_restored_configuration(result)
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001
                logger.exception("Неожиданная ошибка согласования очереди после rollback")
                reconcile_errors = [f"общая ошибка очереди: {truncate(str(exc), 90)}"]
            text = (
                "✅ <b>Откат выполнен</b>\n\n"
                f"Источник: событие <b>#{event_id}</b>\n"
                f"Новое событие истории: <b>#{result['event_id']}</b>\n"
                f"Изменено каналов/групп: <b>{result['targets_changed']}</b>"
            )
            if reconcile_errors:
                text += (
                    "\n\n⚠️ Очередь обновилась не полностью:\n- "
                    + "\n- ".join(html.escape(item) for item in reconcile_errors[:8])
                )
            await callback.message.edit_text(text, reply_markup=backup_menu_keyboard())
