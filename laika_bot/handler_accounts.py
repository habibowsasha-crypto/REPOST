from __future__ import annotations

from .handler_shared import *  # noqa: F403


class AccountHandlersMixin:
    async def account_add(self, callback: CallbackQuery, state: FSMContext) -> None:
        if await self._deny_if_needed(callback):
            return
        await self.login_manager.cancel(callback.from_user.id)
        await state.clear()
        await state.set_state(AccountAuth.phone)
        await self._show_account_phone_prompt(callback.message)
        await callback.answer()

    async def _show_account_phone_prompt(self, message: Message) -> None:
        await message.edit_text(
            "➕ <b>Добавление аккаунта</b>\n\n"
            "Отправьте номер телефона в международном формате, например:\n"
            "<code>+79991234567</code>\n\n"
            "После номера бот попросит логин привязанной почты.\n"
            "Код, пароль Telegram 2FA и пароль почты не записываются в базу.\n"
            "Отмена: /cancel",
            reply_markup=back_main(),
        )

    async def _show_account_email_prompt(self, message: Message, phone: str) -> None:
        await message.answer(
            "📧 <b>Почта Telegram-аккаунта</b>\n\n"
            f"Телефон: <code>{html.escape(phone)}</code>\n\n"
            "Отправьте логин почты, привязанной к этому аккаунту, например:\n"
            "<code>account@gmail.com</code>\n\n"
            "Бот сохранит только адрес почты. Пароль, резервные коды и коды "
            "подтверждения вводить не нужно.\n"
            "Отмена: /cancel",
            reply_markup=back_main(),
        )

    @staticmethod
    def _login_code_message(login) -> str:
        next_line = ""
        if login.next_delivery_text:
            next_line = f"\nСледующий доступный способ: <b>{html.escape(login.next_delivery_text)}</b>."
        return (
            "📩 <b>Telegram принял запрос на код</b>\n\n"
            f"Способ доставки: <b>{html.escape(login.delivery_text)}</b>."
            f"{next_line}\n\n"
            "Откройте официальный Telegram на подключаемом аккаунте и проверьте чат <b>Telegram</b>. "
            "Telegram сам выбирает способ доставки: код не всегда приходит по SMS.\n\n"
            "Отправьте полученный код сюда. Можно с пробелами.\n"
            "Отмена: /cancel"
        )

    async def account_phone(self, message: Message, state: FSMContext) -> None:
        if await self._deny_if_needed(message):
            return
        phone = re.sub(r"[^0-9+]", "", message.text or "")
        if not re.fullmatch(r"\+[0-9]{7,15}", phone):
            await message.answer("Некорректный номер. Пример: <code>+79991234567</code>")
            return
        await state.update_data(account_phone=phone)
        await state.set_state(AccountAuth.email)
        await self._show_account_email_prompt(message, phone)

    async def account_email(self, message: Message, state: FSMContext) -> None:
        if await self._deny_if_needed(message):
            return
        data = await state.get_data()
        phone = data.get("account_phone")
        if not phone:
            await state.clear()
            await message.answer(
                "Сессия добавления потеряна. Начните заново.", reply_markup=main_menu()
            )
            return
        try:
            email_login, email_provider = normalize_email_login(message.text or "")
        except ValueError as exc:
            await message.answer(
                f"❌ {html.escape(str(exc))}\n\n"
                "Пример: <code>account@gmail.com</code>"
            )
            return
        await state.update_data(
            account_email=email_login,
            account_email_provider=email_provider,
        )
        await state.set_state(AccountAuth.email_confirm)
        await message.answer(
            "✅ <b>Проверьте данные</b>\n\n"
            f"📱 Телефон: <code>{html.escape(str(phone))}</code>\n"
            f"📧 Почта: <code>{html.escape(email_login)}</code>\n"
            f"🏷 Сервис: <b>{html.escape(email_provider)}</b>\n\n"
            "После подтверждения Telegram отправит код входа.",
            reply_markup=account_email_confirm_keyboard(),
        )

    async def account_email_confirm(self, callback: CallbackQuery, state: FSMContext) -> None:
        if await self._deny_if_needed(callback):
            return
        data = await state.get_data()
        phone = data.get("account_phone")
        email_login = data.get("account_email")
        email_provider = data.get("account_email_provider")
        if not phone or not email_login or not email_provider:
            await state.clear()
            await callback.message.edit_text(
                "Сессия добавления потеряна. Начните добавление аккаунта заново.",
                reply_markup=main_menu(),
            )
            await callback.answer("Данные не найдены", show_alert=True)
            return
        try:
            login = await self.login_manager.start(callback.from_user.id, str(phone))
        except Exception as exc:  # noqa: BLE001
            logger.exception("Не удалось отправить код")
            await callback.answer(
                f"Не удалось отправить код: {truncate(str(exc), 120)}",
                show_alert=True,
            )
            return
        await state.set_state(AccountAuth.code)
        await callback.message.edit_text(
            self._login_code_message(login), reply_markup=login_code_actions()
        )
        await callback.answer("Код входа запрошен")

    async def account_email_change(self, callback: CallbackQuery, state: FSMContext) -> None:
        if await self._deny_if_needed(callback):
            return
        data = await state.get_data()
        phone = data.get("account_phone")
        if not phone:
            await state.clear()
            await callback.message.edit_text(
                "Сессия добавления потеряна. Начните добавление аккаунта заново.",
                reply_markup=main_menu(),
            )
            await callback.answer("Данные не найдены", show_alert=True)
            return
        await state.set_state(AccountAuth.email)
        await callback.message.edit_text(
            "📧 <b>Изменение почты</b>\n\n"
            f"Телефон: <code>{html.escape(str(phone))}</code>\n\n"
            "Отправьте корректный адрес почты. Пароль от почты не отправляйте.\n"
            "Отмена: /cancel",
            reply_markup=back_main(),
        )
        await callback.answer()

    async def account_resend_code(self, callback: CallbackQuery, state: FSMContext) -> None:
        if await self._deny_if_needed(callback):
            return
        pending = self.login_manager.get(callback.from_user.id)
        if pending is None:
            await state.clear()
            await callback.message.edit_text(
                "Сессия авторизации потеряна. Начните добавление аккаунта заново.",
                reply_markup=main_menu(),
            )
            await callback.answer("Сессия авторизации не найдена", show_alert=True)
            return
        try:
            pending, remaining = await self.login_manager.resend(callback.from_user.id)
        except errors.SendCodeUnavailableError:
            logger.warning(
                "Telegram rejected code resend because no delivery method is available admin_id=%s",
                callback.from_user.id,
            )
            await callback.answer(
                "Telegram не разрешил повторную отправку. Используйте последний код или выберите другой номер.",
                show_alert=True,
            )
            return
        except Exception as exc:  # noqa: BLE001
            logger.exception("Не удалось повторно запросить код")
            await callback.answer(
                f"Не удалось запросить код: {truncate(str(exc), 120)}",
                show_alert=True,
            )
            return
        if remaining > 0:
            await callback.answer(f"Повторный запрос будет доступен через {remaining} сек.", show_alert=True)
            return
        await state.set_state(AccountAuth.code)
        await callback.message.edit_text(self._login_code_message(pending), reply_markup=login_code_actions())
        await callback.answer("Новый код запрошен")

    async def account_restart_phone(self, callback: CallbackQuery, state: FSMContext) -> None:
        if await self._deny_if_needed(callback):
            return
        await self.login_manager.cancel(callback.from_user.id)
        await state.clear()
        await state.set_state(AccountAuth.phone)
        await self._show_account_phone_prompt(callback.message)
        await callback.answer()

    async def account_code(self, message: Message, state: FSMContext) -> None:
        if await self._deny_if_needed(message):
            return
        raw_code = message.text or ""
        await _delete_sensitive_message(message)
        pending = self.login_manager.get(message.from_user.id)
        if not pending:
            await state.clear()
            await message.answer("Сессия авторизации потеряна. Начните добавление аккаунта заново.", reply_markup=main_menu())
            return
        code = re.sub(r"\D", "", raw_code)
        if not code:
            await message.answer("Отправьте числовой код.")
            return
        try:
            await pending.client.sign_in(
                phone=pending.phone,
                code=code,
                phone_code_hash=pending.phone_code_hash,
            )
        except errors.SessionPasswordNeededError:
            await state.set_state(AccountAuth.password)
            await message.answer("🔐 На аккаунте включена 2FA. Отправьте пароль.\nОтмена: /cancel")
            return
        except errors.PhoneCodeInvalidError:
            await message.answer(
                "❌ Код неверный. Проверьте последнее сообщение от Telegram и отправьте код ещё раз.",
                reply_markup=login_code_actions(),
            )
            return
        except errors.PhoneCodeExpiredError:
            await message.answer(
                "⌛ Код истёк. Нажмите «Отправить код повторно».",
                reply_markup=login_code_actions(),
            )
            return
        except Exception as exc:  # noqa: BLE001
            logger.exception("Ошибка кода авторизации")
            await message.answer(
                f"❌ Ошибка авторизации: <code>{html.escape(str(exc))}</code>",
                reply_markup=login_code_actions(),
            )
            return
        await self._save_authorized_account(message, state)

    async def account_password(self, message: Message, state: FSMContext) -> None:
        if await self._deny_if_needed(message):
            return
        password = message.text or ""
        await _delete_sensitive_message(message)
        pending = self.login_manager.get(message.from_user.id)
        if not pending:
            await state.clear()
            await message.answer("Сессия авторизации потеряна.", reply_markup=main_menu())
            return
        try:
            await pending.client.sign_in(password=password)
        except Exception:  # noqa: BLE001
            logger.exception("Ошибка 2FA без сохранения введённого значения")
            await message.answer(
                "❌ Пароль 2FA не принят. Проверьте его и отправьте ещё раз."
            )
            return
        await self._save_authorized_account(message, state)

    async def _save_authorized_account(self, message: Message, state: FSMContext) -> None:
        admin_id = message.from_user.id
        pending = self.login_manager.get(admin_id)
        if not pending:
            return
        get_state_data = getattr(state, "get_data", None)
        state_data = await get_state_data() if get_state_data is not None else {}
        email_login = state_data.get("account_email")
        email_provider = state_data.get("account_email_provider")
        reauth_account_id = state_data.get("account_reauth_id")

        if reauth_account_id is not None:
            target = await self.db.get_account(int(reauth_account_id))
            if target is None or target.status != "unauthorized":
                revoked = await _revoke_pending_login(self.login_manager, admin_id)
                await state.clear()
                cleanup_text = (
                    "Временная сессия отозвана."
                    if revoked
                    else "Временная сессия закрыта; проверьте активные сеансы Telegram."
                )
                await message.answer(
                    "ℹ️ <b>Повторная авторизация больше не требуется</b>\n\n"
                    "Аккаунт уже восстановлен, удалён или его статус изменился. "
                    + cleanup_text,
                    reply_markup=main_menu(),
                )
                return

        try:
            me = await pending.client.get_me()
            session_string = pending.client.session.save()
            encrypted = self.cipher.encrypt(session_string)
            display_name = " ".join(
                part for part in (me.first_name, me.last_name) if part
            ) or str(me.id)
            account = await self.db.upsert_account(
                phone=pending.phone,
                telegram_user_id=int(me.id),
                display_name=display_name,
                username=me.username,
                session_encrypted=encrypted,
                email_login=str(email_login) if email_login else None,
                email_provider=str(email_provider) if email_provider else None,
                expected_account_id=(
                    int(reauth_account_id) if reauth_account_id is not None else None
                ),
            )
            if reauth_account_id is not None and account.id != int(reauth_account_id):
                raise AccountIdentityConflictError(
                    phone=pending.phone,
                    stored_account_id=int(reauth_account_id),
                    stored_telegram_user_id=None,
                    incoming_telegram_user_id=int(me.id),
                )
        except AccountIdentityConflictError:
            logger.warning(
                "Telegram identity conflict blocked during account save admin_id=%s",
                admin_id,
            )
            revoked = await _revoke_pending_login(self.login_manager, admin_id)
            await state.clear()
            cleanup_text = (
                "Временная Telegram-сессия отозвана.\n\n"
                if revoked
                else (
                    "Не удалось подтвердить отзыв временной сессии. "
                    "Проверьте активные сеансы Telegram.\n\n"
                )
            )
            await message.answer(
                "⛔ <b>Аккаунт не сохранён</b>\n\n"
                + cleanup_text
                + "Этот номер уже связан в LikeBot с другой Telegram-личностью. "
                "Старая запись, её история и задания не изменены.\n\n"
                "Чтобы подключить новый Telegram-аккаунт на этом номере, сначала "
                "проверьте и при необходимости удалите старую запись вручную.",
                reply_markup=main_menu(),
            )
            return
        except Exception:  # noqa: BLE001
            logger.exception(
                "Авторизация прошла, но аккаунт не удалось сохранить admin_id=%s",
                admin_id,
            )
            revoked = False
            try:
                revoked = await _revoke_pending_login(self.login_manager, admin_id)
            except Exception:  # noqa: BLE001
                logger.exception(
                    "Не удалось отозвать временную Telegram-сессию admin_id=%s",
                    admin_id,
                )
            await state.clear()
            cleanup_text = (
                "Временная Telegram-сессия отозвана. "
                if revoked
                else (
                    "Не удалось подтвердить отзыв временной сессии; "
                    "проверьте активные сеансы Telegram. "
                )
            )
            await message.answer(
                "❌ <b>Аккаунт не сохранён</b>\n\n"
                "Telegram подтвердил вход, но база данных не смогла сохранить аккаунт. "
                + cleanup_text
                + "Повторите добавление после устранения ошибки.",
                reply_markup=main_menu(),
            )
            return

        try:
            await self.login_manager.cancel(admin_id)
        except Exception:  # noqa: BLE001
            logger.exception(
                "Аккаунт сохранён, но временный клиент не отключился admin_id=%s",
                admin_id,
            )
        await state.clear()
        email_line = (
            f"\nПочта: <code>{html.escape(account.email_login)}</code>"
            if account.email_login
            else "\nПочта: <b>не указана</b>"
        )
        await message.answer(
            "✅ <b>Аккаунт подключён</b>\n\n"
            f"ID: <code>{account.telegram_user_id}</code>\n"
            f"Имя: {html.escape(display_account_name(account.display_name, account.username))}"
            f"{email_line}",
            reply_markup=main_menu(),
        )

    async def _render_account_list(self, message: Message, page: int = 0) -> None:
        accounts = await self.db.list_accounts()
        problem_accounts = [account for account in accounts if account.status == "unauthorized"]
        regular_accounts = [account for account in accounts if account.status != "unauthorized"]
        missing_email_accounts = [account for account in accounts if not account.email_login]
        active = sum(1 for account in regular_accounts if account.is_active)
        disabled = sum(1 for account in regular_accounts if not account.is_active)
        page_accounts, safe_page, total_pages = _paginate(regular_accounts, page)
        rows = [
            (account.id, display_account_name(account.display_name, account.username), account.is_active)
            for account in page_accounts
        ]
        page_line = (
            f"\n📄 Страница: <b>{safe_page + 1}/{total_pages}</b>"
            if len(regular_accounts) > ACCOUNT_PAGE_SIZE
            else ""
        )
        text = (
            "👥 <b>Мои аккаунты</b>\n"
            "<i>Подключённые Telegram-аккаунты</i>\n\n"
            "━━━━━━━━━━━━━━\n"
            f"🟢 Активны: <b>{active}</b>\n"
            f"⚫️ Выключены вручную: <b>{disabled}</b>\n"
            f"⚠️ Проблемные: <b>{len(problem_accounts)}</b>\n"
            f"📧 Без почты: <b>{len(missing_email_accounts)}</b>\n"
            f"📦 Всего: <b>{len(accounts)}</b>"
            f"{page_line}\n"
            "━━━━━━━━━━━━━━\n\n"
            + (
                "Выберите аккаунт 👇"
                if regular_accounts
                else (
                    "Обычных аккаунтов нет. Откройте проблемные 👇"
                    if problem_accounts
                    else "Аккаунтов пока нет. Добавьте первый аккаунт 👇"
                )
            )
        )
        await self._safe_edit_text(
            message,
            text,
            reply_markup=account_list_keyboard(
                rows,
                problem_count=len(problem_accounts),
                missing_email_count=len(missing_email_accounts),
                page=safe_page,
                total_pages=total_pages,
            ),
        )

    @staticmethod
    def _problem_context_label(context: str | None) -> str:
        labels = {
            "health-check": "плановая проверка сессии",
            "manual-profile-refresh": "обновление данных аккаунтов",
            "manual-session-check": "ручная проверка сессии",
            "manual-view": "добавление просмотров",
            "reaction": "выполнение реакции",
            "add-target-probe": "проверка ссылки канала/группы",
            "add-target-finalize": "сохранение канала/группы",
        }
        if not context:
            return "неизвестно"
        if context.startswith("membership:"):
            action = context.split(":", 1)[1]
            return "подписка на канал/группу" if action == "join" else "отписка от канала/группы"
        if context.startswith("resolve-target:"):
            return "доступ к каналу/группе"
        return labels.get(context, context)

    async def _render_problem_account_list(self, message: Message, page: int = 0) -> None:
        accounts = await self.db.list_problem_accounts()
        page_accounts, safe_page, total_pages = _paginate(accounts, page)
        rows = [
            (account.id, display_account_name(account.display_name, account.username))
            for account in page_accounts
        ]
        page_line = (
            f"\n📄 Страница: <b>{safe_page + 1}/{total_pages}</b>"
            if len(accounts) > ACCOUNT_PAGE_SIZE
            else ""
        )
        text = (
            "⚠️ <b>Проблемные аккаунты</b>\n"
            "<i>Сессии сохранены и не удалены</i>\n\n"
            "━━━━━━━━━━━━━━\n"
            f"⚠️ Найдено: <b>{len(accounts)}</b>"
            f"{page_line}\n"
            "━━━━━━━━━━━━━━\n\n"
            + (
                "Выберите аккаунт, чтобы увидеть причину и восстановить доступ 👇"
                if accounts
                else "Проблемных аккаунтов нет ✅"
            )
        )
        await self._safe_edit_text(
            message,
            text,
            reply_markup=problem_account_list_keyboard(
                rows, page=safe_page, total_pages=total_pages
            ),
        )

    async def _render_problem_account_view(self, message: Message, account_id: int) -> bool:
        account = await self.db.get_account(account_id)
        if account is None or account.status != "unauthorized":
            return False
        health = evaluate_account_health(account)
        reason = html.escape(truncate(account.problem_reason or account.last_error or "Неизвестная ошибка"))
        context = html.escape(self._problem_context_label(account.problem_context))
        detected = format_utc_datetime(account.problem_detected_at)
        email_text = html.escape(account.email_login) if account.email_login else "не указана"
        provider_text = html.escape(account.email_provider) if account.email_provider else "-"
        note_text = html.escape(truncate(account.email_note, 120)) if account.email_note else "-"
        await self._safe_edit_text(
            message,
            "⚠️ <b>Проблемный аккаунт</b>\n"
            "━━━━━━━━━━━━━━\n"
            f"👤 Имя: <b>{html.escape(display_account_name(account.display_name, account.username))}</b>\n"
            f"📱 Телефон: <code>{html.escape(account.phone)}</code>\n"
            f"📧 Почта: <code>{email_text}</code>\n"
            f"🏷 Почтовый сервис: <b>{provider_text}</b>\n"
            f"📝 Примечание: <i>{note_text}</i>\n"
            f"🆔 Telegram ID: <code>{account.telegram_user_id}</code>\n"
            "🔌 Статус: <b>⚠️ Сессия недействительна</b>\n"
            f"🩺 Готовность: <b>{health.icon} {health.score}/100 — {health.label}</b>\n"
            f"🕒 Обнаружено: <b>{detected} UTC</b>\n"
            f"🔎 Где обнаружено: <b>{context}</b>\n"
            f"❌ Причина: <code>{reason}</code>\n"
            "━━━━━━━━━━━━━━\n\n"
            "Аккаунт сохранён в базе, но исключён из подписок, просмотров и реакций.",
            reply_markup=problem_account_actions(account.id, has_email=bool(account.email_login)),
        )
        return True

    async def _render_account_view(self, message: Message, account_id: int) -> bool:
        account = await self.db.get_account(account_id)
        if not account:
            return False
        if account.status == "unauthorized":
            return await self._render_problem_account_view(message, account_id)
        status = "🟢 ВКЛ" if account.is_active else "⚫️ ВЫКЛ"
        health = evaluate_account_health(account)
        error = html.escape(truncate(account.last_error)) if account.last_error else "нет"
        email_text = html.escape(account.email_login) if account.email_login else "не указана"
        provider_text = html.escape(account.email_provider) if account.email_provider else "-"
        note_text = html.escape(truncate(account.email_note, 120)) if account.email_note else "-"
        await message.edit_text(
            "👤 <b>Карточка аккаунта</b>\n"
            "━━━━━━━━━━━━━━\n"
            f"👤 Имя: <b>{html.escape(display_account_name(account.display_name, account.username))}</b>\n"
            f"📱 Телефон: <code>{html.escape(account.phone)}</code>\n"
            f"📧 Почта: <code>{email_text}</code>\n"
            f"🏷 Почтовый сервис: <b>{provider_text}</b>\n"
            f"📝 Примечание: <i>{note_text}</i>\n"
            f"🆔 Telegram ID: <code>{account.telegram_user_id}</code>\n"
            f"🔌 Статус: <b>{status}</b>\n"
            f"🩺 Готовность: <b>{health.icon} {health.score}/100 — {health.label}</b>\n"
            f"⚠️ Последняя ошибка: <code>{error}</code>\n"
            "━━━━━━━━━━━━━━",
            reply_markup=account_actions(
                account.id,
                account.is_active,
                has_email=bool(account.email_login),
            ),
        )
        return True

    @staticmethod
    def _account_email_card_text(account) -> str:
        email_text = html.escape(account.email_login) if account.email_login else "не указана"
        provider_text = html.escape(account.email_provider) if account.email_provider else "-"
        note_text = html.escape(truncate(account.email_note, 300)) if account.email_note else "-"
        updated_text = (
            f"{format_utc_datetime(account.email_updated_at)} UTC"
            if account.email_updated_at
            else "-"
        )
        return (
            "📧 <b>Почта аккаунта</b>\n"
            "━━━━━━━━━━━━━━\n"
            f"👤 Аккаунт: <b>{html.escape(display_account_name(account.display_name, account.username))}</b>\n"
            f"📱 Телефон: <code>{html.escape(account.phone)}</code>\n"
            f"📧 Логин почты: <code>{email_text}</code>\n"
            f"🏷 Сервис: <b>{provider_text}</b>\n"
            f"📝 Примечание: <i>{note_text}</i>\n"
            f"🕒 Обновлено: <b>{updated_text}</b>\n"
            "━━━━━━━━━━━━━━\n\n"
            "Пароль почты, резервные коды и одноразовые коды бот не хранит."
        )

    async def _render_account_email_view(self, message: Message, account_id: int) -> bool:
        account = await self.db.get_account(account_id)
        if account is None:
            return False
        await self._safe_edit_text(
            message,
            self._account_email_card_text(account),
            reply_markup=account_email_management_keyboard(
                account.id,
                has_email=bool(account.email_login),
            ),
        )
        return True

    async def _render_missing_email_account_list(
        self, message: Message, page: int = 0
    ) -> None:
        accounts = await self.db.list_accounts_without_email()
        page_accounts, safe_page, total_pages = _paginate(accounts, page)
        rows = [
            (
                account.id,
                f"{display_account_name(account.display_name, account.username)} · {account.phone}",
            )
            for account in page_accounts
        ]
        page_line = (
            f"\n📄 Страница: <b>{safe_page + 1}/{total_pages}</b>"
            if len(accounts) > ACCOUNT_PAGE_SIZE
            else ""
        )
        text = (
            "📧 <b>Аккаунты без указанной почты</b>\n"
            "<i>Добавьте адрес для восстановления и учёта</i>\n\n"
            "━━━━━━━━━━━━━━\n"
            f"Без почты: <b>{len(accounts)}</b>"
            f"{page_line}\n"
            "━━━━━━━━━━━━━━\n\n"
            + (
                "Выберите аккаунт 👇"
                if accounts
                else "У всех аккаунтов почта уже указана ✅"
            )
        )
        await self._safe_edit_text(
            message,
            text,
            reply_markup=missing_email_account_list_keyboard(
                rows, page=safe_page, total_pages=total_pages
            ),
        )

    async def _account_management_payload(
        self, *, user_id: int, filter_key: str, page: int
    ) -> tuple[str, object]:
        if filter_key not in ACCOUNT_FILTERS:
            raise ValueError("Недопустимый фильтр аккаунтов")
        query = self._account_management_queries.get(user_id)
        accounts = await self.db.list_accounts()
        current = utcnow()
        filtered = [
            account
            for account in accounts
            if account_matches(
                account, filter_key=filter_key, query=query, now=current
            )
        ]
        page_accounts, safe_page, total_pages = _paginate(filtered, page)
        rows: list[tuple[int, str, str]] = []
        for account in page_accounts:
            if account.status == "unauthorized":
                icon = "⚠️"
            elif account.flood_until and account.flood_until > current:
                icon = "⏳"
            else:
                icon = "🟢" if account.is_active else "⚫️"
            rows.append(
                (
                    account.id,
                    truncate(
                        display_account_name(account.display_name, account.username),
                        42,
                    ),
                    icon,
                )
            )
        active = sum(
            1
            for account in accounts
            if account.is_active and account.status != "unauthorized"
        )
        disabled = sum(
            1
            for account in accounts
            if not account.is_active and account.status != "unauthorized"
        )
        problem = sum(1 for account in accounts if account.status == "unauthorized")
        flood = sum(
            1
            for account in accounts
            if account.flood_until and account.flood_until > current
        )
        query_line = (
            f"\n🔎 Поиск: <code>{html.escape(query)}</code>"
            if query
            else ""
        )
        page_line = (
            f"\n📄 Страница: <b>{safe_page + 1}/{total_pages}</b>"
            if len(filtered) > ACCOUNT_PAGE_SIZE
            else ""
        )
        text = (
            "🔎 <b>Управление аккаунтами</b>\n"
            f"Фильтр: <b>{ACCOUNT_FILTERS[filter_key]}</b>"
            f"{query_line}\n\n"
            "━━━━━━━━━━━━━━\n"
            f"🟢 Активны: <b>{active}</b>  ·  ⚫️ выключены: <b>{disabled}</b>\n"
            f"⚠️ Проблемные: <b>{problem}</b>  ·  ⏳ FloodWait: <b>{flood}</b>\n"
            f"📋 Найдено: <b>{len(filtered)}</b> из {len(accounts)}"
            f"{page_line}\n"
            "━━━━━━━━━━━━━━\n\n"
            + ("Выберите аккаунт 👇" if filtered else "По этому фильтру ничего не найдено.")
        )
        keyboard = account_management_keyboard(
            rows,
            filter_key=filter_key,
            query_active=bool(query),
            page=safe_page,
            total_pages=total_pages,
        )
        return text, keyboard

    async def account_management_view(
        self, callback: CallbackQuery, state: FSMContext
    ) -> None:
        if await self._deny_if_needed(callback):
            return
        try:
            filter_key, page = parse_account_management_callback(callback.data)
        except ValueError as exc:
            await callback.answer(str(exc), show_alert=True)
            return
        await state.set_state(None)
        text, keyboard = await self._account_management_payload(
            user_id=callback.from_user.id, filter_key=filter_key, page=page
        )
        await self._safe_edit_text(callback.message, text, reply_markup=keyboard)
        await callback.answer()

    async def account_management_search(
        self, callback: CallbackQuery, state: FSMContext
    ) -> None:
        if await self._deny_if_needed(callback):
            return
        parts = (callback.data or "").split(":")
        if len(parts) != 3 or parts[:2] != ["manage", "as"] or parts[2] not in ACCOUNT_FILTERS:
            await callback.answer("Некорректная кнопка поиска", show_alert=True)
            return
        await state.set_state(ManagementSearch.accounts)
        await state.update_data(management_account_filter=parts[2])
        await callback.message.edit_text(
            "🔎 <b>Поиск аккаунта</b>\n\n"
            "Отправьте имя, @username, номер телефона, email или Telegram ID.\n"
            "Запрос хранится только в памяти до перезапуска и не записывается в базу/логи.\n\n"
            "Отмена: /cancel",
            reply_markup=back_main(),
        )
        await callback.answer()

    async def account_management_search_input(
        self, message: Message, state: FSMContext
    ) -> None:
        if await self._deny_if_needed(message):
            return
        data = await state.get_data()
        filter_key = str(data.get("management_account_filter") or "all")
        if filter_key not in ACCOUNT_FILTERS:
            filter_key = "all"
        try:
            query = normalize_management_query(message.text)
        except ValueError as exc:
            await message.answer(f"❌ {html.escape(str(exc))}")
            return
        self._account_management_queries[message.from_user.id] = query
        await state.clear()
        text, keyboard = await self._account_management_payload(
            user_id=message.from_user.id, filter_key=filter_key, page=0
        )
        await message.answer(text, reply_markup=keyboard)

    async def account_management_clear_search(
        self, callback: CallbackQuery, state: FSMContext
    ) -> None:
        if await self._deny_if_needed(callback):
            return
        parts = (callback.data or "").split(":")
        if len(parts) != 3 or parts[:2] != ["manage", "ac"] or parts[2] not in ACCOUNT_FILTERS:
            await callback.answer("Некорректная кнопка", show_alert=True)
            return
        self._account_management_queries.pop(callback.from_user.id, None)
        await state.set_state(None)
        text, keyboard = await self._account_management_payload(
            user_id=callback.from_user.id, filter_key=parts[2], page=0
        )
        await self._safe_edit_text(callback.message, text, reply_markup=keyboard)
        await callback.answer("Поиск сброшен")

    async def _render_account_bulk_menu(self, message: Message) -> None:
        accounts = await self.db.list_accounts()
        regular = [item for item in accounts if item.status != "unauthorized"]
        await self._safe_edit_text(
            message,
            "⚙️ <b>Массовые действия с аккаунтами</b>\n\n"
            f"Обычных аккаунтов: <b>{len(regular)}</b>\n"
            f"Активных: <b>{sum(1 for item in regular if item.is_active)}</b>\n"
            f"Выключенных: <b>{sum(1 for item in regular if not item.is_active)}</b>\n"
            f"Проблемных, которые не будут включены: <b>{len(accounts) - len(regular)}</b>\n\n"
            "Выключение отменяет только ожидающие задания. Уже выполняемая Telegram-операция "
            "не прерывается посередине и завершится безопасно.",
            reply_markup=account_bulk_actions_keyboard(),
        )

    async def account_bulk_menu(self, callback: CallbackQuery) -> None:
        if await self._deny_if_needed(callback):
            return
        await self._render_account_bulk_menu(callback.message)
        await callback.answer()

    async def account_bulk_prepare(self, callback: CallbackQuery) -> None:
        if await self._deny_if_needed(callback):
            return
        try:
            action = parse_bulk_account_action(callback.data, confirmed=False)
        except ValueError as exc:
            await callback.answer(str(exc), show_alert=True)
            return
        if action in {"audit", "refresh"}:
            if self._account_bulk_lock.locked():
                await callback.answer("Массовая операция уже выполняется", show_alert=True)
                return
            await callback.answer("Запускаю проверку…")
            try:
                async with self._account_bulk_lock:
                    if action == "audit":
                        result = await self.jobs.audit_accounts_once()
                        summary = (
                            f"Проверено: {result['checked']} · в проблемные: {result['quarantined']} "
                            f"· временных ошибок: {result['transient_errors']}"
                        )
                    else:
                        result = await self.jobs.refresh_account_profiles_once()
                        summary = (
                            f"Проверено: {result['checked']} · обновлено: {result['updated']} "
                            f"· в проблемные: {result['quarantined']} · ошибок: {result['transient_errors']}"
                        )
            except Exception:  # noqa: BLE001
                logger.exception("Массовая проверка аккаунтов завершилась ошибкой action=%s", action)
                await callback.message.answer(
                    "❌ <b>Массовая проверка не завершена</b>\n\n"
                    "Подробности безопасно записаны в журнал приложения."
                )
                return
            await self._render_account_bulk_menu(callback.message)
            await callback.message.answer(f"✅ <b>Массовая операция завершена</b>\n\n{summary}")
            return
        label = "включить" if action == "enable" else "выключить"
        await callback.message.edit_text(
            f"⚠️ <b>Подтвердите массовое действие</b>\n\n"
            f"Будут {label} все обычные аккаунты. Проблемные записи останутся в карантине.",
            reply_markup=account_bulk_confirm_keyboard(action),
        )
        await callback.answer()

    async def _drop_idle_account_clients(self, account_ids: list[int]) -> tuple[int, int]:
        """Disconnect only clients whose per-account action lock becomes available quickly.

        A busy lock means a Telegram operation is already running. Such a client is
        deliberately left connected so the in-flight action is not interrupted.
        """

        async def drop_one(account_id: int) -> bool:
            lock = self.pool.lock_for(account_id)
            try:
                await asyncio.wait_for(lock.acquire(), timeout=0.05)
            except TimeoutError:
                return False
            try:
                account = await self.db.get_account(account_id)
                if account is None or account.is_active:
                    return False
                await self.pool.drop_while_locked(account_id)
                return True
            finally:
                lock.release()

        if not account_ids:
            return 0, 0
        results = await asyncio.gather(
            *(drop_one(int(account_id)) for account_id in account_ids),
            return_exceptions=True,
        )
        disconnected = 0
        skipped = 0
        for account_id, result in zip(account_ids, results, strict=True):
            if isinstance(result, BaseException):
                skipped += 1
                logger.error(
                    "Не удалось безопасно закрыть клиент после массового выключения account_id=%s: %s",
                    account_id,
                    type(result).__name__,
                )
            elif result:
                disconnected += 1
            else:
                skipped += 1
        return disconnected, skipped

    async def account_bulk_apply(self, callback: CallbackQuery) -> None:
        if await self._deny_if_needed(callback):
            return
        try:
            action = parse_bulk_account_action(callback.data, confirmed=True)
        except ValueError as exc:
            await callback.answer(str(exc), show_alert=True)
            return
        if action not in {"enable", "disable"}:
            await callback.answer("Эта операция не требует подтверждения", show_alert=True)
            return
        if self._account_bulk_lock.locked():
            await callback.answer("Массовая операция уже выполняется", show_alert=True)
            return
        await callback.answer("Применяю изменения…")
        disconnected = skipped_disconnect = 0
        try:
            async with self._account_bulk_lock:
                result = await self.db.bulk_set_accounts_active(action == "enable")
                if action == "disable":
                    disconnected, skipped_disconnect = await self._drop_idle_account_clients(
                        [int(item) for item in result["account_ids"]]
                    )
        except Exception:  # noqa: BLE001
            logger.exception("Массовое изменение аккаунтов завершилось ошибкой action=%s", action)
            await callback.message.edit_text(
                "❌ <b>Массовое действие не завершено</b>\n\n"
                "Изменения не подтверждены. Подробности безопасно записаны в журнал.",
                reply_markup=account_bulk_actions_keyboard(),
            )
            return
        text, keyboard = await self._account_management_payload(
            user_id=callback.from_user.id, filter_key="all", page=0
        )
        await self._safe_edit_text(callback.message, text, reply_markup=keyboard)
        cancelled = (
            int(result["cancelled_join"])
            + int(result["cancelled_reaction"])
            + int(result["cancelled_view"])
        )
        disconnect_line = (
            f"\nКлиентов закрыто безопасно: <b>{disconnected}</b>"
            f" · занятых оставлено: <b>{skipped_disconnect}</b>"
            if action == "disable"
            else ""
        )
        await callback.message.answer(
            "✅ <b>Массовое действие выполнено</b>\n\n"
            f"Изменено аккаунтов: <b>{result['changed']}</b>\n"
            f"Проблемных пропущено: <b>{result['skipped_problem']}</b>\n"
            f"Ожидающих заданий отменено: <b>{cancelled}</b>"
            f"{disconnect_line}"
        )

    async def account_list(self, callback: CallbackQuery) -> None:
        if await self._deny_if_needed(callback):
            return
        page = _page_from_callback(callback.data, "account:list_page:")
        await self._render_account_list(callback.message, page)
        await callback.answer()

    async def account_refresh(self, callback: CallbackQuery) -> None:
        if await self._deny_if_needed(callback):
            return
        page = _page_from_callback(callback.data, "account:refresh_page:")
        await callback.answer("Обновляю данные аккаунтов…")
        result = await self.jobs.refresh_account_profiles_once()
        await self._render_account_list(callback.message, page)
        summary = (
            f"Проверено: {result['checked']} · обновлено: {result['updated']}"
            f" · в проблемные: {result['quarantined']} · ошибок: {result['transient_errors']}"
        )
        await callback.message.answer(f"🔄 <b>Аккаунты обновлены</b>\n\n{summary}")

    async def account_missing_email_list(self, callback: CallbackQuery) -> None:
        if await self._deny_if_needed(callback):
            return
        page = _page_from_callback(callback.data, "account:email_missing_page:")
        await self._render_missing_email_account_list(callback.message, page)
        await callback.answer()

    async def account_email_view(self, callback: CallbackQuery) -> None:
        if await self._deny_if_needed(callback):
            return
        account_id = int(callback.data.rsplit(":", 1)[1])
        if not await self._render_account_email_view(callback.message, account_id):
            await callback.answer("Аккаунт не найден", show_alert=True)
            return
        await callback.answer()

    async def account_email_edit(self, callback: CallbackQuery, state: FSMContext) -> None:
        if await self._deny_if_needed(callback):
            return
        account_id = int(callback.data.rsplit(":", 1)[1])
        account = await self.db.get_account(account_id)
        if account is None:
            await callback.answer("Аккаунт не найден", show_alert=True)
            return
        await self.login_manager.cancel(callback.from_user.id)
        await state.clear()
        await state.update_data(email_account_id=account.id)
        await state.set_state(AccountEmailEdit.address)
        current = (
            f"\nТекущий адрес: <code>{html.escape(account.email_login)}</code>\n"
            if account.email_login
            else "\nТекущий адрес: <b>не указан</b>\n"
        )
        await callback.message.edit_text(
            "📧 <b>Изменение почты аккаунта</b>\n\n"
            f"👤 {html.escape(display_account_name(account.display_name, account.username))}\n"
            f"📱 <code>{html.escape(account.phone)}</code>"
            f"{current}\n"
            "Отправьте новый адрес почты. Пароль и коды не отправляйте.\n"
            "Отмена: /cancel",
            reply_markup=back_main(),
        )
        await callback.answer()

    async def account_email_address_input(self, message: Message, state: FSMContext) -> None:
        if await self._deny_if_needed(message):
            return
        data = await state.get_data()
        account_id = data.get("email_account_id")
        if not account_id:
            await state.clear()
            await message.answer(
                "Сессия изменения почты потеряна.", reply_markup=main_menu()
            )
            return
        try:
            email_login, email_provider = normalize_email_login(message.text or "")
        except ValueError as exc:
            await message.answer(
                f"❌ {html.escape(str(exc))}\n\n"
                "Пример: <code>account@gmail.com</code>"
            )
            return
        updated = await self.db.update_account_email(
            int(account_id),
            email_login=email_login,
            email_provider=email_provider,
        )
        await state.clear()
        if not updated:
            await message.answer("Аккаунт не найден.", reply_markup=main_menu())
            return
        account = await self.db.get_account(int(account_id))
        if account is None:
            await message.answer("Аккаунт не найден.", reply_markup=main_menu())
            return
        await message.answer(
            "✅ <b>Почта сохранена</b>\n\n" + self._account_email_card_text(account),
            reply_markup=account_email_management_keyboard(account.id, has_email=True),
        )

    async def account_email_note(self, callback: CallbackQuery, state: FSMContext) -> None:
        if await self._deny_if_needed(callback):
            return
        account_id = int(callback.data.rsplit(":", 1)[1])
        account = await self.db.get_account(account_id)
        if account is None:
            await callback.answer("Аккаунт не найден", show_alert=True)
            return
        if not account.email_login:
            await callback.answer("Сначала добавьте адрес почты", show_alert=True)
            return
        await self.login_manager.cancel(callback.from_user.id)
        await state.clear()
        await state.update_data(email_account_id=account.id)
        await state.set_state(AccountEmailEdit.note)
        current = html.escape(truncate(account.email_note, 300)) if account.email_note else "-"
        await callback.message.edit_text(
            "📝 <b>Примечание к почте</b>\n\n"
            f"📧 <code>{html.escape(account.email_login)}</code>\n"
            f"Текущее: <i>{current}</i>\n\n"
            "Отправьте короткое несекретное примечание до 500 символов.\n"
            "Отправьте <code>-</code>, чтобы очистить примечание.\n\n"
            "Не указывайте пароль, резервные или одноразовые коды.\n"
            "Отмена: /cancel",
            reply_markup=back_main(),
        )
        await callback.answer()

    async def account_email_note_input(self, message: Message, state: FSMContext) -> None:
        if await self._deny_if_needed(message):
            return
        data = await state.get_data()
        account_id = data.get("email_account_id")
        if not account_id:
            await state.clear()
            await message.answer(
                "⚠️ Сессия изменения примечания потеряна. Откройте карточку "
                "аккаунта и повторите действие.",
                reply_markup=main_menu(),
            )
            return
        raw = " ".join((message.text or "").split())
        note = None if raw in {"", "-"} else raw
        try:
            updated = await self.db.update_account_email_note(int(account_id), note)
        except ValueError as exc:
            await message.answer(f"❌ {html.escape(str(exc))}")
            return
        if not updated:
            await state.clear()
            account = await self.db.get_account(int(account_id))
            if account is None:
                await message.answer("Аккаунт не найден.", reply_markup=main_menu())
                return
            await message.answer(
                "⚠️ Почта уже удалена, поэтому примечание не сохранено.\n\n"
                + self._account_email_card_text(account),
                reply_markup=account_email_management_keyboard(
                    account.id, has_email=bool(account.email_login)
                ),
            )
            return
        await state.clear()
        account = await self.db.get_account(int(account_id))
        if account is None:
            await message.answer("Аккаунт не найден.", reply_markup=main_menu())
            return
        await message.answer(
            "✅ <b>Примечание обновлено</b>\n\n" + self._account_email_card_text(account),
            reply_markup=account_email_management_keyboard(
                account.id,
                has_email=bool(account.email_login),
            ),
        )

    async def account_email_delete_confirm(self, callback: CallbackQuery) -> None:
        if await self._deny_if_needed(callback):
            return
        account_id = int(callback.data.rsplit(":", 1)[1])
        account = await self.db.get_account(account_id)
        if account is None:
            await callback.answer("Аккаунт не найден", show_alert=True)
            return
        if not account.email_login:
            await self._render_account_email_view(callback.message, account.id)
            await callback.answer("Почта уже не указана", show_alert=True)
            return
        await callback.message.edit_text(
            "⚠️ <b>Удалить почту из карточки?</b>\n\n"
            f"Аккаунт: <b>{html.escape(display_account_name(account.display_name, account.username))}</b>\n"
            f"Почта: <code>{html.escape(account.email_login)}</code>\n\n"
            "Telegram-сессия и сам аккаунт удалены не будут.",
            reply_markup=confirm_account_email_delete(account.id),
        )
        await callback.answer()

    async def account_email_delete(self, callback: CallbackQuery) -> None:
        if await self._deny_if_needed(callback):
            return
        account_id = int(callback.data.rsplit(":", 1)[1])
        if not await self.db.clear_account_email(account_id):
            await callback.answer("Аккаунт не найден", show_alert=True)
            return
        await self._render_account_view(callback.message, account_id)
        await callback.answer("Почта удалена из карточки")

    async def problem_account_list(self, callback: CallbackQuery) -> None:
        if await self._deny_if_needed(callback):
            return
        page = _page_from_callback(callback.data, "account:problems_page:")
        await self._render_problem_account_list(callback.message, page)
        await callback.answer()

    async def problem_account_view(self, callback: CallbackQuery) -> None:
        if await self._deny_if_needed(callback):
            return
        account_id = int(callback.data.rsplit(":", 1)[1])
        if not await self._render_problem_account_view(callback.message, account_id):
            await self._render_account_list(callback.message)
            await callback.answer("Аккаунт уже восстановлен или удалён", show_alert=True)
            return
        await callback.answer()

    async def problem_account_check(self, callback: CallbackQuery) -> None:
        if await self._deny_if_needed(callback):
            return
        account_id = int(callback.data.rsplit(":", 1)[1])
        account = await self.db.get_account(account_id)
        if account is None:
            await callback.answer("Аккаунт не найден", show_alert=True)
            return
        await callback.answer("Проверяю сессию…")
        try:
            async with self.pool.lock_for(account.id):
                was_connected = self.pool.has_connected_client(account.id)
                try:
                    await self.pool.ensure_authorized(account)
                    restored = await self.db.restore_problem_account(account.id)
                except ACCOUNT_AUTH_FAILURES:
                    await self.pool.remove_unauthorized_account_while_locked(
                        account.id, context="manual-session-check"
                    )
                    raise
                finally:
                    if (
                        not was_connected
                        and self.pool.has_connected_client(account.id)
                    ):
                        await self.pool.disconnect_client(account.id)
            await self._render_account_view(callback.message, account.id)
            if restored:
                await callback.message.answer(
                    "✅ <b>Аккаунт восстановлен</b>\n\n"
                    "Сессия снова авторизована, аккаунт возвращён в активный пул."
                )
            else:
                await callback.message.answer(
                    "ℹ️ Аккаунт уже был восстановлен или его статус изменился."
                )
        except ACCOUNT_AUTH_FAILURES:
            await self._render_problem_account_view(callback.message, account.id)
            await callback.message.answer(
                "⚠️ Сессия по-прежнему недействительна. Выполните повторную авторизацию."
            )
        except Exception as exc:  # noqa: BLE001
            logger.exception("Ошибка повторной проверки проблемного аккаунта=%s", account.id)
            await callback.message.answer(
                "⚠️ <b>Временная ошибка проверки</b>\n\n"
                f"<code>{html.escape(truncate(str(exc), 300))}</code>\n\n"
                "Статус аккаунта не изменён."
            )

    async def problem_account_check_all(self, callback: CallbackQuery) -> None:
        if await self._deny_if_needed(callback):
            return
        accounts = await self.db.list_problem_accounts()
        if not accounts:
            await self._render_problem_account_list(callback.message)
            await callback.answer("Проблемных аккаунтов нет", show_alert=True)
            return
        await callback.answer("Проверяю проблемные аккаунты…")
        restored = 0
        still_problem = 0
        transient_errors = 0
        for account in accounts:
            try:
                async with self.pool.lock_for(account.id):
                    was_connected = self.pool.has_connected_client(account.id)
                    try:
                        await self.pool.ensure_authorized(account)
                        restored_now = await self.db.restore_problem_account(account.id)
                    except ACCOUNT_AUTH_FAILURES:
                        await self.pool.remove_unauthorized_account_while_locked(
                            account.id, context="manual-session-check"
                        )
                        raise
                    finally:
                        if (
                            not was_connected
                            and self.pool.has_connected_client(account.id)
                        ):
                            await self.pool.disconnect_client(account.id)
                restored += int(restored_now)
            except ACCOUNT_AUTH_FAILURES:
                still_problem += 1
            except asyncio.CancelledError:
                raise
            except Exception:  # noqa: BLE001
                transient_errors += 1
                logger.exception(
                    "Ошибка массовой проверки проблемного аккаунта=%s", account.id
                )
            await asyncio.sleep(0.05)
        await self._render_problem_account_list(callback.message)
        await callback.message.answer(
            "🔄 <b>Проверка завершена</b>\n\n"
            f"✅ Восстановлено: <b>{restored}</b>\n"
            f"⚠️ Остались проблемными: <b>{still_problem}</b>\n"
            f"🌐 Временных ошибок: <b>{transient_errors}</b>"
        )

    async def problem_accounts_clear_confirm(self, callback: CallbackQuery) -> None:
        if await self._deny_if_needed(callback):
            return
        accounts = await self.db.list_problem_accounts()
        if not accounts:
            await self._render_problem_account_list(callback.message)
            await callback.answer("Проблемных аккаунтов нет", show_alert=True)
            return
        await self._safe_edit_text(
            callback.message,
            "⚠️ <b>Очистить список проблемных аккаунтов?</b>\n\n"
            f"Будет окончательно удалено: <b>{len(accounts)}</b>\n\n"
            "Удалятся карточки аккаунтов, зашифрованные Telegram-сессии и "
            "связанная история заданий. Восстановить их через LikeBot будет нельзя.\n\n"
            "Аккаунты, которые успеют восстановиться до выполнения очистки, "
            "будут безопасно пропущены.",
            reply_markup=confirm_problem_accounts_clear(len(accounts)),
        )
        await callback.answer()

    async def problem_accounts_clear(
        self, callback: CallbackQuery, state: FSMContext
    ) -> None:
        if await self._deny_if_needed(callback):
            return
        accounts = await self.db.list_problem_accounts()
        if not accounts:
            await self._render_problem_account_list(callback.message)
            await callback.answer("Список уже пуст", show_alert=True)
            return

        # An old confirmation message may be pressed while a re-authorization
        # flow is still open for the same administrator. Cancel that temporary
        # login before deleting its target row.
        await self.login_manager.cancel(callback.from_user.id)
        await state.clear()
        await callback.answer("Очищаю список…")

        deleted = 0
        skipped = 0
        errors = 0
        for account in sorted(accounts, key=lambda item: int(item.id)):
            account_id = int(account.id)
            try:
                async with self.pool.lock_for(account_id):
                    # A re-authorization callback may have started while this
                    # cleanup was waiting for the account lock. Cancel it again
                    # inside the same critical section before deleting the row.
                    cancel_if_phone = getattr(
                        self.login_manager, "cancel_if_phone", None
                    )
                    account_phone = getattr(account, "phone", None)
                    if cancel_if_phone is not None and account_phone:
                        await cancel_if_phone(
                            callback.from_user.id, str(account_phone)
                        )
                    removed = await self.db.delete_invalid_account(
                        account_id, require_problem=True
                    )
                    if removed:
                        await self.pool.drop_while_locked(account_id)
                        self._shown_login_message_ids.pop(account_id, None)
                        deleted += 1
                    else:
                        skipped += 1
            except asyncio.CancelledError:
                raise
            except Exception:  # noqa: BLE001
                errors += 1
                logger.exception(
                    "Ошибка очистки проблемного аккаунта=%s", account_id
                )

        await self._render_problem_account_list(callback.message)
        await callback.message.answer(
            "🧹 <b>Очистка завершена</b>\n\n"
            f"🗑 Удалено: <b>{deleted}</b>\n"
            f"✅ Восстановлено или уже удалено: <b>{skipped}</b>\n"
            f"⚠️ Ошибок: <b>{errors}</b>"
        )

    async def account_reauth(self, callback: CallbackQuery, state: FSMContext) -> None:
        if await self._deny_if_needed(callback):
            return
        account_id = int(callback.data.rsplit(":", 1)[1])
        account = await self.db.get_account(account_id)
        if account is None:
            await callback.answer("Аккаунт не найден", show_alert=True)
            return
        if account.status != "unauthorized":
            await self._render_account_view(callback.message, account.id)
            await callback.answer(
                "Аккаунт уже восстановлен. Повторная авторизация не запущена.",
                show_alert=True,
            )
            return
        await state.clear()
        await state.update_data(account_reauth_id=account.id)
        async with self.pool.lock_for(account.id):
            current = await self.db.get_account(account.id)
            if current is None or current.status != "unauthorized":
                await state.clear()
                await self._render_problem_account_list(callback.message)
                await callback.answer(
                    "Аккаунт уже восстановлен или удалён", show_alert=True
                )
                return
            await self.pool.drop_while_locked(account.id)
            try:
                login = await self.login_manager.start(
                    callback.from_user.id, current.phone
                )
            except Exception as exc:  # noqa: BLE001
                logger.exception(
                    "Не удалось начать повторную авторизацию account=%s", account.id
                )
                await callback.answer(
                    f"Не удалось запросить код: {truncate(str(exc), 120)}",
                    show_alert=True,
                )
                return
        await state.set_state(AccountAuth.code)
        await callback.message.edit_text(
            "🔐 <b>Повторная авторизация аккаунта</b>\n\n"
            f"👤 {html.escape(display_account_name(account.display_name, account.username))}\n"
            f"📱 <code>{html.escape(current.phone)}</code>\n\n"
            + self._login_code_message(login),
            reply_markup=login_code_actions(),
        )
        await callback.answer("Код входа запрошен")

    async def account_view(self, callback: CallbackQuery) -> None:
        if await self._deny_if_needed(callback):
            return
        account_id = int(callback.data.rsplit(":", 1)[1])
        if not await self._render_account_view(callback.message, account_id):
            await callback.answer("Аккаунт не найден", show_alert=True)
            return
        await callback.answer()

    async def account_toggle(self, callback: CallbackQuery) -> None:
        if await self._deny_if_needed(callback):
            return
        account_id = int(callback.data.rsplit(":", 1)[1])
        account = await self.db.get_account(account_id)
        if not account:
            await callback.answer("Аккаунт не найден", show_alert=True)
            return
        if account.status == "unauthorized":
            await self._render_problem_account_view(callback.message, account.id)
            await callback.answer(
                "Сначала восстановите или авторизуйте аккаунт заново",
                show_alert=True,
            )
            return
        async with self.pool.lock_for(account.id):
            await self.db.set_account_active(account.id, not account.is_active)
            await self.pool.drop_while_locked(account.id)
        await self._render_account_view(callback.message, account.id)
        await callback.answer("Статус аккаунта обновлён")

    async def account_delete_confirm(self, callback: CallbackQuery) -> None:
        if await self._deny_if_needed(callback):
            return
        account_id = int(callback.data.rsplit(":", 1)[1])
        await callback.message.edit_text(
            "⚠️ Удалить аккаунт и зашифрованную сессию из базы?",
            reply_markup=confirm_account_delete(account_id),
        )
        await callback.answer()

    async def account_delete(self, callback: CallbackQuery) -> None:
        if await self._deny_if_needed(callback):
            return
        account_id = int(callback.data.rsplit(":", 1)[1])
        async with self.pool.lock_for(account_id):
            await self.pool.drop_while_locked(account_id)
            await self.db.delete_account(account_id)
        self._shown_login_message_ids.pop(account_id, None)
        await self._render_account_list(callback.message)
        await callback.answer("Аккаунт удалён")

    async def account_login_code_prompt(self, callback: CallbackQuery) -> None:
        if await self._deny_if_needed(callback):
            return
        if (callback.data or "").startswith("account:login_code_check:"):
            return
        account_id = int(callback.data.rsplit(":", 1)[1])
        account = await self.db.get_account(account_id)
        if not account:
            await callback.answer("Аккаунт не найден", show_alert=True)
            return
        name = html.escape(display_account_name(account.display_name, account.username))
        phone = html.escape(account.phone)
        await callback.message.edit_text(
            "🔐 <b>Получение кода входа</b>\n\n"
            f"Аккаунт: <b>{name}</b>\n"
            f"Телефон: <code>{phone}</code>\n\n"
            "Сначала запросите вход на новом устройстве, указав номер этого аккаунта.\n\n"
            "После того как Telegram отправит код в служебный чат аккаунта, "
            "нажмите кнопку ниже.\n\n"
            "<i>Код не сохраняется в базе и не пишется в логи. "
            "Сообщение с кодом будет скрыто через 90 секунд.</i>",
            reply_markup=account_login_code_prompt_keyboard(account.id),
        )
        await callback.answer()

    async def account_login_code_check(self, callback: CallbackQuery) -> None:
        if await self._deny_if_needed(callback):
            return
        account_id = int(callback.data.rsplit(":", 1)[1])
        account = await self.db.get_account(account_id)
        if not account:
            await callback.answer("Аккаунт не найден", show_alert=True)
            return

        name = html.escape(display_account_name(account.display_name, account.username))
        phone = html.escape(account.phone)
        shown = self._shown_login_message_ids.setdefault(account.id, set())

        try:
            hit = await self.pool.fetch_recent_login_code(
                account,
                max_age_seconds=600,
                exclude_message_ids=shown,
                quarantine_context="login-code-check",
            )
        except ACCOUNT_AUTH_FAILURES:
            self._shown_login_message_ids.pop(account.id, None)
            await self._render_problem_account_view(callback.message, account.id)
            await callback.answer(
                "Сессия недействительна. Аккаунт сохранён в проблемных",
                show_alert=True,
            )
            return
        except Exception as exc:  # noqa: BLE001
            logger.exception(
                "Не удалось прочитать служебные сообщения account=%s", account.id
            )
            await callback.answer(
                f"Ошибка чтения: {truncate(str(exc), 100)}",
                show_alert=True,
            )
            return

        if hit is None:
            await callback.message.edit_text(
                "⏳ <b>Нового кода входа пока нет</b>\n\n"
                f"Аккаунт: <b>{name}</b>\n"
                f"Телефон: <code>{phone}</code>\n\n"
                "Сначала начните вход в Telegram на новом устройстве, "
                "используя номер этого аккаунта.\n\n"
                "После получения кода повторите проверку.\n"
                "<i>Учитываются только свежие служебные сообщения Telegram "
                "(не старше 10 минут), которые ещё не показывались.</i>",
                reply_markup=account_login_code_result_keyboard(account.id),
            )
            await callback.answer("Нового кода пока нет")
            return

        shown.add(hit.message_id)
        received = format_utc_datetime(hit.received_at)
        # Intentionally do NOT log hit.code or the full service message body.
        logger.info(
            "Login code delivered to admin account=%s service_message_id=%s",
            account.id,
            hit.message_id,
        )
        code_html = html.escape(hit.code)
        await callback.message.edit_text(
            "✅ <b>Получено новое сообщение от Telegram</b>\n\n"
            f"Аккаунт: <b>{name}</b>\n"
            f"Телефон: <code>{phone}</code>\n"
            f"Получено: <b>{received}</b>\n\n"
            f"Код входа: <code>{code_html}</code>\n\n"
            "Никому не передавайте этот код.\n"
            "Он действует только для текущей попытки входа.\n\n"
            "<i>Сообщение будет автоматически скрыто через 90 секунд.</i>",
            reply_markup=account_login_code_result_keyboard(account.id),
        )
        await callback.answer("Код получен")
        self._schedule_login_code_hide(
            callback.bot,
            callback.message.chat.id,
            callback.message.message_id,
            account.id,
        )

    def _schedule_login_code_hide(
        self,
        bot,
        chat_id: int,
        message_id: int,
        account_id: int,
    ) -> None:
        """After 90s, strip the one-time code from the admin chat message.

        The digits are never written to the database or application logs. If the
        message was already changed by navigation, the edit is silently ignored.
        """

        key = (chat_id, message_id)
        previous = self._login_code_hide_tasks.pop(key, None)
        if previous and not previous.done():
            previous.cancel()

        async def _hide() -> None:
            try:
                await asyncio.sleep(90)
                account = await self.db.get_account(account_id)
                if account is None:
                    with contextlib.suppress(Exception):
                        await bot.delete_message(chat_id, message_id)
                    return
                name = html.escape(
                    display_account_name(account.display_name, account.username)
                )
                phone = html.escape(account.phone)
                text_body = (
                    "🔒 <b>Код входа скрыт</b>\n\n"
                    f"Аккаунт: <b>{name}</b>\n"
                    f"Телефон: <code>{phone}</code>\n\n"
                    "Одноразовый код удалён из этого сообщения.\n"
                    "Если вход ещё не завершён, запросите новый код на устройстве "
                    "и нажмите «Проверить ещё раз»."
                )
                try:
                    await bot.edit_message_text(
                        text_body,
                        chat_id=chat_id,
                        message_id=message_id,
                        reply_markup=account_login_code_result_keyboard(account_id),
                    )
                except TelegramBadRequest as exc:
                    if "message is not modified" not in str(exc).casefold():
                        logger.debug(
                            "login code hide edit skipped account=%s reason=%s",
                            account_id,
                            type(exc).__name__,
                        )
            except asyncio.CancelledError:
                raise
            except Exception:  # noqa: BLE001
                logger.debug(
                    "login code hide task finished with error account=%s",
                    account_id,
                    exc_info=True,
                )
            finally:
                self._login_code_hide_tasks.pop(key, None)

        self._login_code_hide_tasks[key] = asyncio.create_task(
            _hide(), name=f"hide-login-code-{account_id}"
        )

    async def account_session_check(self, callback: CallbackQuery) -> None:
        if await self._deny_if_needed(callback):
            return
        account_id = int(callback.data.rsplit(":", 1)[1])
        account = await self.db.get_account(account_id)
        if not account:
            await callback.answer("Аккаунт не найден", show_alert=True)
            return
        try:
            async with self.pool.lock_for(account.id):
                was_connected = self.pool.has_connected_client(account.id)
                try:
                    await self.pool.ensure_authorized(account)
                    await self.db.restore_problem_account(account.id)
                except ACCOUNT_AUTH_FAILURES:
                    await self.pool.remove_unauthorized_account_while_locked(
                        account.id, context="manual-session-check"
                    )
                    raise
                finally:
                    if (
                        not was_connected
                        and self.pool.has_connected_client(account.id)
                    ):
                        await self.pool.disconnect_client(account.id)
            await callback.answer("Сессия авторизована ✅", show_alert=True)
            await self._render_account_view(callback.message, account.id)
        except ACCOUNT_AUTH_FAILURES:
            self._shown_login_message_ids.pop(account.id, None)
            await self._render_problem_account_view(callback.message, account.id)
            await callback.answer(
                "Сессия недействительна. Аккаунт сохранён в проблемных",
                show_alert=True,
            )
        except Exception as exc:  # noqa: BLE001
            logger.exception("Ошибка проверки сессии account=%s", account.id)
            await callback.answer(
                f"Временная ошибка: {truncate(str(exc), 100)}",
                show_alert=True,
            )
