from __future__ import annotations

from .handler_shared import *  # noqa: F403


class SystemHandlersMixin:
    async def _render_system_health(self, message: Message) -> None:
        stats = await self.db.stats()
        workers = self.jobs.worker_health_snapshot()
        worker_statuses = {str(item["status"]) for item in workers}
        backlog_total = (
            stats["join_backlog"]
            + stats["reaction_backlog"]
            + stats["view_backlog"]
        )
        stuck_running_total = (
            stats["join_stuck_running"]
            + stats["reaction_stuck_running"]
            + stats["view_stuck_running"]
        )

        if (
            stats["problem_accounts"]
            or {"stale", "blocked"} & worker_statuses
            or stuck_running_total
        ):
            overall_icon, overall_label = "🔴", "Нужна проверка"
        elif (
            {"warning", "starting"} & worker_statuses
            or stats["flood_accounts"]
            or stats["account_errors"]
            or stats["target_errors"]
            or backlog_total
        ):
            overall_icon, overall_label = "🟡", "Есть предупреждения"
        else:
            overall_icon, overall_label = "🟢", "Система работает штатно"

        worker_lines: list[str] = []
        status_view = {
            "ok": ("🟢", "работает"),
            "warning": ("🟡", "была ошибка"),
            "stale": ("🔴", "нет свежего сигнала"),
            "blocked": ("🔴", "зависшие задачи"),
            "starting": ("⚪️", "запускается"),
        }
        for item in workers:
            icon, label = status_view[str(item["status"])]
            detail = f" · {format_age_seconds(item['age_seconds'])}"
            if item["name"] == "reaction_worker" and item["running_tasks"]:
                detail += f" · задач {int(item['running_tasks'])}"
                if item["oldest_running_age_seconds"] is not None:
                    detail += (
                        " · старшая "
                        + format_age_seconds(int(item["oldest_running_age_seconds"]))
                    )
            worker_lines.append(
                f"{icon} {html.escape(str(item['label']))}: <b>{label}</b>{detail}"
            )
            if item["status"] in {"warning", "blocked"} and item["last_error"]:
                worker_lines.append(
                    f"   └ <code>{html.escape(truncate(str(item['last_error']), 120))}</code>"
                )
            elif item["last_error"] and item["last_recovered_at"]:
                worker_lines.append("   └ последняя ошибка устранена")

        problem_line = (
            f" · проблемных <b>{stats['problem_accounts']}</b>"
            if stats["problem_accounts"]
            else ""
        )
        warning_lines: list[str] = []
        if stats["flood_accounts"]:
            warning_lines.append(f"⏳ FloodWait: <b>{stats['flood_accounts']}</b>")
        if stats["account_errors"]:
            warning_lines.append(
                f"⚠️ Аккаунты с ошибкой: <b>{stats['account_errors']}</b>"
            )
        if stats["target_errors"]:
            warning_lines.append(
                f"📢 Каналы/группы с ошибкой: <b>{stats['target_errors']}</b>"
            )
        if backlog_total:
            warning_lines.append(
                f"🕒 Ожидают запуска более 5 минут: <b>{backlog_total}</b>"
            )
        if stuck_running_total:
            warning_lines.append(
                "🚨 Зависли в выполнении более 5 минут: "
                f"<b>{stuck_running_total}</b> "
                f"(подписки {stats['join_stuck_running']}, "
                f"реакции {stats['reaction_stuck_running']}, "
                f"просмотры {stats['view_stuck_running']})"
            )
        warning_block = (
            "\n\n⚠️ <b>Требует внимания</b>\n" + "\n".join(warning_lines)
            if warning_lines
            else ""
        )
        alert_settings = getattr(self, "settings", None)
        if bool(getattr(alert_settings, "alerts_enabled", True)):
            alert_interval = max(
                10, int(getattr(alert_settings, "alert_check_interval_seconds", 60))
            )
            alert_repeat = max(
                300, int(getattr(alert_settings, "alert_repeat_seconds", 3600))
            )
            alert_line = (
                "🔔 Критические уведомления: <b>включены</b>"
                f" · проверка {alert_interval} сек."
                f" · повтор {alert_repeat // 60} мин."
            )
        else:
            alert_line = "🔕 Критические уведомления: <b>выключены</b>"

        recovery_service = getattr(self, "recovery", None)
        recovery_enabled = bool(
            getattr(alert_settings, "auto_recovery_enabled", True)
        )
        if recovery_service is not None:
            recovery_state = recovery_service.snapshot()
            if recovery_enabled:
                recovery_interval = max(
                    15,
                    int(
                        getattr(
                            alert_settings, "recovery_check_interval_seconds", 60
                        )
                    ),
                )
                recovery_line = (
                    "🛠 Автовосстановление: <b>включено</b>"
                    f" · проверка {recovery_interval} сек."
                    f" · возвращено заданий {int(recovery_state.get('stuck_requeued') or 0)}"
                    f" · восстановлено аккаунтов {int(recovery_state.get('accounts_restored') or 0)}"
                )
                if recovery_state.get("last_error"):
                    recovery_line += (
                        "\n   └ <code>"
                        + html.escape(
                            truncate(str(recovery_state["last_error"]), 120)
                        )
                        + "</code>"
                    )
            else:
                recovery_line = "🧰 Автовосстановление: <b>выключено</b>"
        else:
            recovery_line = (
                "🛠 Автовосстановление: <b>включено</b>"
                if recovery_enabled
                else "🧰 Автовосстановление: <b>выключено</b>"
            )

        await self._safe_edit_text(
            message,
            "🩺 <b>Состояние LikeBot</b>\n"
            f"{overall_icon} <b>{overall_label}</b>\n\n"
            "━━━━━━━━━━━━━━\n"
            "👥 <b>Аккаунты</b>\n"
            f"Активны: <b>{stats['active_accounts']}</b> / {stats['accounts']}"
            f"{problem_line}\n"
            f"Выключены: <b>{stats['disabled_accounts']}</b>\n\n"
            "⚙️ <b>Фоновые процессы</b>\n"
            + "\n".join(worker_lines)
            + "\n\n"
            + alert_line
            + "\n"
            + recovery_line
            + "\n\n📦 <b>Очереди</b>\n"
            f"Подписки: <b>{stats['join_pending']}</b> ждут · {stats['join_running']} выполняется\n"
            f"Реакции: <b>{stats['reaction_pending']}</b> ждут · {stats['reaction_running']} выполняется\n"
            f"Просмотры: <b>{stats['view_pending']}</b> ждут · {stats['view_running']} выполняется"
            + warning_block
            + "\n━━━━━━━━━━━━━━\n"
            "<i>Задержанной считается задача, готовая к запуску более 5 минут.</i>",
            reply_markup=system_health_keyboard(),
        )

    async def _render_account_health_list(self, message: Message, page: int = 0) -> None:
        rows = await self.db.account_health_overview()
        prepared = [
            (row, evaluate_account_health(row["account"])) for row in rows
        ]
        prepared.sort(
            key=lambda item: (item[1].score, int(item[0]["account"].id))
        )
        page_items, safe_page, total_pages = _paginate(prepared, page)

        levels = {"healthy": 0, "attention": 0, "limited": 0, "critical": 0, "paused": 0}
        total_score = 0
        for _row, health in prepared:
            levels[health.level] = levels.get(health.level, 0) + 1
            total_score += health.score
        average = round(total_score / len(prepared)) if prepared else 0

        buttons: list[tuple[int, str, int, str]] = []
        for row, health in page_items:
            account = row["account"]
            label = truncate(
                display_account_name(account.display_name, account.username), 34
            )
            buttons.append((account.id, label, health.score, health.icon))

        page_line = (
            f"\n📄 Страница: <b>{safe_page + 1}/{total_pages}</b>"
            if len(prepared) > ACCOUNT_PAGE_SIZE
            else ""
        )
        await self._safe_edit_text(
            message,
            "👥 <b>Здоровье аккаунтов</b>\n"
            "<i>Операционная готовность внутри LikeBot</i>\n\n"
            "━━━━━━━━━━━━━━\n"
            f"Средняя оценка: <b>{average}/100</b>\n"
            f"🟢 Готовы: <b>{levels['healthy']}</b>  ·  "
            f"🟡 Внимание: <b>{levels['attention']}</b>\n"
            f"🟠 Ограничены: <b>{levels['limited']}</b>  ·  "
            f"🔴 Критично: <b>{levels['critical']}</b>\n"
            f"⚫️ Выключены: <b>{levels['paused']}</b>"
            f"{page_line}\n"
            "━━━━━━━━━━━━━━\n\n"
            + (
                "Сначала показаны аккаунты с самой низкой оценкой 👇"
                if prepared
                else "Аккаунты ещё не добавлены."
            )
            + "\n\n<i>Это не рейтинг доверия Telegram. Оценка строится только по статусу сессии, FloodWait и ошибкам LikeBot.</i>",
            reply_markup=account_health_list_keyboard(
                buttons, page=safe_page, total_pages=total_pages
            ),
        )

    async def _render_account_health_detail(
        self, message: Message, account_id: int, *, page: int = 0
    ) -> bool:
        rows = await self.db.account_health_overview(account_id)
        row = next(
            (item for item in rows if int(item["account"].id) == account_id),
            None,
        )
        if row is None:
            return False
        account = row["account"]
        health = evaluate_account_health(account)
        now = utcnow()
        flood_text = "нет"
        if account.flood_until and account.flood_until > now:
            remaining = int((account.flood_until - now).total_seconds())
            flood_text = (
                f"ещё {format_remaining_seconds(remaining)} "
                f"(до {format_utc_datetime(account.flood_until)} UTC)"
            )
        last_action = (
            f"{format_utc_datetime(account.last_reaction_at)} UTC"
            if account.last_reaction_at
            else "ещё не было"
        )
        error_text = (
            html.escape(truncate(account.last_error, 300))
            if account.last_error
            else "нет"
        )
        reason_lines = "\n".join(
            f"- {html.escape(reason)}" for reason in health.reasons
        )
        await self._safe_edit_text(
            message,
            "🩺 <b>Здоровье аккаунта</b>\n"
            "━━━━━━━━━━━━━━\n"
            f"👤 <b>{html.escape(display_account_name(account.display_name, account.username))}</b>\n"
            f"📱 <code>{html.escape(account.phone)}</code>\n"
            f"{health.icon} Готовность: <b>{health.score}/100 — {health.label}</b>\n"
            f"🔌 Состояние: <b>{'ВКЛ' if account.is_active else 'ВЫКЛ'}</b>\n"
            f"⏳ FloodWait: <b>{html.escape(flood_text)}</b>\n"
            f"❤️ Последняя реакция: <b>{last_action}</b>\n"
            f"📦 Очередь: <b>{row['pending']}</b> ждут · {row['running']} выполняется\n"
            f"✅ Завершено задач: <b>{row['completed']}</b>\n"
            f"❌ Сохранённых ошибок задач: <b>{row['failed']}</b>\n"
            f"⚠️ Последняя ошибка аккаунта: <code>{error_text}</code>\n\n"
            "📌 <b>Причины оценки</b>\n"
            f"{reason_lines}\n"
            "━━━━━━━━━━━━━━\n"
            "<i>Исторические ошибки показаны для диагностики, но не уменьшают текущую оценку навсегда.</i>",
            reply_markup=account_health_detail_keyboard(account.id, page=page),
        )
        return True

    async def stats(self, callback: CallbackQuery) -> None:
        if await self._deny_if_needed(callback):
            return
        await self._render_system_health(callback.message)
        await callback.answer()

    async def system_refresh(self, callback: CallbackQuery) -> None:
        if await self._deny_if_needed(callback):
            return
        await self._render_system_health(callback.message)
        await callback.answer("Состояние обновлено")

    async def system_accounts(self, callback: CallbackQuery) -> None:
        if await self._deny_if_needed(callback):
            return
        page = _page_from_callback(callback.data, "system:accounts_page:")
        await self._render_account_health_list(callback.message, page)
        await callback.answer()

    async def system_account_detail(self, callback: CallbackQuery) -> None:
        if await self._deny_if_needed(callback):
            return
        parts = (callback.data or "").split(":")
        try:
            account_id = int(parts[2])
            page = max(0, int(parts[3])) if len(parts) > 3 else 0
        except (IndexError, ValueError):
            await callback.answer("Некорректная кнопка", show_alert=True)
            return
        if not await self._render_account_health_detail(
            callback.message, account_id, page=page
        ):
            await callback.answer("Аккаунт не найден", show_alert=True)
            return
        await callback.answer()

    async def _render_analytics_overview(self, message: Message, period: str) -> None:
        snapshot = await self.db.analytics_snapshot(period, top_limit=10)
        await self._safe_edit_text(
            message,
            render_analytics_overview(snapshot),
            reply_markup=statistics_keyboard(period),
        )

    async def system_statistics(self, callback: CallbackQuery) -> None:
        if await self._deny_if_needed(callback):
            return
        await self._render_analytics_overview(callback.message, "day")
        await callback.answer()

    async def analytics_period(self, callback: CallbackQuery) -> None:
        if await self._deny_if_needed(callback):
            return
        try:
            period = parse_analytics_period_callback(
                callback.data, "analytics:period:"
            )
        except ValueError as exc:
            await callback.answer(str(exc), show_alert=True)
            return
        await self._render_analytics_overview(callback.message, period)
        await callback.answer("Аналитика обновлена")

    async def analytics_accounts(self, callback: CallbackQuery) -> None:
        if await self._deny_if_needed(callback):
            return
        try:
            period = parse_analytics_period_callback(
                callback.data, "analytics:accounts:"
            )
        except ValueError as exc:
            await callback.answer(str(exc), show_alert=True)
            return
        snapshot = await self.db.analytics_snapshot(period, top_limit=10)
        await self._safe_edit_text(
            callback.message,
            render_analytics_ranking(snapshot, ranking="accounts"),
            reply_markup=analytics_ranking_keyboard(period),
        )
        await callback.answer()

    async def analytics_targets(self, callback: CallbackQuery) -> None:
        if await self._deny_if_needed(callback):
            return
        try:
            period = parse_analytics_period_callback(
                callback.data, "analytics:targets:"
            )
        except ValueError as exc:
            await callback.answer(str(exc), show_alert=True)
            return
        snapshot = await self.db.analytics_snapshot(period, top_limit=10)
        await self._safe_edit_text(
            callback.message,
            render_analytics_ranking(snapshot, ranking="targets"),
            reply_markup=analytics_ranking_keyboard(period),
        )
        await callback.answer()

    async def analytics_export(self, callback: CallbackQuery) -> None:
        if await self._deny_if_needed(callback):
            return
        try:
            period = parse_analytics_period_callback(
                callback.data, "analytics:export:"
            )
        except ValueError as exc:
            await callback.answer(str(exc), show_alert=True)
            return
        snapshot = await self.db.analytics_snapshot(period, top_limit=None)
        raw = build_analytics_csv(snapshot)
        generated_at = snapshot.get("generated_at")
        timestamp = (
            generated_at.strftime("%Y%m%d_%H%M%S")
            if isinstance(generated_at, datetime)
            else datetime.now(UTC).strftime("%Y%m%d_%H%M%S")
        )
        filename = f"likebot_analytics_{period}_{timestamp}_utc.csv"
        await callback.message.answer_document(
            BufferedInputFile(raw, filename=filename),
            caption=(
                "📊 Расширенная аналитика LikeBot\n"
                f"Период: {ANALYTICS_PERIODS[period].label}"
            ),
        )
        await callback.answer("CSV сформирован")
