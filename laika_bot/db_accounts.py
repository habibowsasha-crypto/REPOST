from __future__ import annotations

from .db_shared import *  # noqa: F403


class AccountDatabaseMixin:
    async def upsert_account(
        self,
        *,
        phone: str,
        telegram_user_id: int,
        display_name: str,
        username: str | None,
        session_encrypted: str,
        email_login: str | None = None,
        email_provider: str | None = None,
        expected_account_id: int | None = None,
    ) -> Account:
        if email_login is not None:
            email_login, detected_provider = normalize_email_login(email_login)
            email_provider = (email_provider or detected_provider).strip()[:255]
        async with self.sessions() as session:
            by_phone = await session.scalar(
                select(Account).where(Account.phone == phone).with_for_update()
            )
            by_identity = await session.scalar(
                select(Account)
                .where(Account.telegram_user_id == telegram_user_id)
                .with_for_update()
            )

            expected_account = None
            if expected_account_id is not None:
                expected_account = await session.get(
                    Account,
                    int(expected_account_id),
                    with_for_update=True,
                )
                if (
                    expected_account is None
                    or expected_account.phone != phone
                    or expected_account.telegram_user_id != telegram_user_id
                    or (by_phone is not None and by_phone.id != expected_account.id)
                    or (by_identity is not None and by_identity.id != expected_account.id)
                ):
                    raise AccountIdentityConflictError(
                        phone=phone,
                        stored_account_id=(
                            expected_account.id if expected_account is not None else expected_account_id
                        ),
                        stored_telegram_user_id=(
                            expected_account.telegram_user_id
                            if expected_account is not None
                            else None
                        ),
                        incoming_telegram_user_id=telegram_user_id,
                    )

            if by_phone is not None and by_phone.telegram_user_id != telegram_user_id:
                raise AccountIdentityConflictError(
                    phone=phone,
                    stored_account_id=by_phone.id,
                    stored_telegram_user_id=by_phone.telegram_user_id,
                    incoming_telegram_user_id=telegram_user_id,
                )
            if by_phone is not None and by_identity is not None and by_phone.id != by_identity.id:
                raise AccountIdentityConflictError(
                    phone=phone,
                    stored_account_id=by_phone.id,
                    stored_telegram_user_id=by_phone.telegram_user_id,
                    incoming_telegram_user_id=telegram_user_id,
                )

            account = expected_account or by_phone or by_identity
            if account is None:
                account = Account(
                    phone=phone,
                    telegram_user_id=telegram_user_id,
                    display_name=display_name,
                    username=username,
                    session_encrypted=session_encrypted,
                    email_login=email_login,
                    email_provider=email_provider,
                    email_updated_at=utcnow() if email_login else None,
                )
                session.add(account)
            else:
                # A Telegram user may legitimately change their phone number. The
                # stable Telegram user id is the identity boundary; the inverse
                # (same phone, different id) is rejected above.
                account.phone = phone
                account.display_name = display_name
                account.username = username
                account.session_encrypted = session_encrypted
                account.is_active = True
                account.status = "ready"
                account.last_error = None
                account.problem_detected_at = None
                account.problem_reason = None
                account.problem_context = None
                account.flood_until = None
                if email_login is not None:
                    account.email_login = email_login
                    account.email_provider = email_provider
                    account.email_updated_at = utcnow()
            await session.commit()
            await session.refresh(account)
            account_id = int(account.id)
        # AI Comments is an isolated optional module. Profile creation must never
        # roll back or misreport an already successful Telegram authorization.
        # Startup migration and the profiles menu retry this idempotently.
        try:
            ensure_profile = getattr(self, "ensure_ai_account_profile", None)
            if ensure_profile is not None:
                await ensure_profile(account_id)
        except Exception:  # noqa: BLE001
            logger.exception(
                "AI account profile auto-create failed account=%s; core account is saved",
                account_id,
            )
        return account

    async def list_accounts(self, *, active_only: bool = False) -> list[Account]:
        async with self.sessions() as session:
            stmt = select(Account).order_by(Account.id)
            if active_only:
                stmt = stmt.where(
                    Account.is_active.is_(True),
                    Account.status != "unauthorized",
                )
            return list((await session.scalars(stmt)).all())

    async def list_problem_accounts(self) -> list[Account]:
        async with self.sessions() as session:
            stmt = (
                select(Account)
                .where(Account.status == "unauthorized")
                .order_by(Account.problem_detected_at.desc(), Account.id)
            )
            return list((await session.scalars(stmt)).all())

    async def list_accounts_without_email(self) -> list[Account]:
        async with self.sessions() as session:
            stmt = (
                select(Account)
                .where((Account.email_login.is_(None)) | (Account.email_login == ""))
                .order_by(Account.status == "unauthorized", Account.id)
            )
            return list((await session.scalars(stmt)).all())

    async def get_account(self, account_id: int) -> Account | None:
        async with self.sessions() as session:
            return await session.get(Account, account_id)

    async def update_account_email(
        self,
        account_id: int,
        *,
        email_login: str,
        email_provider: str,
    ) -> bool:
        email_login, detected_provider = normalize_email_login(email_login)
        email_provider = (email_provider or detected_provider).strip()[:255]
        now = utcnow()
        async with self.sessions() as session:
            result = await session.execute(
                update(Account)
                .where(Account.id == account_id)
                .values(
                    email_login=email_login,
                    email_provider=email_provider,
                    email_updated_at=now,
                    updated_at=now,
                )
            )
            await session.commit()
            return bool(result.rowcount)

    @staticmethod
    def _normalize_email_note(note: str | None) -> str | None:
        if note is None:
            return None
        normalized = " ".join(str(note).split())
        if normalized in {"", "-"}:
            return None
        if len(normalized) > EMAIL_NOTE_MAX_LENGTH:
            raise ValueError(
                f"Примечание не должно быть длиннее {EMAIL_NOTE_MAX_LENGTH} символов"
            )
        return normalized

    async def update_account_email_note(self, account_id: int, note: str | None) -> bool:
        normalized_note = self._normalize_email_note(note)
        now = utcnow()
        async with self.sessions() as session:
            result = await session.execute(
                update(Account)
                .where(
                    Account.id == account_id,
                    Account.email_login.is_not(None),
                    Account.email_login != "",
                )
                .values(
                    email_note=normalized_note,
                    email_updated_at=now,
                    updated_at=now,
                )
            )
            await session.commit()
            return bool(result.rowcount)

    async def clear_account_email(self, account_id: int) -> bool:
        now = utcnow()
        async with self.sessions() as session:
            result = await session.execute(
                update(Account)
                .where(Account.id == account_id)
                .values(
                    email_login=None,
                    email_provider=None,
                    email_note=None,
                    email_updated_at=None,
                    updated_at=now,
                )
            )
            await session.commit()
            return bool(result.rowcount)

    async def update_account_profile(
        self,
        account_id: int,
        *,
        telegram_user_id: int,
        display_name: str,
        username: str | None,
    ) -> bool:
        """Refresh mutable Telegram profile fields and return whether they changed."""

        async with self.sessions() as session:
            account = await session.get(Account, account_id)
            if account is None:
                return False
            if account.telegram_user_id != telegram_user_id:
                raise AccountIdentityConflictError(
                    phone=account.phone,
                    stored_account_id=account.id,
                    stored_telegram_user_id=account.telegram_user_id,
                    incoming_telegram_user_id=telegram_user_id,
                )
            normalized_name = display_name.strip() or str(telegram_user_id)
            normalized_username = username.strip() if username else None
            changed = (
                account.display_name != normalized_name
                or account.username != normalized_username
            )
            account.display_name = normalized_name
            account.username = normalized_username
            account.last_error = None
            account.updated_at = utcnow()
            await session.commit()
            return changed

    async def channel_connect_state(
        self, channel_id: int, *, max_accounts: int
    ) -> tuple[dict[str, int], list[Account]]:
        """Return active account coverage and accounts eligible for a new join job.

        Only the first ``max_accounts`` active accounts belong to the target's
        managed pool. Completed joins and already pending/running joins are not
        offered again. Failed, left, or previously cancelled/opposite-action
        rows are eligible and will be safely reactivated by schedule_join_jobs.
        """

        if max_accounts < 1:
            raise ValueError("Максимум аккаунтов должен быть положительным")
        async with self.sessions() as session:
            accounts = list(
                (
                    await session.scalars(
                        select(Account)
                        .where(
                            Account.is_active.is_(True),
                            Account.status != "unauthorized",
                        )
                        .order_by(Account.id)
                        .limit(max_accounts)
                    )
                ).all()
            )
            account_ids = [account.id for account in accounts]
            jobs: dict[int, JoinJob] = {}
            if account_ids:
                rows = list(
                    (
                        await session.scalars(
                            select(JoinJob).where(
                                JoinJob.channel_id == channel_id,
                                JoinJob.account_id.in_(account_ids),
                            )
                        )
                    ).all()
                )
                jobs = {job.account_id: job for job in rows}

        joined = 0
        pending = 0
        failed = 0
        available: list[Account] = []
        for account in accounts:
            job = jobs.get(account.id)
            if job and job.action == "join" and job.status == "done":
                joined += 1
                continue
            if job and job.status in {"pending", "running"}:
                pending += 1
                continue
            if job and job.status == "failed":
                failed += 1
            available.append(account)

        summary = {
            "total": len(accounts),
            "joined": joined,
            "pending": pending,
            "failed": failed,
            "connectable": len(available),
        }
        return summary, available

    async def latest_pending_join_due(self, channel_id: int) -> datetime | None:
        async with self.sessions() as session:
            return await session.scalar(
                select(func.max(JoinJob.due_at)).where(
                    JoinJob.channel_id == channel_id,
                    JoinJob.action == "join",
                    JoinJob.status == "pending",
                )
            )

    async def set_account_active(self, account_id: int, active: bool) -> None:
        async with self.sessions() as session:
            await session.execute(
                update(Account)
                .where(Account.id == account_id, Account.status != "unauthorized")
                .values(is_active=active, updated_at=utcnow())
            )
            await session.commit()

    async def bulk_set_accounts_active(self, active: bool) -> dict[str, object]:
        """Enable/disable regular accounts atomically without touching quarantine rows.

        Pending jobs are cancelled only when accounts are disabled. Running jobs are
        deliberately left to their current worker ownership and will observe the new
        account state on the next safe boundary.
        """

        now = utcnow()
        async with self.sessions() as session:
            account_ids = list(
                (
                    await session.scalars(
                        select(Account.id).where(
                            Account.status != "unauthorized",
                            Account.is_active.is_(not active),
                        )
                    )
                ).all()
            )
            problem_count = int(
                await session.scalar(
                    select(func.count(Account.id)).where(
                        Account.status == "unauthorized"
                    )
                )
                or 0
            )
            if not account_ids:
                return {
                    "changed": 0,
                    "account_ids": [],
                    "skipped_problem": problem_count,
                    "cancelled_join": 0,
                    "cancelled_reaction": 0,
                    "cancelled_view": 0,
                }

            await session.execute(
                update(Account)
                .where(Account.id.in_(account_ids))
                .values(is_active=active, updated_at=now)
            )
            cancelled_join = cancelled_reaction = cancelled_view = 0
            if not active:
                reason = "Аккаунт выключен массовым действием администратора"
                join_result = await session.execute(
                    update(JoinJob)
                    .where(
                        JoinJob.account_id.in_(account_ids),
                        JoinJob.status == "pending",
                    )
                    .values(
                        status="cancelled",
                        error=reason,
                        completed_at=now,
                        started_at=None,
                    )
                )
                reaction_result = await session.execute(
                    update(ReactionJob)
                    .where(
                        ReactionJob.account_id.in_(account_ids),
                        ReactionJob.status == "pending",
                    )
                    .values(
                        status="cancelled",
                        error=reason,
                        completed_at=now,
                        started_at=None,
                    )
                )
                affected_batches = list(
                    (
                        await session.scalars(
                            select(ViewJob.batch_id)
                            .where(
                                ViewJob.account_id.in_(account_ids),
                                ViewJob.status == "pending",
                            )
                            .distinct()
                        )
                    ).all()
                )
                view_result = await session.execute(
                    update(ViewJob)
                    .where(
                        ViewJob.account_id.in_(account_ids),
                        ViewJob.status == "pending",
                    )
                    .values(
                        status="cancelled",
                        error=reason,
                        completed_at=now,
                        started_at=None,
                    )
                )
                for batch_id in affected_batches:
                    await self._sync_view_batch_status(session, int(batch_id))
                cancelled_join = int(join_result.rowcount or 0)
                cancelled_reaction = int(reaction_result.rowcount or 0)
                cancelled_view = int(view_result.rowcount or 0)
            await session.commit()
            return {
                "changed": len(account_ids),
                "account_ids": [int(item) for item in account_ids],
                "skipped_problem": problem_count,
                "cancelled_join": cancelled_join,
                "cancelled_reaction": cancelled_reaction,
                "cancelled_view": cancelled_view,
            }

    async def quarantine_invalid_account(
        self,
        account_id: int,
        *,
        context: str,
        reason: str = "Сессия аккаунта больше не авторизована",
    ) -> bool:
        """Disable a permanently unauthorized account without losing its identity/history.

        The first transition is an atomic conditional UPDATE. This keeps repeated
        worker failures idempotent on both SQLite and PostgreSQL and guarantees
        that the administrator receives at most one first-detection notification.
        Later detections may refresh the reason/context but never overwrite the
        original detection timestamp.
        """

        now = utcnow()
        safe_reason = (reason or "Сессия аккаунта больше не авторизована")[:4000]
        safe_context = (context or "unknown")[:128]
        async with self.sessions() as session:
            first_update = await session.execute(
                update(Account)
                .where(
                    Account.id == account_id,
                    Account.status != "unauthorized",
                )
                .values(
                    is_active=False,
                    status="unauthorized",
                    last_error=safe_reason,
                    problem_reason=safe_reason,
                    problem_context=safe_context,
                    problem_detected_at=now,
                    flood_until=None,
                    updated_at=now,
                )
            )
            first_detection = bool(first_update.rowcount)
            if not first_detection:
                exists = await session.scalar(
                    select(Account.id).where(Account.id == account_id)
                )
                if exists is None:
                    await session.rollback()
                    return False
                await session.execute(
                    update(Account)
                    .where(Account.id == account_id)
                    .values(
                        is_active=False,
                        status="unauthorized",
                        last_error=safe_reason,
                        problem_reason=safe_reason,
                        problem_context=safe_context,
                        flood_until=None,
                        updated_at=now,
                    )
                )

            cancellation_reason = "Аккаунт отключён: недействительная Telegram-сессия"
            await session.execute(
                update(ReactionJob)
                .where(
                    ReactionJob.account_id == account_id,
                    ReactionJob.status.in_(["pending", "running"]),
                )
                .values(
                    status="cancelled",
                    error=cancellation_reason,
                    completed_at=now,
                    started_at=None,
                )
            )
            await session.execute(
                update(JoinJob)
                .where(
                    JoinJob.account_id == account_id,
                    JoinJob.status.in_(["pending", "running"]),
                )
                .values(
                    status="cancelled",
                    error=cancellation_reason,
                    completed_at=now,
                    started_at=None,
                )
            )
            affected_batches = list(
                (
                    await session.scalars(
                        select(ViewJob.batch_id)
                        .where(
                            ViewJob.account_id == account_id,
                            ViewJob.status.in_(["pending", "running"]),
                        )
                        .distinct()
                    )
                ).all()
            )
            await session.execute(
                update(ViewJob)
                .where(
                    ViewJob.account_id == account_id,
                    ViewJob.status.in_(["pending", "running"]),
                )
                .values(
                    status="cancelled",
                    error=cancellation_reason,
                    completed_at=now,
                    started_at=None,
                )
            )
            for batch_id in affected_batches:
                await self._sync_view_batch_status(session, int(batch_id))
            await session.commit()
            return first_detection

    async def restore_problem_account(self, account_id: int) -> bool:
        """Return an authorized quarantined account to the active pool.

        Only a currently quarantined row may be restored. This prevents a stale
        callback or a racing health check from rewriting another account status.
        """

        async with self.sessions() as session:
            result = await session.execute(
                update(Account)
                .where(
                    Account.id == account_id,
                    Account.status == "unauthorized",
                )
                .values(
                    is_active=True,
                    status="ready",
                    last_error=None,
                    problem_detected_at=None,
                    problem_reason=None,
                    problem_context=None,
                    flood_until=None,
                    updated_at=utcnow(),
                )
            )
            await session.commit()
            return bool(result.rowcount)

    async def delete_account(self, account_id: int) -> None:
        async with self.sessions() as session:
            batch_rows = (
                await session.execute(
                    select(ViewJob.batch_id, func.count(ViewJob.id))
                    .where(ViewJob.account_id == account_id)
                    .group_by(ViewJob.batch_id)
                )
            ).all()
            await session.execute(delete(ViewJob).where(ViewJob.account_id == account_id))
            for batch_id, count in batch_rows:
                batch = await session.get(ViewBatch, int(batch_id))
                if batch:
                    batch.total_jobs = max(0, batch.total_jobs - int(count))
                    batch.skipped_existing += int(count)
                    await self._sync_view_batch_status(session, batch.id)
            await session.execute(delete(ReactionJob).where(ReactionJob.account_id == account_id))
            await session.execute(delete(JoinJob).where(JoinJob.account_id == account_id))
            await session.execute(delete(JobHistoryKey).where(JobHistoryKey.account_id == account_id))
            await session.execute(
                delete(JobHistorySummary).where(JobHistorySummary.account_id == account_id)
            )
            await session.execute(delete(Account).where(Account.id == account_id))
            await session.commit()

    async def delete_invalid_account(
        self, account_id: int, *, require_problem: bool = False
    ) -> bool:
        """Remove a permanently unauthorized account and all of its queued work.

        Explicit child deletes keep SQLite safe even when foreign-key cascades are
        not enabled by the local driver. Bulk cleanup opts into
        ``require_problem``; its status predicate is deliberately repeated inside
        the transaction so a stale callback cannot delete an account restored
        while waiting for its lock. The default preserves the legacy low-level
        cleanup API used immediately after a proven authorization failure.
        """

        async with self.sessions() as session:
            account_filters = [Account.id == account_id]
            if require_problem:
                account_filters.append(Account.status == "unauthorized")
            exists = await session.scalar(
                select(Account.id).where(*account_filters)
            )
            if exists is None:
                return False
            batch_rows = (
                await session.execute(
                    select(ViewJob.batch_id, func.count(ViewJob.id))
                    .where(ViewJob.account_id == account_id)
                    .group_by(ViewJob.batch_id)
                )
            ).all()
            await session.execute(delete(ViewJob).where(ViewJob.account_id == account_id))
            for batch_id, count in batch_rows:
                batch = await session.get(ViewBatch, int(batch_id))
                if batch:
                    batch.total_jobs = max(0, batch.total_jobs - int(count))
                    batch.skipped_existing += int(count)
                    await self._sync_view_batch_status(session, batch.id)
            await session.execute(delete(ReactionJob).where(ReactionJob.account_id == account_id))
            await session.execute(delete(JoinJob).where(JoinJob.account_id == account_id))
            await session.execute(delete(JobHistoryKey).where(JobHistoryKey.account_id == account_id))
            await session.execute(
                delete(JobHistorySummary).where(JobHistorySummary.account_id == account_id)
            )
            result = await session.execute(
                delete(Account).where(*account_filters)
            )
            if not result.rowcount:
                await session.rollback()
                return False
            await session.commit()
            return True

    async def set_account_error(self, account_id: int, error: str, *, flood_until: datetime | None = None) -> None:
        async with self.sessions() as session:
            values: dict[str, object] = {"last_error": error[:4000], "updated_at": utcnow()}
            if flood_until is not None:
                values["flood_until"] = flood_until
            await session.execute(update(Account).where(Account.id == account_id).values(**values))
            await session.commit()

    async def mark_account_healthy(self, account_id: int) -> bool:
        """Clear a transient account error only after a successful identity check.

        Unauthorized/quarantined rows and manually disabled accounts are never
        re-enabled by this helper. An expired FloodWait is cleared, while a future
        FloodWait remains authoritative.
        """

        now = utcnow()
        async with self.sessions() as session:
            account = await session.get(Account, account_id, with_for_update=True)
            if (
                account is None
                or account.status == "unauthorized"
                or not account.is_active
            ):
                return False
            changed = bool(
                account.last_error
                or (account.flood_until is not None and account.flood_until <= now)
            )
            account.last_error = None
            if account.flood_until is not None and account.flood_until <= now:
                account.flood_until = None
            if changed:
                account.updated_at = now
                await session.commit()
            return changed

    async def recover_stuck_jobs(
        self,
        *,
        stale_before: datetime,
        retry_due_at: datetime,
        max_attempts: int,
        max_rows_per_kind: int,
        exclude_join_job_ids: set[int] | None = None,
        exclude_reaction_job_ids: set[int] | None = None,
        exclude_view_job_ids: set[int] | None = None,
    ) -> dict[str, int]:
        """Atomically requeue genuinely stale running rows or fail exhausted rows.

        In-memory active job ids are excluded to avoid racing a still-running
        Telegram request. Every UPDATE also re-checks ``status`` and ``started_at``
        so a concurrent completion cannot be overwritten.
        """

        limit = max(1, int(max_rows_per_kind))
        attempts_limit = max(1, int(max_attempts))
        now = utcnow()
        result = {
            "join_requeued": 0,
            "join_failed": 0,
            "reaction_requeued": 0,
            "reaction_failed": 0,
            "view_requeued": 0,
            "view_failed": 0,
        }
        specs = (
            ("join", JoinJob, set(exclude_join_job_ids or ())),
            ("reaction", ReactionJob, set(exclude_reaction_job_ids or ())),
            ("view", ViewJob, set(exclude_view_job_ids or ())),
        )
        async with self.sessions() as session:
            affected_view_batches: set[int] = set()
            for kind, model, excluded in specs:
                conditions = [
                    model.status == "running",
                    or_(model.started_at.is_(None), model.started_at <= stale_before),
                ]
                if excluded:
                    conditions.append(model.id.not_in(excluded))
                rows = list(
                    (
                        await session.scalars(
                            select(model)
                            .where(*conditions)
                            .order_by(model.started_at, model.id)
                            .limit(limit)
                            .with_for_update(skip_locked=True)
                        )
                    ).all()
                )
                for job in rows:
                    # Re-read predicates in the UPDATE itself. This remains safe if
                    # a worker completes between SELECT and UPDATE.
                    update_conditions = [
                        model.id == job.id,
                        model.status == "running",
                        or_(
                            model.started_at.is_(None),
                            model.started_at <= stale_before,
                        ),
                    ]
                    if int(job.attempts) >= attempts_limit:
                        update_result = await session.execute(
                            update(model)
                            .where(*update_conditions)
                            .values(
                                status="failed",
                                error=(
                                    "Автовосстановление: исчерпан лимит попыток "
                                    "после зависшего выполнения"
                                ),
                                completed_at=now,
                                started_at=None,
                            )
                        )
                        if update_result.rowcount:
                            result[f"{kind}_failed"] += 1
                    else:
                        update_result = await session.execute(
                            update(model)
                            .where(*update_conditions)
                            .values(
                                status="pending",
                                due_at=retry_due_at,
                                error="Автовосстановление: зависшее задание возвращено в очередь",
                                completed_at=None,
                                started_at=None,
                            )
                        )
                        if update_result.rowcount:
                            result[f"{kind}_requeued"] += 1
                    if kind == "view" and update_result.rowcount:
                        affected_view_batches.add(int(job.batch_id))

            for batch_id in affected_view_batches:
                await self._sync_view_batch_status(session, batch_id)
            await session.commit()
        return result

    async def recent_recovery_failure_count(self, *, since: datetime) -> int:
        """Count recent terminal failures created by automatic stale-job recovery."""

        statements = []
        for model in (JoinJob, ReactionJob, ViewJob):
            statements.append(
                select(func.count(model.id).label("failure_count")).where(
                    model.status == "failed",
                    model.completed_at >= since,
                    model.error.like("Автовосстановление:%"),
                )
            )
        union = union_all(*statements).subquery()
        async with self.sessions() as session:
            value = await session.scalar(select(func.coalesce(func.sum(union.c.failure_count), 0)))
            return int(value or 0)
