from __future__ import annotations

from .db_shared import *  # noqa: F403


class ReactionJobDatabaseMixin:
    async def active_reaction_account_ids(self, channel_id: int, message_id: int) -> set[int]:
        async with self.sessions() as session:
            rows = await session.scalars(
                select(ReactionJob.account_id).where(
                    ReactionJob.channel_id == channel_id,
                    ReactionJob.message_id == message_id,
                    ReactionJob.status.in_(["pending", "running", "done"]),
                )
            )
            return set(rows.all())

    async def reaction_message_sources(self, channel_id: int) -> list[tuple[int, str]]:
        async with self.sessions() as session:
            rows = (
                await session.execute(
                    select(ReactionJob.message_id, ReactionJob.source)
                    .where(ReactionJob.channel_id == channel_id)
                    .distinct()
                    .order_by(ReactionJob.message_id)
                )
            ).all()
            return [(int(message_id), str(source)) for message_id, source in rows]

    async def reaction_message_types(
        self, channel_id: int
    ) -> list[tuple[int, str, bool | None]]:
        """Return known post source and image classification for queue reconciliation."""

        async with self.sessions() as session:
            rows = (
                await session.execute(
                    select(
                        ReactionJob.message_id,
                        ReactionJob.source,
                        ReactionJob.post_has_image,
                    )
                    .where(ReactionJob.channel_id == channel_id)
                    .distinct()
                    .order_by(ReactionJob.message_id)
                )
            ).all()
            return [
                (int(message_id), str(source), post_has_image)
                for message_id, source, post_has_image in rows
            ]

    async def trim_pending_reactions_to_limit(self, channel_id: int, limit: int) -> dict[str, int]:
        if limit < 1:
            raise ValueError("Лимит должен быть положительным")
        cancelled = 0
        posts = 0
        async with self.sessions() as session:
            message_ids = list(
                (
                    await session.scalars(
                        select(ReactionJob.message_id)
                        .where(ReactionJob.channel_id == channel_id)
                        .distinct()
                    )
                ).all()
            )
            for message_id in message_ids:
                immutable = int(
                    await session.scalar(
                        select(func.count(ReactionJob.id)).where(
                            ReactionJob.channel_id == channel_id,
                            ReactionJob.message_id == message_id,
                            ReactionJob.status.in_(["running", "done"]),
                        )
                    )
                    or 0
                )
                pending = list(
                    (
                        await session.scalars(
                            select(ReactionJob)
                            .where(
                                ReactionJob.channel_id == channel_id,
                                ReactionJob.message_id == message_id,
                                ReactionJob.status == "pending",
                            )
                            .with_for_update()
                        )
                    ).all()
                )
                allowed_pending = max(0, limit - immutable)
                if len(pending) > allowed_pending:
                    _rng.shuffle(pending)
                    for job in pending[allowed_pending:]:
                        job.status = "cancelled"
                        job.error = "Сокращено лимитом реакций на пост"
                        job.completed_at = utcnow()
                        job.started_at = None
                        cancelled += 1
                    posts += 1
            await session.commit()
        return {"cancelled": cancelled, "posts": posts}

    async def trim_pending_reactions_to_message_limits(
        self, channel_id: int, limits: Mapping[int, int]
    ) -> dict[str, int]:
        """Trim pending jobs to a separate target count for every post."""

        prepared = {int(message_id): max(0, int(limit)) for message_id, limit in limits.items()}
        cancelled = 0
        posts = 0
        async with self.sessions() as session:
            for message_id, limit in prepared.items():
                immutable = int(
                    await session.scalar(
                        select(func.count(ReactionJob.id)).where(
                            ReactionJob.channel_id == channel_id,
                            ReactionJob.message_id == message_id,
                            ReactionJob.status.in_(["running", "done"]),
                        )
                    )
                    or 0
                )
                pending = list(
                    (
                        await session.scalars(
                            select(ReactionJob)
                            .where(
                                ReactionJob.channel_id == channel_id,
                                ReactionJob.message_id == message_id,
                                ReactionJob.status == "pending",
                            )
                            .with_for_update()
                        )
                    ).all()
                )
                allowed_pending = max(0, limit - immutable)
                if len(pending) > allowed_pending:
                    _rng.shuffle(pending)
                    for job in pending[allowed_pending:]:
                        job.status = "cancelled"
                        job.error = "Сокращено процентом для типа поста"
                        job.completed_at = utcnow()
                        job.started_at = None
                        cancelled += 1
                    posts += 1
            await session.commit()
        return {"cancelled": cancelled, "posts": posts}

    async def set_channel_reaction_window(
        self, channel_id: int, minimum_seconds: int, maximum_seconds: int
    ) -> None:
        if minimum_seconds < 0 or maximum_seconds < minimum_seconds:
            raise ValueError("Некорректный период распределения реакций")
        if maximum_seconds > 7 * 24 * 60 * 60:
            raise ValueError("Период не может превышать 7 дней")
        async with self.sessions() as session:
            await session.execute(
                update(Channel)
                .where(Channel.id == channel_id)
                .values(
                    reaction_window_min_seconds=minimum_seconds,
                    reaction_window_max_seconds=maximum_seconds,
                    updated_at=utcnow(),
                )
            )
            await session.commit()

    async def latest_reaction_due_by_account(
        self, account_ids: Iterable[int], *, exclude_channel_id: int | None = None
    ) -> dict[int, datetime]:
        ids = list(dict.fromkeys(int(item) for item in account_ids))
        if not ids:
            return {}
        conditions = [
            ReactionJob.account_id.in_(ids),
            ReactionJob.status.in_(["pending", "running"]),
        ]
        if exclude_channel_id is not None:
            conditions.append(ReactionJob.channel_id != exclude_channel_id)
        async with self.sessions() as session:
            rows = (
                await session.execute(
                    select(ReactionJob.account_id, func.max(ReactionJob.due_at))
                    .where(*conditions)
                    .group_by(ReactionJob.account_id)
                )
            ).all()
        return {int(account_id): due_at for account_id, due_at in rows if due_at is not None}

    async def reschedule_pending_channel_reactions(
        self,
        channel_id: int,
        *,
        minimum_seconds: int,
        maximum_seconds: int,
        account_delay_min_seconds: int,
        account_delay_max_seconds: int,
    ) -> dict[str, int]:
        if minimum_seconds < 0 or maximum_seconds < minimum_seconds:
            raise ValueError("Некорректный период")
        now = utcnow()
        async with self.sessions() as session:
            jobs = list(
                (
                    await session.scalars(
                        select(ReactionJob)
                        .where(
                            ReactionJob.channel_id == channel_id,
                            ReactionJob.status == "pending",
                        )
                        .order_by(ReactionJob.message_id, ReactionJob.id)
                        .with_for_update()
                    )
                ).all()
            )
            fresh = [job for job in jobs if job.attempts == 0]
            skipped_retries = len(jobs) - len(fresh)
            by_message: dict[int, list[ReactionJob]] = {}
            for job in fresh:
                by_message.setdefault(job.message_id, []).append(job)

            other_rows = (
                await session.execute(
                    select(ReactionJob.account_id, func.max(ReactionJob.due_at))
                    .where(
                        ReactionJob.account_id.in_([job.account_id for job in fresh] or [-1]),
                        ReactionJob.channel_id != channel_id,
                        ReactionJob.status.in_(["pending", "running"]),
                    )
                    .group_by(ReactionJob.account_id)
                )
            ).all()
            latest_by_account = {int(a): d for a, d in other_rows if d is not None}
            updated = 0
            for message_jobs in by_message.values():
                _rng.shuffle(message_jobs)
                count = len(message_jobs)
                span = max(0, maximum_seconds - minimum_seconds)
                for index, job in enumerate(message_jobs):
                    if count <= 1 or span == 0:
                        offset = minimum_seconds if span == 0 else _rng.randint(minimum_seconds, maximum_seconds)
                    else:
                        segment_start = minimum_seconds + math.floor(span * index / count)
                        segment_end = minimum_seconds + math.floor(span * (index + 1) / count)
                        if index == count - 1:
                            segment_end = maximum_seconds
                        offset = _rng.randint(segment_start, max(segment_start, segment_end))
                    due_at = now + timedelta(seconds=offset)
                    previous = latest_by_account.get(job.account_id)
                    if previous is not None:
                        cooldown = _rng.randint(account_delay_min_seconds, account_delay_max_seconds)
                        due_at = max(due_at, previous + timedelta(seconds=cooldown))
                    job.due_at = due_at
                    job.error = None
                    latest_by_account[job.account_id] = due_at
                    updated += 1
            await session.commit()
        return {"updated": updated, "skipped_retries": skipped_retries}

    async def cancel_reaction_job(self, job_id: int, error: str) -> None:
        async with self.sessions() as session:
            await session.execute(
                update(ReactionJob)
                .where(ReactionJob.id == job_id)
                .values(status="cancelled", error=error[:4000], completed_at=utcnow(), started_at=None)
            )
            await session.commit()

    async def schedule_reaction_jobs(
        self,
        *,
        channel_id: int,
        message_id: int,
        source: str,
        jobs: Iterable[tuple[int, str, datetime]],
        post_has_image: bool | None = None,
    ) -> int:
        if source not in {"new", "old"}:
            raise ValueError("Недопустимый источник задания реакции")
        prepared_jobs = list(jobs)
        created = 0
        async with self.sessions() as session:
            archived_accounts: set[int] = set()
            if prepared_jobs:
                account_ids = {int(account_id) for account_id, _reaction, _due_at in prepared_jobs}
                archived_accounts = set(
                    (
                        await session.scalars(
                            select(JobHistoryKey.account_id).where(
                                JobHistoryKey.job_kind == "reaction",
                                JobHistoryKey.channel_id == channel_id,
                                JobHistoryKey.message_id == message_id,
                                JobHistoryKey.account_id.in_(account_ids),
                            )
                        )
                    ).all()
                )
            for account_id, reaction, due_at in prepared_jobs:
                if int(account_id) in archived_accounts:
                    continue
                existing = await session.scalar(
                    select(ReactionJob).where(
                        ReactionJob.channel_id == channel_id,
                        ReactionJob.account_id == account_id,
                        ReactionJob.message_id == message_id,
                    )
                )
                if existing:
                    if existing.status in {"cancelled", "failed"}:
                        existing.reaction = reaction
                        existing.source = source
                        existing.due_at = due_at
                        existing.status = "pending"
                        existing.attempts = 0
                        existing.error = None
                        existing.completed_at = None
                        existing.started_at = None
                        existing.post_has_image = post_has_image
                        existing.view_included = True
                        existing.view_confirmed_at = None
                        created += 1
                    elif existing.source == "legacy":
                        # v1.0.3 jobs had no source marker. Adopt them into the
                        # first concrete new/old queue that sees the post.
                        existing.source = source
                    if post_has_image is not None:
                        existing.post_has_image = post_has_image
                    continue
                session.add(
                    ReactionJob(
                        channel_id=channel_id,
                        account_id=account_id,
                        message_id=message_id,
                        reaction=reaction,
                        source=source,
                        post_has_image=post_has_image,
                        due_at=due_at,
                    )
                )
                created += 1
            await session.commit()
        return created

    async def get_due_reaction_job(
        self, now: datetime, *, exclude_account_ids: Iterable[int] | None = None
    ) -> ReactionJob | None:
        conditions = [ReactionJob.status == "pending", ReactionJob.due_at <= now]
        excluded = list(exclude_account_ids or ())
        if excluded:
            conditions.append(ReactionJob.account_id.not_in(excluded))
        async with self.sessions() as session:
            stmt = (
                select(ReactionJob)
                .options(selectinload(ReactionJob.account), selectinload(ReactionJob.channel))
                .where(*conditions)
                .order_by(ReactionJob.due_at, ReactionJob.id)
                .limit(1)
            )
            return await session.scalar(stmt)

    async def mark_reaction_running(self, job_id: int) -> bool:
        async with self.sessions() as session:
            result = await session.execute(
                update(ReactionJob)
                .where(
                    ReactionJob.id == job_id,
                    ReactionJob.status == "pending",
                    ReactionJob.due_at <= utcnow(),
                )
                .values(status="running", started_at=utcnow(), attempts=ReactionJob.attempts + 1)
            )
            await session.commit()
            return bool(result.rowcount)

    async def mark_reaction_view_confirmed(self, job_id: int) -> None:
        """Persist that Telegram accepted (or had already accepted) the view request."""

        async with self.sessions() as session:
            await session.execute(
                update(ReactionJob)
                .where(ReactionJob.id == job_id)
                .values(view_included=True, view_confirmed_at=utcnow())
            )
            await session.commit()

    async def complete_reaction_job(self, job_id: int) -> None:
        completed_at = utcnow()
        async with self.sessions() as session:
            account_id = await session.scalar(
                select(ReactionJob.account_id).where(ReactionJob.id == job_id)
            )
            await session.execute(
                update(ReactionJob)
                .where(ReactionJob.id == job_id)
                .values(status="done", error=None, completed_at=completed_at, started_at=None)
            )
            if account_id is not None:
                await session.execute(
                    update(Account)
                    .where(Account.id == account_id)
                    .values(last_reaction_at=completed_at)
                )
            await session.commit()

    async def retry_reaction_job(self, job_id: int, due_at: datetime, error: str) -> None:
        async with self.sessions() as session:
            await session.execute(
                update(ReactionJob)
                .where(ReactionJob.id == job_id)
                .values(status="pending", due_at=due_at, error=error[:4000], started_at=None)
            )
            await session.commit()

    async def fail_reaction_job(self, job_id: int, error: str) -> None:
        async with self.sessions() as session:
            await session.execute(
                update(ReactionJob).where(ReactionJob.id == job_id).values(
                    status="failed", error=error[:4000], completed_at=utcnow(), started_at=None
                )
            )
            await session.commit()

    async def cancel_pending_reactions(self, channel_id: int, *, source: str | None = None) -> int:
        async with self.sessions() as session:
            conditions = [ReactionJob.channel_id == channel_id, ReactionJob.status == "pending"]
            if source is not None:
                # Legacy jobs were created before separate new/old queues existed.
                # Cancelling either switch must safely stop them as well.
                conditions.append(ReactionJob.source.in_([source, "legacy"]))
            result = await session.execute(
                update(ReactionJob)
                .where(*conditions)
                .values(
                    status="cancelled", error="Отключено администратором",
                    completed_at=utcnow(), started_at=None
                )
            )
            await session.commit()
            return int(result.rowcount or 0)

    async def reaction_counts(self, channel_id: int) -> dict[str, int]:
        result = {"pending": 0, "running": 0, "done": 0, "failed": 0, "cancelled": 0}
        async with self.sessions() as session:
            rows = (
                await session.execute(
                    select(ReactionJob.status, func.count(ReactionJob.id))
                    .where(ReactionJob.channel_id == channel_id)
                    .group_by(ReactionJob.status)
                )
            ).all()
        for status, count in rows:
            if status in result:
                result[status] = int(count)
        async with self.sessions() as session:
            archived = (
                await session.execute(
                    select(
                        JobHistoryChannelSummary.status,
                        JobHistoryChannelSummary.count,
                    ).where(
                        JobHistoryChannelSummary.channel_id == channel_id,
                        JobHistoryChannelSummary.job_kind == "reaction",
                    )
                )
            ).all()
        for status, count in archived:
            status_text = str(status)
            if status_text in result:
                result[status_text] += int(count or 0)
        return result

    async def latest_pending_view_due(self, channel_id: int) -> datetime | None:
        async with self.sessions() as session:
            return await session.scalar(
                select(func.max(ViewJob.due_at)).where(
                    ViewJob.channel_id == channel_id,
                    ViewJob.status == "pending",
                )
            )
