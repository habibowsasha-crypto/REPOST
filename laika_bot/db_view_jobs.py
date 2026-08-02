from __future__ import annotations

from .db_shared import *  # noqa: F403


class ViewJobDatabaseMixin:
    async def viewed_or_scheduled_account_ids(self, channel_id: int, message_id: int) -> set[int]:
        """Return accounts whose view is proven or still reserved by a current job.

        Completed reaction rows created before v1.0.17 are deliberately not treated
        as proof: older releases may have sent the reaction without requesting a view.
        """

        async with self.sessions() as session:
            view_rows = await session.scalars(
                select(ViewJob.account_id).where(
                    ViewJob.channel_id == channel_id,
                    ViewJob.message_id == message_id,
                    ViewJob.status.in_(["pending", "running", "done"]),
                )
            )
            planned_reaction_rows = await session.scalars(
                select(ReactionJob.account_id).where(
                    ReactionJob.channel_id == channel_id,
                    ReactionJob.message_id == message_id,
                    ReactionJob.view_included.is_(True),
                    ReactionJob.status.in_(["pending", "running"]),
                )
            )
            confirmed_reaction_rows = await session.scalars(
                select(ReactionJob.account_id).where(
                    ReactionJob.channel_id == channel_id,
                    ReactionJob.message_id == message_id,
                    ReactionJob.view_confirmed_at.is_not(None),
                )
            )
            archived_proof_rows = await session.scalars(
                select(JobHistoryKey.account_id).where(
                    JobHistoryKey.channel_id == channel_id,
                    JobHistoryKey.message_id == message_id,
                    or_(
                        JobHistoryKey.job_kind == "view",
                        and_(
                            JobHistoryKey.job_kind == "reaction",
                            JobHistoryKey.view_confirmed.is_(True),
                        ),
                    ),
                )
            )
            return (
                set(view_rows.all())
                | set(planned_reaction_rows.all())
                | set(confirmed_reaction_rows.all())
                | set(archived_proof_rows.all())
            )

    async def create_view_batch(
        self,
        *,
        channel_id: int,
        requested_post_count: int,
        posts_found: int,
        accounts_per_post: int,
        selection_mode: str,
        selection_value: int,
        jobs: Iterable[tuple[int, int, datetime]],
        skipped_existing: int = 0,
    ) -> ViewBatch:
        if selection_mode not in {"percent", "count"}:
            raise ValueError("Некорректный режим количества просмотров")
        prepared_jobs = list(jobs)
        async with self.sessions() as session:
            batch = ViewBatch(
                channel_id=channel_id,
                requested_post_count=requested_post_count,
                posts_found=posts_found,
                accounts_per_post=accounts_per_post,
                selection_mode=selection_mode,
                selection_value=selection_value,
                total_jobs=0,
                skipped_existing=max(0, int(skipped_existing)),
            )
            session.add(batch)
            await session.flush()

            existing_by_key: dict[tuple[int, int], ViewJob] = {}
            archived_keys: set[tuple[int, int]] = set()
            if prepared_jobs:
                account_ids = {account_id for account_id, _message_id, _due_at in prepared_jobs}
                message_ids = {message_id for _account_id, message_id, _due_at in prepared_jobs}
                archived_rows = (
                    await session.execute(
                        select(JobHistoryKey.account_id, JobHistoryKey.message_id).where(
                            JobHistoryKey.job_kind == "view",
                            JobHistoryKey.channel_id == channel_id,
                            JobHistoryKey.account_id.in_(account_ids),
                            JobHistoryKey.message_id.in_(message_ids),
                        )
                    )
                ).all()
                archived_keys = {(int(account_id), int(message_id)) for account_id, message_id in archived_rows}
                existing_rows = list(
                    (
                        await session.scalars(
                            select(ViewJob).where(
                                ViewJob.channel_id == channel_id,
                                ViewJob.account_id.in_(account_ids),
                                ViewJob.message_id.in_(message_ids),
                            )
                        )
                    ).all()
                )
                existing_by_key = {
                    (job.account_id, job.message_id): job for job in existing_rows
                }

            created = 0
            duplicate_count = 0
            moved_from_batches: dict[int, int] = {}
            for account_id, message_id, due_at in prepared_jobs:
                key = (int(account_id), int(message_id))
                if key in archived_keys:
                    duplicate_count += 1
                    continue
                existing = existing_by_key.get(key)
                if existing:
                    if existing.status in {"cancelled", "failed"}:
                        previous_batch_id = existing.batch_id
                        if previous_batch_id != batch.id:
                            moved_from_batches[previous_batch_id] = (
                                moved_from_batches.get(previous_batch_id, 0) + 1
                            )
                        existing.batch_id = batch.id
                        existing.due_at = due_at
                        existing.status = "pending"
                        existing.attempts = 0
                        existing.error = None
                        existing.completed_at = None
                        existing.started_at = None
                        created += 1
                    else:
                        duplicate_count += 1
                    continue
                new_job = ViewJob(
                    batch_id=batch.id,
                    channel_id=channel_id,
                    account_id=account_id,
                    message_id=message_id,
                    due_at=due_at,
                )
                session.add(new_job)
                existing_by_key[(account_id, message_id)] = new_job
                created += 1

            batch.total_jobs = created
            batch.skipped_existing += duplicate_count
            for previous_batch_id, moved_count in moved_from_batches.items():
                previous_batch = await session.get(ViewBatch, previous_batch_id)
                if previous_batch is None:
                    continue
                previous_batch.total_jobs = max(0, previous_batch.total_jobs - moved_count)
                await self._sync_view_batch_status(session, previous_batch.id)
            if created == 0:
                batch.status = "done"
                batch.completed_at = utcnow()
            await session.commit()
            await session.refresh(batch)
            return batch

    async def get_view_batch(self, batch_id: int) -> ViewBatch | None:
        async with self.sessions() as session:
            return await session.scalar(
                select(ViewBatch)
                .options(selectinload(ViewBatch.channel))
                .where(ViewBatch.id == batch_id)
            )

    async def latest_view_batch(self, channel_id: int) -> ViewBatch | None:
        async with self.sessions() as session:
            return await session.scalar(
                select(ViewBatch)
                .where(ViewBatch.channel_id == channel_id)
                .order_by(ViewBatch.id.desc())
                .limit(1)
            )

    async def get_due_view_job(self, now: datetime) -> ViewJob | None:
        async with self.sessions() as session:
            return await session.scalar(
                select(ViewJob)
                .join(ViewBatch, ViewBatch.id == ViewJob.batch_id)
                .options(selectinload(ViewJob.account), selectinload(ViewJob.channel))
                .where(
                    ViewJob.status == "pending",
                    ViewJob.due_at <= now,
                    ViewBatch.status != "cancelled",
                )
                .order_by(ViewJob.due_at, ViewJob.id)
                .limit(1)
            )

    async def mark_view_running(self, job_id: int) -> bool:
        """Atomically claim one due view job across concurrent workers."""

        now = utcnow()
        async with self.sessions() as session:
            result = await session.execute(
                update(ViewJob)
                .where(
                    ViewJob.id == job_id,
                    ViewJob.status == "pending",
                    ViewJob.due_at <= now,
                    ViewJob.batch_id.in_(
                        select(ViewBatch.id).where(ViewBatch.status != "cancelled")
                    ),
                )
                .values(
                    status="running",
                    started_at=now,
                    attempts=ViewJob.attempts + 1,
                )
            )
            if result.rowcount != 1:
                await session.rollback()
                return False
            batch_id = await session.scalar(
                select(ViewJob.batch_id).where(ViewJob.id == job_id)
            )
            if batch_id is not None:
                await session.execute(
                    update(ViewBatch)
                    .where(
                        ViewBatch.id == batch_id,
                        ViewBatch.status == "pending",
                    )
                    .values(status="running")
                )
            await session.commit()
            return True

    async def _sync_view_batch_status(self, session: AsyncSession, batch_id: int) -> None:
        batch = await session.get(ViewBatch, batch_id)
        if batch is None or batch.status == "cancelled":
            return
        rows = (
            await session.execute(
                select(ViewJob.status, func.count(ViewJob.id))
                .where(ViewJob.batch_id == batch_id)
                .group_by(ViewJob.status)
            )
        ).all()
        counts = {str(status): int(count) for status, count in rows}
        if counts.get("pending", 0) or counts.get("running", 0):
            batch.status = "running" if counts.get("running", 0) else "pending"
            batch.completed_at = None
        else:
            batch.status = "done"
            batch.completed_at = utcnow()

    async def complete_view_job(self, job_id: int) -> None:
        async with self.sessions() as session:
            job = await session.get(ViewJob, job_id)
            if job is None:
                return
            job.status = "done"
            job.error = None
            job.completed_at = utcnow()
            job.started_at = None
            await self._sync_view_batch_status(session, job.batch_id)
            await session.commit()

    async def retry_view_job(self, job_id: int, due_at: datetime, error: str) -> None:
        async with self.sessions() as session:
            job = await session.get(ViewJob, job_id)
            if job is None:
                return
            batch = await session.get(ViewBatch, job.batch_id)
            if batch and batch.status == "cancelled":
                job.status = "cancelled"
                job.error = "Задача остановлена администратором"
                job.completed_at = utcnow()
                job.started_at = None
            else:
                job.status = "pending"
                job.due_at = due_at
                job.error = error[:4000]
                job.completed_at = None
                job.started_at = None
                if batch:
                    batch.status = "pending"
                    batch.completed_at = None
            await session.commit()

    async def fail_view_job(self, job_id: int, error: str) -> None:
        async with self.sessions() as session:
            job = await session.get(ViewJob, job_id)
            if job is None:
                return
            job.status = "failed"
            job.error = error[:4000]
            job.completed_at = utcnow()
            job.started_at = None
            await self._sync_view_batch_status(session, job.batch_id)
            await session.commit()

    async def cancel_view_batch(self, batch_id: int) -> int:
        async with self.sessions() as session:
            batch = await session.get(ViewBatch, batch_id)
            if batch is None:
                return 0
            result = await session.execute(
                update(ViewJob)
                .where(ViewJob.batch_id == batch_id, ViewJob.status == "pending")
                .values(
                    status="cancelled", error="Остановлено администратором",
                    completed_at=utcnow(), started_at=None
                )
            )
            batch.status = "cancelled"
            batch.completed_at = utcnow()
            await session.commit()
            return int(result.rowcount or 0)

    async def view_counts(self, channel_id: int) -> dict[str, int]:
        result = {"pending": 0, "running": 0, "done": 0, "failed": 0, "cancelled": 0}
        async with self.sessions() as session:
            rows = (
                await session.execute(
                    select(ViewJob.status, func.count(ViewJob.id))
                    .where(ViewJob.channel_id == channel_id)
                    .group_by(ViewJob.status)
                )
            ).all()
        for status, count in rows:
            if status in result:
                result[str(status)] = int(count)
        async with self.sessions() as session:
            archived = (
                await session.execute(
                    select(
                        JobHistoryChannelSummary.status,
                        JobHistoryChannelSummary.count,
                    ).where(
                        JobHistoryChannelSummary.channel_id == channel_id,
                        JobHistoryChannelSummary.job_kind == "view",
                    )
                )
            ).all()
        for status, count in archived:
            status_text = str(status)
            if status_text in result:
                result[status_text] += int(count or 0)
        return result

    async def view_batch_counts(self, batch_id: int) -> dict[str, int]:
        result = {"pending": 0, "running": 0, "done": 0, "failed": 0, "cancelled": 0}
        async with self.sessions() as session:
            rows = (
                await session.execute(
                    select(ViewJob.status, func.count(ViewJob.id))
                    .where(ViewJob.batch_id == batch_id)
                    .group_by(ViewJob.status)
                )
            ).all()
        for status, count in rows:
            if status in result:
                result[str(status)] = int(count)
        return result
