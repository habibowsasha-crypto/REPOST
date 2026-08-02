from __future__ import annotations

from .db_shared import *  # noqa: F403


def _membership_result_conditions(job_id: int, expected_action: str | None) -> list:
    if expected_action is not None and expected_action not in {"join", "leave"}:
        raise ValueError("Недопустимое ожидаемое действие членства")
    conditions = [JoinJob.id == job_id]
    if expected_action is not None:
        # A scheduler may replace an in-flight join with a pending leave (or
        # vice versa). A stale worker result must never overwrite that newer
        # administrator intent.
        conditions.extend(
            [JoinJob.status == "running", JoinJob.action == expected_action]
        )
    return conditions


class MembershipJobDatabaseMixin:
    async def schedule_join_jobs(self, channel_id: int, account_due_pairs: Iterable[tuple[int, datetime]]) -> int:
        return await self._schedule_membership_jobs(channel_id, account_due_pairs, action="join")

    async def schedule_leave_jobs(self, channel_id: int, account_due_pairs: Iterable[tuple[int, datetime]]) -> int:
        return await self._schedule_membership_jobs(channel_id, account_due_pairs, action="leave")

    async def _schedule_membership_jobs(
        self,
        channel_id: int,
        account_due_pairs: Iterable[tuple[int, datetime]],
        *,
        action: str,
    ) -> int:
        if action not in {"join", "leave"}:
            raise ValueError("Недопустимое действие членства")
        changed = 0
        async with self.sessions() as session:
            for account_id, due_at in account_due_pairs:
                existing = await session.scalar(
                    select(JoinJob).where(
                        JoinJob.channel_id == channel_id,
                        JoinJob.account_id == account_id,
                    )
                )
                if existing:
                    final_status = "done" if action == "join" else "left"
                    if existing.action == action and existing.status in {final_status, "running"}:
                        # A matching in-flight operation already owns this row. Resetting
                        # it to pending here would allow a duplicate Telegram action and
                        # make the worker's eventual result stale by construction.
                        continue
                    existing.action = action
                    existing.status = "pending"
                    existing.due_at = due_at
                    existing.error = None
                    existing.completed_at = None
                    existing.started_at = None
                    existing.attempts = 0
                    changed += 1
                    continue
                session.add(
                    JoinJob(
                        channel_id=channel_id,
                        account_id=account_id,
                        action=action,
                        due_at=due_at,
                    )
                )
                changed += 1
            await session.commit()
        return changed

    async def get_due_join_job(self, now: datetime) -> JoinJob | None:
        async with self.sessions() as session:
            stmt = (
                select(JoinJob)
                .options(selectinload(JoinJob.account), selectinload(JoinJob.channel))
                .where(JoinJob.status == "pending", JoinJob.due_at <= now)
                .order_by(JoinJob.due_at, JoinJob.id)
                .limit(1)
            )
            return await session.scalar(stmt)

    async def mark_join_running(
        self, job_id: int, *, expected_action: str | None = None
    ) -> bool:
        if expected_action is not None and expected_action not in {"join", "leave"}:
            raise ValueError("Недопустимое ожидаемое действие членства")
        conditions = [
            JoinJob.id == job_id,
            JoinJob.status == "pending",
            JoinJob.due_at <= utcnow(),
        ]
        if expected_action is not None:
            conditions.append(JoinJob.action == expected_action)
        async with self.sessions() as session:
            result = await session.execute(
                update(JoinJob)
                .where(*conditions)
                .values(status="running", started_at=utcnow(), attempts=JoinJob.attempts + 1)
            )
            await session.commit()
            return bool(result.rowcount)

    async def membership_job_is_running(
        self, job_id: int, *, expected_action: str
    ) -> bool:
        if expected_action not in {"join", "leave"}:
            raise ValueError("Недопустимое ожидаемое действие членства")
        async with self.sessions() as session:
            current = await session.scalar(
                select(JoinJob.id).where(
                    JoinJob.id == job_id,
                    JoinJob.status == "running",
                    JoinJob.action == expected_action,
                )
            )
            return current is not None

    async def complete_join_job(
        self,
        job_id: int,
        *,
        final_status: str = "done",
        expected_action: str | None = None,
    ) -> bool:
        if final_status not in {"done", "left"}:
            raise ValueError("Недопустимый итоговый статус")
        async with self.sessions() as session:
            result = await session.execute(
                update(JoinJob)
                .where(*_membership_result_conditions(job_id, expected_action))
                .values(status=final_status, error=None, completed_at=utcnow(), started_at=None)
            )
            await session.commit()
            return bool(result.rowcount)

    async def retry_join_job(
        self,
        job_id: int,
        due_at: datetime,
        error: str,
        *,
        expected_action: str | None = None,
    ) -> bool:
        async with self.sessions() as session:
            result = await session.execute(
                update(JoinJob)
                .where(*_membership_result_conditions(job_id, expected_action))
                .values(status="pending", due_at=due_at, error=error[:4000], started_at=None)
            )
            await session.commit()
            return bool(result.rowcount)

    async def fail_join_job(
        self,
        job_id: int,
        error: str,
        *,
        expected_action: str | None = None,
    ) -> bool:
        async with self.sessions() as session:
            result = await session.execute(
                update(JoinJob)
                .where(*_membership_result_conditions(job_id, expected_action))
                .values(
                    status="failed", error=error[:4000], completed_at=utcnow(), started_at=None
                )
            )
            await session.commit()
            return bool(result.rowcount)

    async def joined_account_ids(self, channel_id: int) -> list[int]:
        async with self.sessions() as session:
            stmt = (
                select(Account.id)
                .join(JoinJob, JoinJob.account_id == Account.id)
                .where(
                    JoinJob.channel_id == channel_id,
                    JoinJob.action == "join",
                    JoinJob.status == "done",
                    Account.is_active.is_(True),
                    Account.status != "unauthorized",
                )
                .order_by(Account.id)
            )
            return list((await session.scalars(stmt)).all())

    async def membership_counts(self, channel_id: int) -> dict[str, int]:
        async with self.sessions() as session:
            result = {"joined": 0, "pending": 0, "failed": 0, "left": 0}
            rows = (
                await session.execute(
                    select(JoinJob.action, JoinJob.status, func.count(JoinJob.id))
                    .join(Account, Account.id == JoinJob.account_id)
                    .where(
                        JoinJob.channel_id == channel_id,
                        Account.is_active.is_(True),
                        Account.status != "unauthorized",
                    )
                    .group_by(JoinJob.action, JoinJob.status)
                )
            ).all()
            for action, status, count in rows:
                if action == "join" and status == "done":
                    result["joined"] += int(count)
                elif action == "leave" and status == "left":
                    result["left"] += int(count)
                elif status in {"pending", "running"}:
                    result["pending"] += int(count)
                elif status == "failed":
                    result["failed"] += int(count)
            return result
