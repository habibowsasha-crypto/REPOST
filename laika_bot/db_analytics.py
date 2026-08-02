from __future__ import annotations

from .db_shared import *  # noqa: F403


class AnalyticsDatabaseMixin:
    async def prune_job_history(
        self,
        retention_days: int,
        *,
        now: datetime | None = None,
        max_rows_per_kind: int = 10000,
    ) -> dict[str, int]:
        """Archive and delete old terminal reaction/view rows without losing totals.

        Completed dedup/view proof is copied to compact history keys before deletion.
        Join jobs are deliberately retained because each row is the current membership
        state for one account/channel pair, not append-only history.
        """

        if retention_days <= 0:
            raise ValueError("Срок хранения истории должен быть больше нуля")
        if max_rows_per_kind <= 0:
            raise ValueError("Размер пакета очистки должен быть больше нуля")
        current = now or utcnow()
        cutoff = current - timedelta(days=retention_days)
        result = {
            "reaction_deleted": 0,
            "view_deleted": 0,
            "view_batches_deleted": 0,
            "summary_rows_updated": 0,
            "dedup_keys_created": 0,
        }
        terminal_specs = (
            (ReactionJob, "reaction", ("done", "failed", "cancelled")),
            (ViewJob, "view", ("done", "failed", "cancelled")),
        )

        async with self.sessions() as session:
            for model, job_kind, statuses in terminal_specs:
                eligible = and_(
                    model.status.in_(statuses),
                    model.completed_at.is_not(None),
                    model.completed_at < cutoff,
                )
                selected_ids = list(
                    (
                        await session.scalars(
                            select(model.id)
                            .where(eligible)
                            .order_by(model.completed_at, model.id)
                            .limit(max_rows_per_kind)
                        )
                    ).all()
                )
                if not selected_ids:
                    continue
                selected = model.id.in_(selected_ids)
                grouped = (
                    await session.execute(
                        select(model.account_id, model.status, func.count(model.id))
                        .where(selected)
                        .group_by(model.account_id, model.status)
                    )
                ).all()
                for account_id, status, count in grouped:
                    summary = await session.scalar(
                        select(JobHistorySummary)
                        .where(
                            JobHistorySummary.account_id == int(account_id),
                            JobHistorySummary.job_kind == job_kind,
                            JobHistorySummary.status == str(status),
                        )
                        .with_for_update()
                    )
                    if summary is None:
                        session.add(
                            JobHistorySummary(
                                account_id=int(account_id),
                                job_kind=job_kind,
                                status=str(status),
                                count=int(count),
                                updated_at=current,
                            )
                        )
                    else:
                        summary.count += int(count)
                        summary.updated_at = current
                    result["summary_rows_updated"] += 1

                grouped_channels = (
                    await session.execute(
                        select(model.channel_id, model.status, func.count(model.id))
                        .where(selected)
                        .group_by(model.channel_id, model.status)
                    )
                ).all()
                for channel_id, status, count in grouped_channels:
                    channel_summary = await session.scalar(
                        select(JobHistoryChannelSummary)
                        .where(
                            JobHistoryChannelSummary.channel_id == int(channel_id),
                            JobHistoryChannelSummary.job_kind == job_kind,
                            JobHistoryChannelSummary.status == str(status),
                        )
                        .with_for_update()
                    )
                    if channel_summary is None:
                        session.add(
                            JobHistoryChannelSummary(
                                channel_id=int(channel_id),
                                job_kind=job_kind,
                                status=str(status),
                                count=int(count),
                                updated_at=current,
                            )
                        )
                    else:
                        channel_summary.count += int(count)
                        channel_summary.updated_at = current
                    result["summary_rows_updated"] += 1

                view_expression = (
                    case((model.view_confirmed_at.is_not(None), True), else_=False)
                    if model is ReactionJob
                    else literal(True)
                )
                key_select = select(
                    literal(job_kind),
                    model.channel_id,
                    model.account_id,
                    model.message_id,
                    view_expression,
                    literal(current),
                ).where(selected, model.status == "done")
                dialect_name = session.bind.dialect.name if session.bind is not None else ""
                if dialect_name == "sqlite":
                    key_insert = sqlite_insert(JobHistoryKey).from_select(
                        [
                            "job_kind", "channel_id", "account_id", "message_id",
                            "view_confirmed", "archived_at",
                        ],
                        key_select,
                    ).on_conflict_do_nothing(
                        index_elements=[
                            "job_kind", "channel_id", "account_id", "message_id"
                        ]
                    )
                elif dialect_name == "postgresql":
                    key_insert = postgresql_insert(JobHistoryKey).from_select(
                        [
                            "job_kind", "channel_id", "account_id", "message_id",
                            "view_confirmed", "archived_at",
                        ],
                        key_select,
                    ).on_conflict_do_nothing(
                        index_elements=[
                            "job_kind", "channel_id", "account_id", "message_id"
                        ]
                    )
                else:
                    raise RuntimeError(
                        f"Unsupported database dialect for history retention: {dialect_name}"
                    )
                archived_keys = await session.execute(key_insert)
                result["dedup_keys_created"] += int(archived_keys.rowcount or 0)

                deleted_rows = await session.execute(delete(model).where(selected))
                result[f"{job_kind}_deleted"] = int(deleted_rows.rowcount or 0)

            await session.flush()
            empty_batch_ids = list(
                (
                    await session.scalars(
                        select(ViewBatch.id)
                        .outerjoin(ViewJob, ViewJob.batch_id == ViewBatch.id)
                        .where(
                            ViewBatch.status.in_(["done", "cancelled"]),
                            ViewBatch.completed_at.is_not(None),
                            ViewBatch.completed_at < cutoff,
                        )
                        .group_by(ViewBatch.id)
                        .having(func.count(ViewJob.id) == 0)
                    )
                ).all()
            )
            if empty_batch_ids:
                deleted_batches = await session.execute(
                    delete(ViewBatch).where(ViewBatch.id.in_(empty_batch_ids))
                )
                result["view_batches_deleted"] = int(deleted_batches.rowcount or 0)
            await session.commit()
        return result

    async def account_workloads(
        self,
        account_ids: Iterable[int],
        *,
        now: datetime | None = None,
        lookback_hours: int = 24,
    ) -> dict[int, AccountWorkload]:
        """Return bounded recent workload metrics for adaptive scheduling.

        Current queue load and recent outcomes are aggregated across join,
        reaction and view jobs in one SQL statement. Only requested account ids
        are read, and old history cannot permanently disadvantage an account.
        """

        ids = list(dict.fromkeys(int(item) for item in account_ids))
        if not ids:
            return {}
        current = now or utcnow()
        cutoff = current - timedelta(hours=max(1, int(lookback_hours)))

        event_selects = []
        for model, completed_statuses in (
            (JoinJob, ("done", "left")),
            (ReactionJob, ("done",)),
            (ViewJob, ("done",)),
        ):
            event_selects.append(
                select(
                    model.account_id.label("account_id"),
                    case((model.status == "pending", 1), else_=0).label(
                        "pending"
                    ),
                    case((model.status == "running", 1), else_=0).label(
                        "running"
                    ),
                    case(
                        (
                            and_(
                                model.status.in_(completed_statuses),
                                model.completed_at.is_not(None),
                                model.completed_at >= cutoff,
                            ),
                            1,
                        ),
                        else_=0,
                    ).label("completed_recent"),
                    case(
                        (
                            and_(
                                model.status == "failed",
                                model.completed_at.is_not(None),
                                model.completed_at >= cutoff,
                            ),
                            1,
                        ),
                        else_=0,
                    ).label("failed_recent"),
                    model.completed_at.label("completed_at"),
                ).where(model.account_id.in_(ids))
            )

        events = union_all(*event_selects).subquery("account_workload_events")
        aggregates = (
            select(
                events.c.account_id.label("account_id"),
                func.sum(events.c.pending).label("pending"),
                func.sum(events.c.running).label("running"),
                func.sum(events.c.completed_recent).label("completed_recent"),
                func.sum(events.c.failed_recent).label("failed_recent"),
                func.max(events.c.completed_at).label("last_completed_at"),
            )
            .group_by(events.c.account_id)
            .subquery("account_workload_totals")
        )

        async with self.sessions() as session:
            rows = (
                await session.execute(
                    select(
                        Account,
                        func.coalesce(aggregates.c.pending, 0),
                        func.coalesce(aggregates.c.running, 0),
                        func.coalesce(aggregates.c.completed_recent, 0),
                        func.coalesce(aggregates.c.failed_recent, 0),
                        aggregates.c.last_completed_at,
                    )
                    .outerjoin(
                        aggregates, aggregates.c.account_id == Account.id
                    )
                    .where(Account.id.in_(ids))
                )
            ).all()

        result: dict[int, AccountWorkload] = {}
        for (
            account,
            pending,
            running,
            completed_recent,
            failed_recent,
            last_completed_at,
        ) in rows:
            last_action_at = account.last_reaction_at
            if last_completed_at is not None and (
                last_action_at is None or last_completed_at > last_action_at
            ):
                last_action_at = last_completed_at
            result[int(account.id)] = AccountWorkload(
                account_id=int(account.id),
                pending=int(pending or 0),
                running=int(running or 0),
                completed_recent=int(completed_recent or 0),
                failed_recent=int(failed_recent or 0),
                last_action_at=last_action_at,
                flood_until=account.flood_until,
                last_error=account.last_error,
                last_error_at=account.updated_at,
                status=account.status,
                is_active=account.is_active,
            )
        return result

    async def account_health_overview(
        self, account_id: int | None = None
    ) -> list[dict[str, object]]:
        """Return accounts with current queue load and preserved failure history."""

        async with self.sessions() as session:
            account_query = select(Account).order_by(Account.id)
            if account_id is not None:
                account_query = account_query.where(Account.id == account_id)
            accounts = list((await session.scalars(account_query)).all())
            result_by_id: dict[int, dict[str, object]] = {
                account.id: {
                    "account": account,
                    "pending": 0,
                    "running": 0,
                    "failed": 0,
                    "completed": 0,
                }
                for account in accounts
            }
            if not result_by_id:
                return []
            account_ids = tuple(result_by_id)
            for model, completed_statuses in (
                (JoinJob, {"done", "left"}),
                (ReactionJob, {"done"}),
                (ViewJob, {"done"}),
            ):
                rows = (
                    await session.execute(
                        select(model.account_id, model.status, func.count(model.id))
                        .where(model.account_id.in_(account_ids))
                        .group_by(model.account_id, model.status)
                    )
                ).all()
                for account_id, status, count in rows:
                    item = result_by_id.get(int(account_id))
                    if item is None:
                        continue
                    status_text = str(status)
                    if status_text == "pending":
                        item["pending"] = int(item["pending"]) + int(count)
                    elif status_text == "running":
                        item["running"] = int(item["running"]) + int(count)
                    elif status_text == "failed":
                        item["failed"] = int(item["failed"]) + int(count)
                    elif status_text in completed_statuses:
                        item["completed"] = int(item["completed"]) + int(count)

            archived_rows = (
                await session.execute(
                    select(
                        JobHistorySummary.account_id,
                        JobHistorySummary.status,
                        func.sum(JobHistorySummary.count),
                    )
                    .where(JobHistorySummary.account_id.in_(account_ids))
                    .group_by(JobHistorySummary.account_id, JobHistorySummary.status)
                )
            ).all()
            for archived_account_id, status, count in archived_rows:
                item = result_by_id.get(int(archived_account_id))
                if item is None:
                    continue
                status_text = str(status)
                if status_text == "failed":
                    item["failed"] = int(item["failed"]) + int(count or 0)
                elif status_text == "done":
                    item["completed"] = int(item["completed"]) + int(count or 0)
            return list(result_by_id.values())

    async def recent_failure_counts(
        self, window_minutes: int, *, now: datetime | None = None
    ) -> dict[str, int]:
        """Count recent terminal failures across all three queues in one SELECT."""

        minutes = max(1, int(window_minutes))
        current = now or utcnow()
        cutoff = current - timedelta(minutes=minutes)
        failure_events = union_all(
            select(
                literal("join").label("job_kind"),
                JoinJob.id.label("job_id"),
            ).where(
                JoinJob.status == "failed",
                JoinJob.completed_at.is_not(None),
                JoinJob.completed_at >= cutoff,
            ),
            select(
                literal("reaction").label("job_kind"),
                ReactionJob.id.label("job_id"),
            ).where(
                ReactionJob.status == "failed",
                ReactionJob.completed_at.is_not(None),
                ReactionJob.completed_at >= cutoff,
            ),
            select(
                literal("view").label("job_kind"),
                ViewJob.id.label("job_id"),
            ).where(
                ViewJob.status == "failed",
                ViewJob.completed_at.is_not(None),
                ViewJob.completed_at >= cutoff,
            ),
        ).subquery()
        async with self.sessions() as session:
            rows = (
                await session.execute(
                    select(
                        failure_events.c.job_kind,
                        func.count(failure_events.c.job_id),
                    ).group_by(failure_events.c.job_kind)
                )
            ).all()
        result = {"join": 0, "reaction": 0, "view": 0, "total": 0}
        for job_kind, count in rows:
            kind = str(job_kind)
            if kind not in result:
                continue
            result[kind] = int(count or 0)
        result["total"] = result["join"] + result["reaction"] + result["view"]
        return result

    async def analytics_snapshot(
        self,
        period_key: str,
        *,
        now: datetime | None = None,
        top_limit: int | None = 10,
    ) -> dict[str, object]:
        """Return bounded operational analytics for one strict UTC period.

        Reaction and view rows are append-only until retention archives them.
        Join rows intentionally represent the latest membership state for one
        account/target pair, so their counts are state-oriented rather than a full
        append-only audit log. Archived rows have no original completion date and
        are therefore included only in the all-time report.
        """

        current = now or utcnow()
        period, cutoff = resolve_analytics_period(period_key, now=current)
        limit = None if top_limit is None else min(100, max(1, int(top_limit)))
        dialect_name = self.engine.dialect.name

        def seconds_between(later, earlier):
            if dialect_name == "postgresql":
                return func.extract("epoch", later - earlier)
            return (func.julianday(later) - func.julianday(earlier)) * 86400.0

        def live_event_select(model, job_kind: str, statuses: tuple[str, ...]):
            queue_lag_raw = seconds_between(model.started_at, model.due_at)
            execution_raw = seconds_between(model.completed_at, model.started_at)
            conditions = [
                model.status.in_(statuses),
                model.completed_at.is_not(None),
            ]
            if cutoff is not None:
                conditions.append(model.completed_at >= cutoff)
            return select(
                literal(job_kind).label("job_kind"),
                model.account_id.label("account_id"),
                model.channel_id.label("channel_id"),
                model.status.label("status"),
                model.completed_at.label("completed_at"),
                case(
                    (
                        model.started_at.is_not(None),
                        case((queue_lag_raw > 0, queue_lag_raw), else_=0.0),
                    ),
                    else_=None,
                ).label("queue_lag_seconds"),
                case(
                    (
                        and_(
                            model.started_at.is_not(None),
                            model.completed_at.is_not(None),
                            execution_raw >= 0,
                        ),
                        execution_raw,
                    ),
                    else_=None,
                ).label("execution_seconds"),
            ).where(*conditions)

        events = union_all(
            live_event_select(
                JoinJob,
                "join",
                ("done", "left", "failed", "cancelled"),
            ),
            live_event_select(
                ReactionJob,
                "reaction",
                ("done", "failed", "cancelled"),
            ),
            live_event_select(
                ViewJob,
                "view",
                ("done", "failed", "cancelled"),
            ),
        ).subquery("analytics_events")

        queue_events = union_all(
            select(
                literal("join").label("job_kind"),
                JoinJob.account_id.label("account_id"),
                JoinJob.channel_id.label("channel_id"),
                JoinJob.status.label("status"),
            ).where(JoinJob.status.in_(("pending", "running"))),
            select(
                literal("reaction").label("job_kind"),
                ReactionJob.account_id.label("account_id"),
                ReactionJob.channel_id.label("channel_id"),
                ReactionJob.status.label("status"),
            ).where(ReactionJob.status.in_(("pending", "running"))),
            select(
                literal("view").label("job_kind"),
                ViewJob.account_id.label("account_id"),
                ViewJob.channel_id.label("channel_id"),
                ViewJob.status.label("status"),
            ).where(ViewJob.status.in_(("pending", "running"))),
        ).subquery("analytics_queue_events")

        success_flag = case(
            (
                and_(
                    events.c.job_kind == "join",
                    events.c.status.in_(("done", "left")),
                ),
                1,
            ),
            (
                and_(
                    events.c.job_kind.in_(("reaction", "view")),
                    events.c.status == "done",
                ),
                1,
            ),
            else_=0,
        )
        failed_flag = case((events.c.status == "failed", 1), else_=0)
        cancelled_flag = case((events.c.status == "cancelled", 1), else_=0)
        queue_pending_flag = case((queue_events.c.status == "pending", 1), else_=0)
        queue_running_flag = case((queue_events.c.status == "running", 1), else_=0)

        jobs: dict[str, dict[str, object]] = {
            kind: {
                "success": 0,
                "failed": 0,
                "cancelled": 0,
                "terminal": 0,
                "pending": 0,
                "running": 0,
                "success_rate": None,
                "avg_queue_lag_seconds": None,
                "avg_execution_seconds": None,
                "queue_samples": 0,
                "timing_samples": 0,
            }
            for kind in ("join", "reaction", "view")
        }
        accounts_by_id: dict[int, dict[str, object]] = {}
        targets_by_id: dict[int, dict[str, object]] = {}

        def merge_rank(
            target: dict[int, dict[str, object]],
            *,
            object_id: int,
            name: str,
            kind: str | None,
            success: int = 0,
            failed: int = 0,
            cancelled: int = 0,
            pending: int = 0,
            running: int = 0,
        ) -> None:
            item = target.setdefault(
                int(object_id),
                {
                    "id": int(object_id),
                    "name": name or "Без названия",
                    "kind": kind,
                    "success": 0,
                    "failed": 0,
                    "cancelled": 0,
                    "pending": 0,
                    "running": 0,
                },
            )
            item["success"] = int(item["success"]) + int(success or 0)
            item["failed"] = int(item["failed"]) + int(failed or 0)
            item["cancelled"] = int(item["cancelled"]) + int(cancelled or 0)
            item["pending"] = int(item["pending"]) + int(pending or 0)
            item["running"] = int(item["running"]) + int(running or 0)

        def account_label(display_name: object, username: object, account_id: int) -> str:
            label = str(display_name or "").strip()
            username_text = str(username or "").strip().lstrip("@")
            if username_text:
                label = f"{label} (@{username_text})" if label else f"@{username_text}"
            return label or f"Аккаунт #{account_id}"

        async with self.sessions() as session:
            account_counts = (
                await session.execute(
                    select(
                        func.count(Account.id).label("accounts"),
                        func.sum(
                            case(
                                (
                                    and_(
                                        Account.is_active.is_(True),
                                        Account.status != "unauthorized",
                                    ),
                                    1,
                                ),
                                else_=0,
                            )
                        ).label("active_accounts"),
                        func.sum(
                            case((Account.status == "unauthorized", 1), else_=0)
                        ).label("problem_accounts"),
                    )
                )
            ).one()
            target_counts = (
                await session.execute(
                    select(
                        func.count(Channel.id).label("targets"),
                        func.sum(
                            case((Channel.is_active.is_(True), 1), else_=0)
                        ).label("active_targets"),
                        func.sum(
                            case((Channel.kind == "channel", 1), else_=0)
                        ).label("channels"),
                        func.sum(
                            case((Channel.kind == "group", 1), else_=0)
                        ).label("groups"),
                    )
                )
            ).one()

            # Seed rankings with every known account and target. The Telegram UI
            # still shows a bounded top list, while the CSV export can request the
            # complete directory and therefore includes objects with zero activity.
            account_directory_rows = (
                await session.execute(
                    select(Account.id, Account.display_name, Account.username)
                )
            ).all()
            for account_id, display_name, username in account_directory_rows:
                merge_rank(
                    accounts_by_id,
                    object_id=int(account_id),
                    name=account_label(display_name, username, int(account_id)),
                    kind=None,
                )

            target_directory_rows = (
                await session.execute(
                    select(Channel.id, Channel.title, Channel.kind)
                )
            ).all()
            for channel_id, title, target_kind in target_directory_rows:
                merge_rank(
                    targets_by_id,
                    object_id=int(channel_id),
                    name=str(title or f"Цель #{channel_id}"),
                    kind=str(target_kind or "channel"),
                )

            queue_rows = (
                await session.execute(
                    select(
                        queue_events.c.job_kind,
                        queue_events.c.status,
                        func.count(),
                    ).group_by(queue_events.c.job_kind, queue_events.c.status)
                )
            ).all()
            for job_kind, status, count in queue_rows:
                kind_text = str(job_kind)
                status_text = str(status)
                if kind_text in jobs and status_text in {"pending", "running"}:
                    jobs[kind_text][status_text] = int(count or 0)

            global_rows = (
                await session.execute(
                    select(
                        events.c.job_kind,
                        func.sum(success_flag),
                        func.sum(failed_flag),
                        func.sum(cancelled_flag),
                    ).group_by(events.c.job_kind)
                )
            ).all()
            for job_kind, success, failed, cancelled in global_rows:
                kind_text = str(job_kind)
                if kind_text not in jobs:
                    continue
                jobs[kind_text]["success"] = int(success or 0)
                jobs[kind_text]["failed"] = int(failed or 0)
                jobs[kind_text]["cancelled"] = int(cancelled or 0)

            timing_rows = (
                await session.execute(
                    select(
                        events.c.job_kind,
                        func.avg(events.c.queue_lag_seconds),
                        func.avg(events.c.execution_seconds),
                        func.count(events.c.queue_lag_seconds),
                        func.count(events.c.execution_seconds),
                    ).group_by(events.c.job_kind)
                )
            ).all()
            for job_kind, avg_queue, avg_execution, queue_samples, timing_samples in timing_rows:
                kind_text = str(job_kind)
                if kind_text not in jobs:
                    continue
                jobs[kind_text]["avg_queue_lag_seconds"] = (
                    float(avg_queue) if avg_queue is not None else None
                )
                jobs[kind_text]["avg_execution_seconds"] = (
                    float(avg_execution) if avg_execution is not None else None
                )
                jobs[kind_text]["queue_samples"] = int(queue_samples or 0)
                jobs[kind_text]["timing_samples"] = int(timing_samples or 0)

            account_rows = (
                await session.execute(
                    select(
                        Account.id,
                        Account.display_name,
                        Account.username,
                        func.sum(success_flag),
                        func.sum(failed_flag),
                        func.sum(cancelled_flag),
                    )
                    .join(events, events.c.account_id == Account.id)
                    .group_by(Account.id, Account.display_name, Account.username)
                )
            ).all()
            for account_id, display_name, username, success, failed, cancelled in account_rows:
                merge_rank(
                    accounts_by_id,
                    object_id=int(account_id),
                    name=account_label(display_name, username, int(account_id)),
                    kind=None,
                    success=int(success or 0),
                    failed=int(failed or 0),
                    cancelled=int(cancelled or 0),
                )

            account_queue_rows = (
                await session.execute(
                    select(
                        Account.id,
                        Account.display_name,
                        Account.username,
                        func.sum(queue_pending_flag),
                        func.sum(queue_running_flag),
                    )
                    .join(queue_events, queue_events.c.account_id == Account.id)
                    .group_by(Account.id, Account.display_name, Account.username)
                )
            ).all()
            for account_id, display_name, username, pending, running in account_queue_rows:
                merge_rank(
                    accounts_by_id,
                    object_id=int(account_id),
                    name=account_label(display_name, username, int(account_id)),
                    kind=None,
                    pending=int(pending or 0),
                    running=int(running or 0),
                )

            target_rows = (
                await session.execute(
                    select(
                        Channel.id,
                        Channel.title,
                        Channel.kind,
                        func.sum(success_flag),
                        func.sum(failed_flag),
                        func.sum(cancelled_flag),
                    )
                    .join(events, events.c.channel_id == Channel.id)
                    .group_by(Channel.id, Channel.title, Channel.kind)
                )
            ).all()
            for channel_id, title, target_kind, success, failed, cancelled in target_rows:
                merge_rank(
                    targets_by_id,
                    object_id=int(channel_id),
                    name=str(title or f"Цель #{channel_id}"),
                    kind=str(target_kind or "channel"),
                    success=int(success or 0),
                    failed=int(failed or 0),
                    cancelled=int(cancelled or 0),
                )

            target_queue_rows = (
                await session.execute(
                    select(
                        Channel.id,
                        Channel.title,
                        Channel.kind,
                        func.sum(queue_pending_flag),
                        func.sum(queue_running_flag),
                    )
                    .join(queue_events, queue_events.c.channel_id == Channel.id)
                    .group_by(Channel.id, Channel.title, Channel.kind)
                )
            ).all()
            for channel_id, title, target_kind, pending, running in target_queue_rows:
                merge_rank(
                    targets_by_id,
                    object_id=int(channel_id),
                    name=str(title or f"Цель #{channel_id}"),
                    kind=str(target_kind or "channel"),
                    pending=int(pending or 0),
                    running=int(running or 0),
                )

            day_expression = func.date(events.c.completed_at)
            daily_rows = (
                await session.execute(
                    select(
                        day_expression.label("day"),
                        func.sum(success_flag),
                        func.sum(failed_flag),
                        func.sum(cancelled_flag),
                    )
                    .group_by(day_expression)
                    .order_by(day_expression)
                )
            ).all()

            if period.key == "all":
                archived_accounts = (
                    await session.execute(
                        select(
                            JobHistorySummary.account_id,
                            Account.display_name,
                            Account.username,
                            JobHistorySummary.job_kind,
                            JobHistorySummary.status,
                            func.sum(JobHistorySummary.count),
                        )
                        .join(Account, Account.id == JobHistorySummary.account_id)
                        .group_by(
                            JobHistorySummary.account_id,
                            Account.display_name,
                            Account.username,
                            JobHistorySummary.job_kind,
                            JobHistorySummary.status,
                        )
                    )
                ).all()
                for (
                    account_id,
                    display_name,
                    username,
                    job_kind,
                    status,
                    count,
                ) in archived_accounts:
                    kind_text = str(job_kind)
                    status_text = str(status)
                    if kind_text not in jobs:
                        continue
                    amount = int(count or 0)
                    if status_text == "done":
                        jobs[kind_text]["success"] = int(jobs[kind_text]["success"]) + amount
                        success, failed, cancelled = amount, 0, 0
                    elif status_text == "failed":
                        jobs[kind_text]["failed"] = int(jobs[kind_text]["failed"]) + amount
                        success, failed, cancelled = 0, amount, 0
                    elif status_text == "cancelled":
                        jobs[kind_text]["cancelled"] = int(jobs[kind_text]["cancelled"]) + amount
                        success, failed, cancelled = 0, 0, amount
                    else:
                        continue
                    merge_rank(
                        accounts_by_id,
                        object_id=int(account_id),
                        name=account_label(display_name, username, int(account_id)),
                        kind=None,
                        success=success,
                        failed=failed,
                        cancelled=cancelled,
                    )

                archived_targets = (
                    await session.execute(
                        select(
                            JobHistoryChannelSummary.channel_id,
                            Channel.title,
                            Channel.kind,
                            JobHistoryChannelSummary.status,
                            func.sum(JobHistoryChannelSummary.count),
                        )
                        .join(Channel, Channel.id == JobHistoryChannelSummary.channel_id)
                        .group_by(
                            JobHistoryChannelSummary.channel_id,
                            Channel.title,
                            Channel.kind,
                            JobHistoryChannelSummary.status,
                        )
                    )
                ).all()
                for channel_id, title, target_kind, status, count in archived_targets:
                    amount = int(count or 0)
                    status_text = str(status)
                    if status_text == "done":
                        success, failed, cancelled = amount, 0, 0
                    elif status_text == "failed":
                        success, failed, cancelled = 0, amount, 0
                    elif status_text == "cancelled":
                        success, failed, cancelled = 0, 0, amount
                    else:
                        continue
                    merge_rank(
                        targets_by_id,
                        object_id=int(channel_id),
                        name=str(title or f"Цель #{channel_id}"),
                        kind=str(target_kind or "channel"),
                        success=success,
                        failed=failed,
                        cancelled=cancelled,
                    )

        for item in jobs.values():
            success = int(item["success"])
            failed = int(item["failed"])
            cancelled = int(item["cancelled"])
            item["terminal"] = success + failed + cancelled
            denominator = success + failed
            item["success_rate"] = (
                success * 100.0 / denominator if denominator > 0 else None
            )

        def finalize_ranking(rows: dict[int, dict[str, object]]) -> list[dict[str, object]]:
            result: list[dict[str, object]] = []
            for row in rows.values():
                success = int(row["success"])
                failed = int(row["failed"])
                cancelled = int(row["cancelled"])
                denominator = success + failed
                row["terminal"] = success + failed + cancelled
                row["success_rate"] = (
                    success * 100.0 / denominator if denominator > 0 else None
                )
                result.append(row)
            result.sort(
                key=lambda row: (
                    -int(row["success"]),
                    int(row["failed"]),
                    -int(row["terminal"]),
                    -int(row["pending"]) - int(row["running"]),
                    str(row["name"]).casefold(),
                )
            )
            return result

        ranked_accounts = finalize_ranking(accounts_by_id)
        ranked_targets = finalize_ranking(targets_by_id)
        problem_accounts = sorted(
            (row for row in ranked_accounts if int(row["failed"]) > 0),
            key=lambda row: (
                -int(row["failed"]),
                int(row["success"]),
                str(row["name"]).casefold(),
            ),
        )[: (limit if limit is not None else len(ranked_accounts))]
        problem_targets = sorted(
            (row for row in ranked_targets if int(row["failed"]) > 0),
            key=lambda row: (
                -int(row["failed"]),
                int(row["success"]),
                str(row["name"]).casefold(),
            ),
        )[: (limit if limit is not None else len(ranked_targets))]

        total_success = sum(int(item["success"]) for item in jobs.values())
        total_failed = sum(int(item["failed"]) for item in jobs.values())
        total_cancelled = sum(int(item["cancelled"]) for item in jobs.values())
        total_terminal = total_success + total_failed + total_cancelled
        success_denominator = total_success + total_failed

        total_queue_samples = sum(int(item["queue_samples"]) for item in jobs.values())
        total_timing_samples = sum(int(item["timing_samples"]) for item in jobs.values())
        weighted_queue = sum(
            float(item["avg_queue_lag_seconds"] or 0.0) * int(item["queue_samples"])
            for item in jobs.values()
        )
        weighted_execution = sum(
            float(item["avg_execution_seconds"] or 0.0) * int(item["timing_samples"])
            for item in jobs.values()
        )

        daily = [
            {
                "date": day,
                "success": int(success or 0),
                "failed": int(failed or 0),
                "cancelled": int(cancelled or 0),
                "terminal": int(success or 0) + int(failed or 0) + int(cancelled or 0),
            }
            for day, success, failed, cancelled in daily_rows
        ]
        current_pending = sum(int(item["pending"]) for item in jobs.values())
        current_running = sum(int(item["running"]) for item in jobs.values())

        coverage_note = (
            "Во «Всё время» архивные счётчики реакций и просмотров включены в итоги и рейтинги; "
            "их исходные даты и длительность уже недоступны, поэтому график и среднее время считаются "
            "по сохранённым подробным строкам. Подписки отражают последнее состояние пары аккаунт/цель."
            if period.key == "all"
            else "Архивные записи без исходной даты не входят в периодический срез. "
            "Подписки отражают последнее состояние пары аккаунт/цель, а не полный журнал переходов."
        )
        return {
            "period": period.key,
            "period_label": period.label,
            "cutoff": cutoff,
            "generated_at": current,
            "current": {
                "accounts": int(getattr(account_counts, "accounts", 0) or 0),
                "active_accounts": int(getattr(account_counts, "active_accounts", 0) or 0),
                "problem_accounts": int(getattr(account_counts, "problem_accounts", 0) or 0),
                "targets": int(getattr(target_counts, "targets", 0) or 0),
                "active_targets": int(getattr(target_counts, "active_targets", 0) or 0),
                "channels": int(getattr(target_counts, "channels", 0) or 0),
                "groups": int(getattr(target_counts, "groups", 0) or 0),
                "pending": current_pending,
                "running": current_running,
            },
            "jobs": jobs,
            "totals": {
                "success": total_success,
                "failed": total_failed,
                "cancelled": total_cancelled,
                "terminal": total_terminal,
                "success_rate": (
                    total_success * 100.0 / success_denominator
                    if success_denominator > 0
                    else None
                ),
                "timing": {
                    "queue_lag_seconds": (
                        weighted_queue / total_queue_samples
                        if total_queue_samples > 0
                        else None
                    ),
                    "execution_seconds": (
                        weighted_execution / total_timing_samples
                        if total_timing_samples > 0
                        else None
                    ),
                    "queue_samples": total_queue_samples,
                    "execution_samples": total_timing_samples,
                },
            },
            "top_accounts": ranked_accounts[:limit] if limit is not None else ranked_accounts,
            "problem_accounts": problem_accounts,
            "top_targets": ranked_targets[:limit] if limit is not None else ranked_targets,
            "problem_targets": problem_targets,
            "daily": daily,
            "coverage_note": coverage_note,
        }

    async def stats(self) -> dict[str, int]:
        """Return menu and diagnostic counters with bounded database round-trips."""

        now = utcnow()
        backlog_before = now - timedelta(minutes=5)
        stuck_running_before = now - timedelta(minutes=5)
        async with self.sessions() as session:
            result: dict[str, int] = {}

            account_counts = (
                await session.execute(
                    select(
                        func.count(Account.id).label("accounts"),
                        func.sum(
                            case(
                                (
                                    and_(
                                        Account.is_active.is_(True),
                                        Account.status != "unauthorized",
                                    ),
                                    1,
                                ),
                                else_=0,
                            )
                        ).label("active_accounts"),
                        func.sum(
                            case(
                                (
                                    and_(
                                        Account.is_active.is_(False),
                                        Account.status != "unauthorized",
                                    ),
                                    1,
                                ),
                                else_=0,
                            )
                        ).label("disabled_accounts"),
                        func.sum(
                            case((Account.status == "unauthorized", 1), else_=0)
                        ).label("problem_accounts"),
                        func.sum(
                            case(
                                (
                                    and_(
                                        Account.is_active.is_(True),
                                        Account.status != "unauthorized",
                                        Account.flood_until.is_not(None),
                                        Account.flood_until > now,
                                    ),
                                    1,
                                ),
                                else_=0,
                            )
                        ).label("flood_accounts"),
                        func.sum(
                            case(
                                (
                                    and_(
                                        Account.is_active.is_(True),
                                        Account.status != "unauthorized",
                                        Account.last_error.is_not(None),
                                        Account.last_error != "",
                                        or_(
                                            Account.flood_until.is_(None),
                                            Account.flood_until > now,
                                            ~Account.last_error.ilike("FloodWait%"),
                                        ),
                                    ),
                                    1,
                                ),
                                else_=0,
                            )
                        ).label("account_errors"),
                    )
                )
            ).one()
            for key in (
                "accounts",
                "active_accounts",
                "disabled_accounts",
                "problem_accounts",
                "flood_accounts",
                "account_errors",
            ):
                result[key] = int(getattr(account_counts, key) or 0)

            target_counts = (
                await session.execute(
                    select(
                        func.sum(
                            case((Channel.kind == "channel", 1), else_=0)
                        ).label("channels"),
                        func.sum(
                            case((Channel.kind == "group", 1), else_=0)
                        ).label("groups"),
                        func.sum(
                            case(
                                (
                                    and_(
                                        Channel.is_active.is_(True),
                                        Channel.last_error.is_not(None),
                                        Channel.last_error != "",
                                    ),
                                    1,
                                ),
                                else_=0,
                            )
                        ).label("target_errors"),
                    )
                )
            ).one()
            for key in ("channels", "groups", "target_errors"):
                result[key] = int(getattr(target_counts, key) or 0)

            async def load_job_counts(model, prefix: str, statuses: tuple[str, ...]) -> None:
                for status in statuses:
                    result[f"{prefix}_{status}"] = 0
                rows = (
                    await session.execute(
                        select(model.status, func.count(model.id)).group_by(model.status)
                    )
                ).all()
                for status, count in rows:
                    status_text = str(status)
                    key = f"{prefix}_{status_text}"
                    if key in result:
                        result[key] = int(count)
                result[f"{prefix}_backlog"] = int(
                    await session.scalar(
                        select(func.count(model.id)).where(
                            model.status == "pending",
                            model.due_at <= backlog_before,
                        )
                    )
                    or 0
                )
                result[f"{prefix}_stuck_running"] = int(
                    await session.scalar(
                        select(func.count(model.id)).where(
                            model.status == "running",
                            model.started_at.is_not(None),
                            model.started_at <= stuck_running_before,
                        )
                    )
                    or 0
                )

            await load_job_counts(
                ReactionJob,
                "reaction",
                ("pending", "running", "done", "failed", "cancelled"),
            )
            await load_job_counts(
                ViewJob,
                "view",
                ("pending", "running", "done", "failed", "cancelled"),
            )
            await load_job_counts(
                JoinJob,
                "join",
                ("pending", "running", "done", "failed", "left", "cancelled"),
            )

            archived_rows = (
                await session.execute(
                    select(
                        JobHistorySummary.job_kind,
                        JobHistorySummary.status,
                        func.sum(JobHistorySummary.count),
                    ).group_by(JobHistorySummary.job_kind, JobHistorySummary.status)
                )
            ).all()
            for job_kind, status, count in archived_rows:
                key = f"{job_kind}_{status}"
                if key in result:
                    result[key] += int(count or 0)
            return result
