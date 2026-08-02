from __future__ import annotations

import asyncio
import logging
import math
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime, timedelta
from random import SystemRandom

from telethon import errors, functions, types

from ..ai_channel_memory import (
    AI_CHANNEL_MEMORY_FETCH_LIMIT,
    AI_CHANNEL_MEMORY_WINDOW,
    canonical_ai_channel_post_payloads,
)
from ..config import Settings
from ..db import Database
from ..models import Channel, promotion_is_active, utcnow
from ..selection import AccountWorkload, rank_account_ids, select_account_ids
from ..utils import choose_weighted_reaction, parse_channel_link
from .telegram_accounts import (
    ACCOUNT_AUTH_FAILURES,
    ClientPool,
    TargetNotVisibleError,
)

logger = logging.getLogger(__name__)

_rng = SystemRandom()


@dataclass(frozen=True, slots=True)
class RunningReactionTask:
    job_id: int
    account_id: int
    started_at: datetime


class JobService:
    def __init__(self, settings: Settings, db: Database, pool: ClientPool) -> None:
        self.settings = settings
        self.db = db
        self.pool = pool
        self._reaction_schedule_locks: dict[int, asyncio.Lock] = {}
        self._membership_schedule_locks: dict[int, asyncio.Lock] = {}
        self._view_schedule_locks: dict[int, asyncio.Lock] = {}
        self._ai_memory_sync_locks: dict[int, asyncio.Lock] = {}
        self._worker_heartbeats: dict[str, dict[str, object | None]] = {
            name: {
                "last_tick_at": None,
                "last_success_at": None,
                "last_error_at": None,
                "last_error": None,
                "warning_active": False,
                "warning_since_at": None,
                "last_recovered_at": None,
            }
            for name in (
                "account_health",
                "channel_monitor",
                "join_worker",
                "reaction_worker",
                "view_worker",
            )
        }
        self._reaction_tasks: dict[asyncio.Task[bool], RunningReactionTask] = {}
        self._active_membership_job_id: int | None = None
        self._active_view_job_id: int | None = None
        self._last_history_cleanup_at: datetime | None = None

    def _worker_tick(self, name: str) -> None:
        heartbeat = self._worker_heartbeats[name]
        heartbeat["last_tick_at"] = utcnow()

    def _worker_success(self, name: str, *, resolve_warning: bool = True) -> None:
        now = utcnow()
        heartbeat = self._worker_heartbeats[name]
        heartbeat["last_tick_at"] = now
        heartbeat["last_success_at"] = now
        if resolve_warning and bool(heartbeat["warning_active"]):
            heartbeat["warning_active"] = False
            heartbeat["warning_since_at"] = None
            heartbeat["last_recovered_at"] = now

    def _worker_error(self, name: str, exc: BaseException) -> None:
        self._worker_warning(name, f"{type(exc).__name__}: {exc}")

    def _worker_warning(self, name: str, message: str) -> None:
        now = utcnow()
        heartbeat = self._worker_heartbeats[name]
        heartbeat["last_tick_at"] = now
        if not bool(heartbeat["warning_active"]):
            heartbeat["warning_since_at"] = now
        heartbeat["last_error_at"] = now
        heartbeat["last_error"] = message[:500]
        heartbeat["warning_active"] = True

    def worker_health_snapshot(
        self, *, now: datetime | None = None
    ) -> list[dict[str, object | None]]:
        """Return a read-only health snapshot for the five critical workers."""

        current = now or utcnow()
        stale_after = {
            "account_health": 30 * 60,
            "channel_monitor": max(5 * 60, self.settings.monitor_interval_seconds * 5),
            "join_worker": max(5 * 60, self.settings.worker_interval_seconds * 10),
            "reaction_worker": max(2 * 60, self.settings.worker_interval_seconds * 10),
            "view_worker": max(5 * 60, self.settings.worker_interval_seconds * 10),
        }
        labels = {
            "account_health": "Проверка сессий",
            "channel_monitor": "Мониторинг каналов",
            "join_worker": "Подписки и выходы",
            "reaction_worker": "Реакции",
            "view_worker": "Просмотры",
        }
        result: list[dict[str, object | None]] = []
        for name, heartbeat in self._worker_heartbeats.items():
            last_tick_at = heartbeat["last_tick_at"]
            last_success_at = heartbeat["last_success_at"]
            last_error_at = heartbeat["last_error_at"]
            if last_tick_at is None:
                status = "starting"
                age_seconds = None
            else:
                age_seconds = max(0, int((current - last_tick_at).total_seconds()))
                if age_seconds > stale_after[name]:
                    status = "stale"
                elif bool(heartbeat["warning_active"]):
                    status = "warning"
                else:
                    status = "ok"

            running_tasks = 0
            oldest_running_age_seconds = None
            stuck_running_tasks = 0
            if name == "reaction_worker":
                task_ages = [
                    max(0, int((current - item.started_at).total_seconds()))
                    for item in self._reaction_tasks.values()
                ]
                running_tasks = len(task_ages)
                if task_ages:
                    oldest_running_age_seconds = max(task_ages)
                    timeout_seconds = max(
                        1, int(getattr(self.settings, "reaction_attempt_timeout_seconds", 180))
                    )
                    stuck_running_tasks = sum(
                        age > timeout_seconds + 30 for age in task_ages
                    )
                    if stuck_running_tasks:
                        status = "blocked"
            result.append(
                {
                    "name": name,
                    "label": labels[name],
                    "status": status,
                    "age_seconds": age_seconds,
                    "last_tick_at": last_tick_at,
                    "last_success_at": last_success_at,
                    "last_error_at": last_error_at,
                    "warning_since_at": heartbeat["warning_since_at"],
                    "last_error": heartbeat["last_error"],
                    "last_recovered_at": heartbeat["last_recovered_at"],
                    "running_tasks": running_tasks,
                    "oldest_running_age_seconds": oldest_running_age_seconds,
                    "stuck_running_tasks": stuck_running_tasks,
                }
            )
        return result

    def active_job_ids(self) -> dict[str, set[int]]:
        """Return jobs that are still owned by live in-memory Telegram tasks."""

        return {
            "join": (
                {self._active_membership_job_id}
                if self._active_membership_job_id is not None
                else set()
            ),
            "reaction": {item.job_id for item in self._reaction_tasks.values()},
            "view": (
                {self._active_view_job_id}
                if self._active_view_job_id is not None
                else set()
            ),
        }

    def mark_worker_restarting(self, name: str, detail: str) -> None:
        """Expose a supervised local restart in the normal worker health feed."""

        normalized = name.replace("-", "_")
        if normalized in self._worker_heartbeats:
            self._worker_warning(normalized, f"Автовосстановление: {detail}")

    def mark_worker_restarted(self, name: str) -> None:
        """Clear only the supervisor-owned restart marker after a fresh heartbeat.

        A genuine worker warning (for example a failed reaction child task) must
        stay visible until that worker's normal success rule proves recovery.
        """

        normalized = name.replace("-", "_")
        heartbeat = self._worker_heartbeats.get(normalized)
        if heartbeat is None:
            return
        last_error = str(heartbeat.get("last_error") or "")
        if bool(heartbeat.get("warning_active")) and last_error.startswith(
            "Автовосстановление:"
        ):
            self._worker_success(normalized, resolve_warning=True)

    def _can_retry_after_current_attempt(self, job) -> bool:
        # ORM jobs are loaded before the atomic claim increments attempts in DB.
        # Add one so MAX_REACTION_ATTEMPTS is a true total attempt budget.
        current_attempt = int(getattr(job, "attempts", 0)) + 1
        return current_attempt < int(self.settings.max_reaction_attempts)

    def _reaction_lock(self, channel_id: int) -> asyncio.Lock:
        lock = self._reaction_schedule_locks.get(channel_id)
        if lock is None:
            lock = asyncio.Lock()
            self._reaction_schedule_locks[channel_id] = lock
        return lock

    def _membership_lock(self, channel_id: int) -> asyncio.Lock:
        lock = self._membership_schedule_locks.get(channel_id)
        if lock is None:
            lock = asyncio.Lock()
            self._membership_schedule_locks[channel_id] = lock
        return lock

    def _view_lock(self, channel_id: int) -> asyncio.Lock:
        lock = self._view_schedule_locks.get(channel_id)
        if lock is None:
            lock = asyncio.Lock()
            self._view_schedule_locks[channel_id] = lock
        return lock

    def _ai_memory_lock(self, channel_id: int) -> asyncio.Lock:
        lock = self._ai_memory_sync_locks.get(channel_id)
        if lock is None:
            lock = asyncio.Lock()
            self._ai_memory_sync_locks[channel_id] = lock
        return lock

    async def audit_accounts_once(self) -> dict[str, int]:
        """Validate active sessions and quarantine permanently unauthorized rows."""

        checked = 0
        quarantined = 0
        transient_errors = 0
        accounts = await self.db.list_accounts(active_only=True)
        for account in accounts:
            self._worker_tick("account_health")
            try:
                async with self.pool.lock_for(account.id):
                    was_connected = self.pool.has_connected_client(account.id)
                    try:
                        await self.pool.ensure_authorized(account)
                        checked += 1
                    except ACCOUNT_AUTH_FAILURES:
                        if await self.pool.remove_unauthorized_account_while_locked(
                            account.id, context="health-check"
                        ):
                            quarantined += 1
                    finally:
                        # Cleanup must happen while the same account lock is held.
                        # Otherwise another worker could acquire the pooled client
                        # and have it disconnected underneath an active request.
                        if (
                            not was_connected
                            and self.pool.has_connected_client(account.id)
                        ):
                            await self.pool.disconnect_client(account.id)
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001
                transient_errors += 1
                logger.warning(
                    "Account health check deferred account=%s error=%s: %s",
                    account.id,
                    type(exc).__name__,
                    exc,
                )
            await asyncio.sleep(0.1)
        logger.info(
            "Account health check completed checked=%s quarantined=%s transient_errors=%s",
            checked,
            quarantined,
            transient_errors,
        )
        return {
            "checked": checked,
            "quarantined": quarantined,
            "transient_errors": transient_errors,
        }

    async def refresh_account_profiles_once(self) -> dict[str, int]:
        """Refresh names and usernames for all saved Telegram accounts."""

        checked = 0
        updated = 0
        quarantined = 0
        transient_errors = 0
        accounts = [
            account
            for account in await self.db.list_accounts()
            if getattr(account, "status", "ready") != "unauthorized"
        ]
        for account in accounts:
            try:
                async with self.pool.lock_for(account.id):
                    was_connected = self.pool.has_connected_client(account.id)
                    try:
                        client = await self.pool.ensure_authorized(account)
                        me = await client.get_me()
                        if me is None:
                            raise RuntimeError("Telegram не вернул профиль аккаунта")
                        display_name = " ".join(
                            part
                            for part in (
                                getattr(me, "first_name", None),
                                getattr(me, "last_name", None),
                            )
                            if part
                        ) or str(me.id)
                        changed = await self.db.update_account_profile(
                            account.id,
                            telegram_user_id=int(me.id),
                            display_name=display_name,
                            username=getattr(me, "username", None),
                        )
                        checked += 1
                        updated += int(changed)
                    except ACCOUNT_AUTH_FAILURES:
                        if await self.pool.remove_unauthorized_account_while_locked(
                            account.id, context="manual-profile-refresh"
                        ):
                            quarantined += 1
                    finally:
                        if (
                            not was_connected
                            and self.pool.has_connected_client(account.id)
                        ):
                            await self.pool.disconnect_client(account.id)
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001
                transient_errors += 1
                logger.warning(
                    "Account profile refresh deferred account=%s error=%s: %s",
                    account.id,
                    type(exc).__name__,
                    exc,
                )
            await asyncio.sleep(0.05)

        logger.info(
            "Account profile refresh completed checked=%s updated=%s quarantined=%s transient_errors=%s",
            checked,
            updated,
            quarantined,
            transient_errors,
        )
        return {
            "checked": checked,
            "updated": updated,
            "quarantined": quarantined,
            "transient_errors": transient_errors,
        }

    async def _cleanup_history_if_due(self) -> dict[str, int] | None:
        now = utcnow()
        interval = max(
            60,
            int(getattr(self.settings, "job_history_cleanup_interval_seconds", 86400)),
        )
        last_cleanup = self._last_history_cleanup_at
        if last_cleanup is None:
            raw_last_cleanup = await self.db.get_setting(
                "job_history_last_cleanup_at", ""
            )
            try:
                last_cleanup = datetime.fromisoformat(raw_last_cleanup)
            except (TypeError, ValueError):
                # First deployment gets one full interval for a PostgreSQL backup.
                # Persisting the grace timestamp prevents every restart from
                # postponing cleanup forever.
                self._last_history_cleanup_at = now
                await self.db.set_setting(
                    "job_history_last_cleanup_at", now.isoformat()
                )
                return None
            self._last_history_cleanup_at = last_cleanup
        if (now - last_cleanup).total_seconds() < interval:
            return None
        retention_days = max(
            1, int(getattr(self.settings, "job_history_retention_days", 90))
        )
        batch_size = max(
            1, int(getattr(self.settings, "job_history_cleanup_batch_size", 10000))
        )
        result = await self.db.prune_job_history(
            retention_days, now=now, max_rows_per_kind=batch_size
        )
        self._last_history_cleanup_at = now
        await self.db.set_setting("job_history_last_cleanup_at", now.isoformat())
        if result["reaction_deleted"] or result["view_deleted"]:
            logger.info(
                "Job history retention completed days=%s batch=%s reaction=%s "
                "view=%s batches=%s keys=%s",
                retention_days,
                batch_size,
                result["reaction_deleted"],
                result["view_deleted"],
                result["view_batches_deleted"],
                result["dedup_keys_created"],
            )
        return result

    async def account_health_loop(self) -> None:
        while True:
            self._worker_tick("account_health")
            warnings: list[str] = []
            try:
                result = await self.audit_accounts_once()
                transient_errors = int(result.get("transient_errors", 0))
                if transient_errors:
                    warnings.append(
                        f"Временных ошибок проверки аккаунтов: {transient_errors}"
                    )
                await self._cleanup_history_if_due()
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001
                warnings.append(f"{type(exc).__name__}: {exc}")
                logger.exception("Ошибка проверки сессий или очистки истории")

            if warnings:
                self._worker_warning("account_health", " · ".join(warnings))
            else:
                self._worker_success("account_health")
            await asyncio.sleep(900)

    async def _rank_account_ids(
        self,
        account_ids: list[int],
        *,
        now: datetime | None = None,
        planned_actions: Mapping[int, int] | None = None,
    ) -> tuple[list[int], dict[int, AccountWorkload]]:
        current = now or utcnow()
        workload_loader = getattr(self.db, "account_workloads", None)
        if workload_loader is None:
            # Compatibility for isolated service tests and lightweight adapters.
            # Production Database always provides persisted workload metrics.
            workloads = {
                int(account_id): AccountWorkload(account_id=int(account_id))
                for account_id in account_ids
            }
            candidates = list(account_ids)
        else:
            workloads = await workload_loader(account_ids, now=current)
            # An account deleted concurrently with scheduling must not be recreated
            # as a queue row through a stale candidate id.
            candidates = [
                int(account_id)
                for account_id in account_ids
                if int(account_id) in workloads
            ]
        ranked = rank_account_ids(
            workloads,
            candidates,
            now=current,
            planned_actions=planned_actions,
            rng=_rng,
        )
        return ranked, workloads

    @staticmethod
    def _flood_not_before(
        workloads: Mapping[int, AccountWorkload],
        *,
        now: datetime,
    ) -> dict[int, datetime]:
        return {
            account_id: workload.flood_until + timedelta(seconds=1)
            for account_id, workload in workloads.items()
            if workload.flood_until is not None and workload.flood_until > now
        }

    async def _paced_pairs(
        self,
        account_ids: list[int],
        *,
        start_after: datetime | None = None,
        not_before: Mapping[int, datetime] | None = None,
    ) -> list[tuple[int, datetime]]:
        delay_min, delay_max = await self.db.get_membership_delays(
            self.settings.join_delay_min_seconds,
            self.settings.join_delay_max_seconds,
        )
        now = utcnow()
        append_to_existing = bool(start_after and start_after > now)
        cursor = start_after if append_to_existing else now
        pairs: list[tuple[int, datetime]] = []
        for index, account_id in enumerate(account_ids):
            if index == 0 and not append_to_existing:
                due = now
            else:
                cursor += timedelta(seconds=_rng.randint(delay_min, delay_max))
                due = cursor
            minimum_due = (not_before or {}).get(account_id)
            if minimum_due is not None and due < minimum_due:
                due = minimum_due
            if due > cursor:
                cursor = due
            pairs.append((account_id, due))
        logger.info(
            "Membership jobs paced accounts=%s delay=%s-%s append=%s start_after=%s",
            len(account_ids),
            delay_min,
            delay_max,
            append_to_existing,
            start_after,
        )
        return pairs

    async def schedule_channel_joins(self, channel: Channel) -> int:
        accounts = await self.db.list_accounts(active_only=True)
        now = utcnow()
        ranked_ids, workloads = await self._rank_account_ids(
            [item.id for item in accounts], now=now
        )
        selected_ids = ranked_ids[: self.settings.max_accounts_per_channel]
        pairs = await self._paced_pairs(
            selected_ids,
            not_before=self._flood_not_before(workloads, now=now),
        )
        return await self.db.schedule_join_jobs(channel.id, pairs)

    async def schedule_missing_channel_joins(
        self, channel: Channel, *, account_ids: list[int] | None = None
    ) -> dict[str, int]:
        """Append only missing active accounts to a channel's join queue."""

        async with self._membership_lock(channel.id):
            summary, available = await self.db.channel_connect_state(
                channel.id, max_accounts=self.settings.max_accounts_per_channel
            )
            requested = set(account_ids) if account_ids is not None else None
            selected = [
                account
                for account in available
                if requested is None or account.id in requested
            ]
            if not selected:
                return {
                    "available": summary["connectable"],
                    "selected": 0,
                    "scheduled": 0,
                    "skipped": len(requested or ()),
                }

            now = utcnow()
            ranked_ids, workloads = await self._rank_account_ids(
                [account.id for account in selected], now=now
            )
            latest_due = await self.db.latest_pending_join_due(channel.id)
            pairs = await self._paced_pairs(
                ranked_ids,
                start_after=latest_due,
                not_before=self._flood_not_before(workloads, now=now),
            )
            scheduled = await self.db.schedule_join_jobs(channel.id, pairs)
            skipped = max(0, len(requested or ()) - len(selected))
            logger.info(
                "Missing channel accounts scheduled target=%s available=%s selected=%s scheduled=%s skipped=%s",
                channel.id,
                summary["connectable"],
                len(selected),
                scheduled,
                skipped,
            )
            return {
                "available": summary["connectable"],
                "selected": len(selected),
                "scheduled": scheduled,
                "skipped": skipped,
            }

    async def schedule_channel_leaves(self, channel: Channel) -> int:
        # Include disabled accounts too: their saved session may still be subscribed.
        # Leave jobs are administrative cleanup, not engagement load, so their
        # historical ordering is intentionally preserved.
        accounts = (await self.db.list_accounts())[: self.settings.max_accounts_per_channel]
        pairs = await self._paced_pairs([item.id for item in accounts])
        return await self.db.schedule_leave_jobs(channel.id, pairs)

    async def schedule_post(
        self,
        channel: Channel,
        message_id: int,
        *,
        source: str,
        has_image: bool | None = None,
    ) -> int:
        async with self._reaction_lock(channel.id):
            fresh = await self.db.get_channel(channel.id)
            if fresh is None or not fresh.is_active:
                return 0
            if not promotion_is_active(fresh):
                expired = await self.db.close_expired_promotion(fresh.id)
                if expired["expired"]:
                    logger.info(
                        "Promotion expired target=%s cancelled=%s",
                        fresh.id,
                        expired["cancelled"],
                    )
                return 0
            return await self._schedule_post_locked(
                fresh, message_id, source=source, has_image=has_image
            )

    @staticmethod
    def _post_type_percent(channel: Channel, has_image: bool | None) -> int:
        if has_image is True:
            return int(channel.image_post_reaction_percent)
        if has_image is False:
            return int(channel.no_image_post_reaction_percent)
        # Legacy jobs created before v1.0.13 have no stored classification.
        return 100

    def _effective_post_limit(
        self, channel: Channel, joined_count: int, has_image: bool | None
    ) -> tuple[int, int, int]:
        hard_cap = min(joined_count, self.settings.max_accounts_per_channel)
        configured_limit = channel.max_reactions_per_post
        base_limit = hard_cap if configured_limit is None else min(configured_limit, hard_cap)
        percent = self._post_type_percent(channel, has_image)
        if base_limit <= 0 or percent <= 0:
            return base_limit, percent, 0
        scaled = max(1, math.floor(base_limit * percent / 100))
        return base_limit, percent, min(base_limit, scaled)

    async def _schedule_post_locked(
        self,
        channel: Channel,
        message_id: int,
        *,
        source: str,
        has_image: bool | None,
    ) -> int:
        account_ids = await self.db.joined_account_ids(channel.id)
        if not account_ids:
            logger.warning(
                "Reaction jobs skipped: no joined accounts target=%s message=%s source=%s",
                channel.id,
                message_id,
                source,
            )
            return 0

        base_limit, type_percent, post_limit = self._effective_post_limit(
            channel, len(account_ids), has_image
        )
        active_ids = await self.db.active_reaction_account_ids(channel.id, message_id)
        remaining = max(0, post_limit - len(active_ids))
        eligible = [account_id for account_id in account_ids if account_id not in active_ids]
        if remaining <= 0 or not eligible:
            logger.info(
                "Reaction limit already satisfied target=%s message=%s active=%s limit=%s "
                "base_limit=%s post_type=%s percent=%s",
                channel.id,
                message_id,
                len(active_ids),
                post_limit,
                base_limit,
                self._post_type_name(has_image),
                type_percent,
            )
            return 0

        selection_now = utcnow()
        ranked_ids, workloads = await self._rank_account_ids(
            eligible, now=selection_now
        )
        selected_ids = ranked_ids[: min(remaining, len(eligible))]
        # Selection is load-aware; timing order remains random so the healthiest
        # account is not systematically the first visible reaction on every post.
        _rng.shuffle(selected_ids)
        reaction_weights = await self.db.get_reaction_weights_for_channel(channel)
        account_delay_min, account_delay_max = await self.db.get_delays(
            self.settings.default_reaction_delay_min_seconds,
            self.settings.default_reaction_delay_max_seconds,
        )
        window_min = max(0, int(channel.reaction_window_min_seconds))
        window_max = max(window_min, int(channel.reaction_window_max_seconds))
        now = selection_now
        latest_by_account = await self.db.latest_reaction_due_by_account(selected_ids)
        jobs: list[tuple[int, str, datetime]] = []
        count = len(selected_ids)
        span = max(0, window_max - window_min)
        for index, account_id in enumerate(selected_ids):
            # Stratified randomness: every account receives its own portion of the
            # channel window, preventing accidental bursts while keeping timing random.
            if count <= 1 or span == 0:
                offset = window_min if span == 0 else _rng.randint(window_min, window_max)
            else:
                segment_start = window_min + math.floor(span * index / count)
                segment_end = window_min + math.floor(span * (index + 1) / count)
                if index == count - 1:
                    segment_end = window_max
                offset = _rng.randint(segment_start, max(segment_start, segment_end))
            due_at = now + timedelta(seconds=offset)

            # A second, global safety layer protects one account from actions that
            # were scheduled by overlapping posts or other channels too closely.
            previous_due = latest_by_account.get(account_id)
            if previous_due is not None:
                cooldown = _rng.randint(account_delay_min, account_delay_max)
                due_at = max(due_at, previous_due + timedelta(seconds=cooldown))
            workload = workloads.get(account_id)
            if (
                workload is not None
                and workload.flood_until is not None
                and workload.flood_until > now
            ):
                due_at = max(
                    due_at, workload.flood_until + timedelta(seconds=1)
                )
            latest_by_account[account_id] = due_at

            if channel.promotion_mode == "timed" and (
                channel.promotion_until is None or due_at > channel.promotion_until
            ):
                continue
            jobs.append((account_id, choose_weighted_reaction(reaction_weights, _rng), due_at))

        created = await self.db.schedule_reaction_jobs(
            channel_id=channel.id,
            message_id=message_id,
            source=source,
            jobs=jobs,
            post_has_image=has_image,
        )
        logger.info(
            "Reaction jobs scheduled target=%s message=%s source=%s available=%s selected=%s "
            "active_before=%s limit=%s base_limit=%s post_type=%s percent=%s created=%s "
            "window=%s-%s account_delay=%s-%s reactions=%s scope=%s",
            channel.id,
            message_id,
            source,
            len(account_ids),
            len(selected_ids),
            len(active_ids),
            post_limit,
            base_limit,
            self._post_type_name(has_image),
            type_percent,
            created,
            window_min,
            window_max,
            account_delay_min,
            account_delay_max,
            " ".join(reaction_weights),
            "custom" if channel.reactions_json else "default",
        )
        return created

    async def _reconcile_post_limits(self, channel: Channel) -> dict[str, int]:
        fresh = await self.db.get_channel(channel.id)
        if fresh is None:
            return {"cancelled": 0, "created": 0, "posts": 0}

        joined_ids = await self.db.joined_account_ids(fresh.id)
        descriptors = await self.db.reaction_message_types(fresh.id)
        normalized: dict[int, tuple[str, bool | None]] = {}
        for message_id, source, has_image in descriptors:
            if source == "legacy":
                if fresh.new_posts_enabled:
                    normalized_source = "new"
                elif fresh.old_posts_enabled:
                    normalized_source = "old"
                else:
                    continue
            else:
                normalized_source = source
            if normalized_source == "new" and not fresh.new_posts_enabled:
                continue
            if normalized_source == "old" and not fresh.old_posts_enabled:
                continue
            if normalized_source not in {"new", "old"}:
                continue
            previous = normalized.get(message_id)
            # Prefer a known classification over legacy NULL metadata.
            if previous is None or (previous[1] is None and has_image is not None):
                normalized[message_id] = (normalized_source, has_image)

        limits = {
            message_id: self._effective_post_limit(fresh, len(joined_ids), has_image)[2]
            for message_id, (_source, has_image) in normalized.items()
        }
        trimmed = await self.db.trim_pending_reactions_to_message_limits(fresh.id, limits)
        created = 0
        for message_id, (source, has_image) in normalized.items():
            created += await self.schedule_post(
                fresh, message_id, source=source, has_image=has_image
            )

        if fresh.old_posts_enabled and promotion_is_active(fresh):
            try:
                created += await self.enqueue_old_posts(fresh)
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001
                logger.warning(
                    "Old posts could not be refreshed while reconciling limits "
                    "target=%s error=%s: %s",
                    fresh.id,
                    type(exc).__name__,
                    exc,
                )
        logger.info(
            "Post limits reconciled target=%s cancelled=%s created=%s posts=%s "
            "image_percent=%s no_image_percent=%s",
            fresh.id,
            trimmed["cancelled"],
            created,
            len(normalized),
            fresh.image_post_reaction_percent,
            fresh.no_image_post_reaction_percent,
        )
        return {
            "cancelled": trimmed["cancelled"],
            "created": created,
            "posts": len(normalized),
        }

    async def apply_reaction_limit(self, channel: Channel) -> dict[str, int]:
        return await self._reconcile_post_limits(channel)

    async def apply_post_type_percentages(self, channel: Channel) -> dict[str, int]:
        return await self._reconcile_post_limits(channel)

    async def apply_promotion_period(self, channel: Channel) -> dict[str, int]:
        fresh = await self.db.get_channel(channel.id)
        if fresh is None:
            return {"cancelled": 0, "created": 0}
        cancelled = 0
        if fresh.promotion_mode == "timed" and fresh.promotion_until is not None:
            cancelled = await self.db.cancel_pending_reactions_after(fresh.id, fresh.promotion_until)
        created = 0
        if fresh.old_posts_enabled and promotion_is_active(fresh):
            created = await self.enqueue_old_posts(fresh)
        logger.info(
            "Promotion period applied target=%s mode=%s until=%s cancelled_after_end=%s created=%s",
            fresh.id,
            fresh.promotion_mode,
            fresh.promotion_until,
            cancelled,
            created,
        )
        return {"cancelled": cancelled, "created": created}

    async def enqueue_old_posts(self, channel: Channel) -> int:
        messages = await self._get_messages_from_first_resolved_channel(
            channel, limit=channel.old_posts_depth
        )
        if messages is None:
            raise RuntimeError("Нет активного аккаунта, который видит канал или группу")
        posts = self._canonical_posts(messages)
        created = 0
        for message_id, has_image in posts:
            created += await self.schedule_post(
                channel, message_id, source="old", has_image=has_image
            )
        logger.info(
            "Old posts enqueued target=%s posts=%s created_jobs=%s depth=%s",
            channel.id,
            len(posts),
            created,
            channel.old_posts_depth,
        )
        return created

    async def sync_last_seen_to_latest(self, channel: Channel) -> int:
        messages = await self._get_messages_from_first_resolved_channel(channel, limit=1)
        if messages is None:
            return channel.last_seen_message_id
        latest = messages[0].id if messages else channel.last_seen_message_id
        await self.db.set_channel_last_seen(channel.id, latest)
        return latest

    async def _ingest_ai_channel_messages(
        self,
        channel: Channel,
        messages,
    ) -> dict[str, int]:
        payloads = canonical_ai_channel_post_payloads(
            messages,
            limit=AI_CHANNEL_MEMORY_WINDOW,
        )
        if not payloads:
            return {
                "processed": 0,
                "created": 0,
                "updated": 0,
                "unchanged": 0,
                "stale": 0,
                "restored": 0,
            }
        result = await self.db.ingest_ai_channel_posts(channel.id, payloads)
        logger.info(
            "AI channel memory ingested target=%s processed=%s created=%s "
            "updated=%s unchanged=%s stale=%s restored=%s",
            channel.id,
            result["processed"],
            result["created"],
            result["updated"],
            result["unchanged"],
            result["stale"],
            result["restored"],
        )
        return result

    async def sync_ai_channel_memory(self, channel_id: int) -> dict[str, int]:
        """Manually refresh the bounded factual window through an existing account."""

        async with self._ai_memory_lock(channel_id):
            channel = await self.db.get_channel(channel_id)
            if channel is None or channel.kind != "channel" or not channel.is_active:
                raise RuntimeError("Канал выключен или удалён")
            await self.db.ensure_ai_channel_profile(channel.id)
            messages = await self._get_messages_from_first_resolved_channel(
                channel,
                limit=AI_CHANNEL_MEMORY_FETCH_LIMIT,
            )
            if messages is None:
                raise RuntimeError(
                    "Нет активного авторизованного аккаунта с доступом к каналу"
                )
            result = await self._ingest_ai_channel_messages(channel, messages)
            if result["processed"] == 0:
                raise RuntimeError("В канале не найдены доступные публикации")
            return result

    async def _enabled_ai_memory_channel_ids(self) -> set[int]:
        """Resolve the effective memory flag without affecting core monitoring."""

        if not bool(getattr(self.settings, "ai_comments_enabled", False)):
            return set()
        try:
            flags = await self.db.get_ai_comments_flags()
            if not bool(flags.get("ai_comments_enabled", False)):
                return set()
            return await self.db.list_enabled_ai_channel_memory_ids()
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                "AI channel memory disabled for this monitor cycle error=%s: %s",
                type(exc).__name__,
                exc,
            )
            return set()

    async def schedule_recent_views(
        self,
        channel: Channel,
        *,
        post_count: int,
        selection_mode: str,
        selection_value: int,
    ):
        """Queue views for the newest 5/20/50/100 canonical channel posts."""

        if channel.kind != "channel":
            raise ValueError("Просмотры доступны только для каналов")
        if post_count not in {5, 20, 50, 100}:
            raise ValueError("Можно выбрать только последние 5, 20, 50 или 100 постов")
        if selection_mode not in {"percent", "count"}:
            raise ValueError("Некорректный режим количества аккаунтов")
        if selection_value <= 0:
            raise ValueError("Количество аккаунтов должно быть больше нуля")
        if selection_mode == "percent" and selection_value > 100:
            raise ValueError("Процент должен быть от 1 до 100")

        async with self._view_lock(channel.id):
            fresh = await self.db.get_channel(channel.id)
            if fresh is None or not fresh.is_active or fresh.kind != "channel":
                raise RuntimeError("Канал выключен или удалён")

            joined_candidates = await self.db.joined_account_ids(fresh.id)
            ranking_now = utcnow()
            joined_ranked, joined_workloads = await self._rank_account_ids(
                joined_candidates, now=ranking_now
            )
            joined_ids = joined_ranked[: self.settings.max_accounts_per_channel]
            if not joined_ids:
                raise RuntimeError("Нет активных подписанных аккаунтов")

            if selection_mode == "percent":
                accounts_per_post = max(
                    1, math.floor(len(joined_ids) * selection_value / 100)
                )
            else:
                accounts_per_post = min(selection_value, len(joined_ids))
            accounts_per_post = min(accounts_per_post, len(joined_ids))

            fetch_limit = min(1000, max(post_count * 10, post_count + 50))
            messages = await self._get_messages_from_first_resolved_channel(
                fresh, limit=fetch_limit
            )
            if messages is None:
                raise RuntimeError("Нет активного аккаунта, который видит канал")
            canonical = self._canonical_posts(messages)
            selected_posts = canonical[-post_count:]
            if not selected_posts:
                raise RuntimeError("В канале не найдены публикации")

            delay_min, delay_max = await self.db.get_delays(
                self.settings.default_reaction_delay_min_seconds,
                self.settings.default_reaction_delay_max_seconds,
            )
            now = utcnow()
            workloads = {
                account_id: joined_workloads[account_id]
                for account_id in joined_ids
                if account_id in joined_workloads
            }
            planned_actions: dict[int, int] = {}
            latest_due = await self.db.latest_pending_view_due(fresh.id)
            append_to_existing = bool(latest_due and latest_due > now)
            cursor = latest_due if append_to_existing else now
            first_account = not append_to_existing
            skipped_existing = 0
            account_messages: dict[int, list[int]] = {}

            # Newest posts are selected first. The configured delay is applied
            # between accounts, while all selected posts of one account share the
            # same due time. This prevents a 100-post batch from multiplying the
            # delay by every account/post pair.
            for message_id, _has_image in reversed(selected_posts):
                covered = await self.db.viewed_or_scheduled_account_ids(
                    fresh.id, message_id
                )
                eligible = [account_id for account_id in joined_ids if account_id not in covered]
                selected_ids = select_account_ids(
                    workloads,
                    eligible,
                    min(accounts_per_post, len(eligible)),
                    now=now,
                    planned_actions=planned_actions,
                    rng=_rng,
                )
                skipped_existing += max(0, accounts_per_post - len(selected_ids))
                for account_id in selected_ids:
                    account_messages.setdefault(account_id, []).append(message_id)
                    planned_actions[account_id] = planned_actions.get(account_id, 0) + 1

            account_order = rank_account_ids(
                workloads,
                account_messages,
                now=now,
                planned_actions=planned_actions,
                rng=_rng,
            )
            jobs: list[tuple[int, int, datetime]] = []
            for account_id in account_order:
                if first_account:
                    due_at = now
                    first_account = False
                else:
                    cursor += timedelta(seconds=_rng.randint(delay_min, delay_max))
                    due_at = cursor
                workload = workloads.get(account_id)
                if (
                    workload is not None
                    and workload.flood_until is not None
                    and workload.flood_until > now
                ):
                    due_at = max(
                        due_at, workload.flood_until + timedelta(seconds=1)
                    )
                    cursor = max(cursor, due_at)
                for message_id in account_messages[account_id]:
                    jobs.append((account_id, message_id, due_at))

            batch = await self.db.create_view_batch(
                channel_id=fresh.id,
                requested_post_count=post_count,
                posts_found=len(selected_posts),
                accounts_per_post=accounts_per_post,
                selection_mode=selection_mode,
                selection_value=selection_value,
                jobs=jobs,
                skipped_existing=skipped_existing,
            )
            logger.info(
                "Manual view batch scheduled batch=%s target=%s requested_posts=%s "
                "posts_found=%s accounts_per_post=%s mode=%s value=%s jobs=%s "
                "skipped_existing=%s delay=%s-%s append=%s",
                batch.id,
                fresh.id,
                post_count,
                len(selected_posts),
                accounts_per_post,
                selection_mode,
                selection_value,
                batch.total_jobs,
                batch.skipped_existing,
                delay_min,
                delay_max,
                append_to_existing,
            )
            return batch

    async def monitor_loop(self) -> None:
        while True:
            self._worker_tick("channel_monitor")
            try:
                channels = await self.db.list_channels(active_only=True)
                ai_memory_channel_ids = await self._enabled_ai_memory_channel_ids()
                for channel in channels:
                    self._worker_tick("channel_monitor")
                    promotion_active = promotion_is_active(channel)
                    if not promotion_active:
                        expired = await self.db.close_expired_promotion(channel.id)
                        if expired["expired"]:
                            logger.info(
                                "Promotion expired target=%s cancelled=%s",
                                channel.id,
                                expired["cancelled"],
                            )
                    monitor_reactions = bool(
                        promotion_active and channel.new_posts_enabled
                    )
                    ingest_ai_memory = channel.id in ai_memory_channel_ids
                    if monitor_reactions or ingest_ai_memory:
                        await self._monitor_channel(
                            channel,
                            monitor_reactions=monitor_reactions,
                            ingest_ai_memory=ingest_ai_memory,
                        )
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001
                self._worker_error("channel_monitor", exc)
                logger.exception("Ошибка общего цикла мониторинга")
            else:
                self._worker_success("channel_monitor")
            await asyncio.sleep(self.settings.monitor_interval_seconds)

    async def _monitor_channel(
        self,
        channel: Channel,
        *,
        monitor_reactions: bool = True,
        ingest_ai_memory: bool = False,
    ) -> None:
        try:
            fetch_limit = (
                200 if monitor_reactions else AI_CHANNEL_MEMORY_FETCH_LIMIT
            )
            messages = await self._get_messages_from_first_resolved_channel(
                channel,
                limit=fetch_limit,
            )
            if messages is None:
                if monitor_reactions:
                    await self.db.set_channel_error(
                        channel.id,
                        "Нет аккаунта с доступом",
                    )
                elif ingest_ai_memory:
                    logger.warning(
                        "AI channel memory refresh deferred target=%s reason=no-access-account",
                        channel.id,
                    )
                return
            if monitor_reactions:
                new_messages = [
                    message
                    for message in messages
                    if message.id > channel.last_seen_message_id
                ]
                posts = self._canonical_posts(new_messages)
                for message_id, has_image in posts:
                    await self.schedule_post(
                        channel,
                        message_id,
                        source="new",
                        has_image=has_image,
                    )
                if new_messages:
                    await self.db.set_channel_last_seen(
                        channel.id,
                        max(message.id for message in new_messages),
                    )
            if ingest_ai_memory:
                try:
                    async with self._ai_memory_lock(channel.id):
                        await self._ingest_ai_channel_messages(channel, messages)
                except asyncio.CancelledError:
                    raise
                except Exception as exc:  # noqa: BLE001
                    # Memory is an isolated optional capability. Its failure must
                    # never cancel reaction scheduling or mark the core worker dead.
                    logger.warning(
                        "AI channel memory refresh failed target=%s error=%s: %s",
                        channel.id,
                        type(exc).__name__,
                        exc,
                    )
        except Exception as exc:  # noqa: BLE001
            logger.exception("Ошибка мониторинга цели %s", channel.id)
            if monitor_reactions:
                await self.db.set_channel_error(
                    channel.id,
                    f"{type(exc).__name__}: {exc}",
                )

    async def join_worker_loop(self) -> None:
        while True:
            self._worker_tick("join_worker")
            try:
                job = await self.db.get_due_join_job(utcnow())
                if not job:
                    self._worker_success("join_worker")
                    await asyncio.sleep(self.settings.worker_interval_seconds)
                    continue
                if not await self.db.mark_join_running(
                    job.id, expected_action=job.action
                ):
                    self._worker_success("join_worker")
                    continue
                self._active_membership_job_id = int(job.id)
                try:
                    completed = await self._execute_membership_with_timeout(job)
                finally:
                    if self._active_membership_job_id == int(job.id):
                        self._active_membership_job_id = None
                self._worker_success("join_worker", resolve_warning=completed)
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001
                self._worker_error("join_worker", exc)
                logger.exception("Ошибка membership worker")
                await asyncio.sleep(self.settings.worker_interval_seconds)

    async def _execute_membership_with_timeout(self, job) -> bool:
        timeout_seconds = max(
            1, int(getattr(self.settings, "membership_attempt_timeout_seconds", 180))
        )
        try:
            async with asyncio.timeout(timeout_seconds):
                return bool(await self._execute_membership_job(job))
        except TimeoutError:
            error = f"Membership attempt timeout after {timeout_seconds}s"
            expected_action = getattr(job, "action", None)
            self._worker_warning("join_worker", error)
            if self._can_retry_after_current_attempt(job):
                due = utcnow() + timedelta(
                    minutes=min(60, 2 ** max(0, int(job.attempts)))
                )
                await self.db.retry_join_job(
                    job.id, due, error, expected_action=expected_action
                )
            else:
                await self.db.fail_join_job(
                    job.id, error, expected_action=expected_action
                )
            return False

    async def _fresh_job_entities(self, job):
        """Refresh account/target after waiting for the per-account lock.

        Admin actions can disable or delete an account while a worker is waiting.
        Production Database exposes both loaders; lightweight test adapters may
        omit them and keep the historical detached objects.
        """

        account_loader = getattr(self.db, "get_account", None)
        channel_loader = getattr(self.db, "get_channel", None)
        account = (
            await account_loader(job.account.id)
            if account_loader is not None
            else job.account
        )
        channel = (
            await channel_loader(job.channel.id)
            if channel_loader is not None
            else job.channel
        )
        return account, channel

    async def _execute_membership_job(self, job) -> bool:
        if job.action == "join" and (not job.account.is_active or not job.channel.is_active):
            await self.db.fail_join_job(
                job.id,
                "Аккаунт или канал/группа выключены",
                expected_action=job.action,
            )
            return False
        try:
            async with self.pool.lock_for(job.account.id):
                current_check = getattr(self.db, "membership_job_is_running", None)
                if current_check is not None and not await current_check(
                    job.id, expected_action=job.action
                ):
                    logger.info(
                        "Skipped superseded membership action=%s account=%s target=%s",
                        job.action,
                        job.account.id,
                        job.channel.id,
                    )
                    return False
                account, channel = await self._fresh_job_entities(job)
                if account is None or channel is None:
                    logger.info(
                        "Skipped membership for deleted account/target action=%s account=%s target=%s",
                        job.action,
                        job.account.id,
                        job.channel.id,
                    )
                    return False
                if getattr(account, "status", "ready") == "unauthorized" or (
                    job.action == "join"
                    and (not account.is_active or not channel.is_active)
                ):
                    await self.db.fail_join_job(
                        job.id,
                        "Аккаунт или канал/группа выключены",
                        expected_action=job.action,
                    )
                    return False
                try:
                    client = await self.pool.get(account)
                    if job.action == "leave":
                        await self.pool.leave_channel(client, channel)
                    else:
                        parsed = parse_channel_link(channel.link)
                        if parsed.kind == "private":
                            try:
                                await self.pool.resolve_channel(client, channel)
                            except TargetNotVisibleError:
                                # Only the explicit "not visible in dialogs" signal
                                # justifies using the invite. Network, FloodWait and auth
                                # errors must propagate instead of triggering a second
                                # MTProto call.
                                await self.pool.join_channel(client, parsed)
                        else:
                            await self.pool.join_channel(client, parsed)
                except ACCOUNT_AUTH_FAILURES:
                    await self.pool.remove_unauthorized_account_while_locked(
                        job.account.id, context=f"membership:{job.action}"
                    )
                    raise
            final_status = "left" if job.action == "leave" else "done"
            applied = await self.db.complete_join_job(
                job.id,
                final_status=final_status,
                expected_action=job.action,
            )
            if not applied:
                logger.info(
                    "Ignored stale membership completion action=%s account=%s target=%s",
                    job.action,
                    job.account.id,
                    job.channel.id,
                )
                return True
            logger.info(
                "Membership job completed action=%s account=%s target=%s status=%s",
                job.action,
                job.account.id,
                job.channel.id,
                final_status,
            )
            return True
        except ACCOUNT_AUTH_FAILURES:
            await self.db.fail_join_job(
                job.id,
                "Аккаунт потерял авторизацию",
                expected_action=job.action,
            )
        except errors.UserAlreadyParticipantError:
            await self.db.complete_join_job(
                job.id, final_status="done", expected_action=job.action
            )
            return True
        except errors.UserNotParticipantError:
            await self.db.complete_join_job(
                job.id, final_status="left", expected_action=job.action
            )
            return True
        except errors.FloodWaitError as exc:
            due = utcnow() + timedelta(seconds=int(exc.seconds) + 5)
            await self.db.retry_join_job(
                job.id,
                due,
                f"FloodWait {exc.seconds}s",
                expected_action=job.action,
            )
            await self.db.set_account_error(job.account.id, f"FloodWait {exc.seconds}s", flood_until=due)
        except Exception as exc:  # noqa: BLE001
            logger.exception(
                "Ошибка membership action=%s account=%s target=%s",
                job.action,
                job.account.id,
                job.channel.id,
            )
            if self._can_retry_after_current_attempt(job):
                due = utcnow() + timedelta(minutes=min(60, 2 ** max(0, int(job.attempts))))
                await self.db.retry_join_job(
                    job.id,
                    due,
                    f"{type(exc).__name__}: {exc}",
                    expected_action=job.action,
                )
            else:
                await self.db.fail_join_job(
                    job.id,
                    f"{type(exc).__name__}: {exc}",
                    expected_action=job.action,
                )
        return False

    async def view_worker_loop(self) -> None:
        while True:
            self._worker_tick("view_worker")
            try:
                job = await self.db.get_due_view_job(utcnow())
                if not job:
                    self._worker_success("view_worker")
                    await asyncio.sleep(self.settings.worker_interval_seconds)
                    continue
                if not await self.db.mark_view_running(job.id):
                    self._worker_success("view_worker")
                    continue
                self._active_view_job_id = int(job.id)
                try:
                    completed = await self._execute_view_with_timeout(job)
                finally:
                    if self._active_view_job_id == int(job.id):
                        self._active_view_job_id = None
                self._worker_success("view_worker", resolve_warning=completed)
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001
                self._worker_error("view_worker", exc)
                logger.exception("Ошибка view worker")
                await asyncio.sleep(self.settings.worker_interval_seconds)

    async def _execute_view_with_timeout(self, job) -> bool:
        timeout_seconds = max(
            1, int(getattr(self.settings, "view_attempt_timeout_seconds", 180))
        )
        try:
            async with asyncio.timeout(timeout_seconds):
                return bool(await self._execute_view(job))
        except TimeoutError:
            error = f"View attempt timeout after {timeout_seconds}s"
            self._worker_warning("view_worker", error)
            if self._can_retry_after_current_attempt(job):
                due = utcnow() + timedelta(
                    minutes=min(60, 2 ** max(0, int(job.attempts)))
                )
                await self.db.retry_view_job(job.id, due, error)
            else:
                await self.db.fail_view_job(job.id, error)
            return False

    async def _execute_view(self, job) -> bool:
        if not job.account.is_active or not job.channel.is_active or job.channel.kind != "channel":
            await self.db.fail_view_job(job.id, "Аккаунт или канал выключены")
            return False
        if job.account.flood_until and job.account.flood_until > utcnow():
            await self.db.retry_view_job(
                job.id, job.account.flood_until, "Ожидание после FloodWait"
            )
            return False
        try:
            async with self.pool.lock_for(job.account.id):
                account, channel = await self._fresh_job_entities(job)
                if (
                    account is None
                    or channel is None
                    or not account.is_active
                    or getattr(account, "status", "ready") == "unauthorized"
                    or not channel.is_active
                    or channel.kind != "channel"
                ):
                    await self.db.fail_view_job(
                        job.id, "Аккаунт или канал выключены"
                    )
                    return False
                try:
                    client = await self.pool.get(account)
                    entity = await self.pool.resolve_channel(client, channel)
                    try:
                        await client(
                            functions.messages.GetMessagesViewsRequest(
                                peer=entity,
                                id=[job.message_id],
                                increment=True,
                            )
                        )
                    except errors.ChatNotModifiedError:
                        # The account has already marked this publication as viewed.
                        pass
                except ACCOUNT_AUTH_FAILURES:
                    await self.pool.remove_unauthorized_account_while_locked(
                        job.account.id, context="manual-view"
                    )
                    raise
            await self.db.complete_view_job(job.id)
            logger.info(
                "View completed batch=%s account=%s target=%s message=%s",
                job.batch_id,
                job.account.id,
                job.channel.id,
                job.message_id,
            )
            return True
        except ACCOUNT_AUTH_FAILURES:
            await self.db.fail_view_job(job.id, "Аккаунт потерял авторизацию")
        except errors.FloodWaitError as exc:
            due = utcnow() + timedelta(seconds=int(exc.seconds) + 5)
            await self.db.retry_view_job(job.id, due, f"FloodWait {exc.seconds}s")
            await self.db.set_account_error(
                job.account.id, f"FloodWait {exc.seconds}s", flood_until=due
            )
        except Exception as exc:  # noqa: BLE001
            logger.exception(
                "Ошибка просмотра batch=%s account=%s target=%s message=%s",
                job.batch_id,
                job.account.id,
                job.channel.id,
                job.message_id,
            )
            if self._can_retry_after_current_attempt(job):
                due = utcnow() + timedelta(minutes=min(60, 2 ** max(0, int(job.attempts))))
                await self.db.retry_view_job(
                    job.id, due, f"{type(exc).__name__}: {exc}"
                )
            else:
                await self.db.fail_view_job(job.id, f"{type(exc).__name__}: {exc}")
        return False

    async def reaction_worker_loop(self) -> None:
        # Different accounts may execute independently, while account locks and the
        # active-account filter prevent two simultaneous actions from one account.
        max_concurrency = max(1, min(5, self.settings.max_accounts_per_channel))
        running = self._reaction_tasks
        try:
            while True:
                self._worker_tick("reaction_worker")
                finished = [task for task in running if task.done()]
                task_error = False
                task_success = False
                for task in finished:
                    task_state = running.pop(task)
                    try:
                        completed_reaction = bool(task.result())
                    except asyncio.CancelledError:
                        raise
                    except Exception as exc:  # noqa: BLE001
                        task_error = True
                        self._worker_warning(
                            "reaction_worker",
                            f"Reaction task job={task_state.job_id} account={task_state.account_id}: "
                            f"{type(exc).__name__}: {exc}",
                        )
                        logger.exception(
                            "Необработанная ошибка reaction task job=%s account=%s",
                            task_state.job_id,
                            task_state.account_id,
                        )
                    else:
                        # A handled retry/failure proves that the child task returned,
                        # but only a completed Telegram reaction proves recovery.
                        task_success = task_success or completed_reaction

                claimed = False
                while len(running) < max_concurrency:
                    active_accounts = {item.account_id for item in running.values()}
                    job = await self.db.get_due_reaction_job(
                        utcnow(), exclude_account_ids=active_accounts
                    )
                    if not job or not await self.db.mark_reaction_running(job.id):
                        break
                    started_at = utcnow()
                    task = asyncio.create_task(
                        self._execute_reaction_with_timeout(job),
                        name=f"reaction-job-{job.id}",
                    )
                    running[task] = RunningReactionTask(
                        job_id=int(job.id),
                        account_id=int(job.account.id),
                        started_at=started_at,
                    )
                    claimed = True

                if task_error:
                    # Keep the warning visible until a later child task completes
                    # without an unhandled exception.
                    self._worker_success("reaction_worker", resolve_warning=False)
                elif task_success:
                    self._worker_success("reaction_worker", resolve_warning=True)
                else:
                    # An idle polling loop is alive, but it is not proof that a
                    # previously failed child reaction recovered.
                    self._worker_success("reaction_worker", resolve_warning=False)
                if not claimed:
                    await asyncio.sleep(self.settings.worker_interval_seconds)
        except asyncio.CancelledError:
            for task in running:
                task.cancel()
            if running:
                await asyncio.gather(*running, return_exceptions=True)
            running.clear()
            raise
        except Exception as exc:  # noqa: BLE001
            self._worker_error("reaction_worker", exc)
            logger.exception("Ошибка reaction worker")
            for task in running:
                task.cancel()
            if running:
                await asyncio.gather(*running, return_exceptions=True)
            running.clear()
            raise

    async def _execute_reaction_with_timeout(self, job) -> bool:
        timeout_seconds = max(
            1, int(getattr(self.settings, "reaction_attempt_timeout_seconds", 180))
        )
        try:
            async with asyncio.timeout(timeout_seconds):
                return bool(await self._execute_reaction(job))
        except TimeoutError as exc:
            error = f"Reaction attempt timeout after {timeout_seconds}s"
            if self._can_retry_after_current_attempt(job):
                due = utcnow() + timedelta(minutes=min(60, 2 ** max(0, int(job.attempts))))
                await self.db.retry_reaction_job(job.id, due, error)
            else:
                await self.db.fail_reaction_job(job.id, error)
            raise RuntimeError(error) from exc

    async def _execute_reaction(self, job) -> bool:
        if not job.account.is_active or not job.channel.is_active:
            await self.db.fail_reaction_job(job.id, "Аккаунт или канал/группа выключены")
            return False
        if not promotion_is_active(job.channel):
            await self.db.close_expired_promotion(job.channel.id)
            await self.db.cancel_reaction_job(job.id, "Период раскрутки завершён")
            return False
        if job.account.flood_until and job.account.flood_until > utcnow():
            await self.db.retry_reaction_job(job.id, job.account.flood_until, "Ожидание после FloodWait")
            return False
        last_reaction_at = getattr(job.account, "last_reaction_at", None)
        if last_reaction_at is not None:
            delay_min, delay_max = await self.db.get_delays(
                self.settings.default_reaction_delay_min_seconds,
                self.settings.default_reaction_delay_max_seconds,
            )
            earliest = last_reaction_at + timedelta(
                seconds=_rng.randint(delay_min, delay_max)
            )
            if earliest > utcnow():
                await self.db.retry_reaction_job(
                    job.id, earliest, "Пауза между действиями аккаунта"
                )
                return False
        try:
            async with self.pool.lock_for(job.account.id):
                account, channel = await self._fresh_job_entities(job)
                if (
                    account is None
                    or channel is None
                    or not account.is_active
                    or getattr(account, "status", "ready") == "unauthorized"
                    or not channel.is_active
                ):
                    await self.db.fail_reaction_job(
                        job.id, "Аккаунт или канал/группа выключены"
                    )
                    return False
                try:
                    client = await self.pool.get(account)
                    entity = await self.pool.resolve_channel(client, channel)
                    view_status = "not_applicable"
                    if channel.kind == "channel":
                        try:
                            await client(
                                functions.messages.GetMessagesViewsRequest(
                                    peer=entity,
                                    id=[job.message_id],
                                    increment=True,
                                )
                            )
                            view_status = "requested"
                        except errors.ChatNotModifiedError:
                            # Telegram may report that this account has already marked the
                            # post as viewed. The invariant is still satisfied, so the
                            # reaction can continue without turning the job into a retry loop.
                            view_status = "already_viewed"
                        # Persist the proof before sending the reaction. If the reaction
                        # itself fails and is retried, the successful view is still known.
                        await self.db.mark_reaction_view_confirmed(job.id)
                    await client(
                        functions.messages.SendReactionRequest(
                            peer=entity,
                            msg_id=job.message_id,
                            reaction=[types.ReactionEmoji(emoticon=job.reaction)],
                        )
                    )
                except ACCOUNT_AUTH_FAILURES:
                    await self.pool.remove_unauthorized_account_while_locked(
                        job.account.id, context="reaction"
                    )
                    raise
            await self.db.complete_reaction_job(job.id)
            logger.info(
                "Reaction completed account=%s target=%s message=%s reaction=%s "
                "source=%s view=%s",
                job.account.id,
                job.channel.id,
                job.message_id,
                job.reaction,
                job.source,
                view_status,
            )
            return True
        except ACCOUNT_AUTH_FAILURES:
            await self.db.fail_reaction_job(job.id, "Аккаунт потерял авторизацию")
        except errors.FloodWaitError as exc:
            due = utcnow() + timedelta(seconds=int(exc.seconds) + 5)
            await self.db.retry_reaction_job(job.id, due, f"FloodWait {exc.seconds}s")
            await self.db.set_account_error(job.account.id, f"FloodWait {exc.seconds}s", flood_until=due)
        except (errors.ReactionInvalidError, errors.ChatWriteForbiddenError) as exc:
            await self.db.fail_reaction_job(job.id, f"Реакция недоступна: {type(exc).__name__}")
        except Exception as exc:  # noqa: BLE001
            logger.exception(
                "Ошибка реакции account=%s target=%s message=%s",
                job.account.id,
                job.channel.id,
                job.message_id,
            )
            if self._can_retry_after_current_attempt(job):
                due = utcnow() + timedelta(minutes=min(60, 2 ** max(0, int(job.attempts))))
                await self.db.retry_reaction_job(job.id, due, f"{type(exc).__name__}: {exc}")
            else:
                await self.db.fail_reaction_job(job.id, f"{type(exc).__name__}: {exc}")
        return False

    async def _get_messages_from_first_resolved_channel(
        self, channel: Channel, *, limit: int
    ):
        """Resolve and read a target in one uninterrupted account lock.

        Releasing the lock after entity resolution and reacquiring it for
        ``get_messages`` leaves a gap where quarantine or session reset can
        disconnect the selected client. Keep selection, authorization, resolve and
        read inside the same stable per-account critical section.
        """

        joined_ids = await self.db.joined_account_ids(channel.id)
        if not joined_ids:
            joined_ids = [
                account.id
                for account in await self.db.list_accounts(active_only=True)
            ]
        ranked_ids, _workloads = await self._rank_account_ids(joined_ids)
        for account_id in ranked_ids:
            account = await self.db.get_account(account_id)
            if not account or not account.is_active:
                continue
            try:
                async with self.pool.lock_for(account.id):
                    try:
                        channel_loader = getattr(self.db, "get_channel", None)
                        current_channel = (
                            await channel_loader(channel.id)
                            if channel_loader is not None
                            else channel
                        )
                        if current_channel is None or not current_channel.is_active:
                            return None
                        client = await self.pool.get(account)
                        entity = await self.pool.resolve_channel(
                            client, current_channel
                        )
                        return await client.get_messages(entity, limit=limit)
                    except ACCOUNT_AUTH_FAILURES:
                        await self.pool.remove_unauthorized_account_while_locked(
                            account.id, context=f"resolve-target:{channel.id}"
                        )
                        raise
            except ACCOUNT_AUTH_FAILURES:
                continue
            except Exception as exc:  # noqa: BLE001
                logger.debug(
                    "Account cannot read target account=%s target=%s error=%s: %s",
                    account.id,
                    channel.id,
                    type(exc).__name__,
                    exc,
                )
                continue
        return None

    @staticmethod
    def _message_has_image(message) -> bool:
        if getattr(message, "photo", None) is not None:
            return True
        document = getattr(message, "document", None)
        mime_type = str(getattr(document, "mime_type", "") or "").casefold()
        return mime_type.startswith("image/")

    @classmethod
    def _canonical_posts(cls, messages) -> list[tuple[int, bool]]:
        grouped: dict[int, tuple[int, bool]] = {}
        singles: list[tuple[int, bool]] = []
        for message in messages:
            if getattr(message, "action", None) is not None:
                continue
            has_image = cls._message_has_image(message)
            grouped_id = getattr(message, "grouped_id", None)
            if grouped_id:
                current = grouped.get(grouped_id)
                if current is None:
                    grouped[grouped_id] = (message.id, has_image)
                else:
                    grouped[grouped_id] = (min(current[0], message.id), current[1] or has_image)
            else:
                singles.append((message.id, has_image))
        return sorted(set(singles + list(grouped.values())), key=lambda item: item[0])

    @classmethod
    def _canonical_post_ids(cls, messages) -> list[int]:
        """Backward-compatible helper used by earlier tests and integrations."""

        return [message_id for message_id, _has_image in cls._canonical_posts(messages)]

    @staticmethod
    def _post_type_name(has_image: bool | None) -> str:
        if has_image is True:
            return "image"
        if has_image is False:
            return "no_image"
        return "unknown"
