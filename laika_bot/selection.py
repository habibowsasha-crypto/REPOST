from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from random import SystemRandom
from typing import Iterable, Mapping

from .models import utcnow


@dataclass(frozen=True, slots=True)
class AccountWorkload:
    """Persisted operational facts used only to distribute LikeBot work fairly.

    This is not a Telegram trust score. Lower calculated penalty means that the
    account is currently a better candidate for the next queued action.
    """

    account_id: int
    pending: int = 0
    running: int = 0
    completed_recent: int = 0
    failed_recent: int = 0
    last_action_at: datetime | None = None
    flood_until: datetime | None = None
    last_error: str | None = None
    last_error_at: datetime | None = None
    status: str = "ready"
    is_active: bool = True


def _active_error(workload: AccountWorkload, now: datetime) -> bool:
    error = (workload.last_error or "").strip()
    if not error:
        return False
    if (
        workload.last_error_at is not None
        and (now - workload.last_error_at).total_seconds() > 24 * 60 * 60
    ):
        return False
    if (
        workload.flood_until is not None
        and workload.flood_until <= now
        and error.casefold().startswith("floodwait")
    ):
        return False
    return True


def account_selection_penalty(
    workload: AccountWorkload,
    *,
    now: datetime | None = None,
    planned_actions: int = 0,
) -> int:
    """Return a deterministic lower-is-better workload penalty.

    The weights intentionally favour accounts that have rested longer, while
    strongly deprioritising running work, fresh failures and active FloodWait.
    Historical totals are not used, so an old account is not punished forever.
    """

    current = now or utcnow()
    if not workload.is_active or workload.status == "unauthorized":
        return 1_000_000_000

    penalty = 0
    if workload.status != "ready":
        penalty += 100_000

    pending = max(0, int(workload.pending)) + max(0, int(planned_actions))
    running = max(0, int(workload.running))
    completed_recent = min(250, max(0, int(workload.completed_recent)))
    failed_recent = min(20, max(0, int(workload.failed_recent)))

    penalty += pending * 24
    penalty += running * 500
    penalty += completed_recent * 4
    penalty += failed_recent * 120

    if _active_error(workload, current):
        penalty += 300

    if workload.flood_until is not None and workload.flood_until > current:
        remaining_seconds = max(
            0, int((workload.flood_until - current).total_seconds())
        )
        # FloodWait accounts remain available as a last resort, but will be
        # selected only after healthy alternatives. The due time is separately
        # moved beyond flood_until by the scheduler.
        penalty += 1_000_000 + min(86_400, remaining_seconds)

    if workload.last_action_at is None:
        penalty -= 360
    else:
        rest_seconds = max(
            0, int((current - workload.last_action_at).total_seconds())
        )
        penalty -= min(360, rest_seconds // 240)

    return penalty


def rank_account_ids(
    workloads: Mapping[int, AccountWorkload],
    account_ids: Iterable[int],
    *,
    now: datetime | None = None,
    planned_actions: Mapping[int, int] | None = None,
    rng: SystemRandom | None = None,
) -> list[int]:
    """Rank unique candidate ids with small secure jitter for equal workloads."""

    current = now or utcnow()
    randomizer = rng or SystemRandom()
    planned = planned_actions or {}
    unique_ids = list(dict.fromkeys(int(account_id) for account_id in account_ids))

    decorated: list[tuple[int, int, int]] = []
    for account_id in unique_ids:
        workload = workloads.get(account_id)
        if workload is None:
            # Missing metrics must not make an account silently preferred.
            workload = AccountWorkload(account_id=account_id, status="unknown")
        penalty = account_selection_penalty(
            workload,
            now=current,
            planned_actions=int(planned.get(account_id, 0)),
        )
        decorated.append((penalty, randomizer.randrange(0, 10_000), account_id))

    decorated.sort()
    return [account_id for _penalty, _jitter, account_id in decorated]


def select_account_ids(
    workloads: Mapping[int, AccountWorkload],
    account_ids: Iterable[int],
    limit: int,
    *,
    now: datetime | None = None,
    planned_actions: Mapping[int, int] | None = None,
    rng: SystemRandom | None = None,
) -> list[int]:
    if limit <= 0:
        return []
    return rank_account_ids(
        workloads,
        account_ids,
        now=now,
        planned_actions=planned_actions,
        rng=rng,
    )[:limit]
