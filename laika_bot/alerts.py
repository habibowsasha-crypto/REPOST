from __future__ import annotations

import asyncio
import html
import json
import logging
import re
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Awaitable, Callable, Protocol

from .utils import display_account_name, truncate

logger = logging.getLogger(__name__)

ALERT_STATE_SETTING_KEY = "critical_alert_state_v1"


def _utcnow() -> datetime:
    return datetime.now(UTC).replace(tzinfo=None)


def _parse_datetime(value: object) -> datetime | None:
    if not isinstance(value, str) or not value:
        return None
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return None
    if parsed.tzinfo is not None:
        parsed = parsed.astimezone(UTC).replace(tzinfo=None)
    return parsed


def _safe_text(value: object, limit: int = 500) -> str:
    compact = " ".join(str(value).split())
    # Never echo common credential shapes from exception strings to Telegram or logs.
    compact = re.sub(
        r"\b\d{6,12}:[A-Za-z0-9_-]{20,}\b",
        "<redacted-bot-token>",
        compact,
    )
    compact = re.sub(
        (
            r"(?i)\b((?:bot_?token|api_?hash|password|secret|"
            r"session(?:_encryption)?_key)\s*[=:]\s*)[^\s&]+"
        ),
        r"\1<redacted>",
        compact,
    )
    compact = re.sub(
        r"(?i)([a-z][a-z0-9+.-]*://)([^/\s:@]+):([^@\s/]+)@",
        r"\1<redacted>:<redacted>@",
        compact,
    )
    return truncate(compact, limit)


class AlertDatabase(Protocol):
    async def stats(self) -> dict[str, int]: ...

    async def get_setting(self, key: str, default: str) -> str: ...

    async def set_setting(self, key: str, value: str) -> None: ...

    async def recent_failure_counts(
        self, window_minutes: int, *, now: datetime | None = None
    ) -> dict[str, int]: ...


class WorkerSnapshotProvider(Protocol):
    def worker_health_snapshot(
        self, *, now: datetime | None = None
    ) -> list[dict[str, object | None]]: ...


AdminSender = Callable[[str], Awaitable[object]]
Clock = Callable[[], datetime]


@dataclass(slots=True)
class IncidentState:
    first_seen_at: datetime
    last_sent_at: datetime | None
    title: str
    detail: str
    severity: str

    def to_json(self) -> dict[str, str | None]:
        return {
            "first_seen_at": self.first_seen_at.isoformat(),
            "last_sent_at": self.last_sent_at.isoformat() if self.last_sent_at else None,
            "title": self.title,
            "detail": self.detail,
            "severity": self.severity,
        }

    @classmethod
    def from_json(cls, value: object) -> IncidentState | None:
        if not isinstance(value, dict):
            return None
        first_seen_at = _parse_datetime(value.get("first_seen_at"))
        if first_seen_at is None:
            return None
        last_sent_at = _parse_datetime(value.get("last_sent_at"))
        title = _safe_text(value.get("title", "Состояние LikeBot"), 160)
        detail = _safe_text(value.get("detail", ""), 500)
        severity = str(value.get("severity", "warning"))
        if severity not in {"warning", "critical"}:
            severity = "warning"
        return cls(
            first_seen_at=first_seen_at,
            last_sent_at=last_sent_at,
            title=title,
            detail=detail,
            severity=severity,
        )


@dataclass(frozen=True, slots=True)
class AlertTransition:
    kind: str
    key: str
    title: str
    detail: str
    severity: str
    first_seen_at: datetime


class CriticalAlertService:
    """Deduplicated critical-state notifications for the LikeBot administrator.

    Alert state is stored in the existing ``app_settings`` table, so a Railway
    restart does not immediately resend every still-active incident. Delivery
    failures never crash Telegram workers; the same incident is retried later.
    """

    def __init__(
        self,
        settings: object,
        db: AlertDatabase,
        jobs: WorkerSnapshotProvider,
        sender: AdminSender,
        *,
        clock: Clock = _utcnow,
    ) -> None:
        self.settings = settings
        self.db = db
        self.jobs = jobs
        self.sender = sender
        self.clock = clock
        self.started_at = clock()
        self._states: dict[str, IncidentState] = {}
        self._loaded = False
        self._dirty = False
        self._lock = asyncio.Lock()

    @property
    def enabled(self) -> bool:
        return bool(getattr(self.settings, "alerts_enabled", True))

    @property
    def check_interval_seconds(self) -> int:
        return max(10, int(getattr(self.settings, "alert_check_interval_seconds", 60)))

    @property
    def repeat_seconds(self) -> int:
        return max(300, int(getattr(self.settings, "alert_repeat_seconds", 3600)))

    @property
    def send_timeout_seconds(self) -> float:
        return max(0.01, float(getattr(self.settings, "alert_send_timeout_seconds", 15)))

    async def _ensure_loaded(self) -> bool:
        if self._loaded:
            return True
        try:
            raw = await self.db.get_setting(ALERT_STATE_SETTING_KEY, "{}")
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001
            # A transient database failure must not permanently switch the service
            # to an empty state: retry loading on the next monitor iteration.
            logger.warning(
                "Critical alert state could not be loaded error=%s: %s",
                type(exc).__name__,
                _safe_text(exc, 300),
            )
            return False

        self._loaded = True
        try:
            decoded = json.loads(raw)
        except (TypeError, ValueError) as exc:
            logger.warning(
                "Critical alert state ignored because it is invalid JSON: %s", exc
            )
            return True
        if not isinstance(decoded, dict):
            logger.warning("Critical alert state ignored because it is not an object")
            return True
        had_in_memory_changes = self._dirty
        for key, value in decoded.items():
            persisted = IncidentState.from_json(value)
            if persisted is None:
                continue
            normalized_key = str(key)
            current = self._states.get(normalized_key)
            if current is None:
                self._states[normalized_key] = persisted
                continue

            # A transient initial read failure can be followed by a newer in-memory
            # incident. Preserve its current title/detail/severity, but merge the
            # oldest first-seen time and newest delivery time from persistent state.
            current.first_seen_at = min(
                current.first_seen_at, persisted.first_seen_at
            )
            sent_candidates = [
                item
                for item in (current.last_sent_at, persisted.last_sent_at)
                if item is not None
            ]
            current.last_sent_at = max(sent_candidates) if sent_candidates else None
            self._dirty = True
        if had_in_memory_changes:
            self._dirty = True
        return True

    async def _persist_if_dirty(self) -> None:
        # Never overwrite persisted incidents with a partial in-memory snapshot
        # when the initial state read failed. Keep the dirty flag and merge after
        # a later successful load instead.
        if not self._dirty or not self._loaded:
            return
        payload = json.dumps(
            {key: value.to_json() for key, value in sorted(self._states.items())},
            ensure_ascii=False,
            separators=(",", ":"),
        )
        try:
            await self.db.set_setting(ALERT_STATE_SETTING_KEY, payload)
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                "Critical alert state could not be persisted error=%s: %s",
                type(exc).__name__,
                _safe_text(exc, 300),
            )
            return
        self._dirty = False

    async def _send(self, text: str) -> bool:
        try:
            async with asyncio.timeout(self.send_timeout_seconds):
                await self.sender(text)
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                "Critical alert delivery failed error=%s: %s",
                type(exc).__name__,
                _safe_text(exc, 300),
            )
            return False
        return True

    def _transition(
        self,
        *,
        key: str,
        active: bool,
        title: str,
        detail: str,
        severity: str,
        now: datetime,
    ) -> AlertTransition | None:
        current = self._states.get(key)
        normalized_title = _safe_text(title, 120)
        normalized_detail = _safe_text(detail, 300)
        normalized_severity = "critical" if severity == "critical" else "warning"

        if active:
            if current is None:
                current = IncidentState(
                    first_seen_at=now,
                    last_sent_at=None,
                    title=normalized_title,
                    detail=normalized_detail,
                    severity=normalized_severity,
                )
                self._states[key] = current
                self._dirty = True
            else:
                if (
                    current.title != normalized_title
                    or current.detail != normalized_detail
                    or current.severity != normalized_severity
                ):
                    current.title = normalized_title
                    current.detail = normalized_detail
                    current.severity = normalized_severity
                    self._dirty = True
            should_send = current.last_sent_at is None or (
                now - current.last_sent_at
            ).total_seconds() >= self.repeat_seconds
            if should_send:
                return AlertTransition(
                    kind="incident",
                    key=key,
                    title=current.title,
                    detail=current.detail,
                    severity=current.severity,
                    first_seen_at=current.first_seen_at,
                )
            return None

        if current is None:
            return None
        if current.last_sent_at is None:
            del self._states[key]
            self._dirty = True
            return None
        return AlertTransition(
            kind="recovery",
            key=key,
            title=current.title,
            detail="Состояние снова соответствует норме.",
            severity=current.severity,
            first_seen_at=current.first_seen_at,
        )

    @staticmethod
    def _format_transitions(transitions: list[AlertTransition], now: datetime) -> str:
        incidents = [item for item in transitions if item.kind == "incident"]
        recoveries = [item for item in transitions if item.kind == "recovery"]
        lines: list[str] = []
        if incidents:
            critical_count = sum(item.severity == "critical" for item in incidents)
            header_icon = "🚨" if critical_count else "⚠️"
            lines.append(
                f"{header_icon} <b>LikeBot: обнаружено проблем — {len(incidents)}</b>"
            )
            for item in incidents[:5]:
                icon = "🚨" if item.severity == "critical" else "⚠️"
                age_minutes = max(0, int((now - item.first_seen_at).total_seconds() // 60))
                lines.extend(
                    [
                        "",
                        f"{icon} <b>{html.escape(item.title)}</b>",
                        html.escape(item.detail),
                        (
                            f"Ключ: <code>{html.escape(item.key)}</code> · "
                            f"длительность: {age_minutes} мин."
                        ),
                    ]
                )
            if len(incidents) > 5:
                lines.append(f"\nЕщё проблем: <b>{len(incidents) - 5}</b>")

        if recoveries:
            if lines:
                lines.append("\n──────────")
            lines.append(f"✅ <b>Восстановлено состояний — {len(recoveries)}</b>")
            for item in recoveries[:5]:
                duration_minutes = max(
                    0, int((now - item.first_seen_at).total_seconds() // 60)
                )
                lines.append(
                    f"• {html.escape(item.title)} · длилось {duration_minutes} мин."
                )
            if len(recoveries) > 5:
                lines.append(f"• и ещё {len(recoveries) - 5}")

        lines.append("\nОткройте <b>📊 Статистика → 🩺 Состояние LikeBot</b>.")
        return "\n".join(lines)

    async def _flush_transitions(
        self, transitions: list[AlertTransition], now: datetime
    ) -> bool:
        if not transitions:
            await self._persist_if_dirty()
            return True

        # Telegram messages are bounded. Deliver critical incidents first and mark
        # only the transitions actually shown. Remaining incidents are therefore
        # sent on the next monitor iteration instead of being hidden for a cooldown.
        incidents = sorted(
            (item for item in transitions if item.kind == "incident"),
            key=lambda item: (item.severity != "critical", item.first_seen_at, item.key),
        )
        recoveries = sorted(
            (item for item in transitions if item.kind == "recovery"),
            key=lambda item: (item.first_seen_at, item.key),
        )
        delivery_batch = [*incidents[:5], *recoveries[:5]]
        delivered = await self._send(self._format_transitions(delivery_batch, now))
        if delivered:
            for item in delivery_batch:
                if item.kind == "incident":
                    state = self._states.get(item.key)
                    if state is not None:
                        state.last_sent_at = now
                        self._dirty = True
                elif item.key in self._states:
                    del self._states[item.key]
                    self._dirty = True
        await self._persist_if_dirty()
        return delivered

    async def evaluate_once(self) -> None:
        """Read one consistent diagnostic snapshot and send deduplicated alerts."""

        if not self.enabled:
            return
        async with self._lock:
            await self._ensure_loaded()
            now = self.clock()
            transitions: list[AlertTransition] = []

            try:
                stats = await self.db.stats()
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001
                transition = self._transition(
                    key="database:unavailable",
                    active=True,
                    title="База данных недоступна",
                    detail=f"{type(exc).__name__}: {_safe_text(exc, 300)}",
                    severity="critical",
                    now=now,
                )
                if transition:
                    transitions.append(transition)
                await self._flush_transitions(transitions, now)
                return

            startup_grace = max(
                0, int(getattr(self.settings, "alert_startup_grace_seconds", 180))
            )
            operational_checks = (now - self.started_at).total_seconds() >= startup_grace

            # A task-exit alert may have been persisted but not delivered while the
            # process was already failing. Deliver that incident after restart before
            # resolving it. Otherwise a temporary Bot API outage could hide the only
            # evidence of a crashed worker.
            unsent_task_exit_keys: set[str] = set()
            for key, state in list(self._states.items()):
                if not key.startswith("process:task_exit:") or state.last_sent_at is not None:
                    continue
                unsent_task_exit_keys.add(key)
                transition = self._transition(
                    key=key,
                    active=True,
                    title=state.title,
                    detail=state.detail,
                    severity=state.severity,
                    now=now,
                )
                if transition:
                    transitions.append(transition)

            if operational_checks:
                for key in list(self._states):
                    if (
                        key.startswith("process:task_exit:")
                        and key not in unsent_task_exit_keys
                    ):
                        transition = self._transition(
                            key=key,
                            active=False,
                            title="Критическая задача остановилась",
                            detail="",
                            severity="critical",
                            now=now,
                        )
                        if transition:
                            transitions.append(transition)

                warning_after = max(
                    60, int(getattr(self.settings, "alert_worker_warning_seconds", 300))
                )
                worker_rows = self.jobs.worker_health_snapshot(now=now)
                worker_keys: set[str] = set()
                for worker in worker_rows:
                    name = str(worker.get("name", "unknown"))
                    key = f"worker:{name}"
                    worker_keys.add(key)
                    status = str(worker.get("status", "starting"))
                    warning_since_at = worker.get("warning_since_at")
                    if not isinstance(warning_since_at, datetime):
                        # Compatibility with pre-v1.0.25 snapshots and light tests.
                        warning_since_at = worker.get("last_error_at")
                    warning_persistent = bool(
                        status == "warning"
                        and isinstance(warning_since_at, datetime)
                        and (now - warning_since_at).total_seconds() >= warning_after
                    )
                    active = status in {"starting", "stale", "blocked"} or warning_persistent
                    detail_parts = [f"Статус: {status}"]
                    if worker.get("age_seconds") is not None:
                        detail_parts.append(
                            f"последний heartbeat {int(worker['age_seconds'])} сек. назад"
                        )
                    if int(worker.get("stuck_running_tasks") or 0):
                        detail_parts.append(
                            f"зависших задач: {int(worker['stuck_running_tasks'])}"
                        )
                    if worker.get("last_error"):
                        detail_parts.append(
                            f"ошибка: {_safe_text(worker['last_error'], 250)}"
                        )
                    transition = self._transition(
                        key=key,
                        active=active,
                        title=f"Worker: {worker.get('label') or name}",
                        detail=" · ".join(detail_parts),
                        severity=(
                            "critical"
                            if status in {"starting", "stale", "blocked"}
                            else "warning"
                        ),
                        now=now,
                    )
                    if transition:
                        transitions.append(transition)

                for key in list(self._states):
                    if key.startswith("worker:") and key not in worker_keys:
                        transition = self._transition(
                            key=key,
                            active=False,
                            title="Worker",
                            detail="",
                            severity="critical",
                            now=now,
                        )
                        if transition:
                            transitions.append(transition)

                backlog_threshold = max(
                    1, int(getattr(self.settings, "alert_queue_backlog_threshold", 25))
                )
                for kind, label in (
                    ("reaction", "Очередь реакций"),
                    ("view", "Очередь просмотров"),
                    ("join", "Очередь подписок/выходов"),
                ):
                    backlog = int(stats.get(f"{kind}_backlog", 0))
                    stuck = int(stats.get(f"{kind}_stuck_running", 0))
                    active = stuck > 0 or backlog >= backlog_threshold
                    transition = self._transition(
                        key=f"queue:{kind}",
                        active=active,
                        title=label,
                        detail=(
                            f"Просрочено более 5 минут: {backlog} · "
                            f"зависло в running: {stuck}"
                        ),
                        severity="critical" if stuck > 0 else "warning",
                        now=now,
                    )
                    if transition:
                        transitions.append(transition)

                problem_accounts = int(stats.get("problem_accounts", 0))
                transition = self._transition(
                    key="accounts:quarantined",
                    active=problem_accounts > 0,
                    title="Аккаунты требуют повторной авторизации",
                    detail=f"В проблемных аккаунтах: {problem_accounts}",
                    severity="critical",
                    now=now,
                )
                if transition:
                    transitions.append(transition)

                flood_accounts = int(stats.get("flood_accounts", 0))
                active_accounts = int(stats.get("active_accounts", 0))
                flood_threshold = max(
                    1, int(getattr(self.settings, "alert_flood_account_threshold", 5))
                )
                all_active_flooded = active_accounts > 0 and flood_accounts >= active_accounts
                transition = self._transition(
                    key="accounts:flood_pressure",
                    active=flood_accounts >= flood_threshold or all_active_flooded,
                    title="Высокая нагрузка FloodWait",
                    detail=(
                        f"Под FloodWait: {flood_accounts} из {active_accounts} активных аккаунтов"
                    ),
                    severity="critical" if all_active_flooded else "warning",
                    now=now,
                )
                if transition:
                    transitions.append(transition)

                persistent_errors = int(stats.get("account_errors", 0)) + int(
                    stats.get("target_errors", 0)
                )
                persistent_threshold = max(
                    1, int(getattr(self.settings, "alert_persisted_error_threshold", 5))
                )
                transition = self._transition(
                    key="errors:persisted",
                    active=persistent_errors >= persistent_threshold,
                    title="Накопились сохранённые ошибки",
                    detail=(
                        f"Ошибки аккаунтов: {int(stats.get('account_errors', 0))} · "
                        f"ошибки каналов/групп: {int(stats.get('target_errors', 0))}"
                    ),
                    severity="warning",
                    now=now,
                )
                if transition:
                    transitions.append(transition)

                failure_window = max(
                    1, int(getattr(self.settings, "alert_failure_window_minutes", 15))
                )
                failure_threshold = max(
                    1, int(getattr(self.settings, "alert_failure_threshold", 10))
                )
                try:
                    recent_failures = await self.db.recent_failure_counts(
                        failure_window, now=now
                    )
                except asyncio.CancelledError:
                    raise
                except Exception as exc:  # noqa: BLE001
                    transition = self._transition(
                        key="database:unavailable",
                        active=True,
                        title="База данных недоступна",
                        detail=(
                            "Не удалось прочитать свежие ошибки очередей: "
                            f"{type(exc).__name__}: {_safe_text(exc, 300)}"
                        ),
                        severity="critical",
                        now=now,
                    )
                    if transition:
                        transitions.append(transition)
                    await self._flush_transitions(transitions, now)
                    return
                total_failures = int(recent_failures.get("total", 0))
                transition = self._transition(
                    key="errors:recent_failure_spike",
                    active=total_failures >= failure_threshold,
                    title="Резкий рост неуспешных заданий",
                    detail=(
                        f"За {failure_window} мин.: всего {total_failures} · "
                        f"подписки {int(recent_failures.get('join', 0))} · "
                        f"реакции {int(recent_failures.get('reaction', 0))} · "
                        f"просмотры {int(recent_failures.get('view', 0))}"
                    ),
                    severity="critical" if total_failures >= failure_threshold * 2 else "warning",
                    now=now,
                )
                if transition:
                    transitions.append(transition)

            transition = self._transition(
                key="database:unavailable",
                active=False,
                title="База данных недоступна",
                detail="",
                severity="critical",
                now=now,
            )
            if transition:
                transitions.append(transition)

            await self._flush_transitions(transitions, now)

    async def monitor_loop(self) -> None:
        while True:
            await self.evaluate_once()
            await asyncio.sleep(self.check_interval_seconds)

    async def notify_problem_account(
        self, account: object, context: str, reason: str
    ) -> None:
        """Preserve the existing first-quarantine notification and seed dedup state."""

        async with self._lock:
            await self._ensure_loaded()
            now = self.clock()
            try:
                stats = await self.db.stats()
            except Exception:  # noqa: BLE001
                stats = {"active_accounts": 0, "accounts": 0, "problem_accounts": 1}

            email_login = getattr(account, "email_login", None)
            email_line = (
                f"📧 Почта: <code>{html.escape(str(email_login))}</code>\n"
                if email_login
                else "📧 Почта: <b>не указана</b>\n"
            )
            display_name = display_account_name(
                str(getattr(account, "display_name", "")),
                getattr(account, "username", None),
            )
            text = (
                "⚠️ <b>Аккаунт перенесён в проблемные</b>\n\n"
                f"👤 Имя: <b>{html.escape(display_name)}</b>\n"
                f"📱 Телефон: <code>{html.escape(str(getattr(account, 'phone', '—')))}</code>\n"
                f"{email_line}"
                "🆔 Telegram ID: "
                f"<code>{html.escape(str(getattr(account, 'telegram_user_id', '—')))}</code>\n"
                f"❌ Причина: {html.escape(_safe_text(reason, 400))}\n"
                f"🔎 Обнаружено при: <code>{html.escape(_safe_text(context, 120))}</code>\n\n"
                f"Активных аккаунтов: <b>{int(stats.get('active_accounts', 0))}</b> "
                f"из {int(stats.get('accounts', 0))}\n"
                "Откройте: <b>Мои аккаунты → Проблемные аккаунты</b>."
            )
            delivered = await self._send(text)
            problem_count = max(1, int(stats.get("problem_accounts", 1)))
            state = self._states.get("accounts:quarantined")
            if state is None:
                state = IncidentState(
                    first_seen_at=now,
                    last_sent_at=now if delivered else None,
                    title="Аккаунты требуют повторной авторизации",
                    detail=f"В проблемных аккаунтах: {problem_count}",
                    severity="critical",
                )
                self._states["accounts:quarantined"] = state
            else:
                state.detail = f"В проблемных аккаунтах: {problem_count}"
                if delivered:
                    state.last_sent_at = now
            self._dirty = True
            await self._persist_if_dirty()

    async def notify_external_incident(
        self,
        key: str,
        *,
        title: str,
        detail: str,
        severity: str = "warning",
    ) -> bool:
        """Publish a persisted incident owned by another operational service."""

        if not self.enabled:
            return True
        safe_key = _safe_text(key, 160)
        if not safe_key.startswith("recovery:"):
            safe_key = f"recovery:{safe_key}"
        async with self._lock:
            await self._ensure_loaded()
            now = self.clock()
            transition = self._transition(
                key=safe_key,
                active=True,
                title=title,
                detail=detail,
                severity=severity,
                now=now,
            )
            return await self._flush_transitions(
                [transition] if transition else [], now
            )

    async def resolve_external_incident(
        self, key: str, *, title: str | None = None
    ) -> bool:
        """Resolve one persisted external incident after verified recovery."""

        if not self.enabled:
            return True
        safe_key = _safe_text(key, 160)
        if not safe_key.startswith("recovery:"):
            safe_key = f"recovery:{safe_key}"
        async with self._lock:
            await self._ensure_loaded()
            now = self.clock()
            current = self._states.get(safe_key)
            transition = self._transition(
                key=safe_key,
                active=False,
                title=title or (current.title if current else "Автовосстановление"),
                detail="",
                severity=(current.severity if current else "warning"),
                now=now,
            )
            return await self._flush_transitions(
                [transition] if transition else [], now
            )

    async def notify_critical_task_exit(
        self, task_name: str, error: BaseException | None
    ) -> bool:
        """Best-effort deduplicated message before Railway restarts the process."""

        if not self.enabled:
            return True
        async with self._lock:
            await self._ensure_loaded()
            now = self.clock()
            detail = (
                f"{type(error).__name__}: {_safe_text(error, 300)}"
                if error is not None
                else "Задача завершилась без ожидаемого исключения."
            )
            safe_task_name = _safe_text(task_name, 120)
            transition = self._transition(
                key=f"process:task_exit:{safe_task_name}",
                active=True,
                title=f"Остановлена критическая задача: {safe_task_name}",
                detail=(
                    f"Причина: {detail} · процесс будет завершён для перезапуска Railway"
                ),
                severity="critical",
                now=now,
            )
            return await self._flush_transitions(
                [transition] if transition else [], now
            )
