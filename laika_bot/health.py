from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from .models import Account, utcnow


@dataclass(frozen=True, slots=True)
class AccountHealth:
    """One deterministic operational-readiness assessment for an account.

    The score intentionally uses only persisted facts that LikeBot can prove. It is
    not a Telegram trust/risk score and must never be presented as one.
    """

    score: int
    level: str
    icon: str
    label: str
    reasons: tuple[str, ...]


def evaluate_account_health(
    account: Account,
    *,
    now: datetime | None = None,
) -> AccountHealth:
    """Return a 0-100 operational-readiness score for one saved account."""

    current = now or utcnow()
    status = getattr(account, "status", "ready") or "ready"

    if status == "unauthorized":
        return AccountHealth(
            score=0,
            level="critical",
            icon="🔴",
            label="Требуется авторизация",
            reasons=("Telegram-сессия недействительна",),
        )

    score = 100
    reasons: list[str] = []

    if status != "ready":
        score -= 30
        reasons.append(f"Необычный статус: {status}")

    flood_until = getattr(account, "flood_until", None)
    if flood_until is not None and flood_until > current:
        score -= 35
        reasons.append("Активен FloodWait")

    last_error = getattr(account, "last_error", None)
    expired_flood_error = bool(
        last_error
        and flood_until is not None
        and flood_until <= current
        and str(last_error).casefold().startswith("floodwait")
    )
    if last_error and not expired_flood_error:
        score -= 20
        reasons.append("Есть сохранённая ошибка")

    if not bool(getattr(account, "is_active", False)):
        score = min(score, 60)
        reasons.append("Выключен администратором")

    score = max(0, min(100, score))
    if not bool(getattr(account, "is_active", False)):
        level, icon, label = "paused", "⚫️", "Выключен"
    elif score >= 90:
        level, icon, label = "healthy", "🟢", "Готов к работе"
    elif score >= 70:
        level, icon, label = "attention", "🟡", "Нужно внимание"
    elif score >= 40:
        level, icon, label = "limited", "🟠", "Работа ограничена"
    else:
        level, icon, label = "critical", "🔴", "Критическое состояние"

    if not reasons:
        reasons.append("Ошибок и ограничений не обнаружено")

    return AccountHealth(
        score=score,
        level=level,
        icon=icon,
        label=label,
        reasons=tuple(reasons),
    )
