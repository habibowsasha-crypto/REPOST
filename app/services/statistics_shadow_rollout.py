"""Fail-closed readiness contract for the controlled statistics shadow rollout.

The module never changes a feature flag, never writes to the database and never
calls BingX.  It only validates that the independently configured flags match an
explicit rollout stage and emits a secret-free configuration fingerprint.  The
fingerprint lets an operator verify that trading-critical settings stayed
unchanged across Railway redeploys while statistics are enabled gradually.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass
from typing import Any, Mapping


SHADOW_STAGES = (
    "off",
    "period_linkage",
    "financial",
    "funding",
    "full_shadow",
    "reset_test",
)

SHADOW_FLAG_NAMES = (
    "SIGNAL_ANALYTICS_ENABLED",
    "SIGNAL_ANALYTICS_INGRESS_ENABLED",
    "SIGNAL_ANALYTICS_TRACKING_ENABLED",
    "SIGNAL_ANALYTICS_RECOVERY_ENABLED",
    "SIGNAL_ANALYTICS_REPORTS_ENABLED",
    "SIGNAL_ANALYTICS_SIMULATION_ENABLED",
    "STATISTICS_PERIODS_ENABLED",
    "STATISTICS_EXECUTION_RESULTS_ENABLED",
    "STATISTICS_FUNDING_ENABLED",
    "STATISTICS_LINKAGE_ENABLED",
    "STATISTICS_RECOVERY_ENABLED",
    "STATISTICS_QUALITY_ENABLED",
    "STATS_RESET_ENABLED",
    "STATS_V2_REPORTS_ENABLED",
    "FINANCIAL_RECONCILIATION_ENABLED",
)

# Exact profiles are intentional.  A stage mismatch is safer than silently
# enabling a partial financial pipeline that cannot prove its own result.
_STAGE_ENABLED_FLAGS: Mapping[str, frozenset[str]] = {
    "off": frozenset(),
    "period_linkage": frozenset(
        {
            "SIGNAL_ANALYTICS_ENABLED",
            "SIGNAL_ANALYTICS_INGRESS_ENABLED",
            "SIGNAL_ANALYTICS_TRACKING_ENABLED",
            "SIGNAL_ANALYTICS_RECOVERY_ENABLED",
            "SIGNAL_ANALYTICS_REPORTS_ENABLED",
            "STATISTICS_PERIODS_ENABLED",
            "STATISTICS_LINKAGE_ENABLED",
            "STATISTICS_RECOVERY_ENABLED",
            "STATS_V2_REPORTS_ENABLED",
        }
    ),
    "financial": frozenset(
        {
            "SIGNAL_ANALYTICS_ENABLED",
            "SIGNAL_ANALYTICS_INGRESS_ENABLED",
            "SIGNAL_ANALYTICS_TRACKING_ENABLED",
            "SIGNAL_ANALYTICS_RECOVERY_ENABLED",
            "SIGNAL_ANALYTICS_REPORTS_ENABLED",
            "STATISTICS_PERIODS_ENABLED",
            "STATISTICS_EXECUTION_RESULTS_ENABLED",
            "STATISTICS_LINKAGE_ENABLED",
            "STATISTICS_RECOVERY_ENABLED",
            "STATS_V2_REPORTS_ENABLED",
            "FINANCIAL_RECONCILIATION_ENABLED",
        }
    ),
    "funding": frozenset(
        {
            "SIGNAL_ANALYTICS_ENABLED",
            "SIGNAL_ANALYTICS_INGRESS_ENABLED",
            "SIGNAL_ANALYTICS_TRACKING_ENABLED",
            "SIGNAL_ANALYTICS_RECOVERY_ENABLED",
            "SIGNAL_ANALYTICS_REPORTS_ENABLED",
            "STATISTICS_PERIODS_ENABLED",
            "STATISTICS_EXECUTION_RESULTS_ENABLED",
            "STATISTICS_FUNDING_ENABLED",
            "STATISTICS_LINKAGE_ENABLED",
            "STATISTICS_RECOVERY_ENABLED",
            "STATS_V2_REPORTS_ENABLED",
            "FINANCIAL_RECONCILIATION_ENABLED",
        }
    ),
    "full_shadow": frozenset(
        {
            "SIGNAL_ANALYTICS_ENABLED",
            "SIGNAL_ANALYTICS_INGRESS_ENABLED",
            "SIGNAL_ANALYTICS_TRACKING_ENABLED",
            "SIGNAL_ANALYTICS_RECOVERY_ENABLED",
            "SIGNAL_ANALYTICS_REPORTS_ENABLED",
            "STATISTICS_PERIODS_ENABLED",
            "STATISTICS_EXECUTION_RESULTS_ENABLED",
            "STATISTICS_FUNDING_ENABLED",
            "STATISTICS_LINKAGE_ENABLED",
            "STATISTICS_RECOVERY_ENABLED",
            "STATISTICS_QUALITY_ENABLED",
            "STATS_V2_REPORTS_ENABLED",
            "FINANCIAL_RECONCILIATION_ENABLED",
        }
    ),
    "reset_test": frozenset(
        {
            "SIGNAL_ANALYTICS_ENABLED",
            "SIGNAL_ANALYTICS_INGRESS_ENABLED",
            "SIGNAL_ANALYTICS_TRACKING_ENABLED",
            "SIGNAL_ANALYTICS_RECOVERY_ENABLED",
            "SIGNAL_ANALYTICS_REPORTS_ENABLED",
            "STATISTICS_PERIODS_ENABLED",
            "STATISTICS_EXECUTION_RESULTS_ENABLED",
            "STATISTICS_FUNDING_ENABLED",
            "STATISTICS_LINKAGE_ENABLED",
            "STATISTICS_RECOVERY_ENABLED",
            "STATISTICS_QUALITY_ENABLED",
            "STATS_RESET_ENABLED",
            "STATS_V2_REPORTS_ENABLED",
            "FINANCIAL_RECONCILIATION_ENABLED",
        }
    ),
}

_TRADING_FINGERPRINT_FIELDS = (
    "BINGX_VST",
    "BINGX_ENTRY_ORDER_TYPE",
    "MARGIN_MODE",
    "DEFAULT_RISK_PERCENT",
    "DEFAULT_DAILY_RISK_LIMIT_PERCENT",
    "DEFAULT_MAX_OPEN_TRADES",
    "DEFAULT_MAX_PORTFOLIO_RISK",
    "DEFAULT_EXCLUDE_BE_TRADES_FROM_RISK",
    "DEFAULT_TP_LIMIT",
    "DEFAULT_TP_MODE",
    "DEFAULT_BE_AFTER_TP1",
    "DEFAULT_BE_TRIGGER_TP_INDEX",
    "BE_FEE_BUFFER_PERCENT",
    "BE_PLUS_PERCENT",
    "TRADE_EXECUTION_WORKERS",
    "MARKET_TP_BACKGROUND_ENABLED",
    "MARKET_TP_BACKGROUND_RECOVERY_ENABLED",
    "EVENT_DRIVEN_MONITOR_ENABLED",
    "MARKET_PRICE_POLL_INTERVAL_SEC",
    "MARKET_PRICE_STALE_SEC",
    "MONITOR_ACTIVE_INTERVAL_SEC",
    "MONITOR_WORKERS",
    "MONITOR_CRITICAL_INTERVAL_SEC",
    "MONITOR_FULL_RECONCILE_INTERVAL_SEC",
    "BINGX_GLOBAL_MAX_IN_FLIGHT",
    "BINGX_GLOBAL_REQUESTS_PER_SECOND",
    "BINGX_GLOBAL_BURST_LIMIT",
    "BINGX_TRADE_ORDER_REQUESTS_PER_SECOND",
    "BINGX_TRADE_ORDER_BURST_LIMIT",
    "BINGX_REQUEST_QUEUE_TIMEOUT_SECONDS",
)


@dataclass(frozen=True, slots=True)
class ShadowReadinessItem:
    severity: str
    code: str
    message: str


@dataclass(frozen=True, slots=True)
class ShadowReadinessReport:
    expected_stage: str
    actual_stage: str
    ready: bool
    database_backend: str
    require_postgres: bool
    trading_config_fingerprint: str
    enabled_flags: tuple[str, ...]
    missing_required_flags: tuple[str, ...]
    unexpected_enabled_flags: tuple[str, ...]
    items: tuple[ShadowReadinessItem, ...]

    def as_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["items"] = [asdict(item) for item in self.items]
        return payload


def _normalized_stage(value: Any) -> str:
    stage = str(value or "off").strip().lower()
    return stage if stage in SHADOW_STAGES else "invalid"


def _enabled_flags(settings: Any) -> frozenset[str]:
    return frozenset(
        name for name in SHADOW_FLAG_NAMES if bool(getattr(settings, name, False))
    )


def derive_actual_shadow_stage(settings: Any) -> str:
    enabled = _enabled_flags(settings)
    matches = [stage for stage, flags in _STAGE_ENABLED_FLAGS.items() if flags == enabled]
    return matches[0] if len(matches) == 1 else "invalid"


def trading_config_fingerprint(settings: Any) -> str:
    """Return a short, secret-free digest of trading-critical configuration."""

    payload = {
        key: getattr(settings, key, None)
        for key in _TRADING_FINGERPRINT_FIELDS
    }
    canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()[:20]


def database_backend_from_url(value: Any) -> str:
    """Classify the configured backend exactly like the runtime DB layer.

    A non-empty but unsupported URL must never be reported as PostgreSQL.  The
    runtime accepts only ``postgres://`` and ``postgresql://``; every other
    non-empty value is invalid rather than a safe SQLite fallback.
    """

    raw = str(value or "").strip()
    if not raw:
        return "sqlite"
    if raw.startswith(("postgres://", "postgresql://")):
        return "postgres"
    return "invalid"


def _normalized_backend(value: Any) -> str:
    backend = str(value or "").strip().lower()
    return backend if backend in {"postgres", "sqlite"} else "invalid"


def build_statistics_shadow_readiness(
    settings: Any,
    *,
    database_backend: str | None = None,
) -> ShadowReadinessReport:
    """Validate one explicit shadow stage without mutating runtime state."""

    expected = _normalized_stage(
        getattr(settings, "STATISTICS_SHADOW_EXPECTED_STAGE", "off")
    )
    backend = (
        _normalized_backend(database_backend)
        if database_backend is not None
        else database_backend_from_url(getattr(settings, "DATABASE_URL", ""))
    )
    require_postgres = bool(
        getattr(settings, "STATISTICS_SHADOW_REQUIRE_POSTGRES", True)
    )
    enabled = _enabled_flags(settings)
    actual = derive_actual_shadow_stage(settings)
    required = _STAGE_ENABLED_FLAGS.get(expected, frozenset())
    missing = tuple(sorted(required - enabled))
    unexpected = tuple(sorted(enabled - required))
    items: list[ShadowReadinessItem] = []

    def add(severity: str, code: str, message: str) -> None:
        items.append(ShadowReadinessItem(severity, code, message))

    if expected == "invalid":
        add(
            "CRITICAL",
            "shadow_expected_stage_invalid",
            "STATISTICS_SHADOW_EXPECTED_STAGE должен быть одним из: "
            + ", ".join(SHADOW_STAGES),
        )
    if actual == "invalid":
        add(
            "CRITICAL",
            "shadow_flag_matrix_invalid",
            "Набор statistics feature flags не соответствует ни одному утверждённому shadow-профилю.",
        )
    if expected != "invalid" and missing:
        add(
            "CRITICAL",
            "shadow_required_flags_missing",
            "Для ожидаемого этапа не включены обязательные флаги: " + ", ".join(missing),
        )
    if expected != "invalid" and unexpected:
        add(
            "CRITICAL",
            "shadow_unexpected_flags_enabled",
            "Для ожидаемого этапа включены лишние флаги: " + ", ".join(unexpected),
        )
    if expected != "invalid" and actual != "invalid" and expected != actual:
        add(
            "CRITICAL",
            "shadow_stage_mismatch",
            f"Ожидался shadow-этап {expected}, но фактическая матрица соответствует {actual}.",
        )
    if backend == "invalid":
        add(
            "CRITICAL",
            "shadow_database_backend_invalid",
            "DATABASE_URL непустой, но не использует postgres:// или postgresql://.",
        )
    if expected != "off" and require_postgres and backend != "postgres":
        add(
            "CRITICAL",
            "shadow_postgres_required",
            "Для Railway shadow требуется PostgreSQL через DATABASE_URL; SQLite допускается только в локальном тесте.",
        )
    if expected == "off" and enabled:
        add(
            "CRITICAL",
            "shadow_off_with_enabled_flags",
            "Ожидается этап off, но один или несколько statistics-флагов включены.",
        )
    if expected not in {"off", "reset_test"} and bool(
        getattr(settings, "STATS_RESET_ENABLED", False)
    ):
        add(
            "CRITICAL",
            "stats_reset_outside_reset_test",
            "STATS_RESET_ENABLED можно включать только на отдельном этапе reset_test.",
        )
    if expected != "off" and not bool(
        getattr(settings, "EVENT_DRIVEN_MONITOR_ENABLED", True)
    ):
        add(
            "CRITICAL",
            "shadow_tracking_without_event_monitor",
            "Shadow tracking требует EVENT_DRIVEN_MONITOR_ENABLED=true.",
        )
    if expected == "off":
        add(
            "INFO",
            "shadow_deploy_flags_off",
            "Все statistics/financial shadow-флаги выключены; безопасный первый deploy допускается.",
        )
    elif not any(item.severity == "CRITICAL" for item in items):
        add(
            "INFO",
            "shadow_stage_ready",
            f"Матрица флагов соответствует контролируемому этапу {expected}.",
        )

    ready = not any(item.severity == "CRITICAL" for item in items)
    return ShadowReadinessReport(
        expected_stage=expected,
        actual_stage=actual,
        ready=ready,
        database_backend=backend,
        require_postgres=require_postgres,
        trading_config_fingerprint=trading_config_fingerprint(settings),
        enabled_flags=tuple(sorted(enabled)),
        missing_required_flags=missing,
        unexpected_enabled_flags=unexpected,
        items=tuple(items),
    )


def expected_flags_for_stage(stage: str) -> tuple[str, ...]:
    """Expose the immutable profile for docs/tests without returning mutability."""

    normalized = _normalized_stage(stage)
    if normalized == "invalid":
        raise ValueError("unknown statistics shadow stage")
    return tuple(sorted(_STAGE_ENABLED_FLAGS[normalized]))
