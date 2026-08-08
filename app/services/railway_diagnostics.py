from __future__ import annotations

from dataclasses import dataclass
import logging
import os
from typing import Mapping

from app import __version__
from app.services.statistics_shadow_rollout import (
    build_statistics_shadow_readiness,
    database_backend_from_url,
)


_SECRET_MARKERS = ("TOKEN", "SECRET", "KEY", "PASSWORD", "DATABASE_URL", "ENCRYPTION")


@dataclass(frozen=True)
class RailwayDiagnosticItem:
    severity: str
    code: str
    message: str

    def as_dict(self) -> dict[str, str]:
        return {"severity": self.severity, "code": self.code, "message": self.message}


def _configured(value: object) -> bool:
    return bool(str(value or "").strip())


def _has_env(name: str, env: Mapping[str, str] | None = None) -> bool:
    source = os.environ if env is None else env
    return _configured(source.get(name))


def _safe_env_presence(env: Mapping[str, str] | None = None) -> dict[str, bool]:
    """Return only presence flags for Railway variables, never secret values."""
    source = os.environ if env is None else env
    important = [
        "BOT_TOKEN",
        "ADMIN_IDS",
        "ADMIN_ONLY_MODE",
        "DATABASE_URL",
        "DATABASE_PATH",
        "ENCRYPTION_KEY",
        "DEFAULT_EXCHANGE",
        "BINGX_VST",
        "BINGX_PROTECTIVE_CLIENT_ORDER_ID_ENABLED",
        "BINGX_LIVE_TINY_TEST_CONFIRMED",
        "VIP_ALLOWED_SOURCE_CHAT_IDS",
        "VIP_ALLOWED_SENDER_CHAT_TITLES",
        "RAILWAY_EXPECTED_APP_VERSION",
        "STATISTICS_SHADOW_EXPECTED_STAGE",
        "STATISTICS_SHADOW_REQUIRE_POSTGRES",
        "STATISTICS_SHADOW_DB_DIAGNOSTICS_ENABLED",
        "STATISTICS_SHADOW_STRICT_STARTUP",
        "MARKET_EVENT_ROLLOUT_STAGE",
        "MARKET_EVENT_MIGRATION_ENABLED",
        "MARKET_EVENT_MIGRATION_TARGET_GROUP_ID",
        "MARKET_EVENT_MANUAL_RESOLUTION_ENABLED",
    ]
    return {name: _configured(source.get(name)) for name in important}


def build_railway_diagnostics(settings, *, env: Mapping[str, str] | None = None) -> dict:
    """Build a sanitized Railway deployment report.

    The report is intentionally non-secret: it contains only booleans, counts,
    normalized modes and warning text. It is safe for startup logs and support
    screenshots.
    """

    env = os.environ if env is None else env
    items: list[RailwayDiagnosticItem] = []

    def add(severity: str, code: str, message: str) -> None:
        items.append(RailwayDiagnosticItem(severity=severity, code=code, message=message))

    expected_version = str(getattr(settings, "RAILWAY_EXPECTED_APP_VERSION", "") or "").strip()
    expected_version_mode = "auto" if expected_version.lower() == "auto" else ("manual" if expected_version else "disabled")
    if expected_version and expected_version_mode == "manual" and expected_version != __version__:
        add(
            "CRITICAL",
            "version_mismatch",
            f"RAILWAY_EXPECTED_APP_VERSION={expected_version}, но запущен код v{__version__}.",
        )

    if not _configured(getattr(settings, "BOT_TOKEN", "")):
        add("CRITICAL", "bot_token_missing", "BOT_TOKEN не задан.")
    if not _configured(getattr(settings, "ENCRYPTION_KEY", "")):
        add("CRITICAL", "encryption_key_missing", "ENCRYPTION_KEY не задан.")
    if not getattr(settings, "admin_ids", []):
        add("WARNING", "admin_ids_missing", "ADMIN_IDS пустой - админские команды и уведомления будут недоступны.")
    admin_only_mode = bool(getattr(settings, "ADMIN_ONLY_MODE", False))
    if admin_only_mode:
        add(
            "INFO",
            "admin_only_mode_enabled",
            "ADMIN_ONLY_MODE=true: новые сделки, интерактивный Telegram и личные уведомления разрешены только ADMIN_IDS; старые не-админские live executions не бросаются и продолжают защитный lifecycle до terminal.",
        )

    db_url = str(getattr(settings, "DATABASE_URL", "") or "").strip()
    db_path = str(getattr(settings, "DATABASE_PATH", "") or "").strip()
    db_mode = database_backend_from_url(db_url)
    if db_mode == "invalid":
        add(
            "CRITICAL",
            "database_url_scheme_invalid",
            "DATABASE_URL должен начинаться с postgres:// или postgresql://.",
        )
    if not db_url:
        if not db_path:
            add("CRITICAL", "database_missing", "Не задан DATABASE_URL и пустой DATABASE_PATH.")
        elif not db_path.startswith("/data/"):
            add(
                "WARNING",
                "sqlite_without_railway_volume",
                "DATABASE_URL пустой, а DATABASE_PATH не в /data - после redeploy SQLite может потеряться.",
            )

    if getattr(settings, "safe_default_exchange", "bingx") != "bingx":
        add("CRITICAL", "exchange_not_bingx", "Эта сборка должна работать только как BingX-only.")

    if getattr(settings, "BINGX_VST", False):
        add("WARNING", "bingx_vst_enabled", "BINGX_VST=true - это simulated/VST окружение, не production.")

    live_tiny_confirmed = bool(getattr(settings, "BINGX_LIVE_TINY_TEST_CONFIRMED", False))
    protective_client_ids = bool(getattr(settings, "BINGX_PROTECTIVE_CLIENT_ORDER_ID_ENABLED", False))
    if protective_client_ids and not live_tiny_confirmed:
        add(
            "WARNING",
            "protective_client_ids_without_live_ack",
            "BINGX_PROTECTIVE_CLIENT_ORDER_ID_ENABLED=true, но BINGX_LIVE_TINY_TEST_CONFIRMED=false. Для STOP/TP лучше оставить false до tiny-test.",
        )
    if not live_tiny_confirmed:
        add(
            "INFO",
            "live_tiny_test_required",
            "Production confirmed не заявлять: controlled BingX tiny-test ещё не отмечен как пройденный.",
        )

    if getattr(settings, "VIP_REQUIRE_TRUSTED_SOURCE", True) and not getattr(settings, "allowed_source_chat_ids", []):
        add(
            "CRITICAL",
            "trusted_source_ids_missing",
            "VIP_REQUIRE_TRUSTED_SOURCE=true, но VIP_ALLOWED_SOURCE_CHAT_IDS пустой - групповые VIP-сигналы будут fail-closed.",
        )
    if getattr(settings, "VIP_ONLY_GROUP_SIGNALS", True) and getattr(settings, "VIP_EXECUTE_PRIVATE_SIGNALS", False):
        add(
            "WARNING",
            "private_signal_flag_shadowed",
            "VIP_EXECUTE_PRIVATE_SIGNALS=true, но VIP_ONLY_GROUP_SIGNALS=true - приватные сигналы не должны исполняться.",
        )
    if getattr(settings, "VIP_REQUIRE_SENDER_CHAT", True) and not getattr(settings, "allowed_sender_chat_titles", []):
        add(
            "WARNING",
            "sender_chat_titles_missing",
            "VIP_REQUIRE_SENDER_CHAT=true, но VIP_ALLOWED_SENDER_CHAT_TITLES пустой.",
        )

    if not getattr(settings, "EVENT_DRIVEN_MONITOR_ENABLED", True):
        add("WARNING", "event_monitor_disabled", "EVENT_DRIVEN_MONITOR_ENABLED=false - защита будет менее быстрой.")

    if float(getattr(settings, "MARKET_PRICE_POLL_INTERVAL_SEC", 1.0)) > float(getattr(settings, "MARKET_PRICE_STALE_SEC", 8)):
        add(
            "WARNING",
            "price_poll_slower_than_stale",
            "MARKET_PRICE_POLL_INTERVAL_SEC больше MARKET_PRICE_STALE_SEC - price cache может часто считаться stale.",
        )

    if str(getattr(settings, "LOG_LEVEL", "INFO")).upper() == "DEBUG":
        add("WARNING", "debug_log_level", "LOG_LEVEL=DEBUG - использовать только для короткой диагностики.")

    market_event_stage = str(getattr(settings, "MARKET_EVENT_ROLLOUT_STAGE", "off") or "off").lower()
    if market_event_stage in {"group_1541", "global"} and db_mode != "postgres":
        add(
            "WARNING",
            "market_event_rollout_without_postgres",
            "Боевой market-event rollout запущен не на PostgreSQL; для Railway production рекомендуется Postgres.",
        )
    if market_event_stage == "global":
        add(
            "WARNING",
            "market_event_global_rollout_active",
            "MARKET_EVENT_ROLLOUT_STAGE=global: проверьте live-метрики очереди, PostgreSQL и BingX перед закрытием периода статистики.",
        )

    shadow = build_statistics_shadow_readiness(settings, database_backend=db_mode)
    for item in shadow.items:
        add(item.severity, item.code, item.message)

    env_presence = _safe_env_presence(env)
    critical_count = sum(1 for item in items if item.severity == "CRITICAL")
    warning_count = sum(1 for item in items if item.severity == "WARNING")
    info_count = sum(1 for item in items if item.severity == "INFO")
    status = "critical" if critical_count else ("warning" if warning_count else "ok")

    report = {
        "diagnostic_version": 1,
        "app_version": __version__,
        "expected_app_version": expected_version or "",
        "expected_app_version_mode": expected_version_mode,
        "status": status,
        "critical_count": critical_count,
        "warning_count": warning_count,
        "info_count": info_count,
        "exchange": "bingx",
        "db_mode": db_mode,
        "sqlite_path_on_railway_volume": bool(db_path.startswith("/data/")),
        "admin_count": len(getattr(settings, "admin_ids", [])),
        "admin_only_mode": admin_only_mode,
        "trusted_source_chat_count": len(getattr(settings, "allowed_source_chat_ids", [])),
        "sender_chat_title_count": len(getattr(settings, "allowed_sender_chat_titles", [])),
        "bingx_vst": bool(getattr(settings, "BINGX_VST", False)),
        "protective_client_order_id_enabled": protective_client_ids,
        "live_tiny_test_confirmed": live_tiny_confirmed,
        "entry_order_type": str(getattr(settings, "BINGX_ENTRY_ORDER_TYPE", "")).lower(),
        "margin_mode": str(getattr(settings, "MARGIN_MODE", "")).lower(),
        "risk_percent": float(getattr(settings, "DEFAULT_RISK_PERCENT", 0.0)),
        "max_open_trades": int(getattr(settings, "DEFAULT_MAX_OPEN_TRADES", 0)),
        "portfolio_risk_percent": float(getattr(settings, "DEFAULT_MAX_PORTFOLIO_RISK", 0.0)),
        "event_monitor_enabled": bool(getattr(settings, "EVENT_DRIVEN_MONITOR_ENABLED", True)),
        "market_event_rollout_stage": market_event_stage,
        "market_event_migration_enabled": bool(getattr(settings, "MARKET_EVENT_MIGRATION_ENABLED", False)),
        "market_event_target_group_id": int(getattr(settings, "MARKET_EVENT_MIGRATION_TARGET_GROUP_ID", 0)),
        "market_event_manual_resolution_enabled": bool(getattr(settings, "MARKET_EVENT_MANUAL_RESOLUTION_ENABLED", False)),
        "market_price_poll_sec": float(getattr(settings, "MARKET_PRICE_POLL_INTERVAL_SEC", 0.0)),
        "market_price_stale_sec": int(getattr(settings, "MARKET_PRICE_STALE_SEC", 0)),
        "statistics_shadow": shadow.as_dict(),
        "items": [item.as_dict() for item in items],
        "env_presence": env_presence,
    }
    return report


def _sanitize_text(text: str) -> str:
    """Defense-in-depth: strip any accidental secret-looking key/value fragments."""
    if not text:
        return text
    clean = str(text)
    for marker in _SECRET_MARKERS:
        if marker in clean.upper() and "=" in clean:
            return f"<{marker.lower()} redacted>"
    return clean


def format_railway_diagnostics(report: Mapping) -> str:
    """Format sanitized diagnostics for logs."""
    head = (
        "RAILWAY_DIAGNOSTICS_SUMMARY "
        f"version={report.get('app_version')} "
        f"expected_version={report.get('expected_app_version') or '-'} "
        f"expected_version_mode={report.get('expected_app_version_mode') or '-'} "
        f"status={report.get('status')} "
        f"critical={report.get('critical_count')} "
        f"warnings={report.get('warning_count')} "
        f"db={report.get('db_mode')} "
        f"vst={report.get('bingx_vst')} "
        f"protective_client_ids={report.get('protective_client_order_id_enabled')} "
        f"live_tiny_test_confirmed={report.get('live_tiny_test_confirmed')} "
        f"market_event_rollout={report.get('market_event_rollout_stage')} "
        f"statistics_shadow_expected={(report.get('statistics_shadow') or {}).get('expected_stage', '-')} "
        f"statistics_shadow_actual={(report.get('statistics_shadow') or {}).get('actual_stage', '-')} "
        f"statistics_shadow_ready={(report.get('statistics_shadow') or {}).get('ready', False)} "
        f"trading_config_fingerprint={(report.get('statistics_shadow') or {}).get('trading_config_fingerprint', '-')} "
        f"trusted_sources={report.get('trusted_source_chat_count')} "
        f"admins={report.get('admin_count')} "
        f"admin_only={report.get('admin_only_mode')}"
    )
    lines = [head]
    for item in report.get("items", []):
        lines.append(
            "RAILWAY_DIAGNOSTICS_ITEM "
            f"severity={item.get('severity')} code={item.get('code')} "
            f"message={_sanitize_text(str(item.get('message') or ''))}"
        )
    return "\n".join(lines)


def log_railway_diagnostics(settings, *, logger: logging.Logger | None = None) -> dict:
    """Build, log and optionally fail on critical Railway diagnostics."""
    logger = logger or logging.getLogger(__name__)
    report = build_railway_diagnostics(settings)
    text = format_railway_diagnostics(report)
    if report.get("critical_count"):
        logger.error(text)
    elif report.get("warning_count"):
        logger.warning(text)
    else:
        logger.info(text)

    if bool(getattr(settings, "RAILWAY_DIAGNOSTICS_STRICT_STARTUP", False)) and report.get("critical_count"):
        codes = ", ".join(item.get("code", "unknown") for item in report.get("items", []) if item.get("severity") == "CRITICAL")
        raise RuntimeError(f"Railway diagnostics failed: {codes}")
    shadow_report = report.get("statistics_shadow") or {}
    if bool(getattr(settings, "STATISTICS_SHADOW_STRICT_STARTUP", False)) and not bool(shadow_report.get("ready")):
        codes = ", ".join(
            item.get("code", "unknown")
            for item in shadow_report.get("items", [])
            if item.get("severity") == "CRITICAL"
        )
        raise RuntimeError(f"Statistics shadow readiness failed: {codes}")
    return report
