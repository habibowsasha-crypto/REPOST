from __future__ import annotations

from functools import lru_cache
import math
from typing import List

from pydantic import field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


def _finite_setting(value: float, *, name: str) -> float:
    """Return a finite numeric ENV value or fail startup explicitly."""
    parsed = float(value)
    if not math.isfinite(parsed):
        raise ValueError(f"{name} должен быть конечным числом")
    return parsed


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    BOT_TOKEN: str = ""
    ADMIN_IDS: str = ""
    # G66: when true, new trading, interactive bot use and private notifications
    # are restricted to configured ADMIN_IDS. Existing non-admin live executions
    # keep protective monitoring until terminal so enabling the switch never
    # abandons an already-open position.
    ADMIN_ONLY_MODE: bool = True
    DATABASE_URL: str = ""
    DATABASE_PATH: str = "/data/antilud_bingx_core.db"
    # Для Railway предпочтителен PostgreSQL через DATABASE_URL. Второй вариант -
    # SQLite на подключённом Railway Volume /data.
    ENCRYPTION_KEY: str = ""

    # BingX-only сборка. Старые значения mexc/all принимаются только как legacy aliases.
    DEFAULT_EXCHANGE: str = "bingx"

    # BingX USDT-M Perpetual Futures (/openApi/swap/v2/*).
    BINGX_VST: bool = False  # True = BingX VST/simulated environment, not production.
    BINGX_REQUEST_TIMEOUT_SECONDS: int = 10
    BINGX_ENTRY_ORDER_TYPE: str = "limit"  # limit / market
    BINGX_API_TAKER_FEE_RATE: float = 0.0005
    # Off by default: BingX trigger TP/SL clientOrderID support must be verified by a controlled tiny-test.
    BINGX_PROTECTIVE_CLIENT_ORDER_ID_ENABLED: bool = False

    # Legacy MEXC variable names are retained as aliases so old Railway projects
    # do not crash during startup. Runtime routing in this build is BingX-only.
    MEXC_TESTNET: bool = False
    MEXC_REQUEST_TIMEOUT_SECONDS: int = 10
    MEXC_ENTRY_ORDER_TYPE: str = "limit"
    MEXC_API_TAKER_FEE_RATE: float = 0.0005
    EXCHANGE_CALL_TIMEOUT_SEC: int = 10
    EXCHANGE_TRANSIENT_ERROR_NOTIFY_EVERY: int = 3
    EXCHANGE_TRANSIENT_ERROR_MAX_RETRIES: int = 30

    # Safe performance tuning. Shared keep-alive avoids a new TCP/TLS handshake
    # for every menu click, monitor cycle and signal execution.
    BINGX_HTTP_MAX_CONNECTIONS: int = 40
    BINGX_HTTP_MAX_KEEPALIVE_CONNECTIONS: int = 20
    BINGX_HTTP_KEEPALIVE_EXPIRY_SEC: int = 30
    MEXC_HTTP_MAX_CONNECTIONS: int = 40
    MEXC_HTTP_MAX_KEEPALIVE_CONNECTIONS: int = 20
    MEXC_HTTP_KEEPALIVE_EXPIRY_SEC: int = 30
    POSTGRES_POOL_MIN_SIZE: int = 1
    POSTGRES_POOL_MAX_SIZE: int = 12

    # Process-wide BingX request governor. Defaults intentionally mirror the
    # proven MEXC workload profile, while still using BingX-specific ENV names.
    # These limits apply across entries, monitors, recovery and menu diagnostics,
    # not separately per signal.
    # Legacy MEXC names are still accepted below as aliases for old Railway
    # projects, but the BingX-only runtime reads the BINGX_* names.
    BINGX_GLOBAL_MAX_IN_FLIGHT: int = 20
    BINGX_GLOBAL_REQUESTS_PER_SECOND: float = 20.0
    BINGX_GLOBAL_BURST_LIMIT: int = 30
    BINGX_PER_USER_REQUESTS_PER_SECOND: float = 5.0
    BINGX_PER_USER_BURST_LIMIT: int = 8
    BINGX_REQUEST_QUEUE_TIMEOUT_SECONDS: float = 20.0
    BINGX_TRADE_ORDER_REQUESTS_PER_SECOND: float = 10.0
    BINGX_TRADE_ORDER_BURST_LIMIT: int = 10

    # Legacy aliases retained for existing Railway variables.
    MEXC_GLOBAL_MAX_IN_FLIGHT: int = 20
    MEXC_GLOBAL_REQUESTS_PER_SECOND: float = 20.0
    MEXC_GLOBAL_BURST_LIMIT: int = 30
    MEXC_PER_USER_REQUESTS_PER_SECOND: float = 5.0
    MEXC_PER_USER_BURST_LIMIT: int = 8
    MEXC_REQUEST_QUEUE_TIMEOUT_SECONDS: float = 20.0

    MARGIN_MODE: str = "cross"
    # Isolated mode never uses the exchange maximum blindly. The leverage is
    # capped so the approximate liquidation distance remains beyond STOP plus
    # this additional safety buffer.
    ISOLATED_LIQUIDATION_BUFFER_PERCENT: float = 2.0

    DEFAULT_RISK_PERCENT: float = 1.0
    DEFAULT_DAILY_RISK_LIMIT_PERCENT: float = 10.0
    DEFAULT_MAX_OPEN_TRADES: int = 10
    DEFAULT_MAX_PORTFOLIO_RISK: float = 10.0
    DEFAULT_EXCLUDE_BE_TRADES_FROM_RISK: bool = True
    DEFAULT_TP_LIMIT: str = "all"
    DEFAULT_TP_MODE: str = "bell"
    DEFAULT_BE_AFTER_TP1: bool = True
    DEFAULT_BE_TRIGGER_TP_INDEX: int = 1

    BE_FEE_BUFFER_PERCENT: float = 0.12
    BE_PLUS_PERCENT: float = 0.0

    # Fail closed on new deployments: exact source chat IDs must be configured.
    VIP_REQUIRE_TRUSTED_SOURCE: bool = True
    VIP_ALLOWED_SOURCE_CHAT_IDS: str = ""
    VIP_EXECUTE_PRIVATE_SIGNALS: bool = False
    VIP_ONLY_GROUP_SIGNALS: bool = True
    VIP_REQUIRE_SENDER_CHAT: bool = True
    VIP_ALLOWED_SENDER_CHAT_TITLES: str = "Торгаш"
    VIP_SIGNAL_DEDUP_TTL_SECONDS: int = 86400
    VIP_DEBUG_LOGS: bool = False
    VIP_MAX_SIGNAL_AGE_SECONDS: int = 120

    LIMIT_ORDER_TTL_HOURS: int = 24
    MIN_TP_RR: float = 0.1

    # Maximum accounts opened concurrently for one group signal.  v1.0.5 raises
    # the default to the validator cap so up to 20 users can enter the dispatcher
    # in parallel while BingX write calls still remain governed by rate-limit
    # priority gates.
    TRADE_EXECUTION_WORKERS: int = 20
    # v1.0.6a: after MARKET entry + STOP are confirmed, TP writes may continue
    # in a short-lived background task.  This frees dispatcher workers earlier
    # for the next user, while STOP safety remains on the critical path.
    MARKET_TP_BACKGROUND_ENABLED: bool = True
    # v1.0.6b: protected rows with queued/running background TP state are
    # re-scheduled from durable trade_executions metadata after deploy/restart.
    MARKET_TP_BACKGROUND_RECOVERY_ENABLED: bool = True
    # One global queue for all signals. A stale MARKET/LIMIT entry is safer to
    # skip than to execute after price/context has moved.
    TRADE_QUEUE_MAX_SIZE: int = 200
    # Legacy fallback retained for old Railway deployments.
    TRADE_QUEUE_MAX_WAIT_SECONDS: float = 20.0
    MARKET_QUEUE_MAX_WAIT_SECONDS: float = 20.0
    LIMIT_QUEUE_MAX_WAIT_SECONDS: float = 60.0
    TRADE_DISPATCHER_SHUTDOWN_TIMEOUT_SECONDS: float = 30.0
    MAX_MARKET_ENTRY_DEVIATION_PERCENT: float = 0.5
    # Symmetric signal/current-price ratio guard for both LIMIT and MARKET.
    # Example: 0.5326 vs 0.05346 is ~9.96x and is blocked before any BingX write.
    # 0 or 1 disables the guard; normal pullback LIMITs remain allowed.
    MAX_SIGNAL_ENTRY_PRICE_RATIO: float = 5.0
    # Admin-only diagnostic preview for signals that look like a decimal-place
    # mismatch. It never opens a corrected trade automatically.
    SIGNAL_DECIMAL_NORMALIZATION_PREVIEW_ENABLED: bool = True
    SIGNAL_DECIMAL_NORMALIZATION_MAX_DEVIATION_PERCENT: float = 3.0
    SIGNAL_DECIMAL_NORMALIZATION_MAX_POWER: int = 6

    # Telegram delivery is isolated from BingX execution workers.
    TELEGRAM_NOTIFICATION_WORKERS: int = 10
    TELEGRAM_NOTIFICATION_QUEUE_MAX_SIZE: int = 500
    TELEGRAM_NOTIFICATION_MAX_WAIT_SECONDS: float = 30.0

    MONITOR_ACTIVE_INTERVAL_SEC: int = 5
    TP_PARALLEL_LIMIT: int = 4
    MONITOR_WORKERS: int = 2
    MONITOR_CRITICAL_INTERVAL_SEC: int = 5

    # Event-driven group monitoring. One public BingX price read per active
    # symbol triggers targeted per-account verification. Full private-account
    # reconciliation remains as a slower safety fallback.
    EVENT_DRIVEN_MONITOR_ENABLED: bool = True
    MARKET_PRICE_POLL_INTERVAL_SEC: float = 5.0
    MARKET_PRICE_STALE_SEC: int = 15
    EVENT_VERIFY_WORKERS: int = 10
    EVENT_VERIFY_ATTEMPTS: int = 3
    EVENT_VERIFY_RETRY_BASE_SEC: float = 0.5
    # g39 Step 1: read-only market-event evidence snapshots. Disabled by
    # default for safe rollout; when enabled it only persists shadow evidence
    # and never changes queue status, retries, trading decisions or orders.
    MARKET_EVENT_EVIDENCE_SNAPSHOT_ENABLED: bool = False
    MARKET_EVENT_SPLIT_ENTRY_TP_STATE_ENABLED: bool = False
    # g40 Step 2: finite ENTRY/TP event state machine.  The switch is disabled
    # by default so g39 shadow evidence can be observed before terminal behavior
    # is enabled on Railway. STOP protection never uses this state machine.
    MARKET_EVENT_TERMINAL_REVIEW_ENABLED: bool = False
    MARKET_EVENT_MAX_FAST_ATTEMPTS: int = 3
    MARKET_EVENT_MAX_DEEP_ATTEMPTS: int = 2
    MARKET_EVENT_MAX_FINAL_ATTEMPTS: int = 1
    MARKET_EVENT_MANUAL_REVIEW_NOTIFY: bool = True
    # g41 Step 3: one-event exchange read coalescing. Disabled by default for
    # phased Railway rollout. Any exchange write invalidates the event snapshot
    # before and after the write, so protective read-backs remain fresh.
    MARKET_EVENT_READ_COALESCING_ENABLED: bool = False
    MARKET_EVENT_READ_CACHE_TTL_SCALE: float = 1.0
    MARKET_EVENT_LEASE_PREFLIGHT_ENABLED: bool = True
    MARKET_EVENT_LEASE_EXTENSION_SEC: float = 120.0
    # g42 Step 4: controlled migration/rollout for legacy stuck events.  The
    # stage is an explicit contract, not an implicit collection of booleans.
    # off -> no migration; shadow -> metrics only; group_1541 -> one target
    # group; global -> bounded batches for every eligible legacy event.
    MARKET_EVENT_ROLLOUT_STAGE: str = "off"
    MARKET_EVENT_MIGRATION_ENABLED: bool = False
    MARKET_EVENT_MIGRATION_TARGET_GROUP_ID: int = 1541
    MARKET_EVENT_MIGRATION_MIN_ATTEMPTS: int = 6
    MARKET_EVENT_MIGRATION_BATCH_SIZE: int = 4
    MARKET_EVENT_MIGRATION_INTERVAL_SEC: float = 60.0
    MARKET_EVENT_MANUAL_RESOLUTION_ENABLED: bool = False
    MONITOR_FULL_RECONCILE_INTERVAL_SEC: int = 30
    CLOSED_HISTORY_RECONCILE_TIMEOUT_SEC: int = 900
    # Old pending close-history rows can survive deploys/restarts and then look
    # like fresh BTC/LINK closures.  After this age they are archived silently;
    # fresh v1.6.88 closes carry an explicit marker and still notify on timeout.
    CLOSED_HISTORY_STALE_SILENT_AFTER_SEC: int = 3600

    LOG_LEVEL: str = "INFO"

    # Railway deploy diagnostics. All diagnostics are sanitized and never print
    # BOT_TOKEN, ENCRYPTION_KEY, API keys or DATABASE_URL values. Strict startup
    # is opt-in because a read-only audit should not unexpectedly break an old
    # Railway project during upgrade.
    RAILWAY_DIAGNOSTICS_ENABLED: bool = True
    RAILWAY_DIAGNOSTICS_STRICT_STARTUP: bool = False

    # v1.0.7a1 monitor observability. These switches only emit sanitized
    # timings/counters; they never change order flow, retries or monitor cadence.
    MONITOR_DIAGNOSTICS_ENABLED: bool = True
    MONITOR_DIAGNOSTICS_SUMMARY_INTERVAL_SEC: float = 60.0
    MONITOR_HEARTBEAT_INTERVAL_SEC: float = 1.0
    MONITOR_HEARTBEAT_WARNING_SEC: float = 2.0
    MONITOR_HEARTBEAT_CRITICAL_SEC: float = 10.0
    RAILWAY_EXPECTED_APP_VERSION: str = "auto"
    # Set true only after a controlled real/VST BingX tiny-test confirmed the
    # current archive on Railway. This is a diagnostic marker, not a trading flag.
    BINGX_LIVE_TINY_TEST_CONFIRMED: bool = False

    # g5b3g: passive source-signal analytics. Every feature is disabled by
    # default and isolated from ENTRY/STOP/TP/BE/lifecycle. One low-priority
    # DB worker stores normalized signals plus sampled public-price transitions.
    SIGNAL_ANALYTICS_ENABLED: bool = False
    SIGNAL_ANALYTICS_INGRESS_ENABLED: bool = False
    SIGNAL_ANALYTICS_TRACKING_ENABLED: bool = False
    SIGNAL_ANALYTICS_RECOVERY_ENABLED: bool = False
    SIGNAL_ANALYTICS_REPORTS_ENABLED: bool = False
    SIGNAL_ANALYTICS_SIMULATION_ENABLED: bool = False
    SIGNAL_ANALYTICS_QUEUE_MAX: int = 5000
    SIGNAL_ANALYTICS_BATCH_SIZE: int = 100
    SIGNAL_ANALYTICS_FLUSH_SECONDS: float = 2.0
    SIGNAL_ANALYTICS_DB_WORKERS: int = 1
    SIGNAL_ANALYTICS_DEDUP_WINDOW_HOURS: int = 12
    SIGNAL_ANALYTICS_MAX_ACTIVE: int = 5000
    SIGNAL_ANALYTICS_DEFAULT_EXPIRY_HOURS: int = 24
    SIGNAL_ANALYTICS_SHUTDOWN_TIMEOUT_SECONDS: float = 5.0
    SIGNAL_ANALYTICS_SUMMARY_INTERVAL_SEC: float = 60.0

    # g5b3g20 / statistics plan step 10. Runtime features remain independently
    # disabled by default. STATISTICS_SHADOW_EXPECTED_STAGE is a read-only
    # deployment assertion: it never enables a flag and only verifies that the
    # Railway flag matrix matches the explicitly chosen rollout stage.
    STATISTICS_PERIODS_ENABLED: bool = False
    STATISTICS_EXECUTION_RESULTS_ENABLED: bool = False
    STATISTICS_FUNDING_ENABLED: bool = False
    # g37 Package 3: funding recovery is deliberately separate from the
    # trading hot path. Empty history is not treated as confirmed zero until a
    # grace period and repeated empty observations have elapsed.
    STATISTICS_FUNDING_ZERO_CONFIRMATIONS: int = 2
    STATISTICS_FUNDING_ZERO_GRACE_SEC: int = 900
    STATISTICS_FUNDING_MAX_RECOVERY_ATTEMPTS: int = 8
    STATISTICS_FUNDING_RECOVERY_DEADLINE_SEC: int = 21600
    STATISTICS_LINKAGE_ENABLED: bool = False
    STATISTICS_RECOVERY_ENABLED: bool = False
    # g35 package 1: conservative read-only replay for future Railway
    # restart gaps. It is isolated from trading and claims at most a bounded
    # number of source signals per analytics pass.
    STATISTICS_GAP_RECOVERY_INTERVAL_SEC: float = 60.0
    STATISTICS_GAP_RECOVERY_MAX_HOURS: int = 24
    STATISTICS_GAP_RECOVERY_BATCH_SIZE: int = 1
    STATISTICS_GAP_RECOVERY_MAX_ATTEMPTS: int = 8
    STATISTICS_GAP_RECOVERY_RETRY_SEC: float = 300.0
    STATISTICS_QUALITY_ENABLED: bool = False
    STATS_RESET_ENABLED: bool = False
    STATS_V2_REPORTS_ENABLED: bool = False
    STATISTICS_SHADOW_EXPECTED_STAGE: str = "off"
    STATISTICS_SHADOW_REQUIRE_POSTGRES: bool = True
    STATISTICS_SHADOW_DB_DIAGNOSTICS_ENABLED: bool = False
    STATISTICS_SHADOW_STRICT_STARTUP: bool = False

    # g5b3g4 / fee reconciliation step 4. The durable read-only worker is
    # disabled by default and remains isolated from all trading hot paths.
    FINANCIAL_RECONCILIATION_ENABLED: bool = False
    FINANCIAL_RECONCILIATION_WORKERS: int = 1
    FINANCIAL_RECONCILIATION_QUEUE_MAX: int = 200
    FINANCIAL_RECONCILIATION_MAX_ATTEMPTS: int = 6
    FINANCIAL_RECONCILIATION_STALE_PROCESSING_SEC: int = 120
    FINANCIAL_RECONCILIATION_DEADLINE_SEC: int = 900
    FINANCIAL_RECONCILIATION_REQUESTS_PER_SECOND: float = 1.0
    FINANCIAL_RECONCILIATION_LOOKBACK_SEC: int = 2592000
    FINANCIAL_RECONCILIATION_SHUTDOWN_TIMEOUT_SECONDS: float = 10.0

    @model_validator(mode="before")
    @classmethod
    def strip_surrounding_quotes_from_all_env_values(cls, values):
        """Accept Railway values pasted with one pair of surrounding quotes.

        Pydantic cannot parse literal strings such as ``"false"`` or ``"10"``
        as bool/int/float.  Earlier code cleaned only a few string fields, so a
        fully quoted Railway variable list could stop the bot at startup.
        """
        if not isinstance(values, dict):
            return values
        cleaned = dict(values)
        for key, value in cleaned.items():
            if not isinstance(value, str):
                continue
            stripped = value.strip()
            if (
                len(stripped) >= 2
                and stripped[0] == stripped[-1]
                and stripped[0] in {'"', "'"}
            ):
                cleaned[key] = stripped[1:-1].strip()

        # v1.6.57: existing Railway projects may still define only the legacy
        # MEXC_* workload variables.  Copy them into the BingX governor fields
        # when explicit BINGX_* values are absent so the BingX-only runtime and
        # the visible Railway variables stay aligned.
        aliases = {
            "BINGX_GLOBAL_MAX_IN_FLIGHT": "MEXC_GLOBAL_MAX_IN_FLIGHT",
            "BINGX_GLOBAL_REQUESTS_PER_SECOND": "MEXC_GLOBAL_REQUESTS_PER_SECOND",
            "BINGX_GLOBAL_BURST_LIMIT": "MEXC_GLOBAL_BURST_LIMIT",
            "BINGX_PER_USER_REQUESTS_PER_SECOND": "MEXC_PER_USER_REQUESTS_PER_SECOND",
            "BINGX_PER_USER_BURST_LIMIT": "MEXC_PER_USER_BURST_LIMIT",
            "BINGX_REQUEST_QUEUE_TIMEOUT_SECONDS": "MEXC_REQUEST_QUEUE_TIMEOUT_SECONDS",
            "BINGX_TRADE_ORDER_REQUESTS_PER_SECOND": "MEXC_GLOBAL_REQUESTS_PER_SECOND",
            "BINGX_TRADE_ORDER_BURST_LIMIT": "MEXC_GLOBAL_BURST_LIMIT",
        }
        for bingx_key, legacy_key in aliases.items():
            if bingx_key not in cleaned and legacy_key in cleaned:
                cleaned[bingx_key] = cleaned[legacy_key]
        return cleaned

    @field_validator(
        "BOT_TOKEN", "DATABASE_URL", "DATABASE_PATH", "ENCRYPTION_KEY", mode="before"
    )
    @classmethod
    def strip_surrounding_env_quotes(cls, value):
        """Accept Railway values pasted with one pair of surrounding quotes."""
        if not isinstance(value, str):
            return value
        cleaned = value.strip()
        if len(cleaned) >= 2 and cleaned[0] == cleaned[-1] and cleaned[0] in {'"', "'"}:
            return cleaned[1:-1].strip()
        return cleaned

    @field_validator("BINGX_VST", "BINGX_PROTECTIVE_CLIENT_ORDER_ID_ENABLED", "MEXC_TESTNET")
    @classmethod
    def normalize_bingx_vst_flag(cls, value: bool) -> bool:
        # BingX has a separate VST/simulated API domain. The flag is explicit and
        # does not map to production silently.
        return bool(value)

    @field_validator("STATISTICS_SHADOW_EXPECTED_STAGE")
    @classmethod
    def valid_statistics_shadow_stage(cls, value: str) -> str:
        from app.services.statistics_shadow_rollout import SHADOW_STAGES

        normalized = str(value or "off").strip().lower()
        if normalized not in SHADOW_STAGES:
            raise ValueError(
                "STATISTICS_SHADOW_EXPECTED_STAGE должен быть одним из: "
                + ", ".join(SHADOW_STAGES)
            )
        return normalized

    @field_validator("DEFAULT_EXCHANGE")
    @classmethod
    def supported_exchange(cls, value: str) -> str:
        # Любое старое значение безопасно переводится на единственную биржу.
        return "bingx"

    @field_validator("MARGIN_MODE")
    @classmethod
    def valid_margin_mode(cls, value: str) -> str:
        normalized = str(value or "").strip().lower()
        if normalized not in {"cross", "isolated"}:
            raise ValueError("MARGIN_MODE должен быть cross или isolated")
        return normalized

    @field_validator("ISOLATED_LIQUIDATION_BUFFER_PERCENT")
    @classmethod
    def safe_isolated_liquidation_buffer(cls, value: float) -> float:
        value = _finite_setting(value, name="ISOLATED_LIQUIDATION_BUFFER_PERCENT")
        if value < 0.5 or value > 20.0:
            raise ValueError(
                "ISOLATED_LIQUIDATION_BUFFER_PERCENT должен быть от 0.5 до 20"
            )
        return value

    @field_validator("DEFAULT_RISK_PERCENT")
    @classmethod
    def safe_default_risk_percent(cls, value: float) -> float:
        from app.services.risk_engine import validate_risk_percent
        return validate_risk_percent(value)

    @field_validator("DEFAULT_DAILY_RISK_LIMIT_PERCENT")
    @classmethod
    def safe_default_daily_risk_limit_percent(cls, value: float) -> float:
        from app.services.risk_engine import validate_daily_risk_limit_percent
        return validate_daily_risk_limit_percent(value)

    @field_validator("DEFAULT_MAX_PORTFOLIO_RISK")
    @classmethod
    def safe_default_max_portfolio_risk(cls, value: float) -> float:
        from app.services.risk_engine import validate_max_portfolio_risk_percent
        return validate_max_portfolio_risk_percent(value)

    @field_validator("DEFAULT_MAX_OPEN_TRADES")
    @classmethod
    def safe_default_max_open_trades(cls, value: int) -> int:
        from app.services.risk_engine import validate_max_open_trades
        return validate_max_open_trades(value)

    @field_validator("CLOSED_HISTORY_RECONCILE_TIMEOUT_SEC")
    @classmethod
    def safe_closed_history_timeout(cls, value: int) -> int:
        return max(30, min(1800, int(value)))

    @field_validator("CLOSED_HISTORY_STALE_SILENT_AFTER_SEC")
    @classmethod
    def safe_closed_history_stale_silent_after(cls, value: int) -> int:
        return max(0, min(86400, int(value)))

    @field_validator("BINGX_ENTRY_ORDER_TYPE", "MEXC_ENTRY_ORDER_TYPE")
    @classmethod
    def valid_entry_order_type(cls, value: str) -> str:
        normalized = str(value or "").strip().lower()
        if normalized not in {"limit", "market"}:
            raise ValueError("BINGX_ENTRY_ORDER_TYPE должен быть limit или market")
        return normalized

    @field_validator("DEFAULT_TP_MODE")
    @classmethod
    def valid_tp_mode(cls, value: str) -> str:
        normalized = str(value or "").strip().lower()
        if normalized not in {
            "smart",
            "bell",
            "equal",
            "acceleration",
            "early_fixation",
            "manual",
        }:
            raise ValueError(
                "DEFAULT_TP_MODE должен быть smart, bell, equal, acceleration, early_fixation или manual"
            )
        return normalized

    @field_validator("DEFAULT_BE_TRIGGER_TP_INDEX")
    @classmethod
    def valid_be_index(cls, value: int) -> int:
        value = int(value)
        if value < 0 or value > 3:
            raise ValueError("DEFAULT_BE_TRIGGER_TP_INDEX допускает только 0-3")
        return value

    @field_validator("BINGX_REQUEST_TIMEOUT_SECONDS", "MEXC_REQUEST_TIMEOUT_SECONDS", "EXCHANGE_CALL_TIMEOUT_SEC")
    @classmethod
    def safe_exchange_timeout(cls, value: int) -> int:
        value = int(value)
        if value < 5:
            raise ValueError(
                "Таймаут биржи ниже 5 секунд небезопасен для write-запросов"
            )
        return value

    @field_validator("MONITOR_ACTIVE_INTERVAL_SEC", "MONITOR_CRITICAL_INTERVAL_SEC")
    @classmethod
    def safe_monitor_interval(cls, value: int) -> int:
        return max(5, int(value))

    @field_validator("TP_PARALLEL_LIMIT")
    @classmethod
    def safe_tp_parallel_limit(cls, value: int) -> int:
        return max(1, min(4, int(value)))

    @field_validator("MARKET_PRICE_POLL_INTERVAL_SEC")
    @classmethod
    def safe_market_price_poll_interval(cls, value: float) -> float:
        interval = _finite_setting(value, name="MARKET_PRICE_POLL_INTERVAL_SEC")
        if interval < 0.25:
            raise ValueError(
                "MARKET_PRICE_POLL_INTERVAL_SEC должен быть не меньше 0.25 секунды "
                "из-за лимита публичного BingX ticker API."
            )
        return min(10.0, interval)

    @field_validator("MARKET_PRICE_STALE_SEC")
    @classmethod
    def safe_market_price_stale(cls, value: int) -> int:
        return max(3, min(60, int(value)))

    @field_validator("EVENT_VERIFY_WORKERS")
    @classmethod
    def safe_event_verify_workers(cls, value: int) -> int:
        return max(1, min(20, int(value)))

    @field_validator("EVENT_VERIFY_ATTEMPTS")
    @classmethod
    def safe_event_verify_attempts(cls, value: int) -> int:
        return max(1, min(8, int(value)))

    @field_validator("EVENT_VERIFY_RETRY_BASE_SEC")
    @classmethod
    def safe_event_verify_retry_base(cls, value: float) -> float:
        value = _finite_setting(value, name="EVENT_VERIFY_RETRY_BASE_SEC")
        return max(0.5, min(30.0, value))

    @field_validator("MONITOR_FULL_RECONCILE_INTERVAL_SEC")
    @classmethod
    def safe_full_reconcile_interval(cls, value: int) -> int:
        return max(15, min(300, int(value)))

    @field_validator("MONITOR_DIAGNOSTICS_SUMMARY_INTERVAL_SEC")
    @classmethod
    def safe_monitor_diagnostics_summary_interval(cls, value: float) -> float:
        value = _finite_setting(value, name="MONITOR_DIAGNOSTICS_SUMMARY_INTERVAL_SEC")
        return max(15.0, min(600.0, value))

    @field_validator("MONITOR_HEARTBEAT_INTERVAL_SEC")
    @classmethod
    def safe_monitor_heartbeat_interval(cls, value: float) -> float:
        value = _finite_setting(value, name="MONITOR_HEARTBEAT_INTERVAL_SEC")
        return max(0.25, min(10.0, value))

    @field_validator("MONITOR_HEARTBEAT_WARNING_SEC")
    @classmethod
    def safe_monitor_heartbeat_warning(cls, value: float) -> float:
        value = _finite_setting(value, name="MONITOR_HEARTBEAT_WARNING_SEC")
        return max(0.5, min(60.0, value))

    @field_validator("MONITOR_HEARTBEAT_CRITICAL_SEC")
    @classmethod
    def safe_monitor_heartbeat_critical(cls, value: float) -> float:
        value = _finite_setting(value, name="MONITOR_HEARTBEAT_CRITICAL_SEC")
        return max(1.0, min(300.0, value))

    @field_validator("BINGX_HTTP_MAX_CONNECTIONS", "MEXC_HTTP_MAX_CONNECTIONS")
    @classmethod
    def safe_http_connections(cls, value: int) -> int:
        return max(4, min(100, int(value)))

    @field_validator("BINGX_HTTP_MAX_KEEPALIVE_CONNECTIONS", "MEXC_HTTP_MAX_KEEPALIVE_CONNECTIONS")
    @classmethod
    def safe_http_keepalive_connections(cls, value: int) -> int:
        return max(2, min(100, int(value)))

    @field_validator("BINGX_HTTP_KEEPALIVE_EXPIRY_SEC", "MEXC_HTTP_KEEPALIVE_EXPIRY_SEC")
    @classmethod
    def safe_http_keepalive_expiry(cls, value: int) -> int:
        return max(5, min(300, int(value)))

    @field_validator("POSTGRES_POOL_MIN_SIZE")
    @classmethod
    def safe_postgres_pool_min(cls, value: int) -> int:
        return max(1, min(10, int(value)))

    @field_validator("POSTGRES_POOL_MAX_SIZE")
    @classmethod
    def safe_postgres_pool_max(cls, value: int) -> int:
        # The monitor architecture reserves distinct ordinary/advisory paths.
        # A one-connection PostgreSQL pool can self-starve when a worker holds
        # an advisory connection and then needs an ordinary query.
        return max(2, min(30, int(value)))

    @field_validator("MONITOR_WORKERS")
    @classmethod
    def safe_monitor_workers(cls, value: int) -> int:
        return max(1, min(2, int(value)))

    @field_validator("TRADE_EXECUTION_WORKERS")
    @classmethod
    def safe_trade_concurrency(cls, value: int) -> int:
        return max(1, min(20, int(value)))

    @field_validator("TRADE_QUEUE_MAX_SIZE")
    @classmethod
    def safe_trade_queue_size(cls, value: int) -> int:
        return max(20, min(5000, int(value)))

    @field_validator(
        "TRADE_QUEUE_MAX_WAIT_SECONDS",
        "MARKET_QUEUE_MAX_WAIT_SECONDS",
        "LIMIT_QUEUE_MAX_WAIT_SECONDS",
    )
    @classmethod
    def safe_trade_queue_wait(cls, value: float) -> float:
        value = _finite_setting(value, name="TRADE_QUEUE_WAIT_SECONDS")
        return max(1.0, min(120.0, value))

    @field_validator("TRADE_DISPATCHER_SHUTDOWN_TIMEOUT_SECONDS")
    @classmethod
    def safe_trade_shutdown_wait(cls, value: float) -> float:
        value = _finite_setting(
            value, name="TRADE_DISPATCHER_SHUTDOWN_TIMEOUT_SECONDS"
        )
        return max(5.0, min(120.0, value))

    @field_validator("MAX_SIGNAL_ENTRY_PRICE_RATIO")
    @classmethod
    def safe_signal_entry_price_ratio(cls, value: float) -> float:
        value = _finite_setting(value, name="MAX_SIGNAL_ENTRY_PRICE_RATIO")
        if value <= 1.0:
            return 0.0
        return min(1000.0, value)

    @field_validator("MAX_MARKET_ENTRY_DEVIATION_PERCENT")
    @classmethod
    def safe_market_entry_deviation(cls, value: float) -> float:
        value = _finite_setting(value, name="MAX_MARKET_ENTRY_DEVIATION_PERCENT")
        return max(0.0, min(10.0, value))

    @field_validator("SIGNAL_DECIMAL_NORMALIZATION_MAX_DEVIATION_PERCENT")
    @classmethod
    def safe_decimal_normalization_deviation(cls, value: float) -> float:
        value = _finite_setting(value, name="SIGNAL_DECIMAL_NORMALIZATION_MAX_DEVIATION_PERCENT")
        return max(0.01, min(25.0, value))

    @field_validator("SIGNAL_DECIMAL_NORMALIZATION_MAX_POWER")
    @classmethod
    def safe_decimal_normalization_power(cls, value: int) -> int:
        return max(1, min(8, int(value)))

    @field_validator("BINGX_GLOBAL_MAX_IN_FLIGHT", "MEXC_GLOBAL_MAX_IN_FLIGHT")
    @classmethod
    def safe_mexc_global_in_flight(cls, value: int) -> int:
        return max(2, min(100, int(value)))

    @field_validator(
        "BINGX_GLOBAL_REQUESTS_PER_SECOND",
        "BINGX_PER_USER_REQUESTS_PER_SECOND",
        "BINGX_TRADE_ORDER_REQUESTS_PER_SECOND",
        "MEXC_GLOBAL_REQUESTS_PER_SECOND",
        "MEXC_PER_USER_REQUESTS_PER_SECOND",
    )
    @classmethod
    def safe_mexc_request_rate(cls, value: float) -> float:
        value = _finite_setting(value, name="MEXC_REQUESTS_PER_SECOND")
        return max(0.5, min(100.0, value))

    @field_validator("BINGX_GLOBAL_BURST_LIMIT", "BINGX_PER_USER_BURST_LIMIT", "BINGX_TRADE_ORDER_BURST_LIMIT", "MEXC_GLOBAL_BURST_LIMIT", "MEXC_PER_USER_BURST_LIMIT")
    @classmethod
    def safe_mexc_burst(cls, value: int) -> int:
        return max(1, min(200, int(value)))

    @field_validator("BINGX_REQUEST_QUEUE_TIMEOUT_SECONDS", "MEXC_REQUEST_QUEUE_TIMEOUT_SECONDS")
    @classmethod
    def safe_mexc_request_queue_timeout(cls, value: float) -> float:
        value = _finite_setting(value, name="REQUEST_QUEUE_TIMEOUT_SECONDS")
        return max(1.0, min(120.0, value))

    @field_validator("TELEGRAM_NOTIFICATION_WORKERS")
    @classmethod
    def safe_notification_workers(cls, value: int) -> int:
        return max(1, min(30, int(value)))

    @field_validator("TELEGRAM_NOTIFICATION_QUEUE_MAX_SIZE")
    @classmethod
    def safe_notification_queue_size(cls, value: int) -> int:
        return max(20, min(10000, int(value)))

    @field_validator("TELEGRAM_NOTIFICATION_MAX_WAIT_SECONDS")
    @classmethod
    def safe_notification_wait(cls, value: float) -> float:
        value = _finite_setting(value, name="TELEGRAM_NOTIFICATION_MAX_WAIT_SECONDS")
        return max(1.0, min(300.0, value))

    @field_validator("SIGNAL_ANALYTICS_QUEUE_MAX")
    @classmethod
    def safe_signal_analytics_queue_max(cls, value: int) -> int:
        return max(100, min(100_000, int(value)))

    @field_validator("SIGNAL_ANALYTICS_BATCH_SIZE")
    @classmethod
    def safe_signal_analytics_batch_size(cls, value: int) -> int:
        return max(1, min(500, int(value)))

    @field_validator("SIGNAL_ANALYTICS_FLUSH_SECONDS")
    @classmethod
    def safe_signal_analytics_flush_seconds(cls, value: float) -> float:
        value = _finite_setting(value, name="SIGNAL_ANALYTICS_FLUSH_SECONDS")
        return max(0.1, min(10.0, value))

    @field_validator("SIGNAL_ANALYTICS_DB_WORKERS")
    @classmethod
    def one_signal_analytics_db_worker(cls, value: int) -> int:
        # The analytics writer is intentionally serialized so analytics can never
        # fan out PostgreSQL pool pressure. Future stages may revisit this only
        # after measured Railway evidence.
        return 1

    @field_validator("SIGNAL_ANALYTICS_DEDUP_WINDOW_HOURS")
    @classmethod
    def safe_signal_analytics_dedup_window(cls, value: int) -> int:
        return max(1, min(168, int(value)))

    @field_validator("SIGNAL_ANALYTICS_MAX_ACTIVE")
    @classmethod
    def safe_signal_analytics_max_active(cls, value: int) -> int:
        return max(100, min(100_000, int(value)))

    @field_validator("SIGNAL_ANALYTICS_DEFAULT_EXPIRY_HOURS")
    @classmethod
    def safe_signal_analytics_default_expiry_hours(cls, value: int) -> int:
        return max(1, min(168, int(value)))

    @field_validator("SIGNAL_ANALYTICS_SHUTDOWN_TIMEOUT_SECONDS")
    @classmethod
    def safe_signal_analytics_shutdown_timeout(cls, value: float) -> float:
        value = _finite_setting(
            value, name="SIGNAL_ANALYTICS_SHUTDOWN_TIMEOUT_SECONDS"
        )
        return max(1.0, min(30.0, value))

    @field_validator("SIGNAL_ANALYTICS_SUMMARY_INTERVAL_SEC")
    @classmethod
    def safe_signal_analytics_summary_interval(cls, value: float) -> float:
        value = _finite_setting(value, name="SIGNAL_ANALYTICS_SUMMARY_INTERVAL_SEC")
        return max(15.0, min(600.0, value))

    @field_validator("STATISTICS_GAP_RECOVERY_INTERVAL_SEC")
    @classmethod
    def safe_statistics_gap_recovery_interval(cls, value: float) -> float:
        value = _finite_setting(value, name="STATISTICS_GAP_RECOVERY_INTERVAL_SEC")
        return max(30.0, min(3600.0, value))

    @field_validator("STATISTICS_GAP_RECOVERY_MAX_HOURS")
    @classmethod
    def safe_statistics_gap_recovery_hours(cls, value: int) -> int:
        return max(1, min(24, int(value)))

    @field_validator("STATISTICS_GAP_RECOVERY_BATCH_SIZE")
    @classmethod
    def safe_statistics_gap_recovery_batch(cls, value: int) -> int:
        return max(1, min(5, int(value)))

    @field_validator("STATISTICS_GAP_RECOVERY_MAX_ATTEMPTS")
    @classmethod
    def safe_statistics_gap_recovery_attempts(cls, value: int) -> int:
        return max(1, min(20, int(value)))

    @field_validator("STATISTICS_GAP_RECOVERY_RETRY_SEC")
    @classmethod
    def safe_statistics_gap_recovery_retry(cls, value: float) -> float:
        value = _finite_setting(value, name="STATISTICS_GAP_RECOVERY_RETRY_SEC")
        return max(60.0, min(3600.0, value))

    @field_validator("STATISTICS_FUNDING_ZERO_CONFIRMATIONS")
    @classmethod
    def safe_statistics_funding_zero_confirmations(cls, value: int) -> int:
        return max(2, min(5, int(value)))

    @field_validator("STATISTICS_FUNDING_ZERO_GRACE_SEC")
    @classmethod
    def safe_statistics_funding_zero_grace(cls, value: int) -> int:
        return max(60, min(86_400, int(value)))

    @field_validator("STATISTICS_FUNDING_MAX_RECOVERY_ATTEMPTS")
    @classmethod
    def safe_statistics_funding_recovery_attempts(cls, value: int) -> int:
        return max(2, min(30, int(value)))

    @field_validator("STATISTICS_FUNDING_RECOVERY_DEADLINE_SEC")
    @classmethod
    def safe_statistics_funding_recovery_deadline(cls, value: int) -> int:
        return max(900, min(604_800, int(value)))

    @field_validator("FINANCIAL_RECONCILIATION_WORKERS")
    @classmethod
    def one_financial_reconciliation_worker(cls, value: int) -> int:
        # Exact fill history is intentionally serialized and lower-priority than
        # ENTRY. More workers would add no safety value and could increase API
        # pressure during bursts of simultaneous terminal closes.
        return 1

    @field_validator("FINANCIAL_RECONCILIATION_QUEUE_MAX")
    @classmethod
    def safe_financial_reconciliation_queue_max(cls, value: int) -> int:
        return max(20, min(10_000, int(value)))

    @field_validator("FINANCIAL_RECONCILIATION_MAX_ATTEMPTS")
    @classmethod
    def safe_financial_reconciliation_attempts(cls, value: int) -> int:
        return max(1, min(20, int(value)))

    @field_validator("FINANCIAL_RECONCILIATION_STALE_PROCESSING_SEC")
    @classmethod
    def safe_financial_reconciliation_stale_sec(cls, value: int) -> int:
        return max(30, min(3600, int(value)))

    @field_validator("FINANCIAL_RECONCILIATION_DEADLINE_SEC")
    @classmethod
    def safe_financial_reconciliation_deadline_sec(cls, value: int) -> int:
        return max(30, min(86_400, int(value)))

    @field_validator("FINANCIAL_RECONCILIATION_REQUESTS_PER_SECOND")
    @classmethod
    def safe_financial_reconciliation_rps(cls, value: float) -> float:
        value = _finite_setting(
            value, name="FINANCIAL_RECONCILIATION_REQUESTS_PER_SECOND"
        )
        return max(0.1, min(2.0, value))

    @field_validator("FINANCIAL_RECONCILIATION_LOOKBACK_SEC")
    @classmethod
    def safe_financial_reconciliation_lookback(cls, value: int) -> int:
        return max(3600, min(15_552_000, int(value)))

    @field_validator("FINANCIAL_RECONCILIATION_SHUTDOWN_TIMEOUT_SECONDS")
    @classmethod
    def safe_financial_reconciliation_shutdown_timeout(cls, value: float) -> float:
        value = _finite_setting(
            value, name="FINANCIAL_RECONCILIATION_SHUTDOWN_TIMEOUT_SECONDS"
        )
        return max(1.0, min(60.0, value))

    @field_validator("BINGX_API_TAKER_FEE_RATE", "MEXC_API_TAKER_FEE_RATE")
    @classmethod
    def safe_taker_fee_rate(cls, value: float) -> float:
        value = _finite_setting(value, name="MEXC_API_TAKER_FEE_RATE")
        if value < 0 or value > 0.05:
            raise ValueError("BINGX_API_TAKER_FEE_RATE должен быть от 0 до 0.05")
        return value

    @field_validator("BE_FEE_BUFFER_PERCENT", "BE_PLUS_PERCENT")
    @classmethod
    def safe_be_percent(cls, value: float) -> float:
        value = _finite_setting(value, name="BE_PERCENT")
        if value < 0 or value > 10:
            raise ValueError("BE_FEE_BUFFER_PERCENT/BE_PLUS_PERCENT: от 0 до 10")
        return value

    @field_validator("MIN_TP_RR")
    @classmethod
    def safe_min_tp_rr(cls, value: float) -> float:
        value = _finite_setting(value, name="MIN_TP_RR")
        if value < 0 or value > 100:
            raise ValueError("MIN_TP_RR должен быть от 0 до 100")
        return value

    @field_validator("VIP_MAX_SIGNAL_AGE_SECONDS")
    @classmethod
    def safe_signal_age(cls, value: int) -> int:
        return max(0, min(86400, int(value)))

    @field_validator("VIP_SIGNAL_DEDUP_TTL_SECONDS")
    @classmethod
    def safe_dedup_ttl(cls, value: int) -> int:
        return max(0, min(31_536_000, int(value)))

    @field_validator("LIMIT_ORDER_TTL_HOURS")
    @classmethod
    def safe_legacy_limit_ttl(cls, value: int) -> int:
        return max(0, min(168, int(value)))

    @field_validator(
        "EXCHANGE_TRANSIENT_ERROR_NOTIFY_EVERY",
        "EXCHANGE_TRANSIENT_ERROR_MAX_RETRIES",
    )
    @classmethod
    def safe_transient_retry_setting(cls, value: int) -> int:
        return max(1, min(10_000, int(value)))

    @field_validator("MARKET_EVENT_ROLLOUT_STAGE")
    @classmethod
    def safe_market_event_rollout_stage(cls, value: str) -> str:
        normalized = str(value or "off").strip().lower().replace("-", "_")
        aliases = {"group": "group_1541", "1541": "group_1541", "production": "global"}
        normalized = aliases.get(normalized, normalized)
        if normalized not in {"off", "shadow", "group_1541", "global"}:
            raise ValueError(
                "MARKET_EVENT_ROLLOUT_STAGE должен быть off/shadow/group_1541/global"
            )
        return normalized

    @field_validator(
        "MARKET_EVENT_MIGRATION_MIN_ATTEMPTS",
        "MARKET_EVENT_MIGRATION_BATCH_SIZE",
    )
    @classmethod
    def safe_market_event_migration_limits(cls, value: int) -> int:
        return max(1, min(1000, int(value)))

    @field_validator("MARKET_EVENT_MIGRATION_INTERVAL_SEC")
    @classmethod
    def safe_market_event_migration_interval(cls, value: float) -> float:
        return max(5.0, min(3600.0, _finite_setting(value, name="MARKET_EVENT_MIGRATION_INTERVAL_SEC")))

    @field_validator("LOG_LEVEL")
    @classmethod
    def safe_log_level(cls, value: str) -> str:
        normalized = str(value or "INFO").strip().upper()
        if normalized not in {"CRITICAL", "ERROR", "WARNING", "INFO", "DEBUG"}:
            raise ValueError("LOG_LEVEL должен быть CRITICAL/ERROR/WARNING/INFO/DEBUG")
        return normalized

    @model_validator(mode="after")
    def validate_related_limits(self):
        if self.ADMIN_ONLY_MODE and not self.admin_ids:
            raise ValueError("ADMIN_ONLY_MODE=true требует непустой ADMIN_IDS")
        if self.POSTGRES_POOL_MIN_SIZE > self.POSTGRES_POOL_MAX_SIZE:
            raise ValueError(
                "POSTGRES_POOL_MIN_SIZE не может быть больше POSTGRES_POOL_MAX_SIZE"
            )
        if self.BINGX_HTTP_MAX_KEEPALIVE_CONNECTIONS > self.BINGX_HTTP_MAX_CONNECTIONS:
            raise ValueError(
                "BINGX_HTTP_MAX_KEEPALIVE_CONNECTIONS не может быть больше "
                "BINGX_HTTP_MAX_CONNECTIONS"
            )
        if self.MEXC_HTTP_MAX_KEEPALIVE_CONNECTIONS > self.MEXC_HTTP_MAX_CONNECTIONS:
            raise ValueError(
                "MEXC_HTTP_MAX_KEEPALIVE_CONNECTIONS не может быть больше "
                "MEXC_HTTP_MAX_CONNECTIONS"
            )
        stage = self.MARKET_EVENT_ROLLOUT_STAGE
        if stage == "shadow":
            required_shadow = {
                "MARKET_EVENT_EVIDENCE_SNAPSHOT_ENABLED": self.MARKET_EVENT_EVIDENCE_SNAPSHOT_ENABLED,
                "MARKET_EVENT_SPLIT_ENTRY_TP_STATE_ENABLED": self.MARKET_EVENT_SPLIT_ENTRY_TP_STATE_ENABLED,
                "MARKET_EVENT_READ_COALESCING_ENABLED": self.MARKET_EVENT_READ_COALESCING_ENABLED,
            }
            missing_shadow = [name for name, enabled in required_shadow.items() if not enabled]
            if missing_shadow:
                raise ValueError(
                    "market-event shadow rollout требует true: " + ", ".join(missing_shadow)
                )
            if self.MARKET_EVENT_TERMINAL_REVIEW_ENABLED:
                raise ValueError("market-event shadow rollout требует MARKET_EVENT_TERMINAL_REVIEW_ENABLED=false")
            if self.MARKET_EVENT_MIGRATION_ENABLED:
                raise ValueError("market-event shadow rollout не должен изменять старые события")
            if self.MARKET_EVENT_MANUAL_RESOLUTION_ENABLED:
                raise ValueError("market-event shadow rollout требует MARKET_EVENT_MANUAL_RESOLUTION_ENABLED=false")
        elif stage in {"group_1541", "global"}:
            required = {
                "MARKET_EVENT_EVIDENCE_SNAPSHOT_ENABLED": self.MARKET_EVENT_EVIDENCE_SNAPSHOT_ENABLED,
                "MARKET_EVENT_SPLIT_ENTRY_TP_STATE_ENABLED": self.MARKET_EVENT_SPLIT_ENTRY_TP_STATE_ENABLED,
                "MARKET_EVENT_TERMINAL_REVIEW_ENABLED": self.MARKET_EVENT_TERMINAL_REVIEW_ENABLED,
                "MARKET_EVENT_READ_COALESCING_ENABLED": self.MARKET_EVENT_READ_COALESCING_ENABLED,
                "MARKET_EVENT_MIGRATION_ENABLED": self.MARKET_EVENT_MIGRATION_ENABLED,
                "MARKET_EVENT_MANUAL_RESOLUTION_ENABLED": self.MARKET_EVENT_MANUAL_RESOLUTION_ENABLED,
            }
            missing = [name for name, enabled in required.items() if not enabled]
            if missing:
                raise ValueError(
                    "market-event rollout stage требует true: " + ", ".join(missing)
                )
            if self.MARKET_EVENT_MIGRATION_TARGET_GROUP_ID <= 0:
                raise ValueError("MARKET_EVENT_MIGRATION_TARGET_GROUP_ID должен быть положительным")
            if stage == "group_1541" and self.MARKET_EVENT_MIGRATION_BATCH_SIZE != 1:
                raise ValueError("group_1541 rollout требует MARKET_EVENT_MIGRATION_BATCH_SIZE=1")
        return self

    @property
    def enabled_exchanges(self) -> list[str]:
        return ["bingx"]

    def is_exchange_enabled(self, exchange: str) -> bool:
        return (exchange or "").lower().strip() in {"bingx", "mexc"}

    @property
    def safe_default_exchange(self) -> str:
        return "bingx"

    @property
    def admin_ids(self) -> List[int]:
        """Return configured Telegram administrator IDs.

        Railway users often paste values with surrounding single or double
        quotes.  Environment parsers usually remove them, but some deployment
        paths preserve the quote characters.  Strip those characters here so
        quoted and unquoted ADMIN_IDS values behave identically.
        """
        out: List[int] = []
        for raw in (self.ADMIN_IDS or "").replace(";", ",").split(","):
            raw = raw.strip().strip('"').strip("'").strip()
            if raw.lstrip("-").isdigit():
                out.append(int(raw))
        return out

    @property
    def allowed_source_chat_ids(self) -> List[int]:
        out: List[int] = []
        for raw in (
            (self.VIP_ALLOWED_SOURCE_CHAT_IDS or "").replace(";", ",").split(",")
        ):
            raw = raw.strip()
            if raw.lstrip("-").isdigit():
                out.append(int(raw))
        return out

    @property
    def allowed_sender_chat_titles(self) -> List[str]:
        out: List[str] = []
        for raw in (
            (self.VIP_ALLOWED_SENDER_CHAT_TITLES or "").replace(";", ",").split(",")
        ):
            raw = raw.strip().lower().lstrip("@")
            if raw:
                out.append(raw)
        return out


@lru_cache()
def get_settings() -> Settings:
    return Settings()
