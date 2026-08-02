from __future__ import annotations

from functools import lru_cache
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from pydantic import Field, SecretStr, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    bot_token: str = Field(alias="BOT_TOKEN")
    admin_id: int = Field(alias="ADMIN_ID")
    api_id: int = Field(alias="API_ID")
    api_hash: str = Field(alias="API_HASH")
    session_encryption_key: str = Field(alias="SESSION_ENCRYPTION_KEY")
    database_url: str = Field(default="sqlite+aiosqlite:///./data/laika.db", alias="DATABASE_URL")

    # Every AI capability remains fail-closed until its own implementation,
    # explicit Railway permission and explicit administrator preference in DB.
    ai_comments_enabled: bool = Field(default=False, alias="AI_COMMENTS_ENABLED")
    ai_comments_mode: str = Field(default="preview_only", alias="AI_COMMENTS_MODE")
    ai_generation_enabled: bool = Field(default=False, alias="AI_GENERATION_ENABLED")
    ai_dialogues_enabled: bool = Field(default=False, alias="AI_DIALOGUES_ENABLED")
    ai_publication_enabled: bool = Field(default=False, alias="AI_PUBLICATION_ENABLED")
    ai_debug_snapshots_enabled: bool = Field(
        default=False, alias="AI_DEBUG_SNAPSHOTS_ENABLED"
    )

    # Step 10 installed the fail-closed OpenAI Responses API gateway; Step 11
    # reuses it for one explicit draft. The secret is never persisted in the DB.
    openai_api_key: SecretStr | None = Field(default=None, alias="OPENAI_API_KEY")
    openai_model: str = Field(default="gpt-5.6-luna", alias="OPENAI_MODEL")
    openai_gateway_enabled: bool = Field(
        default=False, alias="OPENAI_GATEWAY_ENABLED"
    )
    openai_request_timeout_seconds: float = Field(
        default=30.0, alias="OPENAI_REQUEST_TIMEOUT_SECONDS"
    )
    openai_max_retries: int = Field(default=2, alias="OPENAI_MAX_RETRIES")

    # Step 11: one explicit draft only. These limits are bounded and have safe
    # defaults, so existing Railway deployments do not need new variables.
    ai_generation_max_output_tokens: int = Field(
        default=512, alias="AI_GENERATION_MAX_OUTPUT_TOKENS"
    )
    ai_generation_recent_posts: int = Field(
        default=5, alias="AI_GENERATION_RECENT_POSTS"
    )
    ai_generation_knowledge_chunks: int = Field(
        default=4, alias="AI_GENERATION_KNOWLEDGE_CHUNKS"
    )

    # Step 12: finite dialogue drafts and calendar-day profile quotas.
    ai_dialogue_max_messages: int = Field(default=5, alias="AI_DIALOGUE_MAX_MESSAGES")
    ai_dialogue_min_interval_seconds: int = Field(default=60, alias="AI_DIALOGUE_MIN_INTERVAL_SECONDS")
    ai_dialogue_max_interval_seconds: int = Field(default=600, alias="AI_DIALOGUE_MAX_INTERVAL_SECONDS")
    ai_dialogue_expires_hours: int = Field(default=24, alias="AI_DIALOGUE_EXPIRES_HOURS")
    ai_comments_timezone: str = Field(default="Europe/Moscow", alias="AI_COMMENTS_TIMEZONE")

    monitor_interval_seconds: int = Field(default=30, alias="MONITOR_INTERVAL_SECONDS")
    worker_interval_seconds: int = Field(default=3, alias="WORKER_INTERVAL_SECONDS")
    default_reaction_delay_min_seconds: int = Field(default=300, alias="DEFAULT_REACTION_DELAY_MIN_SECONDS")
    default_reaction_delay_max_seconds: int = Field(default=1800, alias="DEFAULT_REACTION_DELAY_MAX_SECONDS")
    join_delay_min_seconds: int = Field(default=900, alias="JOIN_DELAY_MIN_SECONDS")
    join_delay_max_seconds: int = Field(default=1800, alias="JOIN_DELAY_MAX_SECONDS")
    max_accounts_per_channel: int = Field(default=100, alias="MAX_ACCOUNTS_PER_CHANNEL")
    max_old_posts: int = Field(default=50, alias="MAX_OLD_POSTS")
    max_reaction_attempts: int = Field(default=5, alias="MAX_REACTION_ATTEMPTS")
    reaction_attempt_timeout_seconds: int = Field(
        default=180, alias="REACTION_ATTEMPT_TIMEOUT_SECONDS"
    )
    job_history_retention_days: int = Field(
        default=90, alias="JOB_HISTORY_RETENTION_DAYS"
    )
    job_history_cleanup_interval_seconds: int = Field(
        default=86400, alias="JOB_HISTORY_CLEANUP_INTERVAL_SECONDS"
    )
    job_history_cleanup_batch_size: int = Field(
        default=10000, alias="JOB_HISTORY_CLEANUP_BATCH_SIZE"
    )

    # Automatic recovery. It only requeues proven stale jobs, rechecks saved
    # sessions through the existing identity boundary and restarts core workers
    # within a bounded local budget. It never requests login codes or bypasses
    # Telegram limits.
    auto_recovery_enabled: bool = Field(
        default=True, alias="AUTO_RECOVERY_ENABLED"
    )
    recovery_check_interval_seconds: int = Field(
        default=60, alias="RECOVERY_CHECK_INTERVAL_SECONDS"
    )
    recovery_stuck_after_seconds: int = Field(
        default=600, alias="RECOVERY_STUCK_AFTER_SECONDS"
    )
    recovery_max_jobs_per_cycle: int = Field(
        default=100, alias="RECOVERY_MAX_JOBS_PER_CYCLE"
    )
    recovery_account_recheck_seconds: int = Field(
        default=1800, alias="RECOVERY_ACCOUNT_RECHECK_SECONDS"
    )
    recovery_quarantine_recheck_seconds: int = Field(
        default=21600, alias="RECOVERY_QUARANTINE_RECHECK_SECONDS"
    )
    recovery_max_accounts_per_cycle: int = Field(
        default=5, alias="RECOVERY_MAX_ACCOUNTS_PER_CYCLE"
    )
    recovery_account_check_timeout_seconds: int = Field(
        default=30, alias="RECOVERY_ACCOUNT_CHECK_TIMEOUT_SECONDS"
    )
    recovery_worker_max_restarts: int = Field(
        default=3, alias="RECOVERY_WORKER_MAX_RESTARTS"
    )
    recovery_worker_restart_window_seconds: int = Field(
        default=900, alias="RECOVERY_WORKER_RESTART_WINDOW_SECONDS"
    )
    recovery_worker_restart_backoff_seconds: int = Field(
        default=5, alias="RECOVERY_WORKER_RESTART_BACKOFF_SECONDS"
    )
    recovery_worker_restart_cooldown_seconds: int = Field(
        default=300, alias="RECOVERY_WORKER_RESTART_COOLDOWN_SECONDS"
    )
    membership_attempt_timeout_seconds: int = Field(
        default=180, alias="MEMBERSHIP_ATTEMPT_TIMEOUT_SECONDS"
    )
    view_attempt_timeout_seconds: int = Field(
        default=180, alias="VIEW_ATTEMPT_TIMEOUT_SECONDS"
    )

    # Critical administrator alerts. These defaults are deliberately conservative:
    # alerts describe proven operational incidents and never alter Telegram actions.
    alerts_enabled: bool = Field(default=True, alias="ALERTS_ENABLED")
    alert_check_interval_seconds: int = Field(
        default=60, alias="ALERT_CHECK_INTERVAL_SECONDS"
    )
    alert_repeat_seconds: int = Field(default=3600, alias="ALERT_REPEAT_SECONDS")
    alert_send_timeout_seconds: int = Field(
        default=15, alias="ALERT_SEND_TIMEOUT_SECONDS"
    )
    alert_startup_grace_seconds: int = Field(
        default=180, alias="ALERT_STARTUP_GRACE_SECONDS"
    )
    alert_worker_warning_seconds: int = Field(
        default=300, alias="ALERT_WORKER_WARNING_SECONDS"
    )
    alert_queue_backlog_threshold: int = Field(
        default=25, alias="ALERT_QUEUE_BACKLOG_THRESHOLD"
    )
    alert_flood_account_threshold: int = Field(
        default=5, alias="ALERT_FLOOD_ACCOUNT_THRESHOLD"
    )
    alert_failure_threshold: int = Field(
        default=10, alias="ALERT_FAILURE_THRESHOLD"
    )
    alert_failure_window_minutes: int = Field(
        default=15, alias="ALERT_FAILURE_WINDOW_MINUTES"
    )
    alert_persisted_error_threshold: int = Field(
        default=5, alias="ALERT_PERSISTED_ERROR_THRESHOLD"
    )
    log_level: str = Field(default="INFO", alias="LOG_LEVEL")

    @field_validator(
        "monitor_interval_seconds",
        "worker_interval_seconds",
        "default_reaction_delay_min_seconds",
        "default_reaction_delay_max_seconds",
        "join_delay_min_seconds",
        "join_delay_max_seconds",
        "max_accounts_per_channel",
        "max_old_posts",
        "max_reaction_attempts",
        "reaction_attempt_timeout_seconds",
        "job_history_retention_days",
        "job_history_cleanup_interval_seconds",
        "job_history_cleanup_batch_size",
        "alert_check_interval_seconds",
        "alert_repeat_seconds",
        "alert_send_timeout_seconds",
        "alert_worker_warning_seconds",
        "alert_queue_backlog_threshold",
        "alert_flood_account_threshold",
        "alert_failure_threshold",
        "alert_failure_window_minutes",
        "alert_persisted_error_threshold",
        "recovery_check_interval_seconds",
        "recovery_stuck_after_seconds",
        "recovery_max_jobs_per_cycle",
        "recovery_account_recheck_seconds",
        "recovery_quarantine_recheck_seconds",
        "recovery_max_accounts_per_cycle",
        "recovery_account_check_timeout_seconds",
        "recovery_worker_restart_window_seconds",
        "recovery_worker_restart_backoff_seconds",
        "recovery_worker_restart_cooldown_seconds",
        "membership_attempt_timeout_seconds",
        "view_attempt_timeout_seconds",
    )
    @classmethod
    def positive(cls, value: int) -> int:
        if value <= 0:
            raise ValueError("Значение должно быть больше нуля")
        return value

    @field_validator(
        "alert_startup_grace_seconds", "recovery_worker_max_restarts"
    )
    @classmethod
    def non_negative(cls, value: int) -> int:
        if value < 0:
            raise ValueError("Значение не может быть отрицательным")
        return value

    @field_validator("openai_api_key", mode="before")
    @classmethod
    def normalize_openai_api_key(cls, value: object) -> object:
        if value is None:
            return None
        if isinstance(value, SecretStr):
            text = value.get_secret_value().strip()
        else:
            text = str(value).strip()
        if not text:
            return None
        if any(ord(char) < 32 or ord(char) == 127 for char in text):
            raise ValueError("OPENAI_API_KEY содержит управляющие символы")
        if len(text) > 512:
            raise ValueError("OPENAI_API_KEY слишком длинный")
        return text

    @field_validator("openai_model")
    @classmethod
    def validate_openai_model(cls, value: str) -> str:
        model = value.strip()
        if not model or len(model) > 96:
            raise ValueError("OPENAI_MODEL должен содержать от 1 до 96 символов")
        allowed = set("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789._:-")
        if any(char not in allowed for char in model):
            raise ValueError("OPENAI_MODEL содержит недопустимые символы")
        return model

    @field_validator("openai_request_timeout_seconds")
    @classmethod
    def validate_openai_timeout(cls, value: float) -> float:
        timeout = float(value)
        if not 1.0 <= timeout <= 120.0:
            raise ValueError(
                "OPENAI_REQUEST_TIMEOUT_SECONDS должен быть от 1 до 120"
            )
        return timeout

    @field_validator("openai_max_retries")
    @classmethod
    def validate_openai_retries(cls, value: int) -> int:
        if isinstance(value, bool):
            raise ValueError("OPENAI_MAX_RETRIES должен быть целым числом")
        retries = int(value)
        if not 0 <= retries <= 3:
            raise ValueError("OPENAI_MAX_RETRIES должен быть от 0 до 3")
        return retries

    @field_validator("ai_generation_max_output_tokens")
    @classmethod
    def validate_ai_generation_max_output_tokens(cls, value: int) -> int:
        if isinstance(value, bool):
            raise ValueError("AI_GENERATION_MAX_OUTPUT_TOKENS должен быть целым числом")
        parsed = int(value)
        if not 128 <= parsed <= 2048:
            raise ValueError("AI_GENERATION_MAX_OUTPUT_TOKENS должен быть от 128 до 2048")
        return parsed

    @field_validator("ai_generation_recent_posts")
    @classmethod
    def validate_ai_generation_recent_posts(cls, value: int) -> int:
        if isinstance(value, bool):
            raise ValueError("AI_GENERATION_RECENT_POSTS должен быть целым числом")
        parsed = int(value)
        if not 0 <= parsed <= 8:
            raise ValueError("AI_GENERATION_RECENT_POSTS должен быть от 0 до 8")
        return parsed

    @field_validator("ai_generation_knowledge_chunks")
    @classmethod
    def validate_ai_generation_knowledge_chunks(cls, value: int) -> int:
        if isinstance(value, bool):
            raise ValueError("AI_GENERATION_KNOWLEDGE_CHUNKS должен быть целым числом")
        parsed = int(value)
        if not 0 <= parsed <= 8:
            raise ValueError("AI_GENERATION_KNOWLEDGE_CHUNKS должен быть от 0 до 8")
        return parsed


    @field_validator("ai_dialogue_max_messages")
    @classmethod
    def validate_ai_dialogue_max_messages(cls, value: int) -> int:
        parsed = int(value)
        if not 2 <= parsed <= 5:
            raise ValueError("AI_DIALOGUE_MAX_MESSAGES должен быть от 2 до 5")
        return parsed

    @field_validator("ai_dialogue_min_interval_seconds", "ai_dialogue_max_interval_seconds")
    @classmethod
    def validate_ai_dialogue_interval(cls, value: int) -> int:
        parsed = int(value)
        if not 0 <= parsed <= 86_400:
            raise ValueError("Интервал диалога должен быть от 0 до 86400 секунд")
        return parsed

    @field_validator("ai_dialogue_expires_hours")
    @classmethod
    def validate_ai_dialogue_expires_hours(cls, value: int) -> int:
        parsed = int(value)
        if not 1 <= parsed <= 168:
            raise ValueError("AI_DIALOGUE_EXPIRES_HOURS должен быть от 1 до 168")
        return parsed

    @field_validator("ai_comments_timezone")
    @classmethod
    def validate_ai_comments_timezone(cls, value: str) -> str:
        name = value.strip()
        if not name or len(name) > 64:
            raise ValueError("AI_COMMENTS_TIMEZONE содержит некорректное значение")
        try:
            ZoneInfo(name)
        except ZoneInfoNotFoundError as exc:
            raise ValueError("AI_COMMENTS_TIMEZONE не найден") from exc
        return name

    @field_validator("database_url")
    @classmethod
    def normalize_database_url(cls, value: str) -> str:
        if value.startswith("postgres://"):
            return value.replace("postgres://", "postgresql+asyncpg://", 1)
        if value.startswith("postgresql://") and "+asyncpg" not in value:
            return value.replace("postgresql://", "postgresql+asyncpg://", 1)
        return value

    def openai_api_key_value(self) -> str | None:
        secret = self.openai_api_key
        return secret.get_secret_value() if secret is not None else None

    def validate_ranges(self) -> None:
        if self.default_reaction_delay_min_seconds > self.default_reaction_delay_max_seconds:
            raise ValueError("DEFAULT_REACTION_DELAY_MIN_SECONDS больше максимума")
        if self.join_delay_min_seconds > self.join_delay_max_seconds:
            raise ValueError("JOIN_DELAY_MIN_SECONDS больше максимума")
        if self.ai_dialogue_min_interval_seconds > self.ai_dialogue_max_interval_seconds:
            raise ValueError("AI_DIALOGUE_MIN_INTERVAL_SECONDS больше максимума")


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    settings = Settings()
    settings.validate_ranges()
    return settings
