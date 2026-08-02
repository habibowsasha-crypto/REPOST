from __future__ import annotations

from datetime import datetime
from decimal import Decimal

from sqlalchemy import (
    BigInteger,
    Boolean,
    CheckConstraint,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    Numeric,
    String,
    Text,
    UniqueConstraint,
)
from sqlalchemy.orm import Mapped, mapped_column

from .models import Base, utcnow

AI_COMMENTS_SCHEMA_VERSION = 4
AI_COMMENTS_TABLE_NAMES = (
    "ai_channel_profiles",
    "ai_channel_posts",
    "ai_channel_post_revisions",
    "ai_channel_scenarios",
    "ai_account_profiles",
    "ai_account_profile_revisions",
    "ai_comment_threads",
    "ai_comment_thread_plans",
    "ai_comment_quota_events",
    "ai_comment_messages",
    "ai_generation_jobs",
    "ai_comment_drafts",
    "ai_publication_jobs",
    "ai_knowledge_sources",
    "ai_knowledge_chunks",
    "ai_usage_stats",
    "ai_settings",
)


class AIChannelProfile(Base):
    __tablename__ = "ai_channel_profiles"
    __table_args__ = (
        UniqueConstraint("channel_id", name="uq_ai_channel_profiles_channel"),
        CheckConstraint(
            "profile_version > 0", name="ck_ai_channel_profiles_version"
        ),
        Index("ix_ai_channel_profiles_enabled", "enabled", "id"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    channel_id: Mapped[int | None] = mapped_column(
        ForeignKey(
            "channels.id",
            name="fk_ai_channel_profiles_channel",
            ondelete="SET NULL",
        ),
        nullable=True,
    )
    telegram_channel_id: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    description: Mapped[str | None] = mapped_column(Text, nullable=True)
    topic: Mapped[str | None] = mapped_column(String(128), nullable=True)
    author_style_json: Mapped[str] = mapped_column(
        Text, default="{}", server_default="{}", nullable=False
    )
    audience_style_json: Mapped[str] = mapped_column(
        Text, default="{}", server_default="{}", nullable=False
    )
    methodology_json: Mapped[str] = mapped_column(
        Text, default="[]", server_default="[]", nullable=False
    )
    allowed_topics_json: Mapped[str] = mapped_column(
        Text, default="[]", server_default="[]", nullable=False
    )
    forbidden_topics_json: Mapped[str] = mapped_column(
        Text, default="[]", server_default="[]", nullable=False
    )
    admin_notes: Mapped[str | None] = mapped_column(Text, nullable=True)
    memory_summary: Mapped[str | None] = mapped_column(Text, nullable=True)
    enabled: Mapped[bool] = mapped_column(
        Boolean, default=True, server_default="true", nullable=False
    )
    profile_version: Mapped[int] = mapped_column(
        Integer, default=1, server_default="1", nullable=False
    )
    created_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, default=utcnow, onupdate=utcnow, nullable=False
    )


class AIChannelScenario(Base):
    __tablename__ = "ai_channel_scenarios"
    __table_args__ = (
        CheckConstraint(
            "status IN ('active','completed','cancelled','unknown')",
            name="ck_ai_channel_scenarios_status",
        ),
        Index("ix_ai_channel_scenarios_channel_status", "channel_id", "status"),
        Index("ix_ai_channel_scenarios_symbol_status", "symbol", "status"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    channel_id: Mapped[int | None] = mapped_column(
        ForeignKey(
            "channels.id",
            name="fk_ai_channel_scenarios_channel",
            ondelete="SET NULL",
        ),
        nullable=True,
    )
    telegram_channel_id: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    title: Mapped[str] = mapped_column(String(255), nullable=False)
    symbol: Mapped[str | None] = mapped_column(String(32), nullable=True)
    direction: Mapped[str | None] = mapped_column(String(16), nullable=True)
    status: Mapped[str] = mapped_column(
        String(24), default="unknown", server_default="unknown", nullable=False
    )
    factual_summary: Mapped[str | None] = mapped_column(Text, nullable=True)
    # These stable local identifiers intentionally are not foreign keys. The post
    # table points to a scenario, and making both directions physical FKs would
    # introduce a DDL cycle that SQLite cannot migrate safely.
    opened_by_post_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    closed_by_post_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    started_at: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    ended_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, default=utcnow, onupdate=utcnow, nullable=False
    )


class AIChannelPost(Base):
    __tablename__ = "ai_channel_posts"
    __table_args__ = (
        UniqueConstraint(
            "telegram_channel_id",
            "telegram_message_id",
            name="uq_ai_channel_posts_telegram_message",
        ),
        CheckConstraint("source_revision > 0", name="ck_ai_channel_posts_revision"),
        CheckConstraint(
            "length(normalized_text_hash) = 64",
            name="ck_ai_channel_posts_text_hash",
        ),
        Index("ix_ai_channel_posts_channel_posted", "channel_id", "posted_at", "id"),
        Index("ix_ai_channel_posts_scenario", "linked_scenario_id", "id"),
        Index("ix_ai_channel_posts_deleted", "deleted_at", "id"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    channel_id: Mapped[int | None] = mapped_column(
        ForeignKey(
            "channels.id",
            name="fk_ai_channel_posts_channel",
            ondelete="SET NULL",
        ),
        nullable=True,
    )
    telegram_channel_id: Mapped[int] = mapped_column(BigInteger, nullable=False)
    telegram_message_id: Mapped[int] = mapped_column(BigInteger, nullable=False)
    source_revision: Mapped[int] = mapped_column(
        Integer, default=1, server_default="1", nullable=False
    )
    posted_at: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    edited_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    text: Mapped[str | None] = mapped_column(Text, nullable=True)
    media_type: Mapped[str | None] = mapped_column(String(32), nullable=True)
    media_caption: Mapped[str | None] = mapped_column(Text, nullable=True)
    normalized_text_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    detected_topics_json: Mapped[str] = mapped_column(
        Text, default="[]", server_default="[]", nullable=False
    )
    linked_scenario_id: Mapped[int | None] = mapped_column(
        ForeignKey(
            "ai_channel_scenarios.id",
            name="fk_ai_channel_posts_scenario",
            ondelete="SET NULL",
        ),
        nullable=True,
    )
    deleted_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, default=utcnow, onupdate=utcnow, nullable=False
    )


class AIChannelPostRevision(Base):
    __tablename__ = "ai_channel_post_revisions"
    __table_args__ = (
        UniqueConstraint(
            "post_id",
            "source_revision",
            name="uq_ai_channel_post_revisions_post_revision",
        ),
        CheckConstraint(
            "source_revision > 0",
            name="ck_ai_channel_post_revisions_revision",
        ),
        CheckConstraint(
            "revision_reason IN ('ingested','edited','metadata','deleted',"
            "'restored','backfill')",
            name="ck_ai_channel_post_revisions_reason",
        ),
        CheckConstraint(
            "length(normalized_text_hash) = 64",
            name="ck_ai_channel_post_revisions_hash",
        ),
        Index(
            "ix_ai_channel_post_revisions_channel_message",
            "telegram_channel_id",
            "telegram_message_id",
            "source_revision",
        ),
        Index(
            "ix_ai_channel_post_revisions_post",
            "post_id",
            "source_revision",
        ),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    post_id: Mapped[int] = mapped_column(
        ForeignKey(
            "ai_channel_posts.id",
            name="fk_ai_channel_post_revisions_post",
            ondelete="RESTRICT",
        ),
        nullable=False,
    )
    channel_id: Mapped[int | None] = mapped_column(
        ForeignKey(
            "channels.id",
            name="fk_ai_channel_post_revisions_channel",
            ondelete="SET NULL",
        ),
        nullable=True,
    )
    telegram_channel_id: Mapped[int] = mapped_column(BigInteger, nullable=False)
    telegram_message_id: Mapped[int] = mapped_column(BigInteger, nullable=False)
    source_revision: Mapped[int] = mapped_column(Integer, nullable=False)
    revision_reason: Mapped[str] = mapped_column(String(16), nullable=False)
    posted_at: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    edited_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    text: Mapped[str | None] = mapped_column(Text, nullable=True)
    media_type: Mapped[str | None] = mapped_column(String(32), nullable=True)
    media_caption: Mapped[str | None] = mapped_column(Text, nullable=True)
    normalized_text_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    detected_topics_json: Mapped[str] = mapped_column(
        Text, default="[]", server_default="[]", nullable=False
    )
    deleted_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    recorded_at: Mapped[datetime] = mapped_column(
        DateTime, default=utcnow, nullable=False
    )


class AIAccountProfile(Base):
    __tablename__ = "ai_account_profiles"
    __table_args__ = (
        UniqueConstraint("account_id", name="uq_ai_account_profiles_account"),
        CheckConstraint(
            "min_length > 0 AND min_length <= 1000",
            name="ck_ai_account_profiles_min_length",
        ),
        CheckConstraint(
            "max_length >= min_length AND max_length <= 1000",
            name="ck_ai_account_profiles_length_range",
        ),
        CheckConstraint(
            "daily_limit >= 0 AND daily_limit <= 100",
            name="ck_ai_account_profiles_daily_limit",
        ),
        CheckConstraint(
            "cooldown_seconds >= 0 AND cooldown_seconds <= 604800",
            name="ck_ai_account_profiles_cooldown",
        ),
        CheckConstraint(
            "telegram_user_id IS NULL OR telegram_user_id > 0",
            name="ck_ai_account_profiles_telegram_user",
        ),
        CheckConstraint(
            "profile_version > 0", name="ck_ai_account_profiles_version"
        ),
        CheckConstraint(
            "emoji_rate >= 0 AND emoji_rate <= 1",
            name="ck_ai_account_profiles_emoji_rate",
        ),
        CheckConstraint(
            "question_rate >= 0 AND question_rate <= 1",
            name="ck_ai_account_profiles_question_rate",
        ),
        CheckConstraint(
            "reply_rate >= 0 AND reply_rate <= 1",
            name="ck_ai_account_profiles_reply_rate",
        ),
        CheckConstraint(
            "disagreement_rate >= 0 AND disagreement_rate <= 1",
            name="ck_ai_account_profiles_disagreement_rate",
        ),
        Index(
            "uq_ai_account_profiles_telegram_user",
            "telegram_user_id",
            unique=True,
        ),
        Index("ix_ai_account_profiles_enabled_role", "enabled", "role", "id"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    account_id: Mapped[int | None] = mapped_column(
        ForeignKey(
            "accounts.id",
            name="fk_ai_account_profiles_account",
            ondelete="SET NULL",
        ),
        nullable=True,
    )
    telegram_user_id: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    name: Mapped[str] = mapped_column(String(120), nullable=False)
    knowledge_level: Mapped[str] = mapped_column(String(32), nullable=False)
    role: Mapped[str] = mapped_column(String(32), nullable=False)
    style_json: Mapped[str] = mapped_column(
        Text, default="{}", server_default="{}", nullable=False
    )
    allowed_claims_json: Mapped[str] = mapped_column(
        Text, default="[]", server_default="[]", nullable=False
    )
    forbidden_claims_json: Mapped[str] = mapped_column(
        Text, default="[]", server_default="[]", nullable=False
    )
    min_length: Mapped[int] = mapped_column(
        Integer, default=8, server_default="8", nullable=False
    )
    max_length: Mapped[int] = mapped_column(
        Integer, default=240, server_default="240", nullable=False
    )
    emoji_rate: Mapped[Decimal] = mapped_column(
        Numeric(5, 4), default=Decimal("0"), server_default="0", nullable=False
    )
    question_rate: Mapped[Decimal] = mapped_column(
        Numeric(5, 4), default=Decimal("0"), server_default="0", nullable=False
    )
    reply_rate: Mapped[Decimal] = mapped_column(
        Numeric(5, 4), default=Decimal("0"), server_default="0", nullable=False
    )
    disagreement_rate: Mapped[Decimal] = mapped_column(
        Numeric(5, 4), default=Decimal("0"), server_default="0", nullable=False
    )
    daily_limit: Mapped[int] = mapped_column(
        Integer, default=0, server_default="0", nullable=False
    )
    cooldown_seconds: Mapped[int] = mapped_column(
        Integer, default=0, server_default="0", nullable=False
    )
    enabled: Mapped[bool] = mapped_column(
        Boolean, default=False, server_default="false", nullable=False
    )
    profile_version: Mapped[int] = mapped_column(
        Integer, default=1, server_default="1", nullable=False
    )
    created_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, default=utcnow, onupdate=utcnow, nullable=False
    )


class AIAccountProfileRevision(Base):
    __tablename__ = "ai_account_profile_revisions"
    __table_args__ = (
        UniqueConstraint(
            "profile_id",
            "profile_version",
            name="uq_ai_account_profile_revisions_profile_version",
        ),
        CheckConstraint(
            "profile_version > 0",
            name="ck_ai_account_profile_revisions_version",
        ),
        CheckConstraint(
            "change_reason IN ('auto_created','backfill','updated','regenerated',"
            "'enabled','disabled','retired','restored','reattached')",
            name="ck_ai_account_profile_revisions_reason",
        ),
        CheckConstraint(
            "length(snapshot_hash) = 64",
            name="ck_ai_account_profile_revisions_hash",
        ),
        Index(
            "ix_ai_account_profile_revisions_identity_version",
            "telegram_user_id",
            "profile_version",
        ),
        Index(
            "ix_ai_account_profile_revisions_profile_created",
            "profile_id",
            "created_at",
            "id",
        ),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    profile_id: Mapped[int] = mapped_column(
        ForeignKey(
            "ai_account_profiles.id",
            name="fk_ai_account_profile_revisions_profile",
            ondelete="RESTRICT",
        ),
        nullable=False,
    )
    telegram_user_id: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    profile_version: Mapped[int] = mapped_column(Integer, nullable=False)
    change_reason: Mapped[str] = mapped_column(String(24), nullable=False)
    snapshot_json: Mapped[str] = mapped_column(Text, nullable=False)
    snapshot_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    changed_by: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime, default=utcnow, nullable=False
    )


class AICommentThread(Base):
    __tablename__ = "ai_comment_threads"
    __table_args__ = (
        UniqueConstraint("root_post_id", name="uq_ai_comment_threads_root_post"),
        CheckConstraint(
            "status IN ('planned','generating','review','approved','publishing',"
            "'active','completed','cancelled')",
            name="ck_ai_comment_threads_status",
        ),
        CheckConstraint(
            "max_messages >= 1 AND max_messages <= 5",
            name="ck_ai_comment_threads_max_messages",
        ),
        CheckConstraint("version > 0", name="ck_ai_comment_threads_version"),
        Index("ix_ai_comment_threads_channel_status", "channel_id", "status", "id"),
        Index("ix_ai_comment_threads_status_expires", "status", "expires_at", "id"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    channel_id: Mapped[int | None] = mapped_column(
        ForeignKey(
            "channels.id",
            name="fk_ai_comment_threads_channel",
            ondelete="SET NULL",
        ),
        nullable=True,
    )
    root_post_id: Mapped[int | None] = mapped_column(
        ForeignKey(
            "ai_channel_posts.id",
            name="fk_ai_comment_threads_root_post",
            ondelete="SET NULL",
        ),
        nullable=True,
    )
    status: Mapped[str] = mapped_column(
        String(24), default="planned", server_default="planned", nullable=False
    )
    max_messages: Mapped[int] = mapped_column(
        Integer, default=5, server_default="5", nullable=False
    )
    expires_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    created_by_admin_id: Mapped[int] = mapped_column(BigInteger, nullable=False)
    version: Mapped[int] = mapped_column(
        Integer, default=1, server_default="1", nullable=False
    )
    created_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, default=utcnow, onupdate=utcnow, nullable=False
    )


class AICommentThreadPlan(Base):
    __tablename__ = "ai_comment_thread_plans"
    __table_args__ = (
        UniqueConstraint("thread_id", name="uq_ai_comment_thread_plans_thread"),
        CheckConstraint("root_post_revision > 0", name="ck_ai_comment_thread_plans_revision"),
        CheckConstraint("length(root_post_hash) = 64", name="ck_ai_comment_thread_plans_hash"),
        CheckConstraint("next_position >= 1 AND next_position <= 6", name="ck_ai_comment_thread_plans_next"),
        CheckConstraint("accepted_messages >= 0 AND accepted_messages <= 5", name="ck_ai_comment_thread_plans_accepted"),
        CheckConstraint("min_interval_seconds >= 0", name="ck_ai_comment_thread_plans_min_interval"),
        CheckConstraint("max_interval_seconds >= min_interval_seconds", name="ck_ai_comment_thread_plans_interval_range"),
        CheckConstraint("version > 0", name="ck_ai_comment_thread_plans_version"),
        Index("ix_ai_comment_thread_plans_next", "next_position", "id"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    thread_id: Mapped[int] = mapped_column(
        ForeignKey(
            "ai_comment_threads.id",
            name="fk_ai_comment_thread_plans_thread",
            ondelete="RESTRICT",
        ),
        nullable=False,
    )
    root_post_revision: Mapped[int] = mapped_column(Integer, nullable=False)
    root_post_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    participant_profile_ids_json: Mapped[str] = mapped_column(Text, nullable=False)
    message_plan_json: Mapped[str] = mapped_column(Text, nullable=False)
    topic: Mapped[str | None] = mapped_column(String(128), nullable=True)
    next_position: Mapped[int] = mapped_column(Integer, default=1, server_default="1", nullable=False)
    accepted_messages: Mapped[int] = mapped_column(Integer, default=0, server_default="0", nullable=False)
    min_interval_seconds: Mapped[int] = mapped_column(Integer, default=60, server_default="60", nullable=False)
    max_interval_seconds: Mapped[int] = mapped_column(Integer, default=600, server_default="600", nullable=False)
    cancel_reason_safe: Mapped[str | None] = mapped_column(Text, nullable=True)
    version: Mapped[int] = mapped_column(Integer, default=1, server_default="1", nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, default=utcnow, onupdate=utcnow, nullable=False
    )


class AICommentQuotaEvent(Base):
    __tablename__ = "ai_comment_quota_events"
    __table_args__ = (
        UniqueConstraint("idempotency_key", name="uq_ai_comment_quota_events_idempotency"),
        CheckConstraint(
            "event_type IN ('reply_bonus_grant','reply_bonus_use')",
            name="ck_ai_comment_quota_events_type",
        ),
        CheckConstraint("slots >= 1 AND slots <= 20", name="ck_ai_comment_quota_events_slots"),
        CheckConstraint("length(day_key) = 10", name="ck_ai_comment_quota_events_day"),
        Index(
            "ix_ai_comment_quota_events_profile_day",
            "account_profile_id",
            "day_key",
            "event_type",
            "id",
        ),
        Index(
            "ix_ai_comment_quota_events_source",
            "source_telegram_message_id",
            "id",
        ),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    account_profile_id: Mapped[int | None] = mapped_column(
        ForeignKey(
            "ai_account_profiles.id",
            name="fk_ai_comment_quota_events_profile",
            ondelete="SET NULL",
        ),
        nullable=True,
    )
    thread_id: Mapped[int | None] = mapped_column(
        ForeignKey(
            "ai_comment_threads.id",
            name="fk_ai_comment_quota_events_thread",
            ondelete="SET NULL",
        ),
        nullable=True,
    )
    day_key: Mapped[str] = mapped_column(String(10), nullable=False)
    event_type: Mapped[str] = mapped_column(String(24), nullable=False)
    slots: Mapped[int] = mapped_column(Integer, nullable=False)
    source_telegram_message_id: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    idempotency_key: Mapped[str] = mapped_column(String(160), nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow, nullable=False)


class AICommentMessage(Base):
    __tablename__ = "ai_comment_messages"
    __table_args__ = (
        UniqueConstraint(
            "thread_id",
            "telegram_message_id",
            name="uq_ai_comment_messages_thread_telegram",
        ),
        CheckConstraint(
            "role IN ('draft','published','external')",
            name="ck_ai_comment_messages_role",
        ),
        CheckConstraint(
            "length(text_hash) = 64", name="ck_ai_comment_messages_text_hash"
        ),
        Index("ix_ai_comment_messages_thread_status", "thread_id", "status", "id"),
        Index(
            "ix_ai_comment_messages_profile_published",
            "account_profile_id",
            "published_at",
            "id",
        ),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    thread_id: Mapped[int] = mapped_column(
        ForeignKey(
            "ai_comment_threads.id",
            name="fk_ai_comment_messages_thread",
            ondelete="RESTRICT",
        ),
        nullable=False,
    )
    account_profile_id: Mapped[int | None] = mapped_column(
        ForeignKey(
            "ai_account_profiles.id",
            name="fk_ai_comment_messages_profile",
            ondelete="SET NULL",
        ),
        nullable=True,
    )
    telegram_message_id: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    reply_to_local_message_id: Mapped[int | None] = mapped_column(
        ForeignKey(
            "ai_comment_messages.id",
            name="fk_ai_comment_messages_reply_local",
            ondelete="SET NULL",
        ),
        nullable=True,
    )
    reply_to_telegram_message_id: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    role: Mapped[str] = mapped_column(String(16), nullable=False)
    text: Mapped[str | None] = mapped_column(Text, nullable=True)
    text_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    status: Mapped[str] = mapped_column(String(24), nullable=False)
    generated_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    approved_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    published_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    failure_reason_safe: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, default=utcnow, onupdate=utcnow, nullable=False
    )


class AIGenerationJob(Base):
    __tablename__ = "ai_generation_jobs"
    __table_args__ = (
        UniqueConstraint("idempotency_key", name="uq_ai_generation_jobs_idempotency"),
        CheckConstraint(
            "requested_mode IN ('single','multiple','dialogue')",
            name="ck_ai_generation_jobs_mode",
        ),
        CheckConstraint(
            "requested_count >= 1 AND requested_count <= 5",
            name="ck_ai_generation_jobs_count",
        ),
        CheckConstraint(
            "status IN ('pending','running','succeeded','retry','failed','cancelled')",
            name="ck_ai_generation_jobs_status",
        ),
        CheckConstraint(
            "attempts >= 0 AND max_attempts > 0",
            name="ck_ai_generation_jobs_attempts",
        ),
        CheckConstraint(
            "source_post_revision > 0",
            name="ck_ai_generation_jobs_post_revision",
        ),
        Index("ix_ai_generation_jobs_queue", "status", "due_at", "id"),
        Index("ix_ai_generation_jobs_channel_status", "channel_id", "status", "id"),
        Index("ix_ai_generation_jobs_post", "post_id", "id"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    idempotency_key: Mapped[str] = mapped_column(String(160), nullable=False)
    channel_id: Mapped[int | None] = mapped_column(
        ForeignKey(
            "channels.id",
            name="fk_ai_generation_jobs_channel",
            ondelete="SET NULL",
        ),
        nullable=True,
    )
    post_id: Mapped[int | None] = mapped_column(
        ForeignKey(
            "ai_channel_posts.id",
            name="fk_ai_generation_jobs_post",
            ondelete="SET NULL",
        ),
        nullable=True,
    )
    thread_id: Mapped[int | None] = mapped_column(
        ForeignKey(
            "ai_comment_threads.id",
            name="fk_ai_generation_jobs_thread",
            ondelete="SET NULL",
        ),
        nullable=True,
    )
    requested_mode: Mapped[str] = mapped_column(String(16), nullable=False)
    requested_count: Mapped[int] = mapped_column(Integer, nullable=False)
    source_post_revision: Mapped[int] = mapped_column(Integer, nullable=False)
    requested_by_admin_id: Mapped[int] = mapped_column(BigInteger, nullable=False)
    status: Mapped[str] = mapped_column(
        String(24), default="pending", server_default="pending", nullable=False
    )
    attempts: Mapped[int] = mapped_column(
        Integer, default=0, server_default="0", nullable=False
    )
    max_attempts: Mapped[int] = mapped_column(
        Integer, default=3, server_default="3", nullable=False
    )
    due_at: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    locked_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    locked_by: Mapped[str | None] = mapped_column(String(96), nullable=True)
    error_class: Mapped[str | None] = mapped_column(String(64), nullable=True)
    safe_error: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, default=utcnow, onupdate=utcnow, nullable=False
    )
    completed_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)


class AICommentDraft(Base):
    __tablename__ = "ai_comment_drafts"
    __table_args__ = (
        UniqueConstraint(
            "generation_job_id",
            "alternative_index",
            "revision",
            name="uq_ai_comment_drafts_job_alternative_revision",
        ),
        UniqueConstraint(
            "decision_idempotency_key",
            name="uq_ai_comment_drafts_decision_idempotency",
        ),
        CheckConstraint("alternative_index > 0", name="ck_ai_comment_drafts_alternative"),
        CheckConstraint("revision > 0", name="ck_ai_comment_drafts_revision"),
        CheckConstraint("lock_version > 0", name="ck_ai_comment_drafts_lock_version"),
        CheckConstraint(
            "source_post_revision > 0",
            name="ck_ai_comment_drafts_post_revision",
        ),
        CheckConstraint(
            "length(text_hash) = 64 AND length(source_post_hash) = 64",
            name="ck_ai_comment_drafts_hashes",
        ),
        CheckConstraint(
            "confidence IS NULL OR (confidence >= 0 AND confidence <= 1)",
            name="ck_ai_comment_drafts_confidence",
        ),
        CheckConstraint(
            "estimated_cost_usd IS NULL OR estimated_cost_usd >= 0",
            name="ck_ai_comment_drafts_cost",
        ),
        CheckConstraint(
            "reply_to_kind IN ('post','message')",
            name="ck_ai_comment_drafts_reply_kind",
        ),
        CheckConstraint(
            "status IN ('pending_review','approved','rejected','superseded','expired')",
            name="ck_ai_comment_drafts_status",
        ),
        Index("ix_ai_comment_drafts_thread_status", "thread_id", "status", "id"),
        Index(
            "ix_ai_comment_drafts_profile_created",
            "account_profile_id",
            "created_at",
            "id",
        ),
        Index("ix_ai_comment_drafts_post_revision", "post_id", "source_post_revision"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    generation_job_id: Mapped[int | None] = mapped_column(
        ForeignKey(
            "ai_generation_jobs.id",
            name="fk_ai_comment_drafts_generation_job",
            ondelete="SET NULL",
        ),
        nullable=True,
    )
    thread_id: Mapped[int | None] = mapped_column(
        ForeignKey(
            "ai_comment_threads.id",
            name="fk_ai_comment_drafts_thread",
            ondelete="SET NULL",
        ),
        nullable=True,
    )
    post_id: Mapped[int | None] = mapped_column(
        ForeignKey(
            "ai_channel_posts.id",
            name="fk_ai_comment_drafts_post",
            ondelete="SET NULL",
        ),
        nullable=True,
    )
    account_profile_id: Mapped[int | None] = mapped_column(
        ForeignKey(
            "ai_account_profiles.id",
            name="fk_ai_comment_drafts_profile",
            ondelete="SET NULL",
        ),
        nullable=True,
    )
    supersedes_draft_id: Mapped[int | None] = mapped_column(
        ForeignKey(
            "ai_comment_drafts.id",
            name="fk_ai_comment_drafts_supersedes",
            ondelete="SET NULL",
        ),
        nullable=True,
    )
    alternative_index: Mapped[int] = mapped_column(
        Integer, default=1, server_default="1", nullable=False
    )
    revision: Mapped[int] = mapped_column(
        Integer, default=1, server_default="1", nullable=False
    )
    lock_version: Mapped[int] = mapped_column(
        Integer, default=1, server_default="1", nullable=False
    )
    reply_to_kind: Mapped[str] = mapped_column(String(16), nullable=False)
    reply_to_local_message_id: Mapped[int | None] = mapped_column(
        ForeignKey(
            "ai_comment_messages.id",
            name="fk_ai_comment_drafts_reply_local",
            ondelete="SET NULL",
        ),
        nullable=True,
    )
    reply_to_telegram_message_id: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    text: Mapped[str | None] = mapped_column(Text, nullable=True)
    text_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    topic: Mapped[str | None] = mapped_column(String(128), nullable=True)
    confidence: Mapped[Decimal | None] = mapped_column(Numeric(5, 4), nullable=True)
    knowledge_refs_json: Mapped[str] = mapped_column(
        Text, default="[]", server_default="[]", nullable=False
    )
    warnings_json: Mapped[str] = mapped_column(
        Text, default="[]", server_default="[]", nullable=False
    )
    validation_json: Mapped[str] = mapped_column(
        Text, default="{}", server_default="{}", nullable=False
    )
    status: Mapped[str] = mapped_column(
        String(24), default="pending_review", server_default="pending_review", nullable=False
    )
    model_name: Mapped[str | None] = mapped_column(String(96), nullable=True)
    prompt_version: Mapped[str] = mapped_column(String(48), nullable=False)
    schema_version: Mapped[str] = mapped_column(String(48), nullable=False)
    source_post_revision: Mapped[int] = mapped_column(Integer, nullable=False)
    source_post_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    estimated_cost_usd: Mapped[Decimal | None] = mapped_column(Numeric(18, 8), nullable=True)
    decision_idempotency_key: Mapped[str | None] = mapped_column(String(160), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, default=utcnow, onupdate=utcnow, nullable=False
    )
    reviewed_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    reviewed_by: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    expires_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)


class AIPublicationJob(Base):
    __tablename__ = "ai_publication_jobs"
    __table_args__ = (
        UniqueConstraint("draft_id", name="uq_ai_publication_jobs_draft"),
        UniqueConstraint(
            "idempotency_key", name="uq_ai_publication_jobs_idempotency"
        ),
        CheckConstraint(
            "status IN ('pending','running','published','retry','failed','cancelled')",
            name="ck_ai_publication_jobs_status",
        ),
        CheckConstraint(
            "attempts >= 0 AND max_attempts > 0",
            name="ck_ai_publication_jobs_attempts",
        ),
        CheckConstraint(
            "draft_revision > 0", name="ck_ai_publication_jobs_draft_revision"
        ),
        CheckConstraint(
            "status <> 'published' OR telegram_message_id IS NOT NULL",
            name="ck_ai_publication_jobs_published_message",
        ),
        Index("ix_ai_publication_jobs_queue", "status", "due_at", "id"),
        Index("ix_ai_publication_jobs_account_status", "account_id", "status", "id"),
        Index("ix_ai_publication_jobs_channel_status", "channel_id", "status", "id"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    idempotency_key: Mapped[str] = mapped_column(String(160), nullable=False)
    draft_id: Mapped[int | None] = mapped_column(
        ForeignKey(
            "ai_comment_drafts.id",
            name="fk_ai_publication_jobs_draft",
            ondelete="SET NULL",
        ),
        nullable=True,
    )
    draft_revision: Mapped[int] = mapped_column(Integer, nullable=False)
    account_id: Mapped[int | None] = mapped_column(
        ForeignKey(
            "accounts.id",
            name="fk_ai_publication_jobs_account",
            ondelete="SET NULL",
        ),
        nullable=True,
    )
    account_profile_id: Mapped[int | None] = mapped_column(
        ForeignKey(
            "ai_account_profiles.id",
            name="fk_ai_publication_jobs_profile",
            ondelete="SET NULL",
        ),
        nullable=True,
    )
    channel_id: Mapped[int | None] = mapped_column(
        ForeignKey(
            "channels.id",
            name="fk_ai_publication_jobs_channel",
            ondelete="SET NULL",
        ),
        nullable=True,
    )
    thread_id: Mapped[int | None] = mapped_column(
        ForeignKey(
            "ai_comment_threads.id",
            name="fk_ai_publication_jobs_thread",
            ondelete="SET NULL",
        ),
        nullable=True,
    )
    reply_to_telegram_message_id: Mapped[int] = mapped_column(BigInteger, nullable=False)
    status: Mapped[str] = mapped_column(
        String(24), default="pending", server_default="pending", nullable=False
    )
    attempts: Mapped[int] = mapped_column(
        Integer, default=0, server_default="0", nullable=False
    )
    max_attempts: Mapped[int] = mapped_column(
        Integer, default=3, server_default="3", nullable=False
    )
    due_at: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    locked_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    locked_by: Mapped[str | None] = mapped_column(String(96), nullable=True)
    publish_confirmed_at: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    confirmed_by_admin_id: Mapped[int] = mapped_column(BigInteger, nullable=False)
    telegram_message_id: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    error_class: Mapped[str | None] = mapped_column(String(64), nullable=True)
    safe_error: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, default=utcnow, onupdate=utcnow, nullable=False
    )
    published_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)


class AIKnowledgeSource(Base):
    __tablename__ = "ai_knowledge_sources"
    __table_args__ = (
        UniqueConstraint("source_key", name="uq_ai_knowledge_sources_key"),
        UniqueConstraint(
            "source_type", "sha256", name="uq_ai_knowledge_sources_type_sha"
        ),
        CheckConstraint(
            "source_type IN ('pdf','channel_manual','admin_example')",
            name="ck_ai_knowledge_sources_type",
        ),
        CheckConstraint("total_pages >= 0", name="ck_ai_knowledge_sources_pages"),
        CheckConstraint(
            "length(sha256) = 64", name="ck_ai_knowledge_sources_sha256"
        ),
        CheckConstraint(
            "processing_status IN ('pending','processing','ready','failed')",
            name="ck_ai_knowledge_sources_processing",
        ),
        CheckConstraint(
            "review_status IN ('ready','needs_review','rejected','retired')",
            name="ck_ai_knowledge_sources_review",
        ),
        Index("ix_ai_knowledge_sources_review", "review_status", "id"),
        Index("ix_ai_knowledge_sources_sha", "sha256", "id"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    source_key: Mapped[str] = mapped_column(String(96), nullable=False)
    source_type: Mapped[str] = mapped_column(String(32), nullable=False)
    filename: Mapped[str] = mapped_column(String(512), nullable=False)
    sha256: Mapped[str] = mapped_column(String(64), nullable=False)
    title: Mapped[str] = mapped_column(String(512), nullable=False)
    total_pages: Mapped[int] = mapped_column(Integer, nullable=False)
    text_layer: Mapped[bool] = mapped_column(Boolean, nullable=False)
    processing_status: Mapped[str] = mapped_column(String(24), nullable=False)
    review_status: Mapped[str] = mapped_column(String(24), nullable=False)
    dataset_version: Mapped[str] = mapped_column(String(64), nullable=False)
    external_file_id: Mapped[str | None] = mapped_column(String(255), nullable=True)
    vector_store_file_id: Mapped[str | None] = mapped_column(String(255), nullable=True)
    retired_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, default=utcnow, onupdate=utcnow, nullable=False
    )


class AIKnowledgeChunk(Base):
    __tablename__ = "ai_knowledge_chunks"
    __table_args__ = (
        UniqueConstraint("chunk_key", name="uq_ai_knowledge_chunks_key"),
        UniqueConstraint(
            "source_id", "chunk_hash", name="uq_ai_knowledge_chunks_source_hash"
        ),
        CheckConstraint("page_from > 0", name="ck_ai_knowledge_chunks_page_from"),
        CheckConstraint(
            "page_to >= page_from", name="ck_ai_knowledge_chunks_page_range"
        ),
        CheckConstraint(
            "length(chunk_hash) = 64", name="ck_ai_knowledge_chunks_hash"
        ),
        CheckConstraint(
            "review_status IN ('ready','needs_review','rejected','retired')",
            name="ck_ai_knowledge_chunks_review",
        ),
        CheckConstraint(
            "index_eligible = false OR review_status = 'ready'",
            name="ck_ai_knowledge_chunks_index_review",
        ),
        Index("ix_ai_knowledge_chunks_retrieval", "review_status", "index_eligible", "topic"),
        Index("ix_ai_knowledge_chunks_source_pages", "source_id", "page_from", "page_to"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    source_id: Mapped[int] = mapped_column(
        ForeignKey(
            "ai_knowledge_sources.id",
            name="fk_ai_knowledge_chunks_source",
            ondelete="RESTRICT",
        ),
        nullable=False,
    )
    chunk_key: Mapped[str] = mapped_column(String(160), nullable=False)
    page_from: Mapped[int] = mapped_column(Integer, nullable=False)
    page_to: Mapped[int] = mapped_column(Integer, nullable=False)
    topic: Mapped[str] = mapped_column(String(128), nullable=False)
    chunk_text: Mapped[str | None] = mapped_column(Text, nullable=True)
    chunk_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    keywords_json: Mapped[str] = mapped_column(
        Text, default="[]", server_default="[]", nullable=False
    )
    topics_json: Mapped[str] = mapped_column(
        Text, default="[]", server_default="[]", nullable=False
    )
    difficulty: Mapped[str | None] = mapped_column(String(32), nullable=True)
    review_status: Mapped[str] = mapped_column(String(24), nullable=False)
    index_eligible: Mapped[bool] = mapped_column(
        Boolean, default=False, server_default="false", nullable=False
    )
    contains_visual_candidate: Mapped[bool] = mapped_column(
        Boolean, default=False, server_default="false", nullable=False
    )
    external_chunk_ref: Mapped[str | None] = mapped_column(String(255), nullable=True)
    retired_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, default=utcnow, onupdate=utcnow, nullable=False
    )


class AIUsageStat(Base):
    __tablename__ = "ai_usage_stats"
    __table_args__ = (
        CheckConstraint("input_tokens >= 0", name="ck_ai_usage_stats_input_tokens"),
        CheckConstraint("output_tokens >= 0", name="ck_ai_usage_stats_output_tokens"),
        CheckConstraint("cached_tokens >= 0", name="ck_ai_usage_stats_cached_tokens"),
        CheckConstraint("tool_calls >= 0", name="ck_ai_usage_stats_tool_calls"),
        CheckConstraint("latency_ms >= 0", name="ck_ai_usage_stats_latency"),
        CheckConstraint(
            "cost_usd IS NULL OR cost_usd >= 0", name="ck_ai_usage_stats_cost"
        ),
        Index("ix_ai_usage_stats_created", "created_at", "id"),
        Index("ix_ai_usage_stats_generation", "generation_job_id", "id"),
        Index("ix_ai_usage_stats_success_error", "success", "error_class", "id"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    generation_job_id: Mapped[int | None] = mapped_column(
        ForeignKey(
            "ai_generation_jobs.id",
            name="fk_ai_usage_stats_generation_job",
            ondelete="SET NULL",
        ),
        nullable=True,
    )
    model_name: Mapped[str] = mapped_column(String(96), nullable=False)
    request_id_safe: Mapped[str | None] = mapped_column(String(160), nullable=True)
    input_tokens: Mapped[int] = mapped_column(
        BigInteger, default=0, server_default="0", nullable=False
    )
    output_tokens: Mapped[int] = mapped_column(
        BigInteger, default=0, server_default="0", nullable=False
    )
    cached_tokens: Mapped[int] = mapped_column(
        BigInteger, default=0, server_default="0", nullable=False
    )
    tool_calls: Mapped[int] = mapped_column(
        Integer, default=0, server_default="0", nullable=False
    )
    latency_ms: Mapped[int] = mapped_column(
        BigInteger, default=0, server_default="0", nullable=False
    )
    cost_usd: Mapped[Decimal | None] = mapped_column(Numeric(18, 8), nullable=True)
    success: Mapped[bool] = mapped_column(Boolean, nullable=False)
    error_class: Mapped[str | None] = mapped_column(String(64), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow, nullable=False)


class AISetting(Base):
    __tablename__ = "ai_settings"
    __table_args__ = (
        CheckConstraint("value_version > 0", name="ck_ai_settings_value_version"),
        Index("ix_ai_settings_updated", "updated_at", "key"),
    )

    key: Mapped[str] = mapped_column(String(96), primary_key=True)
    value_json: Mapped[str] = mapped_column(Text, nullable=False)
    value_version: Mapped[int] = mapped_column(
        Integer, default=1, server_default="1", nullable=False
    )
    updated_by: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, default=utcnow, onupdate=utcnow, nullable=False
    )
