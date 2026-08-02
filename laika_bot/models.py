from __future__ import annotations

from datetime import UTC, datetime

from sqlalchemy import BigInteger, Boolean, DateTime, ForeignKey, Index, Integer, String, Text, UniqueConstraint
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


def utcnow() -> datetime:
    return datetime.now(UTC).replace(tzinfo=None)


class Base(DeclarativeBase):
    pass


class Account(Base):
    __tablename__ = "accounts"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    phone: Mapped[str] = mapped_column(String(40), unique=True, nullable=False)
    telegram_user_id: Mapped[int] = mapped_column(BigInteger, unique=True, nullable=False)
    display_name: Mapped[str] = mapped_column(String(255), nullable=False)
    username: Mapped[str | None] = mapped_column(String(64), nullable=True)
    email_login: Mapped[str | None] = mapped_column(String(320), nullable=True)
    email_provider: Mapped[str | None] = mapped_column(String(255), nullable=True)
    email_note: Mapped[str | None] = mapped_column(Text, nullable=True)
    email_updated_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    session_encrypted: Mapped[str] = mapped_column(Text, nullable=False)
    is_active: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)
    status: Mapped[str] = mapped_column(String(32), default="ready", nullable=False)
    flood_until: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    last_reaction_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    last_error: Mapped[str | None] = mapped_column(Text, nullable=True)
    problem_detected_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    problem_reason: Mapped[str | None] = mapped_column(Text, nullable=True)
    problem_context: Mapped[str | None] = mapped_column(String(128), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow, onupdate=utcnow, nullable=False)

    join_jobs: Mapped[list["JoinJob"]] = relationship(back_populates="account", cascade="all, delete-orphan")
    reaction_jobs: Mapped[list["ReactionJob"]] = relationship(back_populates="account", cascade="all, delete-orphan")
    view_jobs: Mapped[list["ViewJob"]] = relationship(back_populates="account", cascade="all, delete-orphan")
    job_history_summaries: Mapped[list["JobHistorySummary"]] = relationship(
        back_populates="account", cascade="all, delete-orphan"
    )
    job_history_keys: Mapped[list["JobHistoryKey"]] = relationship(
        back_populates="account", cascade="all, delete-orphan"
    )


class Channel(Base):
    """A Telegram broadcast channel or megagroup managed by the bot.

    The historical table/model name is kept for backward compatibility with v1.0.0.
    `kind` is either ``channel`` or ``group``.
    """

    __tablename__ = "channels"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    telegram_channel_id: Mapped[int] = mapped_column(BigInteger, unique=True, nullable=False)
    kind: Mapped[str] = mapped_column(String(16), default="channel", server_default="channel", nullable=False)
    title: Mapped[str] = mapped_column(String(255), nullable=False)
    username: Mapped[str | None] = mapped_column(String(64), nullable=True)
    link: Mapped[str] = mapped_column(Text, nullable=False)
    invite_hash: Mapped[str | None] = mapped_column(String(255), nullable=True)
    is_active: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)
    new_posts_enabled: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    old_posts_enabled: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    old_posts_depth: Mapped[int] = mapped_column(Integer, default=20, nullable=False)
    reactions_json: Mapped[str | None] = mapped_column(Text, nullable=True)
    max_reactions_per_post: Mapped[int | None] = mapped_column(Integer, nullable=True)
    reaction_window_min_seconds: Mapped[int] = mapped_column(
        Integer, default=0, server_default="0", nullable=False
    )
    reaction_window_max_seconds: Mapped[int] = mapped_column(
        Integer, default=3600, server_default="3600", nullable=False
    )
    image_post_reaction_percent: Mapped[int] = mapped_column(
        Integer, default=100, server_default="100", nullable=False
    )
    no_image_post_reaction_percent: Mapped[int] = mapped_column(
        Integer, default=100, server_default="100", nullable=False
    )
    promotion_mode: Mapped[str] = mapped_column(
        String(16), default="permanent", server_default="permanent", nullable=False
    )
    promotion_started_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    promotion_until: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    last_seen_message_id: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    last_error: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow, onupdate=utcnow, nullable=False)

    join_jobs: Mapped[list["JoinJob"]] = relationship(back_populates="channel", cascade="all, delete-orphan")
    reaction_jobs: Mapped[list["ReactionJob"]] = relationship(back_populates="channel", cascade="all, delete-orphan")
    view_batches: Mapped[list["ViewBatch"]] = relationship(back_populates="channel", cascade="all, delete-orphan")
    view_jobs: Mapped[list["ViewJob"]] = relationship(back_populates="channel", cascade="all, delete-orphan")
    job_history_summaries: Mapped[list["JobHistoryChannelSummary"]] = relationship(
        back_populates="channel", cascade="all, delete-orphan"
    )
    job_history_keys: Mapped[list["JobHistoryKey"]] = relationship(
        back_populates="channel", cascade="all, delete-orphan"
    )


def promotion_is_active(channel: "Channel", *, now: datetime | None = None) -> bool:
    """Return whether the target-wide promotion window currently permits reactions."""

    if channel.promotion_mode == "permanent":
        return True
    current = now or utcnow()
    return bool(channel.promotion_until and channel.promotion_until > current)


class JoinJob(Base):
    __tablename__ = "join_jobs"
    __table_args__ = (
        UniqueConstraint("channel_id", "account_id", name="uq_join_channel_account"),
        Index("ix_join_jobs_status_due_id", "status", "due_at", "id"),
        Index("ix_join_jobs_account_status", "account_id", "status"),
        Index("ix_join_jobs_status_started_id", "status", "started_at", "id"),
        Index("ix_join_jobs_status_completed_id", "status", "completed_at", "id"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    channel_id: Mapped[int] = mapped_column(ForeignKey("channels.id", ondelete="CASCADE"), nullable=False)
    account_id: Mapped[int] = mapped_column(ForeignKey("accounts.id", ondelete="CASCADE"), nullable=False)
    action: Mapped[str] = mapped_column(String(16), default="join", server_default="join", nullable=False)
    due_at: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    status: Mapped[str] = mapped_column(String(24), default="pending", nullable=False)
    attempts: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    error: Mapped[str | None] = mapped_column(Text, nullable=True)
    completed_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    started_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow, nullable=False)

    channel: Mapped[Channel] = relationship(back_populates="join_jobs")
    account: Mapped[Account] = relationship(back_populates="join_jobs")


class ReactionJob(Base):
    __tablename__ = "reaction_jobs"
    __table_args__ = (
        UniqueConstraint("channel_id", "account_id", "message_id", name="uq_reaction_channel_account_message"),
        Index("ix_reaction_jobs_status_due_id", "status", "due_at", "id"),
        Index("ix_reaction_jobs_account_status", "account_id", "status"),
        Index("ix_reaction_jobs_channel_status", "channel_id", "status"),
        Index("ix_reaction_jobs_status_started_id", "status", "started_at", "id"),
        Index("ix_reaction_jobs_status_completed_id", "status", "completed_at", "id"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    channel_id: Mapped[int] = mapped_column(ForeignKey("channels.id", ondelete="CASCADE"), nullable=False)
    account_id: Mapped[int] = mapped_column(ForeignKey("accounts.id", ondelete="CASCADE"), nullable=False)
    message_id: Mapped[int] = mapped_column(Integer, nullable=False)
    reaction: Mapped[str] = mapped_column(String(64), nullable=False)
    source: Mapped[str] = mapped_column(String(16), default="legacy", server_default="legacy", nullable=False)
    post_has_image: Mapped[bool | None] = mapped_column(Boolean, nullable=True)
    # True means this reaction job is expected to request a channel view before
    # sending the reaction. Existing completed rows from versions below v1.0.17
    # are migrated as False because their view history cannot be proven.
    view_included: Mapped[bool] = mapped_column(
        Boolean, default=True, server_default="true", nullable=False
    )
    view_confirmed_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    due_at: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    status: Mapped[str] = mapped_column(String(24), default="pending", nullable=False)
    attempts: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    error: Mapped[str | None] = mapped_column(Text, nullable=True)
    completed_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    started_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow, nullable=False)

    channel: Mapped[Channel] = relationship(back_populates="reaction_jobs")
    account: Mapped[Account] = relationship(back_populates="reaction_jobs")


class ViewBatch(Base):
    __tablename__ = "view_batches"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    channel_id: Mapped[int] = mapped_column(ForeignKey("channels.id", ondelete="CASCADE"), nullable=False)
    requested_post_count: Mapped[int] = mapped_column(Integer, nullable=False)
    posts_found: Mapped[int] = mapped_column(Integer, nullable=False)
    accounts_per_post: Mapped[int] = mapped_column(Integer, nullable=False)
    selection_mode: Mapped[str] = mapped_column(String(16), nullable=False)
    selection_value: Mapped[int] = mapped_column(Integer, nullable=False)
    total_jobs: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    skipped_existing: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    status: Mapped[str] = mapped_column(String(24), default="pending", nullable=False)
    completed_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow, nullable=False)

    channel: Mapped[Channel] = relationship(back_populates="view_batches")
    jobs: Mapped[list["ViewJob"]] = relationship(back_populates="batch", cascade="all, delete-orphan")


class ViewJob(Base):
    __tablename__ = "view_jobs"
    __table_args__ = (
        UniqueConstraint("channel_id", "account_id", "message_id", name="uq_view_channel_account_message"),
        Index("ix_view_jobs_status_due_id", "status", "due_at", "id"),
        Index("ix_view_jobs_account_status", "account_id", "status"),
        Index("ix_view_jobs_channel_status", "channel_id", "status"),
        Index("ix_view_jobs_batch_status", "batch_id", "status"),
        Index("ix_view_jobs_status_started_id", "status", "started_at", "id"),
        Index("ix_view_jobs_status_completed_id", "status", "completed_at", "id"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    batch_id: Mapped[int] = mapped_column(ForeignKey("view_batches.id", ondelete="CASCADE"), nullable=False)
    channel_id: Mapped[int] = mapped_column(ForeignKey("channels.id", ondelete="CASCADE"), nullable=False)
    account_id: Mapped[int] = mapped_column(ForeignKey("accounts.id", ondelete="CASCADE"), nullable=False)
    message_id: Mapped[int] = mapped_column(Integer, nullable=False)
    due_at: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    status: Mapped[str] = mapped_column(String(24), default="pending", nullable=False)
    attempts: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    error: Mapped[str | None] = mapped_column(Text, nullable=True)
    completed_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    started_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow, nullable=False)

    batch: Mapped[ViewBatch] = relationship(back_populates="jobs")
    channel: Mapped[Channel] = relationship(back_populates="view_jobs")
    account: Mapped[Account] = relationship(back_populates="view_jobs")


class JobHistoryKey(Base):
    """Compact dedup/proof record for archived completed reaction and view jobs."""

    __tablename__ = "job_history_keys"
    __table_args__ = (
        UniqueConstraint(
            "job_kind", "channel_id", "account_id", "message_id",
            name="uq_job_history_key_kind_channel_account_message",
        ),
        Index(
            "ix_job_history_keys_channel_message_kind",
            "channel_id", "message_id", "job_kind",
        ),
        Index("ix_job_history_keys_account_kind", "account_id", "job_kind"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    job_kind: Mapped[str] = mapped_column(String(16), nullable=False)
    channel_id: Mapped[int] = mapped_column(
        ForeignKey("channels.id", ondelete="CASCADE"), nullable=False
    )
    account_id: Mapped[int] = mapped_column(
        ForeignKey("accounts.id", ondelete="CASCADE"), nullable=False
    )
    message_id: Mapped[int] = mapped_column(Integer, nullable=False)
    view_confirmed: Mapped[bool] = mapped_column(
        Boolean, default=False, server_default="false", nullable=False
    )
    archived_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow, nullable=False)

    account: Mapped[Account] = relationship(back_populates="job_history_keys")
    channel: Mapped[Channel] = relationship(back_populates="job_history_keys")


class JobHistoryChannelSummary(Base):
    """Archived terminal job counters grouped by managed channel/group."""

    __tablename__ = "job_history_channel_summaries"
    __table_args__ = (
        UniqueConstraint(
            "channel_id", "job_kind", "status",
            name="uq_job_history_channel_kind_status",
        ),
        Index("ix_job_history_channel_kind_status", "job_kind", "status"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    channel_id: Mapped[int] = mapped_column(
        ForeignKey("channels.id", ondelete="CASCADE"), nullable=False
    )
    job_kind: Mapped[str] = mapped_column(String(16), nullable=False)
    status: Mapped[str] = mapped_column(String(24), nullable=False)
    count: Mapped[int] = mapped_column(
        BigInteger, default=0, server_default="0", nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, default=utcnow, onupdate=utcnow, nullable=False
    )

    channel: Mapped[Channel] = relationship(back_populates="job_history_summaries")


class JobHistorySummary(Base):
    """Archived terminal job counters preserved after retention cleanup."""

    __tablename__ = "job_history_summaries"
    __table_args__ = (
        UniqueConstraint(
            "account_id", "job_kind", "status", name="uq_job_history_account_kind_status"
        ),
        Index("ix_job_history_kind_status", "job_kind", "status"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    account_id: Mapped[int] = mapped_column(
        ForeignKey("accounts.id", ondelete="CASCADE"), nullable=False
    )
    job_kind: Mapped[str] = mapped_column(String(16), nullable=False)
    status: Mapped[str] = mapped_column(String(24), nullable=False)
    count: Mapped[int] = mapped_column(BigInteger, default=0, server_default="0", nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, default=utcnow, onupdate=utcnow, nullable=False
    )

    account: Mapped[Account] = relationship(back_populates="job_history_summaries")


class AppSetting(Base):
    __tablename__ = "app_settings"

    key: Mapped[str] = mapped_column(String(80), primary_key=True)
    value: Mapped[str] = mapped_column(Text, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow, onupdate=utcnow, nullable=False)


class ConfigurationEvent(Base):
    """Safe audit trail for configuration backup, restore and rollback.

    Snapshot JSON uses the portable payload from :mod:`laika_bot.backup`. It
    never contains Telethon StringSession values, login codes, 2FA passwords,
    bot/API secrets, database URLs, private invite hashes or raw phone numbers.
    """

    __tablename__ = "configuration_events"
    __table_args__ = (
        Index("ix_configuration_events_created_id", "created_at", "id"),
        Index("ix_configuration_events_event_type", "event_type"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    event_type: Mapped[str] = mapped_column(String(48), nullable=False)
    status: Mapped[str] = mapped_column(
        String(24), default="done", server_default="done", nullable=False
    )
    source_name: Mapped[str | None] = mapped_column(String(255), nullable=True)
    summary_json: Mapped[str] = mapped_column(
        Text, default="{}", server_default="{}", nullable=False
    )
    snapshot_json: Mapped[str | None] = mapped_column(Text, nullable=True)
    snapshot_sha256: Mapped[str | None] = mapped_column(String(64), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow, nullable=False)
