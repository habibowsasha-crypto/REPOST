from __future__ import annotations

from sqlalchemy import event

from .db_shared import *  # noqa: F403
from .db_shared import _public_inspect


def _enable_sqlite_foreign_keys(dbapi_connection, _connection_record) -> None:
    """Make declared ON DELETE rules real on every SQLite connection."""

    cursor = dbapi_connection.cursor()
    try:
        cursor.execute("PRAGMA foreign_keys=ON")
    finally:
        cursor.close()


class DatabaseCore:
    def __init__(self, url: str) -> None:
        self.engine: AsyncEngine = create_async_engine(url, pool_pre_ping=True)
        if self.engine.dialect.name == "sqlite":
            event.listen(
                self.engine.sync_engine,
                "connect",
                _enable_sqlite_foreign_keys,
            )
        self.sessions = async_sessionmaker(self.engine, expire_on_commit=False, class_=AsyncSession)
        self._instance_lock_connection: AsyncConnection | None = None

    async def init(self) -> None:
        async with self.engine.begin() as conn:
            # Keep the historical schema creation separate even if another
            # module imported AI model metadata before Database.init(). This
            # guarantees that the AI migration can preflight a partial schema
            # before it creates or changes any AI table.
            await conn.run_sync(
                lambda sync: Base.metadata.create_all(
                    sync,
                    tables=[
                        table
                        for table in Base.metadata.sorted_tables
                        if not table.name.startswith("ai_")
                    ],
                )
            )
            await self._apply_compat_migrations(conn)
            # AI Comments is still feature-disabled. Its isolated schema is
            # nevertheless deployed here so a later feature rollout never races
            # the existing auth/reaction/view/join workers on DDL.
            from .ai_comments_migration import upgrade_ai_comments_schema

            await upgrade_ai_comments_schema(conn)

    async def _apply_compat_migrations(self, conn: AsyncConnection) -> None:
        """Backward-compatible schema migrations without an external migration tool."""

        def columns(sync_conn, table_name: str) -> dict[str, dict[str, object]]:
            return {column["name"]: column for column in _public_inspect(sync_conn).get_columns(table_name)}

        def optional_columns(
            sync_conn, table_name: str
        ) -> dict[str, dict[str, object]] | None:
            try:
                return columns(sync_conn, table_name)
            except (NoSuchTableError, AssertionError):
                # Some compatibility tests and very old partial schemas do not yet
                # contain every later table. Migrate only tables that really exist.
                return None

        channel_columns = await conn.run_sync(columns, "channels")
        if "kind" not in channel_columns:
            await conn.execute(
                text("ALTER TABLE channels ADD COLUMN kind VARCHAR(16) NOT NULL DEFAULT 'channel'")
            )
        if "reactions_json" not in channel_columns:
            await conn.execute(text("ALTER TABLE channels ADD COLUMN reactions_json TEXT"))
        if "max_reactions_per_post" not in channel_columns:
            await conn.execute(text("ALTER TABLE channels ADD COLUMN max_reactions_per_post INTEGER"))
        if "reaction_window_min_seconds" not in channel_columns:
            await conn.execute(
                text(
                    "ALTER TABLE channels ADD COLUMN reaction_window_min_seconds "
                    "INTEGER NOT NULL DEFAULT 0"
                )
            )
        if "reaction_window_max_seconds" not in channel_columns:
            await conn.execute(
                text(
                    "ALTER TABLE channels ADD COLUMN reaction_window_max_seconds "
                    "INTEGER NOT NULL DEFAULT 3600"
                )
            )
        if "image_post_reaction_percent" not in channel_columns:
            await conn.execute(
                text(
                    "ALTER TABLE channels ADD COLUMN image_post_reaction_percent "
                    "INTEGER NOT NULL DEFAULT 100"
                )
            )
        if "no_image_post_reaction_percent" not in channel_columns:
            await conn.execute(
                text(
                    "ALTER TABLE channels ADD COLUMN no_image_post_reaction_percent "
                    "INTEGER NOT NULL DEFAULT 100"
                )
            )
        if "promotion_mode" not in channel_columns:
            await conn.execute(
                text(
                    "ALTER TABLE channels ADD COLUMN promotion_mode "
                    "VARCHAR(16) NOT NULL DEFAULT 'permanent'"
                )
            )
        timestamp_type = "TIMESTAMP WITHOUT TIME ZONE" if conn.dialect.name == "postgresql" else "DATETIME"
        if "promotion_started_at" not in channel_columns:
            await conn.execute(
                text(f"ALTER TABLE channels ADD COLUMN promotion_started_at {timestamp_type}")
            )
        if "promotion_until" not in channel_columns:
            await conn.execute(text(f"ALTER TABLE channels ADD COLUMN promotion_until {timestamp_type}"))

        join_columns = await conn.run_sync(columns, "join_jobs")
        if "action" not in join_columns:
            await conn.execute(
                text("ALTER TABLE join_jobs ADD COLUMN action VARCHAR(16) NOT NULL DEFAULT 'join'")
            )
        if "started_at" not in join_columns:
            await conn.execute(
                text(f"ALTER TABLE join_jobs ADD COLUMN started_at {timestamp_type}")
            )

        reaction_columns = await conn.run_sync(columns, "reaction_jobs")
        if "source" not in reaction_columns:
            await conn.execute(
                text("ALTER TABLE reaction_jobs ADD COLUMN source VARCHAR(16) NOT NULL DEFAULT 'legacy'")
            )
        if "post_has_image" not in reaction_columns:
            await conn.execute(text("ALTER TABLE reaction_jobs ADD COLUMN post_has_image BOOLEAN"))
        view_included_added = "view_included" not in reaction_columns
        if view_included_added:
            await conn.execute(
                text(
                    "ALTER TABLE reaction_jobs ADD COLUMN view_included "
                    "BOOLEAN NOT NULL DEFAULT FALSE"
                )
            )
        if "view_confirmed_at" not in reaction_columns:
            await conn.execute(
                text(f"ALTER TABLE reaction_jobs ADD COLUMN view_confirmed_at {timestamp_type}")
            )
        if "started_at" not in reaction_columns:
            await conn.execute(
                text(f"ALTER TABLE reaction_jobs ADD COLUMN started_at {timestamp_type}")
            )
        view_columns = await conn.run_sync(optional_columns, "view_jobs")
        if view_columns is not None and "started_at" not in view_columns:
            await conn.execute(
                text(f"ALTER TABLE view_jobs ADD COLUMN started_at {timestamp_type}")
            )
        if view_included_added:
            # Existing completed reactions cannot prove that a view request really
            # happened: versions below v1.0.14 did not request one at all. Pending
            # and running jobs, however, will execute under the new code and must
            # continue to reserve their future view.
            await conn.execute(
                text(
                    "UPDATE reaction_jobs SET view_included = TRUE "
                    "WHERE status IN ('pending', 'running')"
                )
            )

        # A container restart may interrupt a worker after it marks a job as running.
        # Requeue those jobs so they are not stuck forever.
        await conn.execute(
            text(
                "UPDATE reaction_jobs SET status = 'pending', started_at = NULL, "
                "error = 'Восстановлено после перезапуска' WHERE status = 'running'"
            )
        )
        await conn.execute(
            text(
                "UPDATE join_jobs SET status = 'pending', started_at = NULL, "
                "error = 'Восстановлено после перезапуска' WHERE status = 'running'"
            )
        )
        if view_columns is not None:
            await conn.execute(
                text(
                    "UPDATE view_jobs SET status = 'pending', started_at = NULL, "
                    "error = 'Восстановлено после перезапуска' WHERE status = 'running'"
                )
            )

        # Older releases did not timestamp every terminal failure/cancellation.
        # Use the immutable creation time as a conservative migration baseline so
        # retention can archive those rows without losing their counters.
        await conn.execute(
            text(
                "UPDATE reaction_jobs SET completed_at = created_at "
                "WHERE completed_at IS NULL AND status IN ('done', 'failed', 'cancelled')"
            )
        )
        if view_columns is not None:
            await conn.execute(
                text(
                    "UPDATE view_jobs SET completed_at = created_at "
                    "WHERE completed_at IS NULL AND status IN ('done', 'failed', 'cancelled')"
                )
            )
        view_batch_columns = await conn.run_sync(optional_columns, "view_batches")
        if view_batch_columns is not None:
            await conn.execute(
                text(
                    "UPDATE view_batches SET completed_at = created_at "
                    "WHERE completed_at IS NULL AND status IN ('done', 'cancelled')"
                )
            )

        # Explicit CREATE INDEX is required for existing databases because
        # metadata.create_all() does not add new indexes to tables that already exist.
        index_statements = (
            ("join_jobs", "CREATE INDEX IF NOT EXISTS ix_join_jobs_status_due_id ON join_jobs (status, due_at, id)"),
            ("join_jobs", "CREATE INDEX IF NOT EXISTS ix_join_jobs_account_status ON join_jobs (account_id, status)"),
            ("join_jobs", "CREATE INDEX IF NOT EXISTS ix_join_jobs_status_started_id ON join_jobs (status, started_at, id)"),
            ("join_jobs", "CREATE INDEX IF NOT EXISTS ix_join_jobs_status_completed_id ON join_jobs (status, completed_at, id)"),
            ("reaction_jobs", "CREATE INDEX IF NOT EXISTS ix_reaction_jobs_status_due_id ON reaction_jobs (status, due_at, id)"),
            ("reaction_jobs", "CREATE INDEX IF NOT EXISTS ix_reaction_jobs_account_status ON reaction_jobs (account_id, status)"),
            ("reaction_jobs", "CREATE INDEX IF NOT EXISTS ix_reaction_jobs_channel_status ON reaction_jobs (channel_id, status)"),
            ("reaction_jobs", "CREATE INDEX IF NOT EXISTS ix_reaction_jobs_status_started_id ON reaction_jobs (status, started_at, id)"),
            ("reaction_jobs", "CREATE INDEX IF NOT EXISTS ix_reaction_jobs_status_completed_id ON reaction_jobs (status, completed_at, id)"),
            ("view_jobs", "CREATE INDEX IF NOT EXISTS ix_view_jobs_status_due_id ON view_jobs (status, due_at, id)"),
            ("view_jobs", "CREATE INDEX IF NOT EXISTS ix_view_jobs_account_status ON view_jobs (account_id, status)"),
            ("view_jobs", "CREATE INDEX IF NOT EXISTS ix_view_jobs_channel_status ON view_jobs (channel_id, status)"),
            ("view_jobs", "CREATE INDEX IF NOT EXISTS ix_view_jobs_batch_status ON view_jobs (batch_id, status)"),
            ("view_jobs", "CREATE INDEX IF NOT EXISTS ix_view_jobs_status_started_id ON view_jobs (status, started_at, id)"),
            ("view_jobs", "CREATE INDEX IF NOT EXISTS ix_view_jobs_status_completed_id ON view_jobs (status, completed_at, id)"),
            ("job_history_summaries", "CREATE INDEX IF NOT EXISTS ix_job_history_kind_status ON job_history_summaries (job_kind, status)"),
            ("job_history_channel_summaries", "CREATE INDEX IF NOT EXISTS ix_job_history_channel_kind_status ON job_history_channel_summaries (job_kind, status)"),
            ("job_history_keys", "CREATE INDEX IF NOT EXISTS ix_job_history_keys_channel_message_kind ON job_history_keys (channel_id, message_id, job_kind)"),
            ("job_history_keys", "CREATE INDEX IF NOT EXISTS ix_job_history_keys_account_kind ON job_history_keys (account_id, job_kind)"),
        )
        table_presence: dict[str, bool] = {
            "join_jobs": True,
            "reaction_jobs": True,
            "view_jobs": view_columns is not None,
            "job_history_summaries": (
                await conn.run_sync(optional_columns, "job_history_summaries")
            )
            is not None,
            "job_history_channel_summaries": (
                await conn.run_sync(optional_columns, "job_history_channel_summaries")
            )
            is not None,
            "job_history_keys": (
                await conn.run_sync(optional_columns, "job_history_keys")
            )
            is not None,
        }
        for table_name, statement in index_statements:
            if table_presence.get(table_name, False):
                await conn.execute(text(statement))

        account_columns = await conn.run_sync(columns, "accounts")
        if "last_reaction_at" not in account_columns:
            await conn.execute(
                text(f"ALTER TABLE accounts ADD COLUMN last_reaction_at {timestamp_type}")
            )
        if "problem_detected_at" not in account_columns:
            await conn.execute(
                text(f"ALTER TABLE accounts ADD COLUMN problem_detected_at {timestamp_type}")
            )
        if "problem_reason" not in account_columns:
            await conn.execute(text("ALTER TABLE accounts ADD COLUMN problem_reason TEXT"))
        if "problem_context" not in account_columns:
            await conn.execute(
                text("ALTER TABLE accounts ADD COLUMN problem_context VARCHAR(128)")
            )
        if "email_login" not in account_columns:
            await conn.execute(text("ALTER TABLE accounts ADD COLUMN email_login VARCHAR(320)"))
        if "email_provider" not in account_columns:
            await conn.execute(text("ALTER TABLE accounts ADD COLUMN email_provider VARCHAR(255)"))
        if "email_note" not in account_columns:
            await conn.execute(text("ALTER TABLE accounts ADD COLUMN email_note TEXT"))
        if "email_updated_at" not in account_columns:
            await conn.execute(
                text(f"ALTER TABLE accounts ADD COLUMN email_updated_at {timestamp_type}")
            )

        # Telegram user/channel identifiers are 64-bit values. Older releases used
        # PostgreSQL INTEGER (int32), which rejected valid IDs above 2,147,483,647.
        # SQLite already stores INTEGER values as signed 64-bit, so only PostgreSQL
        # needs a physical column migration.
        if conn.dialect.name == "postgresql":
            account_columns = await conn.run_sync(columns, "accounts")
            user_id_type = account_columns["telegram_user_id"]["type"]
            if not isinstance(user_id_type, BigInteger):
                await conn.execute(
                    text(
                        "ALTER TABLE accounts "
                        "ALTER COLUMN telegram_user_id TYPE BIGINT "
                        "USING telegram_user_id::BIGINT"
                    )
                )

            channel_columns = await conn.run_sync(columns, "channels")
            channel_id_type = channel_columns["telegram_channel_id"]["type"]
            if not isinstance(channel_id_type, BigInteger):
                await conn.execute(
                    text(
                        "ALTER TABLE channels "
                        "ALTER COLUMN telegram_channel_id TYPE BIGINT "
                        "USING telegram_channel_id::BIGINT"
                    )
                )

    async def acquire_instance_lock(self) -> bool:
        """Prevent multiple PostgreSQL replicas from processing the same queues."""

        if self.engine.dialect.name != "postgresql":
            return True
        if self._instance_lock_connection is not None:
            return True
        connection = await self.engine.connect()
        try:
            acquired = bool(
                await connection.scalar(
                    text("SELECT pg_try_advisory_lock(:lock_key)"),
                    {"lock_key": INSTANCE_ADVISORY_LOCK_KEY},
                )
            )
        except Exception:
            await connection.close()
            raise
        if not acquired:
            await connection.close()
            return False
        self._instance_lock_connection = connection
        return True

    async def close(self) -> None:
        connection = self._instance_lock_connection
        self._instance_lock_connection = None
        if connection is not None:
            try:
                await connection.execute(
                    text("SELECT pg_advisory_unlock(:lock_key)"),
                    {"lock_key": INSTANCE_ADVISORY_LOCK_KEY},
                )
            except Exception:  # noqa: BLE001
                logger.exception("Failed to release PostgreSQL instance advisory lock")
            finally:
                try:
                    await connection.close()
                except Exception:  # noqa: BLE001
                    logger.exception("Failed to close PostgreSQL lock connection")
        await self.engine.dispose()
