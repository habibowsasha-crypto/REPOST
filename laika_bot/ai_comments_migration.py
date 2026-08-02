from __future__ import annotations

import json
from collections.abc import Iterable
from typing import Any

from sqlalchemy import exists, insert, inspect, literal, select, update
from sqlalchemy.dialects.postgresql import insert as postgresql_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.ext.asyncio import AsyncConnection
from sqlalchemy.schema import CreateIndex

from .ai_account_profiles import (
    AIAccountProfileError,
    build_auto_profile,
    decode_json_list,
    decode_json_object,
    normalize_profile_data,
    profile_data_database_values,
    profile_style_signature,
    serialize_profile_revision_payload,
)
from .ai_comments_models import (
    AI_COMMENTS_SCHEMA_VERSION,
    AI_COMMENTS_TABLE_NAMES,
    AIAccountProfile,
    AIAccountProfileRevision,
    AIChannelPost,
    AIChannelPostRevision,
    AISetting,
)
from .models import Account, Base, utcnow

SUPPORTED_DIALECTS = frozenset({"sqlite", "postgresql"})
ROLLBACK_CONFIRMATION = "DROP_AI_COMMENTS_SCHEMA_V4"

AI_COMMENTS_DEFAULT_SETTINGS: dict[str, object] = {
    "schema_version": AI_COMMENTS_SCHEMA_VERSION,
    "ai_comments_enabled": False,
    "ai_comments_mode": "preview_only",
    "ai_generation_enabled": False,
    "ai_dialogues_enabled": False,
    "ai_publication_enabled": False,
    "ai_debug_snapshots_enabled": False,
    "purge_enabled": True,
    "retention_posts_days": 180,
    "retention_external_comments_days": 90,
    "retention_rejected_drafts_days": 90,
    "retention_approved_drafts_days": 365,
    "retention_publication_jobs_days": 365,
    "retention_usage_days": 365,
    "retention_aggregate_stats_days": 730,
    "retention_safe_errors_days": 30,
    "retention_debug_snapshots_days": 7,
    "retention_profile_history_days": 180,
    "retention_profile_history_max": 500,
    "retention_retired_knowledge_text_days": 90,
}

AI_COMMENTS_DROP_ORDER = (
    "ai_usage_stats",
    "ai_publication_jobs",
    "ai_comment_drafts",
    "ai_generation_jobs",
    "ai_comment_messages",
    "ai_comment_quota_events",
    "ai_comment_thread_plans",
    "ai_comment_threads",
    "ai_channel_post_revisions",
    "ai_channel_posts",
    "ai_channel_scenarios",
    "ai_account_profile_revisions",
    "ai_account_profiles",
    "ai_channel_profiles",
    "ai_knowledge_chunks",
    "ai_knowledge_sources",
    "ai_settings",
)


class AICommentsMigrationError(RuntimeError):
    """The isolated AI Comments schema is missing or incompatible."""


class AICommentsRollbackConfirmationRequired(AICommentsMigrationError):
    """A destructive rollback was requested without the exact confirmation token."""


def _canonical_json(value: object) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def ai_comments_tables() -> list[Any]:
    return [Base.metadata.tables[name] for name in AI_COMMENTS_TABLE_NAMES]


def _expected_unique_constraints(table: Any) -> set[str]:
    return {
        constraint.name
        for constraint in table.constraints
        if constraint.__class__.__name__ == "UniqueConstraint" and constraint.name
    }


def _foreign_key_signature(item: dict[str, Any]) -> tuple[tuple[str, ...], str, tuple[str, ...], str | None]:
    options = item.get("options") or {}
    ondelete = options.get("ondelete")
    return (
        tuple(item.get("constrained_columns") or ()),
        str(item.get("referred_table")),
        tuple(item.get("referred_columns") or ()),
        str(ondelete).upper() if ondelete else None,
    )


def _expected_foreign_keys(table: Any) -> set[tuple[tuple[str, ...], str, tuple[str, ...], str | None]]:
    expected = set()
    for constraint in table.foreign_key_constraints:
        elements = list(constraint.elements)
        expected.add(
            (
                tuple(element.parent.name for element in elements),
                elements[0].column.table.name,
                tuple(element.column.name for element in elements),
                str(constraint.ondelete).upper() if constraint.ondelete else None,
            )
        )
    return expected


def _preflight_existing_schema(sync_connection: Any) -> None:
    inspector = inspect(sync_connection)
    existing_tables = set(inspector.get_table_names())
    problems: list[str] = []
    for table in ai_comments_tables():
        if table.name not in existing_tables:
            continue
        actual_columns = {column["name"] for column in inspector.get_columns(table.name)}
        expected_columns = {column.name for column in table.columns}
        missing_columns = sorted(expected_columns - actual_columns)
        if missing_columns:
            problems.append(f"{table.name}: отсутствуют колонки {missing_columns}")

        expected_unique = _expected_unique_constraints(table)
        actual_unique = {
            item.get("name")
            for item in inspector.get_unique_constraints(table.name)
            if item.get("name")
        }
        missing_unique = sorted(expected_unique - actual_unique)
        if missing_unique:
            problems.append(f"{table.name}: отсутствуют UNIQUE {missing_unique}")

        expected_foreign_keys = _expected_foreign_keys(table)
        actual_foreign_keys = {
            _foreign_key_signature(item) for item in inspector.get_foreign_keys(table.name)
        }
        missing_foreign_keys = sorted(expected_foreign_keys - actual_foreign_keys)
        if missing_foreign_keys:
            problems.append(
                f"{table.name}: отсутствуют или отличаются foreign keys {missing_foreign_keys}"
            )
    if problems:
        raise AICommentsMigrationError(
            "Обнаружена несовместимая частичная схема AI Comments; автоматическое "
            "изменение существующих AI-данных остановлено:\n" + "\n".join(problems)
        )


def _create_tables(sync_connection: Any) -> None:
    Base.metadata.create_all(sync_connection, tables=ai_comments_tables(), checkfirst=True)


async def _create_missing_indexes(connection: AsyncConnection) -> None:
    for table in ai_comments_tables():
        for index in sorted(table.indexes, key=lambda item: item.name or ""):
            await connection.execute(CreateIndex(index, if_not_exists=True))


async def _seed_default_settings(connection: AsyncConnection) -> None:
    now = utcnow()
    rows = [
        {
            "key": key,
            "value_json": _canonical_json(value),
            "value_version": 1,
            "updated_by": None,
            "created_at": now,
            "updated_at": now,
        }
        for key, value in sorted(AI_COMMENTS_DEFAULT_SETTINGS.items())
    ]
    if connection.dialect.name == "sqlite":
        statement = sqlite_insert(AISetting).values(rows).on_conflict_do_nothing(
            index_elements=[AISetting.key]
        )
    elif connection.dialect.name == "postgresql":
        statement = postgresql_insert(AISetting).values(rows).on_conflict_do_nothing(
            index_elements=[AISetting.key]
        )
    else:
        raise AICommentsMigrationError(
            f"Неподдерживаемый диалект БД: {connection.dialect.name}"
        )
    await connection.execute(statement)


async def _upgrade_schema_version_setting(connection: AsyncConnection) -> None:
    row = (
        await connection.execute(
            select(
                AISetting.value_json,
                AISetting.value_version,
            ).where(AISetting.key == "schema_version")
        )
    ).one_or_none()
    if row is None:
        raise AICommentsMigrationError(
            "ai_settings.schema_version отсутствует после инициализации"
        )
    try:
        current_schema = json.loads(row.value_json)
    except (TypeError, json.JSONDecodeError) as exc:
        raise AICommentsMigrationError(
            "ai_settings.schema_version содержит некорректный JSON"
        ) from exc
    if type(current_schema) is not int:
        raise AICommentsMigrationError(
            "ai_settings.schema_version должен содержать целое число"
        )
    if current_schema == AI_COMMENTS_SCHEMA_VERSION:
        return
    if current_schema not in {1, 2, 3} or AI_COMMENTS_SCHEMA_VERSION != 4:
        raise AICommentsMigrationError(
            "Неподдерживаемый переход схемы AI Comments: "
            f"{current_schema} -> {AI_COMMENTS_SCHEMA_VERSION}"
        )
    current_version = int(row.value_version)
    if current_version < 1:
        raise AICommentsMigrationError(
            "ai_settings.schema_version содержит некорректную версию значения"
        )
    result = await connection.execute(
        update(AISetting)
        .where(
            AISetting.key == "schema_version",
            AISetting.value_version == current_version,
            AISetting.value_json == row.value_json,
        )
        .values(
            value_json=_canonical_json(AI_COMMENTS_SCHEMA_VERSION),
            value_version=current_version + 1,
            updated_at=utcnow(),
        )
    )
    if result.rowcount != 1:
        raise AICommentsMigrationError(
            "Версия схемы была одновременно изменена другим процессом"
        )


async def _backfill_current_post_revisions(connection: AsyncConnection) -> None:
    """Preserve the current snapshot of any posts created before schema v2."""

    revision_exists = exists(
        select(AIChannelPostRevision.id).where(
            AIChannelPostRevision.post_id == AIChannelPost.id,
            AIChannelPostRevision.source_revision == AIChannelPost.source_revision,
        )
    )
    source = select(
        AIChannelPost.id,
        AIChannelPost.channel_id,
        AIChannelPost.telegram_channel_id,
        AIChannelPost.telegram_message_id,
        AIChannelPost.source_revision,
        literal("backfill"),
        AIChannelPost.posted_at,
        AIChannelPost.edited_at,
        AIChannelPost.text,
        AIChannelPost.media_type,
        AIChannelPost.media_caption,
        AIChannelPost.normalized_text_hash,
        AIChannelPost.detected_topics_json,
        AIChannelPost.deleted_at,
        AIChannelPost.updated_at,
    ).where(~revision_exists)
    await connection.execute(
        insert(AIChannelPostRevision).from_select(
            [
                "post_id",
                "channel_id",
                "telegram_channel_id",
                "telegram_message_id",
                "source_revision",
                "revision_reason",
                "posted_at",
                "edited_at",
                "text",
                "media_type",
                "media_caption",
                "normalized_text_hash",
                "detected_topics_json",
                "deleted_at",
                "recorded_at",
            ],
            source,
        )
    )


def _profile_data_from_row(row: dict[str, Any]):
    try:
        return normalize_profile_data(
            {
                "name": row["name"],
                "knowledge_level": row["knowledge_level"],
                "role": row["role"],
                "style": decode_json_object(
                    row["style_json"],
                    field="ai_account_profiles.style_json",
                ),
                "allowed_claims": decode_json_list(
                    row["allowed_claims_json"],
                    field="ai_account_profiles.allowed_claims_json",
                ),
                "forbidden_claims": decode_json_list(
                    row["forbidden_claims_json"],
                    field="ai_account_profiles.forbidden_claims_json",
                ),
                "min_length": int(row["min_length"]),
                "max_length": int(row["max_length"]),
                "emoji_rate": row["emoji_rate"],
                "question_rate": row["question_rate"],
                "reply_rate": row["reply_rate"],
                "disagreement_rate": row["disagreement_rate"],
                "daily_limit": int(row["daily_limit"]),
                "cooldown_seconds": int(row["cooldown_seconds"]),
            }
        )
    except (AIAccountProfileError, TypeError, ValueError) as exc:
        raise AICommentsMigrationError(
            "Существующий профиль аккаунта содержит несовместимые данные"
        ) from exc


async def _backfill_ai_account_profiles(connection: AsyncConnection) -> None:
    """Attach profiles to stable Telegram identities and preserve their v3 audit."""

    account_rows = list(
        (
            await connection.execute(
                select(
                    Account.id,
                    Account.telegram_user_id,
                ).order_by(Account.id)
            )
        ).mappings()
    )
    account_by_id = {int(row["id"]): row for row in account_rows}

    profile_rows = list(
        (
            await connection.execute(
                select(AIAccountProfile.__table__).order_by(AIAccountProfile.id)
            )
        ).mappings()
    )
    seen_identities: dict[int, int] = {}
    profile_by_identity: dict[int, dict[str, Any]] = {}
    for row in profile_rows:
        profile_id = int(row["id"])
        account_id = int(row["account_id"]) if row["account_id"] is not None else None
        telegram_user_id = (
            int(row["telegram_user_id"])
            if row["telegram_user_id"] is not None
            else None
        )
        if account_id is not None:
            account = account_by_id.get(account_id)
            if account is None:
                raise AICommentsMigrationError(
                    "Профиль аккаунта ссылается на отсутствующий Account"
                )
            stable_identity = int(account["telegram_user_id"])
            if telegram_user_id is None:
                await connection.execute(
                    update(AIAccountProfile)
                    .where(
                        AIAccountProfile.id == profile_id,
                        AIAccountProfile.telegram_user_id.is_(None),
                    )
                    .values(telegram_user_id=stable_identity, updated_at=utcnow())
                )
                telegram_user_id = stable_identity
            elif telegram_user_id != stable_identity:
                raise AICommentsMigrationError(
                    "Профиль аккаунта связан с другим Telegram ID; upgrade остановлен"
                )
        if telegram_user_id is not None:
            previous = seen_identities.get(telegram_user_id)
            if previous is not None and previous != profile_id:
                raise AICommentsMigrationError(
                    "Найдены два профиля одной Telegram-личности; upgrade остановлен"
                )
            seen_identities[telegram_user_id] = profile_id
            profile_by_identity[telegram_user_id] = dict(row)

    # A core Account can be deleted while its AI persona remains preserved by
    # the SET NULL policy. If the same Telegram identity is added again, attach
    # that exact profile before deciding whether any new profiles are needed.
    attached_account_ids = {
        int(row["account_id"])
        for row in profile_rows
        if row["account_id"] is not None
    }
    for account in account_rows:
        account_id = int(account["id"])
        telegram_user_id = int(account["telegram_user_id"])
        existing = profile_by_identity.get(telegram_user_id)
        if existing is None or existing["account_id"] is not None:
            continue
        if account_id in attached_account_ids:
            raise AICommentsMigrationError(
                "Account уже связан с другим AI-профилем; upgrade остановлен"
            )
        await connection.execute(
            update(AIAccountProfile)
            .where(
                AIAccountProfile.id == int(existing["id"]),
                AIAccountProfile.account_id.is_(None),
            )
            .values(account_id=account_id, updated_at=utcnow())
        )
        attached_account_ids.add(account_id)
        existing["account_id"] = account_id

    profile_rows = list(
        (
            await connection.execute(
                select(AIAccountProfile.__table__).order_by(AIAccountProfile.id)
            )
        ).mappings()
    )
    occupied_signatures: list[tuple[str, ...]] = []
    for row in profile_rows:
        data = _profile_data_from_row(dict(row))
        occupied_signatures.append(
            profile_style_signature(data.style, role=data.role)
        )

    existing_identity_ids = {
        int(row["telegram_user_id"])
        for row in profile_rows
        if row["telegram_user_id"] is not None
    }
    auto_created_ids: set[int] = set()
    now = utcnow()
    for account in account_rows:
        telegram_user_id = int(account["telegram_user_id"])
        if telegram_user_id in existing_identity_ids:
            continue
        data = build_auto_profile(
            telegram_user_id,
            occupied_signatures=occupied_signatures,
        )
        values = profile_data_database_values(data)
        values.update(
            account_id=int(account["id"]),
            telegram_user_id=telegram_user_id,
            enabled=False,
            profile_version=1,
            created_at=now,
            updated_at=now,
        )
        result = await connection.execute(insert(AIAccountProfile).values(**values))
        profile_id = int(result.inserted_primary_key[0])
        auto_created_ids.add(profile_id)
        existing_identity_ids.add(telegram_user_id)
        occupied_signatures.append(
            profile_style_signature(data.style, role=data.role)
        )

    profile_rows = list(
        (
            await connection.execute(
                select(AIAccountProfile.__table__).order_by(AIAccountProfile.id)
            )
        ).mappings()
    )
    existing_revisions = {
        (int(profile_id), int(profile_version))
        for profile_id, profile_version in (
            await connection.execute(
                select(
                    AIAccountProfileRevision.profile_id,
                    AIAccountProfileRevision.profile_version,
                )
            )
        ).all()
    }
    for row in profile_rows:
        profile_id = int(row["id"])
        profile_version = int(row["profile_version"])
        if (profile_id, profile_version) in existing_revisions:
            continue
        data = _profile_data_from_row(dict(row))
        snapshot_json, snapshot_hash = serialize_profile_revision_payload(
            profile_id=profile_id,
            account_id=(
                int(row["account_id"]) if row["account_id"] is not None else None
            ),
            telegram_user_id=(
                int(row["telegram_user_id"])
                if row["telegram_user_id"] is not None
                else None
            ),
            data=data,
            enabled=bool(row["enabled"]),
            profile_version=profile_version,
        )
        await connection.execute(
            insert(AIAccountProfileRevision).values(
                profile_id=profile_id,
                telegram_user_id=row["telegram_user_id"],
                profile_version=profile_version,
                change_reason=(
                    "auto_created" if profile_id in auto_created_ids else "backfill"
                ),
                snapshot_json=snapshot_json,
                snapshot_hash=snapshot_hash,
                changed_by=None,
                created_at=row["updated_at"] or now,
            )
        )


def _inspect_schema(sync_connection: Any) -> dict[str, Any]:
    inspector = inspect(sync_connection)
    actual_tables = set(inspector.get_table_names())
    missing_tables = sorted(set(AI_COMMENTS_TABLE_NAMES) - actual_tables)
    missing_indexes: dict[str, list[str]] = {}
    missing_unique: dict[str, list[str]] = {}
    missing_foreign_keys: dict[str, list[str]] = {}
    for table in ai_comments_tables():
        if table.name not in actual_tables:
            continue
        actual_indexes = {
            item.get("name") for item in inspector.get_indexes(table.name) if item.get("name")
        }
        expected_indexes = {item.name for item in table.indexes if item.name}
        absent_indexes = sorted(expected_indexes - actual_indexes)
        if absent_indexes:
            missing_indexes[table.name] = absent_indexes

        actual_unique = {
            item.get("name")
            for item in inspector.get_unique_constraints(table.name)
            if item.get("name")
        }
        absent_unique = sorted(_expected_unique_constraints(table) - actual_unique)
        if absent_unique:
            missing_unique[table.name] = absent_unique

        actual_foreign_keys = {
            _foreign_key_signature(item) for item in inspector.get_foreign_keys(table.name)
        }
        absent_foreign_keys = sorted(_expected_foreign_keys(table) - actual_foreign_keys)
        if absent_foreign_keys:
            missing_foreign_keys[table.name] = [repr(item) for item in absent_foreign_keys]

    return {
        "tables": sorted(actual_tables.intersection(AI_COMMENTS_TABLE_NAMES)),
        "missing_tables": missing_tables,
        "missing_indexes": missing_indexes,
        "missing_unique": missing_unique,
        "missing_foreign_keys": missing_foreign_keys,
    }


async def verify_ai_comments_schema(connection: AsyncConnection) -> dict[str, Any]:
    if connection.dialect.name not in SUPPORTED_DIALECTS:
        raise AICommentsMigrationError(
            f"Неподдерживаемый диалект БД: {connection.dialect.name}"
        )
    report = await connection.run_sync(_inspect_schema)
    failures = {
        key: value
        for key, value in report.items()
        if key.startswith("missing_") and value
    }
    settings_rows = (
        await connection.execute(
            select(AISetting.key, AISetting.value_json).where(
                AISetting.key.in_(sorted(AI_COMMENTS_DEFAULT_SETTINGS))
            )
        )
    ).all()
    settings = {key: value for key, value in settings_rows}
    missing_settings = sorted(set(AI_COMMENTS_DEFAULT_SETTINGS) - set(settings))
    if missing_settings:
        failures["missing_settings"] = missing_settings
    try:
        schema_version = json.loads(settings.get("schema_version", "null"))
    except json.JSONDecodeError as exc:
        raise AICommentsMigrationError("ai_settings.schema_version содержит некорректный JSON") from exc
    if schema_version != AI_COMMENTS_SCHEMA_VERSION:
        failures["schema_version"] = {
            "expected": AI_COMMENTS_SCHEMA_VERSION,
            "actual": schema_version,
        }
    if failures:
        raise AICommentsMigrationError(
            "Проверка схемы AI Comments не пройдена: "
            + json.dumps(failures, ensure_ascii=False, sort_keys=True)
        )
    report["settings_count"] = len(settings)
    report["schema_version"] = schema_version
    report["dialect"] = connection.dialect.name
    return report


async def upgrade_ai_comments_schema(connection: AsyncConnection) -> dict[str, Any]:
    """Create and verify the isolated schema without enabling any AI behavior."""

    if connection.dialect.name not in SUPPORTED_DIALECTS:
        raise AICommentsMigrationError(
            f"Неподдерживаемый диалект БД: {connection.dialect.name}"
        )
    await connection.run_sync(_preflight_existing_schema)
    await connection.run_sync(_create_tables)
    await _seed_default_settings(connection)
    await _backfill_current_post_revisions(connection)
    await _backfill_ai_account_profiles(connection)
    # The stable identity index is created only after v2 rows have been checked,
    # repaired from Account where possible and proven duplicate-free.
    await _create_missing_indexes(connection)
    await _upgrade_schema_version_setting(connection)
    return await verify_ai_comments_schema(connection)


def _drop_tables(sync_connection: Any, table_names: Iterable[str]) -> None:
    for table_name in table_names:
        Base.metadata.tables[table_name].drop(sync_connection, checkfirst=True)


async def rollback_ai_comments_schema(
    connection: AsyncConnection,
    *,
    confirmation: str,
) -> dict[str, Any]:
    """Drop only AI Comments tables after an explicit destructive confirmation."""

    if confirmation != ROLLBACK_CONFIRMATION:
        raise AICommentsRollbackConfirmationRequired(
            "Rollback удаляет все данные AI Comments. Сначала создайте backup и "
            f"передайте точное подтверждение {ROLLBACK_CONFIRMATION}."
        )
    if connection.dialect.name not in SUPPORTED_DIALECTS:
        raise AICommentsMigrationError(
            f"Неподдерживаемый диалект БД: {connection.dialect.name}"
        )
    await connection.run_sync(_drop_tables, AI_COMMENTS_DROP_ORDER)
    remaining = await connection.run_sync(
        lambda sync: sorted(
            set(inspect(sync).get_table_names()).intersection(AI_COMMENTS_TABLE_NAMES)
        )
    )
    if remaining:
        raise AICommentsMigrationError(
            f"Rollback не удалил таблицы AI Comments: {remaining}"
        )
    return {
        "dialect": connection.dialect.name,
        "dropped_tables": list(AI_COMMENTS_DROP_ORDER),
        "remaining_ai_tables": remaining,
    }
