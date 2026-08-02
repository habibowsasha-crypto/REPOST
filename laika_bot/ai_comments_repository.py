from __future__ import annotations

import json
import re
from datetime import datetime
from typing import Final

from sqlalchemy import func, select, update
from sqlalchemy.exc import IntegrityError

from .ai_channel_memory import (
    AI_CHANNEL_MEMORY_MIN_WINDOW,
    AI_CHANNEL_MEMORY_WINDOW,
    AI_CHANNEL_PROFILE_FIELDS,
    MAX_DATABASE_ID,
    AIChannelMemoryConflictError,
    AIChannelMemoryError,
    AIChannelMemorySnapshot,
    AIChannelPostIngestResult,
    AIChannelPostPayload,
    AIChannelPostRevisionSnapshot,
    AIChannelPostSnapshot,
    AIChannelProfileSnapshot,
    AIChannelScenarioSnapshot,
    canonical_json,
    decode_json_list,
    decode_json_object,
    normalize_ai_datetime,
    normalize_ai_memory_list,
    normalize_ai_memory_text,
    normalized_ai_channel_post_hash,
    validate_ai_channel_post_payload,
)
from .ai_comments_migration import AI_COMMENTS_DEFAULT_SETTINGS
from .ai_comments_models import (
    AIChannelPost,
    AIChannelPostRevision,
    AIChannelProfile,
    AIChannelScenario,
    AISetting,
    AIUsageStat,
)
from .models import Channel, utcnow

AI_COMMENTS_FLAG_KEYS: Final[tuple[str, ...]] = (
    "ai_comments_enabled",
    "ai_generation_enabled",
    "ai_dialogues_enabled",
    "ai_publication_enabled",
    "ai_debug_snapshots_enabled",
)
AI_COMMENTS_MUTABLE_UI_FLAGS: Final[frozenset[str]] = frozenset(
    {"ai_comments_enabled", "ai_generation_enabled", "ai_dialogues_enabled"}
)
AI_COMMENTS_SETTING_UPDATE_ATTEMPTS = 3
AI_CHANNEL_MEMORY_UPDATE_ATTEMPTS = 4
MAX_BIGINT = MAX_DATABASE_ID


class AICommentsSettingsError(RuntimeError):
    """The isolated AI settings store is missing, corrupt or concurrently busy."""


def _decode_boolean_setting(key: str, value_json: str) -> bool:
    try:
        value = json.loads(value_json)
    except (TypeError, json.JSONDecodeError) as exc:
        raise AICommentsSettingsError(
            f"Настройка {key} содержит некорректный JSON"
        ) from exc
    if type(value) is not bool:
        raise AICommentsSettingsError(
            f"Настройка {key} должна содержать boolean"
        )
    return value


def _optional_json_text(
    values: dict[str, object],
    key: str,
    *,
    field: str,
    max_length: int,
) -> str | None:
    raw = values.get(key)
    if raw is None:
        return None
    try:
        return normalize_ai_memory_text(
            raw,
            field=field,
            max_length=max_length,
        )
    except (TypeError, ValueError) as exc:
        raise AIChannelMemoryError(str(exc)) from exc


def _profile_snapshot(
    profile: AIChannelProfile,
    *,
    channel_title: str,
) -> AIChannelProfileSnapshot:
    author_style = decode_json_object(
        profile.author_style_json,
        field="author_style_json",
    )
    audience_style = decode_json_object(
        profile.audience_style_json,
        field="audience_style_json",
    )
    try:
        main_terms = normalize_ai_memory_list(
            audience_style.get("main_terms"),
            field="main_terms",
        )
    except (TypeError, ValueError) as exc:
        raise AIChannelMemoryError(str(exc)) from exc
    return AIChannelProfileSnapshot(
        id=int(profile.id),
        channel_id=int(profile.channel_id) if profile.channel_id is not None else None,
        telegram_channel_id=(
            int(profile.telegram_channel_id)
            if profile.telegram_channel_id is not None
            else None
        ),
        channel_title=channel_title,
        description=profile.description,
        topic=profile.topic,
        target_audience=_optional_json_text(
            audience_style,
            "target_audience",
            field="target_audience",
            max_length=1_000,
        ),
        language=_optional_json_text(
            author_style,
            "language",
            field="language",
            max_length=64,
        ),
        author_style=_optional_json_text(
            author_style,
            "description",
            field="author_style",
            max_length=1_000,
        ),
        audience_style=_optional_json_text(
            audience_style,
            "description",
            field="audience_style",
            max_length=1_000,
        ),
        typical_comment_length=_optional_json_text(
            author_style,
            "typical_comment_length",
            field="typical_comment_length",
            max_length=128,
        ),
        main_terms=main_terms,
        methodology=decode_json_list(
            profile.methodology_json,
            field="methodology_json",
        ),
        allowed_topics=decode_json_list(
            profile.allowed_topics_json,
            field="allowed_topics_json",
        ),
        forbidden_topics=decode_json_list(
            profile.forbidden_topics_json,
            field="forbidden_topics_json",
        ),
        advertising_restrictions=_optional_json_text(
            author_style,
            "advertising_restrictions",
            field="advertising_restrictions",
            max_length=1_000,
        ),
        admin_notes=profile.admin_notes,
        memory_summary=profile.memory_summary,
        enabled=bool(profile.enabled),
        profile_version=int(profile.profile_version),
        created_at=profile.created_at,
        updated_at=profile.updated_at,
    )


def _post_snapshot(post: AIChannelPost) -> AIChannelPostSnapshot:
    return AIChannelPostSnapshot(
        id=int(post.id),
        channel_id=int(post.channel_id) if post.channel_id is not None else None,
        telegram_channel_id=int(post.telegram_channel_id),
        telegram_message_id=int(post.telegram_message_id),
        source_revision=int(post.source_revision),
        posted_at=post.posted_at,
        edited_at=post.edited_at,
        text=post.text,
        media_type=post.media_type,
        media_caption=post.media_caption,
        normalized_text_hash=post.normalized_text_hash,
        detected_topics=decode_json_list(
            post.detected_topics_json,
            field="detected_topics_json",
        ),
        linked_scenario_id=(
            int(post.linked_scenario_id)
            if post.linked_scenario_id is not None
            else None
        ),
        deleted_at=post.deleted_at,
        created_at=post.created_at,
        updated_at=post.updated_at,
    )


def _post_revision_snapshot(
    revision: AIChannelPostRevision,
) -> AIChannelPostRevisionSnapshot:
    return AIChannelPostRevisionSnapshot(
        id=int(revision.id),
        post_id=int(revision.post_id),
        channel_id=(
            int(revision.channel_id) if revision.channel_id is not None else None
        ),
        telegram_channel_id=int(revision.telegram_channel_id),
        telegram_message_id=int(revision.telegram_message_id),
        source_revision=int(revision.source_revision),
        revision_reason=revision.revision_reason,
        posted_at=revision.posted_at,
        edited_at=revision.edited_at,
        text=revision.text,
        media_type=revision.media_type,
        media_caption=revision.media_caption,
        normalized_text_hash=revision.normalized_text_hash,
        detected_topics=decode_json_list(
            revision.detected_topics_json,
            field="revision.detected_topics_json",
        ),
        deleted_at=revision.deleted_at,
        recorded_at=revision.recorded_at,
    )


def _new_post_revision(
    post: AIChannelPost,
    *,
    reason: str,
) -> AIChannelPostRevision:
    if reason not in {
        "ingested",
        "edited",
        "metadata",
        "deleted",
        "restored",
    }:
        raise ValueError("Некорректная причина ревизии публикации")
    return AIChannelPostRevision(
        post_id=post.id,
        channel_id=post.channel_id,
        telegram_channel_id=post.telegram_channel_id,
        telegram_message_id=post.telegram_message_id,
        source_revision=post.source_revision,
        revision_reason=reason,
        posted_at=post.posted_at,
        edited_at=post.edited_at,
        text=post.text,
        media_type=post.media_type,
        media_caption=post.media_caption,
        normalized_text_hash=post.normalized_text_hash,
        detected_topics_json=post.detected_topics_json,
        deleted_at=post.deleted_at,
        recorded_at=post.updated_at,
    )


def _scenario_snapshot(scenario: AIChannelScenario) -> AIChannelScenarioSnapshot:
    return AIChannelScenarioSnapshot(
        id=int(scenario.id),
        channel_id=(
            int(scenario.channel_id) if scenario.channel_id is not None else None
        ),
        telegram_channel_id=(
            int(scenario.telegram_channel_id)
            if scenario.telegram_channel_id is not None
            else None
        ),
        title=scenario.title,
        symbol=scenario.symbol,
        direction=scenario.direction,
        status=scenario.status,
        factual_summary=scenario.factual_summary,
        opened_by_post_id=(
            int(scenario.opened_by_post_id)
            if scenario.opened_by_post_id is not None
            else None
        ),
        closed_by_post_id=(
            int(scenario.closed_by_post_id)
            if scenario.closed_by_post_id is not None
            else None
        ),
        started_at=scenario.started_at,
        ended_at=scenario.ended_at,
        created_at=scenario.created_at,
        updated_at=scenario.updated_at,
    )


def _validate_database_id(value: int, *, field: str) -> int:
    if (
        not isinstance(value, int)
        or isinstance(value, bool)
        or not 0 < value <= MAX_DATABASE_ID
    ):
        raise ValueError(f"Некорректный {field}")
    return value


def _normalized_profile_value(field: str, value: object) -> object:
    spec = AI_CHANNEL_PROFILE_FIELDS.get(field)
    if spec is None:
        raise ValueError("Некорректное поле профиля канала")
    if spec.kind == "list":
        return normalize_ai_memory_list(value, field=spec.label)
    if value is None:
        return None
    return normalize_ai_memory_text(
        value,
        field=spec.label,
        max_length=spec.max_length,
        allow_empty=False,
    )


def _profile_update_values(
    profile: AIChannelProfile,
    *,
    field: str,
    value: object,
) -> dict[str, object]:
    normalized = _normalized_profile_value(field, value)
    if field in {"description", "topic", "admin_notes", "memory_summary"}:
        return {field: normalized}

    if field in {"methodology", "allowed_topics", "forbidden_topics"}:
        column = {
            "methodology": "methodology_json",
            "allowed_topics": "allowed_topics_json",
            "forbidden_topics": "forbidden_topics_json",
        }[field]
        values = tuple(normalized)
        if field in {"allowed_topics", "forbidden_topics"}:
            other_raw = (
                profile.forbidden_topics_json
                if field == "allowed_topics"
                else profile.allowed_topics_json
            )
            other = decode_json_list(other_raw, field="topics_json")
            overlap = {item.casefold() for item in values}.intersection(
                item.casefold() for item in other
            )
            if overlap:
                raise ValueError(
                    "Одна тема не может быть одновременно разрешённой и запрещённой"
                )
        return {column: canonical_json(list(values))}

    if field in {
        "language",
        "author_style",
        "typical_comment_length",
        "advertising_restrictions",
    }:
        values = decode_json_object(
            profile.author_style_json,
            field="author_style_json",
        )
        key = "description" if field == "author_style" else field
        if normalized is None:
            values.pop(key, None)
        else:
            values[key] = normalized
        return {"author_style_json": canonical_json(values)}

    if field in {"target_audience", "audience_style", "main_terms"}:
        values = decode_json_object(
            profile.audience_style_json,
            field="audience_style_json",
        )
        key = "description" if field == "audience_style" else field
        serialized = list(normalized) if field == "main_terms" else normalized
        if serialized is None or serialized == () or serialized == []:
            values.pop(key, None)
        else:
            values[key] = serialized
        return {"audience_style_json": canonical_json(values)}

    raise ValueError("Некорректное поле профиля канала")


class AICommentsRepositoryMixin:
    async def record_ai_gateway_usage(
        self,
        *,
        model_name: str,
        request_id_safe: str | None,
        input_tokens: int,
        output_tokens: int,
        cached_tokens: int,
        latency_ms: int,
        success: bool,
        error_class: str | None,
    ) -> int:
        """Persist one safe Step 10 gateway audit row without prompts or secrets."""

        model = str(model_name or "").strip()
        if re.fullmatch(r"[A-Za-z0-9_.:-]{1,96}", model) is None:
            raise ValueError("Некорректное название модели OpenAI")

        request_id = None
        if request_id_safe is not None:
            request_id = str(request_id_safe).strip()
            if re.fullmatch(r"[A-Za-z0-9_.:-]{1,160}", request_id) is None:
                raise ValueError("Некорректный безопасный request ID")

        numeric = {
            "input_tokens": input_tokens,
            "output_tokens": output_tokens,
            "cached_tokens": cached_tokens,
            "latency_ms": latency_ms,
        }
        for field, value in numeric.items():
            if (
                not isinstance(value, int)
                or isinstance(value, bool)
                or value < 0
                or value > MAX_BIGINT
            ):
                raise ValueError(f"Некорректное значение {field}")

        if type(success) is not bool:
            raise TypeError("success должен быть boolean")
        safe_error = None
        if error_class is not None:
            safe_error = str(error_class).strip()
            if re.fullmatch(r"[a-z0-9_]{1,64}", safe_error) is None:
                raise ValueError("Некорректный класс ошибки OpenAI")
        if success and safe_error is not None:
            raise ValueError("Успешная проверка не может содержать error_class")
        if not success and safe_error is None:
            raise ValueError("Неуспешная проверка должна содержать error_class")

        async with self.sessions() as session:
            row = AIUsageStat(
                generation_job_id=None,
                model_name=model,
                request_id_safe=request_id,
                input_tokens=input_tokens,
                output_tokens=output_tokens,
                cached_tokens=cached_tokens,
                tool_calls=0,
                latency_ms=latency_ms,
                cost_usd=None,
                success=success,
                error_class=safe_error,
            )
            session.add(row)
            await session.commit()
            await session.refresh(row)
            return int(row.id)

    async def get_ai_comments_flags(self) -> dict[str, bool]:
        """Read fail-closed flags from the isolated settings table."""

        defaults = {
            key: bool(AI_COMMENTS_DEFAULT_SETTINGS[key])
            for key in AI_COMMENTS_FLAG_KEYS
        }
        async with self.sessions() as session:
            rows = (
                await session.execute(
                    select(AISetting.key, AISetting.value_json).where(
                        AISetting.key.in_(AI_COMMENTS_FLAG_KEYS)
                    )
                )
            ).all()
        for key, value_json in rows:
            defaults[str(key)] = _decode_boolean_setting(str(key), value_json)
        return defaults

    async def set_ai_comments_flag(
        self,
        key: str,
        value: bool,
        *,
        updated_by: int,
    ) -> dict[str, object]:
        """Persist one admin-editable flag with an optimistic version guard."""

        if key not in AI_COMMENTS_MUTABLE_UI_FLAGS:
            raise ValueError("Эту настройку нельзя менять из меню")
        if type(value) is not bool:
            raise TypeError("Значение feature flag должно быть boolean")
        if (
            not isinstance(updated_by, int)
            or isinstance(updated_by, bool)
            or not 0 < updated_by <= MAX_BIGINT
        ):
            raise ValueError("Некорректный ID администратора")

        for _attempt in range(AI_COMMENTS_SETTING_UPDATE_ATTEMPTS):
            async with self.sessions() as session:
                row = (
                    await session.execute(
                        select(
                            AISetting.value_json,
                            AISetting.value_version,
                        ).where(AISetting.key == key)
                    )
                ).one_or_none()
                if row is None:
                    raise AICommentsSettingsError(
                        "Схема AI Comments не инициализирована"
                    )
                current_value = _decode_boolean_setting(key, row.value_json)
                current_version = int(row.value_version)
                if current_version < 1:
                    raise AICommentsSettingsError(
                        f"Настройка {key} содержит некорректную версию"
                    )
                if current_value is value:
                    return {
                        "key": key,
                        "value": value,
                        "version": current_version,
                        "changed": False,
                    }

                result = await session.execute(
                    update(AISetting)
                    .where(
                        AISetting.key == key,
                        AISetting.value_version == current_version,
                    )
                    .values(
                        value_json=json.dumps(value),
                        value_version=current_version + 1,
                        updated_by=updated_by,
                        updated_at=utcnow(),
                    )
                )
                if result.rowcount == 1:
                    await session.commit()
                    return {
                        "key": key,
                        "value": value,
                        "version": current_version + 1,
                        "changed": True,
                    }
                await session.rollback()

        raise AICommentsSettingsError(
            "Настройка была одновременно изменена; обновите экран и повторите"
        )

    async def ensure_ai_channel_profile(
        self,
        channel_id: int,
    ) -> AIChannelProfileSnapshot:
        """Create one isolated memory profile for an existing broadcast channel."""

        channel_id = _validate_database_id(channel_id, field="ID канала")
        for _attempt in range(AI_CHANNEL_MEMORY_UPDATE_ATTEMPTS):
            async with self.sessions() as session:
                channel = await session.get(Channel, channel_id)
                if channel is None or channel.kind != "channel":
                    raise AIChannelMemoryError("Канал не найден или больше недоступен")
                profile = await session.scalar(
                    select(AIChannelProfile).where(
                        AIChannelProfile.channel_id == channel_id
                    )
                )
                if profile is not None:
                    return _profile_snapshot(profile, channel_title=channel.title)
                profile = AIChannelProfile(
                    channel_id=channel.id,
                    telegram_channel_id=channel.telegram_channel_id,
                    enabled=True,
                    profile_version=1,
                )
                session.add(profile)
                try:
                    await session.commit()
                except IntegrityError:
                    await session.rollback()
                    continue
                await session.refresh(profile)
                return _profile_snapshot(profile, channel_title=channel.title)
        raise AIChannelMemoryConflictError(
            "Профиль канала был одновременно создан другим процессом"
        )

    async def get_ai_channel_profile(
        self,
        channel_id: int,
    ) -> AIChannelProfileSnapshot | None:
        channel_id = _validate_database_id(channel_id, field="ID канала")
        async with self.sessions() as session:
            row = (
                await session.execute(
                    select(Channel, AIChannelProfile)
                    .join(
                        AIChannelProfile,
                        AIChannelProfile.channel_id == Channel.id,
                    )
                    .where(
                        Channel.id == channel_id,
                        Channel.kind == "channel",
                    )
                )
            ).one_or_none()
            if row is None:
                return None
            channel, profile = row
            return _profile_snapshot(profile, channel_title=channel.title)

    async def list_enabled_ai_channel_memory_ids(self) -> set[int]:
        """Return only active channels explicitly selected for AI memory."""

        async with self.sessions() as session:
            rows = await session.scalars(
                select(Channel.id)
                .join(
                    AIChannelProfile,
                    AIChannelProfile.channel_id == Channel.id,
                )
                .where(
                    Channel.kind == "channel",
                    Channel.is_active.is_(True),
                    AIChannelProfile.enabled.is_(True),
                )
            )
            return {int(channel_id) for channel_id in rows}

    async def update_ai_channel_profile_field(
        self,
        channel_id: int,
        field: str,
        value: object,
        *,
        expected_version: int,
        updated_by: int,
    ) -> dict[str, object]:
        """Update one manual field without overwriting a newer profile revision."""

        channel_id = _validate_database_id(channel_id, field="ID канала")
        _validate_database_id(updated_by, field="ID администратора")
        if (
            not isinstance(expected_version, int)
            or isinstance(expected_version, bool)
            or expected_version < 1
        ):
            raise ValueError("Некорректная версия профиля")

        async with self.sessions() as session:
            row = (
                await session.execute(
                    select(Channel, AIChannelProfile)
                    .join(
                        AIChannelProfile,
                        AIChannelProfile.channel_id == Channel.id,
                    )
                    .where(
                        Channel.id == channel_id,
                        Channel.kind == "channel",
                    )
                )
            ).one_or_none()
            if row is None:
                raise AIChannelMemoryError("Профиль памяти канала не найден")
            channel, profile = row
            current_version = int(profile.profile_version)
            if current_version != expected_version:
                raise AIChannelMemoryConflictError(
                    "Профиль уже изменён. Обновите экран и повторите"
                )
            values = _profile_update_values(profile, field=field, value=value)
            if all(getattr(profile, key) == item for key, item in values.items()):
                return {
                    "changed": False,
                    "version": current_version,
                    "profile": _profile_snapshot(
                        profile,
                        channel_title=channel.title,
                    ),
                }
            next_version = current_version + 1
            values.update(profile_version=next_version, updated_at=utcnow())
            result = await session.execute(
                update(AIChannelProfile)
                .where(
                    AIChannelProfile.id == profile.id,
                    AIChannelProfile.profile_version == current_version,
                )
                .values(**values)
            )
            if result.rowcount != 1:
                await session.rollback()
                raise AIChannelMemoryConflictError(
                    "Профиль уже изменён. Обновите экран и повторите"
                )
            await session.commit()
            refreshed = await session.get(AIChannelProfile, profile.id)
            if refreshed is None:
                raise AIChannelMemoryError("Профиль исчез во время обновления")
            return {
                "changed": True,
                "version": next_version,
                "profile": _profile_snapshot(
                    refreshed,
                    channel_title=channel.title,
                ),
            }

    async def ingest_ai_channel_post(
        self,
        channel_id: int,
        payload: AIChannelPostPayload,
    ) -> AIChannelPostIngestResult:
        """Idempotently store one Telegram post and guard against stale edits."""

        channel_id = _validate_database_id(channel_id, field="ID канала")
        payload = validate_ai_channel_post_payload(payload)
        content_hash = normalized_ai_channel_post_hash(payload)
        topics_json = canonical_json(list(payload.detected_topics))

        for _attempt in range(AI_CHANNEL_MEMORY_UPDATE_ATTEMPTS):
            async with self.sessions() as session:
                channel = await session.get(Channel, channel_id)
                if channel is None or channel.kind != "channel":
                    raise AIChannelMemoryError("Канал не найден или больше недоступен")
                post = await session.scalar(
                    select(AIChannelPost).where(
                        AIChannelPost.telegram_channel_id
                        == channel.telegram_channel_id,
                        AIChannelPost.telegram_message_id
                        == payload.telegram_message_id,
                    )
                )
                if post is None:
                    post = AIChannelPost(
                        channel_id=channel.id,
                        telegram_channel_id=channel.telegram_channel_id,
                        telegram_message_id=payload.telegram_message_id,
                        source_revision=1,
                        posted_at=payload.posted_at,
                        edited_at=payload.edited_at,
                        text=payload.text,
                        media_type=payload.media_type,
                        media_caption=payload.media_caption,
                        normalized_text_hash=content_hash,
                        detected_topics_json=topics_json,
                    )
                    session.add(post)
                    try:
                        await session.flush()
                        session.add(_new_post_revision(post, reason="ingested"))
                        await session.commit()
                    except IntegrityError:
                        await session.rollback()
                        continue
                    await session.refresh(post)
                    return AIChannelPostIngestResult(
                        status="created",
                        post=_post_snapshot(post),
                    )

                current_revision = int(post.source_revision)
                same_content = post.normalized_text_hash == content_hash
                restored = post.deleted_at is not None
                if not same_content and post.edited_at is not None:
                    if payload.edited_at is None or payload.edited_at <= post.edited_at:
                        return AIChannelPostIngestResult(
                            status="stale",
                            post=_post_snapshot(post),
                        )

                values: dict[str, object] = {
                    "channel_id": channel.id,
                    "telegram_channel_id": channel.telegram_channel_id,
                }
                status: str
                if same_content:
                    if (
                        payload.edited_at is not None
                        and (
                            post.edited_at is None
                            or payload.edited_at > post.edited_at
                        )
                    ):
                        values["edited_at"] = payload.edited_at
                    if post.detected_topics_json != topics_json:
                        values["detected_topics_json"] = topics_json
                    if restored:
                        values.update(
                            deleted_at=None,
                            source_revision=current_revision + 1,
                        )
                        status = "restored"
                    else:
                        status = "unchanged"
                else:
                    values.update(
                        source_revision=current_revision + 1,
                        edited_at=payload.edited_at or utcnow(),
                        text=payload.text,
                        media_type=payload.media_type,
                        media_caption=payload.media_caption,
                        normalized_text_hash=content_hash,
                        detected_topics_json=topics_json,
                        deleted_at=None,
                    )
                    status = "restored" if restored else "updated"

                changed = any(
                    getattr(post, key) != item for key, item in values.items()
                )
                if not changed:
                    return AIChannelPostIngestResult(
                        status="unchanged",
                        post=_post_snapshot(post),
                    )
                if same_content and not restored:
                    # Metadata is part of the factual source state too. Bumping the
                    # revision keeps the optimistic guard effective when two refreshes
                    # observe different edit timestamps or topic metadata.
                    values["source_revision"] = current_revision + 1
                    status = "updated"
                values["updated_at"] = utcnow()
                result = await session.execute(
                    update(AIChannelPost)
                    .where(
                        AIChannelPost.id == post.id,
                        AIChannelPost.source_revision == current_revision,
                    )
                    .values(**values)
                )
                if result.rowcount != 1:
                    await session.rollback()
                    continue
                await session.refresh(post)
                revision_reason = (
                    "restored"
                    if status == "restored"
                    else "metadata"
                    if same_content
                    else "edited"
                )
                session.add(_new_post_revision(post, reason=revision_reason))
                try:
                    await session.commit()
                except IntegrityError:
                    await session.rollback()
                    continue
                return AIChannelPostIngestResult(
                    status=status,  # type: ignore[arg-type]
                    post=_post_snapshot(post),
                )

        raise AIChannelMemoryConflictError(
            "Публикация одновременно изменяется другим процессом"
        )

    async def ingest_ai_channel_posts(
        self,
        channel_id: int,
        payloads: tuple[AIChannelPostPayload, ...] | list[AIChannelPostPayload],
    ) -> dict[str, int]:
        counters = {
            "processed": 0,
            "created": 0,
            "updated": 0,
            "unchanged": 0,
            "stale": 0,
            "restored": 0,
        }
        for payload in payloads:
            result = await self.ingest_ai_channel_post(channel_id, payload)
            counters["processed"] += 1
            counters[result.status] += 1
        return counters

    async def mark_ai_channel_post_deleted(
        self,
        channel_id: int,
        telegram_message_id: int,
        *,
        deleted_at: datetime | None = None,
    ) -> AIChannelPostIngestResult:
        """Tombstone a post while retaining its text, hash and revision audit."""

        channel_id = _validate_database_id(channel_id, field="ID канала")
        telegram_message_id = _validate_database_id(
            telegram_message_id,
            field="ID публикации",
        )
        deletion_time = normalize_ai_datetime(
            deleted_at or utcnow(),
            field="deleted_at",
        )
        for _attempt in range(AI_CHANNEL_MEMORY_UPDATE_ATTEMPTS):
            async with self.sessions() as session:
                channel = await session.get(Channel, channel_id)
                if channel is None or channel.kind != "channel":
                    raise AIChannelMemoryError("Канал не найден или больше недоступен")
                post = await session.scalar(
                    select(AIChannelPost).where(
                        AIChannelPost.telegram_channel_id
                        == channel.telegram_channel_id,
                        AIChannelPost.telegram_message_id == telegram_message_id,
                    )
                )
                if post is None:
                    raise AIChannelMemoryError("Публикация не найдена в памяти")
                if deletion_time < post.posted_at:
                    raise ValueError("deleted_at не может быть раньше posted_at")
                if post.deleted_at is not None:
                    return AIChannelPostIngestResult(
                        status="deleted",
                        post=_post_snapshot(post),
                    )
                current_revision = int(post.source_revision)
                result = await session.execute(
                    update(AIChannelPost)
                    .where(
                        AIChannelPost.id == post.id,
                        AIChannelPost.source_revision == current_revision,
                    )
                    .values(
                        deleted_at=deletion_time,
                        source_revision=current_revision + 1,
                        updated_at=utcnow(),
                    )
                )
                if result.rowcount != 1:
                    await session.rollback()
                    continue
                await session.refresh(post)
                session.add(_new_post_revision(post, reason="deleted"))
                try:
                    await session.commit()
                except IntegrityError:
                    await session.rollback()
                    continue
                return AIChannelPostIngestResult(
                    status="deleted",
                    post=_post_snapshot(post),
                )
        raise AIChannelMemoryConflictError(
            "Публикация одновременно изменяется другим процессом"
        )

    async def get_ai_channel_post(
        self,
        channel_id: int,
        post_id: int,
    ) -> AIChannelPostSnapshot | None:
        channel_id = _validate_database_id(channel_id, field="ID канала")
        post_id = _validate_database_id(post_id, field="ID публикации")
        async with self.sessions() as session:
            post = await session.scalar(
                select(AIChannelPost).where(
                    AIChannelPost.id == post_id,
                    AIChannelPost.channel_id == channel_id,
                )
            )
            return _post_snapshot(post) if post is not None else None

    async def list_ai_channel_post_revisions(
        self,
        channel_id: int,
        post_id: int,
        *,
        limit: int = 20,
    ) -> tuple[AIChannelPostRevisionSnapshot, ...]:
        channel_id = _validate_database_id(channel_id, field="ID канала")
        post_id = _validate_database_id(post_id, field="ID публикации")
        if not isinstance(limit, int) or isinstance(limit, bool) or not 1 <= limit <= 100:
            raise ValueError("Лимит ревизий должен быть от 1 до 100")
        async with self.sessions() as session:
            post = await session.scalar(
                select(AIChannelPost.id).where(
                    AIChannelPost.id == post_id,
                    AIChannelPost.channel_id == channel_id,
                )
            )
            if post is None:
                return ()
            revisions = list(
                (
                    await session.scalars(
                        select(AIChannelPostRevision)
                        .where(AIChannelPostRevision.post_id == post_id)
                        .order_by(
                            AIChannelPostRevision.source_revision.desc(),
                            AIChannelPostRevision.id.desc(),
                        )
                        .limit(limit)
                    )
                ).all()
            )
            return tuple(
                _post_revision_snapshot(revision) for revision in revisions
            )

    async def get_ai_channel_memory(
        self,
        channel_id: int,
        *,
        limit: int = AI_CHANNEL_MEMORY_WINDOW,
    ) -> AIChannelMemorySnapshot:
        channel_id = _validate_database_id(channel_id, field="ID канала")
        if not AI_CHANNEL_MEMORY_MIN_WINDOW <= limit <= AI_CHANNEL_MEMORY_WINDOW:
            raise ValueError("Окно памяти должно содержать от 5 до 10 публикаций")
        async with self.sessions() as session:
            row = (
                await session.execute(
                    select(Channel, AIChannelProfile)
                    .join(
                        AIChannelProfile,
                        AIChannelProfile.channel_id == Channel.id,
                    )
                    .where(
                        Channel.id == channel_id,
                        Channel.kind == "channel",
                    )
                )
            ).one_or_none()
            if row is None:
                raise AIChannelMemoryError("Профиль памяти канала не найден")
            channel, profile = row
            posts = list(
                (
                    await session.scalars(
                        select(AIChannelPost)
                        .where(
                            AIChannelPost.channel_id == channel_id,
                            AIChannelPost.deleted_at.is_(None),
                        )
                        .order_by(
                            AIChannelPost.posted_at.desc(),
                            AIChannelPost.telegram_message_id.desc(),
                        )
                        .limit(limit)
                    )
                ).all()
            )
            scenarios = list(
                (
                    await session.scalars(
                        select(AIChannelScenario)
                        .where(
                            AIChannelScenario.channel_id == channel_id,
                            AIChannelScenario.status == "active",
                        )
                        .order_by(
                            AIChannelScenario.started_at.desc(),
                            AIChannelScenario.id.desc(),
                        )
                        .limit(20)
                    )
                ).all()
            )
            stored_count = int(
                await session.scalar(
                    select(func.count(AIChannelPost.id)).where(
                        AIChannelPost.channel_id == channel_id
                    )
                )
                or 0
            )
            deleted_count = int(
                await session.scalar(
                    select(func.count(AIChannelPost.id)).where(
                        AIChannelPost.channel_id == channel_id,
                        AIChannelPost.deleted_at.is_not(None),
                    )
                )
                or 0
            )
            return AIChannelMemorySnapshot(
                profile=_profile_snapshot(profile, channel_title=channel.title),
                recent_posts=tuple(_post_snapshot(post) for post in posts),
                active_scenarios=tuple(
                    _scenario_snapshot(scenario) for scenario in scenarios
                ),
                stored_post_count=stored_count,
                deleted_post_count=deleted_count,
            )

    async def create_ai_channel_scenario(
        self,
        channel_id: int,
        *,
        title: str,
        symbol: str | None = None,
        direction: str | None = None,
        factual_summary: str | None = None,
        opened_by_telegram_message_id: int | None = None,
        started_at: datetime | None = None,
    ) -> AIChannelScenarioSnapshot:
        channel_id = _validate_database_id(channel_id, field="ID канала")
        title_value = normalize_ai_memory_text(
            title,
            field="Название сценария",
            max_length=255,
            allow_empty=False,
        )
        symbol_value = normalize_ai_memory_text(
            symbol,
            field="Торговый символ",
            max_length=32,
        )
        if symbol_value is not None:
            symbol_value = symbol_value.upper()
        direction_value = normalize_ai_memory_text(
            direction,
            field="Направление",
            max_length=16,
        )
        if direction_value is not None:
            direction_value = direction_value.upper()
            if direction_value not in {"LONG", "SHORT"}:
                raise ValueError("Направление должно быть LONG, SHORT или -")
        summary_value = normalize_ai_memory_text(
            factual_summary,
            field="Фактическая сводка",
            max_length=4_000,
        )
        started = normalize_ai_datetime(
            started_at or utcnow(),
            field="started_at",
        )
        if opened_by_telegram_message_id is not None:
            opened_by_telegram_message_id = _validate_database_id(
                opened_by_telegram_message_id,
                field="ID публикации",
            )

        async with self.sessions() as session:
            channel = await session.get(Channel, channel_id)
            if channel is None or channel.kind != "channel":
                raise AIChannelMemoryError("Канал не найден или больше недоступен")
            opening_post = None
            if opened_by_telegram_message_id is not None:
                opening_post = await session.scalar(
                    select(AIChannelPost).where(
                        AIChannelPost.channel_id == channel_id,
                        AIChannelPost.telegram_message_id
                        == opened_by_telegram_message_id,
                        AIChannelPost.deleted_at.is_(None),
                    )
                )
                if opening_post is None:
                    raise AIChannelMemoryError(
                        "Указанная публикация не найдена в памяти этого канала"
                    )
                if opening_post.linked_scenario_id is not None:
                    raise AIChannelMemoryConflictError(
                        "Публикация уже связана с другим сценарием"
                    )
            scenario = AIChannelScenario(
                channel_id=channel.id,
                telegram_channel_id=channel.telegram_channel_id,
                title=title_value,
                symbol=symbol_value,
                direction=direction_value,
                status="active",
                factual_summary=summary_value,
                started_at=started,
            )
            session.add(scenario)
            await session.flush()
            if opening_post is not None:
                scenario.opened_by_post_id = opening_post.id
                opening_post.linked_scenario_id = scenario.id
                opening_post.updated_at = utcnow()
            await session.commit()
            await session.refresh(scenario)
            return _scenario_snapshot(scenario)

    async def link_ai_channel_post_to_scenario(
        self,
        channel_id: int,
        post_id: int,
        scenario_id: int,
    ) -> dict[str, object]:
        channel_id = _validate_database_id(channel_id, field="ID канала")
        post_id = _validate_database_id(post_id, field="ID публикации")
        scenario_id = _validate_database_id(scenario_id, field="ID сценария")
        async with self.sessions() as session:
            post = await session.get(AIChannelPost, post_id)
            scenario = await session.get(AIChannelScenario, scenario_id)
            if (
                post is None
                or scenario is None
                or post.channel_id != channel_id
                or scenario.channel_id != channel_id
                or post.deleted_at is not None
            ):
                raise AIChannelMemoryError(
                    "Публикация или сценарий не принадлежат выбранному каналу"
                )
            if scenario.status != "active":
                raise AIChannelMemoryConflictError("Сценарий уже не активен")
            if post.linked_scenario_id == scenario.id:
                return {
                    "changed": False,
                    "post": _post_snapshot(post),
                    "scenario": _scenario_snapshot(scenario),
                }
            post.linked_scenario_id = scenario.id
            post.updated_at = utcnow()
            if scenario.opened_by_post_id is None:
                scenario.opened_by_post_id = post.id
                scenario.updated_at = utcnow()
            await session.commit()
            await session.refresh(post)
            await session.refresh(scenario)
            return {
                "changed": True,
                "post": _post_snapshot(post),
                "scenario": _scenario_snapshot(scenario),
            }

    async def complete_ai_channel_scenario(
        self,
        channel_id: int,
        scenario_id: int,
        *,
        ended_at: datetime | None = None,
        closed_by_post_id: int | None = None,
    ) -> dict[str, object]:
        channel_id = _validate_database_id(channel_id, field="ID канала")
        scenario_id = _validate_database_id(scenario_id, field="ID сценария")
        ended = normalize_ai_datetime(ended_at or utcnow(), field="ended_at")
        if closed_by_post_id is not None:
            closed_by_post_id = _validate_database_id(
                closed_by_post_id,
                field="ID публикации",
            )
        async with self.sessions() as session:
            scenario = await session.get(AIChannelScenario, scenario_id)
            if scenario is None or scenario.channel_id != channel_id:
                raise AIChannelMemoryError("Сценарий не найден в выбранном канале")
            closing_post = None
            if closed_by_post_id is not None:
                closing_post = await session.get(AIChannelPost, closed_by_post_id)
                if (
                    closing_post is None
                    or closing_post.channel_id != channel_id
                    or closing_post.deleted_at is not None
                ):
                    raise AIChannelMemoryError(
                        "Закрывающая публикация не принадлежит выбранному каналу"
                    )
            if scenario.status == "completed":
                return {
                    "changed": False,
                    "scenario": _scenario_snapshot(scenario),
                }
            if scenario.status != "active":
                raise AIChannelMemoryConflictError(
                    "Завершить можно только активный сценарий"
                )
            if ended < scenario.started_at:
                raise ValueError("ended_at не может быть раньше started_at")
            scenario.status = "completed"
            scenario.ended_at = ended
            scenario.closed_by_post_id = (
                closing_post.id if closing_post is not None else None
            )
            scenario.updated_at = utcnow()
            if closing_post is not None:
                closing_post.linked_scenario_id = scenario.id
                closing_post.updated_at = utcnow()
            await session.commit()
            await session.refresh(scenario)
            return {
                "changed": True,
                "scenario": _scenario_snapshot(scenario),
            }
