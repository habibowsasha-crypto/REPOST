from __future__ import annotations

import math
from collections.abc import Mapping
from datetime import datetime, timedelta, timezone
from typing import Any
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from sqlalchemy import func, select, update
from sqlalchemy.exc import IntegrityError

from .ai_account_profiles import (
    AI_ACCOUNT_PROFILE_HISTORY_LIMIT,
    AI_ACCOUNT_PROFILE_UPDATE_ATTEMPTS,
    AI_ACCOUNT_REPLY_HISTORY_LIMIT,
    PROFILE_REVISION_REASONS,
    AIAccountProfileConflictError,
    AIAccountProfileData,
    AIAccountProfileEligibility,
    AIAccountProfileError,
    AIAccountProfileRevisionSnapshot,
    AIAccountProfileSnapshot,
    AIAccountReplySnapshot,
    build_auto_profile,
    decode_json_list,
    decode_json_object,
    normalize_profile_data,
    profile_data_database_values,
    profile_style_signature,
    serialize_profile_revision_payload,
    validate_database_id,
)
from .ai_comments_models import (
    AIAccountProfile,
    AIAccountProfileRevision,
    AICommentMessage,
    AICommentQuotaEvent,
)
from .models import Account, utcnow


def _profile_data(profile: AIAccountProfile) -> AIAccountProfileData:
    try:
        return normalize_profile_data(
            {
                "name": profile.name,
                "knowledge_level": profile.knowledge_level,
                "role": profile.role,
                "style": decode_json_object(
                    profile.style_json,
                    field="ai_account_profiles.style_json",
                ),
                "allowed_claims": decode_json_list(
                    profile.allowed_claims_json,
                    field="ai_account_profiles.allowed_claims_json",
                ),
                "forbidden_claims": decode_json_list(
                    profile.forbidden_claims_json,
                    field="ai_account_profiles.forbidden_claims_json",
                ),
                "min_length": int(profile.min_length),
                "max_length": int(profile.max_length),
                "emoji_rate": profile.emoji_rate,
                "question_rate": profile.question_rate,
                "reply_rate": profile.reply_rate,
                "disagreement_rate": profile.disagreement_rate,
                "daily_limit": int(profile.daily_limit),
                "cooldown_seconds": int(profile.cooldown_seconds),
            }
        )
    except (TypeError, ValueError) as exc:
        raise AIAccountProfileError(
            "Профиль аккаунта содержит несовместимые данные"
        ) from exc


def _profile_snapshot(
    profile: AIAccountProfile,
    account: Account | None,
) -> AIAccountProfileSnapshot:
    data = _profile_data(profile)
    style = data.style
    favorites = tuple(str(item) for item in style.get("favorite_words", []))
    return AIAccountProfileSnapshot(
        id=int(profile.id),
        account_id=int(profile.account_id) if profile.account_id is not None else None,
        telegram_user_id=(
            int(profile.telegram_user_id)
            if profile.telegram_user_id is not None
            else None
        ),
        account_display_name=account.display_name if account is not None else None,
        account_username=account.username if account is not None else None,
        account_is_active=bool(account.is_active) if account is not None else False,
        account_status=account.status if account is not None else None,
        account_has_session=bool(account.session_encrypted.strip()) if account is not None else False,
        name=data.name,
        knowledge_level=data.knowledge_level,
        role=data.role,
        preset_key=(
            str(style["preset_key"]) if style.get("preset_key") is not None else None
        ),
        tone=str(style["tone"]) if style.get("tone") is not None else None,
        vocabulary=(
            str(style["vocabulary"])
            if style.get("vocabulary") is not None
            else None
        ),
        uppercase_mode=str(style["uppercase_mode"]),
        punctuation_mode=str(style["punctuation_mode"]),
        mistake_level=str(style["mistake_level"]),
        favorite_words=favorites,
        sentence_pattern=(
            str(style["sentence_pattern"])
            if style.get("sentence_pattern") is not None
            else None
        ),
        persona_key=(
            str(style["persona_key"])
            if style.get("persona_key") is not None
            else None
        ),
        generation=int(style.get("generation", 0)),
        allowed_claims=data.allowed_claims,
        forbidden_claims=data.forbidden_claims,
        min_length=data.min_length,
        max_length=data.max_length,
        emoji_rate=data.emoji_rate,
        question_rate=data.question_rate,
        reply_rate=data.reply_rate,
        disagreement_rate=data.disagreement_rate,
        daily_limit=data.daily_limit,
        reply_bonus_slots=data.reply_bonus_slots,
        cooldown_seconds=data.cooldown_seconds,
        enabled=bool(profile.enabled),
        retired=bool(style.get("retired", False)),
        profile_version=int(profile.profile_version),
        created_at=profile.created_at,
        updated_at=profile.updated_at,
    )


def _revision_snapshot(
    revision: AIAccountProfileRevision,
) -> AIAccountProfileRevisionSnapshot:
    return AIAccountProfileRevisionSnapshot(
        id=int(revision.id),
        profile_id=int(revision.profile_id),
        telegram_user_id=(
            int(revision.telegram_user_id)
            if revision.telegram_user_id is not None
            else None
        ),
        profile_version=int(revision.profile_version),
        change_reason=revision.change_reason,
        snapshot_json=revision.snapshot_json,
        snapshot_hash=revision.snapshot_hash,
        changed_by=(
            int(revision.changed_by) if revision.changed_by is not None else None
        ),
        created_at=revision.created_at,
    )


def _reply_snapshot(message: AICommentMessage) -> AIAccountReplySnapshot:
    return AIAccountReplySnapshot(
        id=int(message.id),
        text=message.text,
        status=message.status,
        telegram_message_id=(
            int(message.telegram_message_id)
            if message.telegram_message_id is not None
            else None
        ),
        reply_to_telegram_message_id=(
            int(message.reply_to_telegram_message_id)
            if message.reply_to_telegram_message_id is not None
            else None
        ),
        published_at=message.published_at,
        created_at=message.created_at,
    )


def _profile_revision(
    profile: AIAccountProfile,
    *,
    reason: str,
    changed_by: int | None,
) -> AIAccountProfileRevision:
    if reason not in PROFILE_REVISION_REASONS:
        raise ValueError("Некорректная причина изменения профиля")
    data = _profile_data(profile)
    snapshot_json, snapshot_hash = serialize_profile_revision_payload(
        profile_id=int(profile.id),
        account_id=(int(profile.account_id) if profile.account_id is not None else None),
        telegram_user_id=(
            int(profile.telegram_user_id)
            if profile.telegram_user_id is not None
            else None
        ),
        data=data,
        enabled=bool(profile.enabled),
        profile_version=int(profile.profile_version),
    )
    return AIAccountProfileRevision(
        profile_id=profile.id,
        telegram_user_id=profile.telegram_user_id,
        profile_version=profile.profile_version,
        change_reason=reason,
        snapshot_json=snapshot_json,
        snapshot_hash=snapshot_hash,
        changed_by=changed_by,
        created_at=profile.updated_at,
    )


async def _occupied_signatures(
    session: Any,
    *,
    exclude_profile_id: int | None = None,
) -> list[tuple[str, ...]]:
    statement = select(AIAccountProfile)
    if exclude_profile_id is not None:
        statement = statement.where(AIAccountProfile.id != exclude_profile_id)
    profiles = list((await session.scalars(statement)).all())
    result: list[tuple[str, ...]] = []
    for profile in profiles:
        data = _profile_data(profile)
        result.append(profile_style_signature(data.style, role=data.role))
    return result


def _merged_profile_data(
    current: AIAccountProfileData,
    patch: Mapping[str, object],
) -> AIAccountProfileData:
    allowed_fields = {
        "name",
        "knowledge_level",
        "role",
        "style",
        "allowed_claims",
        "forbidden_claims",
        "min_length",
        "max_length",
        "emoji_rate",
        "question_rate",
        "reply_rate",
        "disagreement_rate",
        "daily_limit",
        "reply_bonus_slots",
        "cooldown_seconds",
    }
    unknown = sorted(set(patch) - allowed_fields)
    if unknown:
        raise ValueError(f"Некорректные поля профиля: {unknown}")
    values: dict[str, object] = {
        "name": current.name,
        "knowledge_level": current.knowledge_level,
        "role": current.role,
        "style": dict(current.style),
        "allowed_claims": current.allowed_claims,
        "forbidden_claims": current.forbidden_claims,
        "min_length": current.min_length,
        "max_length": current.max_length,
        "emoji_rate": current.emoji_rate,
        "question_rate": current.question_rate,
        "reply_rate": current.reply_rate,
        "disagreement_rate": current.disagreement_rate,
        "daily_limit": current.daily_limit,
        "reply_bonus_slots": current.reply_bonus_slots,
        "cooldown_seconds": current.cooldown_seconds,
    }
    values.update({key: value for key, value in patch.items() if key != "style"})
    if "style" in patch:
        style_patch = patch["style"]
        if not isinstance(style_patch, Mapping):
            raise TypeError("Стиль профиля должен быть объектом")
        merged_style = dict(current.style)
        for key, value in style_patch.items():
            if value is None:
                merged_style.pop(str(key), None)
            else:
                merged_style[str(key)] = value
        values["style"] = merged_style
    return normalize_profile_data(values)


def _validate_expected_version(value: int) -> int:
    if not isinstance(value, int) or isinstance(value, bool) or value < 1:
        raise ValueError("Некорректная версия профиля")
    return value


def _day_window_utc(current: datetime, timezone_name: str) -> tuple[datetime, datetime, str]:
    try:
        zone = ZoneInfo(timezone_name)
    except ZoneInfoNotFoundError as exc:
        raise ValueError("Неизвестный часовой пояс AI Comments") from exc
    if current.tzinfo is None:
        current_utc = current.replace(tzinfo=timezone.utc)
    else:
        current_utc = current.astimezone(timezone.utc)
    local = current_utc.astimezone(zone)
    local_start = local.replace(hour=0, minute=0, second=0, microsecond=0)
    local_end = local_start + timedelta(days=1)
    return (
        local_start.astimezone(timezone.utc).replace(tzinfo=None),
        local_end.astimezone(timezone.utc).replace(tzinfo=None),
        local_start.date().isoformat(),
    )


class AIAccountProfilesRepositoryMixin:
    async def ensure_ai_account_profile(
        self,
        account_id: int,
    ) -> AIAccountProfileSnapshot:
        """Create or reattach one stable persona without changing core auth state."""

        account_id = validate_database_id(account_id, field="ID аккаунта")
        for _attempt in range(AI_ACCOUNT_PROFILE_UPDATE_ATTEMPTS):
            async with self.sessions() as session:
                account = await session.get(Account, account_id)
                if account is None:
                    raise AIAccountProfileError("Аккаунт не найден")
                profile_by_identity = await session.scalar(
                    select(AIAccountProfile).where(
                        AIAccountProfile.telegram_user_id == account.telegram_user_id
                    )
                )
                profile_by_account = await session.scalar(
                    select(AIAccountProfile).where(
                        AIAccountProfile.account_id == account.id
                    )
                )
                if (
                    profile_by_identity is not None
                    and profile_by_account is not None
                    and profile_by_identity.id != profile_by_account.id
                ):
                    raise AIAccountProfileError(
                        "Account и Telegram ID связаны с разными AI-профилями"
                    )
                profile = profile_by_identity or profile_by_account
                if profile is not None:
                    if (
                        profile.telegram_user_id is not None
                        and int(profile.telegram_user_id) != int(account.telegram_user_id)
                    ):
                        raise AIAccountProfileError(
                            "AI-профиль принадлежит другой Telegram-личности"
                        )
                    if profile.account_id == account.id and profile.telegram_user_id is not None:
                        return _profile_snapshot(profile, account)
                    current_version = int(profile.profile_version)
                    result = await session.execute(
                        update(AIAccountProfile)
                        .where(
                            AIAccountProfile.id == profile.id,
                            AIAccountProfile.profile_version == current_version,
                        )
                        .values(
                            account_id=account.id,
                            telegram_user_id=account.telegram_user_id,
                            profile_version=current_version + 1,
                            updated_at=utcnow(),
                        )
                    )
                    if result.rowcount != 1:
                        await session.rollback()
                        continue
                    await session.refresh(profile)
                    session.add(
                        _profile_revision(
                            profile,
                            reason="reattached",
                            changed_by=None,
                        )
                    )
                    try:
                        await session.commit()
                    except IntegrityError:
                        await session.rollback()
                        continue
                    return _profile_snapshot(profile, account)

                occupied = await _occupied_signatures(session)
                data = build_auto_profile(
                    int(account.telegram_user_id),
                    occupied_signatures=occupied,
                )
                profile = AIAccountProfile(
                    account_id=account.id,
                    telegram_user_id=account.telegram_user_id,
                    **profile_data_database_values(data),
                    # Profiles exist immediately but remain disabled until the
                    # administrator explicitly admits them to the future pilot.
                    enabled=False,
                    profile_version=1,
                    created_at=utcnow(),
                    updated_at=utcnow(),
                )
                session.add(profile)
                try:
                    await session.flush()
                    session.add(
                        _profile_revision(
                            profile,
                            reason="auto_created",
                            changed_by=None,
                        )
                    )
                    await session.commit()
                except IntegrityError:
                    await session.rollback()
                    continue
                await session.refresh(profile)
                return _profile_snapshot(profile, account)
        raise AIAccountProfileConflictError(
            "Профиль одновременно создаётся или восстанавливается другим процессом"
        )

    async def sync_ai_account_profiles(self) -> dict[str, int]:
        """Idempotently ensure a profile for every currently stored Account."""

        async with self.sessions() as session:
            account_ids = [
                int(value)
                for value in (
                    await session.scalars(select(Account.id).order_by(Account.id))
                ).all()
            ]
            before = int(
                await session.scalar(select(func.count(AIAccountProfile.id))) or 0
            )
            attached_before = int(
                await session.scalar(
                    select(func.count(AIAccountProfile.id)).where(
                        AIAccountProfile.account_id.is_not(None)
                    )
                )
                or 0
            )
        for account_id in account_ids:
            await self.ensure_ai_account_profile(account_id)
        async with self.sessions() as session:
            after = int(
                await session.scalar(select(func.count(AIAccountProfile.id))) or 0
            )
            attached_after = int(
                await session.scalar(
                    select(func.count(AIAccountProfile.id)).where(
                        AIAccountProfile.account_id.is_not(None)
                    )
                )
                or 0
            )
        return {
            "accounts": len(account_ids),
            "created": max(0, after - before),
            "reattached": max(0, attached_after - attached_before),
            "profiles": after,
        }

    async def list_ai_account_profiles(self) -> tuple[AIAccountProfileSnapshot, ...]:
        async with self.sessions() as session:
            rows = (
                await session.execute(
                    select(AIAccountProfile, Account)
                    .outerjoin(Account, Account.id == AIAccountProfile.account_id)
                    .order_by(
                        AIAccountProfile.account_id.is_(None),
                        AIAccountProfile.id,
                    )
                )
            ).all()
            return tuple(_profile_snapshot(profile, account) for profile, account in rows)

    async def get_ai_account_profile(
        self,
        profile_id: int,
    ) -> AIAccountProfileSnapshot | None:
        profile_id = validate_database_id(profile_id, field="ID профиля")
        async with self.sessions() as session:
            row = (
                await session.execute(
                    select(AIAccountProfile, Account)
                    .outerjoin(Account, Account.id == AIAccountProfile.account_id)
                    .where(AIAccountProfile.id == profile_id)
                )
            ).one_or_none()
            if row is None:
                return None
            profile, account = row
            return _profile_snapshot(profile, account)

    async def update_ai_account_profile(
        self,
        profile_id: int,
        patch: Mapping[str, object],
        *,
        expected_version: int,
        updated_by: int,
    ) -> dict[str, object]:
        profile_id = validate_database_id(profile_id, field="ID профиля")
        validate_database_id(updated_by, field="ID администратора")
        expected_version = _validate_expected_version(expected_version)
        async with self.sessions() as session:
            row = (
                await session.execute(
                    select(AIAccountProfile, Account)
                    .outerjoin(Account, Account.id == AIAccountProfile.account_id)
                    .where(AIAccountProfile.id == profile_id)
                )
            ).one_or_none()
            if row is None:
                raise AIAccountProfileError("Профиль не найден")
            profile, account = row
            current_version = int(profile.profile_version)
            if current_version != expected_version:
                raise AIAccountProfileConflictError(
                    "Профиль уже изменён. Обновите экран и повторите"
                )
            current = _profile_data(profile)
            changed_data = _merged_profile_data(current, patch)
            values = profile_data_database_values(changed_data)
            if all(getattr(profile, key) == value for key, value in values.items()):
                return {
                    "changed": False,
                    "version": current_version,
                    "profile": _profile_snapshot(profile, account),
                }
            result = await session.execute(
                update(AIAccountProfile)
                .where(
                    AIAccountProfile.id == profile.id,
                    AIAccountProfile.profile_version == current_version,
                )
                .values(
                    **values,
                    profile_version=current_version + 1,
                    updated_at=utcnow(),
                )
            )
            if result.rowcount != 1:
                await session.rollback()
                raise AIAccountProfileConflictError(
                    "Профиль уже изменён. Обновите экран и повторите"
                )
            await session.refresh(profile)
            session.add(
                _profile_revision(profile, reason="updated", changed_by=updated_by)
            )
            await session.commit()
            return {
                "changed": True,
                "version": current_version + 1,
                "profile": _profile_snapshot(profile, account),
            }

    async def set_ai_account_profile_enabled(
        self,
        profile_id: int,
        enabled: bool,
        *,
        expected_version: int,
        updated_by: int,
    ) -> dict[str, object]:
        if type(enabled) is not bool:
            raise TypeError("enabled должен быть boolean")
        profile_id = validate_database_id(profile_id, field="ID профиля")
        expected_version = _validate_expected_version(expected_version)
        validate_database_id(updated_by, field="ID администратора")
        async with self.sessions() as session:
            row = (
                await session.execute(
                    select(AIAccountProfile, Account)
                    .outerjoin(Account, Account.id == AIAccountProfile.account_id)
                    .where(AIAccountProfile.id == profile_id)
                )
            ).one_or_none()
            if row is None:
                raise AIAccountProfileError("Профиль не найден")
            profile, account = row
            current_version = int(profile.profile_version)
            if current_version != expected_version:
                raise AIAccountProfileConflictError(
                    "Профиль уже изменён. Обновите экран и повторите"
                )
            data = _profile_data(profile)
            style = dict(data.style)
            retired = bool(style.get("retired", False))
            target_retired = False if enabled else retired
            if bool(profile.enabled) is enabled and retired is target_retired:
                return {
                    "changed": False,
                    "version": current_version,
                    "profile": _profile_snapshot(profile, account),
                }
            style["retired"] = target_retired
            values: dict[str, object] = {
                "enabled": enabled,
                "style_json": profile_data_database_values(
                    _merged_profile_data(data, {"style": style})
                )["style_json"],
                "profile_version": current_version + 1,
                "updated_at": utcnow(),
            }
            result = await session.execute(
                update(AIAccountProfile)
                .where(
                    AIAccountProfile.id == profile.id,
                    AIAccountProfile.profile_version == current_version,
                )
                .values(**values)
            )
            if result.rowcount != 1:
                await session.rollback()
                raise AIAccountProfileConflictError(
                    "Профиль уже изменён. Обновите экран и повторите"
                )
            await session.refresh(profile)
            reason = "restored" if enabled and retired else ("enabled" if enabled else "disabled")
            session.add(_profile_revision(profile, reason=reason, changed_by=updated_by))
            await session.commit()
            return {
                "changed": True,
                "version": current_version + 1,
                "profile": _profile_snapshot(profile, account),
            }

    async def regenerate_ai_account_profile(
        self,
        profile_id: int,
        *,
        expected_version: int,
        updated_by: int,
    ) -> dict[str, object]:
        profile_id = validate_database_id(profile_id, field="ID профиля")
        expected_version = _validate_expected_version(expected_version)
        validate_database_id(updated_by, field="ID администратора")
        async with self.sessions() as session:
            row = (
                await session.execute(
                    select(AIAccountProfile, Account)
                    .outerjoin(Account, Account.id == AIAccountProfile.account_id)
                    .where(AIAccountProfile.id == profile_id)
                )
            ).one_or_none()
            if row is None:
                raise AIAccountProfileError("Профиль не найден")
            profile, account = row
            current_version = int(profile.profile_version)
            if current_version != expected_version:
                raise AIAccountProfileConflictError(
                    "Профиль уже изменён. Обновите экран и повторите"
                )
            if profile.telegram_user_id is None:
                raise AIAccountProfileError(
                    "У профиля нет стабильного Telegram ID; перегенерация остановлена"
                )
            current_data = _profile_data(profile)
            current_generation = int(current_data.style.get("generation", 0))
            occupied = await _occupied_signatures(session)
            generated = build_auto_profile(
                int(profile.telegram_user_id),
                occupied_signatures=occupied,
                generation=current_generation + 1,
            )
            if bool(current_data.style.get("retired", False)):
                generated = _merged_profile_data(
                    generated,
                    {"style": {"retired": True}},
                )
            values = profile_data_database_values(generated)
            result = await session.execute(
                update(AIAccountProfile)
                .where(
                    AIAccountProfile.id == profile.id,
                    AIAccountProfile.profile_version == current_version,
                )
                .values(
                    **values,
                    profile_version=current_version + 1,
                    updated_at=utcnow(),
                )
            )
            if result.rowcount != 1:
                await session.rollback()
                raise AIAccountProfileConflictError(
                    "Профиль уже изменён. Обновите экран и повторите"
                )
            await session.refresh(profile)
            session.add(
                _profile_revision(
                    profile,
                    reason="regenerated",
                    changed_by=updated_by,
                )
            )
            await session.commit()
            return {
                "changed": True,
                "version": current_version + 1,
                "profile": _profile_snapshot(profile, account),
            }

    async def retire_ai_account_profile(
        self,
        profile_id: int,
        *,
        expected_version: int,
        updated_by: int,
    ) -> dict[str, object]:
        """Soft-delete a persona while preserving identity and comment audit."""

        profile_id = validate_database_id(profile_id, field="ID профиля")
        expected_version = _validate_expected_version(expected_version)
        validate_database_id(updated_by, field="ID администратора")
        async with self.sessions() as session:
            row = (
                await session.execute(
                    select(AIAccountProfile, Account)
                    .outerjoin(Account, Account.id == AIAccountProfile.account_id)
                    .where(AIAccountProfile.id == profile_id)
                )
            ).one_or_none()
            if row is None:
                raise AIAccountProfileError("Профиль не найден")
            profile, account = row
            current_version = int(profile.profile_version)
            if current_version != expected_version:
                raise AIAccountProfileConflictError(
                    "Профиль уже изменён. Обновите экран и повторите"
                )
            data = _profile_data(profile)
            if bool(data.style.get("retired", False)) and not profile.enabled:
                return {
                    "changed": False,
                    "version": current_version,
                    "profile": _profile_snapshot(profile, account),
                }
            style = dict(data.style)
            style["retired"] = True
            changed = _merged_profile_data(data, {"style": style})
            result = await session.execute(
                update(AIAccountProfile)
                .where(
                    AIAccountProfile.id == profile.id,
                    AIAccountProfile.profile_version == current_version,
                )
                .values(
                    style_json=profile_data_database_values(changed)["style_json"],
                    enabled=False,
                    profile_version=current_version + 1,
                    updated_at=utcnow(),
                )
            )
            if result.rowcount != 1:
                await session.rollback()
                raise AIAccountProfileConflictError(
                    "Профиль уже изменён. Обновите экран и повторите"
                )
            await session.refresh(profile)
            session.add(
                _profile_revision(profile, reason="retired", changed_by=updated_by)
            )
            await session.commit()
            return {
                "changed": True,
                "version": current_version + 1,
                "profile": _profile_snapshot(profile, account),
            }

    async def delete_ai_account_profile(
        self,
        profile_id: int,
        *,
        expected_version: int,
        updated_by: int,
    ) -> dict[str, object]:
        """Compatibility CRUD name: deletion is intentionally a reversible archive."""

        return await self.retire_ai_account_profile(
            profile_id,
            expected_version=expected_version,
            updated_by=updated_by,
        )

    async def list_ai_account_profile_revisions(
        self,
        profile_id: int,
        *,
        limit: int = AI_ACCOUNT_PROFILE_HISTORY_LIMIT,
    ) -> tuple[AIAccountProfileRevisionSnapshot, ...]:
        profile_id = validate_database_id(profile_id, field="ID профиля")
        if not isinstance(limit, int) or isinstance(limit, bool) or not 1 <= limit <= 100:
            raise ValueError("Лимит ревизий должен быть от 1 до 100")
        async with self.sessions() as session:
            revisions = list(
                (
                    await session.scalars(
                        select(AIAccountProfileRevision)
                        .where(AIAccountProfileRevision.profile_id == profile_id)
                        .order_by(
                            AIAccountProfileRevision.profile_version.desc(),
                            AIAccountProfileRevision.id.desc(),
                        )
                        .limit(limit)
                    )
                ).all()
            )
            return tuple(_revision_snapshot(item) for item in revisions)

    async def list_ai_account_reply_history(
        self,
        profile_id: int,
        *,
        limit: int = AI_ACCOUNT_REPLY_HISTORY_LIMIT,
    ) -> tuple[AIAccountReplySnapshot, ...]:
        profile_id = validate_database_id(profile_id, field="ID профиля")
        if not isinstance(limit, int) or isinstance(limit, bool) or not 1 <= limit <= 100:
            raise ValueError("Лимит реплик должен быть от 1 до 100")
        async with self.sessions() as session:
            rows = list(
                (
                    await session.scalars(
                        select(AICommentMessage)
                        .where(
                            AICommentMessage.account_profile_id == profile_id,
                            AICommentMessage.role == "published",
                            AICommentMessage.published_at.is_not(None),
                        )
                        .order_by(
                            AICommentMessage.published_at.desc(),
                            AICommentMessage.id.desc(),
                        )
                        .limit(limit)
                    )
                ).all()
            )
            return tuple(_reply_snapshot(item) for item in rows)

    async def grant_ai_reply_bonus(
        self,
        profile_id: int,
        *,
        source_telegram_message_id: int,
        thread_id: int | None = None,
        now: datetime | None = None,
        timezone_name: str = "UTC",
    ) -> AIAccountProfileEligibility:
        """Unlock the profile's bounded daily reply pool once per local day."""

        profile_id = validate_database_id(profile_id, field="ID профиля")
        source_telegram_message_id = validate_database_id(
            source_telegram_message_id, field="Telegram ID входящего ответа"
        )
        if thread_id is not None:
            thread_id = validate_database_id(thread_id, field="ID диалога")
        current = now or utcnow()
        _day_start, _day_end, day_key = _day_window_utc(current, timezone_name)
        async with self.sessions() as session:
            profile = await session.get(AIAccountProfile, profile_id)
            if profile is None:
                raise AIAccountProfileError("Профиль не найден")
            data = _profile_data(profile)
            if data.reply_bonus_slots <= 0:
                return await self.get_ai_account_profile_eligibility(
                    profile_id, now=current, timezone_name=timezone_name, reply_context=True
                )
            event = AICommentQuotaEvent(
                account_profile_id=profile_id,
                thread_id=thread_id,
                day_key=day_key,
                event_type="reply_bonus_grant",
                slots=data.reply_bonus_slots,
                source_telegram_message_id=source_telegram_message_id,
                idempotency_key=f"reply-grant:{profile_id}:{day_key}",
                created_at=current,
            )
            session.add(event)
            try:
                await session.commit()
            except IntegrityError:
                await session.rollback()
        return await self.get_ai_account_profile_eligibility(
            profile_id, now=current, timezone_name=timezone_name, reply_context=True
        )

    async def consume_ai_reply_bonus_slot(
        self,
        profile_id: int,
        *,
        source_telegram_message_id: int,
        thread_id: int | None = None,
        now: datetime | None = None,
        timezone_name: str = "UTC",
    ) -> bool:
        """Consume one bonus atomically after the normal daily pool is exhausted.

        PostgreSQL serializes concurrent consumers on the profile row. The unique
        idempotency key also makes a repeated publication acknowledgement harmless.
        """

        profile_id = validate_database_id(profile_id, field="ID профиля")
        source_telegram_message_id = validate_database_id(
            source_telegram_message_id, field="Telegram ID ответа"
        )
        if thread_id is not None:
            thread_id = validate_database_id(thread_id, field="ID диалога")
        current = now or utcnow()
        day_start, day_end, day_key = _day_window_utc(current, timezone_name)
        async with self.sessions() as session:
            profile = await session.scalar(
                select(AIAccountProfile)
                .where(AIAccountProfile.id == profile_id)
                .with_for_update()
            )
            if profile is None:
                raise AIAccountProfileError("Профиль не найден")
            data = _profile_data(profile)
            comments_today = int(
                await session.scalar(
                    select(func.count(AICommentMessage.id)).where(
                        AICommentMessage.account_profile_id == profile_id,
                        AICommentMessage.role == "published",
                        AICommentMessage.published_at >= day_start,
                        AICommentMessage.published_at < day_end,
                    )
                )
                or 0
            )
            if comments_today < data.daily_limit:
                return False
            grants = int(
                await session.scalar(
                    select(func.coalesce(func.sum(AICommentQuotaEvent.slots), 0)).where(
                        AICommentQuotaEvent.account_profile_id == profile_id,
                        AICommentQuotaEvent.day_key == day_key,
                        AICommentQuotaEvent.event_type == "reply_bonus_grant",
                    )
                )
                or 0
            )
            uses = int(
                await session.scalar(
                    select(func.coalesce(func.sum(AICommentQuotaEvent.slots), 0)).where(
                        AICommentQuotaEvent.account_profile_id == profile_id,
                        AICommentQuotaEvent.day_key == day_key,
                        AICommentQuotaEvent.event_type == "reply_bonus_use",
                    )
                )
                or 0
            )
            available = min(max(0, grants), data.reply_bonus_slots) - max(0, uses)
            if available <= 0:
                raise AIAccountProfileError("Бонусные слоты для ответов исчерпаны")
            event = AICommentQuotaEvent(
                account_profile_id=profile_id,
                thread_id=thread_id,
                day_key=day_key,
                event_type="reply_bonus_use",
                slots=1,
                source_telegram_message_id=source_telegram_message_id,
                idempotency_key=(
                    f"reply-use:{profile_id}:{thread_id or 0}:"
                    f"{source_telegram_message_id}"
                ),
                created_at=current,
            )
            session.add(event)
            try:
                await session.commit()
            except IntegrityError:
                await session.rollback()
                return False
        return True

    async def get_ai_account_profile_eligibility(
        self,
        profile_id: int,
        *,
        now: datetime | None = None,
        timezone_name: str = "UTC",
        reply_context: bool = False,
    ) -> AIAccountProfileEligibility:
        profile_id = validate_database_id(profile_id, field="ID профиля")
        current = now or utcnow()
        day_start, day_end, day_key = _day_window_utc(current, timezone_name)
        async with self.sessions() as session:
            row = (
                await session.execute(
                    select(AIAccountProfile, Account)
                    .outerjoin(Account, Account.id == AIAccountProfile.account_id)
                    .where(AIAccountProfile.id == profile_id)
                )
            ).one_or_none()
            if row is None:
                raise AIAccountProfileError("Профиль не найден")
            profile, account = row
            data = _profile_data(profile)
            comments_today = int(
                await session.scalar(
                    select(func.count(AICommentMessage.id)).where(
                        AICommentMessage.account_profile_id == profile_id,
                        AICommentMessage.role == "published",
                        AICommentMessage.published_at >= day_start,
                        AICommentMessage.published_at < day_end,
                    )
                )
                or 0
            )
            last_published_at = await session.scalar(
                select(func.max(AICommentMessage.published_at)).where(
                    AICommentMessage.account_profile_id == profile_id,
                    AICommentMessage.role == "published",
                    AICommentMessage.published_at.is_not(None),
                    AICommentMessage.published_at <= current,
                )
            )
            grants = int(
                await session.scalar(
                    select(func.coalesce(func.sum(AICommentQuotaEvent.slots), 0)).where(
                        AICommentQuotaEvent.account_profile_id == profile_id,
                        AICommentQuotaEvent.day_key == day_key,
                        AICommentQuotaEvent.event_type == "reply_bonus_grant",
                    )
                )
                or 0
            )
            uses = int(
                await session.scalar(
                    select(func.coalesce(func.sum(AICommentQuotaEvent.slots), 0)).where(
                        AICommentQuotaEvent.account_profile_id == profile_id,
                        AICommentQuotaEvent.day_key == day_key,
                        AICommentQuotaEvent.event_type == "reply_bonus_use",
                    )
                )
                or 0
            )

        bonus_granted = min(max(0, grants), data.reply_bonus_slots)
        bonus_used = min(max(0, uses), bonus_granted)
        bonus_available = max(0, bonus_granted - bonus_used)
        reason = "Готов"
        allowed = True
        cooldown_remaining = 0
        if bool(data.style.get("retired", False)):
            allowed, reason = False, "Профиль архивирован"
        elif not profile.enabled:
            allowed, reason = False, "Профиль выключен"
        elif account is None:
            allowed, reason = False, "Аккаунт удалён или ещё не привязан"
        elif int(account.telegram_user_id) != int(profile.telegram_user_id or 0):
            allowed, reason = False, "Telegram-личность аккаунта не совпадает"
        elif not account.is_active:
            allowed, reason = False, "Аккаунт выключен"
        elif account.status != "ready":
            allowed, reason = False, "Аккаунт не имеет статуса ready"
        elif not account.session_encrypted.strip():
            allowed, reason = False, "Готовая зашифрованная сессия отсутствует"
        elif account.flood_until is not None and account.flood_until > current:
            allowed, reason = False, "Аккаунт находится в FloodWait"
        elif data.daily_limit <= 0 and not (reply_context and bonus_available > 0):
            allowed, reason = False, "Дневной лимит равен нулю"
        elif comments_today >= data.daily_limit and not (reply_context and bonus_available > 0):
            allowed, reason = False, "Дневной лимит исчерпан"
        elif last_published_at is not None:
            cooldown_until = last_published_at + timedelta(seconds=data.cooldown_seconds)
            if cooldown_until > current:
                cooldown_remaining = max(1, math.ceil((cooldown_until - current).total_seconds()))
                allowed, reason = False, "Cooldown ещё не завершён"

        return AIAccountProfileEligibility(
            profile_id=profile_id,
            allowed=allowed,
            reason=reason,
            comments_today=comments_today,
            daily_limit=data.daily_limit,
            last_published_at=last_published_at,
            cooldown_remaining_seconds=cooldown_remaining,
            reply_bonus_granted=bonus_granted,
            reply_bonus_used=bonus_used,
            reply_bonus_available=bonus_available,
            day_key=day_key,
            day_timezone=timezone_name,
        )

