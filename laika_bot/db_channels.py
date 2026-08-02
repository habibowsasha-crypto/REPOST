from __future__ import annotations

from .db_shared import *  # noqa: F403


class ChannelDatabaseMixin:
    async def add_or_update_channel(
        self,
        *,
        telegram_channel_id: int,
        kind: str,
        title: str,
        username: str | None,
        link: str,
        invite_hash: str | None,
        last_seen_message_id: int,
    ) -> tuple[Channel, bool]:
        if kind not in {"channel", "group"}:
            raise ValueError("Недопустимый тип Telegram-цели")
        async with self.sessions() as session:
            channel = await session.scalar(
                select(Channel).where(Channel.telegram_channel_id == telegram_channel_id)
            )
            created = channel is None
            if channel is None:
                channel = Channel(
                    telegram_channel_id=telegram_channel_id,
                    kind=kind,
                    title=title,
                    username=username,
                    link=link,
                    invite_hash=invite_hash,
                    last_seen_message_id=last_seen_message_id,
                )
                session.add(channel)
            else:
                channel.kind = kind
                channel.title = title
                channel.username = username
                channel.link = link
                channel.invite_hash = invite_hash
                channel.is_active = True
                channel.last_error = None
                channel.updated_at = utcnow()
            await session.commit()
            await session.refresh(channel)
            return channel, created

    async def list_channels(self, *, active_only: bool = False, kind: str | None = None) -> list[Channel]:
        async with self.sessions() as session:
            stmt = select(Channel).order_by(Channel.id)
            if active_only:
                stmt = stmt.where(Channel.is_active.is_(True))
            if kind is not None:
                if kind not in {"channel", "group"}:
                    raise ValueError("Недопустимый фильтр типа")
                stmt = stmt.where(Channel.kind == kind)
            return list((await session.scalars(stmt)).all())

    async def get_channel(self, channel_id: int) -> Channel | None:
        async with self.sessions() as session:
            return await session.get(Channel, channel_id)

    async def set_channel_flag(self, channel_id: int, field: str, value: bool) -> None:
        if field not in {"new_posts_enabled", "old_posts_enabled", "is_active"}:
            raise ValueError("Недопустимое поле канала")
        async with self.sessions() as session:
            await session.execute(
                update(Channel).where(Channel.id == channel_id).values({field: value, "updated_at": utcnow()})
            )
            await session.commit()

    async def copy_channel_settings(
        self, source_channel_id: int, target_channel_id: int
    ) -> Channel:
        """Copy reusable channel behaviour without identity, membership or history.

        A temporary promotion deadline is intentionally not copied: transferring an
        absolute date could silently expire or extend another channel's campaign.
        """

        if source_channel_id == target_channel_id:
            raise ValueError("Нельзя копировать настройки канала в него самого")
        now = utcnow()
        async with self.sessions() as session:
            locked_channels = list(
                (
                    await session.scalars(
                        select(Channel)
                        .where(Channel.id.in_([source_channel_id, target_channel_id]))
                        .order_by(Channel.id)
                        .with_for_update()
                    )
                ).all()
            )
            by_id = {int(item.id): item for item in locked_channels}
            source = by_id.get(source_channel_id)
            target = by_id.get(target_channel_id)
            if source is None or target is None:
                raise ValueError("Исходный или целевой канал не найден")
            if source.kind != "channel" or target.kind != "channel":
                raise ValueError("Копирование поддерживается только между каналами")

            if source.old_posts_depth < 1:
                raise ValueError("Исходный канал содержит некорректную глубину старых постов")
            if (
                source.max_reactions_per_post is not None
                and source.max_reactions_per_post < 1
            ):
                raise ValueError("Исходный канал содержит некорректный лимит реакций")
            if (
                source.reaction_window_min_seconds < 0
                or source.reaction_window_max_seconds
                < source.reaction_window_min_seconds
                or source.reaction_window_max_seconds > 7 * 24 * 60 * 60
            ):
                raise ValueError("Исходный канал содержит некорректный период реакций")
            self._validate_post_type_percent(source.image_post_reaction_percent)
            self._validate_post_type_percent(source.no_image_post_reaction_percent)
            if (
                source.reactions_json is not None
                and self._decode_reaction_weights(source.reactions_json) is None
            ):
                raise ValueError("Исходный канал содержит некорректный набор реакций")

            for field in (
                "new_posts_enabled",
                "old_posts_enabled",
                "old_posts_depth",
                "reactions_json",
                "max_reactions_per_post",
                "reaction_window_min_seconds",
                "reaction_window_max_seconds",
                "image_post_reaction_percent",
                "no_image_post_reaction_percent",
            ):
                setattr(target, field, getattr(source, field))
            target.updated_at = now

            source_profile = await session.scalar(
                select(AppSetting.value).where(
                    AppSetting.key == channel_profile_setting_key(source.id)
                )
            )
            target_profile_key = channel_profile_setting_key(target.id)
            if source_profile is None:
                await session.execute(
                    delete(AppSetting).where(AppSetting.key == target_profile_key)
                )
            else:
                await self._upsert_setting_in_session(
                    session, target_profile_key, str(source_profile), now=now
                )
            await session.commit()
            await session.refresh(target)
            return target

    async def set_channel_depth(self, channel_id: int, depth: int) -> None:
        async with self.sessions() as session:
            await session.execute(
                update(Channel).where(Channel.id == channel_id).values(old_posts_depth=depth, updated_at=utcnow())
            )
            await session.commit()

    async def set_channel_reaction_limit(self, channel_id: int, limit: int | None) -> None:
        if limit is not None and limit < 1:
            raise ValueError("Лимит должен быть положительным")
        async with self.sessions() as session:
            await session.execute(
                update(Channel)
                .where(Channel.id == channel_id)
                .values(max_reactions_per_post=limit, updated_at=utcnow())
            )
            await session.commit()

    async def apply_channel_profile(
        self, channel_id: int, profile: ResolvedChannelProfile
    ) -> Channel | None:
        validate_resolved_channel_profile(profile)
        if (
            profile.max_reactions_per_post is not None
            and profile.max_reactions_per_post < 1
        ):
            raise ValueError("Лимит профиля должен быть положительным")
        self._validate_post_type_percent(profile.image_post_reaction_percent)
        self._validate_post_type_percent(profile.no_image_post_reaction_percent)
        if (
            profile.reaction_window_min_seconds < 0
            or profile.reaction_window_max_seconds
            < profile.reaction_window_min_seconds
            or profile.reaction_window_max_seconds > 7 * 24 * 60 * 60
        ):
            raise ValueError("Некорректный период профиля")

        now = utcnow()
        setting_key = channel_profile_setting_key(channel_id)
        async with self.sessions() as session:
            channel = await session.scalar(
                select(Channel)
                .where(Channel.id == channel_id)
                .with_for_update()
            )
            if channel is None:
                return None
            if channel.kind != "channel":
                raise ValueError("Профили доступны только обычным каналам")
            channel.max_reactions_per_post = profile.max_reactions_per_post
            channel.reaction_window_min_seconds = (
                profile.reaction_window_min_seconds
            )
            channel.reaction_window_max_seconds = (
                profile.reaction_window_max_seconds
            )
            channel.image_post_reaction_percent = (
                profile.image_post_reaction_percent
            )
            channel.no_image_post_reaction_percent = (
                profile.no_image_post_reaction_percent
            )
            channel.updated_at = now

            marker = await session.get(AppSetting, setting_key)
            if marker is None:
                session.add(
                    AppSetting(key=setting_key, value=profile.key, updated_at=now)
                )
            else:
                marker.value = profile.key
                marker.updated_at = now
            await session.commit()
            await session.refresh(channel)
            return channel

    async def set_channel_promotion_period(
        self,
        channel_id: int,
        *,
        days: int | None,
        now: datetime | None = None,
    ) -> Channel | None:
        if days is not None and not 1 <= days <= 365:
            raise ValueError("Период должен быть от 1 до 365 дней")
        started = now or utcnow()
        values: dict[str, object] = {
            "promotion_mode": "permanent" if days is None else "timed",
            "promotion_started_at": started,
            "promotion_until": None if days is None else started + timedelta(days=days),
            "updated_at": started,
        }
        async with self.sessions() as session:
            await session.execute(update(Channel).where(Channel.id == channel_id).values(**values))
            await session.commit()
            return await session.get(Channel, channel_id)

    async def close_expired_promotion(self, channel_id: int, *, now: datetime | None = None) -> dict[str, int]:
        current = now or utcnow()
        async with self.sessions() as session:
            channel = await session.get(Channel, channel_id)
            if (
                channel is None
                or channel.promotion_mode != "timed"
                or channel.promotion_until is None
                or channel.promotion_until > current
            ):
                return {"expired": 0, "cancelled": 0}
            was_enabled = channel.new_posts_enabled or channel.old_posts_enabled
            channel.new_posts_enabled = False
            channel.old_posts_enabled = False
            channel.updated_at = current
            result = await session.execute(
                update(ReactionJob)
                .where(
                    ReactionJob.channel_id == channel_id,
                    ReactionJob.status == "pending",
                )
                .values(
                    status="cancelled", error="Период раскрутки завершён",
                    completed_at=current, started_at=None
                )
            )
            await session.commit()
            cancelled = int(result.rowcount or 0)
            return {"expired": int(bool(was_enabled or cancelled)), "cancelled": cancelled}

    async def cancel_pending_reactions_after(self, channel_id: int, cutoff: datetime) -> int:
        async with self.sessions() as session:
            result = await session.execute(
                update(ReactionJob)
                .where(
                    ReactionJob.channel_id == channel_id,
                    ReactionJob.status == "pending",
                    ReactionJob.due_at > cutoff,
                )
                .values(
                    status="cancelled", error="За пределами периода раскрутки",
                    completed_at=utcnow(), started_at=None
                )
            )
            await session.commit()
            return int(result.rowcount or 0)

    async def set_channel_last_seen(self, channel_id: int, message_id: int) -> None:
        async with self.sessions() as session:
            await session.execute(
                update(Channel)
                .where(Channel.id == channel_id)
                .values(last_seen_message_id=message_id, updated_at=utcnow(), last_error=None)
            )
            await session.commit()

    async def set_channel_error(self, channel_id: int, error: str) -> None:
        async with self.sessions() as session:
            await session.execute(
                update(Channel).where(Channel.id == channel_id).values(last_error=error[:4000], updated_at=utcnow())
            )
            await session.commit()

    async def deactivate_channel(self, channel_id: int) -> None:
        async with self.sessions() as session:
            await session.execute(
                update(Channel)
                .where(Channel.id == channel_id)
                .values(
                    is_active=False,
                    new_posts_enabled=False,
                    old_posts_enabled=False,
                    updated_at=utcnow(),
                )
            )
            await session.commit()

    async def delete_channel(self, channel_id: int) -> None:
        async with self.sessions() as session:
            await session.execute(delete(ViewJob).where(ViewJob.channel_id == channel_id))
            await session.execute(delete(ViewBatch).where(ViewBatch.channel_id == channel_id))
            await session.execute(delete(ReactionJob).where(ReactionJob.channel_id == channel_id))
            await session.execute(delete(JoinJob).where(JoinJob.channel_id == channel_id))
            await session.execute(delete(JobHistoryKey).where(JobHistoryKey.channel_id == channel_id))
            await session.execute(
                delete(JobHistoryChannelSummary).where(
                    JobHistoryChannelSummary.channel_id == channel_id
                )
            )
            await session.execute(
                delete(AppSetting).where(
                    AppSetting.key == channel_profile_setting_key(channel_id)
                )
            )
            await session.execute(delete(Channel).where(Channel.id == channel_id))
            await session.commit()
