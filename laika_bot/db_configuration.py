from __future__ import annotations

from .db_shared import *  # noqa: F403


class ConfigurationDatabaseMixin:
    async def get_setting(self, key: str, default: str) -> str:
        async with self.sessions() as session:
            row = await session.get(AppSetting, key)
            return row.value if row else default

    async def set_setting(self, key: str, value: str) -> None:
        async with self.sessions() as session:
            row = await session.get(AppSetting, key)
            if row:
                row.value = value
            else:
                session.add(AppSetting(key=key, value=value))
            await session.commit()

    async def get_channel_profile_key(
        self, channel: Channel, *, max_accounts_per_channel: int
    ) -> str:
        preferred = await self.get_setting(
            channel_profile_setting_key(channel.id), CUSTOM_PROFILE_KEY
        )
        return detect_channel_profile_key(
            channel,
            max_accounts_per_channel=max_accounts_per_channel,
            preferred_key=preferred,
        )

    @staticmethod
    async def _upsert_setting_in_session(
        session: AsyncSession, key: str, value: str, *, now: datetime
    ) -> None:
        row = await session.get(AppSetting, key)
        if row is None:
            session.add(AppSetting(key=key, value=value, updated_at=now))
        else:
            row.value = value
            row.updated_at = now

    @staticmethod
    def _setting_int(raw: str | None, default: int) -> int:
        try:
            return int(raw) if raw is not None else int(default)
        except (TypeError, ValueError):
            return int(default)

    async def _configuration_payload_from_session(
        self,
        session: AsyncSession,
        *,
        default_reaction_min: int,
        default_reaction_max: int,
        default_membership_min: int,
        default_membership_max: int,
        max_accounts_per_channel: int,
    ) -> dict[str, object]:
        settings_rows = list((await session.scalars(select(AppSetting))).all())
        settings_map = {row.key: row.value for row in settings_rows}
        reaction_weights = self._decode_reaction_weights(settings_map.get("reactions")) or {
            "👍": 1.0
        }

        reaction_min = self._setting_int(
            settings_map.get("reaction_delay_min"), default_reaction_min
        )
        reaction_max = self._setting_int(
            settings_map.get("reaction_delay_max"), default_reaction_max
        )
        if reaction_min < 1 or reaction_max < reaction_min or reaction_max > 86400:
            reaction_min, reaction_max = int(default_reaction_min), int(default_reaction_max)

        membership_min = self._setting_int(
            settings_map.get("membership_delay_min"), default_membership_min
        )
        membership_max = self._setting_int(
            settings_map.get("membership_delay_max"), default_membership_max
        )
        if (
            membership_min < 1
            or membership_max < membership_min
            or membership_max > 86400
        ):
            membership_min, membership_max = (
                int(default_membership_min),
                int(default_membership_max),
            )

        accounts = list((await session.scalars(select(Account).order_by(Account.id))).all())
        targets = list((await session.scalars(select(Channel).order_by(Channel.id))).all())

        account_rows = [
            {
                "telegram_user_id": int(account.telegram_user_id),
                "display_name": str(account.display_name),
                "username": account.username,
                "phone_masked": mask_phone(account.phone),
                "is_active": bool(account.is_active),
                "status": str(account.status),
            }
            for account in accounts
        ]

        target_rows: list[dict[str, object]] = []
        for channel in targets:
            preferred_key = settings_map.get(
                channel_profile_setting_key(channel.id), CUSTOM_PROFILE_KEY
            )
            profile_key = (
                detect_channel_profile_key(
                    channel,
                    max_accounts_per_channel=max_accounts_per_channel,
                    preferred_key=preferred_key,
                )
                if channel.kind == "channel"
                else CUSTOM_PROFILE_KEY
            )
            target_rows.append(
                {
                    "telegram_channel_id": int(channel.telegram_channel_id),
                    "kind": str(channel.kind),
                    "title": str(channel.title),
                    "username": channel.username,
                    "settings": {
                        "is_active": bool(channel.is_active),
                        "new_posts_enabled": bool(channel.new_posts_enabled),
                        "old_posts_enabled": bool(channel.old_posts_enabled),
                        "old_posts_depth": int(channel.old_posts_depth),
                        "reaction_weights": self._decode_reaction_weights(
                            channel.reactions_json
                        ),
                        "max_reactions_per_post": (
                            int(channel.max_reactions_per_post)
                            if channel.max_reactions_per_post is not None
                            else None
                        ),
                        "reaction_window_min_seconds": int(
                            channel.reaction_window_min_seconds
                        ),
                        "reaction_window_max_seconds": int(
                            channel.reaction_window_max_seconds
                        ),
                        "image_post_reaction_percent": int(
                            channel.image_post_reaction_percent
                        ),
                        "no_image_post_reaction_percent": int(
                            channel.no_image_post_reaction_percent
                        ),
                        "promotion_mode": str(channel.promotion_mode),
                        "promotion_started_at": (
                            utc_iso(channel.promotion_started_at)
                            if channel.promotion_started_at is not None
                            else None
                        ),
                        "promotion_until": (
                            utc_iso(channel.promotion_until)
                            if channel.promotion_until is not None
                            else None
                        ),
                        "profile_key": profile_key,
                    },
                }
            )

        return validate_payload(
            {
                "global_settings": {
                    "reaction_weights": reaction_weights,
                    "reaction_delay": {
                        "min_seconds": reaction_min,
                        "max_seconds": reaction_max,
                    },
                    "membership_delay": {
                        "min_seconds": membership_min,
                        "max_seconds": membership_max,
                    },
                },
                "accounts": account_rows,
                "targets": target_rows,
            }
        )

    async def export_configuration_payload(
        self,
        *,
        default_reaction_min: int,
        default_reaction_max: int,
        default_membership_min: int,
        default_membership_max: int,
        max_accounts_per_channel: int,
    ) -> dict[str, object]:
        async with self.sessions() as session:
            return await self._configuration_payload_from_session(
                session,
                default_reaction_min=default_reaction_min,
                default_reaction_max=default_reaction_max,
                default_membership_min=default_membership_min,
                default_membership_max=default_membership_max,
                max_accounts_per_channel=max_accounts_per_channel,
            )

    @staticmethod
    def _validate_restore_runtime_limits(
        payload: Mapping[str, object],
        *,
        max_accounts_per_channel: int,
        max_old_posts: int,
    ) -> None:
        maximum_accounts = int(max_accounts_per_channel)
        maximum_old_posts = int(max_old_posts)
        if maximum_accounts < 1 or maximum_old_posts < 1:
            raise BackupValidationError("Runtime-лимиты LikeBot настроены некорректно")
        for target in payload["targets"]:  # type: ignore[index]
            settings = target["settings"]
            if int(settings["old_posts_depth"]) > maximum_old_posts:
                raise BackupValidationError(
                    "Глубина старых постов в копии превышает MAX_OLD_POSTS"
                )
            limit = settings["max_reactions_per_post"]
            if limit is not None and int(limit) > maximum_accounts:
                raise BackupValidationError(
                    "Лимит реакций в копии превышает MAX_ACCOUNTS_PER_CHANNEL"
                )

    async def preview_configuration_restore(
        self,
        payload: Mapping[str, object],
        *,
        default_reaction_min: int,
        default_reaction_max: int,
        default_membership_min: int,
        default_membership_max: int,
        max_accounts_per_channel: int,
        max_old_posts: int,
    ) -> dict[str, object]:
        normalized = validate_payload(payload)
        self._validate_restore_runtime_limits(
            normalized,
            max_accounts_per_channel=max_accounts_per_channel,
            max_old_posts=max_old_posts,
        )
        async with self.sessions() as session:
            current = await self._configuration_payload_from_session(
                session,
                default_reaction_min=default_reaction_min,
                default_reaction_max=default_reaction_max,
                default_membership_min=default_membership_min,
                default_membership_max=default_membership_max,
                max_accounts_per_channel=max_accounts_per_channel,
            )

        current_targets = {
            int(item["telegram_channel_id"]): item for item in current["targets"]
        }
        matched = changed = unchanged = missing = kind_mismatch = 0
        matched_ids: list[int] = []
        for item in normalized["targets"]:
            telegram_id = int(item["telegram_channel_id"])
            existing = current_targets.get(telegram_id)
            if existing is None:
                missing += 1
                continue
            if existing["kind"] != item["kind"]:
                kind_mismatch += 1
                continue
            matched += 1
            matched_ids.append(telegram_id)
            if existing["settings"] == item["settings"]:
                unchanged += 1
            else:
                changed += 1

        current_accounts = {
            int(item["telegram_user_id"]) for item in current["accounts"]
        }
        backup_accounts = {
            int(item["telegram_user_id"]) for item in normalized["accounts"]
        }
        return {
            "global_changed": current["global_settings"] != normalized["global_settings"],
            "targets_total": len(normalized["targets"]),
            "targets_matched": matched,
            "targets_changed": changed,
            "targets_unchanged": unchanged,
            "targets_missing": missing,
            "targets_kind_mismatch": kind_mismatch,
            "matched_telegram_channel_ids": matched_ids,
            "accounts_total": len(normalized["accounts"]),
            "accounts_matched": len(current_accounts & backup_accounts),
            "accounts_missing": len(backup_accounts - current_accounts),
            "payload_sha256": payload_sha256(normalized),
        }

    async def _prune_configuration_events_in_session(
        self, session: AsyncSession, *, keep: int = CONFIGURATION_EVENT_RETENTION
    ) -> None:
        retention = max(1, int(keep))
        old_ids = list(
            (
                await session.scalars(
                    select(ConfigurationEvent.id)
                    .order_by(desc(ConfigurationEvent.created_at), desc(ConfigurationEvent.id))
                    .offset(retention)
                )
            ).all()
        )
        if old_ids:
            await session.execute(
                delete(ConfigurationEvent).where(ConfigurationEvent.id.in_(old_ids))
            )

    async def record_configuration_export(
        self, payload: Mapping[str, object], *, source_name: str | None = None
    ) -> int:
        normalized = validate_payload(payload)
        summary = {
            "accounts": len(normalized["accounts"]),
            "targets": len(normalized["targets"]),
            "payload_sha256": payload_sha256(normalized),
        }
        async with self.sessions() as session:
            event = ConfigurationEvent(
                event_type="export",
                status="done",
                source_name=sanitize_source_name(source_name),
                summary_json=canonical_json(summary).decode("utf-8"),
            )
            session.add(event)
            await session.flush()
            await self._prune_configuration_events_in_session(session)
            await session.commit()
            return int(event.id)

    async def restore_configuration_payload(
        self,
        payload: Mapping[str, object],
        *,
        source_name: str | None,
        event_type: str = "restore",
        default_reaction_min: int,
        default_reaction_max: int,
        default_membership_min: int,
        default_membership_max: int,
        max_accounts_per_channel: int,
        max_old_posts: int,
    ) -> dict[str, object]:
        if event_type not in {"restore", "rollback"}:
            raise ValueError("Некорректный тип события восстановления")
        normalized = validate_payload(payload)
        self._validate_restore_runtime_limits(
            normalized,
            max_accounts_per_channel=max_accounts_per_channel,
            max_old_posts=max_old_posts,
        )
        now = utcnow()
        async with self.sessions() as session:
            async with session.begin():
                before_payload = await self._configuration_payload_from_session(
                    session,
                    default_reaction_min=default_reaction_min,
                    default_reaction_max=default_reaction_max,
                    default_membership_min=default_membership_min,
                    default_membership_max=default_membership_max,
                    max_accounts_per_channel=max_accounts_per_channel,
                )

                before_snapshot = canonical_json(before_payload)
                if len(before_snapshot) > MAX_BACKUP_BYTES:
                    raise BackupValidationError(
                        "Текущее состояние слишком велико для безопасного снимка отката"
                    )

                global_settings = normalized["global_settings"]
                await self._upsert_setting_in_session(
                    session,
                    "reactions",
                    json.dumps(
                        global_settings["reaction_weights"], ensure_ascii=False
                    ),
                    now=now,
                )
                for key, value in (
                    (
                        "reaction_delay_min",
                        global_settings["reaction_delay"]["min_seconds"],
                    ),
                    (
                        "reaction_delay_max",
                        global_settings["reaction_delay"]["max_seconds"],
                    ),
                    (
                        "membership_delay_min",
                        global_settings["membership_delay"]["min_seconds"],
                    ),
                    (
                        "membership_delay_max",
                        global_settings["membership_delay"]["max_seconds"],
                    ),
                ):
                    await self._upsert_setting_in_session(
                        session, key, str(value), now=now
                    )

                telegram_ids = [
                    int(item["telegram_channel_id"]) for item in normalized["targets"]
                ]
                channels = list(
                    (
                        await session.scalars(
                            select(Channel)
                            .where(Channel.telegram_channel_id.in_(telegram_ids))
                            .with_for_update()
                        )
                    ).all()
                ) if telegram_ids else []
                by_telegram_id = {int(item.telegram_channel_id): item for item in channels}

                matched = changed = unchanged = missing = kind_mismatch = 0
                changed_channel_ids: list[int] = []
                restored_channel_ids: list[int] = []
                for item in normalized["targets"]:
                    telegram_id = int(item["telegram_channel_id"])
                    channel = by_telegram_id.get(telegram_id)
                    if channel is None:
                        missing += 1
                        continue
                    if channel.kind != item["kind"]:
                        kind_mismatch += 1
                        continue
                    matched += 1
                    restored_channel_ids.append(int(channel.id))
                    settings = item["settings"]
                    current_values = {
                        "is_active": bool(channel.is_active),
                        "new_posts_enabled": bool(channel.new_posts_enabled),
                        "old_posts_enabled": bool(channel.old_posts_enabled),
                        "old_posts_depth": int(channel.old_posts_depth),
                        "reaction_weights": self._decode_reaction_weights(
                            channel.reactions_json
                        ),
                        "max_reactions_per_post": channel.max_reactions_per_post,
                        "reaction_window_min_seconds": int(
                            channel.reaction_window_min_seconds
                        ),
                        "reaction_window_max_seconds": int(
                            channel.reaction_window_max_seconds
                        ),
                        "image_post_reaction_percent": int(
                            channel.image_post_reaction_percent
                        ),
                        "no_image_post_reaction_percent": int(
                            channel.no_image_post_reaction_percent
                        ),
                        "promotion_mode": str(channel.promotion_mode),
                        "promotion_started_at": (
                            utc_iso(channel.promotion_started_at)
                            if channel.promotion_started_at is not None
                            else None
                        ),
                        "promotion_until": (
                            utc_iso(channel.promotion_until)
                            if channel.promotion_until is not None
                            else None
                        ),
                    }
                    comparison_values = dict(settings)
                    comparison_values.pop("profile_key", None)
                    if current_values == comparison_values:
                        unchanged += 1
                    else:
                        changed += 1
                        changed_channel_ids.append(int(channel.id))

                    channel.is_active = bool(settings["is_active"])
                    channel.new_posts_enabled = bool(settings["new_posts_enabled"])
                    channel.old_posts_enabled = bool(settings["old_posts_enabled"])
                    channel.old_posts_depth = int(settings["old_posts_depth"])
                    channel.reactions_json = (
                        json.dumps(settings["reaction_weights"], ensure_ascii=False)
                        if settings["reaction_weights"] is not None
                        else None
                    )
                    channel.max_reactions_per_post = settings["max_reactions_per_post"]
                    channel.reaction_window_min_seconds = int(
                        settings["reaction_window_min_seconds"]
                    )
                    channel.reaction_window_max_seconds = int(
                        settings["reaction_window_max_seconds"]
                    )
                    channel.image_post_reaction_percent = int(
                        settings["image_post_reaction_percent"]
                    )
                    channel.no_image_post_reaction_percent = int(
                        settings["no_image_post_reaction_percent"]
                    )
                    channel.promotion_mode = str(settings["promotion_mode"])
                    channel.promotion_started_at = parse_utc_iso(
                        settings["promotion_started_at"],
                        field="promotion_started_at",
                        allow_none=True,
                    )
                    channel.promotion_until = parse_utc_iso(
                        settings["promotion_until"],
                        field="promotion_until",
                        allow_none=True,
                    )
                    channel.updated_at = now

                    preferred_key = (
                        str(settings["profile_key"])
                        if channel.kind == "channel"
                        else CUSTOM_PROFILE_KEY
                    )
                    actual_profile_key = (
                        detect_channel_profile_key(
                            channel,
                            max_accounts_per_channel=max_accounts_per_channel,
                            preferred_key=preferred_key,
                        )
                        if channel.kind == "channel"
                        else CUSTOM_PROFILE_KEY
                    )
                    profile_setting_key = channel_profile_setting_key(channel.id)
                    if channel.kind == "channel":
                        await self._upsert_setting_in_session(
                            session, profile_setting_key, actual_profile_key, now=now
                        )
                    else:
                        await session.execute(
                            delete(AppSetting).where(AppSetting.key == profile_setting_key)
                        )

                    cancellation_reason: str | None = None
                    source: str | None = None
                    if not channel.is_active:
                        cancellation_reason = "Канал отключён восстановлением настроек"
                    elif not channel.new_posts_enabled and not channel.old_posts_enabled:
                        cancellation_reason = "Авто лайк отключён восстановлением настроек"
                    if cancellation_reason is not None:
                        await session.execute(
                            update(ReactionJob)
                            .where(
                                ReactionJob.channel_id == channel.id,
                                ReactionJob.status == "pending",
                            )
                            .values(
                                status="cancelled",
                                error=cancellation_reason,
                                completed_at=now,
                                started_at=None,
                            )
                        )
                        await session.execute(
                            update(JoinJob)
                            .where(
                                JoinJob.channel_id == channel.id,
                                JoinJob.status == "pending",
                            )
                            .values(
                                status="cancelled",
                                error=cancellation_reason,
                                completed_at=now,
                                started_at=None,
                            )
                        )
                        affected_batch_ids = list(
                            (
                                await session.scalars(
                                    select(ViewJob.batch_id)
                                    .where(
                                        ViewJob.channel_id == channel.id,
                                        ViewJob.status == "pending",
                                    )
                                    .distinct()
                                )
                            ).all()
                        )
                        await session.execute(
                            update(ViewJob)
                            .where(
                                ViewJob.channel_id == channel.id,
                                ViewJob.status == "pending",
                            )
                            .values(
                                status="cancelled",
                                error=cancellation_reason,
                                completed_at=now,
                                started_at=None,
                            )
                        )
                        for batch_id in affected_batch_ids:
                            await self._sync_view_batch_status(session, int(batch_id))
                    else:
                        if not channel.new_posts_enabled:
                            source = "new"
                        elif not channel.old_posts_enabled:
                            source = "old"
                        if source is not None:
                            await session.execute(
                                update(ReactionJob)
                                .where(
                                    ReactionJob.channel_id == channel.id,
                                    ReactionJob.status == "pending",
                                    ReactionJob.source == source,
                                )
                                .values(
                                    status="cancelled",
                                    error="Источник отключён восстановлением настроек",
                                    completed_at=now,
                                    started_at=None,
                                )
                            )

                summary = {
                    "global_changed": before_payload["global_settings"]
                    != normalized["global_settings"],
                    "targets_total": len(normalized["targets"]),
                    "targets_matched": matched,
                    "targets_changed": changed,
                    "targets_unchanged": unchanged,
                    "targets_missing": missing,
                    "targets_kind_mismatch": kind_mismatch,
                    "accounts_total": len(normalized["accounts"]),
                    "payload_sha256": payload_sha256(normalized),
                }
                event = ConfigurationEvent(
                    event_type=event_type,
                    status="done",
                    source_name=sanitize_source_name(source_name),
                    summary_json=canonical_json(summary).decode("utf-8"),
                    snapshot_json=before_snapshot.decode("utf-8"),
                    snapshot_sha256=payload_sha256(before_payload),
                    created_at=now,
                )
                session.add(event)
                await session.flush()
                await self._prune_configuration_events_in_session(session)

            return {
                **summary,
                "event_id": int(event.id),
                "changed_channel_ids": changed_channel_ids,
                "restored_channel_ids": restored_channel_ids,
            }

    async def list_configuration_events(
        self, *, limit: int = 10
    ) -> list[ConfigurationEvent]:
        safe_limit = min(50, max(1, int(limit)))
        async with self.sessions() as session:
            return list(
                (
                    await session.scalars(
                        select(ConfigurationEvent)
                        .order_by(
                            desc(ConfigurationEvent.created_at),
                            desc(ConfigurationEvent.id),
                        )
                        .limit(safe_limit)
                    )
                ).all()
            )

    async def get_configuration_event(
        self, event_id: int
    ) -> ConfigurationEvent | None:
        async with self.sessions() as session:
            return await session.get(ConfigurationEvent, int(event_id))

    async def rollback_configuration_event(
        self,
        event_id: int,
        *,
        default_reaction_min: int,
        default_reaction_max: int,
        default_membership_min: int,
        default_membership_max: int,
        max_accounts_per_channel: int,
        max_old_posts: int,
    ) -> dict[str, object]:
        event = await self.get_configuration_event(event_id)
        if event is None or not event.snapshot_json or not event.snapshot_sha256:
            raise BackupValidationError("Для этого события нет снимка для отката")
        try:
            raw_payload = json.loads(event.snapshot_json)
        except json.JSONDecodeError as exc:
            raise BackupValidationError("Снимок истории повреждён") from exc
        normalized = validate_payload(raw_payload)
        if payload_sha256(normalized) != event.snapshot_sha256:
            raise BackupValidationError("Контрольная сумма снимка истории не совпала")
        return await self.restore_configuration_payload(
            normalized,
            source_name=f"history:{event.id}",
            event_type="rollback",
            default_reaction_min=default_reaction_min,
            default_reaction_max=default_reaction_max,
            default_membership_min=default_membership_min,
            default_membership_max=default_membership_max,
            max_accounts_per_channel=max_accounts_per_channel,
            max_old_posts=max_old_posts,
        )

    @staticmethod
    def _decode_reaction_weights(raw: str | None) -> dict[str, float] | None:
        """Decode v1.0.12 weighted JSON and legacy v1.0.11 emoji lists."""

        if not raw:
            return None
        try:
            parsed = json.loads(raw)
        except (TypeError, ValueError):
            return None

        weights: dict[str, float] = {}
        if isinstance(parsed, list):
            for item in parsed:
                reaction = str(item).strip()
                if reaction and reaction not in weights:
                    weights[reaction] = 1.0
        elif isinstance(parsed, dict):
            for item, raw_weight in parsed.items():
                reaction = str(item).strip()
                if not reaction:
                    continue
                try:
                    weight = float(raw_weight)
                except (TypeError, ValueError):
                    continue
                if weight < 0 or weight != weight or weight in {float("inf"), float("-inf")}:
                    continue
                weights[reaction] = weight
        else:
            return None

        if not weights or sum(weights.values()) <= 0:
            return None
        return weights

    @staticmethod
    def _coerce_reaction_weights(
        reactions: Mapping[str, float] | Iterable[str],
    ) -> dict[str, float]:
        weights: dict[str, float] = {}
        if isinstance(reactions, Mapping):
            for item, raw_weight in reactions.items():
                reaction = str(item).strip()
                if reaction:
                    weights[reaction] = float(raw_weight)
        else:
            for item in reactions:
                reaction = str(item).strip()
                if reaction:
                    weights[reaction] = 1.0
        if (
            not weights
            or any(not math.isfinite(weight) or weight < 0 for weight in weights.values())
            or sum(weights.values()) <= 0
        ):
            raise ValueError("Некорректные веса реакций")
        return weights

    @classmethod
    def _decode_reactions(cls, raw: str | None) -> list[str] | None:
        weights = cls._decode_reaction_weights(raw)
        return list(weights) if weights else None

    async def get_reaction_weights(self) -> dict[str, float]:
        """Global weighted reactions used by targets without their own settings."""

        raw = await self.get_setting("reactions", json.dumps({"👍": 1}, ensure_ascii=False))
        return self._decode_reaction_weights(raw) or {"👍": 1.0}

    async def get_reactions(self) -> list[str]:
        """Compatibility view containing only global reaction emoji."""

        return list(await self.get_reaction_weights())

    async def set_reaction_weights(self, weights: Mapping[str, float]) -> None:
        prepared = self._coerce_reaction_weights(weights)
        await self.set_setting("reactions", json.dumps(prepared, ensure_ascii=False))

    async def set_reactions(self, reactions: list[str]) -> None:
        await self.set_reaction_weights({reaction: 1.0 for reaction in reactions})

    async def get_channel_reaction_override_weights(self, channel_id: int) -> dict[str, float] | None:
        async with self.sessions() as session:
            raw = await session.scalar(select(Channel.reactions_json).where(Channel.id == channel_id))
        return self._decode_reaction_weights(raw)

    async def get_channel_reaction_override(self, channel_id: int) -> list[str] | None:
        weights = await self.get_channel_reaction_override_weights(channel_id)
        return list(weights) if weights else None

    async def get_reaction_weights_for_channel(self, channel: Channel) -> dict[str, float]:
        """Return target-specific weights, falling back to global defaults."""

        override = self._decode_reaction_weights(channel.reactions_json)
        return override or await self.get_reaction_weights()

    async def get_reactions_for_channel(self, channel: Channel) -> list[str]:
        """Compatibility view containing only effective target emoji."""

        return list(await self.get_reaction_weights_for_channel(channel))

    async def set_channel_reaction_weights(
        self, channel_id: int, weights: Mapping[str, float]
    ) -> None:
        prepared = self._coerce_reaction_weights(weights)
        async with self.sessions() as session:
            await session.execute(
                update(Channel)
                .where(Channel.id == channel_id)
                .values(
                    reactions_json=json.dumps(prepared, ensure_ascii=False),
                    updated_at=utcnow(),
                )
            )
            await session.commit()

    async def set_channel_reactions(self, channel_id: int, reactions: list[str]) -> None:
        await self.set_channel_reaction_weights(
            channel_id, {reaction: 1.0 for reaction in reactions}
        )

    async def clear_channel_reactions(self, channel_id: int) -> None:
        async with self.sessions() as session:
            await session.execute(
                update(Channel)
                .where(Channel.id == channel_id)
                .values(reactions_json=None, updated_at=utcnow())
            )
            await session.commit()

    @staticmethod
    def _validate_post_type_percent(value: int) -> int:
        percent = int(value)
        if not 0 <= percent <= 100:
            raise ValueError("Процент должен быть от 0 до 100")
        return percent

    async def set_channel_post_type_percent(
        self, channel_id: int, *, post_type: str, percent: int
    ) -> None:
        value = self._validate_post_type_percent(percent)
        if post_type == "image":
            values = {"image_post_reaction_percent": value}
        elif post_type == "no_image":
            values = {"no_image_post_reaction_percent": value}
        else:
            raise ValueError("Неизвестный тип поста")
        async with self.sessions() as session:
            await session.execute(
                update(Channel)
                .where(Channel.id == channel_id)
                .values(**values, updated_at=utcnow())
            )
            await session.commit()

    async def set_channel_post_type_percents(
        self, channel_id: int, *, image_percent: int, no_image_percent: int
    ) -> None:
        image_value = self._validate_post_type_percent(image_percent)
        no_image_value = self._validate_post_type_percent(no_image_percent)
        async with self.sessions() as session:
            await session.execute(
                update(Channel)
                .where(Channel.id == channel_id)
                .values(
                    image_post_reaction_percent=image_value,
                    no_image_post_reaction_percent=no_image_value,
                    updated_at=utcnow(),
                )
            )
            await session.commit()

    async def refresh_pending_reactions(
        self,
        channel_id: int,
        reactions: Mapping[str, float] | Iterable[str],
    ) -> int:
        """Reassign pending jobs using the target's new weighted distribution."""

        weights = self._coerce_reaction_weights(reactions)
        async with self.sessions() as session:
            jobs = list(
                (
                    await session.scalars(
                        select(ReactionJob)
                        .where(
                            ReactionJob.channel_id == channel_id,
                            ReactionJob.status == "pending",
                        )
                        .order_by(ReactionJob.id)
                    )
                ).all()
            )
            for job in jobs:
                job.reaction = choose_weighted_reaction(weights, _rng)
            await session.commit()
            return len(jobs)

    async def refresh_pending_default_reactions(
        self, reactions: Mapping[str, float] | Iterable[str]
    ) -> int:
        """Update pending jobs only for targets that still inherit global defaults."""

        weights = self._coerce_reaction_weights(reactions)
        async with self.sessions() as session:
            jobs = list(
                (
                    await session.scalars(
                        select(ReactionJob)
                        .join(Channel, Channel.id == ReactionJob.channel_id)
                        .where(
                            ReactionJob.status == "pending",
                            Channel.reactions_json.is_(None),
                        )
                        .order_by(ReactionJob.id)
                    )
                ).all()
            )
            for job in jobs:
                job.reaction = choose_weighted_reaction(weights, _rng)
            await session.commit()
            return len(jobs)

    async def _get_delay_range(
        self,
        *,
        minimum_key: str,
        maximum_key: str,
        default_min: int,
        default_max: int,
    ) -> tuple[int, int]:
        min_raw = await self.get_setting(minimum_key, str(default_min))
        max_raw = await self.get_setting(maximum_key, str(default_max))
        try:
            minimum, maximum = int(min_raw), int(max_raw)
        except ValueError:
            return default_min, default_max
        if minimum <= 0 or maximum < minimum:
            return default_min, default_max
        return minimum, maximum

    async def _set_delay_range(
        self,
        *,
        minimum_key: str,
        maximum_key: str,
        minimum: int,
        maximum: int,
    ) -> None:
        async with self.sessions() as session:
            for key, value in ((minimum_key, str(minimum)), (maximum_key, str(maximum))):
                row = await session.get(AppSetting, key)
                if row:
                    row.value = value
                else:
                    session.add(AppSetting(key=key, value=value))
            await session.commit()

    async def get_delays(self, default_min: int, default_max: int) -> tuple[int, int]:
        """Reaction delay range kept under the original public method name."""
        return await self._get_delay_range(
            minimum_key="reaction_delay_min",
            maximum_key="reaction_delay_max",
            default_min=default_min,
            default_max=default_max,
        )

    async def set_delays(self, minimum: int, maximum: int) -> None:
        await self._set_delay_range(
            minimum_key="reaction_delay_min",
            maximum_key="reaction_delay_max",
            minimum=minimum,
            maximum=maximum,
        )

    async def get_membership_delays(self, default_min: int, default_max: int) -> tuple[int, int]:
        """Delay between account subscriptions and unsubscriptions."""
        return await self._get_delay_range(
            minimum_key="membership_delay_min",
            maximum_key="membership_delay_max",
            default_min=default_min,
            default_max=default_max,
        )

    async def set_membership_delays(self, minimum: int, maximum: int) -> None:
        await self._set_delay_range(
            minimum_key="membership_delay_min",
            maximum_key="membership_delay_max",
            minimum=minimum,
            maximum=maximum,
        )

    @staticmethod
    def _validate_reschedule_range(minimum: int, maximum: int) -> None:
        if minimum < 1 or maximum < minimum:
            raise ValueError("Некорректный диапазон задержки")

    async def reschedule_pending_membership_jobs(
        self,
        minimum: int,
        maximum: int,
        *,
        now: datetime | None = None,
    ) -> dict[str, int]:
        """Recalculate pristine pending join/leave jobs using the current range.

        The first waiting job is due immediately; every following job receives a
        cumulative random interval. Retries are deliberately not moved because
        their due time may protect a FloodWait or exponential backoff.
        """
        self._validate_reschedule_range(minimum, maximum)
        base = now or utcnow()
        async with self.sessions() as session:
            total_pending = int(
                await session.scalar(
                    select(func.count(JoinJob.id)).where(JoinJob.status == "pending")
                )
                or 0
            )
            jobs = list(
                (
                    await session.scalars(
                        select(JoinJob)
                        .where(
                            JoinJob.status == "pending",
                            JoinJob.attempts == 0,
                        )
                        .order_by(JoinJob.due_at, JoinJob.id)
                        .with_for_update()
                    )
                ).all()
            )
            cursor = base
            for index, job in enumerate(jobs):
                if index == 0:
                    job.due_at = base
                else:
                    cursor += timedelta(seconds=_rng.randint(minimum, maximum))
                    job.due_at = cursor
                job.error = None
            await session.commit()
        return {
            "rescheduled": len(jobs),
            "skipped_retries": max(0, total_pending - len(jobs)),
        }

    async def reschedule_pending_reaction_jobs(
        self,
        minimum: int,
        maximum: int,
        *,
        now: datetime | None = None,
    ) -> dict[str, int]:
        """Recalculate pristine pending reaction jobs from the current moment.

        Every waiting reaction receives a fresh independent random delay inside
        the configured range. Retries keep their FloodWait/backoff due time.
        """
        self._validate_reschedule_range(minimum, maximum)
        base = now or utcnow()
        async with self.sessions() as session:
            total_pending = int(
                await session.scalar(
                    select(func.count(ReactionJob.id)).where(ReactionJob.status == "pending")
                )
                or 0
            )
            jobs = list(
                (
                    await session.scalars(
                        select(ReactionJob)
                        .where(
                            ReactionJob.status == "pending",
                            ReactionJob.attempts == 0,
                        )
                        .order_by(ReactionJob.due_at, ReactionJob.id)
                        .with_for_update()
                    )
                ).all()
            )
            for job in jobs:
                job.due_at = base + timedelta(seconds=_rng.randint(minimum, maximum))
                job.error = None
            await session.commit()
        return {
            "rescheduled": len(jobs),
            "skipped_retries": max(0, total_pending - len(jobs)),
        }
