from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol

CUSTOM_PROFILE_KEY = "custom"


class ChannelProfileState(Protocol):
    max_reactions_per_post: int | None
    reaction_window_min_seconds: int
    reaction_window_max_seconds: int
    image_post_reaction_percent: int
    no_image_post_reaction_percent: int


@dataclass(frozen=True, slots=True)
class ResolvedChannelProfile:
    key: str
    title: str
    emoji: str
    description: str
    max_reactions_per_post: int | None
    reaction_window_min_seconds: int
    reaction_window_max_seconds: int
    image_post_reaction_percent: int
    no_image_post_reaction_percent: int

    @property
    def label(self) -> str:
        return f"{self.emoji} {self.title}"


@dataclass(frozen=True, slots=True)
class ChannelProfile:
    key: str
    title: str
    emoji: str
    description: str
    max_reactions_per_post: int | None
    reaction_window_min_seconds: int
    reaction_window_max_seconds: int
    image_post_reaction_percent: int
    no_image_post_reaction_percent: int

    def resolve(self, max_accounts_per_channel: int) -> ResolvedChannelProfile:
        maximum = int(max_accounts_per_channel)
        if maximum < 1:
            raise ValueError("Максимум аккаунтов должен быть положительным")
        limit = self.max_reactions_per_post
        if limit is not None:
            limit = min(int(limit), maximum)
        return ResolvedChannelProfile(
            key=self.key,
            title=self.title,
            emoji=self.emoji,
            description=self.description,
            max_reactions_per_post=limit,
            reaction_window_min_seconds=self.reaction_window_min_seconds,
            reaction_window_max_seconds=self.reaction_window_max_seconds,
            image_post_reaction_percent=self.image_post_reaction_percent,
            no_image_post_reaction_percent=self.no_image_post_reaction_percent,
        )


CHANNEL_PROFILES: tuple[ChannelProfile, ...] = (
    ChannelProfile(
        key="cautious",
        title="Осторожный",
        emoji="🟢",
        description="Небольшая нагрузка и плавное распределение реакций.",
        max_reactions_per_post=10,
        reaction_window_min_seconds=30 * 60,
        reaction_window_max_seconds=60 * 60,
        image_post_reaction_percent=50,
        no_image_post_reaction_percent=20,
    ),
    ChannelProfile(
        key="normal",
        title="Обычный",
        emoji="🔵",
        description="Сбалансированный режим для повседневной работы.",
        max_reactions_per_post=25,
        reaction_window_min_seconds=15 * 60,
        reaction_window_max_seconds=30 * 60,
        image_post_reaction_percent=80,
        no_image_post_reaction_percent=50,
    ),
    ChannelProfile(
        key="active",
        title="Активный",
        emoji="🔥",
        description="Максимальный охват с более быстрым распределением.",
        max_reactions_per_post=None,
        reaction_window_min_seconds=5 * 60,
        reaction_window_max_seconds=15 * 60,
        image_post_reaction_percent=100,
        no_image_post_reaction_percent=80,
    ),
)

_PROFILE_BY_KEY = {profile.key: profile for profile in CHANNEL_PROFILES}


def _validate_profiles() -> None:
    if len(_PROFILE_BY_KEY) != len(CHANNEL_PROFILES):
        raise RuntimeError("Ключи профилей каналов должны быть уникальными")
    for profile in CHANNEL_PROFILES:
        if (
            not profile.key
            or len(profile.key) > 21
            or not profile.key.isascii()
            or any(
                not (character.islower() or character.isdigit() or character == "_")
                for character in profile.key
            )
        ):
            raise RuntimeError("Некорректный ключ профиля канала")
        if profile.max_reactions_per_post is not None and profile.max_reactions_per_post < 1:
            raise RuntimeError("Лимит профиля должен быть положительным")
        if profile.reaction_window_min_seconds < 0:
            raise RuntimeError("Минимальный период профиля не может быть отрицательным")
        if profile.reaction_window_max_seconds < profile.reaction_window_min_seconds:
            raise RuntimeError("Максимальный период профиля меньше минимального")
        if profile.reaction_window_max_seconds > 7 * 24 * 60 * 60:
            raise RuntimeError("Период профиля не может превышать 7 дней")
        for percent in (
            profile.image_post_reaction_percent,
            profile.no_image_post_reaction_percent,
        ):
            if not 0 <= percent <= 100:
                raise RuntimeError("Процент профиля должен быть от 0 до 100")


_validate_profiles()


def get_channel_profile(profile_key: str) -> ChannelProfile:
    try:
        return _PROFILE_BY_KEY[str(profile_key)]
    except KeyError as exc:
        raise ValueError("Неизвестный профиль канала") from exc


def resolve_channel_profile(
    profile_key: str, max_accounts_per_channel: int
) -> ResolvedChannelProfile:
    return get_channel_profile(profile_key).resolve(max_accounts_per_channel)


def validate_resolved_channel_profile(profile: ResolvedChannelProfile) -> None:
    definition = get_channel_profile(profile.key)
    if (
        profile.reaction_window_min_seconds
        != definition.reaction_window_min_seconds
        or profile.reaction_window_max_seconds
        != definition.reaction_window_max_seconds
        or profile.image_post_reaction_percent
        != definition.image_post_reaction_percent
        or profile.no_image_post_reaction_percent
        != definition.no_image_post_reaction_percent
    ):
        raise ValueError("Параметры профиля не соответствуют его ключу")
    if definition.max_reactions_per_post is None:
        if profile.max_reactions_per_post is not None:
            raise ValueError("Активный профиль должен использовать все аккаунты")
    elif (
        profile.max_reactions_per_post is None
        or not 1
        <= profile.max_reactions_per_post
        <= definition.max_reactions_per_post
    ):
        raise ValueError("Некорректный лимит разрешённого профиля")


def channel_profile_matches(
    channel: ChannelProfileState,
    profile: ResolvedChannelProfile,
) -> bool:
    return (
        channel.max_reactions_per_post == profile.max_reactions_per_post
        and int(channel.reaction_window_min_seconds)
        == profile.reaction_window_min_seconds
        and int(channel.reaction_window_max_seconds)
        == profile.reaction_window_max_seconds
        and int(channel.image_post_reaction_percent)
        == profile.image_post_reaction_percent
        and int(channel.no_image_post_reaction_percent)
        == profile.no_image_post_reaction_percent
    )


def detect_channel_profile_key(
    channel: ChannelProfileState,
    *,
    max_accounts_per_channel: int,
    preferred_key: str | None = None,
) -> str:
    matches = [
        profile.key
        for profile in CHANNEL_PROFILES
        if channel_profile_matches(
            channel, profile.resolve(max_accounts_per_channel)
        )
    ]
    if preferred_key in matches:
        return str(preferred_key)
    if len(matches) == 1:
        return matches[0]
    return CUSTOM_PROFILE_KEY


def channel_profile_label(profile_key: str) -> str:
    if profile_key == CUSTOM_PROFILE_KEY:
        return "⚙️ Свой"
    profile = get_channel_profile(profile_key)
    return f"{profile.emoji} {profile.title}"


def channel_profile_setting_key(channel_id: int) -> str:
    value = int(channel_id)
    if value < 1:
        raise ValueError("Некорректный ID канала")
    return f"channel_profile:{value}"
