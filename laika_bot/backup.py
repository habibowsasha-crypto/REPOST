from __future__ import annotations

import hashlib
import hmac
import json
import math
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Mapping

BACKUP_FORMAT = "likebot-configuration-backup"
BACKUP_SCHEMA_VERSION = 1
BACKUP_INTEGRITY_ALGORITHM = "HMAC-SHA256"
MAX_BACKUP_BYTES = 2 * 1024 * 1024
MAX_BACKUP_TARGETS = 10_000
MAX_BACKUP_ACCOUNTS = 10_000
MAX_REACTIONS = 100
MAX_EVENT_SOURCE_LENGTH = 255
MAX_APP_VERSION_LENGTH = 40

CHANNEL_CONFIGURATION_FIELDS = (
    "is_active",
    "new_posts_enabled",
    "old_posts_enabled",
    "old_posts_depth",
    "reaction_weights",
    "max_reactions_per_post",
    "reaction_window_min_seconds",
    "reaction_window_max_seconds",
    "image_post_reaction_percent",
    "no_image_post_reaction_percent",
    "promotion_mode",
    "promotion_started_at",
    "promotion_until",
    "profile_key",
)


class BackupValidationError(ValueError):
    """The uploaded file is not a supported LikeBot configuration backup."""


class BackupIntegrityError(BackupValidationError):
    """The backup was changed or signed with another LikeBot encryption key."""


@dataclass(frozen=True, slots=True)
class VerifiedBackup:
    envelope: dict[str, Any]
    payload: dict[str, Any]
    digest: str

    @property
    def created_at(self) -> str:
        return str(self.envelope["created_at"])

    @property
    def app_version(self) -> str:
        return str(self.envelope["app_version"])


def utc_iso(value: datetime | None = None) -> str:
    current = value or datetime.now(UTC)
    if current.tzinfo is None:
        current = current.replace(tzinfo=UTC)
    return current.astimezone(UTC).isoformat(timespec="seconds").replace("+00:00", "Z")


def parse_utc_iso(value: Any, *, field: str, allow_none: bool = False) -> datetime | None:
    if value is None and allow_none:
        return None
    if not isinstance(value, str) or not value or len(value) > 40:
        raise BackupValidationError(f"Некорректное поле {field}")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise BackupValidationError(f"Некорректная дата в поле {field}") from exc
    if parsed.tzinfo is None:
        raise BackupValidationError(f"Дата {field} должна содержать часовой пояс")
    return parsed.astimezone(UTC).replace(tzinfo=None)


def canonical_json(value: Any) -> bytes:
    try:
        encoded = json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        )
    except (TypeError, ValueError) as exc:
        raise BackupValidationError("Резервная копия содержит неподдерживаемые значения") from exc
    return encoded.encode("utf-8")


def payload_sha256(payload: Mapping[str, Any]) -> str:
    return hashlib.sha256(canonical_json(payload)).hexdigest()


def _derive_signing_key(secret: str) -> bytes:
    if not isinstance(secret, str) or len(secret) < 16:
        raise BackupValidationError("Ключ подписи резервной копии не настроен")
    return hashlib.sha256(
        b"LikeBot configuration backup signature v1\x00" + secret.encode("utf-8")
    ).digest()


def _unsigned_envelope(
    payload: Mapping[str, Any], *, app_version: str, created_at: datetime | None = None
) -> dict[str, Any]:
    return {
        "format": BACKUP_FORMAT,
        "schema_version": BACKUP_SCHEMA_VERSION,
        "created_at": utc_iso(created_at),
        "app_version": str(app_version),
        "payload": dict(payload),
    }


def create_backup_bytes(
    payload: Mapping[str, Any],
    *,
    app_version: str,
    signing_secret: str,
    created_at: datetime | None = None,
) -> bytes:
    if not isinstance(app_version, str) or not app_version or len(app_version) > MAX_APP_VERSION_LENGTH:
        raise BackupValidationError("Некорректная версия приложения")
    normalized = validate_payload(payload)
    unsigned = _unsigned_envelope(
        normalized, app_version=app_version, created_at=created_at
    )
    digest = hmac.new(
        _derive_signing_key(signing_secret), canonical_json(unsigned), hashlib.sha256
    ).hexdigest()
    envelope = {
        **unsigned,
        "integrity": {
            "algorithm": BACKUP_INTEGRITY_ALGORITHM,
            "digest": digest,
        },
    }
    rendered = json.dumps(
        envelope,
        ensure_ascii=False,
        sort_keys=True,
        indent=2,
        allow_nan=False,
    ).encode("utf-8") + b"\n"
    if len(rendered) > MAX_BACKUP_BYTES:
        raise BackupValidationError(
            f"Резервная копия превышает {MAX_BACKUP_BYTES // 1024} КБ"
        )
    return rendered


def verify_backup_bytes(raw: bytes, *, signing_secret: str) -> VerifiedBackup:
    if not isinstance(raw, (bytes, bytearray)):
        raise BackupValidationError("Файл резервной копии должен быть бинарным")
    if not raw:
        raise BackupValidationError("Файл резервной копии пуст")
    if len(raw) > MAX_BACKUP_BYTES:
        raise BackupValidationError(
            f"Файл резервной копии превышает {MAX_BACKUP_BYTES // 1024} КБ"
        )
    try:
        decoded = bytes(raw).decode("utf-8-sig")
    except UnicodeDecodeError as exc:
        raise BackupValidationError("Резервная копия должна быть UTF-8 JSON") from exc
    try:
        envelope = json.loads(decoded)
    except json.JSONDecodeError as exc:
        raise BackupValidationError("Файл не является корректным JSON") from exc
    if not isinstance(envelope, dict):
        raise BackupValidationError("Корень резервной копии должен быть объектом JSON")

    allowed_top = {
        "format",
        "schema_version",
        "created_at",
        "app_version",
        "payload",
        "integrity",
    }
    if set(envelope) != allowed_top:
        raise BackupValidationError("Некорректная структура резервной копии")
    if envelope.get("format") != BACKUP_FORMAT:
        raise BackupValidationError("Это не резервная копия конфигурации LikeBot")
    if envelope.get("schema_version") != BACKUP_SCHEMA_VERSION:
        raise BackupValidationError("Версия формата резервной копии не поддерживается")
    parse_utc_iso(envelope.get("created_at"), field="created_at")
    app_version = envelope.get("app_version")
    if not isinstance(app_version, str) or not app_version or len(app_version) > MAX_APP_VERSION_LENGTH:
        raise BackupValidationError("Некорректная версия приложения в резервной копии")

    integrity = envelope.get("integrity")
    if not isinstance(integrity, dict) or set(integrity) != {"algorithm", "digest"}:
        raise BackupValidationError("В резервной копии отсутствует корректная подпись")
    if integrity.get("algorithm") != BACKUP_INTEGRITY_ALGORITHM:
        raise BackupValidationError("Алгоритм подписи резервной копии не поддерживается")
    provided_digest = integrity.get("digest")
    if (
        not isinstance(provided_digest, str)
        or len(provided_digest) != 64
        or any(char not in "0123456789abcdef" for char in provided_digest)
    ):
        raise BackupValidationError("Некорректная подпись резервной копии")

    unsigned = {key: envelope[key] for key in allowed_top if key != "integrity"}
    expected_digest = hmac.new(
        _derive_signing_key(signing_secret), canonical_json(unsigned), hashlib.sha256
    ).hexdigest()
    if not hmac.compare_digest(provided_digest, expected_digest):
        raise BackupIntegrityError(
            "Подпись не совпала: файл изменён или создан с другим SESSION_ENCRYPTION_KEY"
        )

    payload = validate_payload(envelope.get("payload"))
    normalized_envelope = dict(envelope)
    normalized_envelope["payload"] = payload
    return VerifiedBackup(
        envelope=normalized_envelope,
        payload=payload,
        digest=provided_digest,
    )


def sanitize_source_name(value: Any) -> str | None:
    if value is None:
        return None
    cleaned = " ".join(str(value).replace("\x00", "").split()).strip()
    if not cleaned:
        return None
    return cleaned[:MAX_EVENT_SOURCE_LENGTH]


def mask_phone(phone: Any) -> str:
    text = str(phone or "").strip()
    if not text:
        return "не указан"
    digits = "".join(char for char in text if char.isdigit())
    if len(digits) <= 4:
        return "*" * max(1, len(digits))
    prefix = "+" if text.startswith("+") else ""
    return f"{prefix}{digits[:2]}{'*' * max(4, len(digits) - 4)}{digits[-2:]}"


def _strict_dict(value: Any, *, field: str, keys: set[str]) -> dict[str, Any]:
    if not isinstance(value, dict) or set(value) != keys:
        raise BackupValidationError(f"Некорректная структура поля {field}")
    return value


def _bounded_text(
    value: Any,
    *,
    field: str,
    maximum: int,
    allow_none: bool = False,
    allow_empty: bool = False,
) -> str | None:
    if value is None and allow_none:
        return None
    if not isinstance(value, str):
        raise BackupValidationError(f"Поле {field} должно быть строкой")
    if not value and not allow_empty:
        raise BackupValidationError(f"Поле {field} не может быть пустым")
    if len(value) > maximum:
        raise BackupValidationError(f"Поле {field} слишком длинное")
    return value


def _integer(
    value: Any, *, field: str, minimum: int, maximum: int, allow_none: bool = False
) -> int | None:
    if value is None and allow_none:
        return None
    if isinstance(value, bool) or not isinstance(value, int):
        raise BackupValidationError(f"Поле {field} должно быть целым числом")
    if not minimum <= value <= maximum:
        raise BackupValidationError(f"Поле {field} вне допустимого диапазона")
    return value


def _boolean(value: Any, *, field: str) -> bool:
    if not isinstance(value, bool):
        raise BackupValidationError(f"Поле {field} должно быть true/false")
    return value


def validate_reaction_weights(value: Any, *, field: str) -> dict[str, float]:
    if not isinstance(value, dict) or not value or len(value) > MAX_REACTIONS:
        raise BackupValidationError(f"Некорректный набор реакций в {field}")
    normalized: dict[str, float] = {}
    for raw_reaction, raw_weight in value.items():
        reaction = _bounded_text(
            raw_reaction, field=f"{field}.reaction", maximum=64
        )
        if isinstance(raw_weight, bool) or not isinstance(raw_weight, (int, float)):
            raise BackupValidationError(f"Некорректный вес реакции в {field}")
        weight = float(raw_weight)
        if not math.isfinite(weight) or weight < 0 or weight > 1_000_000:
            raise BackupValidationError(f"Некорректный вес реакции в {field}")
        normalized[str(reaction)] = weight
    if sum(normalized.values()) <= 0:
        raise BackupValidationError(f"Сумма весов реакций в {field} должна быть больше нуля")
    return normalized


def validate_payload(value: Any) -> dict[str, Any]:
    payload = _strict_dict(
        value,
        field="payload",
        keys={"global_settings", "accounts", "targets"},
    )

    global_settings = _strict_dict(
        payload["global_settings"],
        field="global_settings",
        keys={"reaction_weights", "reaction_delay", "membership_delay"},
    )
    reaction_delay = _strict_dict(
        global_settings["reaction_delay"],
        field="reaction_delay",
        keys={"min_seconds", "max_seconds"},
    )
    membership_delay = _strict_dict(
        global_settings["membership_delay"],
        field="membership_delay",
        keys={"min_seconds", "max_seconds"},
    )
    normalized_global = {
        "reaction_weights": validate_reaction_weights(
            global_settings["reaction_weights"], field="global_settings.reaction_weights"
        ),
        "reaction_delay": {
            "min_seconds": _integer(
                reaction_delay["min_seconds"],
                field="reaction_delay.min_seconds",
                minimum=1,
                maximum=86400,
            ),
            "max_seconds": _integer(
                reaction_delay["max_seconds"],
                field="reaction_delay.max_seconds",
                minimum=1,
                maximum=86400,
            ),
        },
        "membership_delay": {
            "min_seconds": _integer(
                membership_delay["min_seconds"],
                field="membership_delay.min_seconds",
                minimum=1,
                maximum=86400,
            ),
            "max_seconds": _integer(
                membership_delay["max_seconds"],
                field="membership_delay.max_seconds",
                minimum=1,
                maximum=86400,
            ),
        },
    }
    for key in ("reaction_delay", "membership_delay"):
        if normalized_global[key]["min_seconds"] > normalized_global[key]["max_seconds"]:
            raise BackupValidationError(f"Минимум больше максимума в {key}")

    accounts = payload["accounts"]
    if not isinstance(accounts, list) or len(accounts) > MAX_BACKUP_ACCOUNTS:
        raise BackupValidationError("Некорректный список аккаунтов")
    normalized_accounts: list[dict[str, Any]] = []
    account_ids: set[int] = set()
    for index, item in enumerate(accounts):
        row = _strict_dict(
            item,
            field=f"accounts[{index}]",
            keys={
                "telegram_user_id",
                "display_name",
                "username",
                "phone_masked",
                "is_active",
                "status",
            },
        )
        telegram_user_id = _integer(
            row["telegram_user_id"],
            field=f"accounts[{index}].telegram_user_id",
            minimum=1,
            maximum=9_223_372_036_854_775_807,
        )
        if telegram_user_id in account_ids:
            raise BackupValidationError("В резервной копии повторяется Telegram ID аккаунта")
        account_ids.add(int(telegram_user_id))
        normalized_accounts.append(
            {
                "telegram_user_id": telegram_user_id,
                "display_name": _bounded_text(
                    row["display_name"],
                    field=f"accounts[{index}].display_name",
                    maximum=255,
                ),
                "username": _bounded_text(
                    row["username"],
                    field=f"accounts[{index}].username",
                    maximum=64,
                    allow_none=True,
                ),
                "phone_masked": _bounded_text(
                    row["phone_masked"],
                    field=f"accounts[{index}].phone_masked",
                    maximum=40,
                ),
                "is_active": _boolean(
                    row["is_active"], field=f"accounts[{index}].is_active"
                ),
                "status": _bounded_text(
                    row["status"], field=f"accounts[{index}].status", maximum=32
                ),
            }
        )

    targets = payload["targets"]
    if not isinstance(targets, list) or len(targets) > MAX_BACKUP_TARGETS:
        raise BackupValidationError("Некорректный список каналов и групп")
    normalized_targets: list[dict[str, Any]] = []
    target_ids: set[int] = set()
    for index, item in enumerate(targets):
        row = _strict_dict(
            item,
            field=f"targets[{index}]",
            keys={"telegram_channel_id", "kind", "title", "username", "settings"},
        )
        telegram_channel_id = _integer(
            row["telegram_channel_id"],
            field=f"targets[{index}].telegram_channel_id",
            minimum=-9_223_372_036_854_775_808,
            maximum=9_223_372_036_854_775_807,
        )
        if telegram_channel_id == 0:
            raise BackupValidationError("Telegram ID канала не может быть нулём")
        if telegram_channel_id in target_ids:
            raise BackupValidationError("В резервной копии повторяется Telegram ID канала")
        target_ids.add(int(telegram_channel_id))
        kind = _bounded_text(row["kind"], field=f"targets[{index}].kind", maximum=16)
        if kind not in {"channel", "group"}:
            raise BackupValidationError("Неизвестный тип канала в резервной копии")
        settings = _strict_dict(
            row["settings"],
            field=f"targets[{index}].settings",
            keys=set(CHANNEL_CONFIGURATION_FIELDS),
        )
        reaction_weights = settings["reaction_weights"]
        if reaction_weights is not None:
            reaction_weights = validate_reaction_weights(
                reaction_weights,
                field=f"targets[{index}].settings.reaction_weights",
            )
        minimum_window = _integer(
            settings["reaction_window_min_seconds"],
            field=f"targets[{index}].settings.reaction_window_min_seconds",
            minimum=0,
            maximum=7 * 86400,
        )
        maximum_window = _integer(
            settings["reaction_window_max_seconds"],
            field=f"targets[{index}].settings.reaction_window_max_seconds",
            minimum=0,
            maximum=7 * 86400,
        )
        if minimum_window > maximum_window:
            raise BackupValidationError("Минимальный период канала больше максимального")
        promotion_mode = _bounded_text(
            settings["promotion_mode"],
            field=f"targets[{index}].settings.promotion_mode",
            maximum=16,
        )
        if promotion_mode not in {"permanent", "timed"}:
            raise BackupValidationError("Некорректный режим периода раскрутки")
        promotion_started_at = parse_utc_iso(
            settings["promotion_started_at"],
            field=f"targets[{index}].settings.promotion_started_at",
            allow_none=True,
        )
        promotion_until = parse_utc_iso(
            settings["promotion_until"],
            field=f"targets[{index}].settings.promotion_until",
            allow_none=True,
        )
        if promotion_mode == "timed" and promotion_until is None:
            raise BackupValidationError("У временного периода отсутствует дата окончания")
        if promotion_mode == "permanent" and promotion_until is not None:
            raise BackupValidationError("У постоянного периода не должно быть даты окончания")
        if (
            promotion_started_at is not None
            and promotion_until is not None
            and promotion_until < promotion_started_at
        ):
            raise BackupValidationError("Окончание периода не может быть раньше начала")
        profile_key = _bounded_text(
            settings["profile_key"],
            field=f"targets[{index}].settings.profile_key",
            maximum=24,
        )
        if profile_key not in {"custom", "cautious", "normal", "active"}:
            raise BackupValidationError("Некорректный ключ профиля канала")
        if kind == "group" and profile_key != "custom":
            raise BackupValidationError("Группа не может использовать профиль обычного канала")

        normalized_targets.append(
            {
                "telegram_channel_id": telegram_channel_id,
                "kind": kind,
                "title": _bounded_text(
                    row["title"], field=f"targets[{index}].title", maximum=255
                ),
                "username": _bounded_text(
                    row["username"],
                    field=f"targets[{index}].username",
                    maximum=64,
                    allow_none=True,
                ),
                "settings": {
                    "is_active": _boolean(
                        settings["is_active"],
                        field=f"targets[{index}].settings.is_active",
                    ),
                    "new_posts_enabled": _boolean(
                        settings["new_posts_enabled"],
                        field=f"targets[{index}].settings.new_posts_enabled",
                    ),
                    "old_posts_enabled": _boolean(
                        settings["old_posts_enabled"],
                        field=f"targets[{index}].settings.old_posts_enabled",
                    ),
                    "old_posts_depth": _integer(
                        settings["old_posts_depth"],
                        field=f"targets[{index}].settings.old_posts_depth",
                        minimum=1,
                        maximum=10_000,
                    ),
                    "reaction_weights": reaction_weights,
                    "max_reactions_per_post": _integer(
                        settings["max_reactions_per_post"],
                        field=f"targets[{index}].settings.max_reactions_per_post",
                        minimum=1,
                        maximum=1_000_000,
                        allow_none=True,
                    ),
                    "reaction_window_min_seconds": minimum_window,
                    "reaction_window_max_seconds": maximum_window,
                    "image_post_reaction_percent": _integer(
                        settings["image_post_reaction_percent"],
                        field=f"targets[{index}].settings.image_post_reaction_percent",
                        minimum=0,
                        maximum=100,
                    ),
                    "no_image_post_reaction_percent": _integer(
                        settings["no_image_post_reaction_percent"],
                        field=f"targets[{index}].settings.no_image_post_reaction_percent",
                        minimum=0,
                        maximum=100,
                    ),
                    "promotion_mode": promotion_mode,
                    "promotion_started_at": (
                        utc_iso(promotion_started_at.replace(tzinfo=UTC))
                        if promotion_started_at is not None
                        else None
                    ),
                    "promotion_until": (
                        utc_iso(promotion_until.replace(tzinfo=UTC))
                        if promotion_until is not None
                        else None
                    ),
                    "profile_key": profile_key,
                },
            }
        )

    normalized_accounts.sort(key=lambda item: int(item["telegram_user_id"]))
    normalized_targets.sort(key=lambda item: int(item["telegram_channel_id"]))
    return {
        "global_settings": normalized_global,
        "accounts": normalized_accounts,
        "targets": normalized_targets,
    }
