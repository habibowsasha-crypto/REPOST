from __future__ import annotations

import json
import logging
import math
from collections.abc import Mapping
from datetime import datetime, timedelta
from random import SystemRandom
from typing import Iterable

from sqlalchemy import (
    BigInteger,
    and_,
    case,
    delete,
    desc,
    func,
    inspect,
    literal,
    or_,
    select,
    text,
    union_all,
    update,
)
from sqlalchemy.dialects.postgresql import insert as postgresql_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.exc import NoSuchTableError
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine, AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import selectinload

from .analytics import resolve_analytics_period
from .backup import (
    MAX_BACKUP_BYTES,
    BackupValidationError,
    canonical_json,
    mask_phone,
    parse_utc_iso,
    payload_sha256,
    sanitize_source_name,
    utc_iso,
    validate_payload,
)
from .channel_profiles import (
    CUSTOM_PROFILE_KEY,
    ResolvedChannelProfile,
    channel_profile_setting_key,
    detect_channel_profile_key,
    validate_resolved_channel_profile,
)
from .models import (
    Account,
    AppSetting,
    Base,
    Channel,
    ConfigurationEvent,
    JobHistoryChannelSummary,
    JobHistoryKey,
    JobHistorySummary,
    JoinJob,
    ReactionJob,
    ViewBatch,
    ViewJob,
    utcnow,
)
from .selection import AccountWorkload
from .utils import choose_weighted_reaction, normalize_email_login

_rng = SystemRandom()
logger = logging.getLogger("laika_bot.db")


class AccountIdentityConflictError(RuntimeError):
    """The phone number and Telegram user id point to different stored identities."""

    def __init__(
        self,
        *,
        phone: str,
        stored_account_id: int | None,
        stored_telegram_user_id: int | None,
        incoming_telegram_user_id: int,
    ) -> None:
        super().__init__(
            "Новая Telegram-личность не совпадает с сохранённой записью этого номера"
        )
        self.phone = phone
        self.stored_account_id = stored_account_id
        self.stored_telegram_user_id = stored_telegram_user_id
        self.incoming_telegram_user_id = incoming_telegram_user_id


INSTANCE_ADVISORY_LOCK_KEY = 0x4C41494B41
EMAIL_NOTE_MAX_LENGTH = 500
CONFIGURATION_EVENT_RETENTION = 30


def _public_inspect(subject):
    """Resolve SQLAlchemy inspect through the public db facade for patch compatibility."""

    import sys

    facade = sys.modules.get("laika_bot.db")
    inspector = getattr(facade, "inspect", inspect) if facade is not None else inspect
    return inspector(subject)

# Export private SQL helpers/constants to the deliberately split database mixins.
__all__ = [
    name
    for name in globals()
    if not name.startswith('__') and name != '_public_inspect'
]
