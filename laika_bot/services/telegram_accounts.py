from __future__ import annotations

import asyncio
import logging
import math
import re
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import datetime, timezone

from cryptography.fernet import Fernet, InvalidToken
from telethon import TelegramClient, errors, functions, types
from telethon.sessions import StringSession

from ..config import Settings
from ..db import Database
from ..models import Account, Channel
from ..utils import TELEGRAM_SERVICE_USER_ID, ParsedChannelLink, extract_login_code

logger = logging.getLogger(__name__)

PERMANENT_AUTH_ERRORS = (
    errors.AuthKeyUnregisteredError,
    errors.SessionRevokedError,
    errors.UserDeactivatedError,
    errors.UserDeactivatedBanError,
    errors.AuthKeyDuplicatedError,
    errors.AuthKeyInvalidError,
)


class AccountSessionUnauthorizedError(RuntimeError):
    """The saved MTProto session is permanently unusable and must be quarantined."""

    def __init__(self, account_id: int, message: str = "Сессия аккаунта больше не авторизована") -> None:
        super().__init__(message)
        self.account_id = account_id


class TargetNotVisibleError(RuntimeError):
    """The session is valid but cannot resolve a private target from dialogs."""


ACCOUNT_AUTH_FAILURES = (AccountSessionUnauthorizedError,) + PERMANENT_AUTH_ERRORS


@dataclass(frozen=True)
class ChannelJoinResult:
    entity: object
    joined_now: bool


@dataclass(frozen=True)
class LoginCodeHit:
    """A recent one-time login code found in Telegram service messages.

    The code value must never be logged or written to the database.
    """

    message_id: int
    code: str
    received_at: datetime
    preview: str


class SessionCipher:
    def __init__(self, key: str) -> None:
        try:
            self._fernet = Fernet(key.encode("utf-8"))
        except Exception as exc:  # noqa: BLE001
            raise ValueError("SESSION_ENCRYPTION_KEY должен быть валидным ключом Fernet") from exc

    def encrypt(self, session: str) -> str:
        return self._fernet.encrypt(session.encode("utf-8")).decode("utf-8")

    def decrypt(self, token: str) -> str:
        try:
            return self._fernet.decrypt(token.encode("utf-8")).decode("utf-8")
        except InvalidToken as exc:
            raise ValueError("Не удалось расшифровать Telegram-сессию") from exc


@dataclass
class PendingLogin:
    client: TelegramClient
    phone: str
    phone_code_hash: str
    delivery_text: str
    next_delivery_text: str | None
    resend_available_at: float


def code_delivery_text(code_type: object) -> str:
    """Return a user-facing description without exposing secrets."""

    mapping: tuple[tuple[type, str], ...] = (
        (types.auth.SentCodeTypeApp, "в приложение Telegram на подключаемом аккаунте"),
        (types.auth.SentCodeTypeSms, "по SMS"),
        (types.auth.SentCodeTypeFirebaseSms, "по SMS через системную службу телефона"),
        (types.auth.SentCodeTypeCall, "автоматическим телефонным звонком"),
        (types.auth.SentCodeTypeFlashCall, "коротким входящим звонком"),
        (types.auth.SentCodeTypeMissedCall, "пропущенным звонком"),
        (types.auth.SentCodeTypeFragmentSms, "через Fragment SMS"),
        (types.auth.SentCodeTypeEmailCode, "на привязанную электронную почту"),
        (types.auth.SentCodeTypeSetUpEmailRequired, "после настройки электронной почты в Telegram"),
        (types.auth.SentCodeTypeSmsPhrase, "по SMS в виде кодовой фразы"),
        (types.auth.SentCodeTypeSmsWord, "по SMS в виде кодового слова"),
    )
    for cls, label in mapping:
        if isinstance(code_type, cls):
            return label
    return "способом, выбранным Telegram"


def next_code_delivery_text(code_type: object | None) -> str | None:
    if code_type is None:
        return None
    mapping: tuple[tuple[type, str], ...] = (
        (types.auth.CodeTypeSms, "SMS"),
        (types.auth.CodeTypeCall, "телефонный звонок"),
        (types.auth.CodeTypeFlashCall, "короткий входящий звонок"),
        (types.auth.CodeTypeMissedCall, "пропущенный звонок"),
        (types.auth.CodeTypeFragmentSms, "Fragment SMS"),
    )
    for cls, label in mapping:
        if isinstance(code_type, cls):
            return label
    return "другой доступный способ"


class LoginManager:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.pending: dict[int, PendingLogin] = {}
        self._locks: dict[int, asyncio.Lock] = {}

    def _lock_for(self, admin_id: int) -> asyncio.Lock:
        return self._locks.setdefault(int(admin_id), asyncio.Lock())

    @staticmethod
    async def _disconnect_login_client(
        client: TelegramClient, *, context: str
    ) -> None:
        """Best-effort disconnect without leaving a stale login in memory."""

        try:
            await client.disconnect()
        except asyncio.CancelledError:
            raise
        except Exception:  # noqa: BLE001
            logger.exception(
                "Failed to disconnect Telegram login client context=%s", context
            )

    async def _cancel_unlocked(self, admin_id: int) -> bool:
        login = self.pending.pop(admin_id, None)
        if login is None:
            return False
        await self._disconnect_login_client(login.client, context="cancel")
        return True

    async def start(self, admin_id: int, phone: str) -> PendingLogin:
        async with self._lock_for(admin_id):
            await self._cancel_unlocked(admin_id)
            client = TelegramClient(
                StringSession(), self.settings.api_id, self.settings.api_hash
            )
            try:
                await client.connect()
                sent = await client.send_code_request(phone)
            except BaseException:
                # A failed/cancelled connect must not leave an unattached MTProto
                # transport alive. Never replace the original failure with a
                # secondary disconnect error.
                try:
                    await client.disconnect()
                except BaseException:  # noqa: BLE001
                    logger.exception(
                        "Failed to disconnect unsuccessful Telegram login client"
                    )
                raise
            timeout = max(int(getattr(sent, "timeout", None) or 60), 1)
            login = PendingLogin(
                client=client,
                phone=phone,
                phone_code_hash=sent.phone_code_hash,
                delivery_text=code_delivery_text(sent.type),
                next_delivery_text=next_code_delivery_text(
                    getattr(sent, "next_type", None)
                ),
                resend_available_at=time.monotonic() + timeout,
            )
            self.pending[admin_id] = login
            logger.info(
                "Telegram login code requested admin_id=%s delivery=%s next=%s timeout=%s",
                admin_id,
                type(sent.type).__name__,
                type(sent.next_type).__name__
                if getattr(sent, "next_type", None)
                else None,
                timeout,
            )
            return login

    async def resend(self, admin_id: int) -> tuple[PendingLogin, int]:
        async with self._lock_for(admin_id):
            login = self.get(admin_id)
            if login is None:
                raise RuntimeError("Сессия авторизации не найдена")

            remaining = math.ceil(login.resend_available_at - time.monotonic())
            if remaining > 0:
                return login, remaining

            sent = await login.client.send_code_request(login.phone)
            timeout = max(int(getattr(sent, "timeout", None) or 60), 1)
            login.phone_code_hash = sent.phone_code_hash or login.phone_code_hash
            login.delivery_text = code_delivery_text(sent.type)
            login.next_delivery_text = next_code_delivery_text(
                getattr(sent, "next_type", None)
            )
            login.resend_available_at = time.monotonic() + timeout
            logger.info(
                "Telegram login code resent admin_id=%s delivery=%s next=%s timeout=%s",
                admin_id,
                type(sent.type).__name__,
                type(sent.next_type).__name__
                if getattr(sent, "next_type", None)
                else None,
                timeout,
            )
            return login, 0

    def get(self, admin_id: int) -> PendingLogin | None:
        return self.pending.get(admin_id)

    async def cancel(self, admin_id: int) -> None:
        async with self._lock_for(admin_id):
            await self._cancel_unlocked(admin_id)

    async def cancel_if_phone(self, admin_id: int, phone: str) -> bool:
        """Cancel only the login attempt that still belongs to one account."""

        async with self._lock_for(admin_id):
            login = self.pending.get(admin_id)
            if login is None or login.phone != phone:
                return False
            return await self._cancel_unlocked(admin_id)

    async def revoke_and_cancel(self, admin_id: int) -> bool:
        """Revoke a newly authorized temporary session that was not persisted."""

        async with self._lock_for(admin_id):
            login = self.pending.pop(admin_id, None)
            if login is None:
                return True
            revoked = False
            try:
                await login.client.log_out()
                revoked = True
                logger.info(
                    "Unpersisted temporary Telegram session revoked admin_id=%s",
                    admin_id,
                )
            except asyncio.CancelledError:
                raise
            except Exception:  # noqa: BLE001
                logger.exception(
                    "Failed to revoke unpersisted temporary Telegram session admin_id=%s",
                    admin_id,
                )
            finally:
                await self._disconnect_login_client(
                    login.client, context="revoke-and-cancel"
                )
            return revoked

    async def finish(self, admin_id: int) -> None:
        await self.cancel(admin_id)

    async def close(self) -> None:
        """Disconnect every unfinished login attempt during application shutdown."""

        for admin_id in list(self.pending):
            await self.cancel(admin_id)


class AccountActionLock:
    """A stable non-reentrant asyncio lock that tracks its owning task.

    ``asyncio.Lock.locked()`` only proves that *some* task owns the lock. Runtime
    methods with a ``*_while_locked`` contract need to prove that the current task
    is the owner, otherwise another task could disconnect a live Telegram client.
    """

    def __init__(self) -> None:
        self._lock = asyncio.Lock()
        self._owner: asyncio.Task | None = None

    async def acquire(self) -> bool:
        current = asyncio.current_task()
        if current is None:
            raise RuntimeError("account action lock requires an asyncio task")
        if self._owner is current:
            raise RuntimeError("account action lock is not re-entrant")
        await self._lock.acquire()
        self._owner = current
        return True

    def release(self) -> None:
        current = asyncio.current_task()
        if self._owner is not current:
            raise RuntimeError("account action lock can only be released by its owner")
        self._owner = None
        self._lock.release()

    def locked(self) -> bool:
        return self._lock.locked()

    def owned_by_current_task(self) -> bool:
        return self._owner is asyncio.current_task()

    async def __aenter__(self):
        await self.acquire()
        return self

    async def __aexit__(self, exc_type, exc, tb):
        self.release()
        return False


class ClientPool:
    def __init__(self, settings: Settings, db: Database, cipher: SessionCipher) -> None:
        self.settings = settings
        self.db = db
        self.cipher = cipher
        self._clients: dict[int, TelegramClient] = {}
        self._locks: dict[int, AccountActionLock] = {}
        self._problem_notifier: Callable[[Account, str, str], Awaitable[None]] | None = None

    def set_problem_notifier(
        self, notifier: Callable[[Account, str, str], Awaitable[None]] | None
    ) -> None:
        self._problem_notifier = notifier

    def lock_for(self, account_id: int) -> AccountActionLock:
        return self._locks.setdefault(account_id, AccountActionLock())

    async def get(self, account: Account) -> TelegramClient:
        client = self._clients.get(account.id)
        if client and client.is_connected():
            return client
        if client:
            self._clients.pop(account.id, None)
            await self._disconnect_detached_client(
                client, account_id=account.id, context="replace-disconnected"
            )
        session = self.cipher.decrypt(account.session_encrypted)
        client = TelegramClient(StringSession(session), self.settings.api_id, self.settings.api_hash)
        try:
            await client.connect()
            if not await client.is_user_authorized():
                raise AccountSessionUnauthorizedError(account.id)
            me = await client.get_me()
            if me is None or int(me.id) != int(account.telegram_user_id):
                raise AccountSessionUnauthorizedError(
                    account.id,
                    "Сохранённая сессия принадлежит другому Telegram-аккаунту",
                )
        except AccountSessionUnauthorizedError:
            await self._disconnect_after_failed_connect(client, account.id)
            raise
        except PERMANENT_AUTH_ERRORS as exc:
            await self._disconnect_after_failed_connect(client, account.id)
            raise AccountSessionUnauthorizedError(account.id) from exc
        except BaseException:
            await self._disconnect_after_failed_connect(client, account.id)
            raise
        self._clients[account.id] = client
        return client

    @staticmethod
    async def _disconnect_after_failed_connect(
        client: TelegramClient, account_id: int
    ) -> None:
        """Clean up a never-cached client while preserving the original failure."""

        try:
            await client.disconnect()
        except BaseException:  # noqa: BLE001
            logger.exception(
                "Failed to disconnect unsuccessful Telegram client account=%s",
                account_id,
            )

    @staticmethod
    async def _disconnect_detached_client(
        client: TelegramClient, *, account_id: int, context: str
    ) -> None:
        """Disconnect an already detached client without blocking DB safety work."""

        try:
            await client.disconnect()
        except asyncio.CancelledError:
            raise
        except Exception:  # noqa: BLE001
            logger.exception(
                "Failed to disconnect detached Telegram client account=%s context=%s",
                account_id,
                context,
            )

    async def ensure_authorized(self, account: Account) -> TelegramClient:
        """Validate a saved session without deleting it on transient network failures."""

        client = await self.get(account)
        try:
            if not await client.is_user_authorized():
                raise AccountSessionUnauthorizedError(account.id)
        except AccountSessionUnauthorizedError:
            raise
        except PERMANENT_AUTH_ERRORS as exc:
            raise AccountSessionUnauthorizedError(account.id) from exc
        return client

    async def remove_unauthorized_account(self, account_id: int, *, context: str) -> bool:
        """Serialize client removal and quarantine with every account action.

        Callers that already own ``lock_for(account_id)`` must use
        :meth:`remove_unauthorized_account_while_locked` to avoid a non-reentrant
        lock deadlock.
        """

        async with self.lock_for(account_id):
            return await self.remove_unauthorized_account_while_locked(
                account_id, context=context
            )

    async def remove_unauthorized_account_while_locked(
        self, account_id: int, *, context: str
    ) -> bool:
        """Drop and quarantine while the caller owns the stable account lock."""

        if not self.lock_for(account_id).owned_by_current_task():
            raise RuntimeError(
                "remove_unauthorized_account_while_locked requires lock ownership"
            )
        # Disconnect first: even if the database is temporarily unavailable, an
        # invalid cached session must not remain available to the next worker.
        await self.drop_while_locked(account_id)
        account = await self.db.get_account(account_id)
        reason = "Сессия аккаунта больше не авторизована"
        quarantined = await self.db.quarantine_invalid_account(
            account_id,
            context=context,
            reason=reason,
        )
        if quarantined:
            logger.warning(
                "Unauthorized Telegram account quarantined account=%s context=%s",
                account_id,
                context,
            )
            if account is not None and self._problem_notifier is not None:
                try:
                    await self._problem_notifier(account, context, reason)
                except Exception:  # noqa: BLE001
                    logger.exception(
                        "Failed to notify administrator about problem account=%s",
                        account_id,
                    )
        return quarantined

    def has_connected_client(self, account_id: int) -> bool:
        client = self._clients.get(account_id)
        return bool(client and client.is_connected())

    async def disconnect_client(self, account_id: int) -> None:
        client = self._clients.pop(account_id, None)
        if client:
            await self._disconnect_detached_client(
                client, account_id=account_id, context="pool-disconnect"
            )

    async def drop(self, account_id: int) -> None:
        """Disconnect one account only after its current Telegram action finishes."""

        async with self.lock_for(account_id):
            await self.drop_while_locked(account_id)

    async def drop_while_locked(self, account_id: int) -> None:
        if not self.lock_for(account_id).owned_by_current_task():
            raise RuntimeError("drop_while_locked requires lock ownership")
        await self.disconnect_client(account_id)
        # Keep the per-account lock object stable. Replacing it while another
        # worker is waiting on the old lock could allow concurrent actions for
        # the same account after a session reset/quarantine.

    async def close(self) -> None:
        clients = list(self._clients.values())
        self._clients.clear()
        for client in clients:
            try:
                await client.disconnect()
            except Exception:  # noqa: BLE001
                logger.exception("Ошибка отключения Telegram-клиента")

    async def resolve_channel(self, client: TelegramClient, channel: Channel):
        if channel.username:
            return await client.get_entity(channel.username)
        async for dialog in client.iter_dialogs():
            entity = dialog.entity
            if isinstance(entity, types.Channel) and int(entity.id) == channel.telegram_channel_id:
                return entity
        raise TargetNotVisibleError("Аккаунт не видит приватный канал")

    async def join_channel(self, client: TelegramClient, parsed: ParsedChannelLink) -> ChannelJoinResult:
        if parsed.kind == "private":
            invite = await client(functions.messages.CheckChatInviteRequest(hash=parsed.value))
            if isinstance(invite, types.ChatInviteAlready):
                return ChannelJoinResult(entity=invite.chat, joined_now=False)
            try:
                result = await client(functions.messages.ImportChatInviteRequest(hash=parsed.value))
            except errors.UserAlreadyParticipantError:
                # The membership may have changed between check and import. Resolve it again.
                invite = await client(functions.messages.CheckChatInviteRequest(hash=parsed.value))
                if isinstance(invite, types.ChatInviteAlready):
                    return ChannelJoinResult(entity=invite.chat, joined_now=False)
                raise
            chats = getattr(result, "chats", None) or []
            entity = chats[0] if chats else None
            if entity is None:
                raise RuntimeError("Telegram не вернул данные приватного канала или группы")
            return ChannelJoinResult(entity=entity, joined_now=True)
        entity = await client.get_entity(parsed.value)
        try:
            await client(functions.channels.JoinChannelRequest(channel=entity))
            joined_now = True
        except errors.UserAlreadyParticipantError:
            joined_now = False
        return ChannelJoinResult(entity=entity, joined_now=joined_now)


    async def fetch_recent_login_code(
        self,
        account: Account,
        *,
        max_age_seconds: int = 600,
        exclude_message_ids: set[int] | None = None,
        limit: int = 25,
        quarantine_context: str | None = None,
    ) -> LoginCodeHit | None:
        """Read recent login-code notices from the official Telegram service chat.

        Only messages from user 777000 that look like one-time login codes and
        are younger than ``max_age_seconds`` are considered. Already shown
        message ids can be excluded so the same code is not re-issued.
        """

        excluded = exclude_message_ids or set()
        async with self.lock_for(account.id):
            try:
                client = await self.get(account)
                try:
                    if not await client.is_user_authorized():
                        raise AccountSessionUnauthorizedError(account.id)
                except AccountSessionUnauthorizedError:
                    raise
                except PERMANENT_AUTH_ERRORS as exc:
                    raise AccountSessionUnauthorizedError(account.id) from exc

                try:
                    messages = await client.get_messages(
                        TELEGRAM_SERVICE_USER_ID, limit=limit
                    )
                except PERMANENT_AUTH_ERRORS as exc:
                    raise AccountSessionUnauthorizedError(account.id) from exc
            except ACCOUNT_AUTH_FAILURES:
                if quarantine_context is not None:
                    await self.remove_unauthorized_account_while_locked(
                        account.id, context=quarantine_context
                    )
                raise

        now = datetime.now(timezone.utc)
        hits: list[LoginCodeHit] = []
        for message in messages or []:
            message_id = int(getattr(message, "id", 0) or 0)
            if message_id <= 0 or message_id in excluded:
                continue
            body = getattr(message, "message", None) or getattr(message, "raw_text", None)
            code = extract_login_code(body)
            if not code:
                continue
            received = getattr(message, "date", None)
            if received is None:
                continue
            if received.tzinfo is None:
                received_aware = received.replace(tzinfo=timezone.utc)
            else:
                received_aware = received.astimezone(timezone.utc)
            age = (now - received_aware).total_seconds()
            if age < 0:
                age = 0
            if age > max_age_seconds:
                continue
            # Redacted preview for UI only — never includes the real digits.
            preview = re.sub(r"\d{5,6}", "•••••", " ".join(str(body).split()))
            if len(preview) > 160:
                preview = preview[:159] + "…"
            hits.append(
                LoginCodeHit(
                    message_id=message_id,
                    code=code,
                    received_at=received_aware.replace(tzinfo=None),
                    preview=preview,
                )
            )

        if not hits:
            return None
        hits.sort(key=lambda item: item.received_at, reverse=True)
        return hits[0]

    async def leave_channel(self, client: TelegramClient, channel: Channel) -> None:
        try:
            entity = await self.resolve_channel(client, channel)
        except TargetNotVisibleError:
            # If the dialog is already absent, the account is effectively unsubscribed.
            return
        await client(functions.channels.LeaveChannelRequest(channel=entity))
