"""v1.0.81 keeps real dialogs independent from First DM cooldowns."""

from __future__ import annotations

import asyncio
import datetime as dt
from types import SimpleNamespace


def _seed_account(accounts, user_id: int):
    accounts.upsert_account(
        user_id=user_id,
        session_string=f"session-{user_id}",
        username=f"sender{user_id}",
    )
    accounts.set_participates(user_id, True)
    return accounts.get_account(user_id)


def _pause_first_dm(account: int, *, seconds: int = 300) -> None:
    from db.schema import db_lock, get_connection

    until = dt.datetime.now(dt.timezone.utc) + dt.timedelta(seconds=seconds)
    with db_lock(), get_connection() as conn:
        conn.execute(
            """
            UPDATE accounts
               SET is_paused=1, pause_reason='PeerFlood', cooldown_until=?
             WHERE user_id=?
            """,
            (until.isoformat(), int(account)),
        )


class _Client:
    def __init__(self, error: BaseException | None = None):
        self.error = error
        self.sent: list[str] = []

    def is_connected(self):
        return True

    async def get_input_entity(self, value):
        return value

    async def send_message(self, entity, text):
        self.sent.append(str(text))
        if self.error is not None:
            raise self.error
        return SimpleNamespace(
            id=81000 + len(self.sent),
            date=dt.datetime.now(dt.timezone.utc),
        )


async def _none(*args, **kwargs):
    return None


def _patch_common(monkeypatch, client: _Client) -> None:
    from services import dialog_engine

    monkeypatch.setattr(dialog_engine.monitor_svc, "get_client", lambda uid: client)
    monkeypatch.setattr(
        dialog_engine.monitor_svc,
        "maybe_disconnect_inactive_account",
        _none,
    )
    monkeypatch.setattr(dialog_engine, "_delay_reply", lambda: 0.0)
    monkeypatch.setattr(dialog_engine, "_auto_link_delay", lambda: 60)


def test_real_incoming_reply_sends_during_peerflood_first_dm_pause(app_env, monkeypatch):
    from services import accounts, ai_dialog, dialog_engine, dialog_inbox, dialog_store

    account, target = 108101, 180101
    _seed_account(accounts, account)
    dialog_store.create_after_first_dm(target, account, "Привет, можно вопрос?")
    _pause_first_dm(account)
    client = _Client()
    _patch_common(monkeypatch, client)

    async def classify(*args, **kwargs):
        return ai_dialog.CATEGORY_NORMAL

    async def promo(*args, **kwargs):
        return "Вот канал: https://t.me/+testhash"

    monkeypatch.setattr(ai_dialog, "classify_user_message", classify)
    monkeypatch.setattr(ai_dialog, "generate_promo", promo)

    asyncio.run(
        dialog_engine.handle_incoming_private(
            account,
            target,
            "Да, слушаю",
            telegram_message_id=18010101,
        )
    )

    assert client.sent == ["Вот канал: https://t.me/+testhash"]
    assert dialog_inbox.count_by_status(dialog_inbox.STATUS_DONE) == 1
    assert dialog_store.get_dialog(target)["stage"] == dialog_store.STAGE_PROMO_SENT
    row = accounts.get_account(account)
    assert row["is_paused"] == 1
    assert row["pause_reason"] == "PeerFlood"


def test_post_reply_scheduled_step_sends_during_first_dm_pause(app_env, monkeypatch):
    from services import accounts, ai_dialog, dialog_engine, dialog_store

    account, target = 108102, 180102
    _seed_account(accounts, account)
    dialog_store.create_after_first_dm(target, account, "Привет, можно вопрос?")
    dialog_store.append_history(target, "user", "Да")
    past = dt.datetime.now(dt.timezone.utc) - dt.timedelta(seconds=1)
    dialog_store.set_stage(
        target,
        dialog_store.STAGE_PROMO_SENT,
        bump_outgoing=True,
        link_sent=True,
        auto_link_at=past.isoformat(),
    )
    _pause_first_dm(account)
    client = _Client()
    _patch_common(monkeypatch, client)

    async def apology(*args, **kwargs):
        return "Сорян, не хотел навязываться."

    monkeypatch.setattr(ai_dialog, "generate_smoothing_apology", apology)

    assert asyncio.run(dialog_engine.process_due_auto_links()) == 1
    assert client.sent == ["Сорян, не хотел навязываться."]
    assert dialog_store.get_dialog(target)["stage"] == dialog_store.STAGE_APOLOGY_SENT
    assert accounts.get_account(account)["is_paused"] == 1


def test_silence_followup_remains_blocked_by_first_dm_pause(app_env, monkeypatch):
    from services import accounts, dialog_engine, dialog_store, runtime

    account, target = 108103, 180103
    _seed_account(accounts, account)
    dialog_store.create_after_first_dm(target, account, "Привет, можно вопрос?")
    past = dt.datetime.now(dt.timezone.utc) - dt.timedelta(seconds=1)
    dialog_store.set_stage(
        target,
        dialog_store.STAGE_WAITING_REPLY,
        auto_link_at=past.isoformat(),
    )
    _pause_first_dm(account)
    runtime.set_worker_enabled(True)
    client = _Client()
    _patch_common(monkeypatch, client)

    assert asyncio.run(dialog_engine.process_due_followups()) == 0
    assert client.sent == []
    assert dialog_store.get_dialog(target)["stage"] == dialog_store.STAGE_WAITING_REPLY


def test_dialog_peerflood_retries_only_that_inbox_and_not_all_dialogs(
    app_env, monkeypatch
):
    from db.schema import db_lock, get_connection
    from services import accounts, ai_dialog, dialog_delivery, dialog_engine
    from services import dialog_inbox, dialog_store, spambot

    class FakePeerFlood(Exception):
        pass

    account, target = 108104, 180104
    _seed_account(accounts, account)
    dialog_store.create_after_first_dm(target, account, "Привет, можно вопрос?")
    client = _Client(FakePeerFlood("peer flood"))
    _patch_common(monkeypatch, client)
    monkeypatch.setattr(dialog_engine, "PeerFloodError", FakePeerFlood)

    async def classify(*args, **kwargs):
        return ai_dialog.CATEGORY_NORMAL

    async def promo(*args, **kwargs):
        return "Вот канал: https://t.me/+testhash"

    async def peerflood(account_user_id: int):
        _pause_first_dm(account_user_id)

    monkeypatch.setattr(ai_dialog, "classify_user_message", classify)
    monkeypatch.setattr(ai_dialog, "generate_promo", promo)
    monkeypatch.setattr(spambot, "on_peer_flood", peerflood)

    asyncio.run(
        dialog_engine.handle_incoming_private(
            account,
            target,
            "Да",
            telegram_message_id=18010401,
        )
    )

    assert dialog_inbox.count_by_status(dialog_inbox.STATUS_PENDING) == 1
    outbox = get_connection().execute(
        "SELECT action_kind, status, recovery_next_at FROM dialog_outbox WHERE target_user_id=?",
        (target,),
    ).fetchone()
    assert outbox["status"] == dialog_delivery.STATUS_FAILED
    assert outbox["recovery_next_at"] is not None
    assert accounts.get_account(account)["is_paused"] == 1

    with db_lock(), get_connection() as conn:
        conn.execute(
            "UPDATE dialog_outbox SET recovery_next_at=? WHERE target_user_id=?",
            (
                (dt.datetime.now(dt.timezone.utc) - dt.timedelta(seconds=1)).isoformat(),
                target,
            ),
        )
    client.error = None

    assert asyncio.run(dialog_engine.recover_pending_incoming_messages()) == 1
    assert dialog_inbox.count_by_status(dialog_inbox.STATUS_DONE) == 1
    assert len(client.sent) == 2
    assert dialog_store.get_dialog(target)["stage"] == dialog_store.STAGE_PROMO_SENT
    assert accounts.get_account(account)["is_paused"] == 1
