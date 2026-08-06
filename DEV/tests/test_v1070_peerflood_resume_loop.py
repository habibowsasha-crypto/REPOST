"""v1.0.70 breaks the SpamBot/PeerFlood loop and bounds dialog recovery."""

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


def _seed_claimed_lead(queue, account_id: int, target_id: int):
    queue.upsert_from_activity(
        target_user_id=target_id,
        username=f"target{target_id}",
        first_name="Target",
        source_chat_id=-100123,
        source_account_user_id=account_id,
        access_hash=target_id * 100,
    )
    lead = queue.claim_random_pending(account_id)
    assert lead is not None
    return lead


def test_automatic_spambot_resume_always_creates_fresh_account_interval(app_env, monkeypatch):
    from services import accounts, pacing, runtime, spambot

    account_id = 7001
    runtime.set_worker_enabled(True)
    _seed_account(accounts, account_id)
    now = dt.datetime(2026, 8, 5, 6, 0, tzinfo=dt.timezone.utc)
    monkeypatch.setattr(spambot, "_now", lambda: now)
    monkeypatch.setattr(pacing, "_now", lambda: now)
    monkeypatch.setattr(pacing, "random_account_interval_seconds", lambda acc=None: 180)

    notices: list[str] = []

    async def notify(text: str):
        notices.append(text)

    async def refresh():
        return None

    monkeypatch.setattr(spambot, "notify_admins", notify)
    monkeypatch.setattr(spambot.monitor_svc, "refresh_monitor", refresh)
    future = now + dt.timedelta(seconds=30)
    from db.schema import db_lock, get_connection

    with db_lock(), get_connection() as conn:
        conn.execute(
            "UPDATE accounts SET is_paused=1, pause_reason='PeerFlood', cooldown_until=? WHERE user_id=?",
            (future.isoformat(), account_id),
        )
    spambot._upsert_state(
        account_id,
        status=spambot.STATUS_FREE_PENDING,
        next_check_at=now.isoformat(),
        last_reply="free",
    )

    asyncio.run(spambot.resume_account(account_id, source="spambot_auto"))

    row = accounts.get_account(account_id)
    expected = now + dt.timedelta(seconds=180)
    actual = dt.datetime.fromisoformat(str(row["next_send_at"]).replace("Z", "+00:00"))
    assert actual == expected
    assert pacing.account_is_send_ready(row)[0] is False
    assert "Следующий First DM" in notices[-1]


def test_manual_resume_remains_immediate_admin_override(app_env, monkeypatch):
    from db.schema import db_lock, get_connection
    from services import accounts, spambot

    account_id = 7002
    _seed_account(accounts, account_id)
    future = dt.datetime.now(dt.timezone.utc) + dt.timedelta(minutes=5)
    conn = get_connection()
    with db_lock(), conn:
        conn.execute(
            "UPDATE accounts SET is_paused=1, next_send_at=?, cooldown_until=? WHERE user_id=?",
            (future.isoformat(), future.isoformat(), account_id),
        )

    async def notify(text: str):
        return None

    async def refresh():
        return None

    monkeypatch.setattr(spambot, "notify_admins", notify)
    monkeypatch.setattr(spambot.monitor_svc, "refresh_monitor", refresh)
    asyncio.run(spambot.resume_account(account_id, source="manual"))
    row = accounts.get_account(account_id)
    assert row["next_send_at"] is None
    assert row["cooldown_until"] is None
    assert row["is_paused"] == 0


def test_peerflood_send_stops_same_lead_before_second_account(app_env, monkeypatch):
    from services import accounts, dispatcher, queue

    first_id, second_id, target_id = 7003, 7004, 8003
    first = _seed_account(accounts, first_id)
    second = _seed_account(accounts, second_id)
    lead = _seed_claimed_lead(queue, first_id, target_id)
    calls: list[int] = []

    async def send(client, account_id, current_lead, text, entity=None):
        calls.append(account_id)
        return "peerflood"

    monkeypatch.setattr(dispatcher, "_send_first_dm", send)
    monkeypatch.setattr(dispatcher, "_peerflood_lead_retry_seconds", lambda acc=None: 300)
    monkeypatch.setattr(
        dispatcher.monitor_svc,
        "get_client",
        lambda account_id: SimpleNamespace(is_connected=lambda: True),
    )

    result = asyncio.run(
        dispatcher._attempt_lead_across_accounts(lead, [first, second], text="Привет")
    )

    assert result is False
    assert calls == [first_id]
    current = queue.get_lead(target_id)
    assert current["status"] == queue.STATUS_PENDING
    eligible = dt.datetime.fromisoformat(str(current["eligible_at"]).replace("Z", "+00:00"))
    assert eligible > dt.datetime.now(dt.timezone.utc) + dt.timedelta(minutes=4)
    assert "peerflood_send_account" in str(current["last_error"])


def test_peerflood_during_entity_resolution_stops_cross_account_probe(app_env, monkeypatch):
    from services import accounts, dispatcher, queue

    first_id, second_id, target_id = 7005, 7006, 8005
    first = _seed_account(accounts, first_id)
    second = _seed_account(accounts, second_id)
    lead = _seed_claimed_lead(queue, first_id, target_id)
    resolved_by: list[int] = []

    class FakePeerFloodError(Exception):
        pass

    async def resolve(client, account_id, current_lead):
        resolved_by.append(account_id)
        raise FakePeerFloodError("peer flood")

    async def on_peer_flood(account_id: int):
        return None

    monkeypatch.setattr(dispatcher, "PeerFloodError", FakePeerFloodError)
    monkeypatch.setattr(dispatcher, "_resolve_target_entity", resolve)
    monkeypatch.setattr(dispatcher, "_peerflood_lead_retry_seconds", lambda acc=None: 300)
    monkeypatch.setattr(
        dispatcher.monitor_svc,
        "get_client",
        lambda account_id: SimpleNamespace(is_connected=lambda: True),
    )
    import services.spambot as spambot
    monkeypatch.setattr(spambot, "on_peer_flood", on_peer_flood)

    result = asyncio.run(
        dispatcher._attempt_lead_across_accounts(lead, [first, second], text=None)
    )
    assert result is False
    assert resolved_by == [first_id]
    assert queue.get_lead(target_id)["status"] == queue.STATUS_PENDING


def _seed_prepared_followup(account_id: int, target_id: int):
    from db.schema import db_lock, get_connection
    from services import accounts, dialog_delivery, first_dm_delivery, queue

    _seed_account(accounts, account_id)
    _seed_claimed_lead(queue, account_id, target_id)
    assert first_dm_delivery.prepare(target_id, account_id, "Первое сообщение")
    assert first_dm_delivery.commit_sent(target_id, telegram_message_id=1)
    assert dialog_delivery.prepare(
        target_id,
        account_id,
        dialog_delivery.KIND_FOLLOWUP,
        "Старый follow-up",
        message_kind=dialog_delivery.KIND_FOLLOWUP,
    )
    old = (dt.datetime.now(dt.timezone.utc) - dt.timedelta(minutes=5)).isoformat()
    conn = get_connection()
    with db_lock(), conn:
        conn.execute(
            "UPDATE dialog_outbox SET prepared_at=?, updated_at=? WHERE target_user_id=? AND action_kind=?",
            (old, old, target_id, dialog_delivery.KIND_FOLLOWUP),
        )
    return dialog_delivery.get(target_id, dialog_delivery.KIND_FOLLOWUP)


def test_dialog_recovery_next_at_prevents_every_tick_retry(app_env):
    from services import dialog_delivery

    account_id, target_id = 7007, 8007
    _seed_prepared_followup(account_id, target_id)
    attempts = dialog_delivery.defer_recovery(
        target_id,
        dialog_delivery.KIND_FOLLOWUP,
        "entity_unavailable",
        delay_seconds=900,
    )
    assert attempts == 1
    assert dialog_delivery.list_stale_prepared(older_than_seconds=90, limit=100) == []


def test_unresolvable_old_followup_is_abandoned_after_three_attempts(app_env, monkeypatch):
    from services import dialog_delivery, dialog_engine, dialog_store

    account_id, target_id = 7008, 8008
    row = _seed_prepared_followup(account_id, target_id)

    class Client:
        def is_connected(self):
            return True

        async def get_input_entity(self, value):
            raise ValueError("entity missing")

    monkeypatch.setattr(dialog_engine.monitor_svc, "get_client", lambda account: Client())

    assert asyncio.run(dialog_engine._reconcile_prepared_action(row)) is False
    assert asyncio.run(dialog_engine._reconcile_prepared_action(row)) is False
    assert asyncio.run(dialog_engine._reconcile_prepared_action(row)) is True

    outbox = dialog_delivery.get(target_id, dialog_delivery.KIND_FOLLOWUP)
    dialog = dialog_store.get_dialog(target_id)
    assert outbox["status"] == dialog_delivery.STATUS_FAILED
    assert int(outbox["recovery_attempts"]) == 3
    assert "recovery_exhausted:entity_unavailable" in str(outbox["last_error"])
    assert dialog["stage"] == dialog_store.STAGE_CLOSED
    assert dialog["auto_link_at"] is None


def test_active_dialog_send_resolves_by_username_before_numeric_cache(app_env, monkeypatch):
    from services import audience, dialog_delivery, dialog_engine

    account_id, target_id = 7009, 8009
    _seed_prepared_followup(account_id, target_id)
    from services import runtime
    runtime.set_worker_enabled(True)
    audience.record_first_dm(
        target_id,
        username="known_target",
        access_hash=123456789,
        source_account_user_id=account_id,
    )
    lookups: list[object] = []
    sent_entities: list[object] = []
    entity = SimpleNamespace(user_id=target_id)

    class Client:
        def is_connected(self):
            return True

        async def get_input_entity(self, value):
            lookups.append(value)
            if value == "known_target":
                return entity
            raise ValueError("numeric cache missing")

        async def send_message(self, current_entity, text):
            sent_entities.append(current_entity)
            return SimpleNamespace(id=77, date=None)

    async def cleanup(account):
        return None

    monkeypatch.setattr(dialog_engine.monitor_svc, "get_client", lambda account: Client())
    monkeypatch.setattr(dialog_engine, "_cleanup_disabled_account", cleanup)

    result = asyncio.run(
        dialog_engine._send_prepared_action(
            account_id,
            target_id,
            dialog_delivery.KIND_FOLLOWUP,
            "Старый follow-up",
        )
    )

    assert result == "sent"
    assert lookups == ["known_target"]
    assert sent_entities == [entity]
    assert dialog_delivery.get(target_id, dialog_delivery.KIND_FOLLOWUP)["status"] == "sent"


def test_active_dialog_send_uses_owned_access_hash_when_cache_is_empty(app_env, monkeypatch):
    from services import audience, dialog_delivery, dialog_engine

    account_id, target_id = 7010, 8010
    _seed_prepared_followup(account_id, target_id)
    from services import runtime
    runtime.set_worker_enabled(True)
    audience.record_first_dm(
        target_id,
        username=None,
        access_hash=987654321,
        source_account_user_id=account_id,
    )
    sent_entities: list[object] = []

    class Client:
        def is_connected(self):
            return True

        async def get_input_entity(self, value):
            raise ValueError("cache missing")

        async def send_message(self, current_entity, text):
            sent_entities.append(current_entity)
            return SimpleNamespace(id=78, date=None)

    async def cleanup(account):
        return None

    monkeypatch.setattr(dialog_engine.monitor_svc, "get_client", lambda account: Client())
    monkeypatch.setattr(dialog_engine, "_cleanup_disabled_account", cleanup)

    result = asyncio.run(
        dialog_engine._send_prepared_action(
            account_id,
            target_id,
            dialog_delivery.KIND_FOLLOWUP,
            "Старый follow-up",
        )
    )

    assert result == "sent"
    assert len(sent_entities) == 1
    assert int(getattr(sent_entities[0], "user_id")) == target_id
    assert int(getattr(sent_entities[0], "access_hash")) == 987654321
