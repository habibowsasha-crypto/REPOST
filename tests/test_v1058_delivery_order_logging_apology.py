"""Regression coverage for the six approved v1.0.58 fixes."""

from __future__ import annotations

import asyncio
import datetime as dt
import io
import sys
import time
from types import SimpleNamespace

import pytest
from loguru import logger


@pytest.fixture(autouse=True)
def _purge_main_and_handler_modules_after_test():
    yield
    for name in list(sys.modules):
        if name == "main" or name == "handlers" or name.startswith("handlers."):
            sys.modules.pop(name, None)


def _account(account_id: int) -> None:
    from services import accounts

    accounts.upsert_account(
        user_id=account_id,
        session_string=f"test-session-{account_id}",
        username=f"sender{account_id}",
    )
    accounts.set_participates(account_id, True)


def _claimed_lead(target: int, account: int, *, source: int | None = None):
    from services import queue

    queue.upsert_from_activity(
        target_user_id=target,
        username=f"lead{target}",
        access_hash=target * 10,
        source_account_user_id=source if source is not None else account,
    )
    lead = queue.claim_random_pending(account)
    assert lead is not None
    return lead


class _ConnectedClient:
    def __init__(self, *, send_error: BaseException | None = None, sent: list | None = None):
        self.send_error = send_error
        self.sent = sent if sent is not None else []

    def is_connected(self):
        return True

    async def get_input_entity(self, target):
        return SimpleNamespace(target=target)

    async def send_message(self, entity, text):
        self.sent.append((entity, text))
        if self.send_error is not None:
            raise self.send_error
        return SimpleNamespace(
            id=1000 + len(self.sent),
            date=dt.datetime.now(dt.timezone.utc),
        )


def test_logger_exception_does_not_expose_session_locals(app_env):
    import main

    secret = "SESSION_SECRET_MUST_NOT_APPEAR_123456789"
    sink = io.StringIO()
    main.configure_logging(sink=sink)

    def explode():
        accounts = [{"user_id": 1, "session_string": secret}]
        lead = {"target_user_id": 2, "session_string": secret}
        assert accounts and lead
        raise RuntimeError("safe failure")

    try:
        try:
            explode()
        except RuntimeError:
            logger.exception("unexpected delivery failure")
        output = sink.getvalue()
        assert secret not in output
        assert "session_string" not in output
        assert "safe failure" in output
    finally:
        main.configure_logging(sink=sys.stderr)
        for name in list(sys.modules):
            if name == "main" or name == "handlers" or name.startswith("handlers."):
                sys.modules.pop(name, None)


@pytest.mark.parametrize(
    "rpc_name",
    ["ALLOW_PAYMENT_REQUIRED_1", "ALLOW_PAYMENT_REQUIRED_400", "ALLOW_PAYMENT_REQUIRED_999"],
)
def test_paid_message_required_is_terminal_without_recovery(app_env, rpc_name):
    from db.schema import get_connection
    from services import dispatcher, first_dm_delivery, queue

    class PaidMessageError(Exception):
        def __init__(self, name: str):
            super().__init__("Telegram RPC error")
            self.message = name

    _account(2101)
    lead = _claimed_lead(3101, 2101)
    client = _ConnectedClient(send_error=PaidMessageError(rpc_name))

    result = asyncio.run(
        dispatcher._send_first_dm(
            client,
            2101,
            lead,
            "Как рынок сейчас видишь?",
            entity=object(),
        )
    )
    assert result == "paid_message_required"

    lead_row = get_connection().execute(
        "SELECT status, failure_reason, last_error FROM leads WHERE target_user_id=3101"
    ).fetchone()
    assert lead_row["status"] == queue.STATUS_CANCELLED
    assert lead_row["failure_reason"] == "paid_message_required"
    assert lead_row["last_error"] == "paid_message_required"

    outbox = get_connection().execute(
        "SELECT status, last_error FROM first_dm_outbox WHERE target_user_id=3101"
    ).fetchone()
    assert outbox["status"] == first_dm_delivery.STATUS_FAILED
    assert outbox["last_error"] == "paid_message_required"
    assert first_dm_delivery.get_prepared(3101) is None
    assert first_dm_delivery.list_stale_prepared(older_than_seconds=1) == []
    assert get_connection().execute(
        "SELECT 1 FROM contacts WHERE target_user_id=3101"
    ).fetchone() is None
    assert get_connection().execute(
        "SELECT 1 FROM dialogs WHERE target_user_id=3101"
    ).fetchone() is None


def test_paid_message_required_does_not_try_next_account(app_env, monkeypatch):
    from services import dispatcher, monitor

    class PaidMessageError(Exception):
        message = "ALLOW_PAYMENT_REQUIRED_400"

    _account(2111)
    _account(2112)
    lead = _claimed_lead(3111, 2111, source=2111)
    sent_by: list[int] = []
    clients = {
        2111: _ConnectedClient(send_error=PaidMessageError("paid")),
        2112: _ConnectedClient(),
    }

    async def resolve(client, account_id, current_lead):
        return SimpleNamespace(account_id=account_id)

    async def generated():
        return "Как рынок сейчас видишь?"

    async def notify(*args, **kwargs):
        return None

    for account_id, client in clients.items():
        original = client.send_message

        async def tracked(entity, text, *, _account_id=account_id, _original=original):
            sent_by.append(_account_id)
            return await _original(entity, text)

        client.send_message = tracked

    monkeypatch.setattr(monitor, "get_client", lambda account_id: clients[account_id])
    monkeypatch.setattr(dispatcher, "_resolve_target_entity", resolve)
    monkeypatch.setattr(dispatcher, "generate_first_dm", generated)
    monkeypatch.setattr(dispatcher, "_notify_admins_first_dm", notify)

    result = asyncio.run(
        dispatcher._attempt_lead_across_accounts(
            lead,
            [
                {"user_id": 2111, "participates": 1, "session_string": "s1"},
                {"user_id": 2112, "participates": 1, "session_string": "s2"},
            ],
        )
    )
    assert result is True
    assert sent_by == [2111]


def test_peer_id_invalid_send_rolls_back_prepared(app_env, monkeypatch):
    from db.schema import get_connection
    from services import dispatcher, first_dm_delivery, queue

    class FakePeerIdInvalidError(Exception):
        pass

    monkeypatch.setattr(dispatcher, "PeerIdInvalidError", FakePeerIdInvalidError)
    _account(2201)
    lead = _claimed_lead(3201, 2201)
    client = _ConnectedClient(send_error=FakePeerIdInvalidError("invalid peer"))

    result = asyncio.run(
        dispatcher._send_first_dm(
            client,
            2201,
            lead,
            "Какой таймфрейм чаще смотришь?",
            entity=object(),
        )
    )
    assert result == "peer_invalid"

    lead_row = get_connection().execute(
        "SELECT status, claimed_by_account FROM leads WHERE target_user_id=3201"
    ).fetchone()
    assert lead_row["status"] == queue.STATUS_PENDING
    assert lead_row["claimed_by_account"] is None
    outbox = get_connection().execute(
        "SELECT status, last_error FROM first_dm_outbox WHERE target_user_id=3201"
    ).fetchone()
    assert outbox["status"] == first_dm_delivery.STATUS_FAILED
    assert outbox["last_error"] == "peer_id_invalid_send"
    assert get_connection().execute(
        "SELECT 1 FROM contacts WHERE target_user_id=3201"
    ).fetchone() is None
    assert get_connection().execute(
        "SELECT 1 FROM dialogs WHERE target_user_id=3201"
    ).fetchone() is None


def test_peer_id_invalid_continues_round_with_one_generated_text(app_env, monkeypatch):
    from db.schema import get_connection
    from services import dispatcher, monitor, queue

    class FakePeerIdInvalidError(Exception):
        pass

    monkeypatch.setattr(dispatcher, "PeerIdInvalidError", FakePeerIdInvalidError)
    _account(2211)
    _account(2212)
    lead = _claimed_lead(3211, 2211, source=2211)
    clients = {
        2211: _ConnectedClient(send_error=FakePeerIdInvalidError("invalid peer")),
        2212: _ConnectedClient(),
    }
    resolved: list[int] = []
    generated_calls = 0

    async def resolve(client, account_id, current_lead):
        resolved.append(account_id)
        return SimpleNamespace(account_id=account_id)

    async def generated():
        nonlocal generated_calls
        generated_calls += 1
        return "Сам график разбираешь или идеи смотришь?"

    async def notify(*args, **kwargs):
        return None

    monkeypatch.setattr(monitor, "get_client", lambda account_id: clients[account_id])
    monkeypatch.setattr(dispatcher, "_resolve_target_entity", resolve)
    monkeypatch.setattr(dispatcher, "generate_first_dm", generated)
    monkeypatch.setattr(dispatcher, "_notify_admins_first_dm", notify)

    result = asyncio.run(
        dispatcher._attempt_lead_across_accounts(
            lead,
            [
                {"user_id": 2211, "participates": 1, "session_string": "s1"},
                {"user_id": 2212, "participates": 1, "session_string": "s2"},
            ],
        )
    )
    assert result is True
    assert resolved == [2211, 2212]
    assert generated_calls == 1
    assert [item[1] for item in clients[2211].sent] == [
        "Сам график разбираешь или идеи смотришь?"
    ]
    assert [item[1] for item in clients[2212].sent] == [
        "Сам график разбираешь или идеи смотришь?"
    ]
    row = get_connection().execute(
        "SELECT status, claimed_by_account FROM leads WHERE target_user_id=3211"
    ).fetchone()
    assert row["status"] == queue.STATUS_SENT
    assert int(row["claimed_by_account"]) == 2212


def test_ai_is_not_called_when_no_account_resolves_entity(app_env, monkeypatch):
    from db.schema import get_connection
    from services import dispatcher, monitor, queue

    _account(2301)
    _account(2302)
    lead = _claimed_lead(3301, 2301, source=2301)
    clients = {2301: _ConnectedClient(), 2302: _ConnectedClient()}

    async def no_entity(client, account_id, current_lead):
        return None

    async def forbidden_generation():
        raise AssertionError("AI must not run before entity resolution")

    monkeypatch.setattr(monitor, "get_client", lambda account_id: clients[account_id])
    monkeypatch.setattr(dispatcher, "_resolve_target_entity", no_entity)
    monkeypatch.setattr(dispatcher, "generate_first_dm", forbidden_generation)

    result = asyncio.run(
        dispatcher._attempt_lead_across_accounts(
            lead,
            [
                {"user_id": 2301, "participates": 1, "session_string": "s1"},
                {"user_id": 2302, "participates": 1, "session_string": "s2"},
            ],
        )
    )
    assert result is True
    row = get_connection().execute(
        "SELECT status, failure_reason FROM leads WHERE target_user_id=3301"
    ).fetchone()
    assert row["status"] == queue.STATUS_CANCELLED
    assert row["failure_reason"] == "no_entity_all_accounts"
    assert queue.failed_account_ids(3301, "no_entity") == {2301, 2302}


def test_source_account_priority_is_unchanged(app_env, monkeypatch):
    from services import dispatcher

    lead = {"target_user_id": 1, "source_account_user_id": 2402}
    ready = [
        {"user_id": 2401},
        {"user_id": 2402},
        {"user_id": 2403},
    ]
    monkeypatch.setattr(dispatcher.random, "shuffle", lambda items: items.reverse())
    ordered = dispatcher._order_accounts_for_lead(lead, ready)
    assert int(ordered[0]["user_id"]) == 2402
    assert sorted(int(item["user_id"]) for item in ordered) == [2401, 2402, 2403]


def test_legacy_apology_range_is_clamped_and_persisted(app_env):
    from db.schema import db_lock, get_connection
    from services import runtime

    now = dt.datetime.now(dt.timezone.utc).isoformat()
    conn = get_connection()
    with db_lock(), conn:
        conn.executemany(
            "INSERT OR REPLACE INTO runtime_meta (key, value, updated_at) VALUES (?, ?, ?)",
            [
                (runtime.KEY_PACE_LINK_LO, "60", now),
                (runtime.KEY_PACE_LINK_HI, "600", now),
            ],
        )

    assert runtime.get_auto_link_delay_range() == (60, 60)
    rows = {
        row["key"]: row["value"]
        for row in conn.execute(
            "SELECT key, value FROM runtime_meta WHERE key IN (?, ?)",
            (runtime.KEY_PACE_LINK_LO, runtime.KEY_PACE_LINK_HI),
        ).fetchall()
    }
    assert rows == {
        runtime.KEY_PACE_LINK_LO: "60",
        runtime.KEY_PACE_LINK_HI: "60",
    }


def test_apology_runtime_and_admin_ui_reject_values_above_60(app_env):
    from handlers import menu
    from services import runtime

    assert runtime.set_auto_link_delay_range(1, 600) == (5, 60)
    assert runtime.get_auto_link_delay_range() == (5, 60)
    assert menu._validate_apology_range_input(5, 60) == (5, 60)
    with pytest.raises(ValueError):
        menu._validate_apology_range_input(5, 61)
    with pytest.raises(ValueError):
        menu._validate_apology_range_input(4, 60)


def test_apology_delay_picker_stays_inside_5_to_60(app_env):
    from services import dialog_engine, runtime

    runtime.set_auto_link_delay_range(5, 60)
    values = [dialog_engine._auto_link_delay() for _ in range(100)]
    assert min(values) >= 5
    assert max(values) <= 60


def test_apology_scheduler_runs_independently_at_one_second_cadence(app_env, monkeypatch):
    import main
    from services import dialog_engine

    calls: list[float] = []

    async def process_due():
        calls.append(time.monotonic())
        return 0

    monkeypatch.setattr(dialog_engine, "process_due_auto_links", process_due)
    monkeypatch.setattr(main, "APOLOGY_SCHEDULER_INTERVAL_SECONDS", 0.01)

    async def scenario():
        task = asyncio.create_task(main._apology_due_loop())
        await asyncio.sleep(0.035)
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)

    asyncio.run(scenario())
    assert len(calls) >= 3
    assert max(b - a for a, b in zip(calls, calls[1:])) < 0.03


def test_first_dm_ai_attempts_are_bounded_then_local_fallback(app_env, monkeypatch):
    from services import ai_first_dm, phrases

    recent = "Можно одну вещь спросить?"
    phrases.remember(phrases.KIND_FIRST_DM, recent)
    calls = 0

    async def duplicate(*args, **kwargs):
        nonlocal calls
        calls += 1
        return recent

    monkeypatch.setattr(ai_first_dm, "AI_DM_ENABLED", True)
    monkeypatch.setattr(ai_first_dm, "OPENAI_API_KEY", "test")
    monkeypatch.setattr(ai_first_dm, "_openai_first_dm", duplicate)

    text = asyncio.run(ai_first_dm.generate_first_dm())
    assert calls == ai_first_dm.MAX_AI_FIRST_DM_ATTEMPTS == 3
    assert text != recent
    assert ai_first_dm.validate_first_dm(text)[0]
    assert not ai_first_dm._too_similar_recent(text, [recent])


def test_first_dm_is_simple_and_has_no_trading_topic(app_env):
    from services import ai_first_dm
    from texts.first_dm import FIRST_DM_TEMPLATES

    assert len(FIRST_DM_TEMPLATES) > 20
    for text in FIRST_DM_TEMPLATES:
        assert ai_first_dm.validate_first_dm(text)[0]
        assert not ai_first_dm._TOPIC_RE.search(text)


def test_locked_limits_and_uniqueness_window_remain_unchanged(app_env):
    from services import dialog_store, phrases, runtime

    assert dialog_store.MAX_OUTGOING == 5
    assert runtime.get_account_interval_range() == (120, 420)
    assert runtime.get_global_spacing_range() == (90, 180)
    assert runtime.get_daily_limit() == 125

    for index in range(35):
        phrases.remember(phrases.KIND_PROMO, f"promo-{index}")
    assert len(phrases.recent_texts(phrases.KIND_PROMO, limit=100)) == 20
