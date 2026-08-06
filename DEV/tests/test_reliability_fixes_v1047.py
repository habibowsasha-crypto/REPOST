"""Regression coverage for v1.0.47 reliability and diagnostics fixes."""

from __future__ import annotations

import ast
import asyncio
import datetime as dt
from pathlib import Path


def _seed_claim(account_id: int, target_id: int, *, username: str = "lead"):
    from services import accounts, queue

    accounts.upsert_account(user_id=account_id, session_string="session", username="sender")
    accounts.set_participates(account_id, True)
    queue.upsert_from_activity(
        target_user_id=target_id,
        username=username,
        source_account_user_id=account_id,
        access_hash=123456,
    )
    lead = queue.claim_random_pending(account_id)
    assert lead is not None
    return lead


def test_first_dm_prepare_is_atomic_with_provisional_dialog(app_env):
    from db.schema import get_connection
    from services import dialog_store, first_dm_delivery, queue

    _seed_claim(10, 100)
    assert first_dm_delivery.prepare(100, 10, "Можно спросить?")

    dialog = dialog_store.get_dialog(100)
    assert dialog is not None
    assert dialog["stage"] == dialog_store.STAGE_FIRST_DM_SENDING
    assert int(dialog["outgoing_count"]) == 0

    contact = get_connection().execute(
        "SELECT status FROM contacts WHERE target_user_id=100"
    ).fetchone()
    assert contact["status"] == "sending"
    assert queue.count_by_status(queue.STATUS_CLAIMED) == 1


def test_prepared_delivery_is_not_blindly_requeued(app_env):
    from db.schema import db_lock, get_connection
    from services import first_dm_delivery, queue

    _seed_claim(10, 101)
    assert first_dm_delivery.prepare(101, 10, "Есть секунда?")
    old = (dt.datetime.now(dt.timezone.utc) - dt.timedelta(hours=1)).isoformat()
    conn = get_connection()
    with db_lock(), conn:
        conn.execute("UPDATE leads SET claimed_at=? WHERE target_user_id=101", (old,))
        conn.execute(
            "UPDATE first_dm_outbox SET prepared_at=? WHERE target_user_id=101", (old,)
        )

    assert queue.release_stale_claims(older_than_seconds=900) == 0
    assert queue.count_by_status(queue.STATUS_CLAIMED) == 1


def test_commit_sent_atomically_creates_real_dialog(app_env):
    from db.schema import get_connection
    from services import dialog_store, first_dm_delivery, queue

    _seed_claim(10, 102)
    first_dm_delivery.prepare(102, 10, "Не отвлеку?")
    assert first_dm_delivery.commit_sent(102, telegram_message_id=555)

    dialog = dialog_store.get_dialog(102)
    assert dialog["stage"] == dialog_store.STAGE_WAITING_REPLY
    assert int(dialog["outgoing_count"]) == 1
    assert dialog["history"][0]["text"] == "Не отвлеку?"
    assert queue.count_by_status(queue.STATUS_SENT) == 1
    outbox = get_connection().execute(
        "SELECT status, telegram_message_id FROM first_dm_outbox WHERE target_user_id=102"
    ).fetchone()
    assert outbox["status"] == "sent"
    assert int(outbox["telegram_message_id"]) == 555


def test_incoming_reply_confirms_ambiguous_first_dm(app_env):
    from services import dialog_store, first_dm_delivery, queue

    _seed_claim(10, 103)
    first_dm_delivery.prepare(103, 10, "Можно вопрос?")
    assert first_dm_delivery.confirm_from_incoming(103, 10)
    assert queue.count_by_status(queue.STATUS_SENT) == 1
    assert dialog_store.get_dialog(103)["stage"] == dialog_store.STAGE_WAITING_REPLY


def test_recovery_checks_telegram_before_retry(app_env, monkeypatch):
    from db.schema import db_lock, get_connection
    from services import dispatcher, first_dm_delivery, monitor, queue

    _seed_claim(10, 104)
    first_dm_delivery.prepare(104, 10, "Ты сам торгуешь?")
    old = (dt.datetime.now(dt.timezone.utc) - dt.timedelta(minutes=5)).isoformat()
    conn = get_connection()
    with db_lock(), conn:
        conn.execute(
            "UPDATE first_dm_outbox SET prepared_at=? WHERE target_user_id=104", (old,)
        )

    class Message:
        out = True
        message = "Ты сам торгуешь?"
        id = 9001
        date = dt.datetime.now(dt.timezone.utc)

    class Client:
        def is_connected(self):
            return True

        async def get_input_entity(self, value):
            return value

        async def get_messages(self, entity, limit=30):
            return [Message()]

    monkeypatch.setattr(monitor, "get_client", lambda uid: Client())
    recovered = asyncio.run(dispatcher.recover_ambiguous_first_dms())
    assert recovered == 1
    assert queue.count_by_status(queue.STATUS_SENT) == 1


def test_recovery_returns_unsent_delivery_to_queue(app_env, monkeypatch):
    from db.schema import db_lock, get_connection
    from services import dispatcher, first_dm_delivery, monitor, queue

    _seed_claim(10, 105)
    first_dm_delivery.prepare(105, 10, "Есть минутка?")
    old = (dt.datetime.now(dt.timezone.utc) - dt.timedelta(minutes=5)).isoformat()
    conn = get_connection()
    with db_lock(), conn:
        conn.execute(
            "UPDATE first_dm_outbox SET prepared_at=? WHERE target_user_id=105", (old,)
        )

    class Client:
        def is_connected(self):
            return True

        async def get_input_entity(self, value):
            return value

        async def get_messages(self, entity, limit=30):
            return []

    monkeypatch.setattr(monitor, "get_client", lambda uid: Client())
    recovered = asyncio.run(dispatcher.recover_ambiguous_first_dms())
    assert recovered == 1
    assert queue.count_by_status(queue.STATUS_PENDING) == 1


def test_floodwait_is_visible_and_auto_clears_after_expiry(app_env):
    from db.schema import db_lock, get_connection
    from services import accounts, pacing

    accounts.upsert_account(user_id=20, session_string="s")
    accounts.set_participates(20, True)
    pacing.apply_floodwait(20, 60)
    paused = accounts.get_account(20)
    assert int(paused["is_paused"]) == 1
    assert "FloodWait" in str(paused["pause_reason"])

    past = (dt.datetime.now(dt.timezone.utc) - dt.timedelta(seconds=1)).isoformat()
    conn = get_connection()
    with db_lock(), conn:
        conn.execute("UPDATE accounts SET cooldown_until=? WHERE user_id=20", (past,))
    resumed = accounts.get_account(20)
    assert int(resumed["is_paused"]) == 0
    assert resumed["cooldown_until"] is None


def test_peerflood_default_range_matches_approved_values(app_env):
    from services import runtime

    assert runtime.get_peer_flood_range_seconds() == (60, 90)


def test_spambot_moscow_time_is_normalized_to_utc(app_env):
    from services.spambot import _extract_until

    parsed = _extract_until("Ваш аккаунт ограничен до 10.08.2026 12:00 МСК")
    value = dt.datetime.fromisoformat(parsed)
    assert value.utcoffset() == dt.timedelta(0)
    assert value.hour == 9


def test_ai_calls_have_explicit_timeout(app_env):
    first = Path("services/ai_first_dm.py").read_text(encoding="utf-8")
    dialog = Path("services/ai_dialog.py").read_text(encoding="utf-8")
    config = Path("config.py").read_text(encoding="utf-8")
    assert "asyncio.wait_for" in first
    assert "asyncio.wait_for" in dialog
    assert "AI_REQUEST_TIMEOUT_SECONDS" in config


def test_account_task_cancel_is_awaited(app_env):
    from services import monitor

    async def scenario():
        started = asyncio.Event()

        async def worker():
            started.set()
            await asyncio.sleep(60)

        task = asyncio.create_task(worker(), name="dialog-77-100")
        monitor._track_dialog_task(77, task)
        await started.wait()
        cancelled = await monitor._cancel_dialog_tasks(77)
        assert cancelled == 1
        assert task.cancelled()

    asyncio.run(scenario())


def test_no_silent_broad_exception_handlers(app_env):
    root = Path(".")
    offenders = []
    for path in root.rglob("*.py"):
        if "tests" in path.parts:
            continue
        tree = ast.parse(path.read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            if not isinstance(node, ast.ExceptHandler):
                continue
            broad = node.type is None or (
                isinstance(node.type, ast.Name) and node.type.id == "Exception"
            )
            if not broad:
                continue
            body = " ".join(ast.unparse(item) for item in node.body)
            if "logger." not in body and "raise" not in body:
                offenders.append(f"{path}:{node.lineno}")
    assert offenders == []


def test_explicit_import_requeue_clears_old_delivery_journal(app_env):
    from db.schema import get_connection
    from services import first_dm_delivery, queue

    _seed_claim(10, 106)
    first_dm_delivery.prepare(106, 10, "Первое")
    first_dm_delivery.commit_sent(106, telegram_message_id=1)
    assert get_connection().execute(
        "SELECT 1 FROM first_dm_outbox WHERE target_user_id=106"
    ).fetchone()

    assert queue.force_requeue(target_user_id=106, username="lead")
    assert get_connection().execute(
        "SELECT 1 FROM first_dm_outbox WHERE target_user_id=106"
    ).fetchone() is None
    assert queue.count_by_status(queue.STATUS_PENDING) == 1


def test_ambiguous_delivery_confirmation_does_not_reopen_optout(app_env):
    from db.schema import get_connection
    from services import dialog_store, first_dm_delivery, opt_out

    _seed_claim(10, 107)
    first_dm_delivery.prepare(107, 10, "Можно спросить?")
    opt_out.add(107, "manual_admin")
    assert first_dm_delivery.commit_sent(107, telegram_message_id=2)

    dialog = dialog_store.get_dialog(107)
    assert dialog["stage"] == dialog_store.STAGE_CLOSED
    assert dialog["auto_link_at"] is None
    contact = get_connection().execute(
        "SELECT status FROM contacts WHERE target_user_id=107"
    ).fetchone()
    assert contact["status"] == "completed"
