"""Production-log regressions fixed in v1.0.56."""

from __future__ import annotations

import asyncio
import datetime as dt
from types import SimpleNamespace


def _seed_prepared_first_dm(target: int = 12001, account: int = 501) -> None:
    from db.schema import db_lock, get_connection
    from services import accounts, first_dm_delivery, queue

    accounts.upsert_account(
        user_id=account,
        session_string="session",
        username="sender",
    )
    accounts.set_participates(account, True)
    queue.upsert_from_activity(
        target_user_id=target,
        username="lead",
        source_account_user_id=account,
        access_hash=777,
    )
    assert queue.claim_random_pending(account) is not None
    assert first_dm_delivery.prepare(target, account, "ты торгуешь?")
    old = (dt.datetime.now(dt.timezone.utc) - dt.timedelta(minutes=5)).isoformat()
    conn = get_connection()
    with db_lock(), conn:
        conn.execute(
            "UPDATE first_dm_outbox SET prepared_at=? WHERE target_user_id=?",
            (old, target),
        )


def test_first_dm_recovery_backoff_is_durable(app_env):
    from db.schema import db_lock, get_connection
    from services import first_dm_delivery

    _seed_prepared_first_dm()
    before = first_dm_delivery.list_stale_prepared(older_than_seconds=90)
    assert [row["target_user_id"] for row in before] == [12001]

    assert first_dm_delivery.defer_recovery(
        12001,
        "peer_id_invalid",
        delay_seconds=21600,
    ) == 1
    assert first_dm_delivery.list_stale_prepared(older_than_seconds=90) == []

    row = first_dm_delivery.get_prepared(12001)
    assert int(row["recovery_attempts"]) == 1
    assert row["recovery_last_error"] == "peer_id_invalid"
    assert row["recovery_next_at"] is not None

    past = (dt.datetime.now(dt.timezone.utc) - dt.timedelta(seconds=1)).isoformat()
    conn = get_connection()
    with db_lock(), conn:
        conn.execute(
            "UPDATE first_dm_outbox SET recovery_next_at=? WHERE target_user_id=?",
            (past, 12001),
        )
    assert len(first_dm_delivery.list_stale_prepared(older_than_seconds=90)) == 1


def test_peer_invalid_history_check_is_backed_off_without_rollback(app_env, monkeypatch):
    from services import dispatcher, first_dm_delivery, monitor, telegram_history

    _seed_prepared_first_dm(target=12002, account=502)

    class PeerIdInvalidError(Exception):
        pass

    class Client:
        def is_connected(self):
            return True

    async def resolve(*args, **kwargs):
        return object()

    async def fail(*args, **kwargs):
        raise PeerIdInvalidError("invalid peer")

    monkeypatch.setattr(monitor, "get_client", lambda account: Client())
    monkeypatch.setattr(dispatcher, "_resolve_target_entity", resolve)
    monkeypatch.setattr(telegram_history, "find_outgoing_text_since", fail)

    assert asyncio.run(dispatcher.recover_ambiguous_first_dms()) == 0
    row = first_dm_delivery.get_prepared(12002)
    assert row is not None
    assert int(row["recovery_attempts"]) == 1
    assert "peer_id_invalid" in str(row["recovery_last_error"])
    assert first_dm_delivery.list_stale_prepared(older_than_seconds=90) == []


def test_ai_opening_is_assembled_into_validator_safe_promo(app_env, monkeypatch):
    from services import ai_dialog

    calls = []

    async def fake_openai(history, *, instruction, temperature):
        calls.append((instruction, temperature))
        return "ага, понял тебя"

    monkeypatch.setattr(ai_dialog, "AI_DM_ENABLED", True)
    monkeypatch.setattr(ai_dialog, "OPENAI_API_KEY", "test")
    monkeypatch.setattr(ai_dialog, "_openai_reply", fake_openai)

    text = asyncio.run(
        ai_dialog.generate_promo(
            [{"role": "user", "text": "в основном альты"}],
            category=ai_dialog.CATEGORY_NORMAL,
        )
    )
    body = text.replace("https://t.me/+testhash", "").strip()
    assert len(calls) == 1
    assert ai_dialog._promo_ok(body)
    assert text.count("https://t.me/+testhash") == 1
    assert "ага, понял тебя" in text.lower()


def test_prepared_scheduled_action_does_not_call_ai_again(app_env, monkeypatch):
    from services import dialog_delivery, dialog_engine, dialog_store

    target = 12003
    account = 503
    dialog_store.create_after_first_dm(target, account, "ты торгуешь?")
    past = (dt.datetime.now(dt.timezone.utc) - dt.timedelta(seconds=1)).isoformat()
    dialog_store.set_stage(
        target,
        dialog_store.STAGE_EXPLAINED,
        auto_link_at=past,
    )
    assert dialog_delivery.prepare(
        target,
        account,
        dialog_delivery.KIND_AUTO_LINK,
        "already prepared",
        message_kind=dialog_delivery.KIND_PROMO,
        transition={
            "stage": dialog_store.STAGE_PROMO_SENT,
            "bump_outgoing": True,
            "link_sent": True,
            "clear_auto_link": True,
            "append_history": True,
        },
    )

    async def forbidden(*args, **kwargs):
        raise AssertionError("AI must not be called for an existing PREPARED action")

    monkeypatch.setattr(dialog_engine.ai_dialog, "generate_promo", forbidden)
    assert asyncio.run(dialog_engine.process_due_auto_links()) == 0
    row = dialog_delivery.get(target, dialog_delivery.KIND_AUTO_LINK)
    assert row["status"] == dialog_delivery.STATUS_PREPARED


def test_sent_scheduled_action_repairs_stage_without_ai(app_env, monkeypatch):
    from services import dialog_delivery, dialog_engine, dialog_store

    target = 12004
    account = 504
    dialog_store.create_after_first_dm(target, account, "ты торгуешь?")
    past = (dt.datetime.now(dt.timezone.utc) - dt.timedelta(seconds=1)).isoformat()
    dialog_store.set_stage(
        target,
        dialog_store.STAGE_EXPLAINED,
        auto_link_at=past,
    )
    assert dialog_delivery.prepare(
        target,
        account,
        dialog_delivery.KIND_AUTO_LINK,
        "promo\nhttps://t.me/+testhash",
        message_kind=dialog_delivery.KIND_PROMO,
        transition={
            "stage": dialog_store.STAGE_PROMO_SENT,
            "bump_outgoing": True,
            "link_sent": True,
            "clear_auto_link": True,
            "append_history": True,
        },
    )
    assert dialog_delivery.commit_sent(target, dialog_delivery.KIND_AUTO_LINK)
    # Simulate stale state observed in production logs.
    dialog_store.set_stage(
        target,
        dialog_store.STAGE_EXPLAINED,
        link_sent=False,
        auto_link_at=past,
    )

    async def forbidden(*args, **kwargs):
        raise AssertionError("AI must not be called for an existing SENT action")

    monkeypatch.setattr(dialog_engine.ai_dialog, "generate_promo", forbidden)
    assert asyncio.run(dialog_engine.process_due_auto_links()) == 0
    dialog = dialog_store.get_dialog(target)
    assert dialog["stage"] == dialog_store.STAGE_PROMO_SENT
    assert int(dialog["link_sent"]) == 1
    assert dialog["auto_link_at"] is not None


def test_v1055_first_dm_outbox_schema_migrates_without_data_loss(app_env):
    from db.schema import db_lock, get_connection, init_db

    conn = get_connection()
    with db_lock(), conn:
        conn.execute("DROP TABLE first_dm_outbox")
        conn.execute(
            """
            CREATE TABLE first_dm_outbox (
                target_user_id INTEGER PRIMARY KEY,
                account_user_id INTEGER NOT NULL,
                text TEXT NOT NULL,
                status TEXT NOT NULL,
                prepared_at TEXT NOT NULL,
                telegram_message_id INTEGER,
                sent_at TEXT,
                last_error TEXT,
                updated_at TEXT NOT NULL
            )
            """
        )
        now = dt.datetime.now(dt.timezone.utc).isoformat()
        conn.execute(
            """
            INSERT INTO first_dm_outbox (
                target_user_id, account_user_id, text, status,
                prepared_at, updated_at
            ) VALUES (?, ?, ?, 'prepared', ?, ?)
            """,
            (13001, 601, "old prepared", now, now),
        )

    init_db()
    columns = {
        str(row[1]) for row in conn.execute("PRAGMA table_info(first_dm_outbox)").fetchall()
    }
    assert {"recovery_attempts", "recovery_next_at", "recovery_last_error"} <= columns
    row = conn.execute(
        "SELECT text, status, recovery_attempts FROM first_dm_outbox WHERE target_user_id=?",
        (13001,),
    ).fetchone()
    assert row["text"] == "old prepared"
    assert row["status"] == "prepared"
    assert int(row["recovery_attempts"]) == 0
