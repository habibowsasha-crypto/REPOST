"""Regression coverage for v1.0.84 inactivity-based Telegram dialog deletion."""

from __future__ import annotations

import asyncio
import datetime as dt


def _enable_auto_delete(monkeypatch, *, days: int = 7, for_both: bool = True) -> None:
    import config
    from services import dialog_retention_policy, retention

    monkeypatch.setattr(config, "DIALOG_AUTO_DELETE_ENABLED", True)
    monkeypatch.setattr(config, "DIALOG_AUTO_DELETE_AFTER_DAYS", int(days))
    monkeypatch.setattr(config, "DIALOG_AUTO_DELETE_FOR_BOTH", bool(for_both))
    monkeypatch.setattr(dialog_retention_policy, "DIALOG_AUTO_DELETE_ENABLED", True)
    monkeypatch.setattr(dialog_retention_policy, "DIALOG_AUTO_DELETE_AFTER_DAYS", int(days))
    monkeypatch.setattr(retention, "DIALOG_AUTO_DELETE_ENABLED", True)
    monkeypatch.setattr(retention, "DIALOG_AUTO_DELETE_AFTER_DAYS", int(days))
    monkeypatch.setattr(retention, "DIALOG_AUTO_DELETE_FOR_BOTH", bool(for_both))


def _seed_sent(account_id: int, target_id: int, message_id: int, *, sent_at: str | None = None):
    from services import accounts, first_dm_delivery, queue

    accounts.upsert_account(
        user_id=account_id,
        session_string=f"session-{account_id}",
        username=f"sender{account_id}",
    )
    accounts.set_participates(account_id, True)
    queue.upsert_from_activity(
        target_user_id=target_id,
        username=f"lead{target_id}",
        source_account_user_id=account_id,
        access_hash=900000 + target_id,
    )
    assert queue.claim_random_pending(account_id)
    assert first_dm_delivery.prepare(target_id, account_id, "Привет, можно вопрос?")
    assert first_dm_delivery.commit_sent(
        target_id,
        telegram_message_id=message_id,
        sent_at=sent_at,
    )


def test_first_dm_schedules_seven_days_from_real_send(app_env, monkeypatch):
    from services import dialog_store

    _enable_auto_delete(monkeypatch)
    sent = dt.datetime.now(dt.timezone.utc).replace(microsecond=0)
    _seed_sent(101, 8101, 18101, sent_at=sent.isoformat())

    dialog = dialog_store.get_dialog(8101)
    assert dialog["last_message_at"] == sent.isoformat()
    due = dt.datetime.fromisoformat(dialog["telegram_delete_at"])
    assert due == sent + dt.timedelta(days=7)


def test_incoming_message_restarts_timer_atomically(app_env, monkeypatch):
    from services import dialog_inbox, dialog_store

    _enable_auto_delete(monkeypatch)
    first = dt.datetime.now(dt.timezone.utc) - dt.timedelta(days=6)
    _seed_sent(102, 8102, 18102, sent_at=first.isoformat())
    incoming = dt.datetime.now(dt.timezone.utc).replace(microsecond=0)

    inbox_id = dialog_inbox.enqueue(
        102,
        8102,
        "а",
        telegram_message_id=501,
        received_at=incoming.isoformat(),
    )
    assert inbox_id is not None
    dialog = dialog_store.get_dialog(8102)
    assert dialog["last_message_at"] == incoming.isoformat()
    assert dt.datetime.fromisoformat(dialog["telegram_delete_at"]) == (
        incoming + dt.timedelta(days=7)
    )
    assert dialog["telegram_delete_next_attempt_at"] is None


def test_outgoing_dialog_message_restarts_timer(app_env, monkeypatch):
    from db.schema import db_lock, get_connection
    from services import dialog_delivery, dialog_store

    _enable_auto_delete(monkeypatch)
    first = dt.datetime.now(dt.timezone.utc) - dt.timedelta(days=6)
    _seed_sent(103, 8103, 18103, sent_at=first.isoformat())
    conn = get_connection()
    with db_lock(), conn:
        conn.execute(
            "UPDATE dialogs SET stage='engaged', auto_link_at=NULL WHERE target_user_id=?",
            (8103,),
        )
    sent = dt.datetime.now(dt.timezone.utc).replace(microsecond=0)
    assert dialog_delivery.prepare(
        8103,
        103,
        "qna:manual",
        "Ответ",
        message_kind=dialog_delivery.KIND_QNA,
        transition={"stage": "engaged", "append_history": True},
    )
    assert dialog_delivery.commit_sent(
        8103,
        "qna:manual",
        telegram_message_id=28103,
        sent_at=sent.isoformat(),
    )
    dialog = dialog_store.get_dialog(8103)
    assert dialog["last_message_at"] == sent.isoformat()
    assert dt.datetime.fromisoformat(dialog["telegram_delete_at"]) == (
        sent + dt.timedelta(days=7)
    )


def test_pending_inbox_blocks_deletion_before_account_connection(app_env, monkeypatch):
    from db.schema import db_lock, get_connection
    from services import dialog_inbox, monitor, retention

    _enable_auto_delete(monkeypatch)
    old = dt.datetime.now(dt.timezone.utc) - dt.timedelta(days=8)
    _seed_sent(104, 8104, 18104, sent_at=old.isoformat())
    assert dialog_inbox.enqueue(
        104,
        8104,
        "ещё не обработано",
        telegram_message_id=502,
        received_at=old.isoformat(),
    )
    conn = get_connection()
    past = (dt.datetime.now(dt.timezone.utc) - dt.timedelta(minutes=1)).isoformat()
    with db_lock(), conn:
        conn.execute(
            "UPDATE dialogs SET auto_link_at=NULL, telegram_delete_at=?, telegram_delete_next_attempt_at=NULL WHERE target_user_id=?",
            (past, 8104),
        )

    calls = {"client": 0}

    def get_client(_uid):
        calls["client"] += 1
        raise AssertionError("blocked deletion must not connect to Telegram")

    monkeypatch.setattr(monitor, "get_client", get_client)
    assert asyncio.run(retention.process_due_telegram_deletions()) == 0
    assert calls["client"] == 0
    row = conn.execute(
        "SELECT telegram_delete_next_attempt_at, telegram_delete_last_error FROM dialogs WHERE target_user_id=?",
        (8104,),
    ).fetchone()
    assert row["telegram_delete_next_attempt_at"] is not None
    assert row["telegram_delete_last_error"] == "pending_inbox"


def test_prepared_outbox_blocks_deletion(app_env, monkeypatch):
    from db.schema import db_lock, get_connection
    from services import dialog_delivery, monitor, retention

    _enable_auto_delete(monkeypatch)
    old = dt.datetime.now(dt.timezone.utc) - dt.timedelta(days=8)
    _seed_sent(105, 8105, 18105, sent_at=old.isoformat())
    conn = get_connection()
    with db_lock(), conn:
        conn.execute(
            "UPDATE dialogs SET stage='engaged', auto_link_at=NULL WHERE target_user_id=?",
            (8105,),
        )
    assert dialog_delivery.prepare(
        8105,
        105,
        "qna:pending",
        "Подготовленный ответ",
        message_kind=dialog_delivery.KIND_QNA,
        transition={"stage": "engaged"},
    )
    with db_lock(), conn:
        conn.execute(
            "UPDATE dialogs SET telegram_delete_at=?, telegram_delete_next_attempt_at=NULL WHERE target_user_id=?",
            ((dt.datetime.now(dt.timezone.utc) - dt.timedelta(minutes=1)).isoformat(), 8105),
        )

    monkeypatch.setattr(
        monitor,
        "get_client",
        lambda _uid: (_ for _ in ()).throw(
            AssertionError("blocked deletion must not connect to Telegram")
        ),
    )
    assert asyncio.run(retention.process_due_telegram_deletions()) == 0
    row = conn.execute(
        "SELECT telegram_delete_last_error FROM dialogs WHERE target_user_id=?",
        (8105,),
    ).fetchone()
    assert row["telegram_delete_last_error"] == "pending_outbox"


def test_newer_activity_repairs_stale_due_without_deleting(app_env, monkeypatch):
    from db.schema import db_lock, get_connection
    from services import monitor, retention

    _enable_auto_delete(monkeypatch)
    recent = dt.datetime.now(dt.timezone.utc) - dt.timedelta(days=1)
    _seed_sent(106, 8106, 18106, sent_at=recent.isoformat())
    conn = get_connection()
    with db_lock(), conn:
        conn.execute(
            "UPDATE dialogs SET auto_link_at=NULL, telegram_delete_at=? WHERE target_user_id=?",
            ((dt.datetime.now(dt.timezone.utc) - dt.timedelta(minutes=1)).isoformat(), 8106),
        )
    monkeypatch.setattr(
        monitor,
        "get_client",
        lambda _uid: (_ for _ in ()).throw(
            AssertionError("stale deadline must be repaired before Telegram access")
        ),
    )
    assert asyncio.run(retention.process_due_telegram_deletions()) == 0
    row = conn.execute(
        "SELECT telegram_delete_at, telegram_delete_next_attempt_at FROM dialogs WHERE target_user_id=?",
        (8106,),
    ).fetchone()
    assert dt.datetime.fromisoformat(row["telegram_delete_at"]) == (
        recent + dt.timedelta(days=7)
    )
    assert row["telegram_delete_next_attempt_at"] is None


def test_safe_dialog_deleted_for_both_and_identity_metadata_kept(app_env, monkeypatch):
    from db.schema import db_lock, get_connection
    from services import dialog_store, monitor, retention

    _enable_auto_delete(monkeypatch, for_both=True)
    old = dt.datetime.now(dt.timezone.utc) - dt.timedelta(days=8)
    _seed_sent(107, 8107, 18107, sent_at=old.isoformat())
    conn = get_connection()
    with db_lock(), conn:
        conn.execute(
            """
            UPDATE dialogs
               SET stage='closed', auto_link_at=NULL, last_message_at=?, telegram_delete_at=?,
                   telegram_delete_next_attempt_at=NULL
             WHERE target_user_id=?
            """,
            (old.isoformat(), (old + dt.timedelta(days=7)).isoformat(), 8107),
        )

    class Message:
        def __init__(self, mid: int):
            self.id = mid
            self.date = old

    class Client:
        def __init__(self):
            self.deleted = []

        def is_connected(self):
            return True

        async def get_input_entity(self, value):
            return value

        async def iter_messages(self, entity, min_id=0):
            for mid in (18110, 18109, 18108, 18107):
                if mid > min_id:
                    yield Message(mid)

        async def delete_messages(self, entity, ids, revoke=False):
            self.deleted.append((list(ids), bool(revoke)))

    client = Client()
    monkeypatch.setattr(monitor, "get_client", lambda _uid: client)
    assert asyncio.run(retention.process_due_telegram_deletions()) == 1
    assert client.deleted == [([18107, 18108, 18109, 18110], True)]

    dialog = dialog_store.get_dialog(8107)
    assert dialog["telegram_deleted_at"] is not None
    assert conn.execute(
        "SELECT status FROM leads WHERE target_user_id=?", (8107,)
    ).fetchone()["status"] == "sent"
    assert conn.execute(
        "SELECT COUNT(*) AS c FROM first_dm_events WHERE target_user_id=?", (8107,)
    ).fetchone()["c"] == 1
    assert conn.execute(
        "SELECT sender_account_id FROM contacts WHERE target_user_id=?", (8107,)
    ).fetchone()["sender_account_id"] == 107


def test_retention_menu_describes_inactivity_policy():
    from pathlib import Path

    source = Path("handlers/menu.py").read_text(encoding="utf-8")
    assert "дней без активности" in source
    assert "Новое сообщение запускает отсчёт заново" in source
    assert "Pending inbox и crash-safe outbox защищены" in source
