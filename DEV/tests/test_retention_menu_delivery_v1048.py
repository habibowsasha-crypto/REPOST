"""Regression coverage for v1.0.48 menu, retention and scheduled outbox."""

from __future__ import annotations

import asyncio
import datetime as dt


def _seed_sent_dialog(account_id: int, target_id: int, text: str = "Можно спросить?"):
    from services import accounts, first_dm_delivery, queue

    accounts.upsert_account(
        user_id=account_id,
        session_string="session",
        username=f"sender{account_id}",
    )
    accounts.set_participates(account_id, True)
    queue.upsert_from_activity(
        target_user_id=target_id,
        username=f"lead{target_id}",
        source_account_user_id=account_id,
        access_hash=123456,
    )
    assert queue.claim_random_pending(account_id)
    assert first_dm_delivery.prepare(target_id, account_id, text)
    assert first_dm_delivery.commit_sent(target_id, telegram_message_id=1000 + target_id)


def test_first_dm_commit_sets_retention_and_all_time_counter(app_env):
    from services import dialog_store, first_dm_delivery, queue

    _seed_sent_dialog(10, 201)
    dialog = dialog_store.get_dialog(201)
    first = dt.datetime.fromisoformat(dialog["first_dm_at"])
    tg = dt.datetime.fromisoformat(dialog["telegram_delete_at"])
    local = dt.datetime.fromisoformat(dialog["history_purge_at"])
    assert (tg - first).days == 30
    assert (local - first).days == 180
    assert queue.count_first_dm_total() == 1

    # Idempotent commit must not count the same delivery twice.
    assert first_dm_delivery.commit_sent(201, telegram_message_id=1201)
    assert queue.count_first_dm_total() == 1


def test_scheduled_auto_link_outbox_commits_atomically(app_env):
    from db.schema import db_lock, get_connection
    from services import dialog_delivery, dialog_store

    _seed_sent_dialog(10, 202)
    conn = get_connection()
    now = dt.datetime.now(dt.timezone.utc).isoformat()
    with db_lock(), conn:
        conn.execute(
            "UPDATE dialogs SET stage=?, auto_link_at=? WHERE target_user_id=?",
            (dialog_store.STAGE_EXPLAINED, now, 202),
        )

    assert dialog_delivery.prepare(202, 10, dialog_delivery.KIND_AUTO_LINK, "Вот ссылка")
    assert not dialog_delivery.prepare(202, 10, dialog_delivery.KIND_AUTO_LINK, "Дубль")
    assert dialog_delivery.commit_sent(
        202,
        dialog_delivery.KIND_AUTO_LINK,
        telegram_message_id=777,
    )
    dialog = dialog_store.get_dialog(202)
    assert dialog["stage"] == dialog_store.STAGE_LINK_SENT
    assert int(dialog["link_sent"]) == 1
    assert dialog["history"][-1]["text"] == "Вот ссылка"


def test_scheduled_followup_recovery_checks_telegram(app_env, monkeypatch):
    from db.schema import db_lock, get_connection
    from services import dialog_delivery, dialog_engine, dialog_store, monitor

    _seed_sent_dialog(10, 203)
    old = (dt.datetime.now(dt.timezone.utc) - dt.timedelta(minutes=5)).isoformat()
    conn = get_connection()
    with db_lock(), conn:
        conn.execute(
            "UPDATE dialogs SET stage=?, auto_link_at=? WHERE target_user_id=?",
            (dialog_store.STAGE_WAITING_REPLY, old, 203),
        )
    assert dialog_delivery.prepare(203, 10, dialog_delivery.KIND_FOLLOWUP, "Ты тут?")
    with db_lock(), conn:
        conn.execute(
            "UPDATE dialog_outbox SET prepared_at=? WHERE target_user_id=?",
            (old, 203),
        )

    class Message:
        out = True
        message = "Ты тут?"
        id = 880
        date = dt.datetime.now(dt.timezone.utc)

    class Client:
        def is_connected(self):
            return True

        async def get_input_entity(self, value):
            return value

        async def get_messages(self, entity, limit=40):
            return [Message()]

    monkeypatch.setattr(monitor, "get_client", lambda uid: Client())
    assert asyncio.run(dialog_engine.recover_ambiguous_scheduled_messages()) == 1
    dialog = dialog_store.get_dialog(203)
    assert dialog["stage"] == dialog_store.STAGE_FOLLOWUP_SENT
    assert dialog["history"][-1]["text"] == "Ты тут?"


def test_local_history_purge_keeps_metadata(app_env):
    import json

    from db.schema import db_lock, get_connection
    from services import dialog_delivery, dialog_store, retention

    _seed_sent_dialog(10, 204)
    conn = get_connection()
    old = (dt.datetime.now(dt.timezone.utc) - dt.timedelta(days=181)).isoformat()
    due = (dt.datetime.now(dt.timezone.utc) - dt.timedelta(days=1)).isoformat()
    with db_lock(), conn:
        conn.execute(
            """
            UPDATE dialogs
               SET first_dm_at=?, history_purge_at=?, history_json=?,
                   stage=?, auto_link_at=?
             WHERE target_user_id=?
            """,
            (
                old,
                due,
                json.dumps([{"role": "assistant", "text": "Можно спросить?", "at": old}]),
                dialog_store.STAGE_EXPLAINED,
                due,
                204,
            ),
        )
        conn.execute(
            """
            UPDATE first_dm_outbox SET prepared_at=?, sent_at=?
             WHERE target_user_id=?
            """,
            (old, old, 204),
        )
    assert dialog_delivery.prepare(204, 10, dialog_delivery.KIND_AUTO_LINK, "Текст ссылки")

    assert retention.process_due_local_history_purge() == 1
    dialog = dialog_store.get_dialog(204)
    assert dialog["history"] == []
    assert dialog["history_purged_at"] is None
    assert dialog["history_purge_at"] is not None
    assert int(dialog["account_user_id"]) == 10
    assert dialog["first_dm_at"] is not None
    first_text = conn.execute(
        "SELECT text FROM first_dm_outbox WHERE target_user_id=204"
    ).fetchone()["text"]
    auto_text = conn.execute(
        "SELECT text FROM dialog_outbox WHERE target_user_id=204"
    ).fetchone()["text"]
    assert first_text == ""
    assert auto_text == "Текст ссылки"

def test_telegram_retention_deletes_from_first_dm_for_both_sides(app_env, monkeypatch):
    from db.schema import db_lock, get_connection
    from services import dialog_store, monitor, retention

    _seed_sent_dialog(10, 205)
    conn = get_connection()
    past = (dt.datetime.now(dt.timezone.utc) - dt.timedelta(days=1)).isoformat()
    with db_lock(), conn:
        conn.execute(
            """
            UPDATE dialogs
               SET telegram_delete_at=?, first_dm_message_id=?
             WHERE target_user_id=?
            """,
            (past, 1205, 205),
        )

    class Msg:
        def __init__(self, mid):
            self.id = mid

    class AsyncMessages:
        def __init__(self, ids):
            self.ids = ids

        def __aiter__(self):
            self._it = iter(self.ids)
            return self

        async def __anext__(self):
            try:
                return Msg(next(self._it))
            except StopIteration:
                raise StopAsyncIteration

    class Client:
        deleted = []

        def is_connected(self):
            return True

        async def get_input_entity(self, value):
            return value

        def iter_messages(self, entity, min_id=0):
            assert min_id == 1204
            return AsyncMessages([1208, 1207, 1206, 1205])

        async def delete_messages(self, entity, ids, revoke=False):
            self.deleted.append((list(ids), revoke))

    client = Client()
    monkeypatch.setattr(monitor, "get_client", lambda uid: client)
    assert asyncio.run(retention.process_due_telegram_deletions()) == 1
    assert client.deleted == [([1205, 1206, 1207, 1208], True)]
    dialog = dialog_store.get_dialog(205)
    assert dialog["telegram_deleted_at"] is not None
    assert dialog["stage"] == dialog_store.STAGE_CLOSED


def test_main_dashboard_has_approved_counters_and_accounts(app_env):
    from services import accounts, queue

    _seed_sent_dialog(10, 206)
    block = accounts.dashboard_accounts_block()
    assert "@sender10" in block
    assert "FIRST DM ВКЛЮЧЕНЫ" in block.upper()
    assert queue.count_first_dm_total() == 1

    source = __import__("pathlib").Path("handlers/menu.py").read_text(encoding="utf-8")
    assert "Отправлено сегодня" in source
    assert "Отправлено всего" in source
    assert "Активные диалоги продолжаются всегда" in source


def test_today_counter_survives_account_deletion(app_env):
    from services import accounts, queue

    _seed_sent_dialog(10, 207)
    assert queue.count_first_dm_today() == 1
    assert accounts.delete_account(10)
    assert queue.count_first_dm_today() == 1
    assert queue.count_first_dm_total() == 1


def test_account_retention_warning_counter(app_env):
    from services import retention

    _seed_sent_dialog(10, 208)
    assert retention.count_pending_for_account(10) == 1


def test_local_purge_removes_old_phrase_text(app_env):
    import datetime as dt
    from db.schema import db_lock, get_connection
    from services import retention

    conn = get_connection()
    old = (dt.datetime.now(dt.timezone.utc) - dt.timedelta(days=181)).isoformat()
    with db_lock(), conn:
        conn.execute(
            "INSERT INTO sent_phrases(kind, text, created_at) VALUES(?, ?, ?)",
            ("first_dm", "старый текст", old),
        )
    retention.process_due_local_history_purge()
    row = conn.execute("SELECT COUNT(*) AS c FROM sent_phrases").fetchone()
    assert int(row["c"]) == 0
