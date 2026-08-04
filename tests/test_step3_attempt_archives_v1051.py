"""Regression coverage for Step 3: independent dialog attempts and retention."""

from __future__ import annotations

import asyncio
import datetime as dt
import json


def _seed_sent(account_id: int, target_id: int, message_id: int = 1001) -> None:
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
    assert first_dm_delivery.prepare(target_id, account_id, "Первое обращение")
    assert first_dm_delivery.commit_sent(target_id, telegram_message_id=message_id)


def _requeue_and_send_again(account_id: int, target_id: int, message_id: int = 2000) -> None:
    from services import first_dm_delivery, queue

    assert queue.force_requeue(
        target_user_id=target_id,
        username=f"lead{target_id}",
        source_account_user_id=account_id,
        access_hash=123456,
    )
    assert queue.claim_random_pending(account_id)
    assert first_dm_delivery.prepare(target_id, account_id, "Второе обращение")
    assert first_dm_delivery.commit_sent(target_id, telegram_message_id=message_id)


def test_requeue_archives_previous_attempt_and_keeps_retention(app_env):
    from db.schema import get_connection
    from services import dialog_archive, dialog_store, queue

    _seed_sent(10, 301, 1001)
    before = dialog_store.get_dialog(301)
    assert before is not None

    assert queue.force_requeue(
        target_user_id=301,
        username="lead301",
        source_account_user_id=10,
        access_hash=123456,
    )
    assert dialog_store.get_dialog(301) is None
    assert dialog_archive.count_for_target(301) == 1
    archived = dialog_archive.list_for_target(301)[0]
    assert archived["first_dm_at"] == before["first_dm_at"]
    assert archived["telegram_delete_at"] == before["telegram_delete_at"]
    assert archived["history_purge_at"] == before["history_purge_at"]
    assert "Первое обращение" in archived["history_json"]
    assert archived["first_dm_text"] == "Первое обращение"

    conn = get_connection()
    assert conn.execute(
        "SELECT COUNT(*) AS c FROM first_dm_events WHERE target_user_id=301"
    ).fetchone()["c"] == 1


def test_new_attempt_is_separate_and_bounds_old_telegram_cleanup(app_env):
    from db.schema import get_connection
    from services import dialog_archive, dialog_store

    _seed_sent(10, 302, 1100)
    _requeue_and_send_again(10, 302, 2200)

    current = dialog_store.get_dialog(302)
    assert current is not None
    assert current["first_dm_message_id"] == 2200
    assert current["history"][0]["text"] == "Второе обращение"

    archived = dialog_archive.list_for_target(302)[0]
    assert archived["first_dm_message_id"] == 1100
    assert archived["telegram_delete_until_message_id"] == 2199
    assert archived["next_attempt_first_dm_at"] is not None

    conn = get_connection()
    assert conn.execute(
        "SELECT COUNT(*) AS c FROM first_dm_events WHERE target_user_id=302"
    ).fetchone()["c"] == 2


def test_archived_cleanup_never_deletes_new_attempt_messages(app_env, monkeypatch):
    from db.schema import db_lock, get_connection
    from services import dialog_archive, monitor, retention

    _seed_sent(10, 303, 1200)
    _requeue_and_send_again(10, 303, 2300)
    archive = dialog_archive.list_for_target(303)[0]
    past = (dt.datetime.now(dt.timezone.utc) - dt.timedelta(minutes=1)).isoformat()
    conn = get_connection()
    with db_lock(), conn:
        conn.execute(
            "UPDATE dialog_archives SET telegram_delete_at=? WHERE id=?",
            (past, archive["id"]),
        )

    class Message:
        def __init__(self, mid: int):
            self.id = mid
            self.date = dt.datetime.now(dt.timezone.utc)

    class Client:
        def __init__(self):
            self.deleted = []

        def is_connected(self):
            return True

        async def get_input_entity(self, value):
            return value

        async def iter_messages(self, entity, min_id=0):
            for mid in (2400, 2300, 2299, 1200):
                if mid > min_id:
                    yield Message(mid)

        async def delete_messages(self, entity, ids, revoke=True):
            self.deleted.extend(ids)

    client = Client()
    monkeypatch.setattr(monitor, "get_client", lambda uid: client)
    assert asyncio.run(retention.process_due_telegram_deletions()) == 1
    assert client.deleted == [1200, 2299]
    current = conn.execute(
        "SELECT telegram_deleted_at FROM dialogs WHERE target_user_id=303"
    ).fetchone()
    assert current["telegram_deleted_at"] is None
    archived = conn.execute(
        "SELECT telegram_deleted_at FROM dialog_archives WHERE id=?", (archive["id"],)
    ).fetchone()
    assert archived["telegram_deleted_at"] is not None


def test_local_retention_purges_each_message_by_its_own_age(app_env):
    from db.schema import db_lock, get_connection
    from services import dialog_store, retention

    _seed_sent(10, 304, 1300)
    now = dt.datetime.now(dt.timezone.utc)
    old = (now - dt.timedelta(days=181)).isoformat()
    recent = (now - dt.timedelta(days=1)).isoformat()
    due = (now - dt.timedelta(minutes=1)).isoformat()
    history = [
        {"role": "assistant", "text": "Старое", "at": old},
        {"role": "user", "text": "Недавнее", "at": recent},
    ]
    conn = get_connection()
    with db_lock(), conn:
        conn.execute(
            "UPDATE dialogs SET history_json=?, history_purge_at=? WHERE target_user_id=?",
            (json.dumps(history, ensure_ascii=False), due, 304),
        )
        conn.execute(
            "UPDATE first_dm_outbox SET text='', sent_at=? WHERE target_user_id=?",
            (old, 304),
        )

    assert retention.process_due_local_history_purge() == 1
    dialog = dialog_store.get_dialog(304)
    assert [item["text"] for item in dialog["history"]] == ["Недавнее"]
    next_due = dt.datetime.fromisoformat(dialog["history_purge_at"])
    assert 178 <= (next_due - now).days <= 180
    assert dialog["history_purged_at"] is None


def test_archived_local_retention_keeps_recent_snapshot_text(app_env):
    from db.schema import db_lock, get_connection
    from services import dialog_archive, queue, retention

    _seed_sent(10, 305, 1400)
    now = dt.datetime.now(dt.timezone.utc)
    old = (now - dt.timedelta(days=181)).isoformat()
    recent = (now - dt.timedelta(days=2)).isoformat()
    due = (now - dt.timedelta(minutes=1)).isoformat()
    history = [
        {"role": "assistant", "text": "Старый текст", "at": old},
        {"role": "user", "text": "Свежий текст", "at": recent},
    ]
    conn = get_connection()
    with db_lock(), conn:
        conn.execute(
            "UPDATE dialogs SET history_json=?, history_purge_at=? WHERE target_user_id=?",
            (json.dumps(history, ensure_ascii=False), due, 305),
        )
        conn.execute(
            "UPDATE first_dm_outbox SET sent_at=?, prepared_at=? WHERE target_user_id=?",
            (old, old, 305),
        )
    assert queue.force_requeue(target_user_id=305, source_account_user_id=10)
    archive = dialog_archive.list_for_target(305)[0]
    assert retention.process_due_local_history_purge() == 1
    archived = get_connection().execute(
        "SELECT * FROM dialog_archives WHERE id=?", (archive["id"],)
    ).fetchone()
    kept = json.loads(archived["history_json"])
    assert [item["text"] for item in kept] == ["Свежий текст"]
    assert archived["first_dm_text"] == ""
    assert archived["history_purge_at"] is not None
    assert archived["history_purged_at"] is None


def test_unsent_prepared_attempt_gets_180_day_cleanup_when_archived(app_env):
    from services import accounts, dialog_archive, first_dm_delivery, queue

    accounts.upsert_account(user_id=10, session_string="session", username="sender10")
    accounts.set_participates(10, True)
    queue.upsert_from_activity(
        target_user_id=306,
        username="lead306",
        source_account_user_id=10,
        access_hash=123456,
    )
    assert queue.claim_random_pending(10)
    assert first_dm_delivery.prepare(306, 10, "Подготовленный текст")
    assert queue.force_requeue(target_user_id=306, source_account_user_id=10)
    archived = dialog_archive.list_for_target(306)[0]
    assert archived["first_dm_at"] is None
    assert archived["telegram_delete_at"] is None
    assert archived["history_purge_at"] is not None
    assert "Подготовленный текст" in archived["history_json"]


def test_new_attempt_on_different_account_does_not_bound_old_account_chat(app_env):
    from services import accounts, dialog_archive, first_dm_delivery, queue

    _seed_sent(10, 307, 1500)
    assert queue.force_requeue(
        target_user_id=307,
        username="lead307",
        source_account_user_id=20,
        access_hash=654321,
    )
    accounts.upsert_account(user_id=20, session_string="session20", username="sender20")
    accounts.set_participates(20, True)
    assert queue.claim_random_pending(20)
    assert first_dm_delivery.prepare(307, 20, "Сообщение другого аккаунта")
    assert first_dm_delivery.commit_sent(307, telegram_message_id=900)

    archived = dialog_archive.list_for_target(307)[0]
    assert archived["account_user_id"] == 10
    assert archived["telegram_delete_until_message_id"] is None
    assert archived["next_attempt_first_dm_at"] is None
