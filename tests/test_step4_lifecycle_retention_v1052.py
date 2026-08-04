"""Regression coverage for Step 4 lifecycle and efficient retention."""

from __future__ import annotations

import asyncio
import datetime as dt
from pathlib import Path


def _seed_sent(account_id: int, target_id: int, message_id: int) -> None:
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
        access_hash=123456 + target_id,
    )
    assert queue.claim_random_pending(account_id)
    assert first_dm_delivery.prepare(target_id, account_id, "Можно спросить?")
    assert first_dm_delivery.commit_sent(target_id, telegram_message_id=message_id)


def test_terminal_dialog_does_not_keep_disabled_account_connected(app_env, monkeypatch):
    from services import accounts, dialog_store, monitor

    class FakeClient:
        def __init__(self, *args, **kwargs):
            self.connected = False

        async def connect(self):
            self.connected = True

        async def is_user_authorized(self):
            return True

        def on(self, *args, **kwargs):
            return lambda func: func

        def is_connected(self):
            return self.connected

        async def disconnect(self):
            self.connected = False

    monkeypatch.setattr(monitor, "TelegramClient", FakeClient)
    accounts.upsert_account(user_id=10, session_string="session")
    accounts.set_participates(10, False)
    dialog_store.create_after_first_dm(501, 10, "Первое")

    asyncio.run(monitor.start_monitor())
    assert 10 in monitor.connected_account_ids()
    assert dialog_store.count_open_for_account(10) == 1

    dialog_store.set_stage(501, dialog_store.STAGE_FOLLOWUP_SENT, clear_auto_link=True)
    dialog = dialog_store.get_dialog(501)
    assert dialog["lifecycle_completed_at"] is not None
    assert dialog_store.count_open_for_account(10) == 0
    assert dialog_store.count_active() == 0
    assert [row["target_user_id"] for row in dialog_store.list_recent(active_only=True)] == []
    assert 501 in [row["target_user_id"] for row in dialog_store.list_recent_closed()]

    assert asyncio.run(monitor.maybe_disconnect_inactive_account(10)) is True
    assert 10 not in monitor.connected_account_ids()


def test_terminal_delivery_uses_one_completion_timestamp(app_env):
    from services import dialog_delivery, dialog_store

    _seed_sent(11, 502, 1502)
    dialog_store.set_stage(502, dialog_store.STAGE_EXPLAINED)
    assert dialog_delivery.prepare(
        502, 11, dialog_delivery.KIND_AUTO_LINK, "https://t.me/+testhash"
    )
    assert dialog_delivery.commit_sent(
        502, dialog_delivery.KIND_AUTO_LINK, telegram_message_id=2502
    )
    dialog = dialog_store.get_dialog(502)
    first_completed = dialog["lifecycle_completed_at"]
    assert first_completed is not None
    assert dialog_store.count_active() == 0
    assert dialog_store.count_closed_today() == 1

    # Retention/status touches must not rewrite when the funnel was completed.
    dialog_store.set_stage(502, dialog_store.STAGE_LINK_SENT, clear_auto_link=True)
    assert dialog_store.get_dialog(502)["lifecycle_completed_at"] == first_completed


def test_retention_reuses_one_temporary_client_per_account(app_env, monkeypatch):
    from db.schema import db_lock, get_connection
    from services import monitor, retention

    _seed_sent(12, 503, 1503)
    _seed_sent(12, 504, 1504)
    past = (dt.datetime.now(dt.timezone.utc) - dt.timedelta(minutes=1)).isoformat()
    conn = get_connection()
    with db_lock(), conn:
        conn.execute(
            "UPDATE dialogs SET telegram_delete_at=? WHERE target_user_id IN (?, ?)",
            (past, 503, 504),
        )

    class Message:
        def __init__(self, mid: int):
            self.id = mid
            self.date = dt.datetime.now(dt.timezone.utc)

    class Client:
        def __init__(self):
            self.disconnected = 0
            self.deleted: list[tuple[int, list[int]]] = []

        def is_connected(self):
            return True

        async def get_input_entity(self, value):
            return value

        async def iter_messages(self, entity, min_id=0):
            yield Message(int(min_id) + 1)

        async def delete_messages(self, entity, ids, revoke=True):
            self.deleted.append((int(entity), list(ids)))

        async def disconnect(self):
            self.disconnected += 1

    client = Client()
    opened = {"count": 0}

    async def temporary(session):
        opened["count"] += 1
        return client

    monkeypatch.setattr(monitor, "get_client", lambda uid: None)
    monkeypatch.setattr(retention, "_temporary_client", temporary)
    monkeypatch.setattr(monitor, "maybe_disconnect_inactive_account", lambda uid: _noop())

    assert asyncio.run(retention.process_due_telegram_deletions(limit=10)) == 2
    assert opened["count"] == 1
    assert client.disconnected == 1
    assert len(client.deleted) == 2


def test_retention_streams_large_dialog_in_chunks(app_env):
    from services import retention

    class Message:
        def __init__(self, mid: int):
            self.id = mid
            self.date = dt.datetime.now(dt.timezone.utc)

    class Client:
        def __init__(self):
            self.batches: list[list[int]] = []

        async def iter_messages(self, entity, min_id=0):
            for mid in range(250, 0, -1):
                if mid > min_id:
                    yield Message(mid)

        async def delete_messages(self, entity, ids, revoke=True):
            self.batches.append(list(ids))

    client = Client()
    deleted = asyncio.run(
        retention._delete_attempt_messages(client, 1, 1, {}, batch_size=100)
    )
    assert deleted == 250
    assert [len(batch) for batch in client.batches] == [100, 100, 50]
    assert all(len(batch) <= 100 for batch in client.batches)


def test_deleted_account_retention_becomes_terminal_not_infinite(app_env):
    from services import accounts, dialog_store, retention

    _seed_sent(13, 505, 1505)
    assert accounts.delete_account(13)
    dialog = dialog_store.get_dialog(505)
    assert dialog["stage"] == dialog_store.STAGE_CLOSED
    assert dialog["telegram_delete_abandoned_at"] is not None
    assert dialog["telegram_delete_next_attempt_at"] is None
    assert retention.count_pending_for_account(13) == 0
    stats = retention.retention_stats()
    assert stats["telegram_abandoned"] == 1
    assert retention._list_due_telegram(limit=10) == []


def test_main_cancels_background_due_loop_on_shutdown(app_env):
    source = Path("main.py").read_text(encoding="utf-8")
    assert "background_task.cancel()" in source
    assert "asyncio.gather(background_task, return_exceptions=True)" in source


async def _noop():
    return None


def test_retention_failure_schedules_retry_without_sql_error(app_env, monkeypatch):
    from db.schema import db_lock, get_connection
    from services import monitor, retention

    _seed_sent(14, 506, 1506)
    past = (dt.datetime.now(dt.timezone.utc) - dt.timedelta(minutes=1)).isoformat()
    conn = get_connection()
    with db_lock(), conn:
        conn.execute(
            "UPDATE dialogs SET telegram_delete_at=? WHERE target_user_id=?",
            (past, 506),
        )

    class Client:
        def is_connected(self):
            return True

        async def get_input_entity(self, value):
            raise RuntimeError("entity unavailable")

    monkeypatch.setattr(monitor, "get_client", lambda uid: Client())
    monkeypatch.setattr(monitor, "maybe_disconnect_inactive_account", lambda uid: _noop())

    assert asyncio.run(retention.process_due_telegram_deletions(limit=10)) == 0
    row = conn.execute(
        """
        SELECT telegram_delete_attempts, telegram_delete_next_attempt_at,
               telegram_delete_last_error, telegram_delete_abandoned_at
          FROM dialogs WHERE target_user_id=?
        """,
        (506,),
    ).fetchone()
    assert int(row["telegram_delete_attempts"]) == 1
    assert row["telegram_delete_next_attempt_at"] is not None
    assert "entity unavailable" in str(row["telegram_delete_last_error"])
    assert row["telegram_delete_abandoned_at"] is None
