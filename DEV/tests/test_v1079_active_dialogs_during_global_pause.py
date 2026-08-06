"""v1.0.79 keeps active dialogs independent from the global First DM switch."""

from __future__ import annotations

import asyncio
import datetime as dt


def test_pending_reply_is_processed_after_spambot_free_while_first_dm_paused(
    app_env, monkeypatch
):
    from services import accounts, dialog_engine, dialog_inbox, dialog_store
    from services import monitor, pacing, runtime, spambot

    account_id = 7901
    target_id = 8901
    accounts.upsert_account(
        user_id=account_id,
        session_string=f"session-{account_id}",
        username="sender7901",
    )
    accounts.set_participates(account_id, True)
    dialog_store.create_after_first_dm(target_id, account_id, "Привет, можно вопрос?")

    now = dt.datetime(2026, 8, 5, 19, 0, tzinfo=dt.timezone.utc)
    with __import__("db.schema", fromlist=["db_lock"]).db_lock(), __import__(
        "db.schema", fromlist=["get_connection"]
    ).get_connection() as conn:
        conn.execute(
            """
            UPDATE accounts
               SET is_paused=1, pause_reason='PeerFlood',
                   cooldown_until=?, next_send_at=?
             WHERE user_id=?
            """,
            (
                (now - dt.timedelta(seconds=1)).isoformat(),
                (now - dt.timedelta(seconds=1)).isoformat(),
                account_id,
            ),
        )

    runtime.set_worker_enabled(False)
    monkeypatch.setattr(spambot, "_now", lambda: now)
    monkeypatch.setattr(pacing, "_now", lambda: now)
    monkeypatch.setattr(pacing, "random_account_interval_seconds", lambda acc=None: 180)

    async def notify(text: str):
        return None

    async def refresh():
        return None

    monkeypatch.setattr(spambot, "notify_admins", notify)
    monkeypatch.setattr(spambot.monitor_svc, "refresh_monitor", refresh)

    processed: list[str] = []

    async def fake_body(account, target, text, **kwargs):
        processed.append(text)

    monkeypatch.setattr(dialog_engine, "_handle_incoming_private_body", fake_body)
    monkeypatch.setattr(monitor, "get_client", lambda account: object())

    dialog_inbox.enqueue(
        account_id,
        target_id,
        "Второе",
        telegram_message_id=2208,
    )
    spambot._upsert_state(
        account_id,
        status=spambot.STATUS_FREE_PENDING,
        next_check_at=(now - dt.timedelta(seconds=1)).isoformat(),
        last_reply="free",
    )

    async def scenario():
        assert await spambot.process_due_checks() == 1
        assert runtime.is_worker_enabled() is False
        assert pacing.account_cooldown_seconds(account_id) == 0
        assert await dialog_engine.recover_pending_incoming_messages(limit=10) == 1

    asyncio.run(scenario())

    assert processed == ["Второе"]
    assert dialog_inbox.count_by_status(dialog_inbox.STATUS_DONE) == 1
    row = accounts.get_account(account_id)
    assert row["cooldown_until"] is None
    assert row["next_send_at"] is not None


def test_global_pause_still_blocks_new_first_dm_after_dialog_unblock(app_env, monkeypatch):
    from services import accounts, dispatcher, queue, runtime

    account_id = 7902
    target_id = 8902
    accounts.upsert_account(
        user_id=account_id,
        session_string=f"session-{account_id}",
        username="sender7902",
    )
    accounts.set_participates(account_id, True)
    queue.upsert_from_activity(
        target_user_id=target_id,
        username="target8902",
        first_name="Target",
        access_hash=123456,
        source_chat_id=-1007902,
        source_account_user_id=account_id,
    )
    lead = queue.claim_random_pending(account_id)
    assert lead is not None
    runtime.set_worker_enabled(False)
    sent: list[int] = []

    async def fake_send(*args, **kwargs):
        sent.append(1)
        return "sent"

    monkeypatch.setattr(dispatcher, "_send_first_dm", fake_send)
    result = asyncio.run(
        dispatcher._attempt_lead_across_accounts(
            lead,
            [accounts.get_account(account_id)],
            text="Привет",
            enforce_global_pause=True,
        )
    )

    assert result is False
    assert sent == []
    assert queue.get_lead(target_id)["status"] == queue.STATUS_PENDING
