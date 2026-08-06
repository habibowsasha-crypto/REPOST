"""Global First DM pause and SpamBot recovery regression coverage."""

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


def _pause_account(account_id: int, until: dt.datetime) -> None:
    from db.schema import db_lock, get_connection

    conn = get_connection()
    with db_lock(), conn:
        conn.execute(
            """
            UPDATE accounts
               SET is_paused=1,
                   pause_reason='PeerFlood',
                   cooldown_until=?,
                   next_send_at=?
             WHERE user_id=?
            """,
            (until.isoformat(), until.isoformat(), account_id),
        )


def test_free_pending_account_resumes_dialogs_while_global_first_dm_stays_paused(
    app_env, monkeypatch
):
    from services import accounts, pacing, runtime, spambot

    account_id = 7101
    _seed_account(accounts, account_id)
    now = dt.datetime(2026, 8, 5, 6, 0, tzinfo=dt.timezone.utc)
    _pause_account(account_id, now - dt.timedelta(seconds=1))
    runtime.set_worker_enabled(False)
    monkeypatch.setattr(spambot, "_now", lambda: now)
    monkeypatch.setattr(pacing, "_now", lambda: now)
    monkeypatch.setattr(pacing, "random_account_interval_seconds", lambda acc=None: 240)
    spambot._upsert_state(
        account_id,
        status=spambot.STATUS_FREE_PENDING,
        next_check_at=(now - dt.timedelta(seconds=1)).isoformat(),
        last_reply="free",
    )
    notices: list[str] = []

    async def notify(text: str):
        notices.append(text)

    async def refresh():
        return None

    monkeypatch.setattr(spambot, "notify_admins", notify)
    monkeypatch.setattr(spambot.monitor_svc, "refresh_monitor", refresh)

    actions = asyncio.run(spambot.process_due_checks())

    row = accounts.get_account(account_id)
    state = spambot.get_state(account_id)
    assert actions == 1
    assert runtime.is_worker_enabled() is False
    assert row["is_paused"] == 0
    assert row["pause_reason"] is None
    assert row["cooldown_until"] is None
    next_send = dt.datetime.fromisoformat(str(row["next_send_at"]).replace("Z", "+00:00"))
    assert next_send == now + dt.timedelta(seconds=240)
    assert pacing.account_cooldown_seconds(account_id) == 0
    assert state["status"] == spambot.STATUS_IDLE
    assert state["next_check_at"] is None
    assert "Общая рассылка First DM остаётся на паузе" in notices[-1]

def test_auto_resume_keeps_first_dm_hold_but_unblocks_dialogs(app_env, monkeypatch):
    from services import accounts, pacing, runtime, spambot

    account_id = 7102
    _seed_account(accounts, account_id)
    now = dt.datetime(2026, 8, 5, 6, 10, tzinfo=dt.timezone.utc)
    _pause_account(account_id, now)
    runtime.set_worker_enabled(True)
    monkeypatch.setattr(spambot, "_now", lambda: now)
    monkeypatch.setattr(pacing, "_now", lambda: now)
    monkeypatch.setattr(pacing, "random_account_interval_seconds", lambda acc=None: 240)

    async def notify(text: str):
        return None

    async def refresh():
        return None

    monkeypatch.setattr(spambot, "notify_admins", notify)
    monkeypatch.setattr(spambot.monitor_svc, "refresh_monitor", refresh)

    asyncio.run(spambot.resume_account(account_id, source="spambot_auto"))

    row = accounts.get_account(account_id)
    expected = now + dt.timedelta(seconds=240)
    next_send = dt.datetime.fromisoformat(str(row["next_send_at"]).replace("Z", "+00:00"))
    assert row["is_paused"] == 0
    assert row["cooldown_until"] is None
    assert next_send == expected
    assert pacing.account_cooldown_seconds(account_id) == 0


def test_manual_resume_during_global_pause_does_not_claim_first_dm_is_running(
    app_env, monkeypatch
):
    from services import accounts, runtime, spambot

    account_id = 7103
    _seed_account(accounts, account_id)
    now = dt.datetime(2026, 8, 5, 6, 20, tzinfo=dt.timezone.utc)
    _pause_account(account_id, now + dt.timedelta(minutes=3))
    runtime.set_worker_enabled(False)
    monkeypatch.setattr(spambot, "_now", lambda: now)
    notices: list[str] = []

    async def notify(text: str):
        notices.append(text)

    async def refresh():
        return None

    monkeypatch.setattr(spambot, "notify_admins", notify)
    monkeypatch.setattr(spambot.monitor_svc, "refresh_monitor", refresh)

    asyncio.run(spambot.resume_account(account_id, source="manual"))

    assert accounts.get_account(account_id)["is_paused"] == 0
    assert "Общая рассылка First DM остаётся на паузе" in notices[-1]
    assert "FIRST DM АККАУНТА ВОЗОБНОВЛЕНЫ" not in notices[-1]


def test_dispatch_attempt_cannot_send_after_global_pause_is_enabled(app_env, monkeypatch):
    from services import accounts, dispatcher, queue, runtime

    account_id, target_id = 7104, 8104
    account = _seed_account(accounts, account_id)
    queue.upsert_from_activity(
        target_user_id=target_id,
        username=f"target{target_id}",
        first_name="Target",
        source_chat_id=-100123,
        source_account_user_id=account_id,
        access_hash=123456,
    )
    lead = queue.claim_random_pending(account_id)
    assert lead is not None
    runtime.set_worker_enabled(False)
    sends: list[int] = []

    async def send(client, sender_id, current_lead, text, entity=None):
        sends.append(sender_id)
        return "sent"

    monkeypatch.setattr(dispatcher, "_send_first_dm", send)
    monkeypatch.setattr(
        dispatcher.monitor_svc,
        "get_client",
        lambda uid: SimpleNamespace(is_connected=lambda: True),
    )

    result = asyncio.run(
        dispatcher._attempt_lead_across_accounts(
            lead, [account], text="Привет", enforce_global_pause=True
        )
    )

    assert result is False
    assert sends == []
    assert queue.get_lead(target_id)["status"] == queue.STATUS_PENDING


def test_v1079_migration_unblocks_dialogs_left_by_old_global_pause_hold(
    app_env
):
    from db.schema import close_connection, db_lock, get_connection, init_db
    from services import accounts

    account_id = 7105
    _seed_account(accounts, account_id)
    conn = get_connection()
    with db_lock(), conn:
        conn.execute(
            "DELETE FROM schema_migrations WHERE name='v1_0_79_active_dialogs_ignore_global_first_dm_pause'"
        )
        conn.execute(
            """
            INSERT INTO runtime_meta(key, value, updated_at)
            VALUES ('dm_worker_enabled', '0', datetime('now'))
            ON CONFLICT(key) DO UPDATE SET value='0'
            """
        )
        conn.execute(
            """
            INSERT INTO spambot_state(
                account_user_id, status, last_reply, next_check_at, updated_at
            ) VALUES (?, 'free_pending_resume', 'free', datetime('now'), datetime('now'))
            ON CONFLICT(account_user_id) DO UPDATE SET
                status='free_pending_resume', last_reply='free', next_check_at=datetime('now')
            """,
            (account_id,),
        )
        conn.execute(
            """
            UPDATE accounts
               SET is_paused=1, pause_reason='PeerFlood', cooldown_until=datetime('now', '+60 seconds'), next_send_at=datetime('now', '+60 seconds')
             WHERE user_id=?
            """,
            (account_id,),
        )

    close_connection()
    init_db()

    row = accounts.get_account(account_id)
    state = get_connection().execute(
        "SELECT status, next_check_at FROM spambot_state WHERE account_user_id=?",
        (account_id,),
    ).fetchone()
    assert row["is_paused"] == 0
    assert row["pause_reason"] is None
    assert row["cooldown_until"] is None
    assert row["next_send_at"] is not None
    assert state["status"] == "idle"
    assert state["next_check_at"] is None
