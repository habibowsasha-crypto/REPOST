"""v1.0.72 resets recovered PeerFlood series and deduplicates SpamBot checks."""

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


def _clear_local_pause(account_id: int) -> None:
    from db.schema import db_lock, get_connection

    conn = get_connection()
    with db_lock(), conn:
        conn.execute(
            """
            UPDATE accounts
               SET is_paused=0,
                   pause_reason=NULL,
                   cooldown_until=NULL,
                   next_send_at=NULL
             WHERE user_id=?
            """,
            (int(account_id),),
        )


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


def test_repeated_peerflood_within_ten_minutes_does_not_restart_spambot(
    app_env, monkeypatch
):
    from services import accounts, pacing, runtime, spambot

    account_id = 7201
    _seed_account(accounts, account_id)
    runtime.set_worker_enabled(True)
    runtime.set_peer_flood_range_seconds(60, 60)

    clock = [dt.datetime(2026, 8, 5, 7, 0, tzinfo=dt.timezone.utc)]
    monkeypatch.setattr(spambot, "_now", lambda: clock[0])
    monkeypatch.setattr(accounts, "_now", lambda: clock[0])
    monkeypatch.setattr(accounts, "_now_iso", lambda: clock[0].isoformat())

    checks: list[int] = []
    notices: list[str] = []

    async def fake_check(uid: int, *, force: bool = False):
        checks.append(int(uid))
        return {"result": "free"}

    async def notify(text: str):
        notices.append(text)

    monkeypatch.setattr(spambot, "check_account", fake_check)
    monkeypatch.setattr(spambot, "notify_admins", notify)

    asyncio.run(spambot.on_peer_flood(account_id))
    assert checks == [account_id]
    assert "проверка запущена" in notices[-1]

    # Simulate the account having completed its first local recovery. The
    # rolling peerflood_last_at is intentionally preserved.
    _clear_local_pause(account_id)
    spambot._upsert_state(
        account_id,
        status=spambot.STATUS_IDLE,
        last_reply="resumed:spambot_auto",
        next_check_at=None,
        limited_until=None,
    )
    clock[0] += dt.timedelta(minutes=2)

    notices_before_repeat = len(notices)
    asyncio.run(spambot.on_peer_flood(account_id))

    assert checks == [account_id]
    assert len(notices) == notices_before_repeat
    state = spambot.get_state(account_id)
    assert state["status"] == spambot.STATUS_FREE_PENDING
    assert state["last_reply"] == "peerflood_repeat_spambot_check_suppressed"
    due = dt.datetime.fromisoformat(str(state["next_check_at"]).replace("Z", "+00:00"))
    assert due == clock[0] + dt.timedelta(seconds=60)
    row = accounts.get_account(account_id)
    assert row["is_paused"] == 1
    assert row["pause_reason"] == "PeerFlood"
    assert int(row["peerflood_streak"]) == 2

    # The suppressed check must not strand the account. At local cooldown end it
    # follows the normal automatic-resume path and receives the existing 2-7
    # minute account interval without another SpamBot request.
    monkeypatch.setattr(pacing, "_now", lambda: clock[0])
    monkeypatch.setattr(pacing, "random_account_interval_seconds", lambda acc=None: 180)

    async def refresh():
        return None

    monkeypatch.setattr(spambot.monitor_svc, "refresh_monitor", refresh)
    clock[0] += dt.timedelta(seconds=60)
    actions = asyncio.run(spambot.process_due_checks())
    resumed = accounts.get_account(account_id)
    assert actions == 1
    assert resumed["is_paused"] == 0
    next_send = dt.datetime.fromisoformat(
        str(resumed["next_send_at"]).replace("Z", "+00:00")
    )
    assert next_send == clock[0] + dt.timedelta(seconds=180)
    assert checks == [account_id]


def test_repeated_peerflood_does_not_overwrite_real_limited_spambot_state(
    app_env, monkeypatch
):
    from services import accounts, runtime, spambot

    account_id = 7202
    _seed_account(accounts, account_id)
    runtime.set_peer_flood_range_seconds(60, 60)
    clock = [dt.datetime(2026, 8, 5, 7, 10, tzinfo=dt.timezone.utc)]
    monkeypatch.setattr(spambot, "_now", lambda: clock[0])
    monkeypatch.setattr(accounts, "_now", lambda: clock[0])
    monkeypatch.setattr(accounts, "_now_iso", lambda: clock[0].isoformat())

    # Establish a recent real PeerFlood, then emulate a later official limited
    # result that must remain authoritative.
    accounts.register_peerflood_hit(account_id)
    _clear_local_pause(account_id)
    limited_until = clock[0] + dt.timedelta(hours=2)
    spambot._upsert_state(
        account_id,
        status=spambot.STATUS_LIMITED,
        last_reply="official limited",
        next_check_at=limited_until.isoformat(),
        limited_until=limited_until.isoformat(),
    )
    clock[0] += dt.timedelta(minutes=1)
    checks: list[int] = []

    async def fake_check(uid: int, *, force: bool = False):
        checks.append(int(uid))
        return None

    async def notify(text: str):
        return None

    monkeypatch.setattr(spambot, "check_account", fake_check)
    monkeypatch.setattr(spambot, "notify_admins", notify)
    asyncio.run(spambot.on_peer_flood(account_id))

    state = spambot.get_state(account_id)
    assert checks == []
    assert state["status"] == spambot.STATUS_LIMITED
    assert state["limited_until"] == limited_until.isoformat()


def test_successful_first_dm_clears_complete_peerflood_series(app_env, monkeypatch):
    from db.schema import get_connection
    from services import accounts, dispatcher, queue

    account_id, target_id = 7203, 8203
    _seed_account(accounts, account_id)
    # Preserve every kind of stale series state, including the burst marker.
    for _ in range(3):
        accounts.register_peerflood_hit(account_id)
    conn = get_connection()
    with conn:
        conn.execute(
            "UPDATE accounts SET peerflood_burst_applied_at=? WHERE user_id=?",
            (dt.datetime.now(dt.timezone.utc).isoformat(), account_id),
        )
    lead = _seed_claimed_lead(queue, account_id, target_id)

    class Client:
        async def send_message(self, entity, text):
            return SimpleNamespace(
                id=172,
                date=dt.datetime.now(dt.timezone.utc),
            )

    async def notify(account, current_lead, text):
        return None

    monkeypatch.setattr(dispatcher, "_notify_admins_first_dm", notify)
    result = asyncio.run(
        dispatcher._send_first_dm(
            Client(), account_id, lead, "Привет, можно вопрос?", entity=object()
        )
    )

    assert result == "sent"
    row = accounts.get_account(account_id)
    assert int(row["peerflood_streak"]) == 0
    assert row["peerflood_last_at"] is None
    assert row["peerflood_window_started_at"] is None
    assert row["peerflood_burst_applied_at"] is None
    hit_count = conn.execute(
        "SELECT COUNT(*) AS c FROM peerflood_hits WHERE account_user_id=?",
        (account_id,),
    ).fetchone()["c"]
    assert int(hit_count) == 0
    # Normal successful-send pacing remains intact.
    assert row["last_send_at"] is not None
    assert row["next_send_at"] is not None


def test_next_peerflood_after_success_starts_fresh_at_one_of_five(app_env, monkeypatch):
    from services import accounts, runtime, spambot

    account_id = 7204
    _seed_account(accounts, account_id)
    for _ in range(4):
        accounts.register_peerflood_hit(account_id)
    accounts.reset_peerflood_series_after_success(account_id)
    runtime.set_peer_flood_range_seconds(60, 60)

    now = dt.datetime(2026, 8, 5, 7, 30, tzinfo=dt.timezone.utc)
    monkeypatch.setattr(spambot, "_now", lambda: now)
    monkeypatch.setattr(accounts, "_now", lambda: now)
    monkeypatch.setattr(accounts, "_now_iso", lambda: now.isoformat())
    checks: list[int] = []
    notices: list[str] = []

    async def fake_check(uid: int, *, force: bool = False):
        checks.append(int(uid))
        return None

    async def notify(text: str):
        notices.append(text)

    monkeypatch.setattr(spambot, "check_account", fake_check)
    monkeypatch.setattr(spambot, "notify_admins", notify)
    asyncio.run(spambot.on_peer_flood(account_id))

    assert checks == [account_id]
    assert "1/5" in notices[-1]
    assert "проверка запущена" in notices[-1]
    assert int(accounts.get_account(account_id)["peerflood_streak"]) == 1
