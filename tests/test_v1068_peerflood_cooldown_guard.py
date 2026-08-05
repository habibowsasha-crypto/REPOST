"""v1.0.68 PeerFlood cooldown anti-stacking and persisted-timer repair."""

from __future__ import annotations

import asyncio
import datetime as dt


def _seed_account(accounts, user_id: int) -> None:
    accounts.upsert_account(
        user_id=user_id,
        session_string="session",
        username=f"guard{user_id}",
    )
    accounts.set_participates(user_id, True)


def test_only_one_five_in_ten_extension_per_active_pause(app_env, monkeypatch):
    from services import accounts, runtime, spambot

    account = 6801
    _seed_account(accounts, account)
    runtime.set_peer_flood_range_seconds(75, 75)
    runtime.set_peer_flood_burst_extra_seconds(600)

    start = dt.datetime(2026, 8, 5, 5, 0, tzinfo=dt.timezone.utc)
    current = [start]
    monkeypatch.setattr(accounts, "_now", lambda: current[0])
    monkeypatch.setattr(spambot, "_now", lambda: current[0])

    notices: list[str] = []

    async def fake_notify(text: str) -> None:
        notices.append(text)

    async def fake_check(account_user_id: int, force: bool = False) -> None:
        return None

    monkeypatch.setattr(spambot, "notify_admins", fake_notify)
    monkeypatch.setattr(spambot, "check_account", fake_check)

    for offset in range(0, 100, 10):
        current[0] = start + dt.timedelta(seconds=offset)
        asyncio.run(spambot.on_peer_flood(account))

    row = accounts.get_account(account)
    actual = dt.datetime.fromisoformat(str(row["cooldown_until"]).replace("Z", "+00:00"))
    assert actual == start + dt.timedelta(seconds=675)
    assert row["peerflood_burst_applied_at"] is not None
    assert len(notices) == 2
    assert "Дополнительная пауза" in notices[-1]


def test_repair_clamps_old_52_hour_local_timer_to_configured_ceiling(app_env, monkeypatch):
    from db.schema import db_lock, get_connection
    from services import accounts, runtime

    account = 6802
    _seed_account(accounts, account)
    runtime.set_peer_flood_range_seconds(60, 90)
    runtime.set_peer_flood_burst_extra_seconds(600)

    now = dt.datetime(2026, 8, 5, 5, 0, tzinfo=dt.timezone.utc)
    monkeypatch.setattr(accounts, "_now", lambda: now)
    last_hit = now - dt.timedelta(seconds=30)
    inflated = now + dt.timedelta(hours=52, minutes=7)
    conn = get_connection()
    with db_lock(), conn:
        conn.execute(
            """
            UPDATE accounts
               SET is_paused=1, pause_reason='PeerFlood', cooldown_until=?,
                   peerflood_last_at=?, next_send_at=?
             WHERE user_id=?
            """,
            (inflated.isoformat(), last_hit.isoformat(), inflated.isoformat(), account),
        )

    result = accounts.clamp_peerflood_cooldown(account)
    assert result["changed"] is True
    assert result["cleared"] is False
    expected = last_hit + dt.timedelta(seconds=690)
    row = accounts.get_account(account)
    actual = dt.datetime.fromisoformat(str(row["cooldown_until"]).replace("Z", "+00:00"))
    assert actual == expected
    assert row["next_send_at"] is None


def test_repair_clears_inflated_timer_when_safe_ceiling_already_expired(app_env, monkeypatch):
    from db.schema import db_lock, get_connection
    from services import accounts, runtime

    account = 6803
    _seed_account(accounts, account)
    runtime.set_peer_flood_range_seconds(60, 90)
    runtime.set_peer_flood_burst_extra_seconds(600)

    now = dt.datetime(2026, 8, 5, 5, 0, tzinfo=dt.timezone.utc)
    monkeypatch.setattr(accounts, "_now", lambda: now)
    conn = get_connection()
    with db_lock(), conn:
        conn.execute(
            """
            UPDATE accounts
               SET is_paused=1, pause_reason='PeerFlood', cooldown_until=?,
                   peerflood_last_at=?, peerflood_burst_applied_at=?
             WHERE user_id=?
            """,
            (
                (now + dt.timedelta(hours=52)).isoformat(),
                (now - dt.timedelta(hours=2)).isoformat(),
                (now - dt.timedelta(hours=2)).isoformat(),
                account,
            ),
        )

    repaired = accounts.repair_inflated_peerflood_cooldowns()
    assert [item["user_id"] for item in repaired] == [account]
    row = accounts.get_account(account)
    assert row["is_paused"] == 0
    assert row["cooldown_until"] is None
    assert row["pause_reason"] is None
    assert row["peerflood_burst_applied_at"] is None


def test_floodwait_timer_is_never_shortened_by_peerflood_repair(app_env, monkeypatch):
    from db.schema import db_lock, get_connection
    from services import accounts, runtime

    account = 6804
    _seed_account(accounts, account)
    runtime.set_peer_flood_range_seconds(60, 90)
    runtime.set_peer_flood_burst_extra_seconds(600)

    now = dt.datetime(2026, 8, 5, 5, 0, tzinfo=dt.timezone.utc)
    monkeypatch.setattr(accounts, "_now", lambda: now)
    original = now + dt.timedelta(hours=52)
    conn = get_connection()
    with db_lock(), conn:
        conn.execute(
            """
            UPDATE accounts
               SET is_paused=1, pause_reason='FloodWait 187200s', cooldown_until=?,
                   peerflood_last_at=?
             WHERE user_id=?
            """,
            (original.isoformat(), now.isoformat(), account),
        )

    assert accounts.repair_inflated_peerflood_cooldowns() == []
    row = accounts.get_account(account)
    assert row["cooldown_until"] == original.isoformat()


def test_resume_clears_one_shot_marker_for_future_independent_pause(app_env, monkeypatch):
    from db.schema import db_lock, get_connection
    from services import accounts, spambot

    account = 6805
    _seed_account(accounts, account)
    now = dt.datetime.now(dt.timezone.utc)
    conn = get_connection()
    with db_lock(), conn:
        conn.execute(
            """
            UPDATE accounts
               SET is_paused=1, pause_reason='PeerFlood', cooldown_until=?,
                   peerflood_burst_applied_at=?
             WHERE user_id=?
            """,
            (
                (now + dt.timedelta(minutes=10)).isoformat(),
                now.isoformat(),
                account,
            ),
        )

    async def fake_notify(text: str) -> None:
        return None

    async def fake_refresh() -> None:
        return None

    monkeypatch.setattr(spambot, "notify_admins", fake_notify)
    monkeypatch.setattr(spambot.monitor_svc, "refresh_monitor", fake_refresh)
    asyncio.run(spambot.resume_account(account, source="manual"))
    row = accounts.get_account(account)
    assert row["is_paused"] == 0
    assert row["peerflood_burst_applied_at"] is None
