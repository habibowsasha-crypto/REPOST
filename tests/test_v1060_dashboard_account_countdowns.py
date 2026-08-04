"""Regression coverage for v1.0.60 account countdowns in the main menu."""

from __future__ import annotations

import datetime as dt


def _add_account(user_id: int, username: str = "timer_user"):
    from services import accounts

    accounts.upsert_account(
        user_id=user_id,
        session_string=f"session-{user_id}",
        username=username,
    )
    accounts.set_participates(user_id, True)
    return accounts.get_account(user_id)


def _set_times(user_id: int, **values) -> None:
    from db.schema import db_lock, get_connection

    assignments = ", ".join(f"{key}=?" for key in values)
    params = [*values.values(), int(user_id)]
    conn = get_connection()
    with db_lock(), conn:
        conn.execute(
            f"UPDATE accounts SET {assignments} WHERE user_id=?",
            params,
        )


def test_active_account_shows_persisted_next_first_dm_countdown(app_env, monkeypatch):
    from services import accounts, pacing

    fixed = dt.datetime(2026, 8, 4, 19, 0, 0, tzinfo=dt.timezone.utc)
    monkeypatch.setattr(pacing, "_now", lambda: fixed)
    _add_account(1001)
    _set_times(
        1001,
        next_send_at=(fixed + dt.timedelta(seconds=601)).isoformat(),
        last_send_at=fixed.isoformat(),
    )

    line = accounts.dashboard_account_line(accounts.get_account(1001))

    assert "Следующий First DM через 11м" in line
    assert "First DM включены" in line


def test_ready_account_is_explicitly_marked_ready(app_env, monkeypatch):
    from services import accounts, pacing

    _add_account(1002)
    monkeypatch.setattr(pacing, "seconds_until_global_ready", lambda: 0.0)

    line = accounts.dashboard_account_line(accounts.get_account(1002))

    assert "Готов к First DM" in line


def test_ready_account_surfaces_global_spacing_countdown(app_env, monkeypatch):
    from services import accounts, pacing

    _add_account(1003)
    monkeypatch.setattr(pacing, "seconds_until_global_ready", lambda: 91.0)

    line = accounts.dashboard_account_line(accounts.get_account(1003))

    assert "Готов · общая пауза ещё 2м" in line


def test_daily_limit_is_visible_instead_of_false_ready_status(app_env, monkeypatch):
    from services import accounts, pacing, runtime

    fixed = dt.datetime(2026, 8, 4, 19, 0, 0, tzinfo=dt.timezone.utc)
    monkeypatch.setattr(pacing, "_now", lambda: fixed)
    _add_account(1004)
    _set_times(
        1004,
        daily_sent_date=fixed.date().isoformat(),
        daily_sent_count=runtime.get_daily_limit(),
    )

    line = accounts.dashboard_account_line(accounts.get_account(1004))

    assert "Дневной лимит исчерпан" in line


def test_peerflood_countdown_keeps_priority_over_first_dm_timer(app_env):
    from services import accounts

    _add_account(1005)
    until = dt.datetime.now(dt.timezone.utc) + dt.timedelta(minutes=23)
    _set_times(
        1005,
        is_paused=1,
        pause_reason="PeerFlood",
        cooldown_until=until.isoformat(),
        next_send_at=(until + dt.timedelta(minutes=5)).isoformat(),
    )

    line = accounts.dashboard_account_line(accounts.get_account(1005))

    assert line.startswith("🔴")
    assert "PeerFlood" in line
    assert "Следующий First DM" not in line


def test_seconds_until_account_ready_uses_legacy_last_send_fallback(app_env, monkeypatch):
    from services import accounts, pacing

    fixed = dt.datetime(2026, 8, 4, 19, 0, 0, tzinfo=dt.timezone.utc)
    monkeypatch.setattr(pacing, "_now", lambda: fixed)
    _add_account(1006)
    _set_times(
        1006,
        next_send_at=None,
        last_send_at=(fixed - dt.timedelta(seconds=120)).isoformat(),
        dm_interval_min_sec=600,
        dm_interval_max_sec=900,
    )

    wait = pacing.seconds_until_account_ready(accounts.get_account(1006))

    assert wait == 480.0
