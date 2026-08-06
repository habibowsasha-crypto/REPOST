from __future__ import annotations

import datetime as dt


def _seed_account(accounts, user_id: int = 6201) -> None:
    accounts.upsert_account(
        user_id=user_id,
        session_string="session",
        username=f"user{user_id}",
    )
    accounts.set_participates(user_id, True)
    accounts.set_dm_interval(user_id, 120, 420)


def test_first_four_peerfloods_keep_ordinary_pause_and_pacing(app_env, monkeypatch):
    from services import accounts, runtime

    _seed_account(accounts)
    runtime.set_peer_flood_range_seconds(60, 90)
    runtime.set_peer_flood_burst_extra_seconds(600)
    monkeypatch.setattr(runtime, "pick_peer_flood_seconds", lambda: 75)

    start = dt.datetime(2026, 8, 4, 20, 0, tzinfo=dt.timezone.utc)
    results = []
    for offset in (0, 60, 120, 180):
        monkeypatch.setattr(accounts, "_now", lambda offset=offset: start + dt.timedelta(seconds=offset))
        results.append(accounts.register_peerflood_hit(6201))

    assert [item["pause_seconds"] for item in results] == [75, 75, 75, 75]
    assert not any(item["burst_triggered"] for item in results)
    row = accounts.get_account(6201)
    assert (row["dm_interval_min_sec"], row["dm_interval_max_sec"]) == (120, 420)
    assert row["interval_backup_min"] is None
    assert row["interval_backoff_until"] is None


def test_fifth_peerflood_in_ten_minutes_adds_admin_extra_once(app_env, monkeypatch):
    from db.schema import get_connection
    from services import accounts, runtime

    _seed_account(accounts)
    runtime.set_peer_flood_range_seconds(60, 90)
    runtime.set_peer_flood_burst_extra_seconds(600)
    monkeypatch.setattr(runtime, "pick_peer_flood_seconds", lambda: 75)

    start = dt.datetime(2026, 8, 4, 20, 0, tzinfo=dt.timezone.utc)
    result = None
    for offset in (0, 60, 120, 180, 240):
        monkeypatch.setattr(accounts, "_now", lambda offset=offset: start + dt.timedelta(seconds=offset))
        result = accounts.register_peerflood_hit(6201)

    assert result is not None
    assert result["streak"] == 5
    assert result["burst_triggered"] is True
    assert result["base_pause_seconds"] == 75
    assert result["extra_pause_seconds"] == 600
    assert result["pause_seconds"] == 675
    row = accounts.get_account(6201)
    assert row["peerflood_streak"] == 0
    assert row["peerflood_window_started_at"] is None
    count = get_connection().execute(
        "SELECT COUNT(*) AS c FROM peerflood_hits WHERE account_user_id=?", (6201,)
    ).fetchone()["c"]
    assert count == 0


def test_sixth_peerflood_starts_new_group_without_extra(app_env, monkeypatch):
    from services import accounts, runtime

    _seed_account(accounts)
    runtime.set_peer_flood_burst_extra_seconds(600)
    monkeypatch.setattr(runtime, "pick_peer_flood_seconds", lambda: 80)
    start = dt.datetime(2026, 8, 4, 20, 0, tzinfo=dt.timezone.utc)

    for offset in (0, 60, 120, 180, 240):
        monkeypatch.setattr(accounts, "_now", lambda offset=offset: start + dt.timedelta(seconds=offset))
        accounts.register_peerflood_hit(6201)

    monkeypatch.setattr(accounts, "_now", lambda: start + dt.timedelta(seconds=300))
    sixth = accounts.register_peerflood_hit(6201)
    assert sixth["streak"] == 1
    assert sixth["burst_triggered"] is False
    assert sixth["pause_seconds"] == 80


def test_window_is_truly_rolling_not_chained(app_env, monkeypatch):
    from services import accounts, runtime

    _seed_account(accounts)
    runtime.set_peer_flood_burst_extra_seconds(600)
    monkeypatch.setattr(runtime, "pick_peer_flood_seconds", lambda: 70)
    start = dt.datetime(2026, 8, 4, 20, 0, tzinfo=dt.timezone.utc)

    # At 10:01 the event at 00:00 has expired, so only four remain.
    for offset in (0, 540, 550, 560, 601):
        monkeypatch.setattr(accounts, "_now", lambda offset=offset: start + dt.timedelta(seconds=offset))
        result = accounts.register_peerflood_hit(6201)
    assert result["streak"] == 4
    assert result["burst_triggered"] is False

    # One second later there are five events inside the preceding ten minutes.
    monkeypatch.setattr(accounts, "_now", lambda: start + dt.timedelta(seconds=602))
    result = accounts.register_peerflood_hit(6201)
    assert result["streak"] == 5
    assert result["burst_triggered"] is True
    assert result["pause_seconds"] == 670


def test_admin_extra_setting_is_separate_from_ordinary_peerflood_range(app_env):
    from services import runtime

    runtime.set_peer_flood_range_seconds(60, 90)
    assert runtime.get_peer_flood_burst_extra_seconds() == 600
    assert runtime.set_peer_flood_burst_extra_seconds(900) == 900
    assert runtime.get_peer_flood_burst_extra_seconds() == 900
    assert runtime.get_peer_flood_range_seconds() == (60, 90)
    assert runtime.format_peer_flood_burst_extra() == "15 мин"


def test_admin_screen_explains_five_in_ten_without_changing_normal_pacing(app_env):
    from handlers import menu
    from services import runtime

    runtime.set_peer_flood_range_seconds(60, 90)
    runtime.set_peer_flood_burst_extra_seconds(600)
    text = menu._peerflood_screen()
    burst_text = menu._peerflood_burst_screen()

    assert "1 мин - 1 мин 30 сек" in text
    assert "5 PeerFlood за 10 минут" in text
    assert "+**10 мин**" in text
    assert "Обычный PeerFlood и все остальные настройки не меняются" in burst_text


def test_fifth_concurrent_peerflood_extends_active_cooldown_by_extra(app_env, monkeypatch):
    import asyncio

    from services import accounts, runtime, spambot

    _seed_account(accounts, 6202)
    accounts.set_dm_interval(6202, 120, 120)
    runtime.set_peer_flood_range_seconds(75, 75)
    runtime.set_peer_flood_burst_extra_seconds(600)

    current = [dt.datetime(2026, 8, 4, 20, 0, tzinfo=dt.timezone.utc)]
    monkeypatch.setattr(accounts, "_now", lambda: current[0])
    monkeypatch.setattr(spambot, "_now", lambda: current[0])

    notices: list[str] = []

    async def fake_notify(text: str) -> None:
        notices.append(text)

    async def fake_check(account_user_id: int, force: bool = False) -> None:
        return None

    monkeypatch.setattr(spambot, "notify_admins", fake_notify)
    monkeypatch.setattr(spambot, "check_account", fake_check)

    for offset in (0, 10, 20, 30, 40):
        current[0] = dt.datetime(2026, 8, 4, 20, 0, tzinfo=dt.timezone.utc) + dt.timedelta(seconds=offset)
        asyncio.run(spambot.on_peer_flood(6202))

    row = accounts.get_account(6202)
    expected = dt.datetime(2026, 8, 4, 20, 11, 15, tzinfo=dt.timezone.utc)
    actual = dt.datetime.fromisoformat(str(row["cooldown_until"]).replace("Z", "+00:00"))
    assert actual == expected
    assert row["dm_interval_min_sec"] == 120
    assert row["dm_interval_max_sec"] == 120
    assert len(notices) == 2
    assert "5 PeerFlood за 10 минут" in notices[-1]
    assert "Дополнительная пауза: **10 мин**" in notices[-1]
