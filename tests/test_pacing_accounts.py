"""Pacing and account helpers."""

from __future__ import annotations


def test_account_ready_and_daily(app_env):
    from services import accounts as a
    from services import pacing

    a.upsert_account(user_id=7, session_string="s", username="u")
    a.set_participates(7, True)
    acc = a.get_account(7)
    ok, reason = pacing.account_is_send_ready(acc)
    assert ok, reason

    pacing.record_successful_send(7)
    acc2 = a.get_account(7)
    assert int(acc2["daily_sent_count"]) == 1
    assert acc2.get("last_send_at")


def test_chat_modes(app_env):
    from services import accounts as a
    from services import chats as c
    from db.schema import get_connection

    a.upsert_account(user_id=42, session_string="s")
    assert c.get_chat_mode(42) == c.CHAT_MODE_MANUAL
    assert c.set_chat_mode(42, c.CHAT_MODE_ALL)
    conn = get_connection()
    conn.execute(
        "INSERT INTO account_discovered_chats VALUES (42, -1001, 'G1', NULL, 'channel', 't')"
    )
    conn.execute(
        "INSERT INTO account_discovered_chats VALUES (42, -1002, 'G2', NULL, 'channel', 't')"
    )
    conn.commit()
    assert c.count_watchable(42) == 2
    assert c.toggle_excluded(42, -1002) is True
    assert c.count_watchable(42) == 1
    c.set_chat_mode(42, c.CHAT_MODE_MANUAL)
    assert c.toggle_selected(42, -1001) is True
    assert c.count_watchable(42) == 1


def test_spambot_parse(app_env):
    from services.spambot import parse_spambot_reply

    free = parse_spambot_reply(
        "Good news, no limits are currently applied to your account."
    )
    assert free["result"] == "free"
    limited = parse_spambot_reply("Your account is limited until 2026-08-10 12:00")
    assert limited["result"] == "limited"
    assert limited["limited_until"]


def test_runtime_worker_flag(app_env):
    from services import runtime

    assert runtime.is_worker_enabled() is False
    runtime.set_worker_enabled(True)
    assert runtime.is_worker_enabled() is True
    runtime.set_worker_enabled(False)
    assert runtime.is_worker_enabled() is False


def test_next_send_at_interval(app_env):
    from services import accounts as a
    from services import pacing

    a.upsert_account(user_id=9, session_string="s")
    a.set_participates(9, True)
    pacing.record_successful_send(9)
    acc = a.get_account(9)
    assert acc.get("next_send_at")
    ok, reason = pacing.account_is_send_ready(acc)
    assert not ok
    assert reason == "account_interval"
