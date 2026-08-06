"""v1.0.83 entity-aware administrator dashboard regressions."""

from __future__ import annotations

import importlib


def _add_account(user_id: int, username: str, *, participates: bool) -> None:
    from services import accounts

    accounts.upsert_account(
        user_id=user_id,
        session_string=f"session-{user_id}",
        username=username,
    )
    accounts.set_participates(user_id, participates)


def _add_lead(target_id: int, account_id: int | None = None) -> None:
    from services import queue

    action = queue.upsert_from_activity(
        target_user_id=target_id,
        username=f"user{target_id}",
        access_hash=(target_id * 100 if account_id is not None else None),
        source_chat_id=(9000 + account_id if account_id is not None else None),
        source_account_user_id=account_id,
    )
    assert action == "created"


def test_dashboard_queue_partition_and_per_account_overlap(app_env):
    from db.schema import db_lock, get_connection
    from services import queue

    _add_account(101, "enabled", participates=True)
    _add_account(102, "disabled", participates=False)
    _add_account(103, "reauth", participates=True)

    conn = get_connection()
    with db_lock(), conn:
        conn.execute(
            "UPDATE accounts SET auth_status='reauth_required' WHERE user_id=103"
        )

    # Seen by both enabled and disabled accounts. It is unique in the queue but
    # appears in each account's own availability count until one First DM wins.
    _add_lead(1001, 101)
    queue.record_account_entity(
        target_user_id=1001,
        account_user_id=102,
        access_hash=100100,
        username="user1001",
        source_chat_id=9102,
    )

    # Seen only by a disabled account, so it waits for that account to be enabled.
    _add_lead(1002, 102)

    # No account-owned entity evidence exists.
    _add_lead(1003, None)

    # Evidence exists only on an account that requires reauthorization.
    _add_lead(1004, 103)

    # The enabled account already failed local entity resolution for this lead.
    _add_lead(1005, 101)
    queue.record_account_failure(1005, 101, "no_entity", "cache miss")

    counts = queue.dashboard_availability_counts()
    assert counts == {
        "total_pending": 5,
        "available_enabled": 1,
        "waiting_account_enable": 1,
        "no_available_account": 3,
    }

    assert queue.count_available_for_account(101) == 1
    assert queue.count_available_for_account(102) == 2
    assert queue.count_available_for_account(103) == 1


def test_sent_or_claimed_target_disappears_from_account_availability(app_env):
    from services import queue

    _add_account(201, "sender", participates=True)
    _add_account(202, "also_sees", participates=True)
    _add_lead(2001, 201)
    queue.record_account_entity(
        target_user_id=2001,
        account_user_id=202,
        access_hash=200100,
        username="user2001",
        source_chat_id=9202,
    )

    assert queue.count_available_for_account(201) == 1
    assert queue.count_available_for_account(202) == 1

    lead = queue.claim_random_pending(201)
    assert lead and int(lead["target_user_id"]) == 2001

    assert queue.count_available_for_account(201) == 0
    assert queue.count_available_for_account(202) == 0
    assert queue.dashboard_availability_counts()["total_pending"] == 0


def test_account_dashboard_uses_clear_approved_labels(app_env, monkeypatch):
    from services import accounts, pacing, queue

    _add_account(301, "menu_account", participates=True)
    _add_lead(3001, 301)
    monkeypatch.setattr(pacing, "seconds_until_global_ready", lambda: 0.0)

    line = accounts.dashboard_account_line(accounts.get_account(301))

    assert "First DM включены" in line
    assert "Доступно для First DM: **1**" in line
    assert "Закреплено диалогов: **0**" in line
    assert "Только этот аккаунт видит" not in line
    assert "Готов к First DM" in line
    assert queue.count_available_for_account(301) == 1


def test_main_and_queue_screens_explain_unique_delivery(app_env, monkeypatch):
    from handlers import menu as menu_module, queue_ui as queue_ui_module
    from services import dispatcher, monitor

    menu = importlib.reload(menu_module)
    queue_ui = importlib.reload(queue_ui_module)

    _add_account(401, "screen_account", participates=True)
    _add_lead(4001, 401)

    monkeypatch.setattr(
        dispatcher,
        "worker_status",
        lambda: {"enabled": True, "loop_running": True},
    )
    monkeypatch.setattr(
        monitor,
        "monitor_status",
        lambda: {"running": True, "connected_count": 1},
    )

    main_text = menu._dashboard_text()
    queue_text = queue_ui.queue_screen_text()

    for text in (main_text, queue_text):
        assert "Уникальных пользователей в очереди: **1**" in text
        assert "Доступны включённым аккаунтам: **1**" in text
        assert "Ждут включения аккаунта: **0**" in text
        assert "Нет доступного аккаунта: **0**" in text
        assert "Ждут сообщения" not in text

    assert "Один пользователь может быть доступен нескольким аккаунтам" in main_text
    assert "First DM отправляется только один раз" in main_text
    assert "Доступно для First DM: **1**" in main_text
