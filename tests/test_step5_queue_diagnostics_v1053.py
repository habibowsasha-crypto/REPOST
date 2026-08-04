"""Regression coverage for Step 5 queue, performance and diagnostics."""

from __future__ import annotations

import asyncio
import datetime as dt
from pathlib import Path


def _account(account_id: int) -> None:
    from services import accounts

    accounts.upsert_account(
        user_id=account_id,
        session_string=f"session-{account_id}",
        username=f"sender{account_id}",
    )
    accounts.set_participates(account_id, True)


class _ConnectedClient:
    def is_connected(self):
        return True


def test_queue_uses_indexed_rotating_cursor_not_random_sort(app_env):
    from db.schema import get_connection
    from services import queue

    source = Path("services/queue.py").read_text(encoding="utf-8")
    assert "ORDER BY RANDOM()" not in source

    indexes = {
        str(row[1])
        for row in get_connection().execute("PRAGMA index_list(leads)").fetchall()
    }
    assert "idx_leads_status_target" in indexes

    _account(1)
    for target in (10, 20, 30):
        queue.upsert_from_activity(target_user_id=target)

    queue._claim_cursor = 15
    assert int(queue.claim_random_pending(1)["target_user_id"]) == 20
    queue.release_claim(20, as_pending=True)
    assert int(queue.claim_random_pending(1)["target_user_id"]) == 30
    queue.release_claim(30, as_pending=True)
    assert int(queue.claim_random_pending(1)["target_user_id"]) == 10


def test_no_entity_becomes_terminal_after_all_accounts_checked(app_env, monkeypatch):
    from db.schema import get_connection
    from services import dispatcher, monitor, queue

    _account(10)
    _account(11)
    queue.upsert_from_activity(
        target_user_id=501,
        source_account_user_id=10,
        username=None,
        access_hash=None,
    )
    lead = queue.claim_random_pending(10)
    assert lead is not None

    monkeypatch.setattr(monitor, "get_client", lambda uid: _ConnectedClient())

    async def no_entity(*args, **kwargs):
        return "no_entity"

    monkeypatch.setattr(dispatcher, "_send_first_dm", no_entity)
    accounts = dispatcher._order_accounts_for_lead(
        lead,
        [
            {"user_id": 10, "participates": 1, "session_string": "s"},
            {"user_id": 11, "participates": 1, "session_string": "s"},
        ],
    )
    assert asyncio.run(dispatcher._attempt_lead_across_accounts(lead, accounts, "Привет"))

    row = get_connection().execute(
        "SELECT status, failure_reason, last_error FROM leads WHERE target_user_id=501"
    ).fetchone()
    assert row["status"] == queue.STATUS_CANCELLED
    assert row["failure_reason"] == "no_entity_all_accounts"
    assert "10" in str(row["last_error"]) and "11" in str(row["last_error"])
    assert get_connection().execute(
        "SELECT 1 FROM contacts WHERE target_user_id=501"
    ).fetchone() is None


def test_no_entity_waits_for_unavailable_untried_account(app_env, monkeypatch):
    from db.schema import db_lock, get_connection
    from services import dispatcher, monitor, queue

    _account(10)
    _account(11)
    queue.upsert_from_activity(target_user_id=502, source_account_user_id=10)
    lead = queue.claim_random_pending(10)
    assert lead is not None

    monkeypatch.setattr(monitor, "get_client", lambda uid: _ConnectedClient())

    async def no_entity(*args, **kwargs):
        return "no_entity"

    monkeypatch.setattr(dispatcher, "_send_first_dm", no_entity)
    result = asyncio.run(
        dispatcher._attempt_lead_across_accounts(
            lead, [{"user_id": 10, "participates": 1, "session_string": "s"}], "Привет"
        )
    )
    assert result is False
    row = get_connection().execute(
        "SELECT status, eligible_at, failure_reason FROM leads WHERE target_user_id=502"
    ).fetchone()
    assert row["status"] == queue.STATUS_PENDING
    assert row["eligible_at"] is not None
    assert row["failure_reason"] is None
    assert queue.failed_account_ids(502, "no_entity") == {10}

    # Once account 11 becomes available, account 10 is not retried and the final
    # result becomes terminal only after account 11 also proves unresolvable.
    past = (dt.datetime.now(dt.timezone.utc) - dt.timedelta(seconds=1)).isoformat()
    with db_lock(), get_connection():
        get_connection().execute(
            "UPDATE leads SET eligible_at=? WHERE target_user_id=502", (past,)
        )
    lead2 = queue.claim_random_pending(11)
    assert [int(a["user_id"]) for a in dispatcher._untried_ready_accounts(
        lead2,
        [
            {"user_id": 10, "participates": 1, "session_string": "s"},
            {"user_id": 11, "participates": 1, "session_string": "s"},
        ],
    )] == [11]
    assert asyncio.run(
        dispatcher._attempt_lead_across_accounts(
            lead2, [{"user_id": 11, "participates": 1, "session_string": "s"}], "Привет"
        )
    )
    row = get_connection().execute(
        "SELECT status, failure_reason FROM leads WHERE target_user_id=502"
    ).fetchone()
    assert row["status"] == queue.STATUS_CANCELLED
    assert row["failure_reason"] == "no_entity_all_accounts"


def test_fresh_activity_reopens_terminal_entity_failure(app_env):
    from db.schema import get_connection
    from services import queue

    queue.upsert_from_activity(target_user_id=503, source_account_user_id=10)
    queue.record_account_failure(503, 10, "no_entity", "missing")
    queue.mark_terminal_failure(503, "no_entity_all_accounts", "missing")

    assert queue.upsert_from_activity(
        target_user_id=503,
        username="now_resolvable",
        access_hash=123,
        source_account_user_id=10,
    ) == "refreshed"
    row = get_connection().execute(
        "SELECT status, failure_reason, last_error, send_attempts FROM leads WHERE target_user_id=503"
    ).fetchone()
    assert row["status"] == queue.STATUS_PENDING
    assert row["failure_reason"] is None
    assert row["last_error"] is None
    assert int(row["send_attempts"]) == 0
    assert queue.failed_account_ids(503, "no_entity") == set()


def test_repeated_pre_send_errors_end_with_diagnostic_not_completed_contact(app_env, monkeypatch):
    from db.schema import db_lock, get_connection
    from services import dispatcher, monitor, queue

    _account(12)
    queue.upsert_from_activity(target_user_id=504, source_account_user_id=12)

    async def broken_resolve(*args, **kwargs):
        raise RuntimeError("network resolver unavailable")

    monkeypatch.setattr(dispatcher, "_resolve_target_entity", broken_resolve)
    monkeypatch.setattr(monitor, "get_client", lambda uid: _ConnectedClient())
    for round_no in range(queue.MAX_SEND_ATTEMPTS):
        with db_lock(), get_connection():
            get_connection().execute(
                "UPDATE leads SET eligible_at=? WHERE target_user_id=504",
                ((dt.datetime.now(dt.timezone.utc) - dt.timedelta(seconds=1)).isoformat(),),
            )
        lead = queue.claim_random_pending(12)
        assert lead is not None
        result = asyncio.run(
            dispatcher._attempt_lead_across_accounts(
                lead, [{"user_id": 12, "participates": 1, "session_string": "s"}], "Привет"
            )
        )
        if round_no < queue.MAX_SEND_ATTEMPTS - 1:
            assert result is False

    assert result is True
    row = get_connection().execute(
        "SELECT status, failure_reason, send_attempts, last_error FROM leads WHERE target_user_id=504"
    ).fetchone()
    assert row["status"] == queue.STATUS_CANCELLED
    assert row["failure_reason"] == "max_transient_attempts"
    assert int(row["send_attempts"]) == queue.MAX_SEND_ATTEMPTS
    assert "retryable rounds exhausted" in str(row["last_error"])
    assert get_connection().execute(
        "SELECT 1 FROM contacts WHERE target_user_id=504"
    ).fetchone() is None


def test_group_activity_is_not_logged_at_info_per_message(app_env):
    source = Path("services/monitor.py").read_text(encoding="utf-8")
    assert 'logger.debug(\n                    "Group msg account=' in source
    assert 'logger.info(\n                    "Group msg account=' not in source
    assert 'logger.debug(\n            "Lead created target=' in source


def test_queue_ui_surfaces_terminal_reason(app_env):
    from handlers.queue_ui import queue_screen_text
    from services import queue

    queue.upsert_from_activity(target_user_id=505, username="broken")
    queue.mark_terminal_failure(505, "no_entity_all_accounts", "checked 1,2")
    text = queue_screen_text()
    assert "Последние конечные ошибки" in text
    assert "@broken" in text
    assert "no_entity_all_accounts" in text


def test_retryable_errors_count_once_per_dispatch_round(app_env, monkeypatch):
    from db.schema import get_connection
    from services import dispatcher, monitor, queue

    ready = []
    for account_id in range(20, 25):
        _account(account_id)
        ready.append({"user_id": account_id, "participates": 1, "session_string": "s"})
    queue.upsert_from_activity(target_user_id=506, source_account_user_id=20)
    lead = queue.claim_random_pending(20)
    assert lead is not None

    monkeypatch.setattr(monitor, "get_client", lambda uid: _ConnectedClient())

    async def retryable(*args, **kwargs):
        queue.release_claim(506, as_pending=True)
        return "error"

    monkeypatch.setattr(dispatcher, "_send_first_dm", retryable)
    result = asyncio.run(dispatcher._attempt_lead_across_accounts(lead, ready, "Привет"))
    assert result is False
    row = get_connection().execute(
        "SELECT status, send_attempts, failure_reason FROM leads WHERE target_user_id=506"
    ).fetchone()
    assert row["status"] == queue.STATUS_PENDING
    assert int(row["send_attempts"]) == 1
    assert row["failure_reason"] is None


def test_clear_pending_removes_account_failure_rows(app_env):
    from db.schema import get_connection
    from services import queue

    queue.upsert_from_activity(target_user_id=507)
    queue.record_account_failure(507, 10, "no_entity", "missing")
    assert queue.clear_pending() == 1
    assert get_connection().execute(
        "SELECT 1 FROM lead_account_failures WHERE target_user_id=507"
    ).fetchone() is None


def test_repeated_same_group_activity_does_not_reset_retry_rounds(app_env):
    from db.schema import get_connection
    from services import queue

    queue.upsert_from_activity(
        target_user_id=508, username="same", access_hash=123, source_account_user_id=10
    )
    queue.bump_send_attempts(508, "temporary")
    queue.record_account_failure(508, 10, "no_entity", "missing")

    assert queue.upsert_from_activity(
        target_user_id=508, username="same", access_hash=123, source_account_user_id=10
    ) == "refreshed"
    row = get_connection().execute(
        "SELECT send_attempts, last_error FROM leads WHERE target_user_id=508"
    ).fetchone()
    assert int(row["send_attempts"]) == 1
    assert row["last_error"] is not None
    assert queue.failed_account_ids(508, "no_entity") == {10}


def test_new_identity_evidence_resets_technical_failures(app_env):
    from db.schema import get_connection
    from services import queue

    queue.upsert_from_activity(
        target_user_id=509, username=None, access_hash=None, source_account_user_id=10
    )
    queue.bump_send_attempts(509, "temporary")
    queue.record_account_failure(509, 10, "no_entity", "missing")

    assert queue.upsert_from_activity(
        target_user_id=509, username="appeared", access_hash=999, source_account_user_id=11
    ) == "refreshed"
    row = get_connection().execute(
        "SELECT send_attempts, last_error, failure_reason FROM leads WHERE target_user_id=509"
    ).fetchone()
    assert int(row["send_attempts"]) == 0
    assert row["last_error"] is None
    assert row["failure_reason"] is None
    assert queue.failed_account_ids(509, "no_entity") == set()


def test_identity_refresh_during_attempt_prevents_false_terminal(app_env, monkeypatch):
    from db.schema import get_connection
    from services import dispatcher, monitor, queue

    _account(30)
    _account(31)
    queue.upsert_from_activity(target_user_id=510, source_account_user_id=30)
    lead = queue.claim_random_pending(30)
    assert lead is not None
    monkeypatch.setattr(monitor, "get_client", lambda uid: _ConnectedClient())

    calls = {"n": 0}

    async def refresh_then_no_entity(*args, **kwargs):
        calls["n"] += 1
        if calls["n"] == 1:
            queue.upsert_from_activity(
                target_user_id=510,
                username="new_identity",
                access_hash=777,
                source_account_user_id=31,
            )
        return "no_entity"

    monkeypatch.setattr(dispatcher, "_send_first_dm", refresh_then_no_entity)
    result = asyncio.run(
        dispatcher._attempt_lead_across_accounts(
            lead,
            [
                {"user_id": 30, "participates": 1, "session_string": "s"},
                {"user_id": 31, "participates": 1, "session_string": "s"},
            ],
            "Привет",
        )
    )
    assert result is False
    row = get_connection().execute(
        "SELECT status, username, access_hash, failure_reason FROM leads WHERE target_user_id=510"
    ).fetchone()
    assert row["status"] == queue.STATUS_PENDING
    assert row["username"] == "new_identity"
    assert int(row["access_hash"]) == 777
    assert row["failure_reason"] is None
    assert queue.failed_account_ids(510, "no_entity") == set()


def test_new_participating_account_reopens_no_entity_terminal(app_env):
    from db.schema import get_connection
    from services import accounts, queue

    _account(40)
    queue.upsert_from_activity(target_user_id=511, source_account_user_id=40)
    queue.record_account_failure(511, 40, "no_entity", "missing")
    queue.mark_terminal_failure(511, "no_entity_all_accounts", "checked 40")

    accounts.upsert_account(user_id=41, session_string="session-41", username="new")
    assert accounts.set_participates(41, True)
    row = get_connection().execute(
        "SELECT status, failure_reason, last_error FROM leads WHERE target_user_id=511"
    ).fetchone()
    assert row["status"] == queue.STATUS_PENDING
    assert row["failure_reason"] is None
    assert row["last_error"] == "new_sender_account_available"
    assert queue.failed_account_ids(511, "no_entity") == {40}


def test_max_transient_terminal_is_not_reopened_by_same_activity(app_env):
    from db.schema import get_connection
    from services import queue

    queue.upsert_from_activity(
        target_user_id=512, username="same", access_hash=321, source_account_user_id=50
    )
    queue.mark_terminal_failure(512, "max_transient_attempts", "five rounds")
    action = queue.upsert_from_activity(
        target_user_id=512, username="same", access_hash=321, source_account_user_id=50
    )
    assert action == "skipped_status_cancelled"
    row = get_connection().execute(
        "SELECT status, failure_reason FROM leads WHERE target_user_id=512"
    ).fetchone()
    assert row["status"] == queue.STATUS_CANCELLED
    assert row["failure_reason"] == "max_transient_attempts"
