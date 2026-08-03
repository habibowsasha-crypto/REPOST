"""Lead queue behaviour."""

from __future__ import annotations


def test_upsert_and_claim(app_env):
    from services import accounts as accounts_svc
    from services import opt_out as opt_out_svc
    from services import queue as q

    accounts_svc.upsert_account(user_id=10, session_string="s", username="acc")
    assert q.upsert_from_activity(target_user_id=100, username="lead") == "created"
    assert q.upsert_from_activity(target_user_id=100, username="lead") == "refreshed"
    assert q.count_by_status(q.STATUS_PENDING) == 1

    lead = q.claim_random_pending(10)
    assert lead is not None
    assert int(lead["target_user_id"]) == 100
    assert q.count_by_status(q.STATUS_CLAIMED) == 1

    q.mark_sent(100, 10)
    assert q.count_by_status(q.STATUS_SENT) == 1
    # activity should not reopen sent as pending path for claim
    skipped = q.upsert_from_activity(target_user_id=100)
    assert skipped.startswith("skipped_"), skipped


def test_opt_out_blocks_queue(app_env):
    from services import opt_out as opt_out_svc
    from services import queue as q

    opt_out_svc.add(55, "stop")
    assert q.upsert_from_activity(target_user_id=55) == "skipped_opt_out"


def test_clear_pending(app_env):
    from services import queue as q

    q.upsert_from_activity(target_user_id=1)
    q.upsert_from_activity(target_user_id=2)
    assert q.clear_pending() == 2
    assert q.count_by_status(q.STATUS_PENDING) == 0


def test_release_stale_claims(app_env):
    import datetime as dt
    from db.schema import db_lock, get_connection
    from services import accounts as a
    from services import queue as q

    a.upsert_account(user_id=10, session_string="s")
    q.upsert_from_activity(target_user_id=200, username="x")
    lead = q.claim_random_pending(10)
    assert lead is not None
    # Backdate claimed_at
    old = (dt.datetime.now(dt.timezone.utc) - dt.timedelta(hours=1)).isoformat()
    conn = get_connection()
    with db_lock(), conn:
        conn.execute(
            "UPDATE leads SET claimed_at=? WHERE target_user_id=?",
            (old, 200),
        )
    n = q.release_stale_claims(older_than_seconds=900)
    assert n == 1
    assert q.count_by_status(q.STATUS_PENDING) == 1


def test_cancel_creates_completed_contact(app_env):
    from services import queue as q
    from services import opt_out as o

    q.upsert_from_activity(target_user_id=77)
    q.cancel_lead(77, "privacy")
    assert q.upsert_from_activity(target_user_id=77).startswith("skipped_contact")


def test_mark_sending_and_stale_becomes_sent(app_env):
    import datetime as dt
    from db.schema import db_lock, get_connection
    from services import accounts as a
    from services import queue as q

    a.upsert_account(user_id=10, session_string="s")
    q.upsert_from_activity(target_user_id=88)
    q.claim_random_pending(10)
    q.mark_sending(88, 10)
    old = (dt.datetime.now(dt.timezone.utc) - dt.timedelta(hours=1)).isoformat()
    conn = get_connection()
    with db_lock(), conn:
        conn.execute("UPDATE leads SET claimed_at=? WHERE target_user_id=?", (old, 88))
    n = q.release_stale_claims(older_than_seconds=900)
    assert n == 1
    assert q.count_by_status(q.STATUS_SENT) == 1
