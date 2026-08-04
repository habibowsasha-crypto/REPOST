"""Audience base import/export."""

from __future__ import annotations


def test_audience_record_and_export(app_env):
    from services import audience as a
    from services import opt_out as o

    a.record_first_dm(101, username="trader1")
    a.record_first_dm(102, username="trader2")
    assert a.count() >= 2
    lines = a.export_lines(only_with_dm=True)
    assert lines[0].startswith("user_id")
    assert any("101" in x for x in lines)

    o.add(103, "stop")
    stats = a.import_user_ids([103, 104, -1], source="import")
    assert stats["skipped_opt_out"] == 1
    assert stats["queued"] >= 1
    assert 104 in [r["user_id"] for r in a.list_recent(50)]


def test_parse_ids(app_env):
    from services.audience import parse_ids_from_text

    text = "user_id,username\n111,foo\n222\n  333 444\n"
    ids = parse_ids_from_text(text)
    assert ids == [111, 222, 333, 444]


def test_import_reopens_sent(app_env):
    from services import audience as a
    from services import queue as q
    from db.schema import get_connection, db_lock

    # Simulate already-sent contact
    q.upsert_from_activity(target_user_id=501, username="old")
    lead = q.claim_random_pending(1)
    assert lead
    q.mark_sending(501, 1)
    q.mark_sent(501, 1)
    assert q.count_by_status(q.STATUS_PENDING) == 0

    stats = a.import_user_ids([501], source="import")
    assert stats["queued"] == 1
    assert q.count_by_status(q.STATUS_PENDING) == 1


def test_import_clears_dialog(app_env):
    from db.schema import get_connection
    from services import audience as a
    from services import queue as q
    from services import dialog_store as d

    q.upsert_from_activity(target_user_id=777, username="x")
    lead = q.claim_random_pending(1)
    q.mark_sending(777, 1)
    q.mark_sent(777, 1)
    d.create_after_first_dm(777, 1, "hi?")
    d.set_stage(777, d.STAGE_EXPLAINED, auto_link_at="2099-01-01T00:00:00+00:00")
    assert d.get_dialog(777) is not None

    a.import_user_ids([777])
    assert d.get_dialog(777) is None
    assert q.count_by_status(q.STATUS_PENDING) == 1
    archive = get_connection().execute(
        "SELECT stage, history_json FROM dialog_archives WHERE target_user_id=777"
    ).fetchone()
    assert archive is not None
    assert archive["stage"] == d.STAGE_EXPLAINED
    assert "hi?" in archive["history_json"]
