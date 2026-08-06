from __future__ import annotations

import asyncio
import datetime as dt
from pathlib import Path


def _seed_prepared(target: int, account: int) -> None:
    from db.schema import db_lock, get_connection
    from services import accounts, first_dm_delivery, queue

    accounts.upsert_account(
        user_id=account,
        session_string="session",
        username=f"sender{account}",
    )
    accounts.set_participates(account, True)
    queue.upsert_from_activity(
        target_user_id=target,
        username=f"lead{target}",
        source_account_user_id=account,
        access_hash=777,
    )
    assert queue.claim_random_pending(account) is not None
    assert first_dm_delivery.prepare(target, account, "test first dm")
    old = (dt.datetime.now(dt.timezone.utc) - dt.timedelta(minutes=5)).isoformat()
    due = (dt.datetime.now(dt.timezone.utc) - dt.timedelta(seconds=1)).isoformat()
    conn = get_connection()
    with db_lock(), conn:
        conn.execute(
            """
            UPDATE first_dm_outbox
               SET prepared_at=?, recovery_attempts=2, recovery_next_at=?
             WHERE target_user_id=?
            """,
            (old, due, target),
        )


def test_no_entity_recovery_is_bounded_and_returns_to_shared_queue(app_env, monkeypatch):
    from db.schema import get_connection
    from services import dispatcher, first_dm_delivery, monitor, queue

    target, account = 16301, 16311
    _seed_prepared(target, account)

    class Client:
        def is_connected(self):
            return True

    async def no_entity(*args, **kwargs):
        return None

    monkeypatch.setattr(monitor, "get_client", lambda account_id: Client())
    monkeypatch.setattr(dispatcher, "_resolve_target_entity", no_entity)

    assert asyncio.run(dispatcher.recover_ambiguous_first_dms()) == 1
    assert first_dm_delivery.get_prepared(target) is None
    lead = get_connection().execute(
        "SELECT status FROM leads WHERE target_user_id=?", (target,)
    ).fetchone()
    assert lead["status"] == queue.STATUS_PENDING
    assert queue.failed_account_ids(target, "no_entity") == {account}


def test_peer_invalid_history_recovery_is_bounded(app_env, monkeypatch):
    from db.schema import get_connection
    from services import dispatcher, first_dm_delivery, monitor, queue, telegram_history

    target, account = 16302, 16312
    _seed_prepared(target, account)

    class PeerIdInvalidError(Exception):
        pass

    class Client:
        def is_connected(self):
            return True

    async def resolve(*args, **kwargs):
        return object()

    async def fail(*args, **kwargs):
        raise PeerIdInvalidError("invalid peer")

    monkeypatch.setattr(monitor, "get_client", lambda account_id: Client())
    monkeypatch.setattr(dispatcher, "_resolve_target_entity", resolve)
    monkeypatch.setattr(telegram_history, "find_outgoing_text_since", fail)

    assert asyncio.run(dispatcher.recover_ambiguous_first_dms()) == 1
    assert first_dm_delivery.get_prepared(target) is None
    lead = get_connection().execute(
        "SELECT status FROM leads WHERE target_user_id=?", (target,)
    ).fetchone()
    assert lead["status"] == queue.STATUS_PENDING
    assert queue.failed_account_ids(target, "no_entity") == {account}


def test_ordinary_peerflood_does_not_create_a_new_account_interval(app_env, monkeypatch):
    from db.schema import db_lock, get_connection
    from services import accounts, runtime, spambot

    account = 16321
    accounts.upsert_account(
        user_id=account,
        session_string="session",
        username="sender",
    )
    accounts.set_participates(account, True)
    accounts.set_dm_interval(account, 120, 420)
    runtime.set_peer_flood_range_seconds(75, 75)

    now = dt.datetime(2026, 8, 4, 20, 0, tzinfo=dt.timezone.utc)
    existing_next = now + dt.timedelta(minutes=3)
    with db_lock(), get_connection():
        get_connection().execute(
            "UPDATE accounts SET next_send_at=? WHERE user_id=?",
            (existing_next.isoformat(), account),
        )

    monkeypatch.setattr(accounts, "_now", lambda: now)
    monkeypatch.setattr(spambot, "_now", lambda: now)
    monkeypatch.setattr(runtime, "pick_peer_flood_seconds", lambda: 75)

    async def no_notify(text: str) -> None:
        return None

    async def no_check(account_user_id: int, force: bool = False):
        return None

    monkeypatch.setattr(spambot, "notify_admins", no_notify)
    monkeypatch.setattr(spambot, "check_account", no_check)

    asyncio.run(spambot.on_peer_flood(account))
    row = accounts.get_account(account)
    assert dt.datetime.fromisoformat(row["cooldown_until"]) == now + dt.timedelta(seconds=75)
    assert dt.datetime.fromisoformat(row["next_send_at"]) == existing_next


def test_deleted_account_does_not_inherit_old_peerflood_hits(app_env, monkeypatch):
    from services import accounts, runtime

    account = 16331
    accounts.upsert_account(user_id=account, session_string="old", username="old")
    runtime.set_peer_flood_range_seconds(60, 90)
    monkeypatch.setattr(runtime, "pick_peer_flood_seconds", lambda: 75)
    start = dt.datetime(2026, 8, 4, 20, 0, tzinfo=dt.timezone.utc)

    for offset in (0, 30, 60, 90):
        monkeypatch.setattr(
            accounts,
            "_now",
            lambda offset=offset: start + dt.timedelta(seconds=offset),
        )
        assert accounts.register_peerflood_hit(account)["burst_triggered"] is False

    assert accounts.delete_account(account)
    accounts.upsert_account(user_id=account, session_string="new", username="new")
    monkeypatch.setattr(accounts, "_now", lambda: start + dt.timedelta(seconds=120))
    result = accounts.register_peerflood_hit(account)
    assert result["streak"] == 1
    assert result["burst_triggered"] is False


def test_direct_upgrade_from_rejected_v1061_preserves_later_admin_edit(app_env):
    from db import schema
    from db.schema import db_lock, get_connection
    from services import runtime

    conn = get_connection()
    with db_lock(), conn:
        conn.execute(
            "DELETE FROM schema_migrations WHERE name IN (?, ?, ?)",
            (
                "v1_0_61_peerflood_four_in_ten_base_10m",
                "v1_0_62_peerflood_five_in_ten",
                "v1_0_63_recovery_and_peerflood_cleanup",
            ),
        )
        conn.execute(
            "INSERT INTO schema_migrations(name, applied_at) VALUES (?, ?)",
            ("v1_0_61_peerflood_four_in_ten_base_10m", "2026-08-04T20:00:00+00:00"),
        )
        for key, value in (
            (runtime.KEY_PEER_FLOOD_LO, "120"),
            (runtime.KEY_PEER_FLOOD_HI, "180"),
            (runtime.KEY_PEER_FLOOD_SEC, "150"),
        ):
            conn.execute(
                """
                INSERT INTO runtime_meta(key, value, updated_at) VALUES (?, ?, ?)
                ON CONFLICT(key) DO UPDATE SET value=excluded.value, updated_at=excluded.updated_at
                """,
                (key, value, "2026-08-04T20:05:00+00:00"),
            )

    schema.init_db()
    assert runtime.get_peer_flood_range_seconds() == (120, 180)


def test_v1063_migration_caps_only_stale_rejected_peerflood_pause(app_env):
    from db import schema
    from db.schema import db_lock, get_connection
    from services import accounts, runtime

    account = 16341
    accounts.upsert_account(user_id=account, session_string="session", username="sender")
    now = dt.datetime.now(dt.timezone.utc)
    conn = get_connection()
    with db_lock(), conn:
        v1062 = conn.execute(
            "SELECT applied_at FROM schema_migrations WHERE name=?",
            ("v1_0_62_peerflood_five_in_ten",),
        ).fetchone()["applied_at"]
        conn.execute(
            "INSERT OR IGNORE INTO schema_migrations(name, applied_at) VALUES (?, ?)",
            ("v1_0_61_peerflood_four_in_ten_base_10m", "2026-08-04T19:00:00+00:00"),
        )
        conn.execute(
            "DELETE FROM schema_migrations WHERE name=?",
            ("v1_0_63_recovery_and_peerflood_cleanup",),
        )
        conn.execute(
            """
            UPDATE accounts
               SET is_paused=1, pause_reason='PeerFlood',
                   cooldown_until=?, next_send_at=?, peerflood_last_at=?
             WHERE user_id=?
            """,
            (
                (now + dt.timedelta(minutes=10)).isoformat(),
                (now + dt.timedelta(minutes=15)).isoformat(),
                "2026-08-04T19:30:00+00:00" if str(v1062) > "2026-08-04T19:30:00+00:00" else str(v1062),
                account,
            ),
        )
        runtime.set_peer_flood_range_seconds(60, 90)

    before = dt.datetime.now(dt.timezone.utc)
    schema.init_db()
    row = accounts.get_account(account)
    cooldown = dt.datetime.fromisoformat(str(row["cooldown_until"]).replace("Z", "+00:00"))
    remaining = (cooldown - before).total_seconds()
    assert 55 <= remaining <= 95
    assert row["next_send_at"] is None


def test_new_database_defaults_match_approved_admin_values(app_env):
    from services import runtime

    assert runtime.get_account_interval_range() == (120, 420)
    assert runtime.get_global_spacing_range() == (90, 180)
    assert runtime.get_daily_limit() == 125
    assert runtime.get_ai_reply_delay_range() == (20, 60)
    assert runtime.get_auto_link_delay_range() == (60, 60)
    assert runtime.get_peer_flood_range_seconds() == (60, 90)


def test_release_gate_marks_missing_dev_tools_as_skip_not_pass(app_env, monkeypatch, capsys):
    try:
        from scripts import release_check
    except ModuleNotFoundError:
        from DEV.scripts import release_check

    monkeypatch.setattr(release_check, "check_version", lambda: [])
    monkeypatch.setattr(release_check, "check_python", lambda: [])
    monkeypatch.setattr(release_check, "check_requirements", lambda: [])
    monkeypatch.setattr(release_check, "check_tree", lambda: [])
    monkeypatch.setattr(release_check, "run_command", lambda command: (True, "ok"))
    monkeypatch.setattr(release_check.shutil, "which", lambda tool: None)

    assert release_check.main(["--skip-tests"]) == 0
    output = capsys.readouterr().out
    assert "[SKIP] ruff" in output
    assert "[SKIP] mypy" in output
    assert "[PASS] ruff" not in output
    assert "[PASS] mypy" not in output


def test_release_tree_uses_only_short_ascii_hyphen(app_env):
    root = Path(__file__).resolve().parents[1]
    forbidden = {"\u2013", "\u2014", "\u2212"}
    offenders = []
    for path in root.rglob("*"):
        if not path.is_file() or any(part.startswith(".") and part != ".env.example" for part in path.parts):
            continue
        if any(part in {"__pycache__", ".pytest_cache", ".mypy_cache", ".ruff_cache"} for part in path.parts):
            continue
        try:
            text = path.read_text(encoding="utf-8")
        except (UnicodeError, OSError):
            continue
        if any(char in text for char in forbidden):
            offenders.append(str(path.relative_to(root)))
    assert offenders == []
