"""
Tests for the `groups` table deduplication + UNIQUE-index migration logic.

These tests are fully self-contained: they only use stdlib sqlite3 and do NOT
import any project module (importing project code would trigger config.py which
calls python-decouple and opens sessions.db at import time).

The SQL under test is mirrored verbatim from utils/database/database.py:
  - DELETE FROM groups WHERE rowid NOT IN (SELECT MIN(rowid) FROM groups GROUP BY user_id, group_id)
  - CREATE UNIQUE INDEX IF NOT EXISTS idx_groups_user_group ON groups(user_id, group_id)
"""

import sqlite3
import pytest


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def make_db() -> sqlite3.Connection:
    """Return a fresh in-memory SQLite connection with the `groups` table."""
    conn = sqlite3.connect(":memory:")
    conn.execute("""
        CREATE TABLE groups (
            group_id   INTEGER,
            group_username TEXT,
            user_id    INTEGER
        )
    """)
    conn.commit()
    return conn


def run_migration(conn: sqlite3.Connection) -> None:
    """Execute the exact migration SQL from database.py."""
    conn.execute("""
        DELETE FROM groups
        WHERE rowid NOT IN (
            SELECT MIN(rowid) FROM groups GROUP BY user_id, group_id
        )
    """)
    conn.execute("""
        CREATE UNIQUE INDEX IF NOT EXISTS idx_groups_user_group
        ON groups(user_id, group_id)
    """)
    conn.commit()


def insert_row(conn: sqlite3.Connection, user_id: int, group_id: int,
               username: str = "g") -> None:
    conn.execute(
        "INSERT INTO groups (group_id, group_username, user_id) VALUES (?, ?, ?)",
        (group_id, username, user_id),
    )
    conn.commit()


def insert_or_ignore(conn: sqlite3.Connection, user_id: int,
                     group_id: int, username: str = "g") -> None:
    conn.execute(
        "INSERT OR IGNORE INTO groups (group_id, group_username, user_id) VALUES (?, ?, ?)",
        (group_id, username, user_id),
    )
    conn.commit()


def count_rows(conn: sqlite3.Connection, user_id: int,
               group_id: int) -> int:
    cur = conn.execute(
        "SELECT COUNT(*) FROM groups WHERE user_id = ? AND group_id = ?",
        (user_id, group_id),
    )
    return cur.fetchone()[0]


def total_rows(conn: sqlite3.Connection) -> int:
    return conn.execute("SELECT COUNT(*) FROM groups").fetchone()[0]


# ---------------------------------------------------------------------------
# Test: duplicates collapse to one row per (user_id, group_id)
# ---------------------------------------------------------------------------

class TestDeduplication:

    def test_single_duplicate_pair_collapses(self):
        conn = make_db()
        # Insert the same (user_id, group_id) pair three times
        for _ in range(3):
            insert_row(conn, user_id=1001, group_id=200)

        run_migration(conn)

        assert count_rows(conn, user_id=1001, group_id=200) == 1

    def test_distinct_pairs_are_kept(self):
        conn = make_db()
        insert_row(conn, user_id=1001, group_id=200)
        insert_row(conn, user_id=1001, group_id=201)
        insert_row(conn, user_id=1002, group_id=200)

        run_migration(conn)

        assert total_rows(conn) == 3

    def test_mixed_duplicates_and_unique_rows(self):
        conn = make_db()
        # (1001, 300) appears twice
        insert_row(conn, user_id=1001, group_id=300)
        insert_row(conn, user_id=1001, group_id=300)
        # These are unique
        insert_row(conn, user_id=1001, group_id=301)
        insert_row(conn, user_id=1002, group_id=300)

        run_migration(conn)

        assert count_rows(conn, user_id=1001, group_id=300) == 1
        assert count_rows(conn, user_id=1001, group_id=301) == 1
        assert count_rows(conn, user_id=1002, group_id=300) == 1
        assert total_rows(conn) == 3

    def test_empty_table_stays_empty(self):
        conn = make_db()
        run_migration(conn)
        assert total_rows(conn) == 0

    def test_many_duplicates(self):
        conn = make_db()
        for _ in range(50):
            insert_row(conn, user_id=5000, group_id=999)

        run_migration(conn)

        assert count_rows(conn, user_id=5000, group_id=999) == 1

    def test_same_group_different_users_all_kept(self):
        conn = make_db()
        for uid in range(10):
            insert_row(conn, user_id=uid, group_id=42)

        run_migration(conn)

        assert total_rows(conn) == 10

    def test_same_user_different_groups_all_kept(self):
        conn = make_db()
        for gid in range(10):
            insert_row(conn, user_id=7, group_id=gid)

        run_migration(conn)

        assert total_rows(conn) == 10


# ---------------------------------------------------------------------------
# Test: migration is idempotent (running twice is a no-op)
# ---------------------------------------------------------------------------

class TestIdempotency:

    def test_idempotent_on_empty_table(self):
        conn = make_db()
        run_migration(conn)
        run_migration(conn)
        assert total_rows(conn) == 0

    def test_idempotent_after_dedup(self):
        conn = make_db()
        for _ in range(4):
            insert_row(conn, user_id=20, group_id=10)
        insert_row(conn, user_id=20, group_id=11)

        run_migration(conn)
        snapshot_after_first = total_rows(conn)

        run_migration(conn)  # second run must be a no-op
        assert total_rows(conn) == snapshot_after_first == 2

    def test_idempotent_with_unique_rows(self):
        conn = make_db()
        insert_row(conn, user_id=30, group_id=1)
        insert_row(conn, user_id=30, group_id=2)
        insert_row(conn, user_id=31, group_id=1)

        run_migration(conn)
        run_migration(conn)

        assert total_rows(conn) == 3

    def test_index_creation_does_not_raise_on_second_run(self):
        """CREATE UNIQUE INDEX IF NOT EXISTS must not raise even when called twice."""
        conn = make_db()
        run_migration(conn)
        # Should not raise sqlite3.OperationalError
        run_migration(conn)


# ---------------------------------------------------------------------------
# Test: after the index, INSERT OR IGNORE ignores duplicates
# ---------------------------------------------------------------------------

class TestInsertOrIgnoreAfterMigration:

    def test_insert_or_ignore_blocks_duplicate(self):
        conn = make_db()
        insert_row(conn, user_id=100, group_id=500)
        run_migration(conn)

        insert_or_ignore(conn, user_id=100, group_id=500)

        assert count_rows(conn, user_id=100, group_id=500) == 1

    def test_insert_or_ignore_allows_new_pair(self):
        conn = make_db()
        insert_row(conn, user_id=100, group_id=500)
        run_migration(conn)

        insert_or_ignore(conn, user_id=100, group_id=501)

        assert count_rows(conn, user_id=100, group_id=501) == 1

    def test_insert_or_ignore_multiple_dupes_stay_one(self):
        conn = make_db()
        insert_row(conn, user_id=200, group_id=600)
        run_migration(conn)

        for _ in range(10):
            insert_or_ignore(conn, user_id=200, group_id=600)

        assert count_rows(conn, user_id=200, group_id=600) == 1

    def test_insert_or_ignore_different_user_allowed(self):
        conn = make_db()
        insert_row(conn, user_id=300, group_id=700)
        run_migration(conn)

        insert_or_ignore(conn, user_id=301, group_id=700)

        assert count_rows(conn, user_id=300, group_id=700) == 1
        assert count_rows(conn, user_id=301, group_id=700) == 1

    def test_insert_or_ignore_different_group_allowed(self):
        conn = make_db()
        insert_row(conn, user_id=400, group_id=800)
        run_migration(conn)

        insert_or_ignore(conn, user_id=400, group_id=801)

        assert count_rows(conn, user_id=400, group_id=800) == 1
        assert count_rows(conn, user_id=400, group_id=801) == 1
