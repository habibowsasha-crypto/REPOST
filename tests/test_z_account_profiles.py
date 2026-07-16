from __future__ import annotations

import asyncio
import datetime
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from config import conn
from services.account_profiles import (
    format_account_label,
    get_account_profile,
    refresh_stale_account_profiles,
)
from utils.database.database import create_table
from handlers.dm.dm_handlers import cmd_dm_post


class FakeProfileClient:
    def __init__(self, entity, *, delay: float = 0.0) -> None:
        self.entity = entity
        self.delay = delay

    async def is_user_authorized(self) -> bool:
        return True

    async def get_me(self):
        if self.delay:
            await asyncio.sleep(self.delay)
        return self.entity


class AccountProfileTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        create_table()
        with conn:
            conn.execute("DELETE FROM sessions")

    async def test_old_empty_profile_refreshes_for_start_dm_label(self) -> None:
        user_id = 8322251241
        with conn:
            conn.execute(
                "INSERT INTO sessions(user_id, session_string) VALUES (?, ?)",
                (user_id, "test-session"),
            )
        entity = SimpleNamespace(
            id=user_id,
            username="maxim_trade",
            first_name="Максим",
            last_name="",
        )
        result = await refresh_stale_account_profiles(
            [(user_id, "test-session")],
            active_clients={user_id: FakeProfileClient(entity)},
            timeout_seconds=0.2,
        )
        self.assertEqual(result, (1, 0, 0))
        self.assertEqual(
            format_account_label(user_id, include_id=True, max_length=42),
            "@maxim_trade | 8322251241",
        )

    async def test_long_username_never_truncates_numeric_id(self) -> None:
        user_id = 8868635911
        old = (
            datetime.datetime.now(datetime.timezone.utc)
            - datetime.timedelta(days=2)
        ).isoformat()
        with conn:
            conn.execute(
                """
                INSERT INTO sessions(
                    user_id, session_string, username, profile_updated_at
                ) VALUES (?, ?, ?, ?)
                """,
                (user_id, "test-session", "x" * 32, old),
            )
        label = format_account_label(user_id, include_id=True, max_length=30)
        self.assertTrue(label.endswith(str(user_id)))
        self.assertIn("… | ", label)
        self.assertLessEqual(len(label), 30)

    async def test_refresh_timeout_keeps_last_cached_name(self) -> None:
        user_id = 8636988460
        old = (
            datetime.datetime.now(datetime.timezone.utc)
            - datetime.timedelta(days=2)
        ).isoformat()
        with conn:
            conn.execute(
                """
                INSERT INTO sessions(
                    user_id, session_string, username, profile_updated_at
                ) VALUES (?, ?, ?, ?)
                """,
                (user_id, "test-session", "cached_name", old),
            )
        entity = SimpleNamespace(
            id=user_id,
            username="new_name",
            first_name="Новое",
            last_name="Имя",
        )
        result = await refresh_stale_account_profiles(
            [(user_id, "test-session")],
            active_clients={user_id: FakeProfileClient(entity, delay=0.2)},
            force=True,
            timeout_seconds=0.05,
        )
        self.assertEqual(result, (0, 1, 0))
        profile = get_account_profile(user_id)
        self.assertEqual(profile.username, "cached_name")
        self.assertEqual(
            format_account_label(user_id, include_id=True, max_length=42),
            "@cached_name | 8636988460",
        )


    async def test_start_dm_menu_refreshes_legacy_empty_profile(self) -> None:
        user_id = 8868635911
        with conn:
            conn.execute(
                "INSERT INTO sessions(user_id, session_string) VALUES (?, ?)",
                (user_id, "legacy-session"),
            )

        async def refresh(rows, **_kwargs):
            self.assertEqual(rows, [(user_id, "legacy-session")])
            with conn:
                conn.execute(
                    """
                    UPDATE sessions
                       SET username=?, profile_updated_at=?
                     WHERE user_id=?
                    """,
                    (
                        "refreshed_account",
                        datetime.datetime.now(datetime.timezone.utc).isoformat(),
                        user_id,
                    ),
                )
            return (1, 0, 0)

        event = SimpleNamespace(sender_id=123)
        with patch(
            "handlers.dm.dm_handlers.refresh_stale_account_profiles",
            side_effect=refresh,
        ) as refreshed, patch(
            "handlers.dm.dm_handlers.render_menu", new_callable=AsyncMock
        ) as rendered:
            await cmd_dm_post(event)
        refreshed.assert_awaited_once()
        label = rendered.await_args.kwargs["buttons"][0][0].text
        self.assertIn("@refreshed_account", label)
        self.assertIn(str(user_id), label)

    async def test_profile_refresh_failure_does_not_break_start_dm_menu(self) -> None:
        user_id = 8322251241
        with conn:
            conn.execute(
                """
                INSERT INTO sessions(
                    user_id, session_string, username, profile_updated_at
                ) VALUES (?, ?, ?, ?)
                """,
                (user_id, "test-session", "cached_account", None),
            )
        event = SimpleNamespace(sender_id=123)
        with patch(
            "handlers.dm.dm_handlers.refresh_stale_account_profiles",
            new_callable=AsyncMock,
            side_effect=RuntimeError("temporary Telegram failure"),
        ), patch(
            "handlers.dm.dm_handlers.render_menu", new_callable=AsyncMock
        ) as rendered:
            await cmd_dm_post(event)
        label = rendered.await_args.kwargs["buttons"][0][0].text
        self.assertIn("@cached_account", label)
        self.assertIn(str(user_id), label)

    async def test_start_dm_menu_renders_username_and_full_id(self) -> None:
        user_id = 8322251241
        with conn:
            conn.execute(
                """
                INSERT INTO sessions(
                    user_id, session_string, username, profile_updated_at
                ) VALUES (?, ?, ?, ?)
                """,
                (
                    user_id,
                    "test-session",
                    "menu_account",
                    datetime.datetime.now(datetime.timezone.utc).isoformat(),
                ),
            )
        event = SimpleNamespace(sender_id=123)
        with patch(
            "handlers.dm.dm_handlers.render_menu", new_callable=AsyncMock
        ) as rendered:
            await cmd_dm_post(event)
        rendered.assert_awaited_once()
        buttons = rendered.await_args.kwargs["buttons"]
        label = buttons[0][0].text
        self.assertIn("@menu_account", label)
        self.assertIn(str(user_id), label)


if __name__ == "__main__":
    unittest.main()
