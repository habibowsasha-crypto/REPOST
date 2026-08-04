"""Pytest fixtures: isolated temp DB and required env."""

from __future__ import annotations

import os
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
STUB_ROOT = ROOT / "test_support" / "stubs"

# The production runtime still requires the real packages from requirements.txt.
# Tests can run in a clean/offline audit environment by using minimal local
# adapters only when Telethon/python-decouple are unavailable.
try:
    import decouple  # noqa: F401
    import telethon  # noqa: F401
except ModuleNotFoundError:
    if str(STUB_ROOT) not in sys.path:
        sys.path.insert(0, str(STUB_ROOT))

if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def _purge_app_modules():
    for name in list(sys.modules):
        if (
            name == "config"
            or name == "db"
            or name.startswith("db.")
            or name == "services"
            or name.startswith("services.")
            or name == "texts"
            or name.startswith("texts.")
        ):
            sys.modules.pop(name, None)


@pytest.fixture()
def app_env(tmp_path, monkeypatch):
    db = tmp_path / "test.db"
    monkeypatch.setenv("API_ID", "1")
    monkeypatch.setenv("API_HASH", "test_hash")
    monkeypatch.setenv("BOT_TOKEN", "1:test")
    monkeypatch.setenv("ADMIN_ID_LIST", "999")
    monkeypatch.setenv("DB_PATH", str(db))
    monkeypatch.setenv("BOT_SESSION_PATH", str(tmp_path / "bot_session"))
    monkeypatch.setenv("MEDIA_DIR", str(tmp_path / "media"))
    monkeypatch.setenv("CHANNEL_LINK", "https://t.me/+testhash")
    monkeypatch.setenv("CHANNEL_PITCH", "Бесплатный канал со сливами VIP")
    monkeypatch.setenv("OPENAI_API_KEY", "")
    monkeypatch.setenv("AI_DM_ENABLED", "true")
    _purge_app_modules()
    import config  # noqa: F401
    from db import schema as schema_mod

    # Ensure no stale connection from a previous DB_PATH.
    schema_mod.close_connection()
    schema_mod.init_db()
    yield {"db": str(db)}
    schema_mod.close_connection()
    _purge_app_modules()
