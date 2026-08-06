from __future__ import annotations

import pathlib
import sqlite3

import pytest


def test_version_is_authoritative(app_env):
    import config

    assert config.app_version() == "1.0.84"


def test_missing_version_fails_loudly(app_env, monkeypatch):
    import config

    def broken_read_text(self, *args, **kwargs):
        raise OSError("missing")

    monkeypatch.setattr(pathlib.Path, "read_text", broken_read_text)
    with pytest.raises(RuntimeError, match="VERSION file"):
        config.app_version()


def test_close_connection_releases_sqlite_handle(app_env):
    from db import schema

    first = schema.get_connection()
    first.execute("SELECT 1").fetchone()
    schema.close_connection()
    with pytest.raises(sqlite3.ProgrammingError):
        first.execute("SELECT 1")
    second = schema.get_connection()
    assert second is not first
    assert second.execute("PRAGMA integrity_check").fetchone()[0] == "ok"


def test_dependencies_are_exactly_pinned():
    test_file = pathlib.Path(__file__).resolve()
    root = test_file.parents[1]
    if not (root / "requirements.txt").exists():
        root = test_file.parents[2]
    dev_requirements = (root / "requirements-dev.txt")
    if not dev_requirements.exists():
        dev_requirements = root / "DEV" / "requirements-dev.txt"
    files = (root / "requirements.txt", dev_requirements)
    for dependency_file in files:
        for raw in dependency_file.read_text(encoding="utf-8").splitlines():
            line = raw.strip()
            if not line or line.startswith("#") or line.startswith("-r "):
                continue
            assert "==" in line
            assert not any(op in line for op in (">=", "<=", "~=", "!="))
    assert "pytest" not in (root / "requirements.txt").read_text(encoding="utf-8")


def test_release_gate_static_checks_pass():
    try:
        from scripts import release_check
    except ModuleNotFoundError:
        from DEV.scripts import release_check

    assert release_check.check_version() == []
    assert release_check.check_python() == []
    assert release_check.check_requirements() == []
