"""Import every production module in an isolated temporary environment."""

from __future__ import annotations

import importlib
import os
import pkgutil
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
STUB_ROOT = ROOT / "DEV" / "test_support" / "stubs"


def _ensure_external_modules() -> None:
    try:
        import decouple  # noqa: F401
        import telethon  # noqa: F401
    except ModuleNotFoundError:
        sys.path.insert(0, str(STUB_ROOT))


def module_names() -> list[str]:
    names = ["config", "main"]
    for package_name in ("db", "services", "handlers", "texts", "utils"):
        package_path = ROOT / package_name
        names.append(package_name)
        names.extend(
            module.name
            for module in pkgutil.walk_packages(
                [str(package_path)], prefix=f"{package_name}."
            )
        )
    return sorted(set(names))


def main() -> int:
    _ensure_external_modules()
    if str(ROOT) not in sys.path:
        sys.path.insert(0, str(ROOT))
    with tempfile.TemporaryDirectory(prefix="channel-dm-import-") as tmp:
        os.environ.update(
            {
                "API_ID": "1",
                "API_HASH": "test_hash",
                "BOT_TOKEN": "1:test",
                "ADMIN_ID_LIST": "999",
                "DB_PATH": str(Path(tmp) / "test.db"),
                "BOT_SESSION_PATH": str(Path(tmp) / "bot_session"),
                "MEDIA_DIR": str(Path(tmp) / "media"),
                "CHANNEL_LINK": "https://t.me/+testhash",
                "OPENAI_API_KEY": "",
            }
        )
        imported = 0
        for name in module_names():
            importlib.import_module(name)
            imported += 1
        from db.schema import close_connection

        close_connection()
    print(f"Imported {imported} production modules")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
