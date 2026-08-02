#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys

from laika_bot.ai_comments_migration import (
    ROLLBACK_CONFIRMATION,
    AICommentsMigrationError,
    rollback_ai_comments_schema,
    verify_ai_comments_schema,
)
from laika_bot.db import Database


def _database_url() -> str:
    value = os.environ.get("DATABASE_URL", "").strip()
    if not value:
        raise AICommentsMigrationError(
            "DATABASE_URL не задан. Перед миграцией укажите целевую БД через environment."
        )
    if value.startswith("postgres://"):
        return value.replace("postgres://", "postgresql+asyncpg://", 1)
    if value.startswith("postgresql://") and "+asyncpg" not in value:
        return value.replace("postgresql://", "postgresql+asyncpg://", 1)
    return value


async def _with_lock(database: Database) -> None:
    if not await database.acquire_instance_lock():
        raise AICommentsMigrationError(
            "БД занята запущенным LikeBot. Остановите приложение перед отдельной миграцией."
        )


async def _run(command: str, confirmation: str | None) -> dict[str, object]:
    database = Database(_database_url())
    try:
        if command == "upgrade":
            await _with_lock(database)
            await database.init()
            async with database.engine.connect() as connection:
                report = await verify_ai_comments_schema(connection)
            return {"operation": "upgrade", "status": "ok", **report}
        if command == "verify":
            async with database.engine.connect() as connection:
                report = await verify_ai_comments_schema(connection)
            return {"operation": "verify", "status": "ok", **report}
        if command == "rollback":
            await _with_lock(database)
            async with database.engine.begin() as connection:
                report = await rollback_ai_comments_schema(
                    connection,
                    confirmation=confirmation or "",
                )
            return {"operation": "rollback", "status": "ok", **report}
        raise AICommentsMigrationError(f"Неизвестная операция: {command}")
    finally:
        await database.close()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Отдельная миграция схемы LikeBot AI Comments без запуска AI-функций."
    )
    subparsers = parser.add_subparsers(dest="command", required=True)
    subparsers.add_parser("upgrade", help="Создать/проверить схему и безопасные настройки")
    subparsers.add_parser("verify", help="Проверить таблицы, индексы, FK и schema version")
    rollback = subparsers.add_parser(
        "rollback", help="Удалить только таблицы AI Comments после backup"
    )
    rollback.add_argument(
        "--confirmation",
        required=True,
        help=f"Точное подтверждение: {ROLLBACK_CONFIRMATION}",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        result = asyncio.run(_run(args.command, getattr(args, "confirmation", None)))
    except AICommentsMigrationError as exc:
        print(f"AI COMMENTS SCHEMA FAILED: {exc}", file=sys.stderr)
        return 2
    except Exception:  # noqa: BLE001 - never print a possibly credential-bearing DB exception
        print(
            "AI COMMENTS SCHEMA FAILED: непредвиденная ошибка; DATABASE_URL скрыт",
            file=sys.stderr,
        )
        return 3
    print(json.dumps(result, ensure_ascii=False, sort_keys=True, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
