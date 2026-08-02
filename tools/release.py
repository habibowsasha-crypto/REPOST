#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

try:
    from tools.release_lib import (
        ReleaseError,
        build_release,
        verify_archive,
        verify_project,
        write_report,
    )
except ModuleNotFoundError:
    from release_lib import (  # type: ignore
        ReleaseError,
        build_release,
        verify_archive,
        verify_project,
        write_report,
    )


def _root(value: str) -> Path:
    return Path(value).expanduser().resolve()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Fail-closed проверка и воспроизводимая сборка релизов LikeBot."
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    verify = subparsers.add_parser("verify", help="Проверить исходное дерево")
    verify.add_argument("--project-root", default=".", type=_root)
    verify.add_argument("--report", type=_root)
    verify.add_argument("--skip-tests", action="store_true")
    verify.add_argument("--pytest-arg", action="append", default=[])
    verify.add_argument("--postgres-url", default=os.environ.get("RELEASE_POSTGRES_URL"))
    verify.add_argument("--require-postgres", action="store_true")

    archive = subparsers.add_parser("verify-archive", help="Проверить готовый ZIP и manifest")
    archive.add_argument("archive", type=_root)

    build = subparsers.add_parser("build", help="Проверить и собрать ZIP/patch/audit/SHA-256")
    build.add_argument("--project-root", default=".", type=_root)
    build.add_argument("--previous-archive", required=True, type=_root)
    build.add_argument("--output-dir", default="dist", type=_root)
    build.add_argument("--tag", required=True, help="Например: release_automation")
    build.add_argument("--postgres-url", default=os.environ.get("RELEASE_POSTGRES_URL"))
    build.add_argument("--require-postgres", action="store_true")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        if args.command == "verify":
            report = verify_project(
                args.project_root,
                run_tests=not args.skip_tests,
                pytest_args=args.pytest_arg,
                postgres_url=args.postgres_url,
                require_postgres=args.require_postgres,
            )
            if args.report:
                write_report(report, args.report)
            print(json.dumps(report.to_dict(), ensure_ascii=False, indent=2))
            return 0 if report.passed else 1
        if args.command == "verify-archive":
            manifest = verify_archive(args.archive)
            print(json.dumps(manifest, ensure_ascii=False, indent=2))
            return 0
        if args.command == "build":
            result = build_release(
                args.project_root,
                args.previous_archive,
                args.output_dir,
                tag=args.tag,
                postgres_url=args.postgres_url,
                require_postgres=args.require_postgres,
            )
            print(json.dumps({key: str(value) for key, value in result.items()}, ensure_ascii=False, indent=2))
            return 0
    except ReleaseError as exc:
        print(f"RELEASE FAILED: {exc}", file=sys.stderr)
        return 2
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
