"""Build a deterministic clean ZIP and SHA-256 sidecar."""

from __future__ import annotations

import argparse
import hashlib
import os
import zipfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
EXCLUDED_DIRS = {"__pycache__", ".pytest_cache", ".mypy_cache", ".ruff_cache", ".git", "htmlcov"}
EXCLUDED_NAMES = {".coverage", "coverage.xml"}
EXCLUDED_SUFFIXES = {".pyc", ".pyo", ".db", ".sqlite", ".sqlite3", ".session"}


def included(path: Path) -> bool:
    rel = path.relative_to(ROOT)
    if any(part in EXCLUDED_DIRS for part in rel.parts):
        return False
    if path.name in EXCLUDED_NAMES or path.name == ".env" or path.suffix.lower() in EXCLUDED_SUFFIXES:
        return False
    return path.is_file() and not path.is_symlink()


def build(output: Path) -> tuple[Path, str]:
    output = output.resolve()
    output.parent.mkdir(parents=True, exist_ok=True)
    files = sorted(p for p in ROOT.rglob("*") if included(p) and p.resolve() != output)
    with zipfile.ZipFile(output, "w", compression=zipfile.ZIP_DEFLATED, compresslevel=9) as zf:
        for path in files:
            rel = path.relative_to(ROOT).as_posix()
            info = zipfile.ZipInfo(rel, date_time=(2026, 8, 4, 12, 0, 0))
            info.compress_type = zipfile.ZIP_DEFLATED
            info.external_attr = 0o100644 << 16
            zf.writestr(info, path.read_bytes())
    digest = hashlib.sha256(output.read_bytes()).hexdigest()
    sidecar = output.with_suffix(output.suffix + ".sha256")
    sidecar.write_text(f"{digest}  {output.name}\n", encoding="utf-8")
    return sidecar, digest


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("output", type=Path)
    args = parser.parse_args()
    sidecar, digest = build(args.output)
    print(args.output.resolve())
    print(sidecar)
    print(digest)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
