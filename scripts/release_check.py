"""Reproducible release gate for Channel DM Bot."""

from __future__ import annotations

import argparse
import ast
import hashlib
import os
import re
import shutil
import stat
import subprocess
import sys
import zipfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
FORBIDDEN_PARTS = {"__pycache__", ".pytest_cache", ".mypy_cache", ".ruff_cache", "htmlcov"}
FORBIDDEN_NAMES = {".coverage", "coverage.xml"}
FORBIDDEN_SUFFIXES = {".pyc", ".pyo", ".db", ".sqlite", ".sqlite3", ".session"}
FORBIDDEN_DASHES = {"\u2013", "\u2014", "\u2212"}
SECRET_PATTERNS = (
    re.compile(r"(?i)(bot[_-]?token|api[_-]?hash|openai[_-]?api[_-]?key)\s*=\s*['\"]\S{12,}"),
    re.compile(r"\b\d{7,12}:[A-Za-z0-9_-]{20,}\b"),
)


def python_files() -> list[Path]:
    return [
        p
        for p in ROOT.rglob("*.py")
        if not any(part in FORBIDDEN_PARTS for part in p.parts)
    ]


def check_version() -> list[str]:
    errors: list[str] = []
    version_path = ROOT / "VERSION"
    try:
        value = version_path.read_text(encoding="utf-8").strip()
    except OSError as exc:
        return [f"VERSION unreadable: {exc}"]
    if not re.fullmatch(r"\d+\.\d+\.\d+", value):
        errors.append(f"VERSION must be x.y.z, got {value!r}")
    return errors


def check_python() -> list[str]:
    errors: list[str] = []
    for path in python_files():
        rel = path.relative_to(ROOT)
        try:
            source = path.read_text(encoding="utf-8")
            tree = ast.parse(source, filename=str(rel))
            compile(source, str(rel), "exec")
        except (OSError, UnicodeError, SyntaxError) as exc:
            errors.append(f"{rel}: {exc}")
            continue
        if "test_support/stubs" in rel.as_posix():
            continue
        for node in ast.walk(tree):
            if isinstance(node, ast.ExceptHandler) and node.type is None:
                errors.append(f"{rel}:{node.lineno}: bare except")
            if (
                isinstance(node, ast.ExceptHandler)
                and len(node.body) == 1
                and isinstance(node.body[0], ast.Pass)
                and isinstance(node.type, ast.Name)
                and node.type.id in {"Exception", "BaseException"}
            ):
                errors.append(f"{rel}:{node.lineno}: silent broad exception")
            if isinstance(node, ast.Call) and isinstance(node.func, ast.Name):
                if node.func.id in {"eval", "exec"}:
                    errors.append(f"{rel}:{node.lineno}: forbidden {node.func.id}()")
    return errors


def check_requirements() -> list[str]:
    errors: list[str] = []
    for filename in ("requirements.txt", "requirements-dev.txt"):
        path = ROOT / filename
        if not path.exists():
            errors.append(f"missing {filename}")
            continue
        for lineno, raw in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
            line = raw.strip()
            if not line or line.startswith("#") or line.startswith("-r "):
                continue
            if "==" not in line or any(op in line for op in (">=", "<=", "~=", "!=")):
                errors.append(f"{filename}:{lineno}: dependency is not exactly pinned: {line}")
    runtime = (ROOT / "requirements.txt").read_text(encoding="utf-8")
    if re.search(r"(?m)^pytest", runtime):
        errors.append("pytest must not be a production dependency")
    return errors


def check_tree() -> list[str]:
    errors: list[str] = []
    for path in ROOT.rglob("*"):
        rel = path.relative_to(ROOT)
        if path.is_symlink():
            errors.append(f"symlink forbidden: {rel}")
            continue
        if any(part in FORBIDDEN_PARTS for part in rel.parts):
            errors.append(f"cache forbidden: {rel}")
        if path.is_file() and path.suffix.lower() in FORBIDDEN_SUFFIXES:
            errors.append(f"runtime/private artifact forbidden: {rel}")
        if path.is_file() and path.name in FORBIDDEN_NAMES:
            errors.append(f"coverage artifact forbidden: {rel}")
        if path.is_file() and path.name == ".env":
            errors.append("working .env forbidden")
        if path.is_file() and path.stat().st_size <= 2_000_000:
            try:
                text = path.read_text(encoding="utf-8")
            except (UnicodeError, OSError):
                continue
            if any(char in text for char in FORBIDDEN_DASHES):
                errors.append(f"non-ASCII dash forbidden: {rel}")
            if path.name != ".env.example":
                for pattern in SECRET_PATTERNS:
                    if pattern.search(text):
                        errors.append(f"possible embedded secret: {rel}")
                        break
    return errors


def check_zip(path: Path) -> list[str]:
    errors: list[str] = []
    try:
        with zipfile.ZipFile(path) as zf:
            for info in zf.infolist():
                name = info.filename.replace("\\", "/")
                parts = Path(name).parts
                if name.startswith("/") or ".." in parts:
                    errors.append(f"unsafe ZIP path: {name}")
                mode = (info.external_attr >> 16) & 0xFFFF
                if stat.S_ISLNK(mode):
                    errors.append(f"ZIP symlink forbidden: {name}")
                if any(part in FORBIDDEN_PARTS for part in parts):
                    errors.append(f"ZIP cache forbidden: {name}")
                if Path(name).name in FORBIDDEN_NAMES:
                    errors.append(f"ZIP coverage artifact forbidden: {name}")
                if Path(name).suffix.lower() in FORBIDDEN_SUFFIXES:
                    errors.append(f"ZIP private artifact forbidden: {name}")
    except (OSError, zipfile.BadZipFile) as exc:
        errors.append(f"invalid ZIP: {exc}")
    return errors


def run_command(command: list[str]) -> tuple[bool, str]:
    env = os.environ.copy()
    env["PYTHONDONTWRITEBYTECODE"] = "1"
    proc = subprocess.run(
        command, cwd=ROOT, text=True, capture_output=True, env=env
    )
    output = (proc.stdout + proc.stderr).strip()
    return proc.returncode == 0, output


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--zip", type=Path)
    parser.add_argument("--skip-tests", action="store_true")
    parser.add_argument("--require-dev-tools", action="store_true")
    args = parser.parse_args(argv)

    checks = {
        "version": check_version(),
        "python": check_python(),
        "requirements": check_requirements(),
        "tree": check_tree(),
    }
    if args.zip:
        checks["zip"] = check_zip(args.zip.resolve())

    command_results: list[tuple[str, str, str]] = []
    ok, out = run_command([sys.executable, "scripts/import_check.py"])
    command_results.append(("imports", "PASS" if ok else "FAIL", out))

    if not args.skip_tests:
        ok, out = run_command([sys.executable, "-m", "pytest", "-q", "-p", "no:cacheprovider"])
        command_results.append(("pytest", "PASS" if ok else "FAIL", out))

    for tool, command in (
        ("ruff", ["ruff", "check", "."]),
        ("mypy", ["mypy", "config.py", "db", "services", "texts", "utils"]),
    ):
        if shutil.which(tool):
            ok, out = run_command(command)
            command_results.append((tool, "PASS" if ok else "FAIL", out))
        elif args.require_dev_tools:
            command_results.append((tool, "FAIL", f"{tool} is not installed"))
        else:
            command_results.append((tool, "SKIP", f"{tool} is not installed; check was not executed"))

    failed = False
    for name, errors in checks.items():
        if errors:
            failed = True
            print(f"[FAIL] {name}")
            for error in errors:
                print(f"  - {error}")
        else:
            print(f"[PASS] {name}")
    for name, status, output in command_results:
        print(f"[{status}] {name}")
        if output:
            print(output)
        failed = failed or status == "FAIL"
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
