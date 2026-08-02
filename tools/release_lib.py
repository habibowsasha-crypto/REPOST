from __future__ import annotations

import ast
import asyncio
import compileall
import dataclasses
import datetime as dt
import hashlib
import json
import os
import re
import shutil
import sqlite3
import stat
import subprocess
import sys
import tabnanny
import tempfile
import time
import tokenize
import zipfile
from pathlib import Path, PurePosixPath
from typing import Any, Sequence

PROJECT_NAME = "LikeBot"
MANIFEST_NAME = "RELEASE-MANIFEST.json"
MANIFEST_SCHEMA = 1
DEFAULT_SOURCE_DATE_EPOCH = 946684800  # 2000-01-01T00:00:00Z
MAX_ARCHIVE_ENTRIES = 5_000
MAX_ARCHIVE_FILE_BYTES = 32 * 1024 * 1024
MAX_ARCHIVE_TOTAL_BYTES = 256 * 1024 * 1024
MAX_ARCHIVE_PATH_BYTES = 512
LOCKED_CRYPTOGRAPHY_VERSION = "50.0.0"

LOCK_REQUIREMENT_RE = re.compile(
    r"^([A-Za-z0-9_.-]+)(?:\[[A-Za-z0-9_.-]+(?:,[A-Za-z0-9_.-]+)*\])?"
    r"==([^;\s\\]+)(?:\s*;.*)?(?:\s*\\)?$"
)
LOCK_HASH_RE = re.compile(r"--hash=sha256:([0-9a-f]{64})(?:\s*\\)?$")

EXCLUDED_DIR_NAMES = {
    ".git",
    ".hg",
    ".svn",
    ".idea",
    ".vscode",
    ".pytest_cache",
    ".ruff_cache",
    ".mypy_cache",
    ".tox",
    ".nox",
    "__pycache__",
    "htmlcov",
    "dist",
    "build",
    "data",
    "node_modules",
    "venv",
    ".venv",
}

FORBIDDEN_FILE_NAMES = {
    ".env",
    ".coverage",
    "coverage.xml",
    "pytestdebug.log",
}

FORBIDDEN_SUFFIXES = {
    ".pyc",
    ".pyo",
    ".db",
    ".sqlite",
    ".sqlite3",
    ".session",
    ".log",
    ".pem",
    ".key",
    ".p12",
    ".pfx",
}

TELEGRAM_TOKEN_RE = re.compile(rb"(?<![A-Za-z0-9_-])(\d{6,12}):([A-Za-z0-9_-]{30,})(?![A-Za-z0-9_-])")
OPENAI_KEY_RE = re.compile(
    rb"(?<![A-Za-z0-9_-])sk-(?:proj-)?[A-Za-z0-9_-]{20,}(?![A-Za-z0-9_-])"
)
PRIVATE_KEY_RE = re.compile(rb"-----BEGIN (?:RSA |EC |OPENSSH |DSA )?PRIVATE KEY-----")
CREDENTIAL_URL_TEXT_RE = re.compile(
    r"(?i)\b(?:postgres(?:ql)?|mysql|mariadb|redis|mongodb(?:\+srv)?)://([^\s/:]+):([^\s/@]+)@([^\s/]+)"
)
SENSITIVE_ASSIGNMENT_RE = re.compile(
    r"(?im)^\s*(?:export\s+)?(BOT_TOKEN|API_HASH|SESSION_ENCRYPTION_KEY|DATABASE_URL|OPENAI_API_KEY)\s*=\s*([^\r\n#]+)"
)
SENSITIVE_COLON_RE = re.compile(
    r"(?m)^\s*[\"']?(BOT_TOKEN|API_HASH|SESSION_ENCRYPTION_KEY|DATABASE_URL|OPENAI_API_KEY)[\"']?\s*:\s*[\"']?([^\r\n#\"']+)"
)
PLACEHOLDER_MARKERS = {
    "",
    "changeme",
    "change_me",
    "replace_me",
    "your_bot_token",
    "your_api_hash",
    "your_session_encryption_key",
    "your_database_url",
    "example",
    "placeholder",
    "dummy",
    "test",
    "token",
    "hash",
    "key",
}


class ReleaseError(RuntimeError):
    """A release invariant failed."""


@dataclasses.dataclass(slots=True)
class CheckResult:
    name: str
    status: str
    details: str = ""
    duration_seconds: float = 0.0
    required: bool = True

    def to_dict(self) -> dict[str, Any]:
        return dataclasses.asdict(self)


@dataclasses.dataclass(slots=True)
class VerificationReport:
    project: str
    version: str
    started_at_utc: str
    finished_at_utc: str
    python: str
    platform: str
    checks: list[CheckResult]

    @property
    def passed(self) -> bool:
        if not self.checks:
            return False
        return all(
            item.status != "failed" and (not item.required or item.status == "passed")
            for item in self.checks
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema": 1,
            "project": self.project,
            "version": self.version,
            "passed": self.passed,
            "started_at_utc": self.started_at_utc,
            "finished_at_utc": self.finished_at_utc,
            "python": self.python,
            "platform": self.platform,
            "checks": [item.to_dict() for item in self.checks],
        }


def utc_now_text() -> str:
    return dt.datetime.now(dt.timezone.utc).replace(microsecond=0).isoformat()


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def version_to_token(version: str) -> str:
    if not re.fullmatch(r"\d+\.\d+\.\d+(?:[A-Za-z0-9.-]+)?", version):
        raise ReleaseError(f"Некорректная версия: {version!r}")
    return version.replace(".", "_").replace("-", "_")


def read_project_version(project_root: Path) -> str:
    init_path = project_root / "laika_bot" / "__init__.py"
    try:
        tree = ast.parse(init_path.read_text(encoding="utf-8"), filename=str(init_path))
    except (OSError, SyntaxError) as exc:
        raise ReleaseError(f"Не удалось прочитать версию из {init_path}: {exc}") from exc
    for node in tree.body:
        if isinstance(node, ast.Assign):
            for target in node.targets:
                if isinstance(target, ast.Name) and target.id == "__version__":
                    if isinstance(node.value, ast.Constant) and isinstance(node.value.value, str):
                        version_to_token(node.value.value)
                        return node.value.value
    raise ReleaseError("В laika_bot/__init__.py отсутствует строковый __version__")


def _is_relative_to(path: Path, root: Path) -> bool:
    try:
        path.relative_to(root)
        return True
    except ValueError:
        return False


def _forbidden_path_reason(relative: PurePosixPath) -> str | None:
    parts = relative.parts
    if not parts:
        return "пустой путь"
    if any(part in EXCLUDED_DIR_NAMES for part in parts[:-1]):
        return "служебный или build-каталог"
    name = parts[-1]
    lower_name = name.lower()
    if name in FORBIDDEN_FILE_NAMES:
        return "запрещённый служебный файл"
    if lower_name.startswith(".env.") and lower_name != ".env.example":
        return "секретный env-файл"
    if Path(name).suffix.lower() in FORBIDDEN_SUFFIXES:
        return "запрещённый runtime/credential файл"
    if lower_name.endswith((".zip", ".tar", ".tar.gz", ".tgz", ".7z", ".rar")):
        return "вложенный архив"
    return None


def collect_release_files(project_root: Path) -> list[Path]:
    root = project_root.resolve()
    if not root.is_dir():
        raise ReleaseError(f"Каталог проекта не найден: {root}")
    files: list[Path] = []
    for current, dirs, names in os.walk(root, topdown=True, followlinks=False):
        current_path = Path(current)
        kept_dirs: list[str] = []
        for name in sorted(dirs):
            child = current_path / name
            if child.is_symlink():
                raise ReleaseError(f"Symlink-каталог запрещён: {child.relative_to(root)}")
            if name in EXCLUDED_DIR_NAMES:
                continue
            kept_dirs.append(name)
        dirs[:] = kept_dirs
        for name in sorted(names):
            path = current_path / name
            if path.is_symlink():
                raise ReleaseError(f"Symlink-файл запрещён: {path.relative_to(root)}")
            if not path.is_file():
                raise ReleaseError(f"Неподдерживаемый тип файла: {path.relative_to(root)}")
            relative = PurePosixPath(path.relative_to(root).as_posix())
            reason = _forbidden_path_reason(relative)
            if reason:
                raise ReleaseError(f"{relative}: {reason}")
            files.append(path)
    if not files:
        raise ReleaseError("Каталог проекта не содержит файлов")
    return sorted(files, key=lambda item: item.relative_to(root).as_posix())


def _looks_like_placeholder(raw_value: str) -> bool:
    value = raw_value.strip().strip("'\"").strip()
    lowered = value.lower()
    if lowered in PLACEHOLDER_MARKERS:
        return True
    if any(marker in lowered for marker in ("<", ">", "${", "{{", "example", "placeholder", "changeme", "replace")):
        return True
    if lowered.startswith(("sqlite", "postgresql://user:password@", "postgres://user:password@")):
        return True
    if value and len(set(value)) <= 2:
        return True
    if lowered.startswith("mdawmdawmdaw"):
        return True
    return False


def _looks_like_fake_telegram_token(payload: bytes) -> bool:
    lowered = payload.lower()
    if len(set(payload)) <= 4:
        return True
    if b"abcdefghijklmnopqrstuvwxyz" in lowered or b"0123456789" * 2 in lowered:
        return True
    return any(marker in lowered for marker in (b"placeholder", b"example", b"dummy", b"testtoken"))


def _looks_like_placeholder_credential(username: str, password: str, host: str) -> bool:
    combined = f"{username}:{password}@{host}".lower()
    markers = (
        "user:password",
        "username:password",
        "likebot_test",
        "dbpass",
        "changeme",
        "placeholder",
        "dummy",
        "example",
        "hunter2",
        "redacted",
        "&lt;redacted&gt;",
    )
    return host.lower().endswith((".invalid", ".internal")) or any(marker in combined for marker in markers)


def _sensitive_value_is_placeholder(key: str, raw_value: str) -> bool:
    value = raw_value.strip().strip("'\"").strip()
    if _looks_like_placeholder(value):
        return True
    if key == "BOT_TOKEN":
        match = TELEGRAM_TOKEN_RE.search(value.encode("utf-8", errors="ignore"))
        return bool(match and _looks_like_fake_telegram_token(match.group(2)))
    if key == "DATABASE_URL":
        matches = CREDENTIAL_URL_TEXT_RE.findall(value)
        return bool(matches) and all(_looks_like_placeholder_credential(*item) for item in matches)
    return False


def scan_secret_bytes(relative_path: str, data: bytes) -> list[str]:
    findings: list[str] = []
    for match in TELEGRAM_TOKEN_RE.finditer(data):
        if not _looks_like_fake_telegram_token(match.group(2)):
            findings.append("похожий на настоящий Telegram Bot token")
            break
    if PRIVATE_KEY_RE.search(data):
        findings.append("приватный ключ")
    if OPENAI_KEY_RE.search(data):
        findings.append("похожий на настоящий OpenAI API key")
    text = data.decode("utf-8", errors="ignore")
    for username, password, host in CREDENTIAL_URL_TEXT_RE.findall(text):
        if not _looks_like_placeholder_credential(username, password, host):
            findings.append("URL с логином и паролем")
            break
    if relative_path != ".env.example":
        assignments = [*SENSITIVE_ASSIGNMENT_RE.findall(text), *SENSITIVE_COLON_RE.findall(text)]
        for key, value in assignments:
            if not _sensitive_value_is_placeholder(key, value):
                findings.append(f"literal-секрет {key}")
    return sorted(set(findings))


def scan_source_secrets(project_root: Path, files: Sequence[Path] | None = None) -> list[str]:
    root = project_root.resolve()
    candidates = list(files) if files is not None else collect_release_files(root)
    findings: list[str] = []
    for path in candidates:
        relative = path.relative_to(root).as_posix()
        data = path.read_bytes()
        for finding in scan_secret_bytes(relative, data):
            findings.append(f"{relative}: {finding}")
    return findings


def check_version_consistency(project_root: Path, version: str) -> None:
    readme = (project_root / "README.md").read_text(encoding="utf-8")
    changelog = (project_root / "CHANGELOG.md").read_text(encoding="utf-8")
    audit = (project_root / "AUDIT.txt").read_text(encoding="utf-8")
    readme_match = re.search(r"^#\s+.*?LikeBot\s+v([0-9A-Za-z.-]+)\s*$", readme, re.MULTILINE)
    changelog_match = re.search(r"^##\s+([0-9A-Za-z.-]+)\b", changelog, re.MULTILINE)
    if not readme_match or readme_match.group(1) != version:
        raise ReleaseError(f"README.md не начинается с версии {version}")
    if not changelog_match or changelog_match.group(1) != version:
        raise ReleaseError(f"Первая запись CHANGELOG.md должна быть {version}")
    if f"v{version}" not in audit:
        raise ReleaseError(f"AUDIT.txt не содержит v{version}")


def _normalized_requirement_name(value: str) -> str:
    return re.sub(r"[-_.]+", "-", value).lower()


def _read_exact_input_pins(path: Path, *, allowed_include: str | None = None) -> dict[str, str]:
    pins: dict[str, str] = {}
    include_seen = False
    for line_number, raw in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
        value = raw.strip()
        if not value or value.startswith("#"):
            continue
        if value.startswith("-r "):
            include = value[3:].strip()
            if allowed_include is None or include != allowed_include or include_seen:
                raise ReleaseError(f"{path.name}:{line_number}: недопустимый include {include!r}")
            include_seen = True
            continue
        match = LOCK_REQUIREMENT_RE.fullmatch(value)
        if match is None:
            raise ReleaseError(f"{path.name}:{line_number}: зависимость должна иметь точный pin ==")
        name = _normalized_requirement_name(match.group(1))
        if name in pins:
            raise ReleaseError(f"{path.name}:{line_number}: повтор зависимости {name}")
        pins[name] = match.group(2)
    if not pins:
        raise ReleaseError(f"{path.name}: отсутствуют зависимости")
    if allowed_include is not None and not include_seen:
        raise ReleaseError(f"{path.name}: отсутствует обязательный include {allowed_include}")
    return pins


def _read_hashed_lock(path: Path) -> dict[str, tuple[str, frozenset[str]]]:
    records: list[list[tuple[int, str]]] = []
    current: list[tuple[int, str]] = []
    for line_number, raw in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
        value = raw.strip()
        if not value or value.startswith("#"):
            continue
        if raw[:1].isspace() or value.startswith("--hash="):
            if not current:
                raise ReleaseError(f"{path.name}:{line_number}: hash без зависимости")
            current.append((line_number, value))
            continue
        if current:
            records.append(current)
        current = [(line_number, value)]
    if current:
        records.append(current)

    locked: dict[str, tuple[str, frozenset[str]]] = {}
    for record in records:
        line_number, header = record[0]
        match = LOCK_REQUIREMENT_RE.fullmatch(header)
        if match is None:
            raise ReleaseError(f"{path.name}:{line_number}: lock содержит неточный или внешний requirement")
        name = _normalized_requirement_name(match.group(1))
        hashes = frozenset(
            hash_match.group(1)
            for _, value in record[1:]
            if (hash_match := LOCK_HASH_RE.fullmatch(value)) is not None
        )
        if not hashes:
            raise ReleaseError(f"{path.name}:{line_number}: {name} не имеет SHA-256 hash")
        unexpected = [value for _, value in record[1:] if LOCK_HASH_RE.fullmatch(value) is None]
        if unexpected:
            raise ReleaseError(f"{path.name}:{line_number}: недопустимые lock-опции для {name}")
        if name in locked:
            raise ReleaseError(f"{path.name}:{line_number}: повтор зависимости {name}")
        locked[name] = (match.group(2), hashes)
    if not locked:
        raise ReleaseError(f"{path.name}: lock-файл пуст")
    return locked


def check_dependency_locks(project_root: Path) -> str:
    root = project_root.resolve()
    required_paths = {
        name: root / name
        for name in (
            "requirements.in",
            "requirements.txt",
            "requirements-dev.in",
            "requirements-dev.txt",
            "Dockerfile",
            ".github/workflows/release-validation.yml",
        )
    }
    missing = [name for name, path in required_paths.items() if not path.is_file()]
    if missing:
        raise ReleaseError("Отсутствуют dependency-lock файлы: " + ", ".join(missing))

    production_pins = _read_exact_input_pins(required_paths["requirements.in"])
    development_pins = _read_exact_input_pins(
        required_paths["requirements-dev.in"], allowed_include="requirements.in"
    )
    production_lock = _read_hashed_lock(required_paths["requirements.txt"])
    development_lock = _read_hashed_lock(required_paths["requirements-dev.txt"])

    cryptography_version = production_pins.get("cryptography")
    if cryptography_version != LOCKED_CRYPTOGRAPHY_VERSION:
        raise ReleaseError(
            "cryptography должен быть зафиксирован на security-версии "
            f"{LOCKED_CRYPTOGRAPHY_VERSION}, получено {cryptography_version!r}"
        )
    for name, version in production_pins.items():
        locked = production_lock.get(name)
        if locked is None or locked[0] != version:
            raise ReleaseError(f"requirements.txt не фиксирует direct dependency {name}=={version}")
        dev_locked = development_lock.get(name)
        if dev_locked is None or dev_locked != locked:
            raise ReleaseError(f"requirements-dev.txt расходится с production lock для {name}")
    for name, version in development_pins.items():
        locked = development_lock.get(name)
        if locked is None or locked[0] != version:
            raise ReleaseError(f"requirements-dev.txt не фиксирует dev dependency {name}=={version}")

    dockerfile = required_paths["Dockerfile"].read_text(encoding="utf-8")
    if not re.search(
        r"python\s+-m\s+pip\s+install\s+--no-cache-dir\s+--require-hashes\s+-r\s+requirements\.txt",
        dockerfile,
    ):
        raise ReleaseError("Dockerfile должен устанавливать requirements.txt с --require-hashes")
    workflow = required_paths[".github/workflows/release-validation.yml"].read_text(encoding="utf-8")
    if not re.search(
        r"python\s+-m\s+pip\s+install\s+--require-hashes\s+-r\s+requirements-dev\.txt",
        workflow,
    ):
        raise ReleaseError("CI должен устанавливать requirements-dev.txt с --require-hashes")

    return (
        f"Production packages: {len(production_lock)}, dev packages: {len(development_lock)}, "
        f"cryptography: {LOCKED_CRYPTOGRAPHY_VERSION}"
    )


def _duplicate_definitions(tree: ast.AST, filename: str) -> list[str]:
    findings: list[str] = []

    def inspect_scope(body: list[ast.stmt], scope: str) -> None:
        seen: dict[str, int] = {}
        for node in body:
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
                previous = seen.get(node.name)
                if previous is not None:
                    findings.append(f"{filename}:{node.lineno}: повтор {scope}.{node.name}, впервые строка {previous}")
                else:
                    seen[node.name] = node.lineno
                if isinstance(node, ast.ClassDef):
                    inspect_scope(node.body, f"{scope}.{node.name}")
    if isinstance(tree, ast.Module):
        inspect_scope(tree.body, "module")
    return findings


def _dangerous_calls(tree: ast.AST, filename: str) -> list[str]:
    findings: list[str] = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        if isinstance(node.func, ast.Name) and node.func.id in {"eval", "exec"}:
            findings.append(f"{filename}:{node.lineno}: запрещён {node.func.id}()")
        if isinstance(node.func, ast.Attribute):
            owner = node.func.value.id if isinstance(node.func.value, ast.Name) else None
            if owner == "os" and node.func.attr in {"system", "popen"}:
                findings.append(f"{filename}:{node.lineno}: запрещён os.{node.func.attr}()")
            if owner in {"pickle", "marshal"} and node.func.attr in {"load", "loads"}:
                findings.append(f"{filename}:{node.lineno}: запрещён {owner}.{node.func.attr}()")
            if owner == "yaml" and node.func.attr == "load":
                findings.append(f"{filename}:{node.lineno}: небезопасный yaml.load()")
            if owner == "subprocess":
                for keyword in node.keywords:
                    if keyword.arg == "shell" and isinstance(keyword.value, ast.Constant) and keyword.value.value is True:
                        findings.append(f"{filename}:{node.lineno}: subprocess с shell=True")
    return findings


def static_python_checks(project_root: Path) -> tuple[int, int]:
    python_files = sorted(project_root.rglob("*.py"))
    python_files = [p for p in python_files if not any(part in EXCLUDED_DIR_NAMES for part in p.relative_to(project_root).parts)]
    duplicate_findings: list[str] = []
    danger_findings: list[str] = []
    with tempfile.TemporaryDirectory(prefix="likebot-pycache-") as pycache_dir:
        previous_prefix = sys.pycache_prefix
        sys.pycache_prefix = pycache_dir
        try:
            for path in python_files:
                text = path.read_text(encoding="utf-8")
                tree = ast.parse(text, filename=str(path))
                compile(tree, str(path), "exec")
                duplicate_findings.extend(_duplicate_definitions(tree, path.relative_to(project_root).as_posix()))
                danger_findings.extend(_dangerous_calls(tree, path.relative_to(project_root).as_posix()))
                try:
                    with tokenize.open(path) as source:
                        tabnanny.process_tokens(tokenize.generate_tokens(source.readline))
                except tabnanny.NannyNag as exc:
                    raise ReleaseError(
                        f"Tab/space ambiguity in {path.relative_to(project_root)}:{exc.get_lineno()}: {exc.get_msg()}"
                    ) from exc
            success = compileall.compile_dir(
                str(project_root),
                quiet=2,
                force=True,
                rx=re.compile(r"/(?:\.git|dist|build|__pycache__)/"),
            )
            if not success:
                raise ReleaseError("compileall завершился ошибкой")
        finally:
            sys.pycache_prefix = previous_prefix
    if duplicate_findings:
        raise ReleaseError("Дублирующиеся определения:\n" + "\n".join(duplicate_findings))
    if danger_findings:
        raise ReleaseError("Опасные конструкции:\n" + "\n".join(danger_findings))
    return len(python_files), sum(1 for path in python_files if path.name.startswith("test_"))


def run_process(
    args: Sequence[str],
    *,
    cwd: Path,
    env: dict[str, str] | None = None,
    timeout: int = 900,
    check: bool = True,
) -> subprocess.CompletedProcess[str]:
    merged_env = os.environ.copy()
    if env:
        merged_env.update(env)
    completed = subprocess.run(
        list(args),
        cwd=str(cwd),
        env=merged_env,
        stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        encoding="utf-8",
        errors="replace",
        timeout=timeout,
        check=False,
        shell=False,
    )
    if check and completed.returncode != 0:
        output = completed.stdout[-12_000:]
        raise ReleaseError(f"Команда завершилась с кодом {completed.returncode}: {' '.join(args)}\n{output}")
    return completed


def run_pytest(project_root: Path, pytest_args: Sequence[str] = ()) -> str:
    command = [sys.executable, "-m", "pytest", "-q", *pytest_args]
    result = run_process(command, cwd=project_root, timeout=1_800)
    return result.stdout.strip()


def run_ruff(project_root: Path) -> str:
    command = [sys.executable, "-m", "ruff", "check", "."]
    result = run_process(command, cwd=project_root, timeout=300)
    return result.stdout.strip()


async def _sqlite_smoke_async(project_root: Path) -> str:
    if str(project_root) not in sys.path:
        sys.path.insert(0, str(project_root))
    from sqlalchemy import inspect

    from laika_bot.ai_comments_models import AI_COMMENTS_TABLE_NAMES
    from laika_bot.db import Database

    legacy_schema = """
    CREATE TABLE channels (
        id INTEGER PRIMARY KEY, telegram_channel_id INTEGER NOT NULL,
        title VARCHAR(255) NOT NULL, username VARCHAR(64), link TEXT NOT NULL,
        is_active BOOLEAN NOT NULL, new_posts_enabled BOOLEAN NOT NULL,
        old_posts_enabled BOOLEAN NOT NULL, old_posts_depth INTEGER NOT NULL,
        last_seen_message_id INTEGER NOT NULL, last_error TEXT,
        created_at DATETIME NOT NULL, updated_at DATETIME NOT NULL
    );
    CREATE TABLE accounts (
        id INTEGER PRIMARY KEY, phone VARCHAR(40) NOT NULL,
        telegram_user_id INTEGER NOT NULL, display_name VARCHAR(255) NOT NULL,
        username VARCHAR(64), session_encrypted TEXT NOT NULL,
        is_active BOOLEAN NOT NULL, status VARCHAR(32) NOT NULL,
        flood_until DATETIME, last_reaction_at DATETIME, last_error TEXT,
        created_at DATETIME NOT NULL, updated_at DATETIME NOT NULL
    );
    CREATE TABLE join_jobs (
        id INTEGER PRIMARY KEY, channel_id INTEGER NOT NULL, account_id INTEGER NOT NULL,
        due_at DATETIME NOT NULL, status VARCHAR(24) NOT NULL, attempts INTEGER NOT NULL,
        error TEXT, completed_at DATETIME, created_at DATETIME NOT NULL
    );
    CREATE TABLE reaction_jobs (
        id INTEGER PRIMARY KEY, channel_id INTEGER NOT NULL, account_id INTEGER NOT NULL,
        message_id INTEGER NOT NULL, reaction VARCHAR(64) NOT NULL, due_at DATETIME NOT NULL,
        status VARCHAR(24) NOT NULL, attempts INTEGER NOT NULL, error TEXT,
        completed_at DATETIME, created_at DATETIME NOT NULL
    );
    CREATE TABLE view_jobs (
        id INTEGER PRIMARY KEY, batch_id INTEGER NOT NULL, channel_id INTEGER NOT NULL,
        account_id INTEGER NOT NULL, message_id INTEGER NOT NULL, due_at DATETIME NOT NULL,
        status VARCHAR(24) NOT NULL, attempts INTEGER NOT NULL, error TEXT,
        completed_at DATETIME, created_at DATETIME NOT NULL
    );
    """

    async def inspect_database(database: Database) -> tuple[list[str], dict[str, set[str]], dict[str, set[str]]]:
        async with database.engine.connect() as connection:
            tables = await connection.run_sync(lambda sync: sorted(inspect(sync).get_table_names()))
            columns: dict[str, set[str]] = {}
            indexes: dict[str, set[str]] = {}
            inspected_tables = (
                "accounts",
                "channels",
                "join_jobs",
                "reaction_jobs",
                "view_jobs",
                "ai_generation_jobs",
                "ai_publication_jobs",
                "ai_comment_drafts",
                "ai_account_profiles",
                "ai_account_profile_revisions",
                "ai_knowledge_chunks",
                "ai_settings",
            )
            for table in inspected_tables:
                columns[table] = await connection.run_sync(
                    lambda sync, table=table: {item["name"] for item in inspect(sync).get_columns(table)}
                )
                indexes[table] = await connection.run_sync(
                    lambda sync, table=table: {item["name"] for item in inspect(sync).get_indexes(table)}
                )
            return tables, columns, indexes

    with tempfile.TemporaryDirectory(prefix="likebot-sqlite-") as temp_dir:
        temp = Path(temp_dir)

        fresh_path = temp / "fresh.db"
        fresh = Database(f"sqlite+aiosqlite:///{fresh_path}")
        try:
            await fresh.init()
            await fresh.init()
            fresh_tables, _, _ = await inspect_database(fresh)
        finally:
            await fresh.close()

        legacy_path = temp / "legacy.db"
        sync_connection = sqlite3.connect(legacy_path)
        try:
            sync_connection.executescript(legacy_schema)
            sync_connection.commit()
        finally:
            sync_connection.close()

        legacy = Database(f"sqlite+aiosqlite:///{legacy_path}")
        try:
            await legacy.init()
            await legacy.init()
            legacy_tables, legacy_columns, legacy_indexes = await inspect_database(legacy)
        finally:
            await legacy.close()

    required_tables = {
        "accounts", "channels", "join_jobs", "reaction_jobs", "view_batches",
        "view_jobs", "app_settings", "configuration_events",
    }.union(AI_COMMENTS_TABLE_NAMES)
    for label, tables in (("fresh", fresh_tables), ("legacy", legacy_tables)):
        missing = sorted(required_tables.difference(tables))
        if missing:
            raise ReleaseError(f"SQLite {label}: отсутствуют таблицы {missing}")

    required_columns = {
        "accounts": {"email_login", "problem_reason", "telegram_user_id"},
        "channels": {"kind", "reactions_json", "promotion_mode", "image_post_reaction_percent"},
        "join_jobs": {"action", "started_at"},
        "reaction_jobs": {"source", "view_included", "view_confirmed_at", "started_at"},
        "view_jobs": {"started_at"},
        "ai_generation_jobs": {"idempotency_key", "source_post_revision", "locked_at"},
        "ai_publication_jobs": {"idempotency_key", "draft_revision", "publish_confirmed_at"},
        "ai_comment_drafts": {"revision", "lock_version", "source_post_hash"},
        "ai_account_profiles": {
            "account_id",
            "telegram_user_id",
            "style_json",
            "profile_version",
            "enabled",
        },
        "ai_account_profile_revisions": {
            "profile_id",
            "telegram_user_id",
            "profile_version",
            "snapshot_hash",
        },
        "ai_knowledge_chunks": {"chunk_key", "review_status", "index_eligible"},
        "ai_settings": {"value_json", "value_version", "updated_by"},
    }
    for table, expected in required_columns.items():
        missing = sorted(expected.difference(legacy_columns[table]))
        if missing:
            raise ReleaseError(f"SQLite legacy migration {table}: отсутствуют колонки {missing}")

    required_indexes = {
        "join_jobs": {"ix_join_jobs_status_due_id", "ix_join_jobs_status_started_id"},
        "reaction_jobs": {"ix_reaction_jobs_status_due_id", "ix_reaction_jobs_status_started_id"},
        "view_jobs": {"ix_view_jobs_status_due_id", "ix_view_jobs_status_started_id"},
        "ai_generation_jobs": {"ix_ai_generation_jobs_queue"},
        "ai_publication_jobs": {"ix_ai_publication_jobs_queue"},
        "ai_comment_drafts": {"ix_ai_comment_drafts_thread_status"},
        "ai_account_profiles": {
            "ix_ai_account_profiles_enabled_role",
            "uq_ai_account_profiles_telegram_user",
        },
        "ai_account_profile_revisions": {
            "ix_ai_account_profile_revisions_identity_version",
            "ix_ai_account_profile_revisions_profile_created",
        },
        "ai_knowledge_chunks": {"ix_ai_knowledge_chunks_retrieval"},
        "ai_settings": {"ix_ai_settings_updated"},
    }
    for table, expected in required_indexes.items():
        missing = sorted(expected.difference(legacy_indexes[table]))
        if missing:
            raise ReleaseError(f"SQLite legacy migration {table}: отсутствуют индексы {missing}")

    return (
        f"SQLite fresh/idempotence: {len(fresh_tables)} таблиц; "
        f"legacy migration/idempotence: {len(legacy_tables)} таблиц"
    )


def run_sqlite_smoke(project_root: Path) -> str:
    return asyncio.run(_sqlite_smoke_async(project_root))


def compile_postgresql_schema(project_root: Path) -> str:
    if str(project_root) not in sys.path:
        sys.path.insert(0, str(project_root))
    from sqlalchemy.dialects import postgresql
    from sqlalchemy.schema import CreateIndex, CreateTable

    from laika_bot.ai_comments_models import AI_COMMENTS_TABLE_NAMES
    from laika_bot.models import Base

    dialect = postgresql.dialect()
    statements: list[str] = []
    for table in Base.metadata.sorted_tables:
        statements.append(str(CreateTable(table).compile(dialect=dialect)))
        for index in sorted(table.indexes, key=lambda item: item.name or ""):
            statements.append(str(CreateIndex(index).compile(dialect=dialect)))
    joined = "\n".join(statements)
    if (
        "CREATE TABLE accounts" not in joined
        or "CREATE TABLE ai_generation_jobs" not in joined
        or "CREATE TABLE ai_account_profile_revisions" not in joined
        or "uq_ai_account_profiles_telegram_user" not in joined
        or "BIGINT" not in joined
        or "ON DELETE SET NULL" not in joined
        or not set(AI_COMMENTS_TABLE_NAMES).issubset(Base.metadata.tables)
    ):
        raise ReleaseError("PostgreSQL schema compile не содержит ожидаемые таблицы/BIGINT")
    return f"PostgreSQL DDL: {len(Base.metadata.tables)} таблиц, {len(statements)} statements"


async def _postgres_smoke_async(project_root: Path, url: str) -> str:
    if str(project_root) not in sys.path:
        sys.path.insert(0, str(project_root))
    from sqlalchemy import inspect

    from laika_bot.db import Database

    database = Database(url)
    try:
        await database.init()
        await database.init()
        async with database.engine.connect() as connection:
            tables = await connection.run_sync(lambda sync: sorted(inspect(sync).get_table_names()))
        from laika_bot.ai_comments_models import AI_COMMENTS_TABLE_NAMES

        required = {
            "accounts", "channels", "join_jobs", "reaction_jobs", "view_jobs", "app_settings"
        }.union(AI_COMMENTS_TABLE_NAMES)
        missing = sorted(required.difference(tables))
        if missing:
            raise ReleaseError(f"PostgreSQL smoke: отсутствуют таблицы {missing}")
    finally:
        close = getattr(database, "close", None)
        if close is not None:
            await close()
        else:
            await database.engine.dispose()
    return f"PostgreSQL init/idempotence: {len(tables)} таблиц"


def run_postgres_smoke(project_root: Path, url: str) -> str:
    return asyncio.run(_postgres_smoke_async(project_root, url))


def _safe_archive_member(name: str) -> PurePosixPath:
    if not name or "\x00" in name or "\\" in name:
        raise ReleaseError(f"Небезопасное имя в ZIP: {name!r}")
    path = PurePosixPath(name)
    if path.is_absolute() or any(part in {"", ".", ".."} for part in path.parts):
        raise ReleaseError(f"Небезопасный путь в ZIP: {name!r}")
    if re.fullmatch(r"[A-Za-z]:", path.parts[0]) or any(part.endswith((" ", ".")) for part in path.parts):
        raise ReleaseError(f"Windows-небезопасный путь в ZIP: {name!r}")
    if len(name.encode("utf-8")) > MAX_ARCHIVE_PATH_BYTES:
        raise ReleaseError(f"Слишком длинный путь в ZIP: {name!r}")
    return path


def safe_extract_zip(archive_path: Path, destination: Path) -> Path:
    destination = destination.resolve()
    destination.mkdir(parents=True, exist_ok=True)
    total_size = 0
    seen: set[str] = set()
    roots: set[str] = set()
    with zipfile.ZipFile(archive_path, "r") as archive:
        infos = archive.infolist()
        if len(infos) > MAX_ARCHIVE_ENTRIES:
            raise ReleaseError("ZIP содержит слишком много записей")
        for info in infos:
            relative = _safe_archive_member(info.filename.rstrip("/"))
            roots.add(relative.parts[0])
            normalized = relative.as_posix().casefold()
            if normalized in seen:
                raise ReleaseError(f"Дубликат/case-collision в ZIP: {info.filename}")
            seen.add(normalized)
            mode = info.external_attr >> 16
            if info.flag_bits & 0x1:
                raise ReleaseError(f"Зашифрованная ZIP-запись запрещена: {info.filename}")
            if stat.S_ISLNK(mode):
                raise ReleaseError(f"Symlink в ZIP запрещён: {info.filename}")
            if mode and not (stat.S_ISREG(mode) or stat.S_ISDIR(mode)):
                raise ReleaseError(f"Специальный файл в ZIP запрещён: {info.filename}")
            if info.file_size > MAX_ARCHIVE_FILE_BYTES:
                raise ReleaseError(f"Слишком большой файл в ZIP: {info.filename}")
            total_size += info.file_size
            if total_size > MAX_ARCHIVE_TOTAL_BYTES:
                raise ReleaseError("ZIP превышает общий лимит распаковки")
            target = (destination / Path(*relative.parts)).resolve()
            if not _is_relative_to(target, destination):
                raise ReleaseError(f"ZIP traversal: {info.filename}")
            if info.is_dir():
                target.mkdir(parents=True, exist_ok=True)
                continue
            target.parent.mkdir(parents=True, exist_ok=True)
            with archive.open(info, "r") as source, target.open("wb") as output:
                shutil.copyfileobj(source, output, length=1024 * 1024)
    if len(roots) != 1:
        raise ReleaseError(f"ZIP должен иметь один корневой каталог, найдено: {sorted(roots)}")
    root = destination / next(iter(roots))
    if not root.is_dir():
        raise ReleaseError("Корень ZIP не является каталогом")
    return root


def source_date_epoch() -> int:
    raw = os.environ.get("SOURCE_DATE_EPOCH", str(DEFAULT_SOURCE_DATE_EPOCH))
    try:
        value = int(raw)
    except ValueError as exc:
        raise ReleaseError("SOURCE_DATE_EPOCH должен быть целым Unix timestamp") from exc
    if value < 315532800:  # ZIP epoch: 1980-01-01
        raise ReleaseError("SOURCE_DATE_EPOCH должен быть не раньше 1980-01-01")
    # ZIP stores years in the inclusive range 1980..2107. Validate before
    # opening a temporary artifact so an invalid build cannot leave debris.
    if value > 4354819198:  # 2107-12-31 23:59:58 UTC
        raise ReleaseError("SOURCE_DATE_EPOCH должен быть не позже 2107-12-31")
    return value


def _zip_datetime(epoch: int) -> tuple[int, int, int, int, int, int]:
    moment = dt.datetime.fromtimestamp(epoch, tz=dt.timezone.utc)
    second = moment.second - (moment.second % 2)
    return moment.year, moment.month, moment.day, moment.hour, moment.minute, second


def build_manifest(project_root: Path, archive_root: str, files: Sequence[Path], epoch: int) -> dict[str, Any]:
    root = project_root.resolve()
    entries = []
    for path in files:
        relative = path.relative_to(root).as_posix()
        if relative == MANIFEST_NAME:
            continue
        data = path.read_bytes()
        entries.append({"path": relative, "size": len(data), "sha256": sha256_bytes(data)})
    return {
        "schema": MANIFEST_SCHEMA,
        "project": PROJECT_NAME,
        "version": read_project_version(root),
        "archive_root": archive_root,
        "source_date_epoch": epoch,
        "files": entries,
    }


def write_deterministic_zip(project_root: Path, archive_path: Path, archive_root: str) -> dict[str, Any]:
    files = collect_release_files(project_root)
    epoch = source_date_epoch()
    manifest = build_manifest(project_root, archive_root, files, epoch)
    manifest_bytes = (json.dumps(manifest, ensure_ascii=False, sort_keys=True, indent=2) + "\n").encode("utf-8")
    archive_path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = archive_path.with_suffix(archive_path.suffix + ".tmp")
    timestamp = _zip_datetime(epoch)
    with zipfile.ZipFile(temp_path, "w", compression=zipfile.ZIP_DEFLATED, compresslevel=9, strict_timestamps=True) as archive:
        for path in files:
            relative = path.relative_to(project_root).as_posix()
            if relative == MANIFEST_NAME:
                continue
            info = zipfile.ZipInfo(f"{archive_root}/{relative}", date_time=timestamp)
            info.compress_type = zipfile.ZIP_DEFLATED
            info.create_system = 3
            info.external_attr = (0o100644 << 16)
            archive.writestr(info, path.read_bytes())
        manifest_info = zipfile.ZipInfo(f"{archive_root}/{MANIFEST_NAME}", date_time=timestamp)
        manifest_info.compress_type = zipfile.ZIP_DEFLATED
        manifest_info.create_system = 3
        manifest_info.external_attr = (0o100644 << 16)
        archive.writestr(manifest_info, manifest_bytes)
    temp_path.replace(archive_path)
    return manifest


def verify_release_manifest(extracted_root: Path) -> dict[str, Any]:
    manifest_path = extracted_root / MANIFEST_NAME
    if not manifest_path.is_file():
        raise ReleaseError(f"В архиве отсутствует {MANIFEST_NAME}")
    try:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ReleaseError(f"Некорректный manifest: {exc}") from exc
    if manifest.get("schema") != MANIFEST_SCHEMA:
        raise ReleaseError("Неподдерживаемая schema manifest")
    if manifest.get("project") != PROJECT_NAME:
        raise ReleaseError("Manifest принадлежит другому проекту")
    if manifest.get("archive_root") != extracted_root.name:
        raise ReleaseError("archive_root в manifest не совпадает с ZIP")
    expected: dict[str, dict[str, Any]] = {}
    for item in manifest.get("files", []):
        if not isinstance(item, dict) or not isinstance(item.get("path"), str):
            raise ReleaseError("Некорректная запись files в manifest")
        relative = _safe_archive_member(item["path"])
        key = relative.as_posix()
        if key in expected:
            raise ReleaseError(f"Дубликат файла в manifest: {key}")
        expected[key] = item
    actual_paths: set[str] = set()
    for path in extracted_root.rglob("*"):
        if not path.is_file():
            continue
        relative = path.relative_to(extracted_root).as_posix()
        if relative == MANIFEST_NAME:
            continue
        actual_paths.add(relative)
        item = expected.get(relative)
        if item is None:
            raise ReleaseError(f"Файл отсутствует в manifest: {relative}")
        size = path.stat().st_size
        digest = sha256_file(path)
        if size != item.get("size") or digest != item.get("sha256"):
            raise ReleaseError(f"Manifest checksum mismatch: {relative}")
    missing = sorted(set(expected).difference(actual_paths))
    if missing:
        raise ReleaseError(f"Manifest ссылается на отсутствующие файлы: {missing[:10]}")
    version = read_project_version(extracted_root)
    if version != manifest.get("version"):
        raise ReleaseError("Версия исходников не совпадает с manifest")
    return manifest


def verify_archive(archive_path: Path) -> dict[str, Any]:
    with tempfile.TemporaryDirectory(prefix="likebot-verify-zip-") as temp_dir:
        root = safe_extract_zip(archive_path, Path(temp_dir))
        manifest = verify_release_manifest(root)
        files = collect_release_files(root)
        findings = scan_source_secrets(root, files)
        if findings:
            raise ReleaseError("Секреты в архиве:\n" + "\n".join(findings))
        check_version_consistency(root, str(manifest["version"]))
        return manifest


def _copy_tree_contents(source: Path, destination: Path, *, include_manifest: bool = True) -> None:
    destination.mkdir(parents=True, exist_ok=True)
    for path in sorted(source.rglob("*"), key=lambda item: item.relative_to(source).as_posix()):
        relative = path.relative_to(source)
        if any(part in EXCLUDED_DIR_NAMES for part in relative.parts):
            continue
        if not include_manifest and relative.as_posix() == MANIFEST_NAME:
            continue
        target = destination / relative
        if path.is_symlink():
            raise ReleaseError(f"Symlink при копировании дерева: {relative}")
        if path.is_dir():
            target.mkdir(parents=True, exist_ok=True)
        elif path.is_file():
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(path, target)


def _git_env() -> dict[str, str]:
    return {
        "LC_ALL": "C",
        "LANG": "C",
        "GIT_CONFIG_NOSYSTEM": "1",
        "GIT_AUTHOR_NAME": "LikeBot Release",
        "GIT_AUTHOR_EMAIL": "release@invalid.local",
        "GIT_COMMITTER_NAME": "LikeBot Release",
        "GIT_COMMITTER_EMAIL": "release@invalid.local",
        "GIT_AUTHOR_DATE": "2000-01-01T00:00:00Z",
        "GIT_COMMITTER_DATE": "2000-01-01T00:00:00Z",
    }


def _ensure_git() -> None:
    result = subprocess.run(
        ["git", "--version"],
        stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        shell=False,
        check=False,
    )
    if result.returncode != 0:
        raise ReleaseError("Для создания проверяемого patch требуется git")


def generate_git_patch(old_root: Path, new_root: Path, patch_path: Path) -> None:
    _ensure_git()
    with tempfile.TemporaryDirectory(prefix="likebot-patch-") as temp_dir:
        repo = Path(temp_dir) / "repo"
        repo.mkdir()
        _copy_tree_contents(old_root, repo)
        env = _git_env()
        run_process(["git", "init", "-q"], cwd=repo, env=env)
        run_process(["git", "config", "user.name", "LikeBot Release"], cwd=repo, env=env)
        run_process(["git", "config", "user.email", "release@invalid.local"], cwd=repo, env=env)
        run_process(["git", "add", "-A"], cwd=repo, env=env)
        run_process(["git", "commit", "-q", "-m", "baseline"], cwd=repo, env=env)
        for child in list(repo.iterdir()):
            if child.name == ".git":
                continue
            if child.is_dir():
                shutil.rmtree(child)
            else:
                child.unlink()
        # Stage deletions before copying the deterministic release tree. Old and
        # new ZIP members deliberately share a fixed mtime; a changed file with
        # the same byte length (for example 1.0.36 -> 1.0.37) can otherwise look
        # stat-identical to Git and be omitted from the patch as racily clean.
        run_process(["git", "add", "-A"], cwd=repo, env=env)
        _copy_tree_contents(new_root, repo)
        run_process(["git", "add", "-A"], cwd=repo, env=env)
        diff = run_process(
            ["git", "diff", "--cached", "--binary", "--full-index", "--no-renames", "HEAD", "--", "."],
            cwd=repo,
            env=env,
            check=False,
        )
        if diff.returncode not in {0, 1}:
            raise ReleaseError(f"git diff завершился кодом {diff.returncode}\n{diff.stdout}")
        if not diff.stdout.strip():
            raise ReleaseError("Patch пуст: между версиями нет различий")
        patch_path.parent.mkdir(parents=True, exist_ok=True)
        patch_path.write_text(diff.stdout, encoding="utf-8", newline="\n")


def compare_trees(left: Path, right: Path) -> list[str]:
    differences: list[str] = []
    left_files = {
        path.relative_to(left).as_posix(): path
        for path in left.rglob("*")
        if path.is_file() and ".git" not in path.relative_to(left).parts
    }
    right_files = {
        path.relative_to(right).as_posix(): path
        for path in right.rglob("*")
        if path.is_file() and ".git" not in path.relative_to(right).parts
    }
    for relative in sorted(set(left_files) | set(right_files)):
        if relative not in left_files:
            differences.append(f"только справа: {relative}")
        elif relative not in right_files:
            differences.append(f"только слева: {relative}")
        elif sha256_file(left_files[relative]) != sha256_file(right_files[relative]):
            differences.append(f"содержимое отличается: {relative}")
    return differences


def verify_git_patch(old_root: Path, new_root: Path, patch_path: Path) -> None:
    _ensure_git()
    with tempfile.TemporaryDirectory(prefix="likebot-patch-verify-") as temp_dir:
        repo = Path(temp_dir) / "repo"
        repo.mkdir()
        _copy_tree_contents(old_root, repo)
        env = _git_env()
        run_process(["git", "init", "-q"], cwd=repo, env=env)
        run_process(["git", "apply", "--check", str(patch_path)], cwd=repo, env=env)
        run_process(["git", "apply", "--whitespace=nowarn", str(patch_path)], cwd=repo, env=env)
        differences = compare_trees(repo, new_root)
        if differences:
            raise ReleaseError("Результат patch не совпадает с релизом:\n" + "\n".join(differences[:30]))


def stage_release_tree(project_root: Path, destination: Path, archive_root: str) -> dict[str, Any]:
    files = collect_release_files(project_root)
    for path in files:
        relative = path.relative_to(project_root)
        if relative.as_posix() == MANIFEST_NAME:
            continue
        target = destination / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(path, target)
    epoch = source_date_epoch()
    staged_files = collect_release_files(destination)
    manifest = build_manifest(destination, archive_root, staged_files, epoch)
    (destination / MANIFEST_NAME).write_text(
        json.dumps(manifest, ensure_ascii=False, sort_keys=True, indent=2) + "\n",
        encoding="utf-8",
        newline="\n",
    )
    return manifest


def write_zip_from_staged_tree(staged_root: Path, archive_path: Path, archive_root: str) -> None:
    epoch = source_date_epoch()
    timestamp = _zip_datetime(epoch)
    files = sorted((path for path in staged_root.rglob("*") if path.is_file()), key=lambda p: p.relative_to(staged_root).as_posix())
    archive_path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = archive_path.with_suffix(archive_path.suffix + ".tmp")
    with zipfile.ZipFile(temp_path, "w", compression=zipfile.ZIP_DEFLATED, compresslevel=9, strict_timestamps=True) as archive:
        for path in files:
            relative = path.relative_to(staged_root).as_posix()
            info = zipfile.ZipInfo(f"{archive_root}/{relative}", date_time=timestamp)
            info.compress_type = zipfile.ZIP_DEFLATED
            info.create_system = 3
            info.external_attr = (0o100644 << 16)
            archive.writestr(info, path.read_bytes())
    temp_path.replace(archive_path)


def write_checksum_file(output_path: Path, artifacts: Sequence[Path]) -> None:
    lines = [f"{sha256_file(path)}  {path.name}" for path in artifacts]
    output_path.write_text("\n".join(lines) + "\n", encoding="utf-8", newline="\n")


def _run_check(
    checks: list[CheckResult],
    name: str,
    function,
    *,
    skip: bool = False,
    skip_details: str = "",
    required: bool = True,
) -> Any:
    if skip:
        checks.append(
            CheckResult(
                name=name,
                status="skipped",
                details=skip_details,
                required=required,
            )
        )
        return None
    started = time.perf_counter()
    try:
        value = function()
        if isinstance(value, str):
            details = value
        elif value is None:
            details = ""
        elif isinstance(value, (list, tuple, set, dict)):
            details = f"items: {len(value)}"
        else:
            details = str(value)
    except Exception as exc:
        checks.append(
            CheckResult(
                name=name,
                status="failed",
                details=str(exc),
                duration_seconds=round(time.perf_counter() - started, 3),
                required=required,
            )
        )
        raise
    checks.append(
        CheckResult(
            name=name,
            status="passed",
            details=details,
            duration_seconds=round(time.perf_counter() - started, 3),
            required=required,
        )
    )
    return value


def verify_project(
    project_root: Path,
    *,
    run_tests: bool = True,
    pytest_args: Sequence[str] = (),
    postgres_url: str | None = None,
    require_postgres: bool = False,
) -> VerificationReport:
    root = project_root.resolve()
    started_at = utc_now_text()
    version = read_project_version(root)
    checks: list[CheckResult] = []

    def attempt(name: str, function, **kwargs):
        try:
            return _run_check(checks, name, function, **kwargs)
        except Exception:
            return None

    files = attempt("source-tree", lambda: collect_release_files(root))
    if files is None:
        _run_check(
            checks,
            "secret-scan",
            lambda: None,
            skip=True,
            skip_details="Пропущено: source-tree не прошёл",
            required=False,
        )
    else:
        def secret_check() -> str:
            findings = scan_source_secrets(root, files)
            if findings:
                raise ReleaseError("\n".join(findings))
            return f"Проверено файлов: {len(files)}"

        attempt("secret-scan", secret_check)

    attempt("version-consistency", lambda: check_version_consistency(root, version))
    attempt("dependency-locks", lambda: check_dependency_locks(root))
    attempt(
        "python-static",
        lambda: (
            lambda counts: f"Python-файлов: {counts[0]}, test-модулей: {counts[1]}"
        )(static_python_checks(root)),
    )
    attempt("ruff", lambda: run_ruff(root))
    attempt("postgresql-ddl-compile", lambda: compile_postgresql_schema(root))
    attempt("sqlite-smoke", lambda: run_sqlite_smoke(root))
    attempt(
        "pytest",
        lambda: run_pytest(root, pytest_args),
        skip=not run_tests,
        skip_details="Тесты отключены явным параметром; такой отчёт не является release-ready",
    )
    if postgres_url:
        attempt("postgresql-live-smoke", lambda: run_postgres_smoke(root, postgres_url))
    elif require_postgres:
        attempt(
            "postgresql-live-smoke",
            lambda: (_ for _ in ()).throw(
                ReleaseError("Требуется --postgres-url или RELEASE_POSTGRES_URL")
            ),
        )
    else:
        _run_check(
            checks,
            "postgresql-live-smoke",
            lambda: None,
            skip=True,
            skip_details=(
                "Live PostgreSQL не запрошен; статус отдельной DDL compile "
                "проверки указан выше"
            ),
            required=False,
        )

    finished_at = utc_now_text()
    import platform

    return VerificationReport(
        project=PROJECT_NAME,
        version=version,
        started_at_utc=started_at,
        finished_at_utc=finished_at,
        python=sys.version.split()[0],
        platform=platform.platform(),
        checks=checks,
    )


def write_report(report: VerificationReport, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(report.to_dict(), ensure_ascii=False, sort_keys=True, indent=2) + "\n", encoding="utf-8")


def artifact_names(version: str, previous_version: str, tag: str) -> dict[str, str]:
    if not re.fullmatch(r"[a-z0-9_]+", tag):
        raise ReleaseError("artifact tag допускает только a-z, 0-9 и _")
    current = version_to_token(version)
    previous = version_to_token(previous_version)
    stem = f"laika_bot_v{current}_{tag}"
    return {
        "archive_root": stem,
        "zip": f"{stem}_ready.zip",
        "patch": f"laika_bot_v{previous}_to_v{current}_{tag}.patch",
        "audit": f"{stem}_AUDIT.txt",
        "report": f"{stem}_VERIFY.json",
        "checksums": f"{stem}_ready.sha256.txt",
    }


def build_release(
    project_root: Path,
    previous_archive: Path,
    output_dir: Path,
    *,
    tag: str,
    postgres_url: str | None = None,
    require_postgres: bool = False,
) -> dict[str, Path | str]:
    root = project_root.resolve()
    previous_archive = previous_archive.resolve()
    output_dir = output_dir.resolve()
    if not previous_archive.is_file():
        raise ReleaseError(f"Предыдущий архив не найден: {previous_archive}")
    pre_report = verify_project(
        root,
        run_tests=True,
        pytest_args=(),
        postgres_url=postgres_url,
        require_postgres=require_postgres,
    )
    if not pre_report.passed:
        failed = [item for item in pre_report.checks if item.status == "failed"]
        raise ReleaseError("Pre-build verification failed:\n" + "\n".join(f"{i.name}: {i.details}" for i in failed))

    current_version = read_project_version(root)
    with tempfile.TemporaryDirectory(prefix="likebot-release-") as temp_dir:
        temp = Path(temp_dir)
        previous_root = safe_extract_zip(previous_archive, temp / "previous")
        previous_version = read_project_version(previous_root)
        previous_files = collect_release_files(previous_root)
        previous_findings = scan_source_secrets(previous_root, previous_files)
        if previous_findings:
            raise ReleaseError("Предыдущий архив содержит секреты:\n" + "\n".join(previous_findings))
        check_version_consistency(previous_root, previous_version)
        if (previous_root / MANIFEST_NAME).is_file():
            verify_release_manifest(previous_root)
        if previous_version == current_version:
            raise ReleaseError("Предыдущий архив имеет ту же версию, что и новый релиз")
        names = artifact_names(current_version, previous_version, tag)
        staged_root = temp / names["archive_root"]
        stage_release_tree(root, staged_root, names["archive_root"])
        verify_release_manifest(staged_root)

        output_dir.mkdir(parents=True, exist_ok=True)
        zip_path = output_dir / names["zip"]
        patch_path = output_dir / names["patch"]
        audit_path = output_dir / names["audit"]
        report_path = output_dir / names["report"]
        checksum_path = output_dir / names["checksums"]

        write_zip_from_staged_tree(staged_root, zip_path, names["archive_root"])
        manifest = verify_archive(zip_path)
        shutil.copy2(root / "AUDIT.txt", audit_path)

        # ZIP deliberately normalizes file permissions to regular 0644 files.
        # Generate the patch from the extracted final archive, not directly from
        # the source staging tree, so applying the patch reproduces the exact
        # release payload (including normalized modes) and can be regenerated
        # byte-for-byte from the two published archives.
        with tempfile.TemporaryDirectory(prefix="likebot-final-zip-") as final_temp:
            final_root = safe_extract_zip(zip_path, Path(final_temp))
            verify_release_manifest(final_root)
            generate_git_patch(previous_root, final_root, patch_path)
            verify_git_patch(previous_root, final_root, patch_path)
            final_report = verify_project(
                final_root,
                run_tests=True,
                pytest_args=(),
                postgres_url=postgres_url,
                require_postgres=require_postgres,
            )
        if not final_report.passed:
            failed = [item for item in final_report.checks if item.status == "failed"]
            raise ReleaseError("Final ZIP verification failed:\n" + "\n".join(f"{i.name}: {i.details}" for i in failed))
        write_report(final_report, report_path)
        write_checksum_file(checksum_path, [zip_path, patch_path, audit_path, report_path])

    return {
        "version": current_version,
        "previous_version": previous_version,
        "zip": zip_path,
        "patch": patch_path,
        "audit": audit_path,
        "report": report_path,
        "checksums": checksum_path,
        "zip_sha256": sha256_file(zip_path),
        "manifest_files": len(manifest["files"]),
    }
