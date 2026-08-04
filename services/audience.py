"""Crypto audience base: detailed import/export and optional re-queue."""

from __future__ import annotations

import csv
import datetime as dt
import io
from typing import Any

from db.schema import db_lock, get_connection
from services import opt_out as opt_out_svc
from services import queue as queue_svc

EXPORT_FIELDS = [
    "user_id",
    "username",
    "access_hash",
    "first_name",
    "last_name",
    "source",
    "source_chat_id",
    "source_account_user_id",
    "first_seen_at",
    "first_dm_at",
    "last_touch_at",
    "notes",
    "contact_status",
    "lead_status",
]


def _now_iso() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat()


def _clean_text(value: Any) -> str | None:
    text = str(value or "").strip()
    return text or None


def _clean_username(value: Any) -> str | None:
    text = _clean_text(value)
    return text.lstrip("@") if text else None


def _clean_int(value: Any) -> int | None:
    if value is None or str(value).strip() == "":
        return None
    try:
        return int(str(value).strip())
    except (TypeError, ValueError):
        return None


def count() -> int:
    conn = get_connection()
    row = conn.execute("SELECT COUNT(*) AS c FROM audience").fetchone()
    return int(row["c"] if row else 0)


def upsert(
    user_id: int,
    *,
    username: str | None = None,
    first_name: str | None = None,
    last_name: str | None = None,
    access_hash: int | None = None,
    source: str = "dm",
    source_chat_id: int | None = None,
    source_account_user_id: int | None = None,
    touched_dm: bool = False,
    first_dm_at: str | None = None,
    notes: str | None = None,
) -> None:
    """Insert or refresh an audience row while preserving known metadata."""
    now = _now_iso()
    user_id = int(user_id)
    un = _clean_username(username)
    fn = _clean_text(first_name)
    ln = _clean_text(last_name)
    src = _clean_text(source) or "dm"
    dm_at = _clean_text(first_dm_at) or (now if touched_dm else None)
    conn = get_connection()
    with db_lock(), conn:
        row = conn.execute(
            "SELECT user_id, first_dm_at FROM audience WHERE user_id=?",
            (user_id,),
        ).fetchone()
        if row is None:
            conn.execute(
                """
                INSERT INTO audience (
                    user_id, username, first_name, last_name, access_hash, source,
                    source_chat_id, source_account_user_id,
                    first_seen_at, first_dm_at, last_touch_at, notes
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    user_id,
                    un,
                    fn,
                    ln,
                    access_hash,
                    src,
                    source_chat_id,
                    source_account_user_id,
                    now,
                    dm_at,
                    now,
                    _clean_text(notes),
                ),
            )
            return
        conn.execute(
            """
            UPDATE audience
               SET username=COALESCE(?, username),
                   first_name=COALESCE(?, first_name),
                   last_name=COALESCE(?, last_name),
                   access_hash=COALESCE(?, access_hash),
                   source=COALESCE(?, source),
                   source_chat_id=COALESCE(?, source_chat_id),
                   source_account_user_id=COALESCE(?, source_account_user_id),
                   notes=COALESCE(?, notes),
                   last_touch_at=?,
                   first_dm_at=COALESCE(first_dm_at, ?)
             WHERE user_id=?
            """,
            (
                un,
                fn,
                ln,
                access_hash,
                src,
                source_chat_id,
                source_account_user_id,
                _clean_text(notes),
                now,
                dm_at,
                user_id,
            ),
        )


def record_first_dm(
    user_id: int,
    *,
    username: str | None = None,
    first_name: str | None = None,
    last_name: str | None = None,
    access_hash: int | None = None,
    source_chat_id: int | None = None,
    source_account_user_id: int | None = None,
) -> None:
    upsert(
        user_id,
        username=username,
        first_name=first_name,
        last_name=last_name,
        access_hash=access_hash,
        source="dm",
        source_chat_id=source_chat_id,
        source_account_user_id=source_account_user_id,
        touched_dm=True,
    )


def list_recent(limit: int = 20) -> list[dict[str, Any]]:
    conn = get_connection()
    rows = conn.execute(
        """
        SELECT user_id, username, first_name, last_name, access_hash, source,
               source_account_user_id, first_dm_at, last_touch_at
          FROM audience
         ORDER BY last_touch_at DESC
         LIMIT ?
        """,
        (int(limit),),
    ).fetchall()
    return [dict(r) for r in rows]


def export_rows(*, only_with_dm: bool = True) -> list[dict[str, Any]]:
    conn = get_connection()
    where = "WHERE a.first_dm_at IS NOT NULL" if only_with_dm else ""
    rows = conn.execute(
        f"""
        SELECT a.user_id,
               a.username,
               COALESCE(a.access_hash, l.access_hash) AS access_hash,
               a.first_name,
               a.last_name,
               a.source,
               COALESCE(a.source_chat_id, l.source_chat_id) AS source_chat_id,
               COALESCE(a.source_account_user_id, l.source_account_user_id)
                   AS source_account_user_id,
               a.first_seen_at,
               a.first_dm_at,
               a.last_touch_at,
               a.notes,
               c.status AS contact_status,
               l.status AS lead_status
          FROM audience a
          LEFT JOIN leads l ON l.target_user_id=a.user_id
          LEFT JOIN contacts c ON c.target_user_id=a.user_id
          {where}
         ORDER BY COALESCE(a.first_dm_at, a.last_touch_at) ASC
        """
    ).fetchall()
    return [dict(r) for r in rows]


def export_csv_text(*, only_with_dm: bool = True) -> str:
    buffer = io.StringIO(newline="")
    writer = csv.DictWriter(buffer, fieldnames=EXPORT_FIELDS, extrasaction="ignore")
    writer.writeheader()
    for row in export_rows(only_with_dm=only_with_dm):
        writer.writerow({field: row.get(field) if row.get(field) is not None else "" for field in EXPORT_FIELDS})
    return buffer.getvalue()


def export_csv_bytes(*, only_with_dm: bool = True) -> bytes:
    return export_csv_text(only_with_dm=only_with_dm).encode("utf-8-sig")


def export_lines(*, only_with_dm: bool = True) -> list[str]:
    """Backward-compatible textual export used by old tests/UI."""
    return export_csv_text(only_with_dm=only_with_dm).splitlines()


def _normalize_record(raw: dict[str, Any]) -> dict[str, Any] | None:
    aliases = {str(k or "").strip().lower(): v for k, v in raw.items()}
    uid = _clean_int(aliases.get("user_id", aliases.get("id")))
    if uid is None or uid <= 0:
        return None
    return {
        "user_id": uid,
        "username": _clean_username(aliases.get("username")),
        "access_hash": _clean_int(aliases.get("access_hash")),
        "first_name": _clean_text(aliases.get("first_name")),
        "last_name": _clean_text(aliases.get("last_name")),
        "source": _clean_text(aliases.get("source")) or "import",
        "source_chat_id": _clean_int(aliases.get("source_chat_id")),
        "source_account_user_id": _clean_int(aliases.get("source_account_user_id")),
        "first_dm_at": _clean_text(aliases.get("first_dm_at")),
        "notes": _clean_text(aliases.get("notes")),
    }


def parse_import_text(text: str) -> list[dict[str, Any]]:
    """Parse detailed CSV/TSV/semicolon files and legacy plain numeric IDs."""
    raw = (text or "").lstrip("\ufeff").strip()
    if not raw:
        return []

    lines = [line for line in raw.splitlines() if line.strip()]
    first = lines[0].strip().lower() if lines else ""
    has_header = "user_id" in first or first.startswith("id,") or first.startswith("id;")
    records: list[dict[str, Any]] = []
    seen: set[int] = set()

    if has_header:
        sample = "\n".join(lines[:20])
        try:
            dialect = csv.Sniffer().sniff(sample, delimiters=",;\t")
        except csv.Error:
            dialect = csv.excel
        reader = csv.DictReader(io.StringIO("\n".join(lines)), dialect=dialect)
        for row in reader:
            item = _normalize_record(dict(row))
            if item and item["user_id"] not in seen:
                seen.add(item["user_id"])
                records.append(item)
        # Backward compatibility: a pasted list may start with a CSV header and
        # continue with whitespace-separated IDs on later lines.
        for line in lines[1:]:
            if any(delim in line for delim in (",", ";", "\t")):
                continue
            for token in line.split():
                uid = _clean_int(token)
                if uid and uid > 0 and uid not in seen:
                    seen.add(uid)
                    records.append({
                        "user_id": uid,
                        "username": None,
                        "access_hash": None,
                        "first_name": None,
                        "last_name": None,
                        "source": "import",
                        "source_chat_id": None,
                        "source_account_user_id": None,
                        "first_dm_at": None,
                        "notes": None,
                    })
        return records

    # Legacy mode: one or many numeric IDs in free text.
    for line in lines:
        for token in line.replace(",", " ").replace(";", " ").split():
            uid = _clean_int(token)
            if uid and uid > 0 and uid not in seen:
                seen.add(uid)
                records.append({
                    "user_id": uid,
                    "username": None,
                    "access_hash": None,
                    "first_name": None,
                    "last_name": None,
                    "source": "import",
                    "source_chat_id": None,
                    "source_account_user_id": None,
                    "first_dm_at": None,
                    "notes": None,
                })
    return records


def import_records(records: list[dict[str, Any]], *, source: str = "import") -> dict[str, int]:
    """Store detailed records and re-queue them according to the existing product rule."""
    stats = {
        "recognized": len(records),
        "added_or_touch": 0,
        "queued": 0,
        "skipped_opt_out": 0,
        "skipped_invalid": 0,
        "skipped_queue": 0,
        "with_username": 0,
        "with_access_hash": 0,
        "with_source_account": 0,
    }
    for raw in records:
        item = _normalize_record(raw)
        if not item:
            stats["skipped_invalid"] += 1
            continue
        uid = int(item["user_id"])
        if opt_out_svc.is_opted_out(uid):
            stats["skipped_opt_out"] += 1
            continue
        if item.get("username"):
            stats["with_username"] += 1
        if item.get("access_hash") is not None:
            stats["with_access_hash"] += 1
        if item.get("source_account_user_id") is not None:
            stats["with_source_account"] += 1

        upsert(
            uid,
            username=item.get("username"),
            first_name=item.get("first_name"),
            last_name=item.get("last_name"),
            access_hash=item.get("access_hash"),
            source=item.get("source") or source,
            source_chat_id=item.get("source_chat_id"),
            source_account_user_id=item.get("source_account_user_id"),
            first_dm_at=item.get("first_dm_at"),
            notes=item.get("notes"),
            touched_dm=False,
        )
        stats["added_or_touch"] += 1
        ok = queue_svc.force_requeue(
            target_user_id=uid,
            username=item.get("username"),
            first_name=item.get("first_name"),
            last_name=item.get("last_name"),
            access_hash=item.get("access_hash"),
            source_chat_id=item.get("source_chat_id"),
            source_account_user_id=item.get("source_account_user_id"),
        )
        if ok:
            stats["queued"] += 1
        else:
            stats["skipped_queue"] += 1
    return stats


def import_user_ids(ids: list[int], *, source: str = "import") -> dict[str, int]:
    records = [{"user_id": raw, "source": source} for raw in ids]
    return import_records(records, source=source)


def parse_ids_from_text(text: str) -> list[int]:
    return [int(item["user_id"]) for item in parse_import_text(text)]


def format_line(row: dict[str, Any]) -> str:
    uid = int(row["user_id"])
    un = (row.get("username") or "").strip().lstrip("@")
    label = f"@{un}" if un else f"`{uid}`"
    dm = (row.get("first_dm_at") or "")[:16].replace("T", " ")
    if dm:
        return f"• {label} · `{uid}` · DM {dm}"
    return f"• {label} · `{uid}`"
