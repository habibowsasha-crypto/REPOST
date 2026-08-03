"""Unified visual style for admin menus, buttons and short notices."""

from __future__ import annotations

from typing import Iterable, Optional, Sequence, Union

from telethon import Button

# ── layout ────────────────────────────────────────────────────────────────
DIV = "──────────────"
DIV_SOFT = "· · · · · · · ·"

# ── status dots ───────────────────────────────────────────────────────────
ON = "🟢"
OFF = "🔴"
WAIT = "🟡"
OK = "✅"
WARN = "⚠️"
ERR = "❌"
INFO = "ℹ️"
DOT = "•"


def join(*parts: str) -> str:
    """Join non-empty lines with newlines."""
    lines: list[str] = []
    for p in parts:
        if p is None:
            continue
        s = str(p)
        if s == "":
            lines.append("")
        else:
            lines.append(s)
    return "\n".join(lines)


def title(emoji: str, text: str) -> str:
    return f"{emoji} **{text}**"


def header(emoji: str, text: str) -> str:
    """Screen header: emoji title + divider."""
    return join(title(emoji, text), DIV)


def footer(hint: str = "👇 разделы ниже") -> str:
    return join(DIV, hint)


def kv(label: str, value: str, *, icon: str = "") -> str:
    prefix = f"{icon} " if icon else ""
    return f"{prefix}{label}: **{value}**"


def muted(text: str) -> str:
    return f"`{text}`"


def tree(items: Sequence[tuple[str, str, Union[str, int]]]) -> str:
    """
    items: (icon, label, value)
    Renders:
      ├ ⏳ ждут DM    **3**
      └ ✅ написали   **1**
    """
    if not items:
        return ""
    lines = []
    last = len(items) - 1
    for i, (icon, label, value) in enumerate(items):
        branch = "└" if i == last else "├"
        lines.append(f"{branch} {icon} {label}  **{value}**")
    return "\n".join(lines)


def bullets(items: Sequence[str], *, mark: str = DOT) -> str:
    return "\n".join(f"{mark} {x}" for x in items if x)


def section(name: str, body: str) -> str:
    if not body:
        return f"**{name}**"
    return join(f"**{name}**", body)


def notice(kind: str, text: str) -> str:
    """kind: ok | warn | err | info"""
    icon = {
        "ok": OK,
        "warn": WARN,
        "err": ERR,
        "info": INFO,
    }.get(kind, INFO)
    return f"{icon} {text}"


def screen(emoji: str, name: str, *blocks: str, hint: Optional[str] = None) -> str:
    """Full screen: header + body blocks + optional footer hint."""
    parts: list[str] = [header(emoji, name)]
    for b in blocks:
        if b is None or b == "":
            continue
        parts.append(b)
    if hint is not None:
        parts.append(footer(hint))
    return join(*parts)


# ── buttons ───────────────────────────────────────────────────────────────
def btn(label: str, data: Union[str, bytes]) -> Button:
    if isinstance(data, str):
        data = data.encode()
    return Button.inline(label, data)


def row(*buttons: Button) -> list[Button]:
    return list(buttons)


def rows(*button_rows: Sequence[Button]) -> list[list[Button]]:
    return [list(r) for r in button_rows]


def back_home_row() -> list[Button]:
    return [btn("◀️ Главное меню", b"menu_home")]


def back_row(data: Union[str, bytes], label: str = "◀️ Назад") -> list[Button]:
    return [btn(label, data)]


def nav_pair(
    back_data: Union[str, bytes],
    *,
    back_label: str = "◀️ Назад",
    home: bool = True,
) -> list[list[Button]]:
    out = [back_row(back_data, back_label)]
    if home:
        out.append(back_home_row())
    return out


# ── short callback answers (toasts) ───────────────────────────────────────
DENIED = "Нет доступа"
SAVED = "Сохранено"
UPDATED = "Обновлено"
DONE = "Готово"
DELETED = "Удалено"
ENABLED = "Включено"
DISABLED = "Выключено"
