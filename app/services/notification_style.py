"""Shared Telegram notification style for ANTILUD BingX.

All automatic and settings notifications use compact cards with a stable emoji
vocabulary.  Dynamic values are HTML-escaped here because the bot globally uses
Telegram HTML parse mode.
"""

from __future__ import annotations

import html
import re
from datetime import datetime, timedelta, timezone
from typing import Iterable, Sequence

from app.exchanges.bingx.symbols import bingx_tradfi_exchange_symbol

DIVIDER = "━━━━━━━━━━━━━━━━━━"
EXCHANGE_LABEL = "BingX Futures"
MSK = timezone(timedelta(hours=3))


def esc(value: object, *, limit: int | None = None) -> str:
    text = "" if value is None else str(value)
    if limit is not None and len(text) > limit:
        text = text[: max(0, limit - 1)] + "…"
    return html.escape(text, quote=False)


def fmt_number(value: object, *, decimals: int = 8) -> str:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return esc(value)
    if number == 0:
        return "0"
    text = f"{number:,.{max(0, decimals)}f}".rstrip("0").rstrip(".")
    return text.replace(",", " ")


def fmt_price(value: object) -> str:
    return fmt_number(value, decimals=10)


def fmt_qty(value: object) -> str:
    return fmt_number(value, decimals=10)


def fmt_percent(value: object, *, signed: bool = False) -> str:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return f"{esc(value)}%"
    sign = "+" if signed and number > 0 else ("−" if signed and number < 0 else "")
    return f"{sign}{fmt_number(abs(number), decimals=4)}%"


def fmt_usdt(value: object, *, signed: bool = False) -> str:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return f"{esc(value)} USDT"
    if abs(number) < 0.005:
        return "≈0.00 USDT"
    sign = "+" if signed and number > 0 else ("−" if signed and number < 0 else "")
    return f"{sign}{abs(number):,.2f} USDT".replace(",", " ")


def side_emoji(side: str) -> str:
    return "📈" if str(side).lower() == "long" else "📉"


def trade_identity(symbol: str, side: str) -> str:
    display_symbol = bingx_tradfi_exchange_symbol(symbol) or str(symbol).upper()
    return f"🪙 <b>{esc(display_symbol)}</b>  ·  {side_emoji(side)} <b>{esc(str(side).upper())}</b>"


def footer(*, when: datetime | None = None) -> str:
    moment = (when or datetime.now(MSK)).astimezone(MSK)
    return f"🕒 {moment:%H:%M} МСК"

    # v1.6.18: card() bakes footer() -- which includes a minute-precision
    # timestamp -- into every notification. Hashing the full formatted text for
    # durable-notification dedup (see durable_notifications.notification_key)
    # therefore produced a different key every time the SAME unresolved condition
    # was rebuilt into a fresh card() on a later monitor pass, defeating the
    # "repeated monitor cycles are idempotent" guarantee the dedup was meant to
    # provide. strip_footer() removes only that volatile trailing block so the
    # dedup key reflects content, not wall-clock time; the displayed/stored text
    # itself (with the visible MSK timestamp) is never changed by this.


_FOOTER_PATTERN = re.compile(r"\n\n(?:🏦|🕒)[^\n]*$")


def strip_footer(text: str) -> str:
    """Return card text with the volatile timestamp footer removed.

    Used only for hashing/dedup, never for display. A no-op on text that has
    no matching footer (e.g. legacy plain-text monitor messages).
    """
    return _FOOTER_PATTERN.sub("", text).strip()


def card(
    title: str,
    *,
    symbol: str | None = None,
    side: str | None = None,
    blocks: Sequence[Sequence[str] | str] = (),
    include_footer: bool = True,
    exchange_label: str = EXCHANGE_LABEL,
) -> str:
    """Render a Premium Clean Telegram card.

    The renderer is deliberately centralised: user and admin Telegram messages
    stay visually identical, while raw technical diagnostics remain in logs and
    metadata. Existing callers keep passing simple blocks; this function adds the
    consistent spacing, exchange row and footer.
    """
    lines: list[str] = [title, DIVIDER]
    if symbol and side:
        lines.extend(["", trade_identity(symbol, side)])
        if exchange_label:
            lines.append(f"🏦 {esc(exchange_label)}")
    for block in blocks:
        if isinstance(block, str):
            block_lines = [block]
        else:
            block_lines = [str(item) for item in block if str(item) != ""]
        if not block_lines:
            continue
        lines.append("")
        lines.extend(block_lines)
    if include_footer:
        lines.extend(["", footer()])
    return "\n".join(lines)


def premium_kv_block(items: Iterable[tuple[str, object]]) -> list[str]:
    """Return the visual ┌/├/└ metric block used by Premium Clean cards."""
    pairs = [(str(label), value) for label, value in items if str(label).strip()]
    result: list[str] = []
    for index, (label, value) in enumerate(pairs):
        marker = "└" if index == len(pairs) - 1 else ("┌" if index == 0 else "├")
        result.append(f"{marker} {label}")
        text = esc(value) if not isinstance(value, str) else value
        result.append(f"│ {text}")
    return result


def premium_arrow_lines(items: Iterable[tuple[str, object]]) -> list[str]:
    """Return compact menu rows in the Premium Arrow dashboard style.

    Unlike ``premium_kv_block`` this is intended for Telegram menus/status
    screens where vertical ``│`` continuations become visually heavy. Values
    are kept on the same line after ``→`` for easier reading on mobile.
    """
    result: list[str] = []
    for label, value in items:
        label_text = str(label).strip()
        if not label_text:
            continue
        value_text = esc(value) if not isinstance(value, str) else value
        if "<" in value_text and ">" in value_text:
            result.append(f"{label_text} → {value_text}")
        else:
            result.append(f"{label_text} → <b>{value_text}</b>")
    return result


def premium_section(title: str, *lines: object) -> list[str]:
    """Return a titled Premium Clean section with optional body lines."""
    body = [str(line) for line in lines if str(line) != ""]
    return [title, *body]


def tree_lines(items: Iterable[str], *, last_marker: bool = True) -> list[str]:
    values = [str(item) for item in items]
    result: list[str] = []
    for index, item in enumerate(values):
        marker = "└" if last_marker and index == len(values) - 1 else "├"
        result.append(f"{marker} {item}")
    return result


def details_line(value: object, *, limit: int = 450) -> str:
    return f"🧾 <b>Детали:</b> <code>{esc(value, limit=limit)}</code>"


def system_message(
    title: str, lines: Sequence[str], *, include_footer: bool = True
) -> str:
    return card(title, blocks=(list(lines),), include_footer=include_footer)


def parse_created_at(value: object) -> datetime | None:
    if isinstance(value, datetime):
        dt = value
    elif value:
        text = str(value).strip().replace("Z", "+00:00")
        try:
            dt = datetime.fromisoformat(text)
        except ValueError:
            return None
    else:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def duration_text(created_at: object, *, now: datetime | None = None) -> str:
    started = parse_created_at(created_at)
    if not started:
        return ""
    current = now or datetime.now(timezone.utc)
    seconds = max(
        0,
        int(
            (
                current.astimezone(timezone.utc) - started.astimezone(timezone.utc)
            ).total_seconds()
        ),
    )
    hours, remainder = divmod(seconds, 3600)
    minutes, secs = divmod(remainder, 60)
    if hours:
        return f"{hours} ч {minutes} мин"
    if minutes:
        return f"{minutes} мин {secs} сек"
    return f"{secs} сек"


def ensure_visual_card(
    text: str, *, fallback_title: str = "🔔 <b>УВЕДОМЛЕНИЕ ANTILUD</b>"
) -> str:
    """Convert older plain-text monitor messages to the common visual card.

    New formatters already contain ``DIVIDER`` and pass through unchanged. This
    compatibility layer keeps every rare recovery/error branch visually aligned
    without hiding its original diagnostic details.
    """
    raw = str(text or "").strip()
    if not raw or DIVIDER in raw:
        return raw
    import re

    source_lines = [line.strip() for line in raw.splitlines() if line.strip()]
    if not source_lines:
        return fallback_title

    first = source_lines.pop(0)
    # Keep a leading status emoji, but escape all text from exchange exceptions.
    m_title = re.match(r"^([^\w\s]{1,3})\s*(.*)$", first)
    if m_title:
        emoji, title_text = m_title.group(1), m_title.group(2)
        title = f"{emoji} <b>{esc(title_text.upper(), limit=120)}</b>"
    else:
        title = fallback_title
        source_lines.insert(0, first)

    symbol = None
    side = None
    cleaned: list[str] = []
    trade_re = re.compile(
        r"^(?:Сделка:\s*)?([A-Z0-9_\-]{3,30}USDT)\s+(LONG|SHORT)$", re.I
    )
    symbol_re = re.compile(r"\b([A-Z0-9_\-]{3,30}USDT)\b", re.I)
    for line in source_lines:
        match = trade_re.match(line)
        if match and not symbol:
            symbol, side = match.group(1).upper(), match.group(2).lower()
            continue
        if not symbol:
            sym = symbol_re.search(line)
            if sym:
                symbol = sym.group(1).upper()
        safe = esc(line, limit=700)
        low = line.lower()
        if line[:1] in {
            "✅",
            "❌",
            "⚠",
            "🚨",
            "⏳",
            "ℹ",
            "🎯",
            "⚡",
            "🛑",
            "🔄",
            "🧹",
            "📦",
            "🛡",
            "💵",
            "🔴",
            "🔵",
            "🟡",
        }:
            cleaned.append(safe)
        elif low.startswith(("ошибка", "причина")):
            cleaned.append(f"❌ {safe}")
        elif low.startswith("попытка"):
            cleaned.append(f"🔄 {safe}")
        elif "проверь" in low or "ручн" in low:
            cleaned.append(f"📱 {safe}")
        elif "позици" in low or "объём" in low:
            cleaned.append(f"📦 {safe}")
        elif "stop" in low:
            cleaned.append(f"🛡 {safe}")
        elif "tp" in low:
            cleaned.append(f"🎯 {safe}")
        else:
            cleaned.append(f"• {safe}")

    blocks = (cleaned or ["ℹ️ Событие зафиксировано"],)
    if symbol and side:
        return card(title, symbol=symbol, side=side, blocks=blocks)
    return card(title, blocks=blocks)
