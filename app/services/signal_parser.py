from __future__ import annotations

import math
import hashlib
import json
import logging
import re
from decimal import Decimal, InvalidOperation
from dataclasses import dataclass
from typing import Optional

from app.services.models import Side, Signal
from app.exchanges.bingx.symbols import GOLD_XAU_INTERNAL_SYMBOL


log = logging.getLogger(__name__)


@dataclass
class TradePlan:
    symbol: str
    side: str | None
    order_type: str
    entry: float | None
    stop: float
    tps: list[float]
    tp_percents: list[float] | None = None


@dataclass
class VipParseResult:
    ok: bool
    plan: TradePlan | None = None
    ignored_lines: list[str] | None = None
    error: str | None = None
    multiple: list[str] | None = None
    signal_hash: str | None = None
    format_name: str | None = None
    signal_id: str | None = None
    entry_zone_low: float | None = None
    entry_zone_high: float | None = None
    selected_entry: float | None = None
    confidence: float | None = None
    errors: list[str] | None = None


_NUMBER_RE = re.compile(r"\d+(?:[\.,]\s*\d+)?")
# ANTILUD v3.2.72 used {2,20}; this build keeps {1,20} so real pairs like S/USDT are not lost.
# Lookahead allows pairs like "HYPER/USDTUSDT" (formatter doubled USDT), "BTC/USDT,",
# "BTC/USDT)" — we no longer require a non-alnum delimiter after USDT.
_PAIR_SLASH_RE = re.compile(r"(?i)(?:^|[^A-Z0-9])([A-Z0-9]{1,20})\s*/\s*USDT")
# Plain pattern: BTCUSDT (no slash). We exclude USDT itself as base by requiring
# at least one non-USDT char before USDT (so "USDTUSDT" does NOT match).
_PAIR_PLAIN_RE = re.compile(r"(?i)\b((?!USDT)[A-Z0-9]{1,20}USDT)\b")
# Exact TradFi gold aliases.  They canonicalize to an alphanumeric internal
# symbol; the BingX adapter restores GOLD(XAU)-USDT only at the HTTP boundary.
_GOLD_XAU_PAIR_RE = re.compile(
    r"(?i)(?<![A-Z0-9])(?:XAU\s*[/_-]?\s*USD|GOLD\s*\(\s*XAU\s*\)\s*[-/_]?\s*USDT)(?![A-Z0-9])"
)
_LONG_RE = re.compile(r"(?i)(?:\(\s*long\s*\)|\blong\b|\bлонг\b)")
_SHORT_RE = re.compile(r"(?i)(?:\(\s*short\s*\)|\bshort\b|\bшорт\b)")
_GOLD_BUY_RE = re.compile(r"(?i)\bbuy\b")
_GOLD_SELL_RE = re.compile(r"(?i)\bsell\b")
_ENTRY_KEY_RE = re.compile(r"(?i)(?:\bentry\b|\bтвх\b|\btvx\b|\btbx\b|в\s*х\s*о\s*д)")
_STOP_KEY_RE = re.compile(
    r"(?i)(?:\bstop\b|\bstop\s*loss\b|\bsl\b|\bс/л\b|\bсл\b|с\s*т\s*о\s*п)"
)
_TP_BLOCK_RE = re.compile(
    r"(?i)(?:\btargets?\b|\btake\s*profit\b|\btp\b|\bтп\b|т\s*п|ц\s*е\s*л\s*и|цель)"
)
_MOON_RE = re.compile(r"(?i)(?:to\s+the\s+moon|moon|луна|🌖|🚀)")
_LEVERAGE_RE = re.compile(
    r"(?i)^\s*(?:USDT|Cross(?:\s*\(?\s*\d+X\s*\)?)?|Isolated(?:\s*\(?\s*\d+X\s*\)?)?|(?:leverage|плеч[оа])\s*[:=]?\s*\d+\s*x?|\d+\s*X|x\d+)\s*$"
)
_BROAD_LEVERAGE_RE = re.compile(
    r"(?i)\b(?:leverage|плеч[оа]|cross|isolated)\b.*(?:\d+\s*x?|x\d+)"
)
_PERCENT_ONLY_RE = re.compile(r"^\s*\d+(?:[\.,]\d+)?\s*%\s*$")
_NUMBERED_TP_RE = re.compile(
    r"(?i)^\s*[^A-Za-zА-Яа-я0-9\n]*"
    r"(?:(?:\d{1,2}\s*[\)\:\-]\s+|\d{1,2}\s*\.\s+)"
    r"|(?:tp|тп|target|цель)\s*\d{0,2}\s*[:\)\.\-]?\s+)"
    r"(\d+(?:[\.,]\s*\d+)?)"
)
_EXPLICIT_TP_LINE_RE = re.compile(r"(?i)^\s*(?:tp|тп|target|цель)\s*\d{0,2}\b")
_PURE_TP_PRICE_RE = re.compile(
    r"^\s*(?:[•*\-]\s*)?(\d+(?:[\.,]\s*\d+)?)"
    r"(?:\s*(?:[|/\-]\s*)?\d+(?:[\.,]\s*\d+)?\s*%)?\s*$"
)
_TP_BLOCK_END_RE = re.compile(
    r"(?i)^\s*(?:"
    r"signal\s*id|#\s*id|id\s*[:#]|author|channel|source|comment|note|"
    r"coin\s*:|direction\s*:|направление\s*:|"
    r"entry\s*:|вход\s*:|твх\s*:|"
    r"leverage\s*:|плеч[оа]\s*:|cross\b|isolated\b"
    r")"
)
_TP_PERCENT_RE = re.compile(r"(\d+(?:[\.,]\s*\d+)?)\s*%")
_RANGE_RE = re.compile(r"(\d+(?:[\.,]\s*\d+)?)\s*[-–—]\s*(\d+(?:[\.,]\s*\d+)?)")

_GG_PRICE_TOKEN = r"\d+(?:[\.,]\s*\d+)?(?:\s*[eE]\s*[+-]?\s*\d+)?"
_SCIENTIFIC_PRICE_RE = re.compile(r"\d+(?:[\.,]\s*\d+)?\s*[eE]\s*[+-]?\s*\d+")
_GG_HEADER_RE = re.compile(
    r"(?im)^\s*[^A-Za-z0-9\n]*#\s*([A-Z0-9]{1,30}USDT)\b.*\b\d+\s*[mMhHdD]\b\s*\|\s*.*\bcross\b"
)
_GG_SIGNAL_ID_RE = re.compile(r"(?im)^\s*[^A-Za-z0-9\n]*signal\s*id\s*:\s*([A-Z0-9_-]{1,64})\b")
_GG_ENTRY_RE = re.compile(
    rf"(?i)\b(long|short)\s+entry\s+zone\s*:\s*({_GG_PRICE_TOKEN})\s*[-–—]\s*({_GG_PRICE_TOKEN})"
)
_GG_TARGET_RE = re.compile(
    rf"(?im)^\s*(?:[^A-Za-z0-9\n]*)target\s*\d+\s*:\s*({_GG_PRICE_TOKEN})\s*$"
)
_GG_STOP_RE = re.compile(rf"(?i)\bstop\s*[- ]?\s*loss\s*:\s*({_GG_PRICE_TOKEN})\b")
_GG_REQUIRED_RE = re.compile(r"(?is)signal\s*id\s*:.*entry\s+zone\s*:.*strategy\s+details\s*:.*target\s*1\s*:.*stop\s*[- ]?\s*loss\s*:")

_CHANNEL1_ENTRY_RE = re.compile(
    r"(?i)\b(long|short)\s+entry\s+zone\s*:\s*(\d+(?:[\.,]\s*\d+)?)\s*[-–—]\s*(\d+(?:[\.,]\s*\d+)?)"
)
_CHANNEL1_TARGET_RE = re.compile(
    r"(?im)^\s*(?:[^A-Za-z0-9\n]*)target\s*\d+\s*:\s*(\d+(?:[\.,]\s*\d+)?)"
)
_CHANNEL1_STOP_RE = re.compile(r"(?i)stop\s*[- ]?\s*loss\s*:\s*(\d+(?:[\.,]\s*\d+)?)")
_CHANNEL1_SYMBOL_RE = re.compile(
    r"(?im)^\s*(?!.*\b(?:signal\s*id|#id)\b).*#\s*([A-Z0-9]{1,30}USDT)\b"
)
_CHANNEL_SIGNAL_ID_RE = re.compile(r"(?i)(?:signal\s*id\s*:\s*)?(#\s*ID?\d+|#\s*\d+)\b")
_CHANNEL2_COIN_RE = re.compile(r"(?i)\bcoin\s*:\s*\$?\s*([A-Z0-9]{1,30})\s*/\s*USDT\b")
_CHANNEL2_DIRECTION_RE = re.compile(r"(?i)\bdirection\s*:\s*(long|short)\b")
_CHANNEL2_ENTRY_RE = re.compile(
    r"(?i)\bentry\s*:\s*(\d+(?:[\.,]\s*\d+)?)\s*[-–—]\s*(\d+(?:[\.,]\s*\d+)?)"
)
_CHANNEL2_TARGETS_RE = re.compile(
    r"(?is)\btargets\s*:\s*(.+?)(?:\n\s*\n|\n\s*stop\s*[- ]?\s*loss|$)"
)
_CHANNEL2_STOP_RE = re.compile(r"(?i)\bstop\s*[- ]?\s*loss\s*:\s*(\d+(?:[\.,]\s*\d+)?)")

# Strict Bitcoin Bullets / Banana Bot parser.  This format contains many
# unrelated numbers (leverage, strength, timeframe and RSI), therefore every
# trading value is read only from its own labelled line.  Scientific notation
# is intentionally enabled only in this dedicated branch and GG Shot.
#
# Important hardening rules:
# - horizontal whitespace never consumes CR/LF, so a broken multi-line price
#   cannot be silently glued into a valid number;
# - the decorative prefix may contain spaces/emoji/punctuation, but no Unicode
#   word characters, so prose such as "Комментарий COIN:" is not a field;
# - ASCII mode prevents Unicode case-fold confusables from changing a symbol
#   (for example long-s -> S or dotless-i -> I).
_BULLETS_FORMAT_NAME = "BITCOIN_BULLETS_SIGNAL_FORMAT"
_BULLETS_HSPACE = r"[^\S\r\n]*"
_BULLETS_LINE_PREFIX = r"(?u:[^\w\r\n]*)"
_BULLETS_PRICE_TOKEN = (
    rf"[0-9]+(?:[\.,]{_BULLETS_HSPACE}[0-9]+)?"
    rf"(?:{_BULLETS_HSPACE}[eE]{_BULLETS_HSPACE}[+-]?"
    rf"{_BULLETS_HSPACE}[0-9]+)?"
)
_BULLETS_SIGNAL_ID_LABEL_RE = re.compile(
    rf"(?aim)\bsignal{_BULLETS_HSPACE}id{_BULLETS_HSPACE}:"
)
_BULLETS_SIGNAL_ID_RE = re.compile(
    rf"(?aim)\bsignal{_BULLETS_HSPACE}id{_BULLETS_HSPACE}:"
    rf"{_BULLETS_HSPACE}#?{_BULLETS_HSPACE}([A-Z0-9_-]{{1,64}})\b"
)
_BULLETS_COIN_LABEL_RE = re.compile(
    rf"(?aim)^{_BULLETS_LINE_PREFIX}coin{_BULLETS_HSPACE}:"
)
_BULLETS_COIN_RE = re.compile(
    rf"(?aim)^{_BULLETS_LINE_PREFIX}coin{_BULLETS_HSPACE}:"
    rf"{_BULLETS_HSPACE}[#$]?{_BULLETS_HSPACE}"
    rf"([A-Z0-9]{{1,30}}USDT){_BULLETS_HSPACE}\r?$"
)
_BULLETS_DIRECTION_LABEL_RE = re.compile(
    rf"(?aim)^{_BULLETS_LINE_PREFIX}direction{_BULLETS_HSPACE}:"
)
_BULLETS_DIRECTION_RE = re.compile(
    rf"(?aim)^{_BULLETS_LINE_PREFIX}direction{_BULLETS_HSPACE}:"
    rf"{_BULLETS_HSPACE}(LONG|SHORT)\b"
    rf"(?:{_BULLETS_HSPACE}\|{_BULLETS_HSPACE}type{_BULLETS_HSPACE}:"
    rf"{_BULLETS_HSPACE}[^\r\n]+)?{_BULLETS_HSPACE}\r?$"
)
_BULLETS_ENTRY_LABEL_RE = re.compile(
    rf"(?aim)^{_BULLETS_LINE_PREFIX}entry{_BULLETS_HSPACE}:"
)
_BULLETS_ENTRY_RE = re.compile(
    rf"(?aim)^{_BULLETS_LINE_PREFIX}entry{_BULLETS_HSPACE}:"
    rf"{_BULLETS_HSPACE}({_BULLETS_PRICE_TOKEN})"
    rf"{_BULLETS_HSPACE}[-–—]{_BULLETS_HSPACE}"
    rf"({_BULLETS_PRICE_TOKEN}){_BULLETS_HSPACE}\r?$"
)
_BULLETS_TARGETS_LABEL_RE = re.compile(
    rf"(?aim)^{_BULLETS_LINE_PREFIX}targets{_BULLETS_HSPACE}:"
)
_BULLETS_TARGETS_RE = re.compile(
    rf"(?aim)^{_BULLETS_LINE_PREFIX}targets{_BULLETS_HSPACE}:"
    rf"{_BULLETS_HSPACE}({_BULLETS_PRICE_TOKEN})"
    rf"{_BULLETS_HSPACE}[-–—]{_BULLETS_HSPACE}"
    rf"({_BULLETS_PRICE_TOKEN}){_BULLETS_HSPACE}[-–—]{_BULLETS_HSPACE}"
    rf"({_BULLETS_PRICE_TOKEN}){_BULLETS_HSPACE}[-–—]{_BULLETS_HSPACE}"
    rf"({_BULLETS_PRICE_TOKEN}){_BULLETS_HSPACE}\r?$"
)
_BULLETS_STOP_LABEL_RE = re.compile(
    rf"(?aim)^{_BULLETS_LINE_PREFIX}stop{_BULLETS_HSPACE}[- ]?"
    rf"{_BULLETS_HSPACE}loss{_BULLETS_HSPACE}:"
)
_BULLETS_STOP_RE = re.compile(
    rf"(?aim)^{_BULLETS_LINE_PREFIX}stop{_BULLETS_HSPACE}[- ]?"
    rf"{_BULLETS_HSPACE}loss{_BULLETS_HSPACE}:{_BULLETS_HSPACE}"
    rf"({_BULLETS_PRICE_TOKEN}){_BULLETS_HSPACE}\r?$"
)
_BULLETS_LEVERAGE_RE = re.compile(
    rf"(?aim)^{_BULLETS_LINE_PREFIX}leverage{_BULLETS_HSPACE}:"
    rf"{_BULLETS_HSPACE}([0-9]+){_BULLETS_HSPACE}x"
    rf"{_BULLETS_HSPACE}\r?$"
)
_BULLETS_STRENGTH_RE = re.compile(
    rf"(?aim)^{_BULLETS_LINE_PREFIX}(?:signal{_BULLETS_HSPACE})?"
    rf"strength{_BULLETS_HSPACE}:{_BULLETS_HSPACE}"
    rf"([0-9]+(?:[\.,][0-9]+)?){_BULLETS_HSPACE}/"
    rf"{_BULLETS_HSPACE}100{_BULLETS_HSPACE}\r?$"
)
# Broad same-line label detectors are used only to identify malformed Bullets
# intent and block fallback into the permissive legacy parser.  Actual values
# are still extracted exclusively by the strict line-anchored expressions.
_BULLETS_INTENT_COIN_LABEL_RE = re.compile(
    rf"(?aim)\bcoin{_BULLETS_HSPACE}:"
)
_BULLETS_INTENT_DIRECTION_LABEL_RE = re.compile(
    rf"(?aim)\bdirection{_BULLETS_HSPACE}:"
)
_BULLETS_INTENT_ENTRY_LABEL_RE = re.compile(
    rf"(?aim)\bentry{_BULLETS_HSPACE}:"
)
_BULLETS_INTENT_TARGETS_LABEL_RE = re.compile(
    rf"(?aim)\btargets{_BULLETS_HSPACE}:"
)
_BULLETS_INTENT_STOP_LABEL_RE = re.compile(
    rf"(?aim)\bstop{_BULLETS_HSPACE}[- ]?{_BULLETS_HSPACE}"
    rf"loss{_BULLETS_HSPACE}:"
)


def parse_price(value: str | float | int) -> float:
    if isinstance(value, bool):
        raise ValueError("Цена должна быть числом")
    parsed = (
        float(value)
        if isinstance(value, (int, float))
        else float(str(value).replace(" ", "").replace(",", "."))
    )
    if not math.isfinite(parsed):
        raise ValueError("Цена должна быть конечным числом")
    return parsed


def infer_side(entry: float, stop: float, tps: list[float]) -> str:
    if stop < entry and any(tp > entry for tp in tps):
        return "LONG"
    if stop > entry and any(tp < entry for tp in tps):
        return "SHORT"
    raise ValueError(
        "Невозможно определить направление: TP/STOP стоят некорректно относительно входа."
    )


def _clean_text(text: str) -> str:
    text = text or ""
    text = text.replace("\u00a0", " ").replace("—", "-").replace("–", "-")
    return text.strip()


def _price_from_line(line: str) -> float | None:
    line = line or ""
    if (
        _LEVERAGE_RE.match(line)
        or _BROAD_LEVERAGE_RE.search(line)
        or _PERCENT_ONLY_RE.match(line)
    ):
        return None
    m = _NUMBER_RE.search(line)
    if not m:
        return None
    try:
        value = parse_price(m.group(0))
    except Exception:
        return None
    if value <= 0 or value > 10_000_000:
        return None
    return value


def _extract_symbol(text: str) -> str | None:
    if _GOLD_XAU_PAIR_RE.search(text or ""):
        return GOLD_XAU_INTERNAL_SYMBOL
    m = _PAIR_SLASH_RE.search(text or "")
    if m:
        return f"{m.group(1).upper()}USDT"
    m = _PAIR_PLAIN_RE.search((text or "").upper())
    return m.group(1).upper() if m else None


def find_vip_symbols(text: str) -> list[str]:
    symbols: list[str] = []
    if _GOLD_XAU_PAIR_RE.search(text or ""):
        symbols.append(GOLD_XAU_INTERNAL_SYMBOL)
    for m in _PAIR_SLASH_RE.finditer(text or ""):
        sym = f"{m.group(1).upper()}USDT"
        if sym not in symbols:
            symbols.append(sym)
    for m in _PAIR_PLAIN_RE.finditer((text or "").upper()):
        sym = m.group(1).upper()
        if sym not in symbols:
            symbols.append(sym)
    return symbols


def _extract_keyed_next(lines: list[str], key_re: re.Pattern[str]) -> float | None:
    for i, line in enumerate(lines):
        if not key_re.search(line):
            continue
        # Do not treat TP/STOP/ENTRY label index as price: use range first, then price.
        range_m = _RANGE_RE.search(line)
        if range_m:
            lo, hi, mid = _middle_from_range(range_m.group(1), range_m.group(2))
            return mid
        price = _price_from_line(line)
        if price is not None:
            return price
        for j in range(i + 1, min(i + 4, len(lines))):
            if (
                _LEVERAGE_RE.match(lines[j])
                or _BROAD_LEVERAGE_RE.search(lines[j])
                or _PERCENT_ONLY_RE.match(lines[j])
            ):
                continue
            range_m = _RANGE_RE.search(lines[j])
            if range_m:
                lo, hi, mid = _middle_from_range(range_m.group(1), range_m.group(2))
                return mid
            price = _price_from_line(lines[j])
            if price is not None:
                return price
    return None


def _extract_percent_from_line(line: str) -> float | None:
    matches = list(_TP_PERCENT_RE.finditer(line or ""))
    if not matches:
        return None
    try:
        val = parse_price(matches[-1].group(1))
        if 0 < val <= 100:
            return float(val)
    except Exception:
        return None
    return None


def _extract_tps(lines: list[str]) -> tuple[list[float], list[str], list[float]]:
    tps: list[float] = []
    percents: list[float | None] = []
    ignored: list[str] = []
    in_targets = False
    for line in lines:
        clean = line.strip()
        if not clean:
            continue
        if _STOP_KEY_RE.search(clean):
            in_targets = False
            continue
        if in_targets and _TP_BLOCK_END_RE.search(clean):
            in_targets = False
            ignored.append(clean)
            continue
        if _TP_BLOCK_RE.search(clean) and _price_from_line(clean) is None:
            in_targets = True
            continue
        if in_targets and _MOON_RE.search(clean):
            ignored.append(clean)
            continue
        parsed_price: float | None = None
        numbered = _NUMBERED_TP_RE.match(clean)
        if numbered and (in_targets or _EXPLICIT_TP_LINE_RE.match(clean)):
            parsed_price = parse_price(numbered.group(1))
        elif in_targets and not (
            _LEVERAGE_RE.match(clean)
            or _BROAD_LEVERAGE_RE.search(clean)
            or _PERCENT_ONLY_RE.match(clean)
        ):
            pure = _PURE_TP_PRICE_RE.match(clean)
            if pure:
                parsed_price = parse_price(pure.group(1))
        if parsed_price is not None:
            if parsed_price <= 0 or parsed_price > 10_000_000:
                continue
            tps.append(float(parsed_price))
            percents.append(_extract_percent_from_line(clean))
    # Deduplicate, preserving order. Keep the first percent attached to the TP.
    out: list[float] = []
    out_percents_raw: list[float | None] = []
    seen: set[float] = set()
    for tp, pct in zip(tps, percents):
        key = round(float(tp), 12)
        if key in seen:
            continue
        seen.add(key)
        out.append(float(tp))
        out_percents_raw.append(pct)

    out_percents: list[float] = []
    if (
        out
        and len(out_percents_raw) == len(out)
        and all(p is not None for p in out_percents_raw)
    ):
        vals = [float(p) for p in out_percents_raw if p is not None]
        if len(vals) == len(out) and abs(sum(vals) - 100.0) <= 0.001:
            out_percents = vals
    return out, ignored, out_percents


def _infer_market_side_from_stop_tps(stop: float, tps: list[float]) -> str | None:
    """Infer direction for no-entry MARKET signals from STOP/TP layout only.

    LONG: all TP are above STOP.
    SHORT: all TP are below STOP.
    Ambiguous/mixed layouts are rejected by returning None.
    """
    if tps and all(float(tp) > float(stop) for tp in tps):
        return "LONG"
    if tps and all(float(tp) < float(stop) for tp in tps):
        return "SHORT"
    return None


def _extract_side(
    text: str, entry: float | None, stop: float, tps: list[float]
) -> tuple[str | None, str | None]:
    raw_text = text or ""
    is_gold_signal = bool(_GOLD_XAU_PAIR_RE.search(raw_text))
    # BUY/SELL were added only for the Banana-style XAUUSD format.  Keep the
    # generic parser semantics unchanged so unrelated prose such as "sell zone"
    # cannot override an otherwise valid LONG/SHORT geometry on crypto signals.
    explicit_long = bool(_LONG_RE.search(raw_text)) or bool(
        is_gold_signal and _GOLD_BUY_RE.search(raw_text)
    )
    explicit_short = bool(_SHORT_RE.search(raw_text)) or bool(
        is_gold_signal and _GOLD_SELL_RE.search(raw_text)
    )
    if explicit_long and not explicit_short:
        side = "LONG"
    elif explicit_short and not explicit_long:
        side = "SHORT"
    else:
        if entry is None:
            inferred = _infer_market_side_from_stop_tps(stop, tps)
            if inferred:
                return inferred, None
            return (
                None,
                "Невозможно определить направление MARKET-сигнала без ТВХ: TP должны быть строго выше STOP для LONG или строго ниже STOP для SHORT.",
            )
        try:
            return infer_side(float(entry), float(stop), tps), None
        except Exception as exc:
            return None, str(exc)
    if entry is not None:
        if side == "LONG" and not (
            float(entry) > stop and all(tp > float(entry) for tp in tps)
        ):
            return (
                None,
                "В сигнале указано LONG, но цели/стоп противоречат направлению.",
            )
        if side == "SHORT" and not (
            float(entry) < stop and all(tp < float(entry) for tp in tps)
        ):
            return (
                None,
                "В сигнале указано SHORT, но цели/стоп противоречат направлению.",
            )
    else:
        if side == "LONG" and not all(tp > stop for tp in tps):
            return None, "В MARKET LONG без ТВХ все TP должны быть выше STOP."
        if side == "SHORT" and not all(tp < stop for tp in tps):
            return None, "В MARKET SHORT без ТВХ все TP должны быть ниже STOP."
    return side, None


def _decimal_price(value: str | float | int | Decimal) -> Decimal:
    """Parse a positive decimal price without passing through binary float first."""
    if isinstance(value, Decimal):
        parsed = value
    else:
        parsed = Decimal(str(value).replace(" ", "").replace(",", "."))
    if not parsed.is_finite() or parsed <= 0 or parsed > Decimal("10000000"):
        raise ValueError("Некорректная цена.")
    return parsed


def _middle_from_range(a: str, b: str) -> tuple[float, float, float]:
    # Parse source strings directly with Decimal so midpoint calculations do not
    # inherit a binary-float artefact before the price reaches BingX rounding.
    low_d = _decimal_price(a)
    high_d = _decimal_price(b)
    lo_d, hi_d = (low_d, high_d) if low_d <= high_d else (high_d, low_d)
    mid_d = (lo_d + hi_d) / Decimal("2")
    return float(lo_d), float(hi_d), float(mid_d)


def _extract_signal_id(raw: str) -> str | None:
    for m in _CHANNEL_SIGNAL_ID_RE.finditer(raw or ""):
        value = re.sub(r"\s+", "", m.group(1).upper())
        if value.startswith("#"):
            return value
    return None


def _normalize_channel_targets(tps: list[float]) -> list[float]:
    """Remove duplicate/invalid specialised-channel targets, preserving order."""
    out: list[float] = []
    seen: set[Decimal] = set()
    for raw in tps:
        try:
            value = float(raw)
        except (TypeError, ValueError):
            continue
        if value <= 0 or value > 10_000_000:
            continue
        key = Decimal(str(value)).normalize()
        if key in seen:
            continue
        seen.add(key)
        out.append(value)
    return out


def _validate_direction(
    side: str, entry: float, stop: float, tps: list[float]
) -> str | None:
    if not tps:
        return "Сигнал распознан, но не найдено ни одной корректной цели TP."
    if len(tps) > 20:
        return "Сигнал распознан, но поддерживается максимум 20 уникальных TP."
    if side == "LONG":
        if not stop < entry:
            return "Сигнал распознан, но stop должен быть ниже entry для LONG."
        if not all(tp > entry for tp in tps):
            return "Сигнал распознан, но все TP должны быть выше entry для LONG."
        if any(a >= b for a, b in zip(tps, tps[1:])):
            return "Сигнал распознан, но TP для LONG должны идти строго по возрастанию."
    if side == "SHORT":
        if not stop > entry:
            return "Сигнал распознан, но stop должен быть выше entry для SHORT."
        if not all(tp < entry for tp in tps):
            return "Сигнал распознан, но все TP должны быть ниже entry для SHORT."
        if any(a <= b for a, b in zip(tps, tps[1:])):
            return "Сигнал распознан, но TP для SHORT должны идти строго по убыванию."
    return None


def _make_channel_result(
    *,
    symbol: str,
    side: str,
    entry_low: float,
    entry_high: float,
    entry: float,
    stop: float,
    tps: list[float],
    format_name: str,
    signal_id: str | None,
    source_chat_id: int | None,
) -> VipParseResult:
    tps = _normalize_channel_targets(tps)
    validation_error = _validate_direction(side, entry, stop, tps)
    if validation_error:
        return VipParseResult(
            ok=False,
            error=validation_error,
            format_name=format_name,
            signal_id=signal_id,
            entry_zone_low=entry_low,
            entry_zone_high=entry_high,
            selected_entry=entry,
            confidence=0.95,
            errors=[validation_error],
        )
    plan = TradePlan(
        symbol=symbol,
        side=side,
        order_type="LIMIT",
        entry=float(entry),
        stop=float(stop),
        tps=[float(x) for x in tps],
        tp_percents=[],
    )
    return VipParseResult(
        ok=True,
        plan=plan,
        ignored_lines=[],
        signal_hash=make_signal_hash(plan, source_chat_id=source_chat_id),
        format_name=format_name,
        signal_id=signal_id,
        entry_zone_low=float(entry_low),
        entry_zone_high=float(entry_high),
        selected_entry=float(entry),
        confidence=0.98,
        errors=[],
    )



def _extract_gg_signal_id(raw: str) -> str | None:
    match = _GG_SIGNAL_ID_RE.search(raw or "")
    return match.group(1).upper() if match else None


def _parse_gg_price(value: str) -> float:
    return parse_price(re.sub(r"\s+", "", value or ""))


def _looks_like_gg_shot_signal(raw: str) -> bool:
    text = raw or ""
    return bool(
        _GG_REQUIRED_RE.search(text)
        and _GG_HEADER_RE.search(text)
        and _GG_ENTRY_RE.search(text)
    )


def _parse_gg_shot_signal(
    raw: str, *, source_chat_id: int | None = None
) -> VipParseResult | None:
    if not _looks_like_gg_shot_signal(raw):
        return None
    headers = list(_GG_HEADER_RE.finditer(raw or ""))
    if len(headers) > 1:
        symbols: list[str] = []
        for header in headers:
            symbol = header.group(1).upper()
            if symbol not in symbols:
                symbols.append(symbol)
        return VipParseResult(
            ok=False,
            multiple=symbols,
            error="multiple_signals",
            format_name="GG_SHOT_SIGNAL_FORMAT",
            signal_id=_extract_gg_signal_id(raw),
            confidence=0.95,
        )
    header_m = headers[0] if headers else None
    entry_m = _GG_ENTRY_RE.search(raw or "")
    stop_m = _GG_STOP_RE.search(raw or "")
    signal_id = _extract_gg_signal_id(raw)
    symbol = header_m.group(1).upper() if header_m else ""
    if not symbol:
        return VipParseResult(
            ok=False,
            error="GG_SHOT_SIGNAL_FORMAT: не найдена торговая пара.",
            format_name="GG_SHOT_SIGNAL_FORMAT",
            signal_id=signal_id,
            confidence=0.5,
        )
    if not entry_m:
        return VipParseResult(
            ok=False,
            error="GG_SHOT_SIGNAL_FORMAT: не найдена Entry Zone.",
            format_name="GG_SHOT_SIGNAL_FORMAT",
            signal_id=signal_id,
            confidence=0.7,
        )
    side = entry_m.group(1).upper()
    entry_low, entry_high, entry = _middle_from_range(
        entry_m.group(2), entry_m.group(3)
    )
    tps = [_parse_gg_price(m.group(1)) for m in _GG_TARGET_RE.finditer(raw or "")]
    if not stop_m:
        return VipParseResult(
            ok=False,
            error="GG_SHOT_SIGNAL_FORMAT: не найден Stop-Loss.",
            format_name="GG_SHOT_SIGNAL_FORMAT",
            signal_id=signal_id,
            entry_zone_low=entry_low,
            entry_zone_high=entry_high,
            selected_entry=entry,
            confidence=0.7,
        )
    if not tps:
        return VipParseResult(
            ok=False,
            error="GG_SHOT_SIGNAL_FORMAT: не найдены Target 1/2/3/4.",
            format_name="GG_SHOT_SIGNAL_FORMAT",
            signal_id=signal_id,
            entry_zone_low=entry_low,
            entry_zone_high=entry_high,
            selected_entry=entry,
            confidence=0.7,
        )
    stop = _parse_gg_price(stop_m.group(1))
    return _make_channel_result(
        symbol=symbol,
        side=side,
        entry_low=entry_low,
        entry_high=entry_high,
        entry=entry,
        stop=stop,
        tps=tps,
        format_name="GG_SHOT_SIGNAL_FORMAT",
        signal_id=signal_id,
        source_chat_id=source_chat_id,
    )


def _looks_like_bitcoin_bullets_signal(raw: str) -> bool:
    """Return True only for the complete labelled Bullets signature."""
    text = raw or ""
    return bool(
        _BULLETS_SIGNAL_ID_LABEL_RE.search(text)
        and _BULLETS_COIN_RE.search(text)
        and _BULLETS_DIRECTION_LABEL_RE.search(text)
        and _BULLETS_ENTRY_LABEL_RE.search(text)
        and _BULLETS_TARGETS_LABEL_RE.search(text)
        and _BULLETS_STOP_LABEL_RE.search(text)
    )


def _has_malformed_bitcoin_bullets_signature(raw: str) -> bool:
    """Keep incomplete Bullets posts fail-closed without hijacking CHANNEL_2.

    SIGNAL ID is the distinguishing marker.  CHANNEL_2 uses the same COIN /
    Direction / ENTRY / TARGETS / STOP labels but does not use this marker.
    Requiring SIGNAL ID here therefore preserves the old CHANNEL_2 routing.
    """
    text = raw or ""
    if not _BULLETS_SIGNAL_ID_LABEL_RE.search(text):
        return False
    # Existing CHANNEL_2 deliberately uses COIN: $BASE/USDT and target zones.
    # Never route that approved slash format into the Bullets parser.
    if _CHANNEL2_COIN_RE.search(text):
        return False
    core_labels = sum(
        bool(pattern.search(text))
        for pattern in (
            _BULLETS_INTENT_COIN_LABEL_RE,
            _BULLETS_INTENT_DIRECTION_LABEL_RE,
            _BULLETS_INTENT_ENTRY_LABEL_RE,
            _BULLETS_INTENT_TARGETS_LABEL_RE,
            _BULLETS_INTENT_STOP_LABEL_RE,
        )
    )
    return core_labels >= 3


def _parse_bullets_price_token(value: str, *, field_name: str) -> Decimal:
    """Parse one fully matched positive ASCII price token without float guessing."""
    raw_value = str(value or "")
    if "\r" in raw_value or "\n" in raw_value:
        raise ValueError(
            f"{_BULLETS_FORMAT_NAME}: {field_name} должен находиться в одной строке."
        )
    normalized = re.sub(r"[^\S\r\n]+", "", raw_value).replace(",", ".")
    if not re.fullmatch(
        r"[0-9]+(?:\.[0-9]+)?(?:[eE][+-]?[0-9]+)?",
        normalized,
        flags=re.ASCII,
    ):
        raise ValueError(
            f"{_BULLETS_FORMAT_NAME}: {field_name} содержит повреждённую цену."
        )
    try:
        parsed = Decimal(normalized)
    except (InvalidOperation, ValueError) as exc:
        raise ValueError(
            f"{_BULLETS_FORMAT_NAME}: {field_name} содержит повреждённую цену."
        ) from exc
    if not parsed.is_finite() or parsed <= 0 or parsed > Decimal("10000000"):
        raise ValueError(
            f"{_BULLETS_FORMAT_NAME}: {field_name} должен быть конечной положительной ценой."
        )
    try:
        float_value = float(parsed)
    except (OverflowError, ValueError) as exc:
        raise ValueError(
            f"{_BULLETS_FORMAT_NAME}: {field_name} невозможно безопасно преобразовать."
        ) from exc
    if not math.isfinite(float_value) or float_value <= 0:
        raise ValueError(
            f"{_BULLETS_FORMAT_NAME}: {field_name} выходит за безопасный диапазон."
        )
    return parsed


def _bullets_error(
    message: str,
    *,
    signal_id: str | None = None,
    entry_low: float | None = None,
    entry_high: float | None = None,
    selected_entry: float | None = None,
) -> VipParseResult:
    return VipParseResult(
        ok=False,
        error=message,
        format_name=_BULLETS_FORMAT_NAME,
        signal_id=signal_id,
        entry_zone_low=entry_low,
        entry_zone_high=entry_high,
        selected_entry=selected_entry,
        confidence=0.95,
        errors=[message],
    )


def _validate_bullets_zone_geometry(
    *,
    side: str,
    entry_low: Decimal | float,
    entry_high: Decimal | float,
    stop: Decimal | float,
    tps: list[Decimal] | list[float],
) -> str | None:
    if side == "LONG":
        if not stop < entry_low:
            return (
                f"{_BULLETS_FORMAT_NAME}: для LONG STOP LOSS должен быть "
                "ниже всей ENTRY-зоны."
            )
        if not all(tp > entry_high for tp in tps):
            return (
                f"{_BULLETS_FORMAT_NAME}: для LONG все TARGETS должны быть "
                "выше всей ENTRY-зоны."
            )
        if any(a >= b for a, b in zip(tps, tps[1:])):
            return (
                f"{_BULLETS_FORMAT_NAME}: TARGETS для LONG должны идти "
                "строго по возрастанию."
            )
    elif side == "SHORT":
        if not stop > entry_high:
            return (
                f"{_BULLETS_FORMAT_NAME}: для SHORT STOP LOSS должен быть "
                "выше всей ENTRY-зоны."
            )
        if not all(tp < entry_low for tp in tps):
            return (
                f"{_BULLETS_FORMAT_NAME}: для SHORT все TARGETS должны быть "
                "ниже всей ENTRY-зоны."
            )
        if any(a <= b for a, b in zip(tps, tps[1:])):
            return (
                f"{_BULLETS_FORMAT_NAME}: TARGETS для SHORT должны идти "
                "строго по убыванию."
            )
    else:
        return f"{_BULLETS_FORMAT_NAME}: Direction должен быть LONG или SHORT."
    return None


def _project_bullets_prices_to_float(
    *,
    entry_low: Decimal,
    entry_high: Decimal,
    side: str,
    selected_entry: Decimal,
    stop: Decimal,
    tps: list[Decimal],
) -> tuple[float, float, float, float, list[float]]:
    """Project Decimal prices to the float model without losing safety geometry.

    The rest of ANTILUD intentionally uses floats.  Distinct Decimal values may
    collapse to the same float when a source sends excessive precision.  A
    strict parser must reject that message instead of silently deleting a TP or
    weakening STOP/ENTRY-zone relationships.
    """
    entry_low_f = float(entry_low)
    entry_high_f = float(entry_high)
    selected_entry_f = float(selected_entry)
    stop_f = float(stop)
    tps_f = [float(value) for value in tps]

    all_values = [entry_low_f, entry_high_f, selected_entry_f, stop_f, *tps_f]
    if not all(math.isfinite(value) and value > 0 for value in all_values):
        raise ValueError(
            f"{_BULLETS_FORMAT_NAME}: цены невозможно безопасно представить в торговой модели."
        )
    if entry_low != entry_high and not entry_low_f < entry_high_f:
        raise ValueError(
            f"{_BULLETS_FORMAT_NAME}: границы ENTRY теряют различимость из-за избыточной точности."
        )
    if not entry_low_f <= selected_entry_f <= entry_high_f:
        raise ValueError(
            f"{_BULLETS_FORMAT_NAME}: midpoint ENTRY невозможно безопасно представить."
        )
    if len(tps_f) != 4 or len(set(tps_f)) != 4:
        raise ValueError(
            f"{_BULLETS_FORMAT_NAME}: четыре TARGETS теряют различимость из-за избыточной точности."
        )

    projected_geometry_error = _validate_bullets_zone_geometry(
        side=side,
        entry_low=entry_low_f,
        entry_high=entry_high_f,
        stop=stop_f,
        tps=tps_f,
    )
    if projected_geometry_error:
        raise ValueError(
            f"{_BULLETS_FORMAT_NAME}: цены теряют безопасную геометрию после преобразования."
        )
    return entry_low_f, entry_high_f, selected_entry_f, stop_f, tps_f


def _parse_bitcoin_bullets_signal(
    raw: str, *, source_chat_id: int | None = None
) -> VipParseResult | None:
    if not (
        _looks_like_bitcoin_bullets_signal(raw)
        or _has_malformed_bitcoin_bullets_signature(raw)
    ):
        return None

    text = raw or ""
    format_name = _BULLETS_FORMAT_NAME
    label_patterns: tuple[tuple[str, re.Pattern[str]], ...] = (
        ("SIGNAL ID", _BULLETS_SIGNAL_ID_LABEL_RE),
        ("COIN", _BULLETS_COIN_LABEL_RE),
        ("Direction", _BULLETS_DIRECTION_LABEL_RE),
        ("ENTRY", _BULLETS_ENTRY_LABEL_RE),
        ("TARGETS", _BULLETS_TARGETS_LABEL_RE),
        ("STOP LOSS", _BULLETS_STOP_LABEL_RE),
    )

    counts = {name: len(list(pattern.finditer(text))) for name, pattern in label_patterns}
    missing = [name for name, count in counts.items() if count == 0]
    if missing:
        return _bullets_error(
            f"{format_name}: не найдено обязательное поле {missing[0]}."
        )

    duplicate_fields = [name for name, count in counts.items() if count > 1]
    if duplicate_fields:
        coin_values = [m.group(1).upper() for m in _BULLETS_COIN_RE.finditer(text)]
        if counts["COIN"] > 1 or counts["SIGNAL ID"] > 1:
            return VipParseResult(
                ok=False,
                # Preserve duplicates: two different signals for the same pair
                # are still two plans and the user-facing count must remain 2.
                multiple=coin_values,
                error="multiple_signals",
                format_name=format_name,
                confidence=0.99,
                errors=[
                    f"{format_name}: найдено несколько сигналов в одном сообщении."
                ],
            )
        return _bullets_error(
            f"{format_name}: поле {duplicate_fields[0]} указано несколько раз."
        )

    signal_id_m = _BULLETS_SIGNAL_ID_RE.search(text)
    signal_id = signal_id_m.group(1).upper() if signal_id_m else None
    if not signal_id:
        return _bullets_error(
            f"{format_name}: SIGNAL ID имеет неверный формат."
        )

    coin_m = _BULLETS_COIN_RE.search(text)
    if not coin_m:
        return _bullets_error(
            f"{format_name}: COIN должен иметь формат #ARBUSDT.",
            signal_id=signal_id,
        )
    symbol = coin_m.group(1).upper()
    if symbol.endswith("USDTUSDT"):
        return _bullets_error(
            f"{format_name}: COIN с повторным суффиксом USDT запрещён.",
            signal_id=signal_id,
        )

    direction_m = _BULLETS_DIRECTION_RE.search(text)
    if not direction_m:
        return _bullets_error(
            f"{format_name}: Direction должен быть LONG или SHORT.",
            signal_id=signal_id,
        )
    side = direction_m.group(1).upper()

    entry_m = _BULLETS_ENTRY_RE.search(text)
    if not entry_m:
        return _bullets_error(
            f"{format_name}: ENTRY должен содержать ровно две корректные цены A - B.",
            signal_id=signal_id,
        )
    targets_m = _BULLETS_TARGETS_RE.search(text)
    if not targets_m:
        return _bullets_error(
            f"{format_name}: TARGETS должен содержать ровно четыре самостоятельных цены.",
            signal_id=signal_id,
        )
    stop_m = _BULLETS_STOP_RE.search(text)
    if not stop_m:
        return _bullets_error(
            f"{format_name}: STOP LOSS должен содержать ровно одну корректную цену.",
            signal_id=signal_id,
        )

    try:
        entry_a = _parse_bullets_price_token(entry_m.group(1), field_name="ENTRY")
        entry_b = _parse_bullets_price_token(entry_m.group(2), field_name="ENTRY")
        target_values = [
            _parse_bullets_price_token(targets_m.group(i), field_name=f"TARGETS TP{i}")
            for i in range(1, 5)
        ]
        stop_value = _parse_bullets_price_token(
            stop_m.group(1), field_name="STOP LOSS"
        )
    except ValueError as exc:
        return _bullets_error(str(exc), signal_id=signal_id)

    entry_low_d, entry_high_d = (
        (entry_a, entry_b) if entry_a <= entry_b else (entry_b, entry_a)
    )
    selected_entry_d = (entry_low_d + entry_high_d) / Decimal("2")
    entry_low_preview = float(entry_low_d)
    entry_high_preview = float(entry_high_d)
    selected_entry_preview = float(selected_entry_d)

    geometry_error = _validate_bullets_zone_geometry(
        side=side,
        entry_low=entry_low_d,
        entry_high=entry_high_d,
        stop=stop_value,
        tps=target_values,
    )
    if geometry_error:
        return _bullets_error(
            geometry_error,
            signal_id=signal_id,
            entry_low=entry_low_preview,
            entry_high=entry_high_preview,
            selected_entry=selected_entry_preview,
        )

    try:
        entry_low, entry_high, selected_entry, stop_float, target_floats = (
            _project_bullets_prices_to_float(
                side=side,
                entry_low=entry_low_d,
                entry_high=entry_high_d,
                selected_entry=selected_entry_d,
                stop=stop_value,
                tps=target_values,
            )
        )
    except ValueError as exc:
        return _bullets_error(
            str(exc),
            signal_id=signal_id,
            entry_low=entry_low_preview,
            entry_high=entry_high_preview,
            selected_entry=selected_entry_preview,
        )

    result = _make_channel_result(
        symbol=symbol,
        side=side,
        entry_low=entry_low,
        entry_high=entry_high,
        entry=selected_entry,
        stop=stop_float,
        tps=target_floats,
        format_name=format_name,
        signal_id=signal_id,
        source_chat_id=source_chat_id,
    )
    if result.ok and (result.plan is None or len(result.plan.tps) != 4):
        return _bullets_error(
            f"{_BULLETS_FORMAT_NAME}: внутреннее преобразование изменило количество TARGETS.",
            signal_id=signal_id,
            entry_low=entry_low,
            entry_high=entry_high,
            selected_entry=selected_entry,
        )
    if result.ok:
        leverage_m = _BULLETS_LEVERAGE_RE.search(text)
        log.info(
            "BITCOIN_BULLETS_PARSED signal_id=%s symbol=%s side=%s "
            "entry_low=%s entry_high=%s selected_entry=%s tp_count=%s "
            "source_chat_id=%s scientific_notation=%s "
            "advertised_leverage=%s ignored_for_execution=true",
            signal_id,
            symbol,
            side,
            entry_low,
            entry_high,
            selected_entry,
            len(target_values),
            source_chat_id,
            bool(_SCIENTIFIC_PRICE_RE.search(text)),
            leverage_m.group(1) if leverage_m else None,
        )
    return result


def _parse_channel1_entry_zone_accuracy(
    raw: str, *, source_chat_id: int | None = None
) -> VipParseResult | None:
    entry_m = _CHANNEL1_ENTRY_RE.search(raw or "")
    if not entry_m:
        return None
    symbol_m = _CHANNEL1_SYMBOL_RE.search(raw or "")
    symbol = (
        symbol_m.group(1).upper() if symbol_m else (_extract_symbol(raw or "") or "")
    )
    if not symbol:
        return VipParseResult(
            ok=False,
            error="CHANNEL_1_ENTRY_ZONE_ACCURACY: не найдена торговая пара.",
            format_name="CHANNEL_1_ENTRY_ZONE_ACCURACY",
            signal_id=_extract_signal_id(raw),
            confidence=0.5,
        )
    side = entry_m.group(1).upper()
    entry_low, entry_high, entry = _middle_from_range(
        entry_m.group(2), entry_m.group(3)
    )
    tps = [
        float(parse_price(m.group(1))) for m in _CHANNEL1_TARGET_RE.finditer(raw or "")
    ]
    stop_m = _CHANNEL1_STOP_RE.search(raw or "")
    signal_id = _extract_signal_id(raw)
    if not stop_m:
        return VipParseResult(
            ok=False,
            error="CHANNEL_1_ENTRY_ZONE_ACCURACY: не найден Stop-Loss.",
            format_name="CHANNEL_1_ENTRY_ZONE_ACCURACY",
            signal_id=signal_id,
            entry_zone_low=entry_low,
            entry_zone_high=entry_high,
            selected_entry=entry,
            confidence=0.7,
        )
    if not tps:
        return VipParseResult(
            ok=False,
            error="CHANNEL_1_ENTRY_ZONE_ACCURACY: не найдены Target 1/2/3/4.",
            format_name="CHANNEL_1_ENTRY_ZONE_ACCURACY",
            signal_id=signal_id,
            entry_zone_low=entry_low,
            entry_zone_high=entry_high,
            selected_entry=entry,
            confidence=0.7,
        )
    stop = float(parse_price(stop_m.group(1)))
    return _make_channel_result(
        symbol=symbol,
        side=side,
        entry_low=entry_low,
        entry_high=entry_high,
        entry=entry,
        stop=stop,
        tps=tps,
        format_name="CHANNEL_1_ENTRY_ZONE_ACCURACY",
        signal_id=signal_id,
        source_chat_id=source_chat_id,
    )


def _midpoints_from_dash_separated_target_zones(
    value: str,
) -> tuple[list[float], str | None]:
    """Parse CHANNEL_2 TARGETS as consecutive price-zone pairs.

    Example: ``0.4000 - 0.4200 - 0.4500 - 0.4800`` becomes
    two take-profits: ``0.4100`` and ``0.4650``.  The parser fails closed when
    any numeric boundary is left without its pair; silently opening a trade
    with a shifted TP plan would be unsafe.
    """
    raw = value or ""
    number_tokens = [m.group(0) for m in _NUMBER_RE.finditer(raw)]
    ranges = list(_RANGE_RE.finditer(raw))
    if not number_tokens:
        return [], "CHANNEL_2_SIGNAL_FORMAT: не найдены числовые TARGETS."
    if len(number_tokens) % 2 != 0:
        return [], (
            "CHANNEL_2_SIGNAL_FORMAT: TARGETS должны состоять из пар границ "
            f"ценовых зон; найдено нечётное количество цен ({len(number_tokens)}). "
            "Сделка не открыта."
        )
    if len(ranges) * 2 != len(number_tokens):
        return [], (
            "CHANNEL_2_SIGNAL_FORMAT: не удалось однозначно разбить TARGETS "
            "на ценовые диапазоны вида A - B. Сделка не открыта."
        )
    midpoints: list[float] = []
    for match in ranges:
        _, _, midpoint = _middle_from_range(match.group(1), match.group(2))
        midpoints.append(midpoint)
    return midpoints, None


def _parse_channel2_signal_format(
    raw: str, *, source_chat_id: int | None = None
) -> VipParseResult | None:
    coin_m = _CHANNEL2_COIN_RE.search(raw or "")
    direction_m = _CHANNEL2_DIRECTION_RE.search(raw or "")
    entry_m = _CHANNEL2_ENTRY_RE.search(raw or "")
    if not (coin_m and direction_m and entry_m):
        return None
    symbol = f"{coin_m.group(1).upper()}USDT"
    side = direction_m.group(1).upper()
    entry_low, entry_high, entry = _middle_from_range(
        entry_m.group(1), entry_m.group(2)
    )
    targets_m = _CHANNEL2_TARGETS_RE.search(raw or "")
    stop_m = _CHANNEL2_STOP_RE.search(raw or "")
    signal_id = _extract_signal_id(raw)
    if not stop_m:
        return VipParseResult(
            ok=False,
            error="CHANNEL_2_SIGNAL_FORMAT: не найден STOP LOSS.",
            format_name="CHANNEL_2_SIGNAL_FORMAT",
            signal_id=signal_id,
            entry_zone_low=entry_low,
            entry_zone_high=entry_high,
            selected_entry=entry,
            confidence=0.7,
        )
    if not targets_m:
        return VipParseResult(
            ok=False,
            error="CHANNEL_2_SIGNAL_FORMAT: не найдены TARGETS.",
            format_name="CHANNEL_2_SIGNAL_FORMAT",
            signal_id=signal_id,
            entry_zone_low=entry_low,
            entry_zone_high=entry_high,
            selected_entry=entry,
            confidence=0.7,
        )
    tps, targets_error = _midpoints_from_dash_separated_target_zones(targets_m.group(1))
    if targets_error:
        return VipParseResult(
            ok=False,
            error=targets_error,
            format_name="CHANNEL_2_SIGNAL_FORMAT",
            signal_id=signal_id,
            entry_zone_low=entry_low,
            entry_zone_high=entry_high,
            selected_entry=entry,
            confidence=0.95,
            errors=[targets_error],
        )
    stop = float(parse_price(stop_m.group(1)))
    return _make_channel_result(
        symbol=symbol,
        side=side,
        entry_low=entry_low,
        entry_high=entry_high,
        entry=entry,
        stop=stop,
        tps=tps,
        format_name="CHANNEL_2_SIGNAL_FORMAT",
        signal_id=signal_id,
        source_chat_id=source_chat_id,
    )


def _parse_new_channel_formats(
    raw: str, *, source_chat_id: int | None = None
) -> VipParseResult | None:
    return (
        _parse_gg_shot_signal(raw, source_chat_id=source_chat_id)
        or _parse_bitcoin_bullets_signal(raw, source_chat_id=source_chat_id)
        or _parse_channel1_entry_zone_accuracy(raw, source_chat_id=source_chat_id)
        or _parse_channel2_signal_format(raw, source_chat_id=source_chat_id)
    )


def parse_vip_signal(text: str, source_chat_id: int | None = None) -> VipParseResult:
    raw = _clean_text(text or "")
    new_format_result = _parse_new_channel_formats(raw, source_chat_id=source_chat_id)
    if new_format_result is not None:
        return new_format_result
    if _SCIENTIFIC_PRICE_RE.search(raw):
        return VipParseResult(
            ok=False,
            error=(
                "Scientific notation поддерживается только для строгих "
                "GG_SHOT_SIGNAL_FORMAT и BITCOIN_BULLETS_SIGNAL_FORMAT. "
                "Сделка не открыта."
            ),
            format_name="EXISTING_VIP_STANDARD",
            confidence=0.95,
        )

    symbols = find_vip_symbols(raw)
    if len(symbols) > 1:
        return VipParseResult(ok=False, multiple=symbols, error="multiple_signals")
    symbol = _extract_symbol(raw)
    if not symbol:
        return VipParseResult(
            ok=False, error="Не найдена торговая пара. Пример: KAITO/USDT"
        )

    lines = [ln.strip() for ln in raw.replace("\r\n", "\n").split("\n") if ln.strip()]
    entry = _extract_keyed_next(lines, _ENTRY_KEY_RE)
    stop = _extract_keyed_next(lines, _STOP_KEY_RE)
    tps, ignored, tp_percents = _extract_tps(lines)

    if stop is None:
        return VipParseResult(ok=False, error="Не найден стоп. Пример: Стоп: 0.5228")
    if not tps:
        return VipParseResult(ok=False, error="Не найдено ни одной числовой цели/TP.")
    if len(tps) > 20:
        return VipParseResult(ok=False, error="Поддерживается максимум 20 TP.")

    side, side_error = _extract_side(
        raw,
        float(entry) if entry is not None else None,
        float(stop),
        [float(x) for x in tps],
    )
    if side_error:
        return VipParseResult(ok=False, error=side_error)

    order_type = "LIMIT" if entry is not None else "MARKET"
    plan = TradePlan(
        symbol=symbol,
        side=side,
        order_type=order_type,
        entry=float(entry) if entry is not None else None,
        stop=float(stop),
        tps=[float(x) for x in tps],
        tp_percents=tp_percents,
    )
    return VipParseResult(
        ok=True,
        plan=plan,
        ignored_lines=ignored,
        signal_hash=make_signal_hash(plan, source_chat_id=source_chat_id),
        format_name="EXISTING_VIP_STANDARD",
        confidence=0.9,
    )


def make_signal_hash(plan: TradePlan, source_chat_id: int | None = None) -> str:
    payload = {
        "symbol": plan.symbol,
        "side": plan.side,
        "order_type": plan.order_type,
        "entry": float(plan.entry) if plan.entry is not None else None,
        "stop": float(plan.stop),
        "tps": [float(x) for x in plan.tps],
        "tp_percents": [float(x) for x in (plan.tp_percents or [])],
        "source_chat_id": int(source_chat_id or 0),
    }
    return hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()


def multiple_vip_signals_text(symbols: list[str]) -> str:
    listed = "\n".join([f"{i}. {sym}" for i, sym in enumerate(symbols, start=1)])
    return (
        "⚠️ Найдено несколько VIP-сигналов\n\n"
        f"Найдено {len(symbols)} торговых планов в одном сообщении.\n\n"
        "Для безопасности отправь один сигнал одним сообщением. Сделки не открыты.\n\n"
        f"Найдено:\n{listed}"
    )


def _to_signal(result: VipParseResult, raw: str) -> Optional[Signal]:
    if not result.ok or not result.plan:
        return None
    plan = result.plan
    if not plan.side:
        return None
    side = Side.LONG if str(plan.side).upper() == "LONG" else Side.SHORT
    order_type = str(
        plan.order_type or ("MARKET" if plan.entry is None else "LIMIT")
    ).upper()
    sig = Signal(
        symbol=plan.symbol,
        side=side,
        entry=float(plan.entry) if plan.entry is not None else 0.0,
        stop=float(plan.stop),
        targets=[float(x) for x in plan.tps],
        order_type=order_type,
        target_percents=[float(x) for x in (plan.tp_percents or [])],
        signal_id=result.signal_id or _extract_signal_id(raw),
        source_format=(result.format_name or "EXISTING_VIP_STANDARD"),
        raw_text=raw,
    )
    sig.validate()
    return sig


def parse_signal(text: str) -> Optional[Signal]:
    raw = _clean_text(text or "")
    if not raw:
        return None
    result = parse_vip_signal(raw)
    if result.error == "multiple_signals":
        raise ValueError(multiple_vip_signals_text(result.multiple or []))
    if not result.ok:
        return None
    return _to_signal(result, raw)


def signal_hash(signal: Signal, source_chat_id: int | None = None) -> str:
    plan = TradePlan(
        symbol=signal.symbol,
        side=signal.side.value.upper(),
        order_type=str(getattr(signal, "order_type", "LIMIT")).upper(),
        entry=(
            signal.entry
            if not (
                str(getattr(signal, "order_type", "LIMIT")).upper() == "MARKET"
                and signal.entry <= 0
            )
            else None
        ),
        stop=signal.stop,
        tps=signal.targets,
        tp_percents=list(getattr(signal, "target_percents", []) or []),
    )
    return make_signal_hash(plan, source_chat_id=source_chat_id)
