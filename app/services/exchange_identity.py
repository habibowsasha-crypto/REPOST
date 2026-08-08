from __future__ import annotations

import math
import re
from decimal import Decimal, InvalidOperation
from typing import Any

_SAFE_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$")
_MAX_EXACT_FLOAT_INTEGER = 2**53 - 1

_NULL_TOKENS = {
    "",
    "none",
    "null",
    "nan",
    "+nan",
    "-nan",
    "inf",
    "+inf",
    "-inf",
    "infinity",
    "+infinity",
    "-infinity",
}


def clean_exchange_id(value: Any) -> str:
    """Return a strict scalar exchange identifier or an empty string.

    BingX order/position identifiers are strings or positive integral numbers.
    Containers, booleans, bytes, non-finite values, zero, negatives and
    fractional numeric values are rejected instead of being stringified into
    fake exact identities such as ``"{'id': 1}"`` or ``"[1]"``.
    """

    if value is None or isinstance(value, (bool, bytes, bytearray, memoryview)):
        return ""
    if isinstance(value, (dict, list, tuple, set, frozenset)):
        return ""

    if isinstance(value, int):
        return str(value) if value > 0 else ""

    if isinstance(value, float):
        # JSON integer identifiers should normally decode as ``int``. Accept a
        # small legacy ``123.0`` only while binary float still represents every
        # integer exactly; larger floats can silently change an order id.
        if (
            not math.isfinite(value)
            or value <= 0
            or not value.is_integer()
            or value > _MAX_EXACT_FLOAT_INTEGER
        ):
            return ""
        return str(int(value))

    if isinstance(value, Decimal):
        if not value.is_finite() or value <= 0 or value != value.to_integral_value():
            return ""
        return format(value.to_integral_value(), "f")

    if not isinstance(value, str):
        return ""

    text = value.strip()
    if text.lower() in _NULL_TOKENS:
        return ""
    if any(ord(char) < 32 or ord(char) == 127 for char in text):
        return ""
    if not _SAFE_ID_RE.fullmatch(text):
        return ""

        # Numeric exchange ids must use canonical decimal integer syntax. Reject
        # scientific notation and decimal spellings (``1e3``, ``1.0``) instead of
        # treating them as exact identities. Alphanumeric client ids remain valid.
    if text.isascii() and text.isdigit():
        try:
            numeric = Decimal(text)
        except (InvalidOperation, ValueError):
            return ""
        if not numeric.is_finite() or numeric <= 0:
            return ""
        return text
    try:
        Decimal(text)
    except (InvalidOperation, ValueError):
        return text
    return ""
