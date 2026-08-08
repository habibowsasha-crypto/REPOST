from __future__ import annotations

import hashlib
from functools import lru_cache
from pathlib import Path

TERMS_VERSION = "ANTILUD_TERMS_v1.0"
TERMS_FILENAME = "ANTILUD_USER_AGREEMENT_v1_0.txt"
TERMS_PATH = Path(__file__).resolve().parents[1] / "legal" / TERMS_FILENAME


@lru_cache(maxsize=1)
def terms_bytes() -> bytes:
    return TERMS_PATH.read_bytes()


@lru_cache(maxsize=1)
def terms_hash() -> str:
    return hashlib.sha256(terms_bytes()).hexdigest()
