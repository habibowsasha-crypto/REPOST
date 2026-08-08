from __future__ import annotations

from cryptography.fernet import Fernet


def make_fernet(key: str) -> Fernet:
    if not key:
        raise ValueError("ENCRYPTION_KEY не задан")
    return Fernet(key.encode() if isinstance(key, str) else key)


def encrypt_text(value: str, key: str) -> str:
    return make_fernet(key).encrypt(value.encode("utf-8")).decode("utf-8")


def decrypt_text(value: str, key: str) -> str:
    return make_fernet(key).decrypt(value.encode("utf-8")).decode("utf-8")
