"""Names and display helpers for selectable first-DM modules."""

from __future__ import annotations

from services.first_message_kirill_vip import MODULE_ID as KIRILL_VIP_MODULE


DEFAULT_FIRST_DM_MODULE = "default"
VALID_FIRST_DM_MODULES = {DEFAULT_FIRST_DM_MODULE, KIRILL_VIP_MODULE}


def normalize_first_dm_module(value: str | None) -> str:
    module = str(value or "").strip().lower()
    return module if module in VALID_FIRST_DM_MODULES else DEFAULT_FIRST_DM_MODULE


def first_dm_module_label(value: str | None) -> str:
    module = normalize_first_dm_module(value)
    if module == KIRILL_VIP_MODULE:
        return "👑 VIP Кирилла"
    return "🧩 Текущие фразы"
