from __future__ import annotations

from collections.abc import Sequence

from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup


def ai_account_profiles_list_keyboard(
    profiles: Sequence[tuple[int, str, str]],
    *,
    page: int,
    total_pages: int,
) -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = []
    for profile_id, label, icon in profiles:
        rows.append(
            [
                InlineKeyboardButton(
                    text=f"{icon} {label}",
                    callback_data=f"aic:pr:{profile_id}:{page}",
                )
            ]
        )
    if total_pages > 1:
        navigation: list[InlineKeyboardButton] = []
        if page > 0:
            navigation.append(
                InlineKeyboardButton(
                    text="⬅️",
                    callback_data=f"aic:profiles:{page - 1}",
                )
            )
        navigation.append(
            InlineKeyboardButton(
                text=f"{page + 1}/{total_pages}",
                callback_data=f"aic:profiles:{page}",
            )
        )
        if page + 1 < total_pages:
            navigation.append(
                InlineKeyboardButton(
                    text="➡️",
                    callback_data=f"aic:profiles:{page + 1}",
                )
            )
        rows.append(navigation)
    rows.extend(
        [
            [
                InlineKeyboardButton(
                    text="🔄 Проверить новые аккаунты",
                    callback_data=f"aic:profiles:{page}",
                )
            ],
            [
                InlineKeyboardButton(
                    text="⬅️ К комментариям",
                    callback_data="aic:menu",
                )
            ],
            [InlineKeyboardButton(text="🏠 Главное меню", callback_data="main")],
        ]
    )
    return InlineKeyboardMarkup(inline_keyboard=rows)


def ai_account_profile_keyboard(
    *,
    profile_id: int,
    profile_version: int,
    page: int,
    enabled: bool,
    retired: bool,
) -> InlineKeyboardMarkup:
    toggle_target = 0 if enabled else 1
    toggle_text = "🔴 Выключить" if enabled else ("♻️ Восстановить" if retired else "🟢 Включить")
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="✏️ Основное",
                    callback_data=f"aic:pe:b:{profile_id}:{profile_version}:{page}",
                ),
                InlineKeyboardButton(
                    text="🗣 Стиль",
                    callback_data=f"aic:pe:s:{profile_id}:{profile_version}:{page}",
                ),
            ],
            [
                InlineKeyboardButton(
                    text="⏱ Лимиты",
                    callback_data=f"aic:pe:l:{profile_id}:{profile_version}:{page}",
                ),
                InlineKeyboardButton(
                    text="🛡 Запреты",
                    callback_data=f"aic:pe:c:{profile_id}:{profile_version}:{page}",
                ),
            ],
            [
                InlineKeyboardButton(
                    text=toggle_text,
                    callback_data=(
                        f"aic:pt:{profile_id}:{profile_version}:{toggle_target}:{page}"
                    ),
                ),
                InlineKeyboardButton(
                    text="🔄 Новый характер",
                    callback_data=f"aic:prc:{profile_id}:{profile_version}:{page}",
                ),
            ],
            [
                InlineKeyboardButton(
                    text="📜 История",
                    callback_data=f"aic:ph:{profile_id}:{page}",
                ),
                InlineKeyboardButton(
                    text="🗄 Архивировать",
                    callback_data=f"aic:pdc:{profile_id}:{profile_version}:{page}",
                ),
            ],
            [
                InlineKeyboardButton(
                    text="⬅️ К профилям",
                    callback_data=f"aic:profiles:{page}",
                )
            ],
            [InlineKeyboardButton(text="🏠 Главное меню", callback_data="main")],
        ]
    )


def ai_account_profile_input_keyboard(
    *,
    profile_id: int,
    page: int,
) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="❌ Отменить",
                    callback_data=f"aic:pr:{profile_id}:{page}",
                )
            ],
            [InlineKeyboardButton(text="🏠 Главное меню", callback_data="main")],
        ]
    )


def ai_account_profile_confirm_keyboard(
    *,
    action: str,
    profile_id: int,
    profile_version: int,
    page: int,
) -> InlineKeyboardMarkup:
    apply_prefix = {"regenerate": "pra", "retire": "pda"}.get(action)
    if apply_prefix is None:
        raise ValueError("Некорректное действие подтверждения профиля")
    action_text = "✅ Создать новый характер" if action == "regenerate" else "🗄 Архивировать"
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text=action_text,
                    callback_data=(
                        f"aic:{apply_prefix}:{profile_id}:{profile_version}:{page}"
                    ),
                )
            ],
            [
                InlineKeyboardButton(
                    text="❌ Отменить",
                    callback_data=f"aic:pr:{profile_id}:{page}",
                )
            ],
            [InlineKeyboardButton(text="🏠 Главное меню", callback_data="main")],
        ]
    )


def ai_account_profile_history_keyboard(
    *,
    profile_id: int,
    page: int,
) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="⬅️ К профилю",
                    callback_data=f"aic:pr:{profile_id}:{page}",
                )
            ],
            [
                InlineKeyboardButton(
                    text="⬅️ К профилям",
                    callback_data=f"aic:profiles:{page}",
                )
            ],
            [InlineKeyboardButton(text="🏠 Главное меню", callback_data="main")],
        ]
    )
