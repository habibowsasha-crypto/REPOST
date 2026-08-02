from __future__ import annotations

from collections.abc import Sequence

from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup

from .ai_channel_memory import AI_CHANNEL_PROFILE_FIELDS

AI_COMMENTS_CALLBACK_PREFIX = "aic:"
AI_COMMENTS_CHANNEL_PAGE_SIZE = 8

AI_COMMENTS_MENU_ITEMS: tuple[tuple[str, str], ...] = (
    ("📡 Выбрать канал", "aic:channels"),
    ("🧠 Память канала", "aic:memory"),
    ("🎭 Профили аккаунтов", "aic:profiles"),
    ("📚 Последние публикации", "aic:posts"),
    ("💬 История комментариев", "aic:history"),
    ("🧪 Проверка OpenAI", "aic:test"),
    ("🗣 Создать диалог", "aic:dialogue"),
    ("✅ Черновики", "aic:drafts"),
    ("⚙️ Настройки ИИ", "aic:settings"),
    ("📊 Статистика", "aic:stats"),
)

AI_COMMENTS_PLACEHOLDER_CALLBACKS = frozenset(
    {
        "aic:history",
        "aic:stats",
    }
)


def ai_comments_menu_keyboard() -> InlineKeyboardMarkup:
    rows = [
        [
            InlineKeyboardButton(text=left[0], callback_data=left[1]),
            InlineKeyboardButton(text=right[0], callback_data=right[1]),
        ]
        for left, right in zip(
            AI_COMMENTS_MENU_ITEMS[::2],
            AI_COMMENTS_MENU_ITEMS[1::2],
            strict=True,
        )
    ]
    rows.append(
        [InlineKeyboardButton(text="🏠 Главное меню", callback_data="main")]
    )
    return InlineKeyboardMarkup(inline_keyboard=rows)


def ai_comments_placeholder_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="⬅️ К комментариям", callback_data="aic:menu"
                )
            ],
            [InlineKeyboardButton(text="🏠 Главное меню", callback_data="main")],
        ]
    )


def ai_comments_gateway_keyboard(*, ready: bool) -> InlineKeyboardMarkup:
    label = "🔌 Выполнить DEV-проверку" if ready else "🔒 Проверка недоступна"
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text=label,
                    callback_data="aic:gw:confirm",
                )
            ],
            [
                InlineKeyboardButton(
                    text="⚙️ Настройки AI", callback_data="aic:settings"
                )
            ],
            [
                InlineKeyboardButton(
                    text="⬅️ К комментариям", callback_data="aic:menu"
                )
            ],
            [InlineKeyboardButton(text="🏠 Главное меню", callback_data="main")],
        ]
    )


def ai_comments_gateway_confirm_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="✅ Выполнить один DEV-запрос",
                    callback_data="aic:gw:run",
                )
            ],
            [
                InlineKeyboardButton(
                    text="❌ Отменить", callback_data="aic:gw:cancel"
                )
            ],
            [
                InlineKeyboardButton(
                    text="⬅️ К комментариям", callback_data="aic:menu"
                )
            ],
            [InlineKeyboardButton(text="🏠 Главное меню", callback_data="main")],
        ]
    )


def ai_comments_channel_list_keyboard(
    channels: Sequence[tuple[int, str, bool]],
    *,
    page: int,
    total_pages: int,
) -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = []
    for channel_id, title, active in channels:
        icon = "🟢" if active else "⚫️"
        rows.append(
            [
                InlineKeyboardButton(
                    text=f"{icon} {title}",
                    callback_data=f"aic:channel:{channel_id}:{page}",
                )
            ]
        )

    if total_pages > 1:
        navigation: list[InlineKeyboardButton] = []
        if page > 0:
            navigation.append(
                InlineKeyboardButton(
                    text="⬅️", callback_data=f"aic:channels:{page - 1}"
                )
            )
        navigation.append(
            InlineKeyboardButton(
                text=f"{page + 1}/{total_pages}",
                callback_data=f"aic:channels:{page}",
            )
        )
        if page + 1 < total_pages:
            navigation.append(
                InlineKeyboardButton(
                    text="➡️", callback_data=f"aic:channels:{page + 1}"
                )
            )
        rows.append(navigation)

    rows.extend(
        [
            [
                InlineKeyboardButton(
                    text="⬅️ К комментариям", callback_data="aic:menu"
                )
            ],
            [InlineKeyboardButton(text="🏠 Главное меню", callback_data="main")],
        ]
    )
    return InlineKeyboardMarkup(inline_keyboard=rows)


def ai_comments_channel_keyboard(*, page: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="🧠 Память канала",
                    callback_data="aic:memory",
                ),
                InlineKeyboardButton(
                    text="📚 Публикации",
                    callback_data="aic:posts",
                ),
            ],
            [
                InlineKeyboardButton(
                    text="🔄 Обновить 10 постов",
                    callback_data="aic:m:refresh",
                )
            ],
            [
                InlineKeyboardButton(
                    text="⬅️ К выбору канала",
                    callback_data=f"aic:channels:{page}",
                )
            ],
            [
                InlineKeyboardButton(
                    text="⬅️ К комментариям", callback_data="aic:menu"
                )
            ],
            [InlineKeyboardButton(text="🏠 Главное меню", callback_data="main")],
        ]
    )


def ai_comments_memory_keyboard(
    *,
    channel_id: int,
    profile_version: int,
    active_scenarios: Sequence[tuple[int, str]] = (),
) -> InlineKeyboardMarkup:
    field_buttons = [
        InlineKeyboardButton(
            text=spec.label,
            callback_data=(
                f"aic:m:e:{spec.code}:{channel_id}:{profile_version}"
            ),
        )
        for spec in AI_CHANNEL_PROFILE_FIELDS.values()
    ]
    rows = [
        field_buttons[index : index + 2]
        for index in range(0, len(field_buttons), 2)
    ]
    rows.extend(
        [
            [
                InlineKeyboardButton(
                    text="🔄 Обновить 10 постов",
                    callback_data="aic:m:refresh",
                )
            ],
            [
                InlineKeyboardButton(
                    text="🎯 Новый сценарий",
                    callback_data="aic:s:new",
                ),
                InlineKeyboardButton(
                    text="📚 Публикации",
                    callback_data="aic:posts",
                ),
            ],
        ]
    )
    for scenario_id, label in active_scenarios[:10]:
        rows.append(
            [
                InlineKeyboardButton(
                    text=f"✅ Завершить: {label}",
                    callback_data=f"aic:scf:{scenario_id}",
                )
            ]
        )
    rows.extend(
        [
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


def ai_comments_memory_input_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="❌ Отменить",
                    callback_data="aic:memory",
                )
            ],
            [InlineKeyboardButton(text="🏠 Главное меню", callback_data="main")],
        ]
    )


def ai_comments_recent_posts_keyboard(
    posts: Sequence[tuple[int, int, bool]],
    *,
    page: int,
    total_pages: int,
) -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = []
    for post_id, telegram_message_id, linked in posts:
        icon = "🎯" if linked else "📄"
        rows.append(
            [
                InlineKeyboardButton(
                    text=f"{icon} Пост #{telegram_message_id}",
                    callback_data=f"aic:p:{post_id}:{page}",
                )
            ]
        )
    if total_pages > 1:
        navigation: list[InlineKeyboardButton] = []
        if page > 0:
            navigation.append(
                InlineKeyboardButton(
                    text="⬅️",
                    callback_data=f"aic:posts:{page - 1}",
                )
            )
        navigation.append(
            InlineKeyboardButton(
                text=f"{page + 1}/{total_pages}",
                callback_data=f"aic:posts:{page}",
            )
        )
        if page + 1 < total_pages:
            navigation.append(
                InlineKeyboardButton(
                    text="➡️",
                    callback_data=f"aic:posts:{page + 1}",
                )
            )
        rows.append(navigation)
    rows.extend(
        [
            [
                InlineKeyboardButton(
                    text="🧠 Память канала",
                    callback_data="aic:memory",
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


def ai_comments_post_keyboard(
    *,
    post_id: int,
    page: int,
    active_scenarios: Sequence[tuple[int, str]],
    linked_scenario_id: int | None,
) -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = [
        [
            InlineKeyboardButton(
                text="✍️ Создать один черновик",
                callback_data=f"aic:g:start:{post_id}:{page}",
            )
        ],
        [
            InlineKeyboardButton(
                text="🗣 Создать связанный диалог",
                callback_data=f"aic:dlg:post:{post_id}",
            )
        ],
    ]
    for scenario_id, label in active_scenarios[:10]:
        icon = "✅" if scenario_id == linked_scenario_id else "🔗"
        rows.append(
            [
                InlineKeyboardButton(
                    text=f"{icon} {label}",
                    callback_data=f"aic:pl:{post_id}:{scenario_id}:{page}",
                )
            ]
        )
    rows.extend(
        [
            [
                InlineKeyboardButton(
                    text="⬅️ К публикациям",
                    callback_data=f"aic:posts:{page}",
                )
            ],
            [
                InlineKeyboardButton(
                    text="🧠 Память канала",
                    callback_data="aic:memory",
                )
            ],
            [InlineKeyboardButton(text="🏠 Главное меню", callback_data="main")],
        ]
    )
    return InlineKeyboardMarkup(inline_keyboard=rows)


def ai_comments_scenario_complete_keyboard(
    *,
    scenario_id: int,
) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="✅ Завершить сценарий",
                    callback_data=f"aic:sca:{scenario_id}",
                )
            ],
            [
                InlineKeyboardButton(
                    text="❌ Отменить",
                    callback_data="aic:memory",
                )
            ],
            [InlineKeyboardButton(text="🏠 Главное меню", callback_data="main")],
        ]
    )


def ai_comments_settings_keyboard(
    *,
    stored_enabled: bool,
    generation_stored_enabled: bool = False,
    dialogue_stored_enabled: bool = False,
    editable: bool = True,
) -> InlineKeyboardMarkup:
    def toggle_row(label_text: str, value: bool, *, code: str | None = None):
        target = 0 if value else 1
        icon = "🟢" if value else "🔴"
        label = "ВКЛ" if value else "ВЫКЛ"
        callback = (
            f"aic:flag:confirm:{target}"
            if code is None
            else f"aic:flag:confirm:{code}:{target}"
        )
        return [
            InlineKeyboardButton(
                text=f"{icon} {label_text}: {label}",
                callback_data=callback,
            )
        ]

    if editable:
        rows = [
            toggle_row("AI Comments", stored_enabled),
            toggle_row("Генерация черновика", generation_stored_enabled, code="g"),
            toggle_row("Связанные диалоги", dialogue_stored_enabled, code="d"),
        ]
    else:
        rows = [[
            InlineKeyboardButton(
                text="⚠️ Повторить проверку", callback_data="aic:settings"
            )
        ]]
    rows.extend(
        [
            [
                InlineKeyboardButton(
                    text="⬅️ К комментариям", callback_data="aic:menu"
                )
            ],
            [InlineKeyboardButton(text="🏠 Главное меню", callback_data="main")],
        ]
    )
    return InlineKeyboardMarkup(inline_keyboard=rows)


def ai_comments_flag_confirm_keyboard(
    *,
    target_enabled: bool,
    flag_code: str | None = None,
) -> InlineKeyboardMarkup:
    target = 1 if target_enabled else 0
    action = "Включить" if target_enabled else "Выключить"
    callback = (
        f"aic:flag:set:{target}"
        if flag_code is None
        else f"aic:flag:set:{flag_code}:{target}"
    )
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text=f"✅ {action}", callback_data=callback
                )
            ],
            [InlineKeyboardButton(text="❌ Отменить", callback_data="aic:settings")],
            [
                InlineKeyboardButton(
                    text="⬅️ К комментариям", callback_data="aic:menu"
                )
            ],
            [InlineKeyboardButton(text="🏠 Главное меню", callback_data="main")],
        ]
    )


def ai_comment_profile_selection_keyboard(
    profiles: Sequence[tuple[int, str, bool]],
    *,
    post_id: int,
    page: int,
) -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = []
    for profile_id, label, ready in profiles[:20]:
        icon = "🟢" if ready else "🟡"
        rows.append(
            [
                InlineKeyboardButton(
                    text=f"{icon} {label}",
                    callback_data=f"aic:g:pr:{post_id}:{profile_id}:{page}",
                )
            ]
        )
    rows.extend(
        [
            [
                InlineKeyboardButton(
                    text="⬅️ К публикации",
                    callback_data=f"aic:p:{post_id}:{page}",
                )
            ],
            [InlineKeyboardButton(text="🏠 Главное меню", callback_data="main")],
        ]
    )
    return InlineKeyboardMarkup(inline_keyboard=rows)


def ai_comment_generation_confirm_keyboard(
    *,
    post_id: int,
    profile_id: int,
    page: int,
) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="✅ Создать один черновик",
                    callback_data="aic:g:run",
                )
            ],
            [
                InlineKeyboardButton(
                    text="🎭 Сменить профиль",
                    callback_data=f"aic:g:start:{post_id}:{page}",
                )
            ],
            [
                InlineKeyboardButton(
                    text="❌ Отменить",
                    callback_data=f"aic:p:{post_id}:{page}",
                )
            ],
            [InlineKeyboardButton(text="🏠 Главное меню", callback_data="main")],
        ]
    )


def ai_comment_generation_wait_keyboard(*, post_id: int, page: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="⏳ Генерация выполняется", callback_data="noop"
                )
            ],
            [
                InlineKeyboardButton(
                    text="⬅️ К публикации",
                    callback_data=f"aic:p:{post_id}:{page}",
                )
            ],
        ]
    )


def ai_comment_generation_result_keyboard(
    *,
    draft_id: int,
    post_id: int,
    page: int,
) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="📄 Открыть запись",
                    callback_data=f"aic:d:{draft_id}",
                )
            ],
            [InlineKeyboardButton(text="✅ Все черновики", callback_data="aic:drafts")],
            [
                InlineKeyboardButton(
                    text="⬅️ К публикации",
                    callback_data=f"aic:p:{post_id}:{page}",
                )
            ],
            [InlineKeyboardButton(text="🏠 Главное меню", callback_data="main")],
        ]
    )


def ai_comment_drafts_keyboard(
    drafts: Sequence[tuple[int, str]],
) -> InlineKeyboardMarkup:
    rows = [
        [InlineKeyboardButton(text=label, callback_data=f"aic:d:{draft_id}")]
        for draft_id, label in drafts[:20]
    ]
    rows.extend(
        [
            [InlineKeyboardButton(text="🔄 Обновить", callback_data="aic:drafts")],
            [
                InlineKeyboardButton(
                    text="⬅️ К комментариям", callback_data="aic:menu"
                )
            ],
            [InlineKeyboardButton(text="🏠 Главное меню", callback_data="main")],
        ]
    )
    return InlineKeyboardMarkup(inline_keyboard=rows)


def ai_comment_draft_keyboard(
    *,
    post_id: int | None,
) -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = [
        [InlineKeyboardButton(text="✅ Все черновики", callback_data="aic:drafts")]
    ]
    if post_id is not None:
        rows.append(
            [
                InlineKeyboardButton(
                    text="📚 К публикациям", callback_data="aic:posts"
                )
            ]
        )
    rows.extend(
        [
            [
                InlineKeyboardButton(
                    text="⬅️ К комментариям", callback_data="aic:menu"
                )
            ],
            [InlineKeyboardButton(text="🏠 Главное меню", callback_data="main")],
        ]
    )
    return InlineKeyboardMarkup(inline_keyboard=rows)


def ai_dialogues_menu_keyboard(
    threads: Sequence[tuple[int, str]],
    *,
    has_channel: bool,
) -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = []
    for thread_id, label in threads[:20]:
        rows.append(
            [InlineKeyboardButton(text=label, callback_data=f"aic:dlg:thread:{thread_id}")]
        )
    if has_channel:
        rows.append(
            [InlineKeyboardButton(text="➕ Новый диалог", callback_data="aic:dlg:new")]
        )
    else:
        rows.append(
            [InlineKeyboardButton(text="📡 Выбрать канал", callback_data="aic:channels")]
        )
    rows.extend(
        [
            [InlineKeyboardButton(text="⬅️ К комментариям", callback_data="aic:menu")],
            [InlineKeyboardButton(text="🏠 Главное меню", callback_data="main")],
        ]
    )
    return InlineKeyboardMarkup(inline_keyboard=rows)


def ai_dialogue_post_selection_keyboard(
    posts: Sequence[tuple[int, str]],
) -> InlineKeyboardMarkup:
    rows = [
        [InlineKeyboardButton(text=label, callback_data=f"aic:dlg:post:{post_id}")]
        for post_id, label in posts[:10]
    ]
    rows.extend(
        [
            [InlineKeyboardButton(text="⬅️ К диалогам", callback_data="aic:dialogue")],
            [InlineKeyboardButton(text="🏠 Главное меню", callback_data="main")],
        ]
    )
    return InlineKeyboardMarkup(inline_keyboard=rows)


def ai_dialogue_profile_selection_keyboard(
    profiles: Sequence[tuple[int, str, bool, bool]],
    *,
    selected_ids: Sequence[int],
    max_messages: int,
) -> InlineKeyboardMarkup:
    selected = set(selected_ids)
    rows: list[list[InlineKeyboardButton]] = []
    for profile_id, label, ready, has_capacity in profiles[:30]:
        icon = "✅" if profile_id in selected else ("🟢" if ready and has_capacity else "🔴")
        rows.append(
            [
                InlineKeyboardButton(
                    text=f"{icon} {label}",
                    callback_data=f"aic:dlg:profile:{profile_id}",
                )
            ]
        )
    rows.append(
        [
            InlineKeyboardButton(
                text=("✅ " if max_messages == count else "") + f"{count} репл.",
                callback_data=f"aic:dlg:max:{count}",
            )
            for count in range(2, 6)
        ]
    )
    rows.extend(
        [
            [InlineKeyboardButton(text="✅ Создать план", callback_data="aic:dlg:create")],
            [InlineKeyboardButton(text="⬅️ К публикациям", callback_data="aic:dlg:new")],
            [InlineKeyboardButton(text="🏠 Главное меню", callback_data="main")],
        ]
    )
    return InlineKeyboardMarkup(inline_keyboard=rows)


def ai_dialogue_thread_keyboard(
    *,
    thread_id: int,
    status: str,
    pending_draft_id: int | None,
) -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = []
    if status == "planned":
        rows.append(
            [
                InlineKeyboardButton(
                    text="✍️ Создать следующую реплику",
                    callback_data=f"aic:dlg:next:{thread_id}",
                )
            ]
        )
    if status == "review" and pending_draft_id is not None:
        rows.append(
            [
                InlineKeyboardButton(
                    text="🔎 Проверить реплику",
                    callback_data=f"aic:dlg:review:{thread_id}:{pending_draft_id}",
                )
            ]
        )
    if status not in {"completed", "cancelled"}:
        rows.append(
            [
                InlineKeyboardButton(
                    text="⛔ Отменить диалог",
                    callback_data=f"aic:dlg:cancel:{thread_id}",
                )
            ]
        )
    rows.extend(
        [
            [InlineKeyboardButton(text="⬅️ К диалогам", callback_data="aic:dialogue")],
            [InlineKeyboardButton(text="🏠 Главное меню", callback_data="main")],
        ]
    )
    return InlineKeyboardMarkup(inline_keyboard=rows)


def ai_dialogue_generation_confirm_keyboard(*, thread_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="✅ Создать реплику", callback_data="aic:dlg:run")],
            [
                InlineKeyboardButton(
                    text="❌ Отменить",
                    callback_data=f"aic:dlg:thread:{thread_id}",
                )
            ],
            [InlineKeyboardButton(text="🏠 Главное меню", callback_data="main")],
        ]
    )


def ai_dialogue_review_keyboard(*, thread_id: int, draft_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="✅ Принять в план",
                    callback_data=f"aic:dlg:accept:{thread_id}:{draft_id}",
                )
            ],
            [
                InlineKeyboardButton(
                    text="🗑 Отклонить и создать заново",
                    callback_data=f"aic:dlg:reject:{thread_id}:{draft_id}",
                )
            ],
            [
                InlineKeyboardButton(
                    text="⬅️ К диалогу",
                    callback_data=f"aic:dlg:thread:{thread_id}",
                )
            ],
            [InlineKeyboardButton(text="🏠 Главное меню", callback_data="main")],
        ]
    )


def ai_dialogue_cancel_confirm_keyboard(*, thread_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="⛔ Да, отменить диалог",
                    callback_data=f"aic:dlg:canceldo:{thread_id}",
                )
            ],
            [
                InlineKeyboardButton(
                    text="❌ Не отменять",
                    callback_data=f"aic:dlg:thread:{thread_id}",
                )
            ],
            [InlineKeyboardButton(text="🏠 Главное меню", callback_data="main")],
        ]
    )
