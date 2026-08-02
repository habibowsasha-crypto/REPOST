from __future__ import annotations

from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup

from .channel_profiles import CHANNEL_PROFILES, CUSTOM_PROFILE_KEY
from .models import Channel


def main_menu() -> InlineKeyboardMarkup:
    """Compact two-column navigation used on the main LikeBot screen."""
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="➕ Добавить аккаунт", callback_data="account:add"),
                InlineKeyboardButton(text="👥 Мои аккаунты", callback_data="account:list"),
            ],
            [
                InlineKeyboardButton(text="📢 Каналы", callback_data="channel:list"),
                InlineKeyboardButton(text="👥 Группы", callback_data="group:list"),
            ],
            [
                InlineKeyboardButton(text="❤️ Авто лайк", callback_data="autolike:list"),
                InlineKeyboardButton(text="😀 Реакции", callback_data="settings:reactions"),
            ],
            [
                InlineKeyboardButton(text="⏱ Задержка", callback_data="settings:delay"),
                InlineKeyboardButton(text="📊 Статистика", callback_data="stats"),
            ],
            [InlineKeyboardButton(text="⚙️ Настройки", callback_data="settings:menu")],
            [InlineKeyboardButton(text="💬 Комментарии", callback_data="aic:menu")],
        ]
    )


def system_health_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="👥 Здоровье аккаунтов", callback_data="system:accounts"
                ),
                InlineKeyboardButton(
                    text="📊 Расширенная аналитика", callback_data="system:statistics"
                ),
            ],
            [InlineKeyboardButton(text="🔄 Обновить", callback_data="system:refresh")],
            [InlineKeyboardButton(text="🏠 Главное меню", callback_data="main")],
        ]
    )


def account_health_list_keyboard(
    accounts: list[tuple[int, str, int, str]],
    *,
    page: int = 0,
    total_pages: int = 1,
) -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = []
    for account_id, label, score, icon in accounts:
        rows.append(
            [
                InlineKeyboardButton(
                    text=f"{icon} {score} · {label}",
                    callback_data=f"system:account:{account_id}:{page}",
                )
            ]
        )
    if total_pages > 1:
        navigation: list[InlineKeyboardButton] = []
        if page > 0:
            navigation.append(
                InlineKeyboardButton(
                    text="⬅️", callback_data=f"system:accounts_page:{page - 1}"
                )
            )
        navigation.append(
            InlineKeyboardButton(text=f"{page + 1}/{total_pages}", callback_data="noop")
        )
        if page + 1 < total_pages:
            navigation.append(
                InlineKeyboardButton(
                    text="➡️", callback_data=f"system:accounts_page:{page + 1}"
                )
            )
        rows.append(navigation)
    refresh_callback = f"system:accounts_page:{page}" if page else "system:accounts"
    rows.append([InlineKeyboardButton(text="🔄 Обновить", callback_data=refresh_callback)])
    rows.append([InlineKeyboardButton(text="⬅️ К состоянию системы", callback_data="stats")])
    rows.append([InlineKeyboardButton(text="🏠 Главное меню", callback_data="main")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def account_health_detail_keyboard(account_id: int, *, page: int = 0) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="🔄 Обновить",
                    callback_data=f"system:account:{account_id}:{page}",
                )
            ],
            [
                InlineKeyboardButton(
                    text="👤 Открыть карточку", callback_data=f"account:view:{account_id}"
                )
            ],
            [
                InlineKeyboardButton(
                    text="⬅️ К здоровью аккаунтов",
                    callback_data=(
                        f"system:accounts_page:{page}" if page else "system:accounts"
                    ),
                )
            ],
            [InlineKeyboardButton(text="🏠 Главное меню", callback_data="main")],
        ]
    )


def statistics_keyboard(period: str = "day") -> InlineKeyboardMarkup:
    labels = {
        "day": "24 часа",
        "week": "7 дней",
        "month": "30 дней",
        "all": "Всё время",
    }
    selected = period if period in labels else "day"

    def button(key: str) -> InlineKeyboardButton:
        prefix = "✅ " if key == selected else ""
        return InlineKeyboardButton(
            text=f"{prefix}{labels[key]}",
            callback_data=f"analytics:period:{key}",
        )

    return InlineKeyboardMarkup(
        inline_keyboard=[
            [button("day"), button("week")],
            [button("month"), button("all")],
            [
                InlineKeyboardButton(
                    text="👥 Рейтинг аккаунтов",
                    callback_data=f"analytics:accounts:{selected}",
                ),
                InlineKeyboardButton(
                    text="📢 Рейтинг целей",
                    callback_data=f"analytics:targets:{selected}",
                ),
            ],
            [
                InlineKeyboardButton(
                    text="📥 Скачать CSV",
                    callback_data=f"analytics:export:{selected}",
                )
            ],
            [
                InlineKeyboardButton(
                    text="🔄 Обновить", callback_data=f"analytics:period:{selected}"
                )
            ],
            [InlineKeyboardButton(text="⬅️ К состоянию системы", callback_data="stats")],
            [InlineKeyboardButton(text="🏠 Главное меню", callback_data="main")],
        ]
    )


def analytics_ranking_keyboard(period: str) -> InlineKeyboardMarkup:
    selected = period if period in {"day", "week", "month", "all"} else "day"
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="⬅️ К аналитике",
                    callback_data=f"analytics:period:{selected}",
                )
            ],
            [
                InlineKeyboardButton(
                    text="📥 Скачать CSV",
                    callback_data=f"analytics:export:{selected}",
                )
            ],
            [InlineKeyboardButton(text="🏠 Главное меню", callback_data="main")],
        ]
    )


def back_main() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[[InlineKeyboardButton(text="🏠 Главное меню", callback_data="main")]]
    )


def settings_overview_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="😀 Реакции по умолчанию", callback_data="settings:reactions"),
                InlineKeyboardButton(text="⏱ Задержка", callback_data="settings:delay"),
            ],
            [InlineKeyboardButton(text="💾 Резервные копии", callback_data="backup:menu")],
            [InlineKeyboardButton(text="🏠 Главное меню", callback_data="main")],
        ]
    )



def backup_menu_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="📤 Скачать копию", callback_data="backup:export")],
            [InlineKeyboardButton(text="📥 Восстановить настройки", callback_data="backup:restore")],
            [InlineKeyboardButton(text="🕘 История операций", callback_data="backup:history")],
            [InlineKeyboardButton(text="⚙️ К настройкам", callback_data="settings:menu")],
            [InlineKeyboardButton(text="🏠 Главное меню", callback_data="main")],
        ]
    )


def backup_restore_cancel_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="❌ Отменить", callback_data="backup:cancel")],
            [InlineKeyboardButton(text="⬅️ К резервным копиям", callback_data="backup:menu")],
        ]
    )


def backup_restore_confirm_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="✅ Восстановить настройки", callback_data="backup:restore_apply")],
            [InlineKeyboardButton(text="❌ Отменить", callback_data="backup:cancel")],
        ]
    )


def backup_history_keyboard(
    events: list[tuple[int, str]],
) -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = []
    for event_id, label in events:
        rows.append(
            [
                InlineKeyboardButton(
                    text=label, callback_data=f"backup:event:{event_id}"
                )
            ]
        )
    rows.append([InlineKeyboardButton(text="🔄 Обновить", callback_data="backup:history")])
    rows.append([InlineKeyboardButton(text="⬅️ К резервным копиям", callback_data="backup:menu")])
    rows.append([InlineKeyboardButton(text="🏠 Главное меню", callback_data="main")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def backup_event_keyboard(event_id: int, *, can_rollback: bool) -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = []
    if can_rollback:
        rows.append(
            [
                InlineKeyboardButton(
                    text="↩️ Откатить к этому снимку",
                    callback_data=f"backup:rollback_confirm:{event_id}",
                )
            ]
        )
    rows.append([InlineKeyboardButton(text="⬅️ К истории", callback_data="backup:history")])
    rows.append([InlineKeyboardButton(text="🏠 Главное меню", callback_data="main")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def backup_rollback_confirm_keyboard(event_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="✅ Выполнить откат",
                    callback_data=f"backup:rollback_apply:{event_id}",
                )
            ],
            [InlineKeyboardButton(text="❌ Отменить", callback_data=f"backup:event:{event_id}")],
        ]
    )

def reactions_overview_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="✏️ Изменить реакции по умолчанию",
                    callback_data="settings:reactions:edit",
                )
            ],
            [InlineKeyboardButton(text="⚙️ К настройкам", callback_data="settings:menu")],
            [InlineKeyboardButton(text="🏠 Главное меню", callback_data="main")],
        ]
    )


def delay_overview_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="❤️ Задержка реакций", callback_data="settings:delay:reactions")],
            [InlineKeyboardButton(text="📢 Задержка подписки", callback_data="settings:delay:membership")],
            [InlineKeyboardButton(text="⚙️ К настройкам", callback_data="settings:menu")],
            [InlineKeyboardButton(text="🏠 Главное меню", callback_data="main")],
        ]
    )


def reaction_delay_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="✏️ Изменить задержку реакций",
                    callback_data="settings:delay:reactions:edit",
                )
            ],
            [
                InlineKeyboardButton(
                    text="🔄 Пересчитать текущую очередь",
                    callback_data="settings:delay:reactions:reschedule",
                )
            ],
            [InlineKeyboardButton(text="⬅️ К задержкам", callback_data="settings:delay")],
            [InlineKeyboardButton(text="🏠 Главное меню", callback_data="main")],
        ]
    )


def membership_delay_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="✏️ Изменить задержку подписки",
                    callback_data="settings:delay:membership:edit",
                )
            ],
            [
                InlineKeyboardButton(
                    text="🔄 Пересчитать текущую очередь",
                    callback_data="settings:delay:membership:reschedule",
                )
            ],
            [InlineKeyboardButton(text="⬅️ К задержкам", callback_data="settings:delay")],
            [InlineKeyboardButton(text="🏠 Главное меню", callback_data="main")],
        ]
    )


def reaction_delay_reschedule_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="✅ Пересчитать текущую очередь",
                    callback_data="settings:delay:reactions:reschedule",
                )
            ],
            [
                InlineKeyboardButton(
                    text="➡️ Только для новых задач",
                    callback_data="settings:delay:reactions:keep",
                )
            ],
            [InlineKeyboardButton(text="⬅️ К задержкам", callback_data="settings:delay")],
        ]
    )


def membership_delay_reschedule_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="✅ Пересчитать текущую очередь",
                    callback_data="settings:delay:membership:reschedule",
                )
            ],
            [
                InlineKeyboardButton(
                    text="➡️ Только для новых задач",
                    callback_data="settings:delay:membership:keep",
                )
            ],
            [InlineKeyboardButton(text="⬅️ К задержкам", callback_data="settings:delay")],
        ]
    )


def login_code_actions() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="🔄 Отправить код повторно", callback_data="account:resend_code")],
            [InlineKeyboardButton(text="📱 Ввести другой номер", callback_data="account:restart_phone")],
            [InlineKeyboardButton(text="🏠 Главное меню", callback_data="main")],
        ]
    )


def account_email_confirm_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="✅ Подтвердить и запросить код",
                    callback_data="account:email_confirm",
                )
            ],
            [InlineKeyboardButton(text="✏️ Изменить почту", callback_data="account:email_change")],
            [InlineKeyboardButton(text="📱 Ввести другой номер", callback_data="account:restart_phone")],
            [InlineKeyboardButton(text="🏠 Главное меню", callback_data="main")],
        ]
    )


def account_list_keyboard(
    accounts: list[tuple[int, str, bool]],
    *,
    problem_count: int = 0,
    missing_email_count: int = 0,
    page: int = 0,
    total_pages: int = 1,
) -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = []
    for account_id, label, active in accounts:
        icon = "🟢" if active else "⚫️"
        rows.append(
            [
                InlineKeyboardButton(
                    text=f"{icon} {label}",
                    callback_data=f"account:view:{account_id}",
                )
            ]
        )
    if problem_count:
        rows.append(
            [
                InlineKeyboardButton(
                    text=f"⚠️ Проблемные аккаунты — {problem_count}",
                    callback_data="account:problems",
                )
            ]
        )
    if missing_email_count:
        rows.append(
            [
                InlineKeyboardButton(
                    text=f"📧 Без указанной почты — {missing_email_count}",
                    callback_data="account:email_missing",
                )
            ]
        )
    if total_pages > 1:
        navigation: list[InlineKeyboardButton] = []
        if page > 0:
            navigation.append(
                InlineKeyboardButton(
                    text="⬅️", callback_data=f"account:list_page:{page - 1}"
                )
            )
        navigation.append(
            InlineKeyboardButton(text=f"{page + 1}/{total_pages}", callback_data="noop")
        )
        if page + 1 < total_pages:
            navigation.append(
                InlineKeyboardButton(
                    text="➡️", callback_data=f"account:list_page:{page + 1}"
                )
            )
        rows.append(navigation)
    refresh_callback = f"account:refresh_page:{page}" if page else "account:refresh"
    rows.append([InlineKeyboardButton(text="🔄 Обновить", callback_data=refresh_callback)])
    rows.append(
        [
            InlineKeyboardButton(
                text="🔎 Поиск, фильтры и массовые действия",
                callback_data="manage:a:all:0",
            )
        ]
    )
    rows.append([InlineKeyboardButton(text="➕ Добавить аккаунт", callback_data="account:add")])
    rows.append([InlineKeyboardButton(text="🏠 Главное меню", callback_data="main")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def missing_email_account_list_keyboard(
    accounts: list[tuple[int, str]],
    *,
    page: int = 0,
    total_pages: int = 1,
) -> InlineKeyboardMarkup:
    rows = [
        [
            InlineKeyboardButton(
                text=f"📧 {label}", callback_data=f"account:email_edit:{account_id}"
            )
        ]
        for account_id, label in accounts
    ]
    if total_pages > 1:
        navigation: list[InlineKeyboardButton] = []
        if page > 0:
            navigation.append(
                InlineKeyboardButton(
                    text="⬅️",
                    callback_data=f"account:email_missing_page:{page - 1}",
                )
            )
        navigation.append(
            InlineKeyboardButton(text=f"{page + 1}/{total_pages}", callback_data="noop")
        )
        if page + 1 < total_pages:
            navigation.append(
                InlineKeyboardButton(
                    text="➡️",
                    callback_data=f"account:email_missing_page:{page + 1}",
                )
            )
        rows.append(navigation)
    rows.append([InlineKeyboardButton(text="⬅️ К аккаунтам", callback_data="account:list")])
    rows.append([InlineKeyboardButton(text="🏠 Главное меню", callback_data="main")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def problem_account_list_keyboard(
    accounts: list[tuple[int, str]],
    *,
    page: int = 0,
    total_pages: int = 1,
) -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = [
        [
            InlineKeyboardButton(
                text=f"⚠️ {label}", callback_data=f"account:problem_view:{account_id}"
            )
        ]
        for account_id, label in accounts
    ]
    if total_pages > 1:
        navigation: list[InlineKeyboardButton] = []
        if page > 0:
            navigation.append(
                InlineKeyboardButton(
                    text="⬅️", callback_data=f"account:problems_page:{page - 1}"
                )
            )
        navigation.append(
            InlineKeyboardButton(text=f"{page + 1}/{total_pages}", callback_data="noop")
        )
        if page + 1 < total_pages:
            navigation.append(
                InlineKeyboardButton(
                    text="➡️", callback_data=f"account:problems_page:{page + 1}"
                )
            )
        rows.append(navigation)
    if accounts:
        rows.append([InlineKeyboardButton(text="🔄 Проверить все", callback_data="account:problems_check")])
        rows.append(
            [
                InlineKeyboardButton(
                    text="🧹 Очистить список",
                    callback_data="account:problems_clear_confirm",
                )
            ]
        )
    rows.append([InlineKeyboardButton(text="⬅️ К аккаунтам", callback_data="account:list")])
    rows.append([InlineKeyboardButton(text="🏠 Главное меню", callback_data="main")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def confirm_problem_accounts_clear(count: int) -> InlineKeyboardMarkup:
    """Require an explicit second click before destructive bulk deletion."""

    safe_count = max(0, int(count))
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text=f"🗑 Да, удалить {safe_count}",
                    callback_data="account:problems_clear",
                )
            ],
            [
                InlineKeyboardButton(
                    text="⬅️ Отмена",
                    callback_data="account:problems",
                )
            ],
        ]
    )


def account_actions(
    account_id: int,
    active: bool,
    *,
    has_email: bool = False,
) -> InlineKeyboardMarkup:
    toggle_text = "⛔ Выключить" if active else "✅ Включить"
    email_text = "📧 Изменить почту" if has_email else "📧 Добавить почту"
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text=email_text, callback_data=f"account:email:{account_id}")],
            [
                InlineKeyboardButton(
                    text="🔐 Получить код входа",
                    callback_data=f"account:login_code:{account_id}",
                )
            ],
            [
                InlineKeyboardButton(
                    text="🩺 Проверить сессию",
                    callback_data=f"account:session_check:{account_id}",
                )
            ],
            [InlineKeyboardButton(text=toggle_text, callback_data=f"account:toggle:{account_id}")],
            [InlineKeyboardButton(text="🗑 Удалить аккаунт", callback_data=f"account:delete_confirm:{account_id}")],
            [InlineKeyboardButton(text="⬅️ К аккаунтам", callback_data="account:list")],
            [InlineKeyboardButton(text="🏠 Главное меню", callback_data="main")],
        ]
    )


def problem_account_actions(account_id: int, *, has_email: bool = False) -> InlineKeyboardMarkup:
    email_text = "📧 Изменить почту" if has_email else "📧 Добавить почту"
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text=email_text, callback_data=f"account:email:{account_id}")],
            [
                InlineKeyboardButton(
                    text="🔄 Проверить повторно",
                    callback_data=f"account:problem_check:{account_id}",
                )
            ],
            [
                InlineKeyboardButton(
                    text="🔐 Авторизовать заново",
                    callback_data=f"account:reauth:{account_id}",
                )
            ],
            [
                InlineKeyboardButton(
                    text="🗑 Удалить окончательно",
                    callback_data=f"account:delete_confirm:{account_id}",
                )
            ],
            [InlineKeyboardButton(text="⬅️ К проблемным", callback_data="account:problems")],
            [InlineKeyboardButton(text="🏠 Главное меню", callback_data="main")],
        ]
    )


def account_email_management_keyboard(account_id: int, *, has_email: bool) -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = [
        [
            InlineKeyboardButton(
                text="✏️ Изменить адрес" if has_email else "➕ Добавить адрес",
                callback_data=f"account:email_edit:{account_id}",
            )
        ]
    ]
    if has_email:
        rows.append(
            [
                InlineKeyboardButton(
                    text="📝 Изменить примечание",
                    callback_data=f"account:email_note:{account_id}",
                )
            ]
        )
        rows.append(
            [
                InlineKeyboardButton(
                    text="🗑 Удалить почту",
                    callback_data=f"account:email_delete_confirm:{account_id}",
                )
            ]
        )
    rows.append([InlineKeyboardButton(text="⬅️ К аккаунту", callback_data=f"account:view:{account_id}")])
    rows.append([InlineKeyboardButton(text="🏠 Главное меню", callback_data="main")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def confirm_account_email_delete(account_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="🗑 Да, удалить почту",
                    callback_data=f"account:email_delete:{account_id}",
                )
            ],
            [InlineKeyboardButton(text="⬅️ Отмена", callback_data=f"account:email:{account_id}")],
            [InlineKeyboardButton(text="🏠 Главное меню", callback_data="main")],
        ]
    )


def account_login_code_prompt_keyboard(account_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="🔄 Проверить новые сообщения",
                    callback_data=f"account:login_code_check:{account_id}",
                )
            ],
            [InlineKeyboardButton(text="⬅️ К аккаунту", callback_data=f"account:view:{account_id}")],
            [InlineKeyboardButton(text="🏠 Главное меню", callback_data="main")],
        ]
    )


def account_login_code_result_keyboard(account_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="🔄 Проверить ещё раз",
                    callback_data=f"account:login_code_check:{account_id}",
                )
            ],
            [InlineKeyboardButton(text="⬅️ К аккаунту", callback_data=f"account:view:{account_id}")],
            [InlineKeyboardButton(text="🏠 Главное меню", callback_data="main")],
        ]
    )


def confirm_account_delete(account_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="🗑 Да, удалить", callback_data=f"account:delete:{account_id}")],
            [InlineKeyboardButton(text="⬅️ Отмена", callback_data=f"account:view:{account_id}")],
        ]
    )


def channel_list_keyboard(channels: list[Channel], prefix: str = "channel:view") -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = []
    for channel in channels:
        icon = "🟢" if channel.is_active else "⚫️"
        rows.append(
            [InlineKeyboardButton(text=f"{icon} {channel.title}", callback_data=f"{prefix}:{channel.id}")]
        )
    if prefix == "channel:view":
        rows.append(
            [
                InlineKeyboardButton(
                    text="🔎 Поиск и фильтры",
                    callback_data="manage:t:c:all:0",
                )
            ]
        )
        rows.append([InlineKeyboardButton(text="➕ Добавить канал", callback_data="channel:add")])
    rows.append([InlineKeyboardButton(text="🏠 Главное меню", callback_data="main")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def channel_actions(
    channel_id: int,
    *,
    connectable_count: int | None = None,
    pending_count: int = 0,
    view_batch_id: int | None = None,
    view_pending_count: int = 0,
    profile_text: str = "⚙️ Свой",
) -> InlineKeyboardMarkup:
    if connectable_count is None:
        connect_text = "➕ Подключить аккаунты"
    elif connectable_count > 0:
        connect_text = f"➕ Подключить аккаунты: {connectable_count}"
    elif pending_count > 0:
        connect_text = f"⏳ Подключение аккаунтов: {pending_count}"
    else:
        connect_text = "✅ Все аккаунты подключены"
    if view_batch_id is not None and view_pending_count > 0:
        view_text = f"⏳ Просмотры: {view_pending_count} в очереди"
        view_callback = f"channel:views_batch:{view_batch_id}"
    else:
        view_text = "👁 Добавить просмотры"
        view_callback = f"channel:views:{channel_id}"
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="❤️ Авто лайк", callback_data=f"autolike:view:{channel_id}")],
            [
                InlineKeyboardButton(
                    text=f"🎚 Профиль: {profile_text}",
                    callback_data=f"channel:profile:{channel_id}",
                )
            ],
            [
                InlineKeyboardButton(text="😀 Реакции канала", callback_data=f"channel:reactions:{channel_id}"),
                InlineKeyboardButton(text="🖼 Типы постов", callback_data=f"channel:post_types:{channel_id}"),
            ],
            [InlineKeyboardButton(text=view_text, callback_data=view_callback)],
            [
                InlineKeyboardButton(
                    text="⏱ Период реакций",
                    callback_data=f"channel:reaction_window:{channel_id}",
                )
            ],
            [InlineKeyboardButton(text="🛡 Пауза аккаунта", callback_data="settings:delay:reactions")],
            [
                InlineKeyboardButton(text="👥 Аккаунты", callback_data=f"channel:members:{channel_id}"),
                InlineKeyboardButton(text="📊 Статистика", callback_data=f"channel:stats:{channel_id}"),
            ],
            [
                InlineKeyboardButton(
                    text=connect_text,
                    callback_data=f"channel:connect:{channel_id}",
                )
            ],
            [
                InlineKeyboardButton(
                    text="📋 Копировать настройки",
                    callback_data=f"channel:copy:{channel_id}:0",
                )
            ],
            [InlineKeyboardButton(text="🗑 Удалить канал", callback_data=f"channel:delete_confirm:{channel_id}")],
            [InlineKeyboardButton(text="⬅️ К каналам", callback_data="channel:list")],
            [InlineKeyboardButton(text="🏠 Главное меню", callback_data="main")],
        ]
    )


def channel_profile_keyboard(
    channel_id: int, *, current_profile_key: str
) -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = []
    for profile in CHANNEL_PROFILES:
        prefix = "✅ " if current_profile_key == profile.key else ""
        rows.append(
            [
                InlineKeyboardButton(
                    text=f"{prefix}{profile.emoji} {profile.title}",
                    callback_data=(
                        f"channel:profile_select:{channel_id}:{profile.key}"
                    ),
                )
            ]
        )
    custom_prefix = "✅ " if current_profile_key == CUSTOM_PROFILE_KEY else ""
    rows.extend(
        [
            [
                InlineKeyboardButton(
                    text=f"{custom_prefix}⚙️ Настроить вручную",
                    callback_data=f"autolike:view:{channel_id}",
                )
            ],
            [
                InlineKeyboardButton(
                    text="⬅️ К каналу", callback_data=f"channel:view:{channel_id}"
                )
            ],
            [InlineKeyboardButton(text="🏠 Главное меню", callback_data="main")],
        ]
    )
    return InlineKeyboardMarkup(inline_keyboard=rows)


def channel_profile_confirm_keyboard(
    channel_id: int, profile_key: str
) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="✅ Применить профиль",
                    callback_data=(
                        f"channel:profile_apply:{channel_id}:{profile_key}"
                    ),
                )
            ],
            [
                InlineKeyboardButton(
                    text="⬅️ К профилям", callback_data=f"channel:profile:{channel_id}"
                )
            ],
            [InlineKeyboardButton(text="🏠 Главное меню", callback_data="main")],
        ]
    )


def channel_views_setup_keyboard(channel_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="5 последних",
                    callback_data=f"channel:views_posts:{channel_id}:5",
                ),
                InlineKeyboardButton(
                    text="20 последних",
                    callback_data=f"channel:views_posts:{channel_id}:20",
                ),
            ],
            [
                InlineKeyboardButton(
                    text="50 последних",
                    callback_data=f"channel:views_posts:{channel_id}:50",
                ),
                InlineKeyboardButton(
                    text="100 последних",
                    callback_data=f"channel:views_posts:{channel_id}:100",
                ),
            ],
            [InlineKeyboardButton(text="⬅️ К каналу", callback_data=f"channel:view:{channel_id}")],
            [InlineKeyboardButton(text="🏠 Главное меню", callback_data="main")],
        ]
    )


def channel_views_confirm_keyboard(
    channel_id: int,
    *,
    post_count: int,
    selection_mode: str,
    selection_value: int,
) -> InlineKeyboardMarkup:
    mode_code = "p" if selection_mode == "percent" else "c"
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="🚀 Запустить",
                    callback_data=(
                        f"channel:views_run:{channel_id}:{post_count}:"
                        f"{mode_code}:{selection_value}"
                    ),
                )
            ],
            [
                InlineKeyboardButton(
                    text="25%", callback_data=f"channel:views_accounts:{channel_id}:{post_count}:25"
                ),
                InlineKeyboardButton(
                    text="50%", callback_data=f"channel:views_accounts:{channel_id}:{post_count}:50"
                ),
                InlineKeyboardButton(
                    text="75%", callback_data=f"channel:views_accounts:{channel_id}:{post_count}:75"
                ),
                InlineKeyboardButton(
                    text="100%", callback_data=f"channel:views_accounts:{channel_id}:{post_count}:100"
                ),
            ],
            [
                InlineKeyboardButton(
                    text="✏️ Число или процент",
                    callback_data=f"channel:views_manual:{channel_id}:{post_count}",
                )
            ],
            [
                InlineKeyboardButton(
                    text="📚 Изменить число постов",
                    callback_data=f"channel:views:{channel_id}",
                )
            ],
            [InlineKeyboardButton(text="❌ Отмена", callback_data=f"channel:view:{channel_id}")],
        ]
    )


def channel_views_manual_cancel_keyboard(channel_id: int, post_count: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="↩️ Отмена",
                    callback_data=f"channel:views_posts:{channel_id}:{post_count}",
                )
            ],
            [InlineKeyboardButton(text="🏠 Главное меню", callback_data="main")],
        ]
    )


def channel_view_batch_keyboard(batch_id: int, channel_id: int, *, can_cancel: bool) -> InlineKeyboardMarkup:
    rows = [
        [
            InlineKeyboardButton(
                text="🔄 Обновить", callback_data=f"channel:views_batch:{batch_id}"
            )
        ]
    ]
    if can_cancel:
        rows.append(
            [
                InlineKeyboardButton(
                    text="🛑 Остановить оставшиеся",
                    callback_data=f"channel:views_cancel:{batch_id}",
                )
            ]
        )
    rows.extend(
        [
            [InlineKeyboardButton(text="⬅️ К каналу", callback_data=f"channel:view:{channel_id}")],
            [InlineKeyboardButton(text="🏠 Главное меню", callback_data="main")],
        ]
    )
    return InlineKeyboardMarkup(inline_keyboard=rows)


def channel_post_types_keyboard(
    channel_id: int, *, image_percent: int, no_image_percent: int
) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text=f"🖼 С изображением: {image_percent}%",
                    callback_data=f"channel:post_type_edit:image:{channel_id}",
                )
            ],
            [
                InlineKeyboardButton(
                    text=f"📝 Без изображения: {no_image_percent}%",
                    callback_data=f"channel:post_type_edit:no_image:{channel_id}",
                )
            ],
            [
                InlineKeyboardButton(
                    text="↩️ Оба по 100%",
                    callback_data=f"channel:post_types_reset:{channel_id}",
                )
            ],
            [InlineKeyboardButton(text="⬅️ К каналу", callback_data=f"channel:view:{channel_id}")],
            [InlineKeyboardButton(text="🏠 Главное меню", callback_data="main")],
        ]
    )


def channel_post_type_cancel_keyboard(channel_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="↩️ Отмена", callback_data=f"channel:post_types:{channel_id}"
                )
            ],
            [InlineKeyboardButton(text="🏠 Главное меню", callback_data="main")],
        ]
    )


def channel_connect_overview_keyboard(
    channel_id: int,
    *,
    connectable_count: int,
) -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = []
    if connectable_count > 0:
        rows.extend(
            [
                [
                    InlineKeyboardButton(
                        text=f"✅ Подключить все {connectable_count}",
                        callback_data=f"channel:connect_all:{channel_id}",
                    )
                ],
                [
                    InlineKeyboardButton(
                        text="☑️ Выбрать аккаунты вручную",
                        callback_data=f"channel:connect_manual:{channel_id}:0",
                    )
                ],
            ]
        )
    else:
        rows.append(
            [
                InlineKeyboardButton(
                    text="✅ Недостающих аккаунтов нет",
                    callback_data="noop",
                )
            ]
        )
    rows.extend(
        [
            [
                InlineKeyboardButton(
                    text="🔄 Обновить состояние",
                    callback_data=f"channel:connect_refresh:{channel_id}",
                )
            ],
            [InlineKeyboardButton(text="⬅️ К каналу", callback_data=f"channel:view:{channel_id}")],
            [InlineKeyboardButton(text="🏠 Главное меню", callback_data="main")],
        ]
    )
    return InlineKeyboardMarkup(inline_keyboard=rows)


def channel_connect_all_confirm_keyboard(channel_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="✅ Запустить подключение",
                    callback_data=f"channel:connect_run_all:{channel_id}",
                )
            ],
            [InlineKeyboardButton(text="❌ Отмена", callback_data=f"channel:connect:{channel_id}")],
        ]
    )


def channel_connect_manual_keyboard(
    channel_id: int,
    accounts: list[tuple[int, str]],
    selected_ids: set[int],
    *,
    page: int,
    page_size: int = 8,
) -> InlineKeyboardMarkup:
    total_pages = max(1, (len(accounts) + page_size - 1) // page_size)
    page = max(0, min(page, total_pages - 1))
    start = page * page_size
    current = accounts[start : start + page_size]
    rows: list[list[InlineKeyboardButton]] = []
    for account_id, label in current:
        mark = "☑️" if account_id in selected_ids else "☐"
        rows.append(
            [
                InlineKeyboardButton(
                    text=f"{mark} {label}",
                    callback_data=f"channel:connect_toggle:{channel_id}:{account_id}:{page}",
                )
            ]
        )
    if total_pages > 1:
        nav: list[InlineKeyboardButton] = []
        if page > 0:
            nav.append(
                InlineKeyboardButton(
                    text="⬅️",
                    callback_data=f"channel:connect_manual:{channel_id}:{page - 1}",
                )
            )
        nav.append(
            InlineKeyboardButton(
                text=f"{page + 1}/{total_pages}",
                callback_data="noop",
            )
        )
        if page + 1 < total_pages:
            nav.append(
                InlineKeyboardButton(
                    text="➡️",
                    callback_data=f"channel:connect_manual:{channel_id}:{page + 1}",
                )
            )
        rows.append(nav)
    rows.extend(
        [
            [
                InlineKeyboardButton(
                    text="☑️ Выбрать все",
                    callback_data=f"channel:connect_select_all:{channel_id}:{page}",
                ),
                InlineKeyboardButton(
                    text="🔄 Сбросить",
                    callback_data=f"channel:connect_clear:{channel_id}:{page}",
                ),
            ],
            [
                InlineKeyboardButton(
                    text=f"✅ Подключить выбранные: {len(selected_ids)}",
                    callback_data=f"channel:connect_selected:{channel_id}",
                )
            ],
            [InlineKeyboardButton(text="⬅️ Назад", callback_data=f"channel:connect:{channel_id}")],
        ]
    )
    return InlineKeyboardMarkup(inline_keyboard=rows)


def channel_connect_selected_confirm_keyboard(channel_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="✅ Запустить выбранные",
                    callback_data=f"channel:connect_run_selected:{channel_id}",
                )
            ],
            [
                InlineKeyboardButton(
                    text="✏️ Изменить выбор",
                    callback_data=f"channel:connect_manual:{channel_id}:0",
                )
            ],
            [InlineKeyboardButton(text="❌ Отмена", callback_data=f"channel:connect:{channel_id}")],
        ]
    )


def channel_back_keyboard(channel_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="⬅️ К каналу", callback_data=f"channel:view:{channel_id}")],
            [InlineKeyboardButton(text="🏠 Главное меню", callback_data="main")],
        ]
    )


def confirm_channel_delete(channel_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="🗑 Да, удалить", callback_data=f"channel:delete:{channel_id}")],
            [InlineKeyboardButton(text="⬅️ Отмена", callback_data=f"channel:view:{channel_id}")],
        ]
    )


def channel_reactions_keyboard(channel_id: int, *, has_override: bool) -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = [
        [
            InlineKeyboardButton(
                text="✏️ Изменить реакции канала",
                callback_data=f"channel:reactions_edit:{channel_id}",
            )
        ]
    ]
    if has_override:
        rows.append(
            [
                InlineKeyboardButton(
                    text="↩️ Использовать реакции по умолчанию",
                    callback_data=f"channel:reactions_reset:{channel_id}",
                )
            ]
        )
    rows.extend(
        [
            [InlineKeyboardButton(text="⬅️ К каналу", callback_data=f"channel:view:{channel_id}")],
            [InlineKeyboardButton(text="🏠 Главное меню", callback_data="main")],
        ]
    )
    return InlineKeyboardMarkup(inline_keyboard=rows)


def group_list_keyboard(groups: list[Channel]) -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = []
    for group in groups:
        rows.append([InlineKeyboardButton(text=f"👥 {group.title}", callback_data=f"group:view:{group.id}")])
    rows.append(
        [
            InlineKeyboardButton(
                text="🔎 Поиск и фильтры",
                callback_data="manage:t:g:all:0",
            )
        ]
    )
    rows.append([InlineKeyboardButton(text="➕ Добавить группу", callback_data="group:add")])
    rows.append([InlineKeyboardButton(text="🏠 Главное меню", callback_data="main")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def group_actions(group_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="😀 Реакции группы", callback_data=f"group:reactions:{group_id}")],
            [InlineKeyboardButton(text="❤️ Авто лайк", callback_data=f"autolike:view:{group_id}")],
            [InlineKeyboardButton(text="🚪 Отписаться", callback_data=f"group:leave_confirm:{group_id}")],
            [InlineKeyboardButton(text="⬅️ К группам", callback_data="group:list")],
            [InlineKeyboardButton(text="🏠 Главное меню", callback_data="main")],
        ]
    )


def group_reactions_keyboard(group_id: int, *, has_override: bool) -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = [
        [
            InlineKeyboardButton(
                text="✏️ Изменить реакции группы",
                callback_data=f"group:reactions_edit:{group_id}",
            )
        ]
    ]
    if has_override:
        rows.append(
            [
                InlineKeyboardButton(
                    text="↩️ Использовать реакции по умолчанию",
                    callback_data=f"group:reactions_reset:{group_id}",
                )
            ]
        )
    rows.extend(
        [
            [InlineKeyboardButton(text="⬅️ К группе", callback_data=f"group:view:{group_id}")],
            [InlineKeyboardButton(text="🏠 Главное меню", callback_data="main")],
        ]
    )
    return InlineKeyboardMarkup(inline_keyboard=rows)


def confirm_group_leave(group_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="✅ Да, отписаться", callback_data=f"group:leave:{group_id}")],
            [InlineKeyboardButton(text="⬅️ Назад", callback_data=f"group:view:{group_id}")],
        ]
    )


def autolike_actions(channel: Channel, *, profile_text: str = "⚙️ Свой") -> InlineKeyboardMarkup:
    new_text = "⛔ Выключить новые" if channel.new_posts_enabled else "✅ Включить новые"
    old_text = "⛔ Выключить старые" if channel.old_posts_enabled else "✅ Включить старые"
    is_group = getattr(channel, "kind", "channel") == "group"
    reactions_callback = (
        f"group:reactions:{channel.id}" if is_group else f"channel:reactions:{channel.id}"
    )
    limit = getattr(channel, "max_reactions_per_post", None)
    limit_text = str(limit) if limit else "Все"
    rows: list[list[InlineKeyboardButton]] = []
    if not is_group:
        rows.append(
            [
                InlineKeyboardButton(
                    text=f"🎚 Профиль: {profile_text}",
                    callback_data=f"channel:profile:{channel.id}",
                )
            ]
        )
    rows.extend(
        [
            [
                InlineKeyboardButton(
                    text=new_text,
                    callback_data=f"autolike:toggle_new:{channel.id}",
                )
            ],
            [
                InlineKeyboardButton(
                    text=old_text,
                    callback_data=f"autolike:toggle_old:{channel.id}",
                )
            ],
            [
                InlineKeyboardButton(
                    text=f"📚 Старых постов: {channel.old_posts_depth}",
                    callback_data=f"autolike:depth_menu:{channel.id}",
                )
            ],
            [
                InlineKeyboardButton(
                    text="😀 Реакции", callback_data=reactions_callback
                ),
                InlineKeyboardButton(
                    text=f"🎯 Лимит: {limit_text}",
                    callback_data=f"autolike:limit:{channel.id}",
                ),
            ],
        ]
    )
    if not is_group:
        rows.append(
            [
                InlineKeyboardButton(
                    text="🖼 Типы постов",
                    callback_data=f"channel:post_types:{channel.id}",
                )
            ]
        )
    rows.extend(
        [
            [
                InlineKeyboardButton(text="📅 ТФ", callback_data=f"autolike:period:{channel.id}"),
                InlineKeyboardButton(
                    text="⏱ Период реакций",
                    callback_data=f"channel:reaction_window:{channel.id}",
                ),
            ],
            [InlineKeyboardButton(text="⬅️ К выбору", callback_data="autolike:list")],
            [InlineKeyboardButton(text="🏠 Главное меню", callback_data="main")],
        ]
    )
    return InlineKeyboardMarkup(inline_keyboard=rows)


def reaction_limit_keyboard(channel_id: int, max_accounts: int) -> InlineKeyboardMarkup:
    values = [value for value in (10, 25, 50, 100) if value <= max_accounts]
    rows: list[list[InlineKeyboardButton]] = []
    for index in range(0, len(values), 2):
        rows.append(
            [
                InlineKeyboardButton(
                    text=str(value),
                    callback_data=f"autolike:limit_value:{channel_id}:{value}",
                )
                for value in values[index : index + 2]
            ]
        )
    rows.extend(
        [
            [
                InlineKeyboardButton(
                    text="♾ Все аккаунты",
                    callback_data=f"autolike:limit_value:{channel_id}:all",
                )
            ],
            [
                InlineKeyboardButton(
                    text="✏️ Ввести вручную",
                    callback_data=f"autolike:limit_manual:{channel_id}",
                )
            ],
            [InlineKeyboardButton(text="⬅️ К каналу", callback_data=f"autolike:view:{channel_id}")],
        ]
    )
    return InlineKeyboardMarkup(inline_keyboard=rows)


def reaction_limit_confirm_keyboard(channel_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="✅ Применить", callback_data=f"autolike:limit_apply:{channel_id}"
                )
            ],
            [
                InlineKeyboardButton(
                    text="✏️ Изменить", callback_data=f"autolike:limit_manual:{channel_id}"
                )
            ],
            [
                InlineKeyboardButton(
                    text="❌ Отмена", callback_data=f"autolike:limit_cancel:{channel_id}"
                )
            ],
        ]
    )


def promotion_period_keyboard(channel_id: int, *, permanent: bool) -> InlineKeyboardMarkup:
    rows = [
        [
            InlineKeyboardButton(
                text="⏳ Изменить период",
                callback_data=f"autolike:period_edit:{channel_id}",
            )
        ]
    ]
    if not permanent:
        rows.append(
            [
                InlineKeyboardButton(
                    text="♾ Постоянный",
                    callback_data=f"autolike:period_value:{channel_id}:permanent",
                )
            ]
        )
    rows.append([InlineKeyboardButton(text="⬅️ К каналу", callback_data=f"autolike:view:{channel_id}")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def promotion_period_confirm_keyboard(channel_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="✅ Применить", callback_data=f"autolike:period_apply:{channel_id}"
                )
            ],
            [
                InlineKeyboardButton(
                    text="✏️ Изменить", callback_data=f"autolike:period_edit:{channel_id}"
                )
            ],
            [
                InlineKeyboardButton(
                    text="❌ Отмена", callback_data=f"autolike:period_cancel:{channel_id}"
                )
            ],
        ]
    )


def depth_menu(channel_id: int, max_old_posts: int) -> InlineKeyboardMarkup:
    values = [value for value in (5, 10, 20, 50) if value <= max_old_posts]
    rows: list[list[InlineKeyboardButton]] = []
    for index in range(0, len(values), 2):
        rows.append(
            [
                InlineKeyboardButton(
                    text=str(value), callback_data=f"autolike:set_depth:{channel_id}:{value}"
                )
                for value in values[index : index + 2]
            ]
        )
    rows.append([InlineKeyboardButton(text="⬅️ Назад", callback_data=f"autolike:view:{channel_id}")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def channel_reaction_window_keyboard(channel_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="⚡ 5–15 минут",
                    callback_data=f"channel:reaction_window_value:{channel_id}:300:900",
                ),
                InlineKeyboardButton(
                    text="🕐 15–30 минут",
                    callback_data=f"channel:reaction_window_value:{channel_id}:900:1800",
                ),
            ],
            [
                InlineKeyboardButton(
                    text="🕑 30–60 минут",
                    callback_data=f"channel:reaction_window_value:{channel_id}:1800:3600",
                ),
                InlineKeyboardButton(
                    text="🕒 1–3 часа",
                    callback_data=f"channel:reaction_window_value:{channel_id}:3600:10800",
                ),
            ],
            [
                InlineKeyboardButton(
                    text="✏️ Свой период",
                    callback_data=f"channel:reaction_window_edit:{channel_id}",
                )
            ],
            [
                InlineKeyboardButton(
                    text="🔄 Применить к ожидающим",
                    callback_data=f"channel:reaction_window_reschedule:{channel_id}",
                )
            ],
            [InlineKeyboardButton(text="⬅️ К каналу", callback_data=f"channel:view:{channel_id}")],
            [InlineKeyboardButton(text="🏠 Главное меню", callback_data="main")],
        ]
    )


def channel_reaction_window_after_save_keyboard(channel_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="🔄 Применить к ожидающим",
                    callback_data=f"channel:reaction_window_reschedule:{channel_id}",
                )
            ],
            [
                InlineKeyboardButton(
                    text="⏱ К настройке периода",
                    callback_data=f"channel:reaction_window:{channel_id}",
                )
            ],
            [InlineKeyboardButton(text="⬅️ К каналу", callback_data=f"channel:view:{channel_id}")],
        ]
    )


def account_management_keyboard(
    accounts: list[tuple[int, str, str]],
    *,
    filter_key: str,
    query_active: bool,
    page: int,
    total_pages: int,
) -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = []
    for account_id, label, icon in accounts:
        rows.append(
            [
                InlineKeyboardButton(
                    text=f"{icon} {label}",
                    callback_data=f"account:view:{account_id}",
                )
            ]
        )
    filter_rows = [
        [("all", "Все"), ("active", "Активные")],
        [("disabled", "Выключены"), ("problem", "Проблемные")],
        [("flood", "FloodWait"), ("no_email", "Без почты")],
        [("error", "С ошибками")],
    ]
    for items in filter_rows:
        rows.append(
            [
                InlineKeyboardButton(
                    text=("✅ " if key == filter_key else "") + label,
                    callback_data=f"manage:a:{key}:0",
                )
                for key, label in items
            ]
        )
    search_row = [
        InlineKeyboardButton(
            text="🔎 Изменить поиск" if query_active else "🔎 Поиск",
            callback_data=f"manage:as:{filter_key}",
        )
    ]
    if query_active:
        search_row.append(
            InlineKeyboardButton(
                text="✖️ Сбросить", callback_data=f"manage:ac:{filter_key}"
            )
        )
    rows.append(search_row)
    if total_pages > 1:
        nav: list[InlineKeyboardButton] = []
        if page > 0:
            nav.append(
                InlineKeyboardButton(
                    text="⬅️", callback_data=f"manage:a:{filter_key}:{page - 1}"
                )
            )
        nav.append(InlineKeyboardButton(text=f"{page + 1}/{total_pages}", callback_data="noop"))
        if page + 1 < total_pages:
            nav.append(
                InlineKeyboardButton(
                    text="➡️", callback_data=f"manage:a:{filter_key}:{page + 1}"
                )
            )
        rows.append(nav)
    rows.append([InlineKeyboardButton(text="⚙️ Массовые действия", callback_data="manage:ab")])
    rows.append([InlineKeyboardButton(text="⬅️ К аккаунтам", callback_data="account:list")])
    rows.append([InlineKeyboardButton(text="🏠 Главное меню", callback_data="main")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def account_bulk_actions_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="✅ Включить обычные", callback_data="manage:ab:enable"),
                InlineKeyboardButton(text="⛔ Выключить обычные", callback_data="manage:ab:disable"),
            ],
            [InlineKeyboardButton(text="🩺 Проверить активные сессии", callback_data="manage:ab:audit")],
            [InlineKeyboardButton(text="🔄 Обновить имена и username", callback_data="manage:ab:refresh")],
            [InlineKeyboardButton(text="⬅️ К управлению", callback_data="manage:a:all:0")],
            [InlineKeyboardButton(text="🏠 Главное меню", callback_data="main")],
        ]
    )


def account_bulk_confirm_keyboard(action: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="✅ Подтвердить", callback_data=f"manage:abc:{action}"
                )
            ],
            [InlineKeyboardButton(text="❌ Отмена", callback_data="manage:ab")],
        ]
    )


def target_management_keyboard(
    targets: list[tuple[int, str, bool, bool]],
    *,
    kind_code: str,
    filter_key: str,
    query_active: bool,
    page: int,
    total_pages: int,
) -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = []
    prefix = "channel:view" if kind_code == "c" else "group:view"
    for target_id, label, active, has_error in targets:
        icon = "⚠️" if has_error else ("🟢" if active else "⚫️")
        rows.append(
            [
                InlineKeyboardButton(
                    text=f"{icon} {label}", callback_data=f"{prefix}:{target_id}"
                )
            ]
        )
    for items in (
        [("all", "Все"), ("active", "Активные")],
        [("disabled", "Выключены"), ("error", "С ошибками")],
    ):
        rows.append(
            [
                InlineKeyboardButton(
                    text=("✅ " if key == filter_key else "") + label,
                    callback_data=f"manage:t:{kind_code}:{key}:0",
                )
                for key, label in items
            ]
        )
    search_row = [
        InlineKeyboardButton(
            text="🔎 Изменить поиск" if query_active else "🔎 Поиск",
            callback_data=f"manage:ts:{kind_code}:{filter_key}",
        )
    ]
    if query_active:
        search_row.append(
            InlineKeyboardButton(
                text="✖️ Сбросить", callback_data=f"manage:tc:{kind_code}:{filter_key}"
            )
        )
    rows.append(search_row)
    if total_pages > 1:
        nav: list[InlineKeyboardButton] = []
        if page > 0:
            nav.append(
                InlineKeyboardButton(
                    text="⬅️",
                    callback_data=f"manage:t:{kind_code}:{filter_key}:{page - 1}",
                )
            )
        nav.append(InlineKeyboardButton(text=f"{page + 1}/{total_pages}", callback_data="noop"))
        if page + 1 < total_pages:
            nav.append(
                InlineKeyboardButton(
                    text="➡️",
                    callback_data=f"manage:t:{kind_code}:{filter_key}:{page + 1}",
                )
            )
        rows.append(nav)
    back_callback = "channel:list" if kind_code == "c" else "group:list"
    rows.append([InlineKeyboardButton(text="⬅️ Назад", callback_data=back_callback)])
    rows.append([InlineKeyboardButton(text="🏠 Главное меню", callback_data="main")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def channel_copy_targets_keyboard(
    source_id: int,
    targets: list[tuple[int, str]],
    *,
    page: int,
    total_pages: int,
) -> InlineKeyboardMarkup:
    rows = [
        [
            InlineKeyboardButton(
                text=f"📢 {label}", callback_data=f"channel:copyc:{source_id}:{target_id}"
            )
        ]
        for target_id, label in targets
    ]
    if total_pages > 1:
        nav: list[InlineKeyboardButton] = []
        if page > 0:
            nav.append(
                InlineKeyboardButton(
                    text="⬅️", callback_data=f"channel:copy:{source_id}:{page - 1}"
                )
            )
        nav.append(InlineKeyboardButton(text=f"{page + 1}/{total_pages}", callback_data="noop"))
        if page + 1 < total_pages:
            nav.append(
                InlineKeyboardButton(
                    text="➡️", callback_data=f"channel:copy:{source_id}:{page + 1}"
                )
            )
        rows.append(nav)
    rows.append([InlineKeyboardButton(text="⬅️ К каналу", callback_data=f"channel:view:{source_id}")])
    rows.append([InlineKeyboardButton(text="🏠 Главное меню", callback_data="main")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def channel_copy_confirm_keyboard(source_id: int, target_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="✅ Копировать",
                    callback_data=f"channel:copya:{source_id}:{target_id}",
                )
            ],
            [InlineKeyboardButton(text="❌ Отмена", callback_data=f"channel:copy:{source_id}:0")],
            [InlineKeyboardButton(text="⬅️ К исходному каналу", callback_data=f"channel:view:{source_id}")],
        ]
    )
