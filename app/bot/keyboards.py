from __future__ import annotations

from functools import partial

from app.services.exchange_identity import clean_exchange_id

try:
    from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup
except Exception:  # pragma: no cover
    InlineKeyboardButton = None
    InlineKeyboardMarkup = None


def bind_callback_owner(data: str, owner_id: int | None) -> str:
    """Attach a compact menu owner marker without breaking legacy prefixes."""
    payload = str(data or "")
    if owner_id is None:
        return payload
    bound = f"{payload}|u{int(owner_id)}"
    if len(bound.encode("utf-8")) > 64:
        raise ValueError("Telegram callback_data exceeds 64 bytes after owner binding")
    return bound


def parse_callback_owner(data: str | None) -> tuple[int | None, str]:
    """Return ``(owner_id, original_payload)`` for a bound callback."""
    payload = str(data or "")
    head, marker, tail = payload.rpartition("|u")
    if marker and tail.isdigit():
        return int(tail), head
    return None, payload


def _btn(text: str, data: str, *, owner_id: int | None = None):
    return InlineKeyboardButton(
        text=text, callback_data=bind_callback_owner(data, owner_id)
    )


def _callback_position_id(value) -> str:
    """Return a strict compact position id safe for Telegram callback_data."""
    cleaned = clean_exchange_id(value)
    if not cleaned or len(cleaned.encode("utf-8")) > 40:
        return ""
    return cleaned


def start_welcome_menu(*, api_connected: bool = False, owner_id: int | None = None):
    """Buttons shown under the branded /start welcome card."""
    btn = partial(_btn, owner_id=owner_id)
    if InlineKeyboardMarkup is None:
        return None
    second_text = "🔑 BingX API" if api_connected else "🔑 Подключить BingX API"
    second_data = "menu:exchanges" if api_connected else "api_setup_start:bingx"
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [btn("📊 Панель управления", "menu:home")],
            [btn(second_text, second_data)],
            [btn("❓ Помощь", "menu:help")],
        ]
    )


def main_menu(
    section: str = "home",
    *,
    is_admin: bool = False,
    selected_limit_preset: str | None = None,
    skip_trade_notifications_enabled: bool = False,
    owner_id: int | None = None,
):
    """Compact BingX-only menu. Callback names stay backward-compatible."""
    btn = partial(_btn, owner_id=owner_id)
    if InlineKeyboardMarkup is None:
        return None
    rows = []

    if section == "exchanges":
        rows.append([btn("🏦 BingX", "exchange:bingx")])
        rows.append([btn("⬅️ Назад", "menu:home")])
        return InlineKeyboardMarkup(inline_keyboard=rows)

    if section == "mode":
        skip_label = (
            "🔔 Пропуски сделок: Вкл"
            if bool(skip_trade_notifications_enabled)
            else "🔕 Пропуски сделок: Выкл"
        )
        rows.extend(
            [
                [btn("🤖 Авто", "vip:auto"), btn("👁 Просмотр", "vip:preview")],
                [btn("⏸ Выкл", "vip:off")],
                [btn(skip_label, "menu:skip_notifications")],
                [btn("⬅️ Назад", "menu:home")],
            ]
        )
        return InlineKeyboardMarkup(inline_keyboard=rows)

    if section == "skip_notifications":
        rows.extend(
            [
                [
                    btn("🔔 Включить", "skipnotify:on"),
                    btn("🔕 Выключить", "skipnotify:off"),
                ],
                [btn("⬅️ Назад к режиму", "menu:mode")],
            ]
        )
        return InlineKeyboardMarkup(inline_keyboard=rows)

    if section == "risk":
        rows.extend(
            [
                [btn("🧮 10 сделок по 1%", "riskpreset:be10")],
                [btn("♻️ БУ освобождает риск", "riskbe:on")],
                [btn("🚫 БУ считается риском", "riskbe:off")],
                [btn("⬅️ Назад", "menu:home")],
            ]
        )
        return InlineKeyboardMarkup(inline_keyboard=rows)

    if section == "tp":
        rows.extend(
            [
                [btn("🎯 3 TP", "tplimit:3"), btn("🎯 Все TP", "tplimit:all")],
                [btn("🧠 Умная (ближние больше)", "tp:smart")],
                [btn("🛡️ Ранняя фиксация (70/15/10/5)", "tp:early_fixation")],
                [btn("🚀 Разгон (10/65/20/5)", "tp:acceleration")],
                [btn("🔔 Колокол (центр больше)", "tp:bell")],
                [btn("⚖️ Равными долями", "tp:equal")],
                [
                    btn("✅ % из сигнала", "tpsignal:on"),
                    btn("🚫 % схемой", "tpsignal:off"),
                ],
                [btn("⬅️ Назад", "menu:home")],
            ]
        )
        return InlineKeyboardMarkup(inline_keyboard=rows)

    if section == "limits":
        selected = str(selected_limit_preset or "").strip().lower()

        def _limit_label(preset: str, label: str) -> str:
            return f"✅ {label}" if selected == preset else label

        # Keep the four ready safety profiles and expose the already-existing
        # bounded custom TTL flow. The legacy no-TTL policy remains hidden.
        rows.extend(
            [
                [
                    btn(_limit_label("fast", "⚡ Быстрый"), "limitpreset:fast"),
                    btn(_limit_label("tp2", "🎯 После TP2"), "limitpreset:tp2"),
                ],
                [
                    btn(
                        _limit_label("balanced", "⚖️ Стандартный"),
                        "limitpreset:balanced",
                    ),
                    btn(_limit_label("long", "🛡 Долгий"), "limitpreset:long"),
                ],
                [btn(_limit_label("custom", "🕒 Свой срок"), "limitttl:custom")],
                [btn("📋 Активные лимитки", "limitactive:list")],
                [btn("♻️ Применить текущий режим", "limitapply:preview")],
                [btn("⬅️ Назад", "menu:home")],
            ]
        )
        return InlineKeyboardMarkup(inline_keyboard=rows)

    if section == "be":
        rows.extend(
            [
                [btn("🛡 После TP1", "be:1"), btn("🛡 После TP2", "be:2")],
                [btn("🛡 После TP3", "be:3")],
                [
                    btn(
                        "♻️ Применить к текущим сделкам",
                        "beapply:preview",
                    )
                ],
                [btn("🚫 БУ выкл", "be:off"), btn("⬅️ Назад", "menu:home")],
            ]
        )
        return InlineKeyboardMarkup(inline_keyboard=rows)

    rows.extend(
        [
            [btn("📊 Статус", "menu:status"), btn("💰 Баланс", "menu:balance")],
            [btn("🔑 BingX API", "menu:exchanges"), btn("🟢 Режим", "menu:mode")],
            [btn("⚙️ Риск", "menu:risk"), btn("🎯 Тейки", "menu:tp")],
            [btn("⚖️ Б/У", "menu:be"), btn("⏳ LIMIT", "menu:limits")],
            [
                btn("📂 Сделки", "menu:positions"),
                btn("⏳ Лимитки", "limitactive:list"),
            ],
        ]
    )
    if is_admin:
        rows.append(
            [
                btn("👥 Подписчики", "menu:subscribers"),
                btn("🛡 White-list", "wl_menu:0"),
            ]
        )
        rows.append([btn("📊 Аналитика сигналов", "menu:analytics")])
        rows.append([btn("🧰 Диагностика", "menu:diagnostics")])
    rows.append([btn("❓ Помощь", "menu:help")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def signal_analytics_admin_menu(*, owner_id: int | None = None):
    """Clean administrator controls for the human-readable statistics card."""
    btn = partial(_btn, owner_id=owner_id)
    if InlineKeyboardMarkup is None:
        return None
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [btn("🔄 Обновить", "menu:analytics")],
            [btn("📥 Скачать статистику", "analytics:export")],
            [btn("🗂 Периоды", "analytics:periods"), btn("📚 Все периоды", "analytics:all")],
            [btn("🔧 Технические детали", "analytics:technical")],
            [btn("🧹 Новый период", "analytics:reset")],
            [btn("⬅️ Назад", "menu:home")],
        ]
    )


def statistics_technical_admin_menu(*, owner_id: int | None = None):
    """Advanced statistics controls kept away from the friendly main card."""
    btn = partial(_btn, owner_id=owner_id)
    if InlineKeyboardMarkup is None:
        return None
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [btn("💰 Финансы", "analytics:financial"), btn("🩺 Quality", "analytics:quality")],
            [btn("🧯 Recovery", "analytics:recovery"), btn("📥 Скачать ZIP", "analytics:export")],
            [btn("📊 К статистике", "menu:analytics")],
            [btn("⬅️ Главное меню", "menu:home")],
        ]
    )


def statistics_reset_confirm_menu(
    request_id: int,
    token: str,
    *,
    owner_id: int | None = None,
):
    """One-time durable confirmation for a non-destructive period reset."""
    btn = partial(_btn, owner_id=owner_id)
    if InlineKeyboardMarkup is None:
        return None
    compact_id = int(request_id)
    compact_token = str(token or "")
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [btn("✅ Закрыть период и создать новый", f"statsreset:confirm:{compact_id}:{compact_token}")],
            [btn("❌ Отмена", f"statsreset:cancel:{compact_id}:{compact_token}")],
        ]
    )


def statistics_recovery_confirm_menu(
    audit_id: int,
    *,
    owner_id: int | None = None,
):
    """Confirm an audit-only recovery review request; no repair is performed."""
    btn = partial(_btn, owner_id=owner_id)
    if InlineKeyboardMarkup is None:
        return None
    compact_id = int(audit_id)
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [btn("✅ Запросить ручную проверку", f"statsrecovery:request:{compact_id}")],
            [btn("❌ Отмена", f"statsrecovery:cancel:{compact_id}")],
        ]
    )


def subscribers_admin_menu(*, owner_id: int | None = None):
    """Read-only admin controls for the subscriber summary card."""
    btn = partial(_btn, owner_id=owner_id)
    if InlineKeyboardMarkup is None:
        return None
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [btn("🔄 Обновить", "menu:subscribers")],
            [btn("⬅️ Назад", "menu:home")],
        ]
    )


def active_limits_list_menu(rows: list[dict], *, owner_id: int | None = None):
    """Select one of the owner's pending LIMIT executions."""
    btn = partial(_btn, owner_id=owner_id)
    if InlineKeyboardMarkup is None:
        return None
    keyboard_rows = []
    for row in list(rows or [])[:15]:
        execution_id = int(row.get("id") or 0)
        if execution_id <= 0:
            continue
        symbol = str(row.get("symbol") or "LIMIT").upper()
        side = str(row.get("side") or "").upper()
        entry = str(row.get("entry") or "—")
        label = f"🪙 {symbol} • {side} • {entry}"[:60]
        keyboard_rows.append([btn(label, f"limitactive:view:{execution_id}")])
    keyboard_rows.extend(
        [
            [btn("🔄 Обновить", "limitactive:list")],
            [btn("⏳ LIMIT", "menu:limits"), btn("⬅️ Назад", "menu:home")],
        ]
    )
    return InlineKeyboardMarkup(inline_keyboard=keyboard_rows)


def active_limit_detail_menu(execution_id: int, *, owner_id: int | None = None):
    """Actions for one exact pending LIMIT execution."""
    btn = partial(_btn, owner_id=owner_id)
    if InlineKeyboardMarkup is None:
        return None
    execution_id = int(execution_id)
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [btn("🔄 Безопасно перепроверить", f"limitactive:recheck:{execution_id}")],
            [btn("🗑 Отменить LIMIT", f"limitactive:cancel:{execution_id}")],
            [btn("🔄 Обновить", f"limitactive:view:{execution_id}")],
            [btn("⬅️ К списку", "limitactive:list")],
        ]
    )


def active_limit_cancel_confirm_menu(execution_id: int, *, owner_id: int | None = None):
    """Two-step confirmation before one exact exchange cancellation."""
    btn = partial(_btn, owner_id=owner_id)
    if InlineKeyboardMarkup is None:
        return None
    execution_id = int(execution_id)
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [btn("✅ Да, отменить", f"limitactive:confirm:{execution_id}")],
            [btn("❌ Не отменять", f"limitactive:view:{execution_id}")],
        ]
    )


def active_limit_result_menu(*, owner_id: int | None = None):
    btn = partial(_btn, owner_id=owner_id)
    if InlineKeyboardMarkup is None:
        return None
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [btn("📋 К активным лимиткам", "limitactive:list")],
            [btn("⬅️ Назад", "menu:home")],
        ]
    )


def positions_list_menu(rows: list[dict], *, owner_id: int | None = None):
    """Select one exact live BingX position owned by the current user."""
    btn = partial(_btn, owner_id=owner_id)
    if InlineKeyboardMarkup is None:
        return None
    keyboard_rows = []
    for row in list(rows or [])[:15]:
        position_id = _callback_position_id(row.get("positionId"))
        if not position_id:
            continue
        symbol = str(row.get("symbol") or "POSITION").upper()
        side = str(row.get("side") or "").upper()
        size = str(row.get("size") or "—")
        label = f"🪙 {symbol} • {side} • {size}"[:60]
        keyboard_rows.append([btn(label, f"position:view:{position_id}")])
    keyboard_rows.extend(
        [
            [btn("🔄 Обновить", "position:list")],
            [btn("⬅️ Назад", "menu:home")],
        ]
    )
    return InlineKeyboardMarkup(inline_keyboard=keyboard_rows)


def position_detail_menu(
    position_id: str,
    *,
    owner_id: int | None = None,
    allow_force_be: bool = True,
):
    """Actions for one exact live position."""
    btn = partial(_btn, owner_id=owner_id)
    if InlineKeyboardMarkup is None:
        return None
    pid = _callback_position_id(position_id)
    rows = []
    if not pid:
        return InlineKeyboardMarkup(
            inline_keyboard=[
                [btn("⬅️ К позициям", "position:list")],
            ]
        )
    if allow_force_be:
        rows.append([btn("🔒 Перенести в Б/У", f"position:be:{pid}")])
    rows.extend(
        [
            [btn("🔴 Закрыть позицию", f"position:close:{pid}")],
            [btn("🔄 Обновить", f"position:view:{pid}")],
            [btn("⬅️ К позициям", "position:list")],
        ]
    )
    return InlineKeyboardMarkup(inline_keyboard=rows)


def position_close_confirm_menu(position_id: str, *, owner_id: int | None = None):
    btn = partial(_btn, owner_id=owner_id)
    if InlineKeyboardMarkup is None:
        return None
    pid = _callback_position_id(position_id)
    if not pid:
        return InlineKeyboardMarkup(
            inline_keyboard=[
                [btn("⬅️ К позициям", "position:list")],
            ]
        )
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [btn("✅ Да, закрыть полностью", f"position:closeconfirm:{pid}")],
            [btn("❌ Не закрывать", f"position:view:{pid}")],
        ]
    )


def position_be_confirm_menu(position_id: str, *, owner_id: int | None = None):
    btn = partial(_btn, owner_id=owner_id)
    if InlineKeyboardMarkup is None:
        return None
    pid = _callback_position_id(position_id)
    if not pid:
        return InlineKeyboardMarkup(
            inline_keyboard=[
                [btn("⬅️ К позициям", "position:list")],
            ]
        )
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [btn("✅ Да, поставить Б/У", f"position:beconfirm:{pid}")],
            [btn("❌ Не менять", f"position:view:{pid}")],
        ]
    )


def position_action_result_menu(*, owner_id: int | None = None):
    btn = partial(_btn, owner_id=owner_id)
    if InlineKeyboardMarkup is None:
        return None
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [btn("📂 К позициям", "position:list")],
            [btn("⬅️ Назад", "menu:home")],
        ]
    )


# Static callback markers for safety tests:
# callback_data="be:1"
# callback_data="be:2"
# callback_data="be:3"


def exchange_connected_menu(exchange: str = "bingx", *, owner_id: int | None = None):
    btn = partial(_btn, owner_id=owner_id)
    if InlineKeyboardMarkup is None:
        return None
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [btn("🔌 Отключить BingX API", "api_disconnect:bingx")],
            [btn("🏦 К BingX", "menu:exchanges"), btn("⬅️ Назад", "menu:home")],
        ]
    )


def exchange_need_api_menu(
    exchange: str | None = "bingx",
    terms_required: bool = False,
    *,
    owner_id: int | None = None,
):
    btn = partial(_btn, owner_id=owner_id)
    if InlineKeyboardMarkup is None:
        return None
    rows = []
    if terms_required:
        rows.append([btn("📄 Соглашение и риски", "terms:show")])
    else:
        rows.append(
            [btn("🔑 Подключить BingX API (по шагам)", "api_setup_start:bingx")]
        )
    rows.append([btn("🏦 К BingX", "menu:exchanges"), btn("⬅️ Назад", "menu:home")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def terms_accept_menu(*, owner_id: int | None = None):
    btn = partial(_btn, owner_id=owner_id)
    if InlineKeyboardMarkup is None:
        return None
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [btn("✅ Принимаю условия", "terms:accept")],
            [btn("❌ Не принимаю", "menu:home")],
        ]
    )


def whitelist_add_exchange_picker(target_uid: int, *, owner_id: int | None = None):
    """BingX-only grant picker; signature retained for old callbacks."""
    btn = partial(_btn, owner_id=owner_id)
    if InlineKeyboardMarkup is None:
        return None
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [btn("✅ Дать доступ BingX", f"wladd:{target_uid}:bingx")],
            [btn("❌ Отмена", f"wladd:{target_uid}:cancel")],
        ]
    )


def whitelist_remove_exchange_picker(
    target_uid: int, current: set[str], *, owner_id: int | None = None
):
    btn = partial(_btn, owner_id=owner_id)
    if InlineKeyboardMarkup is None:
        return None
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [btn("🚫 Убрать доступ BingX", f"wlrm:{target_uid}:all")],
            [btn("❌ Отмена", f"wlrm:{target_uid}:cancel")],
        ]
    )


def api_setup_cancel_menu(*, owner_id: int | None = None):
    btn = partial(_btn, owner_id=owner_id)
    if InlineKeyboardMarkup is None:
        return None
    return InlineKeyboardMarkup(
        inline_keyboard=[[btn("❌ Отмена", "api_setup_cancel")]]
    )


def limit_ttl_cancel_menu(*, owner_id: int | None = None):
    btn = partial(_btn, owner_id=owner_id)
    if InlineKeyboardMarkup is None:
        return None
    return InlineKeyboardMarkup(inline_keyboard=[[btn("❌ Отмена", "limitttl:cancel")]])


def wl_users_list_keyboard(
    users: list, page: int = 0, per_page: int = 8, *, owner_id: int | None = None
):
    btn = partial(_btn, owner_id=owner_id)
    if InlineKeyboardMarkup is None:
        return None
    rows = []
    start = page * per_page
    chunk = users[start : start + per_page]
    for u in chunk:
        grants = u.get("whitelist_exchanges") or set()
        wl_short = "BingX" if grants else "—"
        uname = u.get("username") or ""
        uid = u.get("telegram_id")
        label = f"{uid} {('@' + uname) if uname else ''} • BingX • WL:{wl_short}"[:60]
        rows.append([btn(label, f"wl_user:{uid}")])
    nav = []
    if page > 0:
        nav.append(btn("⬅️", f"wl_menu:{page - 1}"))
    if (start + per_page) < len(users):
        nav.append(btn("➡️", f"wl_menu:{page + 1}"))
    if nav:
        rows.append(nav)
    rows.append(
        [
            btn("➕ Добавить юзера", "wl_add_prompt"),
            btn("🔄 Обновить", f"wl_menu:{page}"),
        ]
    )
    rows.append([btn("⬅️ Назад", "menu:home")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def wl_user_card_keyboard(
    target_uid: int,
    grants: set,
    enabled_exchanges: list | None = None,
    *,
    owner_id: int | None = None,
):
    btn = partial(_btn, owner_id=owner_id)
    if InlineKeyboardMarkup is None:
        return None
    has_access = bool(grants)
    rows = []
    if has_access:
        rows.append([btn("➖ Убрать BingX", f"wl_revoke:{target_uid}:all")])
    else:
        rows.append([btn("➕ Дать BingX", f"wl_grant:{target_uid}:bingx")])
    rows.append([btn("⬅️ К списку", "wl_menu:0")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def limit_apply_confirm_menu(token: str, *, owner_id: int | None = None):
    btn = partial(_btn, owner_id=owner_id)
    if InlineKeyboardMarkup is None:
        return None
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [btn("✅ Применить", f"limitapply:confirm:{str(token)[:16]}")],
            [btn("❌ Не менять", "menu:limits")],
        ]
    )


def be_apply_confirm_menu(token: str, *, owner_id: int | None = None):
    """Owner-bound confirmation for rewriting current execution BE snapshots."""
    btn = partial(_btn, owner_id=owner_id)
    if InlineKeyboardMarkup is None:
        return None
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [btn("✅ Применить", f"beapply:confirm:{str(token)[:16]}")],
            [btn("❌ Не менять", "menu:be")],
        ]
    )
