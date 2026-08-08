from __future__ import annotations

import math

from app.services.exchange_factory import exchange_title
from app.exchanges.bingx.symbols import bingx_tradfi_exchange_symbol
from app.services.models import ExecutionResult, Signal
from app.services.trade_notification_policy import (
    MANDATORY_TRADE_WARNING_NOTIFICATION_KIND,
    is_optional_trade_skip_result,
)
from app.services.notification_style import (
    card,
    details_line,
    esc,
    fmt_percent,
    fmt_price,
    fmt_qty,
    fmt_usdt,
    premium_kv_block,
    premium_section,
    system_message,
    tree_lines,
)


def _entry_text(signal: Signal, entry_override: float | None = None) -> str:
    actual = float(entry_override or 0.0)
    if actual > 0:
        return fmt_price(actual)
    if (
        str(getattr(signal, "order_type", "LIMIT")).upper() == "MARKET"
        and float(signal.entry or 0.0) <= 0
    ):
        return "MARKET"
    return fmt_price(signal.entry)


def _stop_text(signal: Signal, result: ExecutionResult | None = None) -> str:
    if result is not None:
        actual = result.payload.get("actual_stop")
        try:
            if float(actual or 0.0) > 0:
                return fmt_price(actual)
        except (TypeError, ValueError):
            pass
    return fmt_price(signal.stop)




def _is_unsupported_symbol_result(result: ExecutionResult) -> bool:
    payload = dict(result.payload or {})
    if payload.get("error_kind") == "unsupported_symbol_on_bingx":
        return True
    if payload.get("user_notification_kind") == "unsupported_symbol_on_bingx":
        return True
    reason = str(result.reason or "").lower()
    return (
        bool(payload.get("symbol_unavailable"))
        and "bingx" in reason
        and ("не поддерж" in reason or "не найд" in reason or "not exist" in reason)
    )


def is_suppressible_trade_skip_notification(result: ExecutionResult) -> bool:
    """Return True only for an explicitly allowlisted routine skip.

    ``status=skipped`` is also used for mandatory fail-closed warnings such as
    missing API credentials, queue rejection, invalid exchange constraints and
    insufficient margin.  Those events are never controlled by the optional
    preference.
    """

    return is_optional_trade_skip_result(result)


def unsupported_symbol_message(signal: Signal, result: ExecutionResult) -> str:
    payload = dict(result.payload or {})
    raw_symbol = str(payload.get("raw_symbol") or signal.symbol or "").upper()
    normalized_symbol = str(payload.get("normalized_symbol") or raw_symbol).upper()
    facts = premium_kv_block(
        (
            ("💵 Вход", _entry_text(signal, result.payload.get("actual_entry"))),
            ("🛡 Stop-Loss", _stop_text(signal, result)),
            ("🎯 Цели", f"{len(signal.targets)} цели" if len(signal.targets) != 1 else "1 цель"),
        )
    )
    return card(
        "🔴 <b>СДЕЛКА НЕ ОТКРЫТА</b>",
        symbol=signal.symbol,
        side=signal.side.value,
        blocks=(
            facts,
            premium_section(
                "⚠️ <b>Причина</b>",
                f"Пара <b>{esc(raw_symbol)}</b> не поддерживается на BingX Futures.",
            ),
            premium_section(
                "🧠 <b>Пояснение</b>",
                f"BingX не вернул контрактные правила для <code>{esc(normalized_symbol)}</code>.",
                "Без этих данных бот не может безопасно рассчитать объём сделки.",
            ),
            premium_section(
                "✅ <b>Действие</b>",
                "Сигнал пропущен. Сделка не открывалась.",
                "API и баланс здесь ни при чём.",
            ),
        ),
    )


def signal_summary(signal: Signal, entry_override: float | None = None) -> str:
    """Compact HTML-safe signal summary retained for compatibility."""
    return "\n".join(
        [
            f"🪙 <b>{esc(bingx_tradfi_exchange_symbol(signal.symbol) or signal.symbol.upper())}</b> • {esc(signal.side.value.upper())}",
            f"💵 Вход: {_entry_text(signal, entry_override)}",
            f"🛡 <b>STOP:</b> {fmt_price(signal.stop)}",
            f"🎯 <b>Целей:</b> {len(signal.targets)}",
        ]
    )


def _targets_block(targets: list, pcts: list) -> list[str]:
    if not targets:
        return ["🎯 <b>Цели:</b> не заданы"]
    rows: list[str] = ["🎯 <b>Цели:</b>"]
    for index, target in enumerate(targets, 1):
        pct = pcts[index - 1] if index - 1 < len(pcts) else None
        suffix = f" • {fmt_percent(pct)} объёма" if pct is not None else ""
        marker = "└" if index == len(targets) else "├"
        rows.append(f"{marker} TP{index}: <b>{fmt_price(target)}</b>{suffix}")
    return rows


def signal_price_anomaly_message(signal: Signal, result: ExecutionResult) -> str:
    payload = dict(result.payload or {})
    entry = payload.get("signal_entry", signal.entry)
    current = payload.get("current_price")
    deviation = payload.get("price_deviation_percent", 0.0)
    ratio_raw = payload.get("price_ratio", 0.0)
    suggested = payload.get("suggested_entry")
    direction = str(payload.get("price_direction") or "").lower()

    try:
        ratio = float(ratio_raw or 0.0)
    except (TypeError, ValueError, OverflowError):
        ratio = 0.0
    if not math.isfinite(ratio) or ratio < 0:
        ratio = 0.0

        # Integer-style wording is useful for ordinary decimal shifts, but trying
        # to round an infinite or astronomically large corrupted ratio can raise or
        # produce an unreadable integer. Keep the renderer total and bounded.
    nearest = max(2, int(round(ratio))) if 2 <= ratio <= 1_000_000 else 0
    if nearest and abs(ratio - nearest) / nearest <= 0.05:
        factor_text = f"почти в {nearest} раз"
    elif 0 < ratio < 1_000_000:
        factor_text = f"примерно в {ratio:.2f} раза"
    elif ratio >= 1_000_000:
        factor_text = "более чем в миллион раз"
    else:
        factor_text = "аномально"

    if direction == "above":
        comparison_text = f"{factor_text} выше текущей цены"
    elif direction == "below":
        comparison_text = f"{factor_text} ниже текущей цены"
    else:
        comparison_text = "аномально отличается от текущей цены"

    try:
        suggested_number = float(suggested or 0.0)
        has_suggestion = math.isfinite(suggested_number) and suggested_number > 0
    except (TypeError, ValueError, OverflowError):
        has_suggestion = False

    explanation = [f"❗ Цена входа {comparison_text}."]
    if has_suggestion or payload.get("decimal_normalization_preview") is True:
        explanation.extend(
            [
                "Вероятна ошибка десятичного разряда в исходном сигнале.",
                "Подробная проверка разряда доступна только администратору.",
            ]
        )
    else:
        explanation.extend(
            [
                "Возможна ошибка цены или устаревший исходный сигнал:",
                "<code>Проверьте ENTRY, STOP и TP в исходном сигнале</code>",
            ]
        )

    try:
        deviation_number = float(deviation)
        deviation_text = (
            f"{deviation_number:+.1f}%"
            if math.isfinite(deviation_number)
            else "не рассчитано"
        )
    except (TypeError, ValueError, OverflowError):
        deviation_text = "не рассчитано"

    if payload.get("mexc_price_band_rejection") is True:
        safety_lines = [
            "🔒 BingX отклонила заявку до открытия позиции.",
            "Сделка не была открыта; бот не исправляет цену автоматически.",
        ]
    else:
        safety_lines = [
            "🔒 В целях безопасности сделка не была отправлена на биржу.",
            "Бот не исправляет цену автоматически и ожидает корректный сигнал.",
        ]

    return card(
        "⛔️ <b>СИГНАЛ ЗАБЛОКИРОВАН</b>",
        symbol=signal.symbol,
        side=signal.side.value,
        blocks=(
            [
                "⚠️ <b>Обнаружено аномальное расхождение цены</b>",
                "",
                f"💵 <b>Вход из сделки:</b> {fmt_price(entry)}",
                f"📊 <b>Текущая цена BingX:</b> {fmt_price(current)}",
                f"📐 <b>Отклонение:</b> {deviation_text}",
            ],
            [
                f"🛡 <b>STOP из сигнала:</b> {_stop_text(signal, result)}",
                f"🎯 <b>Целей:</b> {len(signal.targets)}",
            ],
            explanation,
            safety_lines,
        ),
    )


def admin_decimal_normalization_preview_message(
    signal: Signal, result: ExecutionResult
) -> str:
    """Render admin-only decimal-shift diagnostics for a blocked signal."""
    payload = dict(result.payload or {})
    if payload.get("decimal_normalization_preview") is not True:
        return signal_price_anomaly_message(signal, result)

    factor = str(payload.get("decimal_normalization_factor_text") or "не найден")
    original_entry = payload.get(
        "decimal_original_entry", payload.get("signal_entry", signal.entry)
    )
    normalized_entry = payload.get("decimal_normalized_entry")
    original_stop = payload.get("decimal_original_stop", signal.stop)
    normalized_stop = payload.get("decimal_normalized_stop")
    current = payload.get("decimal_current_price", payload.get("current_price"))
    deviation_after = payload.get("decimal_deviation_after_percent")
    original_targets = list(
        payload.get("decimal_original_targets") or list(signal.targets or [])
    )
    normalized_targets = list(payload.get("decimal_normalized_targets") or [])

    try:
        deviation_text = fmt_percent(float(deviation_after or 0.0), signed=True)
    except (TypeError, ValueError, OverflowError):
        deviation_text = "не рассчитано"

    target_rows: list[str] = ["🎯 <b>Цели после нормализации:</b>"]
    max_rows = min(len(normalized_targets), 9)
    for idx in range(max_rows):
        before = original_targets[idx] if idx < len(original_targets) else "?"
        after = normalized_targets[idx]
        marker = (
            "└" if idx == max_rows - 1 and len(normalized_targets) <= max_rows else "├"
        )
        target_rows.append(
            f"{marker} TP{idx +1}: <code>{fmt_price(before)} → {fmt_price(after)}</code>"
        )
    if len(normalized_targets) > max_rows:
        target_rows.append(f"└ ещё целей: {len(normalized_targets)-max_rows}")
    if len(normalized_targets) == 0:
        target_rows.append("└ не рассчитаны")

    return card(
        "⚠️ <b>СИГНАЛ ПОХОЖ НА ОШИБКУ ДЕСЯТИЧНОГО РАЗРЯДА</b>",
        symbol=signal.symbol,
        side=signal.side.value,
        blocks=(
            [
                "👑 <b>Админская диагностика</b>",
                "Обычным пользователям эта карточка не отправляется.",
            ],
            [
                f"📊 <b>Текущая цена BingX:</b> {fmt_price(current)}",
                f"🧮 <b>Найден единый множитель:</b> <code>{esc(factor)}</code>",
                f"📐 <b>Отклонение после нормализации:</b> {deviation_text}",
            ],
            [
                f"💵 <b>Вход:</b> <code>{fmt_price(original_entry)} → {fmt_price(normalized_entry)}</code>",
                f"🛡 <b>STOP:</b> <code>{fmt_price(original_stop)} → {fmt_price(normalized_stop)}</code>",
            ],
            target_rows,
            [
                "🔒 Сделка не отправлена автоматически.",
                "Для авто-режима нужна отдельная настройка и дополнительные тесты.",
            ],
        ),
    )


def user_result_message(signal: Signal, result: ExecutionResult) -> str:
    exchange = str(result.payload.get("exchange") or "mexc").lower()
    title = exchange_title(exchange)
    side = signal.side.value

    if _is_unsupported_symbol_result(result):
        return unsupported_symbol_message(signal, result)

    if (
        result.status == "skipped"
        and result.payload.get("signal_price_anomaly") is True
    ):
        return signal_price_anomaly_message(signal, result)

    if result.status == "opened":
        sizing = result.payload.get("sizing")
        targets = list(result.payload.get("targets") or [])
        pcts = list(result.payload.get("tp_percents") or [])
        pending_limit = bool(result.payload.get("pending_limit"))
        header = (
            "🔵 <b>LIMIT-ОРДЕР РАЗМЕЩЁН</b>"
            if pending_limit
            else "🟢 <b>ПОЗИЦИЯ ОТКРЫТА</b>"
        )

        facts = [
            f"💵 <b>Вход:</b> {_entry_text(signal, result.payload.get('actual_entry'))}"
        ]
        if sizing:
            facts.append(f"📦 <b>Объём:</b> {fmt_qty(sizing.qty)}")
            effective = result.payload.get("effective_leverage") or sizing.leverage
            facts.append(f"⚙️ <b>Плечо:</b> {int(effective)}x")
            risk_pct = result.payload.get("risk_percent")
            actual_risk = max(0.0, float(getattr(sizing, "risk_usdt", 0.0) or 0.0))
            target_risk = max(
                0.0,
                float(getattr(sizing, "target_risk_usdt", 0.0) or 0.0),
            )
            if target_risk <= 0 and risk_pct is not None:
                try:
                    target_risk = float(sizing.balance_usdt) * float(risk_pct) / 100.0
                except (TypeError, ValueError, ZeroDivisionError):
                    target_risk = actual_risk
            try:
                actual_risk_pct = (
                    actual_risk / float(sizing.balance_usdt) * 100.0
                    if float(sizing.balance_usdt) > 0
                    else 0.0
                )
            except (TypeError, ValueError, ZeroDivisionError):
                actual_risk_pct = 0.0

                # Keep the compact legacy line when lot rounding changed the budget
                # only microscopically.  A material reduction is shown explicitly so
                # users never confuse the configured cap with the executable risk.
            material_rounding = (
                target_risk > 0
                and abs(target_risk - actual_risk) / target_risk >= 0.005
            )
            if risk_pct is None:
                facts.append(f"📊 <b>Фактический риск:</b> {fmt_usdt(actual_risk)}")
            elif material_rounding:
                facts.extend(
                    [
                        f"📊 <b>Заданный риск:</b> {fmt_percent(risk_pct)} • {fmt_usdt(target_risk)}",
                        f"📐 <b>Фактический риск:</b> {fmt_percent(actual_risk_pct)} • {fmt_usdt(actual_risk)}",
                    ]
                )
            else:
                facts.append(
                    f"📊 <b>Риск:</b> {fmt_percent(risk_pct)} • {fmt_usdt(actual_risk)}"
                )

                # v1.6.18: MARKET fills can slip from the pre-trade reference price
                # used for sizing. actual_risk above already reflects any qty-step
                # rounding, but not slippage -- realized_risk_usdt (set only for
                # MARKET entries, using the confirmed fill price) does. Only show
                # this extra line when the gap is large enough to matter; routine
                # spread-crossing on liquid pairs should not add noise to every
                # single MARKET trade notification.
            realized_risk = result.payload.get("realized_risk_usdt")
            realized_risk_pct = result.payload.get("realized_risk_percent")
            slippage_pct = result.payload.get("market_slippage_percent")
            if realized_risk is not None:
                try:
                    realized_risk_f = max(0.0, float(realized_risk))
                    material_slippage = (
                        actual_risk > 0
                        and abs(realized_risk_f - actual_risk) / actual_risk >= 0.05
                    )
                except (TypeError, ValueError, ZeroDivisionError):
                    material_slippage = False
                if material_slippage:
                    facts.append(
                        f"📉 <b>Риск по факту входа:</b> "
                        f"{fmt_percent(realized_risk_pct)} • {fmt_usdt(realized_risk_f)} "
                        f"(проскальзывание {fmt_percent(abs(float(slippage_pct or 0.0)))})"
                    )

        protection = [f"🛡 <b>STOP:</b> {_stop_text(signal, result)}"]
        if pending_limit:
            # Before a LIMIT order reaches a terminal fill, the final executable
            # TP slices are not known yet. Do not display provisional percentages
            # as if those orders already existed on BingX.
            target_rows = _targets_block(targets, [])
            if target_rows:
                target_rows[0] = "🎯 <b>Цели из сигнала:</b>"
            protection.extend(target_rows)
        else:
            protection.extend(_targets_block(targets, pcts))

        status_lines: list[str] = []
        if pending_limit:
            context = result.payload.get("limit_context") or {}
            status_lines.append("✅ STOP установлен до исполнения входа")
            if context.get("status") == "ok":
                current = fmt_price(context.get("current_price"))
                if context.get("waiting_pullback"):
                    status_lines.append(
                        f"⏳ Ожидается откат к входу • рынок {current}"
                    )
                elif context.get("may_fill_immediately"):
                    status_lines.append(
                        f"⚠️ Ордер может исполниться сразу • рынок {current}"
                    )
                else:
                    status_lines.append("⏳ Ожидается исполнение LIMIT")
            else:
                status_lines.append("⏳ Ожидается исполнение LIMIT")
            status_lines.append("🎯 TP будут поставлены после окончательного fill")
            status_lines.append(
                "📐 Фактические объёмы TP будут зафиксированы один раз после fill"
            )
        else:
            if bool(result.payload.get("protected_pending_tp")):
                status_lines.extend(
                    [
                        "✅ STOP подтверждён",
                        f"⏳ TP ставятся фоном: {len(targets)}",
                        "🔒 Позиция уже защищена STOP",
                    ]
                )
            else:
                status_lines.extend(
                    [
                        "✅ STOP подтверждён",
                        f"✅ TP установлены: {len(targets)}",
                        "🔒 Позиция полностью защищена",
                    ]
                )

        norm = result.payload.get("price_normalization") or {}
        normalization_lines: list[str] = []
        if norm:
            normalization_lines.append("🧮 <b>Шаг цены BingX применён</b>")
            if norm.get("original_entry") != norm.get("entry"):
                normalization_lines.append(
                    f"├ Вход: {fmt_price(norm.get('original_entry'))} → {fmt_price(norm.get('entry'))}"
                )
            if norm.get("original_stop") != norm.get("stop"):
                normalization_lines.append(
                    f"└ STOP: {fmt_price(norm.get('original_stop'))} → {fmt_price(norm.get('stop'))}"
                )

        warning_lines: list[str] = []
        if result.payload.get("leverage_warning"):
            warning_lines.append(
                f"⚠️ {esc(result.payload.get('leverage_warning'), limit=300)}"
            )
        if result.payload.get("tp_trimmed"):
            trim = result.payload["tp_trimmed"]
            warning_lines.append(
                f"⚠️ Из-за шага объёма поставлено TP: {int(trim.get('placed')or 0)}/{int(trim.get('requested')or 0)}"
            )

        blocks: list[list[str]] = [facts, protection, status_lines]
        if normalization_lines:
            blocks.append(normalization_lines)
        if warning_lines:
            blocks.append(warning_lines)
        return card(header, symbol=signal.symbol, side=side, blocks=blocks)

    if result.payload.get("market_protection_failed"):
        actual_entry = result.payload.get("actual_entry")
        opened_qty = result.payload.get("opened_qty")
        if result.payload.get("emergency_close_confirmed"):
            return card(
                "🛡 <b>ПОЗИЦИЯ АВАРИЙНО ЗАКРЫТА</b>",
                symbol=signal.symbol,
                side=side,
                blocks=(
                    [
                        f"💵 <b>Вход:</b> {_entry_text(signal, actual_entry)}",
                        f"📦 <b>Закрытый объём:</b> {fmt_qty(opened_qty)}",
                    ],
                    [
                        "🛡 BingX-вход аварийно закрыт",
                        "❌ Защитный STOP не подтвердился",
                        "✅ Бот закрыл весь новый объём market reduce-only",
                        "🔒 Аварийное закрытие подтверждено",
                    ],
                    [details_line(result.reason)],
                ),
            )
        return card(
            "🚨 <b>СРОЧНАЯ ПРОВЕРКА BingX</b>",
            symbol=signal.symbol,
            side=side,
            blocks=(
                [
                    f"💵 <b>Вход:</b> {_entry_text(signal, actual_entry)}",
                    f"📦 <b>Возможный объём:</b> {fmt_qty(opened_qty)}",
                ],
                [
                    "❌ STOP не подтверждён",
                    "❌ Аварийное закрытие не подтверждено",
                    "⚠️ Возможна открытая позиция без защиты",
                ],
                [
                    "🔴 <b>Сделайте прямо сейчас:</b>",
                    *tree_lines(
                        [
                            "Откройте BingX",
                            f"Проверьте позицию {esc(signal.symbol.upper())}",
                            "Проверьте STOP и TP",
                            "При необходимости закройте позицию вручную",
                        ]
                    ),
                ],
                [details_line(result.reason)],
            ),
        )

    if (
        result.status == "skipped"
        and result.payload.get("api_permission_quarantine") is True
        and result.payload.get("api_quarantine_active") is True
    ):
        return card(
            "🔐 <b>BINGX API ПРИОСТАНОВЛЕН</b>",
            symbol=signal.symbol,
            side=side,
            blocks=(
                [
                    "⏭ Сделка не отправлялась на биржу",
                    "🛡 Бот отключил торговые попытки этим API-ключом",
                    f"🧾 <b>Код BingX:</b> <code>{esc(result.payload.get('api_quarantine_error_code') or '100004')}</code>",
                ],
                [
                    "🔧 <b>Что необходимо сделать:</b>",
                    *tree_lines(
                        [
                            "Открыть BingX → API Management",
                            "Включить Read permission",
                            "Включить Futures Trading",
                            "Заново подключить API-ключ в боте",
                        ]
                    ),
                ],
                [details_line(result.reason)],
            ),
        )

    if result.status == "preview":
        targets = list(result.payload.get("targets") or signal.targets or [])
        pcts = list(result.payload.get("tp_percents") or [])
        return card(
            "👁 <b>СИГНАЛ В РЕЖИМЕ ПРОСМОТРА</b>",
            symbol=signal.symbol,
            side=side,
            blocks=(
                [
                    f"💵 <b>Вход:</b> {_entry_text(signal)}",
                    f"🛡 <b>STOP:</b> {_stop_text(signal, result)}",
                ],
                _targets_block(targets, pcts),
                [
                    "🚫 Реальная сделка не открывалась",
                    "ℹ️ Включите режим «Авто» для исполнения",
                ],
            ),
        )

    if result.status == "skipped" and result.payload.get("symbol_unavailable"):
        reason_text = str(result.reason or "").lower()
        permission_error = any(
            marker in reason_text
            for marker in ("no permission", "отказывает", "permission", "kyc", "-1058")
        )
        if permission_error and exchange == "mexc":
            return card(
                "🟡 <b>НЕДОСТАТОЧНО ПРАВ API</b>",
                symbol=signal.symbol,
                side=side,
                blocks=(
                    ["🔐 BingX не разрешила торговлю по этой паре"],
                    [
                        "🔧 <b>Проверьте:</b>",
                        *tree_lines(
                            [
                                "KYC аккаунта",
                                "Разрешение Order Placing / Futures Trading",
                                "Повторное подключение API после изменения прав",
                            ]
                        ),
                    ],
                    [details_line(result.reason)],
                ),
            )
        return card(
            "🟡 <b>ТОРГОВАЯ ПАРА НЕДОСТУПНА</b>",
            symbol=signal.symbol,
            side=side,
            blocks=(
                [f"🏦 Биржа: <b>{esc(title)}</b>", "⏭ Сделка пропущена"],
                [
                    "ℹ️ Контракт может быть приостановлен, переименован или недоступен через API"
                ],
                [details_line(result.reason)],
            ),
        )

    if result.status == "partial_error":
        requested_targets = list(result.payload.get("targets") or [])
        confirmed_orders = list(result.payload.get("tp_orders") or [])
        confirmed_count = len(confirmed_orders)
        requested_count = len(requested_targets)
        return card(
            "🟡 <b>ЗАЩИТА УСТАНОВЛЕНА ЧАСТИЧНО</b>",
            symbol=signal.symbol,
            side=side,
            blocks=(
                [
                    f"💵 <b>Вход:</b> {_entry_text(signal, result.payload.get('actual_entry'))}",
                    f"🛡 <b>STOP:</b> {_stop_text(signal, result)}",
                ],
                [
                    "✅ Позиция открыта",
                    "✅ STOP установлен",
                    f"🎯 <b>TP подтверждено:</b> {confirmed_count}/{requested_count}",
                    (
                        "⚠️ Часть TP не подтверждена"
                        if confirmed_count < requested_count
                        else "⚠️ Состояние TP требует повторной проверки"
                    ),
                    "🔄 Бот запустил автоматическое восстановление",
                ],
                [details_line(result.reason)],
            ),
        )

    if result.status == "manual_required":
        return card(
            "🚨 <b>ТРЕБУЕТСЯ РУЧНАЯ ПРОВЕРКА</b>",
            symbol=signal.symbol,
            side=side,
            blocks=(
                [
                    f"💵 <b>Вход:</b> {_entry_text(signal, result.payload.get('actual_entry'))}",
                    f"🛡 <b>STOP:</b> {_stop_text(signal, result)}",
                ],
                [
                    "❓ Бот не подтвердил полную защиту",
                    "📱 Проверьте позицию, STOP и TP на BingX",
                    "🚫 Не открывайте повторную сделку до проверки",
                ],
                [details_line(result.reason)],
            ),
        )

    if (
        result.status == "skipped"
        and result.payload.get("notification_kind")
        == MANDATORY_TRADE_WARNING_NOTIFICATION_KIND
    ):
        return card(
            "🔴 <b>СДЕЛКА НЕ ОТКРЫТА</b>",
            symbol=signal.symbol,
            side=side,
            blocks=(
                [
                    f"💵 <b>Вход:</b> {_entry_text(signal, result.payload.get('actual_entry'))}",
                    f"🛡 <b>STOP:</b> {_stop_text(signal, result)}",
                    f"🎯 <b>Целей:</b> {len(signal.targets)}",
                ],
                [f"🏦 <b>Биржа:</b> {esc(title)}", details_line(result.reason)],
                [
                    "⚠️ Причина требует внимания и не отключается настройкой",
                    "🔧 Проверьте API, ограничения, баланс и параметры сигнала",
                ],
            ),
        )

    if result.status == "skipped":
        return card(
            "⏭ <b>СИГНАЛ ПРОПУЩЕН</b>",
            symbol=signal.symbol,
            side=side,
            blocks=(
                [
                    f"💵 <b>Вход:</b> {_entry_text(signal, result.payload.get('actual_entry'))}",
                    f"🛡 <b>STOP:</b> {_stop_text(signal, result)}",
                    f"🎯 <b>Целей:</b> {len(signal.targets)}",
                ],
                [f"ℹ️ <b>Причина:</b> {esc(result.reason, limit=500)}"],
            ),
        )

    return card(
        "🔴 <b>СДЕЛКА НЕ ОТКРЫТА</b>",
        symbol=signal.symbol,
        side=side,
        blocks=(
            [
                f"💵 <b>Вход:</b> {_entry_text(signal, result.payload.get('actual_entry'))}",
                f"🛡 <b>STOP:</b> {_stop_text(signal, result)}",
                f"🎯 <b>Целей:</b> {len(signal.targets)}",
            ],
            [f"🏦 <b>Биржа:</b> {esc(title)}", details_line(result.reason)],
            ["🔧 Проверьте API, баланс, права и параметры сигнала"],
        ),
    )


def admin_batch_summary(signal: Signal, results: list[ExecutionResult]) -> str:
    counts: dict[str, int] = {}
    for result in results:
        counts[result.status] = counts.get(result.status, 0) + 1

    pending_limit = sum(
        1
        for result in results
        if result.status == "opened" and bool(result.payload.get("pending_limit"))
    )
    opened_positions = max(0, counts.get("opened", 0) - pending_limit)

    summary = [
        f"👥 <b>Получателей:</b> {len(results)}",
        f"✅ <b>Позиции открыты:</b> {opened_positions}",
    ]
    if pending_limit:
        summary.append(f"⏳ <b>LIMIT ожидает:</b> {pending_limit}")
    summary.extend(
        [
            f"👁 <b>Preview:</b> {counts.get('preview',0)}",
            f"⏭ <b>Пропущено:</b> {counts.get('skipped',0)}",
        ]
    )
    api_quarantined = sum(
        1
        for result in results
        if result.payload.get("api_permission_quarantine") is True
        and result.payload.get("api_quarantine_active") is True
    )
    if api_quarantined:
        summary.append(f"🔐 <b>API приостановлено:</b> {api_quarantined}")
    if counts.get("partial_error", 0):
        summary.append(f"🟡 <b>Частично:</b> {counts.get('partial_error',0)}")
    if counts.get("manual_required", 0):
        summary.append(
            f"🚨 <b>Ручная проверка:</b> {counts.get('manual_required',0)}"
        )
    if counts.get("error", 0):
        summary.append(f"❌ <b>Ошибки:</b> {counts.get('error',0)}")

    dm_unavailable = sum(
        1 for result in results if result.payload.get("dm_unavailable") is True
    )
    if dm_unavailable:
        summary.append(
            f"📵 <b>ЛС недоступны - сделки не открыты:</b> {dm_unavailable}"
        )

    queue_full = sum(
        1
        for result in results
        if (result.payload.get("dispatch") or {}).get("rejected") == "queue_full"
    )
    queue_stale = sum(
        1
        for result in results
        if (result.payload.get("dispatch") or {}).get("rejected") == "queue_stale"
    )
    price_deviation = sum(
        1 for result in results if result.payload.get("market_price_deviation") is True
    )
    signal_price_anomaly = sum(
        1 for result in results if result.payload.get("signal_price_anomaly") is True
    )
    if queue_full:
        summary.append(f"🧱 <b>Очередь переполнена:</b> {queue_full}")
    if queue_stale:
        summary.append(f"⌛ <b>Устарели в очереди:</b> {queue_stale}")
    if price_deviation:
        summary.append(f"📉 <b>Цена ушла дальше лимита:</b> {price_deviation}")
    if signal_price_anomaly:
        summary.append(f"⛔️ <b>Аномальная цена сигнала:</b> {signal_price_anomaly}")

    notify_failed = sum(
        1
        for result in results
        if result.payload.get("notification_delivered") is False
        and not result.payload.get("dm_unavailable")
    )
    if notify_failed:
        summary.append(f"🚨 <b>Не доставлено после исполнения:</b> {notify_failed}")

    dispatch_rows = [
        dict(result.payload.get("dispatch") or {})
        for result in results
        if (result.payload.get("dispatch") or {}).get("queue_wait_ms") is not None
    ]
    timing: list[str] = []
    if dispatch_rows:
        queue_values = [int(row.get("queue_wait_ms") or 0) for row in dispatch_rows]
        exec_values = [int(row.get("execution_ms") or 0) for row in dispatch_rows]
        total_values = [
            int(row.get("signal_to_result_ms") or 0)
            for row in dispatch_rows
            if row.get("signal_to_result_ms") is not None
        ]
        peak_workers = max(int(row.get("peak_active") or 0) for row in dispatch_rows)
        timing.extend(
            [
                f"⏱ <b>Очередь:</b> ср. {sum(queue_values)/len(queue_values)/1000:.2f} сек • max {max(queue_values)/1000:.2f} сек",
                f"⚙️ <b>Исполнение:</b> ср. {sum(exec_values)/len(exec_values)/1000:.2f} сек • max {max(exec_values)/1000:.2f} сек",
                f"👷 <b>Пиковая параллельность:</b> {peak_workers}",
            ]
        )
        if total_values:
            timing.append(
                f"🏁 <b>Сигнал → результат:</b> max {max(total_values)/1000:.2f} сек"
            )

    blocks: list[list[str]] = [summary]
    if timing:
        blocks.append(timing)
    return card(
        "📥 <b>VIP-СИГНАЛ ОБРАБОТАН</b>",
        symbol=signal.symbol,
        side=signal.side.value,
        blocks=blocks,
    )


def system_api_connected_message() -> str:
    return system_message(
        "🟢 <b>BingX API ПОДКЛЮЧЁН</b>",
        [
            "🔑 Ключ проверен и зашифрован",
            "✅ Доступ к фьючерсному счёту подтверждён",
            "🤖 Для торговли включите режим «Авто»",
            "🛡 Доступ White-list выдаёт администратор",
        ],
    )


def system_api_disabled_message() -> str:
    return system_message(
        "🔵 <b>BingX API ОТКЛЮЧЁН</b>",
        [
            "🔌 Ключ больше не используется ботом",
            "👁 Режим автоматически переведён в «Просмотр»",
            "🔒 Новые реальные сделки открываться не будут",
        ],
    )


def system_api_error_message(
    reason: object | None = None, *, rejected: bool = False
) -> str:
    lines = [
        "❌ Ключи не сохранены",
        "🔑 Проверьте API KEY и API SECRET",
        "⚙️ Проверьте разрешение Futures / Order Placing",
    ]
    if rejected:
        lines.insert(0, "🏦 BingX отклонила проверку API")
    else:
        lines.insert(0, "🌐 Не удалось подтвердить соединение с BingX")
    if reason:
        lines.append(details_line(reason, limit=350))
    return system_message("🔴 <b>BingX API НЕ ПОДКЛЮЧЁН</b>", lines)


def system_mode_message(mode: str) -> str:
    mode_l = str(mode).lower()
    if mode_l == "auto":
        lines = [
            "🟢 <b>Новый режим:</b> Авто",
            "🤖 VIP-сигналы будут исполняться автоматически",
        ]
    elif mode_l == "preview":
        lines = [
            "👁 <b>Новый режим:</b> Просмотр",
            "🚫 Реальные сделки открываться не будут",
        ]
    else:
        lines = [
            "⏸ <b>Новый режим:</b> Выключено",
            "🚫 Новые сигналы будут игнорироваться",
        ]
    return system_message("🤖 <b>РЕЖИМ ТОРГОВЛИ ИЗМЕНЁН</b>", lines)


def system_risk_message(
    label: str, value: object, extra: list[str] | None = None
) -> str:
    lines = [f"📊 <b>{esc(label)}:</b> {esc(value)}"]
    if extra:
        lines.extend(extra)
    return system_message("📊 <b>РИСК ОБНОВЛЁН</b>", lines)


def system_tp_message(label: str, value: object, extra: list[str] | None = None) -> str:
    lines = [f"🎯 <b>{esc(label)}:</b> {esc(value)}"]
    if extra:
        lines.extend(extra)
    return system_message("🎯 <b>СХЕМА TP ОБНОВЛЕНА</b>", lines)


def system_be_message(trigger: int) -> str:
    if int(trigger) <= 0:
        lines = [
            "🚫 <b>Умное Б/У:</b> выключено",
            "🛡 Исходный STOP останется без автоматического переноса",
        ]
    else:
        hints = {
            1: "Самый защитный вариант",
            2: "Сбалансированный вариант",
            3: "Более спокойный вариант",
        }
        lines = [
            f"🛡 <b>Перенос STOP:</b> после TP{int(trigger)}",
            f"ℹ️ {hints.get(int(trigger),'')}",
        ]
    return system_message("🛡 <b>РЕЖИМ Б/У ОБНОВЛЁН</b>", lines)


def whitelist_preview_message(signal: Signal) -> str:
    return card(
        "👁 <b>СИГНАЛ В РЕЖИМЕ ПРОСМОТРА</b>",
        symbol=signal.symbol,
        side=signal.side.value,
        blocks=(
            [
                f"💵 <b>Вход:</b> {_entry_text(signal)}",
                f"🛡 <b>STOP:</b> {fmt_price(signal.stop)}",
                f"🎯 <b>Целей:</b> {len(signal.targets)}",
            ],
            [
                "🚫 Реальная сделка не открывалась",
                "🔐 Для автоторговли нужен доступ White-list",
                "👤 Управление White-list доступно только администратору",
            ],
        ),
    )
