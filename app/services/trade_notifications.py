"""Unified visual notifications for trade lifecycle events."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

from app.services.notification_style import (
    card,
    duration_text,
    esc,
    fmt_percent,
    fmt_price,
    fmt_qty,
    fmt_usdt,
    premium_kv_block,
    premium_section,
)


def _fmt_usdt(value: float, with_sign: bool = True) -> str:
    return fmt_usdt(value, signed=with_sign)


def _fmt_pct(value: float) -> str:
    return fmt_percent(value, signed=True)


def _calc_pnl(
    side: str, entry: float, exit_price: float, qty: float, leverage: float = 1.0
) -> float:
    if entry <= 0 or exit_price <= 0 or qty <= 0:
        return 0.0
    return (
        (exit_price - entry) * qty
        if side.lower() == "long"
        else (entry - exit_price) * qty
    )


def _calc_pnl_pct(
    side: str, entry: float, exit_price: float, leverage: float = 1.0
) -> float:
    if entry <= 0 or exit_price <= 0:
        return 0.0
    raw_pct = (exit_price - entry) / entry * 100.0
    if side.lower() == "short":
        raw_pct = -raw_pct
    return raw_pct * max(1.0, leverage)


def _r_result(total_pnl: float, entry: float, stop: float, qty: float) -> str:
    risk = abs(entry - stop) * qty
    if risk <= 1e-12:
        return ""
    return f"{total_pnl / risk:+.2f}R".replace("-", "−")


def _parse_event_time(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        parsed = value
    elif value in (None, "") or isinstance(value, bool):
        return None
    else:
        text = str(value).strip()
        try:
            if isinstance(value, (int, float)) or (
                text.replace(".", "", 1).isdigit() and len(text.split(".", 1)[0]) >= 10
            ):
                epoch = float(value)
                magnitude = abs(epoch)
                if magnitude >= 1e14:
                    epoch /= 1_000_000.0
                elif magnitude >= 1e11:
                    epoch /= 1_000.0
                parsed = datetime.fromtimestamp(epoch, tz=timezone.utc)
            else:
                parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
        except (TypeError, ValueError, OverflowError, OSError):
            return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _fmt_event_time_msk(value: Any) -> str:
    parsed = _parse_event_time(value)
    if parsed is None:
        return ""
    msk = parsed.astimezone(timezone(timedelta(hours=3)))
    return msk.strftime("%d.%m.%Y %H:%M:%S МСК")


def _fmt_latency(start: Any, end: Any) -> str:
    started = _parse_event_time(start)
    finished = _parse_event_time(end)
    if started is None or finished is None:
        return ""
    seconds = max(0.0, (finished - started).total_seconds())
    if seconds < 60:
        return f"{seconds:.1f} сек"
    minutes, remainder = divmod(int(round(seconds)), 60)
    if minutes < 60:
        return f"{minutes} мин {remainder} сек"
    hours, minutes = divmod(minutes, 60)
    return f"{hours} ч {minutes} мин"


def tp_filled_message(
    symbol: str,
    side: str,
    tp_index: int,
    tp_price: float,
    qty: float,
    entry: float,
    total_tps: int,
    leverage: float = 1.0,
    *,
    remaining_qty: float | None = None,
    cumulative_pnl: float | None = None,
    fill_source: str = "position_qty_reduction",
    filled_at: Any = None,
    detected_at: Any = None,
) -> str:
    pnl_usdt = _calc_pnl(side, entry, tp_price, qty)
    pnl_pct = _calc_pnl_pct(side, entry, tp_price, leverage)
    fact_rows = [
        ("💵 Цена фиксации", fmt_price(tp_price)),
        ("📦 Закрытый объём", fmt_qty(qty)),
        ("💰 Расчётный PnL цели", fmt_usdt(pnl_usdt, signed=True)),
        ("📊 ROI к марже", f"≈{fmt_percent(pnl_pct, signed=True)}"),
    ]
    if cumulative_pnl is not None:
        fact_rows.append(("💰 Расчётно зафиксировано", fmt_usdt(cumulative_pnl, signed=True)))
    if remaining_qty is not None:
        fact_rows.append(("📦 Остаток позиции", fmt_qty(remaining_qty)))
    facts = premium_kv_block(fact_rows)
    timing_rows: list[tuple[str, str]] = []
    filled_text = _fmt_event_time_msk(filled_at)
    detected_text = _fmt_event_time_msk(detected_at)
    latency_text = _fmt_latency(filled_at, detected_at)
    if filled_text:
        timing_rows.append(("🕒 Исполнено на BingX", filled_text))
    if detected_text:
        timing_rows.append(("🤖 Обнаружено ботом", detected_text))
    if latency_text:
        timing_rows.append(("⏱ Задержка обнаружения", latency_text))
    timing = premium_kv_block(timing_rows) if timing_rows else None
    source_text = (
        "🔎 Исполнение подтверждено по истории TP-ордера BingX"
        if str(fill_source or "").lower() == "mexc_stoporder_history"
        else "🔎 Исполнение подтверждено по уменьшению позиции и касанию цели"
    )
    return card(
        f"🎯 <b>TP{int(tp_index)} СРАБОТАЛ</b>",
        symbol=symbol,
        side=side,
        blocks=tuple(
            block
            for block in (
                facts,
                timing,
                premium_section(
                "✅ <b>Статус</b>",
                f"Прогресс фиксации: TP{int(tp_index)}/{int(total_tps)}",
                source_text,
                    "🧾 PnL указан без комиссий и фандинга",
                ),
            )
            if block is not None
        ),
    )


def be_set_message(
    symbol: str,
    side: str,
    be_price: float,
    entry: float,
    realized_pnl_usdt: float,
    tp_count_done: int,
    *,
    old_stop: float | None = None,
    remaining_qty: float | None = None,
    tp_filled_at: Any = None,
    be_confirmed_at: Any = None,
) -> str:
    price_rows = [("🎯 Условие", f"сработал TP{int(tp_count_done)}")]
    if old_stop and old_stop > 0:
        price_rows.append(("🔻 Старый STOP", fmt_price(old_stop)))
    price_rows.extend(
        (
            ("⚖️ Новый STOP", fmt_price(be_price)),
            ("💵 Фактический вход", fmt_price(entry)),
        )
    )
    prices = premium_kv_block(price_rows)
    result_rows = [("💰 Расчётно зафиксировано", fmt_usdt(realized_pnl_usdt, signed=True))]
    if remaining_qty is not None:
        result_rows.append(("📦 Остаток позиции", fmt_qty(remaining_qty)))
    result = premium_kv_block(result_rows)
    timing_rows: list[tuple[str, str]] = []
    tp_time_text = _fmt_event_time_msk(tp_filled_at)
    be_time_text = _fmt_event_time_msk(be_confirmed_at)
    latency_text = _fmt_latency(tp_filled_at, be_confirmed_at)
    if tp_time_text:
        timing_rows.append(("🕒 TP исполнен", tp_time_text))
    if be_time_text:
        timing_rows.append(("🛡 Б/У подтверждён", be_time_text))
    if latency_text:
        timing_rows.append(("⏱ TP → Б/У", latency_text))
    timing = premium_kv_block(timing_rows) if timing_rows else None
    return card(
        "⚖️ <b>СТОП ПЕРЕНЕСЁН В Б/У</b>",
        symbol=symbol,
        side=side,
        blocks=tuple(
            block
            for block in (
                prices,
                result,
                timing,
                premium_section(
                    "✅ <b>Статус</b>",
                    "Основной риск снят.",
                    "Остаток позиции защищён.",
                ),
            )
            if block is not None
        ),
    )


def stop_loss_hit_message(
    symbol: str,
    side: str,
    stop_price: float,
    entry: float,
    qty: float,
    realized_pnl_before: float = 0.0,
    *,
    created_at: object = None,
) -> str:
    loss = _calc_pnl(side, entry, stop_price, qty)
    total = loss + realized_pnl_before
    r_value = _r_result(total, entry, stop_price, qty)
    duration = duration_text(created_at)
    stats = [
        f"💵 <b>Цена входа:</b> {fmt_price(entry)}",
        f"🛡 <b>Цена STOP:</b> {fmt_price(stop_price)}",
        f"📦 <b>Закрытый объём:</b> {fmt_qty(qty)}",
        f"🔻 <b>Убыток по STOP:</b> {fmt_usdt(loss, signed=True)}",
    ]
    if realized_pnl_before:
        stats.append(
            f"💰 <b>До STOP зафиксировано:</b> {fmt_usdt(realized_pnl_before, signed=True)}"
        )
    stats.append(f"📊 <b>Расчётный PnL:</b> {fmt_usdt(total, signed=True)}")
    if r_value:
        stats.append(f"📐 <b>Результат:</b> {r_value}")
    if duration:
        stats.append(f"⏱ <b>Продолжительность:</b> {duration}")
    return card(
        "📉 <b>СДЕЛКА ЗАКРЫТА ПО STOP</b>",
        symbol=symbol,
        side=side,
        blocks=(stats, ["🧾 Расчёт без комиссий и фандинга"]),
    )


def be_stop_hit_message(
    symbol: str,
    side: str,
    stop_price: float,
    entry: float,
    qty: float,
    realized_pnl_before: float = 0.0,
    *,
    created_at: object = None,
) -> str:
    be_pnl = _calc_pnl(side, entry, stop_price, qty)
    total = be_pnl + realized_pnl_before
    stats = [
        f"💵 <b>Цена входа:</b> {fmt_price(entry)}",
        f"🔒 <b>Цена BE-STOP:</b> {fmt_price(stop_price)}",
        f"📦 <b>Закрытый объём:</b> {fmt_qty(qty)}",
        f"💰 <b>PnL по BE:</b> {fmt_usdt(be_pnl, signed=True)}",
    ]
    if realized_pnl_before:
        stats.append(
            f"🎯 <b>Через TP:</b> {fmt_usdt(realized_pnl_before, signed=True)}"
        )
    stats.append(f"📊 <b>Расчётный PnL:</b> {fmt_usdt(total, signed=True)}")
    duration = duration_text(created_at)
    if duration:
        stats.append(f"⏱ <b>Продолжительность:</b> {duration}")
    return card(
        "🛡 <b>СДЕЛКА ЗАКРЫТА В Б/У</b>",
        symbol=symbol,
        side=side,
        blocks=(
            stats,
            ["✅ Основной риск был снят заранее", "🧾 Расчёт без комиссий и фандинга"],
        ),
    )


def full_tp_close_message(
    symbol: str,
    side: str,
    total_pnl: float,
    tps_hit: int,
    total_tps: int,
    *,
    entry: float = 0.0,
    exit_price: float = 0.0,
    stop: float = 0.0,
    qty: float = 0.0,
    created_at: object = None,
) -> str:
    stats = [
        f"🎯 <b>Сработало целей:</b> {int(tps_hit)}/{int(total_tps)}",
        f"💰 <b>Расчётный PnL:</b> {fmt_usdt(total_pnl, signed=True)}",
    ]
    if entry > 0:
        stats.insert(0, f"💵 <b>Средний вход:</b> {fmt_price(entry)}")
    if exit_price > 0:
        stats.insert(1, f"📍 <b>Цена при проверке:</b> {fmt_price(exit_price)}")
    r_value = _r_result(total_pnl, entry, stop, qty)
    if r_value:
        stats.append(f"📐 <b>Результат:</b> {r_value}")
    duration = duration_text(created_at)
    if duration:
        stats.append(f"⏱ <b>Продолжительность:</b> {duration}")
    return card(
        "🏆 <b>СДЕЛКА ЗАКРЫТА ПО TP</b>",
        symbol=symbol,
        side=side,
        blocks=(
            stats,
            [
                "✅ Позиция закрыта",
                "🧹 Остаточные ордера удалены",
                "🧾 Расчёт без комиссий и фандинга",
            ],
        ),
    )


def position_closed_message(
    symbol: str,
    side: str,
    realized_pnl: float = 0.0,
    tps_hit: int = 0,
    total_tps: int = 0,
    close_reason: str = "",
    *,
    entry: float = 0.0,
    exit_price: float = 0.0,
    stop: float = 0.0,
    qty: float = 0.0,
    created_at: object = None,
) -> str:
    if realized_pnl > 0.01:
        title = "✅ <b>СДЕЛКА ЗАКРЫТА В ПЛЮС</b>"
    elif realized_pnl < -0.01:
        title = "📉 <b>СДЕЛКА ЗАКРЫТА В МИНУС</b>"
    else:
        title = "🟰 <b>СДЕЛКА ЗАКРЫТА В НОЛЬ</b>"
    stats: list[str] = []
    if entry > 0:
        stats.append(f"💵 <b>Вход:</b> {fmt_price(entry)}")
    if exit_price > 0:
        stats.append(f"📍 <b>Цена при проверке:</b> {fmt_price(exit_price)}")
    if tps_hit and total_tps:
        stats.append(f"🎯 <b>Сработало TP:</b> {int(tps_hit)}/{int(total_tps)}")
    if close_reason:
        stats.append(f"🔎 <b>Определено как:</b> {esc(close_reason, limit=300)}")
    stats.append(f"💰 <b>Расчётный PnL:</b> {fmt_usdt(realized_pnl, signed=True)}")
    r_value = _r_result(realized_pnl, entry, stop, qty)
    if r_value:
        stats.append(f"📐 <b>Результат:</b> {r_value}")
    duration = duration_text(created_at)
    if duration:
        stats.append(f"⏱ <b>Продолжительность:</b> {duration}")
    return card(
        title,
        symbol=symbol,
        side=side,
        blocks=(
            stats,
            ["🧹 Остаточные ордера удалены", "🧾 Расчёт без комиссий и фандинга"],
        ),
    )


def position_cleanup_message(
    symbol: str,
    side: str,
    realized_pnl: float,
    tps_hit: int,
    total_tps: int,
    be_was_set: bool,
    close_type: str,
    *,
    entry: float = 0.0,
    exit_price: float = 0.0,
    stop: float = 0.0,
    qty: float = 0.0,
    created_at: object = None,
) -> str:
    if close_type == "all_tps":
        return full_tp_close_message(
            symbol,
            side,
            realized_pnl,
            tps_hit,
            total_tps,
            entry=entry,
            exit_price=exit_price,
            stop=stop,
            qty=qty,
            created_at=created_at,
        )
    if close_type == "stop":
        return position_closed_message(
            symbol,
            side,
            realized_pnl,
            tps_hit,
            total_tps,
            close_reason="STOP сработал" + (f" после {tps_hit} TP" if tps_hit else ""),
            entry=entry,
            exit_price=exit_price,
            stop=stop,
            qty=qty,
            created_at=created_at,
        )
    if close_type == "be_stop":
        stats = [
            (
                f"🎯 <b>Сработало TP:</b> {int(tps_hit)}/{int(total_tps)}"
                if total_tps
                else "🎯 <b>Сработало TP:</b> 0"
            ),
            f"💰 <b>Расчётный PnL:</b> {fmt_usdt(realized_pnl, signed=True)}",
        ]
        if entry > 0:
            stats.insert(0, f"💵 <b>Вход:</b> {fmt_price(entry)}")
        if exit_price > 0:
            stats.insert(1, f"📍 <b>Цена при проверке:</b> {fmt_price(exit_price)}")
        duration = duration_text(created_at)
        if duration:
            stats.append(f"⏱ <b>Продолжительность:</b> {duration}")
        return card(
            "🛡 <b>СДЕЛКА ЗАКРЫТА В Б/У</b>",
            symbol=symbol,
            side=side,
            blocks=(
                stats,
                [
                    "✅ BE-STOP защитил остаток позиции",
                    "🧹 Остаточные ордера удалены",
                    "🧾 Расчёт без комиссий и фандинга",
                ],
            ),
        )
    return position_closed_message(
        symbol,
        side,
        realized_pnl,
        tps_hit,
        total_tps,
        close_reason="закрыта вручную или вне бота",
        entry=entry,
        exit_price=exit_price,
        stop=stop,
        qty=qty,
        created_at=created_at,
    )


def trade_opened_summary(
    symbol: str,
    side: str,
    entry: float,
    stop: float,
    leverage: int,
    qty: float,
    risk_usdt: float,
    tps: list,
    pcts: list,
) -> str:
    targets = ["🎯 <b>Take-Profit</b>"]
    for index, (target, pct) in enumerate(zip(tps, pcts), 1):
        marker = "└" if index == len(tps) else "├"
        targets.append(f"{marker} TP{index}: <b>{fmt_price(target)}</b> · {fmt_percent(pct)} объёма")
    return card(
        "🟢 <b>СДЕЛКА ОТКРЫТА</b>",
        symbol=symbol,
        side=side,
        blocks=(
            premium_kv_block(
                (
                    ("💵 Вход", fmt_price(entry)),
                    ("🛡 Stop-Loss", fmt_price(stop)),
                    ("⚙️ Риск", fmt_usdt(risk_usdt)),
                    ("📊 Объём", fmt_qty(qty)),
                    ("⚡ Плечо", f"{int(leverage)}x"),
                )
            ),
            premium_section("🛡 <b>Защита</b>", "STOP подтверждён на бирже."),
            targets,
            premium_section("✅ <b>Статус</b>", "Позиция полностью защищена."),
        ),
    )


def analyze_close_result(
    *,
    side: str,
    entry: float,
    stop: float,
    targets: list,
    original_qty: float,
    qty_now: float,
    current_price: float,
    tp_orders_payload: list,
    be_moved: bool,
    be_stop_price: float = 0.0,
) -> dict:
    """Оценить способ закрытия позиции и расчётный PnL.

    TP учитывается только по явному ``filled=True``, который lifecycle guard
    ставит после exact BingX history либо подтверждённого position-delta вместе
    с сохранённым касанием цели. Текущая цена сама по себе не доказывает fill:
    пользователь мог закрыть позицию вручную в прибыли, а TP-ордер отсутствовал.
    """
    side_l = side.lower()
    closed_qty = max(0.0, original_qty - qty_now)
    n_targets = len(targets) if targets else 0
    effective_stop = be_stop_price if (be_moved and be_stop_price > 0) else stop

    # Собираем поставленные TP в порядке близости к entry.
    placed_tps: list[tuple[float, float, bool]] = []  # (price, qty, was_filled_flag)
    if tp_orders_payload:
        for tp_entry in tp_orders_payload:
            if not isinstance(tp_entry, dict):
                continue
            tp_qty_val = float(tp_entry.get("qty") or 0.0)
            tp_price = float(tp_entry.get("target") or tp_entry.get("price") or 0.0)
            # Only the explicit monitor-confirmed fill is admissible here.
            filled_flag = tp_entry.get("filled") is True
            if tp_qty_val > 0 and tp_price > 0:
                placed_tps.append((tp_price, tp_qty_val, filled_flag))
    placed_tps.sort(key=lambda t: t[0], reverse=(side_l == "short"))

    # Never infer a TP from current_price alone. Doing so can misclassify a
    # manual close above/below the target as an exchange TP execution.
    realized_pnl_tps = 0.0
    tps_hit = 0
    consumed_qty = 0.0
    for tp_price, tp_qty, was_filled in placed_tps:
        if not was_filled:
            continue
            # Закрытый объём не может быть меньше суммы реально сработавших
        actual_qty = min(tp_qty, max(0.0, closed_qty - consumed_qty))
        if actual_qty <= 1e-9:
            break
        if side_l == "long":
            pnl = (tp_price - entry) * actual_qty
        else:
            pnl = (entry - tp_price) * actual_qty
        realized_pnl_tps += pnl
        if actual_qty >= tp_qty - 1e-9:
            tps_hit += 1
        consumed_qty += actual_qty

    remaining_closed = max(0.0, closed_qty - consumed_qty)

    if qty_now > 1e-9:
        return {
            "close_type": "partial",
            "tps_hit": tps_hit,
            "realized_pnl_tps": realized_pnl_tps,
            "final_close_pnl": 0.0,
            "total_pnl": realized_pnl_tps,
        }

        # Позиция полностью закрыта
    final_close_pnl = 0.0
    close_type = "unknown"

    if remaining_closed > 1e-9 and current_price > 0:
        # Часть объёма не покрыта TP — закрылась через STOP/BE/manual
        stop_distance = abs(current_price - effective_stop) / max(effective_stop, 1e-9)
        if stop_distance < 0.01:
            close_type = "be_stop" if be_moved else "stop"
            close_price = effective_stop
        else:
            close_type = "unknown"
            close_price = current_price
        if side_l == "long":
            final_close_pnl = (close_price - entry) * remaining_closed
        else:
            final_close_pnl = (entry - close_price) * remaining_closed
    elif tps_hit >= n_targets and n_targets > 0:
        close_type = "all_tps"
    elif tps_hit > 0:
        # Все закрытое прошло через TP но не все TP сработали (странный случай)
        close_type = "all_tps"

    return {
        "close_type": close_type,
        "tps_hit": tps_hit,
        "realized_pnl_tps": realized_pnl_tps,
        "final_close_pnl": final_close_pnl,
        "total_pnl": realized_pnl_tps + final_close_pnl,
    }
