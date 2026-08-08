from __future__ import annotations

import math
from typing import Iterable, List


def _assert_sums_100(d: dict) -> dict:
    for count, vals in d.items():
        s = sum(vals)
        if not math.isfinite(s) or abs(s - 100.0) >= 0.01:
            raise RuntimeError(f"TP distribution table[{count}] sums to {s}, not 100")
    return d


SMART_TABLE = _assert_sums_100(
    {
        1: [100.0],
        2: [40.0, 60.0],
        3: [30.0, 45.0, 25.0],
        4: [25.0, 35.0, 30.0, 10.0],
        5: [24.0, 30.0, 24.0, 14.0, 8.0],
        6: [26.0, 22.0, 18.0, 14.0, 11.0, 9.0],
        7: [24.0, 20.0, 17.0, 14.0, 11.0, 8.0, 6.0],
        8: [23.0, 19.0, 16.0, 13.0, 10.0, 8.0, 6.0, 5.0],
        9: [22.0, 18.0, 15.0, 13.0, 10.0, 8.0, 6.0, 5.0, 3.0],
        10: [22.0, 18.0, 15.0, 12.0, 9.0, 7.0, 6.0, 5.0, 4.0, 2.0],
    }
)

# BELL (колокол): меньше на крайних, больше в середине.
# Полезно если трейдер видит зону повышенной вероятности фиксации в середине цепочки.
BELL_TABLE = _assert_sums_100(
    {
        1: [100.0],
        2: [50.0, 50.0],
        3: [27.4, 45.2, 27.4],
        4: [15.9, 34.1, 34.1, 15.9],
        5: [11.3, 23.6, 30.2, 23.6, 11.3],
        6: [8.7, 17.2, 24.1, 24.1, 17.2, 8.7],
        7: [7.0, 13.1, 19.1, 21.6, 19.1, 13.1, 7.0],
        8: [5.9, 10.4, 15.3, 18.7, 18.5, 15.3, 10.4, 5.5],
        9: [5.0, 8.5, 12.4, 15.6, 17.0, 15.6, 12.4, 8.5, 5.0],
        10: [4.4, 7.2, 10.3, 13.2, 14.9, 14.9, 13.2, 10.3, 7.2, 4.4],
    }
)

# ACCELERATION (Разгон): небольшая фиксация на TP1, основная на TP2,
# затем остаток на продолжение. Режим намеренно использует не более 4 TP.
ACCELERATION_TABLE = _assert_sums_100(
    {
        1: [100.0],
        2: [10.0, 90.0],
        3: [10.0, 65.0, 25.0],
        4: [10.0, 65.0, 20.0, 5.0],
    }
)

# EARLY FIXATION (Ранняя фиксация): основная часть позиции закрывается на TP1,
# но 5% всегда остаются на TP4, когда в сигнале доступно четыре цели. Если
# целей меньше, доля отсутствующих дальних TP присоединяется к последней
# доступной цели, поэтому executable coverage всегда остаётся равным 100%.
EARLY_FIXATION_TABLE = _assert_sums_100(
    {
        1: [100.0],
        2: [70.0, 30.0],
        3: [70.0, 15.0, 15.0],
        4: [70.0, 15.0, 10.0, 5.0],
    }
)


def _normalize(vals: Iterable[float]) -> List[float]:
    nums = [float(v) for v in vals]
    if any(not math.isfinite(v) for v in nums):
        raise ValueError("TP distribution values must be finite")
    nums = [max(v, 0.0001) for v in nums]
    total = sum(nums)
    if total <= 0:
        raise ValueError("TP distribution total must be positive")
    out = [round(v / total * 100.0, 8) for v in nums]
    out[0] = round(out[0] + (100.0 - sum(out)), 8)
    return out


def smart_scale_out_distribution(count: int) -> List[float]:
    if count <= 0:
        return []
    if count in SMART_TABLE:
        return list(SMART_TABLE[count])
    # Убывающая лесенка для 11+ TP: ближние больше, дальние меньше.
    weights = [(count - i) ** 1.35 for i in range(count)]
    return _normalize(weights)


def bell_distribution(count: int) -> List[float]:
    """Колокол: меньше на крайних, больше в середине.

    Для 11+ TP используется гауссиана с sigma = count / 3.5,
    что даёт плавный bell-shape.
    """
    if count <= 0:
        return []
    if count in BELL_TABLE:
        return list(BELL_TABLE[count])
    # Gaussian fallback for 11+ TP
    import math

    center = (count - 1) / 2
    sigma = max(1.0, count / 3.5)
    weights = [
        math.exp(-((i - center) ** 2) / (2 * sigma * sigma)) for i in range(count)
    ]
    return _normalize(weights)


def acceleration_distribution(count: int) -> List[float]:
    """Return the exact Разгон scheme for one to four executable targets.

    Counts above four are deliberately capped. Callers also cap the target
    list itself so percentages and target prices always remain one-to-one.
    """
    if count <= 0:
        return []
    bounded = min(int(count), 4)
    return list(ACCELERATION_TABLE[bounded])


def early_fixation_distribution(count: int) -> List[float]:
    """Return the exact Ранняя фиксация scheme for up to four targets.

    Counts above four are deliberately capped. Callers cap the target list by
    the same rule, keeping target prices and percentages one-to-one.
    """
    if count <= 0:
        return []
    bounded = min(int(count), 4)
    return list(EARLY_FIXATION_TABLE[bounded])


def equal_distribution(count: int) -> List[float]:
    if count <= 0:
        return []
    vals = [100.0 / count for _ in range(count)]
    vals[-1] = round(vals[-1] + (100.0 - sum(vals)), 8)
    return [round(v, 8) for v in vals]


def manual_distribution(values: Iterable[float], count: int) -> List[float]:
    vals = [float(v) for v in values]
    if len(vals) != count:
        raise ValueError("manual TP схема должна совпадать с количеством TP")
    if any(not math.isfinite(v) or v <= 0 for v in vals):
        raise ValueError("manual TP проценты должны быть > 0")
    if abs(sum(vals) - 100.0) > 0.001:
        raise ValueError("manual TP проценты должны давать 100%")
    return vals


def limit_targets(targets: List[float], tp_limit: str) -> List[float]:
    if not targets:
        return []
    value = str(tp_limit or "all").lower().strip()
    if value in {"all", "все", "всё", "0"}:
        return list(targets)
    try:
        n = int(value)
    except ValueError:
        n = len(targets)
    n = max(1, min(n, len(targets)))
    return list(targets[:n])


def limit_targets_for_mode(
    targets: List[float], tp_limit: str, tp_mode: object
) -> List[float]:
    """Apply the user TP limit and the mode-specific hard target cap.

    Разгон and Ранняя фиксация never invent or merge target prices and use at
    most the first four selected targets. Other modes preserve their old
    behavior.
    """
    selected = limit_targets(targets, tp_limit)
    mode_value = str(getattr(tp_mode, "value", tp_mode) or "").strip().lower()
    if mode_value in {"acceleration", "early_fixation"}:
        return selected[:4]
    return selected
