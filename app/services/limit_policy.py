from __future__ import annotations

import math
from datetime import datetime, timezone
from typing import Any, Iterable

POLICY_KEY = "limit_policy_v1"
RUNTIME_KEY = "limit_policy_runtime_v1"
POLICY_VERSION = 1

VALID_TP_MODES = {"none", "tp1", "tp2", "half", "last"}
PRESETS: dict[str, tuple[int, str]] = {
    # Four user-facing profiles. ``no_time`` remains accepted for backward
    # compatibility with policies already saved by older builds, but it is no
    # longer displayed in the simplified menu.
    "fast": (6, "tp1"),
    "tp2": (24, "tp2"),
    "balanced": (24, "half"),
    "long": (72, "last"),
    "no_time": (0, "last"),
}


def _parse_nonnegative_int(value: Any, *, maximum: int | None = None) -> int | None:
    if isinstance(value, bool) or value in (None, ""):
        return None
    try:
        parsed = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    if not math.isfinite(parsed) or not parsed.is_integer() or parsed < 0:
        return None
    result = int(parsed)
    if maximum is not None and result > maximum:
        return None
    return result


def _strict_nonnegative_int(value: Any, *, maximum: int | None = None) -> int:
    parsed = _parse_nonnegative_int(value, maximum=maximum)
    return parsed if parsed is not None else 0


def normalize_ttl_hours(value: Any) -> int:
    # Do not let bool/fractional legacy values silently become 1 hour.
    if isinstance(value, bool) or value in (None, ""):
        return 72
    try:
        parsed = float(value)
    except (TypeError, ValueError, OverflowError):
        return 72
    if not math.isfinite(parsed) or not parsed.is_integer():
        return 72
    hours = int(parsed)
    if hours <= 0:
        return 0
    return max(1, min(168, hours))


def normalize_tp_mode(value: Any) -> str:
    mode = str(value or "last").strip().lower()
    return mode if mode in VALID_TP_MODES else "last"


def threshold_index(mode: str, target_count: int) -> int:
    count = _strict_nonnegative_int(target_count)
    if count <= 0:
        return 0
    mode = normalize_tp_mode(mode)
    if mode == "none":
        return 0
    if mode == "tp1":
        return 1
    if mode == "tp2":
        # Never silently reinterpret "after TP2" as "after TP1".  When a
        # concrete LIMIT contains only one usable target, the TP rule is
        # disabled for that execution while the time/STOP rules remain active.
        return 2 if count >= 2 else 0
    if mode == "half":
        return max(1, int(math.ceil(count / 2.0)))
    return count


def build_policy(
    *, ttl_hours: Any, tp_mode: Any, targets: Iterable[Any], preset: Any = "custom"
) -> dict[str, Any]:
    clean_targets: list[float] = []
    for value in targets:
        if isinstance(value, bool):
            continue
        try:
            number = float(value)
        except (TypeError, ValueError):
            continue
        if math.isfinite(number) and number > 0:
            clean_targets.append(number)
    mode = normalize_tp_mode(tp_mode)
    ttl = normalize_ttl_hours(ttl_hours)
    return {
        "version": POLICY_VERSION,
        "preset": str(preset or "custom").strip().lower()[:32],
        "ttl_hours": ttl,
        "tp_mode": mode,
        "tp_threshold_index": threshold_index(mode, len(clean_targets)),
        "target_count": len(clean_targets),
        "captured_at": datetime.now(timezone.utc).isoformat(),
    }


def read_policy(
    payload: dict[str, Any] | None,
    *,
    fallback_ttl: int = 72,
    fallback_mode: str = "last",
    targets: Iterable[Any] = (),
) -> dict[str, Any]:
    target_values = list(targets)
    raw = (payload or {}).get(POLICY_KEY)
    if not isinstance(raw, dict):
        return build_policy(
            ttl_hours=fallback_ttl,
            tp_mode=fallback_mode,
            targets=target_values,
            preset="legacy",
        )
    policy = build_policy(
        ttl_hours=raw.get("ttl_hours", fallback_ttl),
        tp_mode=raw.get("tp_mode", fallback_mode),
        targets=target_values,
        preset=raw.get("preset", "custom"),
    )
    stored_index = _strict_nonnegative_int(
        raw.get("tp_threshold_index"),
        maximum=int(policy.get("target_count") or 0),
    )
    # An immutable snapshot keeps its original threshold when it remains valid.
    # Repair the known v1.6.11 defect where TP2 on a one-target execution was
    # frozen as TP1.  That unsafe downgrade must not survive an upgrade.
    target_count = _strict_nonnegative_int(policy.get("target_count"))
    if policy["tp_mode"] == "tp2" and target_count < 2:
        policy["tp_threshold_index"] = 0
    elif (policy["tp_mode"] == "none" and stored_index == 0) or (
        1 <= stored_index <= target_count
    ):
        policy["tp_threshold_index"] = stored_index
    policy["captured_at"] = str(raw.get("captured_at") or policy["captured_at"])
    return policy


def passed_tp_count(side: str, current_price: float, targets: Iterable[Any]) -> int:
    try:
        current = float(current_price)
    except (TypeError, ValueError):
        return 0
    if not math.isfinite(current) or current <= 0:
        return 0
    side_l = str(side or "").lower()
    if side_l not in {"long", "short"}:
        return 0
    clean_targets: list[float] = []
    for value in targets:
        if isinstance(value, bool):
            continue
        try:
            target = float(value)
        except (TypeError, ValueError):
            continue
        if math.isfinite(target) and target > 0:
            clean_targets.append(target)
    if side_l == "long" and any(
        a >= b for a, b in zip(clean_targets, clean_targets[1:])
    ):
        return 0
    if side_l == "short" and any(
        a <= b for a, b in zip(clean_targets, clean_targets[1:])
    ):
        return 0
    passed = 0
    for target in clean_targets:
        reached = current >= target if side_l == "long" else current <= target
        if not reached:
            break
        passed += 1
    return passed


def record_tp_touch(
    runtime: dict[str, Any] | None,
    *,
    side: str,
    current_price: Any,
    targets: Iterable[Any],
    observed_at: str | None = None,
) -> tuple[dict[str, Any], bool, int]:
    """Persist the highest observed TP touch independently of the current rule.

    A TP rule can be disabled (legacy ``none`` or TP2 with only one target) and
    later be explicitly replaced for an already pending LIMIT.  Older builds
    recorded touches only when the *current* threshold was active, so a price
    spike could be forgotten and the newly applied policy would incorrectly keep
    a stale LIMIT alive after the market retraced.

    Returns ``(runtime, changed, max_tp_passed)``.  The update is monotonic and
    idempotent; unrelated runtime fields are preserved.
    """
    out = dict(runtime) if isinstance(runtime, dict) else {}
    target_values = list(targets)
    valid_target_count = 0
    for value in target_values:
        if isinstance(value, bool):
            continue
        try:
            parsed_target = float(value)
        except (TypeError, ValueError, OverflowError):
            continue
        if math.isfinite(parsed_target) and parsed_target > 0:
            valid_target_count += 1
    raw_seen = out.get("max_tp_passed")
    parsed_seen = _parse_nonnegative_int(raw_seen, maximum=valid_target_count)
    runtime_repaired = raw_seen not in (None, "") and parsed_seen is None
    seen_before = parsed_seen if parsed_seen is not None else 0
    if runtime_repaired:
        out["max_tp_passed"] = 0
    passed_now = passed_tp_count(side, current_price, target_values)
    max_passed = max(seen_before, passed_now)
    if max_passed <= seen_before:
        return out, runtime_repaired, max_passed
    try:
        observed_price = float(current_price)
        if not math.isfinite(observed_price):
            observed_price = 0.0
    except (TypeError, ValueError, OverflowError):
        observed_price = 0.0
    out.update(
        {
            "max_tp_passed": max_passed,
            "last_observed_price": observed_price,
            "last_touch_at": observed_at or datetime.now(timezone.utc).isoformat(),
        }
    )
    return out, True, max_passed


def tp_mode_label(mode: str) -> str:
    return {
        "none": "не удалять по TP",
        "tp1": "после TP1",
        "tp2": "после TP2",
        "half": "после половины целей",
        "last": "после последней цели",
    }.get(normalize_tp_mode(mode), "после последней цели")


def preset_label(preset: str) -> str:
    return {
        "fast": "⚡ Быстрый",
        "tp2": "🎯 После TP2",
        "balanced": "⚖️ Стандартный",
        "long": "🛡 Долгий",
        "no_time": "♾ Старая настройка без TTL",
        "custom": "🕒 Свой срок",
    }.get(str(preset or "custom").lower(), "🧩 Индивидуальная настройка")
