from __future__ import annotations

import json
import math
from typing import Any, Mapping

from app.services.exchange_identity import clean_exchange_id

_ZERO_TOLERANCE = 1e-12

# Status alone is accepted only after contradictory live-exposure metadata has
# been ruled out. These statuses are written after an authoritative exchange
# read or a confirmed exact LIMIT cancellation.
_CLOSED_POSITION_STATUSES = {
    "closed",
    "closed_pending_history",
    "closed_on_exchange",
    "closed_stop_catchup",
    "closed_on_exchange_cleanup",
}
_CANCELED_ENTRY_STATUSES = {
    "canceled_expired",
    "canceled_tp_progress",
    "canceled_stop_invalidated",
    "canceled_external",
}
_LEGACY_PREVIOUS_STATUSES = {
    "opening_intent",
    "opened",
    "pending_limit",
    "protected",
    "partial_error",
    "manual_required",
    "partial_unrecoverable",
    "closed_pending_history",
    "closed_on_exchange",
    "closed_stop_catchup",
}


def execution_payload_dict(row: Mapping[str, Any]) -> dict[str, Any]:
    """Return durable execution metadata only when it is a JSON object."""

    payload, valid = _payload_parse(row)
    return payload if valid else {}


def finite_number(value: Any) -> float | None:
    """Parse a finite number without accepting bool as 0/1."""

    if isinstance(value, bool):
        return None
    try:
        parsed = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return parsed if math.isfinite(parsed) else None


def _quantity_field(
    source: Mapping[str, Any], key: str
) -> tuple[bool, bool, float | None]:
    """Return ``(present, valid_non_negative, value)`` for an exposure field."""

    if key not in source:
        return False, True, None
    value = finite_number(source.get(key))
    if value is None or value < 0:
        return True, False, None
    return True, True, value


def _payload_parse(row: Mapping[str, Any]) -> tuple[dict[str, Any], bool]:
    """Parse durable execution metadata and preserve malformed-payload state."""

    try:
        raw = row.get("exchange_order_ids_json")
        if raw is None or raw == "":
            raw = "{}"
        payload = raw if isinstance(raw, dict) else json.loads(raw)
    except (TypeError, ValueError, json.JSONDecodeError):
        return {}, False
    return (payload, True) if isinstance(payload, dict) else ({}, False)


def _optional_mapping(
    source: Mapping[str, Any], key: str
) -> tuple[dict[str, Any], bool]:
    """Return an optional nested mapping without hiding type corruption."""

    if key not in source or source.get(key) is None:
        return {}, True
    value = source.get(key)
    return (value, True) if isinstance(value, dict) else ({}, False)


def _strict_non_negative_number_field(
    source: Mapping[str, Any], key: str
) -> tuple[bool, bool, float | None]:
    """Validate an optional finite non-negative numeric field."""

    if key not in source:
        return False, True, None
    value = finite_number(source.get(key))
    if value is None or value < 0:
        return True, False, None
    return True, True, value


def _strict_non_negative_integer_field(
    source: Mapping[str, Any], key: str
) -> tuple[bool, bool, int | None]:
    """Validate an optional integer counter without bool/float truncation."""

    if key not in source:
        return False, True, None
    raw = source.get(key)
    if isinstance(raw, bool):
        return True, False, None
    if isinstance(raw, int):
        return (True, raw >= 0, raw if raw >= 0 else None)
    if isinstance(raw, float):
        if not math.isfinite(raw) or raw < 0 or not raw.is_integer():
            return True, False, None
        return True, True, int(raw)
    return True, False, None


def _strict_optional_bool_field(
    source: Mapping[str, Any], key: str
) -> tuple[bool, bool, bool | None]:
    """Validate an optional boolean without accepting 0/1 or strings."""

    if key not in source or source.get(key) is None:
        return False, True, None
    value = source.get(key)
    if not isinstance(value, bool):
        return True, False, None
    return True, True, value


def _typed_sequence(
    source: Mapping[str, Any], key: str
) -> tuple[bool, bool, list[Any]]:
    """Validate an optional list-like field without accepting strings/dicts."""

    if key not in source or source.get(key) is None:
        return False, True, []
    value = source.get(key)
    if not isinstance(value, (list, tuple, set)):
        return True, False, []
    return True, True, list(value)


def _cleanup_validation_state(value: Any, *, present: bool) -> dict[str, Any]:
    """Validate one cleanup snapshot and expose exact fail-closed hazards."""

    if not present or value is None:
        return {
            "valid": True,
            "verified_clean": False,
            "hazard": False,
            "details": [],
        }
    if not isinstance(value, dict):
        return {
            "valid": False,
            "verified_clean": False,
            "hazard": True,
            "details": ["cleanup_not_mapping"],
        }

    details: list[str] = []
    for key in ("verified_clean", "identity_missing"):
        if key in value and value.get(key) is not None and not isinstance(
            value.get(key), bool
        ):
            details.append(f"{key}_not_bool")

    sequence_values: dict[str, list[Any]] = {}
    for key in (
        "remaining_tracked_algo_ids",
        "remaining_tracked_regular_ids",
        "unidentified_relevant_algo_orders",
        "unidentified_relevant_regular_orders",
        "errors",
    ):
        _present, valid, items = _typed_sequence(value, key)
        if not valid:
            details.append(f"{key}_not_sequence")
        sequence_values[key] = items

    hazard = bool(
        value.get("identity_missing") is True
        or _stable_exchange_ids(sequence_values["remaining_tracked_algo_ids"])
        or _stable_exchange_ids(sequence_values["remaining_tracked_regular_ids"])
        or sequence_values["unidentified_relevant_algo_orders"]
        or sequence_values["unidentified_relevant_regular_orders"]
    )
    return {
        "valid": not details,
        "verified_clean": value.get("verified_clean") is True,
        "hazard": hazard,
        "details": details,
    }


def _parse_utc_iso(value: Any):
    if not isinstance(value, str) or not value.strip():
        return None
    from datetime import datetime, timezone

    try:
        parsed = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _stable_exchange_ids(value: Any) -> list[str]:
    if not isinstance(value, (list, tuple, set)):
        return []
    result: list[str] = []
    for item in value:
        cleaned = clean_exchange_id(item)
        if cleaned:
            result.append(cleaned)
    return sorted(set(result))


def critical_zero_exposure_proof_state(
    payload: Mapping[str, Any],
) -> dict[str, Any]:
    """Validate the durable two-read proof written by the lifecycle guard.

    Unknown schema versions/sources and malformed counters/timestamps are kept
    fail-closed so a corrupted JSON fragment cannot release portfolio risk.
    """

    proof_raw = payload.get("critical_zero_exposure_v1")
    proof = proof_raw if isinstance(proof_raw, dict) else {}
    confirmations_present, confirmations_valid, confirmations = (
        _strict_non_negative_integer_field(proof, "confirmations")
    )
    first_at = _parse_utc_iso(proof.get("first_zero_at"))
    second_at = _parse_utc_iso(proof.get("second_zero_at"))
    last_checked_at = _parse_utc_iso(proof.get("last_checked_at"))
    same_present, same_valid, same_qty = _quantity_field(
        proof, "same_side_position_qty"
    )
    any_present, any_valid, any_qty = _quantity_field(proof, "any_position_qty")
    version_valid = "version" not in proof or proof.get("version") == 1
    source_valid = "source" not in proof or proof.get(
        "source"
    ) == "position_lifecycle_guard"
    last_checked_valid = bool(
        "last_checked_at" not in proof
        or last_checked_at is not None
        and second_at is not None
        and last_checked_at >= second_at
    )
    schema_valid = bool(
        isinstance(proof_raw, dict)
        and version_valid
        and source_valid
        and confirmations_present
        and confirmations_valid
        and confirmations is not None
    )
    independent = bool(
        first_at is not None
        and second_at is not None
        and second_at > first_at
        and last_checked_valid
        and proof.get("second_read_forced") is True
    )
    confirmed = bool(
        schema_valid
        and proof.get("confirmed") is True
        and proof.get("invalidated_at") in (None, "")
        and confirmations is not None
        and confirmations >= 2
        and independent
        and same_present
        and same_valid
        and same_qty is not None
        and same_qty <= _ZERO_TOLERANCE
        and any_present
        and any_valid
        and any_qty is not None
        and any_qty <= _ZERO_TOLERANCE
    )
    return {
        "confirmed": confirmed,
        "schema_valid": schema_valid,
        "confirmations": int(confirmations or 0),
        "independent": independent,
        "same_side_position_qty": same_qty,
        "any_position_qty": any_qty,
        "first_zero_at": first_at,
        "second_zero_at": second_at,
        "last_checked_at": last_checked_at,
    }


def _critical_zero_proof_state(payload: Mapping[str, Any]) -> dict[str, Any]:
    """Backward-compatible private alias for the shared strict parser."""

    return critical_zero_exposure_proof_state(payload)


def manual_required_zero_exposure_release_state(
    row: Mapping[str, Any],
) -> dict[str, Any]:
    """Return the strict shared release decision for legacy manual rows.

    The critical monitor already writes ``critical_zero_exposure_v1`` after two
    independent fresh BingX reads.  Risk, dashboard and symbol-conflict paths
    must consume that same durable proof instead of requiring unrelated legacy
    lifecycle fields such as ``previous_status``.  Every contradictory entry,
    position, STOP/BE, residual or cleanup marker remains fail-closed.
    """

    result: dict[str, Any] = {
        "eligible": False,
        "reason": "not_manual_required",
        "zero_confirmations": 0,
        "zero_proof_confirmed": False,
    }
    if str(row.get("status") or "").strip().lower() != "manual_required":
        return result

    invalid_fields: list[str] = []
    try:
        if int(row.get("user_id") or 0) <= 0:
            invalid_fields.append("user_id")
    except (TypeError, ValueError, OverflowError):
        invalid_fields.append("user_id")
    if not str(row.get("symbol") or "").strip():
        invalid_fields.append("symbol")
    if str(row.get("side") or "").strip().lower() not in {"long", "short"}:
        invalid_fields.append("side")
    qty = finite_number(row.get("qty"))
    if qty is None or qty <= 0:
        invalid_fields.append("qty")
    if invalid_fields:
        result.update(reason="invalid_execution_fields", details=invalid_fields)
        return result

    payload, payload_valid = _payload_parse(row)
    if not payload_valid:
        result["reason"] = "malformed_payload"
        return result

    proof_state = _critical_zero_proof_state(payload)
    result["zero_confirmations"] = int(proof_state["confirmations"])
    result["zero_proof_confirmed"] = bool(proof_state["confirmed"])

    nested: dict[str, dict[str, Any]] = {}
    malformed_nested: list[str] = []
    for key in (
        "lifecycle",
        "be",
        "residual_position_v1",
        "critical_manual_review_v1",
        "opening_intent_reconciliation_v1",
        "stop_diagnostic_v1",
        "manual_position_close_v1",
        "critical_cleanup_reconcile_v1",
    ):
        value, valid = _optional_mapping(payload, key)
        nested[key] = value
        if not valid:
            malformed_nested.append(key)
    if malformed_nested:
        result.update(reason="malformed_nested_metadata", details=malformed_nested)
        return result

    lifecycle = nested["lifecycle"]
    be = nested["be"]
    residual = nested["residual_position_v1"]
    marker = nested["critical_manual_review_v1"]
    opening = nested["opening_intent_reconciliation_v1"]
    stop_diag = nested["stop_diagnostic_v1"]
    manual_close = nested["manual_position_close_v1"]
    cleanup_marker = nested["critical_cleanup_reconcile_v1"]

    reason_text = (
        " ".join(
            str(value or "")
            for value in (
                row.get("reason"),
                lifecycle.get("reason"),
                be.get("error"),
                residual.get("error"),
                marker.get("reason"),
            )
        )
        .strip()
        .lower()
    )

    api_state = str(marker.get("api_state") or "").strip().lower()
    if marker.get("reason") == "api_key_missing" or (
        api_state != "available"
        and "api" in reason_text
        and any(
            token in reason_text
            for token in (
                "missing",
                "not configured",
                "disabled",
                "unavailable",
                "no api",
            )
        )
    ):
        result["reason"] = "api_unavailable"
        return result

    opposite = lifecycle.get("opposite_or_unknown_position_detected")
    if opposite is True or opposite not in (False, None):
        result["reason"] = "opposite_or_unknown_position"
        return result

    for key in ("any_position_qty", "position_qty"):
        present, valid, value = _quantity_field(lifecycle, key)
        if present and not valid:
            result.update(reason="invalid_position_snapshot", details=[key])
            return result
        if present and value is not None and value > _ZERO_TOLERANCE:
            result.update(reason="live_position", live_qty=value, live_field=key)
            return result

    opening_qty_present, opening_qty_valid, opening_qty = (
        _strict_non_negative_number_field(opening, "same_side_position_qty")
    )
    matches_present, matches_valid, open_match_count = (
        _strict_non_negative_integer_field(opening, "open_match_count")
    )
    if not opening_qty_valid or not matches_valid:
        details = []
        if opening_qty_present and not opening_qty_valid:
            details.append("opening.same_side_position_qty")
        if matches_present and not matches_valid:
            details.append("opening.open_match_count")
        result.update(reason="invalid_opening_reconciliation", details=details)
        return result
    order_status = opening.get("order_status")
    if order_status is not None and not isinstance(order_status, dict):
        result.update(
            reason="invalid_opening_reconciliation",
            details=["opening.order_status"],
        )
        return result
    if (
        (opening_qty or 0.0) > _ZERO_TOLERANCE
        or int(open_match_count or 0) > 0
        or "opening_intent" in reason_text
        and "live" in reason_text
    ):
        result["reason"] = "active_or_unknown_entry"
        return result

    invalid_be_fields: list[str] = []
    for key in ("replacement_in_progress", "manual_required"):
        present, valid, _value = _strict_optional_bool_field(be, key)
        if present and not valid:
            invalid_be_fields.append(f"be.{key}")
    if "error" in be and be.get("error") is not None and not isinstance(
        be.get("error"), str
    ):
        invalid_be_fields.append("be.error")
    if invalid_be_fields:
        result.update(reason="malformed_nested_metadata", details=invalid_be_fields)
        return result
    replacement_intent = be.get("replacement_write_intent_v1")
    if replacement_intent is not None and not isinstance(replacement_intent, dict):
        result.update(
            reason="malformed_nested_metadata",
            details=["be.replacement_write_intent_v1"],
        )
        return result
    if (
        be.get("replacement_in_progress") is True
        or isinstance(replacement_intent, dict)
        and bool(replacement_intent)
    ):
        result["reason"] = "be_replacement_in_progress"
        return result
    if be.get("manual_required") is True or str(be.get("error") or "").strip():
        result["reason"] = "unknown_stop_or_be_protection"
        return result

    stop_manual_present, stop_manual_valid, _stop_manual = (
        _strict_optional_bool_field(stop_diag, "manual_required")
    )
    if stop_manual_present and not stop_manual_valid:
        result.update(
            reason="malformed_nested_metadata",
            details=["stop_diagnostic_v1.manual_required"],
        )
        return result
    active_stop_present, active_stop_valid, active_stop_values = _typed_sequence(
        stop_diag, "active_stop_ids"
    )
    if active_stop_present and not active_stop_valid:
        result.update(
            reason="malformed_nested_metadata",
            details=["stop_diagnostic_v1.active_stop_ids"],
        )
        return result
    active_stop_ids = _stable_exchange_ids(active_stop_values)
    if len(active_stop_ids) > 1 or stop_diag.get("manual_required") is True:
        result["reason"] = "unknown_stop_or_be_protection"
        return result
    if "stop" in reason_text and any(
        token in reason_text
        for token in ("unknown", "without positionid", "manual protection", "неизвест")
    ):
        result["reason"] = "unknown_stop_or_be_protection"
        return result

    residual_values: list[float] = []
    invalid_residual_fields: list[str] = []
    residual_manual_present, residual_manual_valid, _residual_manual = (
        _strict_optional_bool_field(residual, "manual_required")
    )
    if residual_manual_present and not residual_manual_valid:
        invalid_residual_fields.append("residual_position_v1.manual_required")
    for key in ("status", "state"):
        if key in residual and residual.get(key) is not None and not isinstance(
            residual.get(key), str
        ):
            invalid_residual_fields.append(f"residual_position_v1.{key}")
    for key in ("position_qty", "same_side_position_qty", "qty", "after_qty"):
        present, valid, value = _strict_non_negative_number_field(residual, key)
        if present and not valid:
            invalid_residual_fields.append(f"residual_position_v1.{key}")
        elif value is not None:
            residual_values.append(value)
    if invalid_residual_fields:
        result.update(
            reason="invalid_residual_snapshot", details=invalid_residual_fields
        )
        return result
    residual_qty = max(residual_values, default=0.0)
    residual_status = str(
        residual.get("status") or residual.get("state") or ""
    ).strip().lower()
    if residual and (
        residual.get("manual_required") is True
        or residual_qty > _ZERO_TOLERANCE
        or residual_status
        not in {
            "",
            "closed",
            "resolved",
            "no_residual",
            "position_closed",
            "market_close_confirmed",
        }
    ):
        result["reason"] = "residual_active_or_unknown"
        return result

    if "closed_cleanup_done" in lifecycle and lifecycle.get(
        "closed_cleanup_done"
    ) not in (True, False, None):
        result.update(
            reason="malformed_nested_metadata",
            details=["lifecycle.closed_cleanup_done"],
        )
        return result

    lifecycle_cleanup_present = "cleanup" in lifecycle
    lifecycle_cleanup = lifecycle.get("cleanup")
    manual_cleanup_present = "cleanup" in manual_close
    manual_cleanup = manual_close.get("cleanup")
    lifecycle_cleanup_state = _cleanup_validation_state(
        lifecycle_cleanup, present=lifecycle_cleanup_present
    )
    manual_cleanup_state = _cleanup_validation_state(
        manual_cleanup, present=manual_cleanup_present
    )
    cleanup_details = [
        f"lifecycle.cleanup.{detail}"
        for detail in lifecycle_cleanup_state["details"]
    ] + [
        f"manual_position_close_v1.cleanup.{detail}"
        for detail in manual_cleanup_state["details"]
    ]
    if cleanup_details:
        result.update(reason="malformed_cleanup_metadata", details=cleanup_details)
        return result
    if lifecycle_cleanup_state["hazard"] or manual_cleanup_state["hazard"]:
        result["reason"] = "unknown_stop_or_cleanup_identity"
        return result

    marker_state = str(cleanup_marker.get("state") or "").strip().lower()
    known_cleanup_states = {
        "resolved",
        "pending_retry",
        "waiting_zero_confirmation",
        "blocked_live_position",
        "blocked_unknown_orders",
        "api_error_no_zero_proof",
    }
    if marker_state and marker_state not in known_cleanup_states:
        result.update(
            reason="malformed_nested_metadata",
            details=["critical_cleanup_reconcile_v1.state"],
        )
        return result
    if marker_state and marker_state != "resolved":
        result["reason"] = "cleanup_reconcile_not_resolved"
        return result
    if marker_state == "resolved" and cleanup_marker.get("verified_clean") is not True:
        result["reason"] = "cleanup_reconcile_not_resolved"
        return result

    cleanup_verified = bool(
        lifecycle.get("closed_cleanup_done") is True
        or lifecycle_cleanup_state["verified_clean"]
        or manual_cleanup_state["verified_clean"]
    )
    if not cleanup_verified:
        result["reason"] = "cleanup_unresolved"
        return result

    if not proof_state["confirmations"]:
        result["reason"] = "zero_proof_missing"
        return result
    if not proof_state["confirmed"]:
        result["reason"] = "zero_proof_invalid_or_incomplete"
        return result

    result.update(eligible=True, reason="eligible")
    return result


def execution_zero_exposure_confirmed(row: Mapping[str, Any]) -> bool:
    """Return True only when durable state proves that no position is exposed.

    Old releases overloaded ``manual_required``: some rows represented a live
    unsafe position, while others represented an already closed position whose
    history or stale-order cleanup still needed review. This classifier keeps
    ambiguous, malformed and contradictory metadata fail-closed.
    """

    status = str(row.get("status") or "").strip().lower()
    payload = execution_payload_dict(row)
    lifecycle_raw = payload.get("lifecycle")
    lifecycle = lifecycle_raw if isinstance(lifecycle_raw, dict) else {}

    # Explicit opposite/unknown exposure overrides every terminal-looking
    # status. A malformed non-boolean marker is also ambiguous and stays live.
    if "opposite_or_unknown_position_detected" in lifecycle:
        opposite = lifecycle.get("opposite_or_unknown_position_detected")
        if opposite is True or opposite not in (False, None):
            return False

    any_present, any_valid, any_qty = _quantity_field(lifecycle, "any_position_qty")
    if any_present and (not any_valid or (any_qty or 0.0) > _ZERO_TOLERANCE):
        return False

    pos_present, pos_valid, position_qty = _quantity_field(lifecycle, "position_qty")
    if pos_present and (not pos_valid or (position_qty or 0.0) > _ZERO_TOLERANCE):
        return False

    if status in _CANCELED_ENTRY_STATUSES | _CLOSED_POSITION_STATUSES:
        return True

    manual_close = payload.get("manual_position_close_v1")
    if isinstance(manual_close, dict) and manual_close.get("confirmed") is True:
        return True

    # v1.0.7g7h2f5g4a: consume the exact same durable two-read proof that the
    # critical monitor already classified as safe. This releases legacy
    # manual_required rows without depending on unrelated previous_status data.
    if status == "manual_required" and manual_required_zero_exposure_release_state(
        row
    ).get("eligible") is True:
        return True

    # Legacy close evidence written after repeated zero-position reads and a
    # locked final recheck. Require a known predecessor plus a meaningful
    # cleanup/history checkpoint; an empty arbitrary dict must not free risk.
    previous_status = str(lifecycle.get("previous_status") or "").strip().lower()
    cleanup = lifecycle.get("cleanup")
    meaningful_checkpoint = (
        isinstance(cleanup, dict)
        and bool(cleanup)
        or isinstance(lifecycle.get("history_reconcile"), dict)
        and bool(lifecycle.get("history_reconcile"))
        or isinstance(lifecycle.get("close_result"), dict)
        and bool(lifecycle.get("close_result"))
        or isinstance(lifecycle.get("closed_cleanup_done"), bool)
    )
    if (
        pos_present
        and pos_valid
        and position_qty is not None
        and position_qty <= _ZERO_TOLERANCE
        and previous_status in _LEGACY_PREVIOUS_STATUSES
        and meaningful_checkpoint
    ):
        return True

    return False


def execution_cleanup_unresolved(row: Mapping[str, Any]) -> bool:
    """Whether a confirmed-closed execution may still own stale live orders."""

    if not execution_zero_exposure_confirmed(row):
        return False

    status = str(row.get("status") or "").strip().lower()
    if status in {"closed", "closed_on_exchange_cleanup"} | _CANCELED_ENTRY_STATUSES:
        return False

    payload = execution_payload_dict(row)
    lifecycle_raw = payload.get("lifecycle")
    lifecycle = lifecycle_raw if isinstance(lifecycle_raw, dict) else {}
    if lifecycle.get("closed_cleanup_done") is True:
        return False

    manual_close = payload.get("manual_position_close_v1")
    if isinstance(manual_close, dict):
        cleanup = manual_close.get("cleanup")
        if isinstance(cleanup, dict):
            return cleanup.get("verified_clean") is not True

    cleanup = lifecycle.get("cleanup")
    if isinstance(cleanup, dict):
        return cleanup.get("verified_clean") is not True

    return status in {
        "closed_pending_history",
        "closed_on_exchange",
        "closed_stop_catchup",
    }


def execution_be_protection_confirmed(row_or_payload: Mapping[str, Any]) -> bool:
    """Return True only for a durable, internally consistent BE checkpoint."""

    if "exchange_order_ids_json" in row_or_payload:
        payload = execution_payload_dict(row_or_payload)
    else:
        payload = dict(row_or_payload)
    be_raw = payload.get("be")
    be = be_raw if isinstance(be_raw, dict) else {}
    if be.get("moved") is not True:
        return False
    if be.get("manual_required") is True or be.get("replacement_in_progress") is True:
        return False
    if be.get("error") not in (None, ""):
        return False
    if be.get("replacement_write_intent_v1") not in (None, ""):
        return False

    stop = finite_number(be.get("stop"))
    if stop is None or stop <= 0:
        return False
    if "qty" in be:
        qty = finite_number(be.get("qty"))
        if qty is None or qty <= 0:
            return False

    # If a replacement identity is present, it must be a strict exchange id.
    for key in (
        "replacement_stop_id",
        "verify_matching_stop_order_id",
    ):
        if key in be and be.get(key) not in (None, ""):
            if not clean_exchange_id(be.get(key)):
                return False
    replacement = be.get("replacement_stop")
    if isinstance(replacement, dict) and replacement:
        candidates = (
            replacement.get("stopPlanOrderId"),
            replacement.get("stopOrderId"),
            replacement.get("orderId"),
            replacement.get("id"),
        )
        if not any(clean_exchange_id(value) for value in candidates):
            return False

    return True
