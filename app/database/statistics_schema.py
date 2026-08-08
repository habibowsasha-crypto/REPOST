"""Additive statistics-v2 schema and idempotent migrations.

This module owns the additive statistics-v2 schema.  Step 3 created durable
period, execution-projection and funding-event storage; step 5 adds durable
projection leases and funding finalization metadata; step 7 adds an append-only
quality/recovery audit ledger; step 8 adds durable reset confirmations and
period audit state.  All runtime feature flags
remain disabled by default, and no trading path reads or waits for these objects.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any


LEGACY_PERIOD_NAME = "legacy_current"
LEGACY_PERIOD_KIND = "legacy"


STATISTICS_V2_SQLITE_SCHEMA = """
CREATE TABLE IF NOT EXISTS statistics_periods (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  name TEXT NOT NULL,
  status TEXT NOT NULL DEFAULT 'active',
  period_kind TEXT NOT NULL DEFAULT 'legacy',
  started_at TEXT NOT NULL,
  closed_at TEXT,
  created_by INTEGER,
  reset_reason TEXT,
  source_version TEXT NOT NULL,
  settings_snapshot_json TEXT NOT NULL DEFAULT '{}',
  created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  CHECK(status IN ('active','closed')),
  CHECK(period_kind IN ('legacy','production','test','shadow'))
);
CREATE UNIQUE INDEX IF NOT EXISTS statistics_periods_name_kind_uidx
  ON statistics_periods(name,period_kind);
CREATE UNIQUE INDEX IF NOT EXISTS statistics_periods_one_active_uidx
  ON statistics_periods(status) WHERE status='active';
CREATE INDEX IF NOT EXISTS statistics_periods_status_started_idx
  ON statistics_periods(status,started_at);

CREATE TABLE IF NOT EXISTS analytics_execution_results (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  execution_id INTEGER NOT NULL UNIQUE,
  analytics_signal_id INTEGER,
  trade_group_id INTEGER,
  period_id INTEGER,
  user_id INTEGER NOT NULL,
  exchange TEXT NOT NULL DEFAULT 'bingx',
  symbol TEXT NOT NULL,
  side TEXT NOT NULL,
  entry_order_type TEXT,
  linkage_status TEXT NOT NULL DEFAULT 'pending',
  first_entry_fill_at TEXT,
  last_entry_fill_at TEXT,
  actual_entry_qty TEXT,
  actual_entry_avg_price TEXT,
  planned_entry_reference TEXT,
  execution_reference_price TEXT,
  initial_stop_price TEXT,
  equity_snapshot_usd TEXT,
  planned_risk_percent TEXT,
  planned_risk_usd TEXT,
  initial_price_risk_usd TEXT,
  initial_risk_percent_of_equity TEXT,
  estimated_fee_risk_usd TEXT,
  expected_loss_at_stop_usd TEXT,
  planned_entry_qty TEXT,
  stop_distance TEXT,
  risk_snapshot_at TEXT,
  risk_snapshot_source TEXT,
  risk_snapshot_status TEXT,
  risk_snapshot_reason TEXT,
  tp_distribution_json TEXT,
  tp_distribution_source TEXT,
  tp_distribution_locked INTEGER NOT NULL DEFAULT 0,
  tp_distribution_version INTEGER NOT NULL DEFAULT 1,
  first_exit_fill_at TEXT,
  last_exit_fill_at TEXT,
  actual_exit_qty TEXT,
  actual_exit_avg_price TEXT,
  signal_max_tp_index INTEGER NOT NULL DEFAULT 0,
  execution_max_tp_index INTEGER NOT NULL DEFAULT 0,
  canonical_terminal_reason TEXT,
  terminal_detail TEXT,
  strategy_gross_pnl TEXT,
  exchange_gross_pnl TEXT,
  gross_pnl_source TEXT,
  trading_fee_signed TEXT,
  trading_fee_cost TEXT,
  funding_signed TEXT,
  settlement_asset TEXT,
  net_pnl TEXT,
  provisional_net_pnl TEXT,
  result_r TEXT,
  provisional_result_r TEXT,
  entry_slippage_bps TEXT,
  limit_price_slippage_bps TEXT,
  execution_duration_seconds INTEGER,
  trading_reconciliation_state TEXT NOT NULL DEFAULT 'pending',
  funding_state TEXT NOT NULL DEFAULT 'not_checked',
  financial_state TEXT NOT NULL DEFAULT 'PENDING',
  volume_parity_status TEXT NOT NULL DEFAULT 'pending',
  completeness_mask INTEGER NOT NULL DEFAULT 0,
  completeness_percent TEXT NOT NULL DEFAULT '0',
  data_quality_status TEXT NOT NULL DEFAULT 'pending',
  ambiguity_reason TEXT,
  legacy_data INTEGER NOT NULL DEFAULT 1,
  result_version INTEGER NOT NULL DEFAULT 1,
  projection_status TEXT NOT NULL DEFAULT 'pending',
  projection_attempts INTEGER NOT NULL DEFAULT 0,
  projection_next_attempt_at TEXT DEFAULT CURRENT_TIMESTAMP,
  projection_deadline_at TEXT,
  projection_processing_started_at TEXT,
  projection_lease_token TEXT,
  projection_last_error TEXT,
  funding_query_start_at TEXT,
  funding_query_end_at TEXT,
  funding_event_count INTEGER NOT NULL DEFAULT 0,
  funding_recovery_attempts INTEGER NOT NULL DEFAULT 0,
  funding_zero_observations INTEGER NOT NULL DEFAULT 0,
  funding_first_empty_at TEXT,
  funding_last_checked_at TEXT,
  funding_recovery_status TEXT NOT NULL DEFAULT 'not_required',
  funding_recovery_reason TEXT,
  funding_finalized_at TEXT,
  quality_reasons_json TEXT NOT NULL DEFAULT '[]',
  final_eligible INTEGER NOT NULL DEFAULT 0,
  simulation_eligible INTEGER NOT NULL DEFAULT 0,
  risk_analysis_eligible INTEGER NOT NULL DEFAULT 0,
  quality_gate_version INTEGER NOT NULL DEFAULT 1,
  quality_evaluated_at TEXT,
  market_event_review_status TEXT NOT NULL DEFAULT 'clear',
  market_event_exclusion_reason TEXT,
  market_event_reviewed_at TEXT,
  created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  finalized_at TEXT
);
CREATE INDEX IF NOT EXISTS analytics_execution_results_period_state_idx
  ON analytics_execution_results(period_id,financial_state,last_exit_fill_at);
CREATE INDEX IF NOT EXISTS analytics_execution_results_signal_idx
  ON analytics_execution_results(analytics_signal_id,execution_id);
CREATE INDEX IF NOT EXISTS analytics_execution_results_group_idx
  ON analytics_execution_results(trade_group_id,execution_id);
CREATE INDEX IF NOT EXISTS analytics_execution_results_user_state_idx
  ON analytics_execution_results(user_id,financial_state,updated_at);
CREATE INDEX IF NOT EXISTS analytics_execution_results_quality_idx
  ON analytics_execution_results(data_quality_status,financial_state);
CREATE INDEX IF NOT EXISTS analytics_execution_results_projection_due_idx
  ON analytics_execution_results(projection_status,projection_next_attempt_at);
CREATE INDEX IF NOT EXISTS analytics_execution_results_quality_refresh_idx
  ON analytics_execution_results(quality_gate_version,quality_evaluated_at,updated_at,execution_id);

CREATE TABLE IF NOT EXISTS financial_funding_events (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  execution_id INTEGER,
  user_id INTEGER NOT NULL,
  exchange TEXT NOT NULL DEFAULT 'bingx',
  symbol TEXT NOT NULL,
  position_side TEXT,
  exchange_event_id TEXT NOT NULL,
  amount_signed TEXT NOT NULL,
  asset TEXT NOT NULL,
  event_time TEXT NOT NULL,
  attribution_status TEXT NOT NULL DEFAULT 'unassigned',
  attribution_reason TEXT,
  source_endpoint TEXT NOT NULL,
  metadata_json TEXT NOT NULL DEFAULT '{}',
  created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  UNIQUE(exchange,user_id,exchange_event_id)
);
CREATE INDEX IF NOT EXISTS financial_funding_events_execution_time_idx
  ON financial_funding_events(execution_id,event_time);
CREATE INDEX IF NOT EXISTS financial_funding_events_user_symbol_time_idx
  ON financial_funding_events(user_id,symbol,event_time);
CREATE INDEX IF NOT EXISTS financial_funding_events_status_time_idx
  ON financial_funding_events(attribution_status,event_time);


CREATE TABLE IF NOT EXISTS statistics_entity_links (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  signal_id INTEGER NOT NULL,
  trade_group_id INTEGER NOT NULL,
  execution_id INTEGER NOT NULL,
  user_id INTEGER NOT NULL,
  symbol TEXT NOT NULL,
  side TEXT NOT NULL,
  source_chat_id INTEGER,
  source_message_id INTEGER,
  identity_fingerprint TEXT NOT NULL,
  linkage_status TEXT NOT NULL DEFAULT 'linked_exact',
  conflict_reason TEXT,
  created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  UNIQUE(execution_id),
  UNIQUE(signal_id,trade_group_id,execution_id),
  UNIQUE(identity_fingerprint)
);
CREATE INDEX IF NOT EXISTS statistics_entity_links_signal_group_idx
  ON statistics_entity_links(signal_id,trade_group_id);
CREATE INDEX IF NOT EXISTS statistics_entity_links_user_symbol_idx
  ON statistics_entity_links(user_id,symbol,created_at);

CREATE TABLE IF NOT EXISTS statistics_reset_requests (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  token_hash TEXT NOT NULL UNIQUE,
  actor_user_id INTEGER NOT NULL,
  reason TEXT NOT NULL,
  status TEXT NOT NULL DEFAULT 'pending',
  expires_at TEXT NOT NULL,
  old_period_id INTEGER NOT NULL,
  new_period_id INTEGER,
  source_version TEXT NOT NULL,
  settings_snapshot_json TEXT NOT NULL DEFAULT '{}',
  created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  applied_at TEXT,
  updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  CHECK(status IN ('pending','applied','cancelled','expired','stale'))
);
CREATE INDEX IF NOT EXISTS statistics_reset_requests_actor_status_idx
  ON statistics_reset_requests(actor_user_id,status,created_at);
CREATE INDEX IF NOT EXISTS statistics_reset_requests_old_period_idx
  ON statistics_reset_requests(old_period_id,status);


CREATE TABLE IF NOT EXISTS statistics_quality_audit (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  audit_key TEXT NOT NULL UNIQUE,
  scan_id TEXT NOT NULL,
  period_id INTEGER,
  entity_type TEXT NOT NULL,
  entity_id TEXT NOT NULL,
  action TEXT NOT NULL,
  issue_code TEXT NOT NULL,
  severity TEXT NOT NULL,
  recoverable INTEGER NOT NULL DEFAULT 0,
  reason TEXT NOT NULL,
  actor_user_id INTEGER,
  before_status TEXT,
  after_status TEXT,
  metadata_json TEXT NOT NULL DEFAULT '{}',
  created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS statistics_quality_audit_period_action_idx
  ON statistics_quality_audit(period_id,action,created_at);
CREATE INDEX IF NOT EXISTS statistics_quality_audit_entity_idx
  ON statistics_quality_audit(entity_type,entity_id,created_at);
"""


STATISTICS_V2_PG_SCHEMA = """
CREATE TABLE IF NOT EXISTS statistics_periods (
  id BIGSERIAL PRIMARY KEY,
  name TEXT NOT NULL,
  status TEXT NOT NULL DEFAULT 'active',
  period_kind TEXT NOT NULL DEFAULT 'legacy',
  started_at TIMESTAMPTZ NOT NULL,
  closed_at TIMESTAMPTZ,
  created_by BIGINT,
  reset_reason TEXT,
  source_version TEXT NOT NULL,
  settings_snapshot_json TEXT NOT NULL DEFAULT '{}',
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  CHECK(status IN ('active','closed')),
  CHECK(period_kind IN ('legacy','production','test','shadow'))
);
CREATE UNIQUE INDEX IF NOT EXISTS statistics_periods_name_kind_pg_uidx
  ON statistics_periods(name,period_kind);
CREATE UNIQUE INDEX IF NOT EXISTS statistics_periods_one_active_pg_uidx
  ON statistics_periods(status) WHERE status='active';
CREATE INDEX IF NOT EXISTS statistics_periods_status_started_pg_idx
  ON statistics_periods(status,started_at);

CREATE TABLE IF NOT EXISTS analytics_execution_results (
  id BIGSERIAL PRIMARY KEY,
  execution_id BIGINT NOT NULL UNIQUE,
  analytics_signal_id BIGINT,
  trade_group_id BIGINT,
  period_id BIGINT,
  user_id BIGINT NOT NULL,
  exchange TEXT NOT NULL DEFAULT 'bingx',
  symbol TEXT NOT NULL,
  side TEXT NOT NULL,
  entry_order_type TEXT,
  linkage_status TEXT NOT NULL DEFAULT 'pending',
  first_entry_fill_at TIMESTAMPTZ,
  last_entry_fill_at TIMESTAMPTZ,
  actual_entry_qty NUMERIC,
  actual_entry_avg_price NUMERIC,
  planned_entry_reference NUMERIC,
  execution_reference_price NUMERIC,
  initial_stop_price NUMERIC,
  equity_snapshot_usd NUMERIC,
  planned_risk_percent NUMERIC,
  planned_risk_usd NUMERIC,
  initial_price_risk_usd NUMERIC,
  initial_risk_percent_of_equity NUMERIC,
  estimated_fee_risk_usd NUMERIC,
  expected_loss_at_stop_usd NUMERIC,
  planned_entry_qty NUMERIC,
  stop_distance NUMERIC,
  risk_snapshot_at TIMESTAMPTZ,
  risk_snapshot_source TEXT,
  risk_snapshot_status TEXT,
  risk_snapshot_reason TEXT,
  tp_distribution_json TEXT,
  tp_distribution_source TEXT,
  tp_distribution_locked INTEGER NOT NULL DEFAULT 0,
  tp_distribution_version INTEGER NOT NULL DEFAULT 1,
  first_exit_fill_at TIMESTAMPTZ,
  last_exit_fill_at TIMESTAMPTZ,
  actual_exit_qty NUMERIC,
  actual_exit_avg_price NUMERIC,
  signal_max_tp_index INTEGER NOT NULL DEFAULT 0,
  execution_max_tp_index INTEGER NOT NULL DEFAULT 0,
  canonical_terminal_reason TEXT,
  terminal_detail TEXT,
  strategy_gross_pnl NUMERIC,
  exchange_gross_pnl NUMERIC,
  gross_pnl_source TEXT,
  trading_fee_signed NUMERIC,
  trading_fee_cost NUMERIC,
  funding_signed NUMERIC,
  settlement_asset TEXT,
  net_pnl NUMERIC,
  provisional_net_pnl NUMERIC,
  result_r NUMERIC,
  provisional_result_r NUMERIC,
  entry_slippage_bps NUMERIC,
  limit_price_slippage_bps NUMERIC,
  execution_duration_seconds BIGINT,
  trading_reconciliation_state TEXT NOT NULL DEFAULT 'pending',
  funding_state TEXT NOT NULL DEFAULT 'not_checked',
  financial_state TEXT NOT NULL DEFAULT 'PENDING',
  volume_parity_status TEXT NOT NULL DEFAULT 'pending',
  completeness_mask INTEGER NOT NULL DEFAULT 0,
  completeness_percent NUMERIC NOT NULL DEFAULT 0,
  data_quality_status TEXT NOT NULL DEFAULT 'pending',
  ambiguity_reason TEXT,
  legacy_data INTEGER NOT NULL DEFAULT 1,
  result_version INTEGER NOT NULL DEFAULT 1,
  projection_status TEXT NOT NULL DEFAULT 'pending',
  projection_attempts INTEGER NOT NULL DEFAULT 0,
  projection_next_attempt_at TIMESTAMPTZ DEFAULT NOW(),
  projection_deadline_at TIMESTAMPTZ,
  projection_processing_started_at TIMESTAMPTZ,
  projection_lease_token TEXT,
  projection_last_error TEXT,
  funding_query_start_at TIMESTAMPTZ,
  funding_query_end_at TIMESTAMPTZ,
  funding_event_count INTEGER NOT NULL DEFAULT 0,
  funding_recovery_attempts INTEGER NOT NULL DEFAULT 0,
  funding_zero_observations INTEGER NOT NULL DEFAULT 0,
  funding_first_empty_at TIMESTAMPTZ,
  funding_last_checked_at TIMESTAMPTZ,
  funding_recovery_status TEXT NOT NULL DEFAULT 'not_required',
  funding_recovery_reason TEXT,
  funding_finalized_at TIMESTAMPTZ,
  quality_reasons_json TEXT NOT NULL DEFAULT '[]',
  final_eligible INTEGER NOT NULL DEFAULT 0,
  simulation_eligible INTEGER NOT NULL DEFAULT 0,
  risk_analysis_eligible INTEGER NOT NULL DEFAULT 0,
  quality_gate_version INTEGER NOT NULL DEFAULT 1,
  quality_evaluated_at TIMESTAMPTZ,
  market_event_review_status TEXT NOT NULL DEFAULT 'clear',
  market_event_exclusion_reason TEXT,
  market_event_reviewed_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  finalized_at TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS analytics_execution_results_period_state_pg_idx
  ON analytics_execution_results(period_id,financial_state,last_exit_fill_at);
CREATE INDEX IF NOT EXISTS analytics_execution_results_signal_pg_idx
  ON analytics_execution_results(analytics_signal_id,execution_id);
CREATE INDEX IF NOT EXISTS analytics_execution_results_group_pg_idx
  ON analytics_execution_results(trade_group_id,execution_id);
CREATE INDEX IF NOT EXISTS analytics_execution_results_user_state_pg_idx
  ON analytics_execution_results(user_id,financial_state,updated_at);
CREATE INDEX IF NOT EXISTS analytics_execution_results_quality_pg_idx
  ON analytics_execution_results(data_quality_status,financial_state);
CREATE INDEX IF NOT EXISTS analytics_execution_results_projection_due_pg_idx
  ON analytics_execution_results(projection_status,projection_next_attempt_at);
CREATE INDEX IF NOT EXISTS analytics_execution_results_quality_refresh_pg_idx
  ON analytics_execution_results(quality_gate_version,quality_evaluated_at,updated_at,execution_id);

CREATE TABLE IF NOT EXISTS financial_funding_events (
  id BIGSERIAL PRIMARY KEY,
  execution_id BIGINT,
  user_id BIGINT NOT NULL,
  exchange TEXT NOT NULL DEFAULT 'bingx',
  symbol TEXT NOT NULL,
  position_side TEXT,
  exchange_event_id TEXT NOT NULL,
  amount_signed NUMERIC NOT NULL,
  asset TEXT NOT NULL,
  event_time TIMESTAMPTZ NOT NULL,
  attribution_status TEXT NOT NULL DEFAULT 'unassigned',
  attribution_reason TEXT,
  source_endpoint TEXT NOT NULL,
  metadata_json TEXT NOT NULL DEFAULT '{}',
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE(exchange,user_id,exchange_event_id)
);
CREATE INDEX IF NOT EXISTS financial_funding_events_execution_time_pg_idx
  ON financial_funding_events(execution_id,event_time);
CREATE INDEX IF NOT EXISTS financial_funding_events_user_symbol_time_pg_idx
  ON financial_funding_events(user_id,symbol,event_time);
CREATE INDEX IF NOT EXISTS financial_funding_events_status_time_pg_idx
  ON financial_funding_events(attribution_status,event_time);


CREATE TABLE IF NOT EXISTS statistics_entity_links (
  id BIGSERIAL PRIMARY KEY,
  signal_id BIGINT NOT NULL,
  trade_group_id BIGINT NOT NULL,
  execution_id BIGINT NOT NULL,
  user_id BIGINT NOT NULL,
  symbol TEXT NOT NULL,
  side TEXT NOT NULL,
  source_chat_id BIGINT,
  source_message_id BIGINT,
  identity_fingerprint TEXT NOT NULL,
  linkage_status TEXT NOT NULL DEFAULT 'linked_exact',
  conflict_reason TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE(execution_id),
  UNIQUE(signal_id,trade_group_id,execution_id),
  UNIQUE(identity_fingerprint)
);
CREATE INDEX IF NOT EXISTS statistics_entity_links_signal_group_pg_idx
  ON statistics_entity_links(signal_id,trade_group_id);
CREATE INDEX IF NOT EXISTS statistics_entity_links_user_symbol_pg_idx
  ON statistics_entity_links(user_id,symbol,created_at);

CREATE TABLE IF NOT EXISTS statistics_reset_requests (
  id BIGSERIAL PRIMARY KEY,
  token_hash TEXT NOT NULL UNIQUE,
  actor_user_id BIGINT NOT NULL,
  reason TEXT NOT NULL,
  status TEXT NOT NULL DEFAULT 'pending',
  expires_at TIMESTAMPTZ NOT NULL,
  old_period_id BIGINT NOT NULL,
  new_period_id BIGINT,
  source_version TEXT NOT NULL,
  settings_snapshot_json TEXT NOT NULL DEFAULT '{}',
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  applied_at TIMESTAMPTZ,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  CHECK(status IN ('pending','applied','cancelled','expired','stale'))
);
CREATE INDEX IF NOT EXISTS statistics_reset_requests_actor_status_pg_idx
  ON statistics_reset_requests(actor_user_id,status,created_at);
CREATE INDEX IF NOT EXISTS statistics_reset_requests_old_period_pg_idx
  ON statistics_reset_requests(old_period_id,status);


CREATE TABLE IF NOT EXISTS statistics_quality_audit (
  id BIGSERIAL PRIMARY KEY,
  audit_key TEXT NOT NULL UNIQUE,
  scan_id TEXT NOT NULL,
  period_id BIGINT,
  entity_type TEXT NOT NULL,
  entity_id TEXT NOT NULL,
  action TEXT NOT NULL,
  issue_code TEXT NOT NULL,
  severity TEXT NOT NULL,
  recoverable INTEGER NOT NULL DEFAULT 0,
  reason TEXT NOT NULL,
  actor_user_id BIGINT,
  before_status TEXT,
  after_status TEXT,
  metadata_json TEXT NOT NULL DEFAULT '{}',
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS statistics_quality_audit_period_action_pg_idx
  ON statistics_quality_audit(period_id,action,created_at);
CREATE INDEX IF NOT EXISTS statistics_quality_audit_entity_pg_idx
  ON statistics_quality_audit(entity_type,entity_id,created_at);
"""


_SQLITE_SIGNAL_COLUMNS = {
    "period_id": "INTEGER",
    "linkage_status": "TEXT NOT NULL DEFAULT 'unlinked_legacy'",
    "linked_at": "TEXT",
    "recovery_status": "TEXT NOT NULL DEFAULT 'not_required'",
    "recovery_method": "TEXT NOT NULL DEFAULT 'none'",
    "recovery_started_at": "TEXT",
    "recovery_completed_at": "TEXT",
    "recovery_confidence": "TEXT NOT NULL DEFAULT 'none'",
    "data_quality_status": "TEXT NOT NULL DEFAULT 'legacy'",
    "data_quality_reason": "TEXT",
    "legacy_data": "INTEGER NOT NULL DEFAULT 1",
    "target_percents_source": "TEXT NOT NULL DEFAULT 'source_or_empty'",
    "recovery_attempts": "INTEGER NOT NULL DEFAULT 0",
    "recovery_next_attempt_at": "TEXT",
    "recovery_processing_started_at": "TEXT",
    "recovery_lease_token": "TEXT",
    "recovery_last_error": "TEXT",
    "recovery_cursor_at": "TEXT",
    "quality_reasons_json": "TEXT NOT NULL DEFAULT '[]'",
    "final_eligible": "INTEGER NOT NULL DEFAULT 0",
    "simulation_eligible": "INTEGER NOT NULL DEFAULT 0",
    "risk_analysis_eligible": "INTEGER NOT NULL DEFAULT 0",
    "quality_gate_version": "INTEGER NOT NULL DEFAULT 1",
    "quality_evaluated_at": "TEXT",
}

_PG_SIGNAL_COLUMNS = (
    "ALTER TABLE signal_analytics_signals ADD COLUMN IF NOT EXISTS period_id BIGINT",
    "ALTER TABLE signal_analytics_signals ADD COLUMN IF NOT EXISTS linkage_status TEXT NOT NULL DEFAULT 'unlinked_legacy'",
    "ALTER TABLE signal_analytics_signals ADD COLUMN IF NOT EXISTS linked_at TIMESTAMPTZ",
    "ALTER TABLE signal_analytics_signals ADD COLUMN IF NOT EXISTS recovery_status TEXT NOT NULL DEFAULT 'not_required'",
    "ALTER TABLE signal_analytics_signals ADD COLUMN IF NOT EXISTS recovery_method TEXT NOT NULL DEFAULT 'none'",
    "ALTER TABLE signal_analytics_signals ADD COLUMN IF NOT EXISTS recovery_started_at TIMESTAMPTZ",
    "ALTER TABLE signal_analytics_signals ADD COLUMN IF NOT EXISTS recovery_completed_at TIMESTAMPTZ",
    "ALTER TABLE signal_analytics_signals ADD COLUMN IF NOT EXISTS recovery_confidence TEXT NOT NULL DEFAULT 'none'",
    "ALTER TABLE signal_analytics_signals ADD COLUMN IF NOT EXISTS data_quality_status TEXT NOT NULL DEFAULT 'legacy'",
    "ALTER TABLE signal_analytics_signals ADD COLUMN IF NOT EXISTS data_quality_reason TEXT",
    "ALTER TABLE signal_analytics_signals ADD COLUMN IF NOT EXISTS legacy_data INTEGER NOT NULL DEFAULT 1",
    "ALTER TABLE signal_analytics_signals ADD COLUMN IF NOT EXISTS target_percents_source TEXT NOT NULL DEFAULT 'source_or_empty'",
    "ALTER TABLE signal_analytics_signals ADD COLUMN IF NOT EXISTS recovery_attempts INTEGER NOT NULL DEFAULT 0",
    "ALTER TABLE signal_analytics_signals ADD COLUMN IF NOT EXISTS recovery_next_attempt_at TIMESTAMPTZ",
    "ALTER TABLE signal_analytics_signals ADD COLUMN IF NOT EXISTS recovery_processing_started_at TIMESTAMPTZ",
    "ALTER TABLE signal_analytics_signals ADD COLUMN IF NOT EXISTS recovery_lease_token TEXT",
    "ALTER TABLE signal_analytics_signals ADD COLUMN IF NOT EXISTS recovery_last_error TEXT",
    "ALTER TABLE signal_analytics_signals ADD COLUMN IF NOT EXISTS recovery_cursor_at TIMESTAMPTZ",
    "ALTER TABLE signal_analytics_signals ADD COLUMN IF NOT EXISTS quality_reasons_json TEXT NOT NULL DEFAULT '[]'",
    "ALTER TABLE signal_analytics_signals ADD COLUMN IF NOT EXISTS final_eligible INTEGER NOT NULL DEFAULT 0",
    "ALTER TABLE signal_analytics_signals ADD COLUMN IF NOT EXISTS simulation_eligible INTEGER NOT NULL DEFAULT 0",
    "ALTER TABLE signal_analytics_signals ADD COLUMN IF NOT EXISTS risk_analysis_eligible INTEGER NOT NULL DEFAULT 0",
    "ALTER TABLE signal_analytics_signals ADD COLUMN IF NOT EXISTS quality_gate_version INTEGER NOT NULL DEFAULT 1",
    "ALTER TABLE signal_analytics_signals ADD COLUMN IF NOT EXISTS quality_evaluated_at TIMESTAMPTZ",
)

_SQLITE_EXECUTION_RESULT_COLUMNS = {
    "projection_status": "TEXT NOT NULL DEFAULT 'pending'",
    "projection_attempts": "INTEGER NOT NULL DEFAULT 0",
    "projection_next_attempt_at": "TEXT DEFAULT CURRENT_TIMESTAMP",
    "projection_deadline_at": "TEXT",
    "projection_processing_started_at": "TEXT",
    "projection_lease_token": "TEXT",
    "projection_last_error": "TEXT",
    "funding_query_start_at": "TEXT",
    "funding_query_end_at": "TEXT",
    "funding_event_count": "INTEGER NOT NULL DEFAULT 0",
    "funding_recovery_attempts": "INTEGER NOT NULL DEFAULT 0",
    "funding_finalized_at": "TEXT",
    "expected_loss_at_stop_usd": "TEXT",
    "planned_entry_qty": "TEXT",
    "stop_distance": "TEXT",
    "risk_snapshot_at": "TEXT",
    "risk_snapshot_source": "TEXT",
    "risk_snapshot_status": "TEXT",
    "risk_snapshot_reason": "TEXT",
    "tp_distribution_json": "TEXT",
    "tp_distribution_source": "TEXT",
    "tp_distribution_locked": "INTEGER NOT NULL DEFAULT 0",
    "tp_distribution_version": "INTEGER NOT NULL DEFAULT 1",
    "funding_zero_observations": "INTEGER NOT NULL DEFAULT 0",
    "funding_first_empty_at": "TEXT",
    "funding_last_checked_at": "TEXT",
    "funding_recovery_status": "TEXT NOT NULL DEFAULT 'not_required'",
    "funding_recovery_reason": "TEXT",
    "quality_reasons_json": "TEXT NOT NULL DEFAULT '[]'",
    "final_eligible": "INTEGER NOT NULL DEFAULT 0",
    "simulation_eligible": "INTEGER NOT NULL DEFAULT 0",
    "risk_analysis_eligible": "INTEGER NOT NULL DEFAULT 0",
    "quality_gate_version": "INTEGER NOT NULL DEFAULT 1",
    "quality_evaluated_at": "TEXT",
    "market_event_review_status": "TEXT NOT NULL DEFAULT 'clear'",
    "market_event_exclusion_reason": "TEXT",
    "market_event_reviewed_at": "TEXT",
}

_PG_EXECUTION_RESULT_COLUMNS = (
    "ALTER TABLE analytics_execution_results ADD COLUMN IF NOT EXISTS projection_status TEXT NOT NULL DEFAULT 'pending'",
    "ALTER TABLE analytics_execution_results ADD COLUMN IF NOT EXISTS projection_attempts INTEGER NOT NULL DEFAULT 0",
    "ALTER TABLE analytics_execution_results ADD COLUMN IF NOT EXISTS projection_next_attempt_at TIMESTAMPTZ DEFAULT NOW()",
    "ALTER TABLE analytics_execution_results ADD COLUMN IF NOT EXISTS projection_deadline_at TIMESTAMPTZ",
    "ALTER TABLE analytics_execution_results ADD COLUMN IF NOT EXISTS projection_processing_started_at TIMESTAMPTZ",
    "ALTER TABLE analytics_execution_results ADD COLUMN IF NOT EXISTS projection_lease_token TEXT",
    "ALTER TABLE analytics_execution_results ADD COLUMN IF NOT EXISTS projection_last_error TEXT",
    "ALTER TABLE analytics_execution_results ADD COLUMN IF NOT EXISTS funding_query_start_at TIMESTAMPTZ",
    "ALTER TABLE analytics_execution_results ADD COLUMN IF NOT EXISTS funding_query_end_at TIMESTAMPTZ",
    "ALTER TABLE analytics_execution_results ADD COLUMN IF NOT EXISTS funding_event_count INTEGER NOT NULL DEFAULT 0",
    "ALTER TABLE analytics_execution_results ADD COLUMN IF NOT EXISTS funding_recovery_attempts INTEGER NOT NULL DEFAULT 0",
    "ALTER TABLE analytics_execution_results ADD COLUMN IF NOT EXISTS funding_finalized_at TIMESTAMPTZ",
    "ALTER TABLE analytics_execution_results ADD COLUMN IF NOT EXISTS expected_loss_at_stop_usd NUMERIC",
    "ALTER TABLE analytics_execution_results ADD COLUMN IF NOT EXISTS planned_entry_qty NUMERIC",
    "ALTER TABLE analytics_execution_results ADD COLUMN IF NOT EXISTS stop_distance NUMERIC",
    "ALTER TABLE analytics_execution_results ADD COLUMN IF NOT EXISTS risk_snapshot_at TIMESTAMPTZ",
    "ALTER TABLE analytics_execution_results ADD COLUMN IF NOT EXISTS risk_snapshot_source TEXT",
    "ALTER TABLE analytics_execution_results ADD COLUMN IF NOT EXISTS risk_snapshot_status TEXT",
    "ALTER TABLE analytics_execution_results ADD COLUMN IF NOT EXISTS risk_snapshot_reason TEXT",
    "ALTER TABLE analytics_execution_results ADD COLUMN IF NOT EXISTS tp_distribution_json TEXT",
    "ALTER TABLE analytics_execution_results ADD COLUMN IF NOT EXISTS tp_distribution_source TEXT",
    "ALTER TABLE analytics_execution_results ADD COLUMN IF NOT EXISTS tp_distribution_locked INTEGER NOT NULL DEFAULT 0",
    "ALTER TABLE analytics_execution_results ADD COLUMN IF NOT EXISTS tp_distribution_version INTEGER NOT NULL DEFAULT 1",
    "ALTER TABLE analytics_execution_results ADD COLUMN IF NOT EXISTS funding_zero_observations INTEGER NOT NULL DEFAULT 0",
    "ALTER TABLE analytics_execution_results ADD COLUMN IF NOT EXISTS funding_first_empty_at TIMESTAMPTZ",
    "ALTER TABLE analytics_execution_results ADD COLUMN IF NOT EXISTS funding_last_checked_at TIMESTAMPTZ",
    "ALTER TABLE analytics_execution_results ADD COLUMN IF NOT EXISTS funding_recovery_status TEXT NOT NULL DEFAULT 'not_required'",
    "ALTER TABLE analytics_execution_results ADD COLUMN IF NOT EXISTS funding_recovery_reason TEXT",
    "ALTER TABLE analytics_execution_results ADD COLUMN IF NOT EXISTS quality_reasons_json TEXT NOT NULL DEFAULT '[]'",
    "ALTER TABLE analytics_execution_results ADD COLUMN IF NOT EXISTS final_eligible INTEGER NOT NULL DEFAULT 0",
    "ALTER TABLE analytics_execution_results ADD COLUMN IF NOT EXISTS simulation_eligible INTEGER NOT NULL DEFAULT 0",
    "ALTER TABLE analytics_execution_results ADD COLUMN IF NOT EXISTS risk_analysis_eligible INTEGER NOT NULL DEFAULT 0",
    "ALTER TABLE analytics_execution_results ADD COLUMN IF NOT EXISTS quality_gate_version INTEGER NOT NULL DEFAULT 1",
    "ALTER TABLE analytics_execution_results ADD COLUMN IF NOT EXISTS quality_evaluated_at TIMESTAMPTZ",
    "ALTER TABLE analytics_execution_results ADD COLUMN IF NOT EXISTS market_event_review_status TEXT NOT NULL DEFAULT 'clear'",
    "ALTER TABLE analytics_execution_results ADD COLUMN IF NOT EXISTS market_event_exclusion_reason TEXT",
    "ALTER TABLE analytics_execution_results ADD COLUMN IF NOT EXISTS market_event_reviewed_at TIMESTAMPTZ",
)

_SQLITE_FILL_COLUMNS = {
    "position_side": "TEXT",
    "liquidity_role": "TEXT",
    "source_endpoint": "TEXT NOT NULL DEFAULT 'bingx_fill_history'",
    "ingested_at": "TEXT",
}

_PG_FILL_COLUMNS = (
    "ALTER TABLE financial_reconciliation_fills ADD COLUMN IF NOT EXISTS position_side TEXT",
    "ALTER TABLE financial_reconciliation_fills ADD COLUMN IF NOT EXISTS liquidity_role TEXT",
    "ALTER TABLE financial_reconciliation_fills ADD COLUMN IF NOT EXISTS source_endpoint TEXT NOT NULL DEFAULT 'bingx_fill_history'",
    "ALTER TABLE financial_reconciliation_fills ADD COLUMN IF NOT EXISTS ingested_at TIMESTAMPTZ",
)


async def _sqlite_columns(conn: Any, table: str) -> set[str]:
    cursor = await conn.execute(f"PRAGMA table_info({table})")
    return {str(row[1]) for row in await cursor.fetchall()}


async def _sqlite_seed_legacy_period(conn: Any, *, source_version: str) -> int | None:
    now = datetime.now(timezone.utc).isoformat()
    await conn.execute(
        """
        INSERT OR IGNORE INTO statistics_periods(
          name,status,period_kind,started_at,source_version,
          settings_snapshot_json,created_at,updated_at
        )
        SELECT ?, 'active', ?, ?, ?, '{}', ?, ?
        WHERE NOT EXISTS (
          SELECT 1 FROM statistics_periods WHERE status='active'
        )
        """,
        (LEGACY_PERIOD_NAME, LEGACY_PERIOD_KIND, now, source_version, now, now),
    )
    cursor = await conn.execute(
        "SELECT id FROM statistics_periods WHERE status='active' ORDER BY id LIMIT 1"
    )
    row = await cursor.fetchone()
    if not row:
        return None
    period_id = int(row[0])
    await conn.execute(
        "UPDATE signal_analytics_signals SET legacy_data=1 "
        "WHERE period_id IS NULL"
    )
    await conn.execute(
        "UPDATE signal_analytics_signals SET period_id=? WHERE period_id IS NULL",
        (period_id,),
    )
    await conn.execute(
        "UPDATE signal_analytics_signals SET "
        "linkage_status=CASE WHEN linkage_status IS NULL OR linkage_status='' "
        "THEN 'unlinked_legacy' ELSE linkage_status END, "
        "recovery_status=CASE WHEN needs_recovery=1 THEN 'pending' "
        "WHEN recovery_status IS NULL OR recovery_status='' THEN 'not_required' "
        "ELSE recovery_status END, "
        "recovery_method=COALESCE(NULLIF(recovery_method,''),'none'), "
        "recovery_confidence=COALESCE(NULLIF(recovery_confidence,''),'none'), "
        "data_quality_status=COALESCE(NULLIF(data_quality_status,''),'legacy'), "
        "data_quality_reason=COALESCE(data_quality_reason,'legacy_backfill_step3') "
        "WHERE legacy_data=1"
    )
    return period_id


async def _pg_seed_legacy_period(conn: Any, *, source_version: str) -> int | None:
    row = await conn.fetchrow(
        """
        INSERT INTO statistics_periods(
          name,status,period_kind,started_at,source_version,settings_snapshot_json
        )
        SELECT $1, 'active', $2, NOW(), $3, '{}'
        WHERE NOT EXISTS (
          SELECT 1 FROM statistics_periods WHERE status='active'
        )
        ON CONFLICT DO NOTHING
        RETURNING id
        """,
        LEGACY_PERIOD_NAME,
        LEGACY_PERIOD_KIND,
        source_version,
    )
    period_id = int(row["id"]) if row else None
    if period_id is None:
        existing = await conn.fetchrow(
            "SELECT id FROM statistics_periods WHERE status='active' ORDER BY id LIMIT 1"
        )
        if existing:
            period_id = int(existing["id"])
    if period_id is None:
        return None
    await conn.execute(
        "UPDATE signal_analytics_signals SET legacy_data=1 "
        "WHERE period_id IS NULL"
    )
    await conn.execute(
        "UPDATE signal_analytics_signals SET period_id=$1 WHERE period_id IS NULL",
        period_id,
    )
    await conn.execute(
        """
        UPDATE signal_analytics_signals SET
          linkage_status=CASE
            WHEN linkage_status IS NULL OR linkage_status='' THEN 'unlinked_legacy'
            ELSE linkage_status END,
          recovery_status=CASE
            WHEN needs_recovery=1 THEN 'pending'
            WHEN recovery_status IS NULL OR recovery_status='' THEN 'not_required'
            ELSE recovery_status END,
          recovery_method=COALESCE(NULLIF(recovery_method,''),'none'),
          recovery_confidence=COALESCE(NULLIF(recovery_confidence,''),'none'),
          data_quality_status=COALESCE(NULLIF(data_quality_status,''),'legacy'),
          data_quality_reason=COALESCE(data_quality_reason,'legacy_backfill_step3')
        WHERE legacy_data=1
        """
    )
    return period_id


async def sqlite_migrate_statistics_v2(
    conn: Any,
    *,
    source_version: str,
    seed_legacy_period: bool = True,
) -> int | None:
    """Idempotently add statistics-v2 columns/indexes to SQLite."""

    signal_columns = await _sqlite_columns(conn, "signal_analytics_signals")
    for column, ddl in _SQLITE_SIGNAL_COLUMNS.items():
        if column not in signal_columns:
            await conn.execute(
                f"ALTER TABLE signal_analytics_signals ADD COLUMN {column} {ddl}"
            )

    result_columns = await _sqlite_columns(conn, "analytics_execution_results")
    for column, ddl in _SQLITE_EXECUTION_RESULT_COLUMNS.items():
        if column not in result_columns:
            await conn.execute(
                f"ALTER TABLE analytics_execution_results ADD COLUMN {column} {ddl}"
            )
    await conn.execute(
        "UPDATE analytics_execution_results SET "
        "projection_status=COALESCE(NULLIF(projection_status,''),'pending'), "
        "projection_attempts=COALESCE(projection_attempts,0), "
        "funding_event_count=COALESCE(funding_event_count,0), "
        "funding_recovery_attempts=COALESCE(funding_recovery_attempts,0), "
        "funding_zero_observations=COALESCE(funding_zero_observations,0), "
        "funding_recovery_status=COALESCE(NULLIF(funding_recovery_status,''),'not_required'), "
        "quality_reasons_json=COALESCE(NULLIF(quality_reasons_json,''),'[]'), "
        "market_event_review_status=COALESCE(NULLIF(market_event_review_status,''),'clear'), "
        "final_eligible=COALESCE(final_eligible,0), "
        "simulation_eligible=COALESCE(simulation_eligible,0), "
        "risk_analysis_eligible=COALESCE(risk_analysis_eligible,0), "
        "projection_next_attempt_at=COALESCE(projection_next_attempt_at,updated_at,created_at,CURRENT_TIMESTAMP)"
    )

    fill_columns = await _sqlite_columns(conn, "financial_reconciliation_fills")
    for column, ddl in _SQLITE_FILL_COLUMNS.items():
        if column not in fill_columns:
            await conn.execute(
                f"ALTER TABLE financial_reconciliation_fills ADD COLUMN {column} {ddl}"
            )
    await conn.execute(
        "UPDATE financial_reconciliation_fills SET "
        "source_endpoint=COALESCE(NULLIF(source_endpoint,''),'bingx_fill_history'), "
        "ingested_at=COALESCE(ingested_at,created_at)"
    )

    indexes = (
        "CREATE INDEX IF NOT EXISTS signal_analytics_period_status_published_idx "
        "ON signal_analytics_signals(period_id,status,published_at)",
        "CREATE INDEX IF NOT EXISTS signal_analytics_period_completed_idx "
        "ON signal_analytics_signals(period_id,completed_at)",
        "CREATE INDEX IF NOT EXISTS signal_analytics_trade_group_idx "
        "ON signal_analytics_signals(trade_group_id)",
        "CREATE INDEX IF NOT EXISTS signal_analytics_recovery_status_idx "
        "ON signal_analytics_signals(needs_recovery,status)",
        "CREATE INDEX IF NOT EXISTS signal_analytics_recovery_due_idx "
        "ON signal_analytics_signals(recovery_status,recovery_next_attempt_at,id)",
        "CREATE INDEX IF NOT EXISTS financial_reconciliation_fills_execution_time_idx "
        "ON financial_reconciliation_fills(execution_id,fill_time)",
        "CREATE INDEX IF NOT EXISTS analytics_execution_results_projection_due_idx "
        "ON analytics_execution_results(projection_status,projection_next_attempt_at)",
        "CREATE INDEX IF NOT EXISTS analytics_execution_results_quality_gate_idx "
        "ON analytics_execution_results(final_eligible,simulation_eligible,risk_analysis_eligible)",
        "CREATE INDEX IF NOT EXISTS signal_analytics_quality_gate_idx "
        "ON signal_analytics_signals(final_eligible,simulation_eligible,risk_analysis_eligible)",
        "CREATE INDEX IF NOT EXISTS analytics_execution_results_quality_refresh_idx "
        "ON analytics_execution_results(quality_gate_version,quality_evaluated_at,updated_at,execution_id)",
        "CREATE INDEX IF NOT EXISTS signal_analytics_quality_refresh_idx "
        "ON signal_analytics_signals(quality_gate_version,quality_evaluated_at,updated_at,id)",
    )
    for statement in indexes:
        await conn.execute(statement)

    if seed_legacy_period:
        return await _sqlite_seed_legacy_period(
            conn, source_version=source_version
        )
    return None


async def pg_preflight_statistics_v2_columns(conn: Any) -> None:
    """Add columns to already-existing PostgreSQL tables before index DDL.

    ``CREATE TABLE IF NOT EXISTS`` does not update an existing table.  The
    monolithic statistics schema also contains indexes that reference newer
    columns, so running it first against an older production database can fail
    with ``UndefinedColumnError``.  ``ALTER TABLE IF EXISTS`` is intentionally
    used here: on a fresh database these statements are harmless no-ops, while
    on an upgraded database they make every index dependency available before
    the full schema is executed.
    """

    statements = (
        *_PG_SIGNAL_COLUMNS,
        *_PG_EXECUTION_RESULT_COLUMNS,
        *_PG_FILL_COLUMNS,
    )
    for statement in statements:
        safe_statement = statement.replace(
            "ALTER TABLE ", "ALTER TABLE IF EXISTS ", 1
        )
        await conn.execute(safe_statement)


async def pg_migrate_statistics_v2(
    conn: Any,
    *,
    source_version: str,
    seed_legacy_period: bool = True,
) -> int | None:
    """Idempotently add statistics-v2 columns/indexes to PostgreSQL."""

    for statement in _PG_SIGNAL_COLUMNS:
        await conn.execute(statement)
    for statement in _PG_EXECUTION_RESULT_COLUMNS:
        await conn.execute(statement)
    await conn.execute(
        "UPDATE analytics_execution_results SET "
        "projection_status=COALESCE(NULLIF(projection_status,''),'pending'), "
        "projection_attempts=COALESCE(projection_attempts,0), "
        "funding_event_count=COALESCE(funding_event_count,0), "
        "funding_recovery_attempts=COALESCE(funding_recovery_attempts,0), "
        "funding_zero_observations=COALESCE(funding_zero_observations,0), "
        "funding_recovery_status=COALESCE(NULLIF(funding_recovery_status,''),'not_required'), "
        "quality_reasons_json=COALESCE(NULLIF(quality_reasons_json,''),'[]'), "
        "market_event_review_status=COALESCE(NULLIF(market_event_review_status,''),'clear'), "
        "final_eligible=COALESCE(final_eligible,0), "
        "simulation_eligible=COALESCE(simulation_eligible,0), "
        "risk_analysis_eligible=COALESCE(risk_analysis_eligible,0), "
        "projection_next_attempt_at=COALESCE(projection_next_attempt_at,updated_at,created_at,NOW())"
    )
    for statement in _PG_FILL_COLUMNS:
        await conn.execute(statement)
    await conn.execute(
        "UPDATE financial_reconciliation_fills SET "
        "source_endpoint=COALESCE(NULLIF(source_endpoint,''),'bingx_fill_history'), "
        "ingested_at=COALESCE(ingested_at,created_at)"
    )
    indexes = (
        "CREATE INDEX IF NOT EXISTS signal_analytics_period_status_published_pg_idx "
        "ON signal_analytics_signals(period_id,status,published_at)",
        "CREATE INDEX IF NOT EXISTS signal_analytics_period_completed_pg_idx "
        "ON signal_analytics_signals(period_id,completed_at)",
        "CREATE INDEX IF NOT EXISTS signal_analytics_trade_group_pg_idx "
        "ON signal_analytics_signals(trade_group_id)",
        "CREATE INDEX IF NOT EXISTS signal_analytics_recovery_status_pg_idx "
        "ON signal_analytics_signals(needs_recovery,status)",
        "CREATE INDEX IF NOT EXISTS signal_analytics_recovery_due_pg_idx "
        "ON signal_analytics_signals(recovery_status,recovery_next_attempt_at,id)",
        "CREATE INDEX IF NOT EXISTS financial_reconciliation_fills_execution_time_pg_idx "
        "ON financial_reconciliation_fills(execution_id,fill_time)",
        "CREATE INDEX IF NOT EXISTS analytics_execution_results_projection_due_pg_idx "
        "ON analytics_execution_results(projection_status,projection_next_attempt_at)",
        "CREATE INDEX IF NOT EXISTS analytics_execution_results_quality_gate_pg_idx "
        "ON analytics_execution_results(final_eligible,simulation_eligible,risk_analysis_eligible)",
        "CREATE INDEX IF NOT EXISTS signal_analytics_quality_gate_pg_idx "
        "ON signal_analytics_signals(final_eligible,simulation_eligible,risk_analysis_eligible)",
        "CREATE INDEX IF NOT EXISTS analytics_execution_results_quality_refresh_pg_idx "
        "ON analytics_execution_results(quality_gate_version,quality_evaluated_at,updated_at,execution_id)",
        "CREATE INDEX IF NOT EXISTS signal_analytics_quality_refresh_pg_idx "
        "ON signal_analytics_signals(quality_gate_version,quality_evaluated_at,updated_at,id)",
    )
    for statement in indexes:
        await conn.execute(statement)

    if seed_legacy_period:
        return await _pg_seed_legacy_period(conn, source_version=source_version)
    return None


async def seed_statistics_legacy_period(
    conn: Any,
    *,
    source_version: str,
    postgres: bool,
) -> int | None:
    """Public migration-tool hook to seed/backfill after a table copy."""

    if postgres:
        return await _pg_seed_legacy_period(conn, source_version=source_version)
    return await _sqlite_seed_legacy_period(conn, source_version=source_version)
