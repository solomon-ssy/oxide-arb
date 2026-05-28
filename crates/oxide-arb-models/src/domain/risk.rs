//! Risk engine domain models.

use crate::{
    enums::risk::{
        BreakerStateName, CircuitBreakerLevel, ReconciliationStatus, RiskAuditEventType,
    },
    types::{MarketId, OpportunityId, TradeId, Usd},
};
use chrono::{DateTime, NaiveDate, Utc};
use rust_decimal::Decimal;
use sea_orm::{DeriveIntoActiveModel, DerivePartialModel, FromQueryResult};
use serde::{Deserialize, Serialize};

// ── Runtime state ───────────────────────────────────────────────────

/// In-memory aggregate of the risk engine's current state.
///
/// Produced by `RiskEngine::snapshot()` and consumed by the persistence
/// layer via `From<&RiskEngineState> for UpsertRiskEngineState`.
/// Not a DB projection — see `RiskStateInfo` for that.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RiskEngineState {
    pub breaker_state: BreakerStateName,
    pub breaker_level: Option<CircuitBreakerLevel>,
    pub is_halted: bool,
    pub halt_reason: Option<String>,
    pub cooldown_until: Option<DateTime<Utc>>,
    pub total_exposure: Usd,
    pub hourly_loss_usd: Usd,
    pub hourly_fee_usd: Usd,
    pub hourly_trade_count: i32,
    pub hourly_success_count: i32,
    pub hourly_miss_count: i32,
    pub hourly_window_start: DateTime<Utc>,
    pub daily_pnl: Usd,
    pub daily_loss_usd: Usd,
    pub daily_fee_usd: Usd,
    pub daily_budget_spent: Usd,
    pub daily_trade_count: i32,
    pub daily_success_count: i32,
    pub daily_miss_count: i32,
    pub daily_window_start: NaiveDate,
    pub weekly_loss_usd: Usd,
    pub weekly_trade_count: i32,
    pub weekly_window_start: NaiveDate,
    pub consecutive_misses: i32,
    pub cooldown_multiplier: i32,
    pub hwm_equity: Usd,
    pub last_emergency_at: Option<DateTime<Utc>>,
    pub last_emergency_reason: Option<String>,
    pub snapshot_at: DateTime<Utc>,
}

// ── DB read models ──────────────────────────────────────────────────

/// 1:1 DB row projection for the `risk_engine_state` singleton.
#[derive(Debug, Clone, Serialize, Deserialize, DerivePartialModel, FromQueryResult)]
#[sea_orm(entity = "crate::entities::risk_state::Entity")]
pub struct RiskStateInfo {
    pub id: i32,
    pub breaker_state: BreakerStateName,
    pub breaker_level: Option<CircuitBreakerLevel>,
    pub is_halted: bool,
    pub halt_reason: Option<String>,
    pub consecutive_misses: i32,
    pub cooldown_until: Option<DateTime<Utc>>,
    pub cooldown_multiplier: i32,
    pub total_exposure: Usd,
    pub hourly_loss_usd: Usd,
    pub hourly_fee_usd: Usd,
    pub hourly_trade_count: i32,
    pub hourly_success_count: i32,
    pub hourly_miss_count: i32,
    pub hourly_window_start: DateTime<Utc>,
    pub daily_loss_usd: Usd,
    pub daily_fee_usd: Usd,
    pub daily_pnl: Usd,
    pub daily_budget_spent: Usd,
    pub daily_trade_count: i32,
    pub daily_success_count: i32,
    pub daily_miss_count: i32,
    pub daily_window_start: NaiveDate,
    pub weekly_loss_usd: Usd,
    pub weekly_trade_count: i32,
    pub weekly_window_start: NaiveDate,
    pub hwm_equity: Usd,
    pub last_emergency_at: Option<DateTime<Utc>>,
    pub last_emergency_reason: Option<String>,
    pub updated_at: DateTime<Utc>,
}

info_from_model!(RiskStateInfo, crate::entities::risk_state::Model, {
    id, breaker_state, breaker_level, is_halted, halt_reason,
    consecutive_misses, cooldown_until, cooldown_multiplier,
    total_exposure, hourly_loss_usd, hourly_fee_usd,
    hourly_trade_count, hourly_success_count, hourly_miss_count,
    hourly_window_start, daily_loss_usd, daily_fee_usd, daily_pnl,
    daily_budget_spent, daily_trade_count, daily_success_count,
    daily_miss_count, daily_window_start, weekly_loss_usd,
    weekly_trade_count, weekly_window_start, hwm_equity,
    last_emergency_at, last_emergency_reason, updated_at,
});

/// Recovery path: restore engine runtime state from the DB projection.
impl From<&RiskStateInfo> for RiskEngineState {
    fn from(info: &RiskStateInfo) -> Self {
        Self {
            breaker_state: info.breaker_state,
            breaker_level: info.breaker_level,
            is_halted: info.is_halted,
            halt_reason: info.halt_reason.clone(),
            cooldown_until: info.cooldown_until,
            total_exposure: info.total_exposure,
            hourly_loss_usd: info.hourly_loss_usd,
            hourly_fee_usd: info.hourly_fee_usd,
            hourly_trade_count: info.hourly_trade_count,
            hourly_success_count: info.hourly_success_count,
            hourly_miss_count: info.hourly_miss_count,
            hourly_window_start: info.hourly_window_start,
            daily_pnl: info.daily_pnl,
            daily_loss_usd: info.daily_loss_usd,
            daily_fee_usd: info.daily_fee_usd,
            daily_budget_spent: info.daily_budget_spent,
            daily_trade_count: info.daily_trade_count,
            daily_success_count: info.daily_success_count,
            daily_miss_count: info.daily_miss_count,
            daily_window_start: info.daily_window_start,
            weekly_loss_usd: info.weekly_loss_usd,
            weekly_trade_count: info.weekly_trade_count,
            weekly_window_start: info.weekly_window_start,
            consecutive_misses: info.consecutive_misses,
            cooldown_multiplier: info.cooldown_multiplier,
            hwm_equity: info.hwm_equity,
            last_emergency_at: info.last_emergency_at,
            last_emergency_reason: info.last_emergency_reason.clone(),
            snapshot_at: info.updated_at,
        }
    }
}

/// Emergency context for kill-switch / circuit-breaker triggers.
/// Runtime-only — not a DB projection.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EmergencyContext {
    pub trigger_level: CircuitBreakerLevel,
    pub reason: String,
    pub risk_state: RiskEngineState,
    pub open_positions_count: u32,
    pub open_reservations_count: u32,
    pub triggered_at: DateTime<Utc>,
}

// ── Write DTOs ──────────────────────────────────────────────────────

/// Upsert payload for the `risk_engine_state` singleton row.
///
/// `updated_at` is database-managed by the Postgres default and update trigger.
#[derive(Debug, Clone, DeriveIntoActiveModel)]
#[sea_orm(active_model = "super::super::entities::risk_state::ActiveModel")]
pub struct UpsertRiskEngineState {
    pub id: i32,
    pub breaker_state: BreakerStateName,
    pub breaker_level: Option<CircuitBreakerLevel>,
    pub is_halted: bool,
    pub halt_reason: Option<String>,
    pub consecutive_misses: i32,
    pub cooldown_until: Option<DateTime<Utc>>,
    pub cooldown_multiplier: i32,
    pub total_exposure: Usd,
    pub hourly_loss_usd: Usd,
    pub hourly_fee_usd: Usd,
    pub hourly_trade_count: i32,
    pub hourly_success_count: i32,
    pub hourly_miss_count: i32,
    pub hourly_window_start: DateTime<Utc>,
    pub daily_loss_usd: Usd,
    pub daily_fee_usd: Usd,
    pub daily_pnl: Usd,
    pub daily_budget_spent: Usd,
    pub daily_trade_count: i32,
    pub daily_success_count: i32,
    pub daily_miss_count: i32,
    pub daily_window_start: NaiveDate,
    pub weekly_loss_usd: Usd,
    pub weekly_trade_count: i32,
    pub weekly_window_start: NaiveDate,
    pub hwm_equity: Usd,
    pub last_emergency_at: Option<DateTime<Utc>>,
    pub last_emergency_reason: Option<String>,
}

impl From<&RiskEngineState> for UpsertRiskEngineState {
    fn from(s: &RiskEngineState) -> Self {
        Self {
            id: 1,
            breaker_state: s.breaker_state,
            breaker_level: s.breaker_level,
            is_halted: s.is_halted,
            halt_reason: s.halt_reason.clone(),
            consecutive_misses: s.consecutive_misses,
            cooldown_until: s.cooldown_until,
            cooldown_multiplier: s.cooldown_multiplier,
            total_exposure: s.total_exposure,
            hourly_loss_usd: s.hourly_loss_usd,
            hourly_fee_usd: s.hourly_fee_usd,
            hourly_trade_count: s.hourly_trade_count,
            hourly_success_count: s.hourly_success_count,
            hourly_miss_count: s.hourly_miss_count,
            hourly_window_start: s.hourly_window_start,
            daily_loss_usd: s.daily_loss_usd,
            daily_fee_usd: s.daily_fee_usd,
            daily_pnl: s.daily_pnl,
            daily_budget_spent: s.daily_budget_spent,
            daily_trade_count: s.daily_trade_count,
            daily_success_count: s.daily_success_count,
            daily_miss_count: s.daily_miss_count,
            daily_window_start: s.daily_window_start,
            weekly_loss_usd: s.weekly_loss_usd,
            weekly_trade_count: s.weekly_trade_count,
            weekly_window_start: s.weekly_window_start,
            hwm_equity: s.hwm_equity,
            last_emergency_at: s.last_emergency_at,
            last_emergency_reason: s.last_emergency_reason.clone(),
        }
    }
}

/// DB row projection for risk audit events.
#[derive(Debug, Clone, Serialize, Deserialize, DerivePartialModel, FromQueryResult)]
#[sea_orm(entity = "crate::entities::risk_audit_event::Entity")]
pub struct RiskAuditEventInfo {
    pub id: i64,
    pub event_type: RiskAuditEventType,
    pub opportunity_id: Option<OpportunityId>,
    pub trade_id: Option<TradeId>,
    pub payload: serde_json::Value,
    pub created_at: DateTime<Utc>,
}

info_from_model!(RiskAuditEventInfo, crate::entities::risk_audit_event::Model, {
    id, event_type, opportunity_id, trade_id, payload, created_at,
});

/// DB row projection for reconciliation reports.
#[derive(Debug, Clone, Serialize, Deserialize, DerivePartialModel, FromQueryResult)]
#[sea_orm(entity = "crate::entities::reconciliation_report::Entity")]
pub struct ReconciliationInfo {
    pub id: i64,
    pub status: ReconciliationStatus,
    pub mismatches: serde_json::Value,
    pub internal_balance: Usd,
    pub external_balance: Usd,
    pub internal_exposure: Usd,
    pub external_exposure: Usd,
    pub reserved: Usd,
    pub tolerance: Usd,
    pub checked_at: DateTime<Utc>,
    pub duration_ms: i64,
}

info_from_model!(ReconciliationInfo, crate::entities::reconciliation_report::Model, {
    id, status, mismatches, internal_balance, external_balance,
    internal_exposure, external_exposure, reserved, tolerance,
    checked_at, duration_ms,
});

/// DB row projection for emergency snapshots.
#[derive(Debug, Clone, Serialize, Deserialize, DerivePartialModel, FromQueryResult)]
#[sea_orm(entity = "crate::entities::emergency_snapshot::Entity")]
pub struct EmergencySnapshotInfo {
    pub id: i64,
    pub trigger_level: CircuitBreakerLevel,
    pub reason: String,
    pub risk_state: serde_json::Value,
    pub open_positions_count: i32,
    pub open_reservations_count: i32,
    pub triggered_at: DateTime<Utc>,
}

info_from_model!(EmergencySnapshotInfo, crate::entities::emergency_snapshot::Model, {
    id, trigger_level, reason, risk_state, open_positions_count,
    open_reservations_count, triggered_at,
});

/// All fields required to persist a new risk audit event.
#[derive(Debug, Clone, DeriveIntoActiveModel)]
#[sea_orm(active_model = "super::super::entities::risk_audit_event::ActiveModel")]
pub struct NewRiskAuditEvent {
    pub event_type: RiskAuditEventType,
    pub opportunity_id: Option<OpportunityId>,
    pub trade_id: Option<TradeId>,
    pub payload: serde_json::Value,
    pub created_at: DateTime<Utc>,
}

/// All fields required to persist a new emergency snapshot.
#[derive(Debug, Clone, DeriveIntoActiveModel)]
#[sea_orm(active_model = "super::super::entities::emergency_snapshot::ActiveModel")]
pub struct NewEmergencySnapshot {
    pub trigger_level: CircuitBreakerLevel,
    pub reason: String,
    pub risk_state: serde_json::Value,
    pub open_positions_count: i32,
    pub open_reservations_count: i32,
    pub triggered_at: DateTime<Utc>,
}

/// All fields required to persist a new reconciliation report.
#[derive(Debug, Clone, DeriveIntoActiveModel)]
#[sea_orm(active_model = "super::super::entities::reconciliation_report::ActiveModel")]
pub struct NewReconciliationReport {
    pub status: ReconciliationStatus,
    pub mismatches: serde_json::Value,
    pub internal_balance: Usd,
    pub external_balance: Usd,
    pub internal_exposure: Usd,
    pub external_exposure: Usd,
    pub reserved: Usd,
    pub tolerance: Usd,
    pub checked_at: DateTime<Utc>,
    pub duration_ms: i64,
}

// ── Value objects ───────────────────────────────────────────────────

/// Per-market exposure summary for risk dashboard.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MarketExposure {
    pub market_id: MarketId,
    pub position_value: Usd,
    pub reserved_value: Usd,
    pub total_exposure: Usd,
}

/// Probability quality metadata consumed by the Kelly calculator.
///
/// Bridges the algorithm crate (calibration output) and the risk crate
/// (sizing input). All fields are `Decimal` — no `f64`.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct ProbabilityInput {
    /// Calibrated win probability after empirical Bayes adjustment.
    pub calibrated_win_prob: Decimal,
    /// Estimated FOK fill probability (from `fill_probability` estimator).
    pub fill_prob: Decimal,
    /// Calibration model confidence (0..1). Low confidence → larger haircut.
    pub calibration_confidence: Decimal,
    /// Number of historical samples used in calibration.
    pub sample_size: u32,
    /// Seconds since the calibration model was last updated.
    pub model_staleness_secs: u64,
    /// Expected slippage as a fraction of cost (0..1).
    pub expected_slippage_pct: Decimal,
    /// Expected failure cost as a fraction of cost (0..1).
    /// Accounts for gas/fees wasted on failed FOK attempts.
    pub expected_failure_cost_pct: Decimal,
}
