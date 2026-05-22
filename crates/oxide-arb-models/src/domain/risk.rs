//! Risk engine domain models.

use crate::enums::risk::{BreakerStateName, CircuitBreakerLevel};
use crate::types::{MarketId, Usd};
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

/// Persisted snapshot of the risk engine state.
///
/// Used for crash recovery: the risk engine loads this on startup and
/// restores its internal FSM, accumulators, and blacklist from it.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RiskEngineSnapshot {
    pub breaker_state: BreakerStateName,
    pub breaker_level: Option<CircuitBreakerLevel>,
    pub breaker_reason: Option<String>,
    pub cooling_until: Option<DateTime<Utc>>,
    pub total_exposure: Usd,
    pub daily_pnl: Usd,
    pub daily_loss: Usd,
    pub weekly_loss: Usd,
    pub hourly_loss: Usd,
    pub hourly_trade_count: u32,
    pub hourly_success_count: u32,
    pub hourly_miss_count: u32,
    pub consecutive_misses: u32,
    /// Number of L2 trips in this session (for exponential cooldown).
    pub l2_trip_count: u32,
    /// Budget already consumed today (cost of executed trades).
    pub daily_budget_spent: Usd,
    pub daily_trade_count: u32,
    pub daily_success_count: u32,
    pub daily_miss_count: u32,
    pub weekly_trade_count: u32,
    /// High-water mark equity for drawdown guard.
    pub hwm_equity: Usd,
    pub snapshot_at: DateTime<Utc>,
}

/// Emergency snapshot for kill-switch / circuit-breaker triggers.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EmergencySnapshot {
    pub trigger_level: CircuitBreakerLevel,
    pub reason: String,
    pub risk_state: RiskEngineSnapshot,
    pub open_positions_count: u32,
    pub open_reservations_count: u32,
    pub triggered_at: DateTime<Utc>,
}

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
#[derive(Debug, Clone, Serialize, Deserialize)]
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
