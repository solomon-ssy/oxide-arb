//! Risk engine domain models.

use crate::enums::risk::{BreakerStateName, CircuitBreakerLevel};
use crate::types::{MarketId, Usd};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// Result of a risk gate evaluation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RiskDecision {
    /// Whether the trade is allowed to proceed.
    pub allowed: bool,
    /// Which checks were evaluated.
    pub checks: Vec<RiskCheck>,
    /// If denied, which check caused the denial.
    pub denial_reason: Option<String>,
    pub evaluated_at: DateTime<Utc>,
}

/// Individual risk check result.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RiskCheck {
    pub name: String,
    pub passed: bool,
    pub detail: Option<String>,
}

/// Persisted snapshot of the risk engine state.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RiskEngineSnapshot {
    pub breaker_state: BreakerStateName,
    pub breaker_level: Option<CircuitBreakerLevel>,
    pub breaker_reason: Option<String>,
    pub cooling_until: Option<DateTime<Utc>>,
    pub total_exposure: Usd,
    pub daily_pnl: Usd,
    pub consecutive_losses: u32,
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
