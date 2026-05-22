//! Immutable decision context for pre-trade risk evaluation.
//!
//! `RiskContext` is a frozen snapshot of all state needed for a single
//! `pre_trade_check()` call. All checks within a decision read from the
//! same context, preventing time-of-check/time-of-use inconsistencies.

use crate::types::DrawdownAction;
use crate::types::StateVersion;
use chrono::{DateTime, Utc};
use oxide_arb_models::domain::opportunity::Opportunity;
use oxide_arb_models::domain::risk::ProbabilityInput;
use oxide_arb_models::types::Usd;
use rust_decimal::Decimal;

/// Circuit breaker gate snapshot for pre-trade evaluation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CircuitBreakerGate {
    pub allows_trading: bool,
    pub is_probe: bool,
}

/// Manual halt gate state.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ManualHaltGate {
    Clear,
    Halted { reason: String },
}

impl ManualHaltGate {
    #[must_use]
    pub const fn allows_trading(&self) -> bool {
        matches!(self, Self::Clear)
    }

    #[must_use]
    pub fn denial_detail(&self) -> Option<String> {
        match self {
            Self::Clear => None,
            Self::Halted { reason } => Some(reason.clone()),
        }
    }
}

/// Blacklist gate state for the trading path.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BlacklistGate {
    Clear,
    Blocked { detail: String },
}

impl BlacklistGate {
    #[must_use]
    pub const fn allows_trading(&self) -> bool {
        matches!(self, Self::Clear)
    }

    #[must_use]
    pub fn denial_detail(&self) -> Option<String> {
        match self {
            Self::Clear => None,
            Self::Blocked { detail } => Some(detail.clone()),
        }
    }
}

/// Immutable snapshot of all state needed for a single pre-trade decision.
///
/// Built once per `pre_trade_check()` call. All pipeline checks read from
/// this context — they never query `RiskMetrics` or lock subsystems directly.
#[derive(Debug, Clone)]
pub struct RiskContext {
    pub state_version: StateVersion,
    pub opportunity: Opportunity,
    pub probability: ProbabilityInput,
    pub market_exposure_before: Usd,
    pub total_exposure_before: Usd,
    pub total_potential_loss: Usd,
    pub active_reservation_count: usize,
    pub reserved_usd: Usd,
    pub open_position_count: usize,
    pub cached_balance: Usd,
    pub ws_disconnect_secs: u64,
    pub open_directional_count_same_side: usize,
    pub daily_directional_trades_same_side: u32,
    pub consecutive_market_misses: u32,
    pub hourly_loss: Usd,
    pub daily_loss: Usd,
    pub daily_budget_remaining: Usd,
    pub weekly_loss: Usd,
    pub daily_pnl: Usd,
    pub circuit_breaker: CircuitBreakerGate,
    pub manual_halt: ManualHaltGate,
    pub blacklist: BlacklistGate,
    pub token_blacklisted: bool,
    pub api_error_count: u64,
    pub api_request_count: u64,
    pub drawdown_factor: Decimal,
    pub drawdown_action: DrawdownAction,
    pub snapshot_at: DateTime<Utc>,
}
