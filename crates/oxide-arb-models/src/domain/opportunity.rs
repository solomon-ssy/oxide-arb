//! Detected endgame opportunity domain models.
//!
//! [`Opportunity`] is the canonical representation of a detected endgame
//! trading opportunity. It carries all information needed for risk
//! evaluation, position sizing, and execution dispatch.
//!
//! Endgame is a single-order strategy: buy tokens expected to settle at
//! $1, or sell tokens expected to settle at $0. No multi-leg orchestration.

use crate::domain::calibration::{CalibrationSnapshot, DurationBucket, PriceZone};
use crate::enums::common::{MarketCategory, Side, StalenessLevel};
use crate::types::{Bps, EventId, MarketId, OpportunityId, Price, Shares, TokenId, Usd};
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

/// Detected endgame opportunity ready for evaluation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Opportunity {
    pub opportunity_id: OpportunityId,
    pub market_id: MarketId,
    pub event_id: EventId,
    pub token_id: TokenId,
    pub side: Side,
    pub payout_model: PayoutModel,
    pub shares: Shares,
    pub entry_price: Price,
    pub total_cost: Usd,
    pub total_fees: Usd,
    pub net_profit: Usd,
    pub expected_net_profit: Usd,
    pub edge_bps: Bps,
    /// Calibration-based resolution probability adjustment.
    pub resolution_adjust: Decimal,
    /// Fraction of available book depth consumed by this order.
    pub depth_used_pct: Decimal,
    pub staleness: StalenessLevel,
    pub category: MarketCategory,
    pub meta: EndgameMeta,
    pub calibration: CalibrationSnapshot,
    pub detected_at: DateTime<Utc>,
}

/// Endgame-specific metadata.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EndgameMeta {
    /// Whether we predict YES outcome.
    pub predicted_yes: bool,
    /// Model confidence (0.0–1.0).
    pub confidence: Decimal,
    /// Expected seconds until convergence.
    pub convergence_duration_secs: u64,
    pub price_zone: PriceZone,
    pub duration_bucket: DurationBucket,
    /// Expected settlement deadline (if known from the market).
    pub settlement_deadline: Option<DateTime<Utc>>,
}

/// Settlement payout model for endgame strategy.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum PayoutModel {
    DirectionalSettlement {
        projected_payout_if_correct: Usd,
        expected_payout: Usd,
        predicted_side: Side,
    },
}

impl PayoutModel {
    /// Single source of truth for expected `PnL` computation.
    pub fn compute_pnl(&self, total_cost: Usd, total_fees: Usd) -> Usd {
        match self {
            Self::DirectionalSettlement {
                expected_payout, ..
            } => *expected_payout - total_cost - total_fees,
        }
    }
}
