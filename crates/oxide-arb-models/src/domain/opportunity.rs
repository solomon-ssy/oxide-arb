//! Detected endgame opportunity domain models.
//!
//! [`Opportunity`] is the canonical representation of a detected endgame
//! trading opportunity. It carries all information needed for risk
//! evaluation, position sizing, and execution dispatch.
//!
//! Endgame is a single-order strategy: buy tokens expected to settle at
//! $1, or sell tokens expected to settle at $0. No multi-leg orchestration.

use crate::{
    domain::calibration::CalibrationSnapshot,
    enums::{
        calibration::{DurationBucket, PriceZone},
        common::{MarketCategory, Side, StalenessLevel},
        opportunity::PayoutModel,
    },
    types::{Bps, EventId, MarketId, OpportunityId, Price, Shares, TokenId, Usd},
};
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
    /// Raw profit assuming the predicted outcome is correct (payout - cost - fees).
    pub net_profit: Usd,
    /// Calibration-adjusted expected net profit: `fused_p * payout - cost - fees`.
    pub expected_net_profit: Usd,
    pub edge_bps: Bps,
    /// Fused calibration probability (output of `ConfidenceFusion`).
    pub resolution_adjust: Decimal,
    /// Fraction of available book depth consumed by this order (0–100).
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
    /// Fused model confidence (0.0–1.0).
    pub confidence: Decimal,
    /// How long the market has been in the convergence zone (seconds).
    pub convergence_duration_secs: u64,
    pub price_zone: PriceZone,
    pub duration_bucket: DurationBucket,
    /// Expected settlement deadline (if known from the market).
    pub settlement_deadline: Option<DateTime<Utc>>,
}
