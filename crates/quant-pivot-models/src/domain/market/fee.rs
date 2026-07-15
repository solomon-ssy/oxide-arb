//! Polymarket fee schedule and quote domain types.

use crate::{
    enums::{common::Side, fee::FeeLiquidityRole},
    types::{MarketId, Price, Shares, TokenId, Usd},
};
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

pub use quant_pivot_error::fee::FeeQuoteError;

/// Fee schedule observed for a Polymarket market.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MarketFeeSchedule {
    pub market_id: MarketId,
    pub fees_enabled: bool,
    pub fee_rate: Decimal,
    pub exponent: Decimal,
    pub taker_only: bool,
    pub rebate_rate: Option<Decimal>,
    pub observed_at: DateTime<Utc>,
}

/// Input for a Polymarket fee quote request.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FeeQuoteInput {
    pub market_id: MarketId,
    pub token_id: TokenId,
    pub side: Side,
    pub liquidity_role: FeeLiquidityRole,
    pub shares: Shares,
    pub price: Price,
}

/// Fee quote returned by the Polymarket fee estimator.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FeeQuote {
    pub fee_usd: Usd,
    pub schedule: Arc<MarketFeeSchedule>,
    pub formula_version: &'static str,
    pub rounded_scale: u32,
}
