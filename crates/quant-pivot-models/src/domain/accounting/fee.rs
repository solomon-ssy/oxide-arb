//! Polymarket fee schedule and quote domain types.
//!
//! Runtime fee authority is market-scoped metadata. Category defaults are only
//! a fallback/baseline and must not silently stand in for missing Live data.

use crate::{
    enums::{
        common::{MarketCategory, Side},
        fee::{FeeLiquidityRole, FeeSource},
    },
    types::{MarketId, Price, Shares, TokenId, Usd},
};
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

pub use quant_pivot_error::fee::FeeQuoteError;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MarketFeeSchedule {
    pub market_id: MarketId,
    pub fees_enabled: bool,
    pub fee_rate: Decimal,
    pub exponent: Decimal,
    pub taker_only: bool,
    pub rebate_rate: Option<Decimal>,
    pub source: FeeSource,
    pub observed_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FeeQuoteInput {
    pub market_id: MarketId,
    pub token_id: TokenId,
    pub category: MarketCategory,
    pub side: Side,
    pub liquidity_role: FeeLiquidityRole,
    pub shares: Shares,
    pub price: Price,
    pub allow_category_fallback: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FeeQuote {
    pub fee_usd: Usd,
    pub schedule: Arc<MarketFeeSchedule>,
    pub formula_version: &'static str,
    pub rounded_scale: u32,
}
