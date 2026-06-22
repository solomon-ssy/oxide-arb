//! Polymarket fee schedule and quote domain types.

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

/// Fee schedule observed for a Polymarket market.
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

/// Persistable fee columns on the `market` table.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MarketFeeColumns {
    pub fees_enabled: bool,
    pub fee_rate: Option<Decimal>,
    pub fee_exponent: Option<Decimal>,
    pub fee_taker_only: Option<bool>,
    pub fee_rebate_rate: Option<Decimal>,
    pub fee_source: Option<String>,
    pub fee_observed_at: Option<DateTime<Utc>>,
}

impl MarketFeeColumns {
    /// Build disabled fee columns for markets without fee metadata.
    #[must_use]
    pub const fn disabled() -> Self {
        Self {
            fees_enabled: true,
            fee_rate: None,
            fee_exponent: None,
            fee_taker_only: None,
            fee_rebate_rate: None,
            fee_source: None,
            fee_observed_at: None,
        }
    }
}

impl MarketFeeSchedule {
    /// Project a fee schedule into `market` table fee columns.
    #[must_use]
    pub fn to_market_fee_columns(&self) -> MarketFeeColumns {
        MarketFeeColumns {
            fees_enabled: self.fees_enabled,
            fee_rate: Some(self.fee_rate),
            fee_exponent: Some(self.exponent),
            fee_taker_only: Some(self.taker_only),
            fee_rebate_rate: self.rebate_rate,
            fee_source: Some(self.source.as_str().to_owned()),
            fee_observed_at: Some(self.observed_at),
        }
    }
}

/// Input for a Polymarket fee quote request.
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

/// Fee quote returned by the Polymarket fee estimator.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FeeQuote {
    pub fee_usd: Usd,
    pub schedule: Arc<MarketFeeSchedule>,
    pub formula_version: &'static str,
    pub rounded_scale: u32,
}
