//! Polymarket fee calculator (concrete struct, not trait per ADR-001).
//!
//! Category rates are **static** (official Polymarket documentation). There is
//! no dedicated fee-rate REST endpoint. Per-token `fees_enabled` and category
//! mapping arrive from the Gamma market sync pipeline — not from a background
//! poll that re-fetches the same hardcoded table.

mod formula;
#[cfg(test)]
mod golden;
mod rate_cache;
#[cfg(test)]
mod reference;

pub use rate_cache::{CategoryFeeParams, FeeSnapshot};

use arc_swap::ArcSwap;
use quant_pivot_models::{
    config::FeesConfig,
    domain::fee::{FeeQuote, FeeQuoteError, FeeQuoteInput, MarketFeeSchedule},
    enums::{
        common::{MarketCategory, Side},
        fee::FeeLiquidityRole,
        quant::QuantRuntimeMode,
    },
    types::{MarketId, Price, Shares, TokenId, Usd},
};
use std::sync::Arc;

/// Polymarket fee calculator with lock-free snapshot reads.
pub struct FeeCalculator {
    rate_cache: Arc<ArcSwap<FeeSnapshot>>,
}

impl FeeCalculator {
    /// Create with compiled-in defaults (same as an empty/minimal TOML).
    pub fn new() -> Self {
        Self::from_config(&FeesConfig::default())
    }

    /// Create from `[polymarket.fees]` configuration.
    pub fn from_config(fees: &FeesConfig) -> Self {
        Self {
            rate_cache: Arc::new(ArcSwap::from_pointee(FeeSnapshot::from_config(fees))),
        }
    }

    /// Calculate the fee for a non-submitting estimate.
    ///
    /// Order-submitting paths must use [`Self::quote_for_mode`] with fail-closed
    /// schedule resolution.
    pub fn calculate(
        &self,
        shares: Shares,
        price: Price,
        category: MarketCategory,
        market_id: &MarketId,
        token_id: &TokenId,
    ) -> Usd {
        let input = FeeQuoteInput {
            market_id: market_id.clone(),
            token_id: token_id.clone(),
            category,
            side: Side::Buy,
            liquidity_role: FeeLiquidityRole::Taker,
            shares,
            price,
            allow_category_fallback: true,
        };
        self.quote(&input).map_or(Usd::ZERO, |quote| quote.fee_usd)
    }

    pub fn quote(&self, input: &FeeQuoteInput) -> Result<FeeQuote, FeeQuoteError> {
        let snapshot = self.rate_cache.load();
        let schedule = match snapshot.market_schedules.get(&input.market_id) {
            Some(schedule) => Arc::clone(schedule),
            None if input.allow_category_fallback => snapshot
                .category_default_schedule(&input.market_id, input.category)
                .ok_or_else(|| FeeQuoteError::MissingSchedule {
                    market_id: input.market_id.to_string(),
                    detail: format!("no category fallback for {}", input.category),
                })?,
            None => {
                return Err(FeeQuoteError::MissingSchedule {
                    market_id: input.market_id.to_string(),
                    detail: "category fallback disabled".into(),
                });
            }
        };

        let fee_usd = if !schedule.fees_enabled || input.liquidity_role == FeeLiquidityRole::Maker {
            Usd::ZERO
        } else {
            formula::calculate_fee(
                input.shares,
                input.price,
                schedule.fee_rate,
                schedule.exponent,
            )
        };

        Ok(FeeQuote {
            fee_usd,
            schedule,
            formula_version: "polymarket-v1",
            rounded_scale: 5,
        })
    }

    /// Mode-aware fee quote: order-submitting modes force `allow_category_fallback = false`.
    pub fn quote_for_mode(
        &self,
        mode: QuantRuntimeMode,
        mut input: FeeQuoteInput,
    ) -> Result<FeeQuote, FeeQuoteError> {
        if mode.allows_order_submission() {
            input.allow_category_fallback = false;
        }
        self.quote(&input)
    }

    pub fn ingest_market_fee_schedules(
        &self,
        schedules: impl IntoIterator<Item = impl Into<Arc<MarketFeeSchedule>>>,
    ) {
        let mut snapshot = self.rate_cache.load().as_ref().clone();
        let mut changed = false;
        for schedule in schedules {
            let schedule = schedule.into();
            snapshot
                .market_schedules
                .insert(schedule.market_id.clone(), schedule);
            changed = true;
        }
        if changed {
            snapshot.updated_at = chrono::Utc::now();
            self.rate_cache.store(Arc::new(snapshot));
        }
    }

    /// Return the current fee snapshot.
    pub fn snapshot(&self) -> Arc<FeeSnapshot> {
        self.rate_cache.load_full()
    }
}

impl Default for FeeCalculator {
    fn default() -> Self {
        Self::new()
    }
}
