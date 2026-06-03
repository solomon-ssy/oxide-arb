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
use oxide_arb_models::{
    config::FeesConfig,
    domain::fee::{FeeQuote, FeeQuoteInput, MarketFeeSchedule},
    enums::{
        common::{MarketCategory, Side},
        fee::FeeLiquidityRole,
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

    /// Calculate the fee for a trade.
    ///
    /// Prefer [`Self::quote`] in new code. This method remains as a thin
    /// adapter for call sites that have not yet been moved to market-scoped
    /// quotes; it uses explicit category fallback and must not be used in Live
    /// fail-closed paths.
    pub fn calculate(
        &self,
        shares: Shares,
        price: Price,
        category: MarketCategory,
        token_id: &TokenId,
    ) -> Usd {
        let input = FeeQuoteInput {
            market_id: MarketId::new(token_id.as_str()),
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

    pub fn quote(&self, input: &FeeQuoteInput) -> Result<FeeQuote, String> {
        let snapshot = self.rate_cache.load();
        let schedule = match snapshot.market_schedules.get(&input.market_id) {
            Some(schedule) => Arc::clone(schedule),
            None if input.allow_category_fallback => snapshot
                .category_default_schedule(&input.market_id, input.category)
                .ok_or_else(|| format!("missing fee schedule for {}", input.market_id))?,
            None => return Err(format!("missing fee schedule for {}", input.market_id)),
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
