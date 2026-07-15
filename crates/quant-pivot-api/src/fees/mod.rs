//! Polymarket fee calculator (concrete struct, not trait per ADR-001).
//!
//! Fee curves are dynamic, market-level CLOB parameters captured by the
//! append-only market-info producer. Missing market truth fails closed.

mod formula;
#[cfg(test)]
mod golden;
mod rate_cache;
#[cfg(test)]
mod reference;

pub use rate_cache::FeeSnapshot;

use arc_swap::ArcSwap;
use quant_pivot_models::{
    domain::fee::{FeeQuote, FeeQuoteError, FeeQuoteInput, MarketFeeSchedule},
    enums::{common::Side, fee::FeeLiquidityRole},
    types::{MarketId, Price, Shares, TokenId, Usd},
};
use std::sync::Arc;

/// Polymarket fee calculator with lock-free snapshot reads.
pub struct FeeCalculator {
    rate_cache: Arc<ArcSwap<FeeSnapshot>>,
}

impl FeeCalculator {
    /// Create an empty cache; missing CLOB market truth fails closed.
    pub fn new() -> Self {
        Self {
            rate_cache: Arc::new(ArcSwap::from_pointee(FeeSnapshot::empty())),
        }
    }

    pub fn quote(&self, input: &FeeQuoteInput) -> Result<FeeQuote, FeeQuoteError> {
        let snapshot = self.rate_cache.load();
        let schedule = snapshot
            .market_schedules
            .get(&input.market_id)
            .cloned()
            .ok_or_else(|| FeeQuoteError::MissingSchedule {
                market_id: input.market_id.to_string(),
                detail: "no CLOB market-info schedule is available".to_owned(),
            })?;

        let fee_usd = if !schedule.fees_enabled || input.liquidity_role == FeeLiquidityRole::Maker {
            Usd::ZERO
        } else {
            formula::calculate_fee(
                input.shares,
                input.price,
                schedule.fee_rate,
                schedule.exponent,
            )?
        };

        Ok(FeeQuote {
            fee_usd,
            schedule,
            formula_version: "polymarket-v1",
            rounded_scale: 5,
        })
    }

    pub fn calculate(
        &self,
        shares: Shares,
        price: Price,
        market_id: &MarketId,
        token_id: &TokenId,
        side: Side,
    ) -> Result<Usd, FeeQuoteError> {
        self.quote(&FeeQuoteInput {
            market_id: market_id.clone(),
            token_id: token_id.clone(),
            side,
            liquidity_role: FeeLiquidityRole::Taker,
            shares,
            price,
        })
        .map(|quote| quote.fee_usd)
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
