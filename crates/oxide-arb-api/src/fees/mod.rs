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

pub use rate_cache::{CategoryFeeParams, FeeRateSource, FeeSnapshot};

use arc_swap::ArcSwap;
use oxide_arb_models::config::FeesConfig;
use oxide_arb_models::enums::common::MarketCategory;
use oxide_arb_models::types::{Price, Shares, TokenId, Usd};
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
    pub fn calculate(
        &self,
        shares: Shares,
        price: Price,
        category: MarketCategory,
        token_id: &TokenId,
    ) -> Usd {
        let snapshot = self.rate_cache.load();

        let enabled = snapshot
            .per_token_enabled
            .get(token_id)
            .copied()
            .unwrap_or(true);

        if !enabled {
            return Usd::ZERO;
        }

        let resolved_category = snapshot
            .token_category
            .get(token_id)
            .copied()
            .unwrap_or(category);

        let params = snapshot
            .category_params
            .get(&resolved_category)
            .copied()
            .unwrap_or_else(|| {
                snapshot
                    .category_params
                    .get(&MarketCategory::Other)
                    .copied()
                    .unwrap_or(CategoryFeeParams {
                        fee_rate: rust_decimal_macros::dec!(0.05),
                        exponent: rust_decimal_macros::dec!(1),
                    })
            });

        formula::calculate_fee(shares, price, params.fee_rate, params.exponent)
    }

    /// Return a clone of the current fee snapshot.
    pub fn snapshot(&self) -> FeeSnapshot {
        self.rate_cache.load().as_ref().clone()
    }

    /// Atomically replace the entire fee snapshot (manual override / verification).
    pub fn replace_snapshot(&self, snapshot: FeeSnapshot) {
        self.rate_cache.store(Arc::new(snapshot));
    }

    /// Ingest per-token metadata from a Gamma sync pass.
    ///
    /// Each entry is `(token_id, fees_enabled, category)` extracted from Gamma
    /// market payloads — the only dynamic fee input Polymarket exposes.
    pub fn ingest_gamma_markets(&self, markets: &[(TokenId, bool, MarketCategory)]) {
        if markets.is_empty() {
            return;
        }

        let mut snapshot = self.rate_cache.load().as_ref().clone();
        for (token_id, enabled, category) in markets {
            snapshot
                .per_token_enabled
                .insert(token_id.clone(), *enabled);
            snapshot.token_category.insert(token_id.clone(), *category);
        }
        snapshot.updated_at = chrono::Utc::now();
        self.rate_cache.store(Arc::new(snapshot));
    }
}

impl Default for FeeCalculator {
    fn default() -> Self {
        Self::new()
    }
}
