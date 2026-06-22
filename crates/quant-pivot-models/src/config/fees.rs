//! Polymarket fee parameters (category rates + formula exponent).
//!
//! Official defaults mirror Polymarket documentation (2026-04-04).
//! Operators may override per category when PM updates rates without a redeploy.

use crate::enums::common::MarketCategory;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use sea_orm::Iterable;
use serde::Deserialize;
use std::collections::HashMap;

/// Fee configuration mounted at `[polymarket.fees]` in TOML.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct FeesConfig {
    /// Volatility exponent in `fee = C × feeRate × p × (1 − p)^exponent`.
    /// Polymarket official value is **1** for all categories.
    pub exponent: Decimal,
    /// Fallback `feeRate` when a category is absent from [`Self::category_rates`].
    pub unknown_category_rate: Decimal,
    /// Per-category `feeRate` (not basis points).
    pub category_rates: HashMap<MarketCategory, Decimal>,
}

impl Default for FeesConfig {
    fn default() -> Self {
        Self {
            exponent: default_exponent(),
            unknown_category_rate: default_unknown_category_rate(),
            category_rates: default_category_rates(),
        }
    }
}

const fn default_exponent() -> Decimal {
    dec!(1)
}

const fn default_unknown_category_rate() -> Decimal {
    dec!(0.05)
}

/// Official Polymarket category rates (2026-04-04).
#[must_use]
pub fn default_category_rates() -> HashMap<MarketCategory, Decimal> {
    HashMap::from([
        (MarketCategory::Geopolitics, dec!(0)),
        (MarketCategory::Sports, dec!(0.03)),
        (MarketCategory::Crypto, dec!(0.072)),
        (MarketCategory::Finance, dec!(0.04)),
        (MarketCategory::Politics, dec!(0.04)),
        (MarketCategory::Tech, dec!(0.04)),
        (MarketCategory::Economics, dec!(0.05)),
        (MarketCategory::Culture, dec!(0.05)),
        (MarketCategory::Weather, dec!(0.05)),
        (MarketCategory::Other, dec!(0.05)),
    ])
}

impl FeesConfig {
    /// Resolve the fee rate for a category, falling back to [`Self::unknown_category_rate`].
    #[must_use]
    pub fn rate_for(&self, category: MarketCategory) -> Decimal {
        self.category_rates
            .get(&category)
            .copied()
            .unwrap_or(self.unknown_category_rate)
    }

    /// `(category, fee_rate, exponent)` for every known category.
    pub fn all_category_rates(&self) -> impl Iterator<Item = (MarketCategory, Decimal, Decimal)> {
        MarketCategory::iter()
            .map(move |category| (category, self.rate_for(category), self.exponent))
    }
}
