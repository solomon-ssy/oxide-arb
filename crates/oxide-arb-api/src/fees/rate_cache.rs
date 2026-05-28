//! ArcSwap-backed market fee schedule book.

use ahash::{HashMap, HashMapExt};
use chrono::{DateTime, Utc};
use oxide_arb_models::{
    config::FeesConfig,
    domain::fee::MarketFeeSchedule,
    enums::{common::MarketCategory, fee::FeeSource},
    types::MarketId,
};
use rust_decimal::Decimal;

/// Snapshot of fee parameters, atomically swapped on refresh.
#[derive(Debug, Clone)]
pub struct FeeSnapshot {
    pub market_schedules: HashMap<MarketId, MarketFeeSchedule>,
    pub category_defaults: HashMap<MarketCategory, CategoryFeeParams>,
    pub updated_at: DateTime<Utc>,
}

impl FeeSnapshot {
    /// Build a snapshot from application configuration.
    #[must_use]
    pub fn from_config(config: &FeesConfig) -> Self {
        let category_defaults = config
            .all_category_rates()
            .map(|(category, fee_rate, exponent)| {
                (category, CategoryFeeParams { fee_rate, exponent })
            })
            .collect();

        Self {
            market_schedules: HashMap::new(),
            category_defaults,
            updated_at: Utc::now(),
        }
    }

    #[must_use]
    pub fn category_default_schedule(
        &self,
        market_id: &MarketId,
        category: MarketCategory,
    ) -> Option<MarketFeeSchedule> {
        let params = self
            .category_defaults
            .get(&category)
            .or_else(|| self.category_defaults.get(&MarketCategory::Other))?;
        Some(MarketFeeSchedule {
            market_id: market_id.clone(),
            fees_enabled: params.fee_rate > Decimal::ZERO,
            fee_rate: params.fee_rate,
            exponent: params.exponent,
            taker_only: true,
            rebate_rate: None,
            source: FeeSource::CategoryDefault,
            observed_at: self.updated_at,
        })
    }
}

/// Fee parameters for a market category.
#[derive(Debug, Clone, Copy)]
pub struct CategoryFeeParams {
    pub fee_rate: Decimal,
    /// Volatility exponent; Polymarket docs specify **1** for all categories.
    pub exponent: Decimal,
}
