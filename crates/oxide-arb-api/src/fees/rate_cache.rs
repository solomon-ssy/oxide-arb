//! ArcSwap-backed fee rate snapshot.

use chrono::{DateTime, Utc};
use oxide_arb_models::config::FeesConfig;
use oxide_arb_models::enums::common::MarketCategory;
use oxide_arb_models::types::TokenId;
use rust_decimal::Decimal;
use std::collections::HashMap;

/// Where the current category rates originated.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FeeRateSource {
    /// Loaded from config defaults (official Polymarket documentation).
    ConfigDefault,
    /// Operator override via config or admin API.
    ManualOverride,
}

/// Snapshot of fee parameters, atomically swapped on refresh.
#[derive(Debug, Clone)]
pub struct FeeSnapshot {
    pub category_params: HashMap<MarketCategory, CategoryFeeParams>,
    pub per_token_enabled: HashMap<TokenId, bool>,
    pub token_category: HashMap<TokenId, MarketCategory>,
    pub updated_at: DateTime<Utc>,
    pub source: FeeRateSource,
}

impl FeeSnapshot {
    /// Build a snapshot from application configuration.
    #[must_use]
    pub fn from_config(config: &FeesConfig) -> Self {
        // TODO(cache): cache category fee parameters with
        // `CacheKey::FeeParams { category }` once runtime fee config updates
        // own invalidation across API and scoring services.
        let category_params = config
            .all_category_rates()
            .map(|(category, fee_rate, exponent)| {
                (category, CategoryFeeParams { fee_rate, exponent })
            })
            .collect();

        Self {
            category_params,
            per_token_enabled: HashMap::new(),
            token_category: HashMap::new(),
            updated_at: Utc::now(),
            source: FeeRateSource::ConfigDefault,
        }
    }
}

/// Fee parameters for a market category.
#[derive(Debug, Clone, Copy)]
pub struct CategoryFeeParams {
    pub fee_rate: Decimal,
    /// Volatility exponent; Polymarket docs specify **1** for all categories.
    pub exponent: Decimal,
}
