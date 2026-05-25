use std::sync::Arc;

use oxide_arb_api::fees::FeeCalculator;
use oxide_arb_error::OxideError;
use oxide_arb_models::domain::trade::CachedFeeParams;
use oxide_arb_models::enums::common::MarketCategory;
use oxide_arb_storage::cache::{CacheKey, TieredCache};
use rust_decimal_macros::dec;

pub struct FeeParamsService {
    cache: Arc<TieredCache>,
    fee_calculator: Arc<FeeCalculator>,
}

impl FeeParamsService {
    pub const fn new(cache: Arc<TieredCache>, fee_calculator: Arc<FeeCalculator>) -> Self {
        Self {
            cache,
            fee_calculator,
        }
    }

    pub async fn get(&self, category: MarketCategory) -> Result<CachedFeeParams, OxideError> {
        let key = CacheKey::FeeParams { category };

        if let Some(cached) = self
            .cache
            .get_json::<CachedFeeParams>(&key)
            .await
            .map_err(|e| OxideError::Internal(format!("fee cache read: {e}")))?
        {
            return Ok(cached);
        }

        let snapshot = self.fee_calculator.snapshot();
        let api_params = snapshot.category_params.get(&category).copied();

        let (fee_rate, exponent) = api_params.map_or_else(
            || (dec!(0.02), rust_decimal::Decimal::ONE),
            |p| (p.fee_rate, p.exponent),
        );

        let params = CachedFeeParams {
            category,
            fee_rate,
            exponent,
            cached_at: chrono::Utc::now(),
        };

        self.cache
            .set_json(&key, &params)
            .await
            .map_err(|e| OxideError::Internal(format!("fee cache write: {e}")))?;
        Ok(params)
    }
}
