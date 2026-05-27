use oxide_arb_error::OxideError;
use oxide_arb_models::{domain::trade::PositionSummary, types::MarketId};
use oxide_arb_repository::traits::PositionRepository;
use oxide_arb_storage::cache::{CacheKey, TieredCache};
use std::sync::Arc;

pub struct PositionSummaryService<R: PositionRepository> {
    cache: Arc<TieredCache>,
    position_repo: Arc<R>,
}

impl<R: PositionRepository> PositionSummaryService<R> {
    pub const fn new(cache: Arc<TieredCache>, position_repo: Arc<R>) -> Self {
        Self {
            cache,
            position_repo,
        }
    }

    pub async fn get(&self, market_id: &MarketId) -> Result<PositionSummary, OxideError> {
        let key = CacheKey::PositionSummary {
            market_id: market_id.clone(),
        };

        if let Some(cached) = self
            .cache
            .get_json::<PositionSummary>(&key)
            .await
            .map_err(|e| OxideError::Internal(format!("position cache read: {e}")))?
        {
            return Ok(cached);
        }

        let open_positions = self.position_repo.find_by_market(market_id).await?;

        let summary = PositionSummary {
            market_id: market_id.clone(),
            total_exposure_usd: open_positions.iter().map(|p| p.total_cost_usd).sum(),
            position_count: open_positions.len(),
            open_positions,
            summarized_at: chrono::Utc::now(),
        };

        self.cache
            .set_json(&key, &summary)
            .await
            .map_err(|e| OxideError::Internal(format!("position cache write: {e}")))?;
        Ok(summary)
    }

    pub async fn invalidate(&self, market_id: &MarketId) -> Result<(), OxideError> {
        self.cache
            .invalidate(&CacheKey::PositionSummary {
                market_id: market_id.clone(),
            })
            .await
            .map_err(|e| OxideError::Internal(format!("position cache invalidate: {e}")))
    }
}
