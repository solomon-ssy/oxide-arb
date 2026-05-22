//! Cached wrapper for [`RiskStateRepository`] singleton state.

use crate::traits::RiskStateRepository;
use oxide_arb_error::storage::StorageError;
use oxide_arb_models::entities::risk_state;
use oxide_arb_storage::cache::{CacheKey, TieredCache};
use std::sync::Arc;

/// Caching decorator for the risk engine singleton state.
pub struct CachedRiskStateRepository<R: RiskStateRepository> {
    inner: R,
    cache: Arc<TieredCache>,
}

impl<R: RiskStateRepository> CachedRiskStateRepository<R> {
    /// Wrap an existing repository with tiered cache support.
    pub const fn new(inner: R, cache: Arc<TieredCache>) -> Self {
        Self { inner, cache }
    }

    async fn invalidate_state(&self) {
        let _ = self.cache.invalidate(&CacheKey::RiskState).await;
    }
}

impl<R: RiskStateRepository> RiskStateRepository for CachedRiskStateRepository<R> {
    async fn load(&self) -> Result<risk_state::Model, StorageError> {
        let key = CacheKey::RiskState;
        if let Some(cached) = self.cache.get_json::<risk_state::Model>(&key).await? {
            return Ok(cached);
        }
        let state = self.inner.load().await?;
        let _ = self.cache.set_json(&key, &state).await;
        Ok(state)
    }

    async fn save(&self, state: risk_state::ActiveModel) -> Result<(), StorageError> {
        self.inner.save(state).await?;
        self.invalidate_state().await;
        Ok(())
    }

    async fn reset_hourly_window(&self) -> Result<(), StorageError> {
        self.inner.reset_hourly_window().await?;
        self.invalidate_state().await;
        Ok(())
    }

    async fn reset_daily_window(&self) -> Result<(), StorageError> {
        self.inner.reset_daily_window().await?;
        self.invalidate_state().await;
        Ok(())
    }

    async fn reset_weekly_window(&self) -> Result<(), StorageError> {
        self.inner.reset_weekly_window().await?;
        self.invalidate_state().await;
        Ok(())
    }
}
