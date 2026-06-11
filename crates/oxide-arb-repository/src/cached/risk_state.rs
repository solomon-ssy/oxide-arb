//! Cached wrapper for [`RiskStateRepository`] singleton state.
//!
//! Cache reads are fail-open ([`CacheManager`] degrades errors and timeouts to
//! a miss), so a Redis outage falls through to the inner repository instead of
//! failing the read.

use crate::traits::RiskStateRepository;
use oxide_arb_error::storage::StorageError;
use oxide_arb_models::domain::{RiskStateInfo, UpsertRiskEngineState};
use oxide_arb_storage::cache::{CacheKey, CacheManager};
use std::sync::Arc;

/// Caching decorator for the risk engine singleton state.
pub struct CachedRiskStateRepository<R: RiskStateRepository> {
    inner: R,
    cache: Arc<CacheManager>,
}

impl<R: RiskStateRepository> CachedRiskStateRepository<R> {
    pub const fn new(inner: R, cache: Arc<CacheManager>) -> Self {
        Self { inner, cache }
    }

    async fn invalidate_state(&self) {
        self.cache.invalidate(&CacheKey::RiskState).await;
    }
}

#[async_trait::async_trait]
impl<R: RiskStateRepository> RiskStateRepository for CachedRiskStateRepository<R> {
    #[inline]
    async fn load(&self) -> Result<RiskStateInfo, StorageError> {
        let key = CacheKey::RiskState;
        if let Some(cached) = self.cache.get_json::<RiskStateInfo>(&key).await {
            return Ok(cached);
        }
        let state = self.inner.load().await?;
        let _ = self.cache.set_json(&key, &state).await;
        Ok(state)
    }

    #[inline]
    async fn upsert(&self, state: UpsertRiskEngineState) -> Result<(), StorageError> {
        self.inner.upsert(state).await?;
        self.invalidate_state().await;
        Ok(())
    }

    #[inline]
    async fn reset_hourly_window(&self) -> Result<(), StorageError> {
        self.inner.reset_hourly_window().await?;
        self.invalidate_state().await;
        Ok(())
    }

    #[inline]
    async fn reset_daily_window(&self) -> Result<(), StorageError> {
        self.inner.reset_daily_window().await?;
        self.invalidate_state().await;
        Ok(())
    }

    #[inline]
    async fn reset_weekly_window(&self) -> Result<(), StorageError> {
        self.inner.reset_weekly_window().await?;
        self.invalidate_state().await;
        Ok(())
    }
}
