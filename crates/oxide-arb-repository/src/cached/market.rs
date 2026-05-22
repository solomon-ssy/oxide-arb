//! Cached wrapper for [`MarketRepository`] using cache-aside reads.

use crate::traits::MarketRepository;
use chrono::{DateTime, Utc};
use oxide_arb_error::storage::StorageError;
use oxide_arb_models::{entities::market, types::MarketId};
use oxide_arb_storage::cache::{CacheKey, TieredCache};
use std::collections::HashSet;
use std::sync::Arc;

/// Caching decorator for market metadata reads.
pub struct CachedMarketRepository<R: MarketRepository> {
    inner: R,
    cache: Arc<TieredCache>,
}

impl<R: MarketRepository> CachedMarketRepository<R> {
    /// Wrap an existing repository with tiered cache support.
    pub const fn new(inner: R, cache: Arc<TieredCache>) -> Self {
        Self { inner, cache }
    }

    async fn invalidate_market(&self, market_id: &MarketId) {
        let _ = self
            .cache
            .invalidate(&CacheKey::MarketEntry {
                market_id: market_id.clone(),
            })
            .await;
        let _ = self
            .cache
            .invalidate(&CacheKey::MarketMetadata {
                market_id: market_id.clone(),
            })
            .await;
        let _ = self.cache.invalidate(&CacheKey::ActiveMarkets).await;
    }
}

impl<R: MarketRepository> MarketRepository for CachedMarketRepository<R> {
    async fn find_by_id(&self, id: &MarketId) -> Result<Option<market::Model>, StorageError> {
        let key = CacheKey::MarketEntry {
            market_id: id.clone(),
        };
        if let Some(cached) = self.cache.get_json::<market::Model>(&key).await? {
            return Ok(Some(cached));
        }
        let result = self.inner.find_by_id(id).await?;
        if let Some(ref model) = result {
            let _ = self.cache.set_json(&key, model).await;
        }
        Ok(result)
    }

    async fn find_active(&self) -> Result<Vec<market::Model>, StorageError> {
        let key = CacheKey::ActiveMarkets;
        if let Some(cached) = self.cache.get_json::<Vec<market::Model>>(&key).await? {
            return Ok(cached);
        }
        let markets = self.inner.find_active().await?;
        let _ = self.cache.set_json(&key, &markets).await;
        Ok(markets)
    }

    async fn find_by_event(&self, event_id: &str) -> Result<Vec<market::Model>, StorageError> {
        self.inner.find_by_event(event_id).await
    }

    async fn find_endgame_candidates(
        &self,
        before_deadline: DateTime<Utc>,
    ) -> Result<Vec<market::Model>, StorageError> {
        self.inner.find_endgame_candidates(before_deadline).await
    }

    async fn find_existing_ids(&self, ids: &[MarketId]) -> Result<HashSet<String>, StorageError> {
        self.inner.find_existing_ids(ids).await
    }

    async fn insert(&self, model: market::ActiveModel) -> Result<market::Model, StorageError> {
        let result = self.inner.insert(model).await?;
        self.invalidate_market(&result.market_id).await;
        Ok(result)
    }

    async fn insert_batch(&self, models: Vec<market::ActiveModel>) -> Result<u64, StorageError> {
        let count = self.inner.insert_batch(models).await?;
        let _ = self.cache.invalidate(&CacheKey::ActiveMarkets).await;
        Ok(count)
    }

    async fn update(&self, model: market::ActiveModel) -> Result<market::Model, StorageError> {
        let result = self.inner.update(model).await?;
        self.invalidate_market(&result.market_id).await;
        Ok(result)
    }

    async fn update_status(
        &self,
        id: &MarketId,
        status: &str,
        outcome: Option<&str>,
    ) -> Result<(), StorageError> {
        self.inner.update_status(id, status, outcome).await?;
        self.invalidate_market(id).await;
        Ok(())
    }
}
