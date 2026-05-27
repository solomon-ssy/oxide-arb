//! Cached wrapper for [`MarketRepository`] using cache-aside reads.

use crate::traits::MarketRepository;
use chrono::{DateTime, Utc};
use oxide_arb_error::storage::StorageError;
use oxide_arb_models::{
    domain::{MarketInfo, UpsertMarket},
    types::MarketId,
};
use oxide_arb_storage::cache::{CacheKey, TieredCache};
use std::{collections::HashSet, sync::Arc};

/// Caching decorator for market metadata reads.
pub struct CachedMarketRepository<R: MarketRepository> {
    inner: R,
    cache: Arc<TieredCache>,
}

impl<R: MarketRepository> CachedMarketRepository<R> {
    pub const fn new(inner: R, cache: Arc<TieredCache>) -> Self {
        Self { inner, cache }
    }

    async fn invalidate_market(&self, market_id: &MarketId) {
        let _ = self
            .cache
            .invalidate(&CacheKey::MarketInfo {
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
    async fn find_by_id(&self, id: &MarketId) -> Result<Option<MarketInfo>, StorageError> {
        let key = CacheKey::MarketInfo {
            market_id: id.clone(),
        };
        if let Some(cached) = self.cache.get_json::<MarketInfo>(&key).await? {
            return Ok(Some(cached));
        }
        let result = self.inner.find_by_id(id).await?;
        if let Some(ref info) = result {
            let _ = self.cache.set_json(&key, info).await;
        }
        Ok(result)
    }

    async fn find_active(&self) -> Result<Vec<MarketInfo>, StorageError> {
        let key = CacheKey::ActiveMarkets;
        if let Some(cached) = self.cache.get_json::<Vec<MarketInfo>>(&key).await? {
            return Ok(cached);
        }
        let markets = self.inner.find_active().await?;
        let _ = self.cache.set_json(&key, &markets).await;
        Ok(markets)
    }

    async fn find_by_event(&self, event_id: &str) -> Result<Vec<MarketInfo>, StorageError> {
        self.inner.find_by_event(event_id).await
    }

    async fn find_endgame_candidates(
        &self,
        before_deadline: DateTime<Utc>,
    ) -> Result<Vec<MarketInfo>, StorageError> {
        self.inner.find_endgame_candidates(before_deadline).await
    }

    async fn find_existing_ids(&self, ids: &[MarketId]) -> Result<HashSet<String>, StorageError> {
        self.inner.find_existing_ids(ids).await
    }

    async fn upsert(&self, dto: UpsertMarket) -> Result<MarketInfo, StorageError> {
        let market_id = dto.market_id.clone();
        let result = self.inner.upsert(dto).await?;
        self.invalidate_market(&market_id).await;
        Ok(result)
    }

    async fn upsert_batch(&self, dtos: Vec<UpsertMarket>) -> Result<u64, StorageError> {
        let count = self.inner.upsert_batch(dtos).await?;
        let _ = self.cache.invalidate(&CacheKey::ActiveMarkets).await;
        Ok(count)
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
