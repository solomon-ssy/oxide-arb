//! Cached wrapper for [`MarketRepository`] using cache-aside reads.
//!
//! Cache reads are fail-open ([`CacheManager`] degrades errors and timeouts to
//! a miss), so a Redis outage falls through to the inner repository instead of
//! failing the read.

use std::{collections::HashSet, sync::Arc};

use quant_pivot_error::storage::StorageError;
use quant_pivot_models::{
    domain::{
        api::MarketPageQuery,
        market::{MarketInfo, UpsertMarket},
        pagination::Paginated,
    },
    enums::market::MarketStatus,
    types::MarketId,
};
use quant_pivot_storage::cache::{CacheKey, CacheManager};

use crate::traits::MarketRepository;

/// Caching decorator for market metadata reads.
pub struct CachedMarketRepository<R: MarketRepository> {
    inner: R,
    cache: Arc<CacheManager>,
}

impl<R: MarketRepository> CachedMarketRepository<R> {
    pub const fn new(inner: R, cache: Arc<CacheManager>) -> Self {
        Self { inner, cache }
    }

    async fn invalidate_market(&self, market_id: &MarketId) {
        self.cache
            .invalidate(&CacheKey::MarketInfo {
                market_id: market_id.clone(),
            })
            .await;
        self.cache
            .invalidate(&CacheKey::MarketMetadata {
                market_id: market_id.clone(),
            })
            .await;
    }
}

#[async_trait::async_trait]
impl<R: MarketRepository> MarketRepository for CachedMarketRepository<R> {
    #[inline]
    async fn find_by_id(&self, id: &MarketId) -> Result<Option<Arc<MarketInfo>>, StorageError> {
        let key = CacheKey::MarketInfo {
            market_id: id.clone(),
        };
        if let Some(cached) = self.cache.get_json::<MarketInfo>(&key).await {
            return Ok(Some(Arc::new(cached)));
        }
        let result = self.inner.find_by_id(id).await?;
        if let Some(ref info) = result {
            let _ = self.cache.set_json(&key, info.as_ref()).await;
        }
        Ok(result)
    }

    #[inline]
    async fn page(&self, query: MarketPageQuery) -> Result<Paginated<MarketInfo>, StorageError> {
        self.inner.page(query).await
    }

    #[inline]
    async fn find_by_ids(&self, ids: &[MarketId]) -> Result<Vec<Arc<MarketInfo>>, StorageError> {
        self.inner.find_by_ids(ids).await
    }

    #[inline]
    async fn find_active(&self) -> Result<Arc<[MarketInfo]>, StorageError> {
        // This projection is intentionally not cached as one value. A full
        // Gamma catalog contains tens of thousands of rows, so serializing it
        // into a single Redis/Moka entry creates a multi-megabyte hot key and
        // makes freshness invalidation more expensive than the indexed query.
        self.inner.find_active().await
    }

    #[inline]
    async fn find_by_event(&self, event_id: &str) -> Result<Vec<Arc<MarketInfo>>, StorageError> {
        self.inner.find_by_event(event_id).await
    }

    #[inline]
    async fn find_existing_ids(&self, ids: &[MarketId]) -> Result<HashSet<String>, StorageError> {
        self.inner.find_existing_ids(ids).await
    }

    #[inline]
    async fn upsert(&self, dto: UpsertMarket) -> Result<Arc<MarketInfo>, StorageError> {
        let market_id = dto.market_id.clone();
        let result = self.inner.upsert(dto).await?;
        self.invalidate_market(&market_id).await;
        Ok(result)
    }

    #[inline]
    async fn upsert_batch(&self, dtos: Vec<UpsertMarket>) -> Result<u64, StorageError> {
        self.inner.upsert_batch(dtos).await
    }

    #[inline]
    async fn update_status(
        &self,
        id: &MarketId,
        status: MarketStatus,
        outcome: Option<&str>,
    ) -> Result<(), StorageError> {
        self.inner.update_status(id, status, outcome).await?;
        self.invalidate_market(id).await;
        Ok(())
    }
}
