//! Cached wrapper for [`MarketRepository`] using cache-aside reads.
//!
//! Cache reads are fail-open ([`CacheManager`] degrades errors and timeouts to
//! a miss), so a Redis outage falls through to the inner repository instead of
//! failing the read.

use crate::traits::MarketRepository;
use chrono::{DateTime, Utc};
use oxide_arb_error::storage::StorageError;
use oxide_arb_models::{
    domain::{MarketInfo, MarketPageQuery, MarketPitSnapshotInfo, Paginated, UpsertMarket},
    types::MarketId,
};
use oxide_arb_storage::cache::{CacheKey, CacheManager};
use std::{collections::HashSet, sync::Arc};

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
        self.cache.invalidate(&CacheKey::ActiveMarkets).await;
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
    async fn latest_pit_snapshots_before(
        &self,
        ids: &[MarketId],
        as_of: DateTime<Utc>,
    ) -> Result<Vec<MarketPitSnapshotInfo>, StorageError> {
        self.inner.latest_pit_snapshots_before(ids, as_of).await
    }

    #[inline]
    async fn find_active(&self) -> Result<Arc<[MarketInfo]>, StorageError> {
        let key = CacheKey::ActiveMarkets;
        if let Some(cached) = self.cache.get_json::<Vec<MarketInfo>>(&key).await {
            return Ok(cached.into());
        }
        let markets = self.inner.find_active().await?;
        let _ = self.cache.set_json(&key, &markets.as_ref()).await;
        Ok(markets)
    }

    #[inline]
    async fn find_by_event(&self, event_id: &str) -> Result<Vec<Arc<MarketInfo>>, StorageError> {
        self.inner.find_by_event(event_id).await
    }

    #[inline]
    async fn find_endgame_candidates(
        &self,
        before_deadline: DateTime<Utc>,
    ) -> Result<Vec<Arc<MarketInfo>>, StorageError> {
        self.inner.find_endgame_candidates(before_deadline).await
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
        let count = self.inner.upsert_batch(dtos).await?;
        self.cache.invalidate(&CacheKey::ActiveMarkets).await;
        Ok(count)
    }

    #[inline]
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
