//! Cached wrapper for [`EventRepository`] using cache-aside reads.
//!
//! Cache reads are fail-open ([`CacheManager`] degrades errors and timeouts to
//! a miss), so a Redis outage falls through to the inner repository instead of
//! failing the read.

use crate::traits::EventRepository;
use oxide_arb_error::storage::StorageError;
use oxide_arb_models::{
    domain::{EventInfo, UpsertEvent},
    types::EventId,
};
use oxide_arb_storage::cache::{CacheKey, CacheManager};
use std::{collections::HashSet, sync::Arc};

/// Caching decorator for event metadata reads.
pub struct CachedEventRepository<R: EventRepository> {
    inner: R,
    cache: Arc<CacheManager>,
}

impl<R: EventRepository> CachedEventRepository<R> {
    pub const fn new(inner: R, cache: Arc<CacheManager>) -> Self {
        Self { inner, cache }
    }

    async fn invalidate_event(&self, event_id: &EventId) {
        self.cache
            .invalidate(&CacheKey::EventInfo {
                event_id: event_id.clone(),
            })
            .await;
    }
}

#[async_trait::async_trait]
impl<R: EventRepository> EventRepository for CachedEventRepository<R> {
    #[inline]
    async fn find_by_id(&self, id: &EventId) -> Result<Option<EventInfo>, StorageError> {
        let key = CacheKey::EventInfo {
            event_id: id.clone(),
        };
        if let Some(cached) = self.cache.get_json::<EventInfo>(&key).await {
            return Ok(Some(cached));
        }
        let result = self.inner.find_by_id(id).await?;
        if let Some(ref info) = result {
            let _ = self.cache.set_json(&key, info).await;
        }
        Ok(result)
    }

    #[inline]
    async fn find_by_ids(&self, ids: &[EventId]) -> Result<Vec<EventInfo>, StorageError> {
        self.inner.find_by_ids(ids).await
    }

    #[inline]
    async fn find_active(&self) -> Result<Vec<EventInfo>, StorageError> {
        self.inner.find_active().await
    }

    #[inline]
    async fn find_existing_ids(&self, ids: &[EventId]) -> Result<HashSet<String>, StorageError> {
        self.inner.find_existing_ids(ids).await
    }

    #[inline]
    async fn upsert(&self, dto: UpsertEvent) -> Result<EventInfo, StorageError> {
        let event_id = dto.event_id.clone();
        let result = self.inner.upsert(dto).await?;
        self.invalidate_event(&event_id).await;
        Ok(result)
    }

    #[inline]
    async fn upsert_batch(&self, dtos: Vec<UpsertEvent>) -> Result<u64, StorageError> {
        self.inner.upsert_batch(dtos).await
    }
}
