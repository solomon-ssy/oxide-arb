//! Cached wrapper for [`EventRepository`] using cache-aside reads.

use crate::traits::EventRepository;
use oxide_arb_error::storage::StorageError;
use oxide_arb_models::{entities::event, types::EventId};
use oxide_arb_storage::cache::{CacheKey, TieredCache};
use std::collections::HashSet;
use std::sync::Arc;

/// Caching decorator for event metadata reads.
pub struct CachedEventRepository<R: EventRepository> {
    inner: R,
    cache: Arc<TieredCache>,
}

impl<R: EventRepository> CachedEventRepository<R> {
    /// Wrap an existing repository with tiered cache support.
    pub const fn new(inner: R, cache: Arc<TieredCache>) -> Self {
        Self { inner, cache }
    }

    async fn invalidate_event(&self, event_id: &EventId) {
        let _ = self
            .cache
            .invalidate(&CacheKey::EventEntry {
                event_id: event_id.clone(),
            })
            .await;
    }
}

impl<R: EventRepository> EventRepository for CachedEventRepository<R> {
    async fn find_by_id(&self, id: &EventId) -> Result<Option<event::Model>, StorageError> {
        let key = CacheKey::EventEntry {
            event_id: id.clone(),
        };
        if let Some(cached) = self.cache.get_json::<event::Model>(&key).await? {
            return Ok(Some(cached));
        }
        let result = self.inner.find_by_id(id).await?;
        if let Some(ref model) = result {
            let _ = self.cache.set_json(&key, model).await;
        }
        Ok(result)
    }

    async fn find_active(&self) -> Result<Vec<event::Model>, StorageError> {
        self.inner.find_active().await
    }

    async fn find_existing_ids(&self, ids: &[EventId]) -> Result<HashSet<String>, StorageError> {
        self.inner.find_existing_ids(ids).await
    }

    async fn insert(&self, model: event::ActiveModel) -> Result<event::Model, StorageError> {
        let result = self.inner.insert(model).await?;
        self.invalidate_event(&result.event_id).await;
        Ok(result)
    }

    async fn insert_batch(&self, models: Vec<event::ActiveModel>) -> Result<u64, StorageError> {
        self.inner.insert_batch(models).await
    }

    async fn update(&self, model: event::ActiveModel) -> Result<event::Model, StorageError> {
        let result = self.inner.update(model).await?;
        self.invalidate_event(&result.event_id).await;
        Ok(result)
    }
}
