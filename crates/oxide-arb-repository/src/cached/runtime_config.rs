//! Cached wrapper for [`RuntimeConfigRepository`] using typed config keys.

use crate::traits::RuntimeConfigRepository;
use oxide_arb_error::storage::StorageError;
use oxide_arb_models::{
    domain::{RuntimeConfigInfo, UpsertRuntimeConfig},
    enums::runtime_config::RuntimeConfigKey,
};
use oxide_arb_storage::cache::{CacheKey, TieredCache};
use std::sync::Arc;

/// Caching decorator for hot-reloadable runtime configuration.
pub struct CachedRuntimeConfigRepository<R: RuntimeConfigRepository> {
    inner: R,
    cache: Arc<TieredCache>,
}

impl<R: RuntimeConfigRepository> CachedRuntimeConfigRepository<R> {
    pub const fn new(inner: R, cache: Arc<TieredCache>) -> Self {
        Self { inner, cache }
    }

    async fn invalidate(&self, key: RuntimeConfigKey) {
        let _ = self
            .cache
            .invalidate(&CacheKey::RuntimeConfig { key })
            .await;
        let _ = self.cache.invalidate(&CacheKey::AllRuntimeConfig).await;
    }
}

#[async_trait::async_trait]
impl<R: RuntimeConfigRepository> RuntimeConfigRepository for CachedRuntimeConfigRepository<R> {
    #[inline]
    async fn get(&self, key: RuntimeConfigKey) -> Result<Option<RuntimeConfigInfo>, StorageError> {
        let cache_key = CacheKey::RuntimeConfig { key };
        if let Some(cached) = self.cache.get_json::<RuntimeConfigInfo>(&cache_key).await? {
            return Ok(Some(cached));
        }
        let result = self.inner.get(key).await?;
        if let Some(ref info) = result {
            let _ = self.cache.set_json(&cache_key, info).await;
        }
        Ok(result)
    }

    #[inline]
    async fn upsert(&self, dto: UpsertRuntimeConfig) -> Result<RuntimeConfigInfo, StorageError> {
        let key = dto.key;
        let result = self.inner.upsert(dto).await?;
        self.invalidate(key).await;
        Ok(result)
    }

    #[inline]
    async fn get_all(&self) -> Result<Vec<RuntimeConfigInfo>, StorageError> {
        let key = CacheKey::AllRuntimeConfig;
        if let Some(cached) = self.cache.get_json::<Vec<RuntimeConfigInfo>>(&key).await? {
            return Ok(cached);
        }
        let values = self.inner.get_all().await?;
        let _ = self.cache.set_json(&key, &values).await;
        Ok(values)
    }

    #[inline]
    async fn delete(&self, key: RuntimeConfigKey) -> Result<bool, StorageError> {
        let deleted = self.inner.delete(key).await?;
        self.invalidate(key).await;
        Ok(deleted)
    }
}
