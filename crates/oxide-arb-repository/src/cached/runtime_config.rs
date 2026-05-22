//! Cached wrapper for [`RuntimeConfigRepository`] using typed config keys.

use crate::traits::RuntimeConfigRepository;
use oxide_arb_error::storage::StorageError;
use oxide_arb_models::entities::runtime_config::{self, RuntimeConfigKey};
use oxide_arb_storage::cache::{CacheKey, TieredCache};
use std::str::FromStr;
use std::sync::Arc;

/// Caching decorator for hot-reloadable runtime configuration.
pub struct CachedRuntimeConfigRepository<R: RuntimeConfigRepository> {
    inner: R,
    cache: Arc<TieredCache>,
}

impl<R: RuntimeConfigRepository> CachedRuntimeConfigRepository<R> {
    /// Wrap an existing repository with tiered cache support.
    pub const fn new(inner: R, cache: Arc<TieredCache>) -> Self {
        Self { inner, cache }
    }

    async fn invalidate_typed(&self, key: RuntimeConfigKey) {
        let _ = self
            .cache
            .invalidate(&CacheKey::RuntimeConfig { key })
            .await;
        let _ = self.cache.invalidate(&CacheKey::AllRuntimeConfig).await;
    }
}

impl<R: RuntimeConfigRepository> RuntimeConfigRepository for CachedRuntimeConfigRepository<R> {
    async fn get(&self, key: &str) -> Result<Option<runtime_config::Model>, StorageError> {
        let Ok(typed_key) = RuntimeConfigKey::from_str(key) else {
            return self.inner.get(key).await;
        };
        self.get_typed(typed_key).await
    }

    async fn get_typed(
        &self,
        key: RuntimeConfigKey,
    ) -> Result<Option<runtime_config::Model>, StorageError> {
        let cache_key = CacheKey::RuntimeConfig { key };
        if let Some(cached) = self
            .cache
            .get_json::<runtime_config::Model>(&cache_key)
            .await?
        {
            return Ok(Some(cached));
        }
        let result = self.inner.get_typed(key).await?;
        if let Some(ref model) = result {
            let _ = self.cache.set_json(&cache_key, model).await;
        }
        Ok(result)
    }

    async fn set(
        &self,
        key: &str,
        value: &serde_json::Value,
        updated_by: &str,
    ) -> Result<runtime_config::Model, StorageError> {
        let result = self.inner.set(key, value, updated_by).await?;
        if let Ok(typed_key) = RuntimeConfigKey::from_str(key) {
            self.invalidate_typed(typed_key).await;
        } else {
            let _ = self.cache.invalidate(&CacheKey::AllRuntimeConfig).await;
        }
        Ok(result)
    }

    async fn set_typed(
        &self,
        key: RuntimeConfigKey,
        value: &serde_json::Value,
        updated_by: &str,
    ) -> Result<runtime_config::Model, StorageError> {
        let result = self.inner.set_typed(key, value, updated_by).await?;
        self.invalidate_typed(key).await;
        Ok(result)
    }

    async fn get_all(&self) -> Result<Vec<runtime_config::Model>, StorageError> {
        let key = CacheKey::AllRuntimeConfig;
        if let Some(cached) = self
            .cache
            .get_json::<Vec<runtime_config::Model>>(&key)
            .await?
        {
            return Ok(cached);
        }
        let values = self.inner.get_all().await?;
        let _ = self.cache.set_json(&key, &values).await;
        Ok(values)
    }

    async fn delete(&self, key: &str) -> Result<bool, StorageError> {
        let deleted = self.inner.delete(key).await?;
        if let Ok(typed_key) = RuntimeConfigKey::from_str(key) {
            self.invalidate_typed(typed_key).await;
        } else {
            let _ = self.cache.invalidate(&CacheKey::AllRuntimeConfig).await;
        }
        Ok(deleted)
    }
}
