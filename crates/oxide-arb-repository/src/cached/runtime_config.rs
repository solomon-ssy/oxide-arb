//! Cached wrapper for immutable runtime config versions.

use crate::traits::RuntimeConfigVersionRepository;
use chrono::{DateTime, Utc};
use oxide_arb_error::storage::StorageError;
use oxide_arb_models::{
    domain::{
        NewRuntimeConfigActivation, NewRuntimeConfigVersion, RuntimeConfigActivationInfo,
        RuntimeConfigVersionInfo, control_factor::NewControlFactorAuditEvent,
    },
    types::RuntimeConfigVersionId,
};
use oxide_arb_storage::cache::{CacheKey, TieredCache};
use std::sync::Arc;

/// Caching decorator for immutable runtime configuration versions.
pub struct CachedRuntimeConfigVersionRepository<R: RuntimeConfigVersionRepository> {
    inner: R,
    cache: Arc<TieredCache>,
}

impl<R: RuntimeConfigVersionRepository> CachedRuntimeConfigVersionRepository<R> {
    pub const fn new(inner: R, cache: Arc<TieredCache>) -> Self {
        Self { inner, cache }
    }

    async fn invalidate_active(&self) {
        let _ = self.cache.invalidate(&CacheKey::ActiveRuntimeConfig).await;
    }
}

#[async_trait::async_trait]
impl<R: RuntimeConfigVersionRepository> RuntimeConfigVersionRepository
    for CachedRuntimeConfigVersionRepository<R>
{
    async fn create_version(
        &self,
        version: NewRuntimeConfigVersion,
    ) -> Result<RuntimeConfigVersionInfo, StorageError> {
        let info = self.inner.create_version(version).await?;
        let key = CacheKey::RuntimeConfigVersion {
            version_id: info.runtime_config_version_id.clone(),
        };
        let _ = self.cache.set_json(&key, &info).await;
        Ok(info)
    }

    async fn activate_version(
        &self,
        activation: NewRuntimeConfigActivation,
    ) -> Result<RuntimeConfigActivationInfo, StorageError> {
        let info = self.inner.activate_version(activation).await?;
        self.invalidate_active().await;
        Ok(info)
    }

    async fn create_version_governed(
        &self,
        version: NewRuntimeConfigVersion,
        audit: NewControlFactorAuditEvent,
    ) -> Result<RuntimeConfigVersionInfo, StorageError> {
        let info = self.inner.create_version_governed(version, audit).await?;
        let key = CacheKey::RuntimeConfigVersion {
            version_id: info.runtime_config_version_id.clone(),
        };
        let _ = self.cache.set_json(&key, &info).await;
        Ok(info)
    }

    async fn activate_version_governed(
        &self,
        activation: NewRuntimeConfigActivation,
        audit: NewControlFactorAuditEvent,
    ) -> Result<RuntimeConfigActivationInfo, StorageError> {
        let info = self
            .inner
            .activate_version_governed(activation, audit)
            .await?;
        self.invalidate_active().await;
        Ok(info)
    }

    async fn load_version(
        &self,
        version_id: &RuntimeConfigVersionId,
    ) -> Result<Option<RuntimeConfigVersionInfo>, StorageError> {
        let key = CacheKey::RuntimeConfigVersion {
            version_id: version_id.clone(),
        };
        if let Some(cached) = self
            .cache
            .get_json::<RuntimeConfigVersionInfo>(&key)
            .await?
        {
            return Ok(Some(cached));
        }
        let result = self.inner.load_version(version_id).await?;
        if let Some(ref info) = result {
            let _ = self.cache.set_json(&key, info).await;
        }
        Ok(result)
    }

    async fn load_by_hash(
        &self,
        config_hash: &str,
    ) -> Result<Option<RuntimeConfigVersionInfo>, StorageError> {
        self.inner.load_by_hash(config_hash).await
    }

    async fn load_current(&self) -> Result<Option<RuntimeConfigVersionInfo>, StorageError> {
        let key = CacheKey::ActiveRuntimeConfig;
        if let Some(cached) = self
            .cache
            .get_json::<RuntimeConfigVersionInfo>(&key)
            .await?
        {
            return Ok(Some(cached));
        }
        let result = self.inner.load_current().await?;
        if let Some(ref info) = result {
            let _ = self.cache.set_json(&key, info).await;
        }
        Ok(result)
    }

    async fn load_active_at(
        &self,
        at: DateTime<Utc>,
    ) -> Result<Option<RuntimeConfigVersionInfo>, StorageError> {
        self.inner.load_active_at(at).await
    }

    async fn list_activations(
        &self,
        limit: u64,
    ) -> Result<Vec<RuntimeConfigActivationInfo>, StorageError> {
        self.inner.list_activations(limit).await
    }
}
