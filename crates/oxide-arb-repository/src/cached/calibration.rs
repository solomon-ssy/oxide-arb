//! Cached wrapper for [`CalibrationRepository`] — DB-first writes with invalidation.
//!
//! Calibration data is read-heavy (every detection cycle) and write-rare
//! (updater tick every 60s). This wrapper provides:
//!
//! - **Reads**: L1+L2 cache lookup → fallback to PG → backfill cache.
//! - **Writes**: delegate to inner repo → invalidate affected keys.

use crate::traits::CalibrationRepository;
use oxide_arb_error::storage::StorageError;
use oxide_arb_models::{
    domain::calibration::{DurationBucket, PriceZone},
    entities::{calibration, calibration_outcome},
    enums::common::MarketCategory,
};
use oxide_arb_storage::cache::{CacheKey, TieredCache};
use std::sync::Arc;

/// Caching decorator for any [`CalibrationRepository`] implementation.
///
/// Generic over the inner repository type to avoid the object-safety
/// limitations of native `async fn` in traits.
pub struct CachedCalibrationRepository<R: CalibrationRepository> {
    inner: R,
    cache: Arc<TieredCache>,
}

impl<R: CalibrationRepository> CachedCalibrationRepository<R> {
    /// Wrap an existing repository with tiered cache support.
    pub const fn new(inner: R, cache: Arc<TieredCache>) -> Self {
        Self { inner, cache }
    }

    const fn bucket_key(
        category: MarketCategory,
        price_zone: PriceZone,
        duration_bucket: DurationBucket,
    ) -> CacheKey {
        CacheKey::CalibrationBucket {
            category,
            price_zone,
            duration_bucket,
        }
    }

    async fn invalidate_all(&self) {
        let _ = self
            .cache
            .invalidate(&CacheKey::AllCalibrationBuckets)
            .await;
    }
}

impl<R: CalibrationRepository> CalibrationRepository for CachedCalibrationRepository<R> {
    async fn get_bucket(
        &self,
        category: MarketCategory,
        price_zone: PriceZone,
        duration_bucket: DurationBucket,
    ) -> Result<Option<calibration::Model>, StorageError> {
        let key = Self::bucket_key(category, price_zone, duration_bucket);
        if let Some(cached) = self.cache.get_json::<calibration::Model>(&key).await? {
            return Ok(Some(cached));
        }
        let result = self
            .inner
            .get_bucket(category, price_zone, duration_bucket)
            .await?;
        if let Some(ref model) = result {
            let _ = self.cache.set_json(&key, model).await;
        }
        Ok(result)
    }

    async fn get_buckets_by_category(
        &self,
        category: MarketCategory,
    ) -> Result<Vec<calibration::Model>, StorageError> {
        self.inner.get_buckets_by_category(category).await
    }

    async fn get_all_buckets(&self) -> Result<Vec<calibration::Model>, StorageError> {
        let key = CacheKey::AllCalibrationBuckets;
        if let Some(cached) = self.cache.get_json::<Vec<calibration::Model>>(&key).await? {
            return Ok(cached);
        }
        let buckets = self.inner.get_all_buckets().await?;
        let _ = self.cache.set_json(&key, &buckets).await;
        Ok(buckets)
    }

    async fn insert_bucket(
        &self,
        bucket: calibration::ActiveModel,
    ) -> Result<calibration::Model, StorageError> {
        let result = self.inner.insert_bucket(bucket).await?;
        self.invalidate_all().await;
        Ok(result)
    }

    async fn update_bucket(
        &self,
        bucket: calibration::ActiveModel,
    ) -> Result<calibration::Model, StorageError> {
        let result = self.inner.update_bucket(bucket).await?;
        let key = Self::bucket_key(result.category, result.price_zone, result.duration_bucket);
        let _ = self.cache.invalidate(&key).await;
        self.invalidate_all().await;
        Ok(result)
    }

    async fn record_outcome(
        &self,
        outcome: calibration_outcome::ActiveModel,
    ) -> Result<(), StorageError> {
        self.inner.record_outcome(outcome).await
    }

    async fn get_unresolved_outcomes(
        &self,
    ) -> Result<Vec<calibration_outcome::Model>, StorageError> {
        self.inner.get_unresolved_outcomes().await
    }

    async fn resolve_outcome(&self, outcome_id: i64, actual_yes: bool) -> Result<(), StorageError> {
        self.inner.resolve_outcome(outcome_id, actual_yes).await?;
        self.invalidate_all().await;
        Ok(())
    }
}
