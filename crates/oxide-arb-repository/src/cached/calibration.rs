//! Cached wrapper for [`CalibrationRepository`] — DB-first writes with invalidation.

use crate::traits::CalibrationRepository;
use oxide_arb_error::storage::StorageError;
use oxide_arb_models::{
    domain::{
        CalibrationBucketInfo, CalibrationOutcomeInfo, NewCalibrationOutcome, UpsertCalibration,
    },
    enums::{
        calibration::{DurationBucket, PriceZone},
        common::MarketCategory,
    },
};
use oxide_arb_storage::cache::{CacheKey, TieredCache};
use std::sync::Arc;

/// Caching decorator for any [`CalibrationRepository`] implementation.
pub struct CachedCalibrationRepository<R: CalibrationRepository> {
    inner: R,
    cache: Arc<TieredCache>,
}

impl<R: CalibrationRepository> CachedCalibrationRepository<R> {
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
    ) -> Result<Option<CalibrationBucketInfo>, StorageError> {
        let key = Self::bucket_key(category, price_zone, duration_bucket);
        if let Some(cached) = self.cache.get_json::<CalibrationBucketInfo>(&key).await? {
            return Ok(Some(cached));
        }
        let result = self
            .inner
            .get_bucket(category, price_zone, duration_bucket)
            .await?;
        if let Some(ref info) = result {
            let _ = self.cache.set_json(&key, info).await;
        }
        Ok(result)
    }

    async fn get_buckets_by_category(
        &self,
        category: MarketCategory,
    ) -> Result<Vec<CalibrationBucketInfo>, StorageError> {
        self.inner.get_buckets_by_category(category).await
    }

    async fn get_all_buckets(&self) -> Result<Vec<CalibrationBucketInfo>, StorageError> {
        let key = CacheKey::AllCalibrationBuckets;
        if let Some(cached) = self
            .cache
            .get_json::<Vec<CalibrationBucketInfo>>(&key)
            .await?
        {
            return Ok(cached);
        }
        let buckets = self.inner.get_all_buckets().await?;
        let _ = self.cache.set_json(&key, &buckets).await;
        Ok(buckets)
    }

    async fn upsert(&self, dto: UpsertCalibration) -> Result<CalibrationBucketInfo, StorageError> {
        let result = self.inner.upsert(dto).await?;
        let key = Self::bucket_key(result.category, result.price_zone, result.duration_bucket);
        let _ = self.cache.invalidate(&key).await;
        self.invalidate_all().await;
        Ok(result)
    }

    async fn create_outcome(
        &self,
        outcome: NewCalibrationOutcome,
    ) -> Result<CalibrationOutcomeInfo, StorageError> {
        self.inner.create_outcome(outcome).await
    }

    async fn get_unresolved_outcomes(&self) -> Result<Vec<CalibrationOutcomeInfo>, StorageError> {
        self.inner.get_unresolved_outcomes().await
    }

    async fn resolve_outcome(&self, outcome_id: i64, actual_yes: bool) -> Result<(), StorageError> {
        self.inner.resolve_outcome(outcome_id, actual_yes).await?;
        self.invalidate_all().await;
        Ok(())
    }
}
