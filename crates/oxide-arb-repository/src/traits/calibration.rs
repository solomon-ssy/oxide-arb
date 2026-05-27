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

pub trait CalibrationRepository: Send + Sync {
    async fn get_bucket(
        &self,
        category: MarketCategory,
        price_zone: PriceZone,
        duration_bucket: DurationBucket,
    ) -> Result<Option<CalibrationBucketInfo>, StorageError>;

    async fn get_buckets_by_category(
        &self,
        category: MarketCategory,
    ) -> Result<Vec<CalibrationBucketInfo>, StorageError>;

    async fn get_all_buckets(&self) -> Result<Vec<CalibrationBucketInfo>, StorageError>;

    /// Insert or update a calibration bucket (`ON CONFLICT DO UPDATE`).
    async fn upsert(
        &self,
        bucket: UpsertCalibration,
    ) -> Result<CalibrationBucketInfo, StorageError>;

    /// Record a new calibration outcome for later resolution.
    async fn create_outcome(
        &self,
        outcome: NewCalibrationOutcome,
    ) -> Result<CalibrationOutcomeInfo, StorageError>;

    async fn get_unresolved_outcomes(&self) -> Result<Vec<CalibrationOutcomeInfo>, StorageError>;

    async fn resolve_outcome(&self, outcome_id: i64, actual_yes: bool) -> Result<(), StorageError>;
}
