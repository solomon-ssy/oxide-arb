use oxide_arb_error::storage::StorageError;
use oxide_arb_models::entities::{calibration, calibration_outcome};

pub trait CalibrationRepository: Send + Sync {
    async fn get_bucket(
        &self,
        category: &str,
        price_zone: &str,
        duration_bucket: &str,
    ) -> Result<Option<calibration::Model>, StorageError>;

    async fn get_buckets_by_category(
        &self,
        category: &str,
    ) -> Result<Vec<calibration::Model>, StorageError>;

    async fn get_all_buckets(&self) -> Result<Vec<calibration::Model>, StorageError>;

    async fn insert_bucket(
        &self,
        bucket: calibration::ActiveModel,
    ) -> Result<calibration::Model, StorageError>;

    async fn update_bucket(
        &self,
        bucket: calibration::ActiveModel,
    ) -> Result<calibration::Model, StorageError>;

    async fn record_outcome(
        &self,
        outcome: calibration_outcome::ActiveModel,
    ) -> Result<(), StorageError>;

    async fn get_unresolved_outcomes(
        &self,
    ) -> Result<Vec<calibration_outcome::Model>, StorageError>;

    async fn resolve_outcome(&self, outcome_id: i64, actual_yes: bool) -> Result<(), StorageError>;
}
