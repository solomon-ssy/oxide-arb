//! Training-dataset ledger repository trait.

use quant_pivot_error::storage::StorageError;
use quant_pivot_models::{
    domain::{
        CompleteTrainingDatasetBuild, NewTrainingDatasetPlan, Paginated, TrainingDatasetInfo,
        TrainingDatasetListQuery,
    },
    types::TrainingDatasetId,
};

/// Persistence port for the frozen training-dataset ledger.
#[async_trait::async_trait]
pub trait TrainingDatasetRepository: Send + Sync {
    /// Persist the immutable plan before materialization starts.
    async fn create_plan(
        &self,
        plan: NewTrainingDatasetPlan,
    ) -> Result<TrainingDatasetInfo, StorageError>;

    /// Look up a training dataset by id.
    async fn find_by_id(
        &self,
        training_dataset_id: &TrainingDatasetId,
    ) -> Result<Option<TrainingDatasetInfo>, StorageError>;

    /// Page the ledger for the operator catalog, newest (`created_at`) first.
    async fn page(
        &self,
        query: TrainingDatasetListQuery,
    ) -> Result<Paginated<TrainingDatasetInfo>, StorageError>;

    /// Claim a planned build. Re-reading an already-building row is idempotent
    /// so a lease-recovered research job can resume deterministic work.
    async fn start_build(
        &self,
        training_dataset_id: &TrainingDatasetId,
    ) -> Result<TrainingDatasetInfo, StorageError>;

    /// Atomically bind every verified artifact field and terminal build status.
    async fn complete_build(
        &self,
        training_dataset_id: &TrainingDatasetId,
        completion: CompleteTrainingDatasetBuild,
    ) -> Result<TrainingDatasetInfo, StorageError>;

    /// Fail a planned/building row without inventing artifact bindings.
    async fn fail_build(
        &self,
        training_dataset_id: &TrainingDatasetId,
        detail: String,
    ) -> Result<TrainingDatasetInfo, StorageError>;

    /// Retire a ready dataset while preserving its immutable artifact binding.
    async fn expire(
        &self,
        training_dataset_id: &TrainingDatasetId,
    ) -> Result<TrainingDatasetInfo, StorageError>;
}
