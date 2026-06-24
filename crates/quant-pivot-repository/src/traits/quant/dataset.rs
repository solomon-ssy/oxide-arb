//! Training-dataset ledger repository trait.

use quant_pivot_error::storage::StorageError;
use quant_pivot_models::{
    domain::{NewTrainingDataset, TrainingDatasetInfo},
    enums::quant::TrainingDatasetStatus,
    types::TrainingDatasetId,
};

/// Persistence port for the frozen training-dataset ledger.
#[async_trait::async_trait]
pub trait TrainingDatasetRepository: Send + Sync {
    /// Insert a new training-dataset row, returning the persisted projection.
    async fn create(
        &self,
        dataset: NewTrainingDataset,
    ) -> Result<TrainingDatasetInfo, StorageError>;

    /// Look up a training dataset by id.
    async fn find_by_id(
        &self,
        training_dataset_id: &TrainingDatasetId,
    ) -> Result<Option<TrainingDatasetInfo>, StorageError>;

    /// Transition a training dataset to `next`, enforcing the lifecycle state
    /// machine. Returns a [`StorageError::Conflict`] on an illegal transition or
    /// a missing row.
    async fn mark_status(
        &self,
        training_dataset_id: &TrainingDatasetId,
        next: TrainingDatasetStatus,
    ) -> Result<TrainingDatasetInfo, StorageError>;
}
