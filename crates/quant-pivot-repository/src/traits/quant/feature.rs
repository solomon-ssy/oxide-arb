use quant_pivot_error::storage::StorageError;
use quant_pivot_models::{
    domain::quant::{FeatureVectorInfo, NewFeatureVector},
    types::FeatureVectorId,
};

/// Feature vector persistence port.
#[async_trait::async_trait]
pub trait FeatureRepository: Send + Sync {
    async fn create(&self, vector: NewFeatureVector) -> Result<FeatureVectorInfo, StorageError>;

    async fn create_batch(
        &self,
        vectors: Vec<NewFeatureVector>,
    ) -> Result<Vec<FeatureVectorInfo>, StorageError>;

    /// Load a persisted vector by primary key.
    async fn find_by_id(
        &self,
        id: &FeatureVectorId,
    ) -> Result<Option<FeatureVectorInfo>, StorageError>;

    /// Batch-load feature vectors by id (chunked `IN` lists). Missing ids are omitted.
    async fn find_by_ids(
        &self,
        ids: &[FeatureVectorId],
    ) -> Result<Vec<FeatureVectorInfo>, StorageError>;
}
