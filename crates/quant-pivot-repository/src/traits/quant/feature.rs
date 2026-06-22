use quant_pivot_error::storage::StorageError;
use quant_pivot_models::domain::{FeatureVectorInfo, NewFeatureVector};

/// Feature vector persistence port.
#[async_trait::async_trait]
pub trait FeatureRepository: Send + Sync {
    async fn create(&self, vector: NewFeatureVector) -> Result<FeatureVectorInfo, StorageError>;

    async fn create_batch(
        &self,
        vectors: Vec<NewFeatureVector>,
    ) -> Result<Vec<FeatureVectorInfo>, StorageError>;
}
