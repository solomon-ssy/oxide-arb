use quant_pivot_error::storage::StorageError;
use quant_pivot_models::{
    domain::{ModelSpecInfo, ModelVersionInfo, NewModelSpec, NewModelVersion},
    types::ModelVersionId,
};

#[async_trait::async_trait]
pub trait ModelRegistryRepository: Send + Sync {
    async fn create_model_spec(&self, spec: NewModelSpec) -> Result<ModelSpecInfo, StorageError>;

    async fn create_model_version(
        &self,
        version: NewModelVersion,
    ) -> Result<ModelVersionInfo, StorageError>;

    /// Look up a model version by id (used by the runtime factory to resolve the
    /// active / shadow artifact for a round).
    async fn find_model_version_by_id(
        &self,
        model_version_id: &ModelVersionId,
    ) -> Result<Option<ModelVersionInfo>, StorageError>;

    async fn publish_model_version(
        &self,
        model_version_id: &ModelVersionId,
    ) -> Result<ModelVersionInfo, StorageError>;

    async fn retire_model_version(
        &self,
        model_version_id: &ModelVersionId,
    ) -> Result<ModelVersionInfo, StorageError>;
}
