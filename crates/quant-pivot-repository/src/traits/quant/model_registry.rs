use quant_pivot_error::storage::StorageError;
use quant_pivot_models::{
    domain::{ModelSpecInfo, ModelVersionInfo, NewModelSpec, NewModelVersion},
    types::{ModelSpecId, ModelVersionId},
};

#[async_trait::async_trait]
pub trait ModelRegistryRepository: Send + Sync {
    async fn create_model_spec(&self, spec: NewModelSpec) -> Result<ModelSpecInfo, StorageError>;

    async fn create_model_version(
        &self,
        version: NewModelVersion,
    ) -> Result<ModelVersionInfo, StorageError>;

    /// The next monotonic version number for a spec (`existing + 1`), honoring
    /// the `(model_spec_id, version)` uniqueness invariant the trainer relies on.
    async fn next_version_for_spec(&self, model_spec_id: &ModelSpecId)
    -> Result<i32, StorageError>;

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
