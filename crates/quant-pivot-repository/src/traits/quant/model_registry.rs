use quant_pivot_error::storage::StorageError;
use quant_pivot_models::{
    domain::{
        ModelSpecInfo, ModelSpecListQuery, ModelVersionInfo, ModelVersionListQuery, NewModelSpec,
        NewModelVersion, Paginated,
    },
    types::{BacktestPathSetId, ModelSpecId, ModelVersionId},
};

#[async_trait::async_trait]
pub trait ModelRegistryRepository: Send + Sync {
    async fn create_model_spec(&self, spec: NewModelSpec) -> Result<ModelSpecInfo, StorageError>;

    /// Look up a model spec by id (used by governance pointer sync to route the
    /// published version onto the Buy vs Sell/exit runtime-config pointer).
    async fn find_model_spec_by_id(
        &self,
        model_spec_id: &ModelSpecId,
    ) -> Result<Option<ModelSpecInfo>, StorageError>;

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

    /// Page the model-spec catalog for the operator console, newest first.
    async fn page_specs(
        &self,
        query: ModelSpecListQuery,
    ) -> Result<Paginated<ModelSpecInfo>, StorageError>;

    /// Page the trained-model version registry for the operator console, newest
    /// (`created_at`) first.
    async fn page_versions(
        &self,
        query: ModelVersionListQuery,
    ) -> Result<Paginated<ModelVersionInfo>, StorageError>;

    /// All currently `Published` versions of a spec, most recent first. Used by
    /// the governance layer to capture a rollback target when publishing and to
    /// resolve the restored version on rollback.
    async fn list_published_for_spec(
        &self,
        model_spec_id: &ModelSpecId,
    ) -> Result<Vec<ModelVersionInfo>, StorageError>;

    async fn publish_model_version(
        &self,
        model_version_id: &ModelVersionId,
    ) -> Result<ModelVersionInfo, StorageError>;

    async fn retire_model_version(
        &self,
        model_version_id: &ModelVersionId,
    ) -> Result<ModelVersionInfo, StorageError>;

    /// Promote a backtested candidate into shadow evaluation (`Candidate → Shadow`).
    ///
    /// Idempotent when the version is already `Shadow` or `Published`.
    async fn promote_model_to_shadow(
        &self,
        model_version_id: &ModelVersionId,
    ) -> Result<ModelVersionInfo, StorageError>;

    /// Restore a retired production version (`Retired → Published`) during rollback.
    ///
    /// Governance-only transition: re-publishes the rollback target without a new
    /// artifact hash.
    async fn restore_model_version(
        &self,
        model_version_id: &ModelVersionId,
    ) -> Result<ModelVersionInfo, StorageError>;

    /// Persist a model version's quality-gate report JSON (the governance layer
    /// writes the gate decision into `quant_model_version.quality_gate_report`
    /// before publishing). Does not change the publication status.
    async fn set_quality_gate_report(
        &self,
        model_version_id: &ModelVersionId,
        quality_gate_report: serde_json::Value,
    ) -> Result<ModelVersionInfo, StorageError>;

    /// Bind (or clear) the CPCV path set that publish/promote quality gates
    /// must evaluate. Does not change publication status. Ownership of the
    /// path set is enforced by the governance layer before calling this.
    async fn set_publish_path_set_id(
        &self,
        model_version_id: &ModelVersionId,
        publish_path_set_id: Option<BacktestPathSetId>,
    ) -> Result<ModelVersionInfo, StorageError>;
}
