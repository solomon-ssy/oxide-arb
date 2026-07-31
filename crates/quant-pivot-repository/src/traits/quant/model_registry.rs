use quant_pivot_error::storage::StorageError;
use quant_pivot_models::{
    domain::{
        api::{ModelPickerSide, ModelSpecListQuery, ModelVersionListQuery},
        pagination::{PageRequest, Paginated},
        quant::{ModelCatalogInfo, ModelSpecInfo, ModelVersionInfo, NewModelSpec, NewModelVersion},
    },
    enums::common::MarketCategory,
    types::{FactorDefinitionId, ModelRunId, ModelSpecId, ModelVersionId},
};

#[async_trait::async_trait]
pub trait ModelRegistryRepository: Send + Sync {
    async fn create_model_spec(&self, spec: NewModelSpec) -> Result<ModelSpecInfo, StorageError>;

    /// Look up a model spec by id (used by governance pointer sync to route the
    /// published version onto the Buy vs Sell/exit runtime-config pointer).
    async fn find_model_spec(
        &self,
        model_spec_id: &ModelSpecId,
    ) -> Result<Option<ModelSpecInfo>, StorageError>;

    async fn create_model_version(
        &self,
        version: NewModelVersion,
    ) -> Result<ModelVersionInfo, StorageError>;

    /// Atomically insert a root training candidate and finalize its already
    /// running, unbound training run as succeeded.
    ///
    /// Version allocation remains authoritative under the owning model-spec
    /// lock. The run receives the inserted version id and uses the immutable
    /// artifact hash as its output hash in the same database transaction. The
    /// version must bind the exact Ready/Training dataset captured by the run.
    /// An exact retry of an already committed run/version pair returns the
    /// stored version; any payload or terminal-state drift fails closed.
    async fn commit_training_model_version(
        &self,
        model_run_id: &ModelRunId,
        version: NewModelVersion,
    ) -> Result<ModelVersionInfo, StorageError>;

    /// The next monotonic version number for a spec (`existing + 1`), honoring
    /// the `(model_spec_id, version)` uniqueness invariant the trainer relies on.
    async fn next_version_for_spec(&self, model_spec_id: &ModelSpecId)
    -> Result<i32, StorageError>;

    /// Look up a model version by id for deep preimage verification and atomic
    /// serving-generation resolution.
    async fn find_model_version(
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

    /// Page verified model contracts whose exact serving plane contains one
    /// immutable factor revision.
    async fn page_factor_usages(
        &self,
        factor_definition_id: &FactorDefinitionId,
        page: PageRequest,
    ) -> Result<Paginated<ModelVersionInfo>, StorageError>;

    /// Return the complete immutable model picker catalog using one typed joined
    /// query. A supplied category is an exact route filter; pooled (`NULL`)
    /// artifacts are never returned as vertical fallbacks.
    async fn list_model_catalog(
        &self,
        side: ModelPickerSide,
        category: Option<MarketCategory>,
    ) -> Result<Vec<ModelCatalogInfo>, StorageError>;
}
