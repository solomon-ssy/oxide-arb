use quant_pivot_error::storage::StorageError;
use quant_pivot_models::{
    domain::{
        api::{ModelPickerSide, ModelSpecListQuery, ModelVersionListQuery},
        pagination::Paginated,
        quant::{
            ModelSpecInfo, ModelVersionInfo, NewModelSpec, NewModelVersion,
            PublishedModelCatalogInfo,
        },
    },
    enums::common::MarketCategory,
    types::{
        BacktestPathSetId, FeatureParityRunId, FeatureParityStateId, ModelRunId, ModelSpecId,
        ModelVersionId, RoleCode, model_quality::QualityGateReport,
    },
};

/// Durable latch permit consumed by the atomic model-publish transaction.
#[derive(Debug, Clone)]
pub enum PublishFeatureParityPermit {
    /// Compare-and-swap against an already governed clear generation.
    ExistingGeneration(FeatureParityStateId),
    /// Mint the first clear generation from the exact pre-publication proof.
    InitializeFromProof {
        actor: String,
        acting_role: Option<RoleCode>,
        reason: String,
    },
}

/// Atomic model publication command. Runtime routing is governed separately by
/// the `ModelRouting` configuration resource.
pub struct PublishModelVersionCommit<'a> {
    pub model_spec_id: &'a ModelSpecId,
    pub model_version_id: &'a ModelVersionId,
    pub feature_parity_permit: PublishFeatureParityPermit,
    pub feature_parity_run_id: &'a FeatureParityRunId,
}

/// Result of the model publication transaction.
pub struct PublishModelVersionResult {
    pub published: ModelVersionInfo,
    pub feature_parity_state_id: FeatureParityStateId,
}

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

    /// Return the complete published picker catalog using one typed joined
    /// query. A supplied category is an exact route filter; pooled (`NULL`)
    /// artifacts are never returned as vertical fallbacks.
    async fn list_published_catalog(
        &self,
        side: ModelPickerSide,
        category: Option<MarketCategory>,
    ) -> Result<Vec<PublishedModelCatalogInfo>, StorageError>;

    /// All currently `Published` versions of a spec, most recent first. Used by
    /// governance inspection and invariant tests; rollback resolves only the
    /// predecessor recorded in the publish audit and never guesses from here.
    async fn list_published_for_spec(
        &self,
        model_spec_id: &ModelSpecId,
    ) -> Result<Vec<ModelVersionInfo>, StorageError>;

    async fn retire_model_version(
        &self,
        model_version_id: &ModelVersionId,
    ) -> Result<ModelVersionInfo, StorageError>;

    /// Publish one immutable, gate-approved artifact without changing routing
    /// or the lifecycle of any other published artifact.
    async fn publish_model_version(
        &self,
        commit: PublishModelVersionCommit<'_>,
    ) -> Result<PublishModelVersionResult, StorageError>;

    /// Promote a backtested candidate into shadow evaluation (`Candidate → Shadow`).
    ///
    /// Idempotent when the version is already `Shadow` or `Published`.
    async fn promote_model_to_shadow(
        &self,
        model_version_id: &ModelVersionId,
    ) -> Result<ModelVersionInfo, StorageError>;

    /// Persist a model version's typed quality-gate report (the governance layer
    /// writes the gate decision into `quant_model_version.quality_gate_report`
    /// before publishing). Does not change the publication status.
    async fn set_quality_gate_report(
        &self,
        model_version_id: &ModelVersionId,
        quality_gate_report: QualityGateReport,
    ) -> Result<ModelVersionInfo, StorageError>;

    /// Bind (or clear) the CPCV path set that publish/promote quality gates
    /// must evaluate. Does not change publication status. Ownership of the
    /// path set is enforced by the governance layer before calling this.
    async fn set_publish_path(
        &self,
        model_version_id: &ModelVersionId,
        publish_path_set_id: Option<BacktestPathSetId>,
    ) -> Result<ModelVersionInfo, StorageError>;
}
