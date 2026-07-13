use chrono::{DateTime, Utc};
use quant_pivot_error::storage::StorageError;
use quant_pivot_models::{
    domain::{
        ModelSpecInfo, ModelSpecListQuery, ModelVersionInfo, ModelVersionListQuery, NewModelSpec,
        NewModelVersion, NewRuntimeConfigActivation, Paginated,
    },
    types::{
        BacktestPathSetId, ContentHash, FeatureParityRunId, FeatureParityStateId, ModelSpecId,
        ModelVersionId, RuntimeConfigActivationId,
    },
};

/// Compare-and-swap permit for one governed model rollback commit.
#[derive(Clone, Copy)]
pub struct RollbackModelVersionCommit<'a> {
    /// Spec whose publication slot is being changed.
    pub model_spec_id: &'a ModelSpecId,
    /// Exact version expected to be the spec's sole published version.
    pub expected_current_model_version_id: &'a ModelVersionId,
    /// Audited retired predecessor to restore.
    pub target_model_version_id: &'a ModelVersionId,
    /// Artifact hash validated by governance before the commit boundary.
    pub expected_target_artifact_hash: &'a ContentHash,
    /// CPCV binding evaluated by the current publish quality gate.
    pub expected_target_publish_path_set_id: Option<&'a BacktestPathSetId>,
    /// Canonical content hash of the exact persisted, passed gate-report JSON.
    pub quality_gate_payload_hash: &'a ContentHash,
    /// Durable clear-latch generation captured for this commit.
    pub feature_parity_state_id: &'a FeatureParityStateId,
    /// Full subject-bound frozen parity permit for the target.
    pub feature_parity_run_id: &'a FeatureParityRunId,
}

/// Exact reverse compare-and-swap used only when the runtime pointer could not
/// be switched after a committed rollback.
#[derive(Clone)]
pub struct CompensateRollbackModelVersionCommit<'a> {
    /// Spec whose just-committed rollback is being reversed.
    pub model_spec_id: &'a ModelSpecId,
    /// Original current version to restore to `Published`.
    pub original_current_model_version_id: &'a ModelVersionId,
    /// Rollback target to return to `Retired`.
    pub failed_target_model_version_id: &'a ModelVersionId,
    /// Exact retirement timestamp minted by the failed switch.
    pub expected_current_retired_at: Option<DateTime<Utc>>,
    /// Exact publication timestamp minted by the failed switch.
    pub expected_target_published_at: Option<DateTime<Utc>>,
    /// Artifact hash validated for the failed target switch.
    pub expected_target_artifact_hash: &'a ContentHash,
    /// CPCV binding evaluated for the failed target switch.
    pub expected_target_publish_path_set_id: Option<&'a BacktestPathSetId>,
    /// Canonical exact gate-report payload used by the failed switch.
    pub quality_gate_payload_hash: &'a ContentHash,
    /// Subject-bound full parity run that authorized the failed switch.
    pub feature_parity_run_id: &'a FeatureParityRunId,
    /// Exact durable activation generation created (or observed) by the failed
    /// pointer switch.
    pub expected_runtime_config_activation_id: &'a RuntimeConfigActivationId,
    /// Optional compensating activation back to the original config. Inserted
    /// atomically with the model-status reversal; `None` means durable config
    /// never left the original generation and is only CAS-verified.
    pub runtime_config_compensation: Option<NewRuntimeConfigActivation>,
}

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

    /// Retire every currently `Published` version of `model_spec_id` except
    /// `model_version_id`, then publish `model_version_id` — all in one
    /// transaction (spec row locked first).
    ///
    /// Returns `(published, retired_predecessor_ids, rollback_target)` where
    /// `rollback_target` is the most recently published predecessor before
    /// retirement (if any).
    async fn publish_replacing_predecessors(
        &self,
        model_spec_id: &ModelSpecId,
        model_version_id: &ModelVersionId,
        feature_parity_state_id: &FeatureParityStateId,
        feature_parity_run_id: &FeatureParityRunId,
    ) -> Result<
        (
            ModelVersionInfo,
            Vec<ModelVersionId>,
            Option<ModelVersionInfo>,
        ),
        StorageError,
    >;

    /// Promote a backtested candidate into shadow evaluation (`Candidate → Shadow`).
    ///
    /// Idempotent when the version is already `Shadow` or `Published`.
    async fn promote_model_to_shadow(
        &self,
        model_version_id: &ModelVersionId,
    ) -> Result<ModelVersionInfo, StorageError>;

    /// Atomically replace the exact currently-published version with one
    /// retired predecessor after every rollback gate has passed.
    ///
    /// The transaction compares the durable clear-latch generation and the
    /// subject-bound full-parity permit, then locks the spec/current/target
    /// rows and performs `Published → Retired` plus `Retired → Published`
    /// as one indivisible commit. The expected current id prevents a stale
    /// operator request from rolling back a newer publication.
    async fn rollback_to_retired_predecessor(
        &self,
        commit: RollbackModelVersionCommit<'_>,
    ) -> Result<(ModelVersionInfo, ModelVersionInfo), StorageError>;

    /// Reverse only the exact just-committed rollback when runtime pointer sync
    /// failed. This is not a generic restore: timestamps, artifact, path set,
    /// canonical gate payload, and subject parity permit must all still match.
    async fn compensate_failed_rollback(
        &self,
        commit: CompensateRollbackModelVersionCommit<'_>,
    ) -> Result<(ModelVersionInfo, ModelVersionInfo), StorageError>;

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
