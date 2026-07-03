//! Admin port for the offline model-governance closure (Phase 3.7).
//!
//! The dependency-inversion boundary between an operator-facing caller (HTTP
//! routes, jobs, tests) and the core governance service. The [`GovernanceActor`]
//! is recorded in the audit trail; Casbin role enforcement is applied at the HTTP
//! layer via `publication:publish` / `publication:rollback` policies.

use async_trait::async_trait;
use quant_pivot_error::QuantResult;

use crate::{
    domain::{GatePreviewIntent, ModelVersionInfo, QualityGateReportView, TrainingDatasetInfo},
    types::{BacktestReportId, ModelVersionId, TrainingDatasetId},
};

/// Who initiated a governance action. Recorded for audit provenance.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GovernanceActor {
    /// Operator / service username.
    pub username: String,
    /// Acting role label, when known (recorded for audit provenance).
    pub role: Option<String>,
}

impl GovernanceActor {
    /// A system actor (background job / automation).
    #[must_use]
    pub fn system() -> Self {
        Self {
            username: "system".to_owned(),
            role: None,
        }
    }
}

/// Service input to publish a candidate / shadow model version.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PublishModelCommand {
    /// The candidate / shadow version to publish.
    pub model_version_id: ModelVersionId,
    /// Operator reason (audited).
    pub reason: String,
}

/// Service input to roll back a published model version to its predecessor.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RollbackModelCommand {
    /// The currently published version to retire.
    pub model_version_id: ModelVersionId,
    /// Operator reason (audited).
    pub reason: String,
}

/// Service input to retire a published model version without restoring a predecessor.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RetireModelCommand {
    /// The published version to retire.
    pub model_version_id: ModelVersionId,
    /// Operator reason (audited).
    pub reason: String,
}

/// Request to promote a `Built` training dataset to `Ready`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PromoteDatasetRequest {
    /// The dataset to promote.
    pub training_dataset_id: TrainingDatasetId,
    /// Operator reason (audited).
    pub reason: String,
}

/// Governance orchestration boundary (publish / rollback / dataset promotion),
/// implemented in `quant-pivot-core` and injected into `AppContext`.
#[async_trait]
pub trait ModelGovernancePort: Send + Sync {
    /// Publish a candidate / shadow version: enforce the quality gate + shadow
    /// stability, persist the gate report, flip the status to `Published`, and
    /// write a governance audit row. Fails if any gate is not cleared.
    async fn publish(
        &self,
        command: PublishModelCommand,
        actor: GovernanceActor,
    ) -> QuantResult<ModelVersionInfo>;

    /// Roll back a published version: retire it and restore its predecessor
    /// (still published), writing a governance audit row. Returns the restored
    /// predecessor.
    async fn rollback(
        &self,
        command: RollbackModelCommand,
        actor: GovernanceActor,
    ) -> QuantResult<ModelVersionInfo>;

    /// Retire a published version without restoring a predecessor. Clears runtime
    /// config pointers when they reference the retired version.
    async fn retire(
        &self,
        command: RetireModelCommand,
        actor: GovernanceActor,
    ) -> QuantResult<ModelVersionInfo>;

    /// Promote a `Built` dataset to `Ready` after a `DatasetReady` gate pass.
    /// An `InsufficientLabels` dataset can never be promoted (repo `Conflict`).
    async fn promote_dataset_ready(
        &self,
        request: PromoteDatasetRequest,
        actor: GovernanceActor,
    ) -> QuantResult<TrainingDatasetInfo>;

    /// Evaluate the quality gate for a version as a read-only dry-run — the same
    /// evaluator `publish` uses, but with no persistence and no state change.
    /// Drives the SPA publish-readiness scorecard. `backtest_report_id` selects
    /// a specific frozen report; `None` uses the version's most recent one.
    async fn preview_gate(
        &self,
        model_version_id: &ModelVersionId,
        intent: GatePreviewIntent,
        backtest_report_id: Option<&BacktestReportId>,
    ) -> QuantResult<QualityGateReportView>;
}
