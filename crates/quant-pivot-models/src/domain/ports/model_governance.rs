//! Admin port for the offline model-governance closure (Phase 3.7).
//!
//! The dependency-inversion boundary between an operator-facing caller (HTTP
//! routes, jobs, tests) and the core governance service. The [`GovernanceActor`]
//! is recorded in the audit trail; Casbin role enforcement is applied at the HTTP
//! layer via `publication:publish` / `publication:rollback` policies.

use async_trait::async_trait;
use quant_pivot_error::QuantResult;

use crate::{
    domain::{
        BindCalibrationRequest, BindPublishPathSetRequest, GatePreviewIntent, ModelVersionInfo,
        QualityGateReportView,
    },
    types::{BacktestReportId, ModelVersionId},
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

/// Governance orchestration boundary (publish / rollback),
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

    /// Roll back a published version only after fresh full frozen parity,
    /// clear-latch generation, current publish-quality/calibration gates, and
    /// artifact validation. Atomically retires the exact current and restores
    /// its audited retired predecessor, then writes the permit-bound audit.
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

    /// Bind a `model_score` calibrator to a candidate version, minting a new
    /// candidate whose `return_model` is `Calibrated { … }` (Phase 11.3 §5).
    async fn bind_calibration(
        &self,
        model_version_id: &ModelVersionId,
        request: BindCalibrationRequest,
        actor: GovernanceActor,
    ) -> QuantResult<ModelVersionInfo>;

    /// Bind the CPCV path set that publish/promote quality gates must evaluate.
    /// The path set must belong to this model version. Candidate/Shadow only.
    async fn bind_publish_path_set(
        &self,
        model_version_id: &ModelVersionId,
        request: BindPublishPathSetRequest,
        actor: GovernanceActor,
    ) -> QuantResult<ModelVersionInfo>;
}
