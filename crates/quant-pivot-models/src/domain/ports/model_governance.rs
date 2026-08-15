//! Port for immutable model evidence and candidate validation.
//!
//! The dependency-inversion boundary between feedback jobs, read-only
//! diagnostics, tests, and the core governance service.

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use quant_pivot_error::QuantResult;
use serde::{Deserialize, Serialize};

use crate::{
    domain::{
        api::{GatePreviewIntent, QualityGateReportView},
        quant::{ModelBootstrapValidationEvidence, ModelVersionInfo},
    },
    enums::quant::DownsideSource,
    types::{
        BacktestPathSetId, BacktestReportId, CalibrationArtifactId, ContentHash, ModelVersionId,
        RoleCode, UserId, model_quality::QualityGateReport,
    },
};

/// Who initiated a governance action. Recorded for audit provenance.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GovernanceActor {
    /// Authenticated internal user identity; absent only for system automation.
    pub user_id: Option<UserId>,
    /// Operator / service username.
    pub username: String,
    /// Acting role label, when known (recorded for audit provenance).
    pub role: Option<RoleCode>,
}

impl GovernanceActor {
    #[must_use]
    pub const fn authenticated(user_id: UserId, username: String, role: RoleCode) -> Self {
        Self {
            user_id: Some(user_id),
            username,
            role: Some(role),
        }
    }

    /// A system actor (background job / automation).
    #[must_use]
    pub fn system() -> Self {
        Self {
            user_id: None,
            username: "system".to_owned(),
            role: None,
        }
    }
}

/// Exact immutable CPCV evidence evaluated by the sole candidate quality gate.
///
/// The path set is carried by the feedback job instead of being rebound onto a
/// mutable model-version row. The governance service verifies both ownership
/// and content hash before evaluation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CandidateQualityGateEvidence {
    Cpcv {
        path_set_id: BacktestPathSetId,
        path_set_hash: ContentHash,
    },
    CalibrationInsufficient,
}

/// Exact CPCV and backtest identities evaluated for first-route bootstrap.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BootstrapQualityGateInput {
    pub candidate: CandidateQualityGateEvidence,
    pub validation_evidence: ModelBootstrapValidationEvidence,
}

/// Exact quality and backtest evidence returned for first-route bootstrap.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BootstrapQualityGateEvidence {
    pub quality_gate_report: QualityGateReport,
    pub validation_evidence: ModelBootstrapValidationEvidence,
}

/// Inputs that seal a new calibrated model artifact from an immutable source.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CalibratedModelSealCommand {
    pub calibrator_ref: CalibrationArtifactId,
    pub downside_source: DownsideSource,
    pub reason: String,
}

/// Governance orchestration boundary for immutable model evidence,
/// implemented in `quant-pivot-core` and injected into `AppContext`.
#[async_trait]
pub trait ModelGovernancePort: Send + Sync {
    /// Evaluate the quality gate for a version as a read-only dry-run.
    /// Drives the SPA quality scorecard. `backtest_report_id` selects a specific
    /// frozen report; `None` uses the version's most recent one.
    async fn preview_gate(
        &self,
        model_version_id: &ModelVersionId,
        intent: GatePreviewIntent,
        backtest_report_id: Option<&BacktestReportId>,
    ) -> QuantResult<QualityGateReportView>;

    /// Evaluate the sole candidate gate at a database-authoritative timestamp.
    async fn evaluate_candidate(
        &self,
        model_version_id: &ModelVersionId,
        evidence: CandidateQualityGateEvidence,
        evaluated_at: DateTime<Utc>,
    ) -> QuantResult<QualityGateReport>;

    /// Evaluate a first-route candidate against one exact latest backtest and
    /// return the immutable source identity alongside the sole gate report.
    async fn evaluate_bootstrap(
        &self,
        model_version_id: &ModelVersionId,
        input: BootstrapQualityGateInput,
        evaluated_at: DateTime<Utc>,
    ) -> QuantResult<BootstrapQualityGateEvidence>;

    /// Derive a new immutable model version whose `return_model` is
    /// `Calibrated { … }`. The source version is never rebound or mutated.
    async fn seal_calibrated_model(
        &self,
        model_version_id: &ModelVersionId,
        command: CalibratedModelSealCommand,
        actor: GovernanceActor,
    ) -> QuantResult<ModelVersionInfo>;
}
