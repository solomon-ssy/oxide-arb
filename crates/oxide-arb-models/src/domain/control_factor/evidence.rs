//! Evidence chain types required by every control-factor value.

use super::materialization::PointInTimeInputManifest;
use crate::{
    domain::evidence::EvidenceSourceRefs,
    enums::control_factor::{
        ControlFactorType, FactorMaturity, QualityGateName, QualityGateOutcome,
    },
    types::{ControlFactorId, MaterializationRunId, StageReportId},
};
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

/// Required evidence chain for a control-factor value.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FactorEvidence {
    pub materialization_run_id: MaterializationRunId,
    pub stage_report_ids: Vec<StageReportId>,
    pub window_from: DateTime<Utc>,
    pub window_to: DateTime<Utc>,
    pub source_delay_secs: u64,
    pub market_count: u32,
    pub event_count: u32,
    pub opportunity_count: u32,
    pub settlement_count: u32,
    pub sample_count: u32,
    pub data_coverage: DataCoverageReport,
    pub point_in_time_inputs: PointInTimeInputManifest,
    pub baseline_config_hash: String,
    pub code_git_sha: String,
    pub dataset_hash: String,
    pub feature_schema_hash: String,
    pub label_schema_hash: String,
    pub query_fingerprint: String,
    pub confidence_interval: ConfidenceInterval,
    pub tail_risk: TailRiskEvidence,
    pub maturity: FactorMaturity,
    pub source_refs: Vec<EvidenceSourceRefs>,
    pub warnings: Vec<EvidenceWarning>,
}

impl FactorEvidence {
    #[must_use]
    pub fn is_sufficient_for_candidate(&self) -> bool {
        self.window_from < self.window_to
            && self.sample_count > 0
            && !self.stage_report_ids.is_empty()
            && !self.baseline_config_hash.is_empty()
            && !self.code_git_sha.is_empty()
            && !self.dataset_hash.is_empty()
            && !self.feature_schema_hash.is_empty()
            && !self.label_schema_hash.is_empty()
            && !self.query_fingerprint.is_empty()
            && self.data_coverage.is_sufficient()
            && self.point_in_time_inputs.production_eligible
    }
}

/// Coverage metrics for the source data used by a factor.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DataCoverageReport {
    pub expected_rows: u64,
    pub observed_rows: u64,
    pub missing_rows: u64,
    pub coverage_ratio: Decimal,
    pub insufficient_reasons: Vec<String>,
}

impl DataCoverageReport {
    #[must_use]
    pub fn is_sufficient(&self) -> bool {
        self.missing_rows == 0
            && self.observed_rows > 0
            && self.coverage_ratio >= Decimal::ONE
            && self.insufficient_reasons.is_empty()
    }
}

/// Confidence interval for an estimated factor effect.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConfidenceInterval {
    pub lower: Decimal,
    pub point_estimate: Decimal,
    pub upper: Decimal,
    pub confidence_level: Decimal,
}

/// Tail-risk evidence that prevents averages from hiding catastrophic buckets.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct TailRiskEvidence {
    pub p95_loss: Decimal,
    pub p99_loss: Decimal,
    pub max_loss: Decimal,
    pub expected_shortfall: Decimal,
}

/// Non-fatal evidence warning retained for audit.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EvidenceWarning {
    pub code: String,
    pub message: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QualityGateDecision {
    pub factor_id: Option<ControlFactorId>,
    pub factor_type: ControlFactorType,
    pub gate_name: QualityGateName,
    pub outcome: QualityGateOutcome,
    pub blocking: bool,
    pub code: String,
    pub message: String,
    pub observed_value: Option<String>,
    pub threshold: Option<String>,
}

impl QualityGateDecision {
    #[must_use]
    pub fn passed(factor_type: ControlFactorType, gate_name: QualityGateName) -> Self {
        Self {
            factor_id: None,
            factor_type,
            gate_name,
            outcome: QualityGateOutcome::Passed,
            blocking: false,
            code: "gate.passed".to_owned(),
            message: "gate passed".to_owned(),
            observed_value: None,
            threshold: None,
        }
    }

    #[must_use]
    pub fn failed(
        factor_type: ControlFactorType,
        gate_name: QualityGateName,
        code: impl Into<String>,
        message: impl Into<String>,
    ) -> Self {
        Self {
            factor_id: None,
            factor_type,
            gate_name,
            outcome: QualityGateOutcome::Failed,
            blocking: true,
            code: code.into(),
            message: message.into(),
            observed_value: None,
            threshold: None,
        }
    }

    #[must_use]
    pub const fn is_blocking_failure(&self) -> bool {
        matches!(self.outcome, QualityGateOutcome::Failed) && self.blocking
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QualityGateEvaluationReport {
    pub evaluated_factor_count: u64,
    pub passed_factor_count: u64,
    pub rejected_factor_count: u64,
    pub report_only_factor_count: u64,
    pub decisions: Vec<QualityGateDecision>,
}

impl QualityGateEvaluationReport {
    #[must_use]
    pub fn has_blocking_failures(&self) -> bool {
        self.decisions
            .iter()
            .any(QualityGateDecision::is_blocking_failure)
    }
}

/// Manual approval metadata retained for governance audit events.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ManualApproval {
    pub approved_by: String,
    pub risk_owner: String,
    pub reason: String,
    pub rollback_target: String,
    pub retrospective_required: bool,
}
