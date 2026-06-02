//! Evidence chain types required by every control-factor value.

use crate::types::{MaterializationRunId, RuntimeConfigVersionId, StageReportId};
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
    pub query_fingerprint: String,
    pub confidence_interval: ConfidenceInterval,
    pub tail_risk: TailRiskEvidence,
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
            && !self.query_fingerprint.is_empty()
            && self.data_coverage.is_sufficient()
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

/// Versioned point-in-time inputs used to rebuild a historical decision context.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PointInTimeInputManifest {
    pub market_metadata_version: String,
    pub token_mapping_version: String,
    pub fee_schedule_version: String,
    pub calibration_snapshot_version: String,
    pub runtime_config_version_id: RuntimeConfigVersionId,
    pub risk_state_snapshot_version: String,
    pub balance_snapshot_version: String,
    pub settlement_truth_version: String,
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

/// Manual approval metadata required for any risk-expanding control change.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ManualApproval {
    pub approved_by: String,
    pub risk_owner: String,
    pub reason: String,
    pub rollback_target: String,
    pub retrospective_required: bool,
}
