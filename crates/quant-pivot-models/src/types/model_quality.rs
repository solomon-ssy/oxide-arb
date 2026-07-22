//! Canonical persisted model-quality evidence.
//!
//! A quality-gate report is a closed, immutable value object. Callers replace
//! the whole report after a new evaluation; they never query or patch arbitrary
//! inner keys. That makes a typed JSONB document the correct relational shape,
//! while `FromJsonQueryResult` keeps deserialization failures at the `SeaORM`
//! persistence boundary.

use chrono::{DateTime, Utc};
use sea_orm::FromJsonQueryResult;
use serde::{Deserialize, Serialize};

use crate::types::{ContentHash, ModelVersionId};

/// System-owned schema version for [`QualityGateReport`].
pub const QUALITY_GATE_REPORT_FORMAT_VERSION: u16 = 1;

/// What a gate evaluation is gating.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind", content = "id")]
pub enum GateSubject {
    /// A model version under candidate, publish, or auto-execution evaluation.
    ModelVersion(ModelVersionId),
}

impl GateSubject {
    /// Subject id rendered for error and audit context.
    #[must_use]
    pub fn id_string(&self) -> String {
        match self {
            Self::ModelVersion(id) => id.to_string(),
        }
    }

    /// Stable subject-kind label.
    #[must_use]
    pub const fn kind(&self) -> &'static str {
        match self {
            Self::ModelVersion(_) => "model_version",
        }
    }
}

/// Governed lifecycle action evaluated by a quality gate.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GateIntent {
    Candidate,
    Publish,
    AutoExecution,
}

impl GateIntent {
    #[must_use]
    pub const fn requires_shadow_stability(self) -> bool {
        matches!(self, Self::Publish | Self::AutoExecution)
    }

    #[must_use]
    pub const fn requires_liquidity_feasibility(self) -> bool {
        matches!(self, Self::AutoExecution)
    }

    #[must_use]
    pub const fn requires_backtest(self) -> bool {
        matches!(self, Self::Candidate | Self::Publish | Self::AutoExecution)
    }

    #[must_use]
    pub const fn wire_name(self) -> &'static str {
        match self {
            Self::Candidate => "candidate",
            Self::Publish => "publish",
            Self::AutoExecution => "auto_execution",
        }
    }
}

/// Stable identity of one governed gate.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GateId {
    SampleCount,
    LabelCoverage,
    MaterializationCoverage,
    NoPitLeakage,
    MaxDrawdown,
    LiquidityExitFeasible,
    ShadowOverlapStability,
    BacktestRequired,
    CpcvRequired,
    RankIc,
    DeflatedSharpe,
    Pbo,
    MinTrackRecordLength,
    TurnoverBudget,
    TailLossBudget,
    SellBaselineUplift,
    HitRate,
    CategoryConcentration,
    SellL2BookFidelity,
    SellFallbackRatio,
    CalibrationRequired,
}

impl GateId {
    #[must_use]
    pub const fn wire_name(self) -> &'static str {
        match self {
            Self::SampleCount => "sample_count",
            Self::LabelCoverage => "label_coverage",
            Self::MaterializationCoverage => "materialization_coverage",
            Self::NoPitLeakage => "no_pit_leakage",
            Self::MaxDrawdown => "max_drawdown",
            Self::LiquidityExitFeasible => "liquidity_exit_feasible",
            Self::ShadowOverlapStability => "shadow_overlap_stability",
            Self::BacktestRequired => "backtest_required",
            Self::CpcvRequired => "cpcv_required",
            Self::RankIc => "rank_ic",
            Self::DeflatedSharpe => "deflated_sharpe",
            Self::Pbo => "pbo",
            Self::MinTrackRecordLength => "min_track_record_length",
            Self::TurnoverBudget => "turnover_budget",
            Self::TailLossBudget => "tail_loss_budget",
            Self::SellBaselineUplift => "sell_baseline_uplift",
            Self::HitRate => "hit_rate",
            Self::CategoryConcentration => "category_concentration",
            Self::SellL2BookFidelity => "sell_l2_book_fidelity",
            Self::SellFallbackRatio => "sell_fallback_ratio",
            Self::CalibrationRequired => "calibration_required",
        }
    }
}

/// One threshold miss projected from the complete scorecard.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QualityGateFailure {
    pub gate: GateId,
    pub observed: String,
    pub threshold: String,
    pub detail: String,
}

/// Whether a gate is blocking or advisory.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GateClass {
    Hard,
    Soft,
}

impl GateClass {
    #[must_use]
    pub const fn wire_name(self) -> &'static str {
        match self {
            Self::Hard => "hard",
            Self::Soft => "soft",
        }
    }
}

/// Evaluated state of one gate.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GateStatus {
    Pass,
    Fail,
    Warn,
    NotApplicable,
}

impl GateStatus {
    #[must_use]
    pub const fn wire_name(self) -> &'static str {
        match self {
            Self::Pass => "pass",
            Self::Fail => "fail",
            Self::Warn => "warn",
            Self::NotApplicable => "not_applicable",
        }
    }
}

/// Complete scorecard row for one gate.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GateOutcome {
    pub gate: GateId,
    pub class: GateClass,
    pub status: GateStatus,
    pub observed: String,
    pub threshold: String,
    pub detail: String,
}

impl GateOutcome {
    /// Project a failed or warning scorecard row onto the failure shape.
    #[must_use]
    pub fn as_failure(&self) -> QualityGateFailure {
        QualityGateFailure {
            gate: self.gate,
            observed: self.observed.clone(),
            threshold: self.threshold.clone(),
            detail: self.detail.clone(),
        }
    }
}

/// Content-addressed quality-gate evaluation persisted on a model version.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, FromJsonQueryResult)]
#[serde(deny_unknown_fields)]
pub struct QualityGateReport {
    pub format_version: u16,
    pub subject: GateSubject,
    pub intent: GateIntent,
    pub evaluated_at: DateTime<Utc>,
    pub gates: Vec<GateOutcome>,
    pub hard_failures: Vec<QualityGateFailure>,
    pub soft_warnings: Vec<QualityGateFailure>,
    pub passed: bool,
    pub report_hash: ContentHash,
}

#[cfg(test)]
mod tests {
    use chrono::Utc;
    use serde_json::json;

    use super::{GateIntent, GateSubject, QUALITY_GATE_REPORT_FORMAT_VERSION, QualityGateReport};
    use crate::types::{ContentHash, ModelVersionId};

    fn report() -> QualityGateReport {
        QualityGateReport {
            format_version: QUALITY_GATE_REPORT_FORMAT_VERSION,
            subject: GateSubject::ModelVersion(ModelVersionId::from_v7()),
            intent: GateIntent::Candidate,
            evaluated_at: Utc::now(),
            gates: Vec::new(),
            hard_failures: Vec::new(),
            soft_warnings: Vec::new(),
            passed: true,
            report_hash: ContentHash::parse(&format!("blake3:{}", "0".repeat(64)))
                .expect("valid hash"),
        }
    }

    #[test]
    fn fixed_report_rejects_unknown_missing_and_wrong_version_shape() {
        let valid = serde_json::to_value(report()).expect("serialize report");
        assert!(serde_json::from_value::<QualityGateReport>(valid.clone()).is_ok());

        let mut unknown = valid.clone();
        unknown["extension"] = json!(true);
        assert!(serde_json::from_value::<QualityGateReport>(unknown).is_err());

        let mut missing = valid.clone();
        missing.as_object_mut().expect("object").remove("passed");
        assert!(serde_json::from_value::<QualityGateReport>(missing).is_err());

        let mut wrong_type = valid;
        wrong_type["format_version"] = json!("1");
        assert!(serde_json::from_value::<QualityGateReport>(wrong_type).is_err());
    }
}
