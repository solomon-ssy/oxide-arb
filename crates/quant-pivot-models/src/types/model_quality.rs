//! Canonical persisted model-quality evidence.
//!
//! A quality-gate report is a closed, immutable value object. Callers replace
//! the whole report after a new evaluation; they never query or patch arbitrary
//! inner keys. That makes a typed JSONB document the correct relational shape,
//! while `FromJsonQueryResult` keeps deserialization failures at the `SeaORM`
//! persistence boundary.

use std::collections::BTreeSet;

use chrono::{DateTime, Utc};
use quant_pivot_error::hashing::CanonicalDigestError;
use sea_orm::FromJsonQueryResult;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{
    hashing::CanonicalDigest,
    types::{ContentHash, ModelVersionId},
};

/// System-owned schema version for [`QualityGateReport`].
pub const QUALITY_GATE_REPORT_FORMAT_VERSION: u16 = 3;
const QUALITY_GATE_REPORT_HASH_DOMAIN: &str = "quant-pivot/model-quality-gate-report";

/// What a gate evaluation is gating.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind", content = "id")]
pub enum GateSubject {
    /// A model version under candidate, publish, or `PolicyAutomatic` evaluation.
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
    RouteActivation,
    PolicyAutomatic,
}

impl GateIntent {
    #[must_use]
    pub const fn requires_shadow_stability(self) -> bool {
        matches!(self, Self::RouteActivation | Self::PolicyAutomatic)
    }

    #[must_use]
    pub const fn requires_liquidity_feasibility(self) -> bool {
        matches!(self, Self::PolicyAutomatic)
    }

    #[must_use]
    pub const fn requires_validation_evidence(self) -> bool {
        matches!(
            self,
            Self::Candidate | Self::RouteActivation | Self::PolicyAutomatic
        )
    }

    #[must_use]
    pub const fn wire_name(self) -> &'static str {
        match self {
            Self::Candidate => "candidate",
            Self::RouteActivation => "route_activation",
            Self::PolicyAutomatic => "policy_automatic",
        }
    }
}

/// Stable identity of one governed gate.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GateId {
    SampleCount,
    LabelCoverage,
    MaterializationCoverage,
    NoPitLeakage,
    MaxDrawdown,
    LiquidityExitFeasible,
    ShadowDecisionOverlap,
    ValidationEvidenceRequired,
    CpcvRequired,
    CpcvPathCount,
    TargetRankIc,
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
    ExplainabilityRequired,
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
            Self::ShadowDecisionOverlap => "shadow_decision_overlap",
            Self::ValidationEvidenceRequired => "validation_evidence_required",
            Self::CpcvRequired => "cpcv_required",
            Self::CpcvPathCount => "cpcv_path_count",
            Self::TargetRankIc => "target_rank_ic",
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
            Self::ExplainabilityRequired => "explainability_required",
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

/// Content-addressed quality-gate evaluation bound into immutable governance evidence.
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

/// Complete inputs used to seal a [`QualityGateReport`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QualityGateReportInput {
    pub subject: GateSubject,
    pub intent: GateIntent,
    pub evaluated_at: DateTime<Utc>,
    pub gates: Vec<GateOutcome>,
}

#[derive(Serialize)]
struct QualityGateReportPreimage<'a> {
    format_version: u16,
    subject: &'a GateSubject,
    intent: GateIntent,
    evaluated_at: DateTime<Utc>,
    gates: &'a [GateOutcome],
    hard_failures: &'a [QualityGateFailure],
    soft_warnings: &'a [QualityGateFailure],
    passed: bool,
}

impl QualityGateReport {
    pub fn try_new(input: QualityGateReportInput) -> Result<Self, QualityGateReportError> {
        let mut identities = BTreeSet::new();
        for outcome in &input.gates {
            if !identities.insert(outcome.gate) {
                return Err(QualityGateReportError::Invalid(format!(
                    "duplicate quality gate `{}`",
                    outcome.gate.wire_name()
                )));
            }
            if (outcome.class == GateClass::Hard && outcome.status == GateStatus::Warn)
                || (outcome.class == GateClass::Soft && outcome.status == GateStatus::Fail)
            {
                return Err(QualityGateReportError::Invalid(format!(
                    "quality gate `{}` has an invalid class/status combination",
                    outcome.gate.wire_name()
                )));
            }
        }
        if input.gates.is_empty() {
            return Err(QualityGateReportError::Invalid(
                "quality gate report must contain a complete scorecard".to_owned(),
            ));
        }
        let hard_failures = input
            .gates
            .iter()
            .filter(|outcome| {
                outcome.class == GateClass::Hard && outcome.status == GateStatus::Fail
            })
            .map(GateOutcome::as_failure)
            .collect::<Vec<_>>();
        let soft_warnings = input
            .gates
            .iter()
            .filter(|outcome| {
                outcome.class == GateClass::Soft && outcome.status == GateStatus::Warn
            })
            .map(GateOutcome::as_failure)
            .collect::<Vec<_>>();
        let passed = hard_failures.is_empty();
        let report_hash = CanonicalDigest::content_hash_typed(
            QUALITY_GATE_REPORT_HASH_DOMAIN,
            u32::from(QUALITY_GATE_REPORT_FORMAT_VERSION),
            &QualityGateReportPreimage {
                format_version: QUALITY_GATE_REPORT_FORMAT_VERSION,
                subject: &input.subject,
                intent: input.intent,
                evaluated_at: input.evaluated_at,
                gates: &input.gates,
                hard_failures: &hard_failures,
                soft_warnings: &soft_warnings,
                passed,
            },
        )?;
        Ok(Self {
            format_version: QUALITY_GATE_REPORT_FORMAT_VERSION,
            subject: input.subject,
            intent: input.intent,
            evaluated_at: input.evaluated_at,
            gates: input.gates,
            hard_failures,
            soft_warnings,
            passed,
            report_hash,
        })
    }

    pub fn validate(&self) -> Result<(), QualityGateReportError> {
        if self.format_version != QUALITY_GATE_REPORT_FORMAT_VERSION {
            return Err(QualityGateReportError::Invalid(format!(
                "unsupported quality gate report format {}",
                self.format_version
            )));
        }
        let rebuilt = Self::try_new(QualityGateReportInput {
            subject: self.subject.clone(),
            intent: self.intent,
            evaluated_at: self.evaluated_at,
            gates: self.gates.clone(),
        })?;
        if *self != rebuilt {
            return Err(QualityGateReportError::Invalid(
                "quality gate report projections or content hash differ from its scorecard"
                    .to_owned(),
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Error)]
pub enum QualityGateReportError {
    #[error("invalid quality gate report: {0}")]
    Invalid(String),
    #[error(transparent)]
    Hash(#[from] CanonicalDigestError),
}

#[cfg(test)]
mod tests {
    use chrono::Utc;
    use serde_json::json;

    use super::{
        GateClass, GateId, GateIntent, GateOutcome, GateStatus, GateSubject, QualityGateReport,
        QualityGateReportInput,
    };
    use crate::types::ModelVersionId;

    impl QualityGateReport {
        fn test_fixture() -> Self {
            Self::try_new(QualityGateReportInput {
                subject: GateSubject::ModelVersion(ModelVersionId::from_v7()),
                intent: GateIntent::Candidate,
                evaluated_at: Utc::now(),
                gates: vec![GateOutcome {
                    gate: GateId::NoPitLeakage,
                    class: GateClass::Hard,
                    status: GateStatus::Pass,
                    observed: "0".to_owned(),
                    threshold: "0".to_owned(),
                    detail: "point-in-time scan is clean".to_owned(),
                }],
            })
            .expect("valid report")
        }
    }

    #[test]
    fn rejects_unknown_missing_shape() {
        let valid =
            serde_json::to_value(QualityGateReport::test_fixture()).expect("serialize report");
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

    #[test]
    fn report_hash_binds_evidence() {
        let report = QualityGateReport::test_fixture();
        assert!(report.validate().is_ok());

        let mut tampered = report;
        tampered.gates[0].observed = "1".to_owned();
        assert!(tampered.validate().is_err());
    }
}
