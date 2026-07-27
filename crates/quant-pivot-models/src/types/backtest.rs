//! Canonical strongly typed backtest persistence documents.

use std::{
    ops::{Deref, DerefMut},
    vec::IntoIter,
};

use chrono::{DateTime, Utc};
use quant_pivot_error::hashing::CanonicalDigestError;
use rust_decimal::Decimal;
use sea_orm::FromJsonQueryResult;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{
    enums::common::MarketCategory,
    hashing::CanonicalDigest,
    types::{
        BacktestReportId, CalibrationArtifactId, ContentHash, DecisionPolicySnapshotId,
        ModelVersionId, Probability, TrainingDatasetId,
    },
};

const CPCV_EVIDENCE_SCHEMA_VERSION: u32 = 1;
/// Fixed decimal precision used by deterministic backtest/comparison metrics.
pub const BACKTEST_METRIC_SCALE: u32 = 12;

/// Deeply verified serving subject bound into one persisted CPCV path set.
///
/// Relational subject IDs remain native columns on `quant_backtest_path_set`.
/// These hashes bind the exact immutable bytes and semantic contracts behind
/// those IDs so a cached path set can be reverified without trusting scalar
/// foreign keys alone.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, FromJsonQueryResult)]
#[serde(deny_unknown_fields)]
pub struct CpcvPathSetSubject {
    pub schema_version: u32,
    pub model_artifact_hash: ContentHash,
    pub serving_contract_hash: ContentHash,
    pub training_dataset_hash: ContentHash,
    pub dataset_manifest_hash: ContentHash,
    pub dataset_artifact_bytes_hash: ContentHash,
    pub policy_snapshot_hash: ContentHash,
}

impl CpcvPathSetSubject {
    #[must_use]
    pub const fn new(
        model_artifact_hash: ContentHash,
        serving_contract_hash: ContentHash,
        training_dataset_hash: ContentHash,
        dataset_manifest_hash: ContentHash,
        dataset_artifact_bytes_hash: ContentHash,
        policy_snapshot_hash: ContentHash,
    ) -> Self {
        Self {
            schema_version: CPCV_EVIDENCE_SCHEMA_VERSION,
            model_artifact_hash,
            serving_contract_hash,
            training_dataset_hash,
            dataset_manifest_hash,
            dataset_artifact_bytes_hash,
            policy_snapshot_hash,
        }
    }

    pub const fn validate(&self) -> Result<(), CpcvEvidenceError> {
        validate_evidence_version(self.schema_version)
    }
}

/// Explicit policy used when a weighted CPCV fold must be resealed without the
/// subject model's production calibration dependency.
///
/// Fold estimators are newly trained and therefore cannot inherit a calibrator
/// fitted against the subject estimator. A calibrated subject must resolve its
/// verified uncalibrated parent and bind that exact parent's heuristic return
/// model; clearing calibration without this lineage is forbidden.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "snake_case", tag = "kind")]
pub enum CpcvFoldCalibrationPolicy {
    /// Classical model families do not carry the weighted return-model layer.
    NotApplicable,
    /// The subject is already an uncalibrated root; preserve its exact
    /// heuristic return model.
    SubjectHeuristic { return_model_hash: ContentHash },
    /// The subject is a calibrated child. Fold training uses the exact
    /// heuristic return model from its deeply verified parent.
    CalibratedSubjectParentHeuristic {
        calibration_artifact_id: CalibrationArtifactId,
        calibration_hash: ContentHash,
        parent_model_version_id: ModelVersionId,
        parent_artifact_hash: ContentHash,
        parent_serving_contract_hash: ContentHash,
        parent_return_model_hash: ContentHash,
    },
}

/// Complete governed methodology commitments for one CPCV run.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, FromJsonQueryResult)]
#[serde(deny_unknown_fields)]
pub struct CpcvMethodologyBinding {
    pub schema_version: u32,
    pub config_hash: ContentHash,
    pub portfolio_caps_hash: ContentHash,
    pub replay_config_hash: ContentHash,
    pub fold_calibration: CpcvFoldCalibrationPolicy,
}

impl CpcvMethodologyBinding {
    #[must_use]
    pub const fn new(
        config_hash: ContentHash,
        portfolio_caps_hash: ContentHash,
        replay_config_hash: ContentHash,
        fold_calibration: CpcvFoldCalibrationPolicy,
    ) -> Self {
        Self {
            schema_version: CPCV_EVIDENCE_SCHEMA_VERSION,
            config_hash,
            portfolio_caps_hash,
            replay_config_hash,
            fold_calibration,
        }
    }

    pub const fn validate(&self) -> Result<(), CpcvEvidenceError> {
        validate_evidence_version(self.schema_version)
    }
}

/// Semantic role of one trained artifact in the frozen CPCV evidence ledger.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "snake_case", tag = "kind")]
pub enum CpcvFoldRole {
    /// One purged/embargoed CPCV combination.
    Validation,
    /// One governed full-window hyperparameter trial.
    Trial { trial_id: u32 },
}

/// Immutable evidence for one ephemeral fold/trial estimator.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CpcvFoldArtifact {
    pub role: CpcvFoldRole,
    pub training_groups_hash: ContentHash,
    pub training_group_count: u64,
    pub model_artifact_hash: ContentHash,
    pub serving_contract_hash: ContentHash,
    pub model_payload_hash: ContentHash,
}

/// Canonically ordered, duplicate-free fold/trial artifact ledger.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, FromJsonQueryResult)]
#[serde(transparent)]
pub struct CpcvFoldArtifacts(Vec<CpcvFoldArtifact>);

impl CpcvFoldArtifacts {
    pub fn try_new(mut artifacts: Vec<CpcvFoldArtifact>) -> Result<Self, CpcvEvidenceError> {
        if artifacts.is_empty() {
            return Err(CpcvEvidenceError::EmptyFoldArtifacts);
        }
        artifacts.sort();
        let value = Self(artifacts);
        value.validate()?;
        Ok(value)
    }

    pub fn validate(&self) -> Result<(), CpcvEvidenceError> {
        if self.0.is_empty() {
            return Err(CpcvEvidenceError::EmptyFoldArtifacts);
        }
        for artifact in &self.0 {
            if artifact.training_group_count == 0 {
                return Err(CpcvEvidenceError::EmptyTrainingGroups {
                    role: artifact.role,
                });
            }
        }
        if self.0.windows(2).any(|window| window[0] >= window[1]) {
            return Err(CpcvEvidenceError::NonCanonicalFoldArtifacts);
        }
        if let Some(window) = self.0.windows(2).find(|window| {
            window[0].role == window[1].role
                && window[0].training_groups_hash == window[1].training_groups_hash
        }) {
            return Err(CpcvEvidenceError::DuplicateFoldArtifact {
                role: window[0].role,
            });
        }
        Ok(())
    }

    #[must_use]
    pub fn validation_count(&self) -> usize {
        self.0
            .iter()
            .filter(|artifact| artifact.role == CpcvFoldRole::Validation)
            .count()
    }

    #[must_use]
    pub fn trial_count(&self) -> usize {
        self.0
            .iter()
            .filter(|artifact| matches!(artifact.role, CpcvFoldRole::Trial { .. }))
            .count()
    }
}

impl Deref for CpcvFoldArtifacts {
    type Target = [CpcvFoldArtifact];

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl IntoIterator for CpcvFoldArtifacts {
    type IntoIter = IntoIter<CpcvFoldArtifact>;
    type Item = CpcvFoldArtifact;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum CpcvEvidenceError {
    #[error("unsupported CPCV evidence schema version {actual}; expected {expected}")]
    UnsupportedVersion { expected: u32, actual: u32 },
    #[error("CPCV fold-artifact ledger must not be empty")]
    EmptyFoldArtifacts,
    #[error("CPCV {role:?} artifact has no training groups")]
    EmptyTrainingGroups { role: CpcvFoldRole },
    #[error("CPCV fold-artifact ledger must be strictly sorted and duplicate-free")]
    NonCanonicalFoldArtifacts,
    #[error("CPCV fold-artifact ledger repeats semantic key {role:?}")]
    DuplicateFoldArtifact { role: CpcvFoldRole },
}

const fn validate_evidence_version(actual: u32) -> Result<(), CpcvEvidenceError> {
    if actual != CPCV_EVIDENCE_SCHEMA_VERSION {
        return Err(CpcvEvidenceError::UnsupportedVersion {
            expected: CPCV_EVIDENCE_SCHEMA_VERSION,
            actual,
        });
    }
    Ok(())
}

/// Expected-versus-realized agreement summary.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, FromJsonQueryResult)]
#[serde(deny_unknown_fields)]
pub struct ExpectedVsRealized {
    pub mean_expected_bps: Decimal,
    pub mean_realized_bps: Decimal,
    pub correlation: Decimal,
    pub bias_bps: Decimal,
}

/// One category's backtest metrics.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CategoryMetric {
    pub category: MarketCategory,
    pub sample_count: u64,
    pub rank_ic: Decimal,
    pub hit_rate: Probability,
    pub mean_realized_bps: Decimal,
}

/// Fixed-schema category metrics persisted as one JSONB value object.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, FromJsonQueryResult)]
#[serde(transparent)]
pub struct CategoryMetrics(Vec<CategoryMetric>);

impl From<Vec<CategoryMetric>> for CategoryMetrics {
    fn from(values: Vec<CategoryMetric>) -> Self {
        Self(values)
    }
}

impl Deref for CategoryMetrics {
    type Target = [CategoryMetric];

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for CategoryMetrics {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl IntoIterator for CategoryMetrics {
    type IntoIter = IntoIter<CategoryMetric>;
    type Item = CategoryMetric;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

/// One cumulative realized-PnL point.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PnlCurvePoint {
    pub decision_at: DateTime<Utc>,
    pub cumulative_realized_pnl_usd: Decimal,
}

/// Portfolio-level `PnL` simulation summary.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, FromJsonQueryResult)]
#[serde(deny_unknown_fields)]
pub struct PnlSimulation {
    pub total_allocated_usd: Decimal,
    pub realized_pnl_usd: Decimal,
    pub gross_return: Decimal,
    pub pnl_curve: Vec<PnlCurvePoint>,
}

/// Canonical hash preimage of every immutable backtest-report semantic field.
///
/// Database-only routing fields (`model_run_id`, `parquet_uri`) are deliberately
/// absent. The persisted report hash is the exact compute artifact identity and
/// therefore matches the research producer byte-for-byte.
#[derive(Debug, Serialize)]
pub struct BacktestReportHashInput<'a> {
    pub backtest_report_id: &'a BacktestReportId,
    pub model_version_id: &'a ModelVersionId,
    pub dataset_id: &'a TrainingDatasetId,
    pub decision_policy_snapshot_id: &'a DecisionPolicySnapshotId,
    pub window_start: DateTime<Utc>,
    pub window_end: DateTime<Utc>,
    pub coverage: Decimal,
    pub sample_count: u64,
    pub missing_feature_count: u64,
    pub rank_ic: Decimal,
    pub sharpe: Decimal,
    pub hit_rate: Probability,
    pub expected_vs_realized: &'a ExpectedVsRealized,
    pub max_drawdown: Decimal,
    pub turnover: Decimal,
    pub liquidity_feasibility: Probability,
    pub category_breakdown: &'a [CategoryMetric],
    pub tail_loss: Decimal,
    pub report_pnl_simulation: &'a PnlSimulation,
}

impl BacktestReportHashInput<'_> {
    /// Hash the exact canonical JSON projection used by every producer and
    /// persistence verifier.
    pub fn content_hash(&self) -> Result<ContentHash, CanonicalDigestError> {
        CanonicalDigest::content_hash_json(self)
    }
}

/// Sharpe distribution across reconstructed CPCV paths.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, FromJsonQueryResult)]
#[serde(deny_unknown_fields)]
pub struct SharpeDistribution {
    pub min: Decimal,
    pub p25: Decimal,
    pub median: Decimal,
    pub p75: Decimal,
    pub max: Decimal,
    pub median_max_drawdown: Option<Decimal>,
    pub median_tail_loss: Option<Decimal>,
    pub baseline_uplift: Option<Decimal>,
}

/// One complete full-timeline reconstructed CPCV path.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BacktestPath {
    pub path_index: u32,
    pub group_returns: Vec<Decimal>,
    pub sharpe: Decimal,
    pub rank_ic: Decimal,
    pub max_drawdown: Decimal,
    pub tail_loss: Decimal,
}

/// Complete reconstructed CPCV paths persisted atomically.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, FromJsonQueryResult)]
#[serde(transparent)]
pub struct BacktestPaths(Vec<BacktestPath>);

impl From<Vec<BacktestPath>> for BacktestPaths {
    fn from(paths: Vec<BacktestPath>) -> Self {
        Self(paths)
    }
}

impl Deref for BacktestPaths {
    type Target = [BacktestPath];

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl IntoIterator for BacktestPaths {
    type IntoIter = IntoIter<BacktestPath>;
    type Item = BacktestPath;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

/// One category's candidate-versus-baseline rank-IC delta.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CategoryRankIcDelta {
    pub category: MarketCategory,
    pub baseline_rank_ic: Decimal,
    pub candidate_rank_ic: Decimal,
    pub rank_ic_delta: Decimal,
}

/// Typed category comparison collection.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, FromJsonQueryResult)]
#[serde(transparent)]
pub struct CategoryRankIcDeltas(Vec<CategoryRankIcDelta>);

impl From<Vec<CategoryRankIcDelta>> for CategoryRankIcDeltas {
    fn from(values: Vec<CategoryRankIcDelta>) -> Self {
        Self(values)
    }
}

impl Deref for CategoryRankIcDeltas {
    type Target = [CategoryRankIcDelta];

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl IntoIterator for CategoryRankIcDeltas {
    type IntoIter = IntoIter<CategoryRankIcDelta>;
    type Item = CategoryRankIcDelta;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

/// Canonical hash preimage of one candidate-versus-baseline comparison.
#[derive(Debug, Serialize)]
pub struct ModelComparisonHashInput<'a> {
    pub baseline_model_version_id: &'a ModelVersionId,
    pub candidate_model_version_id: &'a ModelVersionId,
    pub baseline_report_hash: &'a ContentHash,
    pub candidate_report_hash: &'a ContentHash,
    pub rank_ic_delta: Decimal,
    pub hit_rate_delta: Decimal,
    pub realized_pnl_delta: Decimal,
    pub score_correlation: Decimal,
    pub side_disagreement_rate: Decimal,
    pub common_samples: u64,
    pub category_breakdown_diff: &'a [CategoryRankIcDelta],
}

impl ModelComparisonHashInput<'_> {
    /// Hash the exact canonical JSON projection used by every producer and
    /// persistence verifier.
    pub fn content_hash(&self) -> Result<ContentHash, CanonicalDigestError> {
        CanonicalDigest::content_hash_json(self)
    }
}

#[cfg(test)]
mod tests {
    use rust_decimal_macros::dec;
    use serde_json::json;

    use super::{CpcvFoldArtifact, CpcvFoldArtifacts, CpcvFoldRole, ExpectedVsRealized};
    use crate::types::ContentHash;

    fn hash(seed: char) -> ContentHash {
        ContentHash::parse(&format!("blake3:{}", seed.to_string().repeat(64))).expect("hash")
    }

    #[test]
    fn fixed_rejects_unknown_missing() {
        let valid = json!({
            "mean_expected_bps": "1",
            "mean_realized_bps": "2",
            "correlation": "0.5",
            "bias_bps": "-1"
        });
        let decoded: ExpectedVsRealized =
            serde_json::from_value(valid.clone()).expect("fixed document");
        assert_eq!(decoded.correlation, dec!(0.5));

        let mut unknown = valid.clone();
        unknown["extra"] = json!(true);
        assert!(serde_json::from_value::<ExpectedVsRealized>(unknown).is_err());

        let mut missing = valid;
        missing.as_object_mut().expect("object").remove("bias_bps");
        assert!(serde_json::from_value::<ExpectedVsRealized>(missing).is_err());
    }

    #[test]
    fn ledger_rejects_duplicate_key() {
        let artifact = |model_artifact_hash| CpcvFoldArtifact {
            role: CpcvFoldRole::Validation,
            training_groups_hash: hash('a'),
            training_group_count: 2,
            model_artifact_hash,
            serving_contract_hash: hash('c'),
            model_payload_hash: hash('d'),
        };
        assert!(
            CpcvFoldArtifacts::try_new(vec![artifact(hash('e')), artifact(hash('f'))]).is_err()
        );
    }
}
