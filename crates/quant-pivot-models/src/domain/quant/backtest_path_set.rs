//! Combinatorial Purged Cross-Validation (CPCV) path-set ledger persistence
//! DTOs.

use chrono::{DateTime, Utc};
use quant_pivot_error::hashing::CanonicalDigestError;
use rust_decimal::Decimal;
use sea_orm::{DeriveIntoActiveModel, DerivePartialModel};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{
    entities::quant_backtest_path_set,
    hashing::CanonicalDigest,
    types::{
        BacktestPathSetId, ContentHash, DecisionPolicySnapshotId, ModelRunId, ModelVersionId,
        TrainingDatasetId,
        backtest::{
            BacktestPaths, CpcvEvidenceError, CpcvFoldArtifacts, CpcvMethodologyBinding,
            CpcvPathSetSubject, SharpeDistribution,
        },
    },
};

/// Frozen CPCV + governed trial-grid validation result row.
#[derive(Debug, Clone, Serialize, Deserialize, DerivePartialModel)]
#[sea_orm(entity = "crate::entities::quant_backtest_path_set::Entity")]
pub struct BacktestPathSetInfo {
    pub path_set_id: BacktestPathSetId,
    pub model_version_id: ModelVersionId,
    pub model_run_id: ModelRunId,
    pub training_dataset_id: TrainingDatasetId,
    pub decision_policy_snapshot_id: DecisionPolicySnapshotId,
    pub window_start: DateTime<Utc>,
    pub window_end: DateTime<Utc>,
    /// Exact deeply verified serving, Dataset, and policy preimages.
    pub subject: CpcvPathSetSubject,
    /// Frozen CPCV/trial-grid, portfolio, replay, and fold-calibration policy.
    pub methodology: CpcvMethodologyBinding,
    /// Canonically ordered artifacts produced by every validation fold and
    /// governed full-window trial.
    pub fold_artifacts: CpcvFoldArtifacts,
    /// `phi(N, k)` — the number of reconstructed complete paths.
    pub path_count: i64,
    /// `C(N, k)` — the number of purge/embargo/train/evaluate folds run.
    pub combination_count: i64,
    /// Median of the paths' own rank IC — the hard `RankIc` gate's
    /// data source.
    pub median_rank_ic: Decimal,
    /// `SharpeDistribution { min, p25, median, p75, max }`.
    pub sharpe_distribution: SharpeDistribution,
    /// `Vec<BacktestPath>` (`path_index`, `group_returns`, `sharpe`, `rank_ic`,
    /// `max_drawdown`, `tail_loss`) — the full reconstructed path detail.
    pub paths: BacktestPaths,
    /// The Deflated Sharpe Ratio (`PSR` evaluated at the trial-grid-corrected
    /// benchmark) — in `[0, 1]`, the hard `DeflatedSharpe` gate's
    /// data source.
    pub deflated_sharpe: Decimal,
    /// The expected-maximum-Sharpe benchmark (`SR*`) `deflated_sharpe` was
    /// evaluated against (audit visibility).
    pub dsr_benchmark_sharpe: Decimal,
    /// Probability of Backtest Overfitting — the hard `Pbo` gate's
    /// data source.
    pub pbo: Decimal,
    /// Minimum Track Record Length, in seconds — `None` when the
    /// representative path's Sharpe is non-positive (soft/informational gate).
    pub min_track_record_length_secs: Option<i64>,
    /// DSR multiple-testing N (= `trial_grid_count`). Same population as V.
    pub trial_count: i64,
    /// Governed trial-grid configurations evaluated for CSCV/PBO + DSR N/V.
    pub trial_grid_count: i64,
    /// Audit-only: production `coordinate_search` effective trials (not in DSR N).
    pub coord_search_effective_n: i64,
    /// Content-addressed digest of the persisted path-set payload.
    pub path_set_hash: ContentHash,
    pub created_at: DateTime<Utc>,
}

info_from_model!(
    BacktestPathSetInfo,
    quant_backtest_path_set::Model,
    {
        path_set_id,
        model_version_id,
        model_run_id,
        training_dataset_id,
        decision_policy_snapshot_id,
        window_start,
        window_end,
        subject,
        methodology,
        fold_artifacts,
        path_count,
        combination_count,
        median_rank_ic,
        sharpe_distribution,
        paths,
        deflated_sharpe,
        dsr_benchmark_sharpe,
        pbo,
        min_track_record_length_secs,
        trial_count,
        trial_grid_count,
        coord_search_effective_n,
        path_set_hash,
        created_at,
    }
);

impl BacktestPathSetInfo {
    pub fn expected_hash(&self) -> Result<ContentHash, BacktestPathSetError> {
        validate_path_set_payload(PathSetValidation {
            window_start: self.window_start,
            window_end: self.window_end,
            path_count: self.path_count,
            combination_count: self.combination_count,
            paths: &self.paths,
            fold_artifacts: &self.fold_artifacts,
            trial_count: self.trial_count,
            trial_grid_count: self.trial_grid_count,
            min_track_record_length_secs: self.min_track_record_length_secs,
        })?;
        self.subject.validate()?;
        self.methodology.validate()?;
        expected_path_set_hash(&BacktestPathSetHashInput::from_info(self))
    }

    pub fn verify_hash(&self) -> Result<(), BacktestPathSetError> {
        let expected = self.expected_hash()?;
        if expected != self.path_set_hash {
            return Err(BacktestPathSetError::HashMismatch {
                expected,
                actual: self.path_set_hash,
            });
        }
        Ok(())
    }
}

/// Caller input for a new immutable path set. The canonical hash is absent by
/// design and can only be derived by [`NewBacktestPathSet::try_seal`].
#[derive(Debug, Clone)]
pub struct NewBacktestPathSetInput {
    pub path_set_id: BacktestPathSetId,
    pub model_version_id: ModelVersionId,
    pub model_run_id: ModelRunId,
    pub training_dataset_id: TrainingDatasetId,
    pub decision_policy_snapshot_id: DecisionPolicySnapshotId,
    pub window_start: DateTime<Utc>,
    pub window_end: DateTime<Utc>,
    pub subject: CpcvPathSetSubject,
    pub methodology: CpcvMethodologyBinding,
    pub fold_artifacts: CpcvFoldArtifacts,
    pub path_count: i64,
    pub combination_count: i64,
    pub median_rank_ic: Decimal,
    pub sharpe_distribution: SharpeDistribution,
    pub paths: BacktestPaths,
    pub deflated_sharpe: Decimal,
    pub dsr_benchmark_sharpe: Decimal,
    pub pbo: Decimal,
    pub min_track_record_length_secs: Option<i64>,
    pub trial_count: i64,
    pub trial_grid_count: i64,
    pub coord_search_effective_n: i64,
}

/// Insert payload for `quant_backtest_path_set`.
///
/// Covers every `ActiveModel` column except the DB-managed `created_at`.
#[derive(Debug, Clone, Serialize, Deserialize, DeriveIntoActiveModel)]
#[sea_orm(active_model = "crate::entities::quant_backtest_path_set::ActiveModel")]
pub struct NewBacktestPathSet {
    pub path_set_id: BacktestPathSetId,
    pub model_version_id: ModelVersionId,
    pub model_run_id: ModelRunId,
    pub training_dataset_id: TrainingDatasetId,
    pub decision_policy_snapshot_id: DecisionPolicySnapshotId,
    pub window_start: DateTime<Utc>,
    pub window_end: DateTime<Utc>,
    pub subject: CpcvPathSetSubject,
    pub methodology: CpcvMethodologyBinding,
    pub fold_artifacts: CpcvFoldArtifacts,
    pub path_count: i64,
    pub combination_count: i64,
    pub median_rank_ic: Decimal,
    pub sharpe_distribution: SharpeDistribution,
    pub paths: BacktestPaths,
    pub deflated_sharpe: Decimal,
    pub dsr_benchmark_sharpe: Decimal,
    pub pbo: Decimal,
    pub min_track_record_length_secs: Option<i64>,
    pub trial_count: i64,
    pub trial_grid_count: i64,
    pub coord_search_effective_n: i64,
    path_set_hash: ContentHash,
}

impl NewBacktestPathSet {
    pub fn try_seal(input: NewBacktestPathSetInput) -> Result<Self, BacktestPathSetError> {
        validate_path_set_payload(PathSetValidation {
            window_start: input.window_start,
            window_end: input.window_end,
            path_count: input.path_count,
            combination_count: input.combination_count,
            paths: &input.paths,
            fold_artifacts: &input.fold_artifacts,
            trial_count: input.trial_count,
            trial_grid_count: input.trial_grid_count,
            min_track_record_length_secs: input.min_track_record_length_secs,
        })?;
        input.subject.validate()?;
        input.methodology.validate()?;
        let path_set_hash = expected_path_set_hash(&BacktestPathSetHashInput::from_new(&input))?;
        Ok(Self {
            path_set_id: input.path_set_id,
            model_version_id: input.model_version_id,
            model_run_id: input.model_run_id,
            training_dataset_id: input.training_dataset_id,
            decision_policy_snapshot_id: input.decision_policy_snapshot_id,
            window_start: input.window_start,
            window_end: input.window_end,
            subject: input.subject,
            methodology: input.methodology,
            fold_artifacts: input.fold_artifacts,
            path_count: input.path_count,
            combination_count: input.combination_count,
            median_rank_ic: input.median_rank_ic,
            sharpe_distribution: input.sharpe_distribution,
            paths: input.paths,
            deflated_sharpe: input.deflated_sharpe,
            dsr_benchmark_sharpe: input.dsr_benchmark_sharpe,
            pbo: input.pbo,
            min_track_record_length_secs: input.min_track_record_length_secs,
            trial_count: input.trial_count,
            trial_grid_count: input.trial_grid_count,
            coord_search_effective_n: input.coord_search_effective_n,
            path_set_hash,
        })
    }

    pub fn verify_hash(&self) -> Result<(), BacktestPathSetError> {
        validate_path_set_payload(PathSetValidation {
            window_start: self.window_start,
            window_end: self.window_end,
            path_count: self.path_count,
            combination_count: self.combination_count,
            paths: &self.paths,
            fold_artifacts: &self.fold_artifacts,
            trial_count: self.trial_count,
            trial_grid_count: self.trial_grid_count,
            min_track_record_length_secs: self.min_track_record_length_secs,
        })?;
        self.subject.validate()?;
        self.methodology.validate()?;
        let expected = expected_path_set_hash(&BacktestPathSetHashInput::from(self))?;
        if expected != self.path_set_hash {
            return Err(BacktestPathSetError::HashMismatch {
                expected,
                actual: self.path_set_hash,
            });
        }
        Ok(())
    }

    #[must_use]
    pub const fn path_set_hash(&self) -> ContentHash {
        self.path_set_hash
    }
}

#[derive(Serialize)]
struct BacktestPathSetHashInput<'a> {
    contract: &'static str,
    path_set_id: &'a BacktestPathSetId,
    model_version_id: &'a ModelVersionId,
    model_run_id: &'a ModelRunId,
    training_dataset_id: &'a TrainingDatasetId,
    decision_policy_snapshot_id: &'a DecisionPolicySnapshotId,
    window_start: DateTime<Utc>,
    window_end: DateTime<Utc>,
    subject: &'a CpcvPathSetSubject,
    methodology: &'a CpcvMethodologyBinding,
    fold_artifacts: &'a CpcvFoldArtifacts,
    path_count: i64,
    combination_count: i64,
    median_rank_ic: Decimal,
    sharpe_distribution: &'a SharpeDistribution,
    paths: &'a BacktestPaths,
    deflated_sharpe: Decimal,
    dsr_benchmark_sharpe: Decimal,
    pbo: Decimal,
    min_track_record_length_secs: Option<i64>,
    trial_count: i64,
    trial_grid_count: i64,
    coord_search_effective_n: i64,
}

impl<'a> BacktestPathSetHashInput<'a> {
    const fn from_new(input: &'a NewBacktestPathSetInput) -> Self {
        Self {
            contract: "quant_backtest_path_set_v2",
            path_set_id: &input.path_set_id,
            model_version_id: &input.model_version_id,
            model_run_id: &input.model_run_id,
            training_dataset_id: &input.training_dataset_id,
            decision_policy_snapshot_id: &input.decision_policy_snapshot_id,
            window_start: input.window_start,
            window_end: input.window_end,
            subject: &input.subject,
            methodology: &input.methodology,
            fold_artifacts: &input.fold_artifacts,
            path_count: input.path_count,
            combination_count: input.combination_count,
            median_rank_ic: input.median_rank_ic,
            sharpe_distribution: &input.sharpe_distribution,
            paths: &input.paths,
            deflated_sharpe: input.deflated_sharpe,
            dsr_benchmark_sharpe: input.dsr_benchmark_sharpe,
            pbo: input.pbo,
            min_track_record_length_secs: input.min_track_record_length_secs,
            trial_count: input.trial_count,
            trial_grid_count: input.trial_grid_count,
            coord_search_effective_n: input.coord_search_effective_n,
        }
    }

    const fn from_info(info: &'a BacktestPathSetInfo) -> Self {
        Self {
            contract: "quant_backtest_path_set_v2",
            path_set_id: &info.path_set_id,
            model_version_id: &info.model_version_id,
            model_run_id: &info.model_run_id,
            training_dataset_id: &info.training_dataset_id,
            decision_policy_snapshot_id: &info.decision_policy_snapshot_id,
            window_start: info.window_start,
            window_end: info.window_end,
            subject: &info.subject,
            methodology: &info.methodology,
            fold_artifacts: &info.fold_artifacts,
            path_count: info.path_count,
            combination_count: info.combination_count,
            median_rank_ic: info.median_rank_ic,
            sharpe_distribution: &info.sharpe_distribution,
            paths: &info.paths,
            deflated_sharpe: info.deflated_sharpe,
            dsr_benchmark_sharpe: info.dsr_benchmark_sharpe,
            pbo: info.pbo,
            min_track_record_length_secs: info.min_track_record_length_secs,
            trial_count: info.trial_count,
            trial_grid_count: info.trial_grid_count,
            coord_search_effective_n: info.coord_search_effective_n,
        }
    }
}

impl<'a> From<&'a NewBacktestPathSet> for BacktestPathSetHashInput<'a> {
    fn from(input: &'a NewBacktestPathSet) -> Self {
        Self {
            contract: "quant_backtest_path_set_v2",
            path_set_id: &input.path_set_id,
            model_version_id: &input.model_version_id,
            model_run_id: &input.model_run_id,
            training_dataset_id: &input.training_dataset_id,
            decision_policy_snapshot_id: &input.decision_policy_snapshot_id,
            window_start: input.window_start,
            window_end: input.window_end,
            subject: &input.subject,
            methodology: &input.methodology,
            fold_artifacts: &input.fold_artifacts,
            path_count: input.path_count,
            combination_count: input.combination_count,
            median_rank_ic: input.median_rank_ic,
            sharpe_distribution: &input.sharpe_distribution,
            paths: &input.paths,
            deflated_sharpe: input.deflated_sharpe,
            dsr_benchmark_sharpe: input.dsr_benchmark_sharpe,
            pbo: input.pbo,
            min_track_record_length_secs: input.min_track_record_length_secs,
            trial_count: input.trial_count,
            trial_grid_count: input.trial_grid_count,
            coord_search_effective_n: input.coord_search_effective_n,
        }
    }
}

fn expected_path_set_hash(
    input: &BacktestPathSetHashInput<'_>,
) -> Result<ContentHash, BacktestPathSetError> {
    Ok(CanonicalDigest::content_hash_json(input)?)
}

#[derive(Clone, Copy)]
struct PathSetValidation<'a> {
    window_start: DateTime<Utc>,
    window_end: DateTime<Utc>,
    path_count: i64,
    combination_count: i64,
    paths: &'a BacktestPaths,
    fold_artifacts: &'a CpcvFoldArtifacts,
    trial_count: i64,
    trial_grid_count: i64,
    min_track_record_length_secs: Option<i64>,
}

fn validate_path_set_payload(input: PathSetValidation<'_>) -> Result<(), BacktestPathSetError> {
    input.fold_artifacts.validate()?;
    if input.window_start >= input.window_end {
        return Err(BacktestPathSetError::InvalidShape {
            detail: "window_start must be earlier than window_end".to_owned(),
        });
    }
    let path_count =
        usize::try_from(input.path_count).map_err(|error| BacktestPathSetError::InvalidShape {
            detail: format!("path_count must fit usize: {error}"),
        })?;
    let combination_count = usize::try_from(input.combination_count).map_err(|error| {
        BacktestPathSetError::InvalidShape {
            detail: format!("combination_count must fit usize: {error}"),
        }
    })?;
    let trial_count =
        usize::try_from(input.trial_count).map_err(|error| BacktestPathSetError::InvalidShape {
            detail: format!("trial_count must fit usize: {error}"),
        })?;
    let trial_grid_count = usize::try_from(input.trial_grid_count).map_err(|error| {
        BacktestPathSetError::InvalidShape {
            detail: format!("trial_grid_count must fit usize: {error}"),
        }
    })?;
    if path_count == 0
        || combination_count == 0
        || trial_count == 0
        || trial_count != trial_grid_count
        || path_count != input.paths.len()
        || combination_count != input.fold_artifacts.validation_count()
        || trial_grid_count != input.fold_artifacts.trial_count()
    {
        return Err(BacktestPathSetError::InvalidShape {
            detail: format!(
                "counts disagree: paths={path_count}/{}, combinations={combination_count}/{}, \
                 trials={trial_count}/{trial_grid_count}/{}",
                input.paths.len(),
                input.fold_artifacts.validation_count(),
                input.fold_artifacts.trial_count(),
            ),
        });
    }
    for (expected, path) in input.paths.iter().enumerate() {
        let expected =
            u32::try_from(expected).map_err(|error| BacktestPathSetError::InvalidShape {
                detail: format!("path index does not fit u32: {error}"),
            })?;
        if path.path_index != expected || path.group_returns.is_empty() {
            return Err(BacktestPathSetError::InvalidShape {
                detail: format!(
                    "path {} is non-canonical or has no group returns",
                    path.path_index
                ),
            });
        }
    }
    if input
        .min_track_record_length_secs
        .is_some_and(|value| value < 0)
    {
        return Err(BacktestPathSetError::InvalidShape {
            detail: "min_track_record_length_secs must not be negative".to_owned(),
        });
    }
    Ok(())
}

#[derive(Debug, Error)]
pub enum BacktestPathSetError {
    #[error("invalid CPCV path-set payload: {detail}")]
    InvalidShape { detail: String },
    #[error("CPCV path-set hash mismatch: expected {expected}, got {actual}")]
    HashMismatch {
        expected: ContentHash,
        actual: ContentHash,
    },
    #[error(transparent)]
    Evidence(#[from] CpcvEvidenceError),
    #[error(transparent)]
    Hash(#[from] CanonicalDigestError),
}

#[cfg(test)]
mod tests {
    use chrono::{Duration, Utc};
    use rust_decimal_macros::dec;

    use super::{NewBacktestPathSet, NewBacktestPathSetInput};
    use crate::types::{
        BacktestPathSetId, ContentHash, DecisionPolicySnapshotId, ModelRunId, ModelVersionId,
        TrainingDatasetId,
        backtest::{
            BacktestPath, CpcvFoldArtifact, CpcvFoldArtifacts, CpcvFoldCalibrationPolicy,
            CpcvFoldRole, CpcvMethodologyBinding, CpcvPathSetSubject, SharpeDistribution,
        },
    };

    fn hash(seed: char) -> ContentHash {
        ContentHash::parse(&format!("blake3:{}", seed.to_string().repeat(64))).expect("hash")
    }

    impl NewBacktestPathSet {
        fn test_fixture() -> Self {
            let window_start = Utc::now() - Duration::hours(1);
            Self::try_seal(NewBacktestPathSetInput {
                path_set_id: BacktestPathSetId::from_v7(),
                model_version_id: ModelVersionId::from_v7(),
                model_run_id: ModelRunId::from_v7(),
                training_dataset_id: TrainingDatasetId::from_v7(),
                decision_policy_snapshot_id: DecisionPolicySnapshotId::from_v7(),
                window_start,
                window_end: window_start + Duration::minutes(30),
                subject: CpcvPathSetSubject::new(
                    hash('1'),
                    hash('2'),
                    hash('3'),
                    hash('4'),
                    hash('5'),
                    hash('6'),
                ),
                methodology: CpcvMethodologyBinding::new(
                    hash('7'),
                    hash('8'),
                    hash('9'),
                    CpcvFoldCalibrationPolicy::SubjectHeuristic {
                        return_model_hash: hash('a'),
                    },
                ),
                fold_artifacts: CpcvFoldArtifacts::try_new(vec![
                    CpcvFoldArtifact {
                        role: CpcvFoldRole::Validation,
                        training_groups_hash: hash('b'),
                        training_group_count: 2,
                        model_artifact_hash: hash('c'),
                        serving_contract_hash: hash('d'),
                        model_payload_hash: hash('e'),
                    },
                    CpcvFoldArtifact {
                        role: CpcvFoldRole::Trial { trial_id: 0 },
                        training_groups_hash: hash('f'),
                        training_group_count: 3,
                        model_artifact_hash: hash('1'),
                        serving_contract_hash: hash('2'),
                        model_payload_hash: hash('3'),
                    },
                ])
                .expect("fold artifacts"),
                path_count: 1,
                combination_count: 1,
                median_rank_ic: dec!(0.1),
                sharpe_distribution: SharpeDistribution {
                    min: dec!(0.2),
                    p25: dec!(0.3),
                    median: dec!(0.4),
                    p75: dec!(0.5),
                    max: dec!(0.6),
                    median_max_drawdown: Some(dec!(0.1)),
                    median_tail_loss: Some(dec!(-0.01)),
                    baseline_uplift: Some(dec!(0.02)),
                },
                paths: vec![BacktestPath {
                    path_index: 0,
                    group_returns: vec![dec!(0.01), dec!(-0.005)],
                    sharpe: dec!(0.4),
                    rank_ic: dec!(0.1),
                    max_drawdown: dec!(0.005),
                    tail_loss: dec!(-0.005),
                }]
                .into(),
                deflated_sharpe: dec!(0.95),
                dsr_benchmark_sharpe: dec!(0.1),
                pbo: dec!(0.2),
                min_track_record_length_secs: Some(86_400),
                trial_count: 1,
                trial_grid_count: 1,
                coord_search_effective_n: 2,
            })
            .expect("sealed path set")
        }
    }

    #[test]
    fn hash_rejects_payload_tamper() {
        let sealed = NewBacktestPathSet::test_fixture();
        sealed.verify_hash().expect("sealed hash");
        let mut value = serde_json::to_value(sealed).expect("serialize");
        value["pbo"] = serde_json::json!("0.9");
        let tampered: NewBacktestPathSet = serde_json::from_value(value).expect("deserialize");
        assert!(tampered.verify_hash().is_err());
    }
}
