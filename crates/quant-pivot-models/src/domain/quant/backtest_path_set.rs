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
            CpcvPathSetSubject, CscvSelectionEvidence, SharpeDistribution,
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
    /// Canonically ordered artifacts produced by every subject and governed
    /// trial purge/embargo validation fold.
    pub fold_artifacts: CpcvFoldArtifacts,
    /// `phi(N, k)` — the number of reconstructed complete paths.
    pub path_count: i64,
    /// `C(N, k)` — the number of purge/embargo/train/evaluate folds run.
    pub combination_count: i64,
    /// Median of the paths' target-aligned rank IC — the hard `TargetRankIc` gate's
    /// data source.
    pub median_target_rank_ic: Decimal,
    /// Sharpe and risk-evidence distribution across complete CPCV paths.
    pub sharpe_distribution: SharpeDistribution,
    /// `Vec<BacktestPath>` (`path_index`, `group_returns`, `sharpe`, `target_rank_ic`,
    /// `max_drawdown`, `tail_loss`, `turnover`) — the full reconstructed path detail.
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
    /// Recomputable equal-block CSCV ledger and complementary selection-bias
    /// diagnostics that produced `pbo` and the DSR trial dispersion.
    pub cscv_selection_evidence: CscvSelectionEvidence,
    /// Minimum Track Record Length, in seconds — `None` when the
    /// representative path's Sharpe is non-positive (soft/informational gate).
    pub min_track_record_length_secs: Option<i64>,
    /// Conservative DSR multiple-testing N. This is the ceiling of the
    /// persisted implied independent count when every pairwise correlation is
    /// identified, or the complete raw grid count when a non-duplicate
    /// no-trade trial makes Pearson correlation undefined.
    pub dsr_conservative_independent_trial_count: i64,
    /// Raw governed trial-grid configurations evaluated for CSCV/PBO and the
    /// trial-return population from which DSR N/V evidence is derived.
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
        median_target_rank_ic,
        sharpe_distribution,
        paths,
        deflated_sharpe,
        dsr_benchmark_sharpe,
        pbo,
        cscv_selection_evidence,
        min_track_record_length_secs,
        dsr_conservative_independent_trial_count,
        trial_grid_count,
        coord_search_effective_n,
        path_set_hash,
        created_at,
    }
);

impl BacktestPathSetInfo {
    pub fn expected_hash(&self) -> Result<ContentHash, BacktestPathSetError> {
        PathSetValidation {
            window_start: self.window_start,
            window_end: self.window_end,
            path_count: self.path_count,
            combination_count: self.combination_count,
            paths: &self.paths,
            methodology: &self.methodology,
            fold_artifacts: &self.fold_artifacts,
            dsr_conservative_independent_trial_count: self.dsr_conservative_independent_trial_count,
            trial_grid_count: self.trial_grid_count,
            pbo: self.pbo,
            cscv_selection_evidence: &self.cscv_selection_evidence,
            min_track_record_length_secs: self.min_track_record_length_secs,
        }
        .validate()?;
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
    pub median_target_rank_ic: Decimal,
    pub sharpe_distribution: SharpeDistribution,
    pub paths: BacktestPaths,
    pub deflated_sharpe: Decimal,
    pub dsr_benchmark_sharpe: Decimal,
    pub pbo: Decimal,
    pub cscv_selection_evidence: CscvSelectionEvidence,
    pub min_track_record_length_secs: Option<i64>,
    pub dsr_conservative_independent_trial_count: i64,
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
    pub median_target_rank_ic: Decimal,
    pub sharpe_distribution: SharpeDistribution,
    pub paths: BacktestPaths,
    pub deflated_sharpe: Decimal,
    pub dsr_benchmark_sharpe: Decimal,
    pub pbo: Decimal,
    pub cscv_selection_evidence: CscvSelectionEvidence,
    pub min_track_record_length_secs: Option<i64>,
    pub dsr_conservative_independent_trial_count: i64,
    pub trial_grid_count: i64,
    pub coord_search_effective_n: i64,
    path_set_hash: ContentHash,
}

impl NewBacktestPathSet {
    pub fn try_seal(input: NewBacktestPathSetInput) -> Result<Self, BacktestPathSetError> {
        PathSetValidation {
            window_start: input.window_start,
            window_end: input.window_end,
            path_count: input.path_count,
            combination_count: input.combination_count,
            paths: &input.paths,
            methodology: &input.methodology,
            fold_artifacts: &input.fold_artifacts,
            dsr_conservative_independent_trial_count: input
                .dsr_conservative_independent_trial_count,
            trial_grid_count: input.trial_grid_count,
            pbo: input.pbo,
            cscv_selection_evidence: &input.cscv_selection_evidence,
            min_track_record_length_secs: input.min_track_record_length_secs,
        }
        .validate()?;
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
            median_target_rank_ic: input.median_target_rank_ic,
            sharpe_distribution: input.sharpe_distribution,
            paths: input.paths,
            deflated_sharpe: input.deflated_sharpe,
            dsr_benchmark_sharpe: input.dsr_benchmark_sharpe,
            pbo: input.pbo,
            cscv_selection_evidence: input.cscv_selection_evidence,
            min_track_record_length_secs: input.min_track_record_length_secs,
            dsr_conservative_independent_trial_count: input
                .dsr_conservative_independent_trial_count,
            trial_grid_count: input.trial_grid_count,
            coord_search_effective_n: input.coord_search_effective_n,
            path_set_hash,
        })
    }

    pub fn verify_hash(&self) -> Result<(), BacktestPathSetError> {
        PathSetValidation {
            window_start: self.window_start,
            window_end: self.window_end,
            path_count: self.path_count,
            combination_count: self.combination_count,
            paths: &self.paths,
            methodology: &self.methodology,
            fold_artifacts: &self.fold_artifacts,
            dsr_conservative_independent_trial_count: self.dsr_conservative_independent_trial_count,
            trial_grid_count: self.trial_grid_count,
            pbo: self.pbo,
            cscv_selection_evidence: &self.cscv_selection_evidence,
            min_track_record_length_secs: self.min_track_record_length_secs,
        }
        .validate()?;
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
    window_start_micros: i64,
    window_end_micros: i64,
    subject: &'a CpcvPathSetSubject,
    methodology: &'a CpcvMethodologyBinding,
    fold_artifacts: &'a CpcvFoldArtifacts,
    path_count: i64,
    combination_count: i64,
    median_target_rank_ic: Decimal,
    sharpe_distribution: &'a SharpeDistribution,
    paths: &'a BacktestPaths,
    deflated_sharpe: Decimal,
    dsr_benchmark_sharpe: Decimal,
    pbo: Decimal,
    cscv_selection_evidence: &'a CscvSelectionEvidence,
    min_track_record_length_secs: Option<i64>,
    dsr_conservative_independent_trial_count: i64,
    trial_grid_count: i64,
    coord_search_effective_n: i64,
}

impl<'a> BacktestPathSetHashInput<'a> {
    fn from_new(input: &'a NewBacktestPathSetInput) -> Self {
        Self {
            contract: "quant_backtest_path_set_v4",
            path_set_id: &input.path_set_id,
            model_version_id: &input.model_version_id,
            model_run_id: &input.model_run_id,
            training_dataset_id: &input.training_dataset_id,
            decision_policy_snapshot_id: &input.decision_policy_snapshot_id,
            window_start_micros: input.window_start.timestamp_micros(),
            window_end_micros: input.window_end.timestamp_micros(),
            subject: &input.subject,
            methodology: &input.methodology,
            fold_artifacts: &input.fold_artifacts,
            path_count: input.path_count,
            combination_count: input.combination_count,
            median_target_rank_ic: input.median_target_rank_ic.normalize(),
            sharpe_distribution: &input.sharpe_distribution,
            paths: &input.paths,
            deflated_sharpe: input.deflated_sharpe.normalize(),
            dsr_benchmark_sharpe: input.dsr_benchmark_sharpe.normalize(),
            pbo: input.pbo.normalize(),
            cscv_selection_evidence: &input.cscv_selection_evidence,
            min_track_record_length_secs: input.min_track_record_length_secs,
            dsr_conservative_independent_trial_count: input
                .dsr_conservative_independent_trial_count,
            trial_grid_count: input.trial_grid_count,
            coord_search_effective_n: input.coord_search_effective_n,
        }
    }

    fn from_info(info: &'a BacktestPathSetInfo) -> Self {
        Self {
            contract: "quant_backtest_path_set_v4",
            path_set_id: &info.path_set_id,
            model_version_id: &info.model_version_id,
            model_run_id: &info.model_run_id,
            training_dataset_id: &info.training_dataset_id,
            decision_policy_snapshot_id: &info.decision_policy_snapshot_id,
            window_start_micros: info.window_start.timestamp_micros(),
            window_end_micros: info.window_end.timestamp_micros(),
            subject: &info.subject,
            methodology: &info.methodology,
            fold_artifacts: &info.fold_artifacts,
            path_count: info.path_count,
            combination_count: info.combination_count,
            median_target_rank_ic: info.median_target_rank_ic.normalize(),
            sharpe_distribution: &info.sharpe_distribution,
            paths: &info.paths,
            deflated_sharpe: info.deflated_sharpe.normalize(),
            dsr_benchmark_sharpe: info.dsr_benchmark_sharpe.normalize(),
            pbo: info.pbo.normalize(),
            cscv_selection_evidence: &info.cscv_selection_evidence,
            min_track_record_length_secs: info.min_track_record_length_secs,
            dsr_conservative_independent_trial_count: info.dsr_conservative_independent_trial_count,
            trial_grid_count: info.trial_grid_count,
            coord_search_effective_n: info.coord_search_effective_n,
        }
    }
}

impl<'a> From<&'a NewBacktestPathSet> for BacktestPathSetHashInput<'a> {
    fn from(input: &'a NewBacktestPathSet) -> Self {
        Self {
            contract: "quant_backtest_path_set_v4",
            path_set_id: &input.path_set_id,
            model_version_id: &input.model_version_id,
            model_run_id: &input.model_run_id,
            training_dataset_id: &input.training_dataset_id,
            decision_policy_snapshot_id: &input.decision_policy_snapshot_id,
            window_start_micros: input.window_start.timestamp_micros(),
            window_end_micros: input.window_end.timestamp_micros(),
            subject: &input.subject,
            methodology: &input.methodology,
            fold_artifacts: &input.fold_artifacts,
            path_count: input.path_count,
            combination_count: input.combination_count,
            median_target_rank_ic: input.median_target_rank_ic.normalize(),
            sharpe_distribution: &input.sharpe_distribution,
            paths: &input.paths,
            deflated_sharpe: input.deflated_sharpe.normalize(),
            dsr_benchmark_sharpe: input.dsr_benchmark_sharpe.normalize(),
            pbo: input.pbo.normalize(),
            cscv_selection_evidence: &input.cscv_selection_evidence,
            min_track_record_length_secs: input.min_track_record_length_secs,
            dsr_conservative_independent_trial_count: input
                .dsr_conservative_independent_trial_count,
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
    methodology: &'a CpcvMethodologyBinding,
    fold_artifacts: &'a CpcvFoldArtifacts,
    dsr_conservative_independent_trial_count: i64,
    trial_grid_count: i64,
    pbo: Decimal,
    cscv_selection_evidence: &'a CscvSelectionEvidence,
    min_track_record_length_secs: Option<i64>,
}

impl PathSetValidation<'_> {
    fn validate(self) -> Result<(), BacktestPathSetError> {
        self.fold_artifacts
            .validate_for(&self.methodology.trial_path)?;
        self.cscv_selection_evidence
            .validate_for(&self.methodology.trial_grid)?;
        if self.window_start >= self.window_end {
            return Err(BacktestPathSetError::InvalidShape {
                detail: "window_start must be earlier than window_end".to_owned(),
            });
        }
        self.validate_counts()?;
        self.validate_paths()?;
        self.validate_axis()?;
        if self
            .min_track_record_length_secs
            .is_some_and(|value| value < 0)
        {
            return Err(BacktestPathSetError::InvalidShape {
                detail: "min_track_record_length_secs must not be negative".to_owned(),
            });
        }
        Ok(())
    }

    fn validate_counts(self) -> Result<(), BacktestPathSetError> {
        let path_count = usize::try_from(self.path_count).map_err(|error| {
            BacktestPathSetError::InvalidShape {
                detail: format!("path_count must fit usize: {error}"),
            }
        })?;
        let combination_count = usize::try_from(self.combination_count).map_err(|error| {
            BacktestPathSetError::InvalidShape {
                detail: format!("combination_count must fit usize: {error}"),
            }
        })?;
        let dsr_conservative_independent_trial_count =
            usize::try_from(self.dsr_conservative_independent_trial_count).map_err(|error| {
                BacktestPathSetError::InvalidShape {
                    detail: format!(
                        "dsr_conservative_independent_trial_count must fit usize: {error}"
                    ),
                }
            })?;
        let trial_grid_count = usize::try_from(self.trial_grid_count).map_err(|error| {
            BacktestPathSetError::InvalidShape {
                detail: format!("trial_grid_count must fit usize: {error}"),
            }
        })?;
        let dependence_trial_count = usize::try_from(
            self.cscv_selection_evidence
                .trial_dependence
                .conservative_independent_trial_count(),
        )
        .map_err(|error| BacktestPathSetError::InvalidShape {
            detail: format!("dependence-adjusted trial count must fit usize: {error}"),
        })?;
        if path_count == 0
            || combination_count == 0
            || dsr_conservative_independent_trial_count == 0
            || dsr_conservative_independent_trial_count != dependence_trial_count
            || path_count != self.paths.len()
            || combination_count != self.fold_artifacts.validation_count()
            || trial_grid_count != self.fold_artifacts.trial_count()
            || trial_grid_count != self.methodology.trial_grid.trials.len()
            || self.pbo != self.cscv_selection_evidence.pbo
        {
            return Err(BacktestPathSetError::InvalidShape {
                detail: format!(
                    "counts disagree: paths={path_count}/{}, combinations={combination_count}/{}, \
                     trials={dsr_conservative_independent_trial_count}/{trial_grid_count}/\
                     {dependence_trial_count}/{}",
                    self.paths.len(),
                    self.fold_artifacts.validation_count(),
                    self.fold_artifacts.trial_count(),
                ),
            });
        }
        Ok(())
    }

    fn validate_paths(self) -> Result<(), BacktestPathSetError> {
        for (expected, path) in self.paths.iter().enumerate() {
            let expected =
                u32::try_from(expected).map_err(|error| BacktestPathSetError::InvalidShape {
                    detail: format!("path index does not fit u32: {error}"),
                })?;
            if path.path_index != expected
                || path.group_returns.is_empty()
                || path.decision_times.len() != path.group_returns.len()
                || path.scenario_residuals.len() != path.group_returns.len()
                || path
                    .decision_times
                    .windows(2)
                    .any(|window| window[0] >= window[1])
                || path
                    .decision_times
                    .iter()
                    .any(|at| *at < self.window_start || *at >= self.window_end)
            {
                return Err(BacktestPathSetError::InvalidShape {
                    detail: format!(
                        "path {} has a non-canonical PIT clock or return series",
                        path.path_index
                    ),
                });
            }
        }
        Ok(())
    }

    fn validate_axis(self) -> Result<(), BacktestPathSetError> {
        let first_path = self
            .paths
            .first()
            .ok_or_else(|| BacktestPathSetError::InvalidShape {
                detail: "CPCV path set has no period axis".to_owned(),
            })?;
        if self
            .paths
            .iter()
            .any(|path| path.decision_times != first_path.decision_times)
        {
            return Err(BacktestPathSetError::InvalidShape {
                detail: "CPCV paths do not share one synchronous period axis".to_owned(),
            });
        }
        let period_axis_hash = CanonicalDigest::content_hash_typed(
            "quant-pivot/cscv-period-axis",
            1,
            &first_path.decision_times,
        )?;
        if period_axis_hash != self.cscv_selection_evidence.period_axis_hash
            || usize::try_from(self.cscv_selection_evidence.period_count).ok()
                != Some(first_path.decision_times.len())
        {
            return Err(BacktestPathSetError::InvalidShape {
                detail: "CSCV selection evidence does not bind the persisted CPCV period axis"
                    .to_owned(),
            });
        }
        let block_length =
            usize::try_from(self.cscv_selection_evidence.block_length).map_err(|error| {
                BacktestPathSetError::InvalidShape {
                    detail: format!("CSCV block length must fit usize: {error}"),
                }
            })?;
        for block in &self.cscv_selection_evidence.blocks {
            let block_index = usize::try_from(block.block_index).map_err(|error| {
                BacktestPathSetError::InvalidShape {
                    detail: format!("CSCV block index must fit usize: {error}"),
                }
            })?;
            let start = block_index.checked_mul(block_length).ok_or_else(|| {
                BacktestPathSetError::InvalidShape {
                    detail: "CSCV block start overflowed usize".to_owned(),
                }
            })?;
            let end = start.checked_add(block_length).ok_or_else(|| {
                BacktestPathSetError::InvalidShape {
                    detail: "CSCV block end overflowed usize".to_owned(),
                }
            })?;
            if first_path.decision_times.get(start) != Some(&block.first_period)
                || end
                    .checked_sub(1)
                    .and_then(|index| first_path.decision_times.get(index))
                    != Some(&block.last_period)
            {
                return Err(BacktestPathSetError::InvalidShape {
                    detail: format!(
                        "CSCV block {} does not bind the persisted CPCV period slice",
                        block.block_index
                    ),
                });
            }
        }
        Ok(())
    }
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
    use chrono::{DateTime, Duration, Utc};
    use rust_decimal::Decimal;
    use rust_decimal_macros::dec;

    use super::{NewBacktestPathSet, NewBacktestPathSetInput};
    use crate::{
        hashing::CanonicalDigest,
        types::{
            BacktestPathSetId, ContentHash, DecisionPolicySnapshotId, ModelRunId, ModelVersionId,
            TrainingDatasetId,
            backtest::{
                BacktestPath, CpcvEstimatorIdentity, CpcvFoldArtifact, CpcvFoldArtifacts,
                CpcvFoldCalibrationPolicy, CpcvFoldValidationRegime, CpcvMethodologyBinding,
                CpcvPathSetSubject, CpcvTrialPathBinding, CscvBlockEvidence,
                CscvCombinationEvidence, CscvDegradationEvidence, CscvDegradationUndefinedReason,
                CscvDominanceEvidence, CscvDominanceRelation, CscvDsrTrialCountEvidence,
                CscvSelectionEvidence, CscvTrialBlockStatistic, CscvTrialDependenceEvidence,
                CscvTrialDescriptor, CscvTrialEquivalenceClass, CscvTrialGridBinding,
                CscvTrialPairDependence, CscvTrialPairRelationship, CscvTrialPerformance,
                SharpeDistribution,
            },
        },
    };

    fn hash(seed: char) -> ContentHash {
        ContentHash::parse(&format!("blake3:{}", seed.to_string().repeat(64))).expect("hash")
    }

    impl CscvTrialGridBinding {
        fn test_fixture() -> Self {
            Self::try_new(
                4,
                vec![
                    CscvTrialDescriptor {
                        trial_id: 0,
                        label: "trial-0".to_owned(),
                        config_hash: hash('a'),
                    },
                    CscvTrialDescriptor {
                        trial_id: 1,
                        label: "trial-1".to_owned(),
                        config_hash: hash('b'),
                    },
                ],
            )
            .expect("trial grid")
        }
    }

    fn selection_evidence(periods: &[DateTime<Utc>]) -> CscvSelectionEvidence {
        let blocks = periods
            .iter()
            .enumerate()
            .map(|(index, period)| CscvBlockEvidence {
                block_index: u32::try_from(index).expect("block index"),
                first_period: *period,
                last_period: *period,
                trial_statistics: (0..2)
                    .map(|trial_id| CscvTrialBlockStatistic {
                        trial_id,
                        observation_count: 1,
                        return_sum: Decimal::ZERO,
                        squared_return_sum: Decimal::ZERO,
                    })
                    .collect(),
            })
            .collect();
        let combinations = [[0, 1], [0, 2], [1, 2], [0, 3], [1, 3], [2, 3]]
            .into_iter()
            .enumerate()
            .map(|(index, blocks)| CscvCombinationEvidence {
                combination_index: u32::try_from(index).expect("combination index"),
                in_sample_block_indices: blocks.to_vec(),
                champion_trial_id: 0,
                in_sample_sharpe: Decimal::ZERO,
                out_of_sample_sharpe: Decimal::ZERO,
                out_of_sample_rank_twice: 2,
                below_oos_median: false,
                out_of_sample_loss: false,
            })
            .collect();
        CscvSelectionEvidence {
            schema_version: CscvSelectionEvidence::schema_version(),
            period_count: 4,
            period_axis_hash: CanonicalDigest::content_hash_typed(
                "quant-pivot/cscv-period-axis",
                1,
                &periods,
            )
            .expect("period axis hash"),
            block_count: 4,
            block_length: 1,
            blocks,
            trial_performances: vec![
                CscvTrialPerformance {
                    trial_id: 0,
                    full_sample_sharpe: Decimal::ZERO,
                },
                CscvTrialPerformance {
                    trial_id: 1,
                    full_sample_sharpe: Decimal::ZERO,
                },
            ],
            behavioral_trial_sharpe_variance: Decimal::ZERO,
            trial_dependence: CscvTrialDependenceEvidence {
                raw_pair_count: 1,
                raw_pairs: vec![CscvTrialPairDependence {
                    left_trial_id: 0,
                    right_trial_id: 1,
                    observation_count: 4,
                    cross_product_sum: Decimal::ZERO,
                    relationship: CscvTrialPairRelationship::ExactDuplicate,
                }],
                equivalence_classes: vec![CscvTrialEquivalenceClass {
                    class_id: 0,
                    representative_trial_id: 0,
                    member_trial_ids: vec![0, 1],
                }],
                behavioral_pair_count: 0,
                trial_count_estimation: CscvDsrTrialCountEvidence::DirectBehavioralClassCount {
                    behavioral_trial_count: 1,
                    zero_variance_representative_trial_ids: vec![0],
                    conservative_independent_trial_count: 1,
                },
            },
            combinations,
            negative_logit_count: 0,
            pbo: Decimal::ZERO,
            out_of_sample_loss_count: 0,
            out_of_sample_loss_probability: Decimal::ZERO,
            performance_degradation: CscvDegradationEvidence::Undefined {
                reason: CscvDegradationUndefinedReason::ConstantInSampleChampionPerformance,
            },
            stochastic_dominance: CscvDominanceEvidence {
                evaluation_point_count: 1,
                first_order: CscvDominanceRelation::Equivalent,
                second_order: CscvDominanceRelation::Equivalent,
                max_selected_cdf_excess: Decimal::ZERO,
                min_integrated_cdf_advantage: Decimal::ZERO,
                max_integrated_cdf_advantage: Decimal::ZERO,
            },
        }
    }

    impl NewBacktestPathSet {
        fn test_fixture() -> Self {
            let window_start = Utc::now() - Duration::hours(1);
            let periods = vec![
                window_start + Duration::minutes(5),
                window_start + Duration::minutes(10),
                window_start + Duration::minutes(15),
                window_start + Duration::minutes(20),
            ];
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
                    CpcvTrialPathBinding::try_new(0, vec![0]).expect("trial path"),
                    CscvTrialGridBinding::test_fixture(),
                ),
                fold_artifacts: CpcvFoldArtifacts::try_new(vec![
                    CpcvFoldArtifact {
                        validation_regime: CpcvFoldValidationRegime::PortfolioEconomics,
                        identity: CpcvEstimatorIdentity::Validation {
                            combination_index: 0,
                            test_partitions_hash: hash('b'),
                            test_partition_count: 1,
                            test_groups_hash: hash('c'),
                            test_group_count: 1,
                        },
                        training_groups_hash: hash('b'),
                        training_group_count: 2,
                        calibration_fit_groups_hash: hash('f'),
                        calibration_fit_group_count: 1,
                        scenario_fit_groups_hash: hash('0'),
                        scenario_fit_group_count: 1,
                        model_artifact_hash: hash('c'),
                        serving_contract_hash: hash('d'),
                        model_payload_hash: hash('e'),
                        calibration_function_hash: hash('1'),
                        scenario_economic_function_hash: hash('2'),
                        calibration_artifact_hash: hash('7'),
                        scenario_model_hash: hash('8'),
                    },
                    CpcvFoldArtifact {
                        validation_regime: CpcvFoldValidationRegime::PortfolioEconomics,
                        identity: CpcvEstimatorIdentity::TrialPathValidation {
                            trial_id: 0,
                            path_index: 0,
                            combination_index: 0,
                            test_partitions_hash: hash('b'),
                            test_partition_count: 1,
                            test_groups_hash: hash('c'),
                            test_group_count: 1,
                        },
                        training_groups_hash: hash('f'),
                        training_group_count: 3,
                        calibration_fit_groups_hash: hash('4'),
                        calibration_fit_group_count: 1,
                        scenario_fit_groups_hash: hash('0'),
                        scenario_fit_group_count: 1,
                        model_artifact_hash: hash('1'),
                        serving_contract_hash: hash('2'),
                        model_payload_hash: hash('3'),
                        calibration_function_hash: hash('7'),
                        scenario_economic_function_hash: hash('8'),
                        calibration_artifact_hash: hash('5'),
                        scenario_model_hash: hash('6'),
                    },
                    CpcvFoldArtifact {
                        validation_regime: CpcvFoldValidationRegime::PortfolioEconomics,
                        identity: CpcvEstimatorIdentity::TrialPathValidation {
                            trial_id: 1,
                            path_index: 0,
                            combination_index: 0,
                            test_partitions_hash: hash('b'),
                            test_partition_count: 1,
                            test_groups_hash: hash('c'),
                            test_group_count: 1,
                        },
                        training_groups_hash: hash('f'),
                        training_group_count: 3,
                        calibration_fit_groups_hash: hash('4'),
                        calibration_fit_group_count: 1,
                        scenario_fit_groups_hash: hash('0'),
                        scenario_fit_group_count: 1,
                        model_artifact_hash: hash('2'),
                        serving_contract_hash: hash('3'),
                        model_payload_hash: hash('4'),
                        calibration_function_hash: hash('8'),
                        scenario_economic_function_hash: hash('9'),
                        calibration_artifact_hash: hash('6'),
                        scenario_model_hash: hash('7'),
                    },
                ])
                .expect("fold artifacts"),
                path_count: 1,
                combination_count: 1,
                median_target_rank_ic: dec!(0.1),
                sharpe_distribution: SharpeDistribution {
                    min: dec!(0.2),
                    p25: dec!(0.3),
                    median: dec!(0.4),
                    p75: dec!(0.5),
                    max: dec!(0.6),
                    median_max_drawdown: Some(dec!(0.1)),
                    median_tail_loss: Some(dec!(-0.01)),
                    median_turnover: Some(dec!(0.2)),
                    baseline_uplift: Some(dec!(0.02)),
                },
                paths: vec![BacktestPath {
                    path_index: 0,
                    decision_times: periods.clone(),
                    group_returns: vec![dec!(0.01), dec!(-0.005), dec!(0.01), dec!(-0.005)],
                    scenario_residuals: vec![
                        Some(dec!(0.01)),
                        Some(dec!(-0.005)),
                        Some(dec!(0.01)),
                        Some(dec!(-0.005)),
                    ],
                    sharpe: dec!(0.4),
                    target_rank_ic: dec!(0.1),
                    max_drawdown: dec!(0.005),
                    tail_loss: dec!(-0.005),
                    turnover: Some(dec!(0.2)),
                }]
                .into(),
                deflated_sharpe: dec!(0.95),
                dsr_benchmark_sharpe: dec!(0.1),
                pbo: Decimal::ZERO,
                cscv_selection_evidence: selection_evidence(&periods),
                min_track_record_length_secs: Some(86_400),
                dsr_conservative_independent_trial_count: 1,
                trial_grid_count: 2,
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

    #[test]
    fn hash_uses_persistence_scalars() {
        let mut sealed = NewBacktestPathSet::test_fixture();
        sealed.median_target_rank_ic = dec!(0.1000);
        sealed.deflated_sharpe = dec!(0.95000);
        sealed.dsr_benchmark_sharpe = dec!(0.10000);
        sealed.pbo = dec!(0.00000);
        sealed.window_start =
            DateTime::from_timestamp_micros(sealed.window_start.timestamp_micros())
                .expect("microsecond timestamp")
                + Duration::nanoseconds(999);

        sealed
            .verify_hash()
            .expect("hash must ignore non-semantic decimal scale and sub-microsecond time");
    }

    #[test]
    fn cscv_rejects_incomplete_combinations() {
        let start = Utc::now() - Duration::hours(1);
        let periods = (1..=4)
            .map(|offset| start + Duration::minutes(offset * 5))
            .collect::<Vec<_>>();
        let mut evidence = selection_evidence(&periods);
        evidence.combinations.pop();

        assert!(
            evidence
                .validate_for(&CscvTrialGridBinding::test_fixture())
                .is_err()
        );
    }

    #[test]
    fn cscv_rejects_noncanonical_combinations() {
        let start = Utc::now() - Duration::hours(1);
        let periods = (1..=4)
            .map(|offset| start + Duration::minutes(offset * 5))
            .collect::<Vec<_>>();
        let mut evidence = selection_evidence(&periods);
        evidence.combinations.swap(0, 1);

        assert!(
            evidence
                .validate_for(&CscvTrialGridBinding::test_fixture())
                .is_err()
        );
    }

    #[test]
    fn rejects_cscv_clock_tamper() {
        let mut sealed = NewBacktestPathSet::test_fixture();
        sealed.cscv_selection_evidence.blocks[0].first_period += Duration::seconds(1);

        assert!(sealed.verify_hash().is_err());
    }
}
