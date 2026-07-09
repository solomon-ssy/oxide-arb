//! Combinatorial Purged Cross-Validation with full φ-path reconstruction.
//!
//! Phase 11.5 §3.3, López de Prado *Advances in Financial Machine Learning*
//! Ch.12; path-reconstruction algorithm per the `mlfinlab`
//! `CombinatorialPurgedKFold._fill_backtest_paths` construction, faithfully
//! reproduced here.
//!
//! A single walk-forward backtest gives one number; CPCV gives a
//! **distribution**. Partition the timeline into `N` groups; for every
//! `C(N, k)` way to choose `k` groups as the test set, purge/embargo the
//! complement and train+evaluate one fold. Because each group is a test
//! member in exactly `C(N-1, k-1)` of those combinations, the per-group
//! out-of-sample results can be losslessly reassembled into
//! `φ(N, k) = C(N-1, k-1)` **complete**, full-timeline backtest paths (never
//! a single fragile number, never a "combination = path" simplification).
//!
//! This module is deliberately generic over what an atomic
//! [`crate::validation::TimelineGroup`] represents and how a fold is trained
//! ([`FoldModelSource`]) or evaluated ([`ReplayEngine`]) — the Buy-side wiring
//! (`WeightedFactor` / classical ML) supplies same-`as_of` cross-section
//! groups + [`crate::backtest::PortfolioReplayBacktester`]-backed
//! implementations; Phase 11.5.1 supplies lot-grouped implementations with
//! zero changes here.

use quant_pivot_error::{QuantResult, research::ResearchError};
use quant_pivot_models::types::BacktestPathSetId;
use rayon::prelude::*;
use rust_decimal::{Decimal, prelude::ToPrimitive};
use serde::{Deserialize, Serialize};

use crate::{
    backtest::metrics::sharpe_ratio,
    model::runtime::QuantModelRuntime,
    precision::RESEARCH_DECIMAL_SCALE,
    stats,
    validation::{
        combinatorics::combinations,
        purge::{DefaultPurgedSplitter, PurgeConfig, PurgedSplitter, TimelineGroup},
    },
};

/// CPCV partition configuration.
///
/// `N` groups, `k` of them held out as the test set for every combination.
/// `combinations(n_groups, k_test)` folds are run;
/// `phi(n_groups, k_test) = C(n_groups - 1, k_test - 1)` complete paths are
/// reconstructed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CpcvConfig {
    /// Number of contiguous timeline partitions (`N`).
    pub n_groups: u32,
    /// Number of partitions held out as the test set per combination (`k`).
    pub k_test: u32,
}

impl CpcvConfig {
    /// `φ(N, k) = C(N-1, k-1)`.
    ///
    /// The number of complete, full-timeline backtest paths this config
    /// reconstructs (a combinatorial identity: `C(N,k)·k/N`, always an
    /// integer). **Not** the number of combinations (`C(N,k)`) — a common and
    /// consequential mix-up (the original Phase 11.5 design draft made
    /// exactly this error for `N=8,k=2`: 28 combinations, but only φ=7 paths).
    #[must_use]
    pub fn path_count(&self) -> u64 {
        binomial(
            self.n_groups.saturating_sub(1),
            self.k_test.saturating_sub(1),
        )
    }

    /// `C(N, k)` — the number of purge/embargo/train/evaluate folds this
    /// config runs. Reported for audit visibility; never confused with
    /// [`Self::path_count`].
    #[must_use]
    pub fn combination_count(&self) -> u64 {
        binomial(self.n_groups, self.k_test)
    }
}

fn binomial(n: u32, k: u32) -> u64 {
    if k > n {
        return 0;
    }
    let (n, k) = (u64::from(n), u64::from(k));
    let k = k.min(n - k);
    let mut result: u128 = 1;
    for i in 0..k {
        result = result * u128::from(n - i) / u128::from(i + 1);
    }
    u64::try_from(result).unwrap_or(u64::MAX)
}

/// A row/group-index filter selecting which [`TimelineGroup`]s (by index into
/// the caller's original slice) a fold's training or evaluation is restricted
/// to.
#[derive(Debug, Clone, Default)]
pub struct GroupRowFilter {
    /// Indices into the original `groups` slice, unordered.
    pub group_indices: Vec<usize>,
}

/// Trains one fold's model, restricted to the groups `filter` selects.
///
/// Implementations close over the full underlying example set (or lot set,
/// for Phase 11.5.1) and project it down to `filter`'s rows before training —
/// this is the **only** family-specific seam in the entire CPCV pipeline.
pub trait FoldModelSource: Send + Sync {
    /// Train a fold-scoped model. Errors propagate (a fold that cannot train
    /// — e.g. too few resolved labels after purge — fails the whole CPCV run
    /// rather than silently skipping a path).
    fn train_fold(&self, filter: &GroupRowFilter) -> QuantResult<Box<dyn QuantModelRuntime>>;
}

/// One test group's fold-level evaluation.
#[derive(Debug, Clone, Copy)]
pub struct GroupEvaluation {
    /// Index into the original `groups` slice this evaluation covers.
    pub group_index: usize,
    /// This group's contribution to the reconstructed path's return series
    /// (a fractional per-period return, family-specific in derivation:
    /// realized tick `PnL` / allocated capital for Buy-side, realized lot
    /// proceeds / cost basis for Phase 11.5.1's Sell-side).
    pub return_value: Decimal,
    /// This group's own cross-sectional rank IC (Spearman of score vs.
    /// realized outcome across the group's candidates), when the group has
    /// more than one ranked candidate. `None` for groups where "rank" is not
    /// a meaningful concept (e.g. a single hold-vs-exit decision).
    pub rank_ic: Option<Decimal>,
}

/// Evaluates a trained model against the groups `filter` selects, returning
/// one [`GroupEvaluation`] per selected group.
pub trait ReplayEngine: Send + Sync {
    /// Errors propagate for the same reason as [`FoldModelSource::train_fold`].
    fn evaluate(
        &self,
        model: &dyn QuantModelRuntime,
        filter: &GroupRowFilter,
    ) -> QuantResult<Vec<GroupEvaluation>>;
}

/// A distribution summary of the Sharpe ratio across the reconstructed φ paths.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct SharpeDistribution {
    pub min: Decimal,
    pub p25: Decimal,
    pub median: Decimal,
    pub p75: Decimal,
    pub max: Decimal,
}

/// One complete, full-timeline reconstructed backtest path.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BacktestPath {
    /// Stable index (`0..phi`), deterministic across runs for the same input.
    pub path_index: u32,
    /// Per-group return series, in ascending `as_of` order, one entry per
    /// original group — a genuinely complete walk across the whole window
    /// (never a single combination's `k`-group fragment).
    pub group_returns: Vec<Decimal>,
    /// Sharpe ratio of [`Self::group_returns`] (unannualized; callers
    /// annualize using their own period cadence when displaying).
    pub sharpe: Decimal,
    /// Mean of the path's groups' own `rank_ic` (only over groups where it
    /// was `Some`; `0` if none reported a rank IC).
    pub rank_ic: Decimal,
    /// Maximum peak-to-trough drawdown of the cumulative return curve.
    pub max_drawdown: Decimal,
    /// Mean of the worst 10% of per-group returns (tail loss).
    pub tail_loss: Decimal,
}

/// A full Combinatorial Purged Cross-Validation result: the reconstructed
/// path distribution plus the fold-level provenance needed for audit.
#[derive(Debug, Clone)]
pub struct BacktestPathSet {
    /// Pre-minted identity (Phase 11.5 §4; declared per the Phase 11.0 freeze).
    pub path_set_id: BacktestPathSetId,
    /// The `phi(N, k)` reconstructed complete paths.
    pub paths: Vec<BacktestPath>,
    /// `C(N, k)` — the number of folds run (audit visibility only; **not**
    /// `paths.len()`, see [`CpcvConfig::path_count`]).
    pub combination_count: u64,
    /// Distribution of [`BacktestPath::sharpe`] across `paths`.
    pub sharpe_distribution: SharpeDistribution,
    /// Median of [`BacktestPath::rank_ic`] across `paths` — the Phase 11.5
    /// hard `RankIc` gate's data source (replacing the single-path number).
    pub median_rank_ic: Decimal,
}

/// Inputs to one CPCV run. `groups` must be sorted ascending by `as_of`
/// (the same invariant [`PurgedSplitter`] requires) — index `i` in every
/// [`GroupRowFilter`]/[`GroupEvaluation`] refers to `groups[i]`.
pub struct CpcvRequest<'a> {
    pub path_set_id: BacktestPathSetId,
    pub groups: &'a [TimelineGroup],
    pub cpcv: CpcvConfig,
    pub purge: PurgeConfig,
    pub fold_source: &'a dyn FoldModelSource,
    pub replay: &'a dyn ReplayEngine,
}

/// Runs Combinatorial Purged Cross-Validation and reconstructs the φ-path
/// distribution.
pub trait CombinatorialPurgedBacktester: Send + Sync {
    /// # Errors
    ///
    /// Propagates [`FoldModelSource::train_fold`] / [`ReplayEngine::evaluate`]
    /// failures, and returns [`ResearchError::ValidationMethodology`] for an
    /// invalid `cpcv` config (`k_test >= n_groups`, `n_groups` exceeding the
    /// group count, etc).
    fn run(&self, request: CpcvRequest<'_>) -> QuantResult<BacktestPathSet>;
}

/// The production [`CombinatorialPurgedBacktester`]: purge/embargo splitting
/// (via [`DefaultPurgedSplitter`]) + rayon-parallel fold execution + the
/// `mlfinlab`-style greedy φ-path slot assignment.
#[derive(Debug, Default, Clone, Copy)]
pub struct DefaultCombinatorialPurgedBacktester;

impl DefaultCombinatorialPurgedBacktester {
    /// Construct the backtester.
    #[must_use]
    pub const fn new() -> Self {
        Self
    }
}

/// A contiguous, roughly-equal partition of `groups`'s indices into
/// `n_groups` buckets. Contiguous (not interleaved) so a partition's own
/// interval span stays tight for purge/embargo purposes.
fn partition_indices(group_count: usize, n_groups: u32) -> Vec<Vec<usize>> {
    let n_groups = n_groups as usize;
    (0..n_groups)
        .map(|partition| {
            let start = group_count * partition / n_groups;
            let end = group_count * (partition + 1) / n_groups;
            (start..end).collect()
        })
        .collect()
}

/// The result of running one `C(N,k)` combination's fold.
struct FoldResult {
    /// Which partitions (by index, `0..n_groups`) this combination tested.
    test_partitions: Vec<usize>,
    /// This fold's evaluations, keyed by the original group index.
    evaluations: Vec<GroupEvaluation>,
}

impl CombinatorialPurgedBacktester for DefaultCombinatorialPurgedBacktester {
    fn run(&self, request: CpcvRequest<'_>) -> QuantResult<BacktestPathSet> {
        let CpcvRequest {
            path_set_id,
            groups,
            cpcv,
            purge,
            fold_source,
            replay,
        } = request;
        validate_config(groups.len(), cpcv)?;

        let partitions = partition_indices(groups.len(), cpcv.n_groups);
        let n_groups = cpcv.n_groups as usize;
        let k_test = cpcv.k_test as usize;
        let combos = combinations(n_groups, k_test);
        let splitter = DefaultPurgedSplitter::new();

        let fold_results: Vec<QuantResult<FoldResult>> = combos
            .par_iter()
            .map(|test_partitions| -> QuantResult<FoldResult> {
                let test_group_indices: Vec<usize> = test_partitions
                    .iter()
                    .flat_map(|&p| partitions[p].iter().copied())
                    .collect();
                let split = splitter.split(groups, &test_group_indices, &purge);
                let model = fold_source.train_fold(&GroupRowFilter {
                    group_indices: split.train_indices,
                })?;
                let evaluations = replay.evaluate(
                    model.as_ref(),
                    &GroupRowFilter {
                        group_indices: split.test_indices,
                    },
                )?;
                Ok(FoldResult {
                    test_partitions: test_partitions.clone(),
                    evaluations,
                })
            })
            .collect();
        let mut folds = Vec::with_capacity(fold_results.len());
        for result in fold_results {
            folds.push(result?);
        }

        let path_count = usize::try_from(cpcv.path_count()).unwrap_or(usize::MAX);
        let paths = reconstruct_paths(groups, n_groups, path_count, &partitions, &folds)?;
        let sharpe_distribution = sharpe_distribution(&paths);
        let median_rank_ic = median(&paths.iter().map(|p| p.rank_ic).collect::<Vec<_>>());

        Ok(BacktestPathSet {
            path_set_id,
            paths,
            combination_count: cpcv.combination_count(),
            sharpe_distribution,
            median_rank_ic,
        })
    }
}

fn validate_config(group_count: usize, cpcv: CpcvConfig) -> QuantResult<()> {
    if cpcv.n_groups < 2 {
        return Err(ResearchError::ValidationMethodology {
            detail: format!("cpcv.n_groups must be >= 2, got {}", cpcv.n_groups),
        }
        .into());
    }
    if cpcv.k_test < 1 || cpcv.k_test >= cpcv.n_groups {
        return Err(ResearchError::ValidationMethodology {
            detail: format!(
                "cpcv.k_test must be in 1..n_groups (n_groups={}), got {}",
                cpcv.n_groups, cpcv.k_test
            ),
        }
        .into());
    }
    if group_count < cpcv.n_groups as usize {
        return Err(ResearchError::ValidationMethodology {
            detail: format!(
                "cpcv.n_groups={} exceeds the available timeline group count {group_count}",
                cpcv.n_groups
            ),
        }
        .into());
    }
    Ok(())
}

/// The `mlfinlab`-style greedy φ-path slot assignment: `phi` paths, each with
/// `n_groups` slots (one per partition); walk the folds in a fixed order and,
/// for every partition a fold tested, assign that fold to the first path
/// whose slot for that partition is still empty. By CPCV's combinatorial
/// symmetry (each partition is tested by exactly `C(N-1,k-1) = phi` folds),
/// every slot in every path is filled exactly once by the end.
fn reconstruct_paths(
    groups: &[TimelineGroup],
    n_groups: usize,
    path_count: usize,
    partitions: &[Vec<usize>],
    folds: &[FoldResult],
) -> QuantResult<Vec<BacktestPath>> {
    let mut slots: Vec<Vec<Option<usize>>> = vec![vec![None; n_groups]; path_count];
    for (fold_idx, fold) in folds.iter().enumerate() {
        for &partition in &fold.test_partitions {
            let Some(path) = slots.iter_mut().find(|path| path[partition].is_none()) else {
                return Err(ResearchError::ValidationMethodology {
                    detail: format!(
                        "phi-path reconstruction ran out of empty slots for partition {partition} \
                         at fold {fold_idx} — this indicates a combinatorial-symmetry bug, not a \
                         data problem"
                    ),
                }
                .into());
            };
            path[partition] = Some(fold_idx);
        }
    }

    let mut paths = Vec::with_capacity(path_count);
    for (path_index, path_slots) in slots.into_iter().enumerate() {
        let mut per_group: Vec<Option<GroupEvaluation>> = vec![None; groups.len()];
        for (partition, fold_idx) in path_slots.into_iter().enumerate() {
            let Some(fold_idx) = fold_idx else {
                return Err(ResearchError::ValidationMethodology {
                    detail: format!("path {path_index} left partition {partition} unfilled"),
                }
                .into());
            };
            let fold = &folds[fold_idx];
            for &group_index in &partitions[partition] {
                let evaluation = fold
                    .evaluations
                    .iter()
                    .find(|e| e.group_index == group_index)
                    .copied();
                per_group[group_index] = evaluation;
            }
        }
        let path_index = u32::try_from(path_index).unwrap_or(u32::MAX);
        paths.push(build_path(path_index, &per_group)?);
    }
    Ok(paths)
}

fn build_path(path_index: u32, per_group: &[Option<GroupEvaluation>]) -> QuantResult<BacktestPath> {
    let mut group_returns = Vec::with_capacity(per_group.len());
    let mut rank_ics = Vec::new();
    for (group_index, evaluation) in per_group.iter().enumerate() {
        let Some(evaluation) = evaluation else {
            return Err(ResearchError::ValidationMethodology {
                detail: format!("group {group_index} was never evaluated in path {path_index}"),
            }
            .into());
        };
        group_returns.push(evaluation.return_value);
        if let Some(rank_ic) = evaluation.rank_ic {
            rank_ics.push(rank_ic);
        }
    }

    let sharpe = sharpe_ratio(&group_returns, Decimal::ONE).round_dp(RESEARCH_DECIMAL_SCALE);
    let rank_ic = stats::mean(&rank_ics).round_dp(RESEARCH_DECIMAL_SCALE);
    let max_drawdown = max_drawdown_from_returns(&group_returns);
    let tail_loss = tail_loss_from_returns(&group_returns, Decimal::new(10, 2));

    Ok(BacktestPath {
        path_index,
        group_returns,
        sharpe,
        rank_ic,
        max_drawdown,
        tail_loss,
    })
}

/// Maximum peak-to-trough drawdown of the cumulative-return curve built from
/// `returns` (non-compounding cumulative sum, consistent with
/// [`crate::backtest::metrics::max_drawdown`]'s convention). `0` for an empty
/// or monotonically non-decreasing series.
fn max_drawdown_from_returns(returns: &[Decimal]) -> Decimal {
    let mut cumulative = Decimal::ZERO;
    let mut peak = Decimal::ZERO;
    let mut max_dd = Decimal::ZERO;
    for &r in returns {
        cumulative += r;
        peak = peak.max(cumulative);
        max_dd = max_dd.max(peak - cumulative);
    }
    max_dd.round_dp(RESEARCH_DECIMAL_SCALE)
}

/// Mean of the worst `quantile` fraction of `returns` (tail loss). Mirrors
/// [`crate::backtest::metrics::tail_loss`]'s convention over an abstract
/// return series rather than [`crate::backtest::SampleOutcome`].
fn tail_loss_from_returns(returns: &[Decimal], quantile: Decimal) -> Decimal {
    if returns.is_empty() {
        return Decimal::ZERO;
    }
    let mut sorted = returns.to_vec();
    sorted.sort();
    let n = sorted.len();
    let raw = (Decimal::from(n as u64) * quantile).ceil();
    let take = raw
        .to_u64()
        .and_then(|v| usize::try_from(v).ok())
        .unwrap_or(1)
        .max(1)
        .min(n);
    stats::mean(&sorted[..take]).round_dp(RESEARCH_DECIMAL_SCALE)
}

fn sharpe_distribution(paths: &[BacktestPath]) -> SharpeDistribution {
    let mut sharpes: Vec<Decimal> = paths.iter().map(|p| p.sharpe).collect();
    sharpes.sort();
    SharpeDistribution {
        min: percentile(&sharpes, Decimal::ZERO),
        p25: percentile(&sharpes, Decimal::new(25, 2)),
        median: percentile(&sharpes, Decimal::new(5, 1)),
        p75: percentile(&sharpes, Decimal::new(75, 2)),
        max: percentile(&sharpes, Decimal::ONE),
    }
}

/// `sorted` must already be ascending. Nearest-rank percentile (`0` = min,
/// `1` = max); `0` for an empty series.
fn percentile(sorted: &[Decimal], fraction: Decimal) -> Decimal {
    if sorted.is_empty() {
        return Decimal::ZERO;
    }
    let last = Decimal::from(u64::try_from(sorted.len() - 1).unwrap_or(u64::MAX));
    let index = (last * fraction)
        .round()
        .to_u64()
        .and_then(|value| usize::try_from(value).ok())
        .unwrap_or(0);
    sorted[index.min(sorted.len() - 1)]
}

fn median(values: &[Decimal]) -> Decimal {
    if values.is_empty() {
        return Decimal::ZERO;
    }
    let mut sorted = values.to_vec();
    sorted.sort();
    percentile(&sorted, Decimal::new(5, 1))
}

#[cfg(test)]
mod tests {
    use super::{
        CombinatorialPurgedBacktester, CpcvConfig, CpcvRequest,
        DefaultCombinatorialPurgedBacktester, FoldModelSource, GroupEvaluation, GroupRowFilter,
        ReplayEngine,
    };
    use crate::{
        features::FeatureName,
        model::runtime::{ModelFamily, ModelRuntimeInput, ModelRuntimeOutput, QuantModelRuntime},
        validation::purge::{PurgeConfig, TimelineGroup},
    };
    use async_trait::async_trait;
    use chrono::{TimeZone, Utc};
    use quant_pivot_error::QuantResult;
    use quant_pivot_models::types::{BacktestPathSetId, ContentHash, ModelVersionId};
    use rust_decimal::Decimal;
    use rust_decimal_macros::dec;

    /// A stub runtime: fold identity is irrelevant to these tests (the
    /// [`ReplayEngine`] stub below ignores the model entirely and returns
    /// deterministic synthetic returns), so one trivial implementation
    /// suffices for every fold.
    struct StubRuntime;

    #[async_trait]
    impl QuantModelRuntime for StubRuntime {
        fn model_version_id(&self) -> ModelVersionId {
            ModelVersionId::from_v7()
        }

        fn model_family(&self) -> ModelFamily {
            ModelFamily::WeightedFactor
        }

        fn feature_schema_hash(&self) -> ContentHash {
            ContentHash::parse(format!("blake3:{}", "0".repeat(64))).expect("hash")
        }

        fn required_features(&self) -> Vec<FeatureName> {
            Vec::new()
        }

        async fn infer_batch(&self, _input: ModelRuntimeInput) -> QuantResult<ModelRuntimeOutput> {
            unreachable!("not exercised by these pure-orchestration tests")
        }
    }

    struct StubFoldSource;
    impl FoldModelSource for StubFoldSource {
        fn train_fold(&self, _filter: &GroupRowFilter) -> QuantResult<Box<dyn QuantModelRuntime>> {
            Ok(Box::new(StubRuntime))
        }
    }

    /// Deterministic per-group return `= group_index as bps / 10_000`, so
    /// path reconstruction correctness can be checked exactly.
    struct DeterministicReplay;
    impl ReplayEngine for DeterministicReplay {
        fn evaluate(
            &self,
            _model: &dyn QuantModelRuntime,
            filter: &GroupRowFilter,
        ) -> QuantResult<Vec<GroupEvaluation>> {
            Ok(filter
                .group_indices
                .iter()
                .map(|&group_index| GroupEvaluation {
                    group_index,
                    return_value: Decimal::from(u64::try_from(group_index).unwrap_or(0))
                        / Decimal::from(10_000),
                    rank_ic: Some(dec!(0.1)),
                })
                .collect())
        }
    }

    fn groups(n: i64) -> Vec<TimelineGroup> {
        (0..n)
            .map(|h| {
                let as_of = Utc.timestamp_opt(1_700_000_000 + h * 3_600, 0).unwrap();
                TimelineGroup {
                    as_of,
                    label_horizon_end: as_of,
                }
            })
            .collect()
    }

    #[test]
    fn cpcv_path_count_matches_phi_formula() {
        // phi(8, 2) = C(7, 1) = 7, NOT C(8,2) = 28 (the original doc's error).
        assert_eq!(
            CpcvConfig {
                n_groups: 8,
                k_test: 2
            }
            .path_count(),
            7
        );
        assert_eq!(
            CpcvConfig {
                n_groups: 8,
                k_test: 2
            }
            .combination_count(),
            28
        );
        // phi(6, 2) = C(5, 1) = 5 (matches the stats.stackexchange worked example).
        assert_eq!(
            CpcvConfig {
                n_groups: 6,
                k_test: 2
            }
            .path_count(),
            5
        );
    }

    #[test]
    fn cpcv_produces_phi_not_combination_count_paths() {
        let groups = groups(80);
        let result = DefaultCombinatorialPurgedBacktester::new()
            .run(CpcvRequest {
                path_set_id: BacktestPathSetId::from_v7(),
                groups: &groups,
                cpcv: CpcvConfig {
                    n_groups: 8,
                    k_test: 2,
                },
                purge: PurgeConfig::pct_only(dec!(0)),
                fold_source: &StubFoldSource,
                replay: &DeterministicReplay,
            })
            .expect("cpcv run");
        assert_eq!(
            result.paths.len(),
            7,
            "phi(8,2) = 7 paths, not the 28 combinations"
        );
        assert_eq!(result.combination_count, 28);
    }

    #[test]
    fn cpcv_path_reconstruction_covers_every_group_exactly_once() {
        let groups = groups(80);
        let result = DefaultCombinatorialPurgedBacktester::new()
            .run(CpcvRequest {
                path_set_id: BacktestPathSetId::from_v7(),
                groups: &groups,
                cpcv: CpcvConfig {
                    n_groups: 8,
                    k_test: 2,
                },
                purge: PurgeConfig::pct_only(dec!(0)),
                fold_source: &StubFoldSource,
                replay: &DeterministicReplay,
            })
            .expect("cpcv run");
        for path in &result.paths {
            assert_eq!(
                path.group_returns.len(),
                groups.len(),
                "every path must reconstruct a value for every original group"
            );
        }
    }

    #[test]
    fn cpcv_rejects_k_test_at_or_above_n_groups() {
        let groups = groups(40);
        let result = DefaultCombinatorialPurgedBacktester::new().run(CpcvRequest {
            path_set_id: BacktestPathSetId::from_v7(),
            groups: &groups,
            cpcv: CpcvConfig {
                n_groups: 4,
                k_test: 4,
            },
            purge: PurgeConfig::pct_only(dec!(0)),
            fold_source: &StubFoldSource,
            replay: &DeterministicReplay,
        });
        assert!(result.is_err());
    }

    #[test]
    fn cpcv_never_calls_live_bookstore() {
        // Structural guarantee: `FoldModelSource`/`ReplayEngine` take no
        // network/database handle, so a `DefaultCombinatorialPurgedBacktester`
        // literally cannot reach a live BookStore — enforced by the trait
        // signatures themselves, not by a runtime check. This test exists so
        // the invariant has a named, discoverable anchor in the test suite.
        let groups = groups(16);
        let _ = DefaultCombinatorialPurgedBacktester::new().run(CpcvRequest {
            path_set_id: BacktestPathSetId::from_v7(),
            groups: &groups,
            cpcv: CpcvConfig {
                n_groups: 4,
                k_test: 1,
            },
            purge: PurgeConfig::pct_only(dec!(0)),
            fold_source: &StubFoldSource,
            replay: &DeterministicReplay,
        });
    }

    /// Classical (and future Sell lot) families reuse the same φ-path CPCV
    /// engine via `FoldModelSource` — path count is independent of trainer kind.
    #[test]
    fn cpcv_phi_paths_shared_by_classical_fold_source_contract() {
        let groups = groups(8);
        let path_set = DefaultCombinatorialPurgedBacktester::new()
            .run(CpcvRequest {
                path_set_id: BacktestPathSetId::from_v7(),
                groups: &groups,
                cpcv: CpcvConfig {
                    n_groups: 4,
                    k_test: 2,
                },
                purge: PurgeConfig::pct_only(dec!(0.02)),
                fold_source: &StubFoldSource,
                replay: &DeterministicReplay,
            })
            .expect("cpcv");
        // N=4,k=2 → φ = C(3,1) = 3 (same for WeightedFactor and classical).
        assert_eq!(path_set.paths.len(), 3);
        assert_eq!(path_set.combination_count, 6);
    }
}
