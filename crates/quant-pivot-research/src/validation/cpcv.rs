//! Combinatorial Purged Cross-Validation with full φ-path reconstruction.
//!
//! Purge and embargo follow López de Prado, *Advances in Financial Machine
//! Learning*, Ch. 12; path reconstruction follows the `mlfinlab`
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
//! implementations; supplies lot-grouped implementations with
//! zero changes here.

use std::{
    collections::{BTreeMap, BTreeSet},
    ops::Range,
};

use quant_pivot_error::{QuantError, QuantResult, research::ResearchError};
use quant_pivot_models::{
    domain::quant::PortfolioScenarioVisibility,
    hashing::CanonicalDigest,
    types::{
        BacktestPathSetId, ContentHash,
        backtest::{BacktestPath, CpcvTrialPathBinding, SharpeDistribution},
    },
};
use rayon::prelude::*;
use rust_decimal::{Decimal, prelude::ToPrimitive};

use crate::{
    backtest::{BacktestScenarioContext, PrecomputedBacktestTick, metrics::sharpe_ratio},
    model::{ResolvedCalibration, runtime::QuantModelRuntime, sell_scorer::SellScorerRuntime},
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
    /// consequential mix-up (the original design draft made
    /// exactly this error for `N=8,k=2`: 28 combinations, but only φ=7 paths).
    ///
    /// # Errors
    ///
    /// Returns [`ResearchError::ValidationMethodology`] for invalid zero
    /// dimensions or when the exact combinatorial count exceeds `u64`.
    pub fn path_count(&self) -> QuantResult<u64> {
        let n = self.n_groups.checked_sub(1).ok_or_else(|| {
            methodology("cpcv.n_groups must be positive before computing path count".to_owned())
        })?;
        let k = self.k_test.checked_sub(1).ok_or_else(|| {
            methodology("cpcv.k_test must be positive before computing path count".to_owned())
        })?;
        binomial(n, k)
    }

    /// `C(N, k)` — the number of purge/embargo/train/evaluate folds this
    /// config runs. Reported for audit visibility; never confused with
    /// [`Self::path_count`].
    ///
    /// # Errors
    ///
    /// Returns [`ResearchError::ValidationMethodology`] when `k_test` exceeds
    /// `n_groups` or the exact count exceeds `u64`.
    pub fn combination_count(&self) -> QuantResult<u64> {
        binomial(self.n_groups, self.k_test)
    }

    /// Canonical unique fold set required to reconstruct one precommitted
    /// complete OOS path for every governed trial column.
    pub fn trial_path(&self, path_index: u32) -> QuantResult<CpcvTrialPathBinding> {
        if self.n_groups < 2 || self.k_test == 0 || self.k_test >= self.n_groups {
            return Err(methodology(format!(
                "CPCV trial path requires N>=2 and 0<k<N, got N={} k={}",
                self.n_groups, self.k_test
            )));
        }
        let n_groups = usize::try_from(self.n_groups)
            .map_err(|error| methodology(format!("cpcv.n_groups does not fit usize: {error}")))?;
        let k_test = usize::try_from(self.k_test)
            .map_err(|error| methodology(format!("cpcv.k_test does not fit usize: {error}")))?;
        let path_count = usize::try_from(self.path_count()?)
            .map_err(|error| methodology(format!("CPCV path count does not fit usize: {error}")))?;
        let requested_path = usize::try_from(path_index)
            .map_err(|error| methodology(format!("CPCV path index does not fit usize: {error}")))?;
        if requested_path >= path_count {
            return Err(methodology(format!(
                "CPCV path index {path_index} is outside the configured 0..{path_count} population"
            )));
        }
        let combos = combinations(n_groups, k_test);
        let slots = assign_path_slots(
            n_groups,
            path_count,
            combos
                .iter()
                .enumerate()
                .map(|(fold_index, test_partitions)| (fold_index, test_partitions.as_slice())),
        )?;
        let path_slots = slots.get(requested_path).ok_or_else(|| {
            methodology(format!(
                "CPCV path index {path_index} disappeared after slot assignment"
            ))
        })?;
        let combination_indices = path_slots
            .iter()
            .copied()
            .map(|fold_index| {
                fold_index.ok_or_else(|| {
                    methodology(format!(
                        "path {path_index} contains an unfilled partition slot"
                    ))
                })
            })
            .collect::<QuantResult<BTreeSet<_>>>()?
            .into_iter()
            .map(|fold_index| {
                u32::try_from(fold_index).map_err(|error| {
                    methodology(format!("CPCV fold index does not fit u32: {error}"))
                })
            })
            .collect::<QuantResult<Vec<_>>>()?;
        CpcvTrialPathBinding::try_new(path_index, combination_indices)
            .map_err(|error| methodology(format!("build CPCV trial-path binding: {error}")))
    }
}

fn binomial(n: u32, k: u32) -> QuantResult<u64> {
    if k > n {
        return Err(methodology(format!(
            "cannot compute binomial coefficient C({n}, {k}) with k > n"
        )));
    }
    let (n, k) = (u64::from(n), u64::from(k));
    let k = k.min(n - k);
    let mut result: u128 = 1;
    for i in 0..k {
        result = result
            .checked_mul(u128::from(n - i))
            .ok_or_else(|| methodology(format!("binomial C({n}, {k}) overflowed u128")))?
            / u128::from(i + 1);
    }
    u64::try_from(result).map_err(|error| {
        methodology(format!(
            "binomial C({n}, {k})={result} does not fit u64: {error}"
        ))
    })
}

/// A row/group-index filter selecting which [`TimelineGroup`]s (by index into
/// the caller's original slice) a fold's training or evaluation is restricted
/// to.
#[derive(Debug, Clone, Default)]
pub struct GroupRowFilter {
    /// Strictly ascending, unique indices into the original `groups` slice.
    /// Producers must normalize once before crossing the fold boundary; hot
    /// consumers deliberately do not clone/sort this potentially million-row
    /// vector again.
    pub group_indices: Vec<usize>,
}

/// Immutable semantic identity supplied to one fold-scoped training call.
///
/// Validation identities bind the deterministic combination index together
/// with its exact held-out partitions and groups. This distinction matters:
/// purge/embargo can legitimately leave two different combinations with the
/// same training rows and therefore the same trained model bytes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FoldTrainingIdentity<'a> {
    /// One `C(N,k)` validation combination.
    Validation {
        combination_index: u32,
        test_partitions: &'a [usize],
        test_groups: &'a [usize],
    },
    /// One purge/embargo CPCV combination for a governed hyperparameter
    /// trial. Trial evidence is never produced by fitting and evaluating the
    /// same full window.
    TrialPathValidation {
        trial_id: u32,
        path_index: u32,
        combination_index: u32,
        test_partitions: &'a [usize],
        test_groups: &'a [usize],
    },
}

/// Borrowed input to one fold-scoped training call.
#[derive(Debug, Clone, Copy)]
pub struct FoldTrainingRequest<'a> {
    pub identity: FoldTrainingIdentity<'a>,
    pub filter: &'a GroupRowFilter,
}

/// A fold-trained model, family-polymorphic over the two runtime traits the
/// CPCV pipeline can train and replay.
///
/// This is the **only** family-specific seam in the entire CPCV pipeline
/// (alongside [`FoldModelSource`] / [`ReplayEngine`] themselves) — every
/// other algorithm in this module (purge/embargo, φ-path reconstruction,
/// trial-grid, DSR/PBO) is written purely against these three abstractions
/// and never inspects which variant it holds.
pub enum FoldRuntime {
    /// A model-only Buy runtime used by allocation-independent validators.
    BuyModel(Box<dyn QuantModelRuntime>),
    /// A complete Buy estimator: model, nested calibration, and fold-local
    /// scenario model fitted without observing its outer test population.
    BuyPortfolio(Box<PurgedPortfolioFoldRuntime>),
    /// A Sell-side (`HoldVsExitWeighted`) fold runtime.
    Sell(Box<dyn SellScorerRuntime>),
    /// Policy-fit fold selection over precomputed executable candidate paths.
    Policy(PolicyFoldRuntime),
}

/// Complete fold-local economic estimator for portfolio replay.
pub struct PurgedPortfolioFoldRuntime {
    pub model: Box<dyn QuantModelRuntime>,
    pub calibration: ResolvedCalibration,
    pub calibration_artifact_hash: ContentHash,
    pub calibration_function_hash: ContentHash,
    pub scenario: BacktestScenarioContext,
    pub scenario_economic_function_hash: ContentHash,
    pub model_fit_groups_hash: ContentHash,
    pub calibration_fit_groups_hash: ContentHash,
    pub scenario_fit_groups_hash: ContentHash,
    pub test_groups_hash: ContentHash,
    pub model_fit_groups: Vec<usize>,
    pub calibration_fit_groups: Vec<usize>,
    pub scenario_fit_groups: Vec<usize>,
}

impl PurgedPortfolioFoldRuntime {
    /// Prove that the replay filter is the exact held-out set committed by the
    /// fold identity and is disjoint from both estimator-fit populations.
    pub fn visibility_for(
        &self,
        filter: &GroupRowFilter,
    ) -> QuantResult<PortfolioScenarioVisibility> {
        let test_groups_hash = CanonicalDigest::content_hash_typed(
            "quant-pivot/cpcv-fold-test-groups",
            1,
            &filter.group_indices,
        )?;
        if test_groups_hash != self.test_groups_hash
            || filter.group_indices.iter().any(|index| {
                self.model_fit_groups.binary_search(index).is_ok()
                    || self.calibration_fit_groups.binary_search(index).is_ok()
                    || self.scenario_fit_groups.binary_search(index).is_ok()
            })
        {
            return Err(ResearchError::ValidationMethodology {
                detail: "CPCV portfolio replay is not disjoint from its fold-local estimator fit"
                    .to_owned(),
            }
            .into());
        }
        Ok(PortfolioScenarioVisibility::PurgedCrossValidation {
            fit_evidence_hash: self.scenario.model().pit_residual_panel_hash,
            test_groups_hash,
        })
    }
}

/// Candidate selected using only one fold's purged training groups.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PolicyFoldRuntime {
    pub validation_fold_index: u32,
    pub candidate_index: usize,
    pub candidate_id: String,
    pub training_group_count: u64,
    pub training_utility_bps: Decimal,
}

impl FoldRuntime {
    /// Project to the Buy-side runtime.
    ///
    /// # Errors
    ///
    /// Fails closed (never panics) when a [`ReplayEngine`] impl is wired to
    /// the wrong family's [`FoldModelSource`] — a configuration bug, not a
    /// data problem, but still must not crash the CPCV run.
    pub fn as_buy(&self) -> QuantResult<&dyn QuantModelRuntime> {
        match self {
            Self::BuyModel(runtime) => Ok(runtime.as_ref()),
            Self::BuyPortfolio(runtime) => Ok(runtime.model.as_ref()),
            Self::Sell(_) | Self::Policy(_) => Err(ResearchError::ValidationMethodology {
                detail: "expected a Buy-side FoldRuntime, got Sell".to_owned(),
            }
            .into()),
        }
    }

    /// Project to a complete portfolio fold estimator.
    pub fn as_portfolio_buy(&self) -> QuantResult<&PurgedPortfolioFoldRuntime> {
        match self {
            Self::BuyPortfolio(runtime) => Ok(runtime),
            Self::BuyModel(_) | Self::Sell(_) | Self::Policy(_) => {
                Err(ResearchError::ValidationMethodology {
                    detail: "expected a portfolio Buy FoldRuntime".to_owned(),
                }
                .into())
            }
        }
    }

    /// Project to the Sell-side runtime.
    ///
    /// # Errors
    ///
    /// See [`Self::as_buy`].
    pub fn as_sell(&self) -> QuantResult<&dyn SellScorerRuntime> {
        match self {
            Self::Sell(runtime) => Ok(runtime.as_ref()),
            Self::BuyModel(_) | Self::BuyPortfolio(_) | Self::Policy(_) => {
                Err(ResearchError::ValidationMethodology {
                    detail: "expected a Sell-side FoldRuntime, got Buy".to_owned(),
                }
                .into())
            }
        }
    }

    /// Project to a policy-fit candidate selection.
    pub fn as_policy(&self) -> QuantResult<&PolicyFoldRuntime> {
        match self {
            Self::Policy(runtime) => Ok(runtime),
            Self::BuyModel(_) | Self::BuyPortfolio(_) | Self::Sell(_) => {
                Err(ResearchError::ValidationMethodology {
                    detail: "expected a Policy FoldRuntime, got a model runtime".to_owned(),
                }
                .into())
            }
        }
    }
}

/// Trains one fold's model, restricted to the groups `filter` selects.
///
/// Implementations close over the full underlying example set, or the full lot
/// set for Sell models, and project it down to `filter`'s rows before training —
/// this is the **only** family-specific seam in the entire CPCV pipeline.
pub trait FoldModelSource: Send + Sync {
    /// Train a fold-scoped model. Errors propagate (a fold that cannot train
    /// — e.g. too few resolved labels after purge — fails the whole CPCV run
    /// rather than silently skipping a path).
    fn train_fold(&self, request: FoldTrainingRequest<'_>) -> QuantResult<FoldRuntime>;
}

/// One (predicted score, realized outcome) pair contributed by one evaluated
/// candidate within a [`GroupEvaluation`].
///
/// Pooled across every group in a reconstructed path to compute that path's
/// rank IC (`build_path`) — a single population-level Spearman
/// correlation, not an average of small-sample per-group correlations
/// (statistically more robust, and the only definition that survives atomic
/// units with a single candidate, e.g. one Sell lot per group).
#[derive(Debug, Clone, Copy)]
pub struct RankObservation {
    /// The model's predicted score for this candidate.
    pub score: Decimal,
    /// The realized outcome actually observed for this candidate.
    pub realized: Decimal,
}

/// One test group's fold-level evaluation.
#[derive(Debug, Clone)]
pub struct GroupEvaluation {
    /// Index into the original `groups` slice this evaluation covers.
    pub group_index: usize,
    /// This group's contribution to the reconstructed path's return series
    /// (a fractional per-period return, family-specific in derivation:
    /// realized tick `PnL` / allocated capital for Buy-side, realized lot
    /// proceeds / cost basis for the Sell-side).
    pub return_value: Decimal,
    /// Allocation-independent calibration residual used by the scenario-model
    /// fit after complete OOS path reconstruction.
    pub scenario_residual: Option<Decimal>,
    /// Every (score, realized) pair this group's replay produced — zero for
    /// a group with nothing scored, one for a single-candidate atomic unit
    /// (e.g. one Sell lot's decision walk), many for a multi-candidate
    /// cross-section (e.g. one Buy `as_of` tick's ranked tokens). Pooled
    /// across the whole path by `build_path` to compute rank IC.
    pub rank_observations: Vec<RankObservation>,
    /// Executed cash turnover for this decision tick as a fraction of the
    /// frozen capital base. Portfolio CPCV supplies this only after a complete
    /// path-level stateful replay; allocation-independent evaluators use
    /// `None` rather than inventing a zero.
    pub executed_turnover: Option<Decimal>,
    /// Complete fold-local OOS inference/economic input. Buy portfolio replay
    /// carries this until φ-path reconstruction can run one self-financing
    /// account across the whole ordered path; all other families use `None`.
    pub portfolio_replay: Option<PrecomputedBacktestTick>,
}

/// Stateful economic result produced only after one complete φ-path has been
/// reconstructed from fold-local OOS evidence.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PathEconomicReplay {
    pub group_returns: Vec<Decimal>,
    pub executed_turnover: Decimal,
}

/// Evaluates a trained model against the groups `filter` selects, returning
/// one [`GroupEvaluation`] per selected group.
pub trait ReplayEngine: Send + Sync {
    /// Errors propagate for the same reason as [`FoldModelSource::train_fold`].
    fn evaluate(
        &self,
        model: &FoldRuntime,
        filter: &GroupRowFilter,
    ) -> QuantResult<Vec<GroupEvaluation>>;

    /// Optionally execute one stateful economic replay after the complete
    /// path's OOS groups have been assembled in timeline order. The default is
    /// appropriate only for allocation-independent evaluators whose atomic
    /// group returns require no cross-period capital state.
    fn replay_path(
        &self,
        _path_index: u32,
        _groups: &[TimelineGroup],
        _evaluations: &[&GroupEvaluation],
    ) -> QuantResult<Option<PathEconomicReplay>> {
        Ok(None)
    }
}

/// A full Combinatorial Purged Cross-Validation result: the reconstructed
/// path distribution plus the fold-level provenance needed for audit.
#[derive(Debug, Clone)]
pub struct BacktestPathSet {
    /// Pre-minted identity declared by the frozen run input.
    pub path_set_id: BacktestPathSetId,
    /// The `phi(N, k)` reconstructed complete paths.
    pub paths: Vec<BacktestPath>,
    /// `C(N, k)` — the number of folds run for audit visibility. This is not
    /// `paths.len`; see [`CpcvConfig::path_count`].
    pub combination_count: u64,
    /// Distribution of [`BacktestPath::sharpe`] across `paths`.
    pub sharpe_distribution: SharpeDistribution,
    /// Median of [`BacktestPath::target_rank_ic`] across `paths` — the
    /// hard `TargetRankIc` gate's data source (replacing the single-path number).
    pub median_target_rank_ic: Decimal,
}

/// Inputs to one CPCV run. `groups` must be sorted ascending by `as_of`
/// (the same invariant [`PurgedSplitter`] requires) — index `i` in every
/// [`GroupRowFilter`]/[`GroupEvaluation`] refers to `groups[i]`.
#[derive(Clone, Copy)]
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

    /// Execute only the fold combinations required to reconstruct one
    /// precommitted complete OOS path.
    ///
    /// Slot assignment is still derived from the full deterministic
    /// `C(N, k)` combination population. Every retained fold therefore uses
    /// the exact same test partitions, purged training population, estimator,
    /// and replay contract as [`CombinatorialPurgedBacktester::run`]. This is
    /// an exact projection of one path, not reduced-fold cross-validation.
    ///
    /// # Errors
    ///
    /// Returns [`ResearchError::ValidationMethodology`] when `path_index` is
    /// outside the configured phi-path population or when any selected fold
    /// cannot be trained, replayed, or reconstructed exactly.
    pub fn run_path(&self, request: CpcvRequest<'_>, path_index: u32) -> QuantResult<BacktestPath> {
        let CpcvRequest {
            path_set_id: _,
            groups,
            cpcv,
            purge,
            fold_source,
            replay,
        } = request;
        let n_groups = usize::try_from(cpcv.n_groups)
            .map_err(|error| methodology(format!("cpcv.n_groups does not fit usize: {error}")))?;
        let k_test = usize::try_from(cpcv.k_test)
            .map_err(|error| methodology(format!("cpcv.k_test does not fit usize: {error}")))?;
        validate_config(groups.len(), cpcv, n_groups)?;

        let path_count = usize::try_from(cpcv.path_count()?)
            .map_err(|error| methodology(format!("CPCV path count does not fit usize: {error}")))?;
        let requested_path = usize::try_from(path_index)
            .map_err(|error| methodology(format!("CPCV path index does not fit usize: {error}")))?;
        if requested_path >= path_count {
            return Err(methodology(format!(
                "CPCV path index {path_index} is outside the configured 0..{path_count} population"
            )));
        }

        let partitions = partition_indices(groups.len(), n_groups)?;
        let combos = combinations(n_groups, k_test);
        let slots = assign_path_slots(
            n_groups,
            path_count,
            combos
                .iter()
                .enumerate()
                .map(|(fold_index, test_partitions)| (fold_index, test_partitions.as_slice())),
        )?;
        let path_slots = slots.get(requested_path).ok_or_else(|| {
            methodology(format!(
                "CPCV path index {path_index} disappeared after slot assignment"
            ))
        })?;
        let trial_path = cpcv.trial_path(path_index)?;
        let selected_folds = trial_path
            .combination_indices
            .iter()
            .map(|&fold_index| {
                usize::try_from(fold_index).map_err(|error| {
                    methodology(format!("CPCV fold index does not fit usize: {error}"))
                })
            })
            .collect::<QuantResult<Vec<_>>>()?;
        let fold_results = selected_folds
            .iter()
            .map(|&fold_index| {
                let test_partitions = combos.get(fold_index).ok_or_else(|| {
                    methodology(format!(
                        "path {path_index} references missing fold {fold_index}"
                    ))
                })?;
                run_fold(
                    groups,
                    &purge,
                    &partitions,
                    fold_index,
                    test_partitions,
                    fold_source,
                    replay,
                )
                .map(|fold| (fold_index, fold))
            })
            .collect::<Vec<_>>();
        let mut folds = BTreeMap::new();
        for result in fold_results {
            let (fold_index, fold) = result?;
            folds.insert(fold_index, fold);
        }
        reconstruct_path(
            groups,
            path_index,
            &partitions,
            path_slots,
            replay,
            |fold_index| folds.get(&fold_index),
        )
    }
}

/// A contiguous, roughly-equal partition of `groups`'s indices into
/// `n_groups` buckets. Contiguous (not interleaved) so a partition's own
/// interval span stays tight for purge/embargo purposes.
fn partition_indices(group_count: usize, n_groups: usize) -> QuantResult<Vec<Range<usize>>> {
    (0..n_groups)
        .map(|partition| {
            let start = group_count
                .checked_mul(partition)
                .ok_or_else(|| methodology("CPCV partition start overflowed usize".to_owned()))?
                / n_groups;
            let next_partition = partition
                .checked_add(1)
                .ok_or_else(|| methodology("CPCV partition index overflowed usize".to_owned()))?;
            let end = group_count
                .checked_mul(next_partition)
                .ok_or_else(|| methodology("CPCV partition end overflowed usize".to_owned()))?
                / n_groups;
            Ok(start..end)
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
        let n_groups = usize::try_from(cpcv.n_groups)
            .map_err(|error| methodology(format!("cpcv.n_groups does not fit usize: {error}")))?;
        let k_test = usize::try_from(cpcv.k_test)
            .map_err(|error| methodology(format!("cpcv.k_test does not fit usize: {error}")))?;
        validate_config(groups.len(), cpcv, n_groups)?;

        let partitions = partition_indices(groups.len(), n_groups)?;
        let combos = combinations(n_groups, k_test);

        let fold_results: Vec<QuantResult<FoldResult>> = combos
            .par_iter()
            .enumerate()
            .map(|(combination_index, test_partitions)| {
                run_fold(
                    groups,
                    &purge,
                    &partitions,
                    combination_index,
                    test_partitions,
                    fold_source,
                    replay,
                )
            })
            .collect();
        let mut folds = Vec::with_capacity(fold_results.len());
        for result in fold_results {
            folds.push(result?);
        }

        let path_count = usize::try_from(cpcv.path_count()?)
            .map_err(|error| methodology(format!("CPCV path count does not fit usize: {error}")))?;
        let paths = reconstruct_paths(groups, n_groups, path_count, &partitions, &folds, replay)?;
        let sharpe_distribution = sharpe_distribution(&paths)?;
        let median_target_rank_ic =
            median(&paths.iter().map(|p| p.target_rank_ic).collect::<Vec<_>>())?;

        Ok(BacktestPathSet {
            path_set_id,
            paths,
            combination_count: cpcv.combination_count()?,
            sharpe_distribution,
            median_target_rank_ic,
        })
    }
}

fn run_fold(
    groups: &[TimelineGroup],
    purge: &PurgeConfig,
    partitions: &[Range<usize>],
    combination_index: usize,
    test_partitions: &[usize],
    fold_source: &dyn FoldModelSource,
    replay: &dyn ReplayEngine,
) -> QuantResult<FoldResult> {
    let combination_index_u32 = u32::try_from(combination_index).map_err(|error| {
        methodology(format!("CPCV combination index does not fit u32: {error}"))
    })?;
    let test_group_indices = test_partitions
        .iter()
        .flat_map(|&partition| partitions[partition].clone())
        .collect::<Vec<_>>();
    let split = DefaultPurgedSplitter::new().split(groups, &test_group_indices, purge)?;
    let training_filter = GroupRowFilter {
        group_indices: split.train_indices,
    };
    let test_filter = GroupRowFilter {
        group_indices: split.test_indices,
    };
    let model = fold_source.train_fold(FoldTrainingRequest {
        identity: FoldTrainingIdentity::Validation {
            combination_index: combination_index_u32,
            test_partitions,
            test_groups: &test_filter.group_indices,
        },
        filter: &training_filter,
    })?;
    let mut evaluations = replay.evaluate(&model, &test_filter)?;
    evaluations.sort_unstable_by_key(|evaluation| evaluation.group_index);
    if evaluations.len() != test_filter.group_indices.len()
        || evaluations
            .iter()
            .zip(&test_filter.group_indices)
            .any(|(evaluation, &expected)| evaluation.group_index != expected)
    {
        return Err(methodology(format!(
            "CPCV replay returned {} evaluations for {} exact test groups",
            evaluations.len(),
            test_filter.group_indices.len()
        )));
    }
    Ok(FoldResult {
        test_partitions: test_partitions.to_vec(),
        evaluations,
    })
}

fn validate_config(group_count: usize, cpcv: CpcvConfig, n_groups: usize) -> QuantResult<()> {
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
    if group_count < n_groups {
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
    partitions: &[Range<usize>],
    folds: &[FoldResult],
    replay: &dyn ReplayEngine,
) -> QuantResult<Vec<BacktestPath>> {
    let slots = assign_path_slots(
        n_groups,
        path_count,
        folds
            .iter()
            .enumerate()
            .map(|(fold_index, fold)| (fold_index, fold.test_partitions.as_slice())),
    )?;
    slots
        .iter()
        .enumerate()
        .map(|(path_index, path_slots)| {
            let path_index = u32::try_from(path_index).map_err(|error| {
                methodology(format!("CPCV path index does not fit u32: {error}"))
            })?;
            reconstruct_path(
                groups,
                path_index,
                partitions,
                path_slots,
                replay,
                |fold_index| folds.get(fold_index),
            )
        })
        .collect()
}

fn assign_path_slots<'a>(
    n_groups: usize,
    path_count: usize,
    folds: impl IntoIterator<Item = (usize, &'a [usize])>,
) -> QuantResult<Vec<Vec<Option<usize>>>> {
    let mut slots = vec![vec![None; n_groups]; path_count];
    for (fold_index, test_partitions) in folds {
        for &partition in test_partitions {
            if partition >= n_groups {
                return Err(methodology(format!(
                    "fold {fold_index} references partition {partition} outside 0..{n_groups}"
                )));
            }
            let Some(path) = slots.iter_mut().find(|path| path[partition].is_none()) else {
                return Err(ResearchError::ValidationMethodology {
                    detail: format!(
                        "phi-path reconstruction ran out of empty slots for partition {partition} \
                         at fold {fold_index} — this indicates a combinatorial-symmetry bug, not a \
                         data problem"
                    ),
                }
                .into());
            };
            path[partition] = Some(fold_index);
        }
    }
    Ok(slots)
}

fn reconstruct_path<'a>(
    groups: &[TimelineGroup],
    path_index: u32,
    partitions: &[Range<usize>],
    path_slots: &[Option<usize>],
    replay: &dyn ReplayEngine,
    fold_at: impl Fn(usize) -> Option<&'a FoldResult>,
) -> QuantResult<BacktestPath> {
    if path_slots.len() != partitions.len() {
        return Err(methodology(format!(
            "path {path_index} has {} partition slots, expected {}",
            path_slots.len(),
            partitions.len()
        )));
    }
    let mut per_group: Vec<Option<&GroupEvaluation>> = vec![None; groups.len()];
    for (partition, &fold_index) in path_slots.iter().enumerate() {
        let Some(fold_index) = fold_index else {
            return Err(ResearchError::ValidationMethodology {
                detail: format!("path {path_index} left partition {partition} unfilled"),
            }
            .into());
        };
        let fold = fold_at(fold_index).ok_or_else(|| ResearchError::ValidationMethodology {
            detail: format!("path {path_index} references unavailable fold {fold_index}"),
        })?;
        let partition_range = partitions[partition].clone();
        let start = fold
            .evaluations
            .binary_search_by_key(&partition_range.start, |evaluation| evaluation.group_index)
            .map_err(|_| ResearchError::ValidationMethodology {
                detail: format!(
                    "fold {fold_index} has no evaluation for partition {partition} start {}",
                    partition_range.start
                ),
            })?;
        let end = start.checked_add(partition_range.len()).ok_or_else(|| {
            methodology("CPCV partition evaluation boundary overflowed usize".to_owned())
        })?;
        let evaluations = fold.evaluations.get(start..end).ok_or_else(|| {
            ResearchError::ValidationMethodology {
                detail: format!(
                    "fold {fold_index} has too few evaluations for partition {partition}"
                ),
            }
        })?;
        for (group_index, evaluation) in partition_range.zip(evaluations) {
            if evaluation.group_index != group_index {
                return Err(ResearchError::ValidationMethodology {
                    detail: format!(
                        "fold {fold_index} evaluation order diverged at group {group_index}: got {}",
                        evaluation.group_index
                    ),
                }
                .into());
            }
            per_group[group_index] = Some(evaluation);
        }
    }
    let evaluations = per_group
        .iter()
        .enumerate()
        .map(|(group_index, evaluation)| {
            evaluation.ok_or_else(|| {
                QuantError::from(ResearchError::ValidationMethodology {
                    detail: format!("group {group_index} was never evaluated in path {path_index}"),
                })
            })
        })
        .collect::<QuantResult<Vec<_>>>()?;
    let economic_replay = replay.replay_path(path_index, groups, &evaluations)?;
    build_path(path_index, groups, &evaluations, economic_replay)
}

fn build_path(
    path_index: u32,
    groups: &[TimelineGroup],
    evaluations: &[&GroupEvaluation],
    economic_replay: Option<PathEconomicReplay>,
) -> QuantResult<BacktestPath> {
    if evaluations.len() != groups.len() {
        return Err(methodology(format!(
            "path {path_index} has {} ordered evaluations for {} groups",
            evaluations.len(),
            groups.len()
        )));
    }
    let (group_returns, path_turnover) = if let Some(replay) = economic_replay {
        if replay.group_returns.len() != evaluations.len() {
            return Err(methodology(format!(
                "path {path_index} stateful replay returned {} periods for {} groups",
                replay.group_returns.len(),
                evaluations.len()
            )));
        }
        (replay.group_returns, Some(replay.executed_turnover))
    } else {
        let turnover = evaluations
            .iter()
            .map(|evaluation| evaluation.executed_turnover)
            .collect::<Option<Vec<_>>>()
            .map(|values| stats::mean(&values).round_dp(RESEARCH_DECIMAL_SCALE));
        (
            evaluations
                .iter()
                .map(|evaluation| evaluation.return_value)
                .collect(),
            turnover,
        )
    };
    let mut scenario_residuals = Vec::with_capacity(evaluations.len());
    let mut pooled_scores = Vec::new();
    let mut pooled_realized = Vec::new();
    for evaluation in evaluations {
        scenario_residuals.push(evaluation.scenario_residual);
        for observation in &evaluation.rank_observations {
            pooled_scores.push(observation.score);
            pooled_realized.push(observation.realized);
        }
    }

    let sharpe = sharpe_ratio(&group_returns, Decimal::ONE).round_dp(RESEARCH_DECIMAL_SCALE);
    // A single population-level Spearman correlation over every (score,
    // realized) pair pooled across the whole path — not an average of
    // small-sample per-group correlations. This is the only definition that
    // is both statistically sound for a multi-candidate cross-section
    // (Buy-side) *and* well-defined for a single-candidate atomic unit (one
    // Sell lot per group, where a per-group correlation is structurally
    // undefined).
    let target_rank_ic =
        stats::spearman(&pooled_scores, &pooled_realized).round_dp(RESEARCH_DECIMAL_SCALE);
    let max_drawdown = max_drawdown_from_returns(&group_returns);
    let tail_loss = tail_loss_from_returns(&group_returns, Decimal::new(10, 2))?;
    Ok(BacktestPath {
        path_index,
        decision_times: groups.iter().map(|group| group.decision_at).collect(),
        group_returns,
        scenario_residuals,
        sharpe,
        target_rank_ic,
        max_drawdown,
        tail_loss,
        turnover: path_turnover,
    })
}

/// Maximum peak-to-trough drawdown of the cumulative-return curve built from
/// total-budget-normalized `returns` (non-compounding cumulative sum,
/// consistent with [`crate::backtest::metrics::max_drawdown`]'s convention).
/// The result is bounded to `[0, 1]`; `0` for an empty or monotonically
/// non-decreasing series.
fn max_drawdown_from_returns(returns: &[Decimal]) -> Decimal {
    let mut cumulative = Decimal::ZERO;
    let mut peak = Decimal::ZERO;
    let mut max_dd = Decimal::ZERO;
    for &r in returns {
        cumulative += r;
        peak = peak.max(cumulative);
        max_dd = max_dd.max(peak - cumulative);
    }
    max_dd
        .clamp(Decimal::ZERO, Decimal::ONE)
        .round_dp(RESEARCH_DECIMAL_SCALE)
}

/// Mean return of the worst `quantile` fraction of `returns`. Mirrors
/// [`crate::backtest::metrics::tail_loss`]'s convention over an abstract
/// return series rather than [`crate::backtest::SampleOutcome`].
fn tail_loss_from_returns(returns: &[Decimal], quantile: Decimal) -> QuantResult<Decimal> {
    if returns.is_empty() {
        return Ok(Decimal::ZERO);
    }
    if quantile <= Decimal::ZERO || quantile > Decimal::ONE {
        return Err(methodology(format!(
            "tail-loss quantile must be in (0, 1], got {quantile}"
        )));
    }
    let mut sorted = returns.to_vec();
    sorted.sort();
    let n = sorted.len();
    let n_u64 = u64::try_from(n)
        .map_err(|error| methodology(format!("return count does not fit u64: {error}")))?;
    let raw = Decimal::from(n_u64)
        .checked_mul(quantile)
        .ok_or_else(|| methodology("tail-loss quantile multiplication overflowed".to_owned()))?
        .ceil();
    let take = raw
        .to_u64()
        .ok_or_else(|| methodology(format!("tail-loss count {raw} does not fit u64")))?;
    let take = usize::try_from(take)
        .map_err(|error| methodology(format!("tail-loss count does not fit usize: {error}")))?;
    if take == 0 || take > n {
        return Err(methodology(format!(
            "tail-loss count {take} is outside 1..={n}"
        )));
    }
    Ok(stats::mean(&sorted[..take]).round_dp(RESEARCH_DECIMAL_SCALE))
}

fn sharpe_distribution(paths: &[BacktestPath]) -> QuantResult<SharpeDistribution> {
    let mut sharpes: Vec<Decimal> = paths.iter().map(|p| p.sharpe).collect();
    let mut max_drawdowns: Vec<Decimal> = paths.iter().map(|path| path.max_drawdown).collect();
    let mut tail_losses: Vec<Decimal> = paths.iter().map(|path| path.tail_loss).collect();
    let mut turnovers = paths
        .iter()
        .map(|path| path.turnover)
        .collect::<Option<Vec<_>>>();
    sharpes.sort();
    max_drawdowns.sort();
    tail_losses.sort();
    if let Some(values) = &mut turnovers {
        values.sort();
    }
    Ok(SharpeDistribution {
        min: percentile(&sharpes, Decimal::ZERO)?,
        p25: percentile(&sharpes, Decimal::new(25, 2))?,
        median: percentile(&sharpes, Decimal::new(5, 1))?,
        p75: percentile(&sharpes, Decimal::new(75, 2))?,
        max: percentile(&sharpes, Decimal::ONE)?,
        median_max_drawdown: Some(percentile(&max_drawdowns, Decimal::new(5, 1))?),
        median_tail_loss: Some(percentile(&tail_losses, Decimal::new(5, 1))?),
        median_turnover: turnovers
            .as_deref()
            .map(|values| percentile(values, Decimal::new(5, 1)))
            .transpose()?,
        baseline_uplift: None,
    })
}

/// `sorted` must already be ascending. Nearest-rank percentile (`0` = min,
/// `1` = max); `0` for an empty series.
fn percentile(sorted: &[Decimal], fraction: Decimal) -> QuantResult<Decimal> {
    if sorted.is_empty() {
        return Ok(Decimal::ZERO);
    }
    if !(Decimal::ZERO..=Decimal::ONE).contains(&fraction) {
        return Err(methodology(format!(
            "percentile fraction must be in [0, 1], got {fraction}"
        )));
    }
    let last_index = sorted
        .len()
        .checked_sub(1)
        .ok_or_else(|| methodology("non-empty percentile input has no last index".to_owned()))?;
    let last_u64 = u64::try_from(last_index)
        .map_err(|error| methodology(format!("percentile index does not fit u64: {error}")))?;
    let last = Decimal::from(last_u64);
    let index = (last * fraction)
        .round()
        .to_u64()
        .ok_or_else(|| methodology("percentile index does not fit u64".to_owned()))?;
    let index = usize::try_from(index)
        .map_err(|error| methodology(format!("percentile index does not fit usize: {error}")))?;
    sorted
        .get(index)
        .copied()
        .ok_or_else(|| methodology(format!("percentile index {index} is out of bounds")))
}

fn median(values: &[Decimal]) -> QuantResult<Decimal> {
    if values.is_empty() {
        return Ok(Decimal::ZERO);
    }
    let mut sorted = values.to_vec();
    sorted.sort();
    percentile(&sorted, Decimal::new(5, 1))
}

fn methodology(detail: String) -> QuantError {
    ResearchError::ValidationMethodology { detail }.into()
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use async_trait::async_trait;
    use chrono::{TimeZone, Utc};
    use quant_pivot_error::QuantResult;
    use quant_pivot_models::types::{BacktestPathSetId, ContentHash, ModelVersionId};
    use rust_decimal::Decimal;
    use rust_decimal_macros::dec;

    use super::{
        CombinatorialPurgedBacktester, CpcvConfig, CpcvRequest,
        DefaultCombinatorialPurgedBacktester, FoldModelSource, FoldRuntime, FoldTrainingRequest,
        GroupEvaluation, GroupRowFilter, PathEconomicReplay, RankObservation, ReplayEngine,
        max_drawdown_from_returns,
    };
    use crate::{
        features::FeatureName,
        model::runtime::{ModelFamily, ModelRuntimeInput, ModelRuntimeOutput, QuantModelRuntime},
        validation::purge::{PurgeConfig, TimelineGroup},
    };

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
            ContentHash::parse(&format!("blake3:{}", "0".repeat(64))).expect("hash")
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
        fn train_fold(&self, _request: FoldTrainingRequest<'_>) -> QuantResult<FoldRuntime> {
            Ok(FoldRuntime::BuyModel(Box::new(StubRuntime)))
        }
    }

    struct CountingFoldSource {
        fits: AtomicUsize,
    }

    impl CountingFoldSource {
        const fn new() -> Self {
            Self {
                fits: AtomicUsize::new(0),
            }
        }

        fn fits(&self) -> usize {
            self.fits.load(Ordering::Relaxed)
        }
    }

    impl FoldModelSource for CountingFoldSource {
        fn train_fold(&self, _request: FoldTrainingRequest<'_>) -> QuantResult<FoldRuntime> {
            self.fits.fetch_add(1, Ordering::Relaxed);
            Ok(FoldRuntime::BuyModel(Box::new(StubRuntime)))
        }
    }

    /// Deterministic per-group return `= group_index as bps / 10_000`, so
    /// path reconstruction correctness can be checked exactly.
    struct DeterministicReplay;
    impl ReplayEngine for DeterministicReplay {
        fn evaluate(
            &self,
            _model: &FoldRuntime,
            filter: &GroupRowFilter,
        ) -> QuantResult<Vec<GroupEvaluation>> {
            Ok(filter
                .group_indices
                .iter()
                .map(|&group_index| GroupEvaluation {
                    group_index,
                    return_value: Decimal::from(u64::try_from(group_index).unwrap_or(0))
                        / Decimal::from(10_000),
                    scenario_residual: None,
                    rank_observations: vec![RankObservation {
                        score: dec!(1),
                        realized: dec!(1),
                    }],
                    executed_turnover: None,
                    portfolio_replay: None,
                })
                .collect())
        }
    }

    fn groups(n: i64) -> Vec<TimelineGroup> {
        (0..n)
            .map(|h| {
                let as_of = Utc.timestamp_opt(1_700_000_000 + h * 3_600, 0).unwrap();
                TimelineGroup {
                    decision_at: as_of,
                    label_horizon_end: as_of,
                }
            })
            .collect()
    }

    #[test]
    fn drawdown_clamps_budget() {
        assert_eq!(
            max_drawdown_from_returns(&[dec!(-0.6), dec!(-0.6)]),
            Decimal::ONE
        );
        assert_eq!(
            max_drawdown_from_returns(&[dec!(0.2), dec!(-0.1), dec!(0.3)]),
            dec!(0.1)
        );
    }

    #[test]
    fn cpcv_count_matches_formula() {
        // phi(8, 2) = C(7, 1) = 7, NOT C(8,2) = 28 (the original doc's error).
        assert_eq!(
            CpcvConfig {
                n_groups: 8,
                k_test: 2
            }
            .path_count()
            .expect("path count"),
            7
        );
        assert_eq!(
            CpcvConfig {
                n_groups: 8,
                k_test: 2
            }
            .combination_count()
            .expect("combination count"),
            28
        );
        // phi(6, 2) = C(5, 1) = 5 (matches the stats.stackexchange worked example).
        assert_eq!(
            CpcvConfig {
                n_groups: 6,
                k_test: 2
            }
            .path_count()
            .expect("path count"),
            5
        );
    }

    #[test]
    fn combinatorial_count_rejected_saturated() {
        let config = CpcvConfig {
            n_groups: 100,
            k_test: 50,
        };
        assert!(config.path_count().is_err());
        assert!(config.combination_count().is_err());
    }

    #[test]
    fn cpcv_produces_not_paths() {
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
    fn cpcv_reconstruction_covers_once() {
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
    fn projected_paths_match_full() {
        let groups = groups(80);
        let cpcv = CpcvConfig {
            n_groups: 8,
            k_test: 3,
        };
        let full_source = CountingFoldSource::new();
        let full = DefaultCombinatorialPurgedBacktester::new()
            .run(CpcvRequest {
                path_set_id: BacktestPathSetId::from_v7(),
                groups: &groups,
                cpcv,
                purge: PurgeConfig::pct_only(dec!(0)),
                fold_source: &full_source,
                replay: &DeterministicReplay,
            })
            .expect("full CPCV run");
        assert_eq!(full_source.fits(), 56);
        assert_eq!(full.paths.len(), 21);
        let bound_path = cpcv.trial_path(0).expect("canonical trial path binding");
        assert_eq!(bound_path.path_index, 0);
        assert_eq!(bound_path.combination_indices.len(), 6);

        for expected in &full.paths {
            let projected_source = CountingFoldSource::new();
            let projected = DefaultCombinatorialPurgedBacktester::new()
                .run_path(
                    CpcvRequest {
                        path_set_id: BacktestPathSetId::from_v7(),
                        groups: &groups,
                        cpcv,
                        purge: PurgeConfig::pct_only(dec!(0)),
                        fold_source: &projected_source,
                        replay: &DeterministicReplay,
                    },
                    expected.path_index,
                )
                .expect("exact projected CPCV path");
            assert_eq!(&projected, expected);
            if expected.path_index == 0 {
                assert_eq!(
                    projected_source.fits(),
                    6,
                    "the canonical path must fit only its six unique folds"
                );
            }
        }
    }

    #[test]
    fn cpcv_turnover_crosses_folds() {
        struct ExecutedTurnoverReplay;

        impl ReplayEngine for ExecutedTurnoverReplay {
            fn evaluate(
                &self,
                _model: &FoldRuntime,
                filter: &GroupRowFilter,
            ) -> QuantResult<Vec<GroupEvaluation>> {
                Ok(filter
                    .group_indices
                    .iter()
                    .map(|&group_index| GroupEvaluation {
                        group_index,
                        return_value: Decimal::ZERO,
                        scenario_residual: None,
                        rank_observations: Vec::new(),
                        executed_turnover: Some(Decimal::ONE),
                        portfolio_replay: None,
                    })
                    .collect())
            }
        }

        let groups = groups(80);
        let result = DefaultCombinatorialPurgedBacktester::new()
            .run(CpcvRequest {
                path_set_id: BacktestPathSetId::from_v7(),
                groups: &groups,
                cpcv: CpcvConfig {
                    n_groups: 8,
                    k_test: 2,
                },
                purge: PurgeConfig::pct_only(Decimal::ZERO),
                fold_source: &StubFoldSource,
                replay: &ExecutedTurnoverReplay,
            })
            .expect("cpcv run");

        assert_eq!(
            result.sharpe_distribution.median_turnover,
            Some(Decimal::ONE)
        );
        assert!(
            result
                .paths
                .iter()
                .all(|path| path.turnover == Some(Decimal::ONE))
        );
    }

    #[test]
    fn replay_once_per_path() {
        struct StatefulReplay {
            path_calls: AtomicUsize,
        }

        impl ReplayEngine for StatefulReplay {
            fn evaluate(
                &self,
                _model: &FoldRuntime,
                filter: &GroupRowFilter,
            ) -> QuantResult<Vec<GroupEvaluation>> {
                Ok(filter
                    .group_indices
                    .iter()
                    .map(|&group_index| GroupEvaluation {
                        group_index,
                        return_value: Decimal::ZERO,
                        scenario_residual: None,
                        rank_observations: Vec::new(),
                        executed_turnover: None,
                        portfolio_replay: None,
                    })
                    .collect())
            }

            fn replay_path(
                &self,
                _path_index: u32,
                groups: &[TimelineGroup],
                evaluations: &[&GroupEvaluation],
            ) -> QuantResult<Option<PathEconomicReplay>> {
                assert_eq!(groups.len(), evaluations.len());
                self.path_calls.fetch_add(1, Ordering::Relaxed);
                Ok(Some(PathEconomicReplay {
                    group_returns: vec![dec!(0.001); groups.len()],
                    executed_turnover: dec!(0.25),
                }))
            }
        }

        let groups = groups(80);
        let replay = StatefulReplay {
            path_calls: AtomicUsize::new(0),
        };
        let result = DefaultCombinatorialPurgedBacktester::new()
            .run(CpcvRequest {
                path_set_id: BacktestPathSetId::from_v7(),
                groups: &groups,
                cpcv: CpcvConfig {
                    n_groups: 8,
                    k_test: 2,
                },
                purge: PurgeConfig::pct_only(Decimal::ZERO),
                fold_source: &StubFoldSource,
                replay: &replay,
            })
            .expect("stateful CPCV run");

        assert_eq!(replay.path_calls.load(Ordering::Relaxed), 7);
        assert!(
            result
                .paths
                .iter()
                .all(|path| path.turnover == Some(dec!(0.25)))
        );
    }

    #[test]
    fn cpcv_rejects_k_groups() {
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
    fn cpcv_never_calls_bookstore() {
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

    /// `build_path`'s rank IC is one population-level Spearman correlation
    /// over every group's pooled observations, not an average of per-group
    /// correlations — required for a single-candidate, one-lot-per-group
    /// atomic unit to ever report a non-zero rank IC at all,
    /// and a strictly more robust estimator for a multi-candidate one too.
    #[test]
    fn build_pools_not_mean() {
        struct SingleObservationReplay;
        impl ReplayEngine for SingleObservationReplay {
            fn evaluate(
                &self,
                _model: &FoldRuntime,
                filter: &GroupRowFilter,
            ) -> QuantResult<Vec<GroupEvaluation>> {
                // Each group contributes exactly one (score, realized) pair —
                // the Sell lot-per-group shape, where a per-group Spearman
                // correlation is structurally undefined (needs >= 2 points).
                Ok(filter
                    .group_indices
                    .iter()
                    .map(|&group_index| GroupEvaluation {
                        group_index,
                        return_value: Decimal::ZERO,
                        scenario_residual: None,
                        rank_observations: vec![RankObservation {
                            score: Decimal::from(group_index as u64),
                            realized: Decimal::from(group_index as u64),
                        }],
                        executed_turnover: None,
                        portfolio_replay: None,
                    })
                    .collect())
            }
        }

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
                replay: &SingleObservationReplay,
            })
            .expect("cpcv run");
        // A perfectly monotone (score, realized) pooled series ⇒ rank IC = 1,
        // which no single-observation-per-group per-group-Spearman scheme
        // could ever produce (every per-group Spearman would be 0/undefined).
        for path in &result.paths {
            assert_eq!(
                path.target_rank_ic,
                Decimal::ONE,
                "path {} target_rank_ic",
                path.path_index
            );
        }
        assert_eq!(result.median_target_rank_ic, Decimal::ONE);
    }

    /// [`FoldRuntime::as_sell`] must fail closed (never panic) when the
    /// concrete runtime is actually a Buy one — a configuration bug, not a
    /// data problem.
    #[test]
    fn fold_runtime_rejects_runtime() {
        let runtime = FoldRuntime::BuyModel(Box::new(StubRuntime));
        assert!(runtime.as_sell().is_err());
        assert!(runtime.as_buy().is_ok());
    }

    /// Classical (and future Sell lot) families reuse the same φ-path CPCV
    /// engine via `FoldModelSource` — path count is independent of trainer kind.
    #[test]
    fn cpcv_phi_paths_contract() {
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
