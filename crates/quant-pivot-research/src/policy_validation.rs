//! Leakage-aware statistical validation over executable policy replay rows.
//!
//! Candidate fills are computed by [`crate::policy_replay`] before entering
//! this module. Statistical selection operates only on the intersection where
//! every governed candidate has a terminal executable return, preventing a
//! weak candidate from appearing superior by silently dropping hard rows.

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{Arc, Mutex},
};

use chrono::{DateTime, Duration, Utc};
use quant_pivot_error::{QuantError, QuantResult, research::ResearchError};
use quant_pivot_models::{
    hashing::CanonicalDigest,
    types::{BacktestPathSetId, ContentHash, MarketId},
};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{
    backtest::metrics::sharpe_ratio,
    stats,
    validation::{
        BacktestPathSet, CombinatorialPurgedBacktester, CpcvConfig, CpcvRequest,
        DefaultCombinatorialPurgedBacktester, DsrInput, FoldModelSource, FoldRuntime,
        GroupEvaluation, GroupRowFilter, PboInput, PolicyFoldRuntime, PurgeConfig, RankObservation,
        ReplayEngine, TimelineGroup, TrialPerformanceMatrix, probability_of_backtest_overfitting,
    },
};

pub const POLICY_CPCV_GROUPS: u32 = 8;
pub const POLICY_CPCV_TEST_GROUPS: u32 = 3;
pub const POLICY_CPCV_COMBINATIONS: u64 = 56;
pub const POLICY_CPCV_PATHS: u64 = 21;
pub const POLICY_PBO_BLOCKS: u32 = 8;
pub const POLICY_BOOTSTRAP_REPLICATIONS: usize = 2_000;
/// Hash-bound policy-performance methodology. Bump whenever candidate support,
/// CPCV path selection, DSR/PBO inputs, ESS, or bootstrap semantics change.
pub const POLICY_PERFORMANCE_METHODOLOGY_VERSION: &str =
    "policy_performance_common_support_cpcv_dsr_pbo_v2";

/// One observation's terminal candidate vector. `None` is an explicit replay
/// gap and excludes this observation from every candidate's common support.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PolicyPerformanceObservation {
    pub observation_id: String,
    pub market_id: MarketId,
    pub decision_at: DateTime<Utc>,
    pub label_horizon_end: DateTime<Utc>,
    pub candidate_return_bps: Vec<Option<Decimal>>,
}

/// Immutable methodology input shared by Fit and independent Validate.
pub struct PolicyPerformanceRequest<'a> {
    pub candidate_ids: &'a [String],
    pub observations: &'a [PolicyPerformanceObservation],
    pub experiment_family_hash: &'a ContentHash,
    pub min_embargo_secs: u64,
    pub period_length: Duration,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PolicyCandidatePerformance {
    pub candidate_id: String,
    pub weighted_mean_return_bps: Decimal,
    pub sharpe_ratio: Decimal,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PolicyCpcvFoldAttempt {
    pub fold_index: u32,
    pub test_group_indices: Vec<usize>,
    pub selected_candidate_id: String,
    pub training_group_count: u64,
    pub test_group_count: u64,
    pub training_utility_bps: Decimal,
    pub test_utility_bps: Decimal,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PolicyCpcvPathEvidence {
    pub path_index: u32,
    pub group_returns: Vec<Decimal>,
    pub sharpe_ratio: Decimal,
    pub max_drawdown: Decimal,
    pub tail_loss: Decimal,
}

/// Complete statistical result for one cohort and latency scenario.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PolicyPerformanceSummary {
    pub sample_count: u64,
    pub common_sample_count: u64,
    pub common_candidate_support: Decimal,
    pub effective_sample_size: Decimal,
    pub selected_candidate_id: String,
    pub candidate_performance: Vec<PolicyCandidatePerformance>,
    pub cpcv_combination_count: u64,
    pub cpcv_paths: Vec<PolicyCpcvPathEvidence>,
    pub cpcv_folds: Vec<PolicyCpcvFoldAttempt>,
    pub deflated_sharpe_ratio: Decimal,
    pub dsr_benchmark_sharpe: Decimal,
    pub probability_of_backtest_overfitting: Decimal,
    pub lower_confidence_utility_bps: Decimal,
}

struct CommonObservation<'a> {
    source: &'a PolicyPerformanceObservation,
    returns: Vec<Decimal>,
    uniqueness_weight: Decimal,
}

#[derive(Clone)]
struct PolicyGroup {
    timeline: TimelineGroup,
    candidate_returns_bps: Vec<Decimal>,
    weight: Decimal,
}

struct PolicyValidationBasis<'a> {
    common: Vec<CommonObservation<'a>>,
    groups: Vec<PolicyGroup>,
    selected_candidate: usize,
    candidate_performance: Vec<PolicyCandidatePerformance>,
}

struct PolicySignificance {
    deflated_sharpe_ratio: Decimal,
    dsr_benchmark_sharpe: Decimal,
    probability_of_backtest_overfitting: Decimal,
    effective_sample_size: Decimal,
    lower_confidence_utility_bps: Decimal,
}

struct PolicyFoldSource<'a> {
    candidate_ids: &'a [String],
    groups: &'a [PolicyGroup],
}

impl FoldModelSource for PolicyFoldSource<'_> {
    fn train_fold(&self, filter: &GroupRowFilter) -> QuantResult<FoldRuntime> {
        let candidate_index = best_candidate(self.groups, &filter.group_indices)?;
        let utility = weighted_candidate_mean(self.groups, &filter.group_indices, candidate_index)?;
        Ok(FoldRuntime::Policy(PolicyFoldRuntime {
            candidate_index,
            candidate_id: self.candidate_ids[candidate_index].clone(),
            training_group_count: u64::try_from(filter.group_indices.len()).map_err(|error| {
                methodology(format!(
                    "policy training group count does not fit u64: {error}"
                ))
            })?,
            training_utility_bps: utility,
        }))
    }
}

struct PolicyReplay<'a> {
    groups: &'a [PolicyGroup],
    audit: Arc<Mutex<Vec<PolicyCpcvFoldAttempt>>>,
}

impl ReplayEngine for PolicyReplay<'_> {
    fn evaluate(
        &self,
        model: &FoldRuntime,
        filter: &GroupRowFilter,
    ) -> QuantResult<Vec<GroupEvaluation>> {
        let selected = model.as_policy()?;
        let test_utility =
            weighted_candidate_mean(self.groups, &filter.group_indices, selected.candidate_index)?;
        self.audit
            .lock()
            .map_err(|_| methodology("policy CPCV audit mutex is poisoned".to_owned()))?
            .push(PolicyCpcvFoldAttempt {
                fold_index: 0,
                test_group_indices: filter.group_indices.clone(),
                selected_candidate_id: selected.candidate_id.clone(),
                training_group_count: selected.training_group_count,
                test_group_count: u64::try_from(filter.group_indices.len()).map_err(|error| {
                    methodology(format!("policy test group count does not fit u64: {error}"))
                })?,
                training_utility_bps: selected.training_utility_bps,
                test_utility_bps: test_utility,
            });
        filter
            .group_indices
            .iter()
            .map(|&group_index| {
                let group = self.groups.get(group_index).ok_or_else(|| {
                    methodology(format!(
                        "policy replay group index {group_index} is invalid"
                    ))
                })?;
                let realized = group.candidate_returns_bps[selected.candidate_index];
                Ok(GroupEvaluation {
                    group_index,
                    return_value: realized / Decimal::from(10_000),
                    rank_observations: vec![RankObservation {
                        score: selected.training_utility_bps,
                        realized,
                    }],
                })
            })
            .collect()
    }
}

/// Compute CPCV, DSR, PBO, interval-uniqueness ESS, and market-clustered
/// bootstrap utility from one executable candidate matrix.
pub fn evaluate_policy_performance(
    request: &PolicyPerformanceRequest<'_>,
) -> QuantResult<PolicyPerformanceSummary> {
    validate_request(request)?;
    let basis = build_validation_basis(request)?;
    let (paths, folds) = run_policy_cpcv(request, &basis.groups)?;
    let significance = policy_significance(request, &basis, &paths)?;
    let sample_count = u64::try_from(request.observations.len())
        .map_err(|error| methodology(format!("policy sample count does not fit u64: {error}")))?;
    let common_sample_count = u64::try_from(basis.common.len()).map_err(|error| {
        methodology(format!(
            "policy common sample count does not fit u64: {error}"
        ))
    })?;
    Ok(PolicyPerformanceSummary {
        sample_count,
        common_sample_count,
        common_candidate_support: Decimal::from(common_sample_count) / Decimal::from(sample_count),
        effective_sample_size: significance.effective_sample_size,
        selected_candidate_id: request.candidate_ids[basis.selected_candidate].clone(),
        candidate_performance: basis.candidate_performance,
        cpcv_combination_count: paths.combination_count,
        cpcv_paths: paths
            .paths
            .into_iter()
            .map(|path| PolicyCpcvPathEvidence {
                path_index: path.path_index,
                group_returns: path.group_returns,
                sharpe_ratio: path.sharpe,
                max_drawdown: path.max_drawdown,
                tail_loss: path.tail_loss,
            })
            .collect(),
        cpcv_folds: folds,
        deflated_sharpe_ratio: significance.deflated_sharpe_ratio,
        dsr_benchmark_sharpe: significance.dsr_benchmark_sharpe,
        probability_of_backtest_overfitting: significance.probability_of_backtest_overfitting,
        lower_confidence_utility_bps: significance.lower_confidence_utility_bps,
    })
}

fn build_validation_basis<'a>(
    request: &PolicyPerformanceRequest<'a>,
) -> QuantResult<PolicyValidationBasis<'a>> {
    let weights = interval_uniqueness_weights(request.observations)?;
    let common = request
        .observations
        .iter()
        .zip(weights)
        .filter_map(|(observation, uniqueness_weight)| {
            observation
                .candidate_return_bps
                .iter()
                .copied()
                .collect::<Option<Vec<_>>>()
                .map(|returns| CommonObservation {
                    source: observation,
                    returns,
                    uniqueness_weight,
                })
        })
        .collect::<Vec<_>>();
    if common.is_empty() {
        return Err(methodology(
            "policy performance has zero common executable candidate rows".to_owned(),
        ));
    }
    let groups = build_groups(&common, request.candidate_ids.len())?;
    if groups.len()
        < usize::try_from(POLICY_CPCV_GROUPS).map_err(|error| {
            methodology(format!(
                "policy CPCV group constant does not fit usize: {error}"
            ))
        })?
    {
        return Err(methodology(format!(
            "policy CPCV requires at least {POLICY_CPCV_GROUPS} decision groups, got {}",
            groups.len()
        )));
    }
    let all_groups = (0..groups.len()).collect::<Vec<_>>();
    let selected_candidate = best_candidate(&groups, &all_groups)?;
    let candidate_performance = candidate_performance(request.candidate_ids, &groups)?;
    Ok(PolicyValidationBasis {
        common,
        groups,
        selected_candidate,
        candidate_performance,
    })
}

fn run_policy_cpcv(
    request: &PolicyPerformanceRequest<'_>,
    groups: &[PolicyGroup],
) -> QuantResult<(BacktestPathSet, Vec<PolicyCpcvFoldAttempt>)> {
    let audit = Arc::new(Mutex::new(Vec::with_capacity(
        usize::try_from(POLICY_CPCV_COMBINATIONS).map_err(|error| {
            methodology(format!(
                "policy CPCV combination count does not fit usize: {error}"
            ))
        })?,
    )));
    let fold_source = PolicyFoldSource {
        candidate_ids: request.candidate_ids,
        groups,
    };
    let replay = PolicyReplay {
        groups,
        audit: Arc::clone(&audit),
    };
    let cpcv = CpcvConfig {
        n_groups: POLICY_CPCV_GROUPS,
        k_test: POLICY_CPCV_TEST_GROUPS,
    };
    if cpcv.combination_count()? != POLICY_CPCV_COMBINATIONS
        || cpcv.path_count()? != POLICY_CPCV_PATHS
    {
        return Err(methodology(
            "policy CPCV constants do not resolve to 56 combinations and 21 paths".to_owned(),
        ));
    }
    let timeline = groups
        .iter()
        .map(|group| group.timeline)
        .collect::<Vec<_>>();
    let paths = DefaultCombinatorialPurgedBacktester::new().run(CpcvRequest {
        path_set_id: deterministic_path_set_id(request.experiment_family_hash),
        groups: &timeline,
        cpcv,
        purge: PurgeConfig {
            embargo_pct: Decimal::new(2, 2),
            min_embargo_secs: request.min_embargo_secs,
        },
        fold_source: &fold_source,
        replay: &replay,
    })?;
    drop(replay);
    let mut folds = Arc::try_unwrap(audit)
        .map_err(|_| methodology("policy CPCV audit still has live owners".to_owned()))?
        .into_inner()
        .map_err(|_| methodology("policy CPCV audit mutex is poisoned".to_owned()))?;
    folds.sort_by(|left, right| left.test_group_indices.cmp(&right.test_group_indices));
    for (index, fold) in folds.iter_mut().enumerate() {
        fold.fold_index = u32::try_from(index).map_err(|error| {
            methodology(format!("policy CPCV fold index does not fit u32: {error}"))
        })?;
    }
    if folds.len()
        != usize::try_from(POLICY_CPCV_COMBINATIONS).map_err(|error| {
            methodology(format!(
                "policy CPCV fold count does not fit usize: {error}"
            ))
        })?
        || paths.paths.len()
            != usize::try_from(POLICY_CPCV_PATHS).map_err(|error| {
                methodology(format!(
                    "policy CPCV path count does not fit usize: {error}"
                ))
            })?
    {
        return Err(methodology(format!(
            "policy CPCV produced {} folds and {} paths instead of 56/21",
            folds.len(),
            paths.paths.len()
        )));
    }
    Ok((paths, folds))
}

fn policy_significance(
    request: &PolicyPerformanceRequest<'_>,
    basis: &PolicyValidationBasis<'_>,
    paths: &BacktestPathSet,
) -> QuantResult<PolicySignificance> {
    let representative = paths
        .paths
        .iter()
        .min_by_key(|path| (path.sharpe - paths.sharpe_distribution.median).abs())
        .ok_or_else(|| methodology("policy CPCV produced no representative path".to_owned()))?;
    let candidate_sharpes = basis
        .candidate_performance
        .iter()
        .map(|candidate| candidate.sharpe_ratio)
        .collect::<Vec<_>>();
    let dsr = DsrInput {
        observed_sharpe: representative.sharpe,
        returns_period_count: u64::try_from(representative.group_returns.len()).map_err(
            |error| methodology(format!("policy DSR period count does not fit u64: {error}")),
        )?,
        period_length: request.period_length,
        skewness: stats::skewness(&representative.group_returns),
        kurtosis: stats::kurtosis(&representative.group_returns),
        trial_count: u32::try_from(request.candidate_ids.len()).map_err(|error| {
            methodology(format!("policy DSR trial count does not fit u32: {error}"))
        })?,
        trial_sharpe_variance: stats::variance(&candidate_sharpes),
    }
    .deflated_sharpe_ratio()?;
    let trial_matrix = TrialPerformanceMatrix::from_rows(
        basis
            .groups
            .iter()
            .map(|group| group.timeline.decision_at)
            .collect(),
        basis
            .groups
            .iter()
            .map(|group| {
                group
                    .candidate_returns_bps
                    .iter()
                    .map(|value| *value / Decimal::from(10_000))
                    .collect()
            })
            .collect(),
    )?;
    let pbo = probability_of_backtest_overfitting(
        &trial_matrix,
        &PboInput {
            block_count: POLICY_PBO_BLOCKS,
        },
    )?;
    let effective_sample_size = effective_sample_size(
        &basis
            .common
            .iter()
            .map(|observation| observation.uniqueness_weight)
            .collect::<Vec<_>>(),
    );
    let lower_confidence_utility_bps = clustered_bootstrap_lower_bound(
        &basis.common,
        basis.selected_candidate,
        request.experiment_family_hash,
    )?;
    Ok(PolicySignificance {
        effective_sample_size,
        deflated_sharpe_ratio: dsr.deflated_sharpe,
        dsr_benchmark_sharpe: dsr.benchmark_sharpe,
        probability_of_backtest_overfitting: pbo,
        lower_confidence_utility_bps,
    })
}

fn validate_request(request: &PolicyPerformanceRequest<'_>) -> QuantResult<()> {
    if request.candidate_ids.len() < 2 {
        return Err(methodology(
            "DSR/PBO policy validation requires at least two governed candidates".to_owned(),
        ));
    }
    if request.period_length <= Duration::zero() {
        return Err(methodology(
            "policy validation period length must be positive".to_owned(),
        ));
    }
    let mut candidate_ids = BTreeSet::new();
    if request
        .candidate_ids
        .iter()
        .any(|candidate| candidate.trim().is_empty() || !candidate_ids.insert(candidate))
    {
        return Err(methodology(
            "policy validation candidate ids must be non-empty and unique".to_owned(),
        ));
    }
    let mut observation_ids = BTreeSet::new();
    let mut prior = None;
    for observation in request.observations {
        let candidate_count_matches =
            observation.candidate_return_bps.len() == request.candidate_ids.len();
        let has_positive_horizon = observation.decision_at < observation.label_horizon_end;
        let unique_observation = observation_ids.insert(&observation.observation_id);
        let time_ordered = prior.is_none_or(|value| value <= observation.decision_at);
        if !candidate_count_matches || !has_positive_horizon || !unique_observation || !time_ordered
        {
            return Err(methodology(
                "policy performance observations are malformed or not time ordered".to_owned(),
            ));
        }
        prior = Some(observation.decision_at);
    }
    Ok(())
}

fn interval_uniqueness_weights(
    observations: &[PolicyPerformanceObservation],
) -> QuantResult<Vec<Decimal>> {
    let mut weights = vec![Decimal::ZERO; observations.len()];
    let mut by_market = BTreeMap::<&MarketId, Vec<usize>>::new();
    for (index, observation) in observations.iter().enumerate() {
        by_market
            .entry(&observation.market_id)
            .or_default()
            .push(index);
    }
    for indices in by_market.into_values() {
        let mut deltas = BTreeMap::<DateTime<Utc>, i64>::new();
        for &index in &indices {
            let observation = &observations[index];
            *deltas.entry(observation.decision_at).or_default() += 1;
            *deltas.entry(observation.label_horizon_end).or_default() -= 1;
        }
        let times = deltas.keys().copied().collect::<Vec<_>>();
        let mut concurrency = 0_i64;
        let mut integral = Decimal::ZERO;
        let mut prefix = BTreeMap::<DateTime<Utc>, Decimal>::new();
        for (position, time) in times.iter().copied().enumerate() {
            prefix.insert(time, integral);
            concurrency = concurrency
                .checked_add(deltas[&time])
                .ok_or_else(|| methodology("uniqueness concurrency overflow".to_owned()))?;
            if let Some(next) = times.get(position + 1).copied() {
                let millis = (next - time).num_milliseconds();
                if millis < 0 || concurrency < 0 {
                    return Err(methodology(
                        "uniqueness sweep has a negative span or concurrency".to_owned(),
                    ));
                }
                if concurrency > 0 {
                    integral += Decimal::from(millis) / Decimal::from(concurrency);
                }
            }
        }
        for index in indices {
            let observation = &observations[index];
            let duration_ms =
                (observation.label_horizon_end - observation.decision_at).num_milliseconds();
            let start = prefix.get(&observation.decision_at).ok_or_else(|| {
                methodology("uniqueness prefix is missing an interval start".to_owned())
            })?;
            let end = prefix.get(&observation.label_horizon_end).ok_or_else(|| {
                methodology("uniqueness prefix is missing an interval end".to_owned())
            })?;
            weights[index] = (*end - *start) / Decimal::from(duration_ms);
        }
    }
    Ok(weights)
}

fn effective_sample_size(weights: &[Decimal]) -> Decimal {
    let sum = weights.iter().copied().sum::<Decimal>();
    let squares = weights
        .iter()
        .map(|weight| *weight * *weight)
        .sum::<Decimal>();
    if squares <= Decimal::ZERO {
        Decimal::ZERO
    } else {
        (sum * sum / squares).round_dp(8)
    }
}

fn build_groups(
    common: &[CommonObservation<'_>],
    candidate_count: usize,
) -> QuantResult<Vec<PolicyGroup>> {
    let mut grouped = BTreeMap::<DateTime<Utc>, Vec<&CommonObservation<'_>>>::new();
    for observation in common {
        grouped
            .entry(observation.source.decision_at)
            .or_default()
            .push(observation);
    }
    grouped
        .into_iter()
        .map(|(decision_at, observations)| {
            let label_horizon_end = observations
                .iter()
                .map(|observation| observation.source.label_horizon_end)
                .max()
                .ok_or_else(|| methodology("policy group is unexpectedly empty".to_owned()))?;
            let weight = observations
                .iter()
                .map(|observation| observation.uniqueness_weight)
                .sum::<Decimal>();
            if weight <= Decimal::ZERO {
                return Err(methodology(
                    "policy group has non-positive uniqueness weight".to_owned(),
                ));
            }
            let candidate_returns_bps = (0..candidate_count)
                .map(|candidate| {
                    observations
                        .iter()
                        .map(|observation| {
                            observation.returns[candidate] * observation.uniqueness_weight
                        })
                        .sum::<Decimal>()
                        / weight
                })
                .collect();
            Ok(PolicyGroup {
                timeline: TimelineGroup {
                    decision_at,
                    label_horizon_end,
                },
                candidate_returns_bps,
                weight,
            })
        })
        .collect()
}

fn best_candidate(groups: &[PolicyGroup], group_indices: &[usize]) -> QuantResult<usize> {
    let candidate_count = groups
        .first()
        .map(|group| group.candidate_returns_bps.len())
        .ok_or_else(|| {
            methodology("cannot select a policy candidate from zero groups".to_owned())
        })?;
    (0..candidate_count)
        .map(|candidate| {
            weighted_candidate_mean(groups, group_indices, candidate)
                .map(|utility| (candidate, utility))
        })
        .collect::<QuantResult<Vec<_>>>()?
        .into_iter()
        .max_by(|left, right| left.1.cmp(&right.1).then_with(|| right.0.cmp(&left.0)))
        .map(|(candidate, _)| candidate)
        .ok_or_else(|| methodology("policy fold has no candidate utility".to_owned()))
}

fn weighted_candidate_mean(
    groups: &[PolicyGroup],
    group_indices: &[usize],
    candidate: usize,
) -> QuantResult<Decimal> {
    let mut weighted = Decimal::ZERO;
    let mut total_weight = Decimal::ZERO;
    for &group_index in group_indices {
        let group = groups.get(group_index).ok_or_else(|| {
            methodology(format!(
                "policy group index {group_index} is outside the matrix"
            ))
        })?;
        let value = group.candidate_returns_bps.get(candidate).ok_or_else(|| {
            methodology(format!(
                "policy candidate index {candidate} is outside the matrix"
            ))
        })?;
        weighted += *value * group.weight;
        total_weight += group.weight;
    }
    if total_weight <= Decimal::ZERO {
        return Err(methodology(
            "policy fold has no positive training/test weight after purge".to_owned(),
        ));
    }
    Ok((weighted / total_weight).round_dp(8))
}

fn candidate_performance(
    candidate_ids: &[String],
    groups: &[PolicyGroup],
) -> QuantResult<Vec<PolicyCandidatePerformance>> {
    let indices = (0..groups.len()).collect::<Vec<_>>();
    candidate_ids
        .iter()
        .enumerate()
        .map(|(candidate, candidate_id)| {
            let returns = groups
                .iter()
                .map(|group| group.candidate_returns_bps[candidate] / Decimal::from(10_000))
                .collect::<Vec<_>>();
            Ok(PolicyCandidatePerformance {
                candidate_id: candidate_id.clone(),
                weighted_mean_return_bps: weighted_candidate_mean(groups, &indices, candidate)?,
                sharpe_ratio: sharpe_ratio(&returns, Decimal::ONE),
            })
        })
        .collect()
}

fn deterministic_path_set_id(hash: &ContentHash) -> BacktestPathSetId {
    const NAMESPACE: Uuid = Uuid::from_u128(0x6bc1_1f75_8ca8_4f31_9be5_17ad_1170_0722);
    let canonical = hash.canonical_text();
    BacktestPathSetId::new(Uuid::new_v5(&NAMESPACE, canonical.as_bytes()))
}

fn clustered_bootstrap_lower_bound(
    common: &[CommonObservation<'_>],
    candidate: usize,
    seed_hash: &ContentHash,
) -> QuantResult<Decimal> {
    let mut clusters = BTreeMap::<&MarketId, Vec<&CommonObservation<'_>>>::new();
    for observation in common {
        clusters
            .entry(&observation.source.market_id)
            .or_default()
            .push(observation);
    }
    let clusters = clusters.into_values().collect::<Vec<_>>();
    if clusters.is_empty() {
        return Err(methodology(
            "market-cluster bootstrap has zero clusters".to_owned(),
        ));
    }
    let seed_digest =
        CanonicalDigest::content_hash_json(&(seed_hash, "market_cluster_bootstrap_v1", candidate))
            .map_err(QuantError::from)?;
    let mut seed = [0_u8; 8];
    seed.copy_from_slice(&seed_digest.as_bytes()[..8]);
    let mut state = u64::from_be_bytes(seed);
    let cluster_count = u64::try_from(clusters.len()).map_err(|error| {
        methodology(format!("bootstrap cluster count does not fit u64: {error}"))
    })?;
    let mut utilities = Vec::with_capacity(POLICY_BOOTSTRAP_REPLICATIONS);
    for _ in 0..POLICY_BOOTSTRAP_REPLICATIONS {
        let mut weighted = Decimal::ZERO;
        let mut weight = Decimal::ZERO;
        for _ in 0..clusters.len() {
            state = state
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1_442_695_040_888_963_407);
            let index = usize::try_from(state % cluster_count).map_err(|error| {
                methodology(format!(
                    "bootstrap cluster index does not fit usize: {error}"
                ))
            })?;
            for observation in &clusters[index] {
                weighted += observation.returns[candidate] * observation.uniqueness_weight;
                weight += observation.uniqueness_weight;
            }
        }
        if weight <= Decimal::ZERO {
            return Err(methodology(
                "bootstrap replicate has zero effective weight".to_owned(),
            ));
        }
        utilities.push((weighted / weight).round_dp(8));
    }
    utilities.sort();
    let index = POLICY_BOOTSTRAP_REPLICATIONS
        .checked_mul(5)
        .and_then(|value| value.checked_div(100))
        .ok_or_else(|| methodology("bootstrap percentile index overflow".to_owned()))?;
    utilities
        .get(index)
        .copied()
        .ok_or_else(|| methodology("bootstrap percentile is unavailable".to_owned()))
}

fn methodology(detail: String) -> QuantError {
    ResearchError::ValidationMethodology { detail }.into()
}

#[cfg(test)]
mod tests {
    use chrono::{Duration, TimeZone, Utc};
    use quant_pivot_models::types::{ContentHash, MarketId};
    use rust_decimal::{Decimal, prelude::FromPrimitive};

    use super::{
        POLICY_CPCV_COMBINATIONS, POLICY_CPCV_PATHS, PolicyPerformanceObservation,
        PolicyPerformanceRequest, evaluate_policy_performance,
    };

    fn hash() -> ContentHash {
        ContentHash::parse(&format!("blake3:{}", "7".repeat(64))).expect("hash")
    }

    fn observations() -> Vec<PolicyPerformanceObservation> {
        let start = Utc
            .with_ymd_and_hms(2026, 1, 1, 0, 0, 0)
            .single()
            .expect("time");
        (0..80)
            .map(|index| {
                let index_i64 = i64::from(index);
                let decision_at = start + Duration::hours(index_i64 * 6);
                let trend = Decimal::from_i64(index_i64 % 7 - 3).expect("decimal");
                PolicyPerformanceObservation {
                    observation_id: format!("observation-{index}"),
                    market_id: MarketId::new(format!("market-{}", index % 10)),
                    decision_at,
                    label_horizon_end: decision_at + Duration::hours(24),
                    candidate_return_bps: vec![
                        Some(Decimal::from(35) + trend),
                        Some(Decimal::from(5) - trend * Decimal::from(3)),
                        Some(Decimal::from(-10) + trend * Decimal::from(2)),
                    ],
                }
            })
            .collect()
    }

    #[test]
    fn produces_exact_56_paths() {
        let candidate_ids = vec!["stable".to_owned(), "noisy".to_owned(), "weak".to_owned()];
        let summary = evaluate_policy_performance(&PolicyPerformanceRequest {
            candidate_ids: &candidate_ids,
            observations: &observations(),
            experiment_family_hash: &hash(),
            min_embargo_secs: 3_600,
            period_length: Duration::hours(6),
        })
        .expect("performance");

        assert_eq!(summary.cpcv_combination_count, POLICY_CPCV_COMBINATIONS);
        assert_eq!(summary.cpcv_folds.len(), 56);
        assert_eq!(
            summary.cpcv_paths.len(),
            usize::try_from(POLICY_CPCV_PATHS).expect("usize")
        );
        assert!(
            summary
                .cpcv_paths
                .iter()
                .all(|path| path.group_returns.len() == 80)
        );
        assert_eq!(summary.selected_candidate_id, "stable");
        assert!(summary.effective_sample_size > Decimal::ZERO);
    }

    #[test]
    fn common_support_candidate_symmetric() {
        let mut rows = observations();
        rows[3].candidate_return_bps[2] = None;
        let candidate_ids = vec!["stable".to_owned(), "noisy".to_owned(), "weak".to_owned()];
        let summary = evaluate_policy_performance(&PolicyPerformanceRequest {
            candidate_ids: &candidate_ids,
            observations: &rows,
            experiment_family_hash: &hash(),
            min_embargo_secs: 3_600,
            period_length: Duration::hours(6),
        })
        .expect("performance");

        assert_eq!(summary.sample_count, 80);
        assert_eq!(summary.common_sample_count, 79);
        assert_eq!(
            summary.common_candidate_support,
            Decimal::from(79) / Decimal::from(80)
        );
    }

    #[test]
    fn pbo_requires_real_family() {
        let candidate_ids = vec!["only".to_owned()];
        assert!(
            evaluate_policy_performance(&PolicyPerformanceRequest {
                candidate_ids: &candidate_ids,
                observations: &observations()
                    .into_iter()
                    .map(|mut observation| {
                        observation.candidate_return_bps.truncate(1);
                        observation
                    })
                    .collect::<Vec<_>>(),
                experiment_family_hash: &hash(),
                min_embargo_secs: 3_600,
                period_length: Duration::hours(6),
            })
            .is_err()
        );
    }
}
