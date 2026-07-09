//! Governed learning-to-rank objective for weighted-model training.
//!
//! The trainer optimizes per-`as_of` cross-sections: ranking loss is computed
//! only within one query group, while tail and turnover use a deterministic
//! **`TopN` score-ranked equal-weight pseudo portfolio** (token-keyed) as an
//! optimization proxy. The backtest replay remains the authoritative
//! capital/allocation check.
//!
//! `RankIcWeightedRanknet` is a **simplex black-box surrogate** (`RankNet` pairs
//! weighted by the closed-form `RankIC` swap delta). It is **not** an `XGBoost` /
//! `LightGBM` `LambdaMART` λ-gradient implementation.

use std::collections::BTreeMap;

use quant_pivot_error::{QuantResult, research::ResearchError};
use quant_pivot_models::runtime_config::{
    RankLossKind, ResearchTrainingConfig, TrainingOptimizerKind,
};
use rust_decimal::{
    Decimal,
    prelude::{FromPrimitive, ToPrimitive},
};
use serde::{Deserialize, Serialize};

use crate::{backtest::metrics, precision::RESEARCH_DECIMAL_SCALE, stats};

const BPS_PER_UNIT_RETURN: i64 = 10_000;

/// Full governed objective snapshot frozen into model versions and artifacts.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TrainingObjectiveSpec {
    /// Ranking loss optimized within each `as_of` query group.
    pub rank_loss: RankLossKind,
    /// Simplex optimizer policy.
    pub optimizer: TrainingOptimizerKind,
    /// Weight on lower-tail portfolio-return penalty.
    pub lambda_tail: Decimal,
    /// Lower-tail fraction used by tail penalty.
    pub tail_fraction: Decimal,
    /// Weight on mean allocation turnover penalty.
    pub lambda_turnover: Decimal,
    /// L2 coefficient on `Σ weightᵢ²`.
    pub lambda_l2: Decimal,
    /// Truncation `k` for diagnostic NDCG@k (not part of the training loss).
    pub ndcg_k: u32,
    /// Truncation for `TopN` pseudo-portfolio used by tail/turnover penalties.
    pub pseudo_top_n: u32,
}

impl Default for TrainingObjectiveSpec {
    fn default() -> Self {
        Self {
            rank_loss: RankLossKind::default(),
            optimizer: TrainingOptimizerKind::default(),
            lambda_tail: Decimal::new(5, 1),
            tail_fraction: Decimal::new(10, 2),
            lambda_turnover: Decimal::new(2, 1),
            lambda_l2: Decimal::new(1, 2),
            ndcg_k: 20,
            pseudo_top_n: 20,
        }
    }
}

impl TrainingObjectiveSpec {
    /// Parse the governed runtime-config section into exact `Decimal` weights.
    pub fn from_runtime_config(config: &ResearchTrainingConfig) -> QuantResult<Self> {
        Ok(Self {
            rank_loss: config.rank_loss,
            optimizer: config.optimizer,
            lambda_tail: parse_non_negative_decimal(
                "research.training.lambda_tail",
                &config.lambda_tail.value,
            )?,
            tail_fraction: parse_tail_fraction(&config.tail_fraction.value)?,
            lambda_turnover: parse_non_negative_decimal(
                "research.training.lambda_turnover",
                &config.lambda_turnover.value,
            )?,
            lambda_l2: parse_non_negative_decimal(
                "research.training.lambda_l2",
                &config.lambda_l2.value,
            )?,
            ndcg_k: config.ndcg_k,
            pseudo_top_n: config.pseudo_top_n,
        })
    }
}

/// Component-level objective report stored in metrics/artifacts.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ObjectiveComponentReport {
    /// Mean rank loss across valid query groups.
    pub rank_loss: Decimal,
    /// Positive lower-tail loss over `TopN` pseudo-portfolio returns.
    pub tail_penalty: Decimal,
    /// Mean per-tick `TopN` pseudo-allocation turnover.
    pub turnover_penalty: Decimal,
    /// Raw `Σ weightᵢ²` L2 complexity.
    pub l2_penalty: Decimal,
    /// Weighted total loss minimized by the trainer.
    pub total_loss: Decimal,
    /// Number of cross-section groups evaluated for tail/turnover/L2 context.
    pub group_count: u64,
    /// Number of groups that contributed at least one label-discordant pair to
    /// the rank-loss mean (may be lower than [`Self::group_count`]).
    pub rank_loss_group_count: u64,
    /// Number of label-discordant pairs contributing to rank loss.
    pub pair_count: u64,
}

impl ObjectiveComponentReport {
    /// Objective value used by maximizers: larger is better.
    #[must_use]
    pub fn objective_value(&self) -> Decimal {
        -self.total_loss
    }

    /// Round Decimal fields for stable persisted metrics.
    #[must_use]
    pub fn rounded(&self) -> Self {
        Self {
            rank_loss: self.rank_loss.round_dp(RESEARCH_DECIMAL_SCALE),
            tail_penalty: self.tail_penalty.round_dp(RESEARCH_DECIMAL_SCALE),
            turnover_penalty: self.turnover_penalty.round_dp(RESEARCH_DECIMAL_SCALE),
            l2_penalty: self.l2_penalty.round_dp(RESEARCH_DECIMAL_SCALE),
            total_loss: self.total_loss.round_dp(RESEARCH_DECIMAL_SCALE),
            group_count: self.group_count,
            rank_loss_group_count: self.rank_loss_group_count,
            pair_count: self.pair_count,
        }
    }
}

/// Ranking diagnostics that are **not** part of the training loss.
///
/// Reported so `TopN` product quality (`NDCG@k`) and full-order `Rank IC` can be
/// audited even when the optimizer maximizes a `RankNet`-family surrogate.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RankingDiagnostics {
    /// Mean Spearman `Rank IC` across evaluated query groups.
    pub mean_rank_ic: Decimal,
    /// Mean NDCG@`ndcg_k` across evaluated query groups.
    pub mean_ndcg_at_k: Decimal,
    /// Truncation `k` used for NDCG.
    pub ndcg_k: u32,
    /// Number of groups contributing to the means.
    pub group_count: u64,
}

impl RankingDiagnostics {
    /// Round Decimal fields for stable persisted metrics.
    #[must_use]
    pub fn rounded(&self) -> Self {
        Self {
            mean_rank_ic: self.mean_rank_ic.round_dp(RESEARCH_DECIMAL_SCALE),
            mean_ndcg_at_k: self.mean_ndcg_at_k.round_dp(RESEARCH_DECIMAL_SCALE),
            ndcg_k: self.ndcg_k,
            group_count: self.group_count,
        }
    }
}

/// One reduced training row inside a same-`as_of` query group.
#[derive(Debug, Clone)]
pub(crate) struct SampleRow {
    /// Stable allocation key: `market_id:token_id` (never market-only).
    pub(crate) allocation_key: String,
    pub(crate) signed: Vec<Decimal>,
    pub(crate) label: Decimal,
}

impl SampleRow {
    /// The predicted ranking score for a weight vector.
    #[must_use]
    pub(crate) fn net(&self, weights: &[Decimal]) -> Decimal {
        self.signed.iter().zip(weights).map(|(s, w)| *s * *w).sum()
    }
}

/// Same-`as_of` query group for cross-sectional LTR.
///
/// Grouping by `as_of` happens when the trainer builds the dataset; once formed,
/// the evaluator only needs the rows (pair loss / pseudo portfolio).
#[derive(Debug, Clone)]
pub(crate) struct CrossSectionGroup {
    pub(crate) rows: Vec<SampleRow>,
}

/// Pure objective evaluator shared by coordinate search and optional argmin.
#[derive(Debug, Clone)]
pub(crate) struct ObjectiveEvaluator {
    spec: TrainingObjectiveSpec,
}

impl ObjectiveEvaluator {
    /// Construct an evaluator from a frozen objective snapshot.
    #[must_use]
    pub(crate) const fn new(spec: TrainingObjectiveSpec) -> Self {
        Self { spec }
    }

    /// Objective spec being evaluated.
    #[must_use]
    pub(crate) const fn spec(&self) -> &TrainingObjectiveSpec {
        &self.spec
    }

    /// Evaluate the complete loss decomposition for a weight vector.
    pub(crate) fn evaluate(
        &self,
        weights: &[Decimal],
        groups: &[CrossSectionGroup],
    ) -> QuantResult<ObjectiveComponentReport> {
        let (rank_loss, pair_count, rank_loss_group_count) = self.rank_loss(weights, groups)?;
        let tail_penalty = self.tail_penalty(weights, groups);
        let turnover_penalty = self.turnover_penalty(weights, groups);
        let l2_penalty: Decimal = weights.iter().map(|w| *w * *w).sum();
        let total_loss = rank_loss
            + self.spec.lambda_tail * tail_penalty
            + self.spec.lambda_turnover * turnover_penalty
            + self.spec.lambda_l2 * l2_penalty;
        Ok(ObjectiveComponentReport {
            rank_loss,
            tail_penalty,
            turnover_penalty,
            l2_penalty,
            total_loss,
            group_count: groups.len() as u64,
            rank_loss_group_count,
            pair_count,
        })
    }

    /// Compute ranking diagnostics (Rank IC + NDCG@k) for a weight vector.
    #[must_use]
    pub(crate) fn diagnostics(
        &self,
        weights: &[Decimal],
        groups: &[CrossSectionGroup],
    ) -> RankingDiagnostics {
        if groups.is_empty() {
            return RankingDiagnostics {
                mean_rank_ic: Decimal::ZERO,
                mean_ndcg_at_k: Decimal::ZERO,
                ndcg_k: self.spec.ndcg_k,
                group_count: 0,
            };
        }
        let mut rank_ics = Vec::with_capacity(groups.len());
        let mut ndcgs = Vec::with_capacity(groups.len());
        for group in groups {
            let scores: Vec<Decimal> = group.rows.iter().map(|row| row.net(weights)).collect();
            let labels: Vec<Decimal> = group.rows.iter().map(|row| row.label).collect();
            rank_ics.push(stats::spearman(&scores, &labels));
            ndcgs.push(ndcg_at_k(&scores, &labels, self.spec.ndcg_k as usize));
        }
        RankingDiagnostics {
            mean_rank_ic: stats::mean(&rank_ics),
            mean_ndcg_at_k: stats::mean(&ndcgs),
            ndcg_k: self.spec.ndcg_k,
            group_count: groups.len() as u64,
        }
        .rounded()
    }

    fn rank_loss(
        &self,
        weights: &[Decimal],
        groups: &[CrossSectionGroup],
    ) -> QuantResult<(Decimal, u64, u64)> {
        let mut group_losses = Vec::new();
        let mut pair_count = 0_u64;
        for group in groups {
            let scores: Vec<Decimal> = group.rows.iter().map(|row| row.net(weights)).collect();
            let labels: Vec<Decimal> = group.rows.iter().map(|row| row.label).collect();
            let score_ranks = stats::average_ranks(&scores);
            let label_ranks = stats::average_ranks(&labels);
            let n = group.rows.len();
            let mut group_loss = Decimal::ZERO;
            let mut group_pairs = 0_u64;
            for i in 0..n {
                for j in (i + 1)..n {
                    let label_diff = labels[i] - labels[j];
                    if label_diff.is_zero() {
                        continue;
                    }
                    let pair_weight = match self.spec.rank_loss {
                        RankLossKind::PairwiseRanknet => Decimal::ONE,
                        RankLossKind::RankIcWeightedRanknet => rank_ic_weighted_pair_weight(
                            score_ranks[i],
                            score_ranks[j],
                            label_ranks[i],
                            label_ranks[j],
                            n,
                        ),
                    };
                    if pair_weight <= Decimal::ZERO {
                        continue;
                    }
                    let sign = if label_diff > Decimal::ZERO {
                        Decimal::ONE
                    } else {
                        -Decimal::ONE
                    };
                    let score_diff =
                        decimal_to_f64("rank_loss.score_diff", (scores[i] - scores[j]) * sign)?;
                    let loss = ranknet_logistic_loss(score_diff)?;
                    group_loss += loss * pair_weight;
                    group_pairs = group_pairs.saturating_add(1);
                }
            }
            pair_count = pair_count.saturating_add(group_pairs);
            if group_pairs > 0 {
                group_losses.push(group_loss);
            }
        }
        let rank_loss_group_count = group_losses.len() as u64;
        if group_losses.is_empty() {
            return Ok((Decimal::ZERO, pair_count, rank_loss_group_count));
        }
        Ok((
            stats::mean(&group_losses),
            pair_count,
            rank_loss_group_count,
        ))
    }

    fn tail_penalty(&self, weights: &[Decimal], groups: &[CrossSectionGroup]) -> Decimal {
        if groups.is_empty() {
            return Decimal::ZERO;
        }
        let mut returns = Vec::with_capacity(groups.len());
        for group in groups {
            let allocations = topn_pseudo_allocations(weights, group, self.spec.pseudo_top_n);
            let group_return_bps: Decimal = group
                .rows
                .iter()
                .zip(allocations.iter())
                .map(|(row, allocation)| row.label * *allocation)
                .sum();
            returns.push(group_return_bps / Decimal::from(BPS_PER_UNIT_RETURN));
        }
        returns.sort();
        let raw = (Decimal::from(returns.len() as u64) * self.spec.tail_fraction).ceil();
        let take = raw.to_usize().unwrap_or(1).max(1).min(returns.len());
        let tail_mean = stats::mean(&returns[..take]);
        (-tail_mean).max(Decimal::ZERO)
    }

    fn turnover_penalty(&self, weights: &[Decimal], groups: &[CrossSectionGroup]) -> Decimal {
        let tick_weights: Vec<BTreeMap<String, Decimal>> = groups
            .iter()
            .map(|group| {
                let allocations = topn_pseudo_allocations(weights, group, self.spec.pseudo_top_n);
                let mut by_token = BTreeMap::new();
                for (row, allocation) in group.rows.iter().zip(allocations.iter()) {
                    if *allocation > Decimal::ZERO {
                        let entry = by_token
                            .entry(row.allocation_key.clone())
                            .or_insert(Decimal::ZERO);
                        *entry += *allocation;
                    }
                }
                by_token
            })
            .collect();
        metrics::turnover(&tick_weights)
    }
}

/// Closed-form `RankIC` contribution of swapping a pair's predicted ranks.
///
/// Uses `|Δr̂|_eff = max(|Δr̂|, 1)` so score-rank ties still contribute signal
/// for label-discordant pairs (avoids flat-score plateaus killing the loss).
#[must_use]
pub(crate) fn rank_ic_weighted_pair_weight(
    score_rank_a: Decimal,
    score_rank_b: Decimal,
    label_rank_a: Decimal,
    label_rank_b: Decimal,
    group_len: usize,
) -> Decimal {
    if group_len < 2 {
        return Decimal::ZERO;
    }
    let n = Decimal::from(group_len as u64);
    let denom = n * ((n * n) - Decimal::ONE);
    if denom.is_zero() {
        return Decimal::ZERO;
    }
    let score_sep = (score_rank_a - score_rank_b).abs().max(Decimal::ONE);
    let label_sep = (label_rank_a - label_rank_b).abs();
    Decimal::from(12) * score_sep * label_sep / denom
}

/// NDCG@k with graded relevance = `max(label, 0)` (bps-style labels).
///
/// Ideal ordering sorts by label descending. Returns 0 when IDCG is 0.
#[must_use]
pub(crate) fn ndcg_at_k(scores: &[Decimal], labels: &[Decimal], k: usize) -> Decimal {
    if scores.len() != labels.len() || scores.is_empty() || k == 0 {
        return Decimal::ZERO;
    }
    let take = k.min(scores.len());
    let mut by_score: Vec<(usize, Decimal)> = scores
        .iter()
        .enumerate()
        .map(|(idx, score)| (idx, *score))
        .collect();
    by_score.sort_by(|a, b| b.1.cmp(&a.1).then(a.0.cmp(&b.0)));
    let dcg_rels: Vec<Decimal> = by_score[..take]
        .iter()
        .map(|(idx, _)| labels[*idx].max(Decimal::ZERO))
        .collect();
    let dcg = discounted_gain(&dcg_rels);
    let mut ideal: Vec<Decimal> = labels
        .iter()
        .map(|label| (*label).max(Decimal::ZERO))
        .collect();
    ideal.sort_by(|a, b| b.cmp(a));
    let idcg = discounted_gain(&ideal[..take]);
    if idcg.is_zero() {
        return Decimal::ZERO;
    }
    (dcg / idcg).clamp(Decimal::ZERO, Decimal::ONE)
}

fn discounted_gain(relevances: &[Decimal]) -> Decimal {
    let mut total = Decimal::ZERO;
    for (pos, rel) in relevances.iter().enumerate() {
        if rel.is_zero() {
            continue;
        }
        // log2(pos+2) = ln(pos+2)/ln(2)
        let denom = decimal_log2(Decimal::from((pos + 2) as u64));
        if denom.is_zero() {
            continue;
        }
        total += *rel / denom;
    }
    total
}

fn decimal_log2(value: Decimal) -> Decimal {
    let Some(f) = value.to_f64() else {
        return Decimal::ZERO;
    };
    if f <= 0.0 {
        return Decimal::ZERO;
    }
    Decimal::from_f64(f.log2()).unwrap_or(Decimal::ZERO)
}

/// `TopN` score-ranked **equal-weight** pseudo allocations (token-keyed proxy).
///
/// Selects the top `pseudo_top_n` rows by score (ties broken by `allocation_key`),
/// then assigns each selected row `1 / n_selected`. Negative scores still receive
/// weight — there is no `max(score, 0)` collapse to an empty portfolio.
fn topn_pseudo_allocations(
    weights: &[Decimal],
    group: &CrossSectionGroup,
    pseudo_top_n: u32,
) -> Vec<Decimal> {
    let n = group.rows.len();
    if n == 0 || pseudo_top_n == 0 {
        return vec![Decimal::ZERO; n];
    }
    let scores: Vec<Decimal> = group.rows.iter().map(|row| row.net(weights)).collect();
    let mut order: Vec<usize> = (0..n).collect();
    order.sort_by(|&a, &b| {
        scores[b].cmp(&scores[a]).then_with(|| {
            group.rows[a]
                .allocation_key
                .cmp(&group.rows[b].allocation_key)
        })
    });
    let take = (pseudo_top_n as usize).min(n);
    let equal = Decimal::ONE / Decimal::from(take as u64);
    let mut allocations = vec![Decimal::ZERO; n];
    for &idx in &order[..take] {
        allocations[idx] = equal;
    }
    allocations
}

fn ranknet_logistic_loss(margin: f64) -> QuantResult<Decimal> {
    if !margin.is_finite() {
        return Err(ResearchError::DatasetBuild {
            detail: format!("ranknet logistic loss received non-finite margin {margin}"),
        }
        .into());
    }
    let loss = if margin >= 0.0 {
        (-margin).exp().ln_1p()
    } else {
        -margin + margin.exp().ln_1p()
    };
    if !loss.is_finite() {
        return Err(ResearchError::DatasetBuild {
            detail: format!("ranknet logistic loss produced non-finite value from margin {margin}"),
        }
        .into());
    }
    Decimal::from_f64(loss)
        .map(|value| value.round_dp(RESEARCH_DECIMAL_SCALE))
        .ok_or_else(|| {
            ResearchError::DatasetBuild {
                detail: format!("ranknet logistic loss could not convert {loss} to Decimal"),
            }
            .into()
        })
}

fn decimal_to_f64(field: &str, value: Decimal) -> QuantResult<f64> {
    value.to_f64().ok_or_else(|| {
        ResearchError::DatasetBuild {
            detail: format!("{field} could not convert Decimal {value} to f64"),
        }
        .into()
    })
}

fn parse_non_negative_decimal(field: &'static str, value: &str) -> QuantResult<Decimal> {
    let parsed = value
        .parse::<Decimal>()
        .map_err(|error| ResearchError::DatasetBuild {
            detail: format!("{field} must be a decimal string: {error}"),
        })?;
    if parsed < Decimal::ZERO {
        return Err(ResearchError::DatasetBuild {
            detail: format!("{field} must be >= 0, got {parsed}"),
        }
        .into());
    }
    Ok(parsed)
}

fn parse_tail_fraction(value: &str) -> QuantResult<Decimal> {
    let parsed = value
        .parse::<Decimal>()
        .map_err(|error| ResearchError::DatasetBuild {
            detail: format!("research.training.tail_fraction must be a decimal string: {error}"),
        })?;
    if parsed <= Decimal::ZERO || parsed > Decimal::ONE {
        return Err(ResearchError::DatasetBuild {
            detail: format!("research.training.tail_fraction must be within (0, 1], got {parsed}"),
        }
        .into());
    }
    Ok(parsed)
}

#[cfg(test)]
mod tests {
    use quant_pivot_models::runtime_config::RankLossKind;
    use rust_decimal::Decimal;
    use rust_decimal_macros::dec;

    use super::{
        CrossSectionGroup, ObjectiveEvaluator, SampleRow, TrainingObjectiveSpec, ndcg_at_k,
        rank_ic_weighted_pair_weight, topn_pseudo_allocations,
    };

    fn row(key: &str, signed: Decimal, label: Decimal) -> SampleRow {
        SampleRow {
            allocation_key: key.to_owned(),
            signed: vec![signed],
            label,
        }
    }

    #[test]
    fn correct_pairwise_order_has_lower_loss_than_reversed_order() {
        let group = CrossSectionGroup {
            rows: vec![
                row("m:a", dec!(1), dec!(10)),
                row("m:b", dec!(0), dec!(-10)),
            ],
        };
        let evaluator = ObjectiveEvaluator::new(TrainingObjectiveSpec {
            rank_loss: RankLossKind::PairwiseRanknet,
            ..TrainingObjectiveSpec::default()
        });
        let correct = evaluator
            .evaluate(&[dec!(1)], std::slice::from_ref(&group))
            .expect("eval");
        let reversed_group = CrossSectionGroup {
            rows: vec![
                row("m:a", dec!(0), dec!(10)),
                row("m:b", dec!(1), dec!(-10)),
            ],
        };
        let reversed = evaluator
            .evaluate(&[dec!(1)], &[reversed_group])
            .expect("eval");
        assert!(correct.rank_loss < reversed.rank_loss);
    }

    #[test]
    fn rank_ic_weighted_pair_weight_matches_closed_form_with_ties_floor() {
        let weight = rank_ic_weighted_pair_weight(dec!(1), dec!(3), dec!(2), dec!(4), 3);
        assert_eq!(weight.round_dp(6), dec!(2));
        // Tied score ranks still get |Δr̂|_eff = 1.
        let tied = rank_ic_weighted_pair_weight(dec!(2), dec!(2), dec!(1), dec!(3), 3);
        assert!(tied > Decimal::ZERO);
    }

    #[test]
    fn objective_breakdown_total_matches_weighted_components() {
        let group = CrossSectionGroup {
            rows: vec![
                row("m:a", dec!(1), dec!(-100)),
                row("m:b", dec!(0), dec!(50)),
            ],
        };
        let spec = TrainingObjectiveSpec {
            lambda_tail: dec!(0.5),
            lambda_turnover: dec!(0.2),
            lambda_l2: dec!(0.01),
            ..TrainingObjectiveSpec::default()
        };
        let evaluator = ObjectiveEvaluator::new(spec.clone());
        let report = evaluator.evaluate(&[dec!(1)], &[group]).expect("eval");
        let expected = report.rank_loss
            + spec.lambda_tail * report.tail_penalty
            + spec.lambda_turnover * report.turnover_penalty
            + spec.lambda_l2 * report.l2_penalty;
        assert_eq!(report.total_loss, expected);
    }

    #[test]
    fn ndcg_perfect_order_is_one_and_reversed_is_lower() {
        let labels = vec![dec!(30), dec!(20), dec!(10)];
        let perfect = vec![dec!(3), dec!(2), dec!(1)];
        let reversed = vec![dec!(1), dec!(2), dec!(3)];
        assert_eq!(ndcg_at_k(&perfect, &labels, 3), Decimal::ONE);
        assert!(ndcg_at_k(&reversed, &labels, 3) < ndcg_at_k(&perfect, &labels, 3));
        assert!(ndcg_at_k(&perfect, &labels, 2) <= Decimal::ONE);
    }

    #[test]
    fn topn_pseudo_allocations_only_select_top_n_and_keep_token_keys_separate() {
        let group = CrossSectionGroup {
            rows: vec![
                row("m1:yes", dec!(3), dec!(10)),
                row("m1:no", dec!(2), dec!(5)),
                row("m2:yes", dec!(1), dec!(1)),
            ],
        };
        let alloc = topn_pseudo_allocations(&[dec!(1)], &group, 2);
        assert_eq!(alloc[0], dec!(0.5));
        assert_eq!(alloc[1], dec!(0.5));
        assert_eq!(alloc[2], Decimal::ZERO);
        assert_eq!(alloc.iter().sum::<Decimal>(), Decimal::ONE);
    }

    #[test]
    fn topn_pseudo_allocations_all_negative_scores_still_equal_weight() {
        let group = CrossSectionGroup {
            rows: vec![
                row("m:a", dec!(-3), dec!(10)),
                row("m:b", dec!(-1), dec!(5)),
                row("m:c", dec!(-2), dec!(1)),
            ],
        };
        // Scores = signed * weight = -3, -1, -2 → top-2 by score: m:b then m:c.
        let alloc = topn_pseudo_allocations(&[dec!(1)], &group, 2);
        assert_eq!(alloc[0], Decimal::ZERO);
        assert_eq!(alloc[1], dec!(0.5));
        assert_eq!(alloc[2], dec!(0.5));
        assert_eq!(alloc.iter().sum::<Decimal>(), Decimal::ONE);
    }

    #[test]
    fn rank_loss_group_count_excludes_all_tied_label_groups() {
        let groups = vec![
            CrossSectionGroup {
                rows: vec![
                    row("m:a", dec!(1), dec!(10)),
                    row("m:b", dec!(0), dec!(-10)),
                ],
            },
            CrossSectionGroup {
                rows: vec![row("m:c", dec!(1), dec!(5)), row("m:d", dec!(0), dec!(5))],
            },
        ];
        let evaluator = ObjectiveEvaluator::new(TrainingObjectiveSpec {
            rank_loss: RankLossKind::PairwiseRanknet,
            ..TrainingObjectiveSpec::default()
        });
        let report = evaluator.evaluate(&[dec!(1)], &groups).expect("eval");
        assert_eq!(report.group_count, 2);
        assert_eq!(report.rank_loss_group_count, 1);
        assert_eq!(report.pair_count, 1);
    }

    #[test]
    fn rank_ic_weighted_ranknet_differs_from_plain_pairwise() {
        let group = CrossSectionGroup {
            rows: vec![
                row("m:a", dec!(3), dec!(30)),
                row("m:b", dec!(2), dec!(10)),
                row("m:c", dec!(1), dec!(20)),
            ],
        };
        let pairwise = ObjectiveEvaluator::new(TrainingObjectiveSpec {
            rank_loss: RankLossKind::PairwiseRanknet,
            lambda_tail: Decimal::ZERO,
            lambda_turnover: Decimal::ZERO,
            lambda_l2: Decimal::ZERO,
            ..TrainingObjectiveSpec::default()
        })
        .evaluate(&[dec!(1)], std::slice::from_ref(&group))
        .expect("pairwise");
        let weighted = ObjectiveEvaluator::new(TrainingObjectiveSpec {
            rank_loss: RankLossKind::RankIcWeightedRanknet,
            lambda_tail: Decimal::ZERO,
            lambda_turnover: Decimal::ZERO,
            lambda_l2: Decimal::ZERO,
            ..TrainingObjectiveSpec::default()
        })
        .evaluate(&[dec!(1)], &[group])
        .expect("weighted");
        assert_ne!(pairwise.rank_loss, weighted.rank_loss);
    }

    #[test]
    fn ndcg_k_truncation_is_respected() {
        let labels = vec![dec!(40), dec!(30), dec!(20), dec!(10)];
        let scores = vec![dec!(4), dec!(3), dec!(2), dec!(1)];
        let at_2 = ndcg_at_k(&scores, &labels, 2);
        let at_4 = ndcg_at_k(&scores, &labels, 4);
        assert_eq!(at_2, Decimal::ONE);
        assert_eq!(at_4, Decimal::ONE);
        let reversed = vec![dec!(1), dec!(2), dec!(3), dec!(4)];
        assert!(ndcg_at_k(&reversed, &labels, 2) < at_2);
    }

    #[test]
    fn from_runtime_config_honors_ndcg_k_and_pseudo_top_n() {
        use quant_pivot_models::runtime_config::{
            RankLossKind, ResearchTrainingConfig, TrainingOptimizerKind, wire::DecimalString,
        };

        let config = ResearchTrainingConfig {
            rank_loss: RankLossKind::PairwiseRanknet,
            optimizer: TrainingOptimizerKind::CoordinateSearch,
            lambda_tail: DecimalString::new("0.25"),
            tail_fraction: DecimalString::new("0.05"),
            lambda_turnover: DecimalString::new("0.1"),
            lambda_l2: DecimalString::new("0.02"),
            ndcg_k: 7,
            pseudo_top_n: 4,
        };
        let spec = TrainingObjectiveSpec::from_runtime_config(&config).expect("parse");
        assert_eq!(spec.rank_loss, RankLossKind::PairwiseRanknet);
        assert_eq!(spec.ndcg_k, 7);
        assert_eq!(spec.pseudo_top_n, 4);
        assert_eq!(spec.lambda_tail, dec!(0.25));
        assert_eq!(spec.pseudo_top_n, 4);
    }
}
