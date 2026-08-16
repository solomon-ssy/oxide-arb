//! Governed learning-to-rank objective for weighted-model training.
//!
//! The trainer optimizes per-`as_of` cross-sections: ranking loss is computed
//! only within one query group, while tail and turnover use a deterministic
//! **`TopN` score-ranked equal-weight pseudo portfolio** (token-keyed) as an
//! optimization proxy. The backtest replay remains the authoritative
//! capital/allocation check.
//!
//! `TargetRankIcWeightedRanknet` is a **simplex black-box surrogate** (`RankNet` pairs
//! weighted by the closed-form `RankIC` swap delta). It is **not** an `XGBoost` /
//! `LightGBM` `LambdaMART` λ-gradient implementation.

use std::collections::BTreeMap;

use chrono::{DateTime, Utc};
use quant_pivot_error::{QuantResult, research::ResearchError};
use quant_pivot_models::{
    runtime_config::{RankLossKind, ResearchTrainingConfig},
    types::model_training::TrainingObjectiveSpec,
};
use rust_decimal::{
    Decimal,
    prelude::{FromPrimitive, ToPrimitive},
};
use serde::{Deserialize, Serialize};

use crate::{backtest::metrics, precision::RESEARCH_DECIMAL_SCALE, stats};
const BPS_PER_UNIT_RETURN: i64 = 10_000;

/// Parse the governed runtime-config section into exact `Decimal` weights.
pub fn runtime_training_objective(
    config: &ResearchTrainingConfig,
) -> QuantResult<TrainingObjectiveSpec> {
    Ok(TrainingObjectiveSpec {
        rank_loss: config.rank_loss,
        optimizer: config.optimizer,
        lambda_tail: parse_non_negative_decimal(
            "research.training.lambda_tail",
            config.lambda_tail.value,
        )?,
        tail_fraction: parse_tail_fraction(config.tail_fraction.value)?,
        lambda_turnover: parse_non_negative_decimal(
            "research.training.lambda_turnover",
            config.lambda_turnover.value,
        )?,
        lambda_l2: parse_non_negative_decimal(
            "research.training.lambda_l2",
            config.lambda_l2.value,
        )?,
        ndcg_k: config.ndcg_k,
        pseudo_top_n: config.pseudo_top_n,
    })
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
    pub mean_target_rank_ic: Decimal,
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
            mean_target_rank_ic: self.mean_target_rank_ic.round_dp(RESEARCH_DECIMAL_SCALE),
            mean_ndcg_at_k: self.mean_ndcg_at_k.round_dp(RESEARCH_DECIMAL_SCALE),
            ndcg_k: self.ndcg_k,
            group_count: self.group_count,
        }
    }
}

/// One reduced training row inside a same-`as_of` query group.
#[derive(Debug, Clone, Serialize)]
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
/// the evaluator only needs the rows (pair loss / pseudo portfolio). The
/// `[as_of, label_horizon_end]` interval is retained so trainer CV can apply
/// the same label-horizon purge/embargo as CPCV.
#[derive(Debug, Clone, Serialize)]
pub(crate) struct CrossSectionGroup {
    pub(crate) decision_at: DateTime<Utc>,
    /// Conservative upper bound of member rows' `TrainingLabel::matured_at`.
    pub(crate) label_horizon_end: DateTime<Utc>,
    pub(crate) rows: Vec<SampleRow>,
    /// Immutable label ranks are independent of every candidate weight vector.
    /// Excluding this derived cache from the training-input preimage preserves
    /// the canonical artifact hash while avoiding one sort per objective call.
    #[serde(skip)]
    label_ranks: Vec<Decimal>,
}

impl CrossSectionGroup {
    pub(crate) fn new(
        decision_at: DateTime<Utc>,
        label_horizon_end: DateTime<Utc>,
        rows: Vec<SampleRow>,
    ) -> Self {
        let labels = rows.iter().map(|row| row.label).collect::<Vec<_>>();
        Self {
            decision_at,
            label_horizon_end,
            rows,
            label_ranks: stats::average_ranks(&labels),
        }
    }
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
        let mut group_losses = Vec::new();
        let mut pair_count = 0_u64;
        let mut returns = Vec::with_capacity(groups.len());
        let mut tick_weights = Vec::with_capacity(groups.len());
        for group in groups {
            let scores = group
                .rows
                .iter()
                .map(|row| row.net(weights))
                .collect::<Vec<_>>();
            let (group_loss, group_pairs) = self.group_rank_loss(group, &scores)?;
            pair_count = pair_count.checked_add(group_pairs).ok_or_else(|| {
                ResearchError::ValidationMethodology {
                    detail: "rank-loss total pair count overflow".to_owned(),
                }
            })?;
            if group_pairs > 0 {
                group_losses.push(group_loss);
            }

            let allocations = topn_allocations(&scores, group, self.spec.pseudo_top_n);
            let group_return_bps = group
                .rows
                .iter()
                .zip(&allocations)
                .map(|(row, allocation)| row.label * *allocation)
                .sum::<Decimal>();
            returns.push(group_return_bps / Decimal::from(BPS_PER_UNIT_RETURN));
            let mut by_token = BTreeMap::new();
            for (row, allocation) in group.rows.iter().zip(&allocations) {
                if *allocation > Decimal::ZERO {
                    let entry = by_token
                        .entry(row.allocation_key.clone())
                        .or_insert(Decimal::ZERO);
                    *entry += *allocation;
                }
            }
            tick_weights.push(by_token);
        }
        let rank_loss_group_count = u64::try_from(group_losses.len()).map_err(|error| {
            ResearchError::ValidationMethodology {
                detail: format!("rank-loss group count exceeds u64: {error}"),
            }
        })?;
        let rank_loss = if group_losses.is_empty() {
            Decimal::ZERO
        } else {
            stats::mean(&group_losses)
        };
        let tail_penalty = tail_penalty(&mut returns, self.spec.tail_fraction)?;
        let turnover_penalty = metrics::allocation_churn(&tick_weights);
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
            group_count: u64::try_from(groups.len()).map_err(|error| {
                ResearchError::ValidationMethodology {
                    detail: format!("objective group count exceeds u64: {error}"),
                }
            })?,
            rank_loss_group_count,
            pair_count,
        })
    }

    /// Compute ranking diagnostics (Rank IC + NDCG@k) for a weight vector.
    pub(crate) fn diagnostics(
        &self,
        weights: &[Decimal],
        groups: &[CrossSectionGroup],
    ) -> QuantResult<RankingDiagnostics> {
        if groups.is_empty() {
            return Ok(RankingDiagnostics {
                mean_target_rank_ic: Decimal::ZERO,
                mean_ndcg_at_k: Decimal::ZERO,
                ndcg_k: self.spec.ndcg_k,
                group_count: 0,
            });
        }
        let ndcg_k = usize::try_from(self.spec.ndcg_k).map_err(|error| {
            ResearchError::ValidationMethodology {
                detail: format!("objective ndcg_k exceeds usize: {error}"),
            }
        })?;
        let mut target_rank_ics = Vec::with_capacity(groups.len());
        let mut ndcgs = Vec::with_capacity(groups.len());
        for group in groups {
            let scores: Vec<Decimal> = group.rows.iter().map(|row| row.net(weights)).collect();
            let labels: Vec<Decimal> = group.rows.iter().map(|row| row.label).collect();
            target_rank_ics.push(stats::spearman(&scores, &labels));
            ndcgs.push(ndcg_at_k(&scores, &labels, ndcg_k)?);
        }
        Ok(RankingDiagnostics {
            mean_target_rank_ic: stats::mean(&target_rank_ics),
            mean_ndcg_at_k: stats::mean(&ndcgs),
            ndcg_k: self.spec.ndcg_k,
            group_count: u64::try_from(groups.len()).map_err(|error| {
                ResearchError::ValidationMethodology {
                    detail: format!("ranking diagnostic group count exceeds u64: {error}"),
                }
            })?,
        }
        .rounded())
    }

    fn group_rank_loss(
        &self,
        group: &CrossSectionGroup,
        scores: &[Decimal],
    ) -> QuantResult<(Decimal, u64)> {
        let score_ranks = stats::average_ranks(scores);
        let n = group.rows.len();
        let mut group_loss = Decimal::ZERO;
        let mut group_pairs = 0_u64;
        for i in 0..n {
            for j in (i + 1)..n {
                let label_diff = group.rows[i].label - group.rows[j].label;
                if label_diff.is_zero() {
                    continue;
                }
                let pair_weight = match self.spec.rank_loss {
                    RankLossKind::PairwiseRanknet => Decimal::ONE,
                    RankLossKind::TargetRankIcWeightedRanknet => rank_pair_weight(
                        score_ranks[i],
                        score_ranks[j],
                        group.label_ranks[i],
                        group.label_ranks[j],
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
                group_pairs = group_pairs.checked_add(1).ok_or_else(|| {
                    ResearchError::ValidationMethodology {
                        detail: "rank-loss group pair count overflow".to_owned(),
                    }
                })?;
            }
        }
        Ok((group_loss, group_pairs))
    }
}

fn tail_penalty(returns: &mut [Decimal], fraction: Decimal) -> QuantResult<Decimal> {
    if returns.is_empty() {
        return Ok(Decimal::ZERO);
    }
    returns.sort();
    let raw = (Decimal::from(returns.len() as u64) * fraction).ceil();
    let take = raw
        .to_usize()
        .ok_or_else(|| ResearchError::MatrixBuild {
            detail: format!("tail sample count `{raw}` is outside usize range"),
        })?
        .max(1)
        .min(returns.len());
    let tail_mean = stats::mean(&returns[..take]);
    Ok((-tail_mean).max(Decimal::ZERO))
}

/// Closed-form `RankIC` contribution of swapping a pair's predicted ranks.
///
/// Uses `|Δr̂|_eff = max(|Δr̂|, 1)` so score-rank ties still contribute signal
/// for label-discordant pairs (avoids flat-score plateaus killing the loss).
#[must_use]
pub(crate) fn rank_pair_weight(
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
pub(crate) fn ndcg_at_k(scores: &[Decimal], labels: &[Decimal], k: usize) -> QuantResult<Decimal> {
    if scores.len() != labels.len() {
        return Err(ResearchError::ValidationMethodology {
            detail: format!(
                "NDCG score/label length mismatch: scores={}, labels={}",
                scores.len(),
                labels.len()
            ),
        }
        .into());
    }
    if scores.is_empty() || k == 0 {
        return Ok(Decimal::ZERO);
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
    let dcg = discounted_gain(&dcg_rels)?;
    let mut ideal: Vec<Decimal> = labels
        .iter()
        .map(|label| (*label).max(Decimal::ZERO))
        .collect();
    ideal.sort_by(|a, b| b.cmp(a));
    let idcg = discounted_gain(&ideal[..take])?;
    if idcg.is_zero() {
        return Ok(Decimal::ZERO);
    }
    Ok((dcg / idcg).clamp(Decimal::ZERO, Decimal::ONE))
}

fn discounted_gain(relevances: &[Decimal]) -> QuantResult<Decimal> {
    let mut total = Decimal::ZERO;
    for (pos, rel) in relevances.iter().enumerate() {
        if rel.is_zero() {
            continue;
        }
        // log2(pos+2) = ln(pos+2)/ln(2)
        let rank =
            u64::try_from(pos + 2).map_err(|error| ResearchError::ValidationMethodology {
                detail: format!("NDCG rank exceeds u64: {error}"),
            })?;
        let denom = decimal_log2(Decimal::from(rank))?;
        total += *rel / denom;
    }
    Ok(total)
}

fn decimal_log2(value: Decimal) -> QuantResult<Decimal> {
    let f = value
        .to_f64()
        .ok_or_else(|| ResearchError::ValidationMethodology {
            detail: format!("NDCG logarithm input {value} is not representable as f64"),
        })?;
    if f <= 0.0 {
        return Err(ResearchError::ValidationMethodology {
            detail: format!("NDCG logarithm input must be positive, got {value}"),
        }
        .into());
    }
    let log2 = f.log2();
    if !log2.is_finite() || log2 <= 0.0 {
        return Err(ResearchError::ValidationMethodology {
            detail: format!("NDCG log2 produced invalid value {log2} from {value}"),
        }
        .into());
    }
    Decimal::from_f64(log2).ok_or_else(|| {
        ResearchError::ValidationMethodology {
            detail: format!("NDCG log2 value {log2} is not representable as Decimal"),
        }
        .into()
    })
}

/// `TopN` score-ranked **equal-weight** pseudo allocations (token-keyed proxy).
///
/// Selects the top `pseudo_top_n` rows by score (ties broken by `allocation_key`),
/// then assigns each selected row `1 / n_selected`. Negative scores still receive
/// weight — there is no `max(score, 0)` collapse to an empty portfolio.
#[cfg(test)]
fn topn_pseudo_allocations(
    weights: &[Decimal],
    group: &CrossSectionGroup,
    pseudo_top_n: u32,
) -> Vec<Decimal> {
    let scores = group
        .rows
        .iter()
        .map(|row| row.net(weights))
        .collect::<Vec<_>>();
    topn_allocations(&scores, group, pseudo_top_n)
}

fn topn_allocations(
    scores: &[Decimal],
    group: &CrossSectionGroup,
    pseudo_top_n: u32,
) -> Vec<Decimal> {
    let n = scores.len();
    debug_assert_eq!(n, group.rows.len());
    if n == 0 || pseudo_top_n == 0 {
        return vec![Decimal::ZERO; n];
    }
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

fn parse_non_negative_decimal(field: &'static str, value: Decimal) -> QuantResult<Decimal> {
    if value < Decimal::ZERO {
        return Err(ResearchError::DatasetBuild {
            detail: format!("{field} must be >= 0, got {value}"),
        }
        .into());
    }
    Ok(value)
}

fn parse_tail_fraction(value: Decimal) -> QuantResult<Decimal> {
    if value <= Decimal::ZERO || value > Decimal::ONE {
        return Err(ResearchError::DatasetBuild {
            detail: format!("research.training.tail_fraction must be within (0, 1], got {value}"),
        }
        .into());
    }
    Ok(value)
}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeMap, slice};

    use chrono::{TimeZone, Utc};
    use quant_pivot_models::{
        runtime_config::{
            RankLossKind, ResearchTrainingConfig, TrainingOptimizerKind, wire::DecimalValue,
        },
        types::model_training::TrainingObjectiveSpec,
    };
    use rust_decimal::Decimal;
    use rust_decimal_macros::dec;

    use super::{
        BPS_PER_UNIT_RETURN, CrossSectionGroup, ObjectiveComponentReport, ObjectiveEvaluator,
        SampleRow, metrics, ndcg_at_k, rank_pair_weight, runtime_training_objective, stats,
        tail_penalty, topn_pseudo_allocations,
    };

    fn row(key: &str, signed: Decimal, label: Decimal) -> SampleRow {
        SampleRow {
            allocation_key: key.to_owned(),
            signed: vec![signed],
            label,
        }
    }

    fn group(rows: Vec<SampleRow>) -> CrossSectionGroup {
        let decision_at = Utc.timestamp_opt(1_700_000_000, 0).unwrap();
        CrossSectionGroup::new(decision_at, decision_at, rows)
    }

    fn independent_report(
        evaluator: &ObjectiveEvaluator,
        weights: &[Decimal],
        groups: &[CrossSectionGroup],
    ) -> ObjectiveComponentReport {
        let mut group_losses = Vec::new();
        let mut pair_count = 0_u64;
        for group in groups {
            let scores = group
                .rows
                .iter()
                .map(|row| row.net(weights))
                .collect::<Vec<_>>();
            let (group_loss, group_pairs) = evaluator
                .group_rank_loss(group, &scores)
                .expect("independent rank loss");
            pair_count += group_pairs;
            if group_pairs > 0 {
                group_losses.push(group_loss);
            }
        }
        let rank_loss = if group_losses.is_empty() {
            Decimal::ZERO
        } else {
            stats::mean(&group_losses)
        };

        let allocations = groups
            .iter()
            .map(|group| topn_pseudo_allocations(weights, group, evaluator.spec.pseudo_top_n))
            .collect::<Vec<_>>();
        let mut returns = groups
            .iter()
            .zip(&allocations)
            .map(|(group, group_allocations)| {
                group
                    .rows
                    .iter()
                    .zip(group_allocations)
                    .map(|(row, allocation)| row.label * *allocation)
                    .sum::<Decimal>()
                    / Decimal::from(BPS_PER_UNIT_RETURN)
            })
            .collect::<Vec<_>>();
        let tail_penalty = tail_penalty(&mut returns, evaluator.spec.tail_fraction)
            .expect("independent tail penalty");
        let tick_weights = groups
            .iter()
            .zip(&allocations)
            .map(|(group, group_allocations)| {
                let mut by_token = BTreeMap::new();
                for (row, allocation) in group.rows.iter().zip(group_allocations) {
                    if *allocation > Decimal::ZERO {
                        *by_token
                            .entry(row.allocation_key.clone())
                            .or_insert(Decimal::ZERO) += *allocation;
                    }
                }
                by_token
            })
            .collect::<Vec<_>>();
        let turnover_penalty = metrics::allocation_churn(&tick_weights);
        let l2_penalty = weights.iter().map(|weight| *weight * *weight).sum();
        let total_loss = rank_loss
            + evaluator.spec.lambda_tail * tail_penalty
            + evaluator.spec.lambda_turnover * turnover_penalty
            + evaluator.spec.lambda_l2 * l2_penalty;
        ObjectiveComponentReport {
            rank_loss,
            tail_penalty,
            turnover_penalty,
            l2_penalty,
            total_loss,
            group_count: groups.len() as u64,
            rank_loss_group_count: group_losses.len() as u64,
            pair_count,
        }
    }

    #[test]
    fn correct_pairwise_order_order() {
        let correctly_ranked = group(vec![
            row("m:a", dec!(1), dec!(10)),
            row("m:b", dec!(0), dec!(-10)),
        ]);
        let evaluator = ObjectiveEvaluator::new(TrainingObjectiveSpec {
            rank_loss: RankLossKind::PairwiseRanknet,
            ..TrainingObjectiveSpec::default()
        });
        let correct = evaluator
            .evaluate(&[dec!(1)], slice::from_ref(&correctly_ranked))
            .expect("eval");
        let reversed_group = group(vec![
            row("m:a", dec!(0), dec!(10)),
            row("m:b", dec!(1), dec!(-10)),
        ]);
        let reversed = evaluator
            .evaluate(&[dec!(1)], &[reversed_group])
            .expect("eval");
        assert!(correct.rank_loss < reversed.rank_loss);
    }

    #[test]
    fn target_rank_ic_floor() {
        let weight = rank_pair_weight(dec!(1), dec!(3), dec!(2), dec!(4), 3);
        assert_eq!(weight.round_dp(6), dec!(2));
        // Tied score ranks still get |Δr̂|_eff = 1.
        let tied = rank_pair_weight(dec!(2), dec!(2), dec!(1), dec!(3), 3);
        assert!(tied > Decimal::ZERO);
    }

    #[test]
    fn objective_breakdown_matches_components() {
        let group = group(vec![
            row("m:a", dec!(1), dec!(-100)),
            row("m:b", dec!(0), dec!(50)),
        ]);
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
    fn fused_objective_matches_independent() {
        let groups = vec![
            group(vec![
                row("m:a", dec!(3), dec!(-100)),
                row("m:b", dec!(2), dec!(40)),
                row("m:c", dec!(1), dec!(40)),
            ]),
            group(vec![
                row("m:b", dec!(1), dec!(70)),
                row("m:c", dec!(4), dec!(-30)),
                row("m:d", dec!(2), dec!(10)),
            ]),
            group(vec![
                row("m:a", dec!(-1), dec!(25)),
                row("m:d", dec!(3), dec!(-50)),
                row("m:e", dec!(2), dec!(80)),
            ]),
        ];
        for rank_loss in [
            RankLossKind::PairwiseRanknet,
            RankLossKind::TargetRankIcWeightedRanknet,
        ] {
            let evaluator = ObjectiveEvaluator::new(TrainingObjectiveSpec {
                rank_loss,
                lambda_tail: dec!(0.35),
                tail_fraction: dec!(0.67),
                lambda_turnover: dec!(0.2),
                lambda_l2: dec!(0.01),
                pseudo_top_n: 2,
                ..TrainingObjectiveSpec::default()
            });
            let actual = evaluator.evaluate(&[dec!(0.75)], &groups).expect("fused");
            assert_eq!(
                actual,
                independent_report(&evaluator, &[dec!(0.75)], &groups)
            );
        }
    }

    #[test]
    fn label_ranks_not_serialized() {
        let group = group(vec![
            row("m:a", dec!(1), dec!(10)),
            row("m:b", dec!(0), dec!(-10)),
        ]);
        let serialized = serde_json::to_value(group).expect("serialize group");
        assert!(serialized.get("label_ranks").is_none());
    }

    #[test]
    fn ndcg_perfect_order_lower() {
        let labels = vec![dec!(30), dec!(20), dec!(10)];
        let perfect = vec![dec!(3), dec!(2), dec!(1)];
        let reversed = vec![dec!(1), dec!(2), dec!(3)];
        assert_eq!(ndcg_at_k(&perfect, &labels, 3).expect("ndcg"), Decimal::ONE);
        assert!(
            ndcg_at_k(&reversed, &labels, 3).expect("ndcg")
                < ndcg_at_k(&perfect, &labels, 3).expect("ndcg")
        );
        assert!(ndcg_at_k(&perfect, &labels, 2).expect("ndcg") <= Decimal::ONE);
    }

    #[test]
    fn topn_pseudo_keep_separate() {
        let group = group(vec![
            row("m1:yes", dec!(3), dec!(10)),
            row("m1:no", dec!(2), dec!(5)),
            row("m2:yes", dec!(1), dec!(1)),
        ]);
        let alloc = topn_pseudo_allocations(&[dec!(1)], &group, 2);
        assert_eq!(alloc[0], dec!(0.5));
        assert_eq!(alloc[1], dec!(0.5));
        assert_eq!(alloc[2], Decimal::ZERO);
        assert_eq!(alloc.iter().sum::<Decimal>(), Decimal::ONE);
    }

    #[test]
    fn topn_pseudo_allocations_weight() {
        let group = group(vec![
            row("m:a", dec!(-3), dec!(10)),
            row("m:b", dec!(-1), dec!(5)),
            row("m:c", dec!(-2), dec!(1)),
        ]);
        // Scores = signed * weight = -3, -1, -2 → top-2 by score: m:b then m:c.
        let alloc = topn_pseudo_allocations(&[dec!(1)], &group, 2);
        assert_eq!(alloc[0], Decimal::ZERO);
        assert_eq!(alloc[1], dec!(0.5));
        assert_eq!(alloc[2], dec!(0.5));
        assert_eq!(alloc.iter().sum::<Decimal>(), Decimal::ONE);
    }

    #[test]
    fn rank_loss_excludes_groups() {
        let groups = vec![
            group(vec![
                row("m:a", dec!(1), dec!(10)),
                row("m:b", dec!(0), dec!(-10)),
            ]),
            group(vec![
                row("m:c", dec!(1), dec!(5)),
                row("m:d", dec!(0), dec!(5)),
            ]),
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
    fn target_rank_pairwise_weighting() {
        let group = group(vec![
            row("m:a", dec!(3), dec!(30)),
            row("m:b", dec!(2), dec!(10)),
            row("m:c", dec!(1), dec!(20)),
        ]);
        let pairwise = ObjectiveEvaluator::new(TrainingObjectiveSpec {
            rank_loss: RankLossKind::PairwiseRanknet,
            lambda_tail: Decimal::ZERO,
            lambda_turnover: Decimal::ZERO,
            lambda_l2: Decimal::ZERO,
            ..TrainingObjectiveSpec::default()
        })
        .evaluate(&[dec!(1)], slice::from_ref(&group))
        .expect("pairwise");
        let weighted = ObjectiveEvaluator::new(TrainingObjectiveSpec {
            rank_loss: RankLossKind::TargetRankIcWeightedRanknet,
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
    fn ndcg_k_truncation_respected() {
        let labels = vec![dec!(40), dec!(30), dec!(20), dec!(10)];
        let scores = vec![dec!(4), dec!(3), dec!(2), dec!(1)];
        let at_2 = ndcg_at_k(&scores, &labels, 2).expect("ndcg");
        let at_4 = ndcg_at_k(&scores, &labels, 4).expect("ndcg");
        assert_eq!(at_2, Decimal::ONE);
        assert_eq!(at_4, Decimal::ONE);
        let reversed = vec![dec!(1), dec!(2), dec!(3), dec!(4)];
        assert!(ndcg_at_k(&reversed, &labels, 2).expect("ndcg") < at_2);
    }

    #[test]
    fn runtime_config_honors_n() {
        let config = ResearchTrainingConfig {
            rank_loss: RankLossKind::PairwiseRanknet,
            optimizer: TrainingOptimizerKind::CoordinateSearch,
            lambda_tail: DecimalValue::new(rust_decimal_macros::dec!(0.25)),
            tail_fraction: DecimalValue::new(rust_decimal_macros::dec!(0.05)),
            lambda_turnover: DecimalValue::new(rust_decimal_macros::dec!(0.1)),
            lambda_l2: DecimalValue::new(rust_decimal_macros::dec!(0.02)),
            ndcg_k: 7,
            pseudo_top_n: 4,
        };
        let spec = runtime_training_objective(&config).expect("parse");
        assert_eq!(spec.rank_loss, RankLossKind::PairwiseRanknet);
        assert_eq!(spec.ndcg_k, 7);
        assert_eq!(spec.pseudo_top_n, 4);
        assert_eq!(spec.lambda_tail, dec!(0.25));
        assert_eq!(spec.pseudo_top_n, 4);
    }
}
