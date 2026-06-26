//! Shadow comparison at the signal-candidate / ranking layer (3.7).
//!
//! Pure computation: given the **active** model's ranked candidates and a
//! **shadow** model's ranked candidates for the same `as_of` cross-section, it
//! quantifies how far the shadow diverges from production at the layer Phase 03
//! owns — `TopN` overlap, per-market rank delta, and per-market score delta.
//! (Report-level deltas — capital allocation, would-execute, risk envelope —
//! depend on Phase 04 report generation and are explicitly deferred.)
//!
//! The result is content-addressed and persisted to `quant_shadow_comparison`
//! by the core `ModelRunner`; a `hard_divergence` raises a critical alert but
//! never auto-switches the active model (parent §6 invariant).

use std::collections::{BTreeMap, BTreeSet};

use chrono::{DateTime, Utc};
use quant_pivot_error::QuantResult;
use quant_pivot_models::{
    enums::quant::OutcomeSide,
    types::{ContentHash, ModelVersionId, Probability, ShadowComparisonId},
};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

use crate::{
    hashing::ResearchHasher, model::SignalCandidate, precision::RESEARCH_DECIMAL_SCALE, stats,
};

/// Per-market ranking divergence between active and shadow over common markets.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RankDelta {
    /// Mean absolute rank difference over markets scored by both models.
    pub mean_abs_rank_delta: Decimal,
    /// Largest single rank difference over common markets.
    pub max_rank_delta: u32,
    /// Spearman rank correlation of the common-market rankings (`[-1, 1]`).
    pub spearman: Decimal,
    /// Number of markets scored by both models.
    pub common_markets: u64,
}

/// Per-market composite-score divergence over common markets.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScoreDelta {
    /// Mean absolute composite-score difference over common markets.
    pub mean_abs_score_delta: Decimal,
    /// Largest single composite-score difference over common markets.
    pub max_score_delta: Decimal,
    /// Fraction of common markets where the chosen side disagreed (`[0, 1]`).
    pub side_disagreement_rate: Decimal,
}

/// Realized-outcome divergence, backfilled once labels mature (Phase 04).
///
/// Carried as `Option` on [`ShadowComparison`]; Phase 03 always persists `None`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OutcomeDelta {
    /// Active model's realized return over the matured `TopN` (bps).
    pub active_realized_return_bps: Decimal,
    /// Shadow model's realized return over the matured `TopN` (bps).
    pub shadow_realized_return_bps: Decimal,
    /// `shadow - active` realized return (bps).
    pub delta_bps: Decimal,
}

/// A frozen, content-addressed shadow comparison at the signal/rank layer.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ShadowComparison {
    /// Comparison id (surrogate; excluded from the content hash).
    pub shadow_comparison_id: ShadowComparisonId,
    /// Active (production) model version.
    pub active_model_version_id: ModelVersionId,
    /// Shadow (candidate) model version.
    pub shadow_model_version_id: ModelVersionId,
    /// Decision time of the compared cross-section.
    pub as_of: DateTime<Utc>,
    /// `TopN` market-set overlap (Jaccard) in `[0, 1]`; higher ⇒ more stable.
    pub topn_overlap: Probability,
    /// Per-market rank divergence.
    pub rank_delta: RankDelta,
    /// Per-market score divergence.
    pub score_delta: ScoreDelta,
    /// Realized-outcome divergence, backfilled after maturity (Phase 04).
    pub matured_outcome_delta: Option<OutcomeDelta>,
    /// Whether the score divergence breached the governed threshold.
    pub hard_divergence: bool,
    /// Content hash over the comparison metrics (excludes the surrogate id).
    pub comparison_hash: ContentHash,
}

/// Canonical, surrogate-free projection for content addressing.
#[derive(Serialize)]
struct ComparisonHashInput<'a> {
    active_model_version_id: &'a ModelVersionId,
    shadow_model_version_id: &'a ModelVersionId,
    as_of: DateTime<Utc>,
    topn_overlap: &'a Probability,
    rank_delta: &'a RankDelta,
    score_delta: &'a ScoreDelta,
    hard_divergence: bool,
}

/// One candidate's ranking position projected for comparison.
struct Ranked {
    rank: u32,
    score: Decimal,
    outcome_side: OutcomeSide,
}

/// Compute the active-vs-shadow comparison at the signal/rank layer.
///
/// `top_n` bounds the `TopN` overlap set (by `rank_before_portfolio`);
/// `score_divergence_threshold` (the governed `shadow_diff_threshold`) flips
/// `hard_divergence` when the mean absolute score delta exceeds it.
///
/// # Errors
///
/// Propagates canonical-hash failures when sealing the comparison.
pub fn compute_shadow_comparison(
    active_model_version_id: ModelVersionId,
    shadow_model_version_id: ModelVersionId,
    as_of: DateTime<Utc>,
    active: &[SignalCandidate],
    shadow: &[SignalCandidate],
    top_n: usize,
    score_divergence_threshold: Decimal,
) -> QuantResult<ShadowComparison> {
    let active_index = index_by_market(active);
    let shadow_index = index_by_market(shadow);

    // Common markets, deterministically ordered for spearman alignment.
    let common: Vec<&String> = active_index
        .keys()
        .filter(|market| shadow_index.contains_key(*market))
        .collect();

    let mut active_ranks: Vec<Decimal> = Vec::with_capacity(common.len());
    let mut shadow_ranks: Vec<Decimal> = Vec::with_capacity(common.len());
    let mut rank_delta_sum = Decimal::ZERO;
    let mut max_rank_delta = 0_u32;
    let mut score_delta_sum = Decimal::ZERO;
    let mut max_score_delta = Decimal::ZERO;
    let mut side_disagreements = 0_u64;

    for market in &common {
        let a = &active_index[*market];
        let s = &shadow_index[*market];
        active_ranks.push(Decimal::from(a.rank));
        shadow_ranks.push(Decimal::from(s.rank));

        let rank_diff = a.rank.abs_diff(s.rank);
        rank_delta_sum += Decimal::from(rank_diff);
        max_rank_delta = max_rank_delta.max(rank_diff);

        let score_diff = (a.score - s.score).abs();
        score_delta_sum += score_diff;
        max_score_delta = max_score_delta.max(score_diff);

        if a.outcome_side != s.outcome_side {
            side_disagreements += 1;
        }
    }

    let common_count = common.len() as u64;
    let common_decimal = Decimal::from(common_count);
    let mean_abs_rank_delta = ratio(rank_delta_sum, common_decimal);
    let mean_abs_score_delta = ratio(score_delta_sum, common_decimal);
    let side_disagreement_rate = ratio(Decimal::from(side_disagreements), common_decimal);
    let spearman = stats::spearman(&active_ranks, &shadow_ranks);

    let topn_overlap = topn_overlap(active, shadow, top_n);
    let hard_divergence = mean_abs_score_delta > score_divergence_threshold;

    let rank_delta = RankDelta {
        mean_abs_rank_delta,
        max_rank_delta,
        spearman,
        common_markets: common_count,
    };
    let score_delta = ScoreDelta {
        mean_abs_score_delta,
        max_score_delta,
        side_disagreement_rate,
    };

    let comparison_hash = ResearchHasher::canonical(&ComparisonHashInput {
        active_model_version_id: &active_model_version_id,
        shadow_model_version_id: &shadow_model_version_id,
        as_of,
        topn_overlap: &topn_overlap,
        rank_delta: &rank_delta,
        score_delta: &score_delta,
        hard_divergence,
    })?;

    Ok(ShadowComparison {
        shadow_comparison_id: ShadowComparisonId::from_v7(),
        active_model_version_id,
        shadow_model_version_id,
        as_of,
        topn_overlap,
        rank_delta,
        score_delta,
        matured_outcome_delta: None,
        hard_divergence,
        comparison_hash,
    })
}

/// Index a candidate slice by market id (ranking position + score + side).
fn index_by_market(candidates: &[SignalCandidate]) -> BTreeMap<String, Ranked> {
    candidates
        .iter()
        .map(|candidate| {
            (
                candidate.market_id.as_str().to_owned(),
                Ranked {
                    rank: candidate.rank_before_portfolio,
                    score: candidate.composite_score.inner(),
                    outcome_side: candidate.outcome_side,
                },
            )
        })
        .collect()
}

/// Jaccard overlap of the two `TopN` market sets (by `rank_before_portfolio`).
///
/// Two empty `TopN` sets are identical (`1`); exactly one empty is maximal
/// divergence (`0`); otherwise `|∩| / |∪|`.
fn topn_overlap(
    active: &[SignalCandidate],
    shadow: &[SignalCandidate],
    top_n: usize,
) -> Probability {
    let active_top = top_markets(active, top_n);
    let shadow_top = top_markets(shadow, top_n);
    if active_top.is_empty() && shadow_top.is_empty() {
        return Probability::new(Decimal::ONE);
    }
    if active_top.is_empty() || shadow_top.is_empty() {
        return Probability::new(Decimal::ZERO);
    }
    let intersection = active_top.intersection(&shadow_top).count();
    let union = active_top.union(&shadow_top).count();
    Probability::new(ratio(
        Decimal::from(intersection as u64),
        Decimal::from(union as u64),
    ))
}

/// The market ids ranked within the `TopN` (`rank_before_portfolio <= top_n`).
fn top_markets(candidates: &[SignalCandidate], top_n: usize) -> BTreeSet<String> {
    let bound = u32::try_from(top_n).unwrap_or(u32::MAX);
    candidates
        .iter()
        .filter(|candidate| {
            candidate.rank_before_portfolio >= 1 && candidate.rank_before_portfolio <= bound
        })
        .map(|candidate| candidate.market_id.as_str().to_owned())
        .collect()
}

/// `numerator / denominator` at the research scale, or `0` when empty.
fn ratio(numerator: Decimal, denominator: Decimal) -> Decimal {
    if denominator.is_zero() {
        return Decimal::ZERO;
    }
    (numerator / denominator).round_dp(RESEARCH_DECIMAL_SCALE)
}

#[cfg(test)]
mod tests {
    use super::compute_shadow_comparison;
    use chrono::Utc;
    use quant_pivot_models::{
        enums::quant::OutcomeSide,
        types::{
            MarketId, ModelRunId, ModelVersionId, Price, Probability, SignalCandidateId, TokenId,
        },
    };
    use rust_decimal::Decimal;
    use rust_decimal_macros::dec;

    use crate::model::{ModelExplanation, SignalCandidate};

    fn candidate(
        market: &str,
        rank: u32,
        score: Decimal,
        outcome_side: OutcomeSide,
    ) -> SignalCandidate {
        SignalCandidate {
            signal_candidate_id: SignalCandidateId::from_v7(),
            model_run_id: ModelRunId::from_v7(),
            market_id: MarketId::new(market),
            token_id: TokenId::new("t"),
            outcome_side,
            composite_score: Probability::new(score),
            confidence: Probability::new(dec!(0.8)),
            expected_return_bps: dec!(100),
            downside_bps: dec!(50),
            entry_price_ref: Price::new(dec!(0.5)),
            suggested_horizon_secs: 3_600,
            factor_breakdown: Vec::new(),
            model_explanation: ModelExplanation {
                headline: "t".to_owned(),
                top_positive: Vec::new(),
                top_negative: Vec::new(),
            },
            rejection_warnings: Vec::new(),
            rank_before_portfolio: rank,
            as_of: Utc::now(),
        }
    }

    #[test]
    fn identical_rankings_have_full_overlap_and_no_divergence() {
        let active = vec![
            candidate("a", 1, dec!(0.9), OutcomeSide::Yes),
            candidate("b", 2, dec!(0.7), OutcomeSide::Yes),
        ];
        let shadow = active
            .iter()
            .map(|c| {
                candidate(
                    c.market_id.as_str(),
                    c.rank_before_portfolio,
                    c.composite_score.inner(),
                    c.outcome_side,
                )
            })
            .collect::<Vec<_>>();
        let comparison = compute_shadow_comparison(
            ModelVersionId::from_v7(),
            ModelVersionId::from_v7(),
            Utc::now(),
            &active,
            &shadow,
            5,
            dec!(0.10),
        )
        .expect("comparison");
        assert_eq!(comparison.topn_overlap, Probability::new(dec!(1)));
        assert_eq!(comparison.score_delta.mean_abs_score_delta, dec!(0));
        assert_eq!(comparison.rank_delta.max_rank_delta, 0);
        assert!(!comparison.hard_divergence);
    }

    #[test]
    fn large_score_gap_flags_hard_divergence() {
        let active = vec![candidate("a", 1, dec!(0.90), OutcomeSide::Yes)];
        let shadow = vec![candidate("a", 1, dec!(0.40), OutcomeSide::No)];
        let comparison = compute_shadow_comparison(
            ModelVersionId::from_v7(),
            ModelVersionId::from_v7(),
            Utc::now(),
            &active,
            &shadow,
            5,
            dec!(0.10),
        )
        .expect("comparison");
        assert!(comparison.hard_divergence);
        assert_eq!(comparison.score_delta.mean_abs_score_delta, dec!(0.5));
        assert_eq!(comparison.score_delta.side_disagreement_rate, dec!(1));
    }

    #[test]
    fn shadow_comparison_records_topn_delta() {
        // Active ranks a > b; shadow flips them → rank delta + partial overlap.
        let active = vec![
            candidate("a", 1, dec!(0.90), OutcomeSide::Yes),
            candidate("b", 2, dec!(0.70), OutcomeSide::Yes),
            candidate("c", 3, dec!(0.50), OutcomeSide::Yes),
        ];
        let shadow = vec![
            candidate("b", 1, dec!(0.88), OutcomeSide::Yes),
            candidate("a", 2, dec!(0.72), OutcomeSide::Yes),
            candidate("d", 3, dec!(0.40), OutcomeSide::Yes),
        ];
        let comparison = compute_shadow_comparison(
            ModelVersionId::from_v7(),
            ModelVersionId::from_v7(),
            Utc::now(),
            &active,
            &shadow,
            2,
            dec!(0.10),
        )
        .expect("comparison");
        // a and b are common; both moved one rank.
        assert_eq!(comparison.rank_delta.common_markets, 2);
        assert_eq!(comparison.rank_delta.max_rank_delta, 1);
        assert!(comparison.score_delta.mean_abs_score_delta > dec!(0));
        // TopN(2): active {a,b}, shadow {a,b} → full overlap of the top-2 set.
        assert_eq!(comparison.topn_overlap, Probability::new(dec!(1)));
        // Inverted ranks ⇒ negative spearman over the two common markets.
        assert!(comparison.rank_delta.spearman <= dec!(0));
    }

    #[test]
    fn disjoint_topn_has_zero_overlap() {
        let active = vec![candidate("a", 1, dec!(0.9), OutcomeSide::Yes)];
        let shadow = vec![candidate("b", 1, dec!(0.9), OutcomeSide::Yes)];
        let comparison = compute_shadow_comparison(
            ModelVersionId::from_v7(),
            ModelVersionId::from_v7(),
            Utc::now(),
            &active,
            &shadow,
            5,
            dec!(0.10),
        )
        .expect("comparison");
        assert_eq!(comparison.topn_overlap, Probability::new(dec!(0)));
        assert_eq!(comparison.rank_delta.common_markets, 0);
    }
}
