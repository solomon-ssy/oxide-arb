//! Shadow comparison at the signal-candidate / ranking layer.
//!
//! Pure computation: given the **active** model's ranked candidates and a
//! **shadow** model's ranked candidates for the same `as_of` cross-section, it
//! quantifies how far the shadow diverges from production at the candidate
//! ranking layer: signed `TopN` decision overlap (`market_id` + `outcome_side`),
//! per-market rank delta, and per-market score delta. Report-level deltas such
//! as capital allocation, would-execute, and risk envelope are outside this
//! component's contract.
//!
//! The result is content-addressed and persisted to `quant_shadow_comparison`
//! by the core `ModelRunner`; a `hard_divergence` raises a critical alert but
//! never auto-switches the active model.

use std::collections::{BTreeMap, BTreeSet};

use chrono::{DateTime, Utc};
use quant_pivot_error::QuantResult;
use quant_pivot_models::{
    enums::{
        common::MarketCategory,
        quant::{ModelWeightSource, OutcomeSide},
    },
    types::{
        ContentHash, DecisionPolicySnapshotId, ModelVersionId, PolicyBundleGeneration, Probability,
        ResearchProfileArtifactId, ShadowComparisonId,
        shadow::{ShadowComparison, ShadowRankDelta, ShadowScoreDelta},
    },
};
use rust_decimal::Decimal;
use serde::Serialize;

use crate::{
    hashing::ResearchHasher, model::SignalCandidate, precision::RESEARCH_DECIMAL_SCALE, stats,
};

/// Canonical, surrogate-free projection for content addressing.
#[derive(Serialize)]
struct ComparisonHashInput<'a> {
    champion_model_version_id: &'a ModelVersionId,
    candidate_model_version_id: &'a ModelVersionId,
    champion_serving_contract_hash: ContentHash,
    candidate_serving_contract_hash: ContentHash,
    research_profile_artifact_id: &'a ResearchProfileArtifactId,
    category_scope: Option<MarketCategory>,
    decision_policy_snapshot_id: DecisionPolicySnapshotId,
    decision_policy_snapshot_hash: ContentHash,
    policy_bundle_generation: PolicyBundleGeneration,
    weight_source: ModelWeightSource,
    decision_at: DateTime<Utc>,
    topn_decision_overlap: &'a Probability,
    rank_delta: &'a ShadowRankDelta,
    score_delta: &'a ShadowScoreDelta,
    hard_divergence: bool,
}

/// One candidate's ranking position projected for comparison.
struct Ranked {
    rank: u32,
    score: Decimal,
    outcome_side: OutcomeSide,
}

/// Complete immutable input to one active-vs-shadow comparison.
pub struct ShadowComparisonRequest<'a> {
    pub champion_model_version_id: ModelVersionId,
    pub candidate_model_version_id: ModelVersionId,
    pub champion_serving_contract_hash: ContentHash,
    pub candidate_serving_contract_hash: ContentHash,
    pub research_profile_artifact_id: ResearchProfileArtifactId,
    pub category_scope: Option<MarketCategory>,
    pub decision_policy_snapshot_id: DecisionPolicySnapshotId,
    pub decision_policy_snapshot_hash: ContentHash,
    pub policy_bundle_generation: PolicyBundleGeneration,
    pub weight_source: ModelWeightSource,
    pub decision_at: DateTime<Utc>,
    pub active: &'a [SignalCandidate],
    pub shadow: &'a [SignalCandidate],
    pub top_n: usize,
    pub score_divergence_threshold: Decimal,
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
    request: &ShadowComparisonRequest<'_>,
) -> QuantResult<ShadowComparison> {
    let champion_model_version_id = request.champion_model_version_id;
    let candidate_model_version_id = request.candidate_model_version_id;
    let champion_serving_contract_hash = request.champion_serving_contract_hash;
    let candidate_serving_contract_hash = request.candidate_serving_contract_hash;
    let research_profile_artifact_id = request.research_profile_artifact_id.clone();
    let category_scope = request.category_scope;
    let decision_policy_snapshot_id = request.decision_policy_snapshot_id;
    let decision_policy_snapshot_hash = request.decision_policy_snapshot_hash;
    let policy_bundle_generation = request.policy_bundle_generation;
    let weight_source = request.weight_source;
    let decision_at = request.decision_at;
    let active = request.active;
    let shadow = request.shadow;
    let top_n = request.top_n;
    let score_divergence_threshold = request.score_divergence_threshold;
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

    let topn_decision_overlap = topn_decision_overlap(active, shadow, top_n);
    let hard_divergence = mean_abs_score_delta > score_divergence_threshold;

    let rank_delta = ShadowRankDelta {
        mean_abs_rank_delta,
        max_rank_delta,
        spearman,
        common_markets: common_count,
    };
    let score_delta = ShadowScoreDelta {
        mean_abs_score_delta,
        max_score_delta,
        side_disagreement_rate,
    };

    let comparison_hash = ResearchHasher::canonical(&ComparisonHashInput {
        champion_model_version_id: &champion_model_version_id,
        candidate_model_version_id: &candidate_model_version_id,
        champion_serving_contract_hash,
        candidate_serving_contract_hash,
        research_profile_artifact_id: &research_profile_artifact_id,
        category_scope,
        decision_policy_snapshot_id,
        decision_policy_snapshot_hash,
        policy_bundle_generation,
        weight_source,
        decision_at,
        topn_decision_overlap: &topn_decision_overlap,
        rank_delta: &rank_delta,
        score_delta: &score_delta,
        hard_divergence,
    })?;

    Ok(ShadowComparison {
        shadow_comparison_id: ShadowComparisonId::from_v7(),
        champion_model_version_id,
        candidate_model_version_id,
        champion_serving_contract_hash,
        candidate_serving_contract_hash,
        research_profile_artifact_id,
        category_scope,
        decision_policy_snapshot_id,
        decision_policy_snapshot_hash,
        policy_bundle_generation,
        weight_source,
        decision_at,
        topn_decision_overlap,
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
                candidate.market_id.to_string(),
                Ranked {
                    rank: candidate.rank_before_portfolio,
                    score: candidate.composite_score.inner(),
                    outcome_side: candidate.outcome_side,
                },
            )
        })
        .collect()
}

/// Jaccard overlap of the two signed `TopN` decision sets.
///
/// A decision identity is `(market_id, outcome_side)`: selecting the same
/// market on the opposite side is diametrically different economic behavior
/// and therefore has zero intersection. Empty `TopN` sets carry no stability
/// evidence and score `0`; otherwise the overlap is `|∩| / |∪|`.
fn topn_decision_overlap(
    active: &[SignalCandidate],
    shadow: &[SignalCandidate],
    top_n: usize,
) -> Probability {
    let active_top = top_decisions(active, top_n);
    let shadow_top = top_decisions(shadow, top_n);
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

/// Signed decisions ranked within the `TopN` (`rank_before_portfolio <= top_n`).
fn top_decisions(candidates: &[SignalCandidate], top_n: usize) -> BTreeSet<(String, i8)> {
    let bound = u32::try_from(top_n).unwrap_or(u32::MAX);
    candidates
        .iter()
        .filter(|candidate| {
            candidate.rank_before_portfolio >= 1 && candidate.rank_before_portfolio <= bound
        })
        .map(|candidate| {
            (
                candidate.market_id.to_string(),
                candidate.outcome_side.as_i8(),
            )
        })
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
    use chrono::Utc;
    use quant_pivot_models::{
        enums::quant::{ModelWeightSource, OutcomeSide},
        types::{
            ContentHash, DecisionPolicySnapshotId, MarketId, ModelRunId, ModelVersionId,
            PolicyBundleGeneration, Price, Probability, ResearchProfileArtifactId,
            SignalCandidateId, TokenId, builtin_research_profiles,
        },
    };
    use rust_decimal::Decimal;
    use rust_decimal_macros::dec;

    use super::{ShadowComparisonRequest, compute_shadow_comparison};
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
            win_probability: None,
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
            liquidity_score: Probability::ZERO,
            data_quality_score: Probability::ZERO,
            model_score_percentile: Probability::ZERO,
            decision_at: Utc::now(),
        }
    }

    fn request<'a>(
        active: &'a [SignalCandidate],
        shadow: &'a [SignalCandidate],
        top_n: usize,
        score_divergence_threshold: Decimal,
    ) -> ShadowComparisonRequest<'a> {
        let profile = builtin_research_profiles()
            .expect("built-in profiles")
            .remove(0)
            .profile_ref;
        ShadowComparisonRequest {
            champion_model_version_id: ModelVersionId::from_v7(),
            candidate_model_version_id: ModelVersionId::from_v7(),
            champion_serving_contract_hash: ContentHash::from_bytes([1; 32]),
            candidate_serving_contract_hash: ContentHash::from_bytes([2; 32]),
            research_profile_artifact_id: ResearchProfileArtifactId::from_profile_ref(&profile),
            category_scope: profile
                .resolve_builtin_research_profile()
                .expect("profile")
                .spec
                .category,
            decision_policy_snapshot_id: DecisionPolicySnapshotId::from_v7(),
            decision_policy_snapshot_hash: ContentHash::from_bytes([3; 32]),
            policy_bundle_generation: PolicyBundleGeneration::FIRST,
            weight_source: ModelWeightSource::Artifact,
            decision_at: Utc::now(),
            active,
            shadow,
            top_n,
            score_divergence_threshold,
        }
    }

    #[test]
    fn identical_rankings_no_divergence() {
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
        let comparison = compute_shadow_comparison(&request(&active, &shadow, 5, dec!(0.10)))
            .expect("comparison");
        assert_eq!(comparison.topn_decision_overlap, Probability::new(dec!(1)));
        assert_eq!(comparison.score_delta.mean_abs_score_delta, dec!(0));
        assert_eq!(comparison.rank_delta.max_rank_delta, 0);
        assert!(!comparison.hard_divergence);
    }

    #[test]
    fn large_score_gap_divergence() {
        let active = vec![candidate("a", 1, dec!(0.90), OutcomeSide::Yes)];
        let shadow = vec![candidate("a", 1, dec!(0.40), OutcomeSide::No)];
        let comparison = compute_shadow_comparison(&request(&active, &shadow, 5, dec!(0.10)))
            .expect("comparison");
        assert!(comparison.hard_divergence);
        assert_eq!(comparison.topn_decision_overlap, Probability::ZERO);
        assert_eq!(comparison.score_delta.mean_abs_score_delta, dec!(0.5));
        assert_eq!(comparison.score_delta.side_disagreement_rate, dec!(1));
    }

    #[test]
    fn shadow_comparison_topn_delta() {
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
        let comparison = compute_shadow_comparison(&request(&active, &shadow, 2, dec!(0.10)))
            .expect("comparison");
        // a and b are common; both moved one rank.
        assert_eq!(comparison.rank_delta.common_markets, 2);
        assert_eq!(comparison.rank_delta.max_rank_delta, 1);
        assert!(comparison.score_delta.mean_abs_score_delta > dec!(0));
        // TopN(2): active {a,b}, shadow {a,b} → full overlap of the top-2 set.
        assert_eq!(comparison.topn_decision_overlap, Probability::new(dec!(1)));
        // Inverted ranks ⇒ negative spearman over the two common markets.
        assert!(comparison.rank_delta.spearman <= dec!(0));
    }

    #[test]
    fn disjoint_topn_zero_overlap() {
        let active = vec![candidate("a", 1, dec!(0.9), OutcomeSide::Yes)];
        let shadow = vec![candidate("b", 1, dec!(0.9), OutcomeSide::Yes)];
        let comparison = compute_shadow_comparison(&request(&active, &shadow, 5, dec!(0.10)))
            .expect("comparison");
        assert_eq!(comparison.topn_decision_overlap, Probability::ZERO);
        assert_eq!(comparison.rank_delta.common_markets, 0);
    }

    #[test]
    fn empty_rankings_not_evidence() {
        let comparison =
            compute_shadow_comparison(&request(&[], &[], 5, dec!(0.10))).expect("comparison");

        assert_eq!(comparison.topn_decision_overlap, Probability::ZERO);
        assert_eq!(comparison.rank_delta.common_markets, 0);
        assert!(!comparison.hard_divergence);
    }
}
