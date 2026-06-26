//! Rank score columns derived from factor breakdown.

use quant_pivot_models::types::Probability;

use crate::{
    factors::names::{DATA_QUALITY, LIQUIDITY_DEPTH},
    model::{FactorContribution, SignalCandidate, SignalWarning},
};

/// Normalized liquidity and data-quality scores for ranking and persistence.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RankScores {
    /// Cross-sectional liquidity depth factor score in `[0, 1]`.
    pub liquidity_score: Probability,
    /// Aggregate data-quality factor score in `[0, 1]`.
    pub data_quality_score: Probability,
}

/// Attach rank scores to a candidate from its factor breakdown.
pub fn attach(candidate: &mut SignalCandidate) {
    let scores = RankScores::derive(candidate);
    candidate.liquidity_score = scores.liquidity_score;
    candidate.data_quality_score = scores.data_quality_score;
}

impl RankScores {
    /// Derive scores from `factor_breakdown`; missing factors become zero with warnings.
    #[must_use]
    pub fn derive(candidate: &mut SignalCandidate) -> Self {
        let liquidity_score = score_from_breakdown(&candidate.factor_breakdown, &LIQUIDITY_DEPTH)
            .unwrap_or_else(|| {
                push_missing_warning(candidate, "liquidity_depth");
                Probability::ZERO
            });
        let data_quality_score = score_from_breakdown(&candidate.factor_breakdown, &DATA_QUALITY)
            .unwrap_or_else(|| {
                push_missing_warning(candidate, "data_quality");
                Probability::ZERO
            });
        Self {
            liquidity_score,
            data_quality_score,
        }
    }
}

fn score_from_breakdown(
    breakdown: &[FactorContribution],
    factor_name: &crate::factors::FactorName,
) -> Option<Probability> {
    breakdown
        .iter()
        .find(|contribution| &contribution.name == factor_name)
        .map(|contribution| contribution.normalized_score)
}

fn push_missing_warning(candidate: &mut SignalCandidate, factor: &str) {
    let message = format!("missing {factor} factor in breakdown");
    if !candidate
        .rejection_warnings
        .iter()
        .any(|warning| matches!(warning, SignalWarning::Other(text) if text == &message))
    {
        candidate
            .rejection_warnings
            .push(SignalWarning::Other(message));
    }
}

#[cfg(test)]
mod tests {
    use super::{RankScores, attach};
    use crate::{
        factors::names::{DATA_QUALITY, LIQUIDITY_DEPTH},
        model::{FactorContribution, ModelExplanation, SignalCandidate, SignalWarning},
    };
    use chrono::Utc;
    use quant_pivot_models::{
        enums::{
            factor::FactorFamily,
            quant::{FactorDirection, OutcomeSide},
        },
        types::{
            FactorDefinitionId, MarketId, ModelRunId, Price, Probability, SignalCandidateId,
            TokenId,
        },
    };
    use rust_decimal::Decimal;
    use rust_decimal_macros::dec;

    fn contribution(name: crate::factors::FactorName, score: Decimal) -> FactorContribution {
        FactorContribution {
            definition_id: FactorDefinitionId::from_v7(),
            name,
            family: FactorFamily::Liquidity,
            raw_value: Some(score),
            normalized_score: Probability::new(score),
            weight: dec!(1),
            contribution: score,
            confidence: Probability::new(dec!(1)),
            direction: FactorDirection::Positive,
            explanation: "test".to_owned(),
            source_refs: Vec::new(),
        }
    }

    fn candidate(breakdown: Vec<FactorContribution>) -> SignalCandidate {
        SignalCandidate {
            signal_candidate_id: SignalCandidateId::from_v7(),
            model_run_id: ModelRunId::from_v7(),
            market_id: MarketId::new("0xmarket"),
            token_id: TokenId::new("token-1"),
            outcome_side: OutcomeSide::Yes,
            composite_score: Probability::new(dec!(0.5)),
            confidence: Probability::new(dec!(0.5)),
            expected_return_bps: dec!(100),
            downside_bps: dec!(50),
            entry_price_ref: Price::new(dec!(0.5)),
            suggested_horizon_secs: 3_600,
            factor_breakdown: breakdown,
            model_explanation: ModelExplanation {
                headline: "test".to_owned(),
                top_positive: Vec::new(),
                top_negative: Vec::new(),
            },
            rejection_warnings: Vec::new(),
            rank_before_portfolio: 0,
            liquidity_score: Probability::ZERO,
            data_quality_score: Probability::ZERO,
            model_score_percentile: Probability::ZERO,
            as_of: Utc::now(),
        }
    }

    #[test]
    fn derive_reads_factor_breakdown() {
        let mut candidate = candidate(vec![
            contribution(LIQUIDITY_DEPTH, dec!(0.8)),
            contribution(DATA_QUALITY, dec!(0.9)),
        ]);
        let scores = RankScores::derive(&mut candidate);
        assert_eq!(scores.liquidity_score.inner(), dec!(0.8));
        assert_eq!(scores.data_quality_score.inner(), dec!(0.9));
        assert!(candidate.rejection_warnings.is_empty());
    }

    #[test]
    fn derive_missing_factors_zero_with_warning() {
        let mut candidate = candidate(Vec::new());
        attach(&mut candidate);
        assert_eq!(candidate.liquidity_score, Probability::ZERO);
        assert_eq!(candidate.data_quality_score, Probability::ZERO);
        assert_eq!(candidate.rejection_warnings.len(), 2);
        assert!(
            candidate
                .rejection_warnings
                .iter()
                .all(|warning| matches!(warning, SignalWarning::Other(_)))
        );
    }
}
