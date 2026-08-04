//! Within-batch composite-score percentile annotation.

use quant_pivot_error::{QuantResult, research::ResearchError};
use quant_pivot_models::types::Probability;
use rust_decimal::Decimal;

use crate::model::SignalCandidate;

/// Establish the canonical pre-portfolio order, rank, and score percentile.
///
/// The runtime emits unranked candidates. Every caller that persists or hashes
/// business predictions must pass through this finalizer so online serving,
/// deterministic replay, and fixture generation share the same contract.
pub fn finalize_candidates(candidates: &mut [SignalCandidate]) -> QuantResult<()> {
    candidates.sort_by(|left, right| {
        right
            .composite_score
            .inner()
            .cmp(&left.composite_score.inner())
            .then_with(|| left.market_id.cmp(&right.market_id))
            .then_with(|| left.token_id.cmp(&right.token_id))
            .then_with(|| left.outcome_side.as_str().cmp(right.outcome_side.as_str()))
    });
    for (index, candidate) in candidates.iter_mut().enumerate() {
        candidate.rank_before_portfolio =
            u32::try_from(index + 1).map_err(|error| ResearchError::Inference {
                detail: format!("global candidate rank does not fit u32: {error}"),
            })?;
    }
    annotate(candidates);
    Ok(())
}

/// Annotate each candidate with its within-batch score percentile in `(0, 1]`.
///
/// Ties break on `market_id` for determinism. Empty input is a no-op.
fn annotate(candidates: &mut [SignalCandidate]) {
    let n = candidates.len();
    if n == 0 {
        return;
    }
    let mut order: Vec<usize> = (0..n).collect();
    order.sort_by(|left, right| {
        candidates[*left]
            .composite_score
            .inner()
            .cmp(&candidates[*right].composite_score.inner())
            .then_with(|| {
                candidates[*left]
                    .market_id
                    .cmp(&candidates[*right].market_id)
            })
    });
    let divisor = Decimal::from(n);
    for (rank, index) in order.into_iter().enumerate() {
        let percentile = Decimal::from(rank + 1) / divisor;
        candidates[index].model_score_percentile = Probability::new(percentile);
    }
}

#[cfg(test)]
mod tests {
    use chrono::Utc;
    use quant_pivot_error::QuantResult;
    use quant_pivot_models::{
        enums::quant::OutcomeSide,
        types::{MarketId, ModelRunId, Price, Probability, SignalCandidateId, TokenId},
    };
    use rust_decimal::Decimal;
    use rust_decimal_macros::dec;

    use super::finalize_candidates;
    use crate::model::{ModelExplanation, SignalCandidate, canonical_business_prediction_hash};

    fn candidate(market: &str, score: Decimal) -> SignalCandidate {
        SignalCandidate {
            signal_candidate_id: SignalCandidateId::from_v7(),
            model_run_id: ModelRunId::from_v7(),
            market_id: MarketId::new(market),
            token_id: TokenId::new(format!("token-{market}")),
            outcome_side: OutcomeSide::Yes,
            composite_score: Probability::new(score),
            confidence: Probability::new(dec!(0.5)),
            expected_return_bps: dec!(100),
            downside_bps: dec!(50),
            win_probability: None,
            entry_price_ref: Price::new(dec!(0.5)),
            suggested_horizon_secs: 3_600,
            factor_breakdown: Vec::new(),
            model_explanation: ModelExplanation {
                headline: "test".to_owned(),
                top_positive: Vec::new(),
                top_negative: Vec::new(),
            },
            rejection_warnings: Vec::new(),
            rank_before_portfolio: 1,
            liquidity_score: Probability::ZERO,
            data_quality_score: Probability::ZERO,
            model_score_percentile: Probability::ZERO,
            decision_at: Utc::now(),
        }
    }

    #[test]
    fn finalizer_assigns_rank_percentile() -> QuantResult<()> {
        let mut batch = vec![
            candidate("a", dec!(0.2)),
            candidate("b", dec!(0.8)),
            candidate("c", dec!(0.5)),
        ];
        finalize_candidates(&mut batch)?;
        assert_eq!(batch[0].market_id.as_str(), "b");
        assert_eq!(batch[0].rank_before_portfolio, 1);
        assert_eq!(batch[0].model_score_percentile.inner(), dec!(1));
        assert_eq!(
            batch[2].model_score_percentile.inner(),
            dec!(0.3333333333333333333333333333)
        );
        Ok(())
    }

    #[test]
    fn finalizer_aligns_prediction_hash() -> QuantResult<()> {
        let candidates = vec![
            candidate("a", dec!(0.2)),
            candidate("b", dec!(0.8)),
            candidate("c", dec!(0.5)),
        ];
        let mut online = candidates.clone();
        finalize_candidates(&mut online)?;

        let mut replay = candidates;
        replay.reverse();
        assert_ne!(
            canonical_business_prediction_hash(&online)?,
            canonical_business_prediction_hash(&replay)?,
            "unfinalized replay must not silently match finalized serving output"
        );

        finalize_candidates(&mut replay)?;
        assert_eq!(
            canonical_business_prediction_hash(&online)?,
            canonical_business_prediction_hash(&replay)?,
            "all serving/replay producers must converge through one finalizer"
        );
        Ok(())
    }
}
