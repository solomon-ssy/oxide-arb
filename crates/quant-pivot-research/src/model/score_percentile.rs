//! Within-batch composite-score percentile annotation.

use quant_pivot_models::types::Probability;
use rust_decimal::Decimal;

use crate::model::SignalCandidate;

/// Annotate each candidate with its within-batch score percentile in `(0, 1]`.
///
/// Ties break on `market_id` for determinism. Empty input is a no-op.
pub fn annotate(candidates: &mut [SignalCandidate]) {
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
    use super::annotate;
    use crate::model::{ModelExplanation, SignalCandidate};
    use chrono::Utc;
    use quant_pivot_models::{
        enums::quant::OutcomeSide,
        types::{MarketId, ModelRunId, Price, Probability, SignalCandidateId, TokenId},
    };
    use rust_decimal::Decimal;
    use rust_decimal_macros::dec;

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
            as_of: Utc::now(),
        }
    }

    #[test]
    fn annotate_assigns_monotonic_percentiles() {
        let mut batch = vec![
            candidate("a", dec!(0.2)),
            candidate("b", dec!(0.8)),
            candidate("c", dec!(0.5)),
        ];
        annotate(&mut batch);
        assert_eq!(batch[1].model_score_percentile.inner(), dec!(1));
        assert_eq!(
            batch[0].model_score_percentile.inner(),
            dec!(0.3333333333333333333333333333)
        );
    }
}
