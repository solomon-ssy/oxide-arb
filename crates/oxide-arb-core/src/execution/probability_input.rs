//! Bridge between scored opportunities and the Kelly calculator's
//! `ProbabilityInput` — single canonical construction point.

use oxide_arb_algorithm::scorer::ScoredOpportunity;
use oxide_arb_models::domain::risk::ProbabilityInput;
use rust_decimal_macros::dec;

#[inline]
pub fn build_probability_input(scored: &ScoredOpportunity) -> ProbabilityInput {
    let opp = &scored.opportunity;
    let cal = &opp.calibration;

    ProbabilityInput {
        calibrated_win_prob: cal.fused_probability,
        fill_prob: scored.fill_probability.to_decimal(),
        calibration_confidence: opp.meta.confidence,
        sample_size: cal.sample_size,
        model_staleness_secs: 0,
        expected_slippage_pct: dec!(0.005),
        expected_failure_cost_pct: dec!(0.002),
    }
}
