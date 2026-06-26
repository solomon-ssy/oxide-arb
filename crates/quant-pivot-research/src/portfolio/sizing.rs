//! Pluggable position-sizing models (the "how much per candidate" primitive).
//!
//! A [`SizingModel`] turns one scored [`SignalCandidate`] plus the capital base
//! into a *desired* USD size — the amount to deploy **before** portfolio caps,
//! liquidity, and available-cash convergence (those belong to the allocator /
//! planner). Sizing is a pure function: no I/O, no clock, no mutable state, so a
//! plan is deterministically replayable.
//!
//! # Kelly with confidence shrinkage (production default)
//!
//! Polymarket outcome tokens are binary. We model "hold to the horizon, exit at
//! target or stop" as a two-outcome bet `{win +g, lose −l}`:
//!
//! ```text
//! l = downside_bps / 10_000                      // stop-loss fraction of stake
//! E[r] = expected_return_bps / 10_000            // model's expected mean return
//! g = R · l                                       // target gain (R = target_reward_multiple)
//! q = clamp((E[r] + l) / (g + l), 0, 1)           // recovered win probability
//! f* = (q·g − (1−q)·l) / (g·l)                     // two-outcome Kelly fraction
//! ```
//!
//! `confidence` is the model's **evidence quality**, *not* a calibrated
//! probability, so it never stands in for `q`. Instead it shrinks the bet — the
//! production-standard mitigation for Kelly's sensitivity to edge
//! mis-estimation (fractional Kelly + uncertainty shrinkage):
//!
//! ```text
//! f = clamp(kelly_fraction · confidence_shrink(confidence) · drawdown_scale,
//!           0, max_position_pct)
//! desired_usd = round(f · equity)
//! ```
//!
//! `f* ≤ 0` (no positive edge) ⇒ the candidate is **rejected**, never funded.
//! Every intermediate stays in [`Decimal`]; `f64` never appears, so no precision
//! drift can leak into a money value.

use quant_pivot_error::{QuantError, QuantResult};
use quant_pivot_models::{
    enums::quant::{RejectionReason, SizingModelKind},
    runtime_config::{ConfidenceSizeCurve, DrawdownMultiplierPolicy, SizingModelConfig},
    types::{Bps, Usd},
};
use rust_decimal::Decimal;

use crate::{model::signal::SignalCandidate, precision::RESEARCH_DECIMAL_SCALE};

/// Basis-point denominator (`1 bps = 1/10_000`).
const BPS_PER_UNIT: i64 = 10_000;

/// Decision-time drawdown state driving the conservative scaling policy.
///
/// Phase 4 has no cross-tick equity history, so the only constructible state is
/// [`DrawdownState::neutral`] (no drawdown). The field is kept so Phase 5 can
/// feed a real equity-curve drawdown without changing the sizing signature.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DrawdownState {
    /// Current peak-to-trough drawdown as a fraction in `[0, 1]` (`0` = none).
    pub current_drawdown: Decimal,
}

impl DrawdownState {
    /// The neutral (no-drawdown) state used for every Phase 4 plan.
    #[must_use]
    pub const fn neutral() -> Self {
        Self {
            current_drawdown: Decimal::ZERO,
        }
    }
}

impl Default for DrawdownState {
    fn default() -> Self {
        Self::neutral()
    }
}

/// Inputs to one sizing decision (borrowed candidate + capital base).
pub struct SizingInput<'a> {
    /// The scored candidate being sized.
    pub candidate: &'a SignalCandidate,
    /// Capital base for sizing (`equity = min(net liquidation, budget cap)`).
    pub equity_usd: Usd,
    /// Decision-time drawdown state (drives `drawdown_scaling`).
    pub drawdown_state: DrawdownState,
}

/// The outcome of sizing one candidate.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SizingOutcome {
    /// The candidate carries a fundable size (before portfolio caps).
    Sized(SizingSuggestion),
    /// The candidate must not be funded for the given reason.
    Rejected(RejectionReason),
}

/// A fundable size suggestion plus its audit provenance.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SizingSuggestion {
    /// Desired USD size before portfolio caps / liquidity / available cash.
    pub desired_usd: Usd,
    /// Per-unit edge in basis points (`None` for the edge-free curve model).
    pub edge_bps: Option<Bps>,
    /// Fractional-Kelly multiplier applied (`kelly_fraction · shrink · drawdown`);
    /// `None` for the edge-free curve model.
    pub kelly_fraction_applied: Option<Decimal>,
    /// Whether the per-position equity cap (`max_position_pct`) bound the size.
    pub binding_kelly_cap: bool,
}

/// A position-sizing model (pure: no I/O, no clock, no mutable state).
pub trait SizingModel: Send + Sync {
    /// The model family, recorded on every [`crate::portfolio::SizingPlan`].
    fn kind(&self) -> SizingModelKind;

    /// Suggest a desired USD size for one candidate, or reject it.
    fn suggest(&self, input: &SizingInput<'_>) -> QuantResult<SizingOutcome>;
}

/// Fractional-Kelly sizing with confidence-uncertainty shrinkage.
#[derive(Debug, Clone, Copy)]
pub struct KellySizingModel {
    /// Static fraction of full Kelly (`(0, 1]`).
    kelly_fraction: Decimal,
    /// Hard per-position cap as a fraction of equity (`(0, 1]`).
    max_position_pct: Decimal,
    /// Reward-to-risk multiple `R` (`> 0`): target gain = `R × downside`.
    target_reward_multiple: Decimal,
    /// Confidence → shrinkage curve.
    confidence_weighting: ConfidenceSizeCurve,
    /// Drawdown scaling policy.
    drawdown_scaling: DrawdownMultiplierPolicy,
}

impl KellySizingModel {
    /// Construct a Kelly model from already-parsed parameters.
    #[must_use]
    pub const fn new(
        kelly_fraction: Decimal,
        max_position_pct: Decimal,
        target_reward_multiple: Decimal,
        confidence_weighting: ConfidenceSizeCurve,
        drawdown_scaling: DrawdownMultiplierPolicy,
    ) -> Self {
        Self {
            kelly_fraction,
            max_position_pct,
            target_reward_multiple,
            confidence_weighting,
            drawdown_scaling,
        }
    }
}

impl SizingModel for KellySizingModel {
    fn kind(&self) -> SizingModelKind {
        SizingModelKind::Kelly
    }

    fn suggest(&self, input: &SizingInput<'_>) -> QuantResult<SizingOutcome> {
        let bps = Decimal::from(BPS_PER_UNIT);
        let candidate = input.candidate;

        let loss = candidate.downside_bps / bps;
        // A non-positive downside means the model gave no valid stop; never
        // fabricate a bet structure.
        if loss <= Decimal::ZERO {
            return Ok(SizingOutcome::Rejected(RejectionReason::InvalidEdgeInputs));
        }
        let expected = candidate.expected_return_bps / bps;
        let gain = self.target_reward_multiple * loss;
        if gain <= Decimal::ZERO {
            return Ok(SizingOutcome::Rejected(RejectionReason::InvalidEdgeInputs));
        }

        // Recover the win probability from the bet structure, then re-derive the
        // edge from the clamped `q` so a degenerate input cannot inflate it.
        let q = ((expected + loss) / (gain + loss)).clamp(Decimal::ZERO, Decimal::ONE);
        let edge = q * gain - (Decimal::ONE - q) * loss;
        if edge <= Decimal::ZERO {
            return Ok(SizingOutcome::Rejected(RejectionReason::NoPositiveSignal));
        }
        let f_star = edge / (gain * loss);

        let shrink = confidence_shrink(candidate.confidence.inner(), self.confidence_weighting);
        let drawdown = drawdown_scale(input.drawdown_state, self.drawdown_scaling);
        let multiplier = (self.kelly_fraction * shrink * drawdown).max(Decimal::ZERO);

        let raw_fraction = multiplier * f_star;
        let binding_kelly_cap = raw_fraction > self.max_position_pct;
        let fraction = raw_fraction.min(self.max_position_pct).max(Decimal::ZERO);

        let desired_usd = (input.equity_usd * fraction).round_dp(RESEARCH_DECIMAL_SCALE);
        let edge_bps = Bps::new((edge * bps).round_dp(RESEARCH_DECIMAL_SCALE));

        Ok(SizingOutcome::Sized(SizingSuggestion {
            desired_usd,
            edge_bps: Some(edge_bps),
            kelly_fraction_applied: Some(multiplier.round_dp(RESEARCH_DECIMAL_SCALE)),
            binding_kelly_cap,
        }))
    }
}

/// Build the active sizing model from the governed config.
///
/// Kelly is the only sizing model; the returned trait object keeps the planner
/// decoupled from the concrete type and leaves room for future models.
///
/// # Errors
///
/// Returns [`QuantError::config`] when any decimal parameter is malformed
/// (runtime-config validation rejects these upstream, so this is a hard guard).
pub fn sizing_model_from_config(sizing: &SizingModelConfig) -> QuantResult<Box<dyn SizingModel>> {
    Ok(Box::new(KellySizingModel::new(
        parse_decimal(
            "portfolio.sizing.kelly_fraction",
            &sizing.kelly_fraction.value,
        )?,
        parse_decimal(
            "portfolio.sizing.max_position_pct",
            &sizing.max_position_pct.value,
        )?,
        parse_decimal(
            "portfolio.sizing.target_reward_multiple",
            &sizing.target_reward_multiple.value,
        )?,
        sizing.confidence_weighting,
        sizing.drawdown_scaling,
    )))
}

/// Map a confidence in `[0, 1]` to a shrinkage multiplier in `[0, 1]`.
///
/// `Linear` shrinks proportionally to confidence; `Step` applies conservative
/// bucket constants so a low-evidence candidate is compressed hard.
fn confidence_shrink(confidence: Decimal, curve: ConfidenceSizeCurve) -> Decimal {
    let confidence = confidence.clamp(Decimal::ZERO, Decimal::ONE);
    match curve {
        ConfidenceSizeCurve::Linear => confidence,
        ConfidenceSizeCurve::Step => {
            if confidence < Decimal::new(5, 1) {
                Decimal::new(25, 2)
            } else if confidence < Decimal::new(8, 1) {
                Decimal::new(5, 1)
            } else {
                Decimal::ONE
            }
        }
    }
}

/// Map the drawdown state to a scaling multiplier in `[0, 1]`.
///
/// `Fixed` ignores drawdown (always `1`). `Conservative` scales down linearly
/// with the current drawdown. Phase 4 is always neutral, so both yield `1`.
fn drawdown_scale(state: DrawdownState, policy: DrawdownMultiplierPolicy) -> Decimal {
    match policy {
        DrawdownMultiplierPolicy::Fixed => Decimal::ONE,
        DrawdownMultiplierPolicy::Conservative => {
            (Decimal::ONE - state.current_drawdown).clamp(Decimal::ZERO, Decimal::ONE)
        }
    }
}

/// Parse a config decimal, attributing a malformed value to its field path.
fn parse_decimal(field: &str, value: &str) -> QuantResult<Decimal> {
    value
        .parse::<Decimal>()
        .map_err(|error| QuantError::config(format!("{field} is not a valid decimal: {error}")))
}

#[cfg(test)]
mod tests {
    use super::{
        ConfidenceSizeCurve, DrawdownMultiplierPolicy, DrawdownState, KellySizingModel,
        SizingInput, SizingModel, SizingOutcome,
    };
    use chrono::Utc;
    use quant_pivot_models::{
        enums::quant::{OutcomeSide, RejectionReason, SizingModelKind},
        types::{MarketId, ModelRunId, Price, Probability, SignalCandidateId, TokenId, Usd},
    };
    use rust_decimal::Decimal;
    use rust_decimal_macros::dec;

    use crate::model::signal::{ModelExplanation, SignalCandidate};

    fn candidate(
        expected_bps: Decimal,
        downside_bps: Decimal,
        confidence: Decimal,
    ) -> SignalCandidate {
        SignalCandidate {
            signal_candidate_id: SignalCandidateId::from_v7(),
            model_run_id: ModelRunId::from_v7(),
            market_id: MarketId::new("0xmarket"),
            token_id: TokenId::new("yes"),
            outcome_side: OutcomeSide::Yes,
            composite_score: Probability::new(dec!(0.8)),
            confidence: Probability::new(confidence),
            expected_return_bps: expected_bps,
            downside_bps,
            entry_price_ref: Price::new(dec!(0.5)),
            suggested_horizon_secs: 3_600,
            factor_breakdown: Vec::new(),
            model_explanation: ModelExplanation {
                headline: "t".to_owned(),
                top_positive: Vec::new(),
                top_negative: Vec::new(),
            },
            rejection_warnings: Vec::new(),
            rank_before_portfolio: 0,
            as_of: Utc::now(),
        }
    }

    fn kelly() -> KellySizingModel {
        // Half Kelly, 10% position cap, R = 2, linear confidence shrink, fixed dd.
        KellySizingModel::new(
            dec!(0.5),
            dec!(0.1),
            dec!(2),
            ConfidenceSizeCurve::Linear,
            DrawdownMultiplierPolicy::Fixed,
        )
    }

    fn sized(outcome: SizingOutcome) -> super::SizingSuggestion {
        match outcome {
            SizingOutcome::Sized(s) => s,
            SizingOutcome::Rejected(r) => panic!("expected sized, got {r:?}"),
        }
    }

    #[test]
    fn kelly_positive_edge_sizes_fraction_of_equity() {
        // E[r]=0.02, l=0.01, R=2 → g=0.02; q=(0.02+0.01)/(0.02+0.01)=1.0.
        // edge = 1·0.02 − 0 = 0.02; f* = 0.02/(0.02·0.01)=100 (huge → capped).
        let model = kelly();
        let s = sized(
            model
                .suggest(&SizingInput {
                    candidate: &candidate(dec!(200), dec!(100), dec!(1)),
                    equity_usd: Usd::new(dec!(10000)),
                    drawdown_state: DrawdownState::neutral(),
                })
                .expect("suggest"),
        );
        // Capped at 10% of equity = 1000.
        assert_eq!(s.desired_usd, Usd::new(dec!(1000)));
        assert!(s.binding_kelly_cap, "max_position_pct must bind");
        assert_eq!(s.edge_bps.expect("edge").inner(), dec!(200));
    }

    #[test]
    fn kelly_q_derived_from_expected_return_and_downside() {
        // E[r]=0.005, l=0.01, R=3 → g=0.03; q=(0.005+0.01)/(0.03+0.01)=0.375.
        // edge = 0.375·0.03 − 0.625·0.01 = 0.01125 − 0.00625 = 0.005 (= E[r]).
        // f* = 0.005/(0.03·0.01) = 16.6667 → ·0.5 = capped to 0.1.
        let model = KellySizingModel::new(
            dec!(0.5),
            dec!(0.1),
            dec!(3),
            ConfidenceSizeCurve::Linear,
            DrawdownMultiplierPolicy::Fixed,
        );
        let s = sized(
            model
                .suggest(&SizingInput {
                    candidate: &candidate(dec!(50), dec!(100), dec!(1)),
                    equity_usd: Usd::new(dec!(10000)),
                    drawdown_state: DrawdownState::neutral(),
                })
                .expect("suggest"),
        );
        // edge_bps == expected_return_bps (self-consistency of the derivation).
        assert_eq!(s.edge_bps.expect("edge").inner(), dec!(50));
    }

    #[test]
    fn kelly_non_positive_edge_rejects() {
        // E[r] = −0.01 → edge negative → reject, never funded.
        let model = kelly();
        let outcome = model
            .suggest(&SizingInput {
                candidate: &candidate(dec!(-100), dec!(100), dec!(1)),
                equity_usd: Usd::new(dec!(10000)),
                drawdown_state: DrawdownState::neutral(),
            })
            .expect("suggest");
        assert_eq!(
            outcome,
            SizingOutcome::Rejected(RejectionReason::NoPositiveSignal)
        );
    }

    #[test]
    fn kelly_invalid_downside_rejects() {
        let model = kelly();
        let outcome = model
            .suggest(&SizingInput {
                candidate: &candidate(dec!(200), dec!(0), dec!(1)),
                equity_usd: Usd::new(dec!(10000)),
                drawdown_state: DrawdownState::neutral(),
            })
            .expect("suggest");
        assert_eq!(
            outcome,
            SizingOutcome::Rejected(RejectionReason::InvalidEdgeInputs)
        );
    }

    #[test]
    fn kelly_confidence_shrinks_fraction() {
        // Smaller, uncapped bet so the confidence shrink is observable.
        // E[r]=0.001, l=0.02, R=2 → g=0.04; q=(0.001+0.02)/0.06=0.35.
        // edge = 0.35·0.04 − 0.65·0.02 = 0.014 − 0.013 = 0.001; f*=0.001/(0.04·0.02)=1.25.
        let model = KellySizingModel::new(
            dec!(0.5),
            dec!(0.9),
            dec!(2),
            ConfidenceSizeCurve::Linear,
            DrawdownMultiplierPolicy::Fixed,
        );
        let high = sized(
            model
                .suggest(&SizingInput {
                    candidate: &candidate(dec!(10), dec!(200), dec!(1)),
                    equity_usd: Usd::new(dec!(10000)),
                    drawdown_state: DrawdownState::neutral(),
                })
                .expect("suggest"),
        );
        let low = sized(
            model
                .suggest(&SizingInput {
                    candidate: &candidate(dec!(10), dec!(200), dec!(0.4)),
                    equity_usd: Usd::new(dec!(10000)),
                    drawdown_state: DrawdownState::neutral(),
                })
                .expect("suggest"),
        );
        assert!(
            low.desired_usd.inner() < high.desired_usd.inner(),
            "lower confidence must shrink the bet: {low:?} vs {high:?}"
        );
        // Linear: confidence 0.4 → exactly 0.4× the full-confidence bet.
        assert_eq!(low.desired_usd, high.desired_usd * dec!(0.4));
    }

    #[test]
    fn kelly_step_confidence_weighting_buckets() {
        // Step curve: confidence 0.4 → 0.25 shrink; uncapped small bet shows it.
        let model = KellySizingModel::new(
            dec!(1),
            dec!(0.9),
            dec!(2),
            ConfidenceSizeCurve::Step,
            DrawdownMultiplierPolicy::Fixed,
        );
        let low = sized(
            model
                .suggest(&SizingInput {
                    candidate: &candidate(dec!(10), dec!(200), dec!(0.4)),
                    equity_usd: Usd::new(dec!(10000)),
                    drawdown_state: DrawdownState::neutral(),
                })
                .expect("suggest"),
        );
        let mid = sized(
            model
                .suggest(&SizingInput {
                    candidate: &candidate(dec!(10), dec!(200), dec!(0.6)),
                    equity_usd: Usd::new(dec!(10000)),
                    drawdown_state: DrawdownState::neutral(),
                })
                .expect("suggest"),
        );
        // 0.6 → 0.5 bucket is double the 0.4 → 0.25 bucket.
        assert_eq!(mid.desired_usd, low.desired_usd * dec!(2));
        assert_eq!(model.kind(), SizingModelKind::Kelly);
    }
}
