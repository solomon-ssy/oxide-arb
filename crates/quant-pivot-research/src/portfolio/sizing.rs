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
    enums::quant::{BindingConstraint, RejectionReason, SizingModelKind},
    runtime_config::{
        ConfidenceSizeCurve, DrawdownMultiplierPolicy, KellySafetyConfig, SizingModelConfig,
    },
    types::{Bps, Usd},
};
use rust_decimal::Decimal;

use crate::{model::signal::SignalCandidate, precision::RESEARCH_DECIMAL_SCALE};

/// Basis-point denominator (`1 bps = 1/10_000`).
const BPS_PER_UNIT: i64 = 10_000;

/// Decision-time drawdown state driving the conservative scaling policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DrawdownState {
    /// Current peak-to-trough drawdown as a fraction in `[0, 1]` (`0` = none).
    pub current_drawdown: Decimal,
}

impl DrawdownState {
    /// The neutral no-drawdown state, valid only when no equity history exists.
    #[must_use]
    pub const fn neutral() -> Self {
        Self {
            current_drawdown: Decimal::ZERO,
        }
    }

    /// Conservative merge for repeated ledger reads within one report build.
    #[must_use]
    pub fn conservative_max(self, other: Self) -> Self {
        Self {
            current_drawdown: self.current_drawdown.max(other.current_drawdown),
        }
    }
}

impl Default for DrawdownState {
    fn default() -> Self {
        Self::neutral()
    }
}

/// Correlation-aware shrink inputs for one candidate (Phase 11.3 §6.2).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CorrelationShrinkInput {
    /// Number of same-cluster candidates in the current batch (including self).
    pub cluster_size: u32,
    /// Mean pairwise correlation ρ̄ within the cluster.
    pub mean_rho: Decimal,
}

/// Inputs to one sizing decision (borrowed candidate + capital base).
pub struct SizingInput<'a> {
    /// The scored candidate being sized.
    pub candidate: &'a SignalCandidate,
    /// Strategy capital base for sizing.
    pub capital_base_usd: Usd,
    /// Decision-time drawdown state (drives `drawdown_scaling`).
    pub drawdown_state: DrawdownState,
    /// Wilson CI half-width for the candidate's score bin (edge uncertainty).
    pub edge_uncertainty_half_width: Option<Decimal>,
    /// Correlation-cluster shrink inputs, when correlation cap is enabled.
    pub correlation: Option<CorrelationShrinkInput>,
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
    /// Edge-uncertainty shrink multiplier applied (audit only; Phase 11.3 §6.1).
    pub edge_uncertainty_shrink_applied: Option<Decimal>,
    /// Correlation-cluster shrink multiplier applied (Phase 11.3 §6.2).
    pub correlation_shrink_applied: Option<Decimal>,
    /// Kelly-stage soft binding (confidence / drawdown / correlation shrink).
    pub kelly_stage_binding: Option<BindingConstraint>,
}

/// A position-sizing model (pure: no I/O, no clock, no mutable state).
pub trait SizingModel: Send + Sync {
    /// The model family, recorded on every [`crate::portfolio::SizingPlan`].
    fn kind(&self) -> SizingModelKind;

    /// Suggest a desired USD size for one candidate, or reject it.
    fn suggest(&self, input: &SizingInput<'_>) -> QuantResult<SizingOutcome>;
}

/// Parsed Kelly safety-layer parameters (Phase 11.3 §6).
#[derive(Debug, Clone, Copy)]
pub struct KellySafetyParams {
    edge_uncertainty_k: Decimal,
    edge_uncertainty_floor: Decimal,
    binding_materiality_threshold: Decimal,
}

impl KellySafetyParams {
    #[must_use]
    pub const fn new(
        edge_uncertainty_k: Decimal,
        edge_uncertainty_floor: Decimal,
        binding_materiality_threshold: Decimal,
    ) -> Self {
        Self {
            edge_uncertainty_k,
            edge_uncertainty_floor,
            binding_materiality_threshold,
        }
    }
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
    /// Kelly safety-layer knobs.
    kelly_safety: KellySafetyParams,
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
        kelly_safety: KellySafetyParams,
    ) -> Self {
        Self {
            kelly_fraction,
            max_position_pct,
            target_reward_multiple,
            confidence_weighting,
            drawdown_scaling,
            kelly_safety,
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
        let edge_uncertainty_shrink = edge_uncertainty_shrink(
            input.edge_uncertainty_half_width,
            self.kelly_safety.edge_uncertainty_k,
            self.kelly_safety.edge_uncertainty_floor,
        );
        let correlation_shrink = correlation_shrink(input.correlation);
        let multiplier = (self.kelly_fraction
            * shrink
            * drawdown
            * edge_uncertainty_shrink
            * correlation_shrink)
            .max(Decimal::ZERO);

        let raw_fraction = multiplier * f_star;
        let binding_kelly_cap = raw_fraction > self.max_position_pct;
        let fraction = raw_fraction.min(self.max_position_pct).max(Decimal::ZERO);

        let desired_usd = (input.capital_base_usd * fraction).round_dp(RESEARCH_DECIMAL_SCALE);
        let edge_bps = Bps::new((edge * bps).round_dp(RESEARCH_DECIMAL_SCALE));
        let kelly_stage_binding = resolve_kelly_stage_binding(
            shrink,
            drawdown,
            correlation_shrink,
            self.kelly_safety.binding_materiality_threshold,
        );

        Ok(SizingOutcome::Sized(SizingSuggestion {
            desired_usd,
            edge_bps: Some(edge_bps),
            kelly_fraction_applied: Some(multiplier.round_dp(RESEARCH_DECIMAL_SCALE)),
            binding_kelly_cap,
            edge_uncertainty_shrink_applied: Some(
                edge_uncertainty_shrink.round_dp(RESEARCH_DECIMAL_SCALE),
            ),
            correlation_shrink_applied: Some(correlation_shrink.round_dp(RESEARCH_DECIMAL_SCALE)),
            kelly_stage_binding,
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
pub fn sizing_model_from_config(
    sizing: &SizingModelConfig,
    kelly_safety: &KellySafetyConfig,
) -> QuantResult<Box<dyn SizingModel>> {
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
        KellySafetyParams::new(
            parse_decimal(
                "portfolio.kelly_safety.edge_uncertainty_k",
                &kelly_safety.edge_uncertainty_k.value,
            )?,
            parse_decimal(
                "portfolio.kelly_safety.edge_uncertainty_floor",
                &kelly_safety.edge_uncertainty_floor.value,
            )?,
            parse_decimal(
                "portfolio.kelly_safety.binding_materiality_threshold",
                &kelly_safety.binding_materiality_threshold.value,
            )?,
        ),
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
/// with the current drawdown fraction from the equity ledger.
fn drawdown_scale(state: DrawdownState, policy: DrawdownMultiplierPolicy) -> Decimal {
    match policy {
        DrawdownMultiplierPolicy::Fixed => Decimal::ONE,
        DrawdownMultiplierPolicy::Conservative => {
            (Decimal::ONE - state.current_drawdown).clamp(Decimal::ZERO, Decimal::ONE)
        }
    }
}

/// Edge-uncertainty shrink from the reliability-bin Wilson CI half-width.
fn edge_uncertainty_shrink(half_width: Option<Decimal>, k: Decimal, floor: Decimal) -> Decimal {
    let Some(edge_std) = half_width else {
        return Decimal::ONE;
    };
    (Decimal::ONE - k * edge_std).clamp(floor, Decimal::ONE)
}

/// Correlation-aware multi-bet shrink: `f_i /= 1 + (n−1)·ρ̄`.
fn correlation_shrink(input: Option<CorrelationShrinkInput>) -> Decimal {
    let Some(input) = input else {
        return Decimal::ONE;
    };
    if input.cluster_size <= 1 || input.mean_rho <= Decimal::ZERO {
        return Decimal::ONE;
    }
    let n_minus_one = Decimal::from(input.cluster_size.saturating_sub(1));
    Decimal::ONE / (Decimal::ONE + n_minus_one * input.mean_rho)
}

/// Pick the Kelly-stage soft binding when a shrink multiplier is materially low.
fn resolve_kelly_stage_binding(
    confidence_shrink: Decimal,
    drawdown_shrink: Decimal,
    correlation_shrink: Decimal,
    materiality_threshold: Decimal,
) -> Option<BindingConstraint> {
    let candidates = [
        (confidence_shrink, BindingConstraint::ConfidenceCap),
        (drawdown_shrink, BindingConstraint::DrawdownCap),
        (correlation_shrink, BindingConstraint::CorrelationCap),
    ];
    let (min_shrink, binding) = candidates
        .into_iter()
        .min_by(|(left, _), (right, _)| left.cmp(right))?;
    if min_shrink < materiality_threshold {
        Some(binding)
    } else {
        None
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
        ConfidenceSizeCurve, CorrelationShrinkInput, DrawdownMultiplierPolicy, DrawdownState,
        KellySafetyParams, KellySizingModel, SizingInput, SizingModel, SizingOutcome,
    };
    use chrono::Utc;
    use quant_pivot_models::{
        enums::quant::{BindingConstraint, OutcomeSide, RejectionReason, SizingModelKind},
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
            liquidity_score: Probability::ZERO,
            data_quality_score: Probability::ZERO,
            model_score_percentile: Probability::ZERO,
            as_of: Utc::now(),
        }
    }

    fn kelly_safety() -> KellySafetyParams {
        KellySafetyParams::new(dec!(1), dec!(0.5), dec!(0.9))
    }

    fn sizing_input(candidate: &SignalCandidate, capital: Usd) -> SizingInput<'_> {
        SizingInput {
            candidate,
            capital_base_usd: capital,
            drawdown_state: DrawdownState::neutral(),
            edge_uncertainty_half_width: None,
            correlation: None,
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
            kelly_safety(),
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
                .suggest(&sizing_input(
                    &candidate(dec!(200), dec!(100), dec!(1)),
                    Usd::new(dec!(10000)),
                ))
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
            kelly_safety(),
        );
        let s = sized(
            model
                .suggest(&SizingInput {
                    candidate: &candidate(dec!(50), dec!(100), dec!(1)),
                    capital_base_usd: Usd::new(dec!(10000)),
                    drawdown_state: DrawdownState::neutral(),
                    edge_uncertainty_half_width: None,
                    correlation: None,
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
                capital_base_usd: Usd::new(dec!(10000)),
                drawdown_state: DrawdownState::neutral(),
                edge_uncertainty_half_width: None,
                correlation: None,
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
                capital_base_usd: Usd::new(dec!(10000)),
                drawdown_state: DrawdownState::neutral(),
                edge_uncertainty_half_width: None,
                correlation: None,
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
            kelly_safety(),
        );
        let high = sized(
            model
                .suggest(&SizingInput {
                    candidate: &candidate(dec!(10), dec!(200), dec!(1)),
                    capital_base_usd: Usd::new(dec!(10000)),
                    drawdown_state: DrawdownState::neutral(),
                    edge_uncertainty_half_width: None,
                    correlation: None,
                })
                .expect("suggest"),
        );
        let low = sized(
            model
                .suggest(&SizingInput {
                    candidate: &candidate(dec!(10), dec!(200), dec!(0.4)),
                    capital_base_usd: Usd::new(dec!(10000)),
                    drawdown_state: DrawdownState::neutral(),
                    edge_uncertainty_half_width: None,
                    correlation: None,
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
            kelly_safety(),
        );
        let low = sized(
            model
                .suggest(&SizingInput {
                    candidate: &candidate(dec!(10), dec!(200), dec!(0.4)),
                    capital_base_usd: Usd::new(dec!(10000)),
                    drawdown_state: DrawdownState::neutral(),
                    edge_uncertainty_half_width: None,
                    correlation: None,
                })
                .expect("suggest"),
        );
        let mid = sized(
            model
                .suggest(&SizingInput {
                    candidate: &candidate(dec!(10), dec!(200), dec!(0.6)),
                    capital_base_usd: Usd::new(dec!(10000)),
                    drawdown_state: DrawdownState::neutral(),
                    edge_uncertainty_half_width: None,
                    correlation: None,
                })
                .expect("suggest"),
        );
        // 0.6 → 0.5 bucket is double the 0.4 → 0.25 bucket.
        assert_eq!(mid.desired_usd, low.desired_usd * dec!(2));
        assert_eq!(model.kind(), SizingModelKind::Kelly);
    }

    #[test]
    fn conservative_drawdown_scales_kelly_fraction() {
        let fixed = KellySizingModel::new(
            dec!(0.5),
            dec!(0.9),
            dec!(2),
            ConfidenceSizeCurve::Linear,
            DrawdownMultiplierPolicy::Fixed,
            kelly_safety(),
        );
        let conservative = KellySizingModel::new(
            dec!(0.5),
            dec!(0.9),
            dec!(2),
            ConfidenceSizeCurve::Linear,
            DrawdownMultiplierPolicy::Conservative,
            kelly_safety(),
        );
        let input_candidate = candidate(dec!(10), dec!(200), dec!(1));
        let fixed_size = sized(
            fixed
                .suggest(&SizingInput {
                    candidate: &input_candidate,
                    capital_base_usd: Usd::new(dec!(10000)),
                    drawdown_state: DrawdownState {
                        current_drawdown: dec!(0.2),
                    },
                    edge_uncertainty_half_width: None,
                    correlation: None,
                })
                .expect("fixed suggest"),
        );
        let conservative_size = sized(
            conservative
                .suggest(&SizingInput {
                    candidate: &input_candidate,
                    capital_base_usd: Usd::new(dec!(10000)),
                    drawdown_state: DrawdownState {
                        current_drawdown: dec!(0.2),
                    },
                    edge_uncertainty_half_width: None,
                    correlation: None,
                })
                .expect("conservative suggest"),
        );

        assert_eq!(
            conservative_size.desired_usd,
            fixed_size.desired_usd * dec!(0.8)
        );
    }

    #[test]
    fn kelly_fixed_policy_ignores_drawdown() {
        let fixed = KellySizingModel::new(
            dec!(0.5),
            dec!(0.9),
            dec!(2),
            ConfidenceSizeCurve::Linear,
            DrawdownMultiplierPolicy::Fixed,
            kelly_safety(),
        );
        let input_candidate = candidate(dec!(10), dec!(200), dec!(1));
        let neutral = sized(
            fixed
                .suggest(&SizingInput {
                    candidate: &input_candidate,
                    capital_base_usd: Usd::new(dec!(10000)),
                    drawdown_state: DrawdownState::neutral(),
                    edge_uncertainty_half_width: None,
                    correlation: None,
                })
                .expect("neutral suggest"),
        );
        let in_drawdown = sized(
            fixed
                .suggest(&SizingInput {
                    candidate: &input_candidate,
                    capital_base_usd: Usd::new(dec!(10000)),
                    drawdown_state: DrawdownState {
                        current_drawdown: dec!(0.2),
                    },
                    edge_uncertainty_half_width: None,
                    correlation: None,
                })
                .expect("drawdown suggest"),
        );
        assert_eq!(in_drawdown.desired_usd, neutral.desired_usd);
    }

    #[test]
    fn kelly_edge_uncertainty_shrink_reduces_size_monotonically() {
        let model = kelly();
        let candidate = candidate(dec!(10), dec!(200), dec!(1));
        let capital = Usd::new(dec!(10000));
        let base = sized(
            model
                .suggest(&SizingInput {
                    candidate: &candidate,
                    capital_base_usd: capital,
                    drawdown_state: DrawdownState::neutral(),
                    edge_uncertainty_half_width: None,
                    correlation: None,
                })
                .expect("base"),
        );
        let shrunk = sized(
            model
                .suggest(&SizingInput {
                    candidate: &candidate,
                    capital_base_usd: capital,
                    drawdown_state: DrawdownState::neutral(),
                    edge_uncertainty_half_width: Some(dec!(0.15)),
                    correlation: None,
                })
                .expect("shrunk"),
        );
        assert!(
            shrunk.desired_usd <= base.desired_usd,
            "edge uncertainty must never increase size: base={:?} shrunk={:?}",
            base.desired_usd,
            shrunk.desired_usd
        );
        assert!(
            shrunk.edge_uncertainty_shrink_applied.expect("audit") < Decimal::ONE,
            "non-zero half-width must apply shrink"
        );
    }

    #[test]
    fn correlation_shrink_reduces_size_for_correlated_cluster() {
        let model = kelly();
        let candidate = candidate(dec!(10), dec!(200), dec!(1));
        let capital = Usd::new(dec!(10000));
        let solo = sized(
            model
                .suggest(&SizingInput {
                    candidate: &candidate,
                    capital_base_usd: capital,
                    drawdown_state: DrawdownState::neutral(),
                    edge_uncertainty_half_width: None,
                    correlation: None,
                })
                .expect("solo"),
        );
        let clustered = sized(
            model
                .suggest(&SizingInput {
                    candidate: &candidate,
                    capital_base_usd: capital,
                    drawdown_state: DrawdownState::neutral(),
                    edge_uncertainty_half_width: None,
                    correlation: Some(CorrelationShrinkInput {
                        cluster_size: 3,
                        mean_rho: dec!(0.5),
                    }),
                })
                .expect("clustered"),
        );
        assert!(
            clustered.desired_usd <= solo.desired_usd,
            "correlation shrink must not increase size"
        );
        assert!(clustered.correlation_shrink_applied.expect("audit") < Decimal::ONE);
    }

    #[test]
    fn drawdown_cap_binding_emitted_when_active() {
        let model = KellySizingModel::new(
            dec!(0.5),
            dec!(0.9),
            dec!(2),
            ConfidenceSizeCurve::Linear,
            DrawdownMultiplierPolicy::Conservative,
            KellySafetyParams::new(dec!(1), dec!(0.5), dec!(0.95)),
        );
        let candidate = candidate(dec!(10), dec!(200), dec!(1));
        let suggestion = sized(
            model
                .suggest(&SizingInput {
                    candidate: &candidate,
                    capital_base_usd: Usd::new(dec!(10000)),
                    drawdown_state: DrawdownState {
                        current_drawdown: dec!(0.25),
                    },
                    edge_uncertainty_half_width: None,
                    correlation: None,
                })
                .expect("suggest"),
        );
        assert_eq!(
            suggestion.kelly_stage_binding,
            Some(BindingConstraint::DrawdownCap)
        );
    }

    #[test]
    fn confidence_cap_binding_emitted_when_below_floor() {
        let model = KellySizingModel::new(
            dec!(0.5),
            dec!(0.9),
            dec!(2),
            ConfidenceSizeCurve::Linear,
            DrawdownMultiplierPolicy::Fixed,
            KellySafetyParams::new(dec!(1), dec!(0.5), dec!(0.95)),
        );
        let suggestion = sized(
            model
                .suggest(&SizingInput {
                    candidate: &candidate(dec!(10), dec!(200), dec!(0.2)),
                    capital_base_usd: Usd::new(dec!(10000)),
                    drawdown_state: DrawdownState::neutral(),
                    edge_uncertainty_half_width: None,
                    correlation: None,
                })
                .expect("suggest"),
        );
        assert_eq!(
            suggestion.kelly_stage_binding,
            Some(BindingConstraint::ConfidenceCap)
        );
    }
}
