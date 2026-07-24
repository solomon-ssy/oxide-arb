//! Pluggable position-sizing models (the "how much per candidate" primitive).
//!
//! A [`SizingModel`] turns one scored [`SignalCandidate`] plus the capital base
//! into a *desired* USD size — the amount to deploy **before** portfolio caps,
//! liquidity, and available-cash convergence (those belong to the allocator /
//! planner). Sizing is a pure function: no I/O, no clock, no mutable state, so a
//! plan is deterministically replayable.
//!
//! # Kelly from the calibrated win probability (production default)
//!
//! Polymarket outcome tokens are binary: hold a `Calibrated` position to
//! resolution and it either pays `(1-p)/p` per unit stake (win) or loses the
//! whole stake (lose), where `p` is the executable market price. This is
//! *exactly* the payoff [`ReturnEstimate::win_probability`](crate::model::artifact::ReturnEstimate)'s
//! `E[r] = q·(1-p)/p − (1-q)` already assumes, so Kelly uses
//! the *same* `q` directly — never re-derives a different "implied" win
//! probability from a second, inconsistent bet structure:
//!
//! ```text
//! q = win_probability // calibrated P(win), same q as E[r]
//! p = (walk gross cash + PIT fee) / filled shares // all-in executable price
//! f* = (q − p) / (1 − p) // binary Kelly, b = (1-p)/p odds
//! ```
//!
//! This is the binary Kelly identity `f* = (q·b − (1-q)) / b` after
//! substituting the executable net odds `b = (1-p)/p`.
//! `f* ≤ 0` (i.e. `q ≤ p`, no edge over the market) ⇒ **rejected**, never funded.
//!
//! `win_probability = None` is an uncalibrated model and is rejected before
//! sizing. It never receives a heuristic TP/SL-derived probability or an
//! actionable amount, including in `ReportOnly`.
//!
//! `confidence` is the model's **evidence quality**, *not* a calibrated
//! probability, so it never stands in for `q` in either branch. Instead it
//! shrinks the bet — the production-standard mitigation for Kelly's sensitivity
//! to edge mis-estimation (fractional Kelly + uncertainty shrinkage):
//!
//! ```text
//! multiplier = kelly_fraction · confidence_shrink(confidence) · drawdown_scale
//! · edge_uncertainty_shrink · correlation_shrink
//! raw_fraction = multiplier · f*
//! position_fraction = clamp(raw_fraction, 0, max_position_pct)
//! desired tier = largest executable cash tier ≤ round(position_fraction · equity)
//! ```
//!
//! Every intermediate stays in [`Decimal`]; `f64` never appears, so no precision
//! drift can leak into a money value.

use quant_pivot_error::QuantResult;
use quant_pivot_models::{
    enums::quant::{BindingConstraint, RejectionReason, SizingModelKind},
    runtime_config::{
        ConfidenceSizeCurve, DrawdownMultiplierPolicy, KellySafetyConfig, SizingModelConfig,
    },
    types::{Bps, Probability, Shares, Usd},
};
use rust_decimal::Decimal;

use crate::{
    execution_semantics::{BookWalkFill, ResolutionBuyEconomics},
    model::signal::SignalCandidate,
    precision::RESEARCH_DECIMAL_SCALE,
};

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

/// Correlation-aware shrink inputs for one candidate.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CorrelationShrinkInput {
    /// Number of same-cluster candidates in the current batch (including self).
    pub cluster_size: u32,
    /// Mean pairwise correlation ρ̄ within the cluster.
    pub mean_rho: Decimal,
}

/// One policy-validated cash tier walked against decision-time L2 and PIT fees.
///
/// `cash_budget` is the governed reservation tier. `fill.total_cash_delta` is
/// the expected account debit and may be smaller only for a partial-fill entry
/// template; Kelly uses the fill economics while the allocator reserves the
/// whole governed tier.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecutableSizingTier {
    pub cash_budget: Usd,
    pub fill: BookWalkFill,
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
    /// Policy-validated executable tiers for this exact candidate/capture.
    pub executable_tiers: Vec<ExecutableSizingTier>,
}

/// The outcome of sizing one candidate.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SizingOutcome {
    /// The candidate carries a fundable size (before portfolio caps).
    Sized(Box<SizingSuggestion>),
    /// The candidate must not be funded for the given reason.
    Rejected(RejectionReason),
}

/// A fundable size suggestion plus its audit provenance.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SizingSuggestion {
    /// Desired USD size before portfolio caps / liquidity / available cash.
    pub desired_usd: Usd,
    /// Shares produced by the same walk that selected `desired_usd`.
    pub desired_shares: Shares,
    /// Per-unit edge in basis points (`None` for the edge-free curve model).
    pub edge_bps: Option<Bps>,
    /// Fractional-Kelly multiplier applied (`kelly_fraction · shrink · drawdown`);
    /// `None` for the edge-free curve model.
    pub kelly_fraction_applied: Option<Decimal>,
    /// Whether the per-position equity cap (`max_position_pct`) bound the size.
    pub binding_kelly_cap: bool,
    /// Edge-uncertainty shrink multiplier applied (audit only).
    pub edge_uncertainty_shrink_applied: Option<Decimal>,
    /// Correlation-cluster shrink multiplier applied.
    pub correlation_shrink_applied: Option<Decimal>,
    /// Kelly-stage soft binding (confidence / drawdown / correlation shrink).
    pub kelly_stage_binding: Option<BindingConstraint>,
    /// The full per-stage derivation of `kelly_fraction_applied` (the sizing
    /// waterfall), for the report UI to render each multiplier
    /// step rather than only the already-merged product. `None` for the
    /// edge-free curve model (mirrors `kelly_fraction_applied`).
    pub provenance: Option<SizingProvenance>,
    /// Every Kelly-eligible discrete tier, retained so the planner can safely
    /// snap a constrained allocation down without inventing an unwalked size.
    pub executable_tiers: Vec<ExecutableTierSuggestion>,
}

/// Fee/depth-adjusted Kelly result for one discrete policy tier.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ExecutableTierSuggestion {
    pub cash_budget: Usd,
    pub filled_shares: Shares,
    pub edge_bps: Bps,
    pub f_star: Decimal,
    pub raw_fraction: Decimal,
    pub binding_kelly_cap: bool,
}

/// Every intermediate factor between the raw full-Kelly fraction `f*` and the
/// final, capped position-sizing fraction — the audit trail a "sizing
/// waterfall" visualization renders step by step.
///
/// `f_star × kelly_fraction_config × confidence_shrink × drawdown_shrink ×
/// edge_uncertainty_shrink × correlation_shrink == raw_fraction`, then
/// `raw_fraction` is capped at `position_cap_fraction`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SizingProvenance {
    /// The raw, uncapped full-Kelly fraction, before any fractional-Kelly or
    /// safety-layer shrink. Derived as `(q - p) / (1 - p)` from the calibrated
    /// resolution probability and executable market price.
    pub f_star: Decimal,
    /// The governed static fractional-Kelly constant (e.g. `0.5` for half-Kelly).
    pub kelly_fraction_config: Decimal,
    /// Confidence-curve shrink multiplier.
    pub confidence_shrink: Decimal,
    /// Drawdown-scaling multiplier.
    pub drawdown_shrink: Decimal,
    /// Edge-uncertainty shrink multiplier (duplicated from
    /// `edge_uncertainty_shrink_applied` for a self-contained provenance record).
    pub edge_uncertainty_shrink: Decimal,
    /// Correlation-cluster shrink multiplier (duplicated from
    /// `correlation_shrink_applied` for a self-contained provenance record).
    pub correlation_shrink: Decimal,
    /// `f_star × (product of every multiplier above)` — the fraction before
    /// the per-position equity cap.
    pub raw_fraction: Decimal,
    /// The per-position equity cap (`max_position_pct`) `raw_fraction` was
    /// clamped against.
    pub position_cap_fraction: Decimal,
}

/// A position-sizing model (pure: no I/O, no clock, no mutable state).
pub trait SizingModel: Send + Sync {
    /// The model family, recorded on every sizing plan.
    fn kind(&self) -> SizingModelKind;

    /// Suggest a desired USD size for one candidate, or reject it.
    fn suggest(&self, input: &SizingInput<'_>) -> QuantResult<SizingOutcome>;
}

/// Parsed Kelly safety-layer parameters.
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
        confidence_weighting: ConfidenceSizeCurve,
        drawdown_scaling: DrawdownMultiplierPolicy,
        kelly_safety: KellySafetyParams,
    ) -> Self {
        Self {
            kelly_fraction,
            max_position_pct,
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
        let candidate = input.candidate;

        let Some(win_probability) = candidate.win_probability else {
            return Ok(SizingOutcome::Rejected(
                RejectionReason::ReturnModelUncalibrated,
            ));
        };
        if win_probability <= Probability::ZERO || win_probability > Probability::ONE {
            return Ok(SizingOutcome::Rejected(RejectionReason::InvalidEdgeInputs));
        }
        if input.executable_tiers.is_empty() {
            return Ok(SizingOutcome::Rejected(
                RejectionReason::ExecutableEntryUnavailable,
            ));
        }

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

        let mut saw_valid_fill = false;
        let mut saw_positive_edge = false;
        let mut executable_tiers = Vec::new();
        for tier in &input.executable_tiers {
            let Ok(economics) = ResolutionBuyEconomics::from_fill(&tier.fill) else {
                continue;
            };
            saw_valid_fill = true;
            let edge = economics.edge(win_probability);
            if edge.expected_pnl <= Usd::ZERO {
                continue;
            }
            saw_positive_edge = true;
            let raw_fraction = multiplier * edge.full_kelly_fraction;
            let binding_kelly_cap = raw_fraction > self.max_position_pct;
            let fraction = raw_fraction.min(self.max_position_pct).max(Decimal::ZERO);
            let max_budget = (input.capital_base_usd * fraction).round_dp(RESEARCH_DECIMAL_SCALE);
            if tier.cash_budget > max_budget {
                continue;
            }
            executable_tiers.push(ExecutableTierSuggestion {
                cash_budget: tier.cash_budget,
                filled_shares: edge.economics.filled_shares,
                edge_bps: edge.expected_return_bps.round_dp(RESEARCH_DECIMAL_SCALE),
                f_star: edge.full_kelly_fraction,
                raw_fraction,
                binding_kelly_cap,
            });
        }
        if !saw_valid_fill {
            return Ok(SizingOutcome::Rejected(RejectionReason::InvalidEdgeInputs));
        }
        if !saw_positive_edge {
            return Ok(SizingOutcome::Rejected(RejectionReason::NoPositiveSignal));
        }
        executable_tiers.sort_by_key(|tier| tier.cash_budget);
        executable_tiers.dedup_by_key(|tier| tier.cash_budget);
        let Some(selected) = executable_tiers.last().copied() else {
            return Ok(SizingOutcome::Rejected(RejectionReason::BelowMinSize));
        };
        let kelly_stage_binding = resolve_kelly_stage_binding(
            shrink,
            drawdown,
            correlation_shrink,
            self.kelly_safety.binding_materiality_threshold,
        );

        let round = |value: Decimal| value.round_dp(RESEARCH_DECIMAL_SCALE);
        Ok(SizingOutcome::Sized(Box::new(SizingSuggestion {
            desired_usd: selected.cash_budget,
            desired_shares: selected.filled_shares,
            edge_bps: Some(selected.edge_bps),
            kelly_fraction_applied: Some(round(multiplier)),
            binding_kelly_cap: selected.binding_kelly_cap,
            edge_uncertainty_shrink_applied: Some(round(edge_uncertainty_shrink)),
            correlation_shrink_applied: Some(round(correlation_shrink)),
            kelly_stage_binding,
            provenance: Some(SizingProvenance {
                f_star: round(selected.f_star),
                kelly_fraction_config: round(self.kelly_fraction),
                confidence_shrink: round(shrink),
                drawdown_shrink: round(drawdown),
                edge_uncertainty_shrink: round(edge_uncertainty_shrink),
                correlation_shrink: round(correlation_shrink),
                raw_fraction: round(selected.raw_fraction),
                position_cap_fraction: round(self.max_position_pct),
            }),
            executable_tiers,
        })))
    }
}

/// Build the active sizing model from the governed config.
///
/// Kelly is the only sizing model; the returned trait object keeps the planner
/// decoupled from the concrete type and leaves room for future models.
///
/// # Errors
///
/// Returns a configuration error when any decimal parameter is malformed
/// (runtime-config validation rejects these upstream, so this is a hard guard).
pub fn sizing_model_from_config(
    sizing: &SizingModelConfig,
    kelly_safety: &KellySafetyConfig,
) -> QuantResult<Box<dyn SizingModel>> {
    Ok(Box::new(KellySizingModel::new(
        sizing.kelly_fraction.value,
        sizing.max_position_pct.value,
        sizing.confidence_weighting,
        sizing.drawdown_scaling,
        KellySafetyParams::new(
            kelly_safety.edge_uncertainty_k.value,
            kelly_safety.edge_uncertainty_floor.value,
            kelly_safety.binding_materiality_threshold.value,
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

#[cfg(test)]
mod tests {
    use std::slice;

    use chrono::Utc;
    use quant_pivot_models::{
        domain::market::{book::BookLevel, fee::BuilderFeeAttribution},
        enums::quant::{
            BindingConstraint, FillRequirement, OutcomeSide, RejectionReason, SizingModelKind,
        },
        hashing::CanonicalDigest,
        types::{
            Bps, MarketId, ModelRunId, Price, Probability, Shares, SignalCandidateId, TokenId, Usd,
        },
    };
    use rust_decimal::{Decimal, prelude::ToPrimitive};
    use rust_decimal_macros::dec;

    use super::{
        ConfidenceSizeCurve, CorrelationShrinkInput, DrawdownMultiplierPolicy, DrawdownState,
        ExecutableSizingTier, KellySafetyParams, KellySizingModel, SizingInput, SizingModel,
        SizingOutcome,
    };
    use crate::{
        execution_semantics::{LiquidityRole, PitFeeSchedule, walk_buy_cash_budget},
        model::signal::{ModelExplanation, SignalCandidate},
        portfolio::SizingSuggestion,
        precision::RESEARCH_DECIMAL_SCALE,
    };

    /// A `Calibrated`-path candidate: `win_probability` is `Some`, so
    /// `KellySizingModel` uses the `Resolution` bet structure directly
    /// (`f* = (q - p) / (1 - p)`). Defaults to `q=0.6, p=0.5` (`f*=0.2`,
    /// deliberately modest so shrink-mechanism tests aren't pre-capped by
    /// `max_position_pct`); tests that need a specific `f*` use
    /// [`resolution_candidate`] directly.
    fn candidate(
        expected_bps: Decimal,
        downside_bps: Decimal,
        confidence: Decimal,
    ) -> SignalCandidate {
        resolution_candidate(expected_bps, downside_bps, confidence, dec!(0.6), dec!(0.5))
    }

    /// A `Calibrated`-path candidate with an explicit `(win_probability,
    /// market_price)` pair, for tests that need to control `f*` precisely.
    fn resolution_candidate(
        expected_bps: Decimal,
        downside_bps: Decimal,
        confidence: Decimal,
        win_probability: Decimal,
        market_price: Decimal,
    ) -> SignalCandidate {
        SignalCandidate {
            win_probability: Some(Probability::new(win_probability)),
            entry_price_ref: Price::new(market_price),
            ..base_candidate(expected_bps, downside_bps, confidence)
        }
    }

    /// A `Heuristic`-path candidate: `win_probability` is `None`, so
    /// `KellySizingModel` falls back to the legacy TP/SL bet-structure
    /// recovery. Only reachable in production behind fail-closed publish /
    /// admission gates — exercised here purely to pin the cold-start
    /// `ReportOnly` experimental behavior.
    fn heuristic_candidate(
        expected_bps: Decimal,
        downside_bps: Decimal,
        confidence: Decimal,
    ) -> SignalCandidate {
        SignalCandidate {
            win_probability: None,
            ..base_candidate(expected_bps, downside_bps, confidence)
        }
    }

    fn base_candidate(
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
            rank_before_portfolio: 0,
            liquidity_score: Probability::ZERO,
            data_quality_score: Probability::ZERO,
            model_score_percentile: Probability::ZERO,
            decision_at: Utc::now(),
        }
    }

    impl KellySafetyParams {
        fn test_fixture() -> Self {
            Self::new(dec!(1), dec!(0.5), dec!(0.9))
        }
    }

    fn sizing_input(candidate: &SignalCandidate, capital: Usd) -> SizingInput<'_> {
        SizingInput {
            candidate,
            capital_base_usd: capital,
            drawdown_state: DrawdownState::neutral(),
            edge_uncertainty_half_width: None,
            correlation: None,
            executable_tiers: executable_tiers(candidate, capital),
        }
    }

    fn executable_tiers(candidate: &SignalCandidate, capital: Usd) -> Vec<ExecutableSizingTier> {
        let price = candidate.entry_price_ref;
        if !price.is_positive() {
            return Vec::new();
        }
        let Some(level) = BookLevel::try_from_decimal(price, capital / price * Decimal::TWO) else {
            return Vec::new();
        };
        let schedule = PitFeeSchedule {
            schedule_hash: CanonicalDigest::content_hash_json(&"zero-fee").expect("hash"),
            effective_at: candidate.decision_at,
            available_at: candidate.decision_at,
            platform_rate: Decimal::ZERO,
            exponent: Decimal::ONE,
            taker_only: true,
            builder_maker_fee_bps: Bps::ZERO,
            builder_taker_fee_bps: Bps::ZERO,
            builder_attribution: BuilderFeeAttribution::NoBuilderCode,
        };
        (1..=capital.inner().floor().to_u64().unwrap_or_default())
            .filter_map(|cash| {
                let cash_budget = Usd::new(Decimal::from(cash));
                walk_buy_cash_budget(
                    slice::from_ref(&level),
                    cash_budget,
                    price,
                    FillRequirement::AllOrNothing,
                    &schedule,
                    LiquidityRole::Taker,
                    candidate.decision_at,
                )
                .ok()
                .map(|fill| ExecutableSizingTier { cash_budget, fill })
            })
            .collect()
    }

    impl KellySizingModel {
        fn test_fixture() -> Self {
            // Half Kelly, 10% position cap, linear confidence shrink, fixed drawdown.
            Self::new(
                dec!(0.5),
                dec!(0.1),
                ConfidenceSizeCurve::Linear,
                DrawdownMultiplierPolicy::Fixed,
                KellySafetyParams::test_fixture(),
            )
        }
    }

    impl SizingOutcome {
        fn sized(self) -> SizingSuggestion {
            match self {
                Self::Sized(s) => *s,
                Self::Rejected(r) => panic!("expected sized, got {r:?}"),
            }
        }
    }

    #[test]
    fn kelly_uses_calibrated_directly() {
        // Resolution structure: f* = (q - p) / (1 - p), q=0.99, p=0.01 -> f*≈0.9899.
        let model = KellySizingModel::test_fixture();
        let s = (model
            .suggest(&sizing_input(
                &resolution_candidate(dec!(200), dec!(100), dec!(1), dec!(0.99), dec!(0.01)),
                Usd::new(dec!(10000)),
            ))
            .expect("suggest"))
        .sized();
        // multiplier = 0.5 (kelly_fraction) * f* ≈ 0.495 > max_position_pct (0.1) -> capped.
        assert_eq!(s.desired_usd, Usd::new(dec!(1000)));
        assert!(s.binding_kelly_cap, "max_position_pct must bind");
        // The audit edge is recomputed from the executable fill and calibrated
        // q, never trusted from a quote-price model-output field.
        assert_eq!(s.edge_bps.expect("edge").inner(), dec!(980000));
        assert!(s.provenance.is_some());
    }

    #[test]
    fn kelly_fraction_matches_price() {
        // q=0.7, p=0.4 -> f* = (0.7-0.4)/(1-0.4) = 0.5.
        let model = KellySizingModel::new(
            dec!(1),
            dec!(1),
            ConfidenceSizeCurve::Linear,
            DrawdownMultiplierPolicy::Fixed,
            KellySafetyParams::new(dec!(0), dec!(1), dec!(0.9)),
        );
        let s = (model
            .suggest(&sizing_input(
                &resolution_candidate(dec!(300), dec!(100), dec!(1), dec!(0.7), dec!(0.4)),
                Usd::new(dec!(10000)),
            ))
            .expect("suggest"))
        .sized();
        assert_eq!(s.provenance.expect("provenance").f_star, dec!(0.5));
    }

    #[test]
    fn kelly_win_probability_rejects() {
        // q == p: no edge over the market -> reject, never funded.
        let model = KellySizingModel::test_fixture();
        let outcome = model
            .suggest(&sizing_input(
                &resolution_candidate(dec!(0), dec!(100), dec!(1), dec!(0.5), dec!(0.5)),
                Usd::new(dec!(10000)),
            ))
            .expect("suggest");
        assert_eq!(
            outcome,
            SizingOutcome::Rejected(RejectionReason::NoPositiveSignal)
        );

        // q < p: negative edge -> reject.
        let below = model
            .suggest(&sizing_input(
                &resolution_candidate(dec!(-50), dec!(100), dec!(1), dec!(0.4), dec!(0.5)),
                Usd::new(dec!(10000)),
            ))
            .expect("suggest");
        assert_eq!(
            below,
            SizingOutcome::Rejected(RejectionReason::NoPositiveSignal)
        );
    }

    #[test]
    fn kelly_invalid_market_rejects() {
        let model = KellySizingModel::test_fixture();
        let zero = model
            .suggest(&sizing_input(
                &resolution_candidate(dec!(200), dec!(100), dec!(1), dec!(0.9), dec!(0)),
                Usd::new(dec!(10000)),
            ))
            .expect("suggest");
        assert_eq!(
            zero,
            SizingOutcome::Rejected(RejectionReason::ExecutableEntryUnavailable)
        );

        let one = model
            .suggest(&sizing_input(
                &resolution_candidate(dec!(200), dec!(100), dec!(1), dec!(0.9), dec!(1)),
                Usd::new(dec!(10000)),
            ))
            .expect("suggest");
        assert_eq!(
            one,
            SizingOutcome::Rejected(RejectionReason::NoPositiveSignal)
        );
    }

    #[test]
    fn kelly_rejects_quote_fee() {
        let candidate = resolution_candidate(dec!(600), dec!(100), dec!(1), dec!(0.53), dec!(0.5));
        let schedule = PitFeeSchedule {
            schedule_hash: CanonicalDigest::content_hash_json(&"fee-aware").expect("hash"),
            effective_at: candidate.decision_at,
            available_at: candidate.decision_at,
            platform_rate: dec!(0.2),
            exponent: Decimal::ONE,
            taker_only: true,
            builder_maker_fee_bps: Bps::ZERO,
            builder_taker_fee_bps: Bps::ZERO,
            builder_attribution: BuilderFeeAttribution::NoBuilderCode,
        };
        let level =
            BookLevel::from_decimal_unchecked(candidate.entry_price_ref, Shares::new(dec!(1000)));
        let cash_budget = Usd::new(dec!(25));
        let fill = walk_buy_cash_budget(
            &[level],
            cash_budget,
            candidate.entry_price_ref,
            FillRequirement::AllOrNothing,
            &schedule,
            LiquidityRole::Taker,
            candidate.decision_at,
        )
        .expect("walk");
        let outcome = KellySizingModel::test_fixture()
            .suggest(&SizingInput {
                candidate: &candidate,
                capital_base_usd: Usd::new(dec!(10000)),
                drawdown_state: DrawdownState::neutral(),
                edge_uncertainty_half_width: None,
                correlation: None,
                executable_tiers: vec![ExecutableSizingTier { cash_budget, fill }],
            })
            .expect("suggest");

        assert_eq!(
            outcome,
            SizingOutcome::Rejected(RejectionReason::NoPositiveSignal),
            "q=0.53 beats the 0.50 ask but not the fee-adjusted all-in price"
        );
    }

    #[test]
    fn uncalibrated_return_model_rejects() {
        let outcome = KellySizingModel::test_fixture()
            .suggest(&SizingInput {
                candidate: &heuristic_candidate(dec!(50), dec!(100), dec!(1)),
                capital_base_usd: Usd::new(dec!(10000)),
                drawdown_state: DrawdownState::neutral(),
                edge_uncertainty_half_width: None,
                correlation: None,
                executable_tiers: Vec::new(),
            })
            .expect("suggest");
        assert_eq!(
            outcome,
            SizingOutcome::Rejected(RejectionReason::ReturnModelUncalibrated)
        );
    }

    #[test]
    fn heuristic_kelly_non_rejects() {
        // E[r] = −0.01 → edge negative → reject, never funded. Only reachable
        // via the Heuristic (win_probability=None) cold-start path.
        let model = KellySizingModel::test_fixture();
        let outcome = model
            .suggest(&SizingInput {
                candidate: &heuristic_candidate(dec!(-100), dec!(100), dec!(1)),
                capital_base_usd: Usd::new(dec!(10000)),
                drawdown_state: DrawdownState::neutral(),
                edge_uncertainty_half_width: None,
                correlation: None,
                executable_tiers: Vec::new(),
            })
            .expect("suggest");
        assert_eq!(
            outcome,
            SizingOutcome::Rejected(RejectionReason::ReturnModelUncalibrated)
        );
    }

    #[test]
    fn heuristic_kelly_invalid_rejects() {
        let model = KellySizingModel::test_fixture();
        let outcome = model
            .suggest(&SizingInput {
                candidate: &heuristic_candidate(dec!(200), dec!(0), dec!(1)),
                capital_base_usd: Usd::new(dec!(10000)),
                drawdown_state: DrawdownState::neutral(),
                edge_uncertainty_half_width: None,
                correlation: None,
                executable_tiers: Vec::new(),
            })
            .expect("suggest");
        assert_eq!(
            outcome,
            SizingOutcome::Rejected(RejectionReason::ReturnModelUncalibrated)
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
            ConfidenceSizeCurve::Linear,
            DrawdownMultiplierPolicy::Fixed,
            KellySafetyParams::test_fixture(),
        );
        let high = (model
            .suggest(&SizingInput {
                candidate: &candidate(dec!(10), dec!(200), dec!(1)),
                capital_base_usd: Usd::new(dec!(10000)),
                drawdown_state: DrawdownState::neutral(),
                edge_uncertainty_half_width: None,
                correlation: None,
                executable_tiers: executable_tiers(
                    &candidate(dec!(10), dec!(200), dec!(1)),
                    Usd::new(dec!(10000)),
                ),
            })
            .expect("suggest"))
        .sized();
        let low = (model
            .suggest(&SizingInput {
                candidate: &candidate(dec!(10), dec!(200), dec!(0.4)),
                capital_base_usd: Usd::new(dec!(10000)),
                drawdown_state: DrawdownState::neutral(),
                edge_uncertainty_half_width: None,
                correlation: None,
                executable_tiers: executable_tiers(
                    &candidate(dec!(10), dec!(200), dec!(0.4)),
                    Usd::new(dec!(10000)),
                ),
            })
            .expect("suggest"))
        .sized();
        assert!(
            low.desired_usd.inner() < high.desired_usd.inner(),
            "lower confidence must shrink the bet: {low:?} vs {high:?}"
        );
        // Linear: confidence 0.4 → exactly 0.4× the full-confidence bet.
        assert_eq!(low.desired_usd, high.desired_usd * dec!(0.4));
    }

    #[test]
    fn kelly_step_confidence_buckets() {
        // Step curve: confidence 0.4 → 0.25 shrink; uncapped small bet shows it.
        let model = KellySizingModel::new(
            dec!(1),
            dec!(0.9),
            ConfidenceSizeCurve::Step,
            DrawdownMultiplierPolicy::Fixed,
            KellySafetyParams::test_fixture(),
        );
        let low = (model
            .suggest(&SizingInput {
                candidate: &candidate(dec!(10), dec!(200), dec!(0.4)),
                capital_base_usd: Usd::new(dec!(10000)),
                drawdown_state: DrawdownState::neutral(),
                edge_uncertainty_half_width: None,
                correlation: None,
                executable_tiers: executable_tiers(
                    &candidate(dec!(10), dec!(200), dec!(0.4)),
                    Usd::new(dec!(10000)),
                ),
            })
            .expect("suggest"))
        .sized();
        let mid = (model
            .suggest(&SizingInput {
                candidate: &candidate(dec!(10), dec!(200), dec!(0.6)),
                capital_base_usd: Usd::new(dec!(10000)),
                drawdown_state: DrawdownState::neutral(),
                edge_uncertainty_half_width: None,
                correlation: None,
                executable_tiers: executable_tiers(
                    &candidate(dec!(10), dec!(200), dec!(0.6)),
                    Usd::new(dec!(10000)),
                ),
            })
            .expect("suggest"))
        .sized();
        // 0.6 → 0.5 bucket is double the 0.4 → 0.25 bucket.
        assert_eq!(mid.desired_usd, low.desired_usd * dec!(2));
        assert_eq!(model.kind(), SizingModelKind::Kelly);
    }

    #[test]
    fn conservative_drawdown_scales_fraction() {
        let fixed = KellySizingModel::new(
            dec!(0.5),
            dec!(0.9),
            ConfidenceSizeCurve::Linear,
            DrawdownMultiplierPolicy::Fixed,
            KellySafetyParams::test_fixture(),
        );
        let conservative = KellySizingModel::new(
            dec!(0.5),
            dec!(0.9),
            ConfidenceSizeCurve::Linear,
            DrawdownMultiplierPolicy::Conservative,
            KellySafetyParams::test_fixture(),
        );
        let input_candidate = candidate(dec!(10), dec!(200), dec!(1));
        let fixed_size = (fixed
            .suggest(&SizingInput {
                candidate: &input_candidate,
                capital_base_usd: Usd::new(dec!(10000)),
                drawdown_state: DrawdownState {
                    current_drawdown: dec!(0.2),
                },
                edge_uncertainty_half_width: None,
                correlation: None,
                executable_tiers: executable_tiers(&input_candidate, Usd::new(dec!(10000))),
            })
            .expect("fixed suggest"))
        .sized();
        let conservative_size = (conservative
            .suggest(&SizingInput {
                candidate: &input_candidate,
                capital_base_usd: Usd::new(dec!(10000)),
                drawdown_state: DrawdownState {
                    current_drawdown: dec!(0.2),
                },
                edge_uncertainty_half_width: None,
                correlation: None,
                executable_tiers: executable_tiers(&input_candidate, Usd::new(dec!(10000))),
            })
            .expect("conservative suggest"))
        .sized();

        assert_eq!(
            conservative_size.desired_usd,
            fixed_size.desired_usd * dec!(0.8)
        );
    }

    #[test]
    fn kelly_fixed_ignores_drawdown() {
        let fixed = KellySizingModel::new(
            dec!(0.5),
            dec!(0.9),
            ConfidenceSizeCurve::Linear,
            DrawdownMultiplierPolicy::Fixed,
            KellySafetyParams::test_fixture(),
        );
        let input_candidate = candidate(dec!(10), dec!(200), dec!(1));
        let neutral = (fixed
            .suggest(&SizingInput {
                candidate: &input_candidate,
                capital_base_usd: Usd::new(dec!(10000)),
                drawdown_state: DrawdownState::neutral(),
                edge_uncertainty_half_width: None,
                correlation: None,
                executable_tiers: executable_tiers(&input_candidate, Usd::new(dec!(10000))),
            })
            .expect("neutral suggest"))
        .sized();
        let in_drawdown = (fixed
            .suggest(&SizingInput {
                candidate: &input_candidate,
                capital_base_usd: Usd::new(dec!(10000)),
                drawdown_state: DrawdownState {
                    current_drawdown: dec!(0.2),
                },
                edge_uncertainty_half_width: None,
                correlation: None,
                executable_tiers: executable_tiers(&input_candidate, Usd::new(dec!(10000))),
            })
            .expect("drawdown suggest"))
        .sized();
        assert_eq!(in_drawdown.desired_usd, neutral.desired_usd);
        assert_eq!(
            in_drawdown.kelly_stage_binding, None,
            "a Fixed drawdown policy must never emit DrawdownCap, since it never shrinks"
        );
    }

    #[test]
    fn kelly_stage_binding_immaterial() {
        // confidence=1 (shrink=1), Fixed drawdown (shrink=1), no correlation
        // input (shrink=1): every multiplier is exactly 1, which is not
        // "materially low" under any threshold < 1, so no soft binding fires.
        let model = KellySizingModel::new(
            dec!(0.5),
            dec!(0.9),
            ConfidenceSizeCurve::Linear,
            DrawdownMultiplierPolicy::Fixed,
            KellySafetyParams::new(dec!(1), dec!(0.5), dec!(0.95)),
        );
        let suggestion = (model
            .suggest(&SizingInput {
                candidate: &candidate(dec!(10), dec!(200), dec!(1)),
                capital_base_usd: Usd::new(dec!(10000)),
                drawdown_state: DrawdownState::neutral(),
                edge_uncertainty_half_width: None,
                correlation: None,
                executable_tiers: executable_tiers(
                    &candidate(dec!(10), dec!(200), dec!(1)),
                    Usd::new(dec!(10000)),
                ),
            })
            .expect("suggest"))
        .sized();
        assert_eq!(suggestion.kelly_stage_binding, None);
    }

    #[test]
    fn confidence_cap_not_shrink() {
        // Confidence is only mildly low (0.7 -> shrink=0.7, above the 0.6
        // floor materiality would need); drawdown is severe (0.5 current
        // drawdown -> shrink=0.5, the strictly smaller multiplier) so
        // DrawdownCap — not ConfidenceCap — must be the emitted binding.
        let model = KellySizingModel::new(
            dec!(0.5),
            dec!(0.9),
            ConfidenceSizeCurve::Linear,
            DrawdownMultiplierPolicy::Conservative,
            KellySafetyParams::new(dec!(1), dec!(0.5), dec!(0.95)),
        );
        let suggestion = (model
            .suggest(&SizingInput {
                candidate: &candidate(dec!(10), dec!(200), dec!(0.7)),
                capital_base_usd: Usd::new(dec!(10000)),
                drawdown_state: DrawdownState {
                    current_drawdown: dec!(0.5),
                },
                edge_uncertainty_half_width: None,
                correlation: None,
                executable_tiers: executable_tiers(
                    &candidate(dec!(10), dec!(200), dec!(0.7)),
                    Usd::new(dec!(10000)),
                ),
            })
            .expect("suggest"))
        .sized();
        assert_eq!(
            suggestion.kelly_stage_binding,
            Some(BindingConstraint::DrawdownCap),
            "the strictly smaller shrink (drawdown=0.5 < confidence=0.7) must win the binding \
             attribution, not confidence"
        );
    }

    #[test]
    fn kelly_edge_uncertainty_monotonically() {
        // Half-Kelly, uncapped (max_position_pct=0.9) so the sweep is
        // observable across every half-width rather than clipped flat by the
        // per-position cap.
        let model = KellySizingModel::new(
            dec!(0.5),
            dec!(0.9),
            ConfidenceSizeCurve::Linear,
            DrawdownMultiplierPolicy::Fixed,
            KellySafetyParams::test_fixture(),
        );
        let candidate = candidate(dec!(10), dec!(200), dec!(1));
        let capital = Usd::new(dec!(10000));
        let half_widths = [
            None,
            Some(dec!(0.05)),
            Some(dec!(0.15)),
            Some(dec!(0.3)),
            Some(dec!(0.5)),
        ];
        let mut sizes = Vec::with_capacity(half_widths.len());
        for half_width in half_widths {
            let suggestion = (model
                .suggest(&SizingInput {
                    candidate: &candidate,
                    capital_base_usd: capital,
                    drawdown_state: DrawdownState::neutral(),
                    edge_uncertainty_half_width: half_width,
                    correlation: None,
                    executable_tiers: executable_tiers(&candidate, capital),
                })
                .expect("sized"))
            .sized();
            sizes.push((half_width, suggestion));
        }
        for window in sizes.windows(2) {
            let (prev_width, prev) = &window[0];
            let (next_width, next) = &window[1];
            assert!(
                next.desired_usd <= prev.desired_usd,
                "widening half-width from {prev_width:?} to {next_width:?} must never \
                 increase size: prev={:?} next={:?}",
                prev.desired_usd,
                next.desired_usd
            );
        }
        assert_eq!(
            sizes[0].1.edge_uncertainty_shrink_applied,
            Some(Decimal::ONE),
            "no half-width supplied must apply no shrink"
        );
        assert!(
            sizes
                .last()
                .expect("non-empty")
                .1
                .edge_uncertainty_shrink_applied
                .expect("audit")
                < Decimal::ONE,
            "a wide half-width must apply a strictly-below-1 shrink"
        );
    }

    #[test]
    fn correlation_shrink_reduces_cluster() {
        // Half-Kelly, uncapped (max_position_pct=0.9) so the sweep is
        // observable across every cluster size rather than clipped flat by
        // the per-position cap.
        let model = KellySizingModel::new(
            dec!(0.5),
            dec!(0.9),
            ConfidenceSizeCurve::Linear,
            DrawdownMultiplierPolicy::Fixed,
            KellySafetyParams::test_fixture(),
        );
        let candidate = candidate(dec!(10), dec!(200), dec!(1));
        let capital = Usd::new(dec!(10000));
        let cluster_sizes = [1_u32, 2, 3, 5, 10];
        let mut sizes = Vec::with_capacity(cluster_sizes.len());
        for cluster_size in cluster_sizes {
            let suggestion = (model
                .suggest(&SizingInput {
                    candidate: &candidate,
                    capital_base_usd: capital,
                    drawdown_state: DrawdownState::neutral(),
                    edge_uncertainty_half_width: None,
                    correlation: Some(CorrelationShrinkInput {
                        cluster_size,
                        mean_rho: dec!(0.5),
                    }),
                    executable_tiers: executable_tiers(&candidate, capital),
                })
                .expect("sized"))
            .sized();
            sizes.push((cluster_size, suggestion));
        }
        for window in sizes.windows(2) {
            let (prev_n, prev) = &window[0];
            let (next_n, next) = &window[1];
            assert!(
                next.desired_usd <= prev.desired_usd,
                "growing the cluster from {prev_n} to {next_n} must never increase size: \
                 prev={:?} next={:?}",
                prev.desired_usd,
                next.desired_usd
            );
        }
        assert_eq!(
            sizes[0].1.correlation_shrink_applied,
            Some(Decimal::ONE),
            "a solo candidate (cluster_size=1) must apply no correlation shrink"
        );
        assert!(
            sizes
                .last()
                .expect("non-empty")
                .1
                .correlation_shrink_applied
                .expect("audit")
                < Decimal::ONE,
            "a large correlated cluster must apply a strictly-below-1 shrink"
        );
    }

    #[test]
    fn sizing_provenance_waterfall_applied() {
        let model = KellySizingModel::new(
            dec!(0.5),
            dec!(0.9),
            ConfidenceSizeCurve::Linear,
            DrawdownMultiplierPolicy::Conservative,
            KellySafetyParams::test_fixture(),
        );
        let suggestion = (model
            .suggest(&SizingInput {
                candidate: &candidate(dec!(10), dec!(200), dec!(0.8)),
                capital_base_usd: Usd::new(dec!(10000)),
                drawdown_state: DrawdownState {
                    current_drawdown: dec!(0.1),
                },
                edge_uncertainty_half_width: Some(dec!(0.05)),
                correlation: Some(CorrelationShrinkInput {
                    cluster_size: 3,
                    mean_rho: dec!(0.3),
                }),
                executable_tiers: executable_tiers(
                    &candidate(dec!(10), dec!(200), dec!(0.8)),
                    Usd::new(dec!(10000)),
                ),
            })
            .expect("suggest"))
        .sized();
        let provenance = suggestion.provenance.expect("provenance recorded");
        let reproduced = (provenance.f_star
            * provenance.kelly_fraction_config
            * provenance.confidence_shrink
            * provenance.drawdown_shrink
            * provenance.edge_uncertainty_shrink
            * provenance.correlation_shrink)
            .round_dp(RESEARCH_DECIMAL_SCALE);
        assert_eq!(
            reproduced, provenance.raw_fraction,
            "the waterfall's per-stage product must reproduce raw_fraction exactly"
        );
        assert_eq!(
            provenance.kelly_fraction_config,
            dec!(0.5),
            "kelly_fraction_config must be the static config constant, not the merged multiplier"
        );
        assert_eq!(provenance.position_cap_fraction, dec!(0.9));
    }

    #[test]
    fn drawdown_cap_binding_active() {
        let model = KellySizingModel::new(
            dec!(0.5),
            dec!(0.9),
            ConfidenceSizeCurve::Linear,
            DrawdownMultiplierPolicy::Conservative,
            KellySafetyParams::new(dec!(1), dec!(0.5), dec!(0.95)),
        );
        let candidate = candidate(dec!(10), dec!(200), dec!(1));
        let suggestion = (model
            .suggest(&SizingInput {
                candidate: &candidate,
                capital_base_usd: Usd::new(dec!(10000)),
                drawdown_state: DrawdownState {
                    current_drawdown: dec!(0.25),
                },
                edge_uncertainty_half_width: None,
                correlation: None,
                executable_tiers: executable_tiers(&candidate, Usd::new(dec!(10000))),
            })
            .expect("suggest"))
        .sized();
        assert_eq!(
            suggestion.kelly_stage_binding,
            Some(BindingConstraint::DrawdownCap)
        );
    }

    #[test]
    fn calibrated_return_uses_price() {
        let candidate = resolution_candidate(dec!(200), dec!(500), dec!(1), dec!(0.7), dec!(0.4));
        let capital = Usd::new(dec!(10000));
        let suggestion = (KellySizingModel::new(
            dec!(1),
            dec!(1),
            ConfidenceSizeCurve::Linear,
            DrawdownMultiplierPolicy::Fixed,
            KellySafetyParams::new(dec!(0), dec!(1), dec!(0.9)),
        )
        .suggest(&sizing_input(&candidate, capital))
        .expect("suggest"))
        .sized();
        assert_eq!(suggestion.provenance.expect("provenance").f_star, dec!(0.5));
    }
}
