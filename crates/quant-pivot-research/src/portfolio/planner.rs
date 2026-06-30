//! Governed portfolio planner: the deterministic "how much to buy" closed loop.
//!
//! [`DefaultPortfolioPlanner`] turns scored candidates + the real account
//! capital base + governed budget / constraints / sizing into:
//!
//! 1. per-candidate [`SizingPlan`] (suggested size, binding cap, exposure-after,
//!    Kelly provenance) and [`RiskEnvelope`] (admission inputs + canonical hash),
//! 2. a stable risk-adjusted ranking truncated to `top_n`,
//! 3. a `rejected` summary with a precise [`RejectionReason`] per dropped
//!    candidate, and
//! 4. a persistable [`NewPortfolioPlan`] row.
//!
//! It is pure and deterministic: the same `(candidates, account, config)` always
//! yields the same plan (stable sort, no wall-clock, no randomness). `f64` never
//! appears; every money value stays in a project newtype.

use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use quant_pivot_error::QuantResult;
use quant_pivot_models::{
    domain::quant::NewPortfolioPlan,
    enums::{
        common::MarketCategory,
        quant::{BindingConstraint, RejectionReason, SizingModelKind},
    },
    hashing::CanonicalDigest,
    types::{
        Bps, ContentHash, EventId, MarketId, MarketSelectionId, ModelRunId,
        PortfolioConstraintsSnapshot, PortfolioOptimizerMeta, PortfolioPlanId,
        PortfolioRejectedSummary, PortfolioRiskBudget, Probability, RejectionReasonCount,
        RiskEnvelope, RiskEnvelopeHashInput, Shares, SignalCandidateId, SizingPlan, Usd,
    },
};
use rust_decimal::Decimal;

use crate::{
    backtest::PortfolioCaps,
    model::signal::SignalCandidate,
    portfolio::{
        account::AccountSnapshot,
        allocator::{
            Allocation, AllocationInput, AllocationOutput, CandidateMeta, PortfolioAllocator,
        },
        correlation::CorrelationConstraint,
        optimizer::OptimizerOutcome,
        sizing::{DrawdownState, SizingInput, SizingModel, SizingOutcome, SizingSuggestion},
    },
    precision::RESEARCH_DECIMAL_SCALE,
};

/// Basis-point denominator (`1 bps = 1/10_000`).
const BPS_PER_UNIT: i64 = 10_000;

/// Rank-score discount when an allocation could not respect visible liquidity (0.5).
fn liquidity_infeasible_discount() -> Decimal {
    Decimal::new(5, 1)
}

/// Rank-score discount when scarce budget / exposure room compressed the size (0.9).
fn scarcity_binding_discount() -> Decimal {
    Decimal::new(9, 1)
}

/// One candidate plus the allocation metadata the planner needs (no desired
/// size — the planner computes that via the sizing model).
pub struct PlanCandidate<'a> {
    /// The scored candidate.
    pub candidate: &'a SignalCandidate,
    /// Market category (category exposure cap + breakdown).
    pub category: MarketCategory,
    /// Owning event (event exposure cap), when known.
    pub event_id: Option<EventId>,
    /// Visible liquidity (liquidity-usage cap), when known.
    pub liquidity_usd: Option<Usd>,
    /// Normalized visible liquidity score in `[0, 1]` from decision capture.
    pub liquidity_score: Probability,
}

/// All inputs to one portfolio plan.
pub struct PortfolioPlanInput<'a> {
    /// Pre-minted plan id.
    pub portfolio_plan_id: PortfolioPlanId,
    /// Model run that produced the candidates.
    pub model_run_id: ModelRunId,
    /// Market selection snapshot the candidates came from.
    pub market_selection_id: MarketSelectionId,
    /// Decision time (the report's frozen `as_of`).
    pub as_of: DateTime<Utc>,
    /// Accepted candidates with their allocation metadata.
    pub candidates: Vec<PlanCandidate<'a>>,
    /// Real account capital base + current exposure net.
    pub account: &'a AccountSnapshot,
    /// Decision-time drawdown state derived from the equity history ledger.
    pub drawdown_state: DrawdownState,
    /// Governed budget / exposure / liquidity caps (parsed).
    pub caps: &'a PortfolioCaps,
    /// Configured correlated-exposure cap (recorded in the plan snapshot).
    pub max_correlated_exposure_usd: Usd,
    /// Correlated-cluster constraint that actually binds the optimizer, when
    /// correlation is enabled. `None` ⇒ the correlation cap does not bind.
    pub correlation: Option<&'a CorrelationConstraint>,
    /// The active sizing model.
    pub sizing: &'a dyn SizingModel,
    /// Entry-order slippage budget recorded on each risk envelope.
    pub entry_max_slippage_bps: Bps,
    /// Maximum published recommendations.
    pub top_n: usize,
}

/// A published, ranked recommendation produced by the planner.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlannedRecommendation {
    /// The scored candidate.
    pub candidate: SignalCandidate,
    /// Strong-typed sizing decision.
    pub sizing: SizingPlan,
    /// Strong-typed risk envelope (flags finalized by the 04.2 composer).
    pub risk_envelope: RiskEnvelope,
    /// Portfolio-constraint-adjusted ranking score.
    pub risk_adjusted_score: Probability,
    /// 1-based rank within the published set.
    pub rank: u32,
}

/// A candidate that was not published, with the precise cause.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RejectedCandidate {
    /// The rejected candidate's id.
    pub signal_candidate_id: SignalCandidateId,
    /// Its market.
    pub market_id: MarketId,
    /// Why it was dropped.
    pub reason: RejectionReason,
    /// Human-readable detail.
    pub detail: String,
}

/// The full output of one plan.
pub struct PortfolioPlanOutput {
    /// Published recommendations, ranked and truncated to `top_n`.
    pub planned: Vec<PlannedRecommendation>,
    /// Rejected candidates with reasons.
    pub rejected: Vec<RejectedCandidate>,
    /// The persistable plan row.
    pub plan_row: NewPortfolioPlan,
}

/// A governed portfolio planner.
pub trait PortfolioPlanner: Send + Sync {
    /// Plan one cross-section deterministically.
    fn plan(&self, input: PortfolioPlanInput<'_>) -> QuantResult<PortfolioPlanOutput>;
}

/// The governed portfolio planner over an injected LP/MILP allocator.
#[derive(Clone)]
pub struct DefaultPortfolioPlanner {
    allocator: Arc<dyn PortfolioAllocator>,
}

impl DefaultPortfolioPlanner {
    /// Construct the planner over a portfolio allocator (built from config via
    /// [`crate::portfolio::optimizer::optimizer_from_config`]).
    #[must_use]
    pub fn new(allocator: Arc<dyn PortfolioAllocator>) -> Self {
        Self { allocator }
    }
}

/// Per-candidate state carried from sizing through allocation to ranking.
struct Scored<'a> {
    candidate: &'a SignalCandidate,
    suggestion: SizingSuggestion,
    allocation: Allocation,
    risk_adjusted: Probability,
    liquidity_score: Probability,
}

impl PortfolioPlanner for DefaultPortfolioPlanner {
    fn plan(&self, input: PortfolioPlanInput<'_>) -> QuantResult<PortfolioPlanOutput> {
        let capital_base = input.account.capital_base_usd;
        let sizing_kind = input.sizing.kind();
        let mut rejected: Vec<RejectedCandidate> = Vec::new();

        // 1. Size every candidate; fundable ones proceed to allocation.
        let (metas, mut sized) = size_candidates(
            &input.candidates,
            input.sizing,
            capital_base,
            input.drawdown_state,
            &mut rejected,
        )?;

        // 2. Allocate over the account's current exposure net + available cash
        //    via the injected LP/MILP optimizer (global TopN + capital choice).
        let allocation = self.allocator.allocate(&AllocationInput {
            candidates: metas,
            caps: input.caps,
            initial_exposures: &input.account.exposures,
            available_usd: input.account.available_usd.inner(),
            correlation: input.correlation,
            top_n: input.top_n,
        })?;
        let optimizer_outcome = allocation.outcome.clone();

        // 3. Classify allocations: ranked survivors vs min-size / cap rejections.
        let mut scored = classify_allocations(allocation, &mut sized, input.caps, &mut rejected);

        // 4. Stable rank (parent §21.5). TopN inclusion is enforced by the LP
        //    allocator (MILP cardinality or relaxation recover), so every funded
        //    survivor is published — no second truncation pass.
        scored.sort_by(rank_order);
        let mut planned: Vec<PlannedRecommendation> = Vec::new();
        let mut allocated_total = Usd::ZERO;
        for (index, item) in scored.into_iter().enumerate() {
            allocated_total += item.allocation.allocated_usd;
            planned.push(PlannedRecommendation {
                rank: u32::try_from(index + 1).unwrap_or(u32::MAX),
                sizing: build_sizing_plan(&item, capital_base, input.caps, sizing_kind),
                risk_envelope: build_risk_envelope(&item, &input)?,
                risk_adjusted_score: item.risk_adjusted,
                candidate: item.candidate.clone(),
            });
        }

        let plan_row = build_plan_row(&input, allocated_total, &rejected, &optimizer_outcome);
        Ok(PortfolioPlanOutput {
            planned,
            rejected,
            plan_row,
        })
    }
}

/// Phase 1: size each candidate, routing rejections out and returning the
/// allocator metas plus a lookup from candidate id to its sizing + metadata.
type SizedLookup<'a> = BTreeMap<String, (SizingSuggestion, &'a PlanCandidate<'a>)>;

fn size_candidates<'a>(
    candidates: &'a [PlanCandidate<'a>],
    sizing: &dyn SizingModel,
    capital_base: Usd,
    drawdown_state: DrawdownState,
    rejected: &mut Vec<RejectedCandidate>,
) -> QuantResult<(Vec<CandidateMeta<'a>>, SizedLookup<'a>)> {
    let mut metas: Vec<CandidateMeta<'a>> = Vec::new();
    let mut sized: SizedLookup<'a> = BTreeMap::new();
    for plan_candidate in candidates {
        let outcome = sizing.suggest(&SizingInput {
            candidate: plan_candidate.candidate,
            capital_base_usd: capital_base,
            drawdown_state,
        })?;
        match outcome {
            SizingOutcome::Rejected(reason) => rejected.push(RejectedCandidate {
                signal_candidate_id: plan_candidate.candidate.signal_candidate_id.clone(),
                market_id: plan_candidate.candidate.market_id.clone(),
                reason,
                detail: "sizing produced no fundable size".to_owned(),
            }),
            SizingOutcome::Sized(suggestion) => {
                metas.push(CandidateMeta {
                    candidate: plan_candidate.candidate,
                    desired_usd: suggestion.desired_usd,
                    category: plan_candidate.category,
                    event_id: plan_candidate.event_id.clone(),
                    liquidity_usd: plan_candidate.liquidity_usd,
                });
                sized.insert(
                    plan_candidate.candidate.signal_candidate_id.to_string(),
                    (suggestion, plan_candidate),
                );
            }
        }
    }
    Ok((metas, sized))
}

/// Phase 3: classify each allocation into a ranked survivor or a rejection.
fn classify_allocations<'a>(
    allocation: AllocationOutput,
    sized: &mut SizedLookup<'a>,
    caps: &PortfolioCaps,
    rejected: &mut Vec<RejectedCandidate>,
) -> Vec<Scored<'a>> {
    let min_rec = caps.min_recommendation_usd.max(Decimal::ZERO);
    let mut scored: Vec<Scored<'a>> = Vec::new();
    for alloc in allocation.allocations {
        let Some((suggestion, plan_candidate)) =
            sized.remove(&alloc.signal_candidate_id.to_string())
        else {
            continue;
        };
        let candidate = plan_candidate.candidate;
        if alloc.allocated_usd.inner() < min_rec || alloc.allocated_usd.is_zero() {
            let reason = if suggestion.desired_usd.inner() < min_rec {
                // The Kelly-desired size itself was below the minimum useful ticket.
                RejectionReason::BelowMinSize
            } else if alloc.binding_constraint == BindingConstraint::None {
                // Feasible room existed but the global TopN selection excluded it.
                RejectionReason::BeyondTopN
            } else {
                rejection_for_binding(alloc.binding_constraint)
            };
            rejected.push(RejectedCandidate {
                signal_candidate_id: candidate.signal_candidate_id.clone(),
                market_id: candidate.market_id.clone(),
                reason,
                detail: format!("allocated {} below minimum {min_rec}", alloc.allocated_usd),
            });
            continue;
        }
        let binding = resolve_binding(alloc.binding_constraint, suggestion.binding_kelly_cap);
        let risk_adjusted = risk_adjusted_score(candidate, binding, alloc.liquidity_feasible);
        scored.push(Scored {
            candidate,
            suggestion,
            allocation: Allocation {
                binding_constraint: binding,
                ..alloc
            },
            risk_adjusted,
            liquidity_score: plan_candidate.liquidity_score,
        });
    }
    scored
}

/// Stable ranking order (parent §21.5): risk-adjusted desc → composite desc →
/// liquidity score desc → market id asc → token id asc.
fn rank_order(a: &Scored<'_>, b: &Scored<'_>) -> Ordering {
    b.risk_adjusted
        .cmp(&a.risk_adjusted)
        .then_with(|| {
            b.candidate
                .composite_score
                .cmp(&a.candidate.composite_score)
        })
        .then_with(|| b.liquidity_score.cmp(&a.liquidity_score))
        .then_with(|| {
            a.candidate
                .market_id
                .as_str()
                .cmp(b.candidate.market_id.as_str())
        })
        .then_with(|| {
            a.candidate
                .token_id
                .as_str()
                .cmp(b.candidate.token_id.as_str())
        })
}

/// Map an allocator binding to a rejection reason for a dropped candidate.
const fn rejection_for_binding(binding: BindingConstraint) -> RejectionReason {
    match binding {
        BindingConstraint::PortfolioBudget => RejectionReason::BudgetExhausted,
        BindingConstraint::AvailableCash => RejectionReason::AvailableCashExhausted,
        BindingConstraint::SingleMarketCap => RejectionReason::MarketCapExhausted,
        BindingConstraint::EventCap => RejectionReason::EventCapExhausted,
        BindingConstraint::CategoryCap => RejectionReason::CategoryCapExhausted,
        BindingConstraint::LiquidityCap => RejectionReason::LiquidityInfeasible,
        BindingConstraint::CorrelationCap => RejectionReason::CorrelationCapExhausted,
        _ => RejectionReason::BelowMinSize,
    }
}

/// Project the allocator's solve provenance into the persisted plan metadata.
fn optimizer_meta(outcome: &OptimizerOutcome) -> PortfolioOptimizerMeta {
    PortfolioOptimizerMeta {
        solver: outcome.solver,
        solve_mode: outcome.solve_mode,
        status: outcome.status,
        fell_back_to_relaxation: outcome.fell_back_to_relaxation,
        objective_value: outcome.objective_value,
        elapsed_ms: outcome.elapsed_ms,
        correlation_source: outcome.correlation_source,
        constraint_conflicts: outcome.constraint_conflicts.clone(),
    }
}

/// Resolve the published binding: the allocator's binding, unless the size was
/// the model's own (un-capped externally) choice that hit the Kelly cap.
fn resolve_binding(
    allocator_binding: BindingConstraint,
    binding_kelly_cap: bool,
) -> BindingConstraint {
    if allocator_binding == BindingConstraint::None && binding_kelly_cap {
        BindingConstraint::KellyCap
    } else {
        allocator_binding
    }
}

/// Whether a binding reflects scarce budget / exposure room (rank discount).
const fn is_scarcity_binding(binding: BindingConstraint) -> bool {
    matches!(
        binding,
        BindingConstraint::PortfolioBudget
            | BindingConstraint::AvailableCash
            | BindingConstraint::SingleMarketCap
            | BindingConstraint::EventCap
            | BindingConstraint::CategoryCap
            | BindingConstraint::LiquidityCap
    )
}

/// Portfolio-constraint-adjusted ranking score in `[0, 1]`.
fn risk_adjusted_score(
    candidate: &SignalCandidate,
    binding: BindingConstraint,
    liquidity_feasible: bool,
) -> Probability {
    let mut score = candidate.composite_score.inner() * candidate.confidence.inner();
    if !liquidity_feasible {
        score *= liquidity_infeasible_discount();
    }
    if is_scarcity_binding(binding) {
        score *= scarcity_binding_discount();
    }
    Probability::new(score.clamp(Decimal::ZERO, Decimal::ONE))
}

/// Build the strong-typed sizing plan for a published recommendation.
fn build_sizing_plan(
    item: &Scored<'_>,
    equity: Usd,
    caps: &PortfolioCaps,
    sizing_model: SizingModelKind,
) -> SizingPlan {
    let allocated = item.allocation.allocated_usd;
    let entry = item.candidate.entry_price_ref;
    let suggested_shares = if entry.inner() > Decimal::ZERO {
        (allocated / entry).round_dp(RESEARCH_DECIMAL_SCALE)
    } else {
        Shares::ZERO
    };
    let weight = if equity.inner() > Decimal::ZERO {
        (allocated.inner() / equity.inner()).round_dp(RESEARCH_DECIMAL_SCALE)
    } else {
        Decimal::ZERO
    };
    SizingPlan {
        suggested_usd: allocated,
        suggested_shares,
        max_usd: Usd::new(caps.max_single_recommendation_usd),
        min_usd: Usd::new(caps.min_recommendation_usd),
        portfolio_weight_pct: weight,
        market_exposure_after_usd: item.allocation.market_exposure_after_usd,
        event_exposure_after_usd: item.allocation.event_exposure_after_usd,
        category_exposure_after_usd: item.allocation.category_exposure_after_usd,
        binding_constraint: item.allocation.binding_constraint,
        sizing_reason: sizing_reason(item),
        sizing_model,
        edge_bps: item.suggestion.edge_bps,
        kelly_fraction_applied: item.suggestion.kelly_fraction_applied,
    }
}

/// Build the risk envelope skeleton; the composer finalizes the flags.
///
/// The canonical anchor hash is minted via
/// [`canonical_risk_envelope_hash`](quant_pivot_models::types::canonical_risk_envelope_hash)
/// — the single source of truth that execution admission recomputes to verify
/// the report-layer ↔ execution-layer anchor.
fn build_risk_envelope(
    item: &Scored<'_>,
    input: &PortfolioPlanInput<'_>,
) -> QuantResult<RiskEnvelope> {
    let loss_fraction = item.candidate.downside_bps / Decimal::from(BPS_PER_UNIT);
    let max_loss_usd = (item.allocation.allocated_usd * loss_fraction.max(Decimal::ZERO))
        .round_dp(RESEARCH_DECIMAL_SCALE);
    let max_position_usd = Usd::new(input.caps.max_single_recommendation_usd);
    let max_market_exposure_usd = Usd::new(input.caps.max_market_exposure_usd);
    let max_event_exposure_usd = Usd::new(input.caps.max_event_exposure_usd);
    let max_category_exposure_usd = Usd::new(input.caps.max_category_exposure_usd);

    let envelope_hash: ContentHash = CanonicalDigest::content_hash_json(&RiskEnvelopeHashInput {
        loss_usd: max_loss_usd,
        slippage_bps: input.entry_max_slippage_bps,
        position_usd: max_position_usd,
        market_exposure_usd: max_market_exposure_usd,
        event_exposure_usd: max_event_exposure_usd,
        category_exposure_usd: max_category_exposure_usd,
    })?;

    Ok(RiskEnvelope {
        max_loss_usd,
        max_slippage_bps: input.entry_max_slippage_bps,
        max_position_usd,
        max_market_exposure_usd,
        max_event_exposure_usd,
        max_category_exposure_usd,
        requires_approval: false,
        auto_execution_allowed: false,
        risk_notes: Vec::new(),
        envelope_hash,
    })
}

/// Build the persistable plan row from the plan outcome.
fn build_plan_row(
    input: &PortfolioPlanInput<'_>,
    allocated_total: Usd,
    rejected: &[RejectedCandidate],
    optimizer_outcome: &OptimizerOutcome,
) -> NewPortfolioPlan {
    let total_budget = Usd::new(input.caps.total_budget_usd.max(Decimal::ZERO));
    let remaining = (total_budget - allocated_total).max(Usd::ZERO);
    let risk_budget = PortfolioRiskBudget {
        total_budget_usd: total_budget,
        capital_base_usd: input.account.capital_base_usd,
        reserved_usd: input.account.reserved_usd,
        allocated_usd: allocated_total,
        remaining_usd: remaining,
    };
    let constraints = PortfolioConstraintsSnapshot {
        max_market_exposure_usd: Usd::new(input.caps.max_market_exposure_usd),
        max_event_exposure_usd: Usd::new(input.caps.max_event_exposure_usd),
        max_category_exposure_usd: Usd::new(input.caps.max_category_exposure_usd),
        max_correlated_exposure_usd: input.max_correlated_exposure_usd,
        max_single_recommendation_usd: Usd::new(input.caps.max_single_recommendation_usd),
        min_recommendation_usd: Usd::new(input.caps.min_recommendation_usd),
        liquidity_usage_cap_pct: input.caps.liquidity_usage_cap_pct,
    };

    NewPortfolioPlan {
        portfolio_plan_id: input.portfolio_plan_id.clone(),
        model_run_id: Some(input.model_run_id.clone()),
        market_selection_id: input.market_selection_id.clone(),
        as_of: input.as_of,
        budget_usd: total_budget,
        allocated_usd: allocated_total,
        risk_budget_json: risk_budget,
        constraints_json: constraints,
        rejected_summary: rejected_summary(rejected),
        optimizer_meta_json: optimizer_meta(optimizer_outcome),
    }
}

/// Tally rejected candidates by reason (stable order).
fn rejected_summary(rejected: &[RejectedCandidate]) -> PortfolioRejectedSummary {
    let mut counts: BTreeMap<&'static str, u32> = BTreeMap::new();
    for candidate in rejected {
        *counts.entry(candidate.reason.as_str()).or_insert(0) += 1;
    }
    PortfolioRejectedSummary {
        rejected_count: u32::try_from(rejected.len()).unwrap_or(u32::MAX),
        reasons: counts
            .into_iter()
            .map(|(reason, count)| RejectionReasonCount {
                reason: reason.to_owned(),
                count,
            })
            .collect(),
    }
}

/// Human-readable sizing explanation.
fn sizing_reason(item: &Scored<'_>) -> String {
    let edge = item
        .suggestion
        .edge_bps
        .map_or_else(|| "n/a".to_owned(), |bps| bps.to_string());
    let fraction = item
        .suggestion
        .kelly_fraction_applied
        .unwrap_or(Decimal::ZERO);
    format!(
        "kelly: edge {edge} bps, fraction {fraction}, bound by {}",
        item.allocation.binding_constraint
    )
}
