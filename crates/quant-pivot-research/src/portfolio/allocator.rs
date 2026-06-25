//! Deterministic greedy portfolio allocator (shared by backtest + planner).
//!
//! Sorts candidates by risk-adjusted score and fills each up to its model-desired
//! size, converging on the total-budget / available-cash / per-bucket exposure /
//! liquidity caps. It is **explicitly not** globally optimal; Phase 5 may replace
//! the greedy core with an LP/MILP solver behind the same [`PortfolioAllocator`]
//! trait.
//!
//! Cap room starts from the account's *current* exposure
//! ([`AllocationInput::initial_exposures`]) so a candidate's room is
//! `cap − already-held − this-round-allocated`, and the reported
//! `*_exposure_after_usd` is the projected post-allocation net (existing + new).
//! Every allocation records the single [`BindingConstraint`] that limited it.

use std::cmp::Ordering;
use std::collections::BTreeMap;

use quant_pivot_error::QuantResult;
use quant_pivot_models::{
    enums::{common::MarketCategory, quant::BindingConstraint},
    types::{EventId, ExposureBreakdown, MarketId, SignalCandidateId, Usd},
};
use rust_decimal::Decimal;

use crate::{
    backtest::PortfolioCaps, model::signal::SignalCandidate, precision::RESEARCH_DECIMAL_SCALE,
};

/// A candidate paired with the allocation metadata the caps need.
#[derive(Debug, Clone)]
pub struct CandidateMeta<'a> {
    /// The scored candidate.
    pub candidate: &'a SignalCandidate,
    /// Model-desired size before any portfolio cap (from the [`SizingModel`]).
    ///
    /// [`SizingModel`]: crate::portfolio::SizingModel
    pub desired_usd: Usd,
    /// Market category (category exposure cap + breakdown).
    pub category: MarketCategory,
    /// Owning event (event exposure cap), when known.
    pub event_id: Option<EventId>,
    /// Visible liquidity (liquidity-usage cap), when known.
    pub liquidity_usd: Option<Usd>,
}

/// One capital allocation decision with full binding attribution.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Allocation {
    /// The allocated candidate.
    pub signal_candidate_id: SignalCandidateId,
    /// Market id.
    pub market_id: MarketId,
    /// Capital allocated (USD); `0` when the candidate was not funded.
    pub allocated_usd: Usd,
    /// The single cap that bound the final size.
    pub binding_constraint: BindingConstraint,
    /// Projected market exposure after this allocation (existing net + new).
    pub market_exposure_after_usd: Usd,
    /// Projected event exposure after this allocation (existing net + new).
    pub event_exposure_after_usd: Usd,
    /// Projected category exposure after this allocation (existing net + new).
    pub category_exposure_after_usd: Usd,
    /// Whether the intended size respected the liquidity-usage cap.
    pub liquidity_feasible: bool,
}

/// Allocation request for one cross-section.
pub struct AllocationInput<'a> {
    /// Candidates to consider (any order; sorted internally).
    pub candidates: Vec<CandidateMeta<'a>>,
    /// Portfolio caps (per-bucket / single-rec / min / liquidity).
    pub caps: &'a PortfolioCaps,
    /// Current account exposure net (starting point for cap-room checks).
    pub initial_exposures: &'a ExposureBreakdown,
    /// Available cash (collateral − reserved). Total deployable room is
    /// `min(caps.total_budget_usd, available_usd)`; the binding is attributed to
    /// [`BindingConstraint::AvailableCash`] when cash is the tighter limit.
    pub available_usd: Decimal,
}

/// Allocation result.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AllocationOutput {
    /// One entry per input candidate (including zero-funded ones).
    pub allocations: Vec<Allocation>,
}

/// Portfolio allocation strategy. Phase 04's governed planner reuses this trait.
pub trait PortfolioAllocator: Send + Sync {
    /// Allocate capital across the candidates subject to the caps.
    fn allocate(&self, input: &AllocationInput<'_>) -> QuantResult<AllocationOutput>;
}

/// The deterministic greedy allocator.
#[derive(Debug, Clone, Copy, Default)]
pub struct GreedyPortfolioAllocator;

impl GreedyPortfolioAllocator {
    /// Construct the allocator.
    #[must_use]
    pub const fn new() -> Self {
        Self
    }
}

/// Treat a non-positive per-bucket cap as unlimited.
fn bucket_cap(value: Decimal) -> Decimal {
    if value <= Decimal::ZERO {
        Decimal::MAX
    } else {
        value
    }
}

/// One bounded ceiling under consideration for a candidate's size.
struct Ceiling {
    value: Decimal,
    constraint: BindingConstraint,
}

/// Resolved total-deploy room: the tighter of budget vs available cash, with the
/// binding attributed to whichever was the limiter.
struct BudgetRoom {
    effective: Decimal,
    constraint: BindingConstraint,
}

impl BudgetRoom {
    fn resolve(total_budget_usd: Decimal, available_usd: Decimal) -> Self {
        let total_budget = total_budget_usd.max(Decimal::ZERO);
        let available = available_usd.max(Decimal::ZERO);
        Self {
            effective: total_budget.min(available),
            constraint: if available < total_budget {
                BindingConstraint::AvailableCash
            } else {
                BindingConstraint::PortfolioBudget
            },
        }
    }
}

/// Running per-bucket spend, seeded from the account's current exposure net.
struct SpendState {
    total: Decimal,
    market: BTreeMap<String, Decimal>,
    event: BTreeMap<String, Decimal>,
    category: BTreeMap<MarketCategory, Decimal>,
}

impl SpendState {
    fn seed(exposures: &ExposureBreakdown) -> Self {
        Self {
            total: Decimal::ZERO,
            market: exposures
                .per_market
                .iter()
                .map(|(id, usd)| (id.as_str().to_owned(), usd.inner()))
                .collect(),
            event: exposures
                .per_event
                .iter()
                .map(|(id, usd)| (id.as_str().to_owned(), usd.inner()))
                .collect(),
            category: exposures
                .per_category
                .iter()
                .map(|(category, usd)| (*category, usd.inner()))
                .collect(),
        }
    }

    /// Allocate one candidate, mutating the running spend and returning the
    /// decision with full binding attribution and projected exposure-after.
    fn allocate_one(
        &mut self,
        meta: &CandidateMeta<'_>,
        caps: &PortfolioCaps,
        budget: &BudgetRoom,
    ) -> Allocation {
        let candidate = meta.candidate;
        let market_key = candidate.market_id.as_str().to_owned();
        let event_key = meta
            .event_id
            .as_ref()
            .map_or_else(|| market_key.clone(), |id| id.as_str().to_owned());

        let market_held = self
            .market
            .get(&market_key)
            .copied()
            .unwrap_or(Decimal::ZERO);
        let event_held = self.event.get(&event_key).copied().unwrap_or(Decimal::ZERO);
        let category_held = self
            .category
            .get(&meta.category)
            .copied()
            .unwrap_or(Decimal::ZERO);

        let decision = decide_ceiling(
            meta,
            caps,
            budget,
            self.total,
            market_held,
            event_held,
            category_held,
        );

        // Below the minimum useful size: drop (do not reserve budget); keep the
        // binding so the planner can name the rejection cause.
        let min_rec = caps.min_recommendation_usd.max(Decimal::ZERO);
        let alloc = if decision.alloc_pre >= min_rec && decision.alloc_pre > Decimal::ZERO {
            decision.alloc_pre.round_dp(RESEARCH_DECIMAL_SCALE)
        } else {
            Decimal::ZERO
        };

        if alloc > Decimal::ZERO {
            self.total += alloc;
            *self
                .market
                .entry(market_key.clone())
                .or_insert(Decimal::ZERO) += alloc;
            *self.event.entry(event_key.clone()).or_insert(Decimal::ZERO) += alloc;
            *self.category.entry(meta.category).or_insert(Decimal::ZERO) += alloc;
        }

        Allocation {
            signal_candidate_id: candidate.signal_candidate_id.clone(),
            market_id: candidate.market_id.clone(),
            allocated_usd: Usd::new(alloc),
            binding_constraint: decision.binding,
            market_exposure_after_usd: Usd::new(
                self.market.get(&market_key).copied().unwrap_or(market_held),
            ),
            event_exposure_after_usd: Usd::new(
                self.event.get(&event_key).copied().unwrap_or(event_held),
            ),
            category_exposure_after_usd: Usd::new(
                self.category
                    .get(&meta.category)
                    .copied()
                    .unwrap_or(category_held),
            ),
            liquidity_feasible: decision.liquidity_feasible,
        }
    }
}

/// The chosen pre-min size, its binding cap, and liquidity feasibility.
struct SizeDecision {
    alloc_pre: Decimal,
    binding: BindingConstraint,
    liquidity_feasible: bool,
}

/// Pick the binding ceiling for one candidate given the room left in each cap.
///
/// The model's own desired size is considered first so that on ties an external
/// cap is named the limiter; it only "wins" when strictly smallest.
fn decide_ceiling(
    meta: &CandidateMeta<'_>,
    caps: &PortfolioCaps,
    budget: &BudgetRoom,
    spent_total: Decimal,
    market_held: Decimal,
    event_held: Decimal,
    category_held: Decimal,
) -> SizeDecision {
    let (liquidity_room, liquidity_known) =
        meta.liquidity_usd.map_or((Decimal::MAX, false), |liq| {
            (
                (liq.inner() * caps.liquidity_usage_cap_pct.max(Decimal::ZERO)).max(Decimal::ZERO),
                true,
            )
        });
    let desired = meta.desired_usd.inner().max(Decimal::ZERO);
    let max_single = bucket_cap(caps.max_single_recommendation_usd);

    let mut ceilings = vec![
        Ceiling {
            value: desired,
            constraint: BindingConstraint::None,
        },
        Ceiling {
            value: max_single,
            constraint: BindingConstraint::SingleRecommendationCap,
        },
        Ceiling {
            value: (budget.effective - spent_total).max(Decimal::ZERO),
            constraint: budget.constraint,
        },
        Ceiling {
            value: (bucket_cap(caps.max_market_exposure_usd) - market_held).max(Decimal::ZERO),
            constraint: BindingConstraint::SingleMarketCap,
        },
        Ceiling {
            value: (bucket_cap(caps.max_event_exposure_usd) - event_held).max(Decimal::ZERO),
            constraint: BindingConstraint::EventCap,
        },
        Ceiling {
            value: (bucket_cap(caps.max_category_exposure_usd) - category_held).max(Decimal::ZERO),
            constraint: BindingConstraint::CategoryCap,
        },
    ];
    if liquidity_known {
        ceilings.push(Ceiling {
            value: liquidity_room,
            constraint: BindingConstraint::LiquidityCap,
        });
    }

    let alloc_pre = ceilings
        .iter()
        .map(|c| c.value)
        .min()
        .unwrap_or(Decimal::ZERO);
    let binding = ceilings
        .iter()
        .filter(|c| c.value == alloc_pre)
        .map(|c| c.constraint)
        .next_back()
        .unwrap_or(BindingConstraint::None);
    SizeDecision {
        alloc_pre,
        binding,
        liquidity_feasible: !liquidity_known || desired.min(max_single) <= liquidity_room,
    }
}

/// Deterministic greedy fill order: risk-adjusted score desc, then market/token id.
fn greedy_order(a: &CandidateMeta<'_>, b: &CandidateMeta<'_>) -> Ordering {
    risk_adjusted(b.candidate)
        .cmp(&risk_adjusted(a.candidate))
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

impl PortfolioAllocator for GreedyPortfolioAllocator {
    fn allocate(&self, input: &AllocationInput<'_>) -> QuantResult<AllocationOutput> {
        let budget = BudgetRoom::resolve(input.caps.total_budget_usd, input.available_usd);

        let mut order: Vec<&CandidateMeta<'_>> = input.candidates.iter().collect();
        order.sort_by(|a, b| greedy_order(a, b));

        let mut spend = SpendState::seed(input.initial_exposures);
        let mut allocations: BTreeMap<String, Allocation> = BTreeMap::new();
        for meta in order {
            let allocation = spend.allocate_one(meta, input.caps, &budget);
            allocations.insert(meta.candidate.signal_candidate_id.to_string(), allocation);
        }

        Ok(AllocationOutput {
            allocations: allocations.into_values().collect(),
        })
    }
}

/// Risk-adjusted greedy fill key: `composite_score · confidence`.
fn risk_adjusted(candidate: &SignalCandidate) -> Decimal {
    candidate.composite_score.inner() * candidate.confidence.inner()
}

#[cfg(test)]
mod tests {
    use super::{AllocationInput, CandidateMeta, GreedyPortfolioAllocator, PortfolioAllocator};
    use crate::{
        backtest::PortfolioCaps,
        model::signal::{ModelExplanation, SignalCandidate},
    };
    use chrono::Utc;
    use quant_pivot_models::{
        enums::{
            common::MarketCategory,
            quant::{BindingConstraint, SignalSide},
        },
        types::{
            ExposureBreakdown, MarketId, ModelRunId, Price, Probability, SignalCandidateId,
            TokenId, Usd,
        },
    };
    use rust_decimal::Decimal;
    use rust_decimal_macros::dec;

    fn candidate(market: &str, score: Decimal) -> SignalCandidate {
        SignalCandidate {
            signal_candidate_id: SignalCandidateId::from_v7(),
            model_run_id: ModelRunId::from_v7(),
            market_id: MarketId::new(market),
            token_id: TokenId::new("yes"),
            side: SignalSide::BuyYes,
            composite_score: Probability::new(score),
            confidence: Probability::new(dec!(1)),
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
            rank_before_portfolio: 0,
            as_of: Utc::now(),
        }
    }

    fn caps() -> PortfolioCaps {
        PortfolioCaps {
            total_budget_usd: dec!(1000),
            max_single_recommendation_usd: dec!(300),
            min_recommendation_usd: dec!(10),
            max_market_exposure_usd: dec!(300),
            max_event_exposure_usd: dec!(0),
            max_category_exposure_usd: dec!(400),
            liquidity_usage_cap_pct: dec!(0.1),
        }
    }

    fn meta(
        candidate: &SignalCandidate,
        category: MarketCategory,
        liquidity: Option<Usd>,
    ) -> CandidateMeta<'_> {
        CandidateMeta {
            candidate,
            desired_usd: Usd::new(dec!(300)),
            category,
            event_id: None,
            liquidity_usd: liquidity,
        }
    }

    #[test]
    fn greedy_allocator_respects_budget_and_caps() {
        let c1 = candidate("0xa", dec!(0.9));
        let c2 = candidate("0xb", dec!(0.8));
        let c3 = candidate("0xc", dec!(0.7));
        let metas = vec![
            meta(&c1, MarketCategory::Crypto, Some(Usd::new(dec!(100000)))),
            meta(&c2, MarketCategory::Sports, Some(Usd::new(dec!(1000)))), // liquidity cap = 100
            meta(&c3, MarketCategory::Crypto, Some(Usd::new(dec!(100000)))),
        ];
        let caps = caps();
        let empty = ExposureBreakdown::default();
        let out = GreedyPortfolioAllocator::new()
            .allocate(&AllocationInput {
                candidates: metas,
                caps: &caps,
                initial_exposures: &empty,
                available_usd: caps.total_budget_usd,
            })
            .expect("allocate");

        let total: Decimal = out
            .allocations
            .iter()
            .map(|a| a.allocated_usd.inner())
            .sum();
        assert!(total <= caps.total_budget_usd, "respects total budget");
        let crypto: Decimal = out
            .allocations
            .iter()
            .filter(|a| matches!(a.market_id.as_str(), "0xa" | "0xc"))
            .map(|a| a.allocated_usd.inner())
            .sum();
        assert!(crypto <= dec!(400), "respects category cap, got {crypto}");
        let thin = out
            .allocations
            .iter()
            .find(|a| a.market_id.as_str() == "0xb")
            .expect("c2 allocation");
        assert!(!thin.liquidity_feasible, "thin liquidity flagged");
        assert_eq!(thin.binding_constraint, BindingConstraint::LiquidityCap);
        assert!(thin.allocated_usd.inner() <= dec!(100));
    }

    #[test]
    fn exposure_after_includes_account_snapshot_positions() {
        let c1 = candidate("0xa", dec!(0.9));
        let mut initial = ExposureBreakdown::default();
        initial
            .per_market
            .insert(MarketId::new("0xa"), Usd::new(dec!(250)));
        initial
            .per_category
            .insert(MarketCategory::Crypto, Usd::new(dec!(250)));
        let caps = caps();
        let out = GreedyPortfolioAllocator::new()
            .allocate(&AllocationInput {
                candidates: vec![meta(&c1, MarketCategory::Crypto, None)],
                caps: &caps,
                initial_exposures: &initial,
                available_usd: caps.total_budget_usd,
            })
            .expect("allocate");
        let a = &out.allocations[0];
        // Market cap 300, already holding 250 → only 50 room left.
        assert_eq!(a.allocated_usd, Usd::new(dec!(50)));
        assert_eq!(a.market_exposure_after_usd, Usd::new(dec!(300)));
        assert_eq!(a.binding_constraint, BindingConstraint::SingleMarketCap);
    }

    #[test]
    fn available_cash_binds_below_budget() {
        let c1 = candidate("0xa", dec!(0.9));
        let caps = caps();
        let empty = ExposureBreakdown::default();
        let out = GreedyPortfolioAllocator::new()
            .allocate(&AllocationInput {
                candidates: vec![meta(&c1, MarketCategory::Crypto, None)],
                caps: &caps,
                initial_exposures: &empty,
                available_usd: dec!(120), // cash < desired 300 and < budget 1000
            })
            .expect("allocate");
        let a = &out.allocations[0];
        assert_eq!(a.allocated_usd, Usd::new(dec!(120)));
        assert_eq!(a.binding_constraint, BindingConstraint::AvailableCash);
    }
}
