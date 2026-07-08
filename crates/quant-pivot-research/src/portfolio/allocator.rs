//! Portfolio allocation contract shared by the planner and the backtest plane.
//!
//! Capital allocation is a single LP/MILP code path
//! ([`crate::portfolio::lp::LinearProgrammingPortfolioAllocator`]) — there is no
//! greedy allocator. This module owns the allocation IO contract
//! ([`AllocationInput`] / [`AllocationOutput`] / [`Allocation`]) plus the
//! deterministic *binding-constraint attribution* helper ([`decide_ceiling`]),
//! which the optimizer reuses after solving to label each funded candidate with
//! the single [`BindingConstraint`] that limited its size — keeping report
//! fields identical regardless of the solve path.
//!
//! Cap room starts from the account's *current* exposure
//! ([`AllocationInput::initial_exposures`]); a candidate's room is therefore
//! `cap − already-held − others-allocated`, and the reported
//! `*_exposure_after_usd` is the projected post-allocation net (existing + new).

use std::collections::BTreeMap;

use quant_pivot_error::QuantResult;
use quant_pivot_models::{
    enums::{common::MarketCategory, quant::BindingConstraint},
    types::{EventId, ExposureBreakdown, MarketId, SignalCandidateId, Usd},
};
use rust_decimal::Decimal;

use crate::{
    backtest::PortfolioCaps, model::signal::SignalCandidate,
    portfolio::correlation::CorrelationConstraint, portfolio::optimizer::OptimizerOutcome,
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
    /// The governed sizing anchor (`min(venue_net_liquidation_usd, budget
    /// cap)` — see `portfolio::account::AccountSnapshot`), the denominator
    /// for `caps.max_aggregate_exposure_pct`. Kept a distinct field from
    /// `caps.total_budget_usd` (a raw, venue-unaware config value): the
    /// aggregate-exposure hard cap must use the *same* capital basis as every
    /// other Kelly-safety mechanism, or a configured budget above the venue's
    /// real equity would let the cap exceed the account's actual bankroll.
    pub capital_base_usd: Decimal,
    /// Correlated-cluster exposure constraint, when correlation is enabled.
    /// `None` ⇒ the correlation cap does not bind (Phase 4 behavior).
    pub correlation: Option<&'a CorrelationConstraint>,
    /// Maximum published recommendations (`TopN` inclusion cardinality).
    pub top_n: usize,
}

/// Allocation result: one entry per input candidate plus solver provenance.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AllocationOutput {
    /// One entry per input candidate (including zero-funded ones).
    pub allocations: Vec<Allocation>,
    /// Which solve path produced this allocation (observability).
    pub outcome: OptimizerOutcome,
}

/// Portfolio allocation strategy. The governed planner and the backtest plane
/// both consume this single contract.
pub trait PortfolioAllocator: Send + Sync {
    /// Allocate capital across the candidates subject to the caps.
    fn allocate(&self, input: &AllocationInput<'_>) -> QuantResult<AllocationOutput>;
}

/// Treat a non-positive per-bucket cap as unlimited.
pub(crate) fn bucket_cap(value: Decimal) -> Decimal {
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
pub(crate) struct BudgetRoom {
    pub(crate) effective: Decimal,
    pub(crate) constraint: BindingConstraint,
}

impl BudgetRoom {
    pub(crate) fn resolve(total_budget_usd: Decimal, available_usd: Decimal) -> Self {
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

/// The per-candidate cap-room context for binding attribution, evaluated at the
/// solved allocation (each `*_held` already folds in the other candidates'
/// allocated USD, so the ceilings are "how much more could this candidate have
/// received").
pub(crate) struct CeilingInputs<'a> {
    pub(crate) meta: &'a CandidateMeta<'a>,
    pub(crate) caps: &'a PortfolioCaps,
    pub(crate) budget: &'a BudgetRoom,
    pub(crate) spent_total: Decimal,
    pub(crate) market_held: Decimal,
    pub(crate) event_held: Decimal,
    pub(crate) category_held: Decimal,
    /// Held + others-in-cluster, when the candidate is in a multi-market cluster.
    pub(crate) cluster_held: Option<Decimal>,
    /// Correlated-cluster cap (only meaningful when `cluster_held` is `Some`).
    pub(crate) correlated_cap: Decimal,
    /// Total portfolio exposure held by other candidates (aggregate cap room).
    pub(crate) aggregate_held: Decimal,
    /// The governed capital base (`AllocationInput::capital_base_usd`) — the
    /// aggregate-exposure ceiling's denominator, kept identical to the LP
    /// solver's own bucket constraint (`lp.rs::build_buckets`) so this
    /// explanatory re-derivation never disagrees with what was actually
    /// solved.
    pub(crate) capital_base_usd: Decimal,
}

/// The chosen pre-min size, its binding cap, and liquidity feasibility.
pub(crate) struct SizeDecision {
    pub(crate) alloc_pre: Decimal,
    pub(crate) binding: BindingConstraint,
    pub(crate) liquidity_feasible: bool,
}

/// Pick the binding ceiling for one candidate given the room left in each cap.
///
/// The model's own desired size is considered first so that on ties an external
/// cap is named the limiter; it only "wins" when strictly smallest. Reused by
/// the optimizer to attribute a deterministic [`BindingConstraint`] to each
/// funded candidate after solving.
pub(crate) fn decide_ceiling(input: &CeilingInputs<'_>) -> SizeDecision {
    let meta = input.meta;
    let caps = input.caps;
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
            value: (input.budget.effective - input.spent_total).max(Decimal::ZERO),
            constraint: input.budget.constraint,
        },
        Ceiling {
            value: (bucket_cap(caps.max_market_exposure_usd) - input.market_held)
                .max(Decimal::ZERO),
            constraint: BindingConstraint::SingleMarketCap,
        },
        Ceiling {
            value: (bucket_cap(caps.max_event_exposure_usd) - input.event_held).max(Decimal::ZERO),
            constraint: BindingConstraint::EventCap,
        },
        Ceiling {
            value: (bucket_cap(caps.max_category_exposure_usd) - input.category_held)
                .max(Decimal::ZERO),
            constraint: BindingConstraint::CategoryCap,
        },
    ];
    if liquidity_known {
        ceilings.push(Ceiling {
            value: liquidity_room,
            constraint: BindingConstraint::LiquidityCap,
        });
    }
    if let Some(cluster_held) = input.cluster_held {
        ceilings.push(Ceiling {
            value: (bucket_cap(input.correlated_cap) - cluster_held).max(Decimal::ZERO),
            constraint: BindingConstraint::CorrelationCap,
        });
    }
    if caps.max_aggregate_exposure_pct > Decimal::ZERO && input.capital_base_usd > Decimal::ZERO {
        let aggregate_cap =
            (caps.max_aggregate_exposure_pct * input.capital_base_usd).max(Decimal::ZERO);
        ceilings.push(Ceiling {
            value: (aggregate_cap - input.aggregate_held).max(Decimal::ZERO),
            constraint: BindingConstraint::AggregateExposureCap,
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

/// Running per-bucket exposure net, seeded from the account's current exposure
/// and indexed by cluster for the correlation cap. Built once from a solved
/// allocation set, then queried per candidate for binding attribution.
pub(crate) struct ExposureLedger {
    pub(crate) total: Decimal,
    pub(crate) market: BTreeMap<String, Decimal>,
    pub(crate) event: BTreeMap<String, Decimal>,
    pub(crate) category: BTreeMap<MarketCategory, Decimal>,
    pub(crate) cluster: BTreeMap<usize, Decimal>,
}

impl ExposureLedger {
    /// Seed from the account's current net exposure (zero round spend).
    ///
    /// `total` is the sum of `per_market` (every position contributes there —
    /// see `ExposureBreakdown::from_positions`), matching the aggregate-cap
    /// `held` computation in `lp.rs`. Leaving it at `ZERO` here (as opposed to
    /// every other bucket, which *is* correctly seeded) previously made
    /// `AggregateExposureCap` binding attribution blind to existing holdings —
    /// the LP's own hard constraint was never affected (it sums
    /// `initial_exposures` independently), but the human-readable "why was
    /// this capped" binding could misattribute or omit the aggregate cap
    /// whenever the account already held positions.
    pub(crate) fn seed(exposures: &ExposureBreakdown) -> Self {
        let total = exposures.per_market.values().map(|usd| usd.inner()).sum();
        Self {
            total,
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
            cluster: BTreeMap::new(),
        }
    }

    pub(crate) fn market_held(&self, key: &str) -> Decimal {
        self.market.get(key).copied().unwrap_or(Decimal::ZERO)
    }

    pub(crate) fn event_held(&self, key: &str) -> Decimal {
        self.event.get(key).copied().unwrap_or(Decimal::ZERO)
    }

    pub(crate) fn category_held(&self, category: MarketCategory) -> Decimal {
        self.category
            .get(&category)
            .copied()
            .unwrap_or(Decimal::ZERO)
    }

    pub(crate) fn cluster_held(&self, cluster: usize) -> Decimal {
        self.cluster.get(&cluster).copied().unwrap_or(Decimal::ZERO)
    }

    /// Fold one funded allocation into every bucket it touches.
    pub(crate) fn add(
        &mut self,
        market_key: &str,
        event_key: &str,
        category: MarketCategory,
        cluster: Option<usize>,
        amount: Decimal,
    ) {
        self.total += amount;
        *self
            .market
            .entry(market_key.to_owned())
            .or_insert(Decimal::ZERO) += amount;
        *self
            .event
            .entry(event_key.to_owned())
            .or_insert(Decimal::ZERO) += amount;
        *self.category.entry(category).or_insert(Decimal::ZERO) += amount;
        if let Some(cluster) = cluster {
            *self.cluster.entry(cluster).or_insert(Decimal::ZERO) += amount;
        }
    }
}
