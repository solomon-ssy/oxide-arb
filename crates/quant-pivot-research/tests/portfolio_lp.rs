//! Phase 05.8 acceptance tests for the `good_lp` LP/MILP portfolio allocator.
//!
//! Covers the money-critical invariants: every cap (budget / single / market /
//! event / category / correlation / `TopN`) is respected, budget is only consumed
//! by published names, the correlation cap binds clustered markets, the solve is
//! deterministic and money never leaks `f64`, the relaxation mode is feasible,
//! the expected-return tilt behaves, and `HiGHS` downgrades to microlp when its
//! native feature is not built (no native solver in the default build).

use std::collections::BTreeMap;

use chrono::Utc;
use quant_pivot_models::{
    enums::{
        common::MarketCategory,
        quant::{
            BindingConstraint, CorrelationSource, OutcomeSide, PortfolioSolveMode,
            PortfolioSolverKind,
        },
    },
    types::{
        EventId, ExposureBreakdown, MarketId, ModelRunId, Price, Probability, SignalCandidateId,
        TokenId, Usd,
    },
};
use quant_pivot_research::{
    backtest::PortfolioCaps,
    model::signal::{ModelExplanation, SignalCandidate},
    portfolio::{
        AllocationInput, CandidateMeta, CorrelationConstraint, LinearProgrammingPortfolioAllocator,
        OptimizerConfig, PortfolioAllocator,
    },
};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;

fn candidate(market: &str, composite: Decimal, expected_bps: Decimal) -> SignalCandidate {
    SignalCandidate {
        signal_candidate_id: SignalCandidateId::from_v7(),
        model_run_id: ModelRunId::from_v7(),
        market_id: MarketId::new(market),
        token_id: TokenId::new("yes"),
        outcome_side: OutcomeSide::Yes,
        composite_score: Probability::new(composite),
        confidence: Probability::new(dec!(1)),
        expected_return_bps: expected_bps,
        downside_bps: dec!(100),
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
        as_of: Utc::now(),
    }
}

fn meta<'a>(
    candidate: &'a SignalCandidate,
    desired: Decimal,
    category: MarketCategory,
    event: Option<&str>,
) -> CandidateMeta<'a> {
    CandidateMeta {
        candidate,
        desired_usd: Usd::new(desired),
        category,
        event_id: event.map(EventId::new),
        liquidity_usd: None,
    }
}

const fn caps(
    budget: Decimal,
    max_single: Decimal,
    max_market: Decimal,
    max_category: Decimal,
) -> PortfolioCaps {
    PortfolioCaps {
        total_budget_usd: budget,
        max_single_recommendation_usd: max_single,
        min_recommendation_usd: dec!(10),
        max_market_exposure_usd: max_market,
        max_event_exposure_usd: dec!(0),
        max_category_exposure_usd: max_category,
        liquidity_usage_cap_pct: dec!(0.1),
        max_aggregate_exposure_pct: dec!(0),
    }
}

const fn allocator(
    integer_inclusion: bool,
    lambda: Decimal,
) -> LinearProgrammingPortfolioAllocator {
    LinearProgrammingPortfolioAllocator::new(OptimizerConfig {
        solver: PortfolioSolverKind::Microlp,
        integer_inclusion,
        lambda,
    })
}

#[test]
fn lp_milp_respects_all_caps() {
    let c1 = candidate("0xa", dec!(0.9), dec!(100));
    let c2 = candidate("0xb", dec!(0.8), dec!(100));
    let c3 = candidate("0xc", dec!(0.7), dec!(100));
    let caps = caps(dec!(700), dec!(400), dec!(400), dec!(1000));
    let empty = ExposureBreakdown::default();
    let out = allocator(true, Decimal::ZERO)
        .allocate(&AllocationInput {
            candidates: vec![
                meta(&c1, dec!(1000), MarketCategory::Crypto, None),
                meta(&c2, dec!(1000), MarketCategory::Crypto, None),
                meta(&c3, dec!(1000), MarketCategory::Sports, None),
            ],
            caps: &caps,
            initial_exposures: &empty,
            available_usd: caps.total_budget_usd,
            capital_base_usd: caps.total_budget_usd,
            correlation: None,
            top_n: 10,
        })
        .expect("allocate");

    let total: Decimal = out
        .allocations
        .iter()
        .map(|a| a.allocated_usd.inner())
        .sum();
    assert!(total <= dec!(700), "total budget respected: {total}");
    for allocation in &out.allocations {
        assert!(
            allocation.allocated_usd.inner() <= dec!(400),
            "single-rec cap respected"
        );
        assert!(
            allocation.market_exposure_after_usd.inner() <= dec!(400),
            "per-market cap respected"
        );
    }
    let crypto: Decimal = out
        .allocations
        .iter()
        .filter(|a| matches!(a.market_id.as_str(), "0xa" | "0xb"))
        .map(|a| a.allocated_usd.inner())
        .sum();
    assert!(crypto <= dec!(1000), "category cap respected: {crypto}");
}

#[test]
fn lp_only_consumes_budget_on_published_names() {
    // Budget funds both, but TopN = 1 must publish only the best name and leave
    // the rest of the budget unspent (the core upgrade over greedy pre-spend).
    let c1 = candidate("0xa", dec!(0.9), dec!(100));
    let c2 = candidate("0xb", dec!(0.8), dec!(100));
    let caps = caps(dec!(10000), dec!(1000), dec!(1000), dec!(10000));
    let empty = ExposureBreakdown::default();
    let out = allocator(true, Decimal::ZERO)
        .allocate(&AllocationInput {
            candidates: vec![
                meta(&c1, dec!(1000), MarketCategory::Crypto, None),
                meta(&c2, dec!(1000), MarketCategory::Sports, None),
            ],
            caps: &caps,
            initial_exposures: &empty,
            available_usd: caps.total_budget_usd,
            capital_base_usd: caps.total_budget_usd,
            correlation: None,
            top_n: 1,
        })
        .expect("allocate");

    let funded = out
        .allocations
        .iter()
        .filter(|a| a.allocated_usd.inner() > Decimal::ZERO)
        .count();
    assert_eq!(funded, 1, "only the published name is funded");
    let total: Decimal = out
        .allocations
        .iter()
        .map(|a| a.allocated_usd.inner())
        .sum();
    assert!(
        total <= dec!(1000),
        "budget consumed only by published name"
    );
}

#[test]
fn correlation_cap_binds_clustered_markets() {
    let c1 = candidate("0xa", dec!(0.9), dec!(100));
    let c2 = candidate("0xb", dec!(0.8), dec!(100));
    let caps = caps(dec!(10000), dec!(1000), dec!(1000), dec!(10000));
    let empty = ExposureBreakdown::default();
    let constraint = CorrelationConstraint {
        clusters: vec![vec![MarketId::new("0xa"), MarketId::new("0xb")]],
        cluster_mean_rho: BTreeMap::new(),
        cap_usd: Usd::new(dec!(300)),
        source: CorrelationSource::Historical,
    };
    let out = allocator(true, Decimal::ZERO)
        .allocate(&AllocationInput {
            candidates: vec![
                meta(&c1, dec!(1000), MarketCategory::Crypto, None),
                meta(&c2, dec!(1000), MarketCategory::Sports, None),
            ],
            caps: &caps,
            initial_exposures: &empty,
            available_usd: caps.total_budget_usd,
            capital_base_usd: caps.total_budget_usd,
            correlation: Some(&constraint),
            top_n: 10,
        })
        .expect("allocate");

    let cluster_total: Decimal = out
        .allocations
        .iter()
        .map(|a| a.allocated_usd.inner())
        .sum();
    assert!(
        cluster_total <= dec!(300),
        "correlated-cluster exposure capped: {cluster_total}"
    );
    assert!(
        out.allocations
            .iter()
            .any(|a| a.binding_constraint == BindingConstraint::CorrelationCap),
        "a clustered allocation cites the correlation cap: {:?}",
        out.allocations
    );
}

#[test]
fn lp_output_is_deterministic_for_same_input() {
    let c1 = candidate("0xa", dec!(0.9), dec!(120));
    let c2 = candidate("0xb", dec!(0.85), dec!(80));
    let caps = caps(dec!(1500), dec!(1000), dec!(1000), dec!(5000));
    let empty = ExposureBreakdown::default();
    let run = || {
        allocator(true, dec!(0.5))
            .allocate(&AllocationInput {
                candidates: vec![
                    meta(&c1, dec!(1000), MarketCategory::Crypto, None),
                    meta(&c2, dec!(1000), MarketCategory::Sports, None),
                ],
                caps: &caps,
                initial_exposures: &empty,
                available_usd: caps.total_budget_usd,
                capital_base_usd: caps.total_budget_usd,
                correlation: None,
                top_n: 10,
            })
            .expect("allocate")
    };
    assert_eq!(run().allocations, run().allocations);
}

#[test]
fn lp_money_is_rounded_no_f64_leak() {
    let c1 = candidate("0xa", dec!(0.9), dec!(137));
    let caps = caps(dec!(333), dec!(1000), dec!(1000), dec!(5000));
    let empty = ExposureBreakdown::default();
    let out = allocator(true, dec!(0.3))
        .allocate(&AllocationInput {
            candidates: vec![meta(&c1, dec!(1000), MarketCategory::Crypto, None)],
            caps: &caps,
            initial_exposures: &empty,
            available_usd: caps.total_budget_usd,
            capital_base_usd: caps.total_budget_usd,
            correlation: None,
            top_n: 10,
        })
        .expect("allocate");
    for allocation in &out.allocations {
        let value = allocation.allocated_usd.inner();
        assert_eq!(value, value.round_dp(8), "money stays on the rounded scale");
    }
}

#[test]
fn relaxation_mode_is_feasible_and_labeled() {
    let c1 = candidate("0xa", dec!(0.9), dec!(100));
    let c2 = candidate("0xb", dec!(0.8), dec!(100));
    let caps = caps(dec!(700), dec!(400), dec!(1000), dec!(5000));
    let empty = ExposureBreakdown::default();
    let out = allocator(false, Decimal::ZERO)
        .allocate(&AllocationInput {
            candidates: vec![
                meta(&c1, dec!(1000), MarketCategory::Crypto, None),
                meta(&c2, dec!(1000), MarketCategory::Sports, None),
            ],
            caps: &caps,
            initial_exposures: &empty,
            available_usd: caps.total_budget_usd,
            capital_base_usd: caps.total_budget_usd,
            correlation: None,
            top_n: 10,
        })
        .expect("allocate");
    assert_eq!(
        out.outcome.solve_mode,
        PortfolioSolveMode::ContinuousRelaxation
    );
    let total: Decimal = out
        .allocations
        .iter()
        .map(|a| a.allocated_usd.inner())
        .sum();
    assert!(total <= dec!(700), "relaxation respects budget: {total}");
}

#[test]
fn lambda_tilts_capital_toward_higher_expected_return() {
    // Two equal-conviction names; scarce budget. λ = 0 splits by canonical order;
    // λ > 0 tilts capital to the higher expected-return name.
    let high = candidate("0xa", dec!(0.8), dec!(400));
    let low = candidate("0xb", dec!(0.8), dec!(50));
    let caps = caps(dec!(500), dec!(1000), dec!(1000), dec!(5000));
    let empty = ExposureBreakdown::default();
    let alloc_for = |lambda: Decimal, market: &str| {
        let out = allocator(true, lambda)
            .allocate(&AllocationInput {
                candidates: vec![
                    meta(&high, dec!(1000), MarketCategory::Crypto, None),
                    meta(&low, dec!(1000), MarketCategory::Sports, None),
                ],
                caps: &caps,
                initial_exposures: &empty,
                available_usd: caps.total_budget_usd,
                capital_base_usd: caps.total_budget_usd,
                correlation: None,
                top_n: 10,
            })
            .expect("allocate");
        out.allocations
            .iter()
            .find(|a| a.market_id.as_str() == market)
            .map(|a| a.allocated_usd.inner())
            .unwrap_or_default()
    };
    let tilted = alloc_for(dec!(1), "0xa");
    let neutral = alloc_for(Decimal::ZERO, "0xa");
    assert!(
        tilted >= neutral,
        "edge tilt does not reduce the high-return allocation: {tilted} vs {neutral}"
    );
}

#[test]
fn highs_request_downgrades_to_microlp_without_native_feature() {
    // The default build must never link the native HiGHS backend: a HiGHS
    // request transparently resolves to the pure-Rust microlp solver.
    let config = OptimizerConfig {
        solver: PortfolioSolverKind::Highs,
        integer_inclusion: true,
        lambda: Decimal::ZERO,
    };
    #[cfg(not(feature = "lp-solver-highs"))]
    assert_eq!(config.effective_solver(), PortfolioSolverKind::Microlp);
    #[cfg(feature = "lp-solver-highs")]
    assert_eq!(config.effective_solver(), PortfolioSolverKind::Highs);
}

#[test]
fn lp_milp_falls_back_to_relaxation() {
    use quant_pivot_models::enums::quant::OptimizerSolverStatus;
    use quant_pivot_research::portfolio::debug_test_hooks::{
        self, Guard, MilpBehavior, RelaxBehavior,
    };

    let _guard = Guard::new();
    debug_test_hooks::set_milp(MilpBehavior::Panic);
    debug_test_hooks::set_relax(RelaxBehavior::Normal);

    let c1 = candidate("0xa", dec!(0.9), dec!(100));
    let c2 = candidate("0xb", dec!(0.8), dec!(100));
    let caps = caps(dec!(700), dec!(400), dec!(1000), dec!(5000));
    let empty = ExposureBreakdown::default();
    let out = allocator(true, Decimal::ZERO)
        .allocate(&AllocationInput {
            candidates: vec![
                meta(&c1, dec!(1000), MarketCategory::Crypto, None),
                meta(&c2, dec!(1000), MarketCategory::Sports, None),
            ],
            caps: &caps,
            initial_exposures: &empty,
            available_usd: caps.total_budget_usd,
            capital_base_usd: caps.total_budget_usd,
            correlation: None,
            top_n: 10,
        })
        .expect("allocate must not fail after MILP panic");

    assert!(
        out.outcome.fell_back_to_relaxation,
        "MILP failure must fall back to relaxation"
    );
    assert_eq!(
        out.outcome.status,
        OptimizerSolverStatus::FellBackRelaxation
    );
    assert_eq!(
        out.outcome.solve_mode,
        PortfolioSolveMode::ContinuousRelaxation
    );
    let total: Decimal = out
        .allocations
        .iter()
        .map(|a| a.allocated_usd.inner())
        .sum();
    assert!(
        total > Decimal::ZERO && total <= dec!(700),
        "relaxation fallback must yield a feasible non-empty plan: {total}"
    );
}

#[test]
fn solver_failure_yields_empty_plan_not_panic() {
    use quant_pivot_models::enums::quant::OptimizerSolverStatus;
    use quant_pivot_research::portfolio::debug_test_hooks::{
        self, Guard, MilpBehavior, RelaxBehavior,
    };

    let _guard = Guard::new();
    debug_test_hooks::set_milp(MilpBehavior::FailInfeasible);
    debug_test_hooks::set_relax(RelaxBehavior::Panic);

    let c1 = candidate("0xa", dec!(0.9), dec!(100));
    let caps = caps(dec!(500), dec!(1000), dec!(1000), dec!(5000));
    let empty = ExposureBreakdown::default();
    let out = allocator(true, Decimal::ZERO)
        .allocate(&AllocationInput {
            candidates: vec![meta(&c1, dec!(1000), MarketCategory::Crypto, None)],
            caps: &caps,
            initial_exposures: &empty,
            available_usd: caps.total_budget_usd,
            capital_base_usd: caps.total_budget_usd,
            correlation: None,
            top_n: 10,
        })
        .expect("allocate must return Ok even when both solvers fail");

    assert_eq!(out.outcome.status, OptimizerSolverStatus::SolverUnavailable);
    assert!(out.outcome.fell_back_to_relaxation);
    assert!(
        out.allocations.iter().all(|a| a.allocated_usd.is_zero()),
        "ultimate fallback is an all-zero plan"
    );
    assert!(
        !out.outcome.constraint_conflicts.is_empty(),
        "conflicts must be recorded for observability"
    );
}

#[test]
fn aggregate_exposure_never_exceeds_cap() {
    let c1 = candidate("0xa", dec!(0.9), dec!(100));
    let c2 = candidate("0xb", dec!(0.85), dec!(100));
    let c3 = candidate("0xc", dec!(0.8), dec!(100));
    let mut caps = caps(dec!(1000), dec!(500), dec!(500), dec!(2000));
    caps.max_aggregate_exposure_pct = dec!(0.25);
    let capital_base_usd = caps.total_budget_usd;
    let aggregate_cap = caps.max_aggregate_exposure_pct * capital_base_usd;
    let empty = ExposureBreakdown::default();
    let out = allocator(true, Decimal::ZERO)
        .allocate(&AllocationInput {
            candidates: vec![
                meta(&c1, dec!(400), MarketCategory::Crypto, None),
                meta(&c2, dec!(400), MarketCategory::Sports, None),
                meta(&c3, dec!(400), MarketCategory::Politics, None),
            ],
            caps: &caps,
            initial_exposures: &empty,
            available_usd: caps.total_budget_usd,
            capital_base_usd,
            correlation: None,
            top_n: 10,
        })
        .expect("allocate");
    let total: Decimal = out
        .allocations
        .iter()
        .map(|a| a.allocated_usd.inner())
        .sum();
    assert!(
        total <= aggregate_cap,
        "aggregate exposure {total} must not exceed cap {aggregate_cap}"
    );
    if total > Decimal::ZERO {
        assert!(
            out.allocations
                .iter()
                .any(|a| a.binding_constraint == BindingConstraint::AggregateExposureCap),
            "binding must attribute aggregate cap when it limits allocation"
        );
    }
}

#[test]
fn aggregate_exposure_with_existing_holdings_attributes_binding_correctly() {
    // The account already holds $800 net exposure; the aggregate cap is
    // $1,000 (25% of a $4,000 capital base). Only $200 of headroom remains,
    // so a $1,000 desired allocation must be capped down to (near) $200 and
    // the binding must cite `AggregateExposureCap` — not `None` or another
    // cap — proving `ExposureLedger::seed` folds existing holdings into
    // `total` (Phase 11.3 P1-6).
    let c1 = candidate("0xa", dec!(0.9), dec!(100));
    let mut caps = caps(dec!(10_000), dec!(5_000), dec!(5_000), dec!(10_000));
    caps.max_aggregate_exposure_pct = dec!(0.25);
    let capital_base_usd = dec!(4_000);
    let aggregate_cap = caps.max_aggregate_exposure_pct * capital_base_usd;
    let held = dec!(800);
    let mut initial_exposures = ExposureBreakdown::default();
    initial_exposures
        .per_market
        .insert(MarketId::new("0xheld"), Usd::new(held));
    let out = allocator(true, Decimal::ZERO)
        .allocate(&AllocationInput {
            candidates: vec![meta(&c1, dec!(1_000), MarketCategory::Crypto, None)],
            caps: &caps,
            initial_exposures: &initial_exposures,
            available_usd: caps.total_budget_usd,
            capital_base_usd,
            correlation: None,
            top_n: 10,
        })
        .expect("allocate");
    let total: Decimal = out
        .allocations
        .iter()
        .map(|a| a.allocated_usd.inner())
        .sum();
    let remaining_headroom = aggregate_cap - held;
    assert!(
        total <= remaining_headroom,
        "new allocation {total} must respect remaining aggregate headroom {remaining_headroom}"
    );
    assert!(
        out.allocations
            .iter()
            .any(|a| a.binding_constraint == BindingConstraint::AggregateExposureCap),
        "binding must attribute AggregateExposureCap when existing holdings exhaust headroom: {:?}",
        out.allocations
    );
}

#[test]
fn aggregate_exposure_denominator_is_capital_base_not_total_budget() {
    // `total_budget_usd` (a raw, venue-unaware config value) is set far above
    // `capital_base_usd` (the governed, venue-net-liquidation-capped sizing
    // anchor) — the aggregate cap must track `capital_base_usd`, never the
    // larger, ungoverned config budget (Phase 11.3 §4.3 / P1-5).
    let c1 = candidate("0xa", dec!(0.9), dec!(100));
    let c2 = candidate("0xb", dec!(0.85), dec!(100));
    let mut caps = caps(dec!(100_000), dec!(50_000), dec!(50_000), dec!(100_000));
    caps.max_aggregate_exposure_pct = dec!(0.25);
    let capital_base_usd = dec!(1_000);
    let aggregate_cap_from_capital_base = caps.max_aggregate_exposure_pct * capital_base_usd;
    let aggregate_cap_from_total_budget = caps.max_aggregate_exposure_pct * caps.total_budget_usd;
    assert!(
        aggregate_cap_from_capital_base < aggregate_cap_from_total_budget,
        "fixture must set total_budget_usd far above capital_base_usd"
    );
    let empty = ExposureBreakdown::default();
    let out = allocator(true, Decimal::ZERO)
        .allocate(&AllocationInput {
            candidates: vec![
                meta(&c1, dec!(2_000), MarketCategory::Crypto, None),
                meta(&c2, dec!(2_000), MarketCategory::Sports, None),
            ],
            caps: &caps,
            initial_exposures: &empty,
            available_usd: caps.total_budget_usd,
            capital_base_usd,
            correlation: None,
            top_n: 10,
        })
        .expect("allocate");
    let total: Decimal = out
        .allocations
        .iter()
        .map(|a| a.allocated_usd.inner())
        .sum();
    assert!(
        total <= aggregate_cap_from_capital_base,
        "total {total} must respect the capital_base_usd-denominated cap \
         {aggregate_cap_from_capital_base}, not the much larger \
         total_budget_usd-denominated cap {aggregate_cap_from_total_budget}"
    );
}
