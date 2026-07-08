//! Phase 04.1 acceptance tests for the governed portfolio planner.
//!
//! Covers the money-critical invariants: available-cash convergence, exposure
//! netting against the real account snapshot, stable ranking, min-size drops,
//! deterministic replay, and a stable risk-envelope hash.

use std::sync::Arc;

use chrono::{TimeZone, Utc};
use quant_pivot_models::{
    enums::{
        common::MarketCategory,
        quant::{
            AccountSource, BindingConstraint, OptimizerSolverStatus, OutcomeSide,
            PortfolioSolveMode, PortfolioSolverKind, RejectionReason,
        },
    },
    runtime_config::{ConfidenceSizeCurve, DrawdownMultiplierPolicy},
    types::{
        Bps, MarketId, MarketSelectionId, ModelRunId, PortfolioOptimizerMeta, PortfolioPlanId,
        PositionSnapshot, Price, Probability, Shares, SignalCandidateId, TokenId, Usd,
    },
};
use quant_pivot_research::{
    backtest::PortfolioCaps,
    model::signal::{ModelExplanation, SignalCandidate},
    portfolio::sizing::KellySafetyParams,
    portfolio::{
        AccountSnapshot, DefaultPortfolioPlanner, DrawdownState, KellySizingModel,
        LinearProgrammingPortfolioAllocator, OptimizerConfig, PlanCandidate, PortfolioPlanInput,
        PortfolioPlanner, SizingModel,
    },
};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;

/// The exact-MILP planner over the pure-Rust microlp backend used by the
/// Phase 04.1 acceptance tests (no expected-return tilt: `λ = 0`).
fn planner() -> DefaultPortfolioPlanner {
    let config = OptimizerConfig {
        solver: PortfolioSolverKind::Microlp,
        integer_inclusion: true,
        lambda: Decimal::ZERO,
    };
    DefaultPortfolioPlanner::new(Arc::new(LinearProgrammingPortfolioAllocator::new(config)))
}

/// Optimizer meta fields that must be identical across deterministic replays.
/// `elapsed_ms` is wall-clock observability and is excluded (same contract as
/// backtest `report_hash` omitting optimizer provenance).
fn assert_optimizer_meta_replay_equal(a: &PortfolioOptimizerMeta, b: &PortfolioOptimizerMeta) {
    assert_eq!(a.solver, b.solver);
    assert_eq!(a.solve_mode, b.solve_mode);
    assert_eq!(a.status, b.status);
    assert_eq!(a.fell_back_to_relaxation, b.fell_back_to_relaxation);
    assert_eq!(a.objective_value, b.objective_value);
    assert_eq!(a.correlation_source, b.correlation_source);
    assert_eq!(a.constraint_conflicts, b.constraint_conflicts);
}

fn candidate(
    market: &str,
    composite: Decimal,
    confidence: Decimal,
    expected_bps: Decimal,
    downside_bps: Decimal,
) -> SignalCandidate {
    SignalCandidate {
        signal_candidate_id: SignalCandidateId::from_v7(),
        model_run_id: ModelRunId::from_v7(),
        market_id: MarketId::new(market),
        token_id: TokenId::new("yes"),
        outcome_side: OutcomeSide::Yes,
        composite_score: Probability::new(composite),
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
        as_of: Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap(),
    }
}

const fn caps() -> PortfolioCaps {
    PortfolioCaps {
        total_budget_usd: dec!(10000),
        max_single_recommendation_usd: dec!(1000),
        min_recommendation_usd: dec!(50),
        max_market_exposure_usd: dec!(1000),
        max_event_exposure_usd: dec!(0),
        max_category_exposure_usd: dec!(5000),
        liquidity_usage_cap_pct: dec!(0.1),
        max_aggregate_exposure_pct: dec!(0),
    }
}

fn account(
    equity: Decimal,
    available: Decimal,
    reserved: Decimal,
    positions: Vec<PositionSnapshot>,
) -> AccountSnapshot {
    AccountSnapshot::new(
        Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap(),
        AccountSource::Polymarket,
        Usd::new(equity),
        Usd::new(equity),
        Usd::new(available),
        Usd::new(reserved),
        positions,
    )
}

const fn kelly() -> KellySizingModel {
    KellySizingModel::new(
        dec!(0.5),
        dec!(0.1),
        dec!(2),
        ConfidenceSizeCurve::Linear,
        DrawdownMultiplierPolicy::Fixed,
        KellySafetyParams::new(dec!(1), dec!(0.5), dec!(0.9)),
    )
}

const fn plan_candidate(
    candidate: &SignalCandidate,
    category: MarketCategory,
    liquidity: Option<Usd>,
) -> PlanCandidate<'_> {
    PlanCandidate {
        candidate,
        category,
        event_id: None,
        liquidity_usd: liquidity,
        liquidity_score: Probability::ZERO,
    }
}

fn input<'a>(
    candidates: Vec<PlanCandidate<'a>>,
    account: &'a AccountSnapshot,
    caps: &'a PortfolioCaps,
    sizing: &'a dyn SizingModel,
    top_n: usize,
) -> PortfolioPlanInput<'a> {
    PortfolioPlanInput {
        portfolio_plan_id: PortfolioPlanId::from_v7(),
        model_run_id: ModelRunId::from_v7(),
        market_selection_id: MarketSelectionId::from_v7(),
        as_of: Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap(),
        candidates,
        account,
        caps,
        max_correlated_exposure_usd: Usd::ZERO,
        correlation: None,
        drawdown_state: DrawdownState::neutral(),
        sizing,
        entry_max_slippage_bps: Bps::new(dec!(50)),
        top_n,
        calibration: None,
    }
}

fn position(market: &str, category: MarketCategory, value: Decimal) -> PositionSnapshot {
    PositionSnapshot {
        token_id: TokenId::new("yes"),
        market_id: MarketId::new(market),
        event_id: None,
        category,
        outcome: "Yes".to_owned(),
        size: Shares::new(dec!(100)),
        avg_price: Price::new(dec!(0.4)),
        cur_price: Price::new(dec!(0.5)),
        current_value: Usd::new(value),
        redeemable: false,
    }
}

#[test]
fn planner_total_room_is_min_budget_available() {
    // Equity is large, but available cash is the true ceiling on total deploy.
    let caps = caps();
    let acct = account(dec!(100000), dec!(700), Usd::ZERO.inner(), Vec::new());
    let c1 = candidate("0xa", dec!(0.9), dec!(1), dec!(200), dec!(100));
    let c2 = candidate("0xb", dec!(0.8), dec!(1), dec!(200), dec!(100));
    let model = kelly();
    let out = planner()
        .plan(input(
            vec![
                plan_candidate(&c1, MarketCategory::Crypto, None),
                plan_candidate(&c2, MarketCategory::Sports, None),
            ],
            &acct,
            &caps,
            &model,
            10,
        ))
        .expect("plan");
    let total: Usd = out.planned.iter().map(|p| p.sizing.suggested_usd).sum();
    assert!(
        total.inner() <= dec!(700),
        "total deploy bounded by available cash, got {total}"
    );
    assert_eq!(out.plan_row.allocated_usd, total);
}

#[test]
fn planner_respects_available_usd_after_reserved() {
    // Reserved capital shrinks available below the budget; the second candidate
    // is starved and rejected with the cash reason.
    let caps = caps();
    // available 900 after 100 reserved; each Kelly bet is capped at 1000 but cash
    // only funds the first ~900.
    let acct = account(dec!(100000), dec!(900), dec!(100), Vec::new());
    let c1 = candidate("0xa", dec!(0.9), dec!(1), dec!(500), dec!(100));
    let c2 = candidate("0xb", dec!(0.8), dec!(1), dec!(500), dec!(100));
    let model = kelly();
    let out = planner()
        .plan(input(
            vec![
                plan_candidate(&c1, MarketCategory::Crypto, None),
                plan_candidate(&c2, MarketCategory::Sports, None),
            ],
            &acct,
            &caps,
            &model,
            10,
        ))
        .expect("plan");
    let total: Usd = out.planned.iter().map(|p| p.sizing.suggested_usd).sum();
    assert!(
        total.inner() <= dec!(900),
        "bounded by available, got {total}"
    );
    assert!(
        out.rejected
            .iter()
            .any(|r| r.reason == RejectionReason::AvailableCashExhausted),
        "starved candidate must cite available cash: {:?}",
        out.rejected
    );
}

#[test]
fn exposure_after_includes_account_snapshot_positions() {
    // A 900-USD existing position in 0xa leaves only 100 of the 1000 market cap.
    let caps = caps();
    let acct = account(
        dec!(100000),
        dec!(100000),
        Usd::ZERO.inner(),
        vec![position("0xa", MarketCategory::Crypto, dec!(900))],
    );
    let c1 = candidate("0xa", dec!(0.9), dec!(1), dec!(500), dec!(100));
    let model = kelly();
    let out = planner()
        .plan(input(
            vec![plan_candidate(&c1, MarketCategory::Crypto, None)],
            &acct,
            &caps,
            &model,
            10,
        ))
        .expect("plan");
    let rec = &out.planned[0];
    assert_eq!(rec.sizing.suggested_usd, Usd::new(dec!(100)));
    assert_eq!(rec.sizing.market_exposure_after_usd, Usd::new(dec!(1000)));
    assert_eq!(
        rec.sizing.binding_constraint,
        BindingConstraint::SingleMarketCap
    );
}

#[test]
fn planner_with_real_account_holding_no_positions_is_deterministic() {
    // A brand-new real account (zero positions) is a valid state — no warnings,
    // fully deterministic.
    let caps = caps();
    let acct = account(dec!(5000), dec!(5000), Usd::ZERO.inner(), Vec::new());
    let c1 = candidate("0xa", dec!(0.9), dec!(1), dec!(200), dec!(100));
    let model = kelly();
    let run = || {
        planner()
            .plan(input(
                vec![plan_candidate(&c1, MarketCategory::Crypto, None)],
                &acct,
                &caps,
                &model,
                10,
            ))
            .expect("plan")
    };
    let a = run();
    let b = run();
    assert_eq!(a.planned, b.planned);
    assert_eq!(a.plan_row.allocated_usd, b.plan_row.allocated_usd);
    assert_eq!(a.planned.len(), 1);
}

#[test]
fn min_size_dropped_as_rejected() {
    // Tiny equity → Kelly capped at max_position_pct·equity = 0.1·100 = 10 < min 50.
    let caps = caps();
    let acct = account(dec!(100), dec!(100), Usd::ZERO.inner(), Vec::new());
    let c1 = candidate("0xa", dec!(0.9), dec!(1), dec!(200), dec!(100));
    let model = kelly();
    let out = planner()
        .plan(input(
            vec![plan_candidate(&c1, MarketCategory::Crypto, None)],
            &acct,
            &caps,
            &model,
            10,
        ))
        .expect("plan");
    assert!(out.planned.is_empty());
    assert_eq!(out.rejected.len(), 1);
    assert_eq!(out.rejected[0].reason, RejectionReason::BelowMinSize);
}

#[test]
fn planner_stable_sort_matches_spec() {
    // Equal risk-adjusted scores fall back to composite, then market id asc.
    let caps = caps();
    let acct = account(dec!(100000), dec!(100000), Usd::ZERO.inner(), Vec::new());
    let c_b = candidate("0xb", dec!(0.8), dec!(1), dec!(200), dec!(100));
    let c_a = candidate("0xa", dec!(0.8), dec!(1), dec!(200), dec!(100));
    let c_hi = candidate("0xc", dec!(0.95), dec!(1), dec!(200), dec!(100));
    let model = kelly();
    let out = planner()
        .plan(input(
            vec![
                plan_candidate(&c_b, MarketCategory::Crypto, None),
                plan_candidate(&c_a, MarketCategory::Sports, None),
                plan_candidate(&c_hi, MarketCategory::Politics, None),
            ],
            &acct,
            &caps,
            &model,
            10,
        ))
        .expect("plan");
    let order: Vec<&str> = out
        .planned
        .iter()
        .map(|p| p.candidate.market_id.as_str())
        .collect();
    // Highest score first; tie between 0xa/0xb broken by market id ascending.
    assert_eq!(order, vec!["0xc", "0xa", "0xb"]);
    assert_eq!(out.planned[0].rank, 1);
    assert_eq!(out.planned[2].rank, 3);
}

#[test]
fn risk_envelope_hash_stable() {
    let caps = caps();
    let acct = account(dec!(100000), dec!(100000), Usd::ZERO.inner(), Vec::new());
    let c1 = candidate("0xa", dec!(0.9), dec!(1), dec!(200), dec!(100));
    let model = kelly();
    let plan = || {
        planner()
            .plan(input(
                vec![plan_candidate(&c1, MarketCategory::Crypto, None)],
                &acct,
                &caps,
                &model,
                10,
            ))
            .expect("plan")
    };
    let a = plan();
    let b = plan();
    assert_eq!(
        a.planned[0].risk_envelope.envelope_hash,
        b.planned[0].risk_envelope.envelope_hash,
    );
    assert!(
        a.planned[0]
            .risk_envelope
            .envelope_hash
            .as_str()
            .starts_with("blake3:")
    );
    // Flags are excluded from the hash anchor.
    assert!(!a.planned[0].risk_envelope.requires_approval);
}

#[test]
fn planner_deterministic_replay() {
    let caps = caps();
    let acct = account(
        dec!(50000),
        dec!(50000),
        Usd::ZERO.inner(),
        vec![position("0xa", MarketCategory::Crypto, dec!(200))],
    );
    let c1 = candidate("0xa", dec!(0.9), dec!(0.8), dec!(200), dec!(100));
    let c2 = candidate("0xb", dec!(0.7), dec!(0.9), dec!(150), dec!(80));
    let model = kelly();
    let run = || {
        planner()
            .plan(input(
                vec![
                    plan_candidate(&c1, MarketCategory::Crypto, Some(Usd::new(dec!(100000)))),
                    plan_candidate(&c2, MarketCategory::Sports, Some(Usd::new(dec!(100000)))),
                ],
                &acct,
                &caps,
                &model,
                10,
            ))
            .expect("plan")
    };
    let a = run();
    let b = run();
    assert_eq!(a.planned, b.planned);
    assert_eq!(a.plan_row.allocated_usd, b.plan_row.allocated_usd);
    assert_eq!(a.plan_row.rejected_summary, b.plan_row.rejected_summary);
    assert_optimizer_meta_replay_equal(
        &a.plan_row.optimizer_meta_json,
        &b.plan_row.optimizer_meta_json,
    );
}

#[test]
fn optimizer_meta_recorded_on_plan_row() {
    let caps = caps();
    let acct = account(dec!(100000), dec!(100000), Usd::ZERO.inner(), Vec::new());
    let c1 = candidate("0xa", dec!(0.95), dec!(1), dec!(200), dec!(100));
    let model = kelly();
    let out = planner()
        .plan(input(
            vec![plan_candidate(&c1, MarketCategory::Crypto, None)],
            &acct,
            &caps,
            &model,
            10,
        ))
        .expect("plan");
    assert_eq!(
        out.plan_row.optimizer_meta_json.solver,
        PortfolioSolverKind::Microlp
    );
    assert_eq!(
        out.plan_row.optimizer_meta_json.solve_mode,
        PortfolioSolveMode::MilpExact
    );
    assert_eq!(
        out.plan_row.optimizer_meta_json.status,
        OptimizerSolverStatus::Optimal
    );
    assert!(!out.plan_row.optimizer_meta_json.fell_back_to_relaxation);
}

#[test]
fn lp_top_n_exclusion_is_rejected() {
    let caps = caps();
    let acct = account(dec!(100000), dec!(100000), Usd::ZERO.inner(), Vec::new());
    let c1 = candidate("0xa", dec!(0.95), dec!(1), dec!(200), dec!(100));
    let c2 = candidate("0xb", dec!(0.85), dec!(1), dec!(200), dec!(100));
    let model = kelly();
    let out = planner()
        .plan(input(
            vec![
                plan_candidate(&c1, MarketCategory::Crypto, None),
                plan_candidate(&c2, MarketCategory::Sports, None),
            ],
            &acct,
            &caps,
            &model,
            1,
        ))
        .expect("plan");
    assert_eq!(out.planned.len(), 1);
    assert_eq!(out.planned[0].candidate.market_id.as_str(), "0xa");
    assert!(
        out.rejected
            .iter()
            .any(|r| r.reason == RejectionReason::BeyondTopN && r.market_id.as_str() == "0xb")
    );
}

#[test]
fn planner_consumes_real_drawdown_state() {
    let caps = caps();
    let acct = account(dec!(10000), dec!(10000), Usd::ZERO.inner(), Vec::new());
    let candidate = candidate("0xa", dec!(0.9), dec!(1), dec!(200), dec!(100));
    let conservative = KellySizingModel::new(
        dec!(0.5),
        dec!(0.9),
        dec!(2),
        ConfidenceSizeCurve::Linear,
        DrawdownMultiplierPolicy::Conservative,
        KellySafetyParams::new(dec!(1), dec!(0.5), dec!(0.9)),
    );
    let mut neutral = input(
        vec![plan_candidate(
            &candidate,
            MarketCategory::Crypto,
            Some(Usd::new(dec!(50000))),
        )],
        &acct,
        &caps,
        &conservative,
        1,
    );
    neutral.drawdown_state = DrawdownState::neutral();
    let neutral_out = planner().plan(neutral).expect("neutral plan");

    let mut in_drawdown = input(
        vec![plan_candidate(
            &candidate,
            MarketCategory::Crypto,
            Some(Usd::new(dec!(50000))),
        )],
        &acct,
        &caps,
        &conservative,
        1,
    );
    in_drawdown.drawdown_state = DrawdownState {
        current_drawdown: dec!(0.2),
    };
    let drawdown_out = planner().plan(in_drawdown).expect("drawdown plan");

    assert_eq!(neutral_out.planned.len(), 1);
    assert_eq!(drawdown_out.planned.len(), 1);
    let neutral_kelly = neutral_out.planned[0]
        .sizing
        .kelly_fraction_applied
        .expect("kelly fraction");
    let drawdown_kelly = drawdown_out.planned[0]
        .sizing
        .kelly_fraction_applied
        .expect("kelly fraction");
    assert_eq!(drawdown_kelly, neutral_kelly * dec!(0.8));
}

#[test]
fn planner_deterministic_with_frozen_drawdown() {
    let caps = caps();
    let acct = account(dec!(10000), dec!(10000), Usd::ZERO.inner(), Vec::new());
    let candidate = candidate("0xa", dec!(0.85), dec!(1), dec!(200), dec!(100));
    let conservative = KellySizingModel::new(
        dec!(0.5),
        dec!(0.9),
        dec!(2),
        ConfidenceSizeCurve::Linear,
        DrawdownMultiplierPolicy::Conservative,
        KellySafetyParams::new(dec!(1), dec!(0.5), dec!(0.9)),
    );
    let mut plan_input = input(
        vec![plan_candidate(
            &candidate,
            MarketCategory::Crypto,
            Some(Usd::new(dec!(50000))),
        )],
        &acct,
        &caps,
        &conservative,
        1,
    );
    plan_input.drawdown_state = DrawdownState {
        current_drawdown: dec!(0.15),
    };

    let first = planner().plan(plan_input).expect("first plan");
    let mut replay_input = input(
        vec![plan_candidate(
            &candidate,
            MarketCategory::Crypto,
            Some(Usd::new(dec!(50000))),
        )],
        &acct,
        &caps,
        &conservative,
        1,
    );
    replay_input.drawdown_state = DrawdownState {
        current_drawdown: dec!(0.15),
    };
    let second = planner().plan(replay_input).expect("second plan");
    assert_eq!(first.planned.len(), second.planned.len());
    assert_eq!(
        first.planned[0].sizing.suggested_usd,
        second.planned[0].sizing.suggested_usd
    );
    assert_optimizer_meta_replay_equal(
        &first.plan_row.optimizer_meta_json,
        &second.plan_row.optimizer_meta_json,
    );
}
