//! Full `RiskEngine` integration tests.
//!
//! Uses a `MockMetrics` implementation to exercise the engine's pre-trade,
//! post-trade, tick, halt/resume lifecycle.

use chrono::Utc;
use oxide_arb_models::config::RiskConfig;
use oxide_arb_models::domain::calibration::{BucketKey, CalibrationSnapshot};
use oxide_arb_models::domain::opportunity::{EndgameMeta, Opportunity};
use oxide_arb_models::domain::risk::ProbabilityInput;
use oxide_arb_models::domain::trade::PostTradeInput;
use oxide_arb_models::enums::calibration::{DurationBucket, PriceZone};
use oxide_arb_models::enums::common::{MarketCategory, Side, StalenessLevel, TradeOutcome};
use oxide_arb_models::enums::opportunity::PayoutModel;
use oxide_arb_models::enums::risk::TradeAccountingPhase;
use oxide_arb_models::types::{
    Bps, EventId, MarketId, OpportunityId, Price, Shares, TokenId, TradeId, Usd,
};
use oxide_arb_risk::builder::RiskEngineBuilder;
use oxide_arb_risk::clock::utc_clock;
use oxide_arb_risk::engine::RiskEngine;
use oxide_arb_risk::traits::RiskMetrics;
use oxide_arb_risk::types::{ExecutionRiskEvent, ReportMode};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use std::sync::Arc;

mod support;
use support::MockMetrics;

// ── Test Helpers ───────────────────────────────────────────────────────────

fn test_opportunity() -> Opportunity {
    Opportunity {
        opportunity_id: OpportunityId::new_v7(),
        market_id: MarketId::new("0xtest_market"),
        event_id: EventId::new("test_event"),
        token_id: TokenId::new("12345"),
        side: Side::Buy,
        payout_model: PayoutModel::DirectionalSettlement {
            projected_payout_if_correct: Usd::new(dec!(100)),
            expected_payout: Usd::new(dec!(95)),
            predicted_side: Side::Buy,
        },
        shares: Shares::new(dec!(100)),
        entry_price: Price::new(dec!(0.92)),
        total_cost: Usd::new(dec!(20)),
        total_fees: Usd::new(dec!(0.40)),
        net_profit: Usd::new(dec!(5)),
        expected_net_profit: Usd::new(dec!(4.5)),
        edge_bps: Bps::new(dec!(300)),
        resolution_adjust: dec!(0.95),
        depth_used_pct: dec!(10),
        staleness: StalenessLevel::Fresh,
        category: MarketCategory::Politics,
        meta: EndgameMeta {
            predicted_yes: true,
            confidence: dec!(0.95),
            convergence_duration_secs: 600,
            price_zone: PriceZone::Z97,
            duration_bucket: DurationBucket::Medium,
            settlement_deadline: None,
        },
        calibration: CalibrationSnapshot {
            bucket_key: BucketKey {
                category: MarketCategory::Politics,
                price_zone: PriceZone::Z97,
                duration_bucket: DurationBucket::Medium,
            },
            posterior_mean: dec!(0.93),
            sample_size: 50,
            alpha_prior: dec!(2.0),
            beta_prior: dec!(1.0),
            fallback_tier: 1,
            fused_probability: dec!(0.95),
        },
        detected_at: Utc::now(),
    }
}

const fn test_probability() -> ProbabilityInput {
    ProbabilityInput {
        calibrated_win_prob: dec!(0.99),
        fill_prob: dec!(0.99),
        calibration_confidence: dec!(0.99),
        sample_size: 100,
        model_staleness_secs: 0,
        expected_slippage_pct: dec!(0.001),
        expected_failure_cost_pct: dec!(0.001),
    }
}

fn test_trade_record(outcome: TradeOutcome, profit: Decimal) -> PostTradeInput {
    PostTradeInput {
        trade_id: TradeId::generate(),
        market_id: MarketId::new("0xtest_market"),
        token_id: TokenId::new("test_token"),
        outcome,
        cost_usd: Usd::new(dec!(20)),
        fee_usd: Usd::new(dec!(0.40)),
        net_profit_usd: Some(Usd::new(profit)),
    }
}

fn build_engine(metrics: &dyn RiskMetrics) -> RiskEngine {
    let config = RiskConfig {
        max_total_exposure_usd: dec!(5000),
        max_single_market_exposure_usd: dec!(500),
        max_single_bet_usd: dec!(25),
        max_open_positions: 5,
        max_daily_loss_usd: dec!(75),
        max_weekly_loss_usd: dec!(120),
        daily_budget_usd: dec!(200),
        min_balance_usd: dec!(50),
        reserve_balance_usd: dec!(100),
        min_trade_usd: dec!(1),
        max_consecutive_misses: 3,
        ..RiskConfig::default()
    };

    RiskEngineBuilder::new()
        .config(config)
        .clock(utc_clock())
        .initial_equity(Usd::new(dec!(5000)))
        .build(metrics)
        .expect("engine should build")
}

// ── Healthy engine allows trade ────────────────────────────────────────────

#[tokio::test]
async fn healthy_engine_allows_trade() {
    let metrics = MockMetrics::default();
    let engine = build_engine(&metrics);

    let opp = Arc::new(test_opportunity());
    let prob = test_probability();
    let decision =
        engine.pre_trade_check_core(opp.as_ref(), &prob, &metrics, ReportMode::ShortCircuit);

    assert!(
        decision.allowed,
        "healthy engine should allow: {:?}",
        decision.denial_reason
    );
    assert!(decision.recommended_size.is_some());
}

// ── Halted engine denies trade ─────────────────────────────────────────────

#[tokio::test]
async fn halted_engine_denies_trade() {
    let metrics = MockMetrics::default();
    let engine = build_engine(&metrics);
    engine.halt("manual halt for test".into()).await;

    let opp = Arc::new(test_opportunity());
    let prob = test_probability();
    let decision =
        engine.pre_trade_check_core(opp.as_ref(), &prob, &metrics, ReportMode::ShortCircuit);

    assert!(!decision.allowed);
    assert!(
        decision
            .denial_reason
            .as_deref()
            .unwrap()
            .contains("ManualHalt")
    );
}

// ── Tripped breaker denies trade ───────────────────────────────────────────

#[tokio::test]
async fn tripped_breaker_denies_trade() {
    let metrics = MockMetrics {
        consecutive_misses: 5, // above max_consecutive_misses=3
        ..MockMetrics::default()
    };
    let engine = build_engine(&metrics);

    let trade = test_trade_record(TradeOutcome::Miss, dec!(-5));
    engine
        .on_trade_result(TradeAccountingPhase::Settlement, &trade, &metrics)
        .await
        .unwrap();

    let opp = Arc::new(test_opportunity());
    let prob = test_probability();
    let decision =
        engine.pre_trade_check_core(opp.as_ref(), &prob, &metrics, ReportMode::ShortCircuit);

    assert!(!decision.allowed);
    assert!(
        decision
            .denial_reason
            .as_deref()
            .unwrap()
            .contains("CircuitBreaker")
    );
}

// ── Low balance denies trade ───────────────────────────────────────────────

#[tokio::test]
async fn low_balance_denies_trade() {
    let metrics = MockMetrics {
        balance: Usd::new(dec!(10)), // below min_balance_usd=50
        ..MockMetrics::default()
    };
    let engine = build_engine(&metrics);

    let opp = Arc::new(test_opportunity());
    let prob = test_probability();
    let decision =
        engine.pre_trade_check_core(opp.as_ref(), &prob, &metrics, ReportMode::ShortCircuit);

    assert!(!decision.allowed);
}

// ── Full report runs all checks ────────────────────────────────────────────

#[tokio::test]
async fn full_report_runs_all_checks() {
    let metrics = MockMetrics::default();
    let engine = build_engine(&metrics);
    engine.halt("halt for full report test".into()).await;

    let opp = Arc::new(test_opportunity());
    let prob = test_probability();
    let decision =
        engine.pre_trade_check_core(opp.as_ref(), &prob, &metrics, ReportMode::FullReport);

    assert!(!decision.allowed);
    assert!(
        decision.checks().len() >= 24,
        "full report should have all checks, got {}",
        decision.checks().len()
    );
}

// ── on_trade_result updates accounting ─────────────────────────────────────

#[tokio::test]
async fn on_trade_result_updates_accounting() {
    let metrics = MockMetrics::default();
    let engine = build_engine(&metrics);

    let trade = test_trade_record(TradeOutcome::Success, dec!(5));
    let report = engine
        .on_trade_result(TradeAccountingPhase::Fill, &trade, &metrics)
        .await
        .unwrap();

    assert_eq!(report.snapshot.daily_trade_count, 1);
    assert_eq!(report.snapshot.daily_success_count, 1);
}

// ── tick drives breaker state transitions ──────────────────────────────────

#[tokio::test]
async fn tick_drives_breaker_transitions() {
    let metrics = MockMetrics {
        consecutive_misses: 5,
        ..MockMetrics::default()
    };
    let config = RiskConfig {
        max_consecutive_misses: 3,
        max_total_exposure_usd: dec!(5000),
        max_single_market_exposure_usd: dec!(500),
        max_single_bet_usd: dec!(25),
        max_open_positions: 5,
        max_daily_loss_usd: dec!(75),
        max_weekly_loss_usd: dec!(120),
        daily_budget_usd: dec!(200),
        min_balance_usd: dec!(50),
        reserve_balance_usd: dec!(100),
        min_trade_usd: dec!(1),
        circuit_breaker: oxide_arb_models::config::CircuitBreakerConfig {
            l2_cooldown_secs: 0, // immediate for test
            ..Default::default()
        },
        ..RiskConfig::default()
    };

    let engine = RiskEngineBuilder::new()
        .config(config)
        .clock(utc_clock())
        .initial_equity(Usd::new(dec!(5000)))
        .build(&metrics)
        .unwrap();

    // Trip the breaker via on_trade_result (consecutive misses >= threshold)
    let trade = test_trade_record(TradeOutcome::Miss, dec!(-5));
    engine
        .on_trade_result(TradeAccountingPhase::Settlement, &trade, &metrics)
        .await
        .unwrap();

    // Engine should not allow trading while breaker is open
    // Use a different market from the one we tripped (which got auto-blacklisted)
    let mut opp = test_opportunity();
    opp.market_id = MarketId::new("0xother_market");
    let opp = Arc::new(opp);
    let prob = test_probability();
    let decision =
        engine.pre_trade_check_core(opp.as_ref(), &prob, &metrics, ReportMode::ShortCircuit);
    assert!(!decision.allowed, "should deny while breaker is open");

    std::thread::sleep(std::time::Duration::from_millis(10));
    let transitioned = engine.tick(&metrics).await.unwrap();

    assert!(transitioned, "tick should drive Open → HalfOpen transition");

    // After tick, breaker is in HalfOpen — allows trading (probe mode)
    let metrics_normal = MockMetrics::default();
    let decision = engine.pre_trade_check_core(
        opp.as_ref(),
        &prob,
        &metrics_normal,
        ReportMode::ShortCircuit,
    );
    assert!(
        decision.allowed,
        "HalfOpen allows probe trades: {:?}",
        decision.denial_reason
    );
}

// ── acknowledge_and_resume clears halt ─────────────────────────────────────

#[tokio::test]
async fn acknowledge_and_resume_clears_halt() {
    let metrics = MockMetrics::default();
    let engine = build_engine(&metrics);

    engine.halt("test halt".into()).await;
    assert!(engine.is_halted());

    engine
        .acknowledge_and_resume("operator reviewed and approved")
        .await
        .unwrap();
    assert!(!engine.is_halted());

    let opp = Arc::new(test_opportunity());
    let prob = test_probability();
    let decision =
        engine.pre_trade_check_core(opp.as_ref(), &prob, &metrics, ReportMode::ShortCircuit);
    assert!(
        decision.allowed,
        "should allow after acknowledge_and_resume: {:?}",
        decision.denial_reason
    );
}

// ── heartbeat failures trigger L4 halt ─────────────────────────────────────

#[tokio::test]
async fn heartbeat_failures_trigger_system_halt() {
    let metrics = MockMetrics::default();
    let config = RiskConfig {
        heartbeat_max_failures: 2,
        ..Default::default()
    };
    let engine = RiskEngineBuilder::new()
        .config(config)
        .clock(utc_clock())
        .build(&metrics)
        .unwrap();

    engine.on_execution_event(ExecutionRiskEvent::HeartbeatFailure);
    assert!(
        !engine.is_halted(),
        "single heartbeat failure should not halt yet"
    );

    engine.on_execution_event(ExecutionRiskEvent::HeartbeatFailure);
    assert!(
        engine.is_halted(),
        "max heartbeat failures should trigger L4 halt"
    );

    engine.on_execution_event(ExecutionRiskEvent::HeartbeatSuccess);
    assert!(
        engine.is_halted(),
        "heartbeat success does not auto-resume from L4 halt"
    );
}
