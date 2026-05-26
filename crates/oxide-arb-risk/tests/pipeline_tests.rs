//! Risk pipeline tests.
//!
//! Validates check ordering, short-circuit behaviour, full-report mode,
//! and empty pipeline semantics.

use chrono::Utc;
use num_traits::ToPrimitive;
use oxide_arb_models::config::RiskConfig;
use oxide_arb_models::domain::position::PositionInfo;
use oxide_arb_models::domain::risk::ProbabilityInput;
use oxide_arb_models::enums::common::Side;
use oxide_arb_models::types::MarketId;
use oxide_arb_models::types::Usd;
use oxide_arb_risk::builder::RiskEngineBuilder;
use oxide_arb_risk::clock::utc_clock;
use oxide_arb_risk::context::{BlacklistGate, CircuitBreakerGate, ManualHaltGate, RiskContext};
use oxide_arb_risk::pipeline::{RiskCheck, build_default_pipeline};
use oxide_arb_risk::traits::RiskMetrics;
use oxide_arb_risk::types::{
    DrawdownAction, ReportMode, RiskCheckId, RiskCheckKind, RiskCheckResult, StateVersion,
};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use std::sync::Arc;

fn default_context() -> RiskContext {
    use oxide_arb_models::domain::calibration::{BucketKey, CalibrationSnapshot};
    use oxide_arb_models::domain::opportunity::{EndgameMeta, Opportunity};
    use oxide_arb_models::enums::calibration::{DurationBucket, PriceZone};
    use oxide_arb_models::enums::common::{MarketCategory, Side, StalenessLevel};
    use oxide_arb_models::enums::opportunity::PayoutModel;
    use oxide_arb_models::types::{Bps, EventId, MarketId, OpportunityId, Price, Shares, TokenId};

    let opp = Opportunity {
        opportunity_id: OpportunityId::new_v7(),
        market_id: MarketId::new("0xtest"),
        event_id: EventId::new("evt_test"),
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
    };

    RiskContext {
        state_version: StateVersion::ZERO,
        opportunity: Arc::new(opp),
        probability: ProbabilityInput {
            calibrated_win_prob: dec!(0.95),
            fill_prob: dec!(0.90),
            calibration_confidence: dec!(0.85),
            sample_size: 50,
            model_staleness_secs: 300,
            expected_slippage_pct: dec!(0.005),
            expected_failure_cost_pct: dec!(0.005),
        },
        market_exposure_before: Usd::ZERO,
        total_exposure_before: Usd::new(dec!(100)),
        total_potential_loss: Usd::ZERO,
        active_reservation_count: 0,
        reserved_usd: Usd::ZERO,
        open_position_count: 1,
        cached_balance: Usd::new(dec!(5000)),
        ws_disconnect_secs: 0,
        open_directional_count_same_side: 0,
        daily_directional_trades_same_side: 0,
        consecutive_market_misses: 0,
        hourly_loss: Usd::ZERO,
        daily_loss: Usd::ZERO,
        daily_budget_remaining: Usd::new(dec!(50)),
        weekly_loss: Usd::ZERO,
        daily_pnl: Usd::ZERO,
        circuit_breaker: CircuitBreakerGate {
            allows_trading: true,
            is_probe: false,
        },
        manual_halt: ManualHaltGate::Clear,
        blacklist: BlacklistGate::Clear,
        token_blacklisted: false,
        api_error_count: 0,
        api_request_count: 0,
        drawdown_factor: Decimal::ONE,
        drawdown_action: DrawdownAction::Normal,
        snapshot_at: Utc::now(),
    }
}

// ── Mock for golden test ───────────────────────────────────────────────────

struct GoldenMetrics;
impl RiskMetrics for GoldenMetrics {
    fn total_exposure(&self) -> Usd {
        Usd::new(dec!(100))
    }
    fn market_exposure(&self, _: &MarketId) -> Usd {
        Usd::ZERO
    }
    fn open_position_count(&self) -> usize {
        0
    }
    fn open_positions(&self) -> Vec<PositionInfo> {
        vec![]
    }
    fn cached_balance(&self) -> Usd {
        Usd::new(dec!(5000))
    }
    fn active_reservation_count(&self) -> usize {
        0
    }
    fn reserved_usd(&self) -> Usd {
        Usd::ZERO
    }
    fn open_directional_count(&self, _: Side) -> usize {
        0
    }
    fn daily_directional_trades(&self, _: Side) -> u32 {
        0
    }
    fn consecutive_market_misses(&self, _: &MarketId) -> u32 {
        0
    }
    fn ws_disconnect_secs(&self) -> u64 {
        0
    }
    fn api_error_count(&self) -> u64 {
        0
    }
    fn api_request_count(&self) -> u64 {
        0
    }
}

// ── Golden test: verify 24-check pipeline order via full report ────────────

#[tokio::test]
async fn pipeline_check_order_golden_test() {
    let config = RiskConfig::default();
    let metrics = GoldenMetrics;
    let engine = RiskEngineBuilder::new()
        .config(config)
        .clock(utc_clock())
        .initial_equity(Usd::new(dec!(5000)))
        .build(&metrics)
        .expect("engine build");

    let ctx = default_context();
    let prob = ProbabilityInput {
        calibrated_win_prob: dec!(0.95),
        fill_prob: dec!(0.90),
        calibration_confidence: dec!(0.85),
        sample_size: 50,
        model_staleness_secs: 300,
        expected_slippage_pct: dec!(0.005),
        expected_failure_cost_pct: dec!(0.005),
    };

    // Use FullReport to get all check results in order
    let decision =
        engine.pre_trade_check_core(&ctx.opportunity, &prob, &metrics, ReportMode::FullReport);

    let check_ids: Vec<RiskCheckId> = decision.checks().iter().map(|r| r.check_id).collect();

    let expected = vec![
        RiskCheckId::ManualHalt,
        RiskCheckId::CircuitBreaker,
        RiskCheckId::BlacklistTradingPath,
        RiskCheckId::TokenBlacklist,
        RiskCheckId::MinDepth,
        RiskCheckId::MaxDepthUsage,
        RiskCheckId::Staleness,
        RiskCheckId::DailyBudget,
        RiskCheckId::DailyLossCap,
        RiskCheckId::WeeklyLossCap,
        RiskCheckId::HourlyLossCap,
        RiskCheckId::MaxSingleBet,
        RiskCheckId::MarketExposure,
        RiskCheckId::TotalExposure,
        RiskCheckId::ExposurePct,
        RiskCheckId::PotentialLossCap,
        RiskCheckId::MaxPositions,
        RiskCheckId::WsConnectivity,
        RiskCheckId::ApiErrorRate,
        RiskCheckId::MinBalance,
        RiskCheckId::DirectionalConcentration,
        RiskCheckId::DailyDirectionalBudget,
        RiskCheckId::DuplicateMarket,
        RiskCheckId::DrawdownGuard,
    ];

    assert_eq!(
        check_ids.len(),
        24,
        "expected exactly 24 checks, got {}",
        check_ids.len()
    );
    assert_eq!(check_ids, expected, "pipeline check order mismatch");
}

// ── Short-circuit / full-report behaviour (test-only dynamic pipeline) ─────

struct TestRiskPipeline {
    checks: Vec<Box<dyn RiskCheck>>,
}

impl TestRiskPipeline {
    fn new() -> Self {
        Self { checks: Vec::new() }
    }

    fn register(&mut self, check: Box<dyn RiskCheck>) {
        self.checks.push(check);
    }

    fn evaluate(
        &self,
        ctx: &RiskContext,
        mode: ReportMode,
    ) -> oxide_arb_risk::types::PipelineReport {
        use oxide_arb_risk::types::{PipelineReport, RiskCheckKind};
        use std::time::Instant;

        let pipeline_start = Instant::now();
        let mut results = Vec::with_capacity(self.checks.len());
        let mut has_failed_hard_gate = false;
        let mut first_failure: Option<RiskCheckId> = None;

        for check in &self.checks {
            let check_start = Instant::now();
            let mut result = check.evaluate(ctx);
            result.elapsed_us =
                ToPrimitive::to_u64(&check_start.elapsed().as_micros()).unwrap_or(u64::MAX);

            if !result.passed && check.kind() == RiskCheckKind::Gate {
                has_failed_hard_gate = true;
                if first_failure.is_none() {
                    first_failure = Some(check.id());
                }
            }

            results.push(result);

            if has_failed_hard_gate && mode == ReportMode::ShortCircuit {
                break;
            }
        }

        PipelineReport {
            results,
            has_failed_hard_gate,
            first_failure,
            total_elapsed_us: ToPrimitive::to_u64(&pipeline_start.elapsed().as_micros())
                .unwrap_or(u64::MAX),
        }
    }
}

// ── Static pipeline exposes canonical order ────────────────────────────────

#[test]
fn static_pipeline_check_order_matches_golden() {
    let pipeline = build_default_pipeline(&RiskConfig::default());
    assert_eq!(pipeline.check_order().len(), 24);
}

// ── Short-circuit stops on first gate failure ──────────────────────────────

struct AlwaysFail(RiskCheckId);
impl RiskCheck for AlwaysFail {
    fn id(&self) -> RiskCheckId {
        self.0
    }
    fn kind(&self) -> RiskCheckKind {
        RiskCheckKind::Gate
    }
    fn evaluate(&self, _ctx: &RiskContext) -> RiskCheckResult {
        RiskCheckResult {
            check_id: self.0,
            passed: false,
            detail: Some("forced failure".into()),
            threshold: None,
            actual: None,
            elapsed_us: 0,
        }
    }
}

struct AlwaysPass(RiskCheckId);
impl RiskCheck for AlwaysPass {
    fn id(&self) -> RiskCheckId {
        self.0
    }
    fn kind(&self) -> RiskCheckKind {
        RiskCheckKind::Gate
    }
    fn evaluate(&self, _ctx: &RiskContext) -> RiskCheckResult {
        RiskCheckResult {
            check_id: self.0,
            passed: true,
            detail: None,
            threshold: None,
            actual: None,
            elapsed_us: 0,
        }
    }
}

#[test]
fn short_circuit_stops_on_first_failure() {
    let mut pipeline = TestRiskPipeline::new();
    pipeline.register(Box::new(AlwaysPass(RiskCheckId::ManualHalt)));
    pipeline.register(Box::new(AlwaysFail(RiskCheckId::CircuitBreaker)));
    pipeline.register(Box::new(AlwaysPass(RiskCheckId::MinBalance))); // should never reach

    let ctx = default_context();
    let report = pipeline.evaluate(&ctx, ReportMode::ShortCircuit);

    assert!(report.has_failed_hard_gate);
    assert_eq!(report.first_failure, Some(RiskCheckId::CircuitBreaker));
    // Should only have evaluated 2 checks (pass + fail, then stop)
    assert_eq!(report.results.len(), 2);
}

// ── Full-report evaluates all checks ───────────────────────────────────────

#[test]
fn full_report_evaluates_all_checks() {
    let mut pipeline = TestRiskPipeline::new();
    pipeline.register(Box::new(AlwaysPass(RiskCheckId::ManualHalt)));
    pipeline.register(Box::new(AlwaysFail(RiskCheckId::CircuitBreaker)));
    pipeline.register(Box::new(AlwaysPass(RiskCheckId::MinBalance)));
    pipeline.register(Box::new(AlwaysFail(RiskCheckId::DailyBudget)));

    let ctx = default_context();
    let report = pipeline.evaluate(&ctx, ReportMode::FullReport);

    assert!(report.has_failed_hard_gate);
    assert_eq!(report.first_failure, Some(RiskCheckId::CircuitBreaker));
    // All 4 checks should be evaluated
    assert_eq!(report.results.len(), 4);
}

// ── Static pipeline is never empty ─────────────────────────────────────────

#[test]
fn static_pipeline_has_fixed_checks() {
    let pipeline = build_default_pipeline(&RiskConfig::default());
    assert!(!pipeline.is_empty());
    assert_eq!(pipeline.len(), 24);
}
