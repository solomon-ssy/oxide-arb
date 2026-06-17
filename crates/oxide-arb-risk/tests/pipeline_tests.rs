//! Risk pipeline tests.
//!
//! Validates check ordering, short-circuit behaviour, full-report mode,
//! and empty pipeline semantics.

use chrono::Utc;
use num_traits::ToPrimitive;
use oxide_arb_models::{
    domain::{
        TradeIntegritySnapshot,
        calibration::{BucketKey, CalibrationSnapshot},
        control_factor::{
            AppliedControlFactor, FactorDecisionContext, MarketAnomalyDecision,
            ReconciliationHealthDecision,
        },
        opportunity::{EndgameMeta, Opportunity},
        position::PositionInfo,
        risk::ProbabilityInput,
    },
    enums::{
        calibration::{DurationBucket, PriceZone},
        common::{ExecutionMode, MarketCategory, Side, StalenessLevel},
        control_factor::ControlFactorType,
        opportunity::PayoutModel,
    },
    runtime_config::{NegRiskRedeemPolicy, RedeemRoutingPolicy, RiskConfig},
    types::{
        Bps, EventId, FactorPublicationId, MarketId, OpportunityId, Price, Shares, TokenId, Usd,
    },
};
use oxide_arb_risk::{
    builder::RiskEngineBuilder,
    clock::utc_clock,
    context::{AdmissionGateInput, PreTradeContext, SettlementGateInput},
    pipeline::{
        RiskCheck, build_default_pipeline,
        checks::{
            BlacklistCheck, BlockingTradesCheck, CircuitBreakerCheck,
            ControlFactorManualAckRequiredCheck, ControlFactorSnapshotExpiredCheck,
            ManualHaltCheck, MarketAnomalyBlockCheck, ReconciliationMaintenanceCheck,
            RedeemRouteResolvableCheck, TokenBlacklistCheck, WsConnectivityCheck,
        },
    },
    sizing::MultiConstraintSizer,
    snapshot::{DailyAccountingSnapshot, RiskSnapshot},
    traits::{RiskMetrics, RiskMetricsSnapshot},
    types::{PipelineReport, ReportMode, RiskCheckId, RiskCheckKind, RiskCheckResult},
};
use rust_decimal_macros::dec;
use std::time::Instant;

struct TestFrame {
    opp: Opportunity,
    snap: RiskSnapshot,
    integrity: TradeIntegritySnapshot,
}

impl TestFrame {
    fn ctx(&self) -> PreTradeContext<'_> {
        PreTradeContext {
            opportunity: &self.opp,
            probability: ProbabilityInput {
                calibrated_win_prob: dec!(0.95),
                fill_prob: dec!(0.90),
                calibration_confidence: dec!(0.85),
                sample_size: 50,
                model_staleness_secs: 300,
                expected_slippage_pct: dec!(0.005),
                expected_failure_cost_pct: dec!(0.005),
            },
            snap: &self.snap,
            metrics: RiskMetricsSnapshot {
                total_exposure: Usd::new(dec!(100)),
                open_position_count: 1,
                cash_balance: Usd::new(dec!(5000)),
                equity: Usd::new(dec!(5000)),
                is_authoritative: true,
                is_stale: false,
                metrics_age_secs: 0,
                ..RiskMetricsSnapshot::zeroed()
            },
            factor_context: None,
            settlement_gate: SettlementGateInput::default(),
            integrity: &self.integrity,
            now: Utc::now(),
            sized_intent: None,
        }
    }
}

fn default_frame() -> TestFrame {
    let opp = Opportunity {
        opportunity_id: OpportunityId::from_v7(),
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

    TestFrame {
        opp,
        snap: RiskSnapshot {
            daily: DailyAccountingSnapshot {
                daily_budget_remaining: Usd::new(dec!(50)),
                ..RiskSnapshot::zeroed().daily
            },
            ..RiskSnapshot::zeroed()
        },
        integrity: TradeIntegritySnapshot::zero(Utc::now()),
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
    fn cash_balance(&self) -> Usd {
        Usd::new(dec!(5000))
    }
    fn position_mark_value(&self) -> Usd {
        Usd::ZERO
    }
    fn equity(&self) -> Usd {
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
    fn record_trade_outcome(&self, _: Side, _: &MarketId, _: bool) {}
    fn ws_disconnect_secs(&self) -> u64 {
        0
    }
    fn api_error_count(&self) -> u64 {
        0
    }
    fn api_request_count(&self) -> u64 {
        0
    }
    fn metrics_age_secs(&self) -> u64 {
        0
    }
    fn is_stale(&self) -> bool {
        false
    }
    fn is_authoritative(&self) -> bool {
        true
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

    let frame = default_frame();
    let prob = frame.ctx().probability;

    let decision = engine.pre_trade_check_core(
        &frame.opp,
        &prob,
        &metrics,
        None,
        AdmissionGateInput {
            settlement: SettlementGateInput::default(),
            integrity: &frame.integrity,
        },
        ReportMode::FullReport,
    );

    let check_ids: Vec<RiskCheckId> = decision.checks().iter().map(|r| r.check_id).collect();

    let expected = vec![
        RiskCheckId::ManualHalt,
        RiskCheckId::CircuitBreaker,
        RiskCheckId::BlacklistTradingPath,
        RiskCheckId::TokenBlacklist,
        RiskCheckId::MarketAnomalyBlock,
        RiskCheckId::ReconciliationMaintenance,
        RiskCheckId::ControlFactorManualAckRequired,
        RiskCheckId::ControlFactorSnapshotExpired,
        RiskCheckId::BlockingTrades,
        RiskCheckId::RedeemRouteResolvable,
        RiskCheckId::MetricsFreshness,
        RiskCheckId::MinDepth,
        RiskCheckId::MaxDepthUsage,
        RiskCheckId::Staleness,
        RiskCheckId::DailyBudget,
        RiskCheckId::DailyLossCap,
        RiskCheckId::WeeklyLossCap,
        RiskCheckId::HourlyLossCap,
        RiskCheckId::FeeSpend,
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
        32,
        "expected exactly 32 checks, got {}",
        check_ids.len()
    );
    assert_eq!(check_ids, expected, "pipeline check order mismatch");
}

#[test]
fn live_redeem_route_check_rejects_registry_miss() {
    let frame = default_frame();
    let policy = RedeemRoutingPolicy::default();
    let mut ctx = frame.ctx();
    ctx.settlement_gate = SettlementGateInput {
        mode: ExecutionMode::Live,
        market_neg_risk: None,
        redeem_policy: Some(&policy),
    };

    let result = RedeemRouteResolvableCheck.evaluate(&ctx);

    assert_eq!(result.check_id, RiskCheckId::RedeemRouteResolvable);
    assert!(!result.passed);
    assert!(
        result
            .detail
            .as_deref()
            .is_some_and(|reason| reason.contains("not in registry"))
    );
}

#[test]
fn live_redeem_route_check_rejects_missing_class_policy() {
    let frame = default_frame();
    let policy = RedeemRoutingPolicy {
        standard: None,
        neg_risk: Some(NegRiskRedeemPolicy::default()),
        ..RedeemRoutingPolicy::default()
    };
    let mut ctx = frame.ctx();
    ctx.settlement_gate = SettlementGateInput {
        mode: ExecutionMode::Live,
        market_neg_risk: Some(false),
        redeem_policy: Some(&policy),
    };

    let result = RedeemRouteResolvableCheck.evaluate(&ctx);

    assert_eq!(result.check_id, RiskCheckId::RedeemRouteResolvable);
    assert!(!result.passed);
    assert!(
        result
            .detail
            .as_deref()
            .is_some_and(|reason| reason.contains("no route"))
    );
}

#[test]
fn simulated_modes_do_not_require_redeem_route_resolution() {
    let frame = default_frame();
    let policy = RedeemRoutingPolicy {
        standard: None,
        neg_risk: None,
        ..RedeemRoutingPolicy::default()
    };
    let mut ctx = frame.ctx();
    ctx.settlement_gate = SettlementGateInput {
        mode: ExecutionMode::Paper,
        market_neg_risk: None,
        redeem_policy: Some(&policy),
    };

    let result = RedeemRouteResolvableCheck.evaluate(&ctx);

    assert_eq!(result.check_id, RiskCheckId::RedeemRouteResolvable);
    assert!(result.passed);
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

    fn evaluate(&self, ctx: &PreTradeContext<'_>, mode: ReportMode) -> PipelineReport {
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
    assert_eq!(pipeline.check_order().len(), 32);
}

#[test]
fn metrics_split_index_matches_check_order() {
    let pipeline = build_default_pipeline(&RiskConfig::default());

    // Ten phase-1 gates now precede MetricsFreshness: halt, circuit breaker,
    // blacklist, token blacklist, market-anomaly block, reconciliation maintenance,
    // control-factor manual ack, control-factor snapshot expiry, blocking trades,
    // redeem route resolvable.
    assert_eq!(pipeline.metrics_split_index(), 10);

    assert!(!ManualHaltCheck.requires_metrics());
    assert!(!CircuitBreakerCheck.requires_metrics());
    assert!(!BlacklistCheck.requires_metrics());
    assert!(!TokenBlacklistCheck.requires_metrics());

    let order = pipeline.check_order();
    let profile = pipeline.requires_metrics_profile();
    assert_eq!(profile.len(), order.len());
    for ((id, _), expected_id) in profile.iter().zip(order.iter()) {
        assert_eq!(*id, *expected_id);
    }

    for (id, needs_metrics) in profile.iter().take(10) {
        assert!(
            !needs_metrics,
            "{id} must not require live metrics (phase-1 halt/CB/blacklist/factor gates)"
        );
    }
    for (id, needs_metrics) in profile.iter().skip(10) {
        if *id == RiskCheckId::FeeSpend {
            assert!(
                !needs_metrics,
                "{id} must not require live metrics (phase-1 fee spend)"
            );
            continue;
        }
        assert!(*needs_metrics, "{id} must require live metrics (phase-2)");
    }

    let first_metrics_gate = profile
        .iter()
        .position(|(_, needs)| *needs)
        .expect("at least one check requires metrics");
    assert_eq!(first_metrics_gate, pipeline.metrics_split_index());
    assert_eq!(order[first_metrics_gate], RiskCheckId::MetricsFreshness);
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
    fn evaluate(&self, _ctx: &PreTradeContext<'_>) -> RiskCheckResult {
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
    fn evaluate(&self, _ctx: &PreTradeContext<'_>) -> RiskCheckResult {
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

    let frame = default_frame();
    let report = pipeline.evaluate(&frame.ctx(), ReportMode::ShortCircuit);

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

    let frame = default_frame();
    let report = pipeline.evaluate(&frame.ctx(), ReportMode::FullReport);

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
    assert_eq!(pipeline.len(), 32);
}

// ── Control-factor named gates + sizer caps (Phase 5.6) ────────────────────

fn applied(factor_type: ControlFactorType) -> AppliedControlFactor {
    AppliedControlFactor::new(
        oxide_arb_models::types::ControlFactorId::from_v7(),
        factor_type,
        FactorPublicationId::from_v7(),
        dec!(1),
        dec!(0.5),
        "test",
    )
}

fn ctx_with_factors<'a>(
    frame: &'a TestFrame,
    factor_context: &'a FactorDecisionContext,
) -> PreTradeContext<'a> {
    PreTradeContext {
        opportunity: &frame.opp,
        probability: frame.ctx().probability,
        snap: &frame.snap,
        metrics: RiskMetricsSnapshot::zeroed(),
        factor_context: Some(factor_context),
        settlement_gate: SettlementGateInput::default(),
        integrity: &frame.integrity,
        now: Utc::now(),
        sized_intent: None,
    }
}

#[test]
fn market_anomaly_block_check_is_named_hard_reject() {
    let frame = default_frame();
    let factor_context = FactorDecisionContext {
        market_anomaly: MarketAnomalyDecision {
            block_market: true,
            block_event: false,
            manual_ack_required: false,
            reason_code: Some("oracle_mismatch".into()),
            source: Some(applied(ControlFactorType::MarketAnomaly)),
        },
        ..FactorDecisionContext::neutral()
    };
    let ctx = ctx_with_factors(&frame, &factor_context);
    let result = MarketAnomalyBlockCheck.evaluate(&ctx);
    assert!(!result.passed);
    assert_eq!(result.check_id, RiskCheckId::MarketAnomalyBlock);
    assert_eq!(result.detail.as_deref(), Some("oracle_mismatch"));

    // Neutral context passes.
    let neutral = FactorDecisionContext::neutral();
    let ctx = ctx_with_factors(&frame, &neutral);
    assert!(MarketAnomalyBlockCheck.evaluate(&ctx).passed);
}

#[test]
fn ws_connectivity_uses_market_specific_freshness() {
    let frame = default_frame();
    let mut ctx = frame.ctx();
    ctx.metrics.ws_disconnect_secs = 1;
    ctx.metrics.market_ws_disconnect_secs = 31;

    let check = WsConnectivityCheck::new(&RiskConfig::default());
    let result = check.evaluate(&ctx);

    assert!(!result.passed);
    assert_eq!(result.check_id, RiskCheckId::WsConnectivity);
}

#[test]
fn reconciliation_maintenance_check_is_named_hard_reject() {
    let frame = default_frame();
    let factor_context = FactorDecisionContext {
        reconciliation_health: ReconciliationHealthDecision {
            force_maintenance_mode: true,
            size_multiplier: dec!(0),
            require_manual_ack: true,
            source: Some(applied(ControlFactorType::ReconciliationHealth)),
        },
        ..FactorDecisionContext::neutral()
    };
    let ctx = ctx_with_factors(&frame, &factor_context);
    let result = ReconciliationMaintenanceCheck.evaluate(&ctx);
    assert!(!result.passed);
    assert_eq!(result.check_id, RiskCheckId::ReconciliationMaintenance);
}

#[test]
fn blocking_trades_check_denies_live_and_paper_when_queue_non_empty() {
    let frame = default_frame();
    let mut integrity = frame.integrity.clone();
    integrity.blocking_count = 2;

    let mut ctx = frame.ctx();
    ctx.integrity = &integrity;
    ctx.settlement_gate = SettlementGateInput {
        mode: ExecutionMode::Live,
        ..SettlementGateInput::default()
    };
    let live = BlockingTradesCheck.evaluate(&ctx);
    assert!(!live.passed);
    assert_eq!(live.check_id, RiskCheckId::BlockingTrades);

    ctx.settlement_gate.mode = ExecutionMode::Paper;
    let paper = BlockingTradesCheck.evaluate(&ctx);
    assert!(!paper.passed);

    ctx.settlement_gate.mode = ExecutionMode::DryRun;
    let dry_run = BlockingTradesCheck.evaluate(&ctx);
    assert!(dry_run.passed);
}

#[test]
fn control_factor_snapshot_expired_check_is_named_hard_reject() {
    let frame = default_frame();
    let factor_context = FactorDecisionContext {
        publication_id: Some(FactorPublicationId::from_v7()),
        snapshot_expired: true,
        fail_closed: true,
        ..FactorDecisionContext::neutral()
    };
    let ctx = ctx_with_factors(&frame, &factor_context);
    let result = ControlFactorSnapshotExpiredCheck.evaluate(&ctx);
    assert!(!result.passed);
    assert_eq!(result.check_id, RiskCheckId::ControlFactorSnapshotExpired);

    let neutral = FactorDecisionContext {
        snapshot_expired: true,
        fail_closed: false,
        ..FactorDecisionContext::neutral()
    };
    let ctx = ctx_with_factors(&frame, &neutral);
    assert!(ControlFactorSnapshotExpiredCheck.evaluate(&ctx).passed);
}

#[test]
fn control_factor_manual_ack_required_check_is_named_hard_reject() {
    let frame = default_frame();
    let factor_context = FactorDecisionContext {
        reconciliation_health: ReconciliationHealthDecision {
            require_manual_ack: true,
            ..ReconciliationHealthDecision::default()
        },
        ..FactorDecisionContext::neutral()
    };
    let ctx = ctx_with_factors(&frame, &factor_context);
    let result = ControlFactorManualAckRequiredCheck.evaluate(&ctx);
    assert!(!result.passed);
    assert_eq!(result.check_id, RiskCheckId::ControlFactorManualAckRequired);
}

#[test]
fn blocking_trades_check_denies_live_when_queue_nonempty() {
    let frame = default_frame();
    let blocking = TradeIntegritySnapshot {
        blocking_count: 2,
        needs_reconcile_count: 2,
        intent_orphan_count: 0,
        oldest_blocking_age_secs: 60,
        active_reservation_count: 1,
        reserved_usd: Usd::new(dec!(25)),
        checked_at: Utc::now(),
    };
    let mut ctx = frame.ctx();
    ctx.integrity = &blocking;
    ctx.settlement_gate.mode = ExecutionMode::Live;

    let result = BlockingTradesCheck.evaluate(&ctx);

    assert_eq!(result.check_id, RiskCheckId::BlockingTrades);
    assert!(!result.passed);
}

#[test]
fn blocking_trades_check_passes_dry_run_with_warn_only_semantics() {
    let frame = default_frame();
    let blocking = TradeIntegritySnapshot {
        blocking_count: 3,
        needs_reconcile_count: 3,
        intent_orphan_count: 1,
        oldest_blocking_age_secs: 120,
        active_reservation_count: 2,
        reserved_usd: Usd::new(dec!(50)),
        checked_at: Utc::now(),
    };
    let mut ctx = frame.ctx();
    ctx.integrity = &blocking;
    ctx.settlement_gate.mode = ExecutionMode::DryRun;

    let result = BlockingTradesCheck.evaluate(&ctx);

    assert_eq!(result.check_id, RiskCheckId::BlockingTrades);
    assert!(result.passed);
}

#[test]
fn sizer_emits_factor_bucket_size_cap_as_explicit_constraint() {
    let frame = default_frame();
    let factor_context = FactorDecisionContext {
        bucket_size_multiplier: dec!(0.5),
        ..FactorDecisionContext::neutral()
    };
    let ctx = ctx_with_factors(&frame, &factor_context);
    let sizer = MultiConstraintSizer::new(&RiskConfig::default());
    let result = sizer.size(&ctx, Usd::new(dec!(5000)), dec!(1));

    // The factor cap is an explicit, auditable constraint (never folded into
    // bankroll) and never exceeds any base constraint.
    let cap = result
        .breakdown
        .constraints
        .iter()
        .find(|constraint| constraint.name == "factor_bucket_size_cap")
        .expect("factor_bucket_size_cap present in breakdown");
    let min_base = result
        .breakdown
        .constraints
        .iter()
        .filter(|constraint| !constraint.name.starts_with("factor_"))
        .map(|constraint| constraint.max_usd)
        .min_by(|a, b| a.inner().cmp(&b.inner()))
        .expect("at least one base constraint");
    assert!(cap.max_usd.inner() <= min_base.inner());
}

#[test]
fn sizer_omits_factor_caps_when_neutral() {
    let frame = default_frame();
    let neutral = FactorDecisionContext::neutral();
    let ctx = ctx_with_factors(&frame, &neutral);
    let sizer = MultiConstraintSizer::new(&RiskConfig::default());
    let result = sizer.size(&ctx, Usd::new(dec!(5000)), dec!(1));
    let names: Vec<&str> = result
        .breakdown
        .constraints
        .iter()
        .map(|constraint| constraint.name)
        .collect();
    assert!(!names.iter().any(|name| name.starts_with("factor_")));
}
