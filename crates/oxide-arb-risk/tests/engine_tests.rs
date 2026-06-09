//! Full `RiskEngine` integration tests.
//!
//! Uses a `MockMetrics` implementation to exercise the engine's pre-trade,
//! post-trade, tick, halt/resume lifecycle.

mod support;

use chrono::{DateTime, Utc};
use oxide_arb_error::{OxideError, OxideResult};
use oxide_arb_models::{
    config::{CircuitBreakerConfig, RiskConfig},
    domain::{
        BlacklistInfo, CoreEvent, CoreEventPublisher, PositionInfo, UpsertBlacklistEntry,
        calibration::{BucketKey, CalibrationSnapshot},
        opportunity::{EndgameMeta, Opportunity},
        risk::{
            FillCommit, NewEmergencySnapshot, NewReconciliationReport, NewRiskAuditEvent,
            ProbabilityInput, RiskStateInfo, UpsertRiskEngineState,
        },
        trade::PostTradeInput,
    },
    enums::{
        calibration::{DurationBucket, PriceZone},
        common::{MarketCategory, Side, StalenessLevel, TradeBusinessOutcome},
        opportunity::PayoutModel,
        risk::{BreakerStateName, CircuitBreakerLevel, TradeAccountingPhase},
    },
    types::{Bps, EventId, MarketId, OpportunityId, Price, Shares, TokenId, TradeId, Usd},
};
use oxide_arb_risk::{
    builder::RiskEngineBuilder,
    clock::utc_clock,
    engine::RiskEngine,
    traits::{FillClaim, RiskFillCommitGuard, RiskMetrics, RiskPersistence},
    types::{ExecutionRiskEvent, ReportMode},
};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use std::{
    collections::HashSet,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU32, Ordering},
    },
    thread::sleep,
    time::Duration,
};
use support::MockMetrics;

// ── Test Helpers ───────────────────────────────────────────────────────────

fn test_opportunity() -> Opportunity {
    Opportunity {
        opportunity_id: OpportunityId::from_v7(),
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

fn test_trade_record(outcome: TradeBusinessOutcome, profit: Decimal) -> PostTradeInput {
    PostTradeInput {
        trade_id: TradeId::from_v7(),
        market_id: MarketId::new("0xtest_market"),
        token_id: TokenId::new("test_token"),
        side: Side::Buy,
        outcome,
        cost_usd: Usd::new(dec!(20)),
        fee_usd: Usd::new(dec!(0.40)),
        net_profit_usd: Some(Usd::new(profit)),
        shares: Shares::new(dec!(100)),
        entry_price: Price::new(dec!(0.92)),
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

struct CountingMetrics {
    base: MockMetrics,
    records: AtomicU32,
    misses: AtomicU32,
}

impl CountingMetrics {
    fn new() -> Self {
        Self {
            base: MockMetrics::default(),
            records: AtomicU32::new(0),
            misses: AtomicU32::new(0),
        }
    }

    fn record_count(&self) -> u32 {
        self.records.load(Ordering::Acquire)
    }
}

impl RiskMetrics for CountingMetrics {
    fn total_exposure(&self) -> Usd {
        self.base.total_exposure()
    }
    fn market_exposure(&self, market_id: &MarketId) -> Usd {
        self.base.market_exposure(market_id)
    }
    fn open_position_count(&self) -> usize {
        self.base.open_position_count()
    }
    fn open_positions(&self) -> Vec<PositionInfo> {
        Vec::new()
    }
    fn cash_balance(&self) -> Usd {
        self.base.cash_balance()
    }
    fn position_mark_value(&self) -> Usd {
        self.base.position_mark_value()
    }
    fn equity(&self) -> Usd {
        self.base.equity()
    }
    fn active_reservation_count(&self) -> usize {
        self.base.active_reservation_count()
    }
    fn reserved_usd(&self) -> Usd {
        self.base.reserved_usd()
    }
    fn open_directional_count(&self, side: Side) -> usize {
        self.base.open_directional_count(side)
    }
    fn daily_directional_trades(&self, side: Side) -> u32 {
        self.base.daily_directional_trades(side)
    }
    fn consecutive_market_misses(&self, _market_id: &MarketId) -> u32 {
        self.misses.load(Ordering::Acquire)
    }
    fn record_trade_outcome(&self, _side: Side, _market_id: &MarketId, was_miss: bool) {
        self.records.fetch_add(1, Ordering::AcqRel);
        if was_miss {
            self.misses.fetch_add(1, Ordering::AcqRel);
        } else {
            self.misses.store(0, Ordering::Release);
        }
    }
    fn ws_disconnect_secs(&self) -> u64 {
        self.base.ws_disconnect_secs()
    }
    fn api_error_count(&self) -> u64 {
        self.base.api_error_count()
    }
    fn api_request_count(&self) -> u64 {
        self.base.api_request_count()
    }
    fn metrics_age_secs(&self) -> u64 {
        self.base.metrics_age_secs()
    }
    fn is_stale(&self) -> bool {
        self.base.is_stale()
    }
    fn is_authoritative(&self) -> bool {
        self.base.is_authoritative()
    }
}

struct DedupePersistence {
    applied: Mutex<HashSet<TradeId>>,
    fail_commit: bool,
}

struct DedupeFillGuard<'a> {
    persistence: &'a DedupePersistence,
}

impl DedupePersistence {
    fn new(fail_commit: bool) -> Self {
        Self {
            applied: Mutex::new(HashSet::new()),
            fail_commit,
        }
    }

    fn default_state() -> RiskStateInfo {
        let now = Utc::now();
        RiskStateInfo {
            id: 1,
            breaker_state: BreakerStateName::Closed,
            breaker_level: None,
            is_halted: false,
            halt_reason: None,
            consecutive_misses: 0,
            cooldown_until: None,
            cooldown_multiplier: 0,
            total_exposure: Usd::ZERO,
            hourly_loss_usd: Usd::ZERO,
            hourly_fee_usd: Usd::ZERO,
            hourly_trade_count: 0,
            hourly_success_count: 0,
            hourly_miss_count: 0,
            hourly_window_start: now,
            daily_loss_usd: Usd::ZERO,
            daily_fee_usd: Usd::ZERO,
            daily_pnl: Usd::ZERO,
            daily_budget_spent: Usd::ZERO,
            daily_trade_count: 0,
            daily_success_count: 0,
            daily_miss_count: 0,
            daily_window_start: now.date_naive(),
            weekly_loss_usd: Usd::ZERO,
            weekly_trade_count: 0,
            weekly_window_start: now.date_naive(),
            hwm_equity: Usd::ZERO,
            total_realized_pnl: Usd::ZERO,
            last_emergency_at: None,
            last_emergency_reason: None,
            updated_at: now,
        }
    }
}

#[async_trait::async_trait]
impl RiskFillCommitGuard for DedupeFillGuard<'_> {
    async fn commit(self: Box<Self>, _commit: FillCommit) -> OxideResult<()> {
        if self.persistence.fail_commit {
            return Err(OxideError::Internal("injected fill commit failure".into()));
        }
        Ok(())
    }
}

#[async_trait::async_trait]
impl RiskPersistence for DedupePersistence {
    async fn upsert_state(&self, _state: UpsertRiskEngineState) -> OxideResult<()> {
        Ok(())
    }
    async fn load_state(&self) -> OxideResult<RiskStateInfo> {
        Ok(Self::default_state())
    }
    async fn begin_fill<'a>(
        &'a self,
        trade_id: &TradeId,
        _applied_at: DateTime<Utc>,
    ) -> OxideResult<FillClaim<'a>> {
        let mut applied = self.applied.lock().expect("fill applied lock");
        if !applied.insert(trade_id.clone()) {
            return Ok(FillClaim::AlreadyApplied);
        }
        drop(applied);
        Ok(FillClaim::Claimed(Box::new(DedupeFillGuard {
            persistence: self,
        })))
    }
    async fn upsert_blacklist(&self, _entry: UpsertBlacklistEntry) -> OxideResult<()> {
        Ok(())
    }
    async fn remove_blacklist(&self, _market_id: &MarketId) -> OxideResult<()> {
        Ok(())
    }
    async fn load_blacklist(&self) -> OxideResult<Vec<BlacklistInfo>> {
        Ok(Vec::new())
    }
    async fn create_emergency(&self, _emergency: NewEmergencySnapshot) -> OxideResult<()> {
        Ok(())
    }
    async fn create_reconciliation(&self, _report: NewReconciliationReport) -> OxideResult<()> {
        Ok(())
    }
    async fn create_audit(&self, _audit: NewRiskAuditEvent) -> OxideResult<()> {
        Ok(())
    }
}

// ── Healthy engine allows trade ────────────────────────────────────────────

#[tokio::test]
async fn healthy_engine_allows_trade() {
    let metrics = MockMetrics::default();
    let engine = build_engine(&metrics);

    let opp = Arc::new(test_opportunity());
    let prob = test_probability();
    let decision = engine.pre_trade_check_core(
        opp.as_ref(),
        &prob,
        &metrics,
        None,
        ReportMode::ShortCircuit,
    );

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
    let decision = engine.pre_trade_check_core(
        opp.as_ref(),
        &prob,
        &metrics,
        None,
        ReportMode::ShortCircuit,
    );

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

    let trade = test_trade_record(TradeBusinessOutcome::Miss, dec!(-5));
    engine
        .on_trade_result(TradeAccountingPhase::Settlement, &trade, &metrics)
        .await
        .unwrap();

    let opp = Arc::new(test_opportunity());
    let prob = test_probability();
    let decision = engine.pre_trade_check_core(
        opp.as_ref(),
        &prob,
        &metrics,
        None,
        ReportMode::ShortCircuit,
    );

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
    let decision = engine.pre_trade_check_core(
        opp.as_ref(),
        &prob,
        &metrics,
        None,
        ReportMode::ShortCircuit,
    );

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
        engine.pre_trade_check_core(opp.as_ref(), &prob, &metrics, None, ReportMode::FullReport);

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

    let trade = test_trade_record(TradeBusinessOutcome::Success, dec!(5));
    let report = engine
        .on_trade_result(TradeAccountingPhase::Fill, &trade, &metrics)
        .await
        .unwrap()
        .expect("fill should apply");

    assert_eq!(report.snapshot.daily_trade_count, 1);
    assert_eq!(report.snapshot.daily_success_count, 1);
}

#[tokio::test]
async fn fill_records_miss_exactly_once_under_replay() {
    let metrics = CountingMetrics::new();
    let persistence: Arc<dyn RiskPersistence> = Arc::new(DedupePersistence::new(false));
    let engine = RiskEngineBuilder::new()
        .config(RiskConfig {
            market_miss_blacklist_count: 3,
            max_consecutive_misses: 100,
            ..RiskConfig::default()
        })
        .persistence(persistence)
        .clock(utc_clock())
        .build(&metrics)
        .expect("engine build");
    let trade = test_trade_record(TradeBusinessOutcome::Miss, dec!(0));

    let first = engine
        .on_trade_result(TradeAccountingPhase::Fill, &trade, &metrics)
        .await
        .expect("first fill applies");
    let replay = engine
        .on_trade_result(TradeAccountingPhase::Fill, &trade, &metrics)
        .await
        .expect("replayed fill is idempotent");

    assert!(first.is_some());
    assert!(replay.is_none());
    assert_eq!(metrics.record_count(), 1);
    assert_eq!(metrics.consecutive_market_misses(&trade.market_id), 1);
}

#[tokio::test]
async fn fill_commit_failure_rolls_back_accounting_and_potential_loss() {
    let metrics = CountingMetrics::new();
    let persistence: Arc<dyn RiskPersistence> = Arc::new(DedupePersistence::new(true));
    let engine = RiskEngineBuilder::new()
        .config(RiskConfig::default())
        .persistence(persistence)
        .clock(utc_clock())
        .build(&metrics)
        .expect("engine build");
    let trade = test_trade_record(TradeBusinessOutcome::Success, dec!(0));

    let result = engine
        .on_trade_result(TradeAccountingPhase::Fill, &trade, &metrics)
        .await;

    assert!(result.is_err());
    let snapshot = engine.snapshot(&metrics);
    assert_eq!(snapshot.daily_trade_count, 0);
    assert_eq!(snapshot.daily_fee_usd, Usd::ZERO);
    assert_eq!(engine.load_risk_snapshot().total_potential_loss, Usd::ZERO);
    assert!(engine.is_halted());
}

#[tokio::test]
async fn fee_spend_cap_trips_daily_halt_on_fill() {
    let metrics = MockMetrics::default();
    let config = RiskConfig {
        max_daily_fee_spend_usd: dec!(0.10),
        max_hourly_fee_spend_usd: dec!(0.10),
        max_consecutive_misses: 100,
        ..RiskConfig::default()
    };
    let engine = loss_cap_engine(config, &metrics);
    let trade = test_trade_record(TradeBusinessOutcome::Success, dec!(0));

    let report = engine
        .on_trade_result(TradeAccountingPhase::Fill, &trade, &metrics)
        .await
        .expect("fill accounting")
        .expect("fill applied");

    assert_eq!(report.breaker_tripped, Some(CircuitBreakerLevel::Daily));
    assert!(engine.snapshot(&metrics).is_halted);
}

#[tokio::test]
async fn active_potential_loss_reduces_available_bankroll_to_zero() {
    let metrics = MockMetrics::default();
    let config = RiskConfig {
        bankroll_usd: dec!(5000),
        reserve_balance_usd: dec!(100),
        max_single_bet_usd: dec!(1000),
        max_single_loss_usd: dec!(1000),
        max_single_market_exposure_usd: dec!(5000),
        max_total_exposure_usd: dec!(5000),
        daily_budget_usd: dec!(10000),
        max_consecutive_misses: 100,
        ..RiskConfig::default()
    };
    let engine = loss_cap_engine(config, &metrics);
    let mut trade = test_trade_record(TradeBusinessOutcome::Success, dec!(0));
    trade.cost_usd = Usd::new(dec!(4900));
    trade.fee_usd = Usd::ZERO;
    engine
        .on_trade_result(TradeAccountingPhase::Fill, &trade, &metrics)
        .await
        .expect("fill accounting");

    let opp = Arc::new(test_opportunity());
    let prob = test_probability();
    let decision = engine.pre_trade_check_core(
        opp.as_ref(),
        &prob,
        &metrics,
        None,
        ReportMode::ShortCircuit,
    );

    assert!(!decision.allowed);
    assert!(
        decision
            .denial_reason
            .as_deref()
            .is_some_and(|reason| reason.contains("kelly_upper_bound")),
        "expected Kelly bankroll denial, got {:?}",
        decision.denial_reason
    );
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
        circuit_breaker: CircuitBreakerConfig {
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
    let trade = test_trade_record(TradeBusinessOutcome::Miss, dec!(-5));
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
    let decision = engine.pre_trade_check_core(
        opp.as_ref(),
        &prob,
        &metrics,
        None,
        ReportMode::ShortCircuit,
    );
    assert!(!decision.allowed, "should deny while breaker is open");

    sleep(Duration::from_millis(10));
    let transitioned = engine.tick(&metrics).await.unwrap();

    assert!(transitioned, "tick should drive Open → HalfOpen transition");

    // After tick, breaker is in HalfOpen — allows trading (probe mode)
    let metrics_normal = MockMetrics::default();
    let decision = engine.pre_trade_check_core(
        opp.as_ref(),
        &prob,
        &metrics_normal,
        None,
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
    let decision = engine.pre_trade_check_core(
        opp.as_ref(),
        &prob,
        &metrics,
        None,
        ReportMode::ShortCircuit,
    );
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

// ── Loss cap breaker levels ────────────────────────────────────────────────

fn loss_cap_engine(config: RiskConfig, metrics: &MockMetrics) -> RiskEngine {
    RiskEngineBuilder::new()
        .config(config)
        .clock(utc_clock())
        .initial_equity(Usd::new(dec!(5000)))
        .build(metrics)
        .expect("engine should build")
}

async fn fill_then_settle(engine: &RiskEngine, metrics: &MockMetrics, profit: Decimal) {
    let trade = test_trade_record(TradeBusinessOutcome::Success, profit);
    engine
        .on_trade_result(TradeAccountingPhase::Fill, &trade, metrics)
        .await
        .unwrap();
    engine
        .on_trade_result(TradeAccountingPhase::Settlement, &trade, metrics)
        .await
        .unwrap();
}

#[tokio::test]
async fn weekly_loss_cap_triggers_system_halt() {
    let metrics = MockMetrics::default();
    let config = RiskConfig {
        max_weekly_loss_usd: dec!(50),
        max_daily_loss_usd: dec!(500),
        max_single_loss_usd: dec!(500),
        max_total_exposure_usd: dec!(5000),
        max_single_market_exposure_usd: dec!(500),
        max_single_bet_usd: dec!(25),
        max_open_positions: 5,
        daily_budget_usd: dec!(200),
        min_balance_usd: dec!(50),
        reserve_balance_usd: dec!(100),
        min_trade_usd: dec!(1),
        max_consecutive_misses: 100,
        ..RiskConfig::default()
    };
    let engine = loss_cap_engine(config, &metrics);

    fill_then_settle(&engine, &metrics, dec!(-60)).await;

    let snapshot = engine.snapshot(&metrics);
    assert_eq!(
        snapshot.breaker_level,
        Some(CircuitBreakerLevel::System),
        "weekly breach must trip System-level halt"
    );
    assert!(snapshot.is_halted);
}

#[tokio::test]
async fn single_loss_cap_triggers_daily_halt() {
    let metrics = MockMetrics::default();
    let config = RiskConfig {
        max_single_loss_usd: dec!(30),
        max_weekly_loss_usd: dec!(500),
        max_daily_loss_usd: dec!(500),
        max_total_exposure_usd: dec!(5000),
        max_single_market_exposure_usd: dec!(500),
        max_single_bet_usd: dec!(25),
        max_open_positions: 5,
        daily_budget_usd: dec!(200),
        min_balance_usd: dec!(50),
        reserve_balance_usd: dec!(100),
        min_trade_usd: dec!(1),
        max_consecutive_misses: 100,
        ..RiskConfig::default()
    };
    let engine = loss_cap_engine(config, &metrics);

    fill_then_settle(&engine, &metrics, dec!(-40)).await;

    let snapshot = engine.snapshot(&metrics);
    assert_eq!(
        snapshot.breaker_level,
        Some(CircuitBreakerLevel::Daily),
        "single-loss breach must trip Daily-level halt"
    );
    assert!(snapshot.is_halted);
}

#[tokio::test]
async fn loss_cap_records_emergency_in_snapshot() {
    let metrics = MockMetrics::default();
    let config = RiskConfig {
        max_weekly_loss_usd: dec!(50),
        max_daily_loss_usd: dec!(500),
        max_single_loss_usd: dec!(500),
        max_total_exposure_usd: dec!(5000),
        max_single_market_exposure_usd: dec!(500),
        max_single_bet_usd: dec!(25),
        max_open_positions: 5,
        daily_budget_usd: dec!(200),
        min_balance_usd: dec!(50),
        reserve_balance_usd: dec!(100),
        min_trade_usd: dec!(1),
        max_consecutive_misses: 100,
        ..RiskConfig::default()
    };
    let engine = loss_cap_engine(config, &metrics);

    fill_then_settle(&engine, &metrics, dec!(-60)).await;

    let snapshot = engine.snapshot(&metrics);
    assert!(snapshot.last_emergency_at.is_some());
    assert!(
        snapshot
            .last_emergency_reason
            .as_deref()
            .is_some_and(|reason| reason.contains("weekly loss cap")),
        "expected weekly loss reason, got {:?}",
        snapshot.last_emergency_reason
    );
}

#[tokio::test]
async fn settlement_emits_pnl_update_and_accumulates_lifetime_total() {
    let metrics = MockMetrics::default();
    let (events, rx) = CoreEventPublisher::bounded(16);
    let engine = RiskEngineBuilder::new()
        .config(RiskConfig::default())
        .clock(utc_clock())
        .initial_equity(Usd::new(dec!(5000)))
        .event_publisher(events)
        .build(&metrics)
        .expect("engine should build");

    // Two same-day realized settlements: +10 then +5 → daily == total == 15.
    engine
        .on_trade_result(
            TradeAccountingPhase::Settlement,
            &test_trade_record(TradeBusinessOutcome::Success, dec!(10)),
            &metrics,
        )
        .await
        .expect("first settlement");
    engine
        .on_trade_result(
            TradeAccountingPhase::Settlement,
            &test_trade_record(TradeBusinessOutcome::Success, dec!(5)),
            &metrics,
        )
        .await
        .expect("second settlement");

    let mut last_pnl = None;
    while let Ok(event) = rx.try_recv() {
        if let CoreEvent::PnlUpdate { daily, total } = event {
            last_pnl = Some((daily, total));
        }
    }
    assert_eq!(
        last_pnl,
        Some((Usd::new(dec!(15)), Usd::new(dec!(15)))),
        "latest PnlUpdate carries daily + lifetime total"
    );

    let snapshot = engine.snapshot(&metrics);
    assert_eq!(snapshot.total_realized_pnl, Usd::new(dec!(15)));

    // Restart safety: a fresh engine recovered from the snapshot preserves the
    // lifetime total even though the daily window would reset.
    let recovered = RiskEngineBuilder::new()
        .config(RiskConfig::default())
        .clock(utc_clock())
        .snapshot(snapshot)
        .build(&metrics)
        .expect("engine recovers from snapshot");
    assert_eq!(
        recovered.snapshot(&metrics).total_realized_pnl,
        Usd::new(dec!(15)),
        "lifetime total survives restart"
    );
}
