//! Audit event emission tests.
//!
//! Verifies that every critical engine operation produces the expected
//! `RiskAuditEvent` variant through a capturing mock persistence layer.

mod support;

use oxide_arb_models::{
    config::RiskConfig,
    domain::{
        blacklist::{BlacklistInfo, UpsertBlacklistEntry},
        risk::{
            NewEmergencySnapshot, NewReconciliationReport, NewRiskAuditEvent, RiskStateInfo,
            UpsertRiskEngineState,
        },
        trade::PostTradeInput,
    },
    enums::{
        common::TradeOutcome,
        risk::{BlacklistReason, BreakerStateName, TradeAccountingPhase},
    },
    types::{MarketId, TokenId, TradeId, Usd},
};
use oxide_arb_risk::{
    builder::RiskEngineBuilder, clock::utc_clock, engine::RiskEngine, traits::RiskPersistence,
};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use std::{
    mem::take,
    sync::{Arc, Mutex},
};
use support::MockMetrics;

// ── Capturing Persistence ───────────────────────────────────────────────────

struct CapturingPersistence {
    audits: Mutex<Vec<NewRiskAuditEvent>>,
}

impl CapturingPersistence {
    const fn new() -> Self {
        Self {
            audits: Mutex::new(Vec::new()),
        }
    }

    fn take_audits(&self) -> Vec<NewRiskAuditEvent> {
        take(&mut *self.audits.lock().unwrap())
    }
}

#[async_trait::async_trait]
impl RiskPersistence for CapturingPersistence {
    async fn upsert_state(
        &self,
        _state: UpsertRiskEngineState,
    ) -> oxide_arb_error::OxideResult<()> {
        Ok(())
    }
    async fn load_state(&self) -> oxide_arb_error::OxideResult<RiskStateInfo> {
        let now = chrono::Utc::now();
        Ok(RiskStateInfo {
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
            last_emergency_at: None,
            last_emergency_reason: None,
            updated_at: now,
        })
    }
    async fn upsert_blacklist(
        &self,
        _entry: UpsertBlacklistEntry,
    ) -> oxide_arb_error::OxideResult<()> {
        Ok(())
    }
    async fn remove_blacklist(&self, _market_id: &MarketId) -> oxide_arb_error::OxideResult<()> {
        Ok(())
    }
    async fn load_blacklist(&self) -> oxide_arb_error::OxideResult<Vec<BlacklistInfo>> {
        Ok(Vec::new())
    }
    async fn create_emergency(
        &self,
        _emergency: NewEmergencySnapshot,
    ) -> oxide_arb_error::OxideResult<()> {
        Ok(())
    }
    async fn create_reconciliation(
        &self,
        _report: NewReconciliationReport,
    ) -> oxide_arb_error::OxideResult<()> {
        Ok(())
    }
    async fn create_audit(&self, audit: NewRiskAuditEvent) -> oxide_arb_error::OxideResult<()> {
        self.audits.lock().unwrap().push(audit);
        Ok(())
    }
}

// ── Helpers ─────────────────────────────────────────────────────────────────

fn test_config() -> RiskConfig {
    RiskConfig {
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
        ..RiskConfig::default()
    }
}

fn test_trade(outcome: TradeOutcome, profit: Decimal) -> PostTradeInput {
    PostTradeInput {
        trade_id: TradeId::generate(),
        market_id: MarketId::new("0xaudit_market"),
        token_id: TokenId::new("audit_token"),
        outcome,
        cost_usd: Usd::new(dec!(20)),
        fee_usd: Usd::new(dec!(0.40)),
        net_profit_usd: Some(Usd::new(profit)),
    }
}

fn build_engine_with_persistence(
    persistence: Arc<CapturingPersistence>,
) -> (RiskEngine, MockMetrics) {
    let metrics = MockMetrics::healthy();
    let engine = RiskEngineBuilder::new()
        .config(test_config())
        .clock(utc_clock())
        .persistence(persistence)
        .initial_equity(Usd::new(dec!(5000)))
        .build(&metrics)
        .expect("engine should build");
    (engine, metrics)
}

// ── Tests ───────────────────────────────────────────────────────────────────

#[tokio::test]
async fn on_trade_result_emits_post_trade_update() {
    let persistence = Arc::new(CapturingPersistence::new());
    let (engine, metrics) = build_engine_with_persistence(Arc::clone(&persistence));

    let trade = test_trade(TradeOutcome::Success, dec!(5));
    engine
        .on_trade_result(TradeAccountingPhase::Fill, &trade, &metrics)
        .await
        .unwrap();

    let audits = persistence.take_audits();
    assert!(!audits.is_empty(), "expected at least one audit event");
}

#[tokio::test]
async fn add_blacklist_emits_blacklist_added() {
    let persistence = Arc::new(CapturingPersistence::new());
    let (engine, metrics) = build_engine_with_persistence(Arc::clone(&persistence));

    engine
        .add_blacklist(
            MarketId::new("0xblacklist_test"),
            BlacklistReason::Manual,
            &metrics,
        )
        .await
        .unwrap();

    let audits = persistence.take_audits();
    assert!(!audits.is_empty(), "expected BlacklistAdded audit event");
}

#[tokio::test]
async fn remove_blacklist_emits_blacklist_removed() {
    let persistence = Arc::new(CapturingPersistence::new());
    let (engine, metrics) = build_engine_with_persistence(Arc::clone(&persistence));

    let market_id = MarketId::new("0xremove_test");
    engine
        .add_blacklist(market_id.clone(), BlacklistReason::Manual, &metrics)
        .await
        .unwrap();

    persistence.take_audits(); // clear add events

    engine
        .remove_blacklist(&market_id, "test removal", &metrics)
        .await
        .unwrap();

    let audits = persistence.take_audits();
    assert!(!audits.is_empty(), "expected BlacklistRemoved audit event");
}

#[tokio::test]
async fn reset_circuit_breaker_emits_breaker_reset() {
    let persistence = Arc::new(CapturingPersistence::new());
    let (engine, metrics) = build_engine_with_persistence(Arc::clone(&persistence));

    engine
        .reset_circuit_breaker("operator test reset", &metrics)
        .await
        .unwrap();

    let audits = persistence.take_audits();
    assert!(!audits.is_empty(), "expected BreakerReset audit event");
}

#[tokio::test]
async fn halt_emits_engine_halted() {
    let persistence = Arc::new(CapturingPersistence::new());
    let (engine, _metrics) = build_engine_with_persistence(Arc::clone(&persistence));

    engine.halt("test halt reason".into()).await;

    let audits = persistence.take_audits();
    assert!(!audits.is_empty(), "expected EngineHalted audit event");
}

#[tokio::test]
async fn resume_emits_engine_resumed() {
    let persistence = Arc::new(CapturingPersistence::new());
    let (engine, _metrics) = build_engine_with_persistence(Arc::clone(&persistence));

    engine.halt("halt for resume test".into()).await;
    persistence.take_audits(); // clear halt events

    engine
        .acknowledge_and_resume("operator approved")
        .await
        .unwrap();

    let audits = persistence.take_audits();
    assert!(!audits.is_empty(), "expected EngineResumed audit event");
}
