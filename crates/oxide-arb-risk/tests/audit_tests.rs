//! Audit event emission tests.
//!
//! Verifies that every critical engine operation produces the expected
//! `RiskAuditEvent` variant through a capturing mock persistence layer.

use chrono::Utc;
use oxide_arb_models::config::RiskConfig;
use oxide_arb_models::domain::BlacklistEntry;
use oxide_arb_models::domain::position::PositionInfo;
use oxide_arb_models::domain::risk::{EmergencySnapshot, RiskEngineSnapshot};
use oxide_arb_models::domain::trade::TradeRecord;
use oxide_arb_models::enums::common::{Side, TradeOutcome};
use oxide_arb_models::enums::risk::{BlacklistReason, TradeAccountingPhase};
use oxide_arb_models::types::{Bps, MarketId, TradeId, Usd};
use oxide_arb_risk::audit::RiskAuditEvent;
use oxide_arb_risk::builder::RiskEngineBuilder;
use oxide_arb_risk::clock::utc_clock;
use oxide_arb_risk::engine::RiskEngine;
use oxide_arb_risk::traits::{RiskMetrics, RiskPersistence};
use oxide_arb_risk::types::ReconciliationReport;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use std::sync::{Arc, Mutex};

// ── Mock Metrics ────────────────────────────────────────────────────────────

struct MockMetrics;

impl RiskMetrics for MockMetrics {
    fn total_exposure(&self) -> Usd {
        Usd::new(dec!(100))
    }
    fn market_exposure(&self, _market_id: &MarketId) -> Usd {
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
    fn open_directional_count(&self, _side: Side) -> usize {
        0
    }
    fn daily_directional_trades(&self, _side: Side) -> u32 {
        0
    }
    fn consecutive_market_misses(&self, _market_id: &MarketId) -> u32 {
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

// ── Capturing Persistence ───────────────────────────────────────────────────

struct CapturingPersistence {
    events: Mutex<Vec<RiskAuditEvent>>,
}

impl CapturingPersistence {
    const fn new() -> Self {
        Self {
            events: Mutex::new(Vec::new()),
        }
    }

    fn take_events(&self) -> Vec<RiskAuditEvent> {
        std::mem::take(&mut *self.events.lock().unwrap())
    }
}

#[async_trait::async_trait]
impl RiskPersistence for CapturingPersistence {
    async fn save_snapshot(
        &self,
        _snapshot: &RiskEngineSnapshot,
    ) -> oxide_arb_error::OxideResult<()> {
        Ok(())
    }
    async fn load_snapshot(&self) -> oxide_arb_error::OxideResult<Option<RiskEngineSnapshot>> {
        Ok(None)
    }
    async fn save_blacklist_entry(
        &self,
        _entry: &BlacklistEntry,
    ) -> oxide_arb_error::OxideResult<()> {
        Ok(())
    }
    async fn remove_blacklist_entry(
        &self,
        _market_id: &MarketId,
    ) -> oxide_arb_error::OxideResult<()> {
        Ok(())
    }
    async fn load_blacklist_entries(&self) -> oxide_arb_error::OxideResult<Vec<BlacklistEntry>> {
        Ok(Vec::new())
    }
    async fn save_emergency_snapshot(
        &self,
        _snapshot: &EmergencySnapshot,
    ) -> oxide_arb_error::OxideResult<()> {
        Ok(())
    }
    async fn save_reconciliation_report(
        &self,
        _report: &ReconciliationReport,
    ) -> oxide_arb_error::OxideResult<()> {
        Ok(())
    }
    async fn append_audit_event(&self, event: &RiskAuditEvent) -> oxide_arb_error::OxideResult<()> {
        self.events.lock().unwrap().push(event.clone());
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

fn test_trade(outcome: TradeOutcome, profit: Decimal) -> TradeRecord {
    TradeRecord {
        trade_id: TradeId::generate(),
        market_id: MarketId::new("0xaudit_market"),
        event_id: oxide_arb_models::types::EventId::new("audit_event"),
        token_id: oxide_arb_models::types::TokenId::new("audit_token"),
        status: outcome,
        detected_edge_bps: Bps::new(dec!(300)),
        detected_profit_usd: Usd::new(dec!(5)),
        total_cost_usd: Usd::new(dec!(20)),
        total_fees_usd: Usd::new(dec!(0.40)),
        total_gas_usd: Usd::ZERO,
        net_profit_usd: Usd::new(profit),
        net_profit_projected_usd: Usd::new(profit),
        detection_to_exec_ms: Some(100),
        tx_hash: None,
        confirmed_at: Some(Utc::now()),
        opportunity_snapshot: "{}".into(),
        validation_snapshot: None,
        execution_record: None,
        created_at: Utc::now(),
        updated_at: Utc::now(),
    }
}

async fn build_engine_with_persistence(persistence: Arc<CapturingPersistence>) -> RiskEngine {
    RiskEngineBuilder::new()
        .config(test_config())
        .clock(utc_clock())
        .persistence(persistence)
        .initial_equity(Usd::new(dec!(5000)))
        .build(&MockMetrics)
        .await
        .expect("engine should build")
}

// ── Tests ───────────────────────────────────────────────────────────────────

#[tokio::test]
async fn on_trade_result_emits_post_trade_update() {
    let persistence = Arc::new(CapturingPersistence::new());
    let engine = build_engine_with_persistence(Arc::clone(&persistence)).await;

    let trade = test_trade(TradeOutcome::Success, dec!(5));
    engine
        .on_trade_result(TradeAccountingPhase::Fill, &trade, &MockMetrics)
        .await
        .unwrap();

    let events = persistence.take_events();
    assert!(
        events.iter().any(|e| matches!(e, RiskAuditEvent::PostTradeUpdate { phase, .. } if *phase == TradeAccountingPhase::Fill)),
        "expected PostTradeUpdate(Fill), got: {events:?}"
    );
}

#[tokio::test]
async fn add_blacklist_emits_blacklist_added() {
    let persistence = Arc::new(CapturingPersistence::new());
    let engine = build_engine_with_persistence(Arc::clone(&persistence)).await;

    engine
        .add_blacklist(
            MarketId::new("0xblacklist_test"),
            BlacklistReason::Manual,
            &MockMetrics,
        )
        .await
        .unwrap();

    let events = persistence.take_events();
    assert!(
        events
            .iter()
            .any(|e| matches!(e, RiskAuditEvent::BlacklistAdded { .. })),
        "expected BlacklistAdded, got: {events:?}"
    );
}

#[tokio::test]
async fn remove_blacklist_emits_blacklist_removed() {
    let persistence = Arc::new(CapturingPersistence::new());
    let engine = build_engine_with_persistence(Arc::clone(&persistence)).await;

    let market_id = MarketId::new("0xremove_test");
    engine
        .add_blacklist(market_id.clone(), BlacklistReason::Manual, &MockMetrics)
        .await
        .unwrap();

    persistence.take_events(); // clear add events

    engine
        .remove_blacklist(&market_id, "test removal", &MockMetrics)
        .await
        .unwrap();

    let events = persistence.take_events();
    assert!(
        events
            .iter()
            .any(|e| matches!(e, RiskAuditEvent::BlacklistRemoved { .. })),
        "expected BlacklistRemoved, got: {events:?}"
    );
}

#[tokio::test]
async fn reset_circuit_breaker_emits_breaker_reset() {
    let persistence = Arc::new(CapturingPersistence::new());
    let engine = build_engine_with_persistence(Arc::clone(&persistence)).await;

    engine
        .reset_circuit_breaker("operator test reset", &MockMetrics)
        .await
        .unwrap();

    let events = persistence.take_events();
    assert!(
        events
            .iter()
            .any(|e| matches!(e, RiskAuditEvent::BreakerReset { .. })),
        "expected BreakerReset, got: {events:?}"
    );
}

#[tokio::test]
async fn halt_emits_engine_halted() {
    let persistence = Arc::new(CapturingPersistence::new());
    let engine = build_engine_with_persistence(Arc::clone(&persistence)).await;

    engine.halt("test halt reason".into()).await;

    let events = persistence.take_events();
    assert!(
        events.iter().any(
            |e| matches!(e, RiskAuditEvent::EngineHalted { reason } if reason == "test halt reason")
        ),
        "expected EngineHalted, got: {events:?}"
    );
}

#[tokio::test]
async fn resume_emits_engine_resumed() {
    let persistence = Arc::new(CapturingPersistence::new());
    let engine = build_engine_with_persistence(Arc::clone(&persistence)).await;

    engine.halt("halt for resume test".into()).await;
    persistence.take_events(); // clear halt events

    engine.resume().await.unwrap();

    let events = persistence.take_events();
    assert!(
        events
            .iter()
            .any(|e| matches!(e, RiskAuditEvent::EngineResumed)),
        "expected EngineResumed, got: {events:?}"
    );
}
