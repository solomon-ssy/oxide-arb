//! Boot rehydration and blocking-trade integrity tests.

use chrono::Utc;
use oxide_arb_core::{
    bridge::trading_gate::resume_trading,
    execution::{capital_manager::CapitalManager, fsm::ExecutionFSM},
    exposure::in_memory::InMemoryExposureReservation,
    observability::{alert_dispatcher::AlertDispatcher, metrics_hub::MetricsHub},
    runtime_config::RuntimeConfigStore,
    trade_integrity::TradeIntegrityStore,
};
use oxide_arb_error::{OxideError, trading::TradingError};
use oxide_arb_models::{
    domain::TradeInfo,
    enums::common::{ExecutionMode, MarketCategory, Side, TradeState},
    runtime_config::{NotificationConfig, RuntimeConfig},
    types::{
        EventId, ExecutionId, MarketId, OpportunityId, Price, ReservationId, Shares, TokenId,
        TradeId, Usd,
    },
};
use oxide_arb_repository::traits::TradeRepository;
use oxide_arb_risk::{builder::RiskEngineBuilder, clock::utc_clock, engine::RiskEngine};
use oxide_arb_test_support::{
    mocks::MockTradeRepository,
    risk::{TestRiskMetrics, test_risk_config},
};
use rust_decimal_macros::dec;
use std::sync::Arc;

fn sample_trade(state: TradeState) -> TradeInfo {
    let now = Utc::now();
    TradeInfo {
        trade_id: TradeId::from_v7(),
        execution_id: ExecutionId::from_v7(),
        reservation_id: ReservationId::from_v7(),
        opportunity_id: OpportunityId::from_v7(),
        market_id: MarketId::new("0xabc"),
        event_id: EventId::new("evt-1"),
        token_id: TokenId::new("12345"),
        side: Side::Buy,
        shares: Shares::new(dec!(10)),
        price: Price::new(dec!(0.92)),
        cost_usd: Usd::new(dec!(25)),
        fee_usd: Usd::new(dec!(0.5)),
        detected_edge_bps: None,
        detected_profit_usd: None,
        net_profit_usd: None,
        order_id: None,
        tx_hash: None,
        state,
        business_outcome: None,
        scored_snapshot: serde_json::json!({}),
        category: MarketCategory::Politics,
        needs_reconcile: false,
        reconcile_resolution: None,
        reconciled_at: None,
        reconcile_note: None,
        pre_submit_ctf_balance: None,
        reconcile_attempts: 0,
        reconcile_defer_until: None,
        post_trade_claim_owner: None,
        post_trade_claimed_at: None,
        post_trade_attempts: 0,
        execution_mode: ExecutionMode::Paper,
        latency_ms: None,
        error_message: None,
        submitted_at: Some(now),
        confirmed_at: None,
        created_at: now,
        updated_at: now,
    }
}

fn integrity_store(
    trade_repo: Arc<MockTradeRepository>,
    exposure: Arc<InMemoryExposureReservation>,
) -> TradeIntegrityStore {
    let metrics = Arc::new(MetricsHub::new());
    let alerts = Arc::new(AlertDispatcher::new(&NotificationConfig::default()));
    let fsm = Arc::new(ExecutionFSM::new(Arc::clone(&metrics), Arc::clone(&alerts)));
    TradeIntegrityStore::new(
        trade_repo as Arc<dyn TradeRepository>,
        exposure,
        fsm,
        Arc::new(RuntimeConfigStore::new(RuntimeConfig::default())),
        alerts,
    )
}

#[tokio::test]
async fn boot_rehydrate_restores_submitted_reservation_totals() {
    let trade_repo = Arc::new(MockTradeRepository::default());
    trade_repo.insert(sample_trade(TradeState::Submitted));
    let exposure = Arc::new(InMemoryExposureReservation::new(
        RuntimeConfig::default().risk.exposure_reservation_config(),
    ));
    let capital = Arc::new(CapitalManager::new(
        Arc::clone(&exposure),
        &RuntimeConfig::default().risk.exposure_reservation_config(),
    ));
    let store = integrity_store(Arc::clone(&trade_repo), Arc::clone(&exposure));

    store
        .boot_rehydrate(&capital)
        .await
        .expect("boot rehydrate");

    assert_eq!(exposure.total_reserved_usd_sync(), Usd::new(dec!(25)));
    assert_eq!(exposure.active_count_sync(), 1);
    assert_eq!(store.load().blocking_count, 1);
}

#[tokio::test]
async fn intent_orphan_counts_as_blocking() {
    let trade_repo = Arc::new(MockTradeRepository::default());
    trade_repo.insert(sample_trade(TradeState::Intent));
    let exposure = Arc::new(InMemoryExposureReservation::new(
        RuntimeConfig::default().risk.exposure_reservation_config(),
    ));
    let capital = Arc::new(CapitalManager::new(
        Arc::clone(&exposure),
        &RuntimeConfig::default().risk.exposure_reservation_config(),
    ));
    let store = integrity_store(Arc::clone(&trade_repo), Arc::clone(&exposure));

    store
        .boot_rehydrate(&capital)
        .await
        .expect("boot rehydrate");

    assert_eq!(store.load().blocking_count, 1);
    assert_eq!(store.load().intent_orphan_count, 1);
}

fn test_risk() -> RiskEngine {
    RiskEngineBuilder::new()
        .config(test_risk_config())
        .clock(utc_clock())
        .initial_equity(Usd::new(dec!(5000)))
        .build(&TestRiskMetrics)
        .expect("risk engine build")
}

#[tokio::test]
async fn resume_trading_blocked_when_blocking_trades_exist() {
    let trade_repo = Arc::new(MockTradeRepository::default());
    trade_repo.insert(sample_trade(TradeState::Submitted));
    let exposure = Arc::new(InMemoryExposureReservation::new(
        RuntimeConfig::default().risk.exposure_reservation_config(),
    ));
    let metrics = Arc::new(MetricsHub::new());
    let alerts = Arc::new(AlertDispatcher::new(&NotificationConfig::default()));
    let fsm = Arc::new(ExecutionFSM::new(Arc::clone(&metrics), Arc::clone(&alerts)));
    let store = integrity_store(Arc::clone(&trade_repo), Arc::clone(&exposure));
    store.refresh_async().await.expect("refresh");
    assert_eq!(store.load().blocking_count, 1);

    let risk = test_risk();
    let error = resume_trading(&risk, &fsm, &store, "operator ack")
        .await
        .expect_err("resume must fail while blocking trades exist");
    match error {
        OxideError::Trading(TradingError::BlockingTradesUnresolved { count }) => {
            assert_eq!(count, 1);
        }
        other => panic!("unexpected error: {other:?}"),
    }
    assert!(fsm.is_emergency() || store.load().blocking_count > 0);
}
