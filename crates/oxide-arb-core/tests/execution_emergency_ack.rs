//! Operator ack for non-auto-recoverable execution emergencies.

use oxide_arb_core::{
    execution::fsm::{EmergencyAckError, EmergencyClass, ExecutionFSM},
    exposure::in_memory::InMemoryExposureReservation,
    observability::{alert_dispatcher::AlertDispatcher, metrics_hub::MetricsHub},
    runtime_config::RuntimeConfigStore,
    trade_integrity::TradeIntegrityStore,
};
use oxide_arb_models::{
    runtime_config::{NotificationConfig, RuntimeConfig},
    types::Usd,
};
use oxide_arb_repository::traits::TradeRepository;
use oxide_arb_risk::{builder::RiskEngineBuilder, clock::utc_clock, engine::RiskEngine};
use oxide_arb_test_support::{
    mocks::MockTradeRepository,
    risk::{TestRiskMetrics, test_risk_config},
};
use rust_decimal_macros::dec;
use std::sync::Arc;

fn test_integrity() -> TradeIntegrityStore {
    let metrics = Arc::new(MetricsHub::new());
    let alerts = Arc::new(AlertDispatcher::new(&NotificationConfig::default()));
    let exposure = Arc::new(InMemoryExposureReservation::new(
        RuntimeConfig::default().risk.exposure_reservation_config(),
    ));
    let fsm = Arc::new(ExecutionFSM::new(Arc::clone(&metrics), Arc::clone(&alerts)));
    let trade_repo: Arc<dyn TradeRepository> = Arc::new(MockTradeRepository::default());
    TradeIntegrityStore::new(
        trade_repo,
        exposure,
        fsm,
        Arc::new(RuntimeConfigStore::new(RuntimeConfig::default())),
        alerts,
    )
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
async fn ack_operator_emergency_rejects_venue_fault() {
    let metrics = Arc::new(MetricsHub::new());
    let alerts = Arc::new(AlertDispatcher::new(&NotificationConfig::default()));
    let fsm = Arc::new(ExecutionFSM::new(metrics, alerts));
    fsm.enter_emergency(EmergencyClass::VenueFault, "test");
    let integrity = test_integrity();
    let risk = test_risk();
    let error = fsm
        .ack_operator_emergency(&integrity, &risk)
        .await
        .expect_err("venue fault must not use operator ack");
    assert!(matches!(error, EmergencyAckError::AutoRecoverable));
}

#[tokio::test]
async fn ack_operator_emergency_clears_reservation_fault() {
    let metrics = Arc::new(MetricsHub::new());
    let alerts = Arc::new(AlertDispatcher::new(&NotificationConfig::default()));
    let fsm = Arc::new(ExecutionFSM::new(metrics, alerts));
    fsm.enter_emergency(EmergencyClass::ReservationFault, "reservation leak");
    let integrity = test_integrity();
    let risk = test_risk();
    let class = fsm
        .ack_operator_emergency(&integrity, &risk)
        .await
        .expect("reservation fault ack");
    assert_eq!(class, EmergencyClass::ReservationFault);
    assert!(!fsm.is_emergency());
}
