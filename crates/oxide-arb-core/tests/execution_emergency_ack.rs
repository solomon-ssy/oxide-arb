//! Operator ack for non-auto-recoverable execution emergencies.

use oxide_arb_core::{
    execution::{
        fsm::{EmergencyAckError, EmergencyClass, ExecutionFSM},
        trade_safety_gate::TradeSafetyGate,
    },
    observability::{alert_dispatcher::AlertDispatcher, metrics_hub::MetricsHub},
};
use oxide_arb_models::{runtime_config::NotificationConfig, types::Usd};
use oxide_arb_risk::{builder::RiskEngineBuilder, clock::utc_clock, engine::RiskEngine};
use oxide_arb_test_support::{
    mocks::MockTradeRepository,
    risk::{TestRiskMetrics, test_risk_config},
};
use rust_decimal_macros::dec;
use std::sync::Arc;

fn test_fsm() -> ExecutionFSM {
    let metrics = Arc::new(MetricsHub::new());
    let alerts = Arc::new(AlertDispatcher::new(&NotificationConfig::default()));
    ExecutionFSM::new(metrics, alerts)
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
    let fsm = test_fsm();
    fsm.enter_emergency(EmergencyClass::VenueFault, "test");
    let gate = TradeSafetyGate::new(Arc::new(MockTradeRepository::default()));
    let risk = test_risk();
    let error = fsm
        .ack_operator_emergency(&gate, &risk)
        .await
        .expect_err("venue fault must not use operator ack");
    assert!(matches!(error, EmergencyAckError::AutoRecoverable));
}

#[tokio::test]
async fn ack_operator_emergency_clears_reservation_fault() {
    let fsm = test_fsm();
    fsm.enter_emergency(EmergencyClass::ReservationFault, "reservation leak");
    let gate = TradeSafetyGate::new(Arc::new(MockTradeRepository::default()));
    let risk = test_risk();
    let class = fsm
        .ack_operator_emergency(&gate, &risk)
        .await
        .expect("reservation fault ack");
    assert_eq!(class, EmergencyClass::ReservationFault);
    assert!(!fsm.is_emergency());
}
