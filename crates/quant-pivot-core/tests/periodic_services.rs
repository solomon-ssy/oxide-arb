//! Periodic services wiring and Live startup gates.

#[path = "common/mod.rs"]
mod common;

use oxide_arb_core::{
    app::periodic_services::{ensure_live_metrics_ready, ledger_reconciliation_enabled},
    observability::metrics_hub::MetricsHub,
    service::risk_metrics::{ApiHealthTracker, RiskMetricsState},
};
use oxide_arb_models::enums::common::ExecutionMode;
use std::{sync::Arc, time::Duration};

fn metrics_state() -> Arc<RiskMetricsState> {
    Arc::new(RiskMetricsState::new(Arc::new(ApiHealthTracker::new(
        Duration::from_secs(60),
    ))))
}

#[tokio::test]
async fn live_mode_without_clob_client_fails_closed() {
    let refresher = common::disconnected_metrics_refresh(
        metrics_state(),
        ExecutionMode::Live,
        Arc::new(MetricsHub::new()),
    );
    let error = ensure_live_metrics_ready(ExecutionMode::Live, &refresher)
        .await
        .expect_err("Live without ClobClient must fail closed");
    assert!(
        error.to_string().contains("ClobClient"),
        "unexpected error: {error}"
    );
}

#[test]
fn ledger_reconciliation_is_live_only() {
    assert!(ledger_reconciliation_enabled(ExecutionMode::Live));
    assert!(!ledger_reconciliation_enabled(ExecutionMode::Paper));
    assert!(!ledger_reconciliation_enabled(ExecutionMode::DryRun));
}

#[tokio::test]
async fn paper_mode_skips_metrics_gate() {
    let refresher = common::disconnected_metrics_refresh(
        metrics_state(),
        ExecutionMode::Paper,
        Arc::new(MetricsHub::new()),
    );
    ensure_live_metrics_ready(ExecutionMode::Paper, &refresher)
        .await
        .expect("Paper mode must not hit the Live metrics gate");
}
