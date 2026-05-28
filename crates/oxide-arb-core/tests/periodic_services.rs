//! Periodic services wiring and Live startup gates.

use oxide_arb_core::app::periodic_services::ensure_live_metrics_ready;
use oxide_arb_models::enums::common::ExecutionMode;

#[tokio::test]
async fn live_mode_requires_metrics_refresher() {
    let error = ensure_live_metrics_ready(ExecutionMode::Live, None)
        .await
        .expect_err("Live without ClobClient must fail closed");
    assert!(
        error.to_string().contains("ClobClient"),
        "unexpected error: {error}"
    );
}

#[tokio::test]
async fn paper_mode_skips_metrics_gate() {
    ensure_live_metrics_ready(ExecutionMode::Paper, None)
        .await
        .expect("Paper mode should not require metrics refresher");
}
