//! Phase 0 bootstrap tests.

use quant_pivot_models::{domain::SystemStatus, enums::quant::QuantRuntimeMode};

#[test]
fn report_only_bootstrap_status_uses_quant_runtime_mode() {
    let status = SystemStatus::report_only_bootstrap(QuantRuntimeMode::ReportOnly);
    assert_eq!(status.quant_runtime_mode, QuantRuntimeMode::ReportOnly);
}

#[test]
fn quant_runtime_mode_blocks_orders_in_report_only() {
    assert!(!QuantRuntimeMode::ReportOnly.allows_order_submission());
    assert!(QuantRuntimeMode::AutoExecution.allows_order_submission());
}

#[test]
fn book_store_apply_snapshot_updates_published() {
    use quant_pivot_core::{
        observability::metrics_hub::MetricsHub, pipeline::book_store::BookStore,
    };
    use quant_pivot_models::{
        domain::market::book::BookLevel,
        types::{Price, Shares, TokenId},
    };
    use rust_decimal_macros::dec;
    use std::sync::Arc;

    let store = BookStore::new(Arc::new(MetricsHub::new()));
    let token = TokenId::new("42");
    let bids = Arc::from([BookLevel::from_decimal_unchecked(
        Price::new(dec!(0.50)),
        Shares::new(dec!(10)),
    )]);
    let asks = Arc::from([BookLevel::from_decimal_unchecked(
        Price::new(dec!(0.51)),
        Shares::new(dec!(10)),
    )]);
    store.apply_snapshot(&token, bids, asks, 1, None);
    let loaded = store.load(&token).expect("published snapshot");
    assert_eq!(loaded.bids.len(), 1);
    assert_eq!(loaded.asks.len(), 1);
}
