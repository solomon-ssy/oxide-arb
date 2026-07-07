//! Phase 0 bootstrap tests.

use quant_pivot_core::{ingest::book_store::BookStore, observability::metrics_hub::MetricsHub};
use quant_pivot_models::{
    domain::{SystemStatus, market::book::BookLevel},
    enums::quant::QuantRuntimeMode,
    types::{Price, Shares, TokenId},
};
use rust_decimal_macros::dec;
use std::sync::Arc;

#[test]
fn bootstrap_status_uses_quant_runtime_mode() {
    let status = SystemStatus::bootstrap(QuantRuntimeMode::ReportOnly);
    assert_eq!(status.quant_runtime_mode, QuantRuntimeMode::ReportOnly);
}

#[test]
fn quant_runtime_mode_blocks_orders_in_report_only() {
    assert!(!QuantRuntimeMode::ReportOnly.allows_order_submission());
    assert!(QuantRuntimeMode::AutoExecution.allows_order_submission());
}

#[test]
fn book_store_apply_snapshot_updates_published() {
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
