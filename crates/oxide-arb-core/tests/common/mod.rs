//! Shared integration-test helpers for `oxide-arb-core`.

use oxide_arb_algorithm::calibration::ResolutionCalibrator;
use oxide_arb_core::{
    bridge::execution_mode::ExecutionModeHandle,
    observability::metrics_hub::MetricsHub,
    pipeline::{book_store::BookStore, market_registry::MarketRegistry},
    runtime_config::RuntimeConfigStore,
    service::{
        equity_valuator::EquityValuator,
        risk_metrics::{RiskMetricsRefreshDeps, RiskMetricsRefreshService, RiskMetricsState},
    },
};
use oxide_arb_models::{enums::common::ExecutionMode, runtime_config::RuntimeConfig};
use oxide_arb_repository::postgres::{PgPositionRepository, PgTradeRepository};
use sea_orm::DatabaseConnection;
use std::sync::Arc;

/// Mode-aware metrics refresher wired over disconnected Postgres handles and
/// no CLOB client.
///
/// Suitable for tests that only need the dependency present: callers tolerate
/// refresh failures (post-trade and settlement refreshes are best-effort), so
/// no database or venue needs to be stood up.
pub fn disconnected_metrics_refresh(
    state: Arc<RiskMetricsState>,
    mode: ExecutionMode,
    metrics: Arc<MetricsHub>,
) -> Arc<RiskMetricsRefreshService> {
    let equity_valuator = Arc::new(EquityValuator::new(
        Arc::new(MarketRegistry::new()),
        Arc::new(BookStore::new(Arc::clone(&metrics))),
        Arc::new(ResolutionCalibrator::empty(
            RuntimeConfig::default().detection.calibration,
        )),
    ));
    Arc::new(RiskMetricsRefreshService::new(RiskMetricsRefreshDeps {
        state,
        execution_mode: ExecutionModeHandle::new(mode),
        runtime_config: Arc::new(RuntimeConfigStore::new(RuntimeConfig::default())),
        clob_client: None,
        trade_repo: Arc::new(PgTradeRepository::new(DatabaseConnection::default())),
        position_repo: Arc::new(PgPositionRepository::new(DatabaseConnection::default())),
        equity_valuator,
        metrics,
    }))
}
