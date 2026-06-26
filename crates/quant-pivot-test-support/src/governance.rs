//! Governance / system-status integration harness helpers.

use std::sync::Arc;

use quant_pivot_core::{
    governance::RuntimeModeHandle,
    infra::health_checker::{HealthChecker, HealthCheckerDeps},
    service::catalog_readiness::CatalogReadiness,
};
use quant_pivot_models::config::DeployConfig;
use quant_pivot_storage::postgres::PostgresPool;

use crate::{storage::inert_clickhouse_pool, ws::FixedWsShardHealth};

/// Build a [`HealthChecker`] with catalog + market-data marked operational
/// (no live Gamma sync, CLOB websocket, or `ClickHouse` probe required).
#[must_use]
pub fn operational_health_checker(
    pg: Arc<PostgresPool>,
    runtime_mode: RuntimeModeHandle,
    deploy: &DeployConfig,
) -> Arc<HealthChecker> {
    let catalog = Arc::new(CatalogReadiness::new());
    catalog.mark_ready(1, chrono::Utc::now());
    Arc::new(HealthChecker::new(HealthCheckerDeps {
        pg_pool: pg,
        ch_pool: Arc::new(inert_clickhouse_pool(&deploy.db.clickhouse)),
        ws_health: FixedWsShardHealth::operational(),
        catalog,
        runtime_mode,
    }))
}
