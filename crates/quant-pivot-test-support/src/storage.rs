//! Storage test doubles (never shipped).

use quant_pivot_models::config::ClickHouseConfig;
use quant_pivot_storage::clickhouse::ClickHousePool;

/// Inert `ClickHouse` pool: configured client handle without opening a connection.
///
/// Callers must not invoke [`ClickHousePool::health_check`] — use only when wiring
/// [`quant_pivot_core::infra::health_checker::HealthChecker`] for paths that never
/// probe analytics (e.g. governance `system_status` integration tests).
#[must_use]
pub fn inert_clickhouse_pool(config: &ClickHouseConfig) -> ClickHousePool {
    ClickHousePool::from_config(config)
}
