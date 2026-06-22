//! Subsystem health probes for operator dashboards.

use crate::{governance::RuntimeModeHandle, service::catalog_readiness::CatalogReadiness};
use chrono::Utc;
use quant_pivot_api::ws::ClobWsManager;
use quant_pivot_models::domain::{HealthReport, SubsystemHealth};
use quant_pivot_storage::{clickhouse::ClickHousePool, postgres::PostgresPool};
use std::sync::Arc;

/// Construction dependencies for [`HealthChecker`].
pub struct HealthCheckerDeps {
    pub pg_pool: Arc<PostgresPool>,
    pub ch_pool: Arc<ClickHousePool>,
    pub ws_manager: Arc<ClobWsManager>,
    pub catalog: Arc<CatalogReadiness>,
    pub runtime_mode: RuntimeModeHandle,
}

pub struct HealthChecker {
    pg_pool: Arc<PostgresPool>,
    ch_pool: Arc<ClickHousePool>,
    ws_manager: Arc<ClobWsManager>,
    catalog: Arc<CatalogReadiness>,
    runtime_mode: RuntimeModeHandle,
}

impl HealthChecker {
    pub fn new(deps: HealthCheckerDeps) -> Self {
        Self {
            pg_pool: deps.pg_pool,
            ch_pool: deps.ch_pool,
            ws_manager: deps.ws_manager,
            catalog: deps.catalog,
            runtime_mode: deps.runtime_mode,
        }
    }

    pub async fn check_all(&self) -> HealthReport {
        let (pg, ch, ws) = tokio::join!(self.check_postgres(), self.check_clickhouse(), async {
            self.check_ws()
        },);
        HealthReport::from_checks(vec![pg, ch, ws], Utc::now())
    }

    async fn check_postgres(&self) -> SubsystemHealth {
        let started = std::time::Instant::now();
        match self.pg_pool.health_check().await {
            Ok(()) => SubsystemHealth::healthy(
                "postgres",
                Some(started.elapsed().as_millis().try_into().unwrap_or(u64::MAX)),
            ),
            Err(error) => SubsystemHealth::unhealthy(
                "postgres",
                Some(started.elapsed().as_millis().try_into().unwrap_or(u64::MAX)),
                error.to_string(),
            ),
        }
    }

    async fn check_clickhouse(&self) -> SubsystemHealth {
        let started = std::time::Instant::now();
        match self.ch_pool.health_check().await {
            Ok(()) => SubsystemHealth::healthy(
                "clickhouse",
                Some(started.elapsed().as_millis().try_into().unwrap_or(u64::MAX)),
            ),
            Err(error) => SubsystemHealth::unhealthy(
                "clickhouse",
                Some(started.elapsed().as_millis().try_into().unwrap_or(u64::MAX)),
                error.to_string(),
            ),
        }
    }

    fn check_ws(&self) -> SubsystemHealth {
        let shards = self.ws_manager.shard_health();
        if shards.disconnected > 0 {
            SubsystemHealth::unhealthy("websocket", None, shards.to_string())
        } else {
            SubsystemHealth::healthy("websocket", None)
        }
    }

    #[must_use]
    pub fn catalog(&self) -> Arc<CatalogReadiness> {
        Arc::clone(&self.catalog)
    }

    #[must_use]
    pub fn runtime_mode(&self) -> RuntimeModeHandle {
        self.runtime_mode.clone()
    }
}
