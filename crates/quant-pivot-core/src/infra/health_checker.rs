//! Subsystem health probes for operator dashboards.

use std::{sync::Arc, time::Instant};

use chrono::Utc;
use quant_pivot_api::ws::{ShardHealthSummary, WsShardHealthPort};
use quant_pivot_models::domain::{
    governance::{HealthReport, SubsystemHealth},
    ports::CatalogStatusPort,
};
use quant_pivot_storage::{clickhouse::ClickHousePool, postgres::PostgresPool};

use crate::{
    governance::RuntimeModeHandle, infra::health_alert_state::evaluate_ws_probe,
    service::catalog_readiness::CatalogReadiness,
};

/// Construction dependencies for [`HealthChecker`].
pub struct HealthCheckerDeps {
    pub pg_pool: Arc<PostgresPool>,
    pub ch_pool: Arc<ClickHousePool>,
    pub ws_health: Arc<dyn WsShardHealthPort>,
    pub catalog: Arc<CatalogReadiness>,
    pub runtime_mode: RuntimeModeHandle,
}

pub struct HealthChecker {
    pg_pool: Arc<PostgresPool>,
    ch_pool: Arc<ClickHousePool>,
    ws_health: Arc<dyn WsShardHealthPort>,
    catalog: Arc<CatalogReadiness>,
    runtime_mode: RuntimeModeHandle,
}

impl HealthChecker {
    pub fn new(deps: HealthCheckerDeps) -> Self {
        Self {
            pg_pool: deps.pg_pool,
            ch_pool: deps.ch_pool,
            ws_health: deps.ws_health,
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
        let started = Instant::now();
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
        let started = Instant::now();
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
        if !self.catalog.is_ready() {
            return SubsystemHealth::skipped("websocket", "catalog_warming");
        }

        let shards = self.ws_health.shard_health();
        let age = self.ws_health.last_message_age_ms();

        if age.is_none() {
            return SubsystemHealth::skipped("websocket", "market_data_connecting");
        }

        if shards.disconnected > 0 {
            return SubsystemHealth::unhealthy("websocket", age, shards.to_string());
        }

        evaluate_ws_probe(age, shards)
    }

    #[must_use]
    pub fn catalog(&self) -> Arc<CatalogReadiness> {
        Arc::clone(&self.catalog)
    }

    /// Current CLOB websocket shard connectivity snapshot (for system status).
    #[must_use]
    pub fn ws_shard_health(&self) -> ShardHealthSummary {
        self.ws_health.shard_health()
    }

    /// Milliseconds since the last CLOB websocket message on any shard.
    #[must_use]
    pub fn ws_last_message_age_ms(&self) -> Option<u64> {
        self.ws_health.last_message_age_ms()
    }

    #[must_use]
    pub fn runtime_mode(&self) -> RuntimeModeHandle {
        self.runtime_mode.clone()
    }
}
