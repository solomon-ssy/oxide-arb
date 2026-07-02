//! Subsystem health probes for operator dashboards.

use crate::{
    governance::RuntimeModeHandle, infra::health_alert_state::evaluate_ws_probe,
    service::catalog_readiness::CatalogReadiness,
};
use chrono::Utc;
use quant_pivot_api::ws::{ShardHealthSummary, WsShardHealthPort};
use quant_pivot_models::domain::{CatalogStatusPort, HealthReport, SubsystemHealth};
use quant_pivot_storage::{clickhouse::ClickHousePool, postgres::PostgresPool};
use std::sync::Arc;

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

#[cfg(test)]
mod tests {
    use super::*;
    use quant_pivot_models::{
        config::DeployConfig,
        domain::{SubsystemCheckStatus, WS_MARKET_DATA_STALE_THRESHOLD_MS},
    };
    use quant_pivot_test_support::{
        pg::setup_pg, storage::inert_clickhouse_pool, ws::FixedWsShardHealth,
    };

    fn checker_with(
        catalog: Arc<CatalogReadiness>,
        ws: Arc<dyn WsShardHealthPort>,
        pg: Arc<PostgresPool>,
        deploy: &DeployConfig,
    ) -> HealthChecker {
        HealthChecker::new(HealthCheckerDeps {
            pg_pool: pg,
            ch_pool: Arc::new(inert_clickhouse_pool(&deploy.db.clickhouse)),
            ws_health: ws,
            catalog,
            runtime_mode: RuntimeModeHandle::default(),
        })
    }

    #[tokio::test]
    async fn ws_skipped_during_catalog_warming() {
        let (pg, _container) = setup_pg().await;
        let deploy = DeployConfig::default();
        let checker = checker_with(
            Arc::new(CatalogReadiness::new()),
            FixedWsShardHealth::operational(),
            Arc::new(pg),
            &deploy,
        );

        let check = checker.check_ws();
        assert!(matches!(
            check.status,
            SubsystemCheckStatus::Skipped {
                reason
            } if reason == "catalog_warming"
        ));
    }

    #[tokio::test]
    async fn ws_skipped_while_market_data_connecting() {
        let (pg, _container) = setup_pg().await;
        let deploy = DeployConfig::default();
        let catalog = Arc::new(CatalogReadiness::new());
        catalog.mark_ready(1, Utc::now());
        let checker = checker_with(
            catalog,
            FixedWsShardHealth::with_message_age(None),
            Arc::new(pg),
            &deploy,
        );

        let check = checker.check_ws();
        assert!(matches!(
            check.status,
            SubsystemCheckStatus::Skipped {
                reason
            } if reason == "market_data_connecting"
        ));
    }

    #[tokio::test]
    async fn ws_reports_message_age_when_fresh() {
        let (pg, _container) = setup_pg().await;
        let deploy = DeployConfig::default();
        let catalog = Arc::new(CatalogReadiness::new());
        catalog.mark_ready(1, Utc::now());
        let checker = checker_with(
            catalog,
            FixedWsShardHealth::with_message_age(Some(42)),
            Arc::new(pg),
            &deploy,
        );

        let check = checker.check_ws();
        assert!(check.is_healthy());
        assert_eq!(check.latency_ms, Some(42));
    }

    #[tokio::test]
    async fn ws_unhealthy_when_message_stale() {
        let (pg, _container) = setup_pg().await;
        let deploy = DeployConfig::default();
        let catalog = Arc::new(CatalogReadiness::new());
        catalog.mark_ready(1, Utc::now());
        let checker = checker_with(
            catalog,
            FixedWsShardHealth::with_message_age(Some(WS_MARKET_DATA_STALE_THRESHOLD_MS)),
            Arc::new(pg),
            &deploy,
        );

        let check = checker.check_ws();
        assert!(!check.is_healthy());
        assert_eq!(check.latency_ms, Some(WS_MARKET_DATA_STALE_THRESHOLD_MS));
        assert!(
            check
                .detail
                .as_ref()
                .is_some_and(|detail| detail.contains("no message"))
        );
    }

    #[tokio::test]
    async fn ws_unhealthy_when_shards_disconnected() {
        let (pg, _container) = setup_pg().await;
        let deploy = DeployConfig::default();
        let catalog = Arc::new(CatalogReadiness::new());
        catalog.mark_ready(1, Utc::now());
        let checker = checker_with(
            catalog,
            FixedWsShardHealth::custom(
                ShardHealthSummary {
                    total: 2,
                    disconnected: 1,
                    oldest_disconnected_secs: Some(3),
                    connected_ratio_bps: 5_000,
                },
                Some(10),
            ),
            Arc::new(pg),
            &deploy,
        );

        let check = checker.check_ws();
        assert!(!check.is_healthy());
        assert_eq!(check.latency_ms, Some(10));
        assert!(
            check
                .detail
                .as_ref()
                .is_some_and(|detail| detail.contains("disconnected"))
        );
    }
}
