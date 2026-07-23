//! Health readiness system contracts across `PostgreSQL` and WebSocket probes.

use std::sync::Arc;

use chrono::Utc;
use quant_pivot_api::ws::{ShardHealthSummary, WsShardHealthPort};
use quant_pivot_core::{
    governance::RuntimeControlsHandle,
    infra::health_checker::{HealthChecker, HealthCheckerDeps},
    service::catalog_readiness::CatalogReadiness,
};
use quant_pivot_models::{
    config::DeployConfig,
    domain::governance::{
        SubsystemCheckStatus, SubsystemHealth, WS_MARKET_DATA_STALE_THRESHOLD_MS,
    },
};
use quant_pivot_storage::postgres::PostgresPool;
use quant_pivot_system_tests::{
    postgres::setup_pg,
    support::{storage::inert_clickhouse_pool, ws::WsShardHealth},
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
        runtime_controls: RuntimeControlsHandle::default(),
    })
}

async fn ws_check(checker: &HealthChecker) -> SubsystemHealth {
    checker
        .check_all()
        .await
        .checks
        .into_iter()
        .find(|check| check.name == "websocket")
        .expect("websocket subsystem probe")
}

pub async fn ws_skipped_during_catalog_warming() {
    let (pg, _container) = setup_pg().await;
    let deploy = DeployConfig::default();
    let checker = checker_with(
        Arc::new(CatalogReadiness::new()),
        WsShardHealth::operational(),
        Arc::new(pg),
        &deploy,
    );

    let check = ws_check(&checker).await;
    assert!(matches!(
        check.status,
        SubsystemCheckStatus::Skipped {
            reason
        } if reason == "catalog_warming"
    ));
}

pub async fn ws_skipped_while_market_data_connecting() {
    let (pg, _container) = setup_pg().await;
    let deploy = DeployConfig::default();
    let catalog = Arc::new(CatalogReadiness::new());
    catalog.mark_ready(1, Utc::now());
    let checker = checker_with(
        catalog,
        WsShardHealth::with_message_age(None),
        Arc::new(pg),
        &deploy,
    );

    let check = ws_check(&checker).await;
    assert!(matches!(
        check.status,
        SubsystemCheckStatus::Skipped {
            reason
        } if reason == "market_data_connecting"
    ));
}

pub async fn ws_reports_message_age_when_fresh() {
    let (pg, _container) = setup_pg().await;
    let deploy = DeployConfig::default();
    let catalog = Arc::new(CatalogReadiness::new());
    catalog.mark_ready(1, Utc::now());
    let checker = checker_with(
        catalog,
        WsShardHealth::with_message_age(Some(42)),
        Arc::new(pg),
        &deploy,
    );

    let check = ws_check(&checker).await;
    assert!(check.is_healthy());
    assert_eq!(check.latency_ms, Some(42));
}

pub async fn ws_unhealthy_when_message_stale() {
    let (pg, _container) = setup_pg().await;
    let deploy = DeployConfig::default();
    let catalog = Arc::new(CatalogReadiness::new());
    catalog.mark_ready(1, Utc::now());
    let checker = checker_with(
        catalog,
        WsShardHealth::with_message_age(Some(WS_MARKET_DATA_STALE_THRESHOLD_MS)),
        Arc::new(pg),
        &deploy,
    );

    let check = ws_check(&checker).await;
    assert!(!check.is_healthy());
    assert_eq!(check.latency_ms, Some(WS_MARKET_DATA_STALE_THRESHOLD_MS));
    assert!(
        check
            .detail
            .as_ref()
            .is_some_and(|detail| detail.contains("no message"))
    );
}

pub async fn ws_unhealthy_when_shards_disconnected() {
    let (pg, _container) = setup_pg().await;
    let deploy = DeployConfig::default();
    let catalog = Arc::new(CatalogReadiness::new());
    catalog.mark_ready(1, Utc::now());
    let checker = checker_with(
        catalog,
        WsShardHealth::custom(
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

    let check = ws_check(&checker).await;
    assert!(!check.is_healthy());
    assert_eq!(check.latency_ms, Some(10));
    assert!(
        check
            .detail
            .as_ref()
            .is_some_and(|detail| detail.contains("disconnected"))
    );
}
