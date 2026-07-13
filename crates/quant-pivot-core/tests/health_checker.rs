//! Health checker WS probe integration tests (Postgres testcontainer for pool wiring).

use quant_pivot_api::ws::{ShardHealthSummary, WsShardHealthPort};
use quant_pivot_core::{
    governance::RuntimeModeHandle,
    infra::health_checker::{HealthChecker, HealthCheckerDeps},
    service::catalog_readiness::CatalogReadiness,
};
use quant_pivot_models::{
    config::DeployConfig,
    domain::{SubsystemCheckStatus, SubsystemHealth, WS_MARKET_DATA_STALE_THRESHOLD_MS},
};
use quant_pivot_storage::postgres::PostgresPool;
use quant_pivot_test_support::{pg::setup_pg, storage::inert_clickhouse_pool, ws::WsShardHealth};
use std::sync::Arc;

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

async fn ws_check(checker: &HealthChecker) -> SubsystemHealth {
    checker
        .check_all()
        .await
        .checks
        .into_iter()
        .find(|check| check.name == "websocket")
        .expect("websocket subsystem probe")
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn ws_skipped_during_catalog_warming() {
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

#[tokio::test]
#[ignore = "requires Docker"]
async fn ws_skipped_while_market_data_connecting() {
    let (pg, _container) = setup_pg().await;
    let deploy = DeployConfig::default();
    let catalog = Arc::new(CatalogReadiness::new());
    catalog.mark_ready(1, chrono::Utc::now());
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

#[tokio::test]
#[ignore = "requires Docker"]
async fn ws_reports_message_age_when_fresh() {
    let (pg, _container) = setup_pg().await;
    let deploy = DeployConfig::default();
    let catalog = Arc::new(CatalogReadiness::new());
    catalog.mark_ready(1, chrono::Utc::now());
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

#[tokio::test]
#[ignore = "requires Docker"]
async fn ws_unhealthy_when_message_stale() {
    let (pg, _container) = setup_pg().await;
    let deploy = DeployConfig::default();
    let catalog = Arc::new(CatalogReadiness::new());
    catalog.mark_ready(1, chrono::Utc::now());
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

#[tokio::test]
#[ignore = "requires Docker"]
async fn ws_unhealthy_when_shards_disconnected() {
    let (pg, _container) = setup_pg().await;
    let deploy = DeployConfig::default();
    let catalog = Arc::new(CatalogReadiness::new());
    catalog.mark_ready(1, chrono::Utc::now());
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
