//! `ClickHouse` timeseries repository integration tests (requires Docker).

use chrono::Utc;
use oxide_arb_models::clickhouse::TickEventRow;
use oxide_arb_models::config::AnalyticsConfig;
use oxide_arb_repository::clickhouse::ChTimeseriesRepository;
use oxide_arb_repository::traits::TimeseriesRepository;
use oxide_arb_storage::clickhouse::{ChWriteManager, ClickHousePool};
use std::sync::Arc;
use std::time::Duration;
use tokio_util::sync::CancellationToken;

fn test_ch_config(port: u16) -> AnalyticsConfig {
    AnalyticsConfig {
        clickhouse_url: format!("http://localhost:{port}"),
        clickhouse_database: "default".into(),
        clickhouse_user: "default".into(),
        clickhouse_password: String::new(),
        batch_size: 10,
        flush_interval_secs: 1,
        max_concurrent_inserts: 4,
        max_lag_secs: 10.0,
        lag_probe_interval_secs: 5,
    }
}

async fn setup_timeseries_repo() -> (
    ChTimeseriesRepository,
    CancellationToken,
    testcontainers::ContainerAsync<testcontainers::GenericImage>,
) {
    use testcontainers::runners::AsyncRunner;
    let container = testcontainers::GenericImage::new("clickhouse/clickhouse-server", "24")
        .with_exposed_port(8123.into())
        .with_wait_for(testcontainers::core::WaitFor::message_on_stderr(
            "Ready for connections",
        ))
        .start()
        .await
        .expect("ClickHouse container");
    let port = container.get_host_port_ipv4(8123).await.expect("port");
    let config = test_ch_config(port);
    let pool = ClickHousePool::connect(&config).expect("connect");
    pool.ensure_schema().await.expect("schema");

    let shutdown = CancellationToken::new();
    let write_manager = Arc::new(ChWriteManager::new_without_probe(
        config.max_concurrent_inserts,
    ));
    let repo = ChTimeseriesRepository::new(
        pool.client().clone(),
        &config,
        write_manager,
        shutdown.clone(),
    );

    (repo, shutdown, container)
}

fn sample_tick(token_id: &str, ts: i64) -> TickEventRow {
    TickEventRow {
        token_id: token_id.into(),
        event_type: 1,
        best_bid: 0.94,
        best_ask: 0.95,
        bid_depth_usd: 500.0,
        ask_depth_usd: 400.0,
        spread_bps: 10,
        raw_payload: "{}".into(),
        received_at: ts,
    }
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn timeseries_insert_and_query_roundtrip() {
    let (repo, _shutdown, _container) = setup_timeseries_repo().await;
    let now = Utc::now().timestamp();
    let token = "tok-roundtrip";

    repo.insert_tick_events(&[
        sample_tick(token, now - 2),
        sample_tick(token, now - 1),
        sample_tick(token, now),
    ])
    .await
    .expect("insert");

    tokio::time::sleep(Duration::from_secs(2)).await;

    let rows = repo
        .query_tick_events(
            token,
            Utc::now() - chrono::Duration::minutes(5),
            Utc::now() + chrono::Duration::minutes(1),
            10,
        )
        .await
        .expect("query");

    assert_eq!(rows.len(), 3, "expected all inserted tick events");
    assert_eq!(rows[0].token_id, token);
}
