//! `ClickHouse` integration tests (requires Docker).

use chrono::Utc;
use oxide_arb_models::{clickhouse::TickEventRow, config::AnalyticsConfig};
use oxide_arb_storage::clickhouse::{BatchInserter, ChWriteMetrics, ClickHousePool};
use std::{sync::Arc, time::Duration};
use testcontainers::{
    ImageExt,
    core::{WaitFor, wait::HttpWaitStrategy},
    runners::AsyncRunner,
};
use tokio_util::sync::CancellationToken;

fn test_ch_config(port: u16) -> AnalyticsConfig {
    AnalyticsConfig {
        clickhouse_url: format!("http://localhost:{port}"),
        clickhouse_database: "default".into(),
        clickhouse_user: "default".into(),
        clickhouse_password: String::new(),
        batch_size: 100,
        flush_interval_secs: 5,
        max_concurrent_inserts: 4,
        max_lag_secs: 10.0,
        lag_probe_interval_secs: 5,
    }
}

async fn setup_clickhouse() -> (
    ClickHousePool,
    clickhouse::Client,
    u16,
    testcontainers::ContainerAsync<testcontainers::GenericImage>,
) {
    let container = testcontainers::GenericImage::new("clickhouse/clickhouse-server", "24")
        .with_exposed_port(8123.into())
        .with_wait_for(WaitFor::http(
            HttpWaitStrategy::new("/ping")
                .with_port(8123.into())
                .with_expected_status_code(200u16),
        ))
        .with_startup_timeout(Duration::from_secs(120))
        .start()
        .await
        .expect("ClickHouse container");
    let port = container.get_host_port_ipv4(8123).await.expect("port");
    let config = test_ch_config(port);
    let pool = ClickHousePool::connect(&config).expect("connect");
    pool.ensure_schema().await.expect("schema");
    let client = pool.client().clone();
    (pool, client, port, container)
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn clickhouse_health_check() {
    let (pool, _client, _port, _container) = setup_clickhouse().await;
    pool.health_check().await.expect("health check should pass");
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn clickhouse_schema_idempotent() {
    let (pool, _client, _port, _container) = setup_clickhouse().await;
    pool.ensure_schema()
        .await
        .expect("second schema creation should be idempotent");
}

#[derive(clickhouse::Row, serde::Deserialize)]
struct TableDdl {
    statement: String,
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn clickhouse_table_ttl_policies() {
    let (_pool, client, _port, _container) = setup_clickhouse().await;

    let expected: [(&str, [&str; 2]); 4] = [
        (
            "tick_events",
            [
                "event_date + INTERVAL 90 DAY",
                "event_date + toIntervalDay(90)",
            ],
        ),
        (
            "book_snapshots",
            [
                "snapshot_date + INTERVAL 180 DAY",
                "snapshot_date + toIntervalDay(180)",
            ],
        ),
        (
            "opportunity_audit",
            [
                "audit_date + INTERVAL 365 DAY",
                "audit_date + toIntervalDay(365)",
            ],
        ),
        (
            "calibration_snapshots",
            [
                "snapshot_date + INTERVAL 365 DAY",
                "snapshot_date + toIntervalDay(365)",
            ],
        ),
    ];

    for (table, ttl_fragments) in expected {
        let ddl: TableDdl = client
            .query(&format!("SHOW CREATE TABLE {table}"))
            .fetch_one()
            .await
            .unwrap_or_else(|e| panic!("SHOW CREATE TABLE {table} failed: {e}"));
        assert!(
            ddl.statement.contains("TTL")
                && ttl_fragments
                    .iter()
                    .any(|fragment| ddl.statement.contains(fragment)),
            "table {table} should define the expected TTL; got:\n{}",
            ddl.statement
        );
    }
}

fn sample_tick(token_id: &str, received_at: i64) -> TickEventRow {
    TickEventRow {
        token_id: token_id.into(),
        event_type: 1,
        best_bid: 0.95,
        best_ask: 0.96,
        bid_depth_usd: 1000.0,
        ask_depth_usd: 800.0,
        spread_bps: 10,
        raw_payload: r#"{"test":true}"#.into(),
        received_at,
    }
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn tick_events_direct_insert_roundtrip() {
    let (_pool, client, _port, _container) = setup_clickhouse().await;
    let row = sample_tick("tok-direct", Utc::now().timestamp_millis());
    let mut insert = client
        .insert::<TickEventRow>("tick_events")
        .await
        .expect("insert start");
    insert.write(&row).await.expect("write row");
    insert.end().await.expect("end insert");

    let count: u64 = client
        .query("SELECT count() FROM tick_events WHERE token_id = 'tok-direct'")
        .fetch_one()
        .await
        .expect("count");
    assert_eq!(count, 1);
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn batch_inserter_shutdown_drains_buffer() {
    let (_pool, client, _port, _container) = setup_clickhouse().await;
    let shutdown = CancellationToken::new();
    let metrics = Arc::new(ChWriteMetrics::new());

    let inserter = BatchInserter::new(
        client.clone(),
        "tick_events",
        10_000,
        Duration::from_secs(3600),
        metrics.clone(),
        shutdown.clone(),
    );

    let now = Utc::now().timestamp_millis();
    for i in 0..3 {
        inserter
            .insert(sample_tick(&format!("tok-drain-{i}"), now + i * 1000))
            .await
            .expect("enqueue");
    }

    shutdown.cancel();
    tokio::time::sleep(Duration::from_millis(500)).await;

    let count: u64 = client
        .query("SELECT count() FROM tick_events WHERE token_id LIKE 'tok-drain-%'")
        .fetch_one()
        .await
        .expect("count rows");
    assert_eq!(count, 3, "shutdown should flush all buffered rows");
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn batch_inserter_channel_close_drains_buffer() {
    let (_pool, client, _port, _container) = setup_clickhouse().await;
    let shutdown = CancellationToken::new();
    let metrics = Arc::new(ChWriteMetrics::new());

    let inserter = BatchInserter::new(
        client.clone(),
        "tick_events",
        10_000,
        Duration::from_secs(3600),
        metrics,
        shutdown,
    );

    let now = Utc::now().timestamp_millis();
    inserter
        .insert(sample_tick("tok-close-1", now))
        .await
        .unwrap();
    inserter
        .insert(sample_tick("tok-close-2", now + 1_000))
        .await
        .unwrap();

    inserter.shutdown();
    tokio::time::sleep(Duration::from_millis(500)).await;

    let count: u64 = client
        .query("SELECT count() FROM tick_events WHERE token_id LIKE 'tok-close-%'")
        .fetch_one()
        .await
        .expect("count");
    assert_eq!(count, 2, "dropping sender should flush buffered rows");
}
