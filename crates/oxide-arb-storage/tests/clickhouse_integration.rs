//! `ClickHouse` integration tests (requires Docker).

use chrono::Utc;
use oxide_arb_models::{
    clickhouse::{ChBps, ChPrice, ChSchemaVersion, ChUsd, TickEventRow},
    config::ClickHouseConfig,
    enums::clickhouse::{ChBookEventType, ChFactSource},
    types::{Price, TokenId, Usd},
};
use oxide_arb_storage::clickhouse::{BatchInserter, ChWriteMetrics, ClickHousePool};
use rust_decimal_macros::dec;
use std::{sync::Arc, time::Duration};
use testcontainers::{
    ImageExt,
    core::{WaitFor, wait::HttpWaitStrategy},
    runners::AsyncRunner,
};
use tokio_util::sync::CancellationToken;

fn test_ch_config(port: u16) -> ClickHouseConfig {
    ClickHouseConfig {
        url: format!("http://localhost:{port}"),
        database: "default".into(),
        user: "default".into(),
        password: String::new(),
        batch_size: 100,
        flush_interval_secs: 5,
        max_concurrent_inserts: 4,
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
    let pool = ClickHousePool::connect(&config).await.expect("connect");
    pool.ensure_schema().await.expect("schema");
    let client = pool.client().clone();
    (pool, client, port, container)
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn clickhouse_database_bootstrap_creates_missing_database() {
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
    let config = ClickHouseConfig {
        database: "oxide_arb_bootstrap_it".into(),
        ..test_ch_config(port)
    };

    let pool = ClickHousePool::connect(&config).await.expect("connect");
    pool.ensure_schema().await.expect("schema");

    let count: u64 = pool
        .client()
        .query("SELECT count() FROM system.databases WHERE name = ?")
        .bind("oxide_arb_bootstrap_it")
        .fetch_one()
        .await
        .expect("database should exist");
    assert_eq!(count, 1);
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

#[tokio::test]
#[ignore = "requires Docker"]
async fn clickhouse_fact_contract_uses_decimal_and_enum_columns() {
    let (_pool, client, _port, _container) = setup_clickhouse().await;
    let expected: [(&str, &[&str]); 4] = [
        (
            "tick_events",
            &[
                "`best_bid` Nullable(Decimal(18, 8))",
                "`last_trade_price` Nullable(Decimal(18, 8))",
                "'ShardStatus' = 7",
                "'WsShardStatus' = 11",
            ],
        ),
        (
            "book_l2_replay_hot",
            &[
                "`bid_prices` Array(Decimal(18, 8))",
                "`bid_sizes` Array(Decimal(38, 18))",
            ],
        ),
        (
            "opportunity_detection",
            &[
                "`entry_price` Decimal(18, 8)",
                "`expected_net_profit_usd` Decimal(38, 18)",
                "`fill_probability` Nullable(Decimal(18, 8))",
            ],
        ),
        (
            "opportunity_audit",
            &[
                "`trade_id` Nullable(String)",
                "`settlement_status` Nullable(Enum8('Won' = 1, 'Lost' = 2))",
                "`outcome` Nullable(Enum8('Rejected' = 1, 'Settled' = 2, 'Success' = 3, 'Miss' = 4, 'Failed' = 5))",
            ],
        ),
    ];

    for (table, fragments) in expected {
        let ddl: TableDdl = client
            .query(&format!("SHOW CREATE TABLE {table}"))
            .fetch_one()
            .await
            .unwrap_or_else(|e| panic!("SHOW CREATE TABLE {table} failed: {e}"));
        for fragment in fragments {
            assert!(
                ddl.statement.contains(fragment),
                "table {table} should contain `{fragment}`; got:\n{}",
                ddl.statement
            );
        }
    }
}

fn sample_tick(token_id: &str, received_at: i64) -> TickEventRow {
    TickEventRow {
        token_id: TokenId::new(token_id),
        market_id: None,
        event_type: ChBookEventType::Bbo,
        best_bid: Some(ChPrice::from(Price::new(dec!(0.95)))),
        best_ask: Some(ChPrice::from(Price::new(dec!(0.96)))),
        last_trade_price: None,
        bid_depth_usd: Some(ChUsd::from(Usd::new(dec!(1000)))),
        ask_depth_usd: Some(ChUsd::from(Usd::new(dec!(800)))),
        spread_bps: Some(ChBps::from(dec!(10))),
        book_version: 1,
        raw_payload_json: Some(r#"{"test":true}"#.into()),
        event_time: received_at,
        ingestion_time: received_at,
        sequence: 1,
        source: ChFactSource::WsBbo,
        schema_version: ChSchemaVersion(2),
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
