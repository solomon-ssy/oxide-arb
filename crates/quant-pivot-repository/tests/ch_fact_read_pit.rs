//! `ClickHouse` point-in-time read integration tests (book tie-breaker + resolution).

use quant_pivot_models::{
    clickhouse::{BookSnapshotRow, ChPrice, ChSchemaVersion, ChUsd},
    config::ClickHouseConfig,
    enums::clickhouse::{ChFactSource, ChSnapshotReason},
    types::{MarketId, Price, TokenId},
};
use quant_pivot_repository::{
    clickhouse::ChQuantFactReadRepository, traits::QuantFactReadRepository,
};
use quant_pivot_storage::clickhouse::ClickHousePool;
use rust_decimal::Decimal;
use std::{sync::Arc, time::Duration};
use testcontainers::{
    ImageExt,
    core::{WaitFor, wait::HttpWaitStrategy},
    runners::AsyncRunner,
};

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
    Arc<ClickHousePool>,
    clickhouse::Client,
    testcontainers::ContainerAsync<testcontainers::GenericImage>,
) {
    let container = testcontainers::GenericImage::new("clickhouse/clickhouse-server", "24")
        .with_exposed_port(8123.into())
        .with_wait_for(WaitFor::http(
            HttpWaitStrategy::new("/ping")
                .with_port(8123.into())
                .with_expected_status_code(200u16),
        ))
        .with_startup_timeout(Duration::from_mins(2))
        .start()
        .await
        .expect("ClickHouse container");
    let port = container.get_host_port_ipv4(8123).await.expect("port");
    let pool = Arc::new(
        ClickHousePool::connect(&test_ch_config(port))
            .await
            .expect("connect"),
    );
    pool.ensure_schema().await.expect("schema");
    let client = pool.client().clone();
    (pool, client, container)
}

async fn insert_book_rows(client: &clickhouse::Client, rows: &[BookSnapshotRow]) {
    let mut insert = client
        .insert::<BookSnapshotRow>("book_snapshots")
        .await
        .expect("insert");
    for row in rows {
        insert.write(row).await.expect("write row");
    }
    insert.end().await.expect("end insert");
}

fn book_row(
    token: &str,
    event_time_ms: i64,
    ingestion_time_ms: i64,
    sequence: u64,
    mid: Decimal,
) -> BookSnapshotRow {
    BookSnapshotRow {
        token_id: TokenId::new(token),
        market_id: Some(MarketId::new("0xchpit")),
        snapshot_reason: ChSnapshotReason::Startup,
        top_n: 5,
        bids_json: r#"[["0.48","100"]]"#.to_owned(),
        asks_json: r#"[["0.52","100"]]"#.to_owned(),
        bid_depth_usd: Some(ChUsd::from(Decimal::from(500))),
        ask_depth_usd: None,
        mid_price: Some(ChPrice::from(Price::new(mid))),
        spread_bps: None,
        book_version: 1,
        levels_count: 1,
        event_time: event_time_ms,
        ingestion_time: ingestion_time_ms,
        sequence,
        source: ChFactSource::WsSnapshot,
        schema_version: ChSchemaVersion(2),
    }
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn ch_read_orders_by_event_time_with_tiebreaker() {
    let (pool, client, _container) = setup_clickhouse().await;
    let read = ChQuantFactReadRepository::new(pool);
    let token = TokenId::new("ch-pit-yes");
    let event_time = 1_700_000_000_000_i64;

    insert_book_rows(
        &client,
        &[
            book_row(
                token.as_str(),
                event_time,
                event_time + 1,
                1,
                Decimal::new(49, 2),
            ),
            book_row(
                token.as_str(),
                event_time,
                event_time + 2,
                1,
                Decimal::new(50, 2),
            ),
        ],
    )
    .await;

    let row = read
        .book_snapshot_at(&token, event_time + 5)
        .await
        .expect("read")
        .expect("snapshot");
    assert_eq!(
        row.mid_price.map(ChPrice::to_price),
        Some(Price::new(Decimal::new(50, 2))),
        "tie-breaker must prefer later ingestion_time at same event_time"
    );
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn resolution_at_is_pit_bounded() {
    use quant_pivot_models::clickhouse::MarketResolutionRow;

    let (pool, client, _container) = setup_clickhouse().await;
    let read = ChQuantFactReadRepository::new(pool);
    let market_id = MarketId::new("0xchpit-res");
    let yes = TokenId::new("ch-pit-yes");
    let no = TokenId::new("ch-pit-no");

    let early = 1_700_000_010_000_i64;
    let late = 1_700_000_020_000_i64;
    let as_of = early + 5_000;

    let rows = vec![
        MarketResolutionRow {
            market_id: market_id.clone(),
            winning_token_id: yes.clone(),
            winning_outcome: "Yes".to_owned(),
            asset_token_ids: vec![yes.clone(), no.clone()],
            resolved_at: early,
            observed_at: early,
            source: ChFactSource::WsMarketResolved,
            sequence: 1,
            schema_version: ChSchemaVersion(2),
        },
        MarketResolutionRow {
            market_id: market_id.clone(),
            winning_token_id: no.clone(),
            winning_outcome: "No".to_owned(),
            asset_token_ids: vec![yes.clone(), no.clone()],
            resolved_at: late,
            observed_at: late,
            source: ChFactSource::WsMarketResolved,
            sequence: 2,
            schema_version: ChSchemaVersion(2),
        },
    ];
    let mut insert = client
        .insert::<MarketResolutionRow>("market_resolution_event")
        .await
        .expect("insert");
    for row in &rows {
        insert.write(row).await.expect("write");
    }
    insert.end().await.expect("end");

    let resolved = read
        .resolution_at(&market_id, as_of)
        .await
        .expect("read")
        .expect("resolution");
    assert_eq!(resolved.resolved_at, early);
    assert_eq!(resolved.winning_token_id, yes);
}
