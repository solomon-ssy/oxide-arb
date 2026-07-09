//! `ClickHouse` point-in-time read integration tests (book tie-breaker + resolution).

use chrono::Utc;
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

/// Wait until inserted `book_snapshots` rows are query-visible.
///
/// Fresh MergeTree parts can lag briefly behind HTTP insert ack on a cold
/// testcontainer; PIT reads that race this window return `None`.
async fn wait_for_book_snapshot_rows(client: &clickhouse::Client, token: &TokenId, expected: u64) {
    const ATTEMPTS: usize = 40;
    const PAUSE: Duration = Duration::from_millis(50);
    for attempt in 1..=ATTEMPTS {
        let count: u64 = client
            .query("SELECT count() FROM book_snapshots WHERE token_id = ?")
            .bind(token.clone())
            .fetch_one()
            .await
            .expect("count book_snapshots");
        if count >= expected {
            return;
        }
        if attempt == ATTEMPTS {
            panic!(
                "book_snapshots rows for {} not visible after insert \
                 (count={count}, expected>={expected}, attempts={ATTEMPTS})",
                token.as_str()
            );
        }
        tokio::time::sleep(PAUSE).await;
    }
}

/// Epoch millis inside the `book_snapshots` 180-day TTL window.
///
/// Fixed 2023 fixtures are deleted by `TTL snapshot_date + INTERVAL 180 DAY`
/// once wall-clock time advances past that horizon (observed as intermittent
/// `book_snapshot_at` → `None`).
fn fresh_event_time_ms() -> i64 {
    Utc::now().timestamp_millis()
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
        schema_version: ChSchemaVersion::FIRST,
    }
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn ch_read_orders_by_event_time_with_tiebreaker() {
    let (pool, client, _container) = setup_clickhouse().await;
    let read = ChQuantFactReadRepository::new(pool);
    let token = TokenId::new("ch-pit-yes");
    let event_time = fresh_event_time_ms();

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
    wait_for_book_snapshot_rows(&client, &token, 2).await;

    let row = read
        .book_snapshot_at(&token, event_time + 5)
        .await
        .expect("read")
        .unwrap_or_else(|| {
            panic!(
                "PIT book_snapshot_at returned None for token={} as_of={}",
                token.as_str(),
                event_time + 5
            )
        });
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

    let early = fresh_event_time_ms();
    let late = early + 10_000;
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
            schema_version: ChSchemaVersion::FIRST,
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
            schema_version: ChSchemaVersion::FIRST,
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

#[tokio::test]
#[ignore = "requires Docker ClickHouse"]
async fn trade_tape_window_dedupes_replacing_merge_tree() {
    use quant_pivot_models::clickhouse::{ChPrice, ChSchemaVersion, ChShares, ChUsd, TradeTapeRow};
    use quant_pivot_models::enums::clickhouse::{
        ChTradeParticipantRole, ChTradeSide, ChTradeTapeSource,
    };

    let (pool, client, _container) = setup_clickhouse().await;
    let read = ChQuantFactReadRepository::new(Arc::clone(&pool));
    let market_id = MarketId::new("0xtrade-tape-dedupe");
    let token_id = TokenId::new("tok-dedupe");
    // Stay inside `quant_trade_tape` 365-day TTL.
    let event_time = fresh_event_time_ms();

    let base = TradeTapeRow {
        market_id: market_id.clone(),
        token_id: token_id.clone(),
        event_time,
        ingestion_time: event_time,
        participant_address: "0xparticipant".to_owned(),
        participant_role: ChTradeParticipantRole::Maker,
        side: ChTradeSide::Buy,
        price: ChPrice::from(Price::new(Decimal::new(55, 2))),
        size_shares: ChShares::from(quant_pivot_models::types::Shares::new(Decimal::from(10))),
        notional_usd: ChUsd::from(quant_pivot_models::types::Usd::new(Decimal::from(5))),
        tx_hash: Some("0xtx".to_owned()),
        trade_id: "trade-dedupe-1".to_owned(),
        source: ChTradeTapeSource::OnChain,
        coverage_flags: 0,
        raw_payload_json: None,
        schema_version: ChSchemaVersion::FIRST,
    };
    let mut stale = base.clone();
    stale.ingestion_time = event_time - 1_000;
    let mut fresh = base.clone();
    fresh.ingestion_time = event_time + 1_000;

    let mut insert = client
        .insert::<TradeTapeRow>("quant_trade_tape")
        .await
        .expect("insert");
    insert.write(&stale).await.expect("write stale");
    insert.write(&fresh).await.expect("write fresh");
    insert.end().await.expect("end");

    let rows = read
        .trade_tape_window_by_market(vec![market_id.clone()], event_time - 1, event_time + 1)
        .await
        .expect("read");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].ingestion_time, fresh.ingestion_time);
}
