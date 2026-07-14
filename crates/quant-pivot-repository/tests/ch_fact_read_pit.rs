//! `ClickHouse` point-in-time read integration tests (book tie-breaker + resolution).

use chrono::Utc;
use quant_pivot_models::{
    clickhouse::{BookL2CheckpointRow, BookMicrostructureRow, ChPrice, ChSchemaVersion},
    config::ClickHouseConfig,
    enums::clickhouse::ChFactSource,
    types::{ContentHash, MarketId, Price, Shares, TokenId, Usd},
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
use uuid::Uuid;

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

async fn insert_book_rows(client: &clickhouse::Client, rows: &[BookL2CheckpointRow]) {
    let mut insert = client
        .insert::<BookL2CheckpointRow>("quant_book_l2_checkpoint")
        .await
        .expect("insert");
    for row in rows {
        insert.write(row).await.expect("write row");
    }
    insert.end().await.expect("end insert");
}

async fn insert_microstructure_rows(client: &clickhouse::Client, rows: &[BookMicrostructureRow]) {
    let mut insert = client
        .insert::<BookMicrostructureRow>("book_microstructure_1s")
        .await
        .expect("insert microstructure");
    for row in rows {
        insert.write(row).await.expect("write microstructure row");
    }
    insert.end().await.expect("end microstructure insert");
}

/// Wait until inserted checkpoint rows are query-visible.
///
/// Fresh `MergeTree` parts can lag briefly behind HTTP insert ack on a cold
/// testcontainer; PIT reads that race this window return `None`.
async fn wait_for_book_snapshot_rows(client: &clickhouse::Client, token: &TokenId, expected: u64) {
    const ATTEMPTS: usize = 40;
    const PAUSE: Duration = Duration::from_millis(50);
    for attempt in 1..=ATTEMPTS {
        let count: u64 = client
            .query("SELECT count() FROM quant_book_l2_checkpoint WHERE token_id = ?")
            .bind(token.clone())
            .fetch_one()
            .await
            .expect("count quant_book_l2_checkpoint");
        if count >= expected {
            return;
        }
        assert!(
            attempt < ATTEMPTS,
            "checkpoint rows for {} not visible after insert \
             (count={count}, expected>={expected}, attempts={ATTEMPTS})",
            token.as_str()
        );
        tokio::time::sleep(PAUSE).await;
    }
}

/// Current epoch millis for deterministic point-in-time visibility checks.
fn fresh_event_time_ms() -> i64 {
    Utc::now().timestamp_millis()
}

fn book_row(
    token: &str,
    event_time_ms: i64,
    ingestion_time_ms: i64,
    sequence: u64,
    mid: Decimal,
) -> BookL2CheckpointRow {
    let source_event_hash =
        ContentHash::parse(format!("blake3:{}", "1".repeat(64))).expect("source event hash");
    let checkpoint_hash =
        ContentHash::parse(format!("blake3:{}", "2".repeat(64))).expect("checkpoint hash");
    BookL2CheckpointRow {
        token_id: TokenId::new(token),
        market_id: Some(MarketId::new("0xchpit")),
        stream_session_id: Uuid::nil(),
        token_sequence: sequence,
        bids_json: format!(r#"[["{mid}","100"]]"#),
        asks_json: r#"[["0.52","100"]]"#.to_owned(),
        book_version: 1,
        source_event_hash,
        checkpoint_hash,
        event_time: event_time_ms,
        created_at: ingestion_time_ms,
        schema_version: ChSchemaVersion(2),
    }
}

fn microstructure_row(
    token_id: &TokenId,
    market_id: &MarketId,
    bucket_time: i64,
    available_at: i64,
    mid: Decimal,
) -> BookMicrostructureRow {
    let price = ChPrice::from(Price::new(mid));
    BookMicrostructureRow {
        token_id: token_id.clone(),
        market_id: Some(market_id.clone()),
        bucket_time,
        best_bid_open: None,
        best_bid_high: None,
        best_bid_low: None,
        best_bid_close: None,
        best_ask_open: None,
        best_ask_high: None,
        best_ask_low: None,
        best_ask_close: None,
        spread_bps_min: None,
        spread_bps_avg: None,
        spread_bps_max: None,
        mid_price_open: Some(price),
        mid_price_close: Some(price),
        top1_depth_usd_avg: None,
        top5_depth_usd_avg: None,
        top20_depth_usd_avg: None,
        imbalance_avg: None,
        update_count: 1,
        snapshot_count: 1,
        delta_count: 0,
        delete_count: 0,
        crossed_count: 0,
        invalid_level_count: 0,
        gap_count: 0,
        last_trade_count: 0,
        max_book_age_ms: 0,
        schema_version: ChSchemaVersion::FIRST,
        available_at,
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

    let before_late_arrival = read
        .book_checkpoint_at(&token, event_time + 5, event_time + 1)
        .await
        .expect("read before late arrival")
        .expect("earlier visible revision");
    assert_eq!(
        before_late_arrival.bids_json, r#"[["0.49","100"]]"#,
        "a backdated revision must not be visible before its ingestion time"
    );

    let row = read
        .book_checkpoint_at(&token, event_time + 5, event_time + 5)
        .await
        .expect("read")
        .unwrap_or_else(|| {
            panic!(
                "PIT book_checkpoint_at returned None for token={} as_of={}",
                token.as_str(),
                event_time + 5
            )
        });
    assert_eq!(
        row.bids_json, r#"[["0.50","100"]]"#,
        "tie-breaker must prefer later ingestion_time at same event_time"
    );
}

#[tokio::test]
#[ignore = "requires Docker ClickHouse"]
async fn historical_scans_reject_rows_not_yet_available() {
    let (pool, client, _container) = setup_clickhouse().await;
    let read = ChQuantFactReadRepository::new(pool);
    let event_time = fresh_event_time_ms();

    let market_id = MarketId::new("0xavailability-axis");
    let token_id = TokenId::new("availability-axis-yes");
    let late_ingestion = event_time + 10_000;
    let mut late_book = book_row(
        token_id.as_str(),
        event_time,
        late_ingestion,
        1,
        Decimal::new(50, 2),
    );
    late_book.market_id = Some(market_id.clone());
    insert_book_rows(&client, std::slice::from_ref(&late_book)).await;
    wait_for_book_snapshot_rows(&client, &token_id, 1).await;

    assert!(
        read.observed_markets_between(event_time - 1, event_time + 1, event_time)
            .await
            .expect("markets before ingestion")
            .is_empty(),
        "a historical candidate must not exist before its book ingestion time"
    );
    assert_eq!(
        read.observed_markets_between(event_time - 1, event_time + 1, late_ingestion)
            .await
            .expect("markets after ingestion"),
        vec![market_id.clone()]
    );

    let visible_at = event_time + 1_000;
    let corrected_at = event_time + 2_000;
    insert_microstructure_rows(
        &client,
        &[
            microstructure_row(
                &token_id,
                &market_id,
                event_time,
                visible_at,
                Decimal::new(40, 2),
            ),
            microstructure_row(
                &token_id,
                &market_id,
                event_time,
                corrected_at,
                Decimal::new(60, 2),
            ),
        ],
    )
    .await;

    let before_correction = read
        .mid_price_series(
            vec![token_id.clone()],
            event_time - 1,
            event_time + 1,
            visible_at,
            60,
        )
        .await
        .expect("mid series before correction");
    assert_eq!(before_correction.len(), 1);
    assert_eq!(
        before_correction[0].mid_price.map(ChPrice::to_price),
        Some(Price::new(Decimal::new(40, 2)))
    );

    let after_correction = read
        .mid_price_series(
            vec![token_id],
            event_time - 1,
            event_time + 1,
            corrected_at,
            60,
        )
        .await
        .expect("mid series after correction");
    assert_eq!(after_correction.len(), 1);
    assert_eq!(
        after_correction[0].mid_price.map(ChPrice::to_price),
        Some(Price::new(Decimal::new(60, 2)))
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
            // A correction whose economic time is in range but whose writer
            // observation is not yet visible at `as_of`.
            resolved_at: early + 1_000,
            observed_at: as_of + 1_000,
            source: ChFactSource::WsMarketResolved,
            sequence: 3,
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
        .resolution_at(&market_id, as_of, as_of)
        .await
        .expect("read")
        .expect("resolution");
    assert_eq!(resolved.resolved_at, early);
    assert_eq!(resolved.winning_token_id, yes);

    let corrected = read
        .resolution_at(&market_id, as_of, as_of + 1_000)
        .await
        .expect("read corrected")
        .expect("corrected resolution");
    assert_eq!(corrected.resolved_at, early + 1_000);
    assert_eq!(corrected.winning_token_id, no);

    let before_correction = read
        .resolutions_between(vec![market_id.clone()], early, as_of, as_of)
        .await
        .expect("bounded resolution range");
    assert_eq!(before_correction.len(), 1);
    let after_correction = read
        .resolutions_between(vec![market_id], early, as_of, as_of + 1_000)
        .await
        .expect("visible corrected range");
    assert_eq!(after_correction.len(), 2);
}

#[tokio::test]
#[ignore = "requires Docker ClickHouse"]
async fn domain_observation_preserves_prior_revisions_after_merge() {
    use quant_pivot_models::{
        clickhouse::{ChDecimal64, DomainObservationRow},
        types::{DomainInstrumentKey, DomainSourceId},
    };

    let (pool, client, _container) = setup_clickhouse().await;
    let read = ChQuantFactReadRepository::new(pool);
    let event_time = fresh_event_time_ms();
    let visible_at = event_time + 1_000;
    let corrected_at = event_time + 2_000;
    let instrument_key = DomainInstrumentKey::new("BINANCE:BTCUSDT:1m");
    let base = DomainObservationRow {
        family: "crypto".to_owned(),
        source_id: DomainSourceId::binance(),
        instrument_key: instrument_key.clone(),
        metric: "close".to_owned(),
        value: ChDecimal64::from(Decimal::new(40, 2)),
        event_time,
        publish_time: event_time,
        ingestion_time: visible_at,
        schema_version: ChSchemaVersion::FIRST,
    };
    let mut correction = base.clone();
    correction.value = ChDecimal64::from(Decimal::new(60, 2));
    correction.ingestion_time = corrected_at;

    let mut insert = client
        .insert::<DomainObservationRow>("quant_domain_observation")
        .await
        .expect("insert domain observations");
    insert.write(&base).await.expect("write original");
    insert.write(&correction).await.expect("write correction");
    insert.end().await.expect("end domain insert");
    client
        .query("OPTIMIZE TABLE quant_domain_observation FINAL")
        .execute()
        .await
        .expect("merge domain observation parts");

    let before = read
        .domain_observation_at(&instrument_key, "close", event_time, visible_at)
        .await
        .expect("observation before correction")
        .expect("original observation remains queryable");
    assert_eq!(before.value.to_decimal(), Decimal::new(40, 2));

    let after = read
        .domain_observation_at(&instrument_key, "close", event_time, corrected_at)
        .await
        .expect("observation after correction")
        .expect("corrected observation");
    assert_eq!(after.value.to_decimal(), Decimal::new(60, 2));
}

#[tokio::test]
#[ignore = "requires Docker ClickHouse"]
async fn trade_tape_preserves_prior_revisions_after_merge() {
    use quant_pivot_models::clickhouse::{ChPrice, ChSchemaVersion, ChShares, ChUsd, TradeTapeRow};
    use quant_pivot_models::enums::clickhouse::{
        ChTradeParticipantRole, ChTradeReconciliationStatus, ChTradeSide, ChTradeTapeSource,
    };

    let (pool, client, _container) = setup_clickhouse().await;
    let read = ChQuantFactReadRepository::new(Arc::clone(&pool));
    let market_id = MarketId::new("0xtrade-tape-dedupe");
    let token_id = TokenId::new("tok-dedupe");
    // Keep the test partition close to wall clock for compact integration scans.
    let event_time = fresh_event_time_ms();

    let base = TradeTapeRow {
        market_id: market_id.clone(),
        token_id: token_id.clone(),
        event_time,
        ingestion_time: event_time,
        stream_session_id: None,
        token_sequence: None,
        participant_address: "0xparticipant".to_owned(),
        participant_role: ChTradeParticipantRole::Maker,
        side: ChTradeSide::Buy,
        price: ChPrice::from(Price::new(Decimal::new(55, 2))),
        size_shares: ChShares::from(Shares::new(Decimal::from(10))),
        notional_usd: ChUsd::from(Usd::new(Decimal::from(5))),
        tx_hash: Some("0xtx".to_owned()),
        source_event_id: "trade-dedupe-1".to_owned(),
        source: ChTradeTapeSource::OnChainOrderFilled,
        observed_field_flags: u16::MAX,
        fee_rate_bps: None,
        reconciliation_status: ChTradeReconciliationStatus::OnChainOnly,
        matched_source_event_id: None,
        revision: 1,
        reconciled_at: None,
        raw_payload_json: None,
        schema_version: ChSchemaVersion(2),
    };
    let mut stale = base.clone();
    stale.ingestion_time = event_time - 1_000;
    let mut fresh = base.clone();
    fresh.ingestion_time = event_time + 1_000;
    fresh.revision = 2;
    fresh.reconciliation_status = ChTradeReconciliationStatus::Matched;
    fresh.matched_source_event_id = Some("ws:trade-dedupe-1".to_owned());
    fresh.reconciled_at = Some(fresh.ingestion_time);

    let mut insert = client
        .insert::<TradeTapeRow>("quant_trade_tape")
        .await
        .expect("insert");
    insert.write(&stale).await.expect("write stale");
    insert.write(&fresh).await.expect("write fresh");
    insert.end().await.expect("end");
    client
        .query("OPTIMIZE TABLE quant_trade_tape FINAL")
        .execute()
        .await
        .expect("merge trade tape parts");

    let rows_before_revision = read
        .trade_tape_window_by_market(
            vec![market_id.clone()],
            event_time - 1,
            event_time + 1,
            event_time + 1,
        )
        .await
        .expect("read before revision");
    assert!(rows_before_revision.is_empty());

    let rows = read
        .trade_tape_window_by_market(
            vec![market_id.clone()],
            event_time - 1,
            event_time + 1,
            event_time + 1_000,
        )
        .await
        .expect("read after revision");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].ingestion_time, fresh.ingestion_time);
}
