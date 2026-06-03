//! `ClickHouse` timeseries repository integration tests (requires Docker).

#[path = "common/ch.rs"]
mod ch;

use std::time::Duration;

use ch::setup_timeseries_repo;
use chrono::Utc;
use oxide_arb_models::{
    clickhouse::{
        BookSnapshotRow, CalibrationSnapshotRow, ChBps, ChDecimal64, ChPrice, ChProbability,
        ChSchemaVersion, ChShares, ChUsd, TickEventL2Row, TickEventRow,
    },
    enums::clickhouse::{
        ChBookEventType, ChDurationBucket, ChFactSource, ChMarketCategory, ChPriceZone,
        ChSnapshotReason,
    },
    types::{MarketId, Price, Shares, TokenId, Usd},
};
use oxide_arb_repository::traits::{
    EvidenceTimeseriesRepository, TimeWindow, TimeseriesFactWriter,
};
use rust_decimal_macros::dec;

fn sample_tick(token_id: &str, ts: i64) -> TickEventRow {
    TickEventRow {
        token_id: TokenId::new(token_id),
        market_id: None,
        event_type: ChBookEventType::Bbo,
        best_bid: Some(ChPrice::from(Price::new(dec!(0.94)))),
        best_ask: Some(ChPrice::from(Price::new(dec!(0.95)))),
        last_trade_price: None,
        bid_depth_usd: Some(ChUsd::from(Usd::new(dec!(500)))),
        ask_depth_usd: Some(ChUsd::from(Usd::new(dec!(400)))),
        spread_bps: Some(ChBps::from(dec!(10))),
        book_version: 1,
        raw_payload_json: Some("{}".into()),
        event_time: ts,
        ingestion_time: ts,
        sequence: 1,
        source: ChFactSource::WsBbo,
        schema_version: ChSchemaVersion(2),
    }
}

fn sample_l2(token_id: &str, ts: i64, sequence: u64) -> TickEventL2Row {
    TickEventL2Row {
        token_id: TokenId::new(token_id),
        market_id: Some(MarketId::new("0xch-market")),
        event_type: ChBookEventType::Snapshot,
        bid_prices: vec![ChPrice::from(Price::new(dec!(0.94)))],
        bid_sizes: vec![ChShares::from(Shares::new(dec!(10)))],
        ask_prices: vec![ChPrice::from(Price::new(dec!(0.95)))],
        ask_sizes: vec![ChShares::from(Shares::new(dec!(11)))],
        changed_levels_json: None,
        book_version: sequence,
        levels_count: 2,
        is_full_snapshot: true,
        event_time: ts,
        ingestion_time: ts,
        sequence,
        source: ChFactSource::WsSnapshot,
        schema_version: ChSchemaVersion(2),
    }
}

fn sample_book(token_id: &str, ts: i64, sequence: u64) -> BookSnapshotRow {
    BookSnapshotRow {
        token_id: TokenId::new(token_id),
        market_id: Some(MarketId::new("0xch-market")),
        snapshot_reason: ChSnapshotReason::Periodic,
        top_n: 2,
        bids_json: r#"[["0.94","10"]]"#.into(),
        asks_json: r#"[["0.95","11"]]"#.into(),
        bid_depth_usd: Some(ChUsd::from(Usd::new(dec!(9.4)))),
        ask_depth_usd: Some(ChUsd::from(Usd::new(dec!(10.45)))),
        mid_price: Some(ChPrice::from(Price::new(dec!(0.945)))),
        spread_bps: Some(ChBps::from(dec!(105.82))),
        book_version: sequence,
        levels_count: 2,
        event_time: ts,
        ingestion_time: ts,
        sequence,
        source: ChFactSource::Scanner,
        schema_version: ChSchemaVersion(2),
    }
}

fn sample_calibration(ts: i64, sequence: u64) -> CalibrationSnapshotRow {
    CalibrationSnapshotRow {
        category: ChMarketCategory::Politics,
        price_zone: ChPriceZone::Z97,
        duration_bucket: ChDurationBucket::Medium,
        total_count: 10,
        correct_count: 8,
        alpha_prior: ChDecimal64::from(dec!(2)),
        beta_prior: ChDecimal64::from(dec!(1)),
        posterior_mean: Some(ChProbability::from(dec!(0.8))),
        fallback_tier: 1,
        config_hash: "hash".into(),
        snapshot_hash: "snapshot".into(),
        event_time: ts,
        ingestion_time: ts,
        sequence,
        source: ChFactSource::CalibrationUpdater,
        schema_version: ChSchemaVersion(2),
    }
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn timeseries_insert_and_query_roundtrip() {
    let (repo, shutdown, _container) = setup_timeseries_repo().await;
    let now = Utc::now().timestamp_millis();
    let token = "tok-roundtrip";

    repo.insert_tick_events(&[
        sample_tick(token, now - 2_000),
        sample_tick(token, now - 1_000),
        sample_tick(token, now),
    ])
    .await
    .expect("insert");

    shutdown.cancel();
    tokio::time::sleep(Duration::from_millis(500)).await;

    let rows = repo
        .tick_events(
            &[TokenId::new(token)],
            TimeWindow::new(
                Utc::now() - chrono::Duration::minutes(5),
                Utc::now() + chrono::Duration::minutes(1),
            ),
            10,
        )
        .await
        .expect("query");

    assert_eq!(rows.len(), 3, "expected all inserted tick events");
    assert_eq!(rows[0].token_id, TokenId::new(token));
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn evidence_timeseries_queries_roundtrip_core_fact_tables() {
    let (repo, shutdown, _container) = setup_timeseries_repo().await;
    let now = Utc::now().timestamp_millis();
    let token = TokenId::new("tok-core-facts");

    repo.insert_l2_events(&[
        sample_l2(token.as_str(), now, 2),
        sample_l2(token.as_str(), now, 1),
    ])
    .await
    .expect("insert l2");
    repo.insert_book_snapshots(&[sample_book(token.as_str(), now, 1)])
        .await
        .expect("insert book");
    repo.insert_calibration_snapshots(&[sample_calibration(now, 1)])
        .await
        .expect("insert calibration");

    shutdown.cancel();
    tokio::time::sleep(Duration::from_millis(500)).await;
    let window = TimeWindow::new(
        Utc::now() - chrono::Duration::minutes(5),
        Utc::now() + chrono::Duration::minutes(1),
    );

    let l2 = repo
        .l2_events(std::slice::from_ref(&token), window)
        .await
        .unwrap();
    assert_eq!(l2.len(), 2);
    assert_eq!(
        l2[0].sequence, 1,
        "same timestamp rows must be sequence ordered"
    );

    let books = repo
        .book_snapshots_before(std::slice::from_ref(&token), Utc::now(), 1)
        .await
        .unwrap();
    assert_eq!(books.len(), 1);
    assert_eq!(books[0].snapshot_reason, ChSnapshotReason::Periodic);

    let calibration = repo.calibration_snapshots(window).await.unwrap();
    assert_eq!(calibration.len(), 1);
    assert!(calibration[0].posterior_mean.is_some());
}
