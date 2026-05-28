//! `ClickHouse` timeseries repository integration tests (requires Docker).

#[path = "common/ch.rs"]
mod ch;

use std::time::Duration;

use ch::setup_timeseries_repo;
use chrono::Utc;
use oxide_arb_models::clickhouse::TickEventRow;
use oxide_arb_repository::traits::TimeseriesRepository;

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
