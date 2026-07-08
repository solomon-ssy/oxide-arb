//! Live Binance spot REST kline integration (`GET /api/v3/klines`).
//!
//! Validates the production wire contract against the public Binance API — not
//! wiremock. Covers typed [`BinanceKlineRow`] deserialization and the full
//! [`BinanceKlineSource`] → [`DomainObservation`] path.
//!
//! Run (requires outbound network, no API key):
//! ```bash
//! cargo test -p quant-pivot-api --test integration binance -- --ignored --test-threads=1
//! ```
//!
//! Optional env:
//! - `BINANCE_API_BASE_URL` — override REST host (default: `https://api.binance.com`)

use chrono::{Duration, Utc};
use quant_pivot_api::{
    binance::{BinanceKlineRow, BinanceKlineSource},
    domain::{DomainDataSource, DomainFetchRequest},
};
use quant_pivot_models::{
    config::BinanceSourceConfig,
    enums::domain::{DomainFamily, DomainMetric, KlineInterval},
    types::{BinanceSymbol, DomainInstrumentKey, DomainSourceId},
};
use rust_decimal::Decimal;
use std::env::var;

fn live_config() -> BinanceSourceConfig {
    let mut config = BinanceSourceConfig::default();
    if let Ok(base_url) = var("BINANCE_API_BASE_URL") {
        config.base_url = base_url;
    }
    config
}

fn btcusdt_1m_key() -> DomainInstrumentKey {
    DomainInstrumentKey::binance_kline(
        &BinanceSymbol::parse("BTCUSDT").expect("symbol"),
        KlineInterval::OneMinute,
    )
}

/// A completed-candle window: ends a few minutes before `Utc::now()` so the
/// latest bar is closed and Binance has persisted it.
fn recent_completed_window() -> (chrono::DateTime<Utc>, chrono::DateTime<Utc>) {
    let to = Utc::now() - Duration::minutes(5);
    let from = to - Duration::hours(2);
    (from, to)
}

#[tokio::test]
#[ignore = "requires live Binance spot REST API"]
async fn live_klines_response_matches_typed_wire_schema() {
    let config = live_config();
    let base = config.base_url.trim_end_matches('/');
    let url = format!("{base}/api/v3/klines?symbol=BTCUSDT&interval=1m&limit=5");
    let http = reqwest::Client::new();
    let body = http
        .get(&url)
        .send()
        .await
        .expect("binance GET")
        .error_for_status()
        .expect("binance 2xx")
        .text()
        .await
        .expect("body");

    let rows: Vec<BinanceKlineRow> = serde_json::from_str(&body).expect("typed wire deserialize");
    assert_eq!(rows.len(), 5, "limit=5 must return five klines");

    for row in &rows {
        assert!(row.open_time_ms > 0);
        assert!(row.close_time_ms >= row.open_time_ms);
        assert!(row.close > Decimal::ZERO);
        assert!(row.volume >= Decimal::ZERO);
        assert!(row.trade_count > 0);
    }
}

#[tokio::test]
#[ignore = "requires live Binance spot REST API"]
async fn live_kline_source_fetch_emits_close_observations() {
    let source = BinanceKlineSource::new(live_config());
    let key = btcusdt_1m_key();
    let (from, to) = recent_completed_window();

    let observations = source
        .fetch(DomainFetchRequest {
            instrument_key: key.clone(),
            from_exclusive: from,
            to_inclusive: to,
            bootstrap: false,
        })
        .await
        .expect("fetch");

    assert!(
        observations.len() >= 4,
        "two hours of 1m BTCUSDT should yield many close observations (got {})",
        observations.len()
    );

    for close in &observations {
        assert_eq!(close.family, DomainFamily::Crypto);
        assert_eq!(close.source_id, DomainSourceId::binance());
        assert_eq!(close.instrument_key, key);
        assert_eq!(close.metric, DomainMetric::Close);
        assert!(close.value > Decimal::ZERO);
        assert_eq!(close.publish_time, close.observed_at);
        assert!(close.observed_at > from);
        // Binance `endTime` filters on open time; close time may trail `to` by one interval.
        assert!(close.observed_at <= to + Duration::minutes(1));
    }

    for window in observations.windows(2) {
        assert!(
            window[0].observed_at <= window[1].observed_at,
            "close observations must be ascending by event time"
        );
    }
}

#[tokio::test]
#[ignore = "requires live Binance spot REST API"]
async fn live_kline_source_reports_binance_identity() {
    let source = BinanceKlineSource::new(live_config());
    assert_eq!(source.family(), DomainFamily::Crypto);
    assert_eq!(source.source_id(), DomainSourceId::binance());
}
