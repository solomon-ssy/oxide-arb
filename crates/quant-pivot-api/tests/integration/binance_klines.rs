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
    binance::{
        BinanceAggTrade, BinanceAggTradeSource, BinanceKlineRow, BinanceKlineSource,
        BinanceRequestBudget,
    },
    domain::{DomainDataSource, DomainFetchRequest},
};
use quant_pivot_models::{
    config::BinanceSourceConfig,
    enums::domain::{DomainFamily, DomainMetric, KlineInterval},
    types::{BinanceSymbol, DomainInstrumentKey, DomainSourceId},
};
use rust_decimal::Decimal;
use rustls::crypto::aws_lc_rs;
use std::env::var;

fn live_config() -> BinanceSourceConfig {
    let mut config = BinanceSourceConfig::default();
    if let Ok(base_url) = var("BINANCE_API_BASE_URL") {
        config.rest_url = base_url;
    }
    config
}

fn live_usdm_futures_config() -> BinanceSourceConfig {
    let mut config = BinanceSourceConfig::usdm_futures_default();
    if let Ok(base_url) = var("BINANCE_USDM_FUTURES_API_BASE_URL") {
        config.rest_url = base_url;
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
    let base = config.rest_url.trim_end_matches('/');
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
    let source = BinanceKlineSource::connect(live_config()).expect("source");
    let key = btcusdt_1m_key();
    let (from, to) = recent_completed_window();

    let observations = source
        .fetch(DomainFetchRequest {
            instrument_key: key.clone(),
            from_exclusive: from,
            to_inclusive: to,
            bootstrap: true,
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
    let source = BinanceKlineSource::connect(live_config()).expect("source");
    assert_eq!(source.family(), DomainFamily::Crypto);
    assert_eq!(source.source_id(), DomainSourceId::binance());
}

#[tokio::test]
#[ignore = "requires live Binance USD-M Futures REST API"]
async fn live_hype_usdm_futures_kline_preserves_exact_provenance() {
    let config = live_usdm_futures_config();
    let source = BinanceKlineSource::connect_usdm_futures_with_budget(
        config.clone(),
        BinanceRequestBudget::new(&config).expect("request budget"),
    )
    .expect("USD-M Futures source");
    let key = DomainInstrumentKey::binance_usdm_futures_kline(
        &BinanceSymbol::parse("HYPEUSDT").expect("symbol"),
        KlineInterval::OneHour,
    );
    let to = Utc::now() - Duration::hours(2);
    let observations = source
        .fetch(DomainFetchRequest {
            instrument_key: key.clone(),
            from_exclusive: to - Duration::hours(12),
            to_inclusive: to,
            bootstrap: true,
        })
        .await
        .expect("fetch HYPEUSDT futures klines");
    assert!(!observations.is_empty());
    assert!(observations.iter().all(|observation| {
        observation.source_id == DomainSourceId::binance_usdm_futures()
            && observation.instrument_key == key
            && observation.value > Decimal::ZERO
    }));
}

#[tokio::test]
#[ignore = "requires live Binance spot REST API"]
async fn live_agg_trade_recovery_preserves_exact_source_sequence() {
    let config = live_config();
    let base = config.rest_url.trim_end_matches('/');
    let url = format!("{base}/api/v3/aggTrades?symbol=BTCUSDT&limit=1");
    let body = reqwest::Client::new()
        .get(url)
        .send()
        .await
        .expect("binance GET")
        .error_for_status()
        .expect("binance 2xx")
        .text()
        .await
        .expect("body");
    let latest: Vec<BinanceAggTrade> =
        serde_json::from_str(&body).expect("typed aggregate-trade wire");
    let expected = latest
        .first()
        .expect("one aggregate trade")
        .aggregate_trade_id;
    let source = BinanceAggTradeSource::connect(config).expect("source");
    let reports = source
        .recover_from(
            &BinanceSymbol::parse("BTCUSDT").expect("symbol"),
            expected,
            1,
            Utc::now(),
        )
        .await
        .expect("recover exact aggregate trade");
    assert_eq!(reports.len(), 1);
    assert_eq!(reports[0].source_sequence, expected);
    assert!(reports[0].price.inner() > Decimal::ZERO);
}

#[tokio::test]
#[ignore = "requires live Binance USD-M Futures WebSocket"]
async fn live_hype_usdm_futures_agg_trade_stream_preserves_exact_provenance() {
    let _ = aws_lc_rs::default_provider().install_default();
    let config = live_usdm_futures_config();
    let source = BinanceAggTradeSource::connect_usdm_futures_with_budget(
        config.clone(),
        BinanceRequestBudget::new(&config).expect("request budget"),
    )
    .expect("USD-M Futures aggTrade source");
    let symbol = BinanceSymbol::parse("HYPEUSDT").expect("symbol");
    let mut stream = source.stream(&symbol).await.expect("HYPEUSDT stream");
    let report = tokio::time::timeout(std::time::Duration::from_secs(15), stream.next_report())
        .await
        .expect("HYPEUSDT aggregate trade within 15 seconds")
        .expect("valid HYPEUSDT aggregate trade");
    assert_eq!(
        report.source_id,
        DomainSourceId::binance_usdm_futures_agg_trade()
    );
    assert_eq!(
        report.instrument_key,
        DomainInstrumentKey::binance_usdm_futures_agg_trade(&symbol)
    );
    assert!(report.source_sequence > 0);
    assert!(report.price.inner() > Decimal::ZERO);
}

#[tokio::test]
#[ignore = "requires live Binance public-data archive"]
async fn live_daily_kline_archive_is_checksum_verified_and_contiguous() {
    let source = BinanceKlineSource::connect(live_config()).expect("source");
    let symbol = BinanceSymbol::parse("BTCUSDT").expect("symbol");
    let date = chrono::NaiveDate::from_ymd_opt(2025, 1, 1).expect("archive date");
    let available_at = Utc::now();
    let mut archive = source
        .recover_archive_day(&symbol, KlineInterval::OneMinute, date, available_at)
        .await
        .expect("verified archive")
        .expect("published archive");
    let mut rows = Vec::new();
    while let Some(batch) = archive.next_batch().await.expect("decode archive batch") {
        rows.extend(batch);
    }
    assert_eq!(rows.len(), 1_440, "one UTC day must contain 1,440 1m bars");
    assert!(
        rows.iter()
            .all(|row| row.available_at == Some(available_at))
    );
    for pair in rows.windows(2) {
        assert_eq!(
            pair[1].observed_at - pair[0].observed_at,
            Duration::minutes(1)
        );
    }
}
