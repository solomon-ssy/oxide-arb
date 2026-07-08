//! Binance spot REST kline source (`GET /api/v3/klines`).

mod mapper;
mod wire;

use std::{num::NonZeroU32, sync::Arc, time::Duration};

use governor::{
    Quota, RateLimiter,
    clock::DefaultClock,
    state::{InMemoryState, NotKeyed},
};
use quant_pivot_error::{QuantError, QuantResult, api::ApiError};
use quant_pivot_models::{
    config::BinanceSourceConfig,
    domain::DomainObservation,
    enums::domain::{DomainFamily, KlineInterval, KlineIntervalParseError},
    types::{BinanceSymbol, DomainInstrumentKey, DomainSourceId},
};

use crate::{
    domain::{DomainDataSource, DomainFetchRequest},
    infra::{http::get_text_with_retry, retry::RetryPolicy},
};

pub use wire::{BinanceKlineRow, KLINE_FIELD_COUNT};

type WeightLimiter = RateLimiter<NotKeyed, InMemoryState, DefaultClock>;

/// Weight cost of one kline request (Binance public API).
const KLINE_REQUEST_WEIGHT: u32 = 2;

/// Maximum klines per REST request.
const KLINE_PAGE_SIZE: u32 = 1_000;

/// Binance spot kline client with proactive weight budgeting.
pub struct BinanceKlineSource {
    config: BinanceSourceConfig,
    http: reqwest::Client,
    retry_policy: RetryPolicy,
    weight_limiter: Arc<WeightLimiter>,
}

impl BinanceKlineSource {
    /// Build from deploy configuration.
    #[must_use]
    pub fn new(config: BinanceSourceConfig) -> Self {
        let budget = config.weight_budget_per_min.max(KLINE_REQUEST_WEIGHT);
        let quota = Quota::with_period(Duration::from_mins(1))
            .expect("valid quota")
            .allow_burst(NonZeroU32::new(budget).expect("nonzero budget"));
        // A hung TCP connection must never block an ingest tick indefinitely
        // (R10 ingest hardening); `reqwest::Client::new()` has no request
        // timeout by default.
        let http = reqwest::Client::builder()
            .timeout(Duration::from_millis(config.request_timeout_ms))
            .build()
            .expect("valid reqwest client");
        Self {
            config,
            http,
            retry_policy: RetryPolicy::gamma_default(),
            weight_limiter: Arc::new(RateLimiter::direct(quota)),
        }
    }

    /// Override the HTTP client (tests).
    #[must_use]
    pub fn with_http_client(mut self, http: reqwest::Client) -> Self {
        self.http = http;
        self
    }

    fn parse_instrument(key: &DomainInstrumentKey) -> QuantResult<(BinanceSymbol, KlineInterval)> {
        let rest = key
            .as_str()
            .strip_prefix("BINANCE:")
            .ok_or_else(|| QuantError::config(format!("not a Binance instrument key: {key}")))?;
        let (symbol, interval) = rest.rsplit_once(':').ok_or_else(|| {
            QuantError::config(format!("malformed Binance instrument key: {key}"))
        })?;
        Ok((
            BinanceSymbol::parse(symbol).map_err(|error| QuantError::config(error.to_string()))?,
            interval
                .parse()
                .map_err(|error: KlineIntervalParseError| QuantError::config(error.to_string()))?,
        ))
    }

    async fn acquire_weight(&self) {
        for _ in 0..KLINE_REQUEST_WEIGHT {
            self.weight_limiter.until_ready().await;
        }
    }

    async fn fetch_page(
        &self,
        symbol: &BinanceSymbol,
        interval: KlineInterval,
        start_ms: i64,
        end_ms: i64,
    ) -> QuantResult<Vec<DomainObservation>> {
        self.acquire_weight().await;
        let url = format!(
            "{}/api/v3/klines?symbol={}&interval={}&startTime={}&endTime={}&limit={KLINE_PAGE_SIZE}",
            self.config.base_url.trim_end_matches('/'),
            symbol.as_str(),
            interval.as_str(),
            start_ms,
            end_ms,
        );
        let body = get_text_with_retry(&self.http, &self.retry_policy, &url)
            .await
            .map_err(QuantError::from)?;

        let rows: Vec<wire::BinanceKlineRow> =
            serde_json::from_str(&body).map_err(|error| ApiError::Deserialize {
                context: "binance klines".into(),
                detail: error.to_string(),
            })?;
        let instrument_key = DomainInstrumentKey::binance_kline(symbol, interval);
        let mut observations = Vec::with_capacity(rows.len());
        for row in rows {
            observations.extend(mapper::into_observations(&row, &instrument_key)?);
        }
        Ok(observations)
    }
}

#[async_trait::async_trait]
impl DomainDataSource for BinanceKlineSource {
    fn family(&self) -> DomainFamily {
        DomainFamily::Crypto
    }

    fn source_id(&self) -> DomainSourceId {
        DomainSourceId::binance()
    }

    async fn fetch(&self, request: DomainFetchRequest) -> QuantResult<Vec<DomainObservation>> {
        let (symbol, interval) = Self::parse_instrument(&request.instrument_key)?;
        let mut observations = Vec::new();
        let mut cursor_ms = request.from_exclusive.timestamp_millis();
        let end_ms = request.to_inclusive.timestamp_millis();
        if cursor_ms >= end_ms {
            return Ok(observations);
        }

        loop {
            let page = self
                .fetch_page(&symbol, interval, cursor_ms, end_ms)
                .await?;
            if page.is_empty() {
                break;
            }
            let last_observed = page
                .last()
                .map(|row| row.observed_at.timestamp_millis())
                .expect("non-empty page");
            // One `DomainObservation` per kline row (Close only — see
            // `DomainMetric`'s doc for why volume is not modeled).
            let kline_count = page.len();
            observations.extend(page);
            if last_observed >= end_ms {
                break;
            }
            // Binance returns at most KLINE_PAGE_SIZE klines per request; a short
            // page means we reached the end of the available range.
            if kline_count < KLINE_PAGE_SIZE as usize {
                break;
            }
            // Advance past the last candle's close time so the next page cannot
            // duplicate rows (klines are uniquely identified by open time).
            cursor_ms = last_observed.saturating_add(1);
        }
        Ok(observations)
    }
}

#[cfg(test)]
mod tests {
    use super::{BinanceKlineSource, DomainDataSource, DomainFetchRequest, KLINE_PAGE_SIZE};
    use chrono::{TimeZone, Utc};
    use quant_pivot_models::{
        config::BinanceSourceConfig,
        enums::domain::{DomainMetric, KlineInterval},
        types::{BinanceSymbol, DomainInstrumentKey},
    };
    use wiremock::{
        Mock, MockServer, ResponseTemplate,
        matchers::{method, path, query_param},
    };

    fn full_kline_row(open_time_ms: i64, close: &str) -> serde_json::Value {
        serde_json::json!([
            open_time_ms,
            "0.01",
            "0.02",
            "0.005",
            close,
            "148976.11427815",
            open_time_ms + 59_999,
            "2434.19055334",
            308,
            "1756.87402397",
            "28.46694368",
            "0"
        ])
    }

    #[tokio::test]
    async fn fetch_parses_klines_into_close_observations() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/api/v3/klines"))
            .and(query_param("symbol", "BTCUSDT"))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(serde_json::json!([[
                    1_494_904_000_000_i64,
                    "0.01",
                    "0.02",
                    "0.005",
                    "0.01577100",
                    "148976.11427815",
                    1_499_644_799_999_i64,
                    "2434.19055334",
                    308,
                    "1756.87402397",
                    "28.46694368",
                    "0"
                ]])),
            )
            .mount(&server)
            .await;

        let source = BinanceKlineSource::new(BinanceSourceConfig {
            base_url: server.uri(),
            ..BinanceSourceConfig::default()
        });
        let key = DomainInstrumentKey::binance_kline(
            &BinanceSymbol::parse("BTCUSDT").expect("symbol"),
            KlineInterval::OneMinute,
        );
        let rows = source
            .fetch(DomainFetchRequest {
                instrument_key: key,
                from_exclusive: Utc.with_ymd_and_hms(2017, 7, 1, 0, 0, 0).unwrap(),
                to_inclusive: Utc.with_ymd_and_hms(2017, 7, 10, 0, 0, 0).unwrap(),
                bootstrap: true,
            })
            .await
            .expect("fetch");
        assert_eq!(rows.len(), 1);
        assert!(rows.iter().all(|row| row.metric == DomainMetric::Close));
    }

    #[tokio::test]
    async fn fetch_paginates_until_short_page() {
        let server = MockServer::start().await;
        let range_start = Utc
            .with_ymd_and_hms(2017, 7, 1, 0, 0, 0)
            .unwrap()
            .timestamp_millis();
        let mut full_page: Vec<serde_json::Value> = Vec::with_capacity(KLINE_PAGE_SIZE as usize);
        for index in 0..KLINE_PAGE_SIZE {
            let open_time_ms = range_start + i64::from(index) * 60_000;
            full_page.push(full_kline_row(open_time_ms, "0.01577100"));
        }
        let last_close_ms = range_start + i64::from(KLINE_PAGE_SIZE - 1) * 60_000 + 59_999;
        let next_cursor = last_close_ms + 1;
        let tail = full_kline_row(next_cursor, "0.02");

        Mock::given(method("GET"))
            .and(path("/api/v3/klines"))
            .and(query_param("symbol", "BTCUSDT"))
            .and(query_param("startTime", range_start.to_string()))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!(full_page)))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/api/v3/klines"))
            .and(query_param("symbol", "BTCUSDT"))
            .and(query_param("startTime", next_cursor.to_string()))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!([tail])))
            .mount(&server)
            .await;

        let source = BinanceKlineSource::new(BinanceSourceConfig {
            base_url: server.uri(),
            ..BinanceSourceConfig::default()
        });
        let key = DomainInstrumentKey::binance_kline(
            &BinanceSymbol::parse("BTCUSDT").expect("symbol"),
            KlineInterval::OneMinute,
        );
        let rows = source
            .fetch(DomainFetchRequest {
                instrument_key: key,
                from_exclusive: Utc.with_ymd_and_hms(2017, 7, 1, 0, 0, 0).unwrap(),
                to_inclusive: Utc.with_ymd_and_hms(2017, 7, 10, 0, 0, 0).unwrap(),
                bootstrap: true,
            })
            .await
            .expect("fetch");
        assert_eq!(rows.len(), KLINE_PAGE_SIZE as usize + 1);
    }
}
