//! Binance spot REST kline source (`GET /api/v3/klines`).

use std::{num::NonZeroU32, str::FromStr, sync::Arc, time::Duration};

use chrono::{TimeZone, Utc};
use governor::{
    Quota, RateLimiter,
    clock::DefaultClock,
    state::{InMemoryState, NotKeyed},
};
use quant_pivot_error::{QuantError, QuantResult, api::ApiError};
use quant_pivot_models::{
    config::BinanceSourceConfig,
    domain::DomainObservation,
    enums::domain::{DomainFamily, DomainMetric, KlineInterval, KlineIntervalParseError},
    types::{BinanceSymbol, DomainInstrumentKey, DomainSourceId},
};
use reqwest::StatusCode;
use rust_decimal::Decimal;

use crate::{
    domain::{DomainDataSource, DomainFetchRequest},
    infra::retry::{self, RetryPolicy},
};

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
        Self {
            config,
            http: reqwest::Client::new(),
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
        let http = self.http.clone();
        let body = retry::retry_with_policy(&self.retry_policy, || {
            let http = http.clone();
            let url = url.clone();
            async move {
                let response = http
                    .get(&url)
                    .send()
                    .await
                    .map_err(|error| ApiError::Http {
                        method: "GET",
                        url: url.clone(),
                        status: 0,
                        body: error.to_string(),
                        retryable: true,
                    })?;
                let status = response.status();
                if status.is_success() {
                    return response.text().await.map_err(|error| ApiError::Http {
                        method: "GET",
                        url: url.clone(),
                        status: 0,
                        body: error.to_string(),
                        retryable: true,
                    });
                }
                let retryable = matches!(
                    status,
                    StatusCode::TOO_MANY_REQUESTS
                        | StatusCode::INTERNAL_SERVER_ERROR
                        | StatusCode::BAD_GATEWAY
                        | StatusCode::SERVICE_UNAVAILABLE
                        | StatusCode::GATEWAY_TIMEOUT
                );
                Err(ApiError::Http {
                    method: "GET",
                    url: url.clone(),
                    status: status.as_u16(),
                    body: response.text().await.unwrap_or_default(),
                    retryable,
                })
            }
        })
        .await
        .map_err(QuantError::from)?;

        let rows: Vec<Vec<serde_json::Value>> =
            serde_json::from_str(&body).map_err(|error| ApiError::Deserialize {
                context: "binance klines".into(),
                detail: error.to_string(),
            })?;
        let instrument_key = DomainInstrumentKey::binance_kline(symbol, interval);
        let mut observations = Vec::with_capacity(rows.len() * 2);
        for row in rows {
            observations.extend(decode_kline_row(&instrument_key, &row)?);
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
            observations.extend(page);
            if last_observed >= end_ms {
                break;
            }
            let page_len = observations.len();
            if page_len % (KLINE_PAGE_SIZE as usize * 2) != 0 {
                break;
            }
            cursor_ms = last_observed.saturating_add(1);
        }
        Ok(observations)
    }
}

fn decode_kline_row(
    instrument_key: &DomainInstrumentKey,
    row: &[serde_json::Value],
) -> QuantResult<Vec<DomainObservation>> {
    if row.len() < 7 {
        return Err(QuantError::from(ApiError::Deserialize {
            context: "binance kline row".into(),
            detail: "row too short".into(),
        }));
    }
    let close_time_ms = row[6].as_i64().ok_or_else(|| {
        QuantError::from(ApiError::Deserialize {
            context: "binance kline row".into(),
            detail: "close time missing".into(),
        })
    })?;
    let observed_at = Utc
        .timestamp_millis_opt(close_time_ms)
        .single()
        .ok_or_else(|| {
            QuantError::from(ApiError::Deserialize {
                context: "binance kline row".into(),
                detail: "close time invalid".into(),
            })
        })?;
    let close = parse_decimal_field(row.get(4).ok_or_else(|| {
        QuantError::from(ApiError::Deserialize {
            context: "binance kline row".into(),
            detail: "close missing".into(),
        })
    })?)?;
    let volume = parse_decimal_field(row.get(5).ok_or_else(|| {
        QuantError::from(ApiError::Deserialize {
            context: "binance kline row".into(),
            detail: "volume missing".into(),
        })
    })?)?;
    let base = DomainObservation {
        family: DomainFamily::Crypto,
        source_id: DomainSourceId::binance(),
        instrument_key: instrument_key.clone(),
        metric: DomainMetric::Close,
        value: close,
        observed_at,
        publish_time: observed_at,
    };
    Ok(vec![
        base.clone(),
        DomainObservation {
            metric: DomainMetric::Volume,
            value: volume,
            ..base
        },
    ])
}

fn parse_decimal_field(value: &serde_json::Value) -> QuantResult<Decimal> {
    let text = match value {
        serde_json::Value::String(text) => text.as_str(),
        serde_json::Value::Number(number) => {
            return Decimal::from_str(&number.to_string()).map_err(|error| {
                QuantError::from(ApiError::Deserialize {
                    context: "binance decimal".into(),
                    detail: error.to_string(),
                })
            });
        }
        _ => {
            return Err(QuantError::from(ApiError::Deserialize {
                context: "binance decimal".into(),
                detail: "expected decimal string".into(),
            }));
        }
    };
    Decimal::from_str(text).map_err(|error| {
        QuantError::from(ApiError::Deserialize {
            context: "binance decimal".into(),
            detail: error.to_string(),
        })
    })
}

#[cfg(test)]
mod tests {
    use super::{BinanceKlineSource, DomainDataSource, DomainFetchRequest};
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

    #[tokio::test]
    async fn fetch_parses_klines_into_close_and_volume() {
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
                    1_499_644_799_999_i64
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
        assert_eq!(rows.len(), 2);
        assert!(rows.iter().any(|row| row.metric == DomainMetric::Close));
        assert!(rows.iter().any(|row| row.metric == DomainMetric::Volume));
    }
}
