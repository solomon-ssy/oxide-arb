//! Binance aggregate-trade REST recovery and rotating WebSocket stream.

use std::time::{Duration, Instant};

use chrono::{DateTime, TimeZone, Utc};
use futures_util::StreamExt;
use quant_pivot_error::{QuantError, QuantResult, api::ApiError, ws::WsError};
use quant_pivot_models::{
    config::BinanceSourceConfig,
    domain::CryptoPriceReport,
    hashing::CanonicalDigest,
    types::{BinanceSymbol, ContentHash, DomainInstrumentKey, DomainSourceId, Shares, Usd},
};
use tokio::net::TcpStream;
use tokio_tungstenite::{
    MaybeTlsStream, WebSocketStream, connect_async,
    tungstenite::{Message, protocol::CloseFrame},
};

use super::wire::BinanceAggTrade;
use crate::infra::{http::get_text_with_retry, retry::RetryPolicy};

const MAX_REST_PAGE_SIZE: u16 = 1_000;

/// Public Binance aggregate-trade client. REST recovery and WebSocket payloads
/// map to the exact same immutable fact contract.
pub struct BinanceAggTradeSource {
    config: BinanceSourceConfig,
    http: reqwest::Client,
    retry_policy: RetryPolicy,
}

impl BinanceAggTradeSource {
    pub fn connect(config: BinanceSourceConfig) -> QuantResult<Self> {
        let http = reqwest::Client::builder()
            .timeout(Duration::from_millis(config.request_timeout_ms))
            .build()
            .map_err(|error| ApiError::Sdk(format!("Binance aggTrade HTTP client: {error}")))?;
        Ok(Self {
            config,
            http,
            retry_policy: RetryPolicy::gamma_default(),
        })
    }

    #[must_use]
    pub fn with_http_client(mut self, http: reqwest::Client) -> Self {
        self.http = http;
        self
    }

    /// Recover an inclusive aggregate-trade ID page after a detected gap.
    pub async fn recover_from(
        &self,
        symbol: &BinanceSymbol,
        from_id: u64,
        limit: u16,
        available_at: DateTime<Utc>,
    ) -> QuantResult<Vec<CryptoPriceReport>> {
        let limit = limit.clamp(1, MAX_REST_PAGE_SIZE);
        let url = format!(
            "{}/api/v3/aggTrades?symbol={}&fromId={from_id}&limit={limit}",
            self.config.rest_url.trim_end_matches('/'),
            symbol.as_str(),
        );
        let body = get_text_with_retry(&self.http, &self.retry_policy, &url)
            .await
            .map_err(QuantError::from)?;
        let rows: Vec<BinanceAggTrade> =
            serde_json::from_str(&body).map_err(|error| ApiError::Deserialize {
                context: "binance aggregate trades".into(),
                detail: error.to_string(),
            })?;
        rows.into_iter()
            .map(|row| map_report(symbol, &row, available_at))
            .collect()
    }

    /// Open one raw-symbol stream. The caller reconnects when
    /// [`BinanceAggTradeStream::rotation_due`] becomes true, before Binance's
    /// mandatory 24-hour disconnect.
    pub async fn stream(&self, symbol: &BinanceSymbol) -> QuantResult<BinanceAggTradeStream> {
        let stream_name = format!("{}@aggTrade", symbol.as_str().to_ascii_lowercase());
        let url = format!(
            "{}/{}",
            self.config.websocket_url.trim_end_matches('/'),
            stream_name,
        );
        let (inner, _) = connect_async(&url).await.map_err(|error| {
            QuantError::WebSocket(WsError::ConnectionFailed {
                shard_id: 0,
                reason: format!("Binance aggTrade {symbol}: {error}"),
            })
        })?;
        Ok(BinanceAggTradeStream {
            symbol: symbol.clone(),
            inner,
            rotate_at: Instant::now()
                + Duration::from_secs(self.config.websocket_rotation_secs.min(86_400)),
        })
    }
}

pub struct BinanceAggTradeStream {
    symbol: BinanceSymbol,
    inner: WebSocketStream<MaybeTlsStream<TcpStream>>,
    rotate_at: Instant,
}

impl BinanceAggTradeStream {
    #[must_use]
    pub fn rotation_due(&self) -> bool {
        Instant::now() >= self.rotate_at
    }

    /// Read the next trade fact. Ping/Pong frames are consumed internally;
    /// clean remote close returns a typed connection-closed error.
    pub async fn next_report(&mut self) -> QuantResult<CryptoPriceReport> {
        loop {
            let message = self
                .inner
                .next()
                .await
                .ok_or_else(|| {
                    QuantError::WebSocket(WsError::ConnectionClosed {
                        shard_id: 0,
                        code: None,
                    })
                })?
                .map_err(|error| {
                    QuantError::WebSocket(WsError::ConnectionFailed {
                        shard_id: 0,
                        reason: format!("Binance aggTrade stream read: {error}"),
                    })
                })?;
            match message {
                Message::Text(text) => {
                    let row = serde_json::from_str::<BinanceAggTrade>(text.as_ref())
                        .map_err(WsError::from)?;
                    return map_report(&self.symbol, &row, Utc::now());
                }
                Message::Binary(bytes) => {
                    let row =
                        serde_json::from_slice::<BinanceAggTrade>(&bytes).map_err(WsError::from)?;
                    return map_report(&self.symbol, &row, Utc::now());
                }
                Message::Close(frame) => return Err(closed(frame).into()),
                Message::Ping(_) | Message::Pong(_) | Message::Frame(_) => {}
            }
        }
    }
}

fn map_report(
    symbol: &BinanceSymbol,
    row: &BinanceAggTrade,
    available_at: DateTime<Utc>,
) -> QuantResult<CryptoPriceReport> {
    if row
        .event_type
        .as_deref()
        .is_some_and(|kind| kind != "aggTrade")
    {
        return Err(ApiError::Deserialize {
            context: "binance aggregate trade".into(),
            detail: "unexpected websocket event type".to_owned(),
        }
        .into());
    }
    if row
        .symbol
        .as_deref()
        .is_some_and(|value| value != symbol.as_str())
    {
        return Err(ApiError::Deserialize {
            context: "binance aggregate trade".into(),
            detail: "payload symbol does not match stream binding".to_owned(),
        }
        .into());
    }
    if row.price <= rust_decimal::Decimal::ZERO || row.quantity <= rust_decimal::Decimal::ZERO {
        return Err(ApiError::Deserialize {
            context: "binance aggregate trade".into(),
            detail: "price and quantity must be positive".to_owned(),
        }
        .into());
    }
    let event_time = Utc
        .timestamp_millis_opt(row.trade_time_ms)
        .single()
        .ok_or_else(|| ApiError::Deserialize {
            context: "binance aggregate trade".into(),
            detail: format!("invalid trade time: {}", row.trade_time_ms),
        })?;
    let published_at = match row.event_time_ms {
        Some(timestamp) => Utc
            .timestamp_millis_opt(timestamp)
            .single()
            .ok_or_else(|| ApiError::Deserialize {
                context: "binance aggregate trade".into(),
                detail: format!("invalid event time: {timestamp}"),
            })?,
        None => event_time,
    };
    let raw_report = serde_json::to_string(&row).map_err(|error| ApiError::Deserialize {
        context: "binance aggregate trade serialization".into(),
        detail: error.to_string(),
    })?;
    let report_hash = ContentHash::parse(CanonicalDigest::prefixed_bytes(raw_report.as_bytes()))?;
    Ok(CryptoPriceReport {
        source_id: DomainSourceId::binance_agg_trade(),
        instrument_key: DomainInstrumentKey::binance_agg_trade(symbol),
        source_sequence: row.aggregate_trade_id,
        price: Usd::new(row.price),
        quantity: Some(Shares::new(row.quantity)),
        event_time,
        published_at,
        available_at,
        valid_from: None,
        observations_timestamp: None,
        expires_at: None,
        report_hash,
        raw_report,
    })
}

fn closed(frame: Option<CloseFrame>) -> WsError {
    WsError::ConnectionClosed {
        shard_id: 0,
        code: frame.map(|frame| frame.code.into()),
    }
}

#[cfg(test)]
mod tests {
    use chrono::{TimeZone, Utc};
    use quant_pivot_models::{config::BinanceSourceConfig, types::BinanceSymbol};
    use rust_decimal_macros::dec;
    use wiremock::{
        Mock, MockServer, ResponseTemplate,
        matchers::{method, path, query_param},
    };

    use super::BinanceAggTradeSource;

    #[tokio::test]
    async fn rest_recovery_maps_exact_trade_id_and_timestamps() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/api/v3/aggTrades"))
            .and(query_param("symbol", "BTCUSDT"))
            .and(query_param("fromId", "42"))
            .and(query_param("limit", "1000"))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(serde_json::json!([{
                    "a": 42,
                    "p": "65000.125",
                    "q": "0.01",
                    "f": 100,
                    "l": 101,
                    "T": 1_700_000_000_000_i64,
                    "m": false,
                    "M": true
                }])),
            )
            .mount(&server)
            .await;
        let source = BinanceAggTradeSource::connect(BinanceSourceConfig {
            rest_url: server.uri(),
            ..BinanceSourceConfig::default()
        })
        .expect("source");
        let symbol = BinanceSymbol::parse("BTCUSDT").expect("symbol");
        let available_at = Utc.with_ymd_and_hms(2023, 11, 14, 23, 0, 0).unwrap();
        let reports = source
            .recover_from(&symbol, 42, 2_000, available_at)
            .await
            .expect("recover");
        assert_eq!(reports.len(), 1);
        assert_eq!(reports[0].source_sequence, 42);
        assert_eq!(reports[0].price.inner(), dec!(65000.125));
        assert_eq!(reports[0].available_at, available_at);
    }
}
