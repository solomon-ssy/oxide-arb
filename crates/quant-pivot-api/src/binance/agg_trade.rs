//! Binance aggregate-trade REST recovery and rotating WebSocket stream.

use std::{
    mem,
    time::{Duration, Instant},
};

use chrono::{DateTime, NaiveDate, TimeZone, Utc};
use csv::{ReaderBuilder, StringRecord};
use futures_util::StreamExt;
use quant_pivot_error::{QuantError, QuantResult, api::ApiError, ws::WsError};
use quant_pivot_models::{
    config::BinanceSourceConfig,
    domain::data_plane::CryptoPriceReport,
    enums::domain::BinanceMarketSegment,
    hashing::CanonicalDigest,
    types::{BinanceSymbol, ContentHash, DomainInstrumentKey, DomainSourceId, Shares, Usd},
};
use reqwest::Client;
use rust_decimal::Decimal;
use tokio::{
    net::TcpStream,
    sync::{mpsc, mpsc::Sender},
};
use tokio_tungstenite::{
    MaybeTlsStream, WebSocketStream, connect_async,
    tungstenite::{Message, protocol::CloseFrame},
};

use super::{
    BinanceRequestBudget,
    archive::{
        BinanceArchiveBatchStream, archive_error, decode_single_csv_archive,
        download_verified_archive, parse_archive_field, send_batch,
    },
    validate_system_clock,
    wire::BinanceAggTrade,
};
use crate::infra::{http::get_text_with_retry, retry::RetryPolicy};

const MAX_REST_PAGE_SIZE: u16 = 1_000;
const SPOT_AGG_TRADE_REQUEST_WEIGHT: u32 = 4;
const USDM_FUTURES_AGG_TRADE_REQUEST_WEIGHT: u32 = 20;

/// Public Binance aggregate-trade client. REST recovery and WebSocket payloads
/// map to the exact same immutable fact contract.
pub struct BinanceAggTradeSource {
    market: BinanceMarketSegment,
    config: BinanceSourceConfig,
    http: Client,
    retry_policy: RetryPolicy,
    request_budget: BinanceRequestBudget,
}

impl BinanceAggTradeSource {
    pub fn connect(config: BinanceSourceConfig) -> QuantResult<Self> {
        let request_budget = BinanceRequestBudget::new(&config)?;
        Self::connect_with_budget(config, request_budget)
    }

    pub fn connect_with_budget(
        config: BinanceSourceConfig,
        request_budget: BinanceRequestBudget,
    ) -> QuantResult<Self> {
        Self::connect_for_market(config, request_budget, BinanceMarketSegment::Spot)
    }

    pub fn connect_usdm_futures_with_budget(
        config: BinanceSourceConfig,
        request_budget: BinanceRequestBudget,
    ) -> QuantResult<Self> {
        Self::connect_for_market(config, request_budget, BinanceMarketSegment::UsdmFutures)
    }

    fn connect_for_market(
        config: BinanceSourceConfig,
        request_budget: BinanceRequestBudget,
        market: BinanceMarketSegment,
    ) -> QuantResult<Self> {
        let http = Client::builder()
            .timeout(Duration::from_millis(config.request_timeout_ms))
            .build()
            .map_err(|error| ApiError::Sdk(format!("Binance aggTrade HTTP client: {error}")))?;
        Ok(Self {
            market,
            config,
            http,
            retry_policy: RetryPolicy::gamma_default(),
            request_budget,
        })
    }

    #[must_use]
    pub fn with_http_client(mut self, http: Client) -> Self {
        self.http = http;
        self
    }

    /// Validate local time against Binance's trusted server-time endpoint.
    pub async fn validate_system_clock(&self) -> QuantResult<()> {
        validate_system_clock(
            &self.config,
            &self.http,
            &self.retry_policy,
            &self.request_budget,
            self.market,
        )
        .await
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
        self.query(symbol, Some(from_id), limit, available_at).await
    }

    /// Load the current authoritative aggregate-trade frontier. Binance
    /// defines a request without `fromId/startTime/endTime` as the most recent
    /// trades; `limit=1` gives an exact durable bootstrap point.
    pub async fn latest(
        &self,
        symbol: &BinanceSymbol,
        available_at: DateTime<Utc>,
    ) -> QuantResult<Option<CryptoPriceReport>> {
        let mut reports = self.query(symbol, None, 1, available_at).await?;
        Ok(reports.pop())
    }

    #[must_use]
    pub const fn recovery_poll_interval(&self) -> Duration {
        Duration::from_secs(self.config.agg_trade_recovery_poll_secs)
    }

    async fn query(
        &self,
        symbol: &BinanceSymbol,
        from_id: Option<u64>,
        limit: u16,
        available_at: DateTime<Utc>,
    ) -> QuantResult<Vec<CryptoPriceReport>> {
        self.request_budget
            .acquire(agg_trade_request_weight(self.market))
            .await;
        let base = self.config.rest_url.trim_end_matches('/');
        let prefix = self.rest_prefix();
        let wire_symbol = symbol.as_str();
        let url = from_id.map_or_else(
            || format!("{base}{prefix}/aggTrades?symbol={wire_symbol}&limit={limit}"),
            |from_id| {
                format!(
                    "{base}{prefix}/aggTrades?symbol={wire_symbol}&fromId={from_id}&limit={limit}"
                )
            },
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
            .map(|row| map_report(self.market, symbol, &row, available_at))
            .collect()
    }

    /// Load one immutable official daily aggregate-trade archive, verify its
    /// sidecar SHA-256 checksum, and decode every row using the same fact
    /// contract as REST/WebSocket reports. `None` means Binance has not
    /// published that date yet; a missing checksum or corrupt archive fails.
    pub async fn recover_archive_day(
        &self,
        symbol: &BinanceSymbol,
        date: NaiveDate,
        available_at: DateTime<Utc>,
    ) -> QuantResult<Option<BinanceArchiveBatchStream<CryptoPriceReport>>> {
        let filename = format!("{}-aggTrades-{date}.zip", symbol.as_str());
        let url = format!(
            "{}/{}/daily/aggTrades/{}/{filename}",
            self.config.archive_url.trim_end_matches('/'),
            match self.market {
                BinanceMarketSegment::Spot => "data/spot",
                BinanceMarketSegment::UsdmFutures => "data/futures/um",
            },
            symbol.as_str(),
        );
        let Some(archive) =
            download_verified_archive(&self.http, &self.retry_policy, &url, &filename).await?
        else {
            return Ok(None);
        };
        let market = self.market;
        let symbol = symbol.clone();
        let member = filename.replace(".zip", ".csv");
        let batch_size = self.config.batch_size.max(1);
        let (sender, receiver) = mpsc::channel(2);
        let decoder = tokio::task::spawn_blocking(move || {
            decode_archive_batches(
                market,
                &symbol,
                archive,
                &member,
                available_at,
                batch_size,
                &sender,
            )
        });
        Ok(Some(BinanceArchiveBatchStream::new(receiver, decoder)))
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
            market: self.market,
            symbol: symbol.clone(),
            inner,
            rotate_at: Instant::now()
                + Duration::from_secs(self.config.websocket_rotation_secs.min(86_400)),
        })
    }

    const fn rest_prefix(&self) -> &'static str {
        match self.market {
            BinanceMarketSegment::Spot => "/api/v3",
            BinanceMarketSegment::UsdmFutures => "/fapi/v1",
        }
    }
}

const fn agg_trade_request_weight(market: BinanceMarketSegment) -> u32 {
    match market {
        BinanceMarketSegment::Spot => SPOT_AGG_TRADE_REQUEST_WEIGHT,
        BinanceMarketSegment::UsdmFutures => USDM_FUTURES_AGG_TRADE_REQUEST_WEIGHT,
    }
}

pub struct BinanceAggTradeStream {
    market: BinanceMarketSegment,
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
                    return map_report(self.market, &self.symbol, &row, Utc::now());
                }
                Message::Binary(bytes) => {
                    let row =
                        serde_json::from_slice::<BinanceAggTrade>(&bytes).map_err(WsError::from)?;
                    return map_report(self.market, &self.symbol, &row, Utc::now());
                }
                Message::Close(frame) => return Err(closed(frame).into()),
                Message::Ping(_) | Message::Pong(_) | Message::Frame(_) => {}
            }
        }
    }
}

fn map_report(
    market: BinanceMarketSegment,
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
    if row.price <= Decimal::ZERO || row.quantity <= Decimal::ZERO {
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
        source_id: agg_trade_source_id(market),
        instrument_key: agg_trade_instrument(market, symbol),
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

fn decode_archive_batches(
    market: BinanceMarketSegment,
    symbol: &BinanceSymbol,
    archive: Vec<u8>,
    expected_member: &str,
    available_at: DateTime<Utc>,
    batch_size: usize,
    sender: &Sender<Vec<CryptoPriceReport>>,
) -> QuantResult<()> {
    decode_single_csv_archive(archive, expected_member, |member| {
        let mut reader = ReaderBuilder::new().has_headers(false).from_reader(member);
        let mut reports = Vec::with_capacity(batch_size);
        for (index, row) in reader.records().enumerate() {
            let row = row.map_err(|error| archive_error(format!("invalid CSV row: {error}")))?;
            if index == 0
                && row
                    .get(0)
                    .is_some_and(|field| field.eq_ignore_ascii_case("agg_trade_id"))
            {
                continue;
            }
            reports.push(map_archive_row(market, symbol, &row, available_at)?);
            if reports.len() == batch_size && !send_batch(sender, mem::take(&mut reports)) {
                return Ok(());
            }
        }
        if !send_batch(sender, reports) {
            return Ok(());
        }
        Ok(())
    })
}

#[cfg(test)]
fn decode_archive(
    symbol: &BinanceSymbol,
    archive: Vec<u8>,
    expected_member: &str,
    available_at: DateTime<Utc>,
) -> QuantResult<Vec<CryptoPriceReport>> {
    decode_single_csv_archive(archive, expected_member, |member| {
        let mut reader = ReaderBuilder::new().has_headers(false).from_reader(member);
        let mut reports = Vec::new();
        for (index, row) in reader.records().enumerate() {
            let row = row.map_err(|error| archive_error(format!("invalid CSV row: {error}")))?;
            if index == 0
                && row
                    .get(0)
                    .is_some_and(|field| field.eq_ignore_ascii_case("agg_trade_id"))
            {
                continue;
            }
            reports.push(map_archive_row(
                BinanceMarketSegment::Spot,
                symbol,
                &row,
                available_at,
            )?);
        }
        Ok(reports)
    })
}

fn map_archive_row(
    market: BinanceMarketSegment,
    symbol: &BinanceSymbol,
    row: &StringRecord,
    available_at: DateTime<Utc>,
) -> QuantResult<CryptoPriceReport> {
    if row.len() != 8 {
        return Err(archive_error(format!(
            "aggregate-trade CSV row has {} fields instead of 8",
            row.len()
        ))
        .into());
    }
    let field = |index: usize, name: &str| {
        row.get(index)
            .ok_or_else(|| archive_error(format!("missing `{name}` field")))
    };
    let aggregate_trade_id = parse_archive_field(field(0, "aggregate_trade_id")?, "id")?;
    let price = parse_archive_field(field(1, "price")?, "price")?;
    let quantity = parse_archive_field(field(2, "quantity")?, "quantity")?;
    let _first_trade_id: u64 = parse_archive_field(field(3, "first_trade_id")?, "first id")?;
    let _last_trade_id: u64 = parse_archive_field(field(4, "last_trade_id")?, "last id")?;
    let timestamp: i64 = parse_archive_field(field(5, "timestamp")?, "timestamp")?;
    parse_archive_bool(field(6, "buyer_is_market_maker")?)?;
    parse_archive_bool(field(7, "best_price_match")?)?;
    if price <= Decimal::ZERO || quantity <= Decimal::ZERO {
        return Err(archive_error("price and quantity must be positive").into());
    }
    let event_time = if timestamp >= 100_000_000_000_000 {
        Utc.timestamp_micros(timestamp).single()
    } else {
        Utc.timestamp_millis_opt(timestamp).single()
    }
    .ok_or_else(|| archive_error(format!("invalid archive timestamp: {timestamp}")))?;
    let raw_report = row.iter().collect::<Vec<_>>().join(",");
    let report_hash = ContentHash::parse(CanonicalDigest::prefixed_bytes(raw_report.as_bytes()))?;
    Ok(CryptoPriceReport {
        source_id: agg_trade_source_id(market),
        instrument_key: agg_trade_instrument(market, symbol),
        source_sequence: aggregate_trade_id,
        price: Usd::new(price),
        quantity: Some(Shares::new(quantity)),
        event_time,
        published_at: event_time,
        available_at,
        valid_from: None,
        observations_timestamp: None,
        expires_at: None,
        report_hash,
        raw_report,
    })
}

fn agg_trade_source_id(market: BinanceMarketSegment) -> DomainSourceId {
    match market {
        BinanceMarketSegment::Spot => DomainSourceId::binance_agg_trade(),
        BinanceMarketSegment::UsdmFutures => DomainSourceId::binance_usdm_futures_agg_trade(),
    }
}

fn agg_trade_instrument(
    market: BinanceMarketSegment,
    symbol: &BinanceSymbol,
) -> DomainInstrumentKey {
    match market {
        BinanceMarketSegment::Spot => DomainInstrumentKey::binance_agg_trade(symbol),
        BinanceMarketSegment::UsdmFutures => {
            DomainInstrumentKey::binance_usdm_futures_agg_trade(symbol)
        }
    }
}

fn parse_archive_bool(raw: &str) -> QuantResult<bool> {
    if raw.eq_ignore_ascii_case("true") {
        Ok(true)
    } else if raw.eq_ignore_ascii_case("false") {
        Ok(false)
    } else {
        Err(archive_error(format!("invalid boolean `{raw}`")).into())
    }
}

fn closed(frame: Option<CloseFrame>) -> WsError {
    WsError::ConnectionClosed {
        shard_id: 0,
        code: frame.map(|frame| frame.code.into()),
    }
}

#[cfg(test)]
mod tests {
    use std::io::{Cursor, Write};

    use chrono::{NaiveDate, TimeZone, Utc};
    use futures_util::SinkExt;
    use quant_pivot_models::{
        config::BinanceSourceConfig,
        enums::domain::BinanceMarketSegment,
        types::{BinanceSymbol, DomainInstrumentKey, DomainSourceId},
    };
    use rust_decimal_macros::dec;
    use sha2::{Digest, Sha256};
    use tokio::net::TcpListener;
    use tokio_tungstenite::{accept_async, tungstenite::Message};
    use wiremock::{
        Mock, MockServer, ResponseTemplate,
        matchers::{method, path, query_param},
    };
    use zip::{ZipWriter, write::SimpleFileOptions};

    use super::{BinanceAggTradeSource, agg_trade_request_weight, decode_archive};
    use crate::binance::{BinanceRequestBudget, archive::verify_archive_checksum};

    fn archive_bytes(csv: &str) -> Vec<u8> {
        let mut writer = ZipWriter::new(Cursor::new(Vec::new()));
        writer
            .start_file(
                "BTCUSDT-aggTrades-2025-01-01.csv",
                SimpleFileOptions::default(),
            )
            .expect("start archive member");
        writer.write_all(csv.as_bytes()).expect("write archive CSV");
        writer.finish().expect("finish archive").into_inner()
    }

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

    #[tokio::test]
    async fn latest_bootstrap_omits_from_id_and_preserves_futures_provenance() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/fapi/v1/aggTrades"))
            .and(query_param("symbol", "HYPEUSDT"))
            .and(query_param("limit", "1"))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(serde_json::json!([{
                    "a": 225_496_651_u64,
                    "p": "58.833",
                    "q": "0.1",
                    "f": 300,
                    "l": 300,
                    "T": 1_784_374_523_343_i64,
                    "m": true
                }])),
            )
            .mount(&server)
            .await;
        let config = BinanceSourceConfig {
            rest_url: server.uri(),
            ..BinanceSourceConfig::usdm_futures_default()
        };
        let source = BinanceAggTradeSource::connect_usdm_futures_with_budget(
            config.clone(),
            BinanceRequestBudget::new(&config).expect("budget"),
        )
        .expect("source");
        let symbol = BinanceSymbol::parse("HYPEUSDT").expect("symbol");
        let report = source
            .latest(&symbol, Utc::now())
            .await
            .expect("latest")
            .expect("latest report");
        assert_eq!(report.source_sequence, 225_496_651);
        assert_eq!(
            report.source_id,
            DomainSourceId::binance_usdm_futures_agg_trade()
        );
        assert_eq!(
            report.instrument_key,
            DomainInstrumentKey::binance_usdm_futures_agg_trade(&symbol)
        );
        let requests = server.received_requests().await.expect("requests");
        assert_eq!(requests.len(), 1);
        assert!(
            requests[0]
                .url
                .query_pairs()
                .all(|(name, _)| name != "fromId")
        );
    }

    #[tokio::test]
    async fn websocket_stream_maps_raw_usdm_trade_with_bound_provenance() {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind local WebSocket server");
        let address = listener.local_addr().expect("local WebSocket address");
        let server = tokio::spawn(async move {
            let (connection, _) = listener.accept().await.expect("accept WebSocket client");
            let mut socket = accept_async(connection).await.expect("WebSocket handshake");
            socket
                .send(Message::Text(
                    serde_json::json!({
                        "e": "aggTrade",
                        "E": 1_784_374_523_400_i64,
                        "s": "HYPEUSDT",
                        "a": 225_496_651_u64,
                        "p": "58.833",
                        "q": "0.1",
                        "f": 300,
                        "l": 300,
                        "T": 1_784_374_523_343_i64,
                        "m": true
                    })
                    .to_string()
                    .into(),
                ))
                .await
                .expect("send aggregate-trade frame");
        });
        let config = BinanceSourceConfig {
            websocket_url: format!("ws://{address}"),
            ..BinanceSourceConfig::usdm_futures_default()
        };
        let source = BinanceAggTradeSource::connect_usdm_futures_with_budget(
            config.clone(),
            BinanceRequestBudget::new(&config).expect("budget"),
        )
        .expect("source");
        let symbol = BinanceSymbol::parse("HYPEUSDT").expect("symbol");
        let mut stream = source.stream(&symbol).await.expect("open local stream");
        let report = stream.next_report().await.expect("map aggregate trade");

        assert_eq!(report.source_sequence, 225_496_651);
        assert_eq!(
            report.source_id,
            DomainSourceId::binance_usdm_futures_agg_trade()
        );
        assert_eq!(
            report.instrument_key,
            DomainInstrumentKey::binance_usdm_futures_agg_trade(&symbol)
        );
        assert_eq!(report.price.inner(), dec!(58.833));
        server.await.expect("local WebSocket server");
    }

    #[test]
    fn request_weight_is_source_native() {
        assert_eq!(agg_trade_request_weight(BinanceMarketSegment::Spot), 4);
        assert_eq!(
            agg_trade_request_weight(BinanceMarketSegment::UsdmFutures),
            20
        );
    }

    #[tokio::test]
    async fn usdm_futures_recovery_uses_fapi_and_preserves_provenance() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/fapi/v1/aggTrades"))
            .and(query_param("symbol", "HYPEUSDT"))
            .and(query_param("fromId", "7"))
            .and(query_param("limit", "1000"))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(serde_json::json!([{
                    "a": 7,
                    "p": "42.125",
                    "q": "1.5",
                    "f": 20,
                    "l": 21,
                    "T": 1_753_056_000_000_i64,
                    "m": false,
                    "M": true
                }])),
            )
            .mount(&server)
            .await;
        let config = BinanceSourceConfig {
            rest_url: server.uri(),
            ..BinanceSourceConfig::default()
        };
        let source = BinanceAggTradeSource::connect_usdm_futures_with_budget(
            config.clone(),
            BinanceRequestBudget::new(&config).expect("budget"),
        )
        .expect("source");
        let symbol = BinanceSymbol::parse("HYPEUSDT").expect("symbol");
        let reports = source
            .recover_from(&symbol, 7, 2_000, Utc::now())
            .await
            .expect("recover");
        assert_eq!(reports.len(), 1);
        assert_eq!(
            reports[0].source_id,
            DomainSourceId::binance_usdm_futures_agg_trade()
        );
        assert_eq!(
            reports[0].instrument_key,
            DomainInstrumentKey::binance_usdm_futures_agg_trade(&symbol)
        );
    }

    #[tokio::test]
    async fn official_archive_decodes_through_bounded_batches() {
        let server = MockServer::start().await;
        let bytes = archive_bytes(
            "42,65000.125,0.01,100,101,1735689600010866,False,True\n\
             43,65001.125,0.02,102,103,1735689600011866,True,True\n",
        );
        let filename = "BTCUSDT-aggTrades-2025-01-01.zip";
        let archive_path = format!("/data/spot/daily/aggTrades/BTCUSDT/{filename}");
        let checksum = format!("{}  {filename}\n", hex::encode(Sha256::digest(&bytes)));
        Mock::given(method("GET"))
            .and(path(archive_path.as_str()))
            .respond_with(ResponseTemplate::new(200).set_body_bytes(bytes))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path(format!("{archive_path}.CHECKSUM")))
            .respond_with(ResponseTemplate::new(200).set_body_string(checksum))
            .mount(&server)
            .await;
        let source = BinanceAggTradeSource::connect(BinanceSourceConfig {
            archive_url: server.uri(),
            batch_size: 1,
            ..BinanceSourceConfig::default()
        })
        .expect("source");
        let symbol = BinanceSymbol::parse("BTCUSDT").expect("symbol");
        let available_at = Utc
            .with_ymd_and_hms(2025, 1, 2, 0, 0, 0)
            .single()
            .expect("available at");
        let mut stream = source
            .recover_archive_day(
                &symbol,
                NaiveDate::from_ymd_opt(2025, 1, 1).expect("date"),
                available_at,
            )
            .await
            .expect("verified transport")
            .expect("published archive");
        let first = stream
            .next_batch()
            .await
            .expect("first batch")
            .expect("first batch exists");
        let second = stream
            .next_batch()
            .await
            .expect("second batch")
            .expect("second batch exists");
        assert_eq!(first.len(), 1);
        assert_eq!(second.len(), 1);
        assert_eq!(first[0].source_sequence, 42);
        assert_eq!(second[0].source_sequence, 43);
        assert!(stream.next_batch().await.expect("end of stream").is_none());
    }

    #[test]
    fn archive_checksum_and_microsecond_rows_are_strictly_decoded() {
        let bytes = archive_bytes(
            "agg_trade_id,price,quantity,first_trade_id,last_trade_id,timestamp,\
             buyer_is_market_maker,best_price_match\n\
             42,65000.125,0.01,100,101,1735689600010866,False,True\n",
        );
        let filename = "BTCUSDT-aggTrades-2025-01-01.zip";
        let checksum = format!("{}  {filename}\n", hex::encode(Sha256::digest(&bytes)));
        verify_archive_checksum(filename, &bytes, checksum.as_bytes()).expect("checksum");

        let symbol = BinanceSymbol::parse("BTCUSDT").expect("symbol");
        let available_at = Utc
            .with_ymd_and_hms(2025, 1, 2, 0, 0, 0)
            .single()
            .expect("available at");
        let reports = decode_archive(
            &symbol,
            bytes.clone(),
            "BTCUSDT-aggTrades-2025-01-01.csv",
            available_at,
        )
        .expect("decode archive");
        assert_eq!(reports.len(), 1);
        assert_eq!(reports[0].source_sequence, 42);
        assert_eq!(reports[0].price.inner(), dec!(65000.125));
        assert_eq!(
            reports[0].event_time.timestamp_micros(),
            1_735_689_600_010_866
        );
        assert_eq!(reports[0].available_at, available_at);

        let mut corrupt = bytes;
        corrupt.push(0);
        assert!(verify_archive_checksum(filename, &corrupt, checksum.as_bytes()).is_err());
    }
}
