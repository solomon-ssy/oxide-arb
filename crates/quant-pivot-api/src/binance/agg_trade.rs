//! Binance aggregate-trade REST recovery and rotating WebSocket stream.

use std::{
    fmt::Display,
    io::{Cursor, Read},
    path::Path,
    str::{self, FromStr},
    time::{Duration, Instant},
};

use chrono::{DateTime, NaiveDate, TimeZone, Utc};
use futures_util::StreamExt;
use quant_pivot_error::{QuantError, QuantResult, api::ApiError, ws::WsError};
use quant_pivot_models::{
    config::BinanceSourceConfig,
    domain::CryptoPriceReport,
    hashing::CanonicalDigest,
    types::{BinanceSymbol, ContentHash, DomainInstrumentKey, DomainSourceId, Shares, Usd},
};
use sha2::{Digest, Sha256};
use tokio::net::TcpStream;
use tokio_tungstenite::{
    MaybeTlsStream, WebSocketStream, connect_async,
    tungstenite::{Message, protocol::CloseFrame},
};

use super::wire::BinanceAggTrade;
use crate::infra::{
    http::{get_optional_bytes_with_retry, get_text_with_retry},
    retry::RetryPolicy,
};

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

    /// Load one immutable official daily aggregate-trade archive, verify its
    /// sidecar SHA-256 checksum, and decode every row using the same fact
    /// contract as REST/WebSocket reports. `None` means Binance has not
    /// published that date yet; a missing checksum or corrupt archive fails.
    pub async fn recover_archive_day(
        &self,
        symbol: &BinanceSymbol,
        date: NaiveDate,
        available_at: DateTime<Utc>,
    ) -> QuantResult<Option<Vec<CryptoPriceReport>>> {
        let filename = format!("{}-aggTrades-{date}.zip", symbol.as_str());
        let url = format!(
            "{}/data/spot/daily/aggTrades/{}/{filename}",
            self.config.archive_url.trim_end_matches('/'),
            symbol.as_str(),
        );
        let checksum_url = format!("{url}.CHECKSUM");
        let (archive, checksum) = tokio::try_join!(
            get_optional_bytes_with_retry(&self.http, &self.retry_policy, &url),
            get_optional_bytes_with_retry(&self.http, &self.retry_policy, &checksum_url),
        )?;
        let Some(archive) = archive else {
            if checksum.is_some() {
                return Err(archive_error("checksum exists but archive is absent").into());
            }
            return Ok(None);
        };
        let checksum = checksum
            .ok_or_else(|| archive_error("archive exists without its required checksum"))?;
        verify_archive_checksum(&filename, &archive, &checksum)?;
        let symbol = symbol.clone();
        let rows =
            tokio::task::spawn_blocking(move || decode_archive(&symbol, archive, available_at))
                .await
                .map_err(|error| {
                    QuantError::config(format!("Binance archive decode task: {error}"))
                })??;
        Ok(Some(rows))
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

fn verify_archive_checksum(filename: &str, archive: &[u8], checksum: &[u8]) -> QuantResult<()> {
    let text = str::from_utf8(checksum)
        .map_err(|error| archive_error(format!("checksum is not UTF-8: {error}")))?;
    let mut fields = text.split_whitespace();
    let expected = fields
        .next()
        .filter(|value| value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit()))
        .ok_or_else(|| archive_error("checksum does not start with a SHA-256 digest"))?;
    let named_file = fields
        .next()
        .map(|value| value.trim_start_matches('*'))
        .ok_or_else(|| archive_error("checksum does not name its archive"))?;
    if named_file != filename {
        return Err(archive_error("checksum filename does not match requested archive").into());
    }
    let actual = hex::encode(Sha256::digest(archive));
    if !actual.eq_ignore_ascii_case(expected) {
        return Err(archive_error("archive SHA-256 checksum mismatch").into());
    }
    Ok(())
}

fn decode_archive(
    symbol: &BinanceSymbol,
    archive: Vec<u8>,
    available_at: DateTime<Utc>,
) -> QuantResult<Vec<CryptoPriceReport>> {
    let mut zip = zip::ZipArchive::new(Cursor::new(archive))
        .map_err(|error| archive_error(format!("invalid ZIP: {error}")))?;
    if zip.len() != 1 {
        return Err(archive_error("archive must contain exactly one CSV member").into());
    }
    let mut member = zip
        .by_index(0)
        .map_err(|error| archive_error(format!("cannot open ZIP member: {error}")))?;
    if member.is_dir()
        || !Path::new(member.name())
            .extension()
            .is_some_and(|extension| extension.eq_ignore_ascii_case("csv"))
    {
        return Err(archive_error("archive member must be a CSV file").into());
    }
    let mut csv_bytes = Vec::new();
    member
        .read_to_end(&mut csv_bytes)
        .map_err(|error| archive_error(format!("cannot read ZIP member: {error}")))?;
    let mut reader = csv::ReaderBuilder::new()
        .has_headers(false)
        .from_reader(csv_bytes.as_slice());
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
        reports.push(map_archive_row(symbol, &row, available_at)?);
    }
    Ok(reports)
}

fn map_archive_row(
    symbol: &BinanceSymbol,
    row: &csv::StringRecord,
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
    if price <= rust_decimal::Decimal::ZERO || quantity <= rust_decimal::Decimal::ZERO {
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
        source_id: DomainSourceId::binance_agg_trade(),
        instrument_key: DomainInstrumentKey::binance_agg_trade(symbol),
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

fn parse_archive_field<T>(raw: &str, name: &str) -> QuantResult<T>
where
    T: FromStr,
    T::Err: Display,
{
    raw.parse()
        .map_err(|error| archive_error(format!("invalid {name} `{raw}`: {error}")).into())
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

fn archive_error(detail: impl Into<String>) -> ApiError {
    ApiError::Deserialize {
        context: "Binance public-data aggregate-trade archive".to_owned(),
        detail: detail.into(),
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

    use chrono::{TimeZone, Utc};
    use quant_pivot_models::{config::BinanceSourceConfig, types::BinanceSymbol};
    use rust_decimal_macros::dec;
    use sha2::{Digest, Sha256};
    use wiremock::{
        Mock, MockServer, ResponseTemplate,
        matchers::{method, path, query_param},
    };

    use super::{BinanceAggTradeSource, decode_archive, verify_archive_checksum};

    fn archive_bytes(csv: &str) -> Vec<u8> {
        let mut writer = zip::ZipWriter::new(Cursor::new(Vec::new()));
        writer
            .start_file(
                "BTCUSDT-aggTrades-2025-01-01.csv",
                zip::write::SimpleFileOptions::default(),
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
        let reports = decode_archive(&symbol, bytes.clone(), available_at).expect("decode archive");
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
