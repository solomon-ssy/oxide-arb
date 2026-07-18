//! Binance Spot and USD-M Futures REST kline sources with distinct provenance.

mod agg_trade;
mod archive;
mod mapper;
mod wire;

use std::{mem, num::NonZeroU32, slice, sync::Arc, time::Duration};

use chrono::{DateTime, NaiveDate, TimeZone, Utc};
use governor::{
    Quota, RateLimiter,
    clock::DefaultClock,
    state::{InMemoryState, NotKeyed},
};
use quant_pivot_error::{QuantError, QuantResult, api::ApiError};
use quant_pivot_models::{
    config::BinanceSourceConfig,
    domain::DomainObservation,
    enums::domain::{BinanceMarketSegment, DomainFamily, KlineInterval},
    types::{BinanceSymbol, DomainInstrumentKey, DomainSourceId},
};
use tokio::sync::mpsc;

use crate::{
    domain::{DomainDataSource, DomainFetchRequest},
    infra::{http::get_text_with_retry, retry::RetryPolicy},
};

use self::archive::{
    archive_error, decode_single_csv_archive, download_verified_archive, parse_archive_field,
    send_batch,
};

pub use agg_trade::{BinanceAggTradeSource, BinanceAggTradeStream};
pub use archive::BinanceArchiveBatchStream;
pub use wire::BinanceAggTrade;
pub use wire::{BinanceKlineRow, KLINE_FIELD_COUNT};

type WeightLimiter = RateLimiter<NotKeyed, InMemoryState, DefaultClock>;

/// Weight cost of one kline request (Binance public API).
const KLINE_REQUEST_WEIGHT: u32 = 2;

/// Weight cost of one server-time request.
const SERVER_TIME_REQUEST_WEIGHT: u32 = 1;

/// Maximum klines per REST request.
const KLINE_PAGE_SIZE: u32 = 1_000;

/// Process-shared Binance IP request-weight budget.
///
/// Binance accounts REST usage by source IP rather than API key, so kline
/// polling, aggTrade recovery and clock sampling must share this limiter.
#[derive(Clone)]
pub struct BinanceRequestBudget {
    limiter: Arc<WeightLimiter>,
}

impl BinanceRequestBudget {
    /// Build from the proactive deploy budget.
    pub fn new(config: &BinanceSourceConfig) -> QuantResult<Self> {
        let budget = config.weight_budget_per_min.max(KLINE_REQUEST_WEIGHT);
        let budget = NonZeroU32::new(budget)
            .ok_or_else(|| QuantError::config("Binance weight budget must be non-zero"))?;
        Ok(Self {
            limiter: Arc::new(RateLimiter::direct(Quota::per_minute(budget))),
        })
    }

    async fn acquire(&self, weight: u32) {
        for _ in 0..weight {
            self.limiter.until_ready().await;
        }
    }
}

/// Binance spot kline client with proactive weight budgeting.
pub struct BinanceKlineSource {
    market: BinanceMarketSegment,
    config: BinanceSourceConfig,
    http: reqwest::Client,
    retry_policy: RetryPolicy,
    request_budget: BinanceRequestBudget,
}

impl BinanceKlineSource {
    /// Build from deploy configuration.
    pub fn connect(config: BinanceSourceConfig) -> QuantResult<Self> {
        let request_budget = BinanceRequestBudget::new(&config)?;
        Self::connect_with_budget(config, request_budget)
    }

    /// Build with the process-shared Binance IP request budget.
    pub fn connect_with_budget(
        config: BinanceSourceConfig,
        request_budget: BinanceRequestBudget,
    ) -> QuantResult<Self> {
        Self::connect_for_market(config, request_budget, BinanceMarketSegment::Spot)
    }

    /// Build a USD-M Futures client with its own venue request budget.
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
        // A hung TCP connection must never block an ingest tick indefinitely
        // (R10 ingest hardening); `reqwest::Client::new()` has no request
        // timeout by default.
        let http = reqwest::Client::builder()
            .timeout(Duration::from_millis(config.request_timeout_ms))
            .build()
            .map_err(|error| ApiError::Sdk(format!("Binance HTTP client: {error}")))?;
        Ok(Self {
            market,
            config,
            http,
            retry_policy: RetryPolicy::gamma_default(),
            request_budget,
        })
    }

    /// Override the HTTP client (tests).
    #[must_use]
    pub fn with_http_client(mut self, http: reqwest::Client) -> Self {
        self.http = http;
        self
    }

    fn parse_instrument(
        &self,
        key: &DomainInstrumentKey,
    ) -> QuantResult<(BinanceSymbol, KlineInterval)> {
        let (market, symbol, interval) = key.as_binance_market_kline().ok_or_else(|| {
            QuantError::config(format!("not a canonical Binance kline key: {key}"))
        })?;
        if market != self.market {
            return Err(QuantError::config(format!(
                "Binance {:?} source cannot ingest {:?} instrument `{key}`",
                self.market, market
            )));
        }
        Ok((symbol, interval))
    }

    fn source_id_for_market(&self) -> DomainSourceId {
        match self.market {
            BinanceMarketSegment::Spot => DomainSourceId::binance(),
            BinanceMarketSegment::UsdmFutures => DomainSourceId::binance_usdm_futures(),
        }
    }

    fn instrument_key(
        &self,
        symbol: &BinanceSymbol,
        interval: KlineInterval,
    ) -> DomainInstrumentKey {
        match self.market {
            BinanceMarketSegment::Spot => DomainInstrumentKey::binance_kline(symbol, interval),
            BinanceMarketSegment::UsdmFutures => {
                DomainInstrumentKey::binance_usdm_futures_kline(symbol, interval)
            }
        }
    }

    const fn rest_prefix(&self) -> &'static str {
        match self.market {
            BinanceMarketSegment::Spot => "/api/v3",
            BinanceMarketSegment::UsdmFutures => "/fapi/v1",
        }
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

    /// Load one immutable official daily kline archive, verify the Binance
    /// SHA-256 sidecar, normalize the 2025+ microsecond timestamp format, and
    /// enforce the same OHLCV/continuity invariants as REST.
    pub async fn recover_archive_day(
        &self,
        symbol: &BinanceSymbol,
        interval: KlineInterval,
        date: NaiveDate,
        available_at: DateTime<Utc>,
    ) -> QuantResult<Option<BinanceArchiveBatchStream<DomainObservation>>> {
        let filename = format!("{}-{}-{date}.zip", symbol.as_str(), interval.as_str());
        let url = format!(
            "{}/{}/daily/klines/{}/{}/{filename}",
            self.config.archive_url.trim_end_matches('/'),
            match self.market {
                BinanceMarketSegment::Spot => "data/spot",
                BinanceMarketSegment::UsdmFutures => "data/futures/um",
            },
            symbol.as_str(),
            interval.as_str(),
        );
        let Some(archive) =
            download_verified_archive(&self.http, &self.retry_policy, &url, &filename).await?
        else {
            return Ok(None);
        };
        let instrument_key = self.instrument_key(symbol, interval);
        let member = filename.replace(".zip", ".csv");
        let batch_size = self.config.batch_size.max(1);
        let (sender, receiver) = mpsc::channel(2);
        let decoder = tokio::task::spawn_blocking(move || {
            decode_kline_archive_batches(
                archive,
                &member,
                interval,
                &instrument_key,
                available_at,
                batch_size,
                &sender,
            )
        });
        Ok(Some(BinanceArchiveBatchStream::new(receiver, decoder)))
    }

    async fn fetch_page(
        &self,
        symbol: &BinanceSymbol,
        interval: KlineInterval,
        start_ms: i64,
        end_ms: i64,
    ) -> QuantResult<Vec<DomainObservation>> {
        self.request_budget.acquire(KLINE_REQUEST_WEIGHT).await;
        let url = format!(
            "{}{}/klines?symbol={}&interval={}&startTime={}&endTime={}&limit={KLINE_PAGE_SIZE}",
            self.config.rest_url.trim_end_matches('/'),
            self.rest_prefix(),
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
        validate_kline_rows(&rows, interval)?;
        let instrument_key = self.instrument_key(symbol, interval);
        let mut observations = Vec::with_capacity(rows.len());
        // Binance includes the currently open kline when its open time is in
        // range, even though that row's close time is after `endTime`. An open
        // candle is mutable evidence and must never advance the durable cursor.
        for row in rows.into_iter().filter(|row| row.close_time_ms <= end_ms) {
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
        self.source_id_for_market()
    }

    async fn fetch(&self, request: DomainFetchRequest) -> QuantResult<Vec<DomainObservation>> {
        let (symbol, interval) = self.parse_instrument(&request.instrument_key)?;
        let mut observations = Vec::new();
        let mut cursor_ms = request.from_exclusive.timestamp_millis();
        let end_ms = request.to_inclusive.timestamp_millis();
        if cursor_ms >= end_ms {
            return Ok(observations);
        }
        self.validate_system_clock().await?;

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
                .ok_or_else(|| ApiError::Deserialize {
                    context: "binance kline pagination".to_owned(),
                    detail: "non-empty page lost its terminal observation".to_owned(),
                })?;
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
        validate_observation_continuity(
            &observations,
            request.from_exclusive,
            interval,
            request.bootstrap,
        )?;
        Ok(observations)
    }
}

fn validate_kline_rows(rows: &[wire::BinanceKlineRow], interval: KlineInterval) -> QuantResult<()> {
    let interval_ms = kline_interval_ms(interval)?;
    for (index, row) in rows.iter().enumerate() {
        if row.open <= rust_decimal::Decimal::ZERO
            || row.high <= rust_decimal::Decimal::ZERO
            || row.low <= rust_decimal::Decimal::ZERO
            || row.close <= rust_decimal::Decimal::ZERO
            || row.volume < rust_decimal::Decimal::ZERO
            || row.quote_volume < rust_decimal::Decimal::ZERO
            || row.taker_buy_base_volume < rust_decimal::Decimal::ZERO
            || row.taker_buy_quote_volume < rust_decimal::Decimal::ZERO
            || row.high < row.open.max(row.close)
            || row.low > row.open.min(row.close)
            || row.high < row.low
            || row.close_time_ms != row.open_time_ms.saturating_add(interval_ms - 1)
        {
            return Err(ApiError::Deserialize {
                context: "binance kline semantics".to_owned(),
                detail: format!("invalid OHLCV/time invariant at row {index}"),
            }
            .into());
        }
    }
    for (index, pair) in rows.windows(2).enumerate() {
        validate_adjacent_kline_rows(&pair[0], &pair[1], interval_ms, index + 1)?;
    }
    Ok(())
}

fn kline_interval_ms(interval: KlineInterval) -> QuantResult<i64> {
    i64::try_from(interval.secs().saturating_mul(1_000)).map_err(|error| {
        QuantError::config(format!(
            "Binance kline interval does not fit milliseconds: {error}"
        ))
    })
}

fn validate_adjacent_kline_rows(
    previous: &wire::BinanceKlineRow,
    next: &wire::BinanceKlineRow,
    interval_ms: i64,
    index: usize,
) -> QuantResult<()> {
    let expected = previous.open_time_ms.saturating_add(interval_ms);
    if next.open_time_ms != expected {
        return Err(ApiError::Deserialize {
            context: "binance kline continuity".to_owned(),
            detail: format!(
                "row {index} opens at {} but expected {expected}",
                next.open_time_ms
            ),
        }
        .into());
    }
    Ok(())
}

fn validate_observation_continuity(
    observations: &[DomainObservation],
    from_exclusive: DateTime<Utc>,
    interval: KlineInterval,
    bootstrap: bool,
) -> QuantResult<()> {
    let interval_ms = kline_interval_ms(interval)?;
    if !bootstrap && let Some(first) = observations.first() {
        let expected = from_exclusive
            .timestamp_millis()
            .saturating_add(interval_ms);
        if first.observed_at.timestamp_millis() != expected {
            return Err(ApiError::Deserialize {
                context: "binance kline cursor continuity".to_owned(),
                detail: format!(
                    "first close is {} but cursor requires {expected}",
                    first.observed_at.timestamp_millis()
                ),
            }
            .into());
        }
    }
    for (index, pair) in observations.windows(2).enumerate() {
        let expected = pair[0]
            .observed_at
            .timestamp_millis()
            .saturating_add(interval_ms);
        if pair[1].observed_at.timestamp_millis() != expected {
            return Err(ApiError::Deserialize {
                context: "binance kline cursor continuity".to_owned(),
                detail: format!(
                    "observation {} closes at {} but expected {expected}",
                    index + 1,
                    pair[1].observed_at.timestamp_millis()
                ),
            }
            .into());
        }
    }
    Ok(())
}

fn decode_kline_archive_batches(
    archive: Vec<u8>,
    expected_member: &str,
    interval: KlineInterval,
    instrument_key: &DomainInstrumentKey,
    available_at: DateTime<Utc>,
    batch_size: usize,
    sender: &mpsc::Sender<Vec<DomainObservation>>,
) -> QuantResult<()> {
    let interval_ms = kline_interval_ms(interval)?;
    decode_single_csv_archive(archive, expected_member, |member| {
        let mut reader = csv::ReaderBuilder::new()
            .has_headers(false)
            .from_reader(member);
        let mut previous = None::<wire::BinanceKlineRow>;
        let mut batch = Vec::with_capacity(batch_size);
        for (index, record) in reader.records().enumerate() {
            let record =
                record.map_err(|error| archive_error(format!("invalid CSV row: {error}")))?;
            if index == 0
                && record
                    .get(0)
                    .is_some_and(|field| field.eq_ignore_ascii_case("open_time"))
            {
                continue;
            }
            let row = parse_archive_kline_row(&record)?;
            validate_kline_rows(slice::from_ref(&row), interval)?;
            if let Some(previous) = previous.as_ref() {
                validate_adjacent_kline_rows(previous, &row, interval_ms, index)?;
            }
            let [mut observation] = mapper::into_observations(&row, instrument_key)?;
            observation.available_at = Some(available_at);
            batch.push(observation);
            previous = Some(row);
            if batch.len() == batch_size && !send_batch(sender, mem::take(&mut batch)) {
                return Ok(());
            }
        }
        if !send_batch(sender, batch) {
            return Ok(());
        }
        Ok(())
    })
}

#[cfg(test)]
fn decode_kline_archive(
    archive: Vec<u8>,
    expected_member: &str,
    interval: KlineInterval,
) -> QuantResult<Vec<wire::BinanceKlineRow>> {
    decode_single_csv_archive(archive, expected_member, |member| {
        let mut reader = csv::ReaderBuilder::new()
            .has_headers(false)
            .from_reader(member);
        let mut rows = Vec::new();
        for (index, record) in reader.records().enumerate() {
            let record =
                record.map_err(|error| archive_error(format!("invalid CSV row: {error}")))?;
            if index == 0
                && record
                    .get(0)
                    .is_some_and(|field| field.eq_ignore_ascii_case("open_time"))
            {
                continue;
            }
            rows.push(parse_archive_kline_row(&record)?);
        }
        validate_kline_rows(&rows, interval)?;
        Ok(rows)
    })
}

fn parse_archive_kline_row(row: &csv::StringRecord) -> QuantResult<wire::BinanceKlineRow> {
    if row.len() != wire::KLINE_FIELD_COUNT {
        return Err(archive_error(format!(
            "kline CSV row has {} fields instead of {}",
            row.len(),
            wire::KLINE_FIELD_COUNT
        ))
        .into());
    }
    let field = |index: usize, name: &str| {
        row.get(index)
            .ok_or_else(|| archive_error(format!("missing `{name}` field")))
    };
    Ok(wire::BinanceKlineRow {
        open_time_ms: normalize_archive_timestamp(parse_archive_field(
            field(0, "open_time")?,
            "open_time",
        )?),
        open: parse_archive_field(field(1, "open")?, "open")?,
        high: parse_archive_field(field(2, "high")?, "high")?,
        low: parse_archive_field(field(3, "low")?, "low")?,
        close: parse_archive_field(field(4, "close")?, "close")?,
        volume: parse_archive_field(field(5, "volume")?, "volume")?,
        close_time_ms: normalize_archive_timestamp(parse_archive_field(
            field(6, "close_time")?,
            "close_time",
        )?),
        quote_volume: parse_archive_field(field(7, "quote_volume")?, "quote_volume")?,
        trade_count: parse_archive_field(field(8, "trade_count")?, "trade_count")?,
        taker_buy_base_volume: parse_archive_field(
            field(9, "taker_buy_base_volume")?,
            "taker_buy_base_volume",
        )?,
        taker_buy_quote_volume: parse_archive_field(
            field(10, "taker_buy_quote_volume")?,
            "taker_buy_quote_volume",
        )?,
        ignore: field(11, "ignore")?.to_owned(),
    })
}

const fn normalize_archive_timestamp(timestamp: i64) -> i64 {
    if timestamp.unsigned_abs() >= 100_000_000_000_000 {
        timestamp / 1_000
    } else {
        timestamp
    }
}

async fn validate_system_clock(
    config: &BinanceSourceConfig,
    http: &reqwest::Client,
    retry_policy: &RetryPolicy,
    request_budget: &BinanceRequestBudget,
    market: BinanceMarketSegment,
) -> QuantResult<()> {
    request_budget.acquire(SERVER_TIME_REQUEST_WEIGHT).await;
    let sent_at = Utc::now();
    let path = match market {
        BinanceMarketSegment::Spot => "/api/v3/time",
        BinanceMarketSegment::UsdmFutures => "/fapi/v1/time",
    };
    let url = format!("{}{path}", config.rest_url.trim_end_matches('/'));
    let body = get_text_with_retry(http, retry_policy, &url).await?;
    let received_at = Utc::now();
    let response: wire::BinanceServerTime =
        serde_json::from_str(&body).map_err(|error| ApiError::Deserialize {
            context: "binance server time".to_owned(),
            detail: error.to_string(),
        })?;
    let trusted = Utc
        .timestamp_millis_opt(response.server_time_ms)
        .single()
        .ok_or_else(|| ApiError::Deserialize {
            context: "binance server time".to_owned(),
            detail: format!("invalid serverTime: {}", response.server_time_ms),
        })?;
    validate_clock_sample(sent_at, received_at, trusted, config.max_clock_skew_ms)
}

fn validate_clock_sample(
    sent_at: DateTime<Utc>,
    received_at: DateTime<Utc>,
    trusted: DateTime<Utc>,
    max_clock_skew_ms: u64,
) -> QuantResult<()> {
    let round_trip_ms = (received_at - sent_at).num_milliseconds().unsigned_abs();
    let local_midpoint = sent_at + (received_at - sent_at) / 2;
    let skew_ms = (local_midpoint - trusted).num_milliseconds().unsigned_abs();
    if round_trip_ms > max_clock_skew_ms || skew_ms > max_clock_skew_ms {
        return Err(ApiError::ClockSkew {
            provider: "binance".to_owned(),
            skew_ms,
            max_skew_ms: max_clock_skew_ms,
            round_trip_ms,
        }
        .into());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::io::{Cursor, Write};

    use super::{
        BinanceKlineSource, BinanceRequestBudget, DomainDataSource, DomainFetchRequest,
        KLINE_PAGE_SIZE, decode_kline_archive, validate_clock_sample,
    };
    use chrono::{TimeZone, Utc};
    use quant_pivot_models::{
        config::BinanceSourceConfig,
        enums::domain::{BinanceMarketSegment, DomainMetric, KlineInterval},
        types::{BinanceSymbol, DomainInstrumentKey, DomainSourceId},
    };
    use sha2::{Digest, Sha256};
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

    async fn mock_server_time(server: &MockServer) {
        Mock::given(method("GET"))
            .and(path("/api/v3/time"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "serverTime": Utc::now().timestamp_millis()
            })))
            .mount(server)
            .await;
    }

    fn kline_archive_bytes(csv: &str) -> Vec<u8> {
        let mut writer = zip::ZipWriter::new(Cursor::new(Vec::new()));
        writer
            .start_file(
                "BTCUSDT-1m-2025-01-01.csv",
                zip::write::SimpleFileOptions::default(),
            )
            .expect("start archive member");
        writer.write_all(csv.as_bytes()).expect("write archive CSV");
        writer.finish().expect("finish archive").into_inner()
    }

    #[test]
    fn archive_normalizes_microseconds_and_rejects_kline_gaps() {
        let first = "1735689600000000,4.1507,4.1587,4.1506,4.1554,539.23,\
                     1735689659999999,2240.39,13,401.82,1669.98,0";
        let second = "1735689660000000,4.1554,4.1600,4.1500,4.1580,100.0,\
                      1735689719999999,415.8,4,50.0,207.9,0";
        let rows = decode_kline_archive(
            kline_archive_bytes(&format!("{first}\n{second}\n")),
            "BTCUSDT-1m-2025-01-01.csv",
            KlineInterval::OneMinute,
        )
        .expect("valid archive");
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].open_time_ms, 1_735_689_600_000);
        assert_eq!(rows[0].close_time_ms, 1_735_689_659_999);

        let gap = "1735689720000000,4.1554,4.1600,4.1500,4.1580,100.0,\
                   1735689779999999,415.8,4,50.0,207.9,0";
        assert!(
            decode_kline_archive(
                kline_archive_bytes(&format!("{first}\n{gap}\n")),
                "BTCUSDT-1m-2025-01-01.csv",
                KlineInterval::OneMinute,
            )
            .is_err()
        );
    }

    #[tokio::test]
    async fn archive_transport_requires_sidecar_and_streams_bounded_batches() {
        let server = MockServer::start().await;
        let first = "1735689600000000,4.1507,4.1587,4.1506,4.1554,539.23,\
                     1735689659999999,2240.39,13,401.82,1669.98,0";
        let second = "1735689660000000,4.1554,4.1600,4.1500,4.1580,100.0,\
                      1735689719999999,415.8,4,50.0,207.9,0";
        let archive = kline_archive_bytes(&format!("{first}\n{second}\n"));
        let filename = "BTCUSDT-1m-2025-01-01.zip";
        let archive_path = format!("/data/spot/daily/klines/BTCUSDT/1m/{filename}");
        let checksum = format!("{}  {filename}\n", hex::encode(Sha256::digest(&archive)));
        Mock::given(method("GET"))
            .and(path(archive_path.as_str()))
            .respond_with(ResponseTemplate::new(200).set_body_bytes(archive.clone()))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path(format!("{archive_path}.CHECKSUM")))
            .respond_with(ResponseTemplate::new(200).set_body_string(checksum))
            .mount(&server)
            .await;
        let source = BinanceKlineSource::connect(BinanceSourceConfig {
            archive_url: server.uri(),
            batch_size: 1,
            ..BinanceSourceConfig::default()
        })
        .expect("source");
        let symbol = BinanceSymbol::parse("BTCUSDT").expect("symbol");
        let available_at = Utc.with_ymd_and_hms(2025, 1, 2, 0, 0, 0).unwrap();
        let mut stream = source
            .recover_archive_day(
                &symbol,
                KlineInterval::OneMinute,
                chrono::NaiveDate::from_ymd_opt(2025, 1, 1).expect("date"),
                available_at,
            )
            .await
            .expect("verified transport")
            .expect("published archive");
        let first_batch = stream
            .next_batch()
            .await
            .expect("first batch")
            .expect("first batch exists");
        let second_batch = stream
            .next_batch()
            .await
            .expect("second batch")
            .expect("second batch exists");
        assert_eq!(first_batch.len(), 1);
        assert_eq!(second_batch.len(), 1);
        assert!(stream.next_batch().await.expect("end of stream").is_none());
        assert_eq!(first_batch[0].available_at, Some(available_at));

        let missing_checksum_server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path(archive_path.as_str()))
            .respond_with(ResponseTemplate::new(200).set_body_bytes(archive))
            .mount(&missing_checksum_server)
            .await;
        let missing_checksum_source = BinanceKlineSource::connect(BinanceSourceConfig {
            archive_url: missing_checksum_server.uri(),
            ..BinanceSourceConfig::default()
        })
        .expect("source");
        let error = missing_checksum_source
            .recover_archive_day(
                &symbol,
                KlineInterval::OneMinute,
                chrono::NaiveDate::from_ymd_opt(2025, 1, 1).expect("date"),
                available_at,
            )
            .await
            .err()
            .expect("archive without sidecar must fail closed");
        assert!(error.to_string().contains("required checksum"));
    }

    #[test]
    fn clock_sample_uses_request_midpoint_and_fails_closed_on_rtt_or_skew() {
        let sent = Utc.with_ymd_and_hms(2026, 7, 18, 0, 0, 0).unwrap();
        let received = sent + chrono::Duration::milliseconds(200);
        let midpoint = sent + chrono::Duration::milliseconds(100);
        validate_clock_sample(sent, received, midpoint, 250).expect("trusted midpoint");
        assert!(validate_clock_sample(sent, received, midpoint, 150).is_err());
        assert!(
            validate_clock_sample(
                sent,
                received,
                midpoint + chrono::Duration::milliseconds(300),
                250,
            )
            .is_err()
        );
    }

    #[tokio::test]
    async fn fetch_parses_klines_into_close_observations() {
        let server = MockServer::start().await;
        mock_server_time(&server).await;
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
                    1_494_904_059_999_i64,
                    "2434.19055334",
                    308,
                    "1756.87402397",
                    "28.46694368",
                    "0"
                ]])),
            )
            .mount(&server)
            .await;

        let source = BinanceKlineSource::connect(BinanceSourceConfig {
            rest_url: server.uri(),
            ..BinanceSourceConfig::default()
        })
        .expect("source");
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
    async fn usdm_futures_fetch_uses_fapi_and_preserves_provenance() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/fapi/v1/time"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "serverTime": Utc::now().timestamp_millis()
            })))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/fapi/v1/klines"))
            .and(query_param("symbol", "HYPEUSDT"))
            .and(query_param("interval", "1h"))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(serde_json::json!([[
                    1_753_056_000_000_i64,
                    "42.0",
                    "42.5",
                    "41.75",
                    "42.125",
                    "148976.11427815",
                    1_753_059_599_999_i64,
                    "2434.19055334",
                    308,
                    "1756.87402397",
                    "28.46694368",
                    "0"
                ]])),
            )
            .mount(&server)
            .await;

        let config = BinanceSourceConfig {
            rest_url: server.uri(),
            ..BinanceSourceConfig::default()
        };
        let source = BinanceKlineSource::connect_usdm_futures_with_budget(
            config.clone(),
            BinanceRequestBudget::new(&config).expect("budget"),
        )
        .expect("source");
        let key = DomainInstrumentKey::binance_usdm_futures_kline(
            &BinanceSymbol::parse("HYPEUSDT").expect("symbol"),
            KlineInterval::OneHour,
        );
        let rows = source
            .fetch(DomainFetchRequest {
                instrument_key: key.clone(),
                from_exclusive: Utc.with_ymd_and_hms(2025, 7, 20, 0, 0, 0).unwrap(),
                to_inclusive: Utc.with_ymd_and_hms(2025, 7, 22, 0, 0, 0).unwrap(),
                bootstrap: true,
            })
            .await
            .expect("fetch");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].source_id, DomainSourceId::binance_usdm_futures());
        assert_eq!(rows[0].instrument_key, key);
        assert_eq!(
            rows[0].instrument_key.as_binance_market_kline(),
            Some((
                BinanceMarketSegment::UsdmFutures,
                BinanceSymbol::parse("HYPEUSDT").expect("symbol"),
                KlineInterval::OneHour,
            ))
        );

        let spot_key = DomainInstrumentKey::binance_kline(
            &BinanceSymbol::parse("HYPEUSDT").expect("symbol"),
            KlineInterval::OneHour,
        );
        assert!(
            source
                .fetch(DomainFetchRequest {
                    instrument_key: spot_key,
                    from_exclusive: Utc.with_ymd_and_hms(2025, 7, 20, 0, 0, 0).unwrap(),
                    to_inclusive: Utc.with_ymd_and_hms(2025, 7, 22, 0, 0, 0).unwrap(),
                    bootstrap: true,
                })
                .await
                .is_err(),
            "USD-M Futures source must reject a Spot instrument"
        );
    }

    #[tokio::test]
    async fn fetch_paginates_until_short_page() {
        let server = MockServer::start().await;
        mock_server_time(&server).await;
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

        let source = BinanceKlineSource::connect(BinanceSourceConfig {
            rest_url: server.uri(),
            ..BinanceSourceConfig::default()
        })
        .expect("source");
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

    #[tokio::test]
    async fn fetch_excludes_current_open_kline_past_scan_boundary() {
        let server = MockServer::start().await;
        mock_server_time(&server).await;
        let boundary = Utc.with_ymd_and_hms(2026, 7, 17, 3, 9, 54).unwrap();
        let closed_open_time = boundary.timestamp_millis() - 114_000;
        let current_open_time = closed_open_time + 60_000;
        Mock::given(method("GET"))
            .and(path("/api/v3/klines"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!([
                full_kline_row(closed_open_time, "0.015"),
                full_kline_row(current_open_time, "0.016")
            ])))
            .mount(&server)
            .await;

        let source = BinanceKlineSource::connect(BinanceSourceConfig {
            rest_url: server.uri(),
            ..BinanceSourceConfig::default()
        })
        .expect("source");
        let rows = source
            .fetch(DomainFetchRequest {
                instrument_key: DomainInstrumentKey::binance_kline(
                    &BinanceSymbol::parse("BTCUSDT").expect("symbol"),
                    KlineInterval::OneMinute,
                ),
                from_exclusive: chrono::DateTime::from_timestamp_millis(closed_open_time - 1)
                    .expect("previous close"),
                to_inclusive: boundary,
                bootstrap: false,
            })
            .await
            .expect("fetch");

        assert_eq!(rows.len(), 1);
        assert!(rows[0].observed_at <= boundary);
        assert_eq!(rows[0].value.to_string(), "0.015");
    }
}
