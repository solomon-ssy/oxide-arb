//! Chainlink Data Streams V3 source.
//!
//! This module deliberately has no `DomainDataSource` implementation: signed
//! Data Streams reports are immutable crypto facts, not generic factor
//! observations. Callers persist [`CryptoPriceReport`] first and derive price
//! transitions from adjacent reports of the same source/feed.

use std::{collections::BTreeMap, fmt::Display, sync::Arc, time::Duration};

use chainlink_data_streams_report::{
    feed_id::ID,
    report::{Report, decode_full_report, v3::ReportDataV3},
};
use chainlink_data_streams_sdk::{
    client::Client,
    config::{Config, WebSocketHighAvailability},
    stream::Stream,
};
use chrono::{DateTime, TimeZone, Utc};
use quant_pivot_error::{QuantError, QuantResult, rpc::RpcError};
use quant_pivot_models::{
    config::{ChainlinkDataStreamFeedConfig, ChainlinkDataStreamsSourceConfig},
    domain::data_plane::CryptoPriceReport,
    hashing::CanonicalDigest,
    types::{ChainlinkFeedKey, DomainInstrumentKey, DomainSourceId, Usd},
};
use reqwest::{Client as ReqwestClient, header::DATE};
use rust_decimal::Decimal;

#[derive(Clone)]
struct FeedBinding {
    id: ID,
    decimals: u32,
}

/// Authenticated REST/HA-WebSocket client for the configured V3 feeds.
pub struct ChainlinkDataStreamsSource {
    config: ChainlinkDataStreamsSourceConfig,
    sdk_config: Config,
    client: Arc<Client>,
    clock_http: ReqwestClient,
    feeds: BTreeMap<ChainlinkFeedKey, FeedBinding>,
}

impl ChainlinkDataStreamsSource {
    /// Construct a fail-closed source. Disabled or credential-incomplete
    /// configurations are rejected so callers can keep unrelated workers live.
    pub fn connect(config: ChainlinkDataStreamsSourceConfig) -> Result<Self, RpcError> {
        if !config.enabled {
            return Err(RpcError::ConnectionFailed(
                "Chainlink Data Streams is not configured".into(),
            ));
        }
        let api_key = config.api_key.as_ref().ok_or_else(|| {
            RpcError::ConnectionFailed("Chainlink Data Streams API key is missing".into())
        })?;
        let api_secret = config.api_secret.as_ref().ok_or_else(|| {
            RpcError::ConnectionFailed("Chainlink Data Streams API secret is missing".into())
        })?;
        let sdk_config = Config::new(
            api_key.expose_secret().to_owned(),
            api_secret.expose_secret().to_owned(),
            config.rest_url.clone(),
            config.websocket_url.clone(),
        )
        .with_ws_ha(WebSocketHighAvailability::Enabled)
        .with_ws_max_reconnect(usize::MAX)
        .build()
        .map_err(|error| data_streams_failure("configure", error))?;
        let feeds = config
            .feeds
            .iter()
            .map(|(name, feed)| parse_feed_binding(name, feed))
            .collect::<Result<BTreeMap<_, _>, _>>()?;
        let client = Client::new(sdk_config.clone())
            .map_err(|error| data_streams_failure("REST client", error))?;
        let clock_http = ReqwestClient::builder()
            .timeout(Duration::from_secs(5))
            .build()
            .map_err(|error| data_streams_failure("clock client", error))?;
        Ok(Self {
            config,
            sdk_config,
            client: Arc::new(client),
            clock_http,
            feeds,
        })
    }

    #[must_use]
    pub fn source_id(&self) -> DomainSourceId {
        DomainSourceId::chainlink_data_streams()
    }

    #[must_use]
    pub fn instruments(&self) -> Vec<DomainInstrumentKey> {
        self.feeds
            .keys()
            .map(DomainInstrumentKey::chainlink_data_streams)
            .collect()
    }

    /// Create an HA stream subscribed to every configured feed. The SDK
    /// deduplicates reports received from multiple origins.
    pub async fn stream(&self, feeds: &[ChainlinkFeedKey]) -> QuantResult<Stream> {
        if feeds.is_empty() {
            return Err(data_streams_failure("WebSocket connect", "no feeds requested").into());
        }
        let ids = feeds
            .iter()
            .map(|feed| self.binding(feed).map(|binding| binding.id))
            .collect::<QuantResult<Vec<_>>>()?;
        Stream::new(&self.sdk_config, ids)
            .await
            .map_err(|error| data_streams_failure("WebSocket connect", error).into())
    }

    /// Read and decode one HA-deduplicated signed report from the official SDK
    /// stream, resolving its feed only through the frozen deploy binding.
    pub async fn next_report(
        &self,
        stream: &mut Stream,
        available_at: DateTime<Utc>,
    ) -> QuantResult<CryptoPriceReport> {
        let response = stream
            .read()
            .await
            .map_err(|error| data_streams_failure("WebSocket read", error))?;
        let (feed, binding) = self
            .feeds
            .iter()
            .find(|(_, binding)| binding.id == response.report.feed_id)
            .ok_or_else(|| {
                data_streams_failure(
                    "WebSocket decode",
                    "report feed ID has no frozen configuration binding",
                )
            })?;
        self.decode(feed, binding, response.report, available_at)
    }

    /// Recover sequential reports after a native observations timestamp.
    pub async fn reports_page(
        &self,
        feed: &ChainlinkFeedKey,
        start_timestamp: u128,
        available_at: DateTime<Utc>,
    ) -> QuantResult<Vec<CryptoPriceReport>> {
        let binding = self.binding(feed)?;
        let reports = self
            .client
            .get_reports_page_with_limit(binding.id, start_timestamp, self.config.rest_page_limit)
            .await
            .map_err(|error| data_streams_failure("reports page", error))?;
        let mut reports = reports
            .into_iter()
            .map(|report| self.decode(feed, binding, report, available_at))
            .collect::<QuantResult<Vec<_>>>()?;
        reports.sort_by(|left, right| {
            (left.source_sequence, &left.report_hash)
                .cmp(&(right.source_sequence, &right.report_hash))
        });
        Ok(reports)
    }

    #[must_use]
    pub const fn rest_page_limit(&self) -> usize {
        self.config.rest_page_limit
    }

    /// Freeze the exact report applicable at a market's reference timestamp.
    pub async fn report_at(
        &self,
        feed: &ChainlinkFeedKey,
        timestamp: u128,
        available_at: DateTime<Utc>,
    ) -> QuantResult<CryptoPriceReport> {
        let binding = self.binding(feed)?;
        let response = self
            .client
            .get_report(binding.id, timestamp)
            .await
            .map_err(|error| data_streams_failure("reference report", error))?;
        self.decode(feed, binding, response.report, available_at)
    }

    /// Validate a trusted server-time sample. Callers must mark the source
    /// unhealthy when this fails; report age is not a clock-skew substitute.
    pub fn validate_clock(
        &self,
        local_time: DateTime<Utc>,
        trusted_server_time: DateTime<Utc>,
    ) -> QuantResult<()> {
        let skew = (local_time - trusted_server_time)
            .num_milliseconds()
            .unsigned_abs();
        if skew > self.config.max_clock_skew_ms {
            return Err(QuantError::Rpc(RpcError::CallFailed {
                method: "chainlink_data_streams_clock".into(),
                reason: format!(
                    "clock skew {skew}ms exceeds {}ms",
                    self.config.max_clock_skew_ms
                ),
            }));
        }
        Ok(())
    }

    /// Sample the authenticated REST origin's HTTPS `Date` header and compare
    /// it with the midpoint of the local request interval. Missing/invalid
    /// server time and excessive RTT both fail closed.
    pub async fn validate_system_clock(&self) -> QuantResult<()> {
        let sent_at = Utc::now();
        let response = self
            .clock_http
            .head(&self.config.rest_url)
            .send()
            .await
            .map_err(|error| data_streams_failure("clock sample", error))?;
        let received_at = Utc::now();
        let round_trip_ms = (received_at - sent_at).num_milliseconds().unsigned_abs();
        if round_trip_ms > self.config.max_clock_skew_ms {
            return Err(data_streams_failure(
                "clock sample",
                format!("round-trip {round_trip_ms}ms exceeds trusted clock budget"),
            )
            .into());
        }
        let header = response
            .headers()
            .get(DATE)
            .ok_or_else(|| data_streams_failure("clock sample", "server Date header is missing"))?
            .to_str()
            .map_err(|error| data_streams_failure("clock sample", error))?;
        let trusted = DateTime::parse_from_rfc2822(header)
            .map_err(|error| data_streams_failure("clock sample", error))?
            .with_timezone(&Utc);
        let local_midpoint = sent_at + (received_at - sent_at) / 2;
        self.validate_clock(local_midpoint, trusted)
    }

    fn binding(&self, feed: &ChainlinkFeedKey) -> QuantResult<&FeedBinding> {
        self.feeds.get(feed).ok_or_else(|| {
            QuantError::Rpc(RpcError::CallFailed {
                method: "chainlink_data_streams_feed".into(),
                reason: format!("feed `{feed}` is not configured"),
            })
        })
    }

    fn decode(
        &self,
        feed: &ChainlinkFeedKey,
        binding: &FeedBinding,
        report: Report,
        available_at: DateTime<Utc>,
    ) -> QuantResult<CryptoPriceReport> {
        if report.feed_id != binding.id {
            return Err(data_streams_failure(
                "decode V3",
                "response feed ID does not match frozen binding",
            )
            .into());
        }
        let raw_hex = report
            .full_report
            .strip_prefix("0x")
            .unwrap_or(&report.full_report);
        let full_report = hex::decode(raw_hex)
            .map_err(|error| data_streams_failure("decode fullReport hex", error))?;
        let (_, report_blob) = decode_full_report(&full_report)
            .map_err(|error| data_streams_failure("decode fullReport envelope", error))?;
        let decoded = ReportDataV3::decode(&report_blob)
            .map_err(|error| data_streams_failure("decode V3 report", error))?;
        if decoded.feed_id != binding.id {
            return Err(data_streams_failure(
                "decode V3",
                "report payload feed ID does not match frozen binding",
            )
            .into());
        }
        let price = decimal_price(&decoded.benchmark_price.to_string(), binding.decimals)?;
        let observations_timestamp = utc_seconds(decoded.observations_timestamp, "observations")?;
        let valid_from = utc_seconds(decoded.valid_from_timestamp, "valid from")?;
        let expires_at = utc_seconds(decoded.expires_at, "expires at")?;
        let report_hash = CanonicalDigest::content_hash_bytes(&full_report);
        Ok(CryptoPriceReport {
            source_id: self.source_id(),
            instrument_key: DomainInstrumentKey::chainlink_data_streams(feed),
            source_sequence: u64::from(decoded.observations_timestamp),
            price: Usd::new(price),
            quantity: None,
            event_time: observations_timestamp,
            published_at: observations_timestamp,
            available_at,
            valid_from: Some(valid_from),
            observations_timestamp: Some(observations_timestamp),
            expires_at: Some(expires_at),
            report_hash,
            raw_report: report.full_report,
        })
    }
}

fn parse_feed_binding(
    name: &str,
    config: &ChainlinkDataStreamFeedConfig,
) -> Result<(ChainlinkFeedKey, FeedBinding), RpcError> {
    let name =
        ChainlinkFeedKey::parse(name).map_err(|error| data_streams_failure("feed name", error))?;
    let id = ID::from_hex_str(&config.feed_id)
        .map_err(|error| data_streams_failure("feed ID", error))?;
    if id.0[..2] != [0, 3] {
        return Err(data_streams_failure(
            "feed ID",
            format!("`{name}` is not a V3 feed"),
        ));
    }
    if config.decimals > 28 {
        return Err(data_streams_failure(
            "feed decimals",
            format!("`{name}` exceeds Decimal scale"),
        ));
    }
    Ok((
        name,
        FeedBinding {
            id,
            decimals: config.decimals,
        },
    ))
}

fn decimal_price(raw: &str, decimals: u32) -> QuantResult<Decimal> {
    let raw = raw
        .parse::<Decimal>()
        .map_err(|error| data_streams_failure("benchmark price", error))?;
    let price = raw * Decimal::new(1, decimals);
    if price <= Decimal::ZERO {
        return Err(data_streams_failure("benchmark price", "price must be positive").into());
    }
    Ok(price)
}

fn utc_seconds(seconds: u32, field: &'static str) -> QuantResult<DateTime<Utc>> {
    Utc.timestamp_opt(i64::from(seconds), 0)
        .single()
        .ok_or_else(|| data_streams_failure(field, "timestamp out of range").into())
}

fn data_streams_failure(operation: &str, error: impl Display) -> RpcError {
    RpcError::CallFailed {
        method: format!("chainlink_data_streams_{operation}"),
        reason: error.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use rust_decimal_macros::dec;

    use super::decimal_price;

    #[test]
    fn benchmark_price_uses_scale() {
        assert_eq!(
            decimal_price("123456789", 8).expect("valid price"),
            dec!(1.23456789)
        );
    }

    #[test]
    fn non_positive_benchmark_rejected() {
        assert!(decimal_price("0", 8).is_err());
        assert!(decimal_price("-1", 8).is_err());
    }
}
