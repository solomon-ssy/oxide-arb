//! Public Polymarket Real-Time Data Socket Crypto price adapter.
//!
//! Binance and Chainlink are distinct provenance planes even though they share
//! one WebSocket endpoint and payload shape. A payload can only map to the
//! source/instrument explicitly subscribed by the caller.

use std::{
    collections::{BTreeMap, BTreeSet},
    str,
    time::Duration,
};

use chrono::{DateTime, TimeZone, Utc};
use futures_util::{SinkExt, StreamExt};
use quant_pivot_error::{QuantError, QuantResult, api::ApiError, ws::WsError};
use quant_pivot_models::{
    config::PolymarketRtdsSourceConfig,
    domain::data_plane::CryptoPriceReport,
    hashing::CanonicalDigest,
    types::{DomainInstrumentKey, DomainSourceId, Usd},
};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use tokio::{
    net::TcpStream,
    time::{Interval, MissedTickBehavior},
};
use tokio_tungstenite::{
    MaybeTlsStream, WebSocketStream, connect_async,
    tungstenite::{Message, protocol::CloseFrame},
};

const SUPPORTED_BINANCE_SYMBOLS: &[&str] = &["BTCUSDT", "ETHUSDT", "SOLUSDT", "XRPUSDT"];
const SUPPORTED_CHAINLINK_FEEDS: &[&str] = &["BTC-USD", "ETH-USD", "SOL-USD", "XRP-USD"];

/// RTDS Crypto topic. The source is part of every persisted fact's identity.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RtdsCryptoSource {
    Binance,
    Chainlink,
}

impl RtdsCryptoSource {
    const fn topic(self) -> &'static str {
        match self {
            Self::Binance => "crypto_prices",
            Self::Chainlink => "crypto_prices_chainlink",
        }
    }

    const fn subscription_type(self) -> &'static str {
        match self {
            Self::Binance => "update",
            Self::Chainlink => "*",
        }
    }

    pub fn source_id(self) -> DomainSourceId {
        match self {
            Self::Binance => DomainSourceId::polymarket_rtds_binance(),
            Self::Chainlink => DomainSourceId::polymarket_rtds_chainlink(),
        }
    }

    fn binding(self, instrument: &DomainInstrumentKey) -> QuantResult<String> {
        match self {
            Self::Binance => {
                let symbol = instrument
                    .as_polymarket_rtds_binance_symbol()
                    .filter(|symbol| SUPPORTED_BINANCE_SYMBOLS.contains(&symbol.as_str()))
                    .ok_or_else(|| {
                        QuantError::config(format!(
                            "unsupported RTDS Binance instrument `{instrument}`"
                        ))
                    })?;
                Ok(symbol.as_str().to_ascii_lowercase())
            }
            Self::Chainlink => {
                let feed = instrument
                    .as_polymarket_rtds_chainlink_feed()
                    .filter(|feed| SUPPORTED_CHAINLINK_FEEDS.contains(&feed.as_str()))
                    .ok_or_else(|| {
                        QuantError::config(format!(
                            "unsupported RTDS Chainlink instrument `{instrument}`"
                        ))
                    })?;
                Ok(feed.as_str().replace('-', "/").to_ascii_lowercase())
            }
        }
    }
}

/// Public RTDS client. No wallet or source credentials are accepted.
pub struct PolymarketRtdsSource {
    config: PolymarketRtdsSourceConfig,
}

impl PolymarketRtdsSource {
    #[must_use]
    pub const fn connect(config: PolymarketRtdsSourceConfig) -> Self {
        Self { config }
    }

    /// Open one topic subscription for an exact non-empty instrument set.
    pub async fn stream(
        &self,
        source: RtdsCryptoSource,
        instruments: &[DomainInstrumentKey],
    ) -> QuantResult<PolymarketRtdsStream> {
        let bindings = compile_bindings(source, instruments)?;
        let connect = connect_async(&self.config.websocket_url);
        let (mut inner, _) = tokio::time::timeout(
            Duration::from_millis(self.config.connect_timeout_ms),
            connect,
        )
        .await
        .map_err(|_| ApiError::Timeout {
            operation: "Polymarket RTDS WebSocket connect".to_owned(),
            elapsed_ms: self.config.connect_timeout_ms,
        })?
        .map_err(|error| WsError::ConnectionFailed {
            shard_id: 0,
            reason: format!("Polymarket RTDS connect: {error}"),
        })?;
        let subscription = subscription_message(source, bindings.keys())?;
        inner
            .send(Message::Text(subscription.into()))
            .await
            .map_err(|error| WsError::SubscriptionFailed {
                shard_id: 0,
                token_count: bindings.len(),
                reason: format!("Polymarket RTDS subscribe: {error}"),
            })?;
        let mut keepalive = tokio::time::interval(Duration::from_secs(self.config.keepalive_secs));
        keepalive.set_missed_tick_behavior(MissedTickBehavior::Skip);
        keepalive.tick().await;
        Ok(PolymarketRtdsStream {
            source,
            bindings,
            inner,
            keepalive,
            max_clock_skew_ms: self.config.max_clock_skew_ms,
        })
    }
}

/// One subscribed RTDS topic stream.
pub struct PolymarketRtdsStream {
    source: RtdsCryptoSource,
    bindings: BTreeMap<String, DomainInstrumentKey>,
    inner: WebSocketStream<MaybeTlsStream<TcpStream>>,
    keepalive: Interval,
    max_clock_skew_ms: u64,
}

impl PolymarketRtdsStream {
    /// Read the next validated immutable price report while maintaining the
    /// official text heartbeat.
    pub async fn next_report(&mut self) -> QuantResult<CryptoPriceReport> {
        loop {
            let message = tokio::select! {
                message = self.inner.next() => Some(message),
                _ = self.keepalive.tick() => None,
            };
            let Some(message) = message else {
                self.inner
                    .send(Message::Text("PING".into()))
                    .await
                    .map_err(|error| WsError::ConnectionFailed {
                        shard_id: 0,
                        reason: format!("Polymarket RTDS keepalive: {error}"),
                    })?;
                continue;
            };
            let message = message
                .ok_or(WsError::ConnectionClosed {
                    shard_id: 0,
                    code: None,
                })?
                .map_err(|error| WsError::ConnectionFailed {
                    shard_id: 0,
                    reason: format!("Polymarket RTDS read: {error}"),
                })?;
            match message {
                Message::Text(text)
                    if text.as_str().trim().is_empty()
                        || text.as_str().trim().eq_ignore_ascii_case("PONG") =>
                {
                    // RTDS emits an empty subscription acknowledgement before
                    // the first data update; it carries no source fact.
                }
                Message::Text(text) => {
                    if should_skip_message(self.source, text.as_str())? {
                        continue;
                    }
                    return parse_price_report(
                        self.source,
                        text.as_str(),
                        &self.bindings,
                        Utc::now(),
                        self.max_clock_skew_ms,
                    );
                }
                Message::Binary(bytes) => {
                    if bytes.is_empty() {
                        continue;
                    }
                    let raw = str::from_utf8(&bytes).map_err(|error| ApiError::Deserialize {
                        context: "Polymarket RTDS binary payload".to_owned(),
                        detail: error.to_string(),
                    })?;
                    if should_skip_message(self.source, raw)? {
                        continue;
                    }
                    return parse_price_report(
                        self.source,
                        raw,
                        &self.bindings,
                        Utc::now(),
                        self.max_clock_skew_ms,
                    );
                }
                Message::Ping(payload) => {
                    self.inner
                        .send(Message::Pong(payload))
                        .await
                        .map_err(|error| WsError::ConnectionFailed {
                            shard_id: 0,
                            reason: format!("Polymarket RTDS pong: {error}"),
                        })?;
                }
                Message::Close(frame) => return Err(closed(frame).into()),
                Message::Pong(_) | Message::Frame(_) => {}
            }
        }
    }

    pub async fn close(&mut self) -> QuantResult<()> {
        self.inner.close(None).await.map_err(|error| {
            QuantError::WebSocket(WsError::ConnectionFailed {
                shard_id: 0,
                reason: format!("Polymarket RTDS close: {error}"),
            })
        })
    }
}

#[derive(Serialize)]
struct SubscriptionRequest {
    action: &'static str,
    subscriptions: Vec<Subscription>,
}

#[derive(Serialize)]
struct Subscription {
    topic: &'static str,
    #[serde(rename = "type")]
    message_type: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    filters: Option<String>,
}

fn subscription_message<'a>(
    source: RtdsCryptoSource,
    symbols: impl Iterator<Item = &'a String>,
) -> QuantResult<String> {
    let symbols = symbols.cloned().collect::<Vec<_>>();
    let [symbol] = symbols.as_slice() else {
        return Err(QuantError::config(
            "Polymarket RTDS requires one exact instrument per connection",
        ));
    };
    let filters =
        serde_json::to_string(&serde_json::json!({ "symbol": symbol })).map_err(|error| {
            ApiError::Deserialize {
                context: "Polymarket RTDS exact filter".to_owned(),
                detail: error.to_string(),
            }
        })?;
    serialize_subscriptions(vec![Subscription {
        topic: source.topic(),
        message_type: source.subscription_type(),
        filters: Some(filters),
    }])
}

fn serialize_subscriptions(subscriptions: Vec<Subscription>) -> QuantResult<String> {
    serde_json::to_string(&SubscriptionRequest {
        action: "subscribe",
        subscriptions,
    })
    .map_err(|error| {
        ApiError::Deserialize {
            context: "Polymarket RTDS subscription".to_owned(),
            detail: error.to_string(),
        }
        .into()
    })
}

fn compile_bindings(
    source: RtdsCryptoSource,
    instruments: &[DomainInstrumentKey],
) -> QuantResult<BTreeMap<String, DomainInstrumentKey>> {
    if instruments.is_empty() {
        return Err(QuantError::config(
            "Polymarket RTDS subscription requires at least one instrument",
        ));
    }
    let mut bindings = BTreeMap::new();
    let mut instruments_seen = BTreeSet::new();
    for instrument in instruments {
        if !instruments_seen.insert(instrument.clone()) {
            return Err(QuantError::config(format!(
                "duplicate Polymarket RTDS instrument `{instrument}`"
            )));
        }
        let symbol = source.binding(instrument)?;
        if bindings
            .insert(symbol.clone(), instrument.clone())
            .is_some()
        {
            return Err(QuantError::config(format!(
                "duplicate Polymarket RTDS wire symbol `{symbol}`"
            )));
        }
    }
    Ok(bindings)
}

#[derive(Deserialize)]
struct PriceEnvelope {
    topic: String,
    #[serde(rename = "type")]
    message_type: String,
    timestamp: i64,
    payload: PricePayload,
}

#[derive(Deserialize)]
struct EnvelopeHeader {
    topic: String,
    #[serde(rename = "type")]
    message_type: String,
}

#[derive(Deserialize)]
struct PricePayload {
    symbol: String,
    timestamp: i64,
    value: Decimal,
}

fn should_skip_message(source: RtdsCryptoSource, raw: &str) -> QuantResult<bool> {
    let header: EnvelopeHeader =
        serde_json::from_str(raw).map_err(|error| ApiError::Deserialize {
            context: "Polymarket RTDS message header".to_owned(),
            detail: error.to_string(),
        })?;
    if header.topic != source.topic() {
        return match header.topic.as_str() {
            // The public service can emit the other Crypto topic before the
            // requested subscription becomes authoritative. It must never be
            // re-labelled as this stream's source, so discard it explicitly.
            "crypto_prices" | "crypto_prices_chainlink" => Ok(true),
            _ => Err(invalid_payload(format!(
                "topic `{}` does not match subscription `{}`",
                header.topic,
                source.topic()
            ))
            .into()),
        };
    }
    match header.message_type.as_str() {
        "subscribe" => Ok(true),
        "update" => Ok(false),
        message_type => {
            Err(invalid_payload(format!("unsupported message type `{message_type}`")).into())
        }
    }
}

fn parse_price_report(
    source: RtdsCryptoSource,
    raw: &str,
    bindings: &BTreeMap<String, DomainInstrumentKey>,
    available_at: DateTime<Utc>,
    max_clock_skew_ms: u64,
) -> QuantResult<CryptoPriceReport> {
    let envelope: PriceEnvelope =
        serde_json::from_str(raw).map_err(|error| ApiError::Deserialize {
            context: "Polymarket RTDS price update".to_owned(),
            detail: error.to_string(),
        })?;
    if envelope.topic != source.topic() || envelope.message_type != "update" {
        return Err(invalid_payload("topic/type does not match subscription").into());
    }
    let symbol = envelope.payload.symbol.to_ascii_lowercase();
    let instrument_key = bindings
        .get(&symbol)
        .cloned()
        .ok_or_else(|| invalid_payload(format!("unsubscribed symbol `{symbol}`")))?;
    if envelope.payload.value <= Decimal::ZERO {
        return Err(invalid_payload("price must be positive").into());
    }
    let event_time = timestamp(envelope.payload.timestamp, "payload timestamp")?;
    let published_at = timestamp(envelope.timestamp, "envelope timestamp")?;
    validate_clock(event_time, available_at, max_clock_skew_ms)?;
    validate_clock(published_at, available_at, max_clock_skew_ms)?;
    let source_sequence = u64::try_from(envelope.payload.timestamp)
        .map_err(|error| invalid_payload(format!("negative source timestamp: {error}")))?;
    let report_hash = CanonicalDigest::content_hash_bytes(raw.as_bytes());
    Ok(CryptoPriceReport {
        source_id: source.source_id(),
        instrument_key,
        source_sequence,
        price: Usd::new(envelope.payload.value),
        quantity: None,
        event_time,
        published_at,
        available_at,
        valid_from: None,
        observations_timestamp: (source == RtdsCryptoSource::Chainlink).then_some(event_time),
        expires_at: None,
        report_hash,
        raw_report: raw.to_owned(),
    })
}

fn timestamp(value: i64, field: &str) -> QuantResult<DateTime<Utc>> {
    Utc.timestamp_millis_opt(value)
        .single()
        .ok_or_else(|| invalid_payload(format!("invalid {field}: {value}")).into())
}

fn validate_clock(
    source_time: DateTime<Utc>,
    available_at: DateTime<Utc>,
    max_clock_skew_ms: u64,
) -> QuantResult<()> {
    let skew_ms = (available_at - source_time)
        .num_milliseconds()
        .unsigned_abs();
    if skew_ms > max_clock_skew_ms {
        return Err(invalid_payload(format!(
            "source clock skew {skew_ms}ms exceeds {max_clock_skew_ms}ms"
        ))
        .into());
    }
    Ok(())
}

fn invalid_payload(detail: impl Into<String>) -> ApiError {
    ApiError::Deserialize {
        context: "Polymarket RTDS price update".to_owned(),
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
    use chrono::{TimeZone, Utc};
    use quant_pivot_models::types::{
        BinanceSymbol, ChainlinkFeedKey, DomainInstrumentKey, DomainSourceId,
    };
    use rust_decimal_macros::dec;
    use serde_json::Value;

    use super::{
        RtdsCryptoSource, compile_bindings, parse_price_report, should_skip_message,
        subscription_message,
    };

    #[test]
    fn binance_and_chainlink_payloads_keep_distinct_provenance() {
        let available_at = Utc.with_ymd_and_hms(2025, 7, 23, 20, 21, 4).unwrap();
        let binance = DomainInstrumentKey::polymarket_rtds_binance(
            &BinanceSymbol::parse("BTCUSDT").expect("symbol"),
        );
        let binance_bindings =
            compile_bindings(RtdsCryptoSource::Binance, &[binance]).expect("Binance bindings");
        let binance_report = parse_price_report(
            RtdsCryptoSource::Binance,
            r#"{"topic":"crypto_prices","type":"update","timestamp":1753302064000,"payload":{"symbol":"btcusdt","timestamp":1753302063995,"value":67234.50}}"#,
            &binance_bindings,
            available_at,
            30_000,
        )
        .expect("Binance report");
        assert_eq!(
            binance_report.source_id,
            DomainSourceId::polymarket_rtds_binance()
        );
        assert_eq!(binance_report.price.inner(), dec!(67234.50));
        assert!(binance_report.observations_timestamp.is_none());

        let chainlink = DomainInstrumentKey::polymarket_rtds_chainlink(
            &ChainlinkFeedKey::parse("BTC-USD").expect("feed"),
        );
        let chainlink_bindings = compile_bindings(RtdsCryptoSource::Chainlink, &[chainlink])
            .expect("Chainlink bindings");
        let chainlink_report = parse_price_report(
            RtdsCryptoSource::Chainlink,
            r#"{"topic":"crypto_prices_chainlink","type":"update","timestamp":1753302064000,"payload":{"symbol":"btc/usd","timestamp":1753302063995,"value":67234.50}}"#,
            &chainlink_bindings,
            available_at,
            30_000,
        )
        .expect("Chainlink report");
        assert_eq!(
            chainlink_report.source_id,
            DomainSourceId::polymarket_rtds_chainlink()
        );
        assert_eq!(
            chainlink_report.observations_timestamp,
            Some(chainlink_report.event_time)
        );
        assert_ne!(binance_report.report_hash, chainlink_report.report_hash);
    }

    #[test]
    fn subscription_filters_match_the_official_source_shapes() {
        let binance_symbols = ["btcusdt".to_owned()];
        let binance = subscription_message(RtdsCryptoSource::Binance, binance_symbols.iter())
            .expect("Binance subscription");
        let binance: Value = serde_json::from_str(&binance).expect("JSON");
        assert_eq!(binance["subscriptions"].as_array().map(Vec::len), Some(1));
        assert_eq!(
            binance["subscriptions"][0]["filters"],
            r#"{"symbol":"btcusdt"}"#
        );
        assert_eq!(binance["subscriptions"][0]["type"], "update");

        let chainlink_symbols = ["btc/usd".to_owned()];
        let chainlink = subscription_message(RtdsCryptoSource::Chainlink, chainlink_symbols.iter())
            .expect("Chainlink subscription");
        let first_chainlink: Value = serde_json::from_str(&chainlink).expect("JSON");
        assert_eq!(
            first_chainlink["subscriptions"].as_array().map(Vec::len),
            Some(1)
        );
        assert_eq!(first_chainlink["subscriptions"][0]["type"], "*");
        assert_eq!(
            first_chainlink["subscriptions"][0]["filters"],
            r#"{"symbol":"btc/usd"}"#
        );
        let multiple_symbols = ["btcusdt".to_owned(), "ethusdt".to_owned()];
        assert!(subscription_message(RtdsCryptoSource::Binance, multiple_symbols.iter()).is_err());
    }

    #[test]
    fn wrong_topic_symbol_price_or_clock_fails_closed() {
        let instrument = DomainInstrumentKey::polymarket_rtds_binance(
            &BinanceSymbol::parse("BTCUSDT").expect("symbol"),
        );
        let bindings =
            compile_bindings(RtdsCryptoSource::Binance, &[instrument]).expect("bindings");
        let available_at = Utc.with_ymd_and_hms(2025, 7, 23, 20, 21, 4).unwrap();
        for raw in [
            r#"{"topic":"crypto_prices_chainlink","type":"update","timestamp":1753302064000,"payload":{"symbol":"btcusdt","timestamp":1753302063995,"value":1}}"#,
            r#"{"topic":"crypto_prices","type":"update","timestamp":1753302064000,"payload":{"symbol":"ethusdt","timestamp":1753302063995,"value":1}}"#,
            r#"{"topic":"crypto_prices","type":"update","timestamp":1753302064000,"payload":{"symbol":"btcusdt","timestamp":1753302063995,"value":0}}"#,
            r#"{"topic":"crypto_prices","type":"update","timestamp":1753302000000,"payload":{"symbol":"btcusdt","timestamp":1753302000000,"value":1}}"#,
        ] {
            assert!(
                parse_price_report(
                    RtdsCryptoSource::Binance,
                    raw,
                    &bindings,
                    available_at,
                    30_000,
                )
                .is_err()
            );
        }
    }

    #[test]
    fn undocumented_asset_cannot_enter_public_rtds() {
        let unsupported = DomainInstrumentKey::polymarket_rtds_binance(
            &BinanceSymbol::parse("DOGEUSDT").expect("symbol"),
        );
        assert!(compile_bindings(RtdsCryptoSource::Binance, &[unsupported]).is_err());
        assert!(
            compile_bindings(RtdsCryptoSource::Chainlink, &[]).is_err(),
            "empty all-symbol subscriptions are forbidden"
        );
    }

    #[test]
    fn filtered_subscription_snapshot_is_control_data_not_a_live_fact() {
        let snapshot = r#"{"topic":"crypto_prices","type":"subscribe","payload":{"symbol":"btcusdt","data":[{"timestamp":1753302063995,"value":67234.50}]}}"#;
        assert!(should_skip_message(RtdsCryptoSource::Binance, snapshot).expect("snapshot header"));
        assert!(
            !should_skip_message(
                RtdsCryptoSource::Binance,
                r#"{"topic":"crypto_prices","type":"update"}"#
            )
            .expect("update header")
        );
    }
}
