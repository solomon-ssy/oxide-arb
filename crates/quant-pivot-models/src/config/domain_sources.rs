//! Deploy-time connections for typed external-domain sources.
//!
//! Credentials are held in [`SecretString`] and are expected through
//! `QUANT_PIVOT__DOMAIN_SOURCES__CHAINLINK_DATA_STREAMS__*`. Runtime policy,
//! source readiness, and vertical activation gates do not belong here.

use std::collections::BTreeMap;

use rust_decimal::Decimal;
use secrecy::{ExposeSecret, SecretString};
use serde::{Deserialize, Serialize};

/// External domain data-source connections.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct DomainSourcesConfig {
    pub binance: BinanceSourceConfig,
    pub chainlink_data_streams: ChainlinkDataStreamsSourceConfig,
    pub aviation_weather: AviationWeatherSourceConfig,
    pub ghcnh: GhcnhSourceConfig,
    pub gefs: GefsSourceConfig,
    /// Frozen station metadata used to resolve and ingest supported airport
    /// daily-high markets. A station absent here is unresolved; city-name
    /// guessing is never allowed.
    pub weather_stations: BTreeMap<String, WeatherStationProfileConfig>,
}

impl Default for DomainSourcesConfig {
    fn default() -> Self {
        Self {
            binance: BinanceSourceConfig::default(),
            chainlink_data_streams: ChainlinkDataStreamsSourceConfig::default(),
            aviation_weather: AviationWeatherSourceConfig::default(),
            ghcnh: GhcnhSourceConfig::default(),
            gefs: GefsSourceConfig::default(),
            weather_stations: BTreeMap::from([(
                "KLGA".to_owned(),
                WeatherStationProfileConfig {
                    timezone: "America/New_York".to_owned(),
                    latitude: Decimal::new(407_769, 4),
                    longitude: Decimal::new(-738_740, 4),
                    elevation_meters: Decimal::new(64, 1),
                    ghcnh_station_id: "USW00014732".to_owned(),
                },
            )]),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WeatherStationProfileConfig {
    pub timezone: String,
    pub latitude: Decimal,
    pub longitude: Decimal,
    pub elevation_meters: Decimal,
    pub ghcnh_station_id: String,
}

/// Binance spot REST and aggregate-trade stream connection.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct BinanceSourceConfig {
    pub enabled: bool,
    pub rest_url: String,
    pub websocket_url: String,
    pub archive_url: String,
    pub weight_budget_per_min: u32,
    pub kline_poll_secs: u64,
    pub websocket_rotation_secs: u64,
    pub batch_size: usize,
    pub request_timeout_ms: u64,
}

impl Default for BinanceSourceConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            rest_url: "https://api.binance.com".into(),
            websocket_url: "wss://stream.binance.com:9443/ws".into(),
            archive_url: "https://data.binance.vision".into(),
            weight_budget_per_min: 1_000,
            kline_poll_secs: 30,
            websocket_rotation_secs: 82_800,
            batch_size: 5_000,
            request_timeout_ms: 10_000,
        }
    }
}

/// Chainlink Data Streams REST/WebSocket connection.
#[derive(Debug, Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ChainlinkDataStreamsSourceConfig {
    /// A missing subscription is valid for unrelated reports. Any bound
    /// condition still fails closed at preflight/evaluation.
    pub enabled: bool,
    pub rest_url: String,
    pub websocket_url: String,
    pub api_key: Option<SecretString>,
    pub api_secret: Option<SecretString>,
    /// Logical feed key (`BTC-USD`) to immutable V3 feed metadata.
    pub feeds: BTreeMap<String, ChainlinkDataStreamFeedConfig>,
    pub max_clock_skew_ms: u64,
    pub rest_page_limit: usize,
}

impl PartialEq for ChainlinkDataStreamsSourceConfig {
    fn eq(&self, other: &Self) -> bool {
        self.enabled == other.enabled
            && self.rest_url == other.rest_url
            && self.websocket_url == other.websocket_url
            && exposed(self.api_key.as_ref()) == exposed(other.api_key.as_ref())
            && exposed(self.api_secret.as_ref()) == exposed(other.api_secret.as_ref())
            && self.feeds == other.feeds
            && self.max_clock_skew_ms == other.max_clock_skew_ms
            && self.rest_page_limit == other.rest_page_limit
    }
}

impl Eq for ChainlinkDataStreamsSourceConfig {}

fn exposed(secret: Option<&SecretString>) -> Option<&str> {
    secret.map(ExposeSecret::expose_secret)
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ChainlinkDataStreamFeedConfig {
    pub feed_id: String,
    /// Decimal scale declared by the subscribed feed metadata.
    pub decimals: u32,
}

impl Default for ChainlinkDataStreamsSourceConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            rest_url: "https://api.dataengine.chain.link".into(),
            websocket_url: "wss://ws.dataengine.chain.link".into(),
            api_key: None,
            api_secret: None,
            feeds: BTreeMap::new(),
            max_clock_skew_ms: 2_000,
            rest_page_limit: 1_000,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct AviationWeatherSourceConfig {
    pub enabled: bool,
    pub base_url: String,
    pub poll_secs: u64,
    pub request_timeout_ms: u64,
    /// Delay after station-local midnight before emitting NOAA observation-day
    /// close. This is not Wunderground settlement finalization.
    pub day_close_grace_secs: u64,
}

impl Default for AviationWeatherSourceConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            base_url: "https://aviationweather.gov/api/data".into(),
            poll_secs: 60,
            request_timeout_ms: 10_000,
            day_close_grace_secs: 7_200,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct GhcnhSourceConfig {
    pub enabled: bool,
    pub base_url: String,
    pub request_timeout_ms: u64,
    pub refresh_secs: u64,
    pub calibration_years: u8,
}

impl Default for GhcnhSourceConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            base_url: "https://www.ncei.noaa.gov/oa/global-historical-climatology-network/hourly/access/by-year".into(),
            request_timeout_ms: 120_000,
            refresh_secs: 86_400,
            calibration_years: 2,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct GefsSourceConfig {
    pub enabled: bool,
    pub bucket_url: String,
    pub request_timeout_ms: u64,
    pub poll_secs: u64,
    pub publication_lag_secs: u64,
    pub max_lead_hours: u16,
    pub max_concurrency: usize,
}

impl Default for GefsSourceConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            bucket_url: "https://noaa-gefs-pds.s3.amazonaws.com".into(),
            request_timeout_ms: 30_000,
            poll_secs: 900,
            publication_lag_secs: 18_000,
            max_lead_hours: 240,
            max_concurrency: 8,
        }
    }
}
