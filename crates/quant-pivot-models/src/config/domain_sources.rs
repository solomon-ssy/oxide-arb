//! Deploy-time connections for typed external-domain sources.
//!
//! Provider secrets are zeroizing plaintext deploy values. Runtime policy,
//! source readiness, and vertical activation gates do not belong here.

use std::collections::BTreeMap;

use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use serde::{Deserialize, Serialize};

use self::WeatherHistoricalBindingKind::{ExactStation as Exact, OfficialNearbyProxy as Proxy};
use super::secret::SecretText;

/// Governed NOAA observation-day close grace used by live projection and
/// immutable Weather policy replay. It is methodology, not an ops tuning knob.
pub const WEATHER_OBSERVATION_DAY_CLOSE_GRACE_SECS: u64 = 7_200;

/// External domain data-source connections.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct DomainSourcesConfig {
    pub binance: BinanceSourceConfig,
    pub binance_usdm_futures: BinanceSourceConfig,
    pub polymarket_rtds: PolymarketRtdsSourceConfig,
    pub chainlink_data_streams: ChainlinkDataStreamsSourceConfig,
    pub aviation_weather: AviationWeatherSourceConfig,
    pub ghcnh: GhcnhSourceConfig,
    pub gefs: GefsSourceConfig,
    pub hko_open_data: HkoOpenDataSourceConfig,
    pub airnow: AirNowSourceConfig,
    pub tornado: TornadoSourceConfig,
    pub nhc: NhcSourceConfig,
    pub nasa_gistemp: NasaGistempSourceConfig,
    pub nsidc_sea_ice: NsidcSeaIceSourceConfig,
    pub nws_observation: NwsObservationSourceConfig,
    /// Frozen deploy bindings for non-temperature Weather sources. These are
    /// source-native identities, not market-title guesses; they let expected
    /// source reconciliation and bootstrap ingestion start before linkage
    /// rows exist.
    pub weather_vertical_bindings: WeatherVerticalBindingsConfig,
    /// Frozen station metadata used to resolve and ingest supported airport
    /// daily maximum/minimum markets. A station absent here is unresolved; city-name
    /// guessing is never allowed.
    pub weather_stations: BTreeMap<String, WeatherStationProfileConfig>,
}

impl Default for DomainSourcesConfig {
    fn default() -> Self {
        Self {
            binance: BinanceSourceConfig::default(),
            binance_usdm_futures: BinanceSourceConfig::usdm_futures_default(),
            polymarket_rtds: PolymarketRtdsSourceConfig::default(),
            chainlink_data_streams: ChainlinkDataStreamsSourceConfig::default(),
            aviation_weather: AviationWeatherSourceConfig::default(),
            ghcnh: GhcnhSourceConfig::default(),
            gefs: GefsSourceConfig::default(),
            hko_open_data: HkoOpenDataSourceConfig::default(),
            airnow: AirNowSourceConfig::default(),
            tornado: TornadoSourceConfig::default(),
            nhc: NhcSourceConfig::default(),
            nasa_gistemp: NasaGistempSourceConfig::default(),
            nsidc_sea_ice: NsidcSeaIceSourceConfig::default(),
            nws_observation: NwsObservationSourceConfig::default(),
            weather_vertical_bindings: WeatherVerticalBindingsConfig::default(),
            weather_stations: builtin_weather_station_profiles(),
        }
    }
}

/// Source-native Weather bindings known independently of Gamma linkage.
///
/// Every item is validated at deploy bootstrap and becomes an expected-source
/// ledger row before its first cursor exists.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct WeatherVerticalBindingsConfig {
    pub hko_rainfall: Vec<HkoRainfallBindingConfig>,
    pub hko_daily_temperature: Vec<HkoDailyTemperatureBindingConfig>,
    pub airnow_pm25_reporting_areas: Vec<AirNowPm25ReportingAreaBindingConfig>,
    pub airnow_pm25_sites: Vec<AirNowPm25SiteBindingConfig>,
    pub tornado_regions: Vec<TornadoRegionBindingConfig>,
    pub nhc_historical_storms: Vec<NhcHistoricalStormBindingConfig>,
    pub nws_wind_stations: Vec<NwsWindStationBindingConfig>,
}

impl Default for WeatherVerticalBindingsConfig {
    fn default() -> Self {
        Self {
            hko_rainfall: vec![HkoRainfallBindingConfig {
                place: "North District".to_owned(),
                timezone: "Asia/Hong_Kong".to_owned(),
            }],
            hko_daily_temperature: vec![HkoDailyTemperatureBindingConfig {
                station: "HKO".to_owned(),
                timezone: "Asia/Hong_Kong".to_owned(),
            }],
            airnow_pm25_reporting_areas: vec![
                AirNowPm25ReportingAreaBindingConfig {
                    area: "New York City Region".to_owned(),
                    state: "NY".to_owned(),
                    timezone: "America/New_York".to_owned(),
                },
                AirNowPm25ReportingAreaBindingConfig {
                    area: "Philadelphia".to_owned(),
                    state: "PA".to_owned(),
                    timezone: "America/New_York".to_owned(),
                },
                AirNowPm25ReportingAreaBindingConfig {
                    area: "Columbus".to_owned(),
                    state: "OH".to_owned(),
                    timezone: "America/New_York".to_owned(),
                },
                AirNowPm25ReportingAreaBindingConfig {
                    area: "Chicago".to_owned(),
                    state: "IL".to_owned(),
                    timezone: "America/Chicago".to_owned(),
                },
            ],
            airnow_pm25_sites: vec![AirNowPm25SiteBindingConfig {
                contract_location: "East Rutherford".to_owned(),
                primary_resolution_url:
                    "https://www.airnow.gov/?city=East%20Rutherford&state=NJ&country=USA".to_owned(),
                aqsid: "840340170008".to_owned(),
                site_name: "Union City High School".to_owned(),
                state: "NJ".to_owned(),
                latitude: dec!(40.770908),
                longitude: dec!(-74.036218),
                timezone: "America/New_York".to_owned(),
            }],
            tornado_regions: vec![TornadoRegionBindingConfig {
                region_id: "oklahoma".to_owned(),
                spc_state_code: "OK".to_owned(),
                ncei_state_name: "OKLAHOMA".to_owned(),
                timezone: "America/Chicago".to_owned(),
            }],
            nhc_historical_storms: vec![NhcHistoricalStormBindingConfig {
                basin: "atlantic".to_owned(),
                storm_id: "AL092021".to_owned(),
            }],
            nws_wind_stations: vec![NwsWindStationBindingConfig {
                station: "KMWN".to_owned(),
                timezone: "America/New_York".to_owned(),
            }],
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HkoRainfallBindingConfig {
    pub place: String,
    pub timezone: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HkoDailyTemperatureBindingConfig {
    pub station: String,
    pub timezone: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AirNowPm25ReportingAreaBindingConfig {
    pub area: String,
    pub state: String,
    pub timezone: String,
}

/// Exact preliminary PM2.5 AQI monitoring site used as a contract fallback.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AirNowPm25SiteBindingConfig {
    pub contract_location: String,
    pub primary_resolution_url: String,
    pub aqsid: String,
    pub site_name: String,
    pub state: String,
    pub latitude: Decimal,
    pub longitude: Decimal,
    pub timezone: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TornadoRegionBindingConfig {
    pub region_id: String,
    pub spc_state_code: String,
    pub ncei_state_name: String,
    pub timezone: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NhcHistoricalStormBindingConfig {
    pub basin: String,
    pub storm_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NwsWindStationBindingConfig {
    pub station: String,
    pub timezone: String,
}

/// Hong Kong Observatory public current-weather feed.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct HkoOpenDataSourceConfig {
    pub enabled: bool,
    pub base_url: String,
    pub request_timeout_ms: u64,
    pub rainfall_poll_secs: u64,
    pub daily_temperature_poll_secs: u64,
    /// Maximum monthly partitions inspected newest-first when the latest
    /// documented publication is not yet present in the source file.
    pub daily_temperature_lookback_months: u16,
}

impl Default for HkoOpenDataSourceConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            base_url: "https://data.weather.gov.hk/weatherAPI/opendata".to_owned(),
            request_timeout_ms: 10_000,
            rainfall_poll_secs: 300,
            daily_temperature_poll_secs: 1_800,
            daily_temperature_lookback_months: 24,
        }
    }
}

/// EPA `AirNow` nationwide reporting-area file.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct AirNowSourceConfig {
    pub enabled: bool,
    pub reporting_area_url: String,
    pub hourly_aq_base_url: String,
    pub request_timeout_ms: u64,
    pub poll_secs: u64,
    /// `AirNow` republishes recent hourly observations as preliminary
    /// corrections; workers must re-read at least this window.
    pub correction_lookback_hours: u16,
}

impl Default for AirNowSourceConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            reporting_area_url: "https://files.airnowtech.org/airnow/today/reportingarea.dat"
                .to_owned(),
            hourly_aq_base_url: "https://files.airnowtech.org/airnow".to_owned(),
            request_timeout_ms: 30_000,
            poll_secs: 1_800,
            correction_lookback_hours: 72,
        }
    }
}

/// NOAA SPC preliminary reports and NCEI final Storm Events archive.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct TornadoSourceConfig {
    pub enabled: bool,
    pub spc_base_url: String,
    pub ncei_csv_base_url: String,
    pub request_timeout_ms: u64,
    pub spc_poll_secs: u64,
    pub ncei_refresh_secs: u64,
}

impl Default for TornadoSourceConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            spc_base_url: "https://www.spc.noaa.gov/climo/reports".to_owned(),
            ncei_csv_base_url: "https://www.ncei.noaa.gov/pub/data/swdi/stormevents/csvfiles"
                .to_owned(),
            request_timeout_ms: 120_000,
            spc_poll_secs: 600,
            ncei_refresh_secs: 2_678_400,
        }
    }
}

/// NOAA National Hurricane Center current advisory and HURDAT2 archives.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct NhcSourceConfig {
    pub enabled: bool,
    pub current_storms_url: String,
    pub data_archive_url: String,
    pub request_timeout_ms: u64,
    pub advisory_poll_secs: u64,
    pub best_track_refresh_secs: u64,
}

impl Default for NhcSourceConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            current_storms_url: "https://www.nhc.noaa.gov/CurrentStorms.json".to_owned(),
            data_archive_url: "https://www.nhc.noaa.gov/data/".to_owned(),
            request_timeout_ms: 120_000,
            advisory_poll_secs: 900,
            best_track_refresh_secs: 31_536_000,
        }
    }
}

/// NASA GISS GISTEMP v4 global land-ocean temperature anomaly table.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct NasaGistempSourceConfig {
    pub enabled: bool,
    pub csv_url: String,
    pub request_timeout_ms: u64,
    pub refresh_secs: u64,
}

impl Default for NasaGistempSourceConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            csv_url: "https://data.giss.nasa.gov/gistemp/tabledata_v4/GLB.Ts+dSST.csv".to_owned(),
            request_timeout_ms: 30_000,
            refresh_secs: 2_678_400,
        }
    }
}

/// NOAA/NSIDC Sea Ice Index v4 daily extent files.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct NsidcSeaIceSourceConfig {
    pub enabled: bool,
    pub north_csv_url: String,
    pub south_csv_url: String,
    pub request_timeout_ms: u64,
    pub refresh_secs: u64,
}

impl Default for NsidcSeaIceSourceConfig {
    fn default() -> Self {
        let base = "https://noaadata.apps.nsidc.org/NOAA/G02135";
        Self {
            enabled: true,
            north_csv_url: format!("{base}/north/daily/data/N_seaice_extent_daily_v4.0.csv"),
            south_csv_url: format!("{base}/south/daily/data/S_seaice_extent_daily_v4.0.csv"),
            request_timeout_ms: 120_000,
            refresh_secs: 86_400,
        }
    }
}

/// NOAA/NWS API station observations used for wind extremes.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct NwsObservationSourceConfig {
    pub enabled: bool,
    pub base_url: String,
    pub request_timeout_ms: u64,
    pub poll_secs: u64,
    /// Number of newest station reports scanned to bridge normal null-value
    /// observations without inventing wind values.
    pub lookback_observations: u16,
}

impl Default for NwsObservationSourceConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            base_url: "https://api.weather.gov".to_owned(),
            request_timeout_ms: 30_000,
            poll_secs: 300,
            lookback_observations: 24,
        }
    }
}

/// Public Polymarket Real-Time Data Socket connection.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct PolymarketRtdsSourceConfig {
    pub enabled: bool,
    pub websocket_url: String,
    pub connect_timeout_ms: u64,
    /// Official protocol heartbeat cadence. The RTDS documentation requires a
    /// text `PING` every five seconds.
    pub keepalive_secs: u64,
    pub max_clock_skew_ms: u64,
}

impl Default for PolymarketRtdsSourceConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            websocket_url: "wss://ws-live-data.polymarket.com".to_owned(),
            connect_timeout_ms: 10_000,
            keepalive_secs: 5,
            max_clock_skew_ms: 30_000,
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
    pub ghcnh_station_id: Option<String>,
    pub historical_binding_kind: WeatherHistoricalBindingKind,
}

/// Relationship between the contract station and its official `GHCNh`
/// calibration series.
///
/// Proxy use is explicit and enters the immutable station profile hash; it can
/// never masquerade as exact settlement provenance.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WeatherHistoricalBindingKind {
    ExactStation,
    OfficialNearbyProxy,
    Unavailable,
}

/// Frozen airport profiles in the active Polymarket daily-temperature catalog.
///
/// Coordinates/elevation are sourced from `AviationWeather` station metadata;
/// `GHCNh` ids are official NCEI archive identities.
#[must_use]
pub fn builtin_weather_station_profiles() -> BTreeMap<String, WeatherStationProfileConfig> {
    weather_station_profiles_1()
        .into_iter()
        .chain(weather_station_profiles_2())
        .chain(weather_station_profiles_3())
        .chain(weather_station_profiles_4())
        .collect()
}

fn weather_station_profiles_1() -> Vec<(String, WeatherStationProfileConfig)> {
    vec![
        station(
            "CYYZ",
            "America/Toronto",
            dec!(43.679),
            dec!(-79.629),
            dec!(171),
            "CAN06158731",
            Exact,
        ),
        station(
            "EDDM",
            "Europe/Berlin",
            dec!(48.348),
            dec!(11.813),
            dec!(445),
            "GMI0000EDDM",
            Exact,
        ),
        station(
            "EFHK",
            "Europe/Helsinki",
            dec!(60.327),
            dec!(24.957),
            dec!(56),
            "FIMU0002974",
            Exact,
        ),
        station(
            "EGLC",
            "Europe/London",
            dec!(51.505),
            dec!(0.055),
            dec!(10),
            "UKI0000EGLC",
            Exact,
        ),
        station(
            "EHAM",
            "Europe/Amsterdam",
            dec!(52.315),
            dec!(4.790),
            dec!(-2),
            "NLMU0029295",
            Exact,
        ),
        station(
            "EPWA",
            "Europe/Warsaw",
            dec!(52.163),
            dec!(20.961),
            dec!(107),
            "PLI0000EPBC",
            Proxy,
        ),
        station(
            "FACT",
            "Africa/Johannesburg",
            dec!(-33.965),
            dec!(18.602),
            dec!(48),
            "SFM00068999",
            Exact,
        ),
        station(
            "KATL",
            "America/New_York",
            dec!(33.62972),
            dec!(-84.44223),
            dec!(309),
            "USW00013874",
            Exact,
        ),
        station(
            "KAUS",
            "America/Chicago",
            dec!(30.1831),
            dec!(-97.68063),
            dec!(148),
            "USW00013904",
            Exact,
        ),
        station(
            "KBKF",
            "America/Denver",
            dec!(39.713),
            dec!(-104.758),
            dec!(1703),
            "USW00023036",
            Exact,
        ),
        station(
            "KDAL",
            "America/Chicago",
            dec!(32.83836),
            dec!(-96.83584),
            dec!(148),
            "USW00013960",
            Exact,
        ),
    ]
}

fn weather_station_profiles_2() -> Vec<(String, WeatherStationProfileConfig)> {
    vec![
        station(
            "KHOU",
            "America/Chicago",
            dec!(29.64582),
            dec!(-95.28214),
            dec!(13),
            "USW00012918",
            Exact,
        ),
        station(
            "KLAX",
            "America/Los_Angeles",
            dec!(33.93817),
            dec!(-118.3866),
            dec!(30),
            "USW00023174",
            Exact,
        ),
        station(
            "KLGA",
            "America/New_York",
            dec!(40.77945),
            dec!(-73.88027),
            dec!(9),
            "USW00014732",
            Exact,
        ),
        station(
            "KMIA",
            "America/New_York",
            dec!(25.78806),
            dec!(-80.31692),
            dec!(1),
            "USW00012839",
            Exact,
        ),
        station(
            "KORD",
            "America/Chicago",
            dec!(41.96017),
            dec!(-87.93161),
            dec!(202),
            "USW00094846",
            Exact,
        ),
        station(
            "KSEA",
            "America/Los_Angeles",
            dec!(47.44467),
            dec!(-122.31442),
            dec!(115),
            "USW00024233",
            Exact,
        ),
        station(
            "KSFO",
            "America/Los_Angeles",
            dec!(37.61961),
            dec!(-122.36561),
            dec!(2),
            "USW00023234",
            Exact,
        ),
        station(
            "LEMD",
            "Europe/Madrid",
            dec!(40.466),
            dec!(-3.555),
            dec!(589),
            "SPMU0098221",
            Exact,
        ),
        station(
            "LFPB",
            "Europe/Paris",
            dec!(48.967),
            dec!(2.428),
            dec!(50),
            "FRI0000LFPG",
            Proxy,
        ),
        station(
            "LIMC",
            "Europe/Rome",
            dec!(45.631),
            dec!(8.728),
            dec!(221),
            "ITMU0016064",
            Proxy,
        ),
        station(
            "LLBG",
            "Asia/Jerusalem",
            dec!(32.011),
            dec!(34.887),
            dec!(35),
            "ISM00040179",
            Proxy,
        ),
        station(
            "LTAC",
            "Europe/Istanbul",
            dec!(40.128),
            dec!(32.995),
            dec!(952),
            "TUM00017130",
            Proxy,
        ),
        station(
            "LTFM",
            "Europe/Istanbul",
            dec!(41.262),
            dec!(28.740),
            dec!(99),
            "TUI0000LTFM",
            Exact,
        ),
    ]
}

fn weather_station_profiles_3() -> Vec<(String, WeatherStationProfileConfig)> {
    vec![
        station(
            "MMMX",
            "America/Mexico_City",
            dec!(19.436),
            dec!(-99.072),
            dec!(2224),
            "MXI0000MMMX",
            Exact,
        ),
        station(
            "MPMG",
            "America/Panama",
            dec!(8.967),
            dec!(-79.555),
            dec!(6),
            "PMW00010718",
            Proxy,
        ),
        station(
            "NZWN",
            "Pacific/Auckland",
            dec!(-41.331),
            dec!(174.806),
            dec!(12),
            "NZI0000NZWN",
            Exact,
        ),
        station(
            "OEJN",
            "Asia/Riyadh",
            dec!(21.685),
            dec!(39.166),
            dec!(8),
            "SAI0000OEJN",
            Exact,
        ),
        station_without_historical("OPKC", "Asia/Karachi", dec!(24.902), dec!(67.139), dec!(20)),
        station_without_historical("RCSS", "Asia/Taipei", dec!(25.069), dec!(121.552), dec!(8)),
        station(
            "RJTT",
            "Asia/Tokyo",
            dec!(35.553),
            dec!(139.781),
            dec!(5),
            "JAI0000RJTT",
            Exact,
        ),
        station(
            "RKPK",
            "Asia/Seoul",
            dec!(35.179),
            dec!(128.938),
            dec!(3),
            "KSI0000RKPK",
            Exact,
        ),
        station(
            "RKSI",
            "Asia/Seoul",
            dec!(37.469),
            dec!(126.451),
            dec!(7),
            "KSI0000RKSI",
            Exact,
        ),
        station(
            "RPLL",
            "Asia/Manila",
            dec!(14.507),
            dec!(121.004),
            dec!(15),
            "RPI0000RPLL",
            Exact,
        ),
        station(
            "SAEZ",
            "America/Argentina/Buenos_Aires",
            dec!(-34.822),
            dec!(-58.536),
            dec!(16),
            "ARI0000SAEZ",
            Exact,
        ),
        station(
            "SBGR",
            "America/Sao_Paulo",
            dec!(-23.432),
            dec!(-46.469),
            dec!(745),
            "BRI0000SBGR",
            Exact,
        ),
    ]
}

fn weather_station_profiles_4() -> Vec<(String, WeatherStationProfileConfig)> {
    vec![
        station(
            "VILK",
            "Asia/Kolkata",
            dec!(26.761),
            dec!(80.889),
            dec!(121),
            "INI0000VILK",
            Exact,
        ),
        station(
            "WMKK",
            "Asia/Kuala_Lumpur",
            dec!(2.747),
            dec!(101.714),
            dec!(21),
            "MYI0000WMKK",
            Exact,
        ),
        station(
            "WSSS",
            "Asia/Singapore",
            dec!(1.368),
            dec!(103.982),
            dec!(17),
            "SNI0000WSSS",
            Exact,
        ),
        station(
            "UUWW",
            "Europe/Moscow",
            dec!(55.592),
            dec!(37.261),
            dec!(195),
            "RSI0000UUDD",
            Proxy,
        ),
        station_without_historical(
            "ZBAA",
            "Asia/Shanghai",
            dec!(40.082),
            dec!(116.603),
            dec!(31),
        ),
        station(
            "ZGGG",
            "Asia/Shanghai",
            dec!(23.392),
            dec!(113.307),
            dec!(11),
            "CHI0000ZGGG",
            Exact,
        ),
        station(
            "ZGSZ",
            "Asia/Shanghai",
            dec!(22.639),
            dec!(113.803),
            dec!(18),
            "CHI0000ZGSZ",
            Exact,
        ),
        station_without_historical(
            "ZHHH",
            "Asia/Shanghai",
            dec!(30.783),
            dec!(114.205),
            dec!(33),
        ),
        station(
            "ZSPD",
            "Asia/Shanghai",
            dec!(31.146),
            dec!(121.800),
            dec!(4),
            "CHI0000ZSPD",
            Exact,
        ),
        station_without_historical(
            "ZSQD",
            "Asia/Shanghai",
            dec!(36.362),
            dec!(120.087),
            dec!(2),
        ),
        station(
            "ZUCK",
            "Asia/Shanghai",
            dec!(29.718),
            dec!(106.639),
            dec!(416),
            "CHI0000ZUCK",
            Exact,
        ),
        station(
            "ZUUU",
            "Asia/Shanghai",
            dec!(30.576),
            dec!(103.950),
            dec!(494),
            "CHI0000ZUUU",
            Exact,
        ),
    ]
}

fn station(
    id: &str,
    timezone: &str,
    latitude: Decimal,
    longitude: Decimal,
    elevation_meters: Decimal,
    ghcnh_station_id: &str,
    historical_binding_kind: WeatherHistoricalBindingKind,
) -> (String, WeatherStationProfileConfig) {
    (
        id.to_owned(),
        WeatherStationProfileConfig {
            timezone: timezone.to_owned(),
            latitude,
            longitude,
            elevation_meters,
            ghcnh_station_id: Some(ghcnh_station_id.to_owned()),
            historical_binding_kind,
        },
    )
}

fn station_without_historical(
    id: &str,
    timezone: &str,
    latitude: Decimal,
    longitude: Decimal,
    elevation_meters: Decimal,
) -> (String, WeatherStationProfileConfig) {
    (
        id.to_owned(),
        WeatherStationProfileConfig {
            timezone: timezone.to_owned(),
            latitude,
            longitude,
            elevation_meters,
            ghcnh_station_id: None,
            historical_binding_kind: WeatherHistoricalBindingKind::Unavailable,
        },
    )
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
    pub agg_trade_recovery_poll_secs: u64,
    pub websocket_rotation_secs: u64,
    pub batch_size: usize,
    pub request_timeout_ms: u64,
    /// Maximum trusted difference between the local midpoint clock and
    /// Binance `GET /api/v3/time`.
    pub max_clock_skew_ms: u64,
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
            agg_trade_recovery_poll_secs: 5,
            websocket_rotation_secs: 82_800,
            batch_size: 5_000,
            request_timeout_ms: 10_000,
            max_clock_skew_ms: 2_000,
        }
    }
}

impl BinanceSourceConfig {
    #[must_use]
    pub fn usdm_futures_default() -> Self {
        Self {
            rest_url: "https://fapi.binance.com".into(),
            websocket_url: "wss://fstream.binance.com/ws".into(),
            ..Self::default()
        }
    }
}

/// Chainlink Data Streams REST/WebSocket connection.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ChainlinkDataStreamsSourceConfig {
    /// A missing subscription is valid for unrelated reports. Any bound
    /// condition still fails closed at preflight/evaluation.
    pub enabled: bool,
    pub rest_url: String,
    pub websocket_url: String,
    pub api_key: Option<SecretText>,
    pub api_secret: Option<SecretText>,
    /// Logical feed key (`BTC-USD`) to immutable V3 feed metadata.
    pub feeds: BTreeMap<String, ChainlinkDataStreamFeedConfig>,
    pub max_clock_skew_ms: u64,
    pub rest_page_limit: usize,
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

impl ChainlinkDataStreamsSourceConfig {
    pub fn normalize_credentials(&mut self) {
        if self.api_key.as_ref().is_some_and(SecretText::is_empty) {
            self.api_key = None;
        }
        if self.api_secret.as_ref().is_some_and(SecretText::is_empty) {
            self.api_secret = None;
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
            day_close_grace_secs: WEATHER_OBSERVATION_DAY_CLOSE_GRACE_SECS,
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
    pub max_concurrency: usize,
}

impl Default for GhcnhSourceConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            base_url: "https://www.ncei.noaa.gov/oa/global-historical-climatology-network/hourly/access/by-year".into(),
            request_timeout_ms: 120_000,
            refresh_secs: 86_400,
            calibration_years: 2,
            max_concurrency: 2,
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
