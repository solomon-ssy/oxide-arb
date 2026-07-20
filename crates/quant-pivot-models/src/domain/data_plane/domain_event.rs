//! Source facts and append-only derived domain-event envelopes.

use chrono::{DateTime, NaiveDate, TimeZone, Utc};
use rust_decimal::Decimal;
use sea_orm::FromJsonQueryResult;
use serde::{Deserialize, Serialize};

use crate::{
    clickhouse::{
        ChDecimal64, ChSchemaVersion, CryptoPriceReportRow, WeatherForecastFactRow,
        WeatherObservationFactRow,
    },
    hashing::CanonicalDigest,
    types::{
        ContentHash, DomainEventId, DomainInstrumentKey, DomainMeasurementUnit, DomainSourceId,
        IcaoStation, Shares, TemperatureCelsius, Usd, WeatherTemperatureStatistic, WeatherVariable,
    },
};

/// One source-native crypto price fact. Signed Chainlink reports and Binance
/// aggregate trades share this immutable envelope but retain their raw source
/// identity and timing evidence.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CryptoPriceReport {
    pub source_id: DomainSourceId,
    pub instrument_key: DomainInstrumentKey,
    pub source_sequence: u64,
    pub price: Usd,
    pub quantity: Option<Shares>,
    pub event_time: DateTime<Utc>,
    pub published_at: DateTime<Utc>,
    pub available_at: DateTime<Utc>,
    pub valid_from: Option<DateTime<Utc>>,
    pub observations_timestamp: Option<DateTime<Utc>>,
    pub expires_at: Option<DateTime<Utc>>,
    pub report_hash: ContentHash,
    pub raw_report: String,
}

impl CryptoPriceReport {
    #[must_use]
    pub fn to_clickhouse_row(&self) -> CryptoPriceReportRow {
        CryptoPriceReportRow {
            source_id: self.source_id.clone(),
            instrument_key: self.instrument_key.clone(),
            source_sequence: self.source_sequence,
            price: ChDecimal64::from(self.price.inner()),
            quantity: self
                .quantity
                .map(|quantity| ChDecimal64::from(quantity.inner())),
            event_time: self.event_time.timestamp_millis(),
            published_at: self.published_at.timestamp_millis(),
            available_at: self.available_at.timestamp_millis(),
            valid_from: self.valid_from.map(|value| value.timestamp_millis()),
            observations_timestamp: self
                .observations_timestamp
                .map(|value| value.timestamp_millis()),
            expires_at: self.expires_at.map(|value| value.timestamp_millis()),
            report_hash: self.report_hash.clone(),
            raw_report: self.raw_report.clone(),
            schema_version: ChSchemaVersion::FIRST,
        }
    }

    /// Decode one persisted source fact without inventing missing timestamps.
    #[must_use]
    pub fn from_clickhouse_row(row: CryptoPriceReportRow) -> Option<Self> {
        Some(Self {
            source_id: row.source_id,
            instrument_key: row.instrument_key,
            source_sequence: row.source_sequence,
            price: Usd::new(row.price.to_decimal()),
            quantity: row
                .quantity
                .map(|quantity| Shares::new(quantity.to_decimal())),
            event_time: Utc.timestamp_millis_opt(row.event_time).single()?,
            published_at: Utc.timestamp_millis_opt(row.published_at).single()?,
            available_at: Utc.timestamp_millis_opt(row.available_at).single()?,
            valid_from: decode_optional_millis(row.valid_from).ok()?,
            observations_timestamp: decode_optional_millis(row.observations_timestamp).ok()?,
            expires_at: decode_optional_millis(row.expires_at).ok()?,
            report_hash: row.report_hash,
            raw_report: row.raw_report,
        })
    }
}

#[derive(Debug)]
struct InvalidMillis;

fn decode_optional_millis(value: Option<i64>) -> Result<Option<DateTime<Utc>>, InvalidMillis> {
    value
        .map(|value| {
            Utc.timestamp_millis_opt(value)
                .single()
                .ok_or(InvalidMillis)
        })
        .transpose()
}

/// Source-native aviation observation classification. A correction is explicit
/// because the same observation timestamp may legitimately have multiple facts.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WeatherObservationReportKind {
    Metar,
    Speci,
    Correction,
    /// Historical `GHCNh` calibration fact. It is never projected as live
    /// `AviationWeather` state.
    HistoricalGhcnh,
    /// HKO rolling rainfall window from the public current-weather feed.
    HkoRainfall,
    /// HKO finalized daily maximum/minimum temperature from the climate API.
    HkoDailyTemperature,
    /// Preliminary `AirNow` reporting-area PM2.5 observation or prior-day maximum.
    AirNowPm25AreaObservation,
    /// Preliminary `AirNow` exact monitoring-site PM2.5 AQI observation.
    AirNowPm25SiteObservation,
    /// Preliminary, realtime SPC local storm-report count.
    SpcPreliminaryTornado,
    /// Post-storm NCEI Storm Events tornado count.
    NceiFinalTornado,
    /// NHC realtime tropical-cyclone advisory intensity.
    NhcAdvisory,
    /// NHC post-analysis HURDAT2 best-track intensity.
    NhcBestTrack,
    /// NASA GISTEMP v4 monthly global land-ocean anomaly.
    NasaGistemp,
    /// NOAA/NSIDC Sea Ice Index v4 daily hemisphere extent.
    NsidcSeaIce,
    /// NOAA/NWS API quality-controlled station wind observation.
    NwsStation,
}

/// Immutable long-form Weather observation.
///
/// Station/site/region identity is carried by `subject_key`; the source-native
/// instrument remains explicit. Local date and revision/supersession are
/// assigned only after applying the frozen capability binding in the
/// projection transaction.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WeatherObservationReport {
    pub source_id: DomainSourceId,
    pub instrument_key: DomainInstrumentKey,
    pub subject_key: String,
    pub report_kind: WeatherObservationReportKind,
    pub variable: WeatherVariable,
    pub value: Decimal,
    pub unit: DomainMeasurementUnit,
    pub precision: Decimal,
    pub observed_at: DateTime<Utc>,
    pub valid_from: Option<DateTime<Utc>>,
    pub valid_to: Option<DateTime<Utc>>,
    pub published_at: DateTime<Utc>,
    pub available_at: DateTime<Utc>,
    pub report_hash: ContentHash,
    pub raw_report: String,
}

impl WeatherObservationReport {
    #[must_use]
    pub fn to_clickhouse_row(
        &self,
        local_date: NaiveDate,
        revision: u32,
        supersedes_report_hash: Option<ContentHash>,
    ) -> WeatherObservationFactRow {
        WeatherObservationFactRow {
            source_id: self.source_id.clone(),
            instrument_key: self.instrument_key.clone(),
            subject_key: self.subject_key.clone(),
            local_date: local_date.into(),
            report_kind: self.report_kind.as_str().to_owned(),
            variable: self.variable.as_str().to_owned(),
            value: ChDecimal64::from(self.value),
            unit: self.unit.as_str().to_owned(),
            precision: ChDecimal64::from(self.precision),
            observed_at: self.observed_at.timestamp_millis(),
            valid_from: self.valid_from.map(|value| value.timestamp_millis()),
            valid_to: self.valid_to.map(|value| value.timestamp_millis()),
            published_at: self.published_at.timestamp_millis(),
            available_at: self.available_at.timestamp_millis(),
            revision,
            report_hash: self.report_hash.clone(),
            supersedes_report_hash,
            raw_report: self.raw_report.clone(),
            schema_version: ChSchemaVersion::FIRST,
        }
    }
}

impl WeatherObservationReportKind {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Metar => "metar",
            Self::Speci => "speci",
            Self::Correction => "correction",
            Self::HistoricalGhcnh => "historical_ghcnh",
            Self::HkoRainfall => "hko_rainfall",
            Self::HkoDailyTemperature => "hko_daily_temperature",
            Self::AirNowPm25AreaObservation => "airnow_pm25_area_observation",
            Self::AirNowPm25SiteObservation => "airnow_pm25_site_observation",
            Self::SpcPreliminaryTornado => "spc_preliminary_tornado",
            Self::NceiFinalTornado => "ncei_final_tornado",
            Self::NhcAdvisory => "nhc_advisory",
            Self::NhcBestTrack => "nhc_best_track",
            Self::NasaGistemp => "nasa_gistemp",
            Self::NsidcSeaIce => "nsidc_sea_ice",
            Self::NwsStation => "nws_station",
        }
    }

    #[must_use]
    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "metar" => Some(Self::Metar),
            "speci" => Some(Self::Speci),
            "correction" => Some(Self::Correction),
            "historical_ghcnh" => Some(Self::HistoricalGhcnh),
            "hko_rainfall" => Some(Self::HkoRainfall),
            "hko_daily_temperature" => Some(Self::HkoDailyTemperature),
            "airnow_pm25_area_observation" => Some(Self::AirNowPm25AreaObservation),
            "airnow_pm25_site_observation" => Some(Self::AirNowPm25SiteObservation),
            "spc_preliminary_tornado" => Some(Self::SpcPreliminaryTornado),
            "ncei_final_tornado" => Some(Self::NceiFinalTornado),
            "nhc_advisory" => Some(Self::NhcAdvisory),
            "nhc_best_track" => Some(Self::NhcBestTrack),
            "nasa_gistemp" => Some(Self::NasaGistemp),
            "nsidc_sea_ice" => Some(Self::NsidcSeaIce),
            "nws_station" => Some(Self::NwsStation),
            _ => None,
        }
    }
}

/// Persisted Weather observation with projection-assigned local date/revision.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WeatherObservationFact {
    pub source_id: DomainSourceId,
    pub instrument_key: DomainInstrumentKey,
    pub subject_key: String,
    pub local_date: NaiveDate,
    pub report_kind: WeatherObservationReportKind,
    pub variable: WeatherVariable,
    pub value: Decimal,
    pub unit: DomainMeasurementUnit,
    pub precision: Decimal,
    pub observed_at: DateTime<Utc>,
    pub valid_from: Option<DateTime<Utc>>,
    pub valid_to: Option<DateTime<Utc>>,
    pub published_at: DateTime<Utc>,
    pub available_at: DateTime<Utc>,
    pub revision: u32,
    pub report_hash: ContentHash,
    pub supersedes_report_hash: Option<ContentHash>,
}

impl WeatherObservationFact {
    #[must_use]
    pub fn from_clickhouse_row(row: WeatherObservationFactRow) -> Option<Self> {
        Some(Self {
            source_id: row.source_id,
            instrument_key: row.instrument_key,
            subject_key: row.subject_key,
            local_date: row.local_date.to_naive_date()?,
            report_kind: WeatherObservationReportKind::parse(&row.report_kind)?,
            variable: WeatherVariable::parse(&row.variable)?,
            value: row.value.to_decimal(),
            unit: DomainMeasurementUnit::parse(&row.unit)?,
            precision: row.precision.to_decimal(),
            observed_at: Utc.timestamp_millis_opt(row.observed_at).single()?,
            valid_from: decode_optional_millis(row.valid_from).ok()?,
            valid_to: decode_optional_millis(row.valid_to).ok()?,
            published_at: Utc.timestamp_millis_opt(row.published_at).single()?,
            available_at: Utc.timestamp_millis_opt(row.available_at).single()?,
            revision: row.revision,
            report_hash: row.report_hash,
            supersedes_report_hash: row.supersedes_report_hash,
        })
    }

    #[must_use]
    pub fn station(&self) -> Option<IcaoStation> {
        IcaoStation::parse(&self.subject_key).ok()
    }

    #[must_use]
    pub fn temperature_celsius(&self) -> Option<TemperatureCelsius> {
        (self.variable == WeatherVariable::Temperature
            && self.unit == DomainMeasurementUnit::Celsius)
            .then(|| TemperatureCelsius::new(self.value))
    }
}

/// One raw GEFS ensemble point. Bias is intentionally absent until a real
/// station×lead calibration artifact is available.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WeatherForecastPoint {
    pub source_id: DomainSourceId,
    pub instrument_key: DomainInstrumentKey,
    pub subject_key: String,
    pub variable: WeatherVariable,
    pub value: Decimal,
    pub unit: DomainMeasurementUnit,
    pub precision: Decimal,
    pub reference_time: DateTime<Utc>,
    pub valid_time: DateTime<Utc>,
    pub published_at: DateTime<Utc>,
    pub available_at: DateTime<Utc>,
    pub lead_hours: u16,
    pub member: Option<u16>,
    pub revision: u32,
    pub grid_binding_hash: ContentHash,
    pub run_manifest_hash: ContentHash,
    pub report_hash: ContentHash,
}

impl WeatherForecastPoint {
    #[must_use]
    pub fn to_clickhouse_row(&self) -> WeatherForecastFactRow {
        WeatherForecastFactRow {
            source_id: self.source_id.clone(),
            instrument_key: self.instrument_key.clone(),
            subject_key: self.subject_key.clone(),
            variable: self.variable.as_str().to_owned(),
            value: ChDecimal64::from(self.value),
            unit: self.unit.as_str().to_owned(),
            precision: ChDecimal64::from(self.precision),
            reference_time: self.reference_time.timestamp_millis(),
            valid_time: self.valid_time.timestamp_millis(),
            published_at: self.published_at.timestamp_millis(),
            available_at: self.available_at.timestamp_millis(),
            lead_hours: self.lead_hours,
            member: self.member,
            revision: self.revision,
            grid_binding_hash: self.grid_binding_hash.clone(),
            run_manifest_hash: self.run_manifest_hash.clone(),
            report_hash: self.report_hash.clone(),
            schema_version: ChSchemaVersion::FIRST,
        }
    }

    #[must_use]
    pub fn from_clickhouse_row(row: WeatherForecastFactRow) -> Option<Self> {
        Some(Self {
            source_id: row.source_id,
            instrument_key: row.instrument_key,
            subject_key: row.subject_key,
            variable: WeatherVariable::parse(&row.variable)?,
            value: row.value.to_decimal(),
            unit: DomainMeasurementUnit::parse(&row.unit)?,
            precision: row.precision.to_decimal(),
            reference_time: Utc.timestamp_millis_opt(row.reference_time).single()?,
            valid_time: Utc.timestamp_millis_opt(row.valid_time).single()?,
            published_at: Utc.timestamp_millis_opt(row.published_at).single()?,
            available_at: Utc.timestamp_millis_opt(row.available_at).single()?,
            lead_hours: row.lead_hours,
            member: row.member,
            revision: row.revision,
            grid_binding_hash: row.grid_binding_hash,
            run_manifest_hash: row.run_manifest_hash,
            report_hash: row.report_hash,
        })
    }

    #[must_use]
    pub fn station(&self) -> Option<IcaoStation> {
        IcaoStation::parse(&self.subject_key).ok()
    }

    #[must_use]
    pub fn temperature_celsius(
        &self,
        statistic: WeatherTemperatureStatistic,
    ) -> Option<TemperatureCelsius> {
        let expected = match statistic {
            WeatherTemperatureStatistic::Maximum => WeatherVariable::TemperatureMaximum,
            WeatherTemperatureStatistic::Minimum => WeatherVariable::TemperatureMinimum,
        };
        (self.variable == expected && self.unit == DomainMeasurementUnit::Celsius)
            .then(|| TemperatureCelsius::new(self.value))
    }
}

/// CloudEvents-style immutable envelope with bitemporal/revision provenance.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, FromJsonQueryResult)]
#[serde(deny_unknown_fields)]
pub struct DomainEventEnvelope {
    pub id: DomainEventId,
    pub source: DomainSourceId,
    pub event_type: DomainEventType,
    pub subject: String,
    pub time: DateTime<Utc>,
    pub schema_version: u32,
    pub published_at: DateTime<Utc>,
    pub available_at: DateTime<Utc>,
    pub revision: u32,
    pub supersedes_event_id: Option<DomainEventId>,
    pub payload_hash: ContentHash,
    pub source_checkpoint_hash: ContentHash,
    pub payload: DomainEventPayload,
}

impl DomainEventEnvelope {
    #[must_use]
    pub fn validate_payload_hash(&self) -> bool {
        CanonicalDigest::content_hash_json(&self.payload)
            .is_ok_and(|hash| hash == self.payload_hash)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DomainEventType {
    CryptoPriceTransition,
    WeatherDailyTemperatureExtremeAdvanced,
    WeatherDailyTemperatureExtremeCorrected,
    WeatherObservationDayClosed,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub enum DomainEventPayload {
    CryptoPriceTransition(CryptoPriceTransition),
    WeatherDailyTemperatureExtremeAdvanced(WeatherDailyTemperatureExtremeChange),
    WeatherDailyTemperatureExtremeCorrected(WeatherDailyTemperatureExtremeChange),
    WeatherObservationDayClosed(WeatherObservationDayClosed),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CryptoPriceTransition {
    pub instrument_key: DomainInstrumentKey,
    pub previous_price: Usd,
    pub current_price: Usd,
    pub source_sequence: u64,
    pub gap_generation: u64,
    pub report_hash: ContentHash,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WeatherDailyTemperatureExtremeChange {
    pub station: String,
    pub local_date: NaiveDate,
    pub temperature_statistic: WeatherTemperatureStatistic,
    pub previous_extreme: Option<TemperatureCelsius>,
    pub current_extreme: TemperatureCelsius,
    pub report_hash: ContentHash,
    pub gap_generation: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WeatherObservationDayClosed {
    pub station: String,
    pub local_date: NaiveDate,
    pub temperature_statistic: WeatherTemperatureStatistic,
    pub final_noaa_extreme: TemperatureCelsius,
    pub last_report_hash: ContentHash,
    pub gap_generation: u64,
}
