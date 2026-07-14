//! Source facts and append-only derived domain-event envelopes.

use chrono::{DateTime, NaiveDate, TimeZone, Utc};
use sea_orm::FromJsonQueryResult;
use serde::{Deserialize, Serialize};

use crate::{
    clickhouse::{
        ChDecimal64, ChSchemaVersion, CryptoPriceReportRow, WeatherForecastPointRow,
        WeatherObservationReportRow,
    },
    hashing::CanonicalDigest,
    types::{
        ContentHash, DomainEventId, DomainInstrumentKey, DomainSourceId, IcaoStation, Shares,
        TemperatureCelsius, Usd,
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
}

/// Immutable `AviationWeather` METAR/SPECI/COR fact. Station-local date and
/// revision/supersession are assigned only after applying the frozen station
/// profile in the projection transaction.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WeatherObservationReport {
    pub source_id: DomainSourceId,
    pub station: IcaoStation,
    pub report_kind: WeatherObservationReportKind,
    pub temperature: TemperatureCelsius,
    pub precision_celsius: rust_decimal::Decimal,
    pub observation_time: DateTime<Utc>,
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
    ) -> WeatherObservationReportRow {
        WeatherObservationReportRow {
            source_id: self.source_id.clone(),
            station: self.station.to_string(),
            local_date: local_date.to_string(),
            report_kind: self.report_kind.as_str().to_owned(),
            temperature_celsius: ChDecimal64::from(self.temperature.value()),
            precision_celsius: ChDecimal64::from(self.precision_celsius),
            observation_time: self.observation_time.timestamp_millis(),
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
        }
    }

    #[must_use]
    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "metar" => Some(Self::Metar),
            "speci" => Some(Self::Speci),
            "correction" => Some(Self::Correction),
            "historical_ghcnh" => Some(Self::HistoricalGhcnh),
            _ => None,
        }
    }
}

/// Persisted Weather observation with projection-assigned local date/revision.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WeatherObservationFact {
    pub source_id: DomainSourceId,
    pub station: IcaoStation,
    pub local_date: NaiveDate,
    pub report_kind: WeatherObservationReportKind,
    pub temperature: TemperatureCelsius,
    pub precision_celsius: rust_decimal::Decimal,
    pub observation_time: DateTime<Utc>,
    pub published_at: DateTime<Utc>,
    pub available_at: DateTime<Utc>,
    pub revision: u32,
    pub report_hash: ContentHash,
    pub supersedes_report_hash: Option<ContentHash>,
}

impl WeatherObservationFact {
    #[must_use]
    pub fn from_clickhouse_row(row: WeatherObservationReportRow) -> Option<Self> {
        Some(Self {
            source_id: row.source_id,
            station: IcaoStation::parse(row.station).ok()?,
            local_date: row.local_date.parse().ok()?,
            report_kind: WeatherObservationReportKind::parse(&row.report_kind)?,
            temperature: TemperatureCelsius::new(row.temperature_celsius.to_decimal()),
            precision_celsius: row.precision_celsius.to_decimal(),
            observation_time: Utc.timestamp_millis_opt(row.observation_time).single()?,
            published_at: Utc.timestamp_millis_opt(row.published_at).single()?,
            available_at: Utc.timestamp_millis_opt(row.available_at).single()?,
            revision: row.revision,
            report_hash: row.report_hash,
            supersedes_report_hash: row.supersedes_report_hash,
        })
    }
}

/// One raw GEFS ensemble point. Bias is intentionally absent until a real
/// station×lead calibration artifact is available.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WeatherForecastPoint {
    pub source_id: DomainSourceId,
    pub station: IcaoStation,
    pub reference_time: DateTime<Utc>,
    pub valid_time: DateTime<Utc>,
    pub available_at: DateTime<Utc>,
    pub lead_hours: u16,
    pub member: u8,
    pub tmax_celsius: TemperatureCelsius,
    pub grid_binding_hash: ContentHash,
    pub run_manifest_hash: ContentHash,
}

impl WeatherForecastPoint {
    #[must_use]
    pub fn from_clickhouse_row(row: WeatherForecastPointRow) -> Option<Self> {
        Some(Self {
            source_id: row.source_id,
            station: IcaoStation::parse(row.station).ok()?,
            reference_time: Utc.timestamp_millis_opt(row.reference_time).single()?,
            valid_time: Utc.timestamp_millis_opt(row.valid_time).single()?,
            available_at: Utc.timestamp_millis_opt(row.available_at).single()?,
            lead_hours: row.lead_hours,
            member: row.member,
            tmax_celsius: TemperatureCelsius::new(row.tmax_celsius.to_decimal()),
            grid_binding_hash: row.grid_binding_hash,
            run_manifest_hash: row.run_manifest_hash,
        })
    }
}

/// CloudEvents-style immutable envelope with bitemporal/revision provenance.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, FromJsonQueryResult)]
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

crate::jsonb_active!(DomainEventEnvelope);

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
    WeatherDailyHighAdvanced,
    WeatherDailyHighCorrected,
    WeatherObservationDayClosed,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub enum DomainEventPayload {
    CryptoPriceTransition(CryptoPriceTransition),
    WeatherDailyHighAdvanced(WeatherDailyHighChange),
    WeatherDailyHighCorrected(WeatherDailyHighChange),
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
pub struct WeatherDailyHighChange {
    pub station: String,
    pub local_date: NaiveDate,
    pub previous_high: Option<TemperatureCelsius>,
    pub current_high: TemperatureCelsius,
    pub report_hash: ContentHash,
    pub gap_generation: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WeatherObservationDayClosed {
    pub station: String,
    pub local_date: NaiveDate,
    pub final_noaa_high: TemperatureCelsius,
    pub last_report_hash: ContentHash,
    pub gap_generation: u64,
}
