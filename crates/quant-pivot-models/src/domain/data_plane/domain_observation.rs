//! External-vertical domain observations (Phase 11.2.2).
//!
//! A [`DomainObservation`] is one point-in-time metric reading from an external
//! feature source (a Binance kline field, a Chainlink oracle round). The long
//! format is shared by every vertical: `(family, source, instrument, metric,
//! value, event_time)` — adding a vertical, metric, or source never changes
//! this shape.

use std::str::FromStr;

use chrono::{DateTime, TimeZone, Utc};
use rust_decimal::Decimal;
use sea_orm::{DeriveIntoActiveModel, DerivePartialModel, FromJsonQueryResult};
use serde::{Deserialize, Serialize};

use crate::{
    clickhouse::{ChDecimal64, ChSchemaVersion, DomainObservationRow},
    entities::quant_domain_source_cursor,
    enums::domain::{DomainFamily, DomainMetric},
    types::{ContentHash, DomainInstrumentKey, DomainSourceId},
};

/// One normalized external-source metric reading.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DomainObservation {
    /// Vertical the observation serves.
    pub family: DomainFamily,
    /// Which source produced it.
    pub source_id: DomainSourceId,
    /// Canonical instrument key.
    pub instrument_key: DomainInstrumentKey,
    /// Metric dimension.
    pub metric: DomainMetric,
    /// Metric value (unit defined by the metric).
    pub value: Decimal,
    /// PIT event time: candle **close** time / oracle round `updatedAt`. This
    /// is the time the datum became knowable — never the window open.
    pub observed_at: DateTime<Utc>,
    /// When the source published the datum (lag bound; `>= observed_at`).
    pub publish_time: DateTime<Utc>,
    /// Time at which this revision became visible to this system.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub available_at: Option<DateTime<Utc>>,
}

impl DomainObservation {
    /// Convert to the `ClickHouse` fact row shape at write time.
    #[must_use]
    pub fn into_clickhouse_row(self, ingestion_time: DateTime<Utc>) -> DomainObservationRow {
        DomainObservationRow {
            family: self.family.as_str().to_owned(),
            source_id: self.source_id,
            instrument_key: self.instrument_key,
            metric: self.metric.as_str().to_owned(),
            value: ChDecimal64::from(self.value),
            event_time: self.observed_at.timestamp_millis(),
            publish_time: self.publish_time.timestamp_millis(),
            ingestion_time: ingestion_time.timestamp_millis(),
            schema_version: ChSchemaVersion::FIRST,
        }
    }

    /// Decode a `ClickHouse` fact row back into the domain shape.
    ///
    /// Returns `None` when the persisted family / metric label is unknown to
    /// this build (fail-closed: an unreadable row never becomes a value).
    #[must_use]
    pub fn from_clickhouse_row(row: &DomainObservationRow) -> Option<Self> {
        Some(Self {
            family: DomainFamily::from_str(&row.family).ok()?,
            source_id: row.source_id.clone(),
            instrument_key: row.instrument_key.clone(),
            metric: DomainMetric::from_str(&row.metric).ok()?,
            value: row.value.to_decimal(),
            observed_at: millis_to_utc(row.event_time)?,
            publish_time: millis_to_utc(row.publish_time)?,
            available_at: Some(millis_to_utc(row.ingestion_time)?),
        })
    }
}

crate::pg_enum! {
    type_name = "qp_domain_cursor_status",
    /// Typed lifecycle status for a domain-source ingest cursor.
    @derive(PartialOrd, Ord)
    pub enum DomainCursorStatus {
        /// No checkpoint yet; the historical backfill has not started.
        Bootstrap => "bootstrap",
        /// Backfilling history toward the live edge.
        Backfilling => "backfilling",
        /// At the live edge, ingesting incrementally.
        Live => "live",
        /// The last tick failed; the cursor did not advance.
        Failed => "error",
    }
}

/// Typed, source-native ingest checkpoint. Equal event times remain distinct
/// through source sequence/report identity, so corrections are never skipped.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, FromJsonQueryResult)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub enum DomainSourceCheckpoint {
    BinanceKline {
        close_time: DateTime<Utc>,
    },
    BinanceAggTrade {
        aggregate_trade_id: u64,
        event_time: DateTime<Utc>,
    },
    ChainlinkDataStreams {
        observations_timestamp: DateTime<Utc>,
        report_hash: ContentHash,
    },
    PolymarketRtds {
        source_timestamp: DateTime<Utc>,
        envelope_timestamp: DateTime<Utc>,
        report_hash: ContentHash,
    },
    AviationWeather {
        available_at: DateTime<Utc>,
        published_at: DateTime<Utc>,
        observation_time: DateTime<Utc>,
        revision: u32,
        report_hash: ContentHash,
    },
    Ghcnh {
        last_hour: DateTime<Utc>,
        file_hash: ContentHash,
        unpublished_years: Vec<i32>,
    },
    Gefs {
        reference_time: DateTime<Utc>,
        request_hash: ContentHash,
        manifest_hash: ContentHash,
    },
    GefsBackfill {
        completed_reference_time: DateTime<Utc>,
        request_hash: ContentHash,
        manifest_hash: ContentHash,
    },
    HkoRainfall {
        window_end: DateTime<Utc>,
        published_at: DateTime<Utc>,
        report_hash: ContentHash,
    },
    HkoDailyTemperature {
        day_end: DateTime<Utc>,
        available_at: DateTime<Utc>,
        response_hash: ContentHash,
        report_hash: ContentHash,
    },
    AirNowPm25Area {
        valid_time: DateTime<Utc>,
        available_at: DateTime<Utc>,
        report_hash: ContentHash,
        correction_scan_hour: DateTime<Utc>,
    },
    AirNowPm25Forecast {
        reference_time: DateTime<Utc>,
        max_valid_time: DateTime<Utc>,
        available_at: DateTime<Utc>,
        file_hash: ContentHash,
    },
    AirNowPm25Site {
        last_valid_time: Option<DateTime<Utc>>,
        available_at: DateTime<Utc>,
        last_report_hash: Option<ContentHash>,
        correction_scan_hour: DateTime<Utc>,
    },
    SpcTornado {
        report_window_end: DateTime<Utc>,
        available_at: DateTime<Utc>,
        report_hash: ContentHash,
    },
    NceiStormEvents {
        report_window_end: DateTime<Utc>,
        collection_date: chrono::NaiveDate,
        file_hash: ContentHash,
    },
    NhcAdvisory {
        issuance: DateTime<Utc>,
        storm_id: String,
        advisory_number: String,
        report_hash: ContentHash,
    },
    NhcHurdat2 {
        last_observation: DateTime<Utc>,
        collection_date: chrono::NaiveDate,
        file_hash: ContentHash,
    },
    NasaGistemp {
        last_month_end: DateTime<Utc>,
        available_at: DateTime<Utc>,
        file_hash: ContentHash,
    },
    NsidcSeaIce {
        last_day_end: DateTime<Utc>,
        available_at: DateTime<Utc>,
        file_hash: ContentHash,
    },
    NwsObservation {
        observed_at: DateTime<Utc>,
        available_at: DateTime<Utc>,
        report_hash: ContentHash,
    },
}

impl DomainSourceCheckpoint {
    #[must_use]
    pub const fn event_time(&self) -> DateTime<Utc> {
        match self {
            Self::BinanceKline { close_time } => *close_time,
            Self::BinanceAggTrade { event_time, .. } => *event_time,
            Self::ChainlinkDataStreams {
                observations_timestamp,
                ..
            } => *observations_timestamp,
            Self::PolymarketRtds {
                source_timestamp, ..
            } => *source_timestamp,
            Self::AviationWeather {
                observation_time, ..
            } => *observation_time,
            Self::Ghcnh { last_hour, .. } => *last_hour,
            Self::Gefs { reference_time, .. } | Self::AirNowPm25Forecast { reference_time, .. } => {
                *reference_time
            }
            Self::AirNowPm25Site {
                last_valid_time: Some(last_valid_time),
                ..
            } => *last_valid_time,
            Self::AirNowPm25Site {
                last_valid_time: None,
                correction_scan_hour,
                ..
            } => *correction_scan_hour,
            Self::GefsBackfill {
                completed_reference_time,
                ..
            } => *completed_reference_time,
            Self::HkoRainfall { window_end, .. } => *window_end,
            Self::HkoDailyTemperature { day_end, .. } => *day_end,
            Self::AirNowPm25Area { valid_time, .. } => *valid_time,
            Self::SpcTornado {
                report_window_end, ..
            }
            | Self::NceiStormEvents {
                report_window_end, ..
            } => *report_window_end,
            Self::NhcAdvisory { issuance, .. } => *issuance,
            Self::NhcHurdat2 {
                last_observation, ..
            } => *last_observation,
            Self::NasaGistemp { last_month_end, .. } => *last_month_end,
            Self::NsidcSeaIce { last_day_end, .. } => *last_day_end,
            Self::NwsObservation { observed_at, .. } => *observed_at,
        }
    }

    /// Timestamp used to evaluate source liveness. Immutable archive cursors
    /// describe old economic events by design, so their successful refresh
    /// time—not the last historical event—is the health clock. Live feeds and
    /// forecast cycles continue to use their source-effective event time.
    #[must_use]
    pub const fn freshness_time(&self, cursor_updated_at: DateTime<Utc>) -> DateTime<Utc> {
        match self {
            Self::Ghcnh { .. }
            | Self::GefsBackfill { .. }
            | Self::HkoDailyTemperature { .. }
            | Self::NceiStormEvents { .. }
            | Self::NhcHurdat2 { .. }
            | Self::NasaGistemp { .. }
            | Self::NsidcSeaIce { .. } => cursor_updated_at,
            _ => self.event_time(),
        }
    }
}

/// Persisted ingest checkpoint for one `(source, instrument)` stream.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, DerivePartialModel)]
#[sea_orm(entity = "crate::entities::quant_domain_source_cursor::Entity")]
pub struct DomainSourceCursorInfo {
    pub source_id: DomainSourceId,
    pub instrument_key: DomainInstrumentKey,
    pub checkpoint_json: DomainSourceCheckpoint,
    pub checkpoint_hash: ContentHash,
    pub status: DomainCursorStatus,
    /// Detail from the most recent failed tick; `None` when the last tick
    /// for this instrument succeeded (R10 ingest hardening).
    pub last_error: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

info_from_model!(
    DomainSourceCursorInfo,
    quant_domain_source_cursor::Model,
    {
        source_id,
        instrument_key,
        checkpoint_json,
        checkpoint_hash,
        status,
        last_error,
        created_at,
        updated_at,
    }
);

/// Upsert payload for the durable domain-source cursor.
#[derive(Debug, Clone, Serialize, Deserialize, DeriveIntoActiveModel)]
#[sea_orm(active_model = "crate::entities::quant_domain_source_cursor::ActiveModel")]
pub struct UpsertDomainSourceCursor {
    pub source_id: DomainSourceId,
    pub instrument_key: DomainInstrumentKey,
    pub checkpoint_json: DomainSourceCheckpoint,
    pub checkpoint_hash: ContentHash,
    pub status: DomainCursorStatus,
    /// Set on a failed tick; explicitly cleared to `None` on the next
    /// success so a resolved error never lingers in the read view.
    pub last_error: Option<String>,
    pub updated_at: DateTime<Utc>,
}

fn millis_to_utc(timestamp_ms: i64) -> Option<DateTime<Utc>> {
    Utc.timestamp_millis_opt(timestamp_ms).single()
}

#[cfg(test)]
mod tests {
    use super::DomainObservation;
    use crate::{
        enums::domain::{DomainFamily, DomainMetric, KlineInterval},
        types::{BinanceSymbol, DomainInstrumentKey, DomainSourceId},
    };
    use chrono::{TimeZone, Utc};
    use rust_decimal_macros::dec;

    #[test]
    fn observation_roundtrips_clickhouse_row() {
        let ingestion_time = Utc.with_ymd_and_hms(2026, 7, 1, 12, 1, 2).unwrap();
        let observation = DomainObservation {
            family: DomainFamily::Crypto,
            source_id: DomainSourceId::binance(),
            instrument_key: DomainInstrumentKey::binance_kline(
                &BinanceSymbol::parse("BTCUSDT").expect("symbol"),
                KlineInterval::OneMinute,
            ),
            metric: DomainMetric::Close,
            value: dec!(104250.12345678),
            observed_at: Utc.with_ymd_and_hms(2026, 7, 1, 12, 1, 0).unwrap(),
            publish_time: Utc.with_ymd_and_hms(2026, 7, 1, 12, 1, 1).unwrap(),
            available_at: Some(ingestion_time),
        };
        let row = observation.clone().into_clickhouse_row(ingestion_time);
        assert_eq!(row.family, "crypto");
        assert_eq!(row.metric, "close");
        let back = DomainObservation::from_clickhouse_row(&row).expect("decode");
        assert_eq!(back, observation);
    }

    #[test]
    fn unknown_family_label_fails_closed() {
        let observation = DomainObservation {
            family: DomainFamily::Crypto,
            source_id: DomainSourceId::binance(),
            instrument_key: DomainInstrumentKey::binance_kline(
                &BinanceSymbol::parse("BTCUSDT").expect("symbol"),
                KlineInterval::OneMinute,
            ),
            metric: DomainMetric::Close,
            value: dec!(1),
            observed_at: Utc::now(),
            publish_time: Utc::now(),
            available_at: None,
        };
        let mut row = observation.into_clickhouse_row(Utc::now());
        "not_a_family".clone_into(&mut row.family);
        assert!(DomainObservation::from_clickhouse_row(&row).is_none());
    }
}
