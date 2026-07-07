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
use sea_orm::{DeriveIntoActiveModel, DerivePartialModel, FromQueryResult};
use serde::{Deserialize, Serialize};

use crate::{
    clickhouse::{ChDecimal64, ChSchemaVersion, DomainObservationRow},
    enums::domain::{DomainFamily, DomainMetric},
    types::{DomainInstrumentKey, DomainSourceId},
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
        })
    }
}

/// Typed lifecycle status for a domain-source ingest cursor.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DomainCursorStatus {
    /// No checkpoint yet; the historical backfill has not started.
    Bootstrap,
    /// Backfilling history toward the live edge.
    Backfilling,
    /// At the live edge, ingesting incrementally.
    Live,
    /// The last tick failed; the cursor did not advance.
    Error,
}

impl DomainCursorStatus {
    /// Stable persisted label.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Bootstrap => "bootstrap",
            Self::Backfilling => "backfilling",
            Self::Live => "live",
            Self::Error => "error",
        }
    }

    /// Decode a persisted label.
    #[must_use]
    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "bootstrap" => Some(Self::Bootstrap),
            "backfilling" => Some(Self::Backfilling),
            "live" => Some(Self::Live),
            "error" => Some(Self::Error),
            _ => None,
        }
    }
}

/// Persisted ingest checkpoint for one `(source, instrument)` stream.
#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, DerivePartialModel, FromQueryResult,
)]
#[sea_orm(entity = "crate::entities::quant_domain_source_cursor::Entity")]
pub struct DomainSourceCursorInfo {
    pub source_id: DomainSourceId,
    pub instrument_key: DomainInstrumentKey,
    pub last_event_time: DateTime<Utc>,
    pub status: String,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

info_from_model!(
    DomainSourceCursorInfo,
    crate::entities::quant_domain_source_cursor::Model,
    {
        source_id,
        instrument_key,
        last_event_time,
        status,
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
    pub last_event_time: DateTime<Utc>,
    pub status: String,
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
        };
        let row = observation.clone().into_clickhouse_row(Utc::now());
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
        };
        let mut row = observation.into_clickhouse_row(Utc::now());
        "not_a_family".clone_into(&mut row.family);
        assert!(DomainObservation::from_clickhouse_row(&row).is_none());
    }
}
