//! External-vertical domain observations.
//!
//! A [`DomainObservation`] is one point-in-time metric reading from an external
//! feature source (a Binance kline field, a Chainlink oracle round). The long
//! format is shared by every vertical: `(family, source, instrument, metric,
//! value, event_time)` — adding a vertical, metric, or source never changes
//! this shape.

use std::{cmp::Ordering, str::FromStr};

use chrono::{DateTime, NaiveDate, TimeZone, Utc};
use rust_decimal::Decimal;
use sea_orm::{DeriveIntoActiveModel, DerivePartialModel, FromJsonQueryResult};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{
    clickhouse::{ChDecimal64, ChSchemaVersion, DomainObservationRow},
    entities::quant_domain_source_cursor,
    enums::domain::{DomainFamily, DomainMetric},
    hashing::CanonicalDigest,
    types::{ContentHash, DomainInstrumentKey, DomainSourceId, EvmBlockHash},
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
            family: self.family.to_string(),
            source_id: self.source_id,
            instrument_key: self.instrument_key,
            metric: self.metric.to_string(),
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
            value: Decimal::from(row.value),
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
    PolymarketCtfResolution {
        finalized_block: u64,
        block_hash: EvmBlockHash,
        block_time: DateTime<Utc>,
    },
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
    Ghcnd {
        last_day_end: DateTime<Utc>,
        file_hash: ContentHash,
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
    HkoDailyRainfall {
        day_end: DateTime<Utc>,
        available_at: DateTime<Utc>,
        file_hash: ContentHash,
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
        collection_date: NaiveDate,
        file_hash: ContentHash,
    },
    NceiTornadoTimeSeries {
        last_period_end: DateTime<Utc>,
        available_at: DateTime<Utc>,
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
        collection_date: NaiveDate,
        file_hash: ContentHash,
    },
    NasaGistemp {
        last_period_end: DateTime<Utc>,
        available_at: DateTime<Utc>,
        file_hash: ContentHash,
    },
    NsidcDailySeaIce {
        last_day_end: DateTime<Utc>,
        available_at: DateTime<Utc>,
        file_hash: ContentHash,
    },
    NsidcMonthlySeaIce {
        last_month_end: DateTime<Utc>,
        available_at: DateTime<Utc>,
        partition_set_hash: ContentHash,
    },
    NwsObservation {
        observed_at: DateTime<Utc>,
        available_at: DateTime<Utc>,
        report_hash: ContentHash,
    },
}

/// Validation failures for source-native Crypto checkpoint construction and ordering.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum CryptoCheckpointError {
    #[error("unsupported Crypto source `{source_id}`")]
    UnsupportedSource { source_id: DomainSourceId },
    #[error("Crypto report/checkpoint binding mismatch: {detail}")]
    BindingMismatch { detail: &'static str },
    #[error("Crypto checkpoint type changed within one source binding")]
    CheckpointTypeChanged,
    #[error("invalid persisted Crypto timestamp `{field}`: {value}")]
    InvalidTimestamp { field: &'static str, value: i64 },
}

/// Hash-independent source-native ordering key for one Crypto checkpoint.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum CryptoCheckpointKey {
    BinanceAggTrade {
        aggregate_trade_id: u64,
    },
    ChainlinkDataStreams {
        observations_timestamp: DateTime<Utc>,
    },
    PolymarketRtds {
        source_timestamp: DateTime<Utc>,
        envelope_timestamp: DateTime<Utc>,
    },
}

impl DomainSourceCheckpoint {
    /// Immutable report hash carried by checkpoints whose source key can revise.
    #[must_use]
    pub const fn crypto_report_hash(&self) -> Option<ContentHash> {
        match self {
            Self::ChainlinkDataStreams { report_hash, .. }
            | Self::PolymarketRtds { report_hash, .. } => Some(*report_hash),
            _ => None,
        }
    }

    /// Project the checkpoint into its hash-independent source ordering key.
    pub const fn crypto_order_key(&self) -> Result<CryptoCheckpointKey, CryptoCheckpointError> {
        match self {
            Self::BinanceAggTrade {
                aggregate_trade_id, ..
            } => Ok(CryptoCheckpointKey::BinanceAggTrade {
                aggregate_trade_id: *aggregate_trade_id,
            }),
            Self::ChainlinkDataStreams {
                observations_timestamp,
                ..
            } => Ok(CryptoCheckpointKey::ChainlinkDataStreams {
                observations_timestamp: *observations_timestamp,
            }),
            Self::PolymarketRtds {
                source_timestamp,
                envelope_timestamp,
                ..
            } => Ok(CryptoCheckpointKey::PolymarketRtds {
                source_timestamp: *source_timestamp,
                envelope_timestamp: *envelope_timestamp,
            }),
            _ => Err(CryptoCheckpointError::CheckpointTypeChanged),
        }
    }

    /// Compare an incoming Crypto checkpoint with this durable frontier.
    ///
    /// The returned ordering is `incoming.cmp(self)`. Report hashes never
    /// participate in ordering: an equal source-native key with a different
    /// hash is equivocation and must be rejected by the caller.
    pub fn compare_crypto(&self, incoming: &Self) -> Result<Ordering, CryptoCheckpointError> {
        let ordering = match (self, incoming) {
            (
                Self::BinanceAggTrade {
                    aggregate_trade_id: current,
                    ..
                },
                Self::BinanceAggTrade {
                    aggregate_trade_id: incoming,
                    ..
                },
            ) => incoming.cmp(current),
            (
                Self::ChainlinkDataStreams {
                    observations_timestamp: current,
                    ..
                },
                Self::ChainlinkDataStreams {
                    observations_timestamp: incoming,
                    ..
                },
            ) => incoming.cmp(current),
            (
                Self::PolymarketRtds {
                    source_timestamp: current_source,
                    envelope_timestamp: current_envelope,
                    ..
                },
                Self::PolymarketRtds {
                    source_timestamp: incoming_source,
                    envelope_timestamp: incoming_envelope,
                    ..
                },
            ) => (*incoming_source, *incoming_envelope).cmp(&(*current_source, *current_envelope)),
            _ => return Err(CryptoCheckpointError::CheckpointTypeChanged),
        };
        Ok(ordering)
    }

    /// Source-order bounds used to keep `ClickHouse` reads behind the committed frontier.
    pub fn crypto_query_frontier(&self) -> Result<(u64, i64), CryptoCheckpointError> {
        match self {
            Self::BinanceAggTrade {
                aggregate_trade_id, ..
            } => Ok((*aggregate_trade_id, i64::MAX)),
            Self::ChainlinkDataStreams {
                observations_timestamp,
                ..
            } => Ok((
                u64::try_from(observations_timestamp.timestamp()).map_err(|_| {
                    CryptoCheckpointError::BindingMismatch {
                        detail: "negative Chainlink observations timestamp",
                    }
                })?,
                observations_timestamp.timestamp_millis(),
            )),
            Self::PolymarketRtds {
                source_timestamp,
                envelope_timestamp,
                ..
            } => Ok((
                u64::try_from(source_timestamp.timestamp_millis()).map_err(|_| {
                    CryptoCheckpointError::BindingMismatch {
                        detail: "negative RTDS source timestamp",
                    }
                })?,
                envelope_timestamp.timestamp_millis(),
            )),
            _ => Err(CryptoCheckpointError::CheckpointTypeChanged),
        }
    }

    /// Validate that a persisted projection head is the same source-native checkpoint.
    pub fn validate_crypto_head(
        &self,
        source_id: &DomainSourceId,
        source_sequence: u64,
        event_time: DateTime<Utc>,
        report_hash: ContentHash,
    ) -> Result<(), CryptoCheckpointError> {
        let matches = match self {
            Self::BinanceAggTrade {
                aggregate_trade_id,
                event_time: checkpoint_time,
            } => {
                (source_id == &DomainSourceId::binance_agg_trade()
                    || source_id == &DomainSourceId::binance_futures_trade())
                    && *aggregate_trade_id == source_sequence
                    && same_millis(*checkpoint_time, event_time)
            }
            Self::ChainlinkDataStreams {
                observations_timestamp,
                report_hash: checkpoint_hash,
            } => {
                source_id == &DomainSourceId::chainlink_data_streams()
                    && u64::try_from(observations_timestamp.timestamp()).ok()
                        == Some(source_sequence)
                    && same_millis(*observations_timestamp, event_time)
                    && *checkpoint_hash == report_hash
            }
            Self::PolymarketRtds {
                source_timestamp,
                report_hash: checkpoint_hash,
                ..
            } => {
                (source_id == &DomainSourceId::polymarket_rtds_binance()
                    || source_id == &DomainSourceId::polymarket_rtds_chainlink())
                    && u64::try_from(source_timestamp.timestamp_millis()).ok()
                        == Some(source_sequence)
                    && same_millis(*source_timestamp, event_time)
                    && *checkpoint_hash == report_hash
            }
            _ => false,
        };
        if !matches {
            return Err(CryptoCheckpointError::BindingMismatch {
                detail: "projection head differs from its committed checkpoint",
            });
        }
        Ok(())
    }

    #[must_use]
    pub const fn event_time(&self) -> DateTime<Utc> {
        match self {
            Self::PolymarketCtfResolution { block_time, .. } => *block_time,
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
            Self::Ghcnd { last_day_end, .. } | Self::NsidcDailySeaIce { last_day_end, .. } => {
                *last_day_end
            }
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
            Self::HkoDailyRainfall { day_end, .. } | Self::HkoDailyTemperature { day_end, .. } => {
                *day_end
            }
            Self::AirNowPm25Area { valid_time, .. } => *valid_time,
            Self::SpcTornado {
                report_window_end, ..
            }
            | Self::NceiStormEvents {
                report_window_end, ..
            } => *report_window_end,
            Self::NceiTornadoTimeSeries {
                last_period_end, ..
            }
            | Self::NasaGistemp {
                last_period_end, ..
            } => *last_period_end,
            Self::NhcAdvisory { issuance, .. } => *issuance,
            Self::NhcHurdat2 {
                last_observation, ..
            } => *last_observation,
            Self::NsidcMonthlySeaIce { last_month_end, .. } => *last_month_end,
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
            | Self::Ghcnd { .. }
            | Self::GefsBackfill { .. }
            | Self::HkoDailyRainfall { .. }
            | Self::HkoDailyTemperature { .. }
            | Self::NceiStormEvents { .. }
            | Self::NceiTornadoTimeSeries { .. }
            | Self::NhcHurdat2 { .. }
            | Self::NasaGistemp { .. }
            | Self::NsidcDailySeaIce { .. }
            | Self::NsidcMonthlySeaIce { .. } => cursor_updated_at,
            _ => self.event_time(),
        }
    }
}

const fn same_millis(left: DateTime<Utc>, right: DateTime<Utc>) -> bool {
    left.timestamp_millis() == right.timestamp_millis()
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
    /// for this instrument succeeded; failures remain isolated per instrument.
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

impl DomainSourceCursorInfo {
    /// Revalidate persisted checkpoint content and status/error semantics.
    pub fn validate(&self) -> Result<(), String> {
        validate_cursor_content(
            &self.checkpoint_json,
            self.checkpoint_hash,
            self.status,
            self.last_error.as_deref(),
        )
    }
}

/// Result of an atomic domain-source cursor compare-and-set.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DomainSourceCursorCasOutcome {
    /// The supplied checkpoint became the durable cursor.
    Advanced(DomainSourceCursorInfo),
    /// The expected checkpoint no longer matched; contains the durable winner.
    Conflict(DomainSourceCursorInfo),
}

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

impl UpsertDomainSourceCursor {
    /// Validate content addressing and the closed status/error shape.
    pub fn validate(&self) -> Result<(), String> {
        validate_cursor_content(
            &self.checkpoint_json,
            self.checkpoint_hash,
            self.status,
            self.last_error.as_deref(),
        )
    }
}

fn validate_cursor_content(
    checkpoint: &DomainSourceCheckpoint,
    checkpoint_hash: ContentHash,
    status: DomainCursorStatus,
    last_error: Option<&str>,
) -> Result<(), String> {
    let expected_hash = CanonicalDigest::content_hash_json(checkpoint)
        .map_err(|error| format!("failed to hash domain-source checkpoint: {error}"))?;
    if checkpoint_hash != expected_hash {
        return Err("domain-source checkpoint hash does not match checkpoint content".to_owned());
    }
    match (status, last_error) {
        (DomainCursorStatus::Failed, Some(detail)) if !detail.trim().is_empty() => Ok(()),
        (DomainCursorStatus::Failed, _) => {
            Err("failed domain-source cursor requires a non-empty error".to_owned())
        }
        (_, None) => Ok(()),
        (_, Some(_)) => {
            Err("non-failed domain-source cursor cannot retain an error detail".to_owned())
        }
    }
}

fn millis_to_utc(timestamp_ms: i64) -> Option<DateTime<Utc>> {
    Utc.timestamp_millis_opt(timestamp_ms).single()
}

#[cfg(test)]
mod tests {
    use std::cmp::Ordering;

    use chrono::{Duration, TimeZone, Utc};
    use rust_decimal_macros::dec;

    use super::{DomainObservation, DomainSourceCheckpoint};
    use crate::{
        enums::domain::{DomainFamily, DomainMetric, KlineInterval},
        types::{BinanceSymbol, ContentHash, DomainInstrumentKey, DomainSourceId},
    };

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
    fn unknown_family_label_rejects() {
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

    #[test]
    fn crypto_checkpoint_orders() {
        let source = Utc.with_ymd_and_hms(2026, 8, 29, 12, 0, 0).unwrap();
        let current = DomainSourceCheckpoint::PolymarketRtds {
            source_timestamp: source,
            envelope_timestamp: source,
            report_hash: hash('a'),
        };
        let incoming = DomainSourceCheckpoint::PolymarketRtds {
            source_timestamp: source,
            envelope_timestamp: source + Duration::milliseconds(1),
            report_hash: hash('b'),
        };
        assert_eq!(
            current.compare_crypto(&incoming).expect("RTDS order"),
            Ordering::Greater
        );
        assert_eq!(
            incoming.crypto_query_frontier().expect("query frontier"),
            (
                u64::try_from(source.timestamp_millis()).expect("source timestamp"),
                (source + Duration::milliseconds(1)).timestamp_millis(),
            )
        );
    }

    fn hash(seed: char) -> ContentHash {
        ContentHash::parse(&format!("blake3:{}", seed.to_string().repeat(64))).expect("hash")
    }
}
