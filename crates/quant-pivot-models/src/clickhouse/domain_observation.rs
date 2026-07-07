use serde::{Deserialize, Serialize};

use crate::{
    clickhouse::{ChDecimal64, ChSchemaVersion},
    types::{DomainInstrumentKey, DomainSourceId},
};

/// `ClickHouse` row for the long-format `quant_domain_observation` table.
///
/// Deliberately schema-stable along every extension axis: `family`, `metric`
/// and `source_id` are `LowCardinality(String)` wire labels (new verticals,
/// metrics, and sources are pure data), and the instrument key embeds the
/// venue/interval. PIT reads filter `event_time <= as_of - source_delay` with
/// the stable `(event_time, ingestion_time)` tie-break.
#[derive(Debug, Clone, clickhouse::Row, Serialize, Deserialize)]
pub struct DomainObservationRow {
    /// [`crate::enums::domain::DomainFamily`] wire label (e.g. `crypto`).
    pub family: String,
    /// Source label (e.g. `binance`, `chainlink`).
    pub source_id: DomainSourceId,
    /// Canonical instrument key (e.g. `BINANCE:BTCUSDT:1m`).
    pub instrument_key: DomainInstrumentKey,
    /// [`crate::enums::domain::DomainMetric`] wire label (e.g. `close`).
    pub metric: String,
    /// Metric value (unit defined by the metric; quote currency for prices).
    pub value: ChDecimal64,
    /// PIT event time (candle close / oracle round update), epoch ms.
    pub event_time: i64,
    /// When the source published the datum (lag bound), epoch ms.
    pub publish_time: i64,
    /// When this row was ingested, epoch ms.
    pub ingestion_time: i64,
    pub schema_version: ChSchemaVersion,
}
