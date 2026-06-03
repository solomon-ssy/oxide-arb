use serde::{Deserialize, Serialize};

use crate::{
    clickhouse::{ChDecimal64, ChProbability, ChSchemaVersion},
    enums::clickhouse::{ChDurationBucket, ChFactSource, ChMarketCategory, ChPriceZone},
};

/// `ClickHouse` row for `calibration_snapshots` table.
#[derive(Debug, Clone, clickhouse::Row, Serialize, Deserialize)]
pub struct CalibrationSnapshotRow {
    pub category: ChMarketCategory,
    pub price_zone: ChPriceZone,
    pub duration_bucket: ChDurationBucket,
    pub total_count: u32,
    pub correct_count: u32,
    pub alpha_prior: ChDecimal64,
    pub beta_prior: ChDecimal64,
    pub posterior_mean: Option<ChProbability>,
    pub fallback_tier: u8,
    pub config_hash: String,
    pub snapshot_hash: String,
    /// Business event time in epoch milliseconds.
    pub event_time: i64,
    /// Writer ingestion time in epoch milliseconds.
    pub ingestion_time: i64,
    /// Stable tie-breaker for same event/ingestion time rows.
    pub sequence: u64,
    pub source: ChFactSource,
    pub schema_version: ChSchemaVersion,
}
