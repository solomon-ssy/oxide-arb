use serde::{Deserialize, Serialize};

/// `ClickHouse` row for `calibration_snapshots` table.
#[derive(Debug, Clone, clickhouse::Row, Serialize, Deserialize)]
pub struct CalibrationSnapshotRow {
    pub category: String,
    pub price_zone: String,
    pub duration_bucket: String,
    pub total_count: u32,
    pub correct_count: u32,
    pub alpha_prior: f64,
    pub beta_prior: f64,
    pub posterior_mean: f64,
    pub snapshot_time: i64,
}
