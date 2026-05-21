use serde::{Deserialize, Serialize};

/// `ClickHouse` row for `book_snapshots` table.
#[derive(Debug, Clone, clickhouse::Row, Serialize, Deserialize)]
pub struct BookSnapshotRow {
    pub token_id: String,
    pub snapshot_time: i64,
    pub bids: String,
    pub asks: String,
    pub bid_depth_usd: f64,
    pub ask_depth_usd: f64,
    pub mid_price: f64,
    pub spread_bps: u32,
    pub levels_count: u16,
}
