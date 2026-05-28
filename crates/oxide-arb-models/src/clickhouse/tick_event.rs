use serde::{Deserialize, Serialize};

/// `ClickHouse` row for `tick_events` table.
///
/// Analytics fields use `f64` to match the `ClickHouse` `Float64` wire type used
/// by the storage DDL.
#[derive(Debug, Clone, clickhouse::Row, Serialize, Deserialize)]
pub struct TickEventRow {
    pub token_id: String,
    pub event_type: u8,
    pub best_bid: f64,
    pub best_ask: f64,
    pub bid_depth_usd: f64,
    pub ask_depth_usd: f64,
    pub spread_bps: u32,
    pub raw_payload: String,
    /// Epoch milliseconds (matches `DateTime64(3, 'UTC')` wire encoding).
    pub received_at: i64,
}
