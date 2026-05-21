use serde::{Deserialize, Serialize};

/// `ClickHouse` row for `tick_events` table.
///
/// Financial fields use `f64` as the `clickhouse` crate maps `Decimal64(8)` to `f64`
/// at the wire protocol level. Precision is enforced by the DDL column type.
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
    pub received_at: i64,
}
