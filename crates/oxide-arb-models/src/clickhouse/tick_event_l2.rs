use serde::{Deserialize, Serialize};

/// `ClickHouse` row for `tick_events_l2` table — full L2 orderbook state.
///
/// Uses `Array(Decimal64(8))` columns to store price levels without JSON
/// parsing overhead. Supports both full snapshots and incremental deltas.
#[derive(Debug, Clone, clickhouse::Row, Serialize, Deserialize)]
pub struct TickEventL2Row {
    pub token_id: String,
    /// 1 = snapshot (full book replacement), 2 = delta (incremental update).
    pub event_type: u8,
    /// Bid prices ordered best-to-worst (descending).
    pub bid_prices: Vec<f64>,
    /// Bid sizes corresponding to `bid_prices`.
    pub bid_sizes: Vec<f64>,
    /// Ask prices ordered best-to-worst (ascending).
    pub ask_prices: Vec<f64>,
    /// Ask sizes corresponding to `ask_prices`.
    pub ask_sizes: Vec<f64>,
    /// For delta events: JSON-encoded changed level indices (nullable).
    pub changed_levels: Option<String>,
    /// Microsecond-precision receive timestamp (epoch millis).
    pub received_at: i64,
}
