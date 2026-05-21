use serde::{Deserialize, Serialize};

/// `ClickHouse` row for `signal_data` table.
#[derive(Debug, Clone, clickhouse::Row, Serialize, Deserialize)]
pub struct SignalDataRow {
    pub market_id: String,
    pub signal_name: String,
    pub signal_value: f64,
    pub metadata: String,
    pub recorded_at: i64,
}
