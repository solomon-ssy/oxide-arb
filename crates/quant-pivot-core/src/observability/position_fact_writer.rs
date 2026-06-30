//! Fire-and-forget `ClickHouse` sink for position-lot ledger events.

use std::sync::Arc;

use quant_pivot_models::clickhouse::QuantPositionEventRow;
use quant_pivot_storage::write::AsyncWriter;

/// Enqueues already-projected position-lot rows into the analytics stream.
pub struct PositionEventWriter {
    writer: Arc<AsyncWriter<QuantPositionEventRow>>,
}

impl PositionEventWriter {
    #[must_use]
    pub const fn new(writer: Arc<AsyncWriter<QuantPositionEventRow>>) -> Self {
        Self { writer }
    }

    pub fn write(&self, row: QuantPositionEventRow) {
        self.writer.write(row);
    }
}
