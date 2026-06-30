//! Fire-and-forget `ClickHouse` sink for capital-allocation ledger events.

use std::sync::Arc;

use quant_pivot_models::clickhouse::QuantCapitalAllocationEventRow;
use quant_pivot_storage::write::AsyncWriter;

/// Enqueues already-projected capital-allocation rows into the analytics stream.
pub struct CapitalAllocationEventWriter {
    writer: Arc<AsyncWriter<QuantCapitalAllocationEventRow>>,
}

impl CapitalAllocationEventWriter {
    #[must_use]
    pub const fn new(writer: Arc<AsyncWriter<QuantCapitalAllocationEventRow>>) -> Self {
        Self { writer }
    }

    pub fn write(&self, row: QuantCapitalAllocationEventRow) {
        self.writer.write(row);
    }
}
