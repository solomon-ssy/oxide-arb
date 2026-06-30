//! Fire-and-forget `ClickHouse` sink for execution lifecycle events.

use std::sync::Arc;

use quant_pivot_models::clickhouse::QuantExecutionEventRow;
use quant_pivot_storage::write::AsyncWriter;

/// Enqueues already-projected execution-event rows into the analytics stream.
pub struct ExecutionEventWriter {
    writer: Arc<AsyncWriter<QuantExecutionEventRow>>,
}

impl ExecutionEventWriter {
    #[must_use]
    pub const fn new(writer: Arc<AsyncWriter<QuantExecutionEventRow>>) -> Self {
        Self { writer }
    }

    pub fn write(&self, row: QuantExecutionEventRow) {
        self.writer.write(row);
    }
}
