//! Fire-and-forget `ClickHouse` sink for long-format factor events.
//!
//! Like the feature-event sink, this is a dumb, non-blocking writer: it owns the
//! buffered [`AsyncWriter`] for the `quant_factor_event` stream and nothing else.
//! Row projection (schema-governed, run-tagged, clock-injected) lives in the
//! research factor writer, so this type never needs the registry or the clock.

use std::sync::Arc;

use quant_pivot_models::clickhouse::QuantFactorEventRow;
use quant_pivot_storage::write::AsyncWriter;

/// Enqueues already-projected factor-event rows into the analytics stream.
pub struct FactorEventWriter {
    writer: Arc<AsyncWriter<QuantFactorEventRow>>,
}

impl FactorEventWriter {
    /// Build a writer over a pre-wired async fact stream.
    #[must_use]
    pub const fn new(writer: Arc<AsyncWriter<QuantFactorEventRow>>) -> Self {
        Self { writer }
    }

    /// Enqueue one factor-event row (non-blocking).
    pub fn write(&self, row: QuantFactorEventRow) {
        self.writer.write(row);
    }

    /// Enqueue a batch of already-projected factor-event rows.
    pub fn write_batch(&self, rows: Vec<QuantFactorEventRow>) {
        for row in rows {
            self.writer.write(row);
        }
    }
}
