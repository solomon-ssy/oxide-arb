//! Fire-and-forget `ClickHouse` sink for long-format feature events.
//!
//! This is a dumb, non-blocking sink: it owns the buffered [`AsyncWriter`] for
//! the `quant_feature_event` stream and nothing else. Row projection (which is
//! schema-governed and clock-injected) lives in the research feature writer, so
//! this type never needs the schema or the wall clock.

use std::sync::Arc;

use quant_pivot_models::clickhouse::QuantFeatureEventRow;
use quant_pivot_storage::write::AsyncWriter;

/// Enqueues already-projected feature-event rows into the analytics stream.
pub struct FeatureEventWriter {
    writer: Arc<AsyncWriter<QuantFeatureEventRow>>,
}

impl FeatureEventWriter {
    /// Build a writer over a pre-wired async fact stream.
    #[must_use]
    pub const fn new(writer: Arc<AsyncWriter<QuantFeatureEventRow>>) -> Self {
        Self { writer }
    }

    /// Enqueue one feature-event row (non-blocking).
    pub fn write(&self, row: QuantFeatureEventRow) {
        self.writer.write(row);
    }

    /// Enqueue a batch of already-projected feature-event rows.
    pub fn write_batch(&self, rows: Vec<QuantFeatureEventRow>) {
        for row in rows {
            self.writer.write(row);
        }
    }
}
