//! Fire-and-forget `ClickHouse` sink for final recommendation-attribution events.
//!
//! Attribution rows are authoritative in Postgres. This writer is a best-effort
//! analytics mirror and is only called after the PG transaction commits.

use std::sync::Arc;

use quant_pivot_models::clickhouse::QuantRecommendationAttributionEventRow;
use quant_pivot_storage::write::AsyncWriter;

/// Enqueues already-projected attribution-event rows into the analytics stream.
pub struct AttributionEventWriter {
    writer: Arc<AsyncWriter<QuantRecommendationAttributionEventRow>>,
}

impl AttributionEventWriter {
    /// Build a writer over a pre-wired async fact stream.
    #[must_use]
    pub const fn new(writer: Arc<AsyncWriter<QuantRecommendationAttributionEventRow>>) -> Self {
        Self { writer }
    }

    /// Enqueue one attribution-event row.
    pub fn write(&self, row: QuantRecommendationAttributionEventRow) {
        self.writer.write(row);
    }
}
