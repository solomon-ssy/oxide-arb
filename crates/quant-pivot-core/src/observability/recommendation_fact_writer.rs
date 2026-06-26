//! Fire-and-forget `ClickHouse` sink for published recommendation events.
//!
//! Report rows are authoritative in Postgres. This writer is a best-effort
//! analytics mirror and is only called after the report transaction commits.

use std::sync::Arc;

use quant_pivot_models::clickhouse::QuantRecommendationEventRow;
use quant_pivot_storage::write::AsyncWriter;

/// Enqueues already-projected recommendation-event rows into the analytics stream.
pub struct RecommendationEventWriter {
    writer: Arc<AsyncWriter<QuantRecommendationEventRow>>,
}

impl RecommendationEventWriter {
    /// Build a writer over a pre-wired async fact stream.
    #[must_use]
    pub const fn new(writer: Arc<AsyncWriter<QuantRecommendationEventRow>>) -> Self {
        Self { writer }
    }

    /// Enqueue one recommendation-event row.
    pub fn write(&self, row: QuantRecommendationEventRow) {
        self.writer.write(row);
    }

    /// Enqueue a batch of recommendation-event rows.
    pub fn write_batch(&self, rows: Vec<QuantRecommendationEventRow>) {
        for row in rows {
            self.writer.write(row);
        }
    }
}
