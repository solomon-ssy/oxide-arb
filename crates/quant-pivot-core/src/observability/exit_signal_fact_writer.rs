//! Fire-and-forget `ClickHouse` sink for exit-signal evaluation audit events
//! (thesis-invalidation re-inference and opportunistic Sell).
//!
//! Postgres carries the authoritative exit ledger; this writer is a best-effort
//! analytics mirror that records every model-driven exit-signal evaluation —
//! including shadow evaluations that never submitted — so the shadow period can
//! be measured ex-post against realized hold outcomes.

use std::sync::Arc;

use quant_pivot_models::clickhouse::QuantExitSignalEvaluationEventRow;
use quant_pivot_storage::write::{AsyncWriter, AsyncWriterConfig};

use crate::observability::metrics_hub::MetricsHub;

/// Enqueues exit-signal evaluation audit rows into the analytics stream.
pub struct ExitSignalEvaluationEventWriter {
    writer: Arc<AsyncWriter<QuantExitSignalEvaluationEventRow>>,
}

impl ExitSignalEvaluationEventWriter {
    /// Build a writer over a pre-wired async fact stream.
    #[must_use]
    pub const fn new(writer: Arc<AsyncWriter<QuantExitSignalEvaluationEventRow>>) -> Self {
        Self { writer }
    }

    /// A self-contained drop-only writer with no `ClickHouse` sink, for tests and
    /// bootstrap paths that need the evaluator wired without the analytics plane.
    /// Rows are enqueued and never drained (best-effort); do not assert on them.
    #[must_use]
    pub fn drop_only(metrics: &MetricsHub) -> Self {
        let name = "quant_exit_signal_evaluation_event";
        let (writer, _worker) = AsyncWriter::new(
            AsyncWriterConfig::new(name),
            |_rows| Box::pin(async { Ok(()) }),
            metrics.async_writer_dropped.with_label_values(&[name]),
            metrics.async_writer_observability(name),
        );
        Self::new(Arc::new(writer))
    }

    /// Enqueue one exit-signal evaluation audit row.
    pub fn write(&self, row: QuantExitSignalEvaluationEventRow) {
        self.writer.write(row);
    }
}
