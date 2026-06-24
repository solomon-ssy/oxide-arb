//! Fire-and-forget `ClickHouse` sink for pre-portfolio signal-candidate events.
//!
//! Like the feature- and factor-event sinks, this is a dumb, non-blocking writer:
//! it owns the buffered [`AsyncWriter`] for the `quant_signal_candidate_event`
//! stream and nothing else. Row projection (entry/target/stop derivation,
//! run-tagging) lives in the research signal projection, so this type never needs
//! the runtime or the clock.

use std::sync::Arc;

use quant_pivot_models::clickhouse::QuantSignalCandidateEventRow;
use quant_pivot_storage::write::AsyncWriter;

/// Enqueues already-projected signal-candidate-event rows into the analytics stream.
pub struct SignalCandidateEventWriter {
    writer: Arc<AsyncWriter<QuantSignalCandidateEventRow>>,
}

impl SignalCandidateEventWriter {
    /// Build a writer over a pre-wired async fact stream.
    #[must_use]
    pub const fn new(writer: Arc<AsyncWriter<QuantSignalCandidateEventRow>>) -> Self {
        Self { writer }
    }

    /// Enqueue one signal-candidate-event row (non-blocking).
    pub fn write(&self, row: QuantSignalCandidateEventRow) {
        self.writer.write(row);
    }

    /// Enqueue a batch of already-projected signal-candidate-event rows.
    pub fn write_batch(&self, rows: Vec<QuantSignalCandidateEventRow>) {
        for row in rows {
            self.writer.write(row);
        }
    }
}
