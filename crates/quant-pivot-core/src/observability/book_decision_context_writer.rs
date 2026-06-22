//! Fire-and-forget CH writer for immutable decision-time book contexts.

use crate::infra::async_writer::AsyncWriter;
use oxide_arb_models::clickhouse::BookDecisionContextRow;
use std::sync::Arc;

/// Non-blocking writer for `ClickHouse` `book_decision_contexts` rows.
pub struct BookDecisionContextWriter {
    writer: Arc<AsyncWriter<BookDecisionContextRow>>,
}

impl BookDecisionContextWriter {
    pub const fn new(writer: Arc<AsyncWriter<BookDecisionContextRow>>) -> Self {
        Self { writer }
    }

    pub fn write(&self, row: BookDecisionContextRow) -> bool {
        self.writer.write(row)
    }
}
