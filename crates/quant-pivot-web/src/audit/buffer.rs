//! Non-blocking producer handle in front of the operation-log writer.
//!
//! The [`OperationAudit`](crate::middleware) middleware enqueues finished rows
//! here; enqueue is a non-blocking [`AsyncWriter::write`] that drops (and counts
//! via Prometheus) on a full/closed channel, so audit persistence can never
//! delay or fail a business response. The paired `AsyncWriter` worker — built
//! and spawned by the app runtime — batches rows into Postgres.

use std::sync::Arc;

use quant_pivot_models::domain::NewOperationLog;
use quant_pivot_storage::write::AsyncWriter;

/// Producer handle for the operation-log pipeline, shared via [`AppState`].
///
/// Cloneable and cheap to share — it holds an `Arc` to the shared writer.
///
/// [`AppState`]: crate::state::AppState
#[derive(Clone)]
pub struct OperationLogBuffer {
    writer: Arc<AsyncWriter<NewOperationLog>>,
}

impl OperationLogBuffer {
    /// Wrap a shared operation-log [`AsyncWriter`] handle.
    #[must_use]
    pub const fn new(writer: Arc<AsyncWriter<NewOperationLog>>) -> Self {
        Self { writer }
    }

    /// Enqueue a finished operation-log row without blocking.
    ///
    /// On a full or closed channel the row is dropped and counted by the
    /// writer's Prometheus counter — the audit log is best-effort and must never
    /// delay or fail a business response.
    pub fn try_enqueue(&self, log: NewOperationLog) {
        let _ = self.writer.write(log);
    }
}
