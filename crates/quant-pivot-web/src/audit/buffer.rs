//! Non-blocking bounded buffer in front of the operation-log writer.
//!
//! The [`OperationAudit`](crate::middleware) middleware enqueues finished rows
//! here with a non-blocking `try_send`; the [`spawn_operation_log_writer`] task
//! drains and batches them into Postgres. Enqueue never blocks the response: if
//! the channel is full (writer lagging / down), the row is dropped and counted.
//!
//! [`spawn_operation_log_writer`]: crate::audit::spawn_operation_log_writer

use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

use quant_pivot_models::domain::NewOperationLog;

/// Producer handle for the operation-log pipeline, shared via [`AppState`].
///
/// Cloneable and cheap to share: cloning duplicates the `flume` sender and the
/// shared drop counter.
///
/// [`AppState`]: crate::state::AppState
#[derive(Clone)]
pub struct OperationLogBuffer {
    tx: flume::Sender<NewOperationLog>,
    dropped: Arc<AtomicU64>,
}

impl OperationLogBuffer {
    /// Create a bounded buffer and its paired receiver.
    ///
    /// Pass the receiver to [`spawn_operation_log_writer`]; keep the buffer in
    /// [`AppState`] for the audit middleware.
    ///
    /// [`spawn_operation_log_writer`]: crate::audit::spawn_operation_log_writer
    /// [`AppState`]: crate::state::AppState
    #[must_use]
    pub fn new(capacity: usize) -> (Self, flume::Receiver<NewOperationLog>) {
        let (tx, rx) = flume::bounded(capacity);
        (
            Self {
                tx,
                dropped: Arc::new(AtomicU64::new(0)),
            },
            rx,
        )
    }

    /// Enqueue a finished operation-log row without blocking.
    ///
    /// On a full or closed channel the row is dropped and the drop counter is
    /// incremented — the audit log is best-effort and must never delay or fail a
    /// business response.
    pub fn try_enqueue(&self, log: NewOperationLog) {
        if self.tx.try_send(log).is_err() {
            let dropped = self.dropped.fetch_add(1, Ordering::Relaxed) + 1;
            tracing::warn!(
                dropped_total = dropped,
                "operation-log buffer full or closed; dropping audit row"
            );
        }
    }

    /// Total number of rows dropped because the channel was full or closed.
    #[must_use]
    pub fn dropped(&self) -> u64 {
        self.dropped.load(Ordering::Relaxed)
    }
}
