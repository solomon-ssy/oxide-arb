//! Process-local wake signal from the intent-approval path to the
//! authorized-intent dispatcher worker.
//!
//! The **durable** work queue is Postgres: `Authorized` intent rows,
//! dequeued under a per-intent `SELECT … FOR UPDATE` claim. This signal only
//! collapses the approve→submit latency from the poll cadence to near-immediate;
//! it carries no work itself and is never authoritative. Losing a wake is
//! harmless — the dispatcher's periodic poll is the durable backstop (it also
//! picks up retried defers and crash-recovery work). For the same reason a pure
//! in-memory channel/queue is deliberately *not* used as the queue: a crash must
//! never lose an approved intent.

use std::sync::Arc;

use tokio::sync::Notify;

/// Cloneable wake handle shared between the approval producer and the dispatcher.
#[derive(Clone)]
pub struct DispatchWake {
    notify: Arc<Notify>,
}

impl DispatchWake {
    #[must_use]
    pub fn new() -> Self {
        Self {
            notify: Arc::new(Notify::new()),
        }
    }

    /// Signal that a newly authorized intent is ready to submit.
    ///
    /// Uses `notify_one`: if the dispatcher is not currently waiting, a single
    /// permit is buffered so the next [`Self::wait`] returns immediately (one
    /// coalesced wake is enough — the poll backstop catches any further work).
    pub fn wake(&self) {
        self.notify.notify_one();
    }

    /// Wait for the next wake (returns immediately if a permit is buffered).
    pub async fn wait(&self) {
        self.notify.notified().await;
    }
}

impl Default for DispatchWake {
    fn default() -> Self {
        Self::new()
    }
}
