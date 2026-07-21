//! Non-blocking nudge for system-status broadcasts after first market-data event.

use std::sync::Arc;

use tokio::sync::{Notify, futures::Notified};

/// Lightweight wake handle shared by ingest and status publishers.
#[derive(Clone, Default)]
pub struct SystemStatusNudge {
    inner: Arc<Notify>,
}

impl SystemStatusNudge {
    pub fn nudge(&self) {
        self.inner.notify_waiters();
    }

    pub fn notified(&self) -> Notified<'_> {
        self.inner.notified()
    }
}
