//! Process-local wake hint for the durable settlement discovery poll.

use std::sync::Arc;

use tokio::sync::Notify;

/// Coalesced wake permit; `PostgreSQL` remains the only work source of truth.
#[derive(Clone, Default)]
pub struct SettlementDiscoveryWake {
    notify: Arc<Notify>,
}

impl SettlementDiscoveryWake {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    pub fn wake(&self) {
        self.notify.notify_one();
    }

    pub async fn wait(&self) {
        self.notify.notified().await;
    }
}
