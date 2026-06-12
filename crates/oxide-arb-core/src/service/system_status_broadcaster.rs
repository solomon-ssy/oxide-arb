//! Periodic + nudged `SystemStatusChanged` emission for WebSocket dashboards.

use crate::{
    control::status::{SYSTEM_STATUS_BROADCAST_INTERVAL, SystemStatusNudge},
    service::system_status_publisher::SharedSystemStatusPublisher,
};
use tokio::time::MissedTickBehavior;
use tokio_util::sync::CancellationToken;

/// Runs the status broadcast loop until `shutdown` is cancelled.
pub struct SystemStatusBroadcaster {
    publisher: SharedSystemStatusPublisher,
    nudge: SystemStatusNudge,
    interval: std::time::Duration,
}

impl SystemStatusBroadcaster {
    /// Build a broadcaster with the default 5s cadence.
    #[must_use]
    pub const fn new(publisher: SharedSystemStatusPublisher, nudge: SystemStatusNudge) -> Self {
        Self {
            publisher,
            nudge,
            interval: SYSTEM_STATUS_BROADCAST_INTERVAL,
        }
    }

    /// Override the interval (primarily for tests).
    #[must_use]
    pub const fn with_interval(mut self, interval: std::time::Duration) -> Self {
        self.interval = interval;
        self
    }

    /// Run until `shutdown` is cancelled.
    pub async fn run(self, shutdown: CancellationToken) {
        let mut ticker = tokio::time::interval(self.interval);
        ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                () = shutdown.cancelled() => {
                    tracing::info!("system status broadcaster shutting down");
                    return;
                }
                _ = ticker.tick() => {
                    self.publisher.publish_now();
                }
                () = self.nudge.wait_notified() => {
                    self.publisher.publish_now();
                }
            }
        }
    }
}
