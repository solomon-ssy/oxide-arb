//! Publishes [`CoreEvent::SystemStatusChanged`] snapshots onto the real-time bus.

use crate::control::{mode_transition::CoreRuntimeControlDeps, status::build_system_status};
use oxide_arb_models::domain::{CoreEvent, CoreEventPublisher};
use std::{sync::Arc, time::Instant};

/// Builds a live [`SystemStatus`](oxide_arb_models::domain::SystemStatus) and
/// emits it as `SystemStatusChanged` — the single outbound path for dashboard
/// status pushes.
pub struct SystemStatusPublisher {
    deps: CoreRuntimeControlDeps,
    events: CoreEventPublisher,
    started_at: Instant,
}

impl SystemStatusPublisher {
    /// Create a publisher bound to the live control dependencies and boot instant.
    #[must_use]
    pub const fn new(
        deps: CoreRuntimeControlDeps,
        events: CoreEventPublisher,
        started_at: Instant,
    ) -> Self {
        Self {
            deps,
            events,
            started_at,
        }
    }

    /// Snapshot current status and publish (non-blocking, drop-on-full).
    pub fn publish_now(&self) {
        let status = build_system_status(&self.deps, self.started_at);
        self.events.publish(CoreEvent::SystemStatusChanged(status));
    }
}

/// Cheap cloneable handle to a shared publisher.
pub type SharedSystemStatusPublisher = Arc<SystemStatusPublisher>;
