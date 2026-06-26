//! System-status WS broadcast helper (governed control-plane mutations).

use quant_pivot_models::domain::{CoreEvent, CoreEventPublisher, RuntimeControlPort, SystemStatus};
use std::sync::{Arc, OnceLock};

use super::runtime_control::QuantRuntimeControl;

/// Publishes [`CoreEvent::SystemStatusChanged`] after mode / kill-switch mutations.
///
/// Registered once during [`GovernanceBundle`](crate::app::bundles::GovernanceBundle)
/// assembly so both [`QuantRuntimeControl`](super::runtime_control::QuantRuntimeControl)
/// and [`KillSwitchControl`](super::kill_switch::KillSwitchControl) share the same
/// fan-out path without circular construction deps.
pub struct SystemStatusPublisher {
    events: CoreEventPublisher,
    control: OnceLock<Arc<QuantRuntimeControl>>,
}

impl SystemStatusPublisher {
    #[must_use]
    pub fn new(events: CoreEventPublisher) -> Arc<Self> {
        Arc::new(Self {
            events,
            control: OnceLock::new(),
        })
    }

    /// Wire the live status projector after [`QuantRuntimeControl`] is constructed.
    pub fn register(&self, control: Arc<QuantRuntimeControl>) {
        let _ = self.control.set(control);
    }

    /// Fan out the current operational snapshot to WS subscribers.
    pub fn publish(&self) {
        let Some(control) = self.control.get() else {
            tracing::warn!("system status publisher invoked before registration");
            return;
        };
        let status: SystemStatus = control.system_status();
        self.events.publish(CoreEvent::SystemStatusChanged(status));
    }
}
