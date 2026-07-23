//! System-status WS broadcast helper (governed control-plane mutations).

use std::sync::{Arc, OnceLock};

use quant_pivot_models::domain::{
    api::SystemStatusView,
    governance::SystemStatus,
    ports::{RuntimeControlPort, SystemCapabilityPort},
    runtime::{CoreEvent, CoreEventPublisher},
};

/// Publishes [`CoreEvent::SystemStatusChanged`] after mode / kill-switch mutations.
///
/// Registered once during governance-bundle construction
/// assembly so both runtime control and [`KillSwitchControl`](super::kill_switch::KillSwitchControl)
/// share the same fan-out path without circular construction deps.
pub struct SystemStatusPublisher {
    events: CoreEventPublisher,
    control: OnceLock<Arc<dyn RuntimeControlPort>>,
    capabilities: OnceLock<Arc<dyn SystemCapabilityPort>>,
}

impl SystemStatusPublisher {
    #[must_use]
    pub fn new(events: CoreEventPublisher) -> Arc<Self> {
        Arc::new(Self {
            events,
            control: OnceLock::new(),
            capabilities: OnceLock::new(),
        })
    }

    /// Wire the live status projector after runtime control is constructed.
    pub fn register(&self, control: Arc<dyn RuntimeControlPort>) {
        let _ = self.control.set(control);
    }

    /// Wire capability projection after runtime control exists.
    pub fn register_capabilities(&self, capabilities: Arc<dyn SystemCapabilityPort>) {
        let _ = self.capabilities.set(capabilities);
    }

    /// Latest projected snapshot without publishing (dedup / diagnostics).
    #[must_use]
    pub fn peek(&self) -> Option<SystemStatus> {
        self.control.get().map(|control| control.system_status())
    }

    /// Fan out the current operational snapshot to WS subscribers.
    pub fn publish(&self) {
        let Some(status) = self.peek() else {
            tracing::warn!("system status publisher invoked before registration");
            return;
        };
        let Some(capability_service) = self.capabilities.get() else {
            tracing::warn!("system status publisher invoked before capability registration");
            return;
        };
        let capabilities = capability_service.refresh_operational_capabilities(&status);
        self.events
            .publish(CoreEvent::SystemStatusChanged(Box::new(SystemStatusView {
                runtime: status,
                capabilities,
            })));
    }
}
