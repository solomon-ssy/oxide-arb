//! System-status WS broadcast helper (governed control-plane mutations).

use quant_pivot_models::domain::{
    BootstrapPort, CoreEvent, CoreEventPublisher, RuntimeControlPort, SystemStatus,
    SystemStatusView,
};
use std::sync::{Arc, OnceLock};

/// Publishes [`CoreEvent::SystemStatusChanged`] after mode / kill-switch mutations.
///
/// Registered once during [`GovernanceBundle`](crate::app::bundles::GovernanceBundle)
/// assembly so both runtime control and [`KillSwitchControl`](super::kill_switch::KillSwitchControl)
/// share the same fan-out path without circular construction deps.
pub struct SystemStatusPublisher {
    events: CoreEventPublisher,
    control: OnceLock<Arc<dyn RuntimeControlPort>>,
    bootstrap: OnceLock<Arc<dyn BootstrapPort>>,
}

impl SystemStatusPublisher {
    #[must_use]
    pub fn new(events: CoreEventPublisher) -> Arc<Self> {
        Arc::new(Self {
            events,
            control: OnceLock::new(),
            bootstrap: OnceLock::new(),
        })
    }

    /// Wire the live status projector after runtime control is constructed.
    pub fn register(&self, control: Arc<dyn RuntimeControlPort>) {
        let _ = self.control.set(control);
    }

    /// Wire capability projection after bootstrap and runtime control exist.
    pub fn register_bootstrap(&self, bootstrap: Arc<dyn BootstrapPort>) {
        let _ = self.bootstrap.set(bootstrap);
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
        let Some(bootstrap) = self.bootstrap.get() else {
            tracing::warn!("system status publisher invoked before bootstrap registration");
            return;
        };
        let capabilities = bootstrap.refresh_operational_capabilities(&status);
        self.events
            .publish(CoreEvent::SystemStatusChanged(Box::new(SystemStatusView {
                runtime: status,
                bootstrap: bootstrap.view(),
                capabilities,
            })));
    }
}
