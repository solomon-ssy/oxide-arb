//! Live [`SystemStatus`] assembly shared by the runtime control port and the
//! WebSocket status publisher.

use crate::control::mode_transition::CoreRuntimeControlDeps;
use chrono::Utc;
use oxide_arb_models::domain::{CatalogStatusPort, SystemStatus};
use oxide_arb_risk::traits::RiskMetrics;
use std::{
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::sync::{Notify, futures::Notified};

/// Default interval between periodic `SystemStatusChanged` pushes.
pub const SYSTEM_STATUS_BROADCAST_INTERVAL: Duration = Duration::from_secs(5);

/// Build the aggregate system status snapshot from live subsystem handles.
pub fn build_system_status(deps: &CoreRuntimeControlDeps, started_at: Instant) -> SystemStatus {
    let metrics = deps.metrics.as_ref();
    let snapshot = deps.risk_engine.snapshot(metrics);
    SystemStatus {
        execution_mode: deps.execution_mode.current(),
        breaker_state: snapshot.breaker_state,
        uptime_secs: started_at.elapsed().as_secs(),
        active_markets: u32::try_from(deps.market_registry.active_markets().len())
            .unwrap_or(u32::MAX),
        open_positions: u32::try_from(metrics.open_position_count()).unwrap_or(u32::MAX),
        pending_reservations: u32::try_from(deps.exposure.active_count_sync()).unwrap_or(u32::MAX),
        total_exposure: snapshot.total_exposure,
        daily_pnl: snapshot.daily_pnl,
        catalog: deps.catalog.catalog_state(),
        checked_at: Utc::now(),
    }
}

/// Shared nudge handle for immediate status broadcasts outside the periodic tick.
#[derive(Clone, Default)]
pub struct SystemStatusNudge {
    inner: Arc<Notify>,
}

impl SystemStatusNudge {
    /// Signal the broadcaster to publish a fresh snapshot now.
    pub fn nudge(&self) {
        self.inner.notify_waiters();
    }

    pub(crate) fn wait_notified(&self) -> Notified<'_> {
        self.inner.notified()
    }
}
