//! Application context — system composition root and lifecycle manager.
//!
//! `AppContext` owns all subsystem instances and orchestrates startup,
//! run, and graceful shutdown. The struct is decomposed into four bundles
//! to avoid a 40+ field god struct.

pub mod bootstrap;
pub mod build;
pub mod lifecycle;
pub mod periodic_services;
pub mod task_id;
pub mod task_registry;

mod bundles;
mod runtime_tasks;
mod web_services;

pub use bundles::*;

use crate::{
    app::task_registry::PendingTaskQueue,
    bridge::execution_mode::ExecutionModeHandle,
    control::status::SystemStatusNudge,
    infra::health_checker::HealthChecker,
    runtime_config::{RuntimeConfigApplicator, RuntimeConfigStore},
    service::{
        detection_readiness::DetectionReadiness, runtime_lifecycle::LatestUnhealthySubsystems,
    },
    trade_integrity::TradeIntegrityStore,
};
use flume::Receiver;
use oxide_arb_models::{
    config::DeployConfig,
    domain::{CoreEvent, CoreEventPublisher},
};
use parking_lot::Mutex;
use std::{sync::Arc, time::Instant};
use tokio_util::sync::CancellationToken;

/// Max trades claimed per post-trade relay drain iteration.
pub(crate) const POST_TRADE_RELAY_BATCH_SIZE: u64 = 128;

/// System composition root — owns all subsystems.
///
/// Decomposed into four bundles (`InfraBundle`, `DataBundle`,
/// `RiskBundle`, `TradingBundle`) to avoid a 40+ flat-field struct.
pub struct AppContext {
    /// Deploy configuration (restart to apply).
    pub config: Arc<DeployConfig>,
    /// Active runtime-config snapshot (hot-reloadable via the applicator).
    pub runtime_config: Arc<RuntimeConfigStore>,
    /// Activation propagation surface (also the web `RuntimeConfigPort`).
    pub applicator: Arc<RuntimeConfigApplicator>,
    /// Atomically swappable live execution mode shared by every hot-path reader.
    pub execution_mode: ExecutionModeHandle,
    /// Non-blocking producer handle for the real-time event bus.
    pub events: CoreEventPublisher,
    /// Receiver consumed once by the WebSocket broadcaster task.
    pub event_rx: Mutex<Option<Receiver<CoreEvent>>>,
    pub infra: InfraBundle,
    pub data: DataBundle,
    pub risk: RiskBundle,
    pub trading: TradingBundle,
    pub control: ControlFactorBundle,
    pub settlement: SettlementBundle,
    pub runtime: RuntimeChannels,
    /// Durable-trade integrity store (reservation rehydrate + blocking snapshot).
    pub trade_integrity: Arc<TradeIntegrityStore>,
    pub shutdown: CancellationToken,
    pub pending_tasks: PendingTaskQueue,
    /// Process boot instant shared by uptime reporting and status broadcasts.
    pub started_at: Instant,
    /// Nudge handle for immediate system-status WebSocket pushes.
    pub system_status_nudge: SystemStatusNudge,
    /// Hot-path detection gate mirrored from operational phase on status publish.
    pub detection_readiness: Arc<DetectionReadiness>,
    /// Shared health probe runner (single instance for control + periodic task).
    pub health_checker: Arc<HealthChecker>,
    /// Latest failing subsystem names from the health tick (feeds lifecycle).
    pub unhealthy_subsystems: Arc<LatestUnhealthySubsystems>,
}
