//! Application context — system composition root and lifecycle manager.

pub mod bootstrap;
pub mod build;
pub mod fact_writer_tasks;
pub mod lifecycle;
pub mod periodic_services;
pub mod runtime_tasks;
pub mod task_id;
pub mod task_registry;
pub mod web_services;

mod bundles;

pub use bundles::*;

use crate::{
    governance::RuntimeModeHandle,
    infra::health_checker::HealthChecker,
    pipeline::data_quality::BookDataQualityService,
    runtime_config::{RuntimeConfigApplicator, RuntimeConfigStore},
    service::{catalog_readiness::CatalogReadiness, system_status_nudge::SystemStatusNudge},
};
use flume::Receiver;
use parking_lot::Mutex;
use quant_pivot_models::{
    config::DeployConfig,
    domain::{CoreEvent, CoreEventPublisher, PointInTimeDataSource, RuntimeControlPort},
};
use std::sync::Arc;
use tokio_util::sync::CancellationToken;

/// System composition root — Phase 0 bundles only.
pub struct AppContext {
    pub config: Arc<DeployConfig>,
    pub shutdown: CancellationToken,
    pub events: CoreEventPublisher,
    pub event_rx: Mutex<Option<Receiver<CoreEvent>>>,
    pub infra: InfraBundle,
    pub data: DataBundle,
    pub governance: GovernanceBundle,
    pub health_checker: Arc<HealthChecker>,
    pub runtime_control: Arc<dyn RuntimeControlPort>,
    pub catalog: Arc<CatalogReadiness>,
    pub data_quality: Arc<BookDataQualityService>,
    /// Live point-in-time source for Phase 3 feature/report builders. The
    /// historical (ClickHouse-backed) source lands in Phase 3.
    pub pit_source: Arc<dyn PointInTimeDataSource>,
    pub status_nudge: SystemStatusNudge,
}

impl AppContext {
    pub fn runtime_config(&self) -> Arc<RuntimeConfigStore> {
        Arc::clone(&self.governance.runtime_config)
    }

    pub fn applicator(&self) -> Arc<RuntimeConfigApplicator> {
        Arc::clone(&self.governance.applicator)
    }

    pub fn runtime_mode(&self) -> RuntimeModeHandle {
        self.governance.runtime_mode.clone()
    }
}
