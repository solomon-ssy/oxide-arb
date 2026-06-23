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
    runtime_config::{RuntimeConfigApplicator, RuntimeConfigStore},
};
use flume::Receiver;
use parking_lot::Mutex;
use quant_pivot_models::{
    config::DeployConfig,
    domain::{CoreEvent, CoreEventPublisher},
};
use quant_pivot_research::artifact::ArtifactStore;
use std::sync::Arc;
use tokio_util::sync::CancellationToken;

/// System composition root — bundles are the sole subsystem containers.
pub struct AppContext {
    pub config: Arc<DeployConfig>,
    pub shutdown: CancellationToken,
    pub events: CoreEventPublisher,
    pub event_rx: Mutex<Option<Receiver<CoreEvent>>>,
    pub infra: InfraBundle,
    pub data: DataBundle,
    pub governance: GovernanceBundle,
    pub research: ResearchBundle,
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

    /// Content-addressed artifact store (`deploy.research.artifact_root`).
    pub fn artifact_store(&self) -> Arc<dyn ArtifactStore> {
        Arc::clone(&self.research.artifact_store)
    }
}
