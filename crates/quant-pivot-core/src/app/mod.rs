//! Application context — system composition root and lifecycle manager.

pub mod backtest;
pub mod bootstrap;
pub mod build;
pub mod fact_writer_tasks;
pub mod lifecycle;
pub mod model_training;
pub mod periodic_services;
pub mod runtime_tasks;
pub mod task_id;
pub mod task_registry;
pub mod training_dataset;
pub mod web_services;

mod bundles;

pub use bundles::*;

use crate::{
    governance::RuntimeModeHandle,
    runtime_config::{RuntimeConfigApplicator, RuntimeConfigStore},
    service::{
        factor_pipeline::FactorPipelineService, feature_pipeline::FeaturePipelineService,
        model_runner::ModelRunner,
    },
};
use flume::Receiver;
use parking_lot::Mutex;
use quant_pivot_models::{
    config::DeployConfig,
    domain::{CoreEvent, CoreEventPublisher, PointInTimeDataSource},
};
use quant_pivot_repository::traits::FactorRepository;
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
    pub account: AccountBundle,
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

    /// Online feature pipeline (3.2): invoke per round with a frozen config snapshot.
    pub const fn feature_pipeline(&self) -> &FeaturePipelineService {
        &self.research.feature_pipeline
    }

    /// Online factor pipeline (3.3): invoke per round after feature vectors persist.
    pub fn factor_pipeline(&self) -> &FactorPipelineService {
        self.research.factor_pipeline.as_ref()
    }

    /// Online inference orchestrator (3.4): selection/features/factors → candidates.
    pub fn model_runner(&self) -> Arc<ModelRunner> {
        Arc::clone(&self.research.model_runner)
    }

    /// Postgres persistence for factor definitions and values (3.3).
    pub fn factor_repo(&self) -> Arc<dyn FactorRepository> {
        Arc::clone(&self.research.factor_repo)
    }

    /// Live point-in-time source for online feature / report builders.
    pub fn live_pit(&self) -> &dyn PointInTimeDataSource {
        self.data.pit_source.as_ref()
    }
}
