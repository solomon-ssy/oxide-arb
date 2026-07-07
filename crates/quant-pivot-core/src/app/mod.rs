//! Application context — system composition root and lifecycle manager.

pub mod attribution_worker;
pub mod book_update_coalescer;
pub mod bootstrap;
pub mod build;
pub mod execution_dispatcher;
pub mod exit_monitor_worker;
pub mod fact_writer_tasks;
pub mod intent_service;
pub mod lifecycle;
pub mod periodic_services;
pub mod ports;
pub mod reconciliation_worker;
pub mod report_scheduler;
pub mod research_job;
pub mod research_job_worker;
pub mod runtime_tasks;
pub mod settlement_redeem_worker;
pub mod system_status_broadcaster;
pub mod task_id;
pub mod task_registry;
pub mod trade_tape_worker;
pub mod web_services;

mod bundles;

pub use bundles::*;

use crate::{
    execution::{DispatchWake, IntentLifecyclePublisher},
    governance::{KillSwitchHandle, RuntimeModeHandle},
    infra::schedule::ReportScheduleRunner,
    report::ReportLifecycleService,
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
    domain::{CoreEvent, CoreEventPublisher, ExecutionSubmitPort, PointInTimeDataSource},
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
    /// Shared `quant.intent` lifecycle fan-out (bootstrap singleton).
    pub intent_lifecycle: Arc<IntentLifecyclePublisher>,
    pub event_rx: Mutex<Option<Receiver<CoreEvent>>>,
    pub infra: InfraBundle,
    pub data: DataBundle,
    pub governance: GovernanceBundle,
    pub research: ResearchBundle,
    pub account: AccountBundle,
    pub report: ReportBundle,
    pub execution: ExecutionBundle,
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

    /// Lock-free operational kill-switch state for admission / exit hot paths.
    pub fn kill_switch_handle(&self) -> KillSwitchHandle {
        self.governance.kill_switch_handle.clone()
    }

    /// Content-addressed artifact store (`deploy.research.artifact_root`).
    pub fn artifact_store(&self) -> Arc<dyn ArtifactStore> {
        Arc::clone(&self.research.artifact_store)
    }

    /// Online feature pipeline (3.2): invoke per round with a frozen config snapshot.
    pub fn feature_pipeline(&self) -> &FeaturePipelineService {
        self.research.feature_pipeline.as_ref()
    }

    /// Online factor pipeline (3.3): invoke per round after feature vectors persist.
    pub fn factor_pipeline(&self) -> &FactorPipelineService {
        self.research.factor_pipeline.as_ref()
    }

    /// Online inference orchestrator (3.4): selection/features/factors → candidates.
    pub fn model_runner(&self) -> Arc<ModelRunner> {
        Arc::clone(&self.research.model_runner)
    }

    /// Report lifecycle service (04.2): trigger → build → transaction → publish.
    pub fn report_lifecycle(&self) -> Arc<ReportLifecycleService> {
        Arc::clone(&self.report.lifecycle)
    }

    /// Report schedule runner (04.3): cron/interval fire + ad-hoc enqueue.
    pub fn report_scheduler(&self) -> Arc<dyn ReportScheduleRunner> {
        Arc::clone(&self.report.scheduler)
    }

    /// Postgres persistence for factor definitions and values (3.3).
    pub fn factor_repo(&self) -> Arc<dyn FactorRepository> {
        Arc::clone(&self.research.factor_repo)
    }

    /// Live point-in-time source for online feature / report builders.
    pub fn live_pit(&self) -> &dyn PointInTimeDataSource {
        self.data.pit_source.as_ref()
    }

    /// Entry-execution dispatcher (05.4): the single intent → venue submit bridge.
    pub fn execution_dispatcher(&self) -> Arc<dyn ExecutionSubmitPort> {
        Arc::clone(&self.execution.dispatcher)
    }

    /// Approve→submit wake signal shared by the intent service (producer) and the
    /// `auto_execution` dispatcher worker (consumer).
    pub fn execution_wake(&self) -> DispatchWake {
        self.execution.dispatch_wake.clone()
    }
}
