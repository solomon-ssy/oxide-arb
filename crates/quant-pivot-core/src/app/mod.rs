//! Application context — system composition root and lifecycle manager.

pub mod book_update_coalescer;
pub mod bootstrap;
pub mod build;
pub mod capability_gate;
pub mod crypto_kline_ingest_worker;
pub mod crypto_live_ingest_worker;
pub mod crypto_rtds_ingest_worker;
pub mod domain_event_outbox_worker;
pub mod domain_source_supervisor;
pub mod entry_condition_evaluation_outbox_worker;
pub mod entry_condition_worker;
pub mod exchange_history_worker;
pub mod execution_dispatcher;
pub mod exit_monitor_worker;
pub mod fact_writer_tasks;
pub mod fresh_boot_orchestrator;
pub mod intent_service;
pub mod lifecycle;
pub mod outcome_reconciliation_worker;
pub mod periodic_services;
pub mod ports;
pub mod reconciliation_worker;
pub mod report_scheduler;
pub mod research_job;
pub mod research_job_worker;
pub mod research_readiness_worker;
pub mod runtime_control_sync;
pub mod runtime_tasks;
pub mod settlement_workers;
pub mod system_status_broadcaster;
pub mod task_id;
pub mod task_registry;
pub mod weather_backfill_worker;
pub mod weather_ingest_worker;
pub mod weather_public_ingest_worker;
pub mod web_services;

mod bundles;
mod clob_market_info_worker;

use std::sync::Arc;

pub use bundles::{
    AccountBundle, AccountBundleDeps, DataBundle, DataBundleDeps, ExecutionBundle,
    ExecutionBundleDeps, GovernanceBundle, GovernanceBundleDeps, InfraBundle, PgRepositories,
    ReportBundle, ReportBundleDeps, ResearchBundle, ResearchBundleDeps, RuntimeSnapshot,
};
use flume::Receiver;
use parking_lot::Mutex;
use quant_pivot_api::wallet::WalletTopology;
use quant_pivot_compute::ComputeExecutor;
use quant_pivot_models::{
    config::DeployConfig,
    domain::{
        ports::ExecutionSubmitPort,
        runtime::{CoreEvent, CoreEventPublisher},
    },
};
use quant_pivot_repository::traits::FactorRepository;
use quant_pivot_research::{artifact::ArtifactStore, pit::PointInTimeSnapshotSource};
use tokio_util::sync::CancellationToken;

use crate::{
    execution::{DispatchWake, IntentLifecyclePublisher},
    governance::RuntimeControlsHandle,
    report::{ReportCoordinator, ReportLifecycleService},
    runtime_config::{DecisionPolicyStore, PolicySnapshotApplicator},
    service::{
        factor_pipeline::FactorPipelineService, feature_pipeline::FeaturePipelineService,
        model_runner::ModelRunner,
    },
};

/// System composition root — bundles are the sole subsystem containers.
pub struct AppContext {
    pub config: Arc<DeployConfig>,
    /// Boot-verified venue wallet identity reused by settlement readiness.
    pub wallet: WalletTopology,
    pub compute: Arc<ComputeExecutor>,
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
    pub fn runtime_config(&self) -> Arc<DecisionPolicyStore> {
        Arc::clone(&self.governance.runtime_config)
    }

    pub fn applicator(&self) -> Arc<PolicySnapshotApplicator> {
        Arc::clone(&self.governance.applicator)
    }

    pub fn runtime_controls(&self) -> RuntimeControlsHandle {
        self.governance.runtime_controls.clone()
    }

    /// Content-addressed Local or S3-compatible artifact store.
    pub fn artifact_store(&self) -> Arc<dyn ArtifactStore> {
        Arc::clone(&self.research.artifact_store)
    }

    /// Online feature pipeline: invoke per round with a frozen config snapshot.
    pub fn feature_pipeline(&self) -> &FeaturePipelineService {
        self.research.feature_pipeline.as_ref()
    }

    /// Online factor pipeline: invoke per round after feature vectors persist.
    pub fn factor_pipeline(&self) -> &FactorPipelineService {
        self.research.factor_pipeline.as_ref()
    }

    /// Online inference orchestrator: selection/features/factors → candidates.
    pub fn model_runner(&self) -> Arc<ModelRunner> {
        Arc::clone(&self.research.model_runner)
    }

    /// Report lifecycle service: trigger → build → transaction → publish.
    pub fn report_lifecycle(&self) -> Arc<ReportLifecycleService> {
        Arc::clone(&self.report.lifecycle)
    }

    /// Durable `PostgreSQL` report schedule coordinator and global build worker.
    pub fn report_coordinator(&self) -> Arc<ReportCoordinator> {
        Arc::clone(&self.report.coordinator)
    }

    /// Postgres persistence for factor definitions and values.
    pub fn factor_repo(&self) -> Arc<dyn FactorRepository> {
        Arc::clone(&self.research.factor_repo)
    }

    /// Durable point-in-time source shared by serving and replay.
    pub fn pit_source(&self) -> &dyn PointInTimeSnapshotSource {
        self.data.pit_source.as_ref()
    }

    /// Entry-execution dispatcher: the single intent → venue submit bridge.
    pub fn execution_dispatcher(&self) -> Arc<dyn ExecutionSubmitPort> {
        Arc::clone(&self.execution.dispatcher)
    }

    /// Approve→submit wake signal shared by the intent service (producer) and the
    /// Authorized-intent dispatcher worker (consumer).
    pub fn execution_wake(&self) -> DispatchWake {
        self.execution.dispatch_wake.clone()
    }
}
