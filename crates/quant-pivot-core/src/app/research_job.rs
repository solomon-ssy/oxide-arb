//! Durable research-job engine + [`ResearchJobPort`] implementation.
//!
//! The engine is the shared spine between the HTTP enqueue path
//! ([`CoreResearchJobPort`]) and the [`ResearchJobWorker`](super::research_job_worker):
//! both hold a clone of [`ResearchJobEngine`] so cancellation tokens, the ledger
//! repository, the event bus, and the boot-epoch instance id are shared.

use std::sync::Arc;

use async_trait::async_trait;
use dashmap::DashMap;
use serde::Serialize;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use quant_pivot_error::{QuantError, QuantResult, research::ResearchError, storage::StorageError};
use quant_pivot_models::{
    domain::{
        BacktestJobParams, BuildTrainingDatasetRequest, CoreEvent, CoreEventPublisher,
        JobSubmitContext, MaterializationRunEvent, NewResearchJob, Paginated, ResearchJobError,
        ResearchJobInfo, ResearchJobListQuery, ResearchJobPort, ResearchJobView,
        RunBacktestRequest, TrainModelRequest,
    },
    enums::quant::{ResearchJobErrorCode, ResearchJobKind, ResearchJobStatus},
    types::{BacktestReportId, ModelVersionId, ResearchJobId, TrainingDatasetId},
};
use quant_pivot_repository::traits::ResearchJobRepository;

/// Shared, cheaply-cloneable handle wiring the job ledger, event bus, live
/// cancellation-token registry, and this process's boot epoch id.
#[derive(Clone)]
pub struct ResearchJobEngine {
    repo: Arc<dyn ResearchJobRepository>,
    events: CoreEventPublisher,
    cancels: Arc<DashMap<ResearchJobId, CancellationToken>>,
    instance_id: Arc<str>,
}

impl ResearchJobEngine {
    /// Wire a fresh engine for this process (mints a boot-epoch instance id).
    #[must_use]
    pub fn new(repo: Arc<dyn ResearchJobRepository>, events: CoreEventPublisher) -> Self {
        Self {
            repo,
            events,
            cancels: Arc::new(DashMap::new()),
            instance_id: Arc::from(Uuid::now_v7().to_string().as_str()),
        }
    }

    /// The shared ledger repository.
    #[must_use]
    pub fn repo(&self) -> &Arc<dyn ResearchJobRepository> {
        &self.repo
    }

    /// This process's lease-owner id (boot epoch).
    #[must_use]
    pub fn instance_id(&self) -> &str {
        &self.instance_id
    }

    /// Register a live cancellation token for a running job.
    pub fn register_cancel(&self, job_id: &ResearchJobId, token: CancellationToken) {
        self.cancels.insert(job_id.clone(), token);
    }

    /// Drop a job's cancellation token once it has left the running state.
    pub fn clear_cancel(&self, job_id: &ResearchJobId) {
        self.cancels.remove(job_id);
    }

    /// Signal an in-flight running job to stop cooperatively.
    fn signal_cancel(&self, job_id: &ResearchJobId) -> bool {
        self.cancels.get(job_id).is_some_and(|token| {
            token.cancel();
            true
        })
    }

    /// Publish a job-scoped progress event without a full ledger projection.
    pub fn publish_progress(
        &self,
        job_id: &ResearchJobId,
        kind: ResearchJobKind,
        result_ref: Option<Uuid>,
        status: ResearchJobStatus,
        phase: Option<String>,
        pct: Option<f64>,
    ) {
        let run_id = result_ref.map_or_else(|| job_id.to_string(), |uuid| uuid.to_string());
        self.events
            .publish(CoreEvent::MaterializationRun(MaterializationRunEvent::job(
                job_id.to_string(),
                run_id,
                kind.into(),
                status.into(),
                phase,
                pct,
            )));
    }

    /// Publish a `materialization.run_update` lifecycle/progress event.
    pub fn publish(&self, info: &ResearchJobInfo, phase: Option<String>, pct: Option<f64>) {
        let run_id = info
            .result_ref
            .map_or_else(|| info.job_id.to_string(), |uuid| uuid.to_string());
        self.events
            .publish(CoreEvent::MaterializationRun(MaterializationRunEvent::job(
                info.job_id.to_string(),
                run_id,
                info.kind.into(),
                info.status.into(),
                phase,
                pct,
            )));
    }
}

/// Core implementation of [`ResearchJobPort`] — the HTTP enqueue/cancel/retry surface.
pub struct CoreResearchJobPort {
    engine: ResearchJobEngine,
    max_recovery_attempts: i32,
}

impl CoreResearchJobPort {
    /// Wire the port from a shared engine + the recovery-attempt cap.
    #[must_use]
    pub const fn new(engine: ResearchJobEngine, max_recovery_attempts: i32) -> Self {
        Self {
            engine,
            max_recovery_attempts,
        }
    }

    fn new_job(
        &self,
        kind: ResearchJobKind,
        params: serde_json::Value,
        model_spec_id: Option<quant_pivot_models::types::ModelSpecId>,
        runtime_config_version_id: Option<quant_pivot_models::types::RuntimeConfigVersionId>,
        parent_job_id: Option<ResearchJobId>,
        ctx: &JobSubmitContext,
    ) -> NewResearchJob {
        NewResearchJob {
            job_id: ResearchJobId::from_v7(),
            kind,
            status: ResearchJobStatus::Queued,
            model_spec_id,
            runtime_config_version_id,
            params_json: params,
            requested_by: ctx.requested_by.clone(),
            acting_role: ctx.acting_role.clone(),
            parent_job_id,
            recovery_attempt: 0,
            max_recovery_attempts: self.max_recovery_attempts,
        }
    }

    async fn enqueue(&self, job: NewResearchJob) -> QuantResult<ResearchJobView> {
        let info = self
            .engine
            .repo
            .enqueue(job)
            .await
            .map_err(QuantError::from)?;
        self.engine.publish(&info, None, None);
        Ok(ResearchJobView::from(info))
    }
}

fn to_params<T: Serialize>(value: &T) -> QuantResult<serde_json::Value> {
    serde_json::to_value(value).map_err(|error| {
        QuantError::from(ResearchError::Serialization {
            detail: format!("research job params serialization failed: {error}"),
        })
    })
}

#[async_trait]
impl ResearchJobPort for CoreResearchJobPort {
    async fn enqueue_dataset_build(
        &self,
        mut request: BuildTrainingDatasetRequest,
        ctx: JobSubmitContext,
    ) -> QuantResult<ResearchJobView> {
        // Pre-assign the result id so re-runs after a crash reproduce the same
        // content-addressed dataset row (effectively-once via idempotent write).
        if request.training_dataset_id.is_none() {
            request.training_dataset_id = Some(TrainingDatasetId::from_v7());
        }
        let model_spec_id = Some(request.model_spec_id.clone());
        let runtime_config_version_id = Some(request.runtime_config_version_id.clone());
        let params = to_params(&request)?;
        let job = self.new_job(
            ResearchJobKind::DatasetBuild,
            params,
            model_spec_id,
            runtime_config_version_id,
            None,
            &ctx,
        );
        self.enqueue(job).await
    }

    async fn enqueue_model_train(
        &self,
        mut request: TrainModelRequest,
        ctx: JobSubmitContext,
    ) -> QuantResult<ResearchJobView> {
        if request.model_version_id.is_none() {
            request.model_version_id = Some(ModelVersionId::from_v7());
        }
        let model_spec_id = Some(request.model_spec_id.clone());
        let runtime_config_version_id = Some(request.runtime_config_version_id.clone());
        let params = to_params(&request)?;
        let job = self.new_job(
            ResearchJobKind::ModelTrain,
            params,
            model_spec_id,
            runtime_config_version_id,
            None,
            &ctx,
        );
        self.enqueue(job).await
    }

    async fn enqueue_backtest(
        &self,
        model_version_id: ModelVersionId,
        mut request: RunBacktestRequest,
        ctx: JobSubmitContext,
    ) -> QuantResult<ResearchJobView> {
        if request.backtest_report_id.is_none() {
            request.backtest_report_id = Some(BacktestReportId::from_v7());
        }
        let runtime_config_version_id = Some(request.runtime_config_version_id.clone());
        let params = to_params(&BacktestJobParams {
            model_version_id,
            request,
        })?;
        let job = self.new_job(
            ResearchJobKind::Backtest,
            params,
            None,
            runtime_config_version_id,
            None,
            &ctx,
        );
        self.enqueue(job).await
    }

    async fn list(&self, query: ResearchJobListQuery) -> QuantResult<Paginated<ResearchJobView>> {
        Ok(self
            .engine
            .repo
            .page(query)
            .await
            .map_err(QuantError::from)?
            .map(ResearchJobView::from))
    }

    async fn get(&self, job_id: &ResearchJobId) -> QuantResult<Option<ResearchJobView>> {
        Ok(self
            .engine
            .repo
            .find_by_id(job_id)
            .await
            .map_err(QuantError::from)?
            .map(ResearchJobView::from))
    }

    async fn cancel(
        &self,
        job_id: &ResearchJobId,
        reason: String,
        _ctx: JobSubmitContext,
    ) -> QuantResult<ResearchJobView> {
        let info = self
            .engine
            .repo
            .find_by_id(job_id)
            .await
            .map_err(QuantError::from)?
            .ok_or_else(|| {
                QuantError::from(StorageError::not_found(
                    quant_pivot_error::storage::entity::QUANT_RESEARCH_JOB,
                    job_id,
                ))
            })?;
        if info.status.is_terminal() {
            // Idempotent no-op: already terminal.
            return Ok(ResearchJobView::from(info));
        }
        let error = ResearchJobError::new(ResearchJobErrorCode::Cancelled, &reason);
        // Queued → terminal-cancel atomically; running → cooperative token signal.
        if self
            .engine
            .repo
            .cancel_if_queued(job_id, error)
            .await
            .map_err(QuantError::from)?
        {
            let refreshed = self.reload(job_id).await?;
            self.engine.publish(&refreshed, None, None);
            return Ok(ResearchJobView::from(refreshed));
        }
        self.engine.signal_cancel(job_id);
        // Return the current (still running) view; the worker flips it to
        // cancelled and the UI observes the change over WS / polling.
        Ok(ResearchJobView::from(info))
    }

    async fn retry(
        &self,
        job_id: &ResearchJobId,
        _reason: String,
        ctx: JobSubmitContext,
    ) -> QuantResult<ResearchJobView> {
        let info = self
            .engine
            .repo
            .find_by_id(job_id)
            .await
            .map_err(QuantError::from)?
            .ok_or_else(|| {
                QuantError::from(StorageError::not_found(
                    quant_pivot_error::storage::entity::QUANT_RESEARCH_JOB,
                    job_id,
                ))
            })?;
        if info.status.is_active() {
            return Err(QuantError::from(StorageError::state_conflict(
                quant_pivot_error::storage::entity::QUANT_RESEARCH_JOB,
                Some(job_id),
                "cannot retry a job that is still queued or running",
            )));
        }
        let job = self.new_job(
            info.kind,
            info.params_json.clone(),
            info.model_spec_id.clone(),
            info.runtime_config_version_id.clone(),
            Some(info.job_id.clone()),
            &ctx,
        );
        self.enqueue(job).await
    }
}

impl CoreResearchJobPort {
    async fn reload(&self, job_id: &ResearchJobId) -> QuantResult<ResearchJobInfo> {
        self.engine
            .repo
            .find_by_id(job_id)
            .await
            .map_err(QuantError::from)?
            .ok_or_else(|| {
                QuantError::from(StorageError::not_found(
                    quant_pivot_error::storage::entity::QUANT_RESEARCH_JOB,
                    job_id,
                ))
            })
    }
}
