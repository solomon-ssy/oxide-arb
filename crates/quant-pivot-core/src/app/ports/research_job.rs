//! Core implementation of [`ResearchJobPort`] — the HTTP enqueue/cancel/retry surface.

use async_trait::async_trait;
use serde::Serialize;

use quant_pivot_error::{
    QuantError, QuantResult,
    research::ResearchError,
    storage::{StorageError, entity},
};
use quant_pivot_models::{
    domain::{
        BacktestJobParams, BiasTableFitJobParams, BuildTrainingDatasetRequest,
        CpcvBacktestJobParams, FitBiasTableRequest, FitModelCalibratorRequest,
        FitTradePolicyRequest, JobSubmitContext, ModelCalibrationFitJobParams, ModelTrainJobParams,
        NewResearchJob, Paginated, ResearchJobError, ResearchJobInfo, ResearchJobListQuery,
        ResearchJobPort, ResearchJobView, RunBacktestRequest, RunCpcvBacktestRequest,
        TradePolicyFitJobParams, TradePolicyValidationJobParams, TrainModelRequest,
    },
    enums::quant::{ResearchJobErrorCode, ResearchJobKind, ResearchJobStatus},
    types::{
        BacktestPathSetId, BacktestReportId, DecisionPolicySnapshotId, ModelSpecId, ModelVersionId,
        ResearchJobId, TrainingDatasetId,
    },
};

use crate::app::research_job::ResearchJobEngine;
use quant_pivot_repository::traits::{PolicyRepository, TrainingDatasetRepository};
use std::sync::Arc;

/// Core implementation of [`ResearchJobPort`] — the HTTP enqueue/cancel/retry surface.
pub struct CoreResearchJobPort {
    engine: ResearchJobEngine,
    training_datasets: Arc<dyn TrainingDatasetRepository>,
    runtime_configs: Arc<dyn PolicyRepository>,
    max_recovery_attempts: i32,
}

impl CoreResearchJobPort {
    /// Wire the port from a shared engine + the recovery-attempt cap.
    #[must_use]
    pub const fn new(
        engine: ResearchJobEngine,
        training_datasets: Arc<dyn TrainingDatasetRepository>,
        runtime_configs: Arc<dyn PolicyRepository>,
        max_recovery_attempts: i32,
    ) -> Self {
        Self {
            engine,
            training_datasets,
            runtime_configs,
            max_recovery_attempts,
        }
    }

    fn new_job(
        &self,
        kind: ResearchJobKind,
        params: serde_json::Value,
        model_spec_id: Option<ModelSpecId>,
        decision_policy_snapshot_id: Option<DecisionPolicySnapshotId>,
        parent_job_id: Option<ResearchJobId>,
        ctx: &JobSubmitContext,
    ) -> NewResearchJob {
        NewResearchJob {
            job_id: ResearchJobId::from_v7(),
            kind,
            status: ResearchJobStatus::Queued,
            model_spec_id,
            decision_policy_snapshot_id,
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
            .repo()
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
        let decision_policy_snapshot_id = Some(request.decision_policy_snapshot_id.clone());
        let params = to_params(&request)?;
        let job = self.new_job(
            ResearchJobKind::DatasetBuild,
            params,
            model_spec_id,
            decision_policy_snapshot_id,
            None,
            &ctx,
        );
        self.enqueue(job).await
    }

    async fn enqueue_model_train(
        &self,
        request: TrainModelRequest,
        ctx: JobSubmitContext,
    ) -> QuantResult<ResearchJobView> {
        let dataset = self
            .training_datasets
            .find_by_id(&request.training_dataset_id)
            .await
            .map_err(QuantError::from)?
            .ok_or_else(|| StorageError::NotFound {
                entity: "training_dataset",
                id: request.training_dataset_id.to_string(),
            })?;
        let model_spec_id = Some(dataset.model_spec_id.clone());
        let decision_policy_snapshot_id = Some(dataset.decision_policy_snapshot_id.clone());
        let params = to_params(&ModelTrainJobParams {
            model_version_id: ModelVersionId::from_v7(),
            request,
        })?;
        let job = self.new_job(
            ResearchJobKind::ModelTrain,
            params,
            model_spec_id,
            decision_policy_snapshot_id,
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
        let decision_policy_snapshot_id = Some(request.decision_policy_snapshot_id.clone());
        let params = to_params(&BacktestJobParams {
            model_version_id,
            request,
        })?;
        let job = self.new_job(
            ResearchJobKind::Backtest,
            params,
            None,
            decision_policy_snapshot_id,
            None,
            &ctx,
        );
        self.enqueue(job).await
    }

    async fn enqueue_cpcv_backtest(
        &self,
        model_version_id: ModelVersionId,
        mut request: RunCpcvBacktestRequest,
        ctx: JobSubmitContext,
    ) -> QuantResult<ResearchJobView> {
        if request.path_set_id.is_none() {
            request.path_set_id = Some(BacktestPathSetId::from_v7());
        }
        let decision_policy_snapshot_id = Some(request.decision_policy_snapshot_id.clone());
        let params = to_params(&CpcvBacktestJobParams {
            model_version_id,
            request,
        })?;
        let job = self.new_job(
            ResearchJobKind::CpcvBacktest,
            params,
            None,
            decision_policy_snapshot_id,
            None,
            &ctx,
        );
        self.enqueue(job).await
    }

    async fn enqueue_bias_table_fit(
        &self,
        request: FitBiasTableRequest,
        decision_policy_snapshot_id: DecisionPolicySnapshotId,
        ctx: JobSubmitContext,
    ) -> QuantResult<ResearchJobView> {
        let params = to_params(&BiasTableFitJobParams {
            request,
            decision_policy_snapshot_id: decision_policy_snapshot_id.clone(),
        })?;
        let job = self.new_job(
            ResearchJobKind::BiasTableFit,
            params,
            None,
            Some(decision_policy_snapshot_id),
            None,
            &ctx,
        );
        self.enqueue(job).await
    }

    async fn enqueue_model_calibration_fit(
        &self,
        request: FitModelCalibratorRequest,
        decision_policy_snapshot_id: DecisionPolicySnapshotId,
        ctx: JobSubmitContext,
    ) -> QuantResult<ResearchJobView> {
        let params = to_params(&ModelCalibrationFitJobParams {
            request,
            decision_policy_snapshot_id: decision_policy_snapshot_id.clone(),
        })?;
        let job = self.new_job(
            ResearchJobKind::ModelCalibrationFit,
            params,
            None,
            Some(decision_policy_snapshot_id),
            None,
            &ctx,
        );
        self.enqueue(job).await
    }

    async fn enqueue_trade_policy_fit(
        &self,
        request: FitTradePolicyRequest,
        ctx: JobSubmitContext,
    ) -> QuantResult<ResearchJobView> {
        let runtime_config = self
            .runtime_configs
            .load_active_at(request.selection.pit_cutoff)
            .await
            .map_err(QuantError::from)?
            .ok_or_else(|| StorageError::NotFound {
                entity: "decision_policy_snapshot",
                id: format!("active_at: {}", request.selection.pit_cutoff),
            })?;
        let decision_policy_snapshot_id = Some(runtime_config.decision_policy_snapshot_id);
        let params = to_params(&TradePolicyFitJobParams {
            training_dataset_id: TrainingDatasetId::from_v7(),
            request,
        })?;
        let job = self.new_job(
            ResearchJobKind::TradePolicyFit,
            params,
            None,
            decision_policy_snapshot_id,
            None,
            &ctx,
        );
        self.enqueue(job).await
    }

    async fn enqueue_trade_policy_validation(
        &self,
        request: TradePolicyValidationJobParams,
        ctx: JobSubmitContext,
    ) -> QuantResult<ResearchJobView> {
        let params = to_params(&request)?;
        let job = self.new_job(
            ResearchJobKind::TradePolicyValidation,
            params,
            None,
            None,
            None,
            &ctx,
        );
        self.enqueue(job).await
    }

    async fn list(&self, query: ResearchJobListQuery) -> QuantResult<Paginated<ResearchJobView>> {
        Ok(self
            .engine
            .repo()
            .page(query)
            .await
            .map_err(QuantError::from)?
            .map(ResearchJobView::from))
    }

    async fn get(&self, job_id: &ResearchJobId) -> QuantResult<Option<ResearchJobView>> {
        Ok(self
            .engine
            .repo()
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
            .repo()
            .find_by_id(job_id)
            .await
            .map_err(QuantError::from)?
            .ok_or_else(|| {
                QuantError::from(StorageError::not_found(entity::QUANT_RESEARCH_JOB, job_id))
            })?;
        if info.status.is_terminal() {
            // Idempotent no-op: already terminal.
            return Ok(ResearchJobView::from(info));
        }
        if info.kind == ResearchJobKind::FeatureParity && info.status == ResearchJobStatus::Queued {
            return Err(QuantError::from(StorageError::state_conflict(
                entity::QUANT_RESEARCH_JOB,
                Some(job_id),
                "queued feature parity jobs are safety-critical and cannot be cancelled",
            )));
        }
        let error = ResearchJobError::new(ResearchJobErrorCode::Cancelled, &reason);
        // Queued → terminal-cancel atomically; running → cooperative token signal.
        if self
            .engine
            .repo()
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
            .repo()
            .find_by_id(job_id)
            .await
            .map_err(QuantError::from)?
            .ok_or_else(|| {
                QuantError::from(StorageError::not_found(entity::QUANT_RESEARCH_JOB, job_id))
            })?;
        if info.status.is_active() {
            return Err(QuantError::from(StorageError::state_conflict(
                entity::QUANT_RESEARCH_JOB,
                Some(job_id),
                "cannot retry a job that is still queued or running",
            )));
        }
        let job = self.new_job(
            info.kind,
            info.params_json.clone(),
            info.model_spec_id.clone(),
            info.decision_policy_snapshot_id.clone(),
            Some(info.job_id.clone()),
            &ctx,
        );
        self.enqueue(job).await
    }
}

impl CoreResearchJobPort {
    async fn reload(&self, job_id: &ResearchJobId) -> QuantResult<ResearchJobInfo> {
        self.engine
            .repo()
            .find_by_id(job_id)
            .await
            .map_err(QuantError::from)?
            .ok_or_else(|| {
                QuantError::from(StorageError::not_found(entity::QUANT_RESEARCH_JOB, job_id))
            })
    }
}
