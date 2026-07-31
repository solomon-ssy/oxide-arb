//! Admin port for the durable async research-job ledger.
//!
//! The HTTP layer enqueues long-running research tasks (dataset build / model
//! train / backtest) through this port and never blocks on execution: a
//! `ResearchJobWorker` in `quant-pivot-core` leases and runs them off the HTTP
//! hot path, streaming progress over WebSocket and the ledger.

use async_trait::async_trait;
use quant_pivot_error::QuantResult;

use crate::{
    domain::{
        api::{
            BuildTrainingDatasetRequest, FitBiasTableRequest, FitModelCalibratorRequest,
            FitTradePolicyRequest, ResearchJobListQuery, ResearchJobView, RunBacktestRequest,
            RunCpcvBacktestRequest, TradePolicyValidationJobParams, TrainModelRequest,
        },
        pagination::Paginated,
        ports::GovernanceActor,
    },
    types::{DecisionPolicySnapshotId, ModelVersionId, ResearchJobId},
};

/// Governance/attribution context captured when a job is submitted or mutated.
#[derive(Debug, Clone)]
pub struct JobSubmitContext {
    /// The `X-Acting-Role` the operator submitted under.
    pub acting_role: String,
    /// The authenticated operator (user id / subject), when known.
    pub requested_by: Option<String>,
}

/// Dependency-inversion boundary between the HTTP layer and the core job engine.
#[async_trait]
pub trait ResearchJobPort: Send + Sync {
    /// Enqueue an offline training-dataset build.
    async fn enqueue_dataset_build(
        &self,
        request: BuildTrainingDatasetRequest,
        ctx: JobSubmitContext,
    ) -> QuantResult<ResearchJobView>;

    /// Enqueue an offline model-training run.
    async fn enqueue_model_train(
        &self,
        request: TrainModelRequest,
        ctx: JobSubmitContext,
    ) -> QuantResult<ResearchJobView>;

    /// Enqueue a point-in-time backtest of a registered model version.
    async fn enqueue_backtest(
        &self,
        model_version_id: ModelVersionId,
        request: RunBacktestRequest,
        ctx: JobSubmitContext,
    ) -> QuantResult<ResearchJobView>;

    /// Enqueue Combinatorial Purged Cross-Validation + governed trial-grid
    /// validation.
    async fn enqueue_cpcv_backtest(
        &self,
        model_version_id: ModelVersionId,
        request: RunCpcvBacktestRequest,
        ctx: JobSubmitContext,
    ) -> QuantResult<ResearchJobView>;

    /// Enqueue a favorite-longshot bias-table fit.
    ///
    /// The active runtime-config version is frozen at enqueue so the fit reads
    /// the exact `factors.structural.favorite_longshot` parameters that governed
    /// the request on replay.
    async fn enqueue_bias_table_fit(
        &self,
        request: FitBiasTableRequest,
        decision_policy_snapshot_id: DecisionPolicySnapshotId,
        ctx: JobSubmitContext,
    ) -> QuantResult<ResearchJobView>;

    /// Enqueue a model-score `ProbabilityCalibrator` fit.
    ///
    /// The active runtime-config version is frozen at enqueue so the fit
    /// replays through the exact `model.calibration` parameters (method
    /// sample floors, embargo gap) that governed the request.
    async fn enqueue_model_calibration_fit(
        &self,
        request: FitModelCalibratorRequest,
        decision_policy_snapshot_id: DecisionPolicySnapshotId,
        actor: GovernanceActor,
        ctx: JobSubmitContext,
    ) -> QuantResult<ResearchJobView>;

    /// Enqueue a governed executable trade-policy fit.
    async fn enqueue_trade_policy_fit(
        &self,
        request: FitTradePolicyRequest,
        ctx: JobSubmitContext,
    ) -> QuantResult<ResearchJobView>;

    /// Enqueue independent row-level validation of one immutable Draft policy.
    async fn enqueue_trade_policy_validation(
        &self,
        request: TradePolicyValidationJobParams,
        ctx: JobSubmitContext,
    ) -> QuantResult<ResearchJobView>;

    /// Page the job ledger for the operator task center.
    async fn list(&self, query: ResearchJobListQuery) -> QuantResult<Paginated<ResearchJobView>>;

    /// Load a single job (UI poll target).
    async fn get(&self, job_id: &ResearchJobId) -> QuantResult<Option<ResearchJobView>>;

    /// Cancel a job: terminal if still queued, cooperative if running.
    async fn cancel(
        &self,
        job_id: &ResearchJobId,
        reason: String,
        ctx: JobSubmitContext,
    ) -> QuantResult<ResearchJobView>;

    /// Re-enqueue a terminal job with its frozen params (records retry lineage).
    async fn retry(
        &self,
        job_id: &ResearchJobId,
        reason: String,
        ctx: JobSubmitContext,
    ) -> QuantResult<ResearchJobView>;
}
