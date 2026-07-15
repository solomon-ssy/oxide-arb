//! Durable research-job admin HTTP contract.
//!
//! The async job ledger is the single UI surface for long-running research tasks
//! (dataset build / model train / backtest). The SPA:
//!
//! 1. `POST` the existing build/train/backtest endpoints → receives a
//!    [`ResearchJobView`] (HTTP 202, `status = queued`).
//! 2. Tracks progress live over the `materialization.run_update` WS channel and
//!    falls back to `GET /research/jobs/{id}` polling.
//! 3. May `POST /research/jobs/{id}/cancel` (cooperative) or
//!    `/retry` (clone params into a fresh job, recording lineage).

use chrono::{DateTime, Utc};
use quant_pivot_macros::NormalizePageQuery;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;
use validator::Validate;

use crate::{
    domain::{
        FitTradePolicyRequest, ResearchJobInfo, RunBacktestRequest, RunCpcvBacktestRequest,
        RunFullFeatureParityRequest, TrainModelRequest, pagination::PageRequest,
    },
    enums::quant::{ResearchJobKind, ResearchJobStatus},
    types::{
        DatasetCoverage, FeatureParityRunId, ModelSpecId, ModelVersionId, ResearchJobError,
        ResearchJobId, ResearchJobProgress, RuntimeConfigVersionId, TradePolicyArtifactId,
        TradePolicyValidationRunId, TrainingDatasetId,
    },
};

/// Server-owned durable envelope for one trade-policy fit.
///
/// The Dataset id is assigned once at enqueue and survives lease recovery and
/// explicit job retry. It is not accepted from the HTTP fit request.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TradePolicyFitJobParams {
    pub training_dataset_id: TrainingDatasetId,
    #[serde(flatten)]
    pub request: FitTradePolicyRequest,
}

/// Frozen governance identity for an independently executed policy validation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TradePolicyValidationJobParams {
    pub validation_run_id: TradePolicyValidationRunId,
    pub artifact_id: TradePolicyArtifactId,
    pub actor_id: Uuid,
    pub reason: String,
}

/// Frozen params for a deterministic feature-parity replay job.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FeatureParityJobParams {
    pub parity_run_id: FeatureParityRunId,
    /// Frozen writer grace period. It is `max(10 minutes, 2 × the longest
    /// enabled report cadence)` at enqueue time and starts at first pending
    /// observation, never at queue creation.
    pub materialization_timeout_secs: u64,
    #[serde(flatten)]
    pub request: RunFullFeatureParityRequest,
}

/// Internal durable envelope for model training. The public request contains
/// only dataset id + reason; the worker-owned result id is assigned at enqueue.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ModelTrainJobParams {
    pub model_version_id: ModelVersionId,
    #[serde(flatten)]
    pub request: TrainModelRequest,
}

/// Frozen params for a `backtest` job: the path model version + the replay body.
///
/// Dataset-build and model-train jobs freeze their request bodies directly;
/// backtest additionally needs the model version taken from the route path.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BacktestJobParams {
    pub model_version_id: ModelVersionId,
    #[serde(flatten)]
    pub request: RunBacktestRequest,
}

/// Frozen params for a `cpcv_backtest` job (Phase 11.5): the path model
/// version + the CPCV/trial-grid request body.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CpcvBacktestJobParams {
    pub model_version_id: ModelVersionId,
    #[serde(flatten)]
    pub request: RunCpcvBacktestRequest,
}

/// Outbound projection of one durable research job.
#[derive(Debug, Clone, Serialize)]
pub struct ResearchJobView {
    pub job_id: ResearchJobId,
    pub kind: ResearchJobKind,
    pub status: ResearchJobStatus,
    pub model_spec_id: Option<ModelSpecId>,
    pub runtime_config_version_id: Option<RuntimeConfigVersionId>,
    /// Frozen request body (for the detail drawer / retry preview).
    pub params: Value,
    /// Live progress snapshot (phase + processed/total), when the run has ticked.
    pub progress: Option<ResearchJobProgress>,
    /// Completion fraction in `[0, 1]`, when a positive total is known.
    pub progress_pct: Option<f64>,
    /// Terminal result id (dataset / model version / backtest report).
    pub result_ref: Option<Uuid>,
    /// Structured failure payload on terminal `failed`.
    pub error: Option<ResearchJobError>,
    /// Build/backtest coverage diagnostics.
    pub coverage_json: Option<DatasetCoverage>,
    pub requested_by: Option<String>,
    pub acting_role: String,
    pub parent_job_id: Option<ResearchJobId>,
    /// Number of automatic crash-recovery re-queues so far.
    pub recovery_attempt: i32,
    pub max_recovery_attempts: i32,
    pub lease_expires_at: Option<DateTime<Utc>>,
    pub started_at: Option<DateTime<Utc>>,
    pub finished_at: Option<DateTime<Utc>>,
    pub heartbeat_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl From<ResearchJobInfo> for ResearchJobView {
    fn from(info: ResearchJobInfo) -> Self {
        let progress = info.progress_json.clone();
        let progress_pct = progress.as_ref().and_then(ResearchJobProgress::pct);
        let error = info.error_json.clone();
        Self {
            job_id: info.job_id,
            kind: info.kind,
            status: info.status,
            model_spec_id: info.model_spec_id,
            runtime_config_version_id: info.runtime_config_version_id,
            params: info.params_json,
            progress,
            progress_pct,
            result_ref: info.result_ref,
            error,
            coverage_json: info.coverage_json,
            requested_by: info.requested_by,
            acting_role: info.acting_role,
            parent_job_id: info.parent_job_id,
            recovery_attempt: info.recovery_attempt,
            max_recovery_attempts: info.max_recovery_attempts,
            lease_expires_at: info.lease_expires_at,
            started_at: info.started_at,
            finished_at: info.finished_at,
            heartbeat_at: info.heartbeat_at,
            created_at: info.created_at,
            updated_at: info.updated_at,
        }
    }
}

/// Paginated filter for the research-job ledger catalog.
#[derive(Debug, Clone, Default, Deserialize, NormalizePageQuery)]
pub struct ResearchJobListQuery {
    pub kind: Option<ResearchJobKind>,
    pub status: Option<ResearchJobStatus>,
    pub model_spec_id: Option<ModelSpecId>,
    /// Exact terminal result artifact. For feature-parity jobs this is the
    /// `parity_run_id`, enabling a durable run → job audit deep-link.
    pub result_ref: Option<Uuid>,
    pub from: Option<DateTime<Utc>>,
    pub to: Option<DateTime<Utc>>,
    #[normalize_page]
    #[serde(flatten)]
    pub page: PageRequest,
}

/// Governed body for cooperative job cancellation.
#[derive(Debug, Clone, Deserialize, Validate)]
pub struct CancelResearchJobRequest {
    #[validate(length(min = 1, max = 512))]
    pub reason: String,
}

/// Governed body for re-enqueuing a terminal job with its frozen params.
#[derive(Debug, Clone, Deserialize, Validate)]
pub struct RetryResearchJobRequest {
    #[validate(length(min = 1, max = 512))]
    pub reason: String,
}
