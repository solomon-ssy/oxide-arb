//! Training-dataset admin HTTP contract (Phase 3.5).
//!
//! These types are the **UI integration surface** for offline dataset plan/build.
//! The Admin SPA (Phase 07) should:
//!
//! 1. Let the operator pick a frozen [`RuntimeConfigVersionId`] and [`ModelSpecId`].
//! 2. `POST /research/training-datasets/plan` — validate the window and show
//!    `planned_samples` before committing to a long build.
//! 3. `POST /research/training-datasets/build` — materialize using the same body
//!    plus the `training_dataset_id` returned from step 2 (stable plan → build).
//! 4. Poll `GET /research/training-datasets/{id}` until `status` is terminal
//!    (`built`, `insufficient_labels`, `failed`, `ready`, or `expired`).
//!
//! Leakage violations abort the build with an HTTP error and **do not** write a
//! ledger row (distinct from terminal `failed`, which persists diagnostics).
//!
//! Terminal semantics mirror [`TrainingDatasetStatus`]: trainer/backtest gates in
//! 03.6 consume only `ready` datasets; `insufficient_labels` still persists the
//! Parquet artifact for inspection.

use chrono::{DateTime, Utc};
use quant_pivot_macros::NormalizePageQuery;
use serde::{Deserialize, Serialize};
use validator::Validate;

use crate::{
    domain::{TrainingDatasetInfo, pagination::PageRequest},
    enums::quant::TrainingDatasetStatus,
    types::{
        ContentHash, DatasetCoverage, ModelSpecId, RuntimeConfigVersionId, SchemaVersion,
        TrainingDatasetId, TrainingSampleSource, default_sample_sources,
    },
};

/// Inbound body for plan and build endpoints (shared window/config fields).
///
/// **Plan** ignores [`Self::training_dataset_id`] (always mints a fresh id).
/// **Build** should pass the id returned by plan so polling and artifacts align.
///
/// `Serialize` is derived so the request can be frozen verbatim into a durable
/// research-job's `params_json` (the async job ledger replays it on execute).
#[derive(Debug, Clone, Serialize, Deserialize, Validate)]
#[validate(schema(function = "validate_build_training_dataset_request"))]
pub struct BuildTrainingDatasetRequest {
    /// Target model specification (trainer binds artifacts to this spec).
    pub model_spec_id: ModelSpecId,
    /// Frozen runtime-config version governing feature/factor/label schemas.
    pub runtime_config_version_id: RuntimeConfigVersionId,
    /// Inclusive first sample `as_of`.
    pub window_start: DateTime<Utc>,
    /// Exclusive window end (samples are strictly before this instant).
    pub window_end: DateTime<Utc>,
    /// Deterministic sample grid step in seconds (`>= 1`).
    #[validate(range(min = 1))]
    pub sample_interval_secs: u64,
    /// Forward label horizons in seconds (one label column per horizon).
    #[validate(length(min = 1))]
    pub horizons_secs: Vec<u64>,
    /// Feature source visibility delay in seconds (PIT cutoff).
    #[validate(range(min = 1))]
    pub source_delay_secs: u64,
    /// Feature schema version to materialize (defaults to v1).
    #[serde(default = "default_feature_schema_version")]
    pub feature_schema_version: SchemaVersion,
    /// Sample sources to materialize. Defaults to historical PIT + live attribution.
    #[serde(default = "default_sample_sources")]
    pub sample_sources: Vec<TrainingSampleSource>,
    /// Operator reason recorded on the operation log (UI should require non-empty).
    #[validate(length(min = 1, max = 512))]
    pub reason: String,
    /// Pre-assigned id from a prior **plan** response; omit on plan, required on build
    /// for stable UI polling (build re-plans samples but reuses this id).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub training_dataset_id: Option<TrainingDatasetId>,
}

crate::half_open_window_request!(BuildTrainingDatasetRequest);

const fn default_feature_schema_version() -> SchemaVersion {
    SchemaVersion::FIRST
}

/// Dry-plan response — no ledger row is written.
#[derive(Debug, Clone, Serialize)]
pub struct TrainingDatasetPlanView {
    /// Pre-assigned id that the subsequent build will use (stable across plan → build).
    pub training_dataset_id: TrainingDatasetId,
    pub model_spec_id: ModelSpecId,
    pub runtime_config_version_id: RuntimeConfigVersionId,
    pub window_start: DateTime<Utc>,
    pub window_end: DateTime<Utc>,
    /// Number of `(as_of, market)` samples the build would iterate (spine size
    /// plus live-attribution + exit-decision rows). An **upper bound**: the exact
    /// eligible count only emerges from the build's coverage (per-`as_of`
    /// liquidity/data-quality eligibility is applied during materialization).
    pub planned_samples: u64,
    /// Deterministic historical spine size (selection × alive instants), the
    /// dominant term of [`Self::planned_samples`], exposed for UI transparency.
    pub spine_upper_bound: u64,
    /// Whether the plan exceeds the global hard cap — the UI must block build and
    /// prompt the operator to narrow the window / interval / selection.
    pub hard_cap_exceeded: bool,
    /// Estimated samples after point-in-time selection eligibility:
    /// [`Self::planned_samples`] scaled by the sampled keep-rate (falls back to
    /// the upper bound when the estimate is disabled/unavailable).
    pub estimated_eligible_samples: u64,
    /// Sampled fraction of candidate markets passing the PIT selection funnel, in
    /// `[0, 1]`. `None` when the estimate is disabled or has no candidates.
    pub keep_rate: Option<f64>,
    /// Number of `(market, slice)` eligibility trials backing [`Self::keep_rate`].
    pub keep_rate_sample_size: u64,
}

/// Ledger projection returned to the UI after build and on poll.
#[derive(Debug, Clone, Serialize)]
pub struct TrainingDatasetView {
    pub training_dataset_id: TrainingDatasetId,
    pub model_spec_id: ModelSpecId,
    pub window_start: DateTime<Utc>,
    pub window_end: DateTime<Utc>,
    /// Lifecycle state — UI should map to badges and gate trainer actions on `ready`.
    pub status: TrainingDatasetStatus,
    pub feature_schema_hash: ContentHash,
    pub factor_schema_hash: ContentHash,
    pub label_schema_hash: ContentHash,
    pub dataset_hash: ContentHash,
    pub parquet_uri: String,
    pub sample_count: i64,
    /// Build diagnostics: planned vs built examples, decode failures, label skips, etc.
    pub coverage_json: DatasetCoverage,
    pub runtime_config_version_id: RuntimeConfigVersionId,
    pub created_at: DateTime<Utc>,
}

/// Paginated filter for the training-dataset ledger catalog.
///
/// `from` / `to` bound `created_at`; `status` narrows the lifecycle state; the
/// pagination window is the shared [`PageRequest`], flattened so the query
/// string stays flat (`?page=&size=`).
#[derive(Debug, Clone, Default, Deserialize, NormalizePageQuery)]
pub struct TrainingDatasetListQuery {
    pub model_spec_id: Option<ModelSpecId>,
    pub status: Option<TrainingDatasetStatus>,
    pub from: Option<DateTime<Utc>>,
    pub to: Option<DateTime<Utc>>,
    #[normalize_page]
    #[serde(flatten)]
    pub page: PageRequest,
}

impl From<TrainingDatasetInfo> for TrainingDatasetView {
    fn from(info: TrainingDatasetInfo) -> Self {
        Self {
            training_dataset_id: info.training_dataset_id,
            model_spec_id: info.model_spec_id,
            window_start: info.window_start,
            window_end: info.window_end,
            status: info.status,
            feature_schema_hash: info.feature_schema_hash,
            factor_schema_hash: info.factor_schema_hash,
            label_schema_hash: info.label_schema_hash,
            dataset_hash: info.dataset_hash,
            parquet_uri: info.parquet_uri.to_string(),
            sample_count: info.sample_count,
            coverage_json: info.coverage_json,
            runtime_config_version_id: info.runtime_config_version_id,
            created_at: info.created_at,
        }
    }
}
