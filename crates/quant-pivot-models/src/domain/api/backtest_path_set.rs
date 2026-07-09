//! CPCV validation admin HTTP contract (Phase 11.5).
//!
//! UI surface for Combinatorial Purged Cross-Validation + governed trial-grid
//! validation of a registered model version:
//!
//! 1. `POST /research/models/{id}/cpcv-backtest` — run CPCV + the trial grid
//!    over a historical window (PIT only, never the live `BookStore`),
//!    persist a [`BacktestPathSetView`].
//! 2. `GET /research/backtest-path-sets/{id}` — fetch a stored path set.
//! 3. `GET /research/backtest-path-sets` — paginated catalog.

use chrono::{DateTime, Utc};
use quant_pivot_macros::NormalizePageQuery;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use validator::Validate;

use crate::{
    domain::{BacktestPathSetInfo, pagination::PageRequest},
    types::{
        BacktestPathSetId, ModelRunId, ModelVersionId, RuntimeConfigVersionId, TrainingDatasetId,
    },
};

/// Inbound body for `POST /research/models/{id}/cpcv-backtest` (the model
/// version id is taken from the path).
///
/// `Serialize` is derived so the request can be frozen into a durable
/// research job's `params_json` and replayed on execute.
#[derive(Debug, Clone, Serialize, Deserialize, Validate)]
pub struct RunCpcvBacktestRequest {
    /// Frozen, PIT-materialized dataset the model version was trained on.
    pub training_dataset_id: TrainingDatasetId,
    /// Frozen runtime-config version governing `research.validation.*` (CPCV
    /// partitions, purge/embargo, trial grid, PBO block count, gate
    /// thresholds) + portfolio caps + provenance.
    pub runtime_config_version_id: RuntimeConfigVersionId,
    /// Model family to validate: `"weighted_factor"` or `"classical:<kind>"` —
    /// the exact same training-time parameter [`RunBacktestRequest`]'s sibling
    /// `POST /research/models/{id}/train` accepts, since CPCV re-trains every
    /// fold from scratch and must reproduce the candidate's own training
    /// configuration.
    #[validate(length(min = 1))]
    pub model_family: String,
    /// Supervised target label name (e.g. `"settlement_outcome"`).
    #[validate(length(min = 1))]
    pub label_name: String,
    /// Horizon of the target label in seconds (`0` for horizon-independent labels).
    pub label_horizon_secs: u64,
    /// Model-intrinsic prediction horizon in seconds (frozen into every
    /// ephemeral fold/trial artifact).
    #[validate(range(min = 1))]
    pub prediction_horizon_secs: u64,
    /// Operator reason recorded on the operation log.
    #[validate(length(min = 1, max = 512))]
    pub reason: String,
    /// Pre-assigned path-set id frozen at async enqueue for effectively-once
    /// recovery; omit on direct calls — the job engine mints one before
    /// persisting params.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub path_set_id: Option<BacktestPathSetId>,
}

/// Stored CPCV path-set result returned after a run and on fetch.
#[derive(Debug, Clone, Serialize)]
pub struct BacktestPathSetView {
    pub path_set_id: BacktestPathSetId,
    pub model_version_id: ModelVersionId,
    pub model_run_id: ModelRunId,
    pub training_dataset_id: TrainingDatasetId,
    pub runtime_config_version_id: RuntimeConfigVersionId,
    pub window_start: DateTime<Utc>,
    pub window_end: DateTime<Utc>,
    pub path_count: i64,
    pub combination_count: i64,
    pub median_rank_ic: Decimal,
    pub sharpe_distribution: serde_json::Value,
    pub paths: serde_json::Value,
    pub deflated_sharpe: Decimal,
    pub dsr_benchmark_sharpe: Decimal,
    pub pbo: Decimal,
    pub min_track_record_length_secs: Option<i64>,
    pub trial_count: i64,
    pub trial_grid_count: i64,
    pub coord_search_effective_n: i64,
    pub created_at: DateTime<Utc>,
}

/// Paginated filter for the append-only CPCV path-set ledger catalog.
#[derive(Debug, Clone, Default, Deserialize, NormalizePageQuery)]
pub struct BacktestPathSetListQuery {
    pub model_version_id: Option<ModelVersionId>,
    pub from: Option<DateTime<Utc>>,
    pub to: Option<DateTime<Utc>>,
    #[normalize_page]
    #[serde(flatten)]
    pub page: PageRequest,
}

impl From<BacktestPathSetInfo> for BacktestPathSetView {
    fn from(info: BacktestPathSetInfo) -> Self {
        Self {
            path_set_id: info.path_set_id,
            model_version_id: info.model_version_id,
            model_run_id: info.model_run_id,
            training_dataset_id: info.training_dataset_id,
            runtime_config_version_id: info.runtime_config_version_id,
            window_start: info.window_start,
            window_end: info.window_end,
            path_count: info.path_count,
            combination_count: info.combination_count,
            median_rank_ic: info.median_rank_ic,
            sharpe_distribution: info.sharpe_distribution,
            paths: info.paths,
            deflated_sharpe: info.deflated_sharpe,
            dsr_benchmark_sharpe: info.dsr_benchmark_sharpe,
            pbo: info.pbo,
            min_track_record_length_secs: info.min_track_record_length_secs,
            trial_count: info.trial_count,
            trial_grid_count: info.trial_grid_count,
            coord_search_effective_n: info.coord_search_effective_n,
            created_at: info.created_at,
        }
    }
}
