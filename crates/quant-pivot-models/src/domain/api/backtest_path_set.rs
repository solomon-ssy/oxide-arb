//! CPCV validation admin HTTP contract.
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
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use validator::Validate;

use crate::{
    domain::{pagination::PageRequest, quant::BacktestPathSetInfo},
    types::{
        BacktestPathSetId, ContentHash, DecisionPolicySnapshotId, ModelRunId, ModelVersionId,
        TrainingDatasetId,
        backtest::{
            BacktestPaths, CpcvFoldArtifacts, CpcvMethodologyBinding, CpcvPathSetSubject,
            CscvSelectionEvidence, SharpeDistribution,
        },
    },
};

/// Inbound body for `POST /research/models/{id}/cpcv-backtest` (the model
/// version id is taken from the path).
///
/// `Serialize` is derived so the request can be frozen into a durable
/// research job's `params_json` and replayed on execute.
///
/// Model family, input contract, supervised target, and prediction horizon are
/// deliberately absent: the server resolves them from the model version's
/// linked dataset and immutable model specification.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Validate, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct RunCpcvBacktestRequest {
    /// Frozen, PIT-materialized dataset the model version was trained on.
    pub training_dataset_id: TrainingDatasetId,
    /// Frozen runtime-config version governing `research.validation.*` (CPCV
    /// partitions, purge/embargo, trial grid, PBO block count, gate
    /// thresholds) + portfolio caps + provenance.
    pub decision_policy_snapshot_id: DecisionPolicySnapshotId,
    /// Operator reason recorded on the operation log.
    #[validate(length(min = 1, max = 512))]
    pub reason: String,
    /// Pre-assigned path-set id frozen at async enqueue for effectively-once
    /// recovery; omit on direct calls — the job engine mints one before
    /// persisting params.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub path_set_id: Option<BacktestPathSetId>,
}

#[cfg(test)]
mod request_tests {
    use serde_json::Value;

    use super::RunCpcvBacktestRequest;

    fn request() -> Value {
        serde_json::json!({
            "training_dataset_id": uuid::Uuid::now_v7(),
            "decision_policy_snapshot_id": uuid::Uuid::now_v7(),
            "reason": "validate the frozen candidate"
        })
    }

    #[test]
    fn cpcv_request_accepts_reason() {
        serde_json::from_value::<RunCpcvBacktestRequest>(request())
            .expect("minimal frozen CPCV request");
    }

    #[test]
    fn cpcv_request_rejects_fields() {
        for field in [
            "model_family",
            "label_name",
            "label_horizon_secs",
            "prediction_horizon_secs",
        ] {
            let mut value = request();
            value[field] = serde_json::json!(if field == "model_family" {
                "weighted_factor"
            } else {
                "client_override"
            });
            assert!(
                serde_json::from_value::<RunCpcvBacktestRequest>(value).is_err(),
                "legacy client-owned field `{field}` must fail closed"
            );
        }
    }
}

/// Stored CPCV path-set result returned after a run and on fetch.
#[derive(Debug, Clone, Serialize)]
pub struct BacktestPathSetView {
    pub path_set_id: BacktestPathSetId,
    pub model_version_id: ModelVersionId,
    pub model_run_id: ModelRunId,
    pub training_dataset_id: TrainingDatasetId,
    pub decision_policy_snapshot_id: DecisionPolicySnapshotId,
    pub window_start: DateTime<Utc>,
    pub window_end: DateTime<Utc>,
    pub subject: CpcvPathSetSubject,
    pub methodology: CpcvMethodologyBinding,
    pub fold_artifacts: CpcvFoldArtifacts,
    pub path_count: i64,
    pub combination_count: i64,
    pub median_target_rank_ic: Decimal,
    pub sharpe_distribution: SharpeDistribution,
    pub paths: BacktestPaths,
    pub deflated_sharpe: Decimal,
    pub dsr_benchmark_sharpe: Decimal,
    pub pbo: Decimal,
    pub cscv_selection_evidence: CscvSelectionEvidence,
    pub min_track_record_length_secs: Option<i64>,
    pub dsr_conservative_independent_trial_count: i64,
    pub trial_grid_count: i64,
    pub coord_search_effective_n: i64,
    pub path_set_hash: ContentHash,
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
            decision_policy_snapshot_id: info.decision_policy_snapshot_id,
            window_start: info.window_start,
            window_end: info.window_end,
            subject: info.subject,
            methodology: info.methodology,
            fold_artifacts: info.fold_artifacts,
            path_count: info.path_count,
            combination_count: info.combination_count,
            median_target_rank_ic: info.median_target_rank_ic,
            sharpe_distribution: info.sharpe_distribution,
            paths: info.paths,
            deflated_sharpe: info.deflated_sharpe,
            dsr_benchmark_sharpe: info.dsr_benchmark_sharpe,
            pbo: info.pbo,
            cscv_selection_evidence: info.cscv_selection_evidence,
            min_track_record_length_secs: info.min_track_record_length_secs,
            dsr_conservative_independent_trial_count: info.dsr_conservative_independent_trial_count,
            trial_grid_count: info.trial_grid_count,
            coord_search_effective_n: info.coord_search_effective_n,
            path_set_hash: info.path_set_hash,
            created_at: info.created_at,
        }
    }
}
