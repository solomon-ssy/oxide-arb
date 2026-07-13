//! Shadow-comparison ledger persistence DTOs (append-only governance evidence).

use crate::{
    entities::quant_shadow_comparison,
    types::{ContentHash, ModelVersionId, Probability, ShadowComparisonId},
};
use chrono::{DateTime, Utc};
use sea_orm::{DeriveIntoActiveModel, DerivePartialModel, FromQueryResult};
use serde::{Deserialize, Serialize};

/// Frozen, content-addressed shadow-comparison row.
#[derive(Debug, Clone, Serialize, Deserialize, DerivePartialModel, FromQueryResult)]
#[sea_orm(entity = "crate::entities::quant_shadow_comparison::Entity")]
pub struct ShadowComparisonInfo {
    pub shadow_comparison_id: ShadowComparisonId,
    pub active_model_version_id: ModelVersionId,
    pub shadow_model_version_id: ModelVersionId,
    pub decision_at: DateTime<Utc>,
    pub topn_overlap: Probability,
    pub rank_delta_json: serde_json::Value,
    pub score_delta_json: serde_json::Value,
    pub matured_outcome_json: Option<serde_json::Value>,
    pub hard_divergence: bool,
    pub comparison_hash: ContentHash,
    pub created_at: DateTime<Utc>,
}

info_from_model!(
    ShadowComparisonInfo,
    quant_shadow_comparison::Model,
    {
        shadow_comparison_id,
        active_model_version_id,
        shadow_model_version_id,
        decision_at,
        topn_overlap,
        rank_delta_json,
        score_delta_json,
        matured_outcome_json,
        hard_divergence,
        comparison_hash,
        created_at,
    }
);

/// Insert payload for `quant_shadow_comparison` (omits DB-managed `created_at`).
#[derive(Debug, Clone, Serialize, Deserialize, DeriveIntoActiveModel)]
#[sea_orm(active_model = "crate::entities::quant_shadow_comparison::ActiveModel")]
pub struct NewShadowComparison {
    pub shadow_comparison_id: ShadowComparisonId,
    pub active_model_version_id: ModelVersionId,
    pub shadow_model_version_id: ModelVersionId,
    pub decision_at: DateTime<Utc>,
    pub topn_overlap: Probability,
    pub rank_delta_json: serde_json::Value,
    pub score_delta_json: serde_json::Value,
    pub matured_outcome_json: Option<serde_json::Value>,
    pub hard_divergence: bool,
    pub comparison_hash: ContentHash,
}

/// Aggregate stability of a shadow version over a recent comparison window, used
/// by the publish gate (`min_shadow_overlap_stability` + no `hard_divergence`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ShadowStabilitySummary {
    /// Shadow version the summary describes.
    pub shadow_model_version_id: ModelVersionId,
    /// Number of comparisons observed in the window.
    pub sample_count: u64,
    /// Earliest comparison `decision_at` in the window (window coverage start).
    pub window_start: Option<DateTime<Utc>>,
    /// Latest comparison `decision_at` in the window (window coverage end).
    pub window_end: Option<DateTime<Utc>>,
    /// Mean `TopN` overlap across the window in `[0, 1]`.
    pub mean_topn_overlap: Probability,
    /// Whether any comparison in the window flagged a hard divergence.
    pub any_hard_divergence: bool,
}
