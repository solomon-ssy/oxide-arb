//! Shadow-comparison ledger persistence DTOs (append-only governance evidence).

use chrono::{DateTime, Utc};
use sea_orm::{DeriveIntoActiveModel, DerivePartialModel};
use serde::{Deserialize, Serialize};

use crate::{
    entities::quant_shadow_comparison,
    enums::{common::MarketCategory, quant::ModelWeightSource},
    types::{
        ContentHash, DecisionPolicySnapshotId, ModelVersionId, PolicyBundleGeneration, Probability,
        ResearchProfileArtifactId, ShadowComparisonId,
        shadow::{ShadowMaturedOutcomeDelta, ShadowRankDelta, ShadowScoreDelta},
    },
};

/// Frozen, content-addressed shadow-comparison row.
#[derive(Debug, Clone, Serialize, Deserialize, DerivePartialModel)]
#[sea_orm(entity = "crate::entities::quant_shadow_comparison::Entity")]
pub struct ShadowComparisonInfo {
    pub shadow_comparison_id: ShadowComparisonId,
    pub active_model_version_id: ModelVersionId,
    pub shadow_model_version_id: ModelVersionId,
    pub active_serving_contract_hash: ContentHash,
    pub shadow_serving_contract_hash: ContentHash,
    pub research_profile_artifact_id: ResearchProfileArtifactId,
    pub category_scope: Option<MarketCategory>,
    pub decision_policy_snapshot_id: DecisionPolicySnapshotId,
    pub decision_policy_snapshot_hash: ContentHash,
    pub policy_bundle_generation: PolicyBundleGeneration,
    pub weight_source: ModelWeightSource,
    pub decision_at: DateTime<Utc>,
    pub topn_overlap: Probability,
    pub rank_delta_json: ShadowRankDelta,
    pub score_delta_json: ShadowScoreDelta,
    pub matured_outcome_json: Option<ShadowMaturedOutcomeDelta>,
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
        active_serving_contract_hash,
        shadow_serving_contract_hash,
        research_profile_artifact_id,
        category_scope,
        decision_policy_snapshot_id,
        decision_policy_snapshot_hash,
        policy_bundle_generation,
        weight_source,
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
    pub active_serving_contract_hash: ContentHash,
    pub shadow_serving_contract_hash: ContentHash,
    pub research_profile_artifact_id: ResearchProfileArtifactId,
    pub category_scope: Option<MarketCategory>,
    pub decision_policy_snapshot_id: DecisionPolicySnapshotId,
    pub decision_policy_snapshot_hash: ContentHash,
    pub policy_bundle_generation: PolicyBundleGeneration,
    pub weight_source: ModelWeightSource,
    pub decision_at: DateTime<Utc>,
    pub topn_overlap: Probability,
    pub rank_delta_json: ShadowRankDelta,
    pub score_delta_json: ShadowScoreDelta,
    pub matured_outcome_json: Option<ShadowMaturedOutcomeDelta>,
    pub hard_divergence: bool,
    pub comparison_hash: ContentHash,
}

/// Aggregate stability of a shadow version over a recent comparison window, used
/// by the route-promotion gate (`min_shadow_overlap_stability` + no `hard_divergence`).
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

/// Exact immutable identity and half-open window used by the F10 gate.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ShadowObservationQuery {
    pub active_model_version_id: ModelVersionId,
    pub shadow_model_version_id: ModelVersionId,
    pub active_serving_contract_hash: ContentHash,
    pub shadow_serving_contract_hash: ContentHash,
    pub research_profile_artifact_id: ResearchProfileArtifactId,
    pub category_scope: Option<MarketCategory>,
    pub decision_policy_snapshot_id: DecisionPolicySnapshotId,
    pub decision_policy_snapshot_hash: ContentHash,
    pub policy_bundle_generation: PolicyBundleGeneration,
    pub window_start: DateTime<Utc>,
    pub window_end: DateTime<Utc>,
}

/// Exact aggregate over one [`ShadowObservationQuery`].
///
/// Empty windows carry no synthetic overlap or timestamp.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ShadowObservationWindow {
    pub sample_count: u64,
    pub first_decision_at: Option<DateTime<Utc>>,
    pub last_decision_at: Option<DateTime<Utc>>,
    pub mean_topn_overlap: Option<Probability>,
    pub any_hard_divergence: bool,
}
