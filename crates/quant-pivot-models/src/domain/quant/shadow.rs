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
    pub champion_model_version_id: ModelVersionId,
    pub candidate_model_version_id: ModelVersionId,
    pub champion_serving_contract_hash: ContentHash,
    pub candidate_serving_contract_hash: ContentHash,
    pub research_profile_artifact_id: ResearchProfileArtifactId,
    pub category_scope: Option<MarketCategory>,
    pub decision_policy_snapshot_id: DecisionPolicySnapshotId,
    pub decision_policy_snapshot_hash: ContentHash,
    pub policy_bundle_generation: PolicyBundleGeneration,
    pub weight_source: ModelWeightSource,
    pub decision_at: DateTime<Utc>,
    pub topn_decision_overlap: Probability,
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
        champion_model_version_id,
        candidate_model_version_id,
        champion_serving_contract_hash,
        candidate_serving_contract_hash,
        research_profile_artifact_id,
        category_scope,
        decision_policy_snapshot_id,
        decision_policy_snapshot_hash,
        policy_bundle_generation,
        weight_source,
        decision_at,
        topn_decision_overlap,
        rank_delta_json,
        score_delta_json,
        matured_outcome_json,
        hard_divergence,
        comparison_hash,
        created_at,
    }
);

impl ShadowComparisonInfo {
    /// Whether a replay carries the exact immutable semantic content already
    /// sealed by this row. The generated ledger id and database timestamp are
    /// intentionally excluded from content-addressed idempotency.
    #[must_use]
    pub fn matches_new(&self, comparison: &NewShadowComparison) -> bool {
        self.champion_model_version_id == comparison.champion_model_version_id
            && self.candidate_model_version_id == comparison.candidate_model_version_id
            && self.champion_serving_contract_hash == comparison.champion_serving_contract_hash
            && self.candidate_serving_contract_hash == comparison.candidate_serving_contract_hash
            && self.research_profile_artifact_id == comparison.research_profile_artifact_id
            && self.category_scope == comparison.category_scope
            && self.decision_policy_snapshot_id == comparison.decision_policy_snapshot_id
            && self.decision_policy_snapshot_hash == comparison.decision_policy_snapshot_hash
            && self.policy_bundle_generation == comparison.policy_bundle_generation
            && self.weight_source == comparison.weight_source
            && self.decision_at == comparison.decision_at
            && self.topn_decision_overlap == comparison.topn_decision_overlap
            && self.rank_delta_json == comparison.rank_delta_json
            && self.score_delta_json == comparison.score_delta_json
            && self.matured_outcome_json == comparison.matured_outcome_json
            && self.hard_divergence == comparison.hard_divergence
            && self.comparison_hash == comparison.comparison_hash
    }
}

/// Insert payload for `quant_shadow_comparison` (omits DB-managed `created_at`).
#[derive(Debug, Clone, Serialize, Deserialize, DeriveIntoActiveModel)]
#[sea_orm(active_model = "crate::entities::quant_shadow_comparison::ActiveModel")]
pub struct NewShadowComparison {
    pub shadow_comparison_id: ShadowComparisonId,
    pub champion_model_version_id: ModelVersionId,
    pub candidate_model_version_id: ModelVersionId,
    pub champion_serving_contract_hash: ContentHash,
    pub candidate_serving_contract_hash: ContentHash,
    pub research_profile_artifact_id: ResearchProfileArtifactId,
    pub category_scope: Option<MarketCategory>,
    pub decision_policy_snapshot_id: DecisionPolicySnapshotId,
    pub decision_policy_snapshot_hash: ContentHash,
    pub policy_bundle_generation: PolicyBundleGeneration,
    pub weight_source: ModelWeightSource,
    pub decision_at: DateTime<Utc>,
    pub topn_decision_overlap: Probability,
    pub rank_delta_json: ShadowRankDelta,
    pub score_delta_json: ShadowScoreDelta,
    pub matured_outcome_json: Option<ShadowMaturedOutcomeDelta>,
    pub hard_divergence: bool,
    pub comparison_hash: ContentHash,
}

/// Aggregate stability of a shadow version over a recent comparison window, used
/// by the route-promotion gate (`min_shadow_decision_overlap` + no
/// `hard_divergence`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ShadowStabilitySummary {
    /// Shadow version the summary describes.
    pub candidate_model_version_id: ModelVersionId,
    /// Number of comparisons observed in the window.
    pub sample_count: u64,
    /// Earliest comparison `decision_at` in the window (window coverage start).
    pub window_start: Option<DateTime<Utc>>,
    /// Latest comparison `decision_at` in the window (window coverage end).
    pub window_end: Option<DateTime<Utc>>,
    /// Mean signed `TopN` decision overlap across the window in `[0, 1]`.
    pub mean_topn_decision_overlap: Probability,
    /// Whether any comparison in the window flagged a hard divergence.
    pub any_hard_divergence: bool,
}

/// Exact immutable identity and half-open window used by the F10 gate.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ShadowObservationQuery {
    pub champion_model_version_id: ModelVersionId,
    pub candidate_model_version_id: ModelVersionId,
    pub champion_serving_contract_hash: ContentHash,
    pub candidate_serving_contract_hash: ContentHash,
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
    pub mean_topn_decision_overlap: Option<Probability>,
    pub any_hard_divergence: bool,
}
