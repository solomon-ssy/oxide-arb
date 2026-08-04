//! Canonical fixed-schema shadow-comparison values.

use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use sea_orm::FromJsonQueryResult;
use serde::{Deserialize, Serialize};

use crate::{
    enums::{common::MarketCategory, quant::ModelWeightSource},
    types::{
        ContentHash, DecisionPolicySnapshotId, ModelVersionId, PolicyBundleGeneration, Probability,
        ResearchProfileArtifactId, ShadowComparisonId,
    },
};

/// Per-market ranking divergence over the markets scored by both models.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, FromJsonQueryResult)]
#[serde(deny_unknown_fields)]
pub struct ShadowRankDelta {
    pub mean_abs_rank_delta: Decimal,
    pub max_rank_delta: u32,
    pub spearman: Decimal,
    pub common_markets: u64,
}

/// Per-market composite-score divergence over the markets scored by both models.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, FromJsonQueryResult)]
#[serde(deny_unknown_fields)]
pub struct ShadowScoreDelta {
    pub mean_abs_score_delta: Decimal,
    pub max_score_delta: Decimal,
    pub side_disagreement_rate: Decimal,
}

/// Realized-outcome divergence backfilled after labels mature.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, FromJsonQueryResult)]
#[serde(deny_unknown_fields)]
pub struct ShadowMaturedOutcomeDelta {
    pub active_realized_return_bps: Decimal,
    pub shadow_realized_return_bps: Decimal,
    pub delta_bps: Decimal,
}

/// Frozen, content-addressed shadow comparison at the signal/rank layer.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ShadowComparison {
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
    pub rank_delta: ShadowRankDelta,
    pub score_delta: ShadowScoreDelta,
    pub matured_outcome_delta: Option<ShadowMaturedOutcomeDelta>,
    pub hard_divergence: bool,
    pub comparison_hash: ContentHash,
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::ShadowRankDelta;

    #[test]
    fn rank_rejects_unknown_schema() {
        let valid = json!({
            "mean_abs_rank_delta": "1",
            "max_rank_delta": 2,
            "spearman": "0.8",
            "common_markets": 10
        });
        assert!(serde_json::from_value::<ShadowRankDelta>(valid.clone()).is_ok());

        let mut incomplete = valid.clone();
        incomplete
            .as_object_mut()
            .expect("object")
            .remove("common_markets");
        assert!(serde_json::from_value::<ShadowRankDelta>(incomplete).is_err());

        let mut unknown = valid;
        unknown["unversioned_extension"] = json!(true);
        assert!(serde_json::from_value::<ShadowRankDelta>(unknown).is_err());
    }
}
