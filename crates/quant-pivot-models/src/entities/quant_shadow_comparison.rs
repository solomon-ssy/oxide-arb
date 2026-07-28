//! `quant_shadow_comparison` table entity.

use chrono::{DateTime, Utc};
use sea_orm::entity::prelude::*;

use super::{quant_model_version, research_profile_artifact};
use crate::{
    enums::{common::MarketCategory, quant::ModelWeightSource},
    types::{
        ContentHash, DecisionPolicySnapshotId, ModelVersionId, PolicyBundleGeneration, Probability,
        ResearchProfileArtifactId, ShadowComparisonId,
        shadow::{ShadowMaturedOutcomeDelta, ShadowRankDelta, ShadowScoreDelta},
    },
};

#[sea_orm::model]
#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "quant_shadow_comparison")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub shadow_comparison_id: ShadowComparisonId,
    pub active_model_version_id: ModelVersionId,
    pub shadow_model_version_id: ModelVersionId,
    pub active_serving_contract_hash: ContentHash,
    pub shadow_serving_contract_hash: ContentHash,
    pub research_profile_artifact_id: ResearchProfileArtifactId,
    #[sea_orm(column_type = r#"custom("qp_market_category")"#)]
    pub category_scope: Option<MarketCategory>,
    pub decision_policy_snapshot_id: DecisionPolicySnapshotId,
    pub decision_policy_snapshot_hash: ContentHash,
    pub policy_bundle_generation: PolicyBundleGeneration,
    pub weight_source: ModelWeightSource,
    pub decision_at: DateTime<Utc>,
    pub topn_overlap: Probability,
    #[sea_orm(column_type = "JsonBinary")]
    pub rank_delta_json: ShadowRankDelta,
    #[sea_orm(column_type = "JsonBinary")]
    pub score_delta_json: ShadowScoreDelta,
    #[sea_orm(column_type = "JsonBinary", nullable)]
    pub matured_outcome_json: Option<ShadowMaturedOutcomeDelta>,
    pub hard_divergence: bool,
    pub comparison_hash: ContentHash,
    pub created_at: DateTime<Utc>,

    #[sea_orm(
        belongs_to,
        relation_enum = "ActiveVersion",
        from = "active_model_version_id",
        to = "model_version_id"
    )]
    pub active_version: BelongsTo<quant_model_version::Entity>,
    #[sea_orm(
        belongs_to,
        relation_enum = "ShadowVersion",
        from = "shadow_model_version_id",
        to = "model_version_id"
    )]
    pub shadow_version: BelongsTo<quant_model_version::Entity>,
    #[sea_orm(
        belongs_to,
        from = "research_profile_artifact_id",
        to = "research_profile_artifact_id"
    )]
    pub research_profile: BelongsTo<research_profile_artifact::Entity>,
}

impl ActiveModelBehavior for ActiveModel {}
