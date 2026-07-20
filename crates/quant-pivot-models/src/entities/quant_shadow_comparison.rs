//! `quant_shadow_comparison` table entity.

use crate::{
    enums::quant::ModelWeightSource,
    types::{
        ContentHash, ModelVersionId, Probability, ShadowComparisonId,
        shadow::{ShadowMaturedOutcomeDelta, ShadowRankDelta, ShadowScoreDelta},
    },
};
use chrono::{DateTime, Utc};
use sea_orm::entity::prelude::*;

#[sea_orm::model]
#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "quant_shadow_comparison")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub shadow_comparison_id: ShadowComparisonId,
    pub active_model_version_id: ModelVersionId,
    pub shadow_model_version_id: ModelVersionId,
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
    pub active_version: BelongsTo<super::quant_model_version::Entity>,
    #[sea_orm(
        belongs_to,
        relation_enum = "ShadowVersion",
        from = "shadow_model_version_id",
        to = "model_version_id"
    )]
    pub shadow_version: BelongsTo<super::quant_model_version::Entity>,
}

impl ActiveModelBehavior for ActiveModel {}
