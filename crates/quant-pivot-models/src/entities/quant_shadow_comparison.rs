//! `quant_shadow_comparison` table entity.

use crate::types::{ContentHash, ModelVersionId, Probability, ShadowComparisonId};
use chrono::{DateTime, Utc};
use sea_orm::entity::prelude::*;

#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "quant_shadow_comparison")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub shadow_comparison_id: ShadowComparisonId,
    pub active_model_version_id: ModelVersionId,
    pub shadow_model_version_id: ModelVersionId,
    pub decision_at: DateTime<Utc>,
    pub topn_overlap: Probability,
    #[sea_orm(column_type = "JsonBinary")]
    pub rank_delta_json: Json,
    #[sea_orm(column_type = "JsonBinary")]
    pub score_delta_json: Json,
    #[sea_orm(column_type = "JsonBinary")]
    pub matured_outcome_json: Option<Json>,
    pub hard_divergence: bool,
    pub comparison_hash: ContentHash,
    pub created_at: DateTime<Utc>,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {
    #[sea_orm(
        belongs_to = "super::quant_model_version::Entity",
        from = "Column::ActiveModelVersionId",
        to = "super::quant_model_version::Column::ModelVersionId"
    )]
    ActiveVersion,
    #[sea_orm(
        belongs_to = "super::quant_model_version::Entity",
        from = "Column::ShadowModelVersionId",
        to = "super::quant_model_version::Column::ModelVersionId"
    )]
    ShadowVersion,
}

impl ActiveModelBehavior for ActiveModel {}
