//! `quant_model_candidate_manifest` schema snapshot.

use sea_orm::entity::prelude::*;

#[sea_orm::model]
#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "quant_model_candidate_manifest")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub manifest_id: Uuid,
    pub feedback_cycle_id: Uuid,
    #[sea_orm(column_type = "Text")]
    pub candidate_recipe_hash: String,
    pub model_version_id: Uuid,
    #[sea_orm(column_type = "Text")]
    pub promotion_gate_hash: String,
    #[sea_orm(column_type = "Text")]
    pub manifest_hash: String,
    #[sea_orm(column_type = "JsonBinary")]
    pub document: Json,
    pub created_at: DateTimeWithTimeZone,
    #[sea_orm(
        belongs_to,
        from = "feedback_cycle_id",
        to = "feedback_cycle_id",
        on_update = "NoAction",
        on_delete = "NoAction"
    )]
    pub quant_feedback_cycle: BelongsTo<super::quant_feedback_cycle::Entity>,
    #[sea_orm(
        belongs_to,
        from = "model_version_id",
        to = "model_version_id",
        on_update = "NoAction",
        on_delete = "NoAction"
    )]
    pub quant_model_version: BelongsTo<super::quant_model_version::Entity>,
}

impl ActiveModelBehavior for ActiveModel {}
