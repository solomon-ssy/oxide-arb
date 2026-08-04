//! `quant_feedback_recipe_template` immutable catalog entity.

use chrono::{DateTime, Utc};
use sea_orm::entity::prelude::*;

use super::{quant_model_spec, research_profile_artifact, user};
use crate::{
    domain::ports::FeedbackRecipeTemplate,
    enums::{model::ModelFamily, quant::FeedbackRecipeTemplateStatus},
    runtime_config::BuyModelRoute,
    types::{
        ContentHash, FeedbackRecipeTemplateId, ModelSpecId, ResearchProfileArtifactId, RoleCode,
        UserId,
    },
};

#[sea_orm::model]
#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "quant_feedback_recipe_template")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub recipe_template_id: FeedbackRecipeTemplateId,
    #[sea_orm(primary_key, auto_increment = false)]
    pub revision: i32,
    pub research_profile_artifact_id: ResearchProfileArtifactId,
    #[sea_orm(column_type = "JsonBinary")]
    pub route: BuyModelRoute,
    pub model_family: ModelFamily,
    pub model_spec_id: ModelSpecId,
    pub status: FeedbackRecipeTemplateStatus,
    pub catalog_priority: i32,
    pub approved_by_user_id: Option<UserId>,
    pub approved_by_role: Option<RoleCode>,
    pub approved_at: Option<DateTime<Utc>>,
    pub governance_reason: String,
    pub template_hash: ContentHash,
    #[sea_orm(column_type = "JsonBinary")]
    pub template: FeedbackRecipeTemplate,
    #[sea_orm(default_expr = "Expr::current_timestamp()")]
    pub created_at: DateTime<Utc>,

    #[sea_orm(
        belongs_to,
        relation_enum = "ResearchProfileArtifact",
        from = "research_profile_artifact_id",
        to = "research_profile_artifact_id"
    )]
    pub research_profile_artifact: BelongsTo<research_profile_artifact::Entity>,
    #[sea_orm(
        belongs_to,
        relation_enum = "ModelSpec",
        from = "model_spec_id",
        to = "model_spec_id"
    )]
    pub model_spec: BelongsTo<quant_model_spec::Entity>,
    #[sea_orm(
        belongs_to,
        relation_enum = "ApprovedByUser",
        from = "approved_by_user_id",
        to = "id"
    )]
    pub approved_by_user: BelongsTo<Option<user::Entity>>,
}

impl ActiveModelBehavior for ActiveModel {}
