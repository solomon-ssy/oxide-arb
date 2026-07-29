//! Immutable governed research-profile registry.

use chrono::{DateTime, Utc};
use sea_orm::entity::prelude::*;

use super::{
    quant_feedback_cycle, quant_model_version, quant_order_intent, quant_recommendation,
    quant_recommendation_report,
};
use crate::types::{
    ContentHash, ResearchProfileArtifactId, ResearchProfileId, ResearchProfileSpec,
};

#[sea_orm::model]
#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "research_profile_artifact")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub research_profile_artifact_id: ResearchProfileArtifactId,
    pub research_profile_id: ResearchProfileId,
    pub version: i32,
    pub content_hash: ContentHash,
    #[sea_orm(column_type = "JsonBinary")]
    pub spec: ResearchProfileSpec,
    #[sea_orm(column_type = "Text")]
    pub published_by: String,
    pub published_at: DateTime<Utc>,
    #[sea_orm(column_type = "Text")]
    pub governance_reason: String,
    #[sea_orm(default_expr = "Expr::current_timestamp()")]
    pub created_at: DateTime<Utc>,

    #[sea_orm(has_many, relation_enum = "RecommendationReport")]
    pub recommendation_report: HasMany<quant_recommendation_report::Entity>,
    #[sea_orm(has_many, relation_enum = "Recommendation")]
    pub recommendation: HasMany<quant_recommendation::Entity>,
    #[sea_orm(has_many, relation_enum = "ModelVersion")]
    pub model_version: HasMany<quant_model_version::Entity>,
    #[sea_orm(has_many, relation_enum = "OrderIntent")]
    pub order_intent: HasMany<quant_order_intent::Entity>,
    #[sea_orm(has_many, relation_enum = "FeedbackCycle")]
    pub feedback_cycle: HasMany<quant_feedback_cycle::Entity>,
}

impl ActiveModelBehavior for ActiveModel {}
