//! `quant_attribution_artifact` immutable artifact index.

use chrono::{DateTime, Utc};
use sea_orm::entity::prelude::*;

use super::{quant_feedback_cycle, quant_model_version, quant_order_intent, quant_recommendation};
use crate::{
    enums::quant::{AttributionArtifactKind, AttributionCohort},
    types::{
        ArtifactUri, AttributionArtifactId, ContentHash, FeedbackCycleId, ModelVersionId,
        OrderIntentId, RecommendationId,
    },
};

#[sea_orm::model]
#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "quant_attribution_artifact")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub attribution_artifact_id: AttributionArtifactId,
    pub artifact_kind: AttributionArtifactKind,
    pub source_cohort: AttributionCohort,
    pub source_feedback_cycle_id: FeedbackCycleId,
    pub model_version_id: Option<ModelVersionId>,
    pub recommendation_id: Option<RecommendationId>,
    pub order_intent_id: Option<OrderIntentId>,
    pub artifact_uri: ArtifactUri,
    pub artifact_hash: ContentHash,
    pub source_cutoff: DateTime<Utc>,
    pub available_at: DateTime<Utc>,
    pub created_at: DateTime<Utc>,

    #[sea_orm(
        belongs_to,
        relation_enum = "FeedbackCycle",
        from = "source_feedback_cycle_id",
        to = "feedback_cycle_id"
    )]
    pub feedback_cycle: BelongsTo<quant_feedback_cycle::Entity>,
    #[sea_orm(
        belongs_to,
        relation_enum = "ModelVersion",
        from = "model_version_id",
        to = "model_version_id"
    )]
    pub model_version: BelongsTo<Option<quant_model_version::Entity>>,
    #[sea_orm(
        belongs_to,
        relation_enum = "Recommendation",
        from = "recommendation_id",
        to = "recommendation_id"
    )]
    pub recommendation: BelongsTo<Option<quant_recommendation::Entity>>,
    #[sea_orm(
        belongs_to,
        relation_enum = "OrderIntent",
        from = "order_intent_id",
        to = "order_intent_id"
    )]
    pub order_intent: BelongsTo<Option<quant_order_intent::Entity>>,
}

impl ActiveModelBehavior for ActiveModel {}
