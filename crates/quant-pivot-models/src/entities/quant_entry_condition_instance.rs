//! Recommendation-level durable entry-condition state.

use chrono::{DateTime, Utc};
use sea_orm::entity::prelude::*;
use uuid::Uuid;

use crate::{
    enums::quant::EntryConditionState,
    types::{
        ConditionTruth, ContentHash, EntryConditionArtifactId, EntryConditionFoldState,
        EntryConditionInstanceId, OrderIntentId, RecommendationId,
    },
};

#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "quant_entry_condition_instance")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub condition_instance_id: EntryConditionInstanceId,
    pub recommendation_id: RecommendationId,
    pub artifact_id: Option<EntryConditionArtifactId>,
    pub artifact_hash: Option<ContentHash>,
    pub state: EntryConditionState,
    #[sea_orm(column_type = "JsonBinary", nullable)]
    pub truth_json: Option<ConditionTruth>,
    pub revision: i64,
    pub evaluation_hash: Option<ContentHash>,
    pub input_fingerprint: Option<ContentHash>,
    pub continuity_hash: Option<ContentHash>,
    #[sea_orm(column_type = "JsonBinary")]
    pub fold_state_json: EntryConditionFoldState,
    pub confirmation_started_at: Option<DateTime<Utc>>,
    pub last_evaluated_at: Option<DateTime<Utc>>,
    pub next_evaluation_at: Option<DateTime<Utc>>,
    pub expires_at: DateTime<Utc>,
    pub lease_owner: Option<Uuid>,
    pub lease_expires_at: Option<DateTime<Utc>>,
    pub lease_epoch: i64,
    pub claimed_by_intent_id: Option<OrderIntentId>,
    pub claim_admission_state_version: Option<ContentHash>,
    pub consumed_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {
    #[sea_orm(
        belongs_to = "super::quant_recommendation::Entity",
        from = "Column::RecommendationId",
        to = "super::quant_recommendation::Column::RecommendationId"
    )]
    Recommendation,
    #[sea_orm(
        belongs_to = "super::quant_entry_condition_artifact::Entity",
        from = "Column::ArtifactId",
        to = "super::quant_entry_condition_artifact::Column::ArtifactId"
    )]
    Artifact,
    #[sea_orm(has_many = "super::quant_entry_condition_audit::Entity")]
    Audit,
    #[sea_orm(has_many = "super::quant_order_intent::Entity")]
    OrderIntent,
}

impl Related<super::quant_recommendation::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::Recommendation.def()
    }
}

impl Related<super::quant_entry_condition_artifact::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::Artifact.def()
    }
}

impl Related<super::quant_entry_condition_audit::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::Audit.def()
    }
}

impl Related<super::quant_order_intent::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::OrderIntent.def()
    }
}

impl ActiveModelBehavior for ActiveModel {}
