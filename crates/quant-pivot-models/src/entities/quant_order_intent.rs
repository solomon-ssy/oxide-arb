//! `quant_order_intent` table entity.

use crate::{
    enums::quant::{ApprovalStatus, OrderIntentStatus, QuantRuntimeMode},
    types::{ContentHash, EntryOrderSpec, ExitPolicySpec, OrderIntentId, RecommendationId},
};
use chrono::{DateTime, Utc};
use sea_orm::entity::prelude::*;
use uuid::Uuid;

#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "quant_order_intent")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub order_intent_id: OrderIntentId,
    pub recommendation_id: RecommendationId,
    pub runtime_mode: QuantRuntimeMode,
    #[sea_orm(column_type = "Text")]
    pub intent_kind: String,
    pub status: OrderIntentStatus,
    pub approval_status: ApprovalStatus,
    pub approved_by: Option<Uuid>,
    #[sea_orm(column_type = "Text", nullable)]
    pub approval_reason: Option<String>,
    pub approved_at: Option<DateTime<Utc>>,
    #[sea_orm(column_type = "JsonBinary")]
    pub entry_order_json: EntryOrderSpec,
    #[sea_orm(column_type = "JsonBinary")]
    pub exit_policy_json: ExitPolicySpec,
    pub risk_envelope_hash: ContentHash,
    pub expires_at: DateTime<Utc>,
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
    #[sea_orm(has_many = "super::quant_execution_order::Entity")]
    ExecutionOrder,
}

impl Related<super::quant_recommendation::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::Recommendation.def()
    }
}

impl Related<super::quant_execution_order::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::ExecutionOrder.def()
    }
}

impl ActiveModelBehavior for ActiveModel {}
