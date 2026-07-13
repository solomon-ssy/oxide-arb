//! `quant_order_intent` table entity.

use crate::{
    enums::{
        execution::{ExitReason, ExitState, OrderIntentKind},
        quant::{ApprovalStatus, EntryTriggerState, OrderIntentStatus, QuantRuntimeMode},
    },
    types::{
        ContentHash, EntryOrderSpec, EntryTrigger, ExitPolicySpec, ModelVersionId, OrderIntentId,
        Price, RecommendationId, RuntimeConfigVersionId, ScaleOutState,
    },
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
    pub runtime_config_version_id: RuntimeConfigVersionId,
    pub model_version_id: ModelVersionId,
    pub intent_kind: OrderIntentKind,
    pub status: OrderIntentStatus,
    pub approval_status: ApprovalStatus,
    pub approved_by: Option<Uuid>,
    #[sea_orm(column_type = "Text", nullable)]
    pub approval_reason: Option<String>,
    pub approved_at: Option<DateTime<Utc>>,
    #[sea_orm(column_type = "Text", nullable)]
    pub policy_id: Option<String>,
    pub policy_hash: Option<ContentHash>,
    #[sea_orm(column_type = "Text", nullable)]
    pub status_reason: Option<String>,
    #[sea_orm(column_type = "Text", nullable)]
    pub admission_trace_ref: Option<String>,
    #[sea_orm(column_type = "JsonBinary")]
    pub entry_trigger_json: EntryTrigger,
    #[sea_orm(column_type = "JsonBinary")]
    pub entry_order_json: EntryOrderSpec,
    #[sea_orm(column_type = "JsonBinary")]
    pub exit_policy_json: ExitPolicySpec,
    pub risk_envelope_hash: ContentHash,
    pub expires_at: DateTime<Utc>,
    pub entry_trigger_state: EntryTriggerState,
    pub trigger_confirming_since: Option<DateTime<Utc>>,
    pub trigger_last_observed_at: Option<DateTime<Utc>>,
    pub trigger_ready_at: Option<DateTime<Utc>>,
    pub exit_state: ExitState,
    pub exit_reason: Option<ExitReason>,
    pub next_check_at: Option<DateTime<Utc>>,
    pub peak_mark_price: Option<Price>,
    pub last_signal_recheck_at: Option<DateTime<Utc>>,
    #[sea_orm(column_type = "JsonBinary")]
    pub scale_out_state: ScaleOutState,
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
    #[sea_orm(has_one = "super::quant_capital_allocation::Entity")]
    CapitalAllocation,
    #[sea_orm(has_one = "super::quant_position::Entity")]
    Position,
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

impl Related<super::quant_capital_allocation::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::CapitalAllocation.def()
    }
}

impl Related<super::quant_position::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::Position.def()
    }
}

impl ActiveModelBehavior for ActiveModel {}
