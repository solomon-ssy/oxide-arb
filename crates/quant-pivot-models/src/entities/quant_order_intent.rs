//! `quant_order_intent` table entity.

use crate::{
    enums::{
        execution::{ExitReason, ExitState, OrderIntentKind},
        quant::{ApprovalStatus, OrderIntentStatus, QuantRuntimeMode},
    },
    types::{
        ContentHash, EntryConditionInstanceId, EntryOrderSpec, ExitPolicySpec,
        ExitReinferenceObservation, ModelVersionId, OrderIntentId, Price, RecommendationId,
        ResearchProfileRef, RuntimeConfigVersionId, ScaleOutState,
    },
};
use chrono::{DateTime, Utc};
use sea_orm::entity::prelude::*;
use uuid::Uuid;

#[sea_orm::model]
#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "quant_order_intent")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub order_intent_id: OrderIntentId,
    pub recommendation_id: RecommendationId,
    pub runtime_mode: QuantRuntimeMode,
    pub runtime_config_version_id: RuntimeConfigVersionId,
    pub model_version_id: ModelVersionId,
    #[sea_orm(column_type = "JsonBinary")]
    pub profile_ref: ResearchProfileRef,
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
    pub condition_instance_id: EntryConditionInstanceId,
    #[sea_orm(column_type = "JsonBinary")]
    pub entry_order_json: EntryOrderSpec,
    #[sea_orm(column_type = "JsonBinary")]
    pub exit_policy_json: ExitPolicySpec,
    pub risk_envelope_hash: ContentHash,
    pub expires_at: DateTime<Utc>,
    pub exit_state: ExitState,
    pub exit_reason: Option<ExitReason>,
    pub next_check_at: Option<DateTime<Utc>>,
    pub peak_mark_price: Option<Price>,
    pub last_signal_recheck_at: Option<DateTime<Utc>>,
    #[sea_orm(column_type = "JsonBinary", nullable)]
    pub latest_reinference_json: Option<ExitReinferenceObservation>,
    #[sea_orm(column_type = "JsonBinary")]
    pub scale_out_state: ScaleOutState,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,

    #[sea_orm(
        belongs_to,
        relation_enum = "Recommendation",
        from = "recommendation_id",
        to = "recommendation_id"
    )]
    pub recommendation: BelongsTo<super::quant_recommendation::Entity>,
    #[sea_orm(
        belongs_to,
        relation_enum = "ConditionInstance",
        from = "condition_instance_id",
        to = "condition_instance_id"
    )]
    pub condition_instance: BelongsTo<super::quant_entry_condition_instance::Entity>,
    #[sea_orm(has_many, relation_enum = "ExecutionOrder")]
    pub execution_order: HasMany<super::quant_execution_order::Entity>,
    #[sea_orm(has_one, relation_enum = "CapitalAllocation")]
    pub capital_allocation: HasOne<super::quant_capital_allocation::Entity>,
    #[sea_orm(has_one, relation_enum = "Position")]
    pub position: HasOne<super::quant_position::Entity>,
}

impl ActiveModelBehavior for ActiveModel {}
