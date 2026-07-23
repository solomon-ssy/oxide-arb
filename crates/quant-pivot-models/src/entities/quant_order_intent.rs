//! `quant_order_intent` table entity.

use chrono::{DateTime, Utc};
use sea_orm::entity::prelude::*;

use super::{
    decision_policy_snapshot, quant_capital_allocation, quant_entry_condition_instance,
    quant_execution_account, quant_execution_order, quant_model_version, quant_position,
    quant_recommendation, quant_settlement_inventory_lot, research_profile_artifact,
};
use crate::{
    enums::{
        execution::{ExitReason, ExitState, OrderIntentKind},
        quant::{ApprovalStatus, OrderIntentStatus, QuantRuntimeMode},
    },
    types::{
        ContentHash, DecisionPolicySnapshotId, EntryConditionInstanceId, EntryOrderSpec,
        ExecutionAccountId, ExitPolicySpec, ExitReinferenceObservation, ModelVersionId,
        OrderIntentId, Price, RecommendationId, ResearchProfileArtifactId, ScaleOutState, UserId,
    },
};

#[sea_orm::model]
#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "quant_order_intent")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub order_intent_id: OrderIntentId,
    pub recommendation_id: RecommendationId,
    pub execution_account_id: ExecutionAccountId,
    pub runtime_mode: QuantRuntimeMode,
    pub decision_policy_snapshot_id: DecisionPolicySnapshotId,
    pub model_version_id: ModelVersionId,
    pub research_profile_artifact_id: ResearchProfileArtifactId,
    pub intent_kind: OrderIntentKind,
    pub status: OrderIntentStatus,
    pub approval_status: ApprovalStatus,
    pub approved_by: Option<UserId>,
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
        relation_enum = "ResearchProfileArtifact",
        from = "research_profile_artifact_id",
        to = "research_profile_artifact_id"
    )]
    pub research_profile_artifact: BelongsTo<research_profile_artifact::Entity>,

    #[sea_orm(
        belongs_to,
        relation_enum = "Recommendation",
        from = "recommendation_id",
        to = "recommendation_id"
    )]
    pub recommendation: BelongsTo<quant_recommendation::Entity>,
    #[sea_orm(
        belongs_to,
        relation_enum = "ExecutionAccount",
        from = "execution_account_id",
        to = "execution_account_id"
    )]
    pub execution_account: BelongsTo<quant_execution_account::Entity>,
    #[sea_orm(
        belongs_to,
        relation_enum = "ConditionInstance",
        from = "condition_instance_id",
        to = "condition_instance_id"
    )]
    pub condition_instance: BelongsTo<quant_entry_condition_instance::Entity>,
    #[sea_orm(
        belongs_to,
        relation_enum = "DecisionPolicySnapshot",
        from = "decision_policy_snapshot_id",
        to = "decision_policy_snapshot_id"
    )]
    pub decision_policy_snapshot: BelongsTo<decision_policy_snapshot::Entity>,
    #[sea_orm(
        belongs_to,
        relation_enum = "ModelVersion",
        from = "model_version_id",
        to = "model_version_id"
    )]
    pub model_version: BelongsTo<quant_model_version::Entity>,
    #[sea_orm(has_many, relation_enum = "ExecutionOrder")]
    pub execution_order: HasMany<quant_execution_order::Entity>,
    #[sea_orm(has_one, relation_enum = "CapitalAllocation")]
    pub capital_allocation: HasOne<quant_capital_allocation::Entity>,
    #[sea_orm(has_one, relation_enum = "Position")]
    pub position: HasOne<quant_position::Entity>,
    #[sea_orm(has_many, relation_enum = "SettlementInventoryLot")]
    pub settlement_inventory_lot: HasMany<quant_settlement_inventory_lot::Entity>,
}

impl ActiveModelBehavior for ActiveModel {}
