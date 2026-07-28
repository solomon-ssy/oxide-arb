//! Append-only audit event written atomically with a policy activation.

use chrono::{DateTime, Utc};
use sea_orm::entity::prelude::*;

use super::{decision_policy_snapshot, policy_activation, user};
use crate::{
    enums::runtime_config::{ConfigResourceKind, PolicyActorKind},
    types::{
        AuditEventId, ContentHash, DecisionPolicySnapshotId, ModelGovernanceAuditId,
        PolicyActivationId, PolicyBundleGeneration, PolicyRevisionId, PromotionPermitId, UserId,
    },
};

#[sea_orm::model]
#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "policy_activation_audit")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub audit_event_id: AuditEventId,
    #[sea_orm(unique)]
    pub policy_activation_id: PolicyActivationId,
    pub bundle_generation: PolicyBundleGeneration,
    pub resource_kind: ConfigResourceKind,
    pub policy_revision_id: PolicyRevisionId,
    pub decision_policy_snapshot_id: DecisionPolicySnapshotId,
    pub snapshot_hash: ContentHash,
    pub activation_request_hash: ContentHash,
    pub actor_kind: PolicyActorKind,
    pub actor_user_id: Option<UserId>,
    #[sea_orm(column_type = "Text")]
    pub actor_label: String,
    #[sea_orm(column_type = "Text")]
    pub reason: String,
    pub occurred_at: DateTime<Utc>,
    pub promotion_permit_id: Option<PromotionPermitId>,
    pub promotion_transaction_hash: Option<ContentHash>,
    pub model_governance_audit_id: Option<ModelGovernanceAuditId>,
    #[sea_orm(default_expr = "Expr::current_timestamp()")]
    pub created_at: DateTime<Utc>,

    #[sea_orm(
        belongs_to,
        relation_enum = "Activation",
        from = "policy_activation_id",
        to = "policy_activation_id"
    )]
    pub activation: BelongsTo<policy_activation::Entity>,
    #[sea_orm(
        belongs_to,
        relation_enum = "Snapshot",
        from = "decision_policy_snapshot_id",
        to = "decision_policy_snapshot_id"
    )]
    pub snapshot: BelongsTo<decision_policy_snapshot::Entity>,
    #[sea_orm(
        belongs_to,
        relation_enum = "ActorUser",
        from = "actor_user_id",
        to = "id"
    )]
    pub actor_user: BelongsTo<Option<user::Entity>>,
}

impl ActiveModelBehavior for ActiveModel {}
