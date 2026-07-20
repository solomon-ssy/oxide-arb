//! `policy_activation` table entity.

use crate::{
    enums::runtime_config::{ConfigResourceKind, PolicyActivationKind, PolicyActorKind},
    types::{
        AuditEventId, ContentHash, DecisionPolicySnapshotId, PolicyActivationId, PolicyApprovalId,
        PolicyBundleGeneration, PolicyIdempotencyKey, PolicyRevisionId, UserId,
    },
};
use chrono::{DateTime, Utc};
use sea_orm::entity::prelude::*;

#[sea_orm::model]
#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "policy_activation")]
pub struct Model {
    pub bundle_generation: PolicyBundleGeneration,
    pub expected_bundle_generation: PolicyBundleGeneration,
    #[sea_orm(primary_key, auto_increment = false)]
    pub policy_activation_id: PolicyActivationId,
    pub resource_kind: ConfigResourceKind,
    pub policy_revision_id: PolicyRevisionId,
    pub decision_policy_snapshot_id: DecisionPolicySnapshotId,
    #[sea_orm(unique)]
    pub policy_approval_id: PolicyApprovalId,
    #[sea_orm(default_expr = "Expr::current_timestamp()")]
    pub activated_at: DateTime<Utc>,
    pub activated_by_kind: PolicyActorKind,
    pub activated_by_user_id: Option<UserId>,
    #[sea_orm(column_type = "Text")]
    pub activated_by_label: String,
    #[sea_orm(column_type = "Text")]
    pub reason: String,
    pub activation_kind: PolicyActivationKind,
    pub expected_active_revision_id: Option<PolicyRevisionId>,
    pub previous_policy_revision_id: Option<PolicyRevisionId>,
    pub rollback_target_revision_id: Option<PolicyRevisionId>,
    pub preflight_token_hash: ContentHash,
    #[sea_orm(unique)]
    #[sea_orm(column_type = "Text")]
    pub idempotency_key: PolicyIdempotencyKey,
    pub activation_request_hash: ContentHash,
    pub audit_event_id: AuditEventId,
    #[sea_orm(default_expr = "Expr::current_timestamp()")]
    pub created_at: DateTime<Utc>,

    #[sea_orm(
        belongs_to,
        relation_enum = "Revision",
        from = "policy_revision_id",
        to = "policy_revision_id"
    )]
    pub revision: BelongsTo<super::policy_revision::Entity>,
    #[sea_orm(
        belongs_to,
        relation_enum = "Approval",
        from = "policy_approval_id",
        to = "policy_approval_id"
    )]
    pub approval: BelongsTo<super::policy_approval::Entity>,

    #[sea_orm(
        belongs_to,
        relation_enum = "Snapshot",
        from = "decision_policy_snapshot_id",
        to = "decision_policy_snapshot_id"
    )]
    pub snapshot: BelongsTo<super::decision_policy_snapshot::Entity>,
    #[sea_orm(
        belongs_to,
        relation_enum = "ActivatedByUser",
        from = "activated_by_user_id",
        to = "id"
    )]
    pub activated_by_user: BelongsTo<Option<super::user::Entity>>,
}

impl ActiveModelBehavior for ActiveModel {}
