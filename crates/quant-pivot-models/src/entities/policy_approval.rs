//! Append-only policy approval decisions.

use crate::{
    enums::runtime_config::{ConfigResourceKind, PolicyActorKind, PolicyApprovalDecision},
    runtime_config::PolicyValidationSubject,
    types::{ContentHash, PolicyApprovalId, PolicyRevisionId, UserId},
};
use chrono::{DateTime, Utc};
use sea_orm::entity::prelude::*;

#[sea_orm::model]
#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "policy_approval")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub policy_approval_id: PolicyApprovalId,
    pub policy_revision_id: PolicyRevisionId,
    pub resource_kind: ConfigResourceKind,
    pub revision_hash: ContentHash,
    #[sea_orm(column_type = "JsonBinary")]
    pub validation_subject: Option<PolicyValidationSubject>,
    pub decision: PolicyApprovalDecision,
    pub decided_by_kind: PolicyActorKind,
    pub decided_by_user_id: Option<UserId>,
    #[sea_orm(column_type = "Text")]
    pub decided_by_label: String,
    #[sea_orm(column_type = "Text")]
    pub reason: String,
    pub decided_at: DateTime<Utc>,
    pub expires_at: Option<DateTime<Utc>>,
    #[sea_orm(default_expr = "Expr::current_timestamp()")]
    pub created_at: DateTime<Utc>,

    #[sea_orm(
        belongs_to,
        relation_enum = "Revision",
        from = "policy_revision_id",
        to = "policy_revision_id"
    )]
    pub revision: BelongsTo<super::policy_revision::Entity>,
    #[sea_orm(has_one, relation_enum = "Activation")]
    pub activation: HasOne<super::policy_activation::Entity>,
    #[sea_orm(
        belongs_to,
        relation_enum = "DecidedByUser",
        from = "decided_by_user_id",
        to = "id"
    )]
    pub decided_by_user: BelongsTo<Option<super::user::Entity>>,
}

impl ActiveModelBehavior for ActiveModel {}
