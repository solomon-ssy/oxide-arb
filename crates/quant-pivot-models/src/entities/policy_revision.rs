//! Immutable document revisions for independently governed policy resources.

use chrono::{DateTime, Utc};
use sea_orm::entity::prelude::*;

use super::{policy_activation, policy_approval, user};
use crate::{
    enums::runtime_config::{ConfigResourceKind, PolicyActorKind, PolicyRevisionStatus},
    runtime_config::{PolicyDocument, PolicyValidationEvidence},
    types::{ContentHash, PolicyRevisionId, SchemaVersion, UserId},
};

#[sea_orm::model]
#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "policy_revision")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub policy_revision_id: PolicyRevisionId,
    pub resource_kind: ConfigResourceKind,
    pub schema_version: SchemaVersion,
    #[sea_orm(unique)]
    pub revision_hash: ContentHash,
    #[sea_orm(column_type = "JsonBinary")]
    pub document: PolicyDocument,
    pub status: PolicyRevisionStatus,
    #[sea_orm(column_type = "JsonBinary")]
    pub validation_evidence: Option<PolicyValidationEvidence>,
    pub validated_at: Option<DateTime<Utc>>,
    pub preflight_token_hash: Option<ContentHash>,
    pub preflight_expires_at: Option<DateTime<Utc>>,
    pub created_by_kind: PolicyActorKind,
    pub created_by_user_id: Option<UserId>,
    #[sea_orm(column_type = "Text")]
    pub created_by_label: String,
    #[sea_orm(column_type = "Text")]
    pub reason: String,
    #[sea_orm(default_expr = "Expr::current_timestamp()")]
    pub created_at: DateTime<Utc>,

    #[sea_orm(has_many, relation_enum = "Approval")]
    pub approval: HasMany<policy_approval::Entity>,
    #[sea_orm(has_many, relation_enum = "Activation")]
    pub activation: HasMany<policy_activation::Entity>,
    #[sea_orm(
        belongs_to,
        relation_enum = "CreatedByUser",
        from = "created_by_user_id",
        to = "id"
    )]
    pub created_by_user: BelongsTo<Option<user::Entity>>,
}

impl ActiveModelBehavior for ActiveModel {}
