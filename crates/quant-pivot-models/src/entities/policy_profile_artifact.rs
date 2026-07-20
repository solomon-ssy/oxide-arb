//! Content-addressed immutable policy-profile definitions.

use chrono::{DateTime, Utc};
use sea_orm::entity::prelude::*;

use crate::{
    enums::runtime_config::{PolicyActorKind, ProfileArtifactKind},
    runtime_config::PolicyProfileDocument,
    types::{ContentHash, ProfileArtifactId, SchemaVersion, UserId},
};

#[sea_orm::model]
#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "policy_profile_artifact")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub profile_artifact_id: ProfileArtifactId,
    pub kind: ProfileArtifactKind,
    pub schema_version: SchemaVersion,
    #[sea_orm(column_type = "JsonBinary")]
    pub document: PolicyProfileDocument,
    pub content_hash: ContentHash,
    pub created_by_kind: PolicyActorKind,
    pub created_by_user_id: Option<UserId>,
    #[sea_orm(column_type = "Text")]
    pub created_by_label: String,
    #[sea_orm(column_type = "Text")]
    pub reason: String,
    #[sea_orm(default_expr = "Expr::current_timestamp()")]
    pub created_at: DateTime<Utc>,

    #[sea_orm(
        belongs_to,
        relation_enum = "CreatedByUser",
        from = "created_by_user_id",
        to = "id"
    )]
    pub created_by_user: BelongsTo<Option<super::user::Entity>>,
}

impl ActiveModelBehavior for ActiveModel {}
