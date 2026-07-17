//! Append-only runtime-config approval decisions.

use crate::{
    enums::runtime_config::RuntimeConfigApprovalDecision,
    types::{ContentHash, RuntimeConfigApprovalId, RuntimeConfigVersionId},
};
use chrono::{DateTime, Utc};
use sea_orm::entity::prelude::*;

#[sea_orm::model]
#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "runtime_config_approval")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub runtime_config_approval_id: RuntimeConfigApprovalId,
    pub runtime_config_version_id: RuntimeConfigVersionId,
    pub config_hash: ContentHash,
    pub decision: RuntimeConfigApprovalDecision,
    #[sea_orm(column_type = "Text")]
    pub decided_by: String,
    #[sea_orm(column_type = "Text")]
    pub reason: String,
    pub decided_at: DateTime<Utc>,
    pub expires_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,

    #[sea_orm(
        belongs_to,
        relation_enum = "Version",
        from = "runtime_config_version_id",
        to = "runtime_config_version_id"
    )]
    pub version: BelongsTo<super::runtime_config_version::Entity>,
}

impl ActiveModelBehavior for ActiveModel {}
