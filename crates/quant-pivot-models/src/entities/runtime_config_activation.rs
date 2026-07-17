//! `runtime_config_activation` table entity.

use crate::{
    enums::runtime_config::RuntimeConfigActivationKind,
    types::{
        AuditEventId, RuntimeConfigActivationId, RuntimeConfigApprovalId, RuntimeConfigVersionId,
    },
};
use chrono::{DateTime, Utc};
use sea_orm::entity::prelude::*;

#[sea_orm::model]
#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "runtime_config_activation")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub runtime_config_activation_id: RuntimeConfigActivationId,
    pub runtime_config_version_id: RuntimeConfigVersionId,
    pub runtime_config_approval_id: Option<RuntimeConfigApprovalId>,
    pub activated_at: DateTime<Utc>,
    #[sea_orm(column_type = "Text")]
    pub activated_by: String,
    #[sea_orm(column_type = "Text")]
    pub reason: String,
    pub activation_kind: RuntimeConfigActivationKind,
    pub previous_runtime_config_version_id: Option<RuntimeConfigVersionId>,
    pub rollback_target_version_id: Option<RuntimeConfigVersionId>,
    pub audit_event_id: Option<AuditEventId>,
    pub created_at: DateTime<Utc>,

    #[sea_orm(
        belongs_to,
        relation_enum = "Version",
        from = "runtime_config_version_id",
        to = "runtime_config_version_id"
    )]
    pub version: BelongsTo<super::runtime_config_version::Entity>,
    #[sea_orm(
        belongs_to,
        relation_enum = "Approval",
        from = "runtime_config_approval_id",
        to = "runtime_config_approval_id"
    )]
    pub approval: BelongsTo<Option<super::runtime_config_approval::Entity>>,
}

impl ActiveModelBehavior for ActiveModel {}
