//! `runtime_config_activation` table entity.

use crate::{
    enums::runtime_config::RuntimeConfigActivationKind,
    types::{RuntimeConfigActivationId, RuntimeConfigVersionId},
};
use chrono::{DateTime, Utc};
use sea_orm::entity::prelude::*;

#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "runtime_config_activation")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub runtime_config_activation_id: RuntimeConfigActivationId,
    pub runtime_config_version_id: RuntimeConfigVersionId,
    pub activated_at: DateTime<Utc>,
    #[sea_orm(column_type = "Text")]
    pub activated_by: String,
    #[sea_orm(column_type = "Text")]
    pub reason: String,
    pub activation_kind: RuntimeConfigActivationKind,
    pub previous_runtime_config_version_id: Option<RuntimeConfigVersionId>,
    pub rollback_target_version_id: Option<RuntimeConfigVersionId>,
    pub audit_event_id: Option<i64>,
    pub created_at: DateTime<Utc>,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {
    #[sea_orm(
        belongs_to = "super::runtime_config_version::Entity",
        from = "Column::RuntimeConfigVersionId",
        to = "super::runtime_config_version::Column::RuntimeConfigVersionId"
    )]
    Version,
    #[sea_orm(
        belongs_to = "super::control_factor_audit_event::Entity",
        from = "Column::AuditEventId",
        to = "super::control_factor_audit_event::Column::Id"
    )]
    AuditEvent,
}

impl Related<super::runtime_config_version::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::Version.def()
    }
}

impl Related<super::control_factor_audit_event::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::AuditEvent.def()
    }
}

impl ActiveModelBehavior for ActiveModel {}
