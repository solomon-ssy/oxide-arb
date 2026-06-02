//! `control_factor_audit_event` table entity.

use crate::{
    enums::control_factor::ControlAuditEventType,
    types::{ControlFactorId, FactorPublicationId},
};
use chrono::{DateTime, Utc};
use sea_orm::entity::prelude::*;

#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "control_factor_audit_event")]
pub struct Model {
    #[sea_orm(primary_key)]
    pub id: i64,
    pub event_type: ControlAuditEventType,
    pub factor_id: Option<ControlFactorId>,
    pub publication_id: Option<FactorPublicationId>,
    #[sea_orm(column_type = "Text")]
    pub actor: String,
    #[sea_orm(column_type = "Text")]
    pub reason: String,
    #[sea_orm(column_type = "JsonBinary")]
    pub payload: Json,
    pub created_at: DateTime<Utc>,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {
    #[sea_orm(
        belongs_to = "super::control_factor_value::Entity",
        from = "Column::FactorId",
        to = "super::control_factor_value::Column::FactorId"
    )]
    Factor,
    #[sea_orm(
        belongs_to = "super::control_factor_publication::Entity",
        from = "Column::PublicationId",
        to = "super::control_factor_publication::Column::PublicationId"
    )]
    Publication,
}

impl Related<super::control_factor_value::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::Factor.def()
    }
}

impl Related<super::control_factor_publication::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::Publication.def()
    }
}

impl ActiveModelBehavior for ActiveModel {}
