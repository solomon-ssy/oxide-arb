//! `control_factor_publication_factor` membership table entity.

use crate::types::{ControlFactorId, FactorPublicationId};
use chrono::{DateTime, Utc};
use sea_orm::entity::prelude::*;

#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "control_factor_publication_factor")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub publication_id: FactorPublicationId,
    #[sea_orm(primary_key, auto_increment = false)]
    pub factor_id: ControlFactorId,
    pub created_at: DateTime<Utc>,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {
    #[sea_orm(
        belongs_to = "super::control_factor_publication::Entity",
        from = "Column::PublicationId",
        to = "super::control_factor_publication::Column::PublicationId"
    )]
    Publication,
    #[sea_orm(
        belongs_to = "super::control_factor_value::Entity",
        from = "Column::FactorId",
        to = "super::control_factor_value::Column::FactorId"
    )]
    Factor,
}

impl Related<super::control_factor_publication::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::Publication.def()
    }
}

impl Related<super::control_factor_value::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::Factor.def()
    }
}

impl ActiveModelBehavior for ActiveModel {}
