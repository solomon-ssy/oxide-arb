//! Durable derived state for one configured report schedule.

use chrono::{DateTime, Utc};
use sea_orm::entity::prelude::*;

use crate::types::{ContentHash, RuntimeConfigVersionId};

#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "quant_report_schedule_state")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub schedule_id: String,
    pub runtime_config_version_id: RuntimeConfigVersionId,
    pub spec_hash: ContentHash,
    pub next_scheduled_for: DateTime<Utc>,
    pub last_materialized_for: Option<DateTime<Utc>>,
    pub enabled: bool,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {
    #[sea_orm(
        belongs_to = "super::runtime_config_version::Entity",
        from = "Column::RuntimeConfigVersionId",
        to = "super::runtime_config_version::Column::RuntimeConfigVersionId"
    )]
    RuntimeConfigVersion,
}

impl Related<super::runtime_config_version::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::RuntimeConfigVersion.def()
    }
}

impl ActiveModelBehavior for ActiveModel {}
