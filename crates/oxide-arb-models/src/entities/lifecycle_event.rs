//! `lifecycle_events` table entity.

use chrono::{DateTime, Utc};
use sea_orm::entity::prelude::*;

#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "lifecycle_event")]
pub struct Model {
    #[sea_orm(primary_key)]
    pub id: i64,
    #[sea_orm(column_type = "Text")]
    pub phase: String,
    #[sea_orm(column_type = "Text", nullable)]
    pub stage: Option<String>,
    #[sea_orm(column_type = "Text")]
    pub message: String,
    #[sea_orm(column_type = "Text", nullable)]
    pub metadata: Option<String>,
    pub created_at: DateTime<Utc>,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {}

impl ActiveModelBehavior for ActiveModel {}
