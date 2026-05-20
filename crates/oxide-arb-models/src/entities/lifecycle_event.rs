//! `lifecycle_events` table entity.

use crate::enums::lifecycle::{LifecyclePhase, LifecycleRecorder};
use crate::types::OpportunityId;
use chrono::{DateTime, Utc};
use sea_orm::entity::prelude::*;

#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "lifecycle_event")]
pub struct Model {
    #[sea_orm(primary_key)]
    pub id: i64,
    pub opportunity_id: OpportunityId,
    pub phase: LifecyclePhase,
    pub recorder: LifecycleRecorder,
    #[sea_orm(column_type = "Text", nullable)]
    pub detail: Option<String>,
    pub created_at: DateTime<Utc>,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {}

impl ActiveModelBehavior for ActiveModel {}
