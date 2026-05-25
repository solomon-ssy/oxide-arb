//! `outbox_event` table entity.

use crate::enums::outbox::{OutboxAggregateType, OutboxEventType};
use crate::types::{AggregateId, OutboxEventId};
use chrono::{DateTime, Utc};
use sea_orm::entity::prelude::*;

#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "outbox_event")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub event_id: OutboxEventId,
    pub aggregate_type: OutboxAggregateType,
    pub aggregate_id: AggregateId,
    pub event_type: OutboxEventType,
    #[sea_orm(column_type = "JsonBinary")]
    pub payload: serde_json::Value,
    pub publish_attempts: i32,
    pub published_at: Option<DateTime<Utc>>,
    #[sea_orm(column_type = "Text", nullable)]
    pub last_error: Option<String>,
    #[sea_orm(column_type = "Text", nullable)]
    pub dead_letter_reason: Option<String>,
    pub created_at: DateTime<Utc>,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {}

impl ActiveModelBehavior for ActiveModel {}
