//! `emergency_snapshots` table entity.

use crate::enums::risk::CircuitBreakerLevel;
use chrono::{DateTime, Utc};
use sea_orm::entity::prelude::*;

#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "emergency_snapshot")]
pub struct Model {
    #[sea_orm(primary_key)]
    pub id: i64,
    pub trigger_level: CircuitBreakerLevel,
    #[sea_orm(column_type = "Text")]
    pub reason: String,
    #[sea_orm(column_type = "JsonBinary")]
    pub risk_state: serde_json::Value,
    pub open_positions_count: i32,
    pub open_reservations_count: i32,
    pub triggered_at: DateTime<Utc>,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {}

impl ActiveModelBehavior for ActiveModel {}
