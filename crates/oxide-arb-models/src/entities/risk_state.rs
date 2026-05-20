//! `risk_engine_state` table entity (singleton row).

use crate::enums::risk::{BreakerStateName, CircuitBreakerLevel};
use crate::types::Usd;
use chrono::{DateTime, Utc};
use sea_orm::entity::prelude::*;

#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "risk_engine_state")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub id: i32,
    pub breaker_state: BreakerStateName,
    pub breaker_level: Option<CircuitBreakerLevel>,
    pub breaker_reason: Option<String>,
    pub cooling_until: Option<DateTime<Utc>>,
    pub total_exposure: Usd,
    pub daily_pnl: Usd,
    pub consecutive_losses: i32,
    pub updated_at: DateTime<Utc>,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {}

impl ActiveModelBehavior for ActiveModel {}
