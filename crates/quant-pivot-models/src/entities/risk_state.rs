//! `risk_engine_state` table entity (singleton row).

use crate::{
    enums::risk::{BreakerStateName, CircuitBreakerLevel},
    types::Usd,
};
use chrono::{DateTime, NaiveDate, Utc};
use sea_orm::entity::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel, Serialize, Deserialize)]
#[sea_orm(table_name = "risk_engine_state")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub id: i32,
    pub breaker_state: BreakerStateName,
    pub breaker_level: Option<CircuitBreakerLevel>,
    pub is_halted: bool,
    #[sea_orm(column_type = "Text", nullable)]
    pub halt_reason: Option<String>,
    pub consecutive_misses: i32,
    pub cooldown_until: Option<DateTime<Utc>>,
    pub cooldown_multiplier: i32,
    pub total_exposure: Usd,
    pub hourly_loss_usd: Usd,
    pub hourly_fee_usd: Usd,
    pub hourly_trade_count: i32,
    pub hourly_success_count: i32,
    pub hourly_miss_count: i32,
    pub hourly_window_start: DateTime<Utc>,
    pub daily_loss_usd: Usd,
    pub daily_fee_usd: Usd,
    pub daily_pnl: Usd,
    pub daily_budget_spent: Usd,
    pub daily_trade_count: i32,
    pub daily_success_count: i32,
    pub daily_miss_count: i32,
    pub daily_window_start: NaiveDate,
    pub weekly_loss_usd: Usd,
    pub weekly_trade_count: i32,
    pub weekly_window_start: NaiveDate,
    pub hwm_equity: Usd,
    /// Lifetime cumulative realized `PnL` (same accounting basis as `daily_pnl`).
    /// Write-only telemetry: never read by any pre-trade gate.
    pub total_realized_pnl: Usd,
    pub last_emergency_at: Option<DateTime<Utc>>,
    #[sea_orm(column_type = "Text", nullable)]
    pub last_emergency_reason: Option<String>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {}

impl ActiveModelBehavior for ActiveModel {}
