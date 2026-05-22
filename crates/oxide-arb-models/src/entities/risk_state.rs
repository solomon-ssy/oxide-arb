//! `risk_engine_state` table entity (singleton row).

use crate::enums::risk::{BreakerStateName, CircuitBreakerLevel};
use crate::types::Usd;
use chrono::{DateTime, NaiveDate, Utc};
use oxide_arb_macros::ActiveModelDefaults;
use sea_orm::entity::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(
    Clone, Debug, PartialEq, Eq, DeriveEntityModel, ActiveModelDefaults, Serialize, Deserialize,
)]
#[sea_orm(table_name = "risk_engine_state")]
#[active_defaults(
    default(id, 1_i32),
    default(breaker_state, BreakerStateName::Active),
    default(is_halted, false),
    default(consecutive_misses, 0_i32),
    default(cooldown_multiplier, 1_i32),
    default(total_exposure, Usd::ZERO),
    default(hourly_loss_usd, Usd::ZERO),
    default(hourly_fee_usd, Usd::ZERO),
    timestamp(hourly_window_start),
    default(daily_loss_usd, Usd::ZERO),
    default(daily_fee_usd, Usd::ZERO),
    default(daily_pnl, Usd::ZERO),
    default(daily_window_start, Utc::now().date_naive()),
    default(weekly_loss_usd, Usd::ZERO),
    default(weekly_window_start, Utc::now().date_naive()),
    timestamp(updated_at, always),
)]
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
    pub hourly_window_start: DateTime<Utc>,
    pub daily_loss_usd: Usd,
    pub daily_fee_usd: Usd,
    pub daily_pnl: Usd,
    pub daily_window_start: NaiveDate,
    pub weekly_loss_usd: Usd,
    pub weekly_window_start: NaiveDate,
    pub last_emergency_at: Option<DateTime<Utc>>,
    #[sea_orm(column_type = "Text", nullable)]
    pub last_emergency_reason: Option<String>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {}
