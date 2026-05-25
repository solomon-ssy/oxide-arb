//! `accounting_periods` table entity.

use crate::enums::common::ReportType;
use crate::types::{PeriodId, Probability, Usd};
use chrono::{DateTime, NaiveDate, Utc};
use sea_orm::entity::prelude::*;

#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "accounting_period")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub period_id: PeriodId,
    pub period_type: ReportType,
    pub start_date: NaiveDate,
    pub end_date: NaiveDate,
    pub realized_pnl: Usd,
    pub total_fees: Usd,
    pub trade_count: i32,
    pub win_count: i32,
    pub loss_count: i32,
    pub miss_count: i32,
    pub max_drawdown: Usd,
    pub sharpe_ratio: Option<Probability>,
    pub finalized: bool,
    pub created_at: DateTime<Utc>,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {}

impl ActiveModelBehavior for ActiveModel {}
