//! `report` table entity — daily and weekly trading report snapshots.
//!
//! Report rows identify the period via `period_start` / `period_end`
//! dates and carry the full JSONB payload. The primary key `id`
//! is an application-generated string (e.g. `"daily_2025-06-01"`).

use crate::{enums::common::ReportType, types::ReportId};
use chrono::{DateTime, NaiveDate, Utc};
use sea_orm::entity::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel, Serialize, Deserialize)]
#[sea_orm(table_name = "report")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub id: ReportId,

    /// Report classification (daily / weekly).
    pub report_type: ReportType,

    /// Period start date.
    pub period_start: NaiveDate,

    /// Period end date (same as `period_start` for daily reports).
    pub period_end: NaiveDate,

    /// JSONB report payload.
    #[sea_orm(column_type = "JsonBinary")]
    pub payload: serde_json::Value,

    /// Timestamp when the report was generated.
    pub created_at: DateTime<Utc>,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {}

impl ActiveModelBehavior for ActiveModel {}
