//! Report domain DTOs for the `report` table.

use crate::enums::common::ReportType;
use chrono::NaiveDate;
use sea_orm::DeriveIntoActiveModel;

/// Payload to insert a new report record.
///
/// Derives `DeriveIntoActiveModel` — calling `.into_active_model()` produces
/// an `ActiveModel` with these fields `Set(...)` and all others `NotSet`.
/// The database default fills `created_at` at insert time.
#[derive(Debug, Clone, DeriveIntoActiveModel)]
#[sea_orm(active_model = "super::super::entities::report::ActiveModel")]
pub struct NewReport {
    pub id: String,
    pub report_type: ReportType,
    pub period_start: NaiveDate,
    pub period_end: NaiveDate,
    pub payload: serde_json::Value,
}

impl NewReport {
    /// Convenience constructor for a daily report.
    #[must_use]
    pub fn daily(date: NaiveDate, payload: serde_json::Value) -> Self {
        Self {
            id: format!("daily_{date}"),
            report_type: ReportType::Daily,
            period_start: date,
            period_end: date,
            payload,
        }
    }

    /// Convenience constructor for a weekly report.
    #[must_use]
    pub fn weekly(week_start: NaiveDate, week_end: NaiveDate, payload: serde_json::Value) -> Self {
        Self {
            id: format!("weekly_{week_start}_{week_end}"),
            report_type: ReportType::Weekly,
            period_start: week_start,
            period_end: week_end,
            payload,
        }
    }
}
