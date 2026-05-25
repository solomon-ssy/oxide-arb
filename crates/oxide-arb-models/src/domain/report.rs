//! Report domain DTOs for the `report` table.

use crate::enums::common::ReportType;
use crate::types::ReportId;
use chrono::{DateTime, NaiveDate, Utc};
use sea_orm::{DeriveIntoActiveModel, DerivePartialModel, FromQueryResult};
use serde::{Deserialize, Serialize};

// ── Read ──────────────────────────────────────────────────────────────

/// DB row projection for the `report` table.
#[derive(Debug, Clone, Serialize, Deserialize, DerivePartialModel, FromQueryResult)]
#[sea_orm(entity = "crate::entities::report::Entity")]
pub struct ReportInfo {
    pub id: ReportId,
    pub report_type: ReportType,
    pub period_start: NaiveDate,
    pub period_end: NaiveDate,
    pub payload: serde_json::Value,
    pub created_at: DateTime<Utc>,
}

info_from_model!(ReportInfo, crate::entities::report::Model, {
    id, report_type, period_start, period_end, payload, created_at,
});

// ── Write ─────────────────────────────────────────────────────────────

/// Upsert payload for the `report` table (ON CONFLICT updates payload).
#[derive(Debug, Clone, DeriveIntoActiveModel)]
#[sea_orm(active_model = "super::super::entities::report::ActiveModel")]
pub struct UpsertReport {
    pub id: ReportId,
    pub report_type: ReportType,
    pub period_start: NaiveDate,
    pub period_end: NaiveDate,
    pub payload: serde_json::Value,
}

impl UpsertReport {
    /// Convenience constructor for a daily report.
    #[must_use]
    pub fn daily(date: NaiveDate, payload: serde_json::Value) -> Self {
        Self {
            id: ReportId::new(format!("daily_{date}")),
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
            id: ReportId::new(format!("weekly_{week_start}_{week_end}")),
            report_type: ReportType::Weekly,
            period_start: week_start,
            period_end: week_end,
            payload,
        }
    }
}
