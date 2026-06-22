//! Repository trait for report persistence.

use chrono::NaiveDate;
use quant_pivot_error::storage::StorageError;
use quant_pivot_models::{
    domain::{ReportInfo, UpsertReport},
    enums::common::ReportType,
};

/// Data access for the `report` table.
#[async_trait::async_trait]
pub trait ReportRepository: Send + Sync {
    /// Insert or upsert a report (replaces payload on conflict).
    async fn upsert(&self, report: UpsertReport) -> Result<(), StorageError>;

    /// Persist a daily report as a JSONB payload.
    async fn save_daily(
        &self,
        date: NaiveDate,
        payload: serde_json::Value,
    ) -> Result<(), StorageError>;

    /// Persist a weekly report as a JSONB payload.
    async fn save_weekly(
        &self,
        week_start: NaiveDate,
        week_end: NaiveDate,
        payload: serde_json::Value,
    ) -> Result<(), StorageError>;

    /// Find reports of a given type, newest first.
    async fn find_by_type(
        &self,
        report_type: ReportType,
        limit: u64,
    ) -> Result<Vec<ReportInfo>, StorageError>;

    /// Daily settlement reports whose calendar `date` (`period_start`) falls in
    /// the inclusive `[from, to]` UTC range, ordered oldest first.
    async fn find_daily_between(
        &self,
        from: NaiveDate,
        to: NaiveDate,
    ) -> Result<Vec<ReportInfo>, StorageError>;

    /// Find the most recent report of a given type.
    async fn find_latest(
        &self,
        report_type: ReportType,
    ) -> Result<Option<ReportInfo>, StorageError>;
}
