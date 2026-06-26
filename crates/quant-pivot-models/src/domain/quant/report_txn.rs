//! Atomic report-creation transaction input.
//!
//! Produced by the 04.2 composer and written as one Postgres transaction:
//! `account_snapshot → data_quality_snapshot → portfolio_plan → report → recommendations`.

use super::{
    NewAccountSnapshot, NewPortfolioPlan, NewRecommendation, NewRecommendationReport,
    NewReportDataQualitySnapshot,
};
use crate::domain::governance::NewOperationLog;

/// All rows written atomically when a recommendation report is created.
#[derive(Debug, Clone)]
pub struct NewReportTransaction {
    /// Decision-time capital snapshot (FK target for the report header).
    pub account_snapshot: NewAccountSnapshot,
    /// Per-fire data-quality snapshot (FK target for the report header).
    pub data_quality_snapshot: NewReportDataQualitySnapshot,
    /// Portfolio plan (FK target for the report header).
    pub portfolio_plan: NewPortfolioPlan,
    /// The report header.
    pub report: NewRecommendationReport,
    /// The published recommendations.
    pub recommendations: Vec<NewRecommendation>,
    /// Operator/audit trail row committed with the authoritative report rows.
    pub operation_log: NewOperationLog,
}
