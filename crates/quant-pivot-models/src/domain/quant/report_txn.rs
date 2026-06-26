//! Atomic report-creation transaction input.
//!
//! Produced by the 04.2 composer and written as one Postgres transaction:
//! `account_snapshot → portfolio_plan → report → recommendations`. Grouping the
//! rows guarantees the report's `account_snapshot_ref` / `portfolio_plan_id`
//! foreign keys are satisfied within the same transaction.

use super::{NewAccountSnapshot, NewPortfolioPlan, NewRecommendation, NewRecommendationReport};
use crate::domain::governance::NewOperationLog;

/// All rows written atomically when a recommendation report is created.
#[derive(Debug, Clone)]
pub struct NewReportTransaction {
    /// Decision-time capital snapshot (FK target for the report header).
    pub account_snapshot: NewAccountSnapshot,
    /// Portfolio plan (FK target for the report header).
    pub portfolio_plan: NewPortfolioPlan,
    /// The report header.
    pub report: NewRecommendationReport,
    /// The published recommendations.
    pub recommendations: Vec<NewRecommendation>,
    /// Operator/audit trail row committed with the authoritative report rows.
    pub operation_log: NewOperationLog,
}
