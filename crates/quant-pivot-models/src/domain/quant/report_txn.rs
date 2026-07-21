//! Atomic report-creation transaction input.
//!
//! Produced by the composer and written as one Postgres transaction:
//! `account_snapshot → equity_snapshot → data_quality_snapshot → portfolio_plan → report → recommendations`.

use super::{
    NewAccountSnapshot, NewEntryConditionArtifact, NewEntryConditionInstance, NewEquitySnapshot,
    NewFeatureParityRun, NewPortfolioPlan, NewRecommendation, NewRecommendationReport,
    NewReportDataQualitySnapshot, NewReportFactDelivery, NewResearchJob, OrderIntentInfo,
    RecommendationReportInfo, ReportFactDeliveryInfo, ReportRunClaim, ReportRunInfo,
};
use crate::{domain::governance::NewOperationLog, types::FeatureParityStateId};

/// Sampled parity run and durable research job committed with one report.
///
/// The report row is the FK parent for the parity run. Keeping both children in
/// the same Postgres transaction prevents a published report from existing
/// without its mandatory replay job, and prevents an orphan replay from racing
/// a failed report commit.
#[derive(Debug, Clone)]
pub struct NewReportFeatureParity {
    pub run: NewFeatureParityRun,
    pub job: NewResearchJob,
}

/// All rows written atomically when a recommendation report is created.
#[derive(Debug, Clone)]
pub struct NewReportTransaction {
    /// Clear-latch generation acquired immediately before commit. The PG
    /// repository compares it under the same advisory lock used to open/clear
    /// the latch, closing the report-commit TOCTOU window.
    pub feature_parity_state_id: Option<FeatureParityStateId>,
    /// Decision-time capital snapshot (FK target for the report header).
    pub account_snapshot: NewAccountSnapshot,
    /// Strategy-capital equity curve snapshot used for drawdown-aware sizing.
    pub equity_snapshot: NewEquitySnapshot,
    /// Per-fire data-quality snapshot (FK target for the report header).
    pub data_quality_snapshot: NewReportDataQualitySnapshot,
    /// Portfolio plan (FK target for the report header).
    pub portfolio_plan: NewPortfolioPlan,
    /// The report header.
    pub report: NewRecommendationReport,
    /// The published recommendations.
    pub recommendations: Vec<NewRecommendation>,
    /// Immutable conditional artifacts referenced by this report's recommendations.
    pub entry_condition_artifacts: Vec<NewEntryConditionArtifact>,
    /// Exactly one durable shadow instance per published recommendation,
    /// including `NotRequired` instances for immediate entry.
    pub entry_condition_instances: Vec<NewEntryConditionInstance>,
    /// Mandatory at repository commit for every report. The composer leaves it
    /// empty only while the report id does not yet have its atomic parity job;
    /// the lifecycle coordinator fills it immediately before persistence.
    /// Pre-inference reports carry a report-scoped parity subject, never an
    /// invented model-run id.
    pub sampled_feature_parity: Option<NewReportFeatureParity>,
    /// Required durable fact-bundle outbox row. The lifecycle writes the
    /// content-addressed object first, then attaches this row before PG commit.
    pub fact_delivery: Option<NewReportFactDelivery>,
    /// Operator/audit trail row committed with the authoritative report rows.
    pub operation_log: NewOperationLog,
}

/// Atomic result of preparing a complete report and completing its build run.
#[derive(Debug, Clone)]
pub struct PreparedReportOutcome {
    pub report: RecommendationReportInfo,
    pub run: ReportRunInfo,
}

/// Result of settling a previously leased fact delivery.
///
/// Claim loss is an expected compare-and-set outcome: another transaction may
/// cancel, replace, or reclaim the delivery while this worker performs
/// `ClickHouse` I/O. Missing rows and persistence failures remain errors.
#[derive(Debug, Clone)]
#[must_use]
pub enum FactDeliverySettlement<T> {
    Applied(T),
    ClaimLost(Box<ReportFactDeliveryInfo>),
}

impl<T> FactDeliverySettlement<T> {
    pub fn into_applied(self) -> Result<T, Box<ReportFactDeliveryInfo>> {
        match self {
            Self::Applied(value) => Ok(value),
            Self::ClaimLost(delivery) => Err(delivery),
        }
    }
}

/// Atomic publication result used to emit post-commit events.
#[derive(Debug, Clone)]
pub struct PublishReportOutcome {
    pub report: RecommendationReportInfo,
    pub delivery: ReportFactDeliveryInfo,
    pub superseded_reports: Vec<RecommendationReportInfo>,
    pub obsoleted_reports: Vec<RecommendationReportInfo>,
    pub invalidated_intents: Vec<OrderIntentInfo>,
}

impl PublishReportOutcome {
    /// Whether the candidate became the new current authority.
    #[must_use]
    pub const fn published(&self) -> bool {
        self.report.status.is_current_authority()
    }
}

/// Prepared report write command, including the exact lease CAS identity.
#[derive(Debug, Clone)]
pub struct CreatePreparedReport {
    pub run_claim: ReportRunClaim,
    pub transaction: NewReportTransaction,
}
